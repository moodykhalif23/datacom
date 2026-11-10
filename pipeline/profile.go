package pipeline

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"runtime"
	"sort"
	"strings"
	"sync"
)

// ColumnStats holds the profile of one column.
type ColumnStats struct {
	Name     string           `json:"name"`
	Kind     string           `json:"kind"`
	Count    int64            `json:"count"`
	Nulls    int64            `json:"nulls"`
	NullPct  float64          `json:"null_pct"`
	Distinct int64            `json:"distinct"` // exact up to the cap, then estimated
	Min      string           `json:"min,omitempty"`
	Max      string           `json:"max,omitempty"`
	Mean     *float64         `json:"mean,omitempty"`
	StdDev   *float64         `json:"stddev,omitempty"`
	Top      []ValueCount     `json:"top,omitempty"`
	Personal map[string]int64 `json:"personal_data,omitempty"`
	kind     Kind
	idCol    bool
	piiSeen  int64
	pii      [4]int64
	mean, m2 float64 // Welford running mean and squared distance
	minF     float64
	maxF     float64
	minS     string
	maxS     string
	seen     map[string]int64
	capped   bool
	sketch   *hll
	fixed    bool
}

// ValueCount is a value and how often it appeared.
type ValueCount struct {
	Value string `json:"value"`
	Count int64  `json:"count"`
}

// Report is a whole-dataset profile.
type Report struct {
	Rows       int64         `json:"rows"`
	Columns    []ColumnStats `json:"columns"`
	Duplicate  int64         `json:"duplicate_rows"`
	DupChecked int64         `json:"rows_checked_for_duplicates"`
	Issues     []string      `json:"issues"`
}

func Profile(t *Table, kinds []Kind, topN, distinctCap int) *Report {
	if topN <= 0 {
		topN = 5
	}
	if distinctCap <= 0 {
		distinctCap = 100_000
	}
	rep := &Report{}
	cols := make([]ColumnStats, len(t.Header))
	for i, h := range t.Header {
		cols[i] = ColumnStats{Name: h, seen: map[string]int64{}, idCol: idColumn(h)}
	}
	workers := min(runtime.GOMAXPROCS(0), len(cols))
	if n, ok := t.run.ctx.Value(parallelKey{}).(int); ok && n > 0 {
		workers = min(workers, n)
	}
	workers = max(workers, 1)
	dupCap := int64(distinctCap) * 10 // bounds the exact duplicate scan
	feeds := make([]chan []Row, workers+1)
	var wg sync.WaitGroup
	for w := range feeds {
		feeds[w] = make(chan []Row, 4)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if w == workers {
				rep.scanDuplicates(feeds[w], dupCap)
				return
			}
			for b := range feeds[w] {
				for _, r := range b {
					for i := w; i < len(cols); i += workers {
						cols[i].add(r.Get(i), distinctCap)
					}
				}
			}
		}()
	}
	batch := make([]Row, 0, profileBatch)
	for r := range t.Rows {
		rep.Rows++
		if rep.Rows == 1 {
			// Clean fills kinds before it emits its first row, so reading
			// them after the first receive is race-free.
			for i := range cols {
				if i < len(kinds) && kinds[i] != KindEmpty {
					cols[i].kind, cols[i].fixed = kinds[i], true
				}
			}
		}
		if batch = append(batch, r); len(batch) == profileBatch {
			for _, f := range feeds {
				f <- batch
			}
			batch = make([]Row, 0, profileBatch)
		}
	}
	for _, f := range feeds {
		if len(batch) > 0 {
			f <- batch
		}
		close(f)
	}
	wg.Wait()
	for i := range cols {
		c := &cols[i]
		if c.kind == KindEmpty {
			c.kind = KindText
		}
		c.Kind = c.kind.String()
		if rep.Rows > 0 {
			c.NullPct = round(100*float64(c.Nulls)/float64(rep.Rows), 1)
		}
		if c.capped {
			c.Distinct = c.sketch.count()
		} else {
			c.Distinct = int64(len(c.seen))
		}
		switch c.kind {
		case KindInt, KindFloat:
			c.Min, c.Max = fmtNum(c.minF), fmtNum(c.maxF)
			if c.Count > 0 {
				mean, sd := round(c.mean, 2), round(math.Sqrt(c.m2/float64(c.Count)), 2)
				c.Mean, c.StdDev = &mean, &sd
			}
		default:
			c.Min, c.Max = c.minS, c.maxS
		}
		if !c.capped {
			top := make([]ValueCount, 0, len(c.seen))
			for v, n := range c.seen {
				top = append(top, ValueCount{v, n})
			}
			sort.Slice(top, func(a, b int) bool {
				if top[a].Count != top[b].Count {
					return top[a].Count > top[b].Count
				}
				return top[a].Value < top[b].Value
			})
			if len(top) > topN {
				top = top[:topN]
			}
			c.Top = top
		}
		c.seen, c.sketch = nil, nil
		if c.NullPct >= 50 {
			rep.Issues = append(rep.Issues, fmt.Sprintf("%s: %.0f%% missing", c.Name, c.NullPct))
		}
		if issue := c.personalIssue(); issue != "" {
			rep.Issues = append(rep.Issues, issue)
		}
		if c.Distinct == 1 && c.Count > 1 {
			rep.Issues = append(rep.Issues, fmt.Sprintf("%s: constant value %q", c.Name, c.Min))
		}
		if rep.Rows >= 100 && c.Distinct == c.Count && c.kind == KindText {
			rep.Issues = append(rep.Issues, fmt.Sprintf("%s: unique per row (identifier?)", c.Name))
		}
		if c.kind == KindText && len(c.Top) > 1 {
			keys := map[string]int{}
			for _, tv := range c.Top {
				keys[strings.ToLower(tv.Value)]++
			}
			for k, n := range keys {
				if n > 1 {
					rep.Issues = append(rep.Issues, fmt.Sprintf("%s: inconsistent casing for %q (clean -fold-case picks the most common spelling)", c.Name, k))
				}
			}
		}
	}
	if rep.Duplicate > 0 {
		rep.Issues = append(rep.Issues, rep.duplicates())
	}
	rep.Columns = cols
	return rep
}

const profileBatch = 256

func (rep *Report) scanDuplicates(feed <-chan []Row, limit int64) {
	hashes := map[uint64]struct{}{}
	for b := range feed {
		for _, r := range b {
			if rep.DupChecked == limit {
				break
			}
			rep.DupChecked++
			h := hashRow(r)
			if _, dup := hashes[h]; dup {
				rep.Duplicate++
			} else {
				hashes[h] = struct{}{}
			}
		}
	}
}

func (c *ColumnStats) add(v string, distinctCap int) {
	v = strings.TrimSpace(v)
	if IsNull(v) {
		c.Nulls++
		return
	}
	c.Count++
	k := c.kind
	if !c.fixed && c.kind != KindText {
		k = Infer(v)
		c.kind = Widen(c.kind, k)
	}
	if f, ok := numeric(v, k); ok {
		delta := f - c.mean
		c.mean += delta / float64(c.Count)
		c.m2 += delta * (f - c.mean)
		if c.Count == 1 || f < c.minF {
			c.minF = f
		}
		if c.Count == 1 || f > c.maxF {
			c.maxF = f
		}
	}
	if c.piiSeen < piiSample {
		c.piiSeen++
		c.scanPersonal(v)
	}
	if c.Count == 1 || v < c.minS {
		c.minS = v
	}
	if c.Count == 1 || v > c.maxS {
		c.maxS = v
	}
	if !c.capped {
		c.seen[v]++
		if len(c.seen) > distinctCap {
			c.capped = true
			c.sketch = newHLL()
			for s := range c.seen {
				c.sketch.add(hash64(hashSeed, s))
			}
		}
	} else {
		c.sketch.add(hash64(hashSeed, v))
	}
}

// piiSample is how many values per column the profile checks for personal data.
const piiSample = 10_000

func (c *ColumnStats) scanPersonal(v string) {
	var n [4]int
	if c.idCol && hasDigit(v) {
		n[maskID] = 1
	} else {
		RedactText(v, RedactAll, "", &n)
	}
	for k, m := range n {
		if m > 0 {
			c.pii[k]++
		}
	}
}

// personalIssue names the personal data found in the column and the flag
// that masks it.
func (c *ColumnStats) personalIssue() string {
	var kinds, flags []string
	for k, m := range c.pii {
		if m == 0 {
			continue
		}
		if c.Personal == nil {
			c.Personal = map[string]int64{}
		}
		c.Personal[redactNames[k].single] = m
		kinds = append(kinds, commas(m)+" "+redactNames[k].noun+plural(m))
		flags = append(flags, redactNames[k].plural)
	}
	if len(kinds) == 0 {
		return ""
	}
	where := ""
	if c.piiSeen < c.Count {
		where = " in the first " + commas(c.piiSeen) + " values"
	}
	return fmt.Sprintf("%s: personal data%s, %s (clean -redact %s masks it)", c.Name, where, strings.Join(kinds, ", "), strings.Join(flags, ","))
}

func plural(n int64) string {
	if n == 1 {
		return ""
	}
	return "s"
}

// duplicates describes the duplicate count and, when the scan stopped early,
// how much of the file it covered, so a partial count never reads as a total.
func (r *Report) duplicates() string {
	s := fmt.Sprintf("%d duplicate rows", r.Duplicate)
	if r.DupChecked < r.Rows {
		s += fmt.Sprintf(" in the first %s rows (clean -dedupe counts them all)", commas(r.DupChecked))
	}
	return s
}

func round(f float64, places int) float64 {
	p := math.Pow(10, float64(places))
	return math.Round(f*p) / p
}

// WriteJSON emits the report as indented JSON.
func (r *Report) WriteJSON(w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(r)
}

// WriteText emits a human-readable report.
func (r *Report) WriteText(w io.Writer) {
	fmt.Fprintf(w, "rows: %d   columns: %d   duplicate rows: %d", r.Rows, len(r.Columns), r.Duplicate)
	if r.DupChecked < r.Rows {
		fmt.Fprintf(w, " in the first %s rows", commas(r.DupChecked))
	}
	fmt.Fprint(w, "\n\n")
	for _, c := range r.Columns {
		fmt.Fprintf(w, "%s  (%s)\n", c.Name, c.Kind)
		fmt.Fprintf(w, "  filled %d   missing %d (%.1f%%)   distinct %d\n", c.Count, c.Nulls, c.NullPct, c.Distinct)
		if c.Mean != nil {
			fmt.Fprintf(w, "  min %s   max %s   mean %.2f   stddev %.2f\n", c.Min, c.Max, *c.Mean, *c.StdDev)
		} else if c.Min != "" {
			fmt.Fprintf(w, "  min %q   max %q\n", trunc(c.Min, 30), trunc(c.Max, 30))
		}
		if len(c.Top) > 0 && c.Distinct > 1 {
			parts := make([]string, len(c.Top))
			for i, tv := range c.Top {
				parts[i] = fmt.Sprintf("%s (%d)", trunc(tv.Value, 24), tv.Count)
			}
			fmt.Fprintf(w, "  top: %s\n", strings.Join(parts, ", "))
		}
		fmt.Fprintln(w)
	}
	if len(r.Issues) > 0 {
		fmt.Fprintln(w, "issues:")
		for _, is := range r.Issues {
			fmt.Fprintf(w, "  - %s\n", is)
		}
	}
}

// numeric parses v only for numeric columns.
func numeric(v string, k Kind) (float64, bool) {
	if k != KindInt && k != KindFloat {
		return 0, false
	}
	return ParseFloat(v)
}
