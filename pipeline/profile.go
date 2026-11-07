package pipeline

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
)

// ColumnStats is the profile of one column.
type ColumnStats struct {
	Name     string       `json:"name"`
	Kind     string       `json:"kind"`
	Count    int64        `json:"count"`
	Nulls    int64        `json:"nulls"`
	NullPct  float64      `json:"null_pct"`
	Distinct int64        `json:"distinct"` // exact up to the cap, then estimated
	Min      string       `json:"min,omitempty"`
	Max      string       `json:"max,omitempty"`
	Mean     *float64     `json:"mean,omitempty"`
	StdDev   *float64     `json:"stddev,omitempty"`
	Top      []ValueCount `json:"top,omitempty"`
	Examples []string     `json:"examples,omitempty"`
	kind     Kind
	sum, sq  float64
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
	Rows      int64         `json:"rows"`
	Columns   []ColumnStats `json:"columns"`
	Duplicate int64         `json:"duplicate_rows"`
	Issues    []string      `json:"issues"`
}

// Profile consumes the whole table and returns summary statistics.
// Distinct values are tracked exactly up to distinctCap per column, after
// which a hash sketch keeps the estimate cheap.
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
		cols[i] = ColumnStats{Name: h, seen: map[string]int64{}}
		if i < len(kinds) && kinds[i] != KindEmpty {
			cols[i].kind, cols[i].fixed = kinds[i], true
		}
	}
	rowHashes := map[uint64]struct{}{}
	for r := range t.Rows {
		rep.Rows++
		if rep.Rows <= int64(distinctCap)*10 {
			h := hashRow(r)
			if _, dup := rowHashes[h]; dup {
				rep.Duplicate++
			} else {
				rowHashes[h] = struct{}{}
			}
		}
		for i := range cols {
			c := &cols[i]
			v := strings.TrimSpace(r.Get(i))
			if IsNull(v) {
				c.Nulls++
				continue
			}
			c.Count++
			k := c.kind
			if !c.fixed && c.kind != KindText {
				k = Infer(v)
				c.kind = Widen(c.kind, k)
			}
			if f, ok := ParseFloat(v); ok && (k == KindInt || k == KindFloat) {
				c.sum += f
				c.sq += f * f
				if c.Count == 1 || f < c.minF {
					c.minF = f
				}
				if c.Count == 1 || f > c.maxF {
					c.maxF = f
				}
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
						c.sketch.add(hashStr(s))
					}
				}
			} else {
				c.sketch.add(hashStr(v))
			}
			if len(c.Examples) < 3 {
				dup := false
				for _, e := range c.Examples {
					if e == v {
						dup = true
					}
				}
				if !dup {
					c.Examples = append(c.Examples, v)
				}
			}
		}
	}
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
				mean := c.sum / float64(c.Count)
				sd := math.Sqrt(math.Max(0, c.sq/float64(c.Count)-mean*mean))
				mean, sd = round(mean, 2), round(sd, 2)
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
		if c.Distinct == 1 && c.Count > 1 {
			rep.Issues = append(rep.Issues, fmt.Sprintf("%s: constant value %q", c.Name, c.Min))
		}
		if c.Count > 0 && c.Distinct == c.Count && c.kind == KindText && rep.Rows > 1 {
			rep.Issues = append(rep.Issues, fmt.Sprintf("%s: unique per row (identifier?)", c.Name))
		}
		if c.kind == KindText && len(c.Top) > 1 {
			keys := map[string]int{}
			for _, tv := range c.Top {
				keys[strings.ToLower(tv.Value)]++
			}
			for k, n := range keys {
				if n > 1 {
					rep.Issues = append(rep.Issues, fmt.Sprintf("%s: inconsistent casing for %q", c.Name, k))
				}
			}
		}
	}
	if rep.Duplicate > 0 {
		rep.Issues = append(rep.Issues, fmt.Sprintf("%d duplicate rows", rep.Duplicate))
	}
	rep.Columns = cols
	return rep
}

func hashStr(s string) uint64 {
	const prime = 1099511628211
	var h uint64 = 14695981039346656037
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime
	}
	return h
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
	fmt.Fprintf(w, "rows: %d   columns: %d   duplicate rows: %d\n\n", r.Rows, len(r.Columns), r.Duplicate)
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

func trunc(s string, n int) string {
	rs := []rune(s)
	if len(rs) <= n {
		return s
	}
	return string(rs[:n-1]) + "…"
}
