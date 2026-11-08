package pipeline

import (
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
)

type AggSpec struct {
	Func string // count, sum, avg, min, max, distinct, first, last
	Col  int    // -1 when counting rows
	Name string
}

// ParseAggs parses "count, sum(amount), avg(amount):average, max(date)".
func ParseAggs(header []string, spec string) ([]AggSpec, error) {
	var out []AggSpec
	for _, part := range strings.Split(spec, ",") {
		expr, alias, _ := strings.Cut(strings.TrimSpace(part), ":")
		if expr == "" {
			continue
		}
		fn, col := expr, ""
		if i := strings.IndexByte(expr, '('); i > 0 && strings.HasSuffix(expr, ")") {
			fn, col = expr[:i], strings.TrimSpace(expr[i+1:len(expr)-1])
		}
		fn = strings.ToLower(strings.TrimSpace(fn))
		a := AggSpec{Func: fn, Col: -1, Name: strings.TrimSpace(alias)}
		switch fn {
		case "count":
		case "sum", "avg", "min", "max", "distinct", "first", "last":
			if col == "" {
				return nil, fmt.Errorf("%s needs a column", fn)
			}
		default:
			return nil, fmt.Errorf("unknown aggregate %q", fn)
		}
		if col != "" && col != "*" {
			if a.Col = Index(header, col); a.Col < 0 {
				return nil, fmt.Errorf("unknown column %q", col)
			}
		}
		if a.Name == "" {
			a.Name = "count"
			if a.Col >= 0 {
				a.Name = fn + "_" + SnakeHeader(header[a.Col])
			}
		}
		out = append(out, a)
	}
	return out, nil
}

// accum holds the running state of one aggregate in one group.
type accum struct {
	n           int64
	sum         float64
	minF, maxF  float64
	minS, maxS  string
	first, last string
	numeric     bool
	seen        map[string]struct{}
}

func (a *accum) add(v string, spec AggSpec) {
	if spec.Func == "count" {
		if spec.Col < 0 || v != "" {
			a.n++
		}
		return
	}
	if v == "" {
		return
	}
	if a.n == 0 {
		a.first, a.numeric = v, true
	}
	a.n++
	a.last = v
	switch spec.Func {
	case "distinct":
		if a.seen == nil {
			a.seen = map[string]struct{}{}
		}
		a.seen[v] = struct{}{}
	case "sum", "avg", "min", "max":
		f, ok := ParseFloat(v)
		a.numeric = a.numeric && ok
		if ok {
			a.sum += f
			a.minF, a.maxF = fold(a.n, a.minF, f, math.Min), fold(a.n, a.maxF, f, math.Max)
		}
		if a.n == 1 || v < a.minS {
			a.minS = v
		}
		if a.n == 1 || v > a.maxS {
			a.maxS = v
		}
	}
}

// fold combines the running value with v, or starts from v on the first one.
func fold(n int64, cur, v float64, f func(a, b float64) float64) float64 {
	if n == 1 {
		return v
	}
	return f(cur, v)
}

func (a *accum) result(spec AggSpec) string {
	switch spec.Func {
	case "count":
		return strconv.FormatInt(a.n, 10)
	case "distinct":
		return strconv.Itoa(len(a.seen))
	case "first":
		return a.first
	case "last":
		return a.last
	case "sum":
		return fmtNum(a.sum)
	case "avg":
		if a.n == 0 {
			return ""
		}
		return fmtNum(a.sum / float64(a.n))
	case "min":
		if a.numeric {
			return fmtNum(a.minF)
		}
		return a.minS
	case "max":
		if a.numeric {
			return fmtNum(a.maxF)
		}
		return a.maxS
	}
	return ""
}

// fmtNum prints whole numbers without decimals and others with two.
func fmtNum(f float64) string {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return ""
	}
	if f == math.Trunc(f) && math.Abs(f) < 1e15 {
		return strconv.FormatInt(int64(f), 10)
	}
	return strconv.FormatFloat(f, 'f', 2, 64)
}

// Aggregate groups rows by the named columns and computes aggs per group.
// Memory grows with the number of distinct groups, not the number of rows.
func Aggregate(in *Table, groupBy []string, aggs []AggSpec) (*Table, error) {
	gidx := make([]int, len(groupBy))
	hdr := make([]string, 0, len(groupBy)+len(aggs))
	for i, g := range groupBy {
		if gidx[i] = Index(in.Header, g); gidx[i] < 0 {
			return nil, fmt.Errorf("unknown group column %q", g)
		}
		hdr = append(hdr, in.Header[gidx[i]])
	}
	for _, a := range aggs {
		hdr = append(hdr, a.Name)
	}
	out, em := in.child(hdr)
	go func() {
		defer em.close()
		type group struct {
			key  Row
			accs []accum
		}
		groups := map[string]*group{}
		var order []string
		var kb strings.Builder
		for batch := range in.Rows {
			for _, r := range batch {
				kb.Reset()
				for _, i := range gidx {
					kb.WriteString(r.Get(i))
					kb.WriteByte(0)
				}
				k := kb.String()
				g := groups[k]
				if g == nil {
					g = &group{key: make(Row, len(gidx)), accs: make([]accum, len(aggs))}
					for j, i := range gidx {
						g.key[j] = r.Get(i)
					}
					groups[k] = g
					order = append(order, k)
				}
				for j, a := range aggs {
					g.accs[j].add(r.Get(a.Col), a)
				}
			}
		}
		if mon := in.monitor(); mon != nil && len(groups) > 100_000 {
			mon.Note("group-by produced %s groups; memory grows with group count.", commas(int64(len(groups))))
		}
		for _, k := range order {
			g := groups[k]
			row := append(make(Row, 0, len(hdr)), g.key...)
			for j, a := range aggs {
				row = append(row, g.accs[j].result(a))
			}
			if !em.send(row) {
				return
			}
		}
	}()
	return out, nil
}

// Sort orders rows by key; a leading "-" means descending. Numbers sort
// numerically, nulls last. This stage buffers every row, so apply it after
// filters or aggregates when the input is large.
func Sort(in *Table, key string) (*Table, error) {
	desc := strings.HasPrefix(key, "-")
	i := Index(in.Header, strings.TrimPrefix(key, "-"))
	if i < 0 {
		return nil, fmt.Errorf("unknown sort column %q", key)
	}
	out, em := in.child(in.Header)
	go func() {
		defer em.close()
		var rows []Row
		for batch := range in.Rows {
			for _, r := range batch {
				rows = append(rows, r)
			}
		}
		if mon := in.monitor(); mon != nil && len(rows) > 100_000 {
			mon.Note("sort buffered %s rows in memory; sort after -group or -limit when the input is large.", commas(int64(len(rows))))
		}
		// Parse each key once instead of on every comparison.
		keyed := make([]keyedRow, len(rows))
		for n, r := range rows {
			keyed[n] = keyedRow{sortKeyOf(r.Get(i)), r}
		}
		rows = nil
		slices.SortStableFunc(keyed, func(a, b keyedRow) int {
			if a.key.empty != b.key.empty {
				if a.key.empty { // empty cells last in both directions
					return 1
				}
				return -1
			}
			c := a.key.compare(b.key)
			if desc {
				return -c
			}
			return c
		})
		for _, k := range keyed {
			if !em.send(k.row) {
				return
			}
		}
	}()
	return out, nil
}

// sortKey is a cell parsed once for sorting: numbers compare numerically,
// text case-insensitively, as in compare.
type sortKey struct {
	num   float64
	text  string
	isNum bool
	empty bool
}

type keyedRow struct {
	key sortKey
	row Row
}

func sortKeyOf(v string) sortKey {
	if v == "" {
		return sortKey{empty: true}
	}
	k := sortKey{text: strings.ToLower(v)}
	k.num, k.isNum = ParseFloat(v)
	return k
}

func (a sortKey) compare(b sortKey) int {
	if a.isNum && b.isNum {
		return cmpFloat(a.num, b.num)
	}
	return strings.Compare(a.text, b.text)
}

// compare orders two cell values: numbers numerically, text
// case-insensitively, and empty values after everything else.
func compare(a, b string) int {
	switch {
	case a == b:
		return 0
	case a == "":
		return 1
	case b == "":
		return -1
	}
	if fa, ok := ParseFloat(a); ok {
		if fb, ok := ParseFloat(b); ok {
			return cmpFloat(fa, fb)
		}
	}
	return strings.Compare(strings.ToLower(a), strings.ToLower(b))
}

func cmpFloat(a, b float64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}
