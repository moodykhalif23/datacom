package pipeline

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

// AggSpec is one aggregate: Func over Col, output as Name.
type AggSpec struct {
	Func string // count, sum, avg, min, max, distinct, first, last
	Col  int    // -1 for count(*)
	Name string
}

// ParseAggs parses "count, sum(amount), avg(amount):average, max(date)".
func ParseAggs(header []string, spec string) ([]AggSpec, error) {
	var out []AggSpec
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		expr, alias, _ := strings.Cut(part, ":")
		fn, col := expr, ""
		if i := strings.IndexByte(expr, '('); i > 0 && strings.HasSuffix(expr, ")") {
			fn, col = expr[:i], expr[i+1:len(expr)-1]
		}
		fn = strings.ToLower(strings.TrimSpace(fn))
		col = strings.TrimSpace(col)
		a := AggSpec{Func: fn, Col: -1}
		switch fn {
		case "count":
			if col != "" && col != "*" {
				a.Col = Index(header, col)
			}
		case "sum", "avg", "min", "max", "distinct", "first", "last":
			if col == "" {
				return nil, fmt.Errorf("%s needs a column", fn)
			}
			a.Col = Index(header, col)
			if a.Col < 0 {
				return nil, fmt.Errorf("unknown column %q", col)
			}
		default:
			return nil, fmt.Errorf("unknown aggregate %q", fn)
		}
		if a.Col == -1 && col != "" && col != "*" {
			return nil, fmt.Errorf("unknown column %q", col)
		}
		a.Name = strings.TrimSpace(alias)
		if a.Name == "" {
			if a.Col < 0 {
				a.Name = "count"
			} else {
				a.Name = fn + "_" + SnakeHeader(header[a.Col])
			}
		}
		out = append(out, a)
	}
	return out, nil
}

type accum struct {
	n        int64
	sum      float64
	min, max float64
	minS     string
	maxS     string
	numeric  bool
	seen     map[string]struct{}
	first    string
	last     string
	started  bool
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
	a.n++
	a.last = v
	if !a.started {
		a.first = v
	}
	switch spec.Func {
	case "distinct":
		if a.seen == nil {
			a.seen = map[string]struct{}{}
		}
		a.seen[v] = struct{}{}
	case "sum", "avg", "min", "max":
		f, ok := ParseFloat(v)
		if !a.started {
			a.numeric = ok
		}
		if ok && a.numeric {
			a.sum += f
			if !a.started || f < a.min {
				a.min = f
			}
			if !a.started || f > a.max {
				a.max = f
			}
		} else {
			a.numeric = false
			if !a.started || v < a.minS {
				a.minS = v
			}
			if !a.started || v > a.maxS {
				a.maxS = v
			}
		}
	}
	a.started = true
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
			return fmtNum(a.min)
		}
		return a.minS
	case "max":
		if a.numeric {
			return fmtNum(a.max)
		}
		return a.maxS
	}
	return ""
}

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
	for i, g := range groupBy {
		gidx[i] = Index(in.Header, g)
		if gidx[i] < 0 {
			return nil, fmt.Errorf("unknown group column %q", g)
		}
	}
	hdr := make([]string, 0, len(groupBy)+len(aggs))
	for _, i := range gidx {
		hdr = append(hdr, in.Header[i])
	}
	for _, a := range aggs {
		hdr = append(hdr, a.Name)
	}
	out := &Table{Header: hdr, Err: in.Err}
	ch := make(chan Row, 1024)
	out.Rows = ch
	go func() {
		defer close(ch)
		type group struct {
			key  []string
			accs []accum
		}
		groups := map[string]*group{}
		order := []string{}
		var kb strings.Builder
		for r := range in.Rows {
			kb.Reset()
			for _, i := range gidx {
				kb.WriteString(r.Get(i))
				kb.WriteByte(0)
			}
			k := kb.String()
			g := groups[k]
			if g == nil {
				g = &group{key: make([]string, len(gidx)), accs: make([]accum, len(aggs))}
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
		for _, k := range order {
			g := groups[k]
			row := make(Row, 0, len(hdr))
			row = append(row, g.key...)
			for j, a := range aggs {
				row = append(row, g.accs[j].result(a))
			}
			ch <- row
		}
	}()
	return out, nil
}

// Sort orders rows by key; a leading "-" means descending. Numeric values
// sort numerically. This stage buffers every row, so apply it after
// filters or aggregates when the input is large.
func Sort(in *Table, key string) (*Table, error) {
	desc := strings.HasPrefix(key, "-")
	key = strings.TrimPrefix(key, "-")
	i := Index(in.Header, key)
	if i < 0 {
		return nil, fmt.Errorf("unknown sort column %q", key)
	}
	out := &Table{Header: in.Header, Err: in.Err}
	ch := make(chan Row, 1024)
	out.Rows = ch
	go func() {
		defer close(ch)
		var rows []Row
		for r := range in.Rows {
			rows = append(rows, r)
		}
		sort.SliceStable(rows, func(a, b int) bool {
			va, vb := rows[a].Get(i), rows[b].Get(i)
			fa, oka := ParseFloat(va)
			fb, okb := ParseFloat(vb)
			var less bool
			switch {
			case va == "" && vb != "":
				return false // nulls last regardless of direction
			case vb == "" && va != "":
				return true
			case oka && okb:
				less = fa < fb
			default:
				less = strings.ToLower(va) < strings.ToLower(vb)
			}
			if desc {
				return !less && va != vb
			}
			return less
		})
		for _, r := range rows {
			ch <- r
		}
	}()
	return out, nil
}
