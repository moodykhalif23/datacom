package pipeline

import (
	"fmt"
	"strings"
)

// Filter keeps rows for which every expression is true.
func Filter(in *Table, conds []Expr) *Table {
	if len(conds) == 0 {
		return in
	}
	out, ch := in.child(in.Header)
	go func() {
		defer close(ch)
	rows:
		for r := range in.Rows {
			for _, c := range conds {
				if !c(r).truthy() {
					continue rows
				}
			}
			if !out.send(ch, r) {
				return
			}
		}
	}()
	return out
}

// Derive appends one column per "name = expression". Later definitions
// can use earlier ones.
func Derive(in *Table, defs []string) (*Table, error) {
	if len(defs) == 0 {
		return in, nil
	}
	hdr := append([]string(nil), in.Header...)
	exprs := make([]Expr, len(defs))
	for i, d := range defs {
		name, src, ok := strings.Cut(d, "=")
		if name = strings.TrimSpace(name); !ok || name == "" {
			return nil, fmt.Errorf("derive needs \"name = expression\", got %q", d)
		}
		e, err := Compile(hdr, src)
		if err != nil {
			return nil, err
		}
		exprs[i] = e
		hdr = append(hdr, name)
	}
	out, ch := in.child(hdr)
	go func() {
		defer close(ch)
		for r := range in.Rows {
			r = fit(r, len(in.Header))
			for _, e := range exprs {
				r = append(r, e(r).String())
			}
			if !out.send(ch, r) {
				return
			}
		}
	}()
	return out, nil
}

// Select keeps only the named columns, in order. "old:new" renames.
func Select(in *Table, cols []string) (*Table, error) {
	if len(cols) == 0 {
		return in, nil
	}
	idx := make([]int, len(cols))
	hdr := make([]string, len(cols))
	for i, c := range cols {
		name, alias, _ := strings.Cut(c, ":")
		j := Index(in.Header, name)
		if j < 0 {
			return nil, fmt.Errorf("unknown column %q", name)
		}
		idx[i] = j
		hdr[i] = in.Header[j]
		if alias = strings.TrimSpace(alias); alias != "" {
			hdr[i] = alias
		}
	}
	out, ch := in.child(hdr)
	go func() {
		defer close(ch)
		for r := range in.Rows {
			nr := make(Row, len(idx))
			for i, j := range idx {
				nr[i] = r.Get(j)
			}
			if !out.send(ch, nr) {
				return
			}
		}
	}()
	return out, nil
}

// Limit passes through at most n rows (n <= 0 means unlimited) and then
// stops the upstream stages instead of draining them.
func Limit(in *Table, n int) *Table {
	if n <= 0 {
		return in
	}
	out, ch := in.child(in.Header)
	go func() {
		defer close(ch)
		i := 0
		for r := range in.Rows {
			if !out.send(ch, r) {
				return
			}
			if i++; i == n {
				in.run.cancel()
				return
			}
		}
	}()
	return out
}
