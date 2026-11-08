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
	out, em := in.child(in.Header)
	go func() {
		defer em.close()
		for batch := range in.Rows {
		rows:
			for _, r := range batch {
				for _, c := range conds {
					if !c(r).truthy() {
						continue rows
					}
				}
				if !em.send(r) {
					return
				}
			}
		}
	}()
	return out
}

// Derive adds one column per "name = expression". A name that already exists
// is overwritten in place, so "status = lower(status)" folds a column without
// a rename, and later expressions see the earlier results.
func Derive(in *Table, defs []string) (*Table, error) {
	if len(defs) == 0 {
		return in, nil
	}
	hdr := append([]string(nil), in.Header...)
	type derived struct {
		idx int
		e   Expr
	}
	cols := make([]derived, len(defs))
	for i, d := range defs {
		name, src, ok := strings.Cut(d, "=")
		if name = strings.TrimSpace(name); !ok || name == "" {
			return nil, fmt.Errorf("derive needs \"name = expression\", got %q", d)
		}
		e, err := Compile(hdr, src)
		if err != nil {
			return nil, err
		}
		idx := Index(hdr, name)
		if idx < 0 {
			idx = len(hdr)
			hdr = append(hdr, name)
		}
		cols[i] = derived{idx, e}
	}
	out, em := in.child(hdr)
	go func() {
		defer em.close()
		for batch := range in.Rows {
			for _, r := range batch {
				r = fit(r, len(hdr))
				for _, c := range cols {
					r[c.idx] = c.e(r).String()
				}
				if !em.send(r) {
					return
				}
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
	out, em := in.child(hdr)
	go func() {
		defer em.close()
		for batch := range in.Rows {
			for _, r := range batch {
				nr := make(Row, len(idx))
				for i, j := range idx {
					nr[i] = r.Get(j)
				}
				if !em.send(nr) {
					return
				}
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
	out, em := in.child(in.Header)
	go func() {
		defer em.close()
		i := 0
		for batch := range in.Rows {
			for _, r := range batch {
				if !em.send(r) {
					return
				}
				if i++; i == n {
					in.run.cancel()
					return
				}
			}
		}
	}()
	return out
}
