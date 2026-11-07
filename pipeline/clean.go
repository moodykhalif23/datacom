package pipeline

import (
	"strconv"
	"strings"
	"time"
)

type CleanOptions struct {
	SnakeHeaders bool
	Dedupe       bool
	DropEmpty    bool
	Types        bool
	Sample       int // rows used to infer column kinds (default 1000)
}

type Schema struct {
	Columns []string
	Kinds   []Kind
}

func Clean(in *Table, o CleanOptions) (*Table, *Schema) {
	if o.Sample <= 0 {
		o.Sample = 1000
	}
	hdr := make([]string, len(in.Header))
	for i, h := range in.Header {
		h = strings.TrimSpace(h)
		if h == "" {
			h = "col_" + strconv.Itoa(i+1)
		}
		if o.SnakeHeaders {
			h = SnakeHeader(h)
		}
		hdr[i] = h
	}
	uniqueHeaders(hdr)
	schema := &Schema{Columns: hdr, Kinds: make([]Kind, len(hdr))}
	out, ch := in.child(hdr)

	go func() {
		defer close(ch)
		buf := make([]Row, 0, o.Sample)
		var wait <-chan time.Time // live inputs must not stall waiting for a full sample
	sample:
		for len(buf) < o.Sample {
			select {
			case r, ok := <-in.Rows:
				if !ok {
					break sample
				}
				buf = append(buf, r)
				if o.Types {
					for i := 0; i < len(hdr) && i < len(r); i++ {
						schema.Kinds[i] = Widen(schema.Kinds[i], Infer(r[i]))
					}
				}
				if wait == nil {
					wait = time.After(500 * time.Millisecond)
				}
			case <-wait:
				break sample
			}
		}
		for i, k := range schema.Kinds {
			if k == KindEmpty {
				schema.Kinds[i] = KindText
			}
		}
		var seen map[uint64]struct{}
		if o.Dedupe {
			seen = map[uint64]struct{}{}
		}
		emit := func(r Row) bool {
			r = fit(r, len(hdr))
			empty := true
			for i := range r {
				if o.Types {
					r[i] = Normalize(r[i], schema.Kinds[i])
				} else {
					r[i] = Normalize(r[i], KindText)
				}
				empty = empty && r[i] == ""
			}
			if empty && o.DropEmpty {
				return true
			}
			if seen != nil {
				h := hashRow(r)
				if _, dup := seen[h]; dup {
					return true
				}
				seen[h] = struct{}{}
			}
			return out.send(ch, r)
		}
		for _, r := range buf {
			if !emit(r) {
				return
			}
		}
		for r := range in.Rows {
			if !emit(r) {
				return
			}
		}
	}()
	return out, schema
}

// fit pads or trims r to n columns.
func fit(r Row, n int) Row {
	if len(r) == n {
		return r
	}
	if len(r) > n {
		return r[:n]
	}
	return append(r, make(Row, n-len(r))...)
}

func uniqueHeaders(h []string) {
	seen := make(map[string]int, len(h))
	for i, name := range h {
		if n := seen[name]; n > 0 {
			h[i] = name + "_" + strconv.Itoa(n+1)
		}
		seen[name]++
	}
}
