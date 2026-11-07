package pipeline

import (
	"hash/fnv"
	"strings"
)

// CleanOptions controls the Clean stage.
type CleanOptions struct {
	SnakeHeaders bool // rename "Total Amount" -> total_amount
	Dedupe       bool // drop rows identical to one already seen
	DropEmpty    bool // drop rows where every value is null
	Sample       int  // rows used to infer column kinds (default 1000)
	Types        bool // normalize numbers, dates, bools
}

// Schema is what Clean learned about the columns.
type Schema struct {
	Columns []string
	Kinds   []Kind
}

// Clean normalizes headers and values, drops empty and duplicate rows,
// and returns the cleaned stream with its inferred schema. Kinds are
// inferred from the first Sample rows, which are buffered then replayed.
func Clean(in *Table, o CleanOptions) (*Table, *Schema) {
	if o.Sample <= 0 {
		o.Sample = 1000
	}
	hdr := make([]string, len(in.Header))
	for i, h := range in.Header {
		h = strings.TrimSpace(h)
		if h == "" {
			h = "col_" + itoa(i+1)
		}
		if o.SnakeHeaders {
			h = SnakeHeader(h)
		}
		hdr[i] = h
	}
	hdr = uniqueHeaders(hdr)
	schema := &Schema{Columns: hdr, Kinds: make([]Kind, len(hdr))}
	out := &Table{Header: hdr, Err: in.Err}
	ch := make(chan Row, 1024)
	out.Rows = ch

	go func() {
		defer close(ch)
		buf := make([]Row, 0, o.Sample)
		for r := range in.Rows {
			buf = append(buf, r)
			if o.Types {
				for i := 0; i < len(hdr) && i < len(r); i++ {
					schema.Kinds[i] = Widen(schema.Kinds[i], Infer(r[i]))
				}
			}
			if len(buf) >= o.Sample {
				break
			}
		}
		if o.Types {
			for i, k := range schema.Kinds {
				if k == KindEmpty {
					schema.Kinds[i] = KindText
				}
			}
		}
		seen := map[uint64]struct{}{}
		emit := func(r Row) {
			if len(r) < len(hdr) {
				padded := make(Row, len(hdr))
				copy(padded, r)
				r = padded
			} else if len(r) > len(hdr) {
				r = r[:len(hdr)]
			}
			empty := true
			for i := range r {
				if o.Types {
					r[i] = Normalize(r[i], schema.Kinds[i])
				} else if IsNull(r[i]) {
					r[i] = ""
				} else {
					r[i] = strings.TrimSpace(r[i])
				}
				if r[i] != "" {
					empty = false
				}
			}
			if empty && o.DropEmpty {
				return
			}
			if o.Dedupe {
				h := hashRow(r)
				if _, dup := seen[h]; dup {
					return
				}
				seen[h] = struct{}{}
			}
			ch <- r
		}
		for _, r := range buf {
			emit(r)
		}
		for r := range in.Rows {
			emit(r)
		}
	}()
	return out, schema
}

func hashRow(r Row) uint64 {
	h := fnv.New64a()
	for _, v := range r {
		h.Write([]byte(v))
		h.Write([]byte{0})
	}
	return h.Sum64()
}

func uniqueHeaders(h []string) []string {
	seen := map[string]int{}
	for i, name := range h {
		n := seen[name]
		seen[name] = n + 1
		if n > 0 {
			h[i] = name + "_" + itoa(n+1)
		}
	}
	return h
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	p := len(b)
	neg := i < 0
	if neg {
		i = -i
	}
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		p--
		b[p] = '-'
	}
	return string(b[p:])
}
