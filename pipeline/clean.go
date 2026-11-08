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
	FoldCase     bool   // in categorical columns, spell a value the way it is spelled most often
	PhonePrefix  string // country calling code for national phone numbers, e.g. "254"
	Sample       int    // rows used to infer column kinds (default 1000)
}

// Fold tables stop growing at maxFoldKeys distinct values: past that a column
// is not categorical and is left alone.
const maxFoldKeys = 100

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
	tallies := make([]tally, len(hdr))
	out, em := in.child(hdr)

	go func() {
		defer em.close()
		buf := make([]Row, 0, o.Sample)
		// A live input must not stall waiting for a full sample.
		var wait <-chan time.Time
	sample:
		for len(buf) < o.Sample {
			select {
			case batch, ok := <-in.Rows:
				if !ok {
					break sample
				}
				for _, r := range batch {
					buf = append(buf, r)
					if o.Types {
						for i := 0; i < len(hdr) && i < len(r); i++ {
							tallies[i].add(Infer(r[i]))
						}
					}
				}
				if wait == nil {
					wait = time.After(100 * time.Millisecond)
				}
			case <-wait:
				break sample
			}
		}
		for i := range schema.Kinds {
			schema.Kinds[i] = tallies[i].kind()
		}
		var folds []map[string]string
		var folded []int
		var foldExample []string
		if o.FoldCase {
			folds, folded, foldExample = make([]map[string]string, len(hdr)), make([]int, len(hdr)), make([]string, len(hdr))
			for i := range hdr {
				if schema.Kinds[i] == KindText {
					folds[i] = foldTable(buf, i)
				}
			}
		}
		var seen map[uint64]struct{}
		if o.Dedupe {
			seen = map[uint64]struct{}{}
		}
		mon := in.monitor()
		emit := func(r Row) bool {
			if mon != nil && len(r) != len(hdr) {
				if len(r) < len(hdr) {
					mon.Padded.Add(1)
				} else {
					mon.Trimmed.Add(1)
				}
			}
			r = fit(r, len(hdr))
			empty := true
			for i := range r {
				k := KindText
				if o.Types {
					k = schema.Kinds[i]
				}
				raw := r[i]
				var ok bool
				if r[i], ok = normalizeWith(raw, k, o.PhonePrefix); !ok && mon != nil {
					mon.Bad(hdr[i], k, raw)
				}
				if folds != nil && folds[i] != nil && r[i] != "" {
					key := strings.ToLower(r[i])
					if c, known := folds[i][key]; known {
						if c != r[i] {
							if folded[i] == 0 {
								foldExample[i] = r[i] + " to " + c
							}
							r[i] = c
							folded[i]++
						}
					} else if len(folds[i]) < maxFoldKeys*10 {
						folds[i][key] = r[i]
					}
				}
				empty = empty && r[i] == ""
			}
			if empty && o.DropEmpty {
				if mon != nil {
					mon.Empty.Add(1)
				}
				return true
			}
			if seen != nil {
				h := hashRow(r)
				if _, dup := seen[h]; dup {
					if mon != nil {
						mon.Dupes.Add(1)
					}
					return true
				}
				seen[h] = struct{}{}
			}
			return em.send(r)
		}
		for _, r := range buf {
			if !emit(r) {
				return
			}
		}
		for batch := range in.Rows {
			for _, r := range batch {
				if !emit(r) {
					return
				}
			}
		}
		if mon != nil {
			for i, n := range folded {
				if n > 0 {
					mon.Note("%s: %s values respelled to match the most common form, e.g. %s.", hdr[i], commas(int64(n)), foldExample[i])
				}
			}
		}
	}()
	return out, schema
}

// foldTable maps each distinct lower-cased value of column i in the sample to
// its most common spelling (ties go to the spelling seen first), or nil when
// the column has more than maxFoldKeys distinct values and so is not
// categorical.
func foldTable(sample []Row, i int) map[string]string {
	type spellings struct {
		count map[string]int
		order []string // first-seen order, for ties
	}
	counts := map[string]*spellings{}
	for _, r := range sample {
		if i >= len(r) {
			continue
		}
		v, _ := normalize(r[i], KindText)
		if v == "" {
			continue
		}
		key := strings.ToLower(v)
		sp := counts[key]
		if sp == nil {
			if len(counts) >= maxFoldKeys {
				return nil
			}
			sp = &spellings{count: map[string]int{}}
			counts[key] = sp
		}
		if sp.count[v] == 0 {
			sp.order = append(sp.order, v)
		}
		sp.count[v]++
	}
	table := make(map[string]string, len(counts))
	for key, sp := range counts {
		best, n := "", 0
		for _, v := range sp.order {
			if c := sp.count[v]; c > n {
				best, n = v, c
			}
		}
		table[key] = best
	}
	return table
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
