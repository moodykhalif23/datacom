package pipeline

import (
	"fmt"
	"strings"
)

// Join appends the columns of right to each row of left with a matching key.
// Keys compare case-insensitively; unmatched rows get empty columns.
func Join(left, right *Table, on string) (*Table, error) {
	lk, rk, _ := strings.Cut(on, "=")
	if rk == "" {
		rk = lk
	}
	li, ri := Index(left.Header, lk), Index(right.Header, rk)
	if li < 0 || ri < 0 {
		return nil, fmt.Errorf("join key %q not found in both inputs", on)
	}
	hdr := append([]string(nil), left.Header...)
	for i, h := range right.Header {
		if i != ri {
			hdr = append(hdr, h)
		}
	}
	uniqueHeaders(hdr)
	width := len(right.Header) - 1
	out, em := left.child(hdr)
	go func() {
		defer em.close()
		lookup := map[string][]Row{}
		for batch := range right.Rows {
			for _, r := range batch {
				r = fit(r, len(right.Header))
				k := strings.ToLower(r[ri])
				lookup[k] = append(lookup[k], append(r[:ri:ri], r[ri+1:]...))
			}
		}
		if err := right.Err(); err != nil {
			out.fail(err)
			return
		}
		for batch := range left.Rows {
			for _, l := range batch {
				l = fit(l, len(left.Header))
				matches := lookup[strings.ToLower(l[li])]
				if len(matches) == 0 {
					matches = []Row{make(Row, width)}
					if mon := left.monitor(); mon != nil {
						mon.Unmatched.Add(1)
					}
				}
				for _, m := range matches {
					if !em.send(append(append(make(Row, 0, len(hdr)), l...), m...)) {
						return
					}
				}
			}
		}
	}()
	return out, nil
}
