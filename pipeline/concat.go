package pipeline

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

// Concat reads the tables open returns, one after another, as one table
// named after the first's columns. Later tables are matched by column name:
// a missing column is left empty and extra columns are dropped and noted.
func Concat(ctx context.Context, names []string, open func(ctx context.Context, i int) (*Table, func() error, error)) (*Table, error) {
	t, ch := newRoot(ctx, nil)
	t.root = false
	first, closeFirst, err := open(t.run.ctx, 0)
	if err != nil {
		t.run.cancel()
		return nil, err
	}
	t.Header = first.Header
	go func() {
		defer close(ch)
		cur, closeCur := first, closeFirst
		for i := 0; i < len(names); i++ {
			if i > 0 {
				var err error
				if cur, closeCur, err = open(t.run.ctx, i); err != nil {
					t.fail(err)
					return
				}
			}
			mapping, extra := columnMap(t.Header, cur.Header)
			if len(extra) > 0 && t.run.mon != nil {
				t.run.mon.Note("%s: columns %s are not in %s and were dropped.", names[i], strings.Join(extra, ", "), names[0])
			}
			for r := range cur.Rows {
				if mapping != nil {
					out := make(Row, len(mapping))
					for j, k := range mapping {
						out[j] = r.Get(k)
					}
					r = out
				}
				if !t.send(ch, r) {
					closeCur()
					return
				}
			}
			err := cur.Err()
			closeCur()
			if err != nil {
				t.fail(fmt.Errorf("%s: %w", names[i], err))
				return
			}
		}
	}()
	return t, nil
}

// columnMap returns, for each column of want, its index in got (-1 when
// missing), or nil when got already has want's columns in order.
func columnMap(want, got []string) ([]int, []string) {
	same := len(want) == len(got)
	m := make([]int, len(want))
	used := make([]bool, len(got))
	for i, w := range want {
		m[i] = -1
		for j, g := range got {
			if !used[j] && strings.EqualFold(strings.TrimSpace(w), strings.TrimSpace(g)) {
				m[i], used[j] = j, true
				break
			}
		}
		same = same && m[i] == i
	}
	var extra []string
	for j, u := range used {
		if !u {
			extra = append(extra, got[j])
		}
	}
	if same {
		return nil, extra
	}
	return m, extra
}

// SeenSet is a set of row hashes shared by pipelines that dedupe against
// each other, split into shards so parallel writers rarely contend.
type SeenSet struct {
	shards [64]struct {
		mu sync.Mutex
		m  map[uint64]struct{}
	}
}

func NewSeenSet() *SeenSet {
	s := &SeenSet{}
	for i := range s.shards {
		s.shards[i].m = map[uint64]struct{}{}
	}
	return s
}

// Add reports whether h was new, adding it if so.
func (s *SeenSet) Add(h uint64) bool {
	sh := &s.shards[h%64]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if _, ok := sh.m[h]; ok {
		return false
	}
	sh.m[h] = struct{}{}
	return true
}
