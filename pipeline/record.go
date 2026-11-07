// Package pipeline streams tabular data through clean, filter, aggregate
// and output stages. Each stage is a goroutine joined by a bounded channel,
// so memory per row stays constant however many stages are chained and
// however large the input is.
package pipeline

import (
	"context"
	"strings"
	"sync"
)

// Row is one record. Values line up with Header by index.
type Row []string

func (r Row) Get(i int) string {
	if i < 0 || i >= len(r) {
		return ""
	}
	return r[i]
}

type Table struct {
	Header []string
	Rows   <-chan Row
	run    *run
}

// run is the state shared by every stage of one pipeline.
type run struct {
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	err    error
}

const chanBuf = 512

func newRoot(ctx context.Context, header []string) (*Table, chan Row) {
	ctx, cancel := context.WithCancel(ctx)
	t := &Table{Header: header, run: &run{ctx: ctx, cancel: cancel}}
	ch := make(chan Row, chanBuf)
	t.Rows = ch
	return t, ch
}

// child creates the next stage, sharing this pipeline's context and error.
func (t *Table) child(header []string) (*Table, chan Row) {
	ch := make(chan Row, chanBuf)
	return &Table{Header: header, Rows: ch, run: t.run}, ch
}

func (t *Table) send(ch chan<- Row, r Row) bool {
	select {
	case ch <- r:
		return true
	case <-t.run.ctx.Done():
		return false
	}
}

func (t *Table) fail(err error) {
	if err == nil {
		return
	}
	t.run.mu.Lock()
	defer t.run.mu.Unlock()
	if t.run.err == nil {
		t.run.err = err
		t.run.cancel()
	}
}

func (t *Table) Err() error {
	t.run.mu.Lock()
	err := t.run.err
	t.run.mu.Unlock()
	if err != nil {
		return err
	}
	if err := t.run.ctx.Err(); err == context.Canceled {
		return nil // caller cancelled on purpose
	}
	return t.run.ctx.Err()
}

// Index returns the column position for name, case-insensitive, or -1.
func Index(header []string, name string) int {
	for i, h := range header {
		if strings.EqualFold(h, strings.TrimSpace(name)) {
			return i
		}
	}
	return -1
}

// Kind is an inferred column type.
type Kind int

const (
	KindEmpty Kind = iota
	KindInt
	KindFloat
	KindBool
	KindDate
	KindText
)

func (k Kind) String() string {
	return [...]string{"empty", "int", "float", "bool", "date", "text"}[k]
}

// Widen returns the narrowest kind that holds both a and b.
func Widen(a, b Kind) Kind {
	switch {
	case a == KindEmpty:
		return b
	case b == KindEmpty || a == b:
		return a
	case (a == KindInt && b == KindFloat) || (a == KindFloat && b == KindInt):
		return KindFloat
	}
	return KindText
}

// hash64 folds s into a running FNV-1a hash; start with hashSeed.
const hashSeed uint64 = 14695981039346656037

func hash64(h uint64, s string) uint64 {
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	return h
}

// hashRow hashes every value with a separator so ("ab","c") != ("a","bc").
func hashRow(r Row) uint64 {
	h := hashSeed
	for _, v := range r {
		h = hash64(h, v)
		h ^= 0xff
		h *= 1099511628211
	}
	return h
}
