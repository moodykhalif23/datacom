// Package pipeline streams tabular data through clean, filter, aggregate
// and output stages. Each stage is a goroutine joined by a bounded channel,
// so memory per row stays constant however many stages are chained and
// however large the input is.
package pipeline

import (
	"context"
	"errors"
	"iter"
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
	Rows   <-chan []Row // batches of rows; Each walks them one row at a time
	run    *run
	root   bool
	live   bool // a followed file: hand every row on at once instead of filling a batch
}

// run is the state shared by every stage of one pipeline.
type run struct {
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	err    error
	mon    *Monitor
}

// Stages hand rows on in batches so the channel is locked once per batch
// rather than once per row. chanBuf batches may be in flight between two
// stages, so about chanBuf*batchSize rows per stage boundary.
const (
	chanBuf   = 4
	batchSize = 256
)

type liveKey struct{}

// WithLive marks a context whose source never ends, such as a followed
// file, so every row is passed downstream as soon as it is read.
func WithLive(ctx context.Context) context.Context {
	return context.WithValue(ctx, liveKey{}, true)
}

func newRoot(ctx context.Context, header []string) (*Table, *emitter) {
	ctx, cancel := context.WithCancel(ctx)
	mon, _ := ctx.Value(monKey{}).(*Monitor)
	live, _ := ctx.Value(liveKey{}).(bool)
	t := &Table{Header: header, run: &run{ctx: ctx, cancel: cancel, mon: mon}, root: true, live: live}
	return t, t.emitter()
}

// child creates the next stage, sharing this pipeline's context and error.
func (t *Table) child(header []string) (*Table, *emitter) {
	c := &Table{Header: header, run: t.run, live: t.live}
	return c, c.emitter()
}

func (t *Table) emitter() *emitter {
	ch := make(chan []Row, chanBuf)
	t.Rows = ch
	return &emitter{t: t, ch: ch}
}

// Each yields the table's rows in order. Stages and callers read a table
// through it; the batches behind it are an implementation detail.
func (t *Table) Each() iter.Seq[Row] {
	return func(yield func(Row) bool) {
		for batch := range t.Rows {
			for _, r := range batch {
				if !yield(r) {
					return
				}
			}
		}
	}
}

// emitter collects the rows a stage produces and sends them on a batch at
// a time. close flushes what is left and closes the channel.
type emitter struct {
	t   *Table
	ch  chan []Row
	buf []Row
}

func (e *emitter) send(r Row) bool {
	e.buf = append(e.buf, r)
	if len(e.buf) < batchSize && !e.t.live {
		return true
	}
	return e.flush()
}

func (e *emitter) flush() bool {
	if len(e.buf) == 0 {
		return true
	}
	b := e.buf
	e.buf = nil
	// Try without watching ctx first, so rows already read keep flowing
	// after a stop and partial output is complete up to the error.
	select {
	case e.ch <- b:
	default:
		select {
		case e.ch <- b:
		case <-e.t.run.ctx.Done():
			return false
		}
	}
	if e.t.root && e.t.run.mon != nil {
		e.t.run.mon.RowsIn.Add(int64(len(b)))
	}
	return true
}

func (e *emitter) close() {
	e.flush()
	close(e.ch)
}

// fail records the first error and stops every stage. A cancellation is a
// clean stop, not an error.
func (t *Table) fail(err error) {
	if err == nil {
		return
	}
	if errors.Is(err, context.Canceled) {
		t.run.cancel()
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
	KindPhone
	KindText
)

func (k Kind) String() string {
	return [...]string{"empty", "int", "float", "bool", "date", "phone", "text"}[k]
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

const hashSeed uint64 = 14695981039346656037 // FNV-1a offset basis

// hash64 folds s into the running FNV-1a hash h.
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

// WithMonitor returns a context whose pipelines report to m (nil detaches).
func WithMonitor(ctx context.Context, m *Monitor) context.Context {
	return context.WithValue(ctx, monKey{}, m)
}

type monKey struct{}

// monitor returns the attached Monitor, or nil.
func (t *Table) monitor() *Monitor { return t.run.mon }
