package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"os"
	"sync"
)

// InputDigest records one source a run read: its size and SHA-256.
// Complete is false when a stream was not read to its end.
type InputDigest struct {
	Source   string `json:"source"`
	Role     string `json:"role,omitempty"`
	Bytes    int64  `json:"bytes"`
	SHA256   string `json:"sha256"`
	Complete bool   `json:"complete"`
	Error    string `json:"error,omitempty"`
}

type Ledger struct {
	mu     sync.Mutex
	wg     sync.WaitGroup
	seen   map[string]bool
	inputs []*trackedInput
}

type trackedInput struct {
	mu sync.Mutex
	d  InputDigest
	h  hash.Hash
}

func NewLedger() *Ledger { return &Ledger{seen: map[string]bool{}} }

type ledgerKey struct{}

// WithLedger returns a context whose sources are recorded in l.
func WithLedger(ctx context.Context, l *Ledger) context.Context {
	return context.WithValue(ctx, ledgerKey{}, l)
}

func ledgerFrom(ctx context.Context) *Ledger {
	l, _ := ctx.Value(ledgerKey{}).(*Ledger)
	return l
}

func (l *Ledger) add(src string) *trackedInput {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.seen[src] {
		return nil
	}
	l.seen[src] = true
	t := &trackedInput{d: InputDigest{Source: src}, h: sha256.New()}
	l.inputs = append(l.inputs, t)
	return t
}

func (l *Ledger) file(path string) {
	t := l.add(path)
	if t == nil {
		return
	}
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		f, err := os.Open(path)
		if err == nil {
			var n int64
			n, err = io.Copy(t.h, f)
			f.Close()
			t.d.Bytes = n
		}
		t.mu.Lock()
		defer t.mu.Unlock()
		if err != nil {
			t.d.Error = err.Error()
			return
		}
		t.d.Complete = true
	}()
}

func (l *Ledger) stream(src string, r io.Reader) io.Reader {
	t := l.add(src)
	if t == nil {
		return r
	}
	return &digestReader{r: r, t: t}
}

type digestReader struct {
	r io.Reader
	t *trackedInput
}

func (d *digestReader) Read(p []byte) (int, error) {
	n, err := d.r.Read(p)
	d.t.mu.Lock()
	d.t.h.Write(p[:n])
	d.t.d.Bytes += int64(n)
	if errors.Is(err, io.EOF) {
		d.t.d.Complete = true
	}
	d.t.mu.Unlock()
	return n, err
}

// Track records a reader that did not come from Open, such as a followed
// file, when ctx carries a Ledger.
func Track(ctx context.Context, src string, r io.Reader) io.Reader {
	if l := ledgerFrom(ctx); l != nil {
		return l.stream(src, r)
	}
	return r
}

// Inputs waits for file hashes to finish and returns every digest in the
// order the sources were opened.
func (l *Ledger) Inputs() []InputDigest {
	l.wg.Wait()
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]InputDigest, len(l.inputs))
	for i, t := range l.inputs {
		t.mu.Lock()
		out[i] = t.d
		out[i].SHA256 = hex.EncodeToString(t.h.Sum(nil))
		t.mu.Unlock()
	}
	return out
}

// HashWriter passes writes through to w and keeps their size and SHA-256.
type HashWriter struct {
	w io.Writer
	h hash.Hash
	n int64
}

func NewHashWriter(w io.Writer) *HashWriter { return &HashWriter{w: w, h: sha256.New()} }

func (hw *HashWriter) Write(p []byte) (int, error) {
	n, err := hw.w.Write(p)
	hw.h.Write(p[:n])
	hw.n += int64(n)
	return n, err
}

// Sum returns the SHA-256 of everything written so far and its size.
func (hw *HashWriter) Sum() (string, int64) { return hex.EncodeToString(hw.h.Sum(nil)), hw.n }
