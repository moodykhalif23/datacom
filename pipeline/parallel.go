package pipeline

import (
	"runtime"
	"sync"
	"time"
)

const (
	batchRows  = 64
	batchFlush = 20 * time.Millisecond
)

// ordered runs f over the rows of in on every core and hands each result
// to emit in input order, so a stage keeps its first-seen semantics while
// the costly part runs in parallel. A partial batch is flushed after a
// short pause, so slow live inputs are not held back.
func ordered[T any](in *Table, f func(Row) T, emit func(Row, T) bool) {
	type batch struct {
		rows []Row
		res  []T
		done chan struct{}
	}
	workers := runtime.GOMAXPROCS(0)
	work := make(chan *batch, workers)
	queue := make(chan *batch, workers*2)
	ctx := in.run.ctx
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for b := range work {
				b.res = make([]T, len(b.rows))
				for i, r := range b.rows {
					b.res[i] = f(r)
				}
				close(b.done)
			}
		}()
	}
	go func() {
		defer close(queue)
		defer close(work)
		b := &batch{done: make(chan struct{})}
		timer := time.NewTimer(time.Hour)
		timer.Stop()
		flush := func() bool {
			timer.Stop()
			select {
			case work <- b:
			case <-ctx.Done():
				return false
			}
			select {
			case queue <- b:
			case <-ctx.Done():
				return false
			}
			b = &batch{done: make(chan struct{})}
			return true
		}
		for {
			select {
			case r, ok := <-in.Rows:
				if !ok {
					if len(b.rows) > 0 {
						flush()
					}
					return
				}
				if b.rows = append(b.rows, r); len(b.rows) == 1 {
					timer.Reset(batchFlush)
				}
				if len(b.rows) == batchRows && !flush() {
					return
				}
			case <-timer.C:
				if !flush() {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	defer wg.Wait()
	for b := range queue {
		select {
		case <-b.done:
		case <-ctx.Done():
			return
		}
		for i, r := range b.rows {
			if !emit(r, b.res[i]) {
				return
			}
		}
	}
}
