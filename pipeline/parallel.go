package pipeline

import (
	"context"
	"runtime"
	"sync"
	"time"
)

type parallelKey struct{}

func WithParallelism(ctx context.Context, n int) context.Context {
	return context.WithValue(ctx, parallelKey{}, n)
}

const (
	batchRows  = 64
	batchFlush = 20 * time.Millisecond
)

// ordered runs f over head and then every row of in on all cores, in
// batches, and calls emit with each result in input order.
func ordered[T any](in *Table, head []Row, f func(Row) T, emit func(Row, T) bool) {
	type batch struct {
		rows []Row
		res  []T
		done chan struct{}
	}
	workers := runtime.GOMAXPROCS(0)
	if n, ok := in.run.ctx.Value(parallelKey{}).(int); ok && n > 0 {
		workers = min(workers, n)
	}
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
		hand := func() bool {
			timer.Stop()
			if !offer(ctx, work, b) || !offer(ctx, queue, b) {
				return false
			}
			b = &batch{done: make(chan struct{})}
			return true
		}
		add := func(r Row) bool {
			if b.rows = append(b.rows, r); len(b.rows) == 1 {
				timer.Reset(batchFlush)
			}
			return len(b.rows) < batchRows || hand()
		}
		for _, r := range head {
			if !add(r) {
				return
			}
		}
		for {
			select {
			case r, ok := <-in.Rows:
				if !ok {
					if len(b.rows) > 0 {
						hand()
					}
					return
				}
				if !add(r) {
					return
				}
			case <-timer.C:
				if !hand() {
					return
				}
			case <-ctx.Done():
			drain:
				for range cap(in.Rows) {
					select {
					case r, ok := <-in.Rows:
						if !ok {
							break drain
						}
						if !add(r) {
							return
						}
					default:
						break drain
					}
				}
				if len(b.rows) > 0 {
					hand()
				}
				return
			}
		}
	}()
	defer wg.Wait()
	for b := range queue {
		<-b.done
		for i, r := range b.rows {
			if !emit(r, b.res[i]) {
				return
			}
		}
	}
}

// offer hands v over the way Table.send does: rows read before a stop still
// go through when there is room, and only a blocked handoff gives up.
func offer[V any](ctx context.Context, ch chan<- V, v V) bool {
	select {
	case ch <- v:
		return true
	default:
	}
	select {
	case ch <- v:
		return true
	case <-ctx.Done():
		return false
	}
}
