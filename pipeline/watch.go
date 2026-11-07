package pipeline

import (
	"context"
	"io"
	"os"
	"time"
)

// Follow returns a reader over path that keeps delivering data as the file
// grows, like tail -f. It never reports EOF; cancel ctx to stop.
func Follow(ctx context.Context, path string) (io.Reader, error) {
	if path == "" || path == "-" {
		return os.Stdin, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &follower{ctx: ctx, path: path, f: f}, nil
}

type follower struct {
	ctx  context.Context
	path string
	f    *os.File
	off  int64
}

func (t *follower) Read(p []byte) (int, error) {
	for {
		n, err := t.f.Read(p)
		t.off += int64(n)
		if n > 0 || err != io.EOF {
			return n, err
		}
		if st, err := os.Stat(t.path); err == nil && st.Size() < t.off {
			if f, err := os.Open(t.path); err == nil { // rotated or truncated
				t.f.Close()
				t.f, t.off = f, 0
				continue
			}
		}
		select {
		case <-time.After(250 * time.Millisecond):
		case <-t.ctx.Done():
			return 0, t.ctx.Err()
		}
	}
}

// Windows splits in into tumbling windows of the given duration.
func Windows(in *Table, every time.Duration) <-chan *Table {
	out := make(chan *Table)
	go func() {
		defer close(out)
		tick := time.NewTicker(every)
		defer tick.Stop()
		cur, ch := in.child(in.Header)
		out <- cur
		for {
			select {
			case r, ok := <-in.Rows:
				if !ok {
					close(ch)
					return
				}
				if !cur.send(ch, r) {
					close(ch)
					return
				}
			case <-tick.C:
				close(ch)
				cur, ch = in.child(in.Header)
				select {
				case out <- cur:
				case <-in.run.ctx.Done():
					close(ch)
					return
				}
			}
		}
	}()
	return out
}
