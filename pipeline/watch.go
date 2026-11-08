package pipeline

import (
	"context"
	"io"
	"os"
	"strconv"
	"strings"
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
		cur, em := in.child(in.Header)
		out <- cur
		for {
			select {
			case batch, ok := <-in.Rows:
				if !ok {
					em.close()
					return
				}
				for _, r := range batch {
					if !em.send(r) {
						em.close()
						return
					}
				}
			case <-tick.C:
				em.close()
				cur, em = in.child(in.Header)
				select {
				case out <- cur:
				case <-in.run.ctx.Done():
					em.close()
					return
				}
			}
		}
	}()
	return out
}

// Trend appends a "<col>_change" column for every non-key column, holding
// the difference from the previous window's row with the same first
// keyCols values ("new" when there was none). It returns the rows seen,
// to pass as prev on the next window.
func Trend(in *Table, keyCols int, prev map[string]Row) (*Table, map[string]Row) {
	hdr := append([]string(nil), in.Header...)
	for _, h := range in.Header[keyCols:] {
		hdr = append(hdr, h+"_change")
	}
	seen := map[string]Row{}
	out, em := in.child(hdr)
	go func() {
		defer em.close()
		for batch := range in.Rows {
			for _, r := range batch {
				r = fit(r, len(in.Header))
				key := strings.Join(r[:keyCols], "\x00")
				seen[key] = r
				last, ok := prev[key]
				for i := keyCols; i < len(in.Header); i++ {
					switch cur, err := strconv.ParseFloat(r[i], 64); {
					case !ok:
						r = append(r, "new")
					case err != nil:
						r = append(r, "")
					default:
						before, _ := strconv.ParseFloat(last.Get(i), 64)
						r = append(r, strconv.FormatFloat(cur-before, 'f', -1, 64))
					}
				}
				if !em.send(r) {
					return
				}
			}
		}
	}()
	return out, seen
}
