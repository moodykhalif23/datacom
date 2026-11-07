package pipeline

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

type remote struct {
	ctx      context.Context
	url      string
	client   *http.Client
	body     io.ReadCloser
	cancel   context.CancelFunc
	timer    *time.Timer
	offset   int64 // bytes delivered to the caller
	skip     int64 // bytes of the current response to discard
	attempts int
	done     bool
}

// Tunables, variables so tests can shorten them.
var (
	remoteIdleTimeout = 30 * time.Second
	remoteBackoff     = time.Second
	remoteMaxBackoff  = 30 * time.Second
	remoteMaxAttempts = 12
)

func newRemote(ctx context.Context, url string) *remote {
	return &remote{ctx: ctx, url: url, client: &http.Client{
		Transport: &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			ResponseHeaderTimeout: remoteIdleTimeout,
			TLSHandshakeTimeout:   remoteIdleTimeout,
		},
	}}
}

func (r *remote) Read(p []byte) (int, error) {
	for {
		if r.done {
			return 0, io.EOF
		}
		if r.body == nil {
			if err := r.connect(); err != nil {
				return 0, err
			}
		}
		r.timer.Reset(remoteIdleTimeout)
		n, err := r.body.Read(p)
		if r.skip > 0 && n > 0 {
			d := min(int64(n), r.skip)
			r.skip -= d
			copy(p, p[d:n])
			n -= int(d)
		}
		r.offset += int64(n)
		if err == nil {
			r.attempts = 0
			if n == 0 {
				continue
			}
			return n, nil
		}
		r.close()
		if err == io.EOF {
			r.done = true
			return n, nil
		}
		if n > 0 {
			return n, nil // deliver what we have; reconnect on the next call
		}
		if err := r.backoff(err); err != nil {
			return 0, err
		}
	}
}

func (r *remote) connect() error {
	for {
		reqCtx, cancel := context.WithCancel(r.ctx)
		req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, r.url, nil)
		if err != nil {
			cancel()
			return err
		}
		if r.offset > 0 {
			req.Header.Set("Range", "bytes="+strconv.FormatInt(r.offset, 10)+"-")
		}
		resp, err := r.client.Do(req)
		if err == nil {
			switch {
			case resp.StatusCode == http.StatusPartialContent:
				r.skip = 0
			case resp.StatusCode == http.StatusRequestedRangeNotSatisfiable:
				resp.Body.Close()
				cancel()
				r.done = true // offset is at the end of the resource
				return nil
			case resp.StatusCode >= 200 && resp.StatusCode < 300:
				r.skip = r.offset // server ignored Range: skip the prefix
			case resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests:
				resp.Body.Close()
				err = fmt.Errorf("server returned %s", resp.Status)
			default:
				resp.Body.Close()
				cancel()
				return fmt.Errorf("fetch %s: %s", r.url, resp.Status)
			}
		}
		if err == nil {
			r.body, r.cancel = resp.Body, cancel
			r.timer = time.AfterFunc(remoteIdleTimeout, cancel)
			return nil
		}
		cancel()
		if err := r.backoff(err); err != nil {
			return err
		}
	}
}

func (r *remote) backoff(cause error) error {
	if r.ctx.Err() != nil {
		return r.ctx.Err()
	}
	r.attempts++
	if r.attempts >= remoteMaxAttempts {
		return fmt.Errorf("fetch %s: giving up after %d attempts: %w", r.url, r.attempts, cause)
	}
	wait := min(remoteBackoff<<uint(r.attempts-1), remoteMaxBackoff)
	select {
	case <-time.After(wait):
		return nil
	case <-r.ctx.Done():
		return r.ctx.Err()
	}
}

func (r *remote) close() {
	if r.timer != nil {
		r.timer.Stop()
	}
	if r.body != nil {
		r.body.Close()
		r.body = nil
	}
	if r.cancel != nil {
		r.cancel()
		r.cancel = nil
	}
}
