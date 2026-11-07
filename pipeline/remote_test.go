package pipeline

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// flaky serves data over HTTP while misbehaving in configurable ways.
type flaky struct {
	data      []byte
	ranges    bool  // honour Range requests
	dropAfter int   // bytes to send before killing the connection
	failFirst int32 // number of requests that misbehave
	stall     bool  // hang instead of dropping
	status    int   // non-200 status to return while failing
	requests  atomic.Int32
}

func (f *flaky) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	n := f.requests.Add(1)
	failing := n <= f.failFirst
	if failing && f.status != 0 {
		w.WriteHeader(f.status)
		return
	}
	body := f.data
	if rg := r.Header.Get("Range"); rg != "" && f.ranges {
		off, _ := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(rg, "bytes="), "-"))
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", off, len(body)-1, len(body)))
		w.WriteHeader(http.StatusPartialContent)
		body = body[off:]
	}
	if !failing {
		w.Write(body)
		return
	}
	w.Write(body[:min(f.dropAfter, len(body))])
	w.(http.Flusher).Flush()
	if f.stall {
		time.Sleep(2 * time.Second)
		return
	}
	panic(http.ErrAbortHandler) // drops the TCP connection mid-body
}

func TestRemoteSurvivesBadNetworks(t *testing.T) {
	remoteIdleTimeout, remoteBackoff = 150*time.Millisecond, 10*time.Millisecond
	var data bytes.Buffer
	data.WriteString("id,amount\n")
	for i := 0; i < 60_000; i++ {
		fmt.Fprintf(&data, "%d,%d.50\n", i, i%997)
	}
	cases := map[string]*flaky{
		"drop with range":    {ranges: true, dropAfter: 100_000, failFirst: 5},
		"drop without range": {ranges: false, dropAfter: 100_000, failFirst: 3},
		"stall then resume":  {ranges: true, dropAfter: 50_000, failFirst: 1, stall: true},
		"503 then ok":        {ranges: true, failFirst: 2, status: 503},
	}
	for name, srv := range cases {
		t.Run(name, func(t *testing.T) {
			srv.data = data.Bytes()
			s := httptest.NewServer(srv)
			defer s.Close()
			r, f, closeFn, err := Open(context.Background(), s.URL+"/x.csv", FormatAuto)
			if err != nil {
				t.Fatal(err)
			}
			defer closeFn()
			got, err := io.ReadAll(r)
			if err != nil {
				t.Fatal(err)
			}
			if f != FormatCSV || !bytes.Equal(got, srv.data) {
				t.Fatalf("format %v, got %d bytes want %d (requests %d)", f, len(got), len(srv.data), srv.requests.Load())
			}
			if srv.requests.Load() <= srv.failFirst {
				t.Fatalf("expected retries, saw %d requests", srv.requests.Load())
			}
		})
	}
}

func TestRemoteGivesUp(t *testing.T) {
	remoteBackoff, remoteMaxAttempts = time.Millisecond, 3
	s := httptest.NewServer(&flaky{failFirst: 99, status: 500})
	defer s.Close()
	if _, err := io.ReadAll(newRemote(context.Background(), s.URL)); err == nil || !strings.Contains(err.Error(), "giving up") {
		t.Fatalf("err = %v", err)
	}
	s404 := httptest.NewServer(http.NotFoundHandler())
	defer s404.Close()
	if _, err := io.ReadAll(newRemote(context.Background(), s404.URL)); err == nil || !strings.Contains(err.Error(), "404") {
		t.Fatalf("err = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := io.ReadAll(newRemote(ctx, s.URL)); err != context.Canceled {
		t.Fatalf("err = %v", err)
	}
}
