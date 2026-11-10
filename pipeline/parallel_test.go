package pipeline

import (
	"context"
	"io"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestOrderedStagesStopCleanly(t *testing.T) {
	base := runtime.NumGoroutine()
	for i := 0; i < 10; i++ {
		tb := Read(context.Background(), io.MultiReader(strings.NewReader("a,b\n"), &bigCSV{}), FormatCSV)
		tb = Redact(tb, RedactAll, "")
		tb, _ = Quality(tb, QualityRules{}, nil)
		ix, _ := NewNearIndex(NearDupOptions{Threshold: 0.8, MemoryMB: 1})
		tb, _ = NearDedupe(tb, ix, nil)
		tb = Limit(tb, 1)
		if n, err := Write(io.Discard, tb, FormatCSV); err != nil || n != 1 {
			t.Fatalf("n=%d err=%v", n, err)
		}
	}
	deadline := time.Now().Add(3 * time.Second)
	for runtime.NumGoroutine() > base+2 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if g := runtime.NumGoroutine(); g > base+2 {
		t.Fatalf("goroutines leaked: %d > %d", g, base)
	}
}

func TestOrderedFlushesSlowInput(t *testing.T) {
	pr, pw := io.Pipe()
	defer pw.Close()
	go io.WriteString(pw, "body\nfirst row of a live feed\n")
	tb := Read(context.Background(), pr, FormatCSV)
	tb = Redact(tb, RedactAll, "")
	start := time.Now()
	select {
	case r := <-tb.Rows:
		if r[0] != "first row of a live feed" || time.Since(start) > time.Second {
			t.Fatalf("row %q after %s", r, time.Since(start))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("a lone row was held back waiting for a full batch")
	}
}
