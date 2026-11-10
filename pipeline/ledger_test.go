package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLedger(t *testing.T) {
	l := NewLedger()
	ctx := WithLedger(context.Background(), l)
	sum := func(s string) string { h := sha256.Sum256([]byte(s)); return hex.EncodeToString(h[:]) }

	io.ReadAll(Track(ctx, "full", strings.NewReader("abc")))
	part := Track(ctx, "part", strings.NewReader("abcdef"))
	part.Read(make([]byte, 2))
	body := strings.Repeat("x,1\n", 50000)
	path := filepath.Join(t.TempDir(), "big.csv")
	os.WriteFile(path, []byte("a,b\n"+body), 0o644)
	r, f, closeFn, err := Open(ctx, path, FormatAuto)
	if err != nil {
		t.Fatal(err)
	}
	tb := Limit(Read(ctx, r, f), 3)
	collect(tb)
	closeFn()

	got := l.Inputs()
	if len(got) != 3 {
		t.Fatalf("inputs = %+v", got)
	}
	if got[0].SHA256 != sum("abc") || !got[0].Complete || got[0].Bytes != 3 {
		t.Errorf("full stream: %+v", got[0])
	}
	if got[1].Complete || got[1].Bytes != 2 || got[1].SHA256 != sum("ab") {
		t.Errorf("partial stream: %+v", got[1])
	}
	if got[2].SHA256 != sum("a,b\n"+body) || !got[2].Complete || got[2].Bytes != int64(len(body)+4) {
		t.Errorf("file read only in part must still be hashed whole: %+v", got[2])
	}
	hw := NewHashWriter(io.Discard)
	io.WriteString(hw, "hello")
	if s, n := hw.Sum(); s != sum("hello") || n != 5 {
		t.Errorf("HashWriter = %s %d", s, n)
	}
}
