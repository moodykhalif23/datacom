package pipeline

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

func table(t *testing.T, csvText string) *Table {
	t.Helper()
	tb := Read(context.Background(), strings.NewReader(csvText), FormatCSV)
	if err := tb.Err(); err != nil {
		t.Fatal(err)
	}
	return tb
}

func collect(tb *Table) []string {
	var out []string
	for r := range tb.Rows {
		out = append(out, strings.Join(r, ","))
	}
	return out
}

func TestInferAndNormalize(t *testing.T) {
	cases := []struct {
		in   string
		kind Kind
		norm string
	}{
		{"1,250.00", KindFloat, "1250"},
		{"$3,400", KindInt, "3400"},
		{"KES 3,400.50", KindFloat, "3400.5"},
		{"1200 USD", KindInt, "1200"},
		{"Room 101", KindText, "Room 101"},
		{"(500)", KindInt, "-500"},
		{"007", KindText, "007"},
		{"15/02/2025", KindDate, "2025-02-15"},
		{"2025-06-01T10:05:00Z", KindDate, "2025-06-01T10:05:00Z"},
		{"Yes", KindBool, "true"},
		{"N/A", KindEmpty, ""},
		{"  Acme   Ltd ", KindText, "Acme Ltd"},
	}
	for _, c := range cases {
		if k := Infer(c.in); k != c.kind {
			t.Errorf("Infer(%q) = %v, want %v", c.in, k, c.kind)
		}
		if n := Normalize(c.in, c.kind); n != c.norm {
			t.Errorf("Normalize(%q) = %q, want %q", c.in, n, c.norm)
		}
	}
}

func TestCleanDedupeAndSnake(t *testing.T) {
	tb := table(t, "Total Amount (USD),Name,Name\n\"1,000\",a\n\"1,000\",a\n,\n2,b,c,extra\n")
	out, schema := Clean(tb, CleanOptions{SnakeHeaders: true, Dedupe: true, DropEmpty: true, Types: true})
	rows := collect(out)
	if got := strings.Join(out.Header, ","); got != "total_amount_usd,name,name_2" {
		t.Fatalf("header = %q", got)
	}
	if strings.Join(rows, ";") != "1000,a,;2,b,c" {
		t.Fatalf("rows = %v", rows)
	}
	if schema.Kinds[0] != KindInt {
		t.Fatalf("kind = %v", schema.Kinds[0])
	}
}

func TestFilterAggregateSort(t *testing.T) {
	tb := table(t, "region,amount\nA,10\nB,5\nA,20\nB,\nC,1\n")
	c1, err := ParseCond(tb.Header, "amount >= 5")
	if err != nil {
		t.Fatal(err)
	}
	f := Filter(tb, []*Cond{c1})
	aggs, err := ParseAggs(f.Header, "count,sum(amount),avg(amount):mean")
	if err != nil {
		t.Fatal(err)
	}
	g, err := Aggregate(f, []string{"region"}, aggs)
	if err != nil {
		t.Fatal(err)
	}
	s, err := Sort(g, "-sum_amount")
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(collect(s), ";"); got != "A,2,30,15;B,1,5,5" {
		t.Fatalf("got %q", got)
	}
}

func TestJSONRoundTrip(t *testing.T) {
	src := `{"data":[{"b":"2","a":"x"},{"a":"y","b":null}]}`
	tb := Read(context.Background(), strings.NewReader(src), FormatJSON)
	var buf bytes.Buffer
	if n, err := Write(&buf, tb, FormatCSV); err != nil || n != 2 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if got := buf.String(); got != "a,b\nx,2\ny,\n" {
		t.Fatalf("got %q", got)
	}
	tb = table(t, "id,ok,name\n1,true,x\n")
	buf.Reset()
	Write(&buf, tb, FormatNDJSON)
	if got := buf.String(); got != `{"id":1,"ok":true,"name":"x"}`+"\n" {
		t.Fatalf("got %q", got)
	}
}

func TestProfile(t *testing.T) {
	tb := table(t, "id,city,amt\n1,Nairobi,10\n2,nairobi,20\n3,Mombasa,\n")
	rep := Profile(tb, nil, 5, 0)
	if rep.Rows != 3 {
		t.Fatalf("rows = %d", rep.Rows)
	}
	amt := rep.Columns[2]
	if amt.Kind != "int" || amt.Nulls != 1 || *amt.Mean != 15 {
		t.Fatalf("amt = %+v", amt)
	}
	if rep.Columns[1].Distinct != 3 || len(rep.Issues) != 1 {
		t.Fatalf("distinct = %d issues = %v", rep.Columns[1].Distinct, rep.Issues)
	}
}

func TestGuessFormat(t *testing.T) {
	cases := map[string]Format{
		`[{"a":1}]`:            FormatJSON,
		"{\"a\":1}\n{\"a\":2}": FormatNDJSON,
		"a\tb\n1\t2":           FormatTSV,
		"a,b\n1,2":             FormatCSV,
	}
	for in, want := range cases {
		if got := guessFormat("", bufio.NewReader(strings.NewReader(in))); got != want {
			t.Errorf("guessFormat(%q) = %v want %v", in, got, want)
		}
	}
}

func TestHLL(t *testing.T) {
	h := newHLL()
	for i := 0; i < 500_000; i++ {
		h.add(hash64(hashSeed, "id-"+strconv.Itoa(i)))
	}
	if n := h.count(); n < 490_000 || n > 510_000 {
		t.Fatalf("estimate %d too far from 500000", n)
	}
}

// bigCSV is an endless CSV source for leak and cancellation tests.
type bigCSV struct{ n int }

func (b *bigCSV) Read(p []byte) (int, error) {
	line := "x,1\n"
	n := 0
	for n+len(line) <= len(p) {
		copy(p[n:], line)
		n += len(line)
		b.n++
	}
	return n, nil
}

// TestNoGoroutineLeak checks that Limit and Write errors stop every stage.
func TestNoGoroutineLeak(t *testing.T) {
	base := runtime.NumGoroutine()
	for i := 0; i < 20; i++ {
		src := &bigCSV{}
		tb := Read(context.Background(), io.MultiReader(strings.NewReader("a,b\n"), src), FormatCSV)
		tb, _ = Clean(tb, CleanOptions{Types: true})
		c, _ := ParseCond(tb.Header, "b == 1")
		tb = Limit(Filter(tb, []*Cond{c}), 100)
		if n, err := Write(io.Discard, tb, FormatCSV); err != nil || n != 100 {
			t.Fatalf("n=%d err=%v", n, err)
		}
	}
	// A writer that fails must also unwind the chain.
	tb := Read(context.Background(), io.MultiReader(strings.NewReader("a,b\n"), &bigCSV{}), FormatCSV)
	if _, err := Write(failWriter{}, tb, FormatNDJSON); err == nil {
		t.Fatal("expected write error")
	}
	deadline := time.Now().Add(3 * time.Second)
	for runtime.NumGoroutine() > base+2 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if g := runtime.NumGoroutine(); g > base+2 {
		t.Fatalf("goroutines leaked: %d > %d", g, base)
	}
}

type failWriter struct{}

func (failWriter) Write([]byte) (int, error) { return 0, errBroken }

var errBroken = errors.New("broken pipe")

func FuzzReadCSV(f *testing.F) {
	f.Add("a,b\n1,2\n")
	f.Add("\"unterminated,x\n1\n")
	f.Add("\uFEFFh1,,h1\n,\n\"a\nb\",c\n")
	f.Fuzz(func(t *testing.T, s string) {
		tb := Read(context.Background(), strings.NewReader(s), FormatCSV)
		tb, _ = Clean(tb, CleanOptions{Types: true, Dedupe: true, DropEmpty: true, SnakeHeaders: true})
		Profile(tb, nil, 3, 10)
	})
}

func FuzzReadJSON(f *testing.F) {
	f.Add(`[{"a":1},{"a":"x","b":[1,2]}]`)
	f.Add("{\"a\":1}\n{\"a\":2}\n")
	f.Add(`{"data":[`)
	f.Fuzz(func(t *testing.T, s string) {
		for _, fm := range []Format{FormatJSON, FormatNDJSON} {
			tb := Read(context.Background(), strings.NewReader(s), fm)
			Write(io.Discard, tb, FormatCSV)
		}
	})
}

func FuzzValues(f *testing.F) {
	f.Add("KES 1,200.50")
	f.Add("(12)")
	f.Add("2025-13-45")
	f.Fuzz(func(t *testing.T, s string) {
		Normalize(s, Infer(s))
		ParseCond([]string{"c"}, "c "+s)
	})
}
