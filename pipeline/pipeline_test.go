package pipeline

import (
	"bufio"
	"bytes"
	"strings"
	"testing"
)

func table(t *testing.T, csvText string) *Table {
	t.Helper()
	tb := Read(strings.NewReader(csvText), FormatCSV)
	if *tb.Err != nil {
		t.Fatal(*tb.Err)
	}
	return tb
}

func collect(tb *Table) [][]string {
	var out [][]string
	for r := range tb.Rows {
		out = append(out, r)
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
	tb := table(t, "Total Amount (USD),Name\n\"1,000\",a\n\"1,000\",a\n,\n2,b\n")
	out, schema := Clean(tb, CleanOptions{SnakeHeaders: true, Dedupe: true, DropEmpty: true, Types: true})
	rows := collect(out)
	if got := strings.Join(out.Header, ","); got != "total_amount_usd,name" {
		t.Fatalf("header = %q", got)
	}
	if len(rows) != 2 || rows[0][0] != "1000" || rows[1][0] != "2" {
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
	rows := collect(s)
	want := "A,2,30,15;B,1,5,5"
	var parts []string
	for _, r := range rows {
		parts = append(parts, strings.Join(r, ","))
	}
	if got := strings.Join(parts, ";"); got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestJSONRoundTrip(t *testing.T) {
	src := `[{"b":"2","a":"x"},{"a":"y","b":null}]`
	tb := Read(strings.NewReader(src), FormatJSON)
	var buf bytes.Buffer
	n, err := Write(&buf, tb, FormatCSV)
	if err != nil || n != 2 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if got := buf.String(); got != "a,b\nx,2\ny,\n" {
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
	if rep.Columns[1].Distinct != 3 {
		t.Fatalf("distinct = %d", rep.Columns[1].Distinct)
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
		h.add(hashStr("id-" + itoa(i)))
	}
	if n := h.count(); n < 480_000 || n > 520_000 {
		t.Fatalf("estimate %d too far from 500000", n)
	}
}
