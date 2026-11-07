package pipeline

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
)

func TestMonitorCountsHiccups(t *testing.T) {
	src := "id,amount,day\n1,10,2025-01-01\n2,ten,2025-01-02\n3,30,yesterday\n4,40\n5,50,2025-01-05,extra\n,,\n1,10,2025-01-01\n"
	mon := NewMonitor()
	tb := Read(WithMonitor(context.Background(), mon), mon.Meter(strings.NewReader(src)), FormatCSV)
	tb, _ = Clean(tb, CleanOptions{Types: true, Dedupe: true, DropEmpty: true})
	right := table(t, "id,region\n1,A\n2,B\n")
	tb, err := Join(tb, right, "id")
	if err != nil {
		t.Fatal(err)
	}
	tb, _ = Sort(tb, "-amount")
	if _, err := Write(io.Discard, tb, FormatCSV); err != nil {
		t.Fatal(err)
	}
	r := mon.Report()
	if r.RowsIn != 7 || r.RowsOut != 5 || r.Bytes != int64(len(src)) {
		t.Fatalf("rows in %d out %d bytes %d", r.RowsIn, r.RowsOut, r.Bytes)
	}
	if r.Padded != 1 || r.Trimmed != 1 || r.Empty != 1 || r.Dupes != 1 || r.Unmatched != 3 {
		t.Fatalf("padded %d trimmed %d empty %d dupes %d unmatched %d", r.Padded, r.Trimmed, r.Empty, r.Dupes, r.Unmatched)
	}
	if len(r.Bad) != 2 || r.Bad[0].Count != 1 || r.Bad[1].Count != 1 {
		t.Fatalf("bad = %+v", r.Bad)
	}
	var buf bytes.Buffer
	r.WriteText(&buf)
	for _, want := range []string{`"ten"`, `"yesterday"`, "1 duplicate", "no join match", "too few columns"} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("report lacks %q:\n%s", want, buf.String())
		}
	}
	buf.Reset()
	if err := r.WriteJSON(&buf); err != nil || !strings.Contains(buf.String(), `"untyped_values":[{"column":"amount"`) && !strings.Contains(buf.String(), `"untyped_values":[{"column":"day"`) {
		t.Fatalf("json: %v %s", err, buf.String())
	}
	if !strings.Contains(mon.progress(), "rows") {
		t.Fatal("progress line")
	}
}

func TestMonitorRecordsError(t *testing.T) {
	mon := NewMonitor()
	tb := Read(WithMonitor(context.Background(), mon), strings.NewReader("a\n1\n"), FormatCSV)
	if _, err := Write(failWriter{}, tb, FormatCSV); err == nil {
		t.Fatal("expected error")
	}
	if r := mon.Report(); r.Error == "" {
		t.Fatal("error not recorded")
	}
}
