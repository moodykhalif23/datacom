package pipeline

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
)

// read runs a source through Clean and collects rows and the error.
func read(t *testing.T, src string, f Format) ([]string, []string, error) {
	t.Helper()
	tb := Read(context.Background(), strings.NewReader(src), f)
	tb, _ = Clean(tb, CleanOptions{Types: true, DropEmpty: true})
	rows := collect(tb)
	return tb.Header, rows, tb.Err()
}

func TestEdgeInputs(t *testing.T) {
	cases := []struct {
		name, src string
		f         Format
		header    string
		rows      int
		wantErr   bool
	}{
		{"empty file", "", FormatCSV, "", 0, false},
		{"header only", "a,b\n", FormatCSV, "a,b", 0, false},
		{"crlf", "a,b\r\n1,2\r\n3,4\r\n", FormatCSV, "a,b", 2, false},
		{"bom and quoted newline", "\uFEFFa,b\n\"x\ny\",2\n", FormatCSV, "a,b", 1, false},
		{"blank headers", ",,\n1,2,3\n", FormatCSV, "col_1,col_2,col_3", 1, false},
		{"duplicate headers", "a,a,a\n1,2,3\n", FormatCSV, "a,a_2,a_3", 1, false},
		{"unicode headers", "Jina,Kiasi (KES)\nAmina,10\n", FormatCSV, "Jina,Kiasi (KES)", 1, false},
		{"json empty array", "[]", FormatJSON, "", 0, false},
		{"json wrapper", `{"meta":1,"data":[{"a":1}]}`, FormatJSON, "a", 1, false},
		{"json nested values", `[{"a":{"x":1},"b":[1,2]}]`, FormatJSON, "a,b", 1, false},
		{"json truncated", `[{"a":1},{"a":`, FormatJSON, "a", 1, true},
		{"ndjson blank lines", "{\"a\":1}\n\n{\"a\":2}\n", FormatNDJSON, "a", 2, false},
		{"ndjson garbage line", "{\"a\":1}\nnot json\n", FormatNDJSON, "a", 1, true},
		{"unsupported format", "x", Format("yaml"), "", 0, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			hdr, rows, err := read(t, c.src, c.f)
			if (err != nil) != c.wantErr {
				t.Fatalf("err = %v", err)
			}
			if got := strings.Join(hdr, ","); got != c.header || len(rows) != c.rows {
				t.Fatalf("header %q rows %d", got, len(rows))
			}
		})
	}
}

func TestWideAndHugeCells(t *testing.T) {
	var sb strings.Builder
	for i := 0; i < 5000; i++ {
		fmt.Fprintf(&sb, "c%d,", i)
	}
	sb.WriteString("last\n")
	for i := 0; i < 5000; i++ {
		fmt.Fprintf(&sb, "%d,", i)
	}
	sb.WriteString("end\n")
	hdr, rows, err := read(t, sb.String(), FormatCSV)
	if err != nil || len(hdr) != 5001 || len(rows) != 1 {
		t.Fatalf("wide: %d columns, %d rows, %v", len(hdr), len(rows), err)
	}
	huge := strings.Repeat("x", 8<<20)
	hdr, rows, err = read(t, "a,b\n\""+huge+"\",1\n", FormatCSV)
	if err != nil || len(rows) != 1 || !strings.HasPrefix(rows[0], "xxxx") {
		t.Fatalf("huge cell: %v %d", err, len(rows))
	}
}

func TestCorruptInputsFailCleanly(t *testing.T) {
	var gz bytes.Buffer
	w := gzip.NewWriter(&gz)
	w.Write([]byte("a,b\n1,2\n3,4\n5,6\n"))
	w.Close()
	truncated := gz.Bytes()[:gz.Len()-8] // drop the gzip trailer
	path := t.TempDir() + "/t.csv.gz"
	os.WriteFile(path, truncated, 0o644)
	r, f, closeFn, err := Open(context.Background(), path, FormatAuto)
	if err != nil {
		t.Fatal(err)
	}
	defer closeFn()
	tb := Read(context.Background(), r, f)
	collect(tb)
	if tb.Err() == nil {
		t.Fatal("truncated gzip should report an error")
	}
	for name, data := range map[string][]byte{"x.xlsx": []byte("PK\x03\x04junk"), "x.parquet": []byte("PAR1junkPAR1")} {
		p := t.TempDir() + "/" + name
		os.WriteFile(p, data, 0o644)
		r, f, closeFn, err := Open(context.Background(), p, FormatAuto)
		if err != nil {
			t.Fatal(err)
		}
		tb := Read(context.Background(), r, f)
		collect(tb)
		closeFn()
		if tb.Err() == nil {
			t.Fatalf("%s: corrupt file should report an error", name)
		}
	}
}

func TestErrorsNameTheProblem(t *testing.T) {
	tb := table(t, "a,b\n1,2\n")
	if _, err := Select(tb, []string{"zzz"}); err == nil || !strings.Contains(err.Error(), `"zzz"`) {
		t.Fatalf("select: %v", err)
	}
	if _, err := Sort(tb, "nope"); err == nil || !strings.Contains(err.Error(), `"nope"`) {
		t.Fatalf("sort: %v", err)
	}
	if _, err := ParseAggs(tb.Header, "sum(c)"); err == nil || !strings.Contains(err.Error(), `"c"`) {
		t.Fatalf("agg: %v", err)
	}
	if _, err := Join(tb, tb, "x"); err == nil || !strings.Contains(err.Error(), `"x"`) {
		t.Fatalf("join: %v", err)
	}
	if _, err := Derive(tb, []string{"broken"}); err == nil || !strings.Contains(err.Error(), "name = expression") {
		t.Fatalf("derive: %v", err)
	}
	if _, err := Compile(tb.Header, "a >= "); err == nil || !strings.Contains(err.Error(), `in "a >= "`) {
		t.Fatalf("compile: %v", err)
	}
}
