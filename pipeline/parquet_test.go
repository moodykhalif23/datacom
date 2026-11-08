package pipeline

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParquetRoundTrip(t *testing.T) {
	src := "id,amount,ok,day,when,name\n1,1250.5,true,2025-01-15,2025-01-15T10:30:00Z,Acme Ltd\n2,,false,,,Zażółć 🌍\n3,7,yes,15/02/2025,2025-05-02 18:00:00,\n"
	path := t.TempDir() + "/out.parquet"
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := Write(f, table(t, src), FormatParquet); err != nil || n != 3 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	f.Close()
	r, format, closeFn, err := Open(context.Background(), path, FormatAuto)
	if err != nil || format != FormatParquet {
		t.Fatalf("open: %v %v", format, err)
	}
	defer closeFn()
	back := Read(context.Background(), r, format)
	rows := collect(back)
	got := strings.Join(back.Header, ",") + "|" + strings.Join(rows, ";")
	want := "id,amount,ok,day,when,name|1,1250.5,true,2025-01-15,2025-01-15T10:30:00Z,Acme Ltd;2,,false,,,Zażółć 🌍;3,7,true,2025-02-15,2025-05-02T18:00:00Z,"
	if got != want {
		t.Fatalf("round trip\n got %q\nwant %q\n err %v", got, want, back.Err())
	}
	// Spooled input (not an *os.File) takes the temp-file path.
	data, _ := os.ReadFile(path)
	if n := len(collect(ReadParquet(context.Background(), strings.NewReader(string(data))))); n != 3 {
		t.Fatalf("spooled rows = %d", n)
	}
}

// TestParquetCorpus reads the sample files that ship with parquet-go:
// every encoding, codec, nested and legacy-typed file must open and
// yield rows without error.
func TestParquetCorpus(t *testing.T) {
	out, err := exec("go", "list", "-m", "-f", "{{.Dir}}", "github.com/parquet-go/parquet-go")
	if err != nil {
		t.Skip("go list failed: ", err)
	}
	dir := strings.TrimSpace(out)
	files, _ := filepath.Glob(filepath.Join(dir, "testdata", "*.parquet"))
	if len(files) == 0 {
		t.Skip("parquet-go testdata not found")
	}
	for _, path := range files {
		name := filepath.Base(path)
		t.Run(name, func(t *testing.T) {
			r, format, closeFn, err := Open(context.Background(), path, FormatAuto)
			if err != nil {
				t.Fatal(err)
			}
			defer closeFn()
			if format != FormatParquet {
				t.Fatalf("detected %v", format)
			}
			tb := Read(context.Background(), r, format)
			rows := collect(tb)
			if err := tb.Err(); err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			t.Logf("%s: %d columns, %d rows", name, len(tb.Header), len(rows))
		})
	}
}

func TestParquetDecimalAndNames(t *testing.T) {
	if got := columnName([]string{"tags", "list", "element"}); got != "tags" {
		t.Fatalf("columnName = %q", got)
	}
	if got := columnName([]string{"attrs", "key_value", "value"}); got != "attrs.value" {
		t.Fatalf("columnName = %q", got)
	}
	cases := map[string]string{
		"12345|2":  "123.45",
		"-5|3":     "-0.005",
		"7|0":      "7",
		"-12345|2": "-123.45",
	}
	for in, want := range cases {
		n, s, _ := strings.Cut(in, "|")
		var v int64
		for _, c := range strings.TrimPrefix(n, "-") {
			v = v*10 + int64(c-'0')
		}
		if n[0] == '-' {
			v = -v
		}
		scale := int(s[0] - '0')
		if got := decimal(parquetInt64(v), scale); got != want {
			t.Errorf("decimal(%s) = %q want %q", in, got, want)
		}
	}
}
