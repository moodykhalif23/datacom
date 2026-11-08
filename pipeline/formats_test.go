package pipeline

import (
	"archive/zip"
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf16"
)

func readFile(t *testing.T, path string) (*Table, []string) {
	t.Helper()
	r, f, closeFn, err := Open(context.Background(), path, FormatAuto)
	if err != nil {
		t.Fatal(err)
	}
	defer closeFn()
	tb := Read(context.Background(), r, f)
	rows := collect(tb)
	if err := tb.Err(); err != nil {
		t.Fatal(err)
	}
	return tb, rows
}

func TestDelimiterSniffing(t *testing.T) {
	for src, want := range map[string]string{
		"a;b;c\n1;2;3\n":                   "a,b,c|1,2,3",
		"a|b|c\n1|2|3\n":                   "a,b,c|1,2,3",
		"a\tb\tc\n1\t2\t3\n":               "a,b,c|1,2,3",
		"a,b,c\n1,2,3\n":                   "a,b,c|1,2,3",
		"\"x;y\",b\n1,2\n":                 "x;y,b|1,2", // semicolons inside quotes do not count
		"name;note\nAnn;\"a, b; c\"\n":     "name,note|Ann,\"a, b; c\"",
		"amount,note\n\"1,250\",\"a;b\"\n": "amount,note|\"1,250\",a;b",
	} {
		tb := Read(context.Background(), strings.NewReader(src), FormatCSV)
		var buf bytes.Buffer
		Write(&buf, tb, FormatCSV)
		if got := strings.ReplaceAll(strings.TrimSpace(buf.String()), "\n", "|"); got != want {
			t.Errorf("%q: got %q, want %q", src, got, want)
		}
	}
}

func TestUTF16AndZipInput(t *testing.T) {
	dir := t.TempDir()
	text := "name,amount\nAmina,1250\nOtieno,\"3,400\"\n"
	for _, c := range []struct {
		name string
		big  bool
	}{{"le.csv", false}, {"be.csv", true}} {
		var b bytes.Buffer
		for _, u := range utf16.Encode([]rune("\uFEFF" + text)) {
			if c.big {
				b.Write([]byte{byte(u >> 8), byte(u)})
			} else {
				b.Write([]byte{byte(u), byte(u >> 8)})
			}
		}
		p := filepath.Join(dir, c.name)
		os.WriteFile(p, b.Bytes(), 0o644)
		tb, rows := readFile(t, p)
		if strings.Join(tb.Header, ",") != "name,amount" || strings.Join(rows, ";") != "Amina,1250;Otieno,3,400" {
			t.Fatalf("%s: header %v rows %q", c.name, tb.Header, rows)
		}
	}
	zp := filepath.Join(dir, "export.zip")
	zf, _ := os.Create(zp)
	zw := zip.NewWriter(zf)
	w, _ := zw.Create("README.txt")
	w.Write([]byte("not data"))
	w, _ = zw.Create("exports/sales.csv")
	w.Write([]byte(text))
	zw.Close()
	zf.Close()
	tb, rows := readFile(t, zp)
	if strings.Join(tb.Header, ",") != "name,amount" || len(rows) != 2 {
		t.Fatalf("zip: header %v rows %q", tb.Header, rows)
	}
	empty := filepath.Join(dir, "nodata.zip")
	zf, _ = os.Create(empty)
	zw = zip.NewWriter(zf)
	w, _ = zw.Create("notes.txt")
	w.Write([]byte("x"))
	zw.Close()
	zf.Close()
	if _, _, _, err := Open(context.Background(), empty, FormatAuto); err == nil || !strings.Contains(err.Error(), "no csv") {
		t.Fatalf("empty zip: err = %v", err)
	}
}

func TestHTMLAndSQLOutput(t *testing.T) {
	src := "id,name,amount,paid,when\n1,Acme <Ltd>,\"1,250.00\",yes,2025-01-15\n2,O'Neil,500,no,15/02/2025\n3,KRA,abc,maybe,n/a\n"
	var buf bytes.Buffer
	if n, err := Write(&buf, table(t, src), FormatHTML); err != nil || n != 3 {
		t.Fatalf("html: n=%d err=%v", n, err)
	}
	out := buf.String()
	for _, want := range []string{"<meta charset=\"utf-8\">", "<th>amount</th>", "<td>Acme &lt;Ltd&gt;</td>", "<td>O&#39;Neil</td>", "</table>"} {
		if !strings.Contains(out, want) {
			t.Errorf("html output lacks %q:\n%s", want, out)
		}
	}
	buf.Reset()
	if n, err := WriteSQL(&buf, table(t, src), "sales 2025"); err != nil || n != 3 {
		t.Fatalf("sql: n=%d err=%v", n, err)
	}
	out = buf.String()
	for _, want := range []string{
		`CREATE TABLE IF NOT EXISTS "sales 2025" (`,
		`"id" BIGINT`, `"name" TEXT`, `"amount" DOUBLE PRECISION`, `"paid" BOOLEAN`, `"when" DATE`,
		`INSERT INTO "sales 2025" ("id", "name", "amount", "paid", "when") VALUES`,
		`(1, 'Acme <Ltd>', 1250, TRUE, '2025-01-15')`,
		`(2, 'O''Neil', 500, FALSE, '2025-02-15')`,
		`(3, 'KRA', NULL, NULL, NULL);`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("sql output lacks %q:\n%s", want, out)
		}
	}
}
