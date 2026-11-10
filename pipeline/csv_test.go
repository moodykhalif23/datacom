package pipeline

import (
	"context"
	"encoding/csv"
	"io"
	"reflect"
	"strings"
	"testing"
)

func wholeCSV(s string, comma rune, trim bool) []Row {
	cr := csv.NewReader(strings.NewReader(s))
	cr.FieldsPerRecord = -1
	cr.LazyQuotes = true
	cr.TrimLeadingSpace = trim
	cr.Comma = comma
	var rows []Row
	for {
		rec, err := cr.Read()
		if err != nil {
			return rows
		}
		rows = append(rows, rec)
	}
}

// trickle returns at most n bytes per Read, so pieces end at every offset.
type trickle struct {
	s string
	n int
}

func (t *trickle) Read(p []byte) (int, error) {
	if t.s == "" {
		return 0, io.EOF
	}
	k := min(len(p), t.n, len(t.s))
	copy(p, t.s[:k])
	t.s = t.s[k:]
	return k, nil
}

func splitCSV(t *testing.T, s string, comma rune, trim bool, step int) []Row {
	t.Helper()
	src := &csvPieces{r: &trickle{s, step}, s: csvSplit{comma: byte(comma), trim: trim}}
	var rows []Row
	for {
		b, err := src.next(1)
		if b == nil {
			if err != io.EOF {
				t.Fatalf("next: %v", err)
			}
			return rows
		}
		got, err := parseCSV(context.Background(), b, comma, trim)
		if err != nil {
			t.Fatalf("parse %q: %v", b, err)
		}
		rows = append(rows, got...)
	}
}

func checkSplit(t *testing.T, s string) {
	t.Helper()
	for _, comma := range []rune{',', ';', '\t'} {
		for _, trim := range []bool{true, false} {
			want := wholeCSV(s, comma, trim)
			for _, step := range []int{1, 2, 3, 7, 64, 1 << 20} {
				if got := splitCSV(t, s, comma, trim, step); !reflect.DeepEqual(got, want) {
					t.Fatalf("comma %q trim %v step %d on %q:\n got %q\nwant %q", comma, trim, step, s, got, want)
				}
			}
		}
	}
}

func TestCSVSplit(t *testing.T) {
	for _, s := range []string{
		"", "\n", "a", "a,b\n1,2\n", "a,b\r\n1,2\r\n", "a,b\n1,2",
		"\"a\nb\",c\n\"d\"\"e\",f\n",
		"\"x\" y,z\n1,2\n", "a,\"b\"\r\nc\n", "\"a\"\r\"b\"\n1\n",
		" \"a,b\",c\n", "\t\"a\tb\"\tc\n", "\u00a0\"a,b\",c\n", "\u2003\"x\n\",y\n",
		"5\" pipe,ok\n\"open\nnever closed",
		"a,b\n\n\n1,2\n", "   \n\"\"\n", "\"\"\"\",\"\n\"\n",
		"\uFEFF\"h\",x\n1,2\n", "a\r", "\"a\"\r", "\"a\"\r\n",
		"x,\"\xff\xfe\",y\n\xe2\x80\n",
	} {
		checkSplit(t, s)
	}
}

func FuzzCSVSplit(f *testing.F) {
	f.Add("a,b\n\"1\n2\",3\n")
	f.Add("\"x\"\"y\" ,z\r\n \"q\",\"w\"\r\n")
	f.Add("\t\"a\"\t\tb\n\u00a0\"c,d\"\n")
	f.Fuzz(func(t *testing.T, s string) {
		checkSplit(t, s)
	})
}

func TestReadCSVMatchesSerial(t *testing.T) {
	var b strings.Builder
	b.WriteString("\uFEFFid,note,amount\n")
	for i := range 20000 {
		switch i % 4 {
		case 0:
			b.WriteString("1,plain,10\n")
		case 1:
			b.WriteString("2,\"two\nlines, and a comma\",20\r\n")
		case 2:
			b.WriteString("3, \"spaced \"\"quote\"\"\",30\n")
		default:
			b.WriteString("4,5\" bare,40\n")
		}
	}
	s := b.String()
	want := wholeCSV(s, ',', true)
	tb := Read(context.Background(), strings.NewReader(s), FormatCSV)
	got := []Row{tb.Header}
	for r := range tb.Rows {
		got = append(got, r)
	}
	if err := tb.Err(); err != nil {
		t.Fatal(err)
	}
	want[0][0] = strings.TrimPrefix(want[0][0], "\uFEFF")
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("parallel reader differs from encoding/csv: %d rows against %d", len(got), len(want))
	}
}

func TestTSVKeepsEmptyCells(t *testing.T) {
	tb := Read(context.Background(), strings.NewReader("a\tb\tc\n1\t\t3\n"), FormatTSV)
	r := <-tb.Rows
	if !reflect.DeepEqual(r, Row{"1", "", "3"}) {
		t.Fatalf("got %q", r)
	}
}
