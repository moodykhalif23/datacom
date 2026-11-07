package pipeline

import (
	"archive/zip"
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"testing"
	"testing/iotest"
	"time"
)

func TestExpr(t *testing.T) {
	hdr := []string{"amount", "status", "Txn Date", "name"}
	row := Row{"1,250.50", "Paid", "2025-03-09", ""}
	cases := map[string]string{
		"amount * 2":                              "2501",
		"round(amount / 3, 2)":                    "416.83",
		"amount > 1000 and status == paid":        "true",
		"status != paid or amount < 0":            "false",
		"[Txn Date] >= 2025-01-01":                "true",
		"year([Txn Date]) + month([Txn Date])":    "2028",
		"name is null":                            "true",
		"name not null":                           "false",
		"coalesce(name, 'anon')":                  "anon",
		"if(amount > 1000, 'high', 'low')":        "high",
		"status ~ AI":                             "true",
		"status =~ ^p":                            "true",
		"status !~ ^p":                            "false",
		"concat(upper(status), '-', len(status))": "PAID-4",
		"not (amount > 1)":                        "false",
		"-amount % 7":                             "-4.5",
		"amount / 0":                              "",
		"missing_col == 'x'":                      "false",
	}
	for src, want := range cases {
		e, err := Compile(hdr, src)
		if err != nil {
			t.Errorf("%s: %v", src, err)
			continue
		}
		if got := e(row).String(); got != want {
			t.Errorf("%s = %q, want %q", src, got, want)
		}
	}
	for _, bad := range []string{"amount >", "nope(1)", "(1 + 2", "if(1)"} {
		if _, err := Compile(hdr, bad); err == nil {
			t.Errorf("%q should not compile", bad)
		}
	}
}

func TestDeriveAndFilter(t *testing.T) {
	tb := table(t, "price,qty\n10,3\n5,\n2,10\n")
	tb, err := Derive(tb, []string{"total = price * qty", "big = total > 20"})
	if err != nil {
		t.Fatal(err)
	}
	keep, _ := Compile(tb.Header, "total not null")
	rows := collect(Filter(tb, []Expr{keep}))
	if got := strings.Join(rows, ";"); got != "10,3,30,true;2,10,20,false" {
		t.Fatalf("got %q (header %v)", got, tb.Header)
	}
}

func TestJoin(t *testing.T) {
	left := table(t, "id,branch,amount\n1,NBO,10\n2,MSA,20\n3,KSM,30\n")
	right := table(t, "code,region,amount\nNBO,Central,x\nMSA,Coast,y\nMSA,Coast2,z\n")
	j, err := Join(left, right, "branch=code")
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(j.Header, ","); got != "id,branch,amount,region,amount_2" {
		t.Fatalf("header %q", got)
	}
	if got := strings.Join(collect(j), ";"); got != "1,NBO,10,Central,x;2,MSA,20,Coast,y;2,MSA,20,Coast2,z;3,KSM,30,," {
		t.Fatalf("rows %q", got)
	}
}

func TestJoinRightError(t *testing.T) {
	left := table(t, "k,v\n1,a\n")
	right := Read(context.Background(), io.MultiReader(strings.NewReader("k,w\n"), iotest.ErrReader(errBroken)), FormatCSV)
	j, err := Join(left, right, "k")
	if err != nil {
		t.Fatal(err)
	}
	collect(j)
	if j.Err() != errBroken {
		t.Fatalf("err = %v", j.Err())
	}
}

func TestWindowsAndFollow(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "*.ndjson")
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString("{\"n\":1}\n")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, err := Follow(ctx, f.Name())
	if err != nil {
		t.Fatal(err)
	}
	tb := Read(ctx, r, FormatNDJSON)
	wins := Windows(tb, 300*time.Millisecond)
	w1 := <-wins
	go func() {
		time.Sleep(50 * time.Millisecond)
		f.WriteString("{\"n\":2}\n")
		time.Sleep(400 * time.Millisecond)
		f.WriteString("{\"n\":3}\n")
	}()
	if got := strings.Join(collect(w1), ";"); got != "1;2" {
		t.Fatalf("window 1 = %q", got)
	}
	w2 := <-wins
	if got := strings.Join(collect(w2), ";"); got != "3" {
		t.Fatalf("window 2 = %q", got)
	}
	cancel()
	for range wins {
	}
}

// xlsxFixture builds a two-sheet workbook with shared strings, a date
// style, a boolean, an inline string and a gap column.
func xlsxFixture(t *testing.T) string {
	t.Helper()
	parts := map[string]string{
		"xl/workbook.xml":            `<workbook xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Empty" r:id="rId1"/><sheet name="Sales" r:id="rId2"/></sheets></workbook>`,
		"xl/_rels/workbook.xml.rels": `<Relationships><Relationship Id="rId1" Target="worksheets/sheet1.xml"/><Relationship Id="rId2" Target="worksheets/sheet2.xml"/></Relationships>`,
		"xl/sharedStrings.xml":       `<sst><si><t>Name</t></si><si><t>Amount</t></si><si><r><t>Ac</t></r><r><t>me</t></r></si><si><t>Date</t></si><si><t>Paid</t></si></sst>`,
		"xl/styles.xml":              `<styleSheet><numFmts><numFmt numFmtId="164" formatCode="dd/mm/yyyy"/></numFmts><cellXfs><xf numFmtId="0"/><xf numFmtId="164"/><xf numFmtId="2"/></cellXfs></styleSheet>`,
		"xl/worksheets/sheet1.xml":   `<worksheet><sheetData/></worksheet>`,
		"xl/worksheets/sheet2.xml": `<worksheet><sheetData>
<row r="1"><c r="A1" t="s"><v>0</v></c><c r="B1" t="s"><v>1</v></c><c r="C1" t="s"><v>3</v></c><c r="D1" t="s"><v>4</v></c></row>
<row r="2"><c r="A2" t="s"><v>2</v></c><c r="B2" s="2"><v>1234.5</v></c><c r="C2" s="1"><v>45658</v></c><c r="D2" t="b"><v>1</v></c></row>
<row r="3"><c r="A3" t="inlineStr"><is><t>Beta</t></is></c><c r="D3" t="b"><v>0</v></c></row>
</sheetData></worksheet>`,
	}
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for name, body := range parts {
		w, _ := zw.Create(name)
		w.Write([]byte(body))
	}
	zw.Close()
	path := t.TempDir() + "/book.xlsx"
	os.WriteFile(path, buf.Bytes(), 0o644)
	return path
}

func TestReadXLSX(t *testing.T) {
	path := xlsxFixture(t)
	r, f, closeFn, err := Open(context.Background(), path, FormatAuto)
	if err != nil || f != FormatXLSX {
		t.Fatalf("open: %v %v", f, err)
	}
	defer closeFn()
	tb := ReadXLSX(context.Background(), r, "Sales")
	rows := collect(tb)
	if err := tb.Err(); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(tb.Header, ","); got != "Name,Amount,Date,Paid" {
		t.Fatalf("header %q", got)
	}
	if got := strings.Join(rows, ";"); got != "Acme,1234.5,2025-01-01,true;Beta,,,false" {
		t.Fatalf("rows %q", got)
	}
	// Spooled (non-file) input and a bad sheet name.
	data, _ := os.ReadFile(path)
	if n := len(collect(ReadXLSX(context.Background(), bytes.NewReader(data), "2"))); n != 2 {
		t.Fatalf("spooled rows = %d", n)
	}
	bad := ReadXLSX(context.Background(), bytes.NewReader(data), "Nope")
	collect(bad)
	if bad.Err() == nil {
		t.Fatal("expected sheet error")
	}
}
