package pipeline

import (
	"archive/zip"
	"bytes"
	"compress/flate"
	"context"
	"encoding/binary"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf16"

	"github.com/klauspost/compress/zstd"
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

func TestXMLReadWrite(t *testing.T) {
	src := `<?xml version="1.0"?>
<export>
  <rows>
    <row id="1"><name>Acme Ltd</name><amount>1250</amount><address><city>Nairobi</city></address><tag>a</tag><tag>b</tag></row>
    <row id="2"><name>Kenya Power</name><amount/><address><city>Mombasa</city></address></row>
  </rows>
</export>`
	tb := ReadXML(context.Background(), strings.NewReader(src))
	rows := collect(tb)
	if err := tb.Err(); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(tb.Header, ","); got != "id,name,amount,address.city,tag,tag_2" {
		t.Fatalf("header %q", got)
	}
	if got := strings.Join(rows, ";"); got != "1,Acme Ltd,1250,Nairobi,a,b;2,Kenya Power,,Mombasa,," {
		t.Fatalf("rows %q", got)
	}
	// plain repeated elements under the root, with a declared Latin-1 encoding
	latin := "<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?><items><item><n>caf\xe9</n></item><item><n>x</n></item></items>"
	tb = ReadXML(context.Background(), strings.NewReader(latin))
	if rows := collect(tb); tb.Err() != nil || strings.Join(rows, ";") != "café;x" {
		t.Fatalf("latin1: rows %q err %v", rows, tb.Err())
	}
	var buf bytes.Buffer
	if n, err := Write(&buf, table(t, "Txn ID,Amount (KES),1st\n1,\"a<b\",x\n"), FormatXML); err != nil || n != 1 {
		t.Fatalf("write: n=%d err=%v", n, err)
	}
	out := buf.String()
	if !strings.Contains(out, "<row><Txn_ID>1</Txn_ID><Amount__KES_>a&lt;b</Amount__KES_><_1st>x</_1st></row>") {
		t.Fatalf("xml out:\n%s", out)
	}
	back := ReadXML(context.Background(), strings.NewReader(out))
	if rows := collect(back); strings.Join(rows, ";") != "1,a<b,x" {
		t.Fatalf("round trip %q", rows)
	}
	dir := t.TempDir()
	p := filepath.Join(dir, "data.txt") // no telling extension: the format comes from the content
	os.WriteFile(p, []byte(src), 0o644)
	if _, rows := readFile(t, p); len(rows) != 2 {
		t.Fatalf("sniffed xml: %q", rows)
	}
}

func TestODSFixtureAndRoundTrip(t *testing.T) {
	tb, rows := readFile(t, "../testdata/messy.ods")
	if got := strings.Join(tb.Header, "|"); got != "Invoice No|Customer Name |Region|Amount (KES)|Invoice Date|Paid?|Email" {
		t.Fatalf("header %q", got)
	}
	// Calc typed 1,250.00 as a number but kept $3,400, (500) and 15/02/2025 as text; the empty row is gone.
	if len(rows) != 8 || !strings.HasPrefix(rows[0], "1001,Acme Ltd,Nairobi,1250,2025-01-15,Yes,") || rows[1] != "1002, Kenya Power ,nairobi,$3,400,15/02/2025,no,billing@kplc.go.ke" {
		t.Fatalf("rows %q", rows)
	}
	for _, bad := range []string{"2", "Sheet9"} {
		r, _, closeFn, _ := Open(context.Background(), "../testdata/messy.ods", FormatAuto)
		x := ReadODS(context.Background(), r, bad)
		collect(x)
		closeFn()
		if err := x.Err(); err == nil || !strings.Contains(err.Error(), "not found") {
			t.Fatalf("sheet %q: err = %v", bad, err)
		}
	}
	dir := t.TempDir()
	p := filepath.Join(dir, "out.ods")
	f, _ := os.Create(p)
	src := "name,amount,when,note\nAmina,1250.5,2025-01-15,\"a\nb\"\nOtieno,007,2025-01-15T09:30:00Z,x & y\n"
	if n, err := Write(f, table(t, src), FormatODS); err != nil || n != 2 {
		t.Fatalf("write ods: n=%d err=%v", n, err)
	}
	f.Close()
	back, rows := readFile(t, p)
	if strings.Join(back.Header, ",") != "name,amount,when,note" || strings.Join(rows, ";") != "Amina,1250.5,2025-01-15,a\nb;Otieno,007,2025-01-15T09:30:00,x & y" {
		t.Fatalf("round trip: header %v rows %q", back.Header, rows)
	}
}

func TestZstdInput(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "data.csv.zst")
	f, _ := os.Create(p)
	zw, _ := zstd.NewWriter(f)
	zw.Write([]byte("a;b\n1;2\n"))
	zw.Close()
	f.Close()
	tb, rows := readFile(t, p)
	if strings.Join(tb.Header, ",") != "a,b" || strings.Join(rows, ";") != "1,2" {
		t.Fatalf("zstd: header %v rows %q", tb.Header, rows)
	}
}

// avroFile hand-encodes a small object container file so the reader is
// tested against the specification rather than against our own writer.
func avroFile(codec string, records [][]byte) []byte {
	schema := `{"type":"record","name":"T","namespace":"x","fields":[
	 {"name":"id","type":"long"},{"name":"name","type":"string"},{"name":"amount","type":["null","double"]},
	 {"name":"when","type":{"type":"int","logicalType":"date"}},
	 {"name":"addr","type":{"type":"record","name":"A","fields":[{"name":"city","type":"string"}]}},
	 {"name":"tags","type":{"type":"array","items":"string"}},{"name":"ok","type":"boolean"},
	 {"name":"kind","type":{"type":"enum","name":"K","symbols":["a","b"]}},
	 {"name":"price","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}}]}`
	var b []byte
	b = append(b, "Obj\x01"...)
	b = appendAvroLong(b, 2)
	b = appendAvroBytes(b, []byte("avro.schema"))
	b = appendAvroBytes(b, []byte(schema))
	b = appendAvroBytes(b, []byte("avro.codec"))
	b = appendAvroBytes(b, []byte(codec))
	b = appendAvroLong(b, 0)
	sync := bytes.Repeat([]byte{7}, 16)
	b = append(b, sync...)
	var block []byte
	for _, r := range records {
		block = append(block, r...)
	}
	if codec == "deflate" {
		var zb bytes.Buffer
		fw, _ := flate.NewWriter(&zb, 6)
		fw.Write(block)
		fw.Close()
		block = zb.Bytes()
	}
	b = appendAvroLong(b, int64(len(records)))
	b = appendAvroBytes(b, block)
	return append(b, sync...)
}

func avroRecord(id int64, name string, amount *float64, day int64, city string, tags []string, ok bool, kind int64, cents int64) []byte {
	var b []byte
	b = appendAvroLong(b, id)
	b = appendAvroBytes(b, []byte(name))
	if amount == nil {
		b = appendAvroLong(b, 0)
	} else {
		b = binary.LittleEndian.AppendUint64(appendAvroLong(b, 1), math.Float64bits(*amount))
	}
	b = appendAvroLong(b, day)
	b = appendAvroBytes(b, []byte(city))
	if len(tags) > 0 { // a block of items, then the empty block that ends the array
		b = appendAvroLong(b, int64(len(tags)))
		for _, t := range tags {
			b = appendAvroBytes(b, []byte(t))
		}
	}
	b = appendAvroLong(b, 0)
	if ok {
		b = append(b, 1)
	} else {
		b = append(b, 0)
	}
	b = appendAvroLong(b, kind)
	dec := big.NewInt(cents).Bytes()
	if cents < 0 {
		dec = []byte{0xff, 0xfe, 0x0c} // -500 in two's complement
	}
	return appendAvroBytes(b, dec)
}

func TestAvroReadWrite(t *testing.T) {
	amt := 1250.5
	recs := [][]byte{
		avroRecord(1, "Acme", &amt, 20103, "Nairobi", []string{"a", "b"}, true, 0, 125050),
		avroRecord(2, "Kenya Power", nil, 20134, "Mombasa", nil, false, 1, -500),
	}
	want := "1,Acme,1250.5,2025-01-15,Nairobi,[\"a\",\"b\"],true,a,1250.50;2,Kenya Power,,2025-02-15,Mombasa,null,false,b,-5.00"
	for _, codec := range []string{"null", "deflate"} {
		tb := ReadAvro(context.Background(), bytes.NewReader(avroFile(codec, recs)))
		rows := collect(tb)
		if err := tb.Err(); err != nil {
			t.Fatalf("%s: %v", codec, err)
		}
		if got := strings.Join(tb.Header, ","); got != "id,name,amount,when,addr.city,tags,ok,kind,price" {
			t.Fatalf("header %q", got)
		}
		if got := strings.Join(rows, ";"); got != want {
			t.Fatalf("%s rows %q", codec, got)
		}
	}
	dir := t.TempDir()
	p := filepath.Join(dir, "data.bin") // detected by its magic, not its name
	os.WriteFile(p, avroFile("null", recs), 0o644)
	if _, rows := readFile(t, p); len(rows) != 2 {
		t.Fatalf("sniffed avro: %q", rows)
	}
	// our writer, read back by our reader
	src := "Invoice No,Name,Amount (KES),Paid?,When,Stamp\n1,Acme,\"1,250.50\",yes,2025-01-15,2025-01-15T09:30:00Z\n2,KRA,500,no,n/a,\n3,X,abc,maybe,15/02/2025,\n"
	var buf bytes.Buffer
	if n, err := Write(&buf, table(t, src), FormatAvro); err != nil || n != 3 {
		t.Fatalf("write avro: n=%d err=%v", n, err)
	}
	back := ReadAvro(context.Background(), bytes.NewReader(buf.Bytes()))
	rows := collect(back)
	if err := back.Err(); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(back.Header, ","); got != "Invoice_No,Name,Amount__KES_,Paid_,When,Stamp" {
		t.Fatalf("header %q", got)
	}
	if got := strings.Join(rows, ";"); got != "1,Acme,1250.5,true,2025-01-15,2025-01-15T09:30:00.000Z;2,KRA,500,false,,;3,X,,,2025-02-15," {
		t.Fatalf("round trip %q", got)
	}
}
