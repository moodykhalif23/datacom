package pipeline

import (
	"archive/zip"
	"bufio"
	"bytes"
	"fmt"
	"html"
	"io"
	"os"
	"path"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

func sniffDelimiter(br *bufio.Reader) rune {
	head, _ := br.Peek(64 << 10)
	if i := bytes.IndexByte(head, '\n'); i >= 0 {
		head = head[:i]
	}
	counts := map[byte]int{}
	quoted := false
	for _, c := range head {
		switch c {
		case '"':
			quoted = !quoted
		case ',', ';', '\t', '|':
			if !quoted {
				counts[c]++
			}
		}
	}
	best, n := byte(','), counts[',']
	for _, c := range []byte{';', '\t', '|'} {
		if counts[c] > n {
			best, n = c, counts[c]
		}
	}
	return rune(best)
}

// utf16Reader turns UTF-16 text, as Excel's "Unicode Text" export writes
// it, into UTF-8. The byte order mark has already been consumed.
type utf16Reader struct {
	r   io.Reader
	big bool
	out []byte
	odd []byte // a trailing byte waiting for its partner
	hi  rune   // a high surrogate waiting for its low half
}

func newUTF16Reader(r io.Reader, bigEndian bool) *utf16Reader {
	return &utf16Reader{r: r, big: bigEndian}
}

func (u *utf16Reader) Read(p []byte) (int, error) {
	for len(u.out) == 0 {
		buf := make([]byte, 32<<10)
		n, err := u.r.Read(buf)
		raw := append(u.odd, buf[:n]...)
		u.odd = nil
		if len(raw)%2 == 1 {
			u.odd, raw = raw[len(raw)-1:], raw[:len(raw)-1]
		}
		for i := 0; i+1 < len(raw); i += 2 {
			var c rune
			if u.big {
				c = rune(raw[i])<<8 | rune(raw[i+1])
			} else {
				c = rune(raw[i+1])<<8 | rune(raw[i])
			}
			switch {
			case u.hi != 0:
				c, u.hi = utf16.DecodeRune(u.hi, c), 0
			case utf16.IsSurrogate(c) && c < 0xDC00:
				u.hi = c
				continue
			}
			u.out = utf8.AppendRune(u.out, c)
		}
		if err != nil {
			if len(u.out) == 0 {
				return 0, err
			}
			break
		}
	}
	n := copy(p, u.out)
	u.out = u.out[n:]
	return n, nil
}

// zipKind tells an Excel workbook and an OpenDocument spreadsheet from a
// plain zip archive by the files inside.
func zipKind(f *os.File) Format {
	st, err := f.Stat()
	if err != nil {
		return FormatZip
	}
	zr, err := zip.NewReader(f, st.Size())
	if err != nil {
		return FormatZip
	}
	for _, e := range zr.File {
		switch e.Name {
		case "xl/workbook.xml":
			return FormatXLSX
		case "mimetype":
			if rc, err := e.Open(); err == nil {
				b, _ := io.ReadAll(io.LimitReader(rc, 128))
				rc.Close()
				if strings.Contains(string(b), "opendocument.spreadsheet") {
					return FormatODS
				}
			}
		}
	}
	return FormatZip
}

// zipEntry opens the first data file inside a zip archive: an export that
// arrives zipped is read as if it had been unpacked. The entry's name is
// returned so the format can be guessed from it.
func zipEntry(f *os.File) (io.ReadCloser, string, error) {
	st, err := f.Stat()
	if err != nil {
		return nil, "", err
	}
	zr, err := zip.NewReader(f, st.Size())
	if err != nil {
		return nil, "", fmt.Errorf("not a zip archive: %w", err)
	}
	for _, e := range zr.File {
		if e.FileInfo().IsDir() || strings.HasPrefix(path.Base(e.Name), ".") {
			continue
		}
		if fm, ok := FormatFromName(e.Name); ok && fm != FormatMarkdown && fm != FormatHTML && fm != FormatSQL {
			rc, err := e.Open()
			return rc, e.Name, err
		}
	}
	return nil, "", fmt.Errorf("the zip archive has no csv, tsv, json, ndjson, xlsx or parquet file in it")
}

// writeHTML writes a plain table that opens in any browser or pastes into
// a document.
func writeHTML(w *bufio.Writer, t *Table) (int, error) {
	w.WriteString("<!doctype html>\n<meta charset=\"utf-8\">\n<title>datacom</title>\n" +
		"<style>table{border-collapse:collapse;font:14px system-ui,sans-serif}th,td{border:1px solid #ddd;padding:4px 8px;text-align:left;white-space:nowrap}th{background:#f3f3f3}</style>\n<table>\n<thead><tr>")
	for _, h := range t.Header {
		w.WriteString("<th>" + html.EscapeString(h) + "</th>")
	}
	w.WriteString("</tr></thead>\n<tbody>\n")
	n := 0
	for r := range t.Rows {
		w.WriteString("<tr>")
		for i := range t.Header {
			w.WriteString("<td>" + html.EscapeString(r.Get(i)) + "</td>")
		}
		if _, err := w.WriteString("</tr>\n"); err != nil {
			return n, err
		}
		n++
	}
	_, err := w.WriteString("</tbody>\n</table>\n")
	return n, err
}

func WriteSQL(w io.Writer, t *Table, table string) (int, error) {
	bw := bufio.NewWriterSize(w, 1<<20)
	const sample, perInsert = 1000, 500
	var head []Row
	tallies := make([]tally, len(t.Header))
	timed := make([]bool, len(t.Header))
	for r := range t.Rows {
		head = append(head, r)
		for i := 0; i < len(tallies) && i < len(r); i++ {
			k := Infer(r[i])
			tallies[i].add(k)
			timed[i] = timed[i] || k == KindDate && len(strings.TrimSpace(r[i])) > 10
		}
		if len(head) == sample {
			break
		}
	}
	kinds := make([]Kind, len(t.Header))
	cols := make([]string, len(t.Header))
	for i, h := range t.Header {
		kinds[i] = tallies[i].kind()
		typ := "TEXT"
		switch kinds[i] {
		case KindInt:
			typ = "BIGINT"
		case KindFloat:
			typ = "DOUBLE PRECISION"
		case KindBool:
			typ = "BOOLEAN"
		case KindDate:
			typ = "DATE"
			if timed[i] {
				typ = "TIMESTAMP"
			}
		}
		cols[i] = sqlIdent(h) + " " + typ
	}
	name := sqlIdent(table)
	fmt.Fprintf(bw, "CREATE TABLE IF NOT EXISTS %s (\n  %s\n);\n", name, strings.Join(cols, ",\n  "))
	names := make([]string, len(t.Header))
	for i, h := range t.Header {
		names[i] = sqlIdent(h)
	}
	insert := "INSERT INTO " + name + " (" + strings.Join(names, ", ") + ") VALUES\n"
	n := 0
	write := func(r Row) error {
		if n%perInsert == 0 {
			if n > 0 {
				bw.WriteString(";\n")
			}
			bw.WriteString(insert)
		} else {
			bw.WriteString(",\n")
		}
		bw.WriteByte('(')
		for i, k := range kinds {
			if i > 0 {
				bw.WriteString(", ")
			}
			bw.WriteString(sqlValue(r.Get(i), k))
		}
		_, err := bw.WriteString(")")
		n++
		return err
	}
	for _, r := range head {
		if err := write(r); err != nil {
			return n, err
		}
	}
	for r := range t.Rows {
		if err := write(r); err != nil {
			return n, err
		}
	}
	if n > 0 {
		bw.WriteString(";\n")
	}
	if err := bw.Flush(); err != nil {
		return n, err
	}
	return n, t.Err()
}

func sqlIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func sqlValue(v string, k Kind) string {
	if IsNull(v) {
		return "NULL"
	}
	switch k {
	case KindInt, KindFloat:
		if n := Normalize(v, k); n != "" {
			if _, ok := ParseFloat(n); ok && !isCode(n) {
				return n
			}
		}
		return "NULL"
	case KindBool:
		switch Normalize(v, KindBool) {
		case "true":
			return "TRUE"
		case "false":
			return "FALSE"
		}
		return "NULL"
	case KindDate:
		if _, ok := ParseDate(v); ok {
			return "'" + Normalize(v, KindDate) + "'"
		}
		return "NULL"
	}
	return "'" + strings.ReplaceAll(v, "'", "''") + "'"
}
