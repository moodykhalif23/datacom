package pipeline

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"regexp"
	"strings"
	"unicode/utf8"
)

// Write streams t to w in format f and returns the number of rows written
// and the first error from the writer or any upstream stage.
func Write(w io.Writer, t *Table, f Format) (int, error) {
	bw := bufio.NewWriterSize(w, 1<<20)
	var n int
	var err error
	switch f {
	case FormatCSV, FormatTSV, "":
		n, err = writeCSV(bw, t, f == FormatTSV)
	case FormatNDJSON, "jsonl":
		n, err = writeJSON(bw, t, false)
	case FormatJSON:
		n, err = writeJSON(bw, t, true)
	case FormatTable, FormatMarkdown:
		n, err = writeTable(bw, t, f == FormatMarkdown)
	case FormatHTML:
		n, err = writeHTML(bw, t)
	case FormatXML:
		n, err = writeXML(bw, t)
	case FormatODS:
		n, err = writeODS(bw, t)
	case FormatAvro:
		n, err = writeAvro(bw, t)
	case FormatSQL:
		return WriteSQL(w, t, "data")
	case FormatXLSX:
		n, err = writeXLSX(bw, t)
	case FormatParquet:
		n, err = writeParquet(bw, t)
	default:
		return 0, fmt.Errorf("unsupported output format %q", f)
	}
	if err == nil {
		err = bw.Flush()
	}
	if err == nil {
		err = t.Err()
	}
	if mon := t.monitor(); mon != nil {
		mon.RowsOut.Add(int64(n))
		mon.Fail(err)
	}
	if err != nil {
		t.fail(err) // stop upstream goroutines
	}
	return n, err
}

func writeCSV(w io.Writer, t *Table, tsv bool) (int, error) {
	cw := csv.NewWriter(w)
	if tsv {
		cw.Comma = '\t'
	}
	if err := cw.Write(t.Header); err != nil {
		return 0, err
	}
	n := 0
	for r := range t.Rows {
		if err := cw.Write(r); err != nil {
			return n, err
		}
		n++
	}
	cw.Flush()
	return n, cw.Error()
}

var jsonNumber = regexp.MustCompile(`^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$`)

func writeJSON(w *bufio.Writer, t *Table, array bool) (int, error) {
	keys := make([][]byte, len(t.Header))
	for i, h := range t.Header {
		keys[i] = append(appendJSONString(nil, h), ':')
	}
	if array {
		w.WriteString("[")
	}
	n := 0
	buf := make([]byte, 0, 1024)
	for r := range t.Rows {
		buf = buf[:0]
		if array {
			if n > 0 {
				buf = append(buf, ',')
			}
			buf = append(buf, "\n  "...)
		}
		buf = append(buf, '{')
		for i, k := range keys {
			if i > 0 {
				buf = append(buf, ',')
			}
			buf = appendJSONValue(append(buf, k...), r.Get(i))
		}
		buf = append(buf, '}')
		if !array {
			buf = append(buf, '\n')
		}
		if _, err := w.Write(buf); err != nil {
			return n, err
		}
		n++
	}
	if array {
		w.WriteString("\n]\n")
	}
	return n, nil
}

// appendJSONValue writes canonical numbers and booleans as JSON numbers and
// booleans, empty strings as null, and everything else as a string.
func appendJSONValue(b []byte, v string) []byte {
	switch {
	case v == "":
		return append(b, "null"...)
	case v == "true" || v == "false":
		return append(b, v...)
	case len(v) <= 20 && jsonNumber.MatchString(v):
		return append(b, v...)
	}
	return appendJSONString(b, v)
}

const hexDigits = "0123456789abcdef"

func appendJSONString(b []byte, s string) []byte {
	b = append(b, '"')
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c == '"' || c == '\\':
			b = append(b, '\\', c)
		case c == '\n':
			b = append(b, '\\', 'n')
		case c == '\r':
			b = append(b, '\\', 'r')
		case c == '\t':
			b = append(b, '\\', 't')
		case c < 0x20:
			b = append(b, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xf])
		default:
			b = append(b, c)
		}
	}
	return append(b, '"')
}

func writeTable(w io.Writer, t *Table, md bool) (int, error) {
	const maxWidth = 40
	var rows []Row
	widths := make([]int, len(t.Header))
	numeric := make([]bool, len(t.Header))
	for i, h := range t.Header {
		widths[i] = utf8.RuneCountInString(h)
		numeric[i] = true
	}
	for r := range t.Rows {
		for i := range t.Header {
			v := r.Get(i)
			widths[i] = max(widths[i], min(utf8.RuneCountInString(v), maxWidth))
			if v != "" && numeric[i] {
				_, numeric[i] = ParseFloat(v)
			}
		}
		rows = append(rows, r)
	}
	line := func(r Row) string {
		parts := make([]string, len(t.Header))
		for i := range t.Header {
			v := trunc(r.Get(i), maxWidth)
			pad := strings.Repeat(" ", widths[i]-utf8.RuneCountInString(v))
			if numeric[i] {
				parts[i] = pad + v
			} else {
				parts[i] = v + pad
			}
		}
		if md {
			return "| " + strings.Join(parts, " | ") + " |"
		}
		return strings.Join(parts, "  ")
	}
	seps := make([]string, len(widths))
	for i, wd := range widths {
		switch {
		case md && numeric[i]:
			seps[i] = strings.Repeat("-", wd) + ":"
		case md:
			seps[i] = strings.Repeat("-", wd+1)
		default:
			seps[i] = strings.Repeat("─", wd)
		}
	}
	fmt.Fprintln(w, line(Row(t.Header)))
	if md {
		fmt.Fprintln(w, "|"+strings.Join(seps, "|")+"|")
	} else {
		fmt.Fprintln(w, strings.Join(seps, "  "))
	}
	for _, r := range rows {
		if _, err := fmt.Fprintln(w, line(r)); err != nil {
			return 0, err
		}
	}
	return len(rows), nil
}

// trunc shortens s to n runes with an ellipsis.
func trunc(s string, n int) string {
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	return string([]rune(s)[:n-1]) + "…"
}
