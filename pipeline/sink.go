package pipeline

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strings"
	"unicode/utf8"
)

// Write streams t to w in format f and returns the number of rows written.
// Table and Markdown buffer rows to size columns; the others stream.
func Write(w io.Writer, t *Table, f Format) (int, error) {
	bw := bufio.NewWriterSize(w, 1<<20)
	defer bw.Flush()
	var n int
	var err error
	switch f {
	case FormatCSV, FormatTSV, "":
		n, err = writeCSV(bw, t, f == FormatTSV)
	case FormatNDJSON:
		n, err = writeNDJSON(bw, t)
	case FormatJSON:
		n, err = writeJSON(bw, t)
	case FormatTable:
		n, err = writeTable(bw, t, false)
	case FormatMarkdown:
		n, err = writeTable(bw, t, true)
	default:
		return 0, fmt.Errorf("unsupported output format %q", f)
	}
	if err != nil {
		return n, err
	}
	if t.Err != nil && *t.Err != nil {
		return n, *t.Err
	}
	return n, nil
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

func rowObject(t *Table, r Row) map[string]any {
	m := make(map[string]any, len(t.Header))
	for i, h := range t.Header {
		m[h] = jsonValue(r.Get(i))
	}
	return m
}

var jsonNumber = regexp.MustCompile(`^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$`)

// jsonValue emits canonical numbers and booleans as JSON numbers and
// booleans, empty strings as null, and everything else as a string.
func jsonValue(v string) any {
	switch {
	case v == "":
		return nil
	case v == "true":
		return true
	case v == "false":
		return false
	case len(v) <= 20 && jsonNumber.MatchString(v):
		return json.Number(v)
	}
	return v
}

func writeNDJSON(w io.Writer, t *Table) (int, error) {
	enc := json.NewEncoder(w)
	n := 0
	for r := range t.Rows {
		if err := enc.Encode(rowObject(t, r)); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

func writeJSON(w io.Writer, t *Table) (int, error) {
	if _, err := io.WriteString(w, "["); err != nil {
		return 0, err
	}
	n := 0
	for r := range t.Rows {
		b, err := json.Marshal(rowObject(t, r))
		if err != nil {
			return n, err
		}
		if n > 0 {
			io.WriteString(w, ",")
		}
		io.WriteString(w, "\n  ")
		w.Write(b)
		n++
	}
	_, err := io.WriteString(w, "\n]\n")
	return n, err
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
			if l := utf8.RuneCountInString(v); l > widths[i] {
				widths[i] = min(l, maxWidth)
			}
			if v != "" && numeric[i] {
				if _, ok := ParseFloat(v); !ok {
					numeric[i] = false
				}
			}
		}
		rows = append(rows, r)
	}
	cell := func(v string, i int) string {
		if utf8.RuneCountInString(v) > maxWidth {
			v = string([]rune(v)[:maxWidth-1]) + "…"
		}
		pad := widths[i] - utf8.RuneCountInString(v)
		if numeric[i] {
			return strings.Repeat(" ", pad) + v
		}
		return v + strings.Repeat(" ", pad)
	}
	line := func(r Row) string {
		parts := make([]string, len(t.Header))
		for i := range t.Header {
			parts[i] = cell(r.Get(i), i)
		}
		if md {
			return "| " + strings.Join(parts, " | ") + " |"
		}
		return strings.Join(parts, "  ")
	}
	fmt.Fprintln(w, line(Row(t.Header)))
	if md {
		seps := make([]string, len(widths))
		for i, wd := range widths {
			if numeric[i] {
				seps[i] = strings.Repeat("-", wd) + ":"
			} else {
				seps[i] = strings.Repeat("-", wd+1)
			}
		}
		fmt.Fprintln(w, "|"+strings.Join(seps, "|")+"|")
	} else {
		seps := make([]string, len(widths))
		for i, wd := range widths {
			seps[i] = strings.Repeat("─", wd)
		}
		fmt.Fprintln(w, strings.Join(seps, "  "))
	}
	for _, r := range rows {
		fmt.Fprintln(w, line(r))
	}
	return len(rows), nil
}
