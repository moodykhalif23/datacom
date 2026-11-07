package pipeline

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Format is an input or output encoding.
type Format string

const (
	FormatAuto     Format = "auto"
	FormatCSV      Format = "csv"
	FormatTSV      Format = "tsv"
	FormatJSON     Format = "json"
	FormatNDJSON   Format = "ndjson"
	FormatTable    Format = "table"
	FormatMarkdown Format = "md"
)

// Open returns a reader for path ("" or "-" means stdin), transparently
// decompressing gzip, plus the format guessed from the name or content.
func Open(path string, hint Format) (io.Reader, Format, func() error, error) {
	var r io.Reader
	closeFn := func() error { return nil }
	if path == "" || path == "-" {
		r = os.Stdin
	} else {
		f, err := os.Open(path)
		if err != nil {
			return nil, "", nil, err
		}
		r = f
		closeFn = f.Close
	}
	br := bufio.NewReaderSize(r, 1<<20)
	if magic, _ := br.Peek(2); len(magic) == 2 && magic[0] == 0x1f && magic[1] == 0x8b {
		gz, err := gzip.NewReader(br)
		if err != nil {
			closeFn()
			return nil, "", nil, err
		}
		br = bufio.NewReaderSize(gz, 1<<20)
	}
	f := hint
	if f == "" || f == FormatAuto {
		f = guessFormat(path, br)
	}
	return br, f, closeFn, nil
}

func guessFormat(path string, br *bufio.Reader) Format {
	name := strings.ToLower(strings.TrimSuffix(path, ".gz"))
	switch filepath.Ext(name) {
	case ".csv":
		return FormatCSV
	case ".tsv", ".tab":
		return FormatTSV
	case ".json":
		return FormatJSON
	case ".ndjson", ".jsonl":
		return FormatNDJSON
	}
	head, _ := br.Peek(4096)
	t := bytes.TrimLeft(head, " \t\r\n\uFEFF")
	switch {
	case len(t) > 0 && t[0] == '[':
		return FormatJSON
	case len(t) > 0 && t[0] == '{':
		if i := bytes.IndexByte(t, '\n'); i > 0 && bytes.HasPrefix(bytes.TrimSpace(t[i:]), []byte("{")) {
			return FormatNDJSON
		}
		return FormatJSON
	case bytes.Count(t, []byte("\t")) > bytes.Count(t, []byte(",")):
		return FormatTSV
	}
	return FormatCSV
}

// Read streams rows from r in the given format. Errors surface through
// Table.Err after the channel closes.
func Read(r io.Reader, f Format) *Table {
	var err error
	t := &Table{Err: &err}
	switch f {
	case FormatCSV, FormatTSV:
		return readCSV(r, f == FormatTSV, t)
	case FormatNDJSON:
		return readNDJSON(r, t)
	case FormatJSON:
		return readJSON(r, t)
	}
	err = fmt.Errorf("unsupported input format %q", f)
	ch := make(chan Row)
	close(ch)
	t.Rows = ch
	return t
}

func readCSV(r io.Reader, tsv bool, t *Table) *Table {
	cr := csv.NewReader(r)
	cr.ReuseRecord = false
	cr.FieldsPerRecord = -1
	cr.LazyQuotes = true
	cr.TrimLeadingSpace = true
	if tsv {
		cr.Comma = '\t'
	}
	hdr, err := cr.Read()
	if err != nil {
		*t.Err = fmt.Errorf("read header: %w", err)
		ch := make(chan Row)
		close(ch)
		t.Rows = ch
		return t
	}
	if len(hdr) > 0 {
		hdr[0] = strings.TrimPrefix(hdr[0], "\uFEFF")
	}
	t.Header = hdr
	ch := make(chan Row, 1024)
	t.Rows = ch
	go func() {
		defer close(ch)
		for {
			rec, err := cr.Read()
			if err == io.EOF {
				return
			}
			if err != nil {
				*t.Err = err
				return
			}
			ch <- Row(rec)
		}
	}()
	return t
}

// readNDJSON infers the header from the first object and keeps key order
// stable; keys that appear later are appended.
func readNDJSON(r io.Reader, t *Table) *Table {
	dec := json.NewDecoder(r)
	ch := make(chan Row, 1024)
	t.Rows = ch
	first := map[string]any{}
	if err := dec.Decode(&first); err != nil {
		if err != io.EOF {
			*t.Err = fmt.Errorf("read first object: %w", err)
		}
		close(ch)
		return t
	}
	t.Header = sortedKeys(first)
	go func() {
		defer close(ch)
		ch <- objToRow(first, t.Header)
		for {
			obj := map[string]any{}
			if err := dec.Decode(&obj); err != nil {
				if err != io.EOF {
					*t.Err = err
				}
				return
			}
			ch <- objToRow(obj, t.Header)
		}
	}()
	return t
}

// readJSON streams a top-level array of objects token by token.
func readJSON(r io.Reader, t *Table) *Table {
	dec := json.NewDecoder(r)
	ch := make(chan Row, 1024)
	t.Rows = ch
	tok, err := dec.Token()
	if err != nil {
		*t.Err = fmt.Errorf("read json: %w", err)
		close(ch)
		return t
	}
	if d, ok := tok.(json.Delim); !ok || d != '[' {
		// Allow {"data":[...]} style wrappers: skip to the first array.
		for {
			if d, ok := tok.(json.Delim); ok && d == '[' {
				break
			}
			tok, err = dec.Token()
			if err != nil {
				*t.Err = fmt.Errorf("json: no array of objects found")
				close(ch)
				return t
			}
		}
	}
	if !dec.More() {
		close(ch)
		return t
	}
	first := map[string]any{}
	if err := dec.Decode(&first); err != nil {
		*t.Err = err
		close(ch)
		return t
	}
	t.Header = sortedKeys(first)
	go func() {
		defer close(ch)
		ch <- objToRow(first, t.Header)
		for dec.More() {
			obj := map[string]any{}
			if err := dec.Decode(&obj); err != nil {
				*t.Err = err
				return
			}
			ch <- objToRow(obj, t.Header)
		}
	}()
	return t
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func objToRow(m map[string]any, header []string) Row {
	row := make(Row, len(header))
	for i, k := range header {
		row[i] = scalar(m[k])
	}
	return row
}

func scalar(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(x)
	default:
		b, _ := json.Marshal(x)
		return string(b)
	}
}
