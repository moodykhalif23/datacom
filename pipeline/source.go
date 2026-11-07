package pipeline

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
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

// FormatFromName guesses a format from a file name or URL path.
func FormatFromName(name string) (Format, bool) {
	name = strings.ToLower(strings.TrimSuffix(name, ".gz"))
	switch path.Ext(name) {
	case ".csv":
		return FormatCSV, true
	case ".tsv", ".tab":
		return FormatTSV, true
	case ".json":
		return FormatJSON, true
	case ".ndjson", ".jsonl":
		return FormatNDJSON, true
	case ".xlsx":
		return FormatXLSX, true
	case ".parquet", ".pq":
		return FormatParquet, true
	case ".md":
		return FormatMarkdown, true
	}
	return "", false
}

// Open returns a buffered reader for a file path, an http(s) URL, or stdin
// ("" or "-"), transparently decompressing gzip, plus the format guessed
// from the name or content when hint is auto.
func Open(ctx context.Context, src string, hint Format) (io.Reader, Format, func() error, error) {
	var r io.Reader
	closeFn := func() error { return nil }
	switch {
	case src == "" || src == "-":
		r = os.Stdin
	case strings.HasPrefix(src, "http://") || strings.HasPrefix(src, "https://"):
		r = newRemote(ctx, src)
	default:
		f, err := os.Open(src)
		if err != nil {
			return nil, "", nil, err
		}
		r, closeFn = f, f.Close
	}
	if f, ok := r.(*os.File); ok {
		if rf := randomAccessFormat(src, f); rf != "" {
			return f, rf, closeFn, nil // zip and parquet need the seekable file itself
		}
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
		f = guessFormat(src, br)
	}
	return br, f, closeFn, nil
}

func guessFormat(name string, br *bufio.Reader) Format {
	if f, ok := FormatFromName(name); ok && f != FormatMarkdown {
		return f
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

// Read streams rows from r. Cancelling ctx stops the pipeline; errors are
// reported by Table.Err once the Rows channel closes.
func Read(ctx context.Context, r io.Reader, f Format) *Table {
	switch f {
	case FormatCSV, FormatTSV:
		return readCSV(ctx, r, f == FormatTSV)
	case FormatNDJSON, FormatJSON:
		return readJSON(ctx, r, f == FormatJSON)
	case FormatXLSX:
		return ReadXLSX(ctx, r, "")
	case FormatParquet:
		return ReadParquet(ctx, r)
	}
	t, ch := newRoot(ctx, nil)
	t.fail(fmt.Errorf("unsupported input format %q", f))
	close(ch)
	return t
}

func readCSV(ctx context.Context, r io.Reader, tsv bool) *Table {
	cr := csv.NewReader(r)
	cr.FieldsPerRecord = -1
	cr.LazyQuotes = true
	cr.TrimLeadingSpace = true
	if tsv {
		cr.Comma = '\t'
	}
	hdr, err := cr.Read()
	if len(hdr) > 0 {
		hdr[0] = strings.TrimPrefix(hdr[0], "\uFEFF")
	}
	t, ch := newRoot(ctx, hdr)
	if err != nil {
		if err != io.EOF {
			t.fail(fmt.Errorf("read header: %w", err))
		}
		close(ch)
		return t
	}
	go func() {
		defer close(ch)
		for {
			rec, err := cr.Read()
			if err != nil {
				if err != io.EOF {
					t.fail(err)
				}
				return
			}
			if !t.send(ch, rec) {
				return
			}
		}
	}()
	return t
}

func readJSON(ctx context.Context, r io.Reader, array bool) *Table {
	dec := json.NewDecoder(r)
	if array {
		for {
			tok, err := dec.Token()
			if err != nil {
				t, ch := newRoot(ctx, nil)
				if err != io.EOF {
					t.fail(fmt.Errorf("json: no array found: %w", err))
				}
				close(ch)
				return t
			}
			if d, ok := tok.(json.Delim); ok && d == '[' {
				break
			}
		}
	}
	first := map[string]any{}
	err := io.EOF
	if !array || dec.More() {
		err = dec.Decode(&first)
	}
	t, ch := newRoot(ctx, sortedKeys(first))
	if err != nil {
		if err != io.EOF {
			t.fail(fmt.Errorf("read first object: %w", err))
		}
		close(ch)
		return t
	}
	go func() {
		defer close(ch)
		obj := first
		for {
			if !t.send(ch, objToRow(obj, t.Header)) {
				return
			}
			if array && !dec.More() {
				return
			}
			obj = map[string]any{}
			if err := dec.Decode(&obj); err != nil {
				if err != io.EOF {
					t.fail(err)
				}
				return
			}
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
	}
	b, _ := json.Marshal(v)
	return string(b)
}

// randomAccessFormat recognises a workbook or Parquet file by extension or
// by its signature; these formats cannot be streamed through a pipe.
func randomAccessFormat(name string, f *os.File) Format {
	if fm, _ := FormatFromName(name); fm == FormatXLSX || fm == FormatParquet {
		return fm
	}
	if f == os.Stdin {
		return ""
	}
	var magic [4]byte
	if n, _ := f.ReadAt(magic[:], 0); n == 4 {
		switch string(magic[:]) {
		case "PK\x03\x04":
			return FormatXLSX
		case "PAR1":
			return FormatParquet
		}
	}
	return ""
}
