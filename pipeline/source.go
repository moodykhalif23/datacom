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
	"strconv"
	"strings"

	"github.com/klauspost/compress/zstd"
)

// Format names an input or output encoding.
type Format string

const (
	FormatAuto     Format = "auto"
	FormatCSV      Format = "csv"
	FormatTSV      Format = "tsv"
	FormatJSON     Format = "json"
	FormatNDJSON   Format = "ndjson"
	FormatTable    Format = "table"
	FormatMarkdown Format = "md"
	FormatHTML     Format = "html"
	FormatSQL      Format = "sql"
	FormatZip      Format = "zip"
	FormatXML      Format = "xml"
)

// FormatFromName guesses a format from a file name or URL path.
func FormatFromName(name string) (Format, bool) {
	name = strings.TrimSuffix(strings.TrimSuffix(name, ".gz"), ".zst")
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
	case ".html", ".htm":
		return FormatHTML, true
	case ".sql":
		return FormatSQL, true
	case ".zip":
		return FormatZip, true
	case ".xml":
		return FormatXML, true
	case ".ods":
		return FormatODS, true
	case ".avro":
		return FormatAvro, true
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
	case IsCloud(src):
		u, sign, err := cloudURL(src)
		if err != nil {
			return nil, "", nil, err
		}
		rm := newRemote(ctx, u)
		rm.sign = sign
		r = rm
	default:
		f, err := os.Open(src)
		if err != nil {
			return nil, "", nil, err
		}
		r, closeFn = f, f.Close
	}
	if l := ledgerFrom(ctx); l != nil {
		if f, ok := r.(*os.File); ok && f != os.Stdin {
			l.file(src)
		} else if src == "" || src == "-" {
			r = l.stream("stdin", r)
		} else {
			r = l.stream(src, r)
		}
	}
	if f, ok := r.(*os.File); ok {
		rf := randomAccessFormat(src, f)
		if rf == FormatZip {
			entry, name, err := zipEntry(f)
			if err != nil {
				closeFn()
				return nil, "", nil, err
			}
			fileClose := closeFn
			r, src, closeFn = entry, name, func() error { entry.Close(); return fileClose() }
			if fm, _ := FormatFromName(name); fm == FormatXLSX || fm == FormatParquet || fm == FormatODS {
				return r, fm, closeFn, nil // these spool the entry to a temporary file themselves
			}
		} else if rf != "" {
			return f, rf, closeFn, nil // xlsx and parquet need the seekable file itself
		}
	}
	br, err := Decompress(r, false)
	if err != nil {
		closeFn()
		return nil, "", nil, err
	}
	f := hint
	if f == "" || f == FormatAuto {
		f = guessFormat(src, br)
	}
	return br, f, closeFn, nil
}

// Decompress returns r with gzip or zstd compression and a UTF-16 byte order
// mark removed, detected from the first bytes. Open and the watch follower
// share it, so a growing .gz file is read the same way as a finished one.
// live is for a reader that never reports EOF: gzip members are then read
// one at a time, so the last rows of a member are delivered as soon as they
// are decoded instead of being held while the reader waits for the next
// member's header to appear.
func Decompress(r io.Reader, live bool) (*bufio.Reader, error) {
	br := bufio.NewReaderSize(r, 1<<20)
	magic, _ := br.Peek(2)
	switch {
	case len(magic) == 2 && magic[0] == 0x1f && magic[1] == 0x8b:
		gz, err := gzip.NewReader(br)
		if err != nil {
			return nil, err
		}
		if live {
			gz.Multistream(false)
			br = bufio.NewReaderSize(&liveGzip{src: br, gz: gz}, 1<<20)
		} else {
			br = bufio.NewReaderSize(gz, 1<<20)
		}
	case len(magic) == 2 && magic[0] == 0x28 && magic[1] == 0xb5: // zstd
		zr, err := zstd.NewReader(br)
		if err != nil {
			return nil, err
		}
		br = bufio.NewReaderSize(zr, 1<<20)
	}
	if bom, _ := br.Peek(2); len(bom) == 2 && (bom[0] == 0xff && bom[1] == 0xfe || bom[0] == 0xfe && bom[1] == 0xff) {
		br.Discard(2)
		br = bufio.NewReaderSize(newUTF16Reader(br, bom[0] == 0xfe), 1<<20)
	}
	return br, nil
}

// liveGzip reads a growing gzip file member by member. Go's reader would
// otherwise hold the final decoded bytes of a member while it blocks on the
// next header, which on a followed file may never come.
type liveGzip struct {
	src io.Reader
	gz  *gzip.Reader
}

func (l *liveGzip) Read(p []byte) (int, error) {
	for {
		n, err := l.gz.Read(p)
		if err != io.EOF {
			return n, err
		}
		if n > 0 {
			return n, nil
		}
		if err := l.gz.Reset(l.src); err != nil { // blocks until the next member arrives
			return 0, err
		}
		l.gz.Multistream(false)
	}
}

func guessFormat(name string, br *bufio.Reader) Format {
	if f, ok := FormatFromName(name); ok && f != FormatMarkdown && f != FormatHTML && f != FormatSQL && f != FormatZip {
		return f
	}
	head, _ := br.Peek(4096)
	t := bytes.TrimLeft(head, " \t\r\n\uFEFF")
	switch {
	case bytes.HasPrefix(head, avroMagic):
		return FormatAvro
	case len(t) > 0 && t[0] == '<':
		return FormatXML
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
	case FormatCSV:
		br, ok := r.(*bufio.Reader)
		if !ok {
			br = bufio.NewReader(r)
		}
		return readCSV(ctx, br, sniffDelimiter(br))
	case FormatTSV:
		return readCSV(ctx, r, '\t')
	case FormatNDJSON, FormatJSON, "jsonl":
		return readJSON(ctx, r, f == FormatJSON)
	case FormatXLSX:
		return ReadXLSX(ctx, r, "")
	case FormatODS:
		return ReadODS(ctx, r, "")
	case FormatParquet:
		return ReadParquet(ctx, r)
	case FormatXML:
		return ReadXML(ctx, r)
	case FormatAvro:
		return ReadAvro(ctx, r)
	}
	t, ch := newRoot(ctx, nil)
	t.fail(fmt.Errorf("unsupported input format %q", f))
	close(ch)
	return t
}

func readCSV(ctx context.Context, r io.Reader, comma rune) *Table {
	cr := csv.NewReader(r)
	cr.FieldsPerRecord = -1
	cr.LazyQuotes = true
	cr.TrimLeadingSpace = true
	cr.Comma = comma
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
	var keys []string
	first := map[string]any{}
	err := io.EOF
	if !array || dec.More() {
		keys, first, err = firstObject(dec)
	}
	t, ch := newRoot(ctx, keys)
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

// firstObject decodes one object token by token so the column order is
// the order the keys were written in.
func firstObject(dec *json.Decoder) ([]string, map[string]any, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, nil, err
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return nil, nil, fmt.Errorf("json: expected an object, got %v", tok)
	}
	var keys []string
	m := map[string]any{}
	for dec.More() {
		k, err := dec.Token()
		if err != nil {
			return nil, nil, err
		}
		var v any
		if err := dec.Decode(&v); err != nil {
			return nil, nil, err
		}
		name := fmt.Sprint(k)
		if _, dup := m[name]; !dup {
			keys = append(keys, name)
		}
		m[name] = v
	}
	_, err = dec.Token() // closing brace
	return keys, m, err
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
	if fm, _ := FormatFromName(name); fm == FormatXLSX || fm == FormatParquet || fm == FormatZip || fm == FormatODS {
		return fm
	}
	if f == os.Stdin {
		return ""
	}
	var magic [4]byte
	if n, _ := f.ReadAt(magic[:], 0); n == 4 {
		switch string(magic[:]) {
		case "PK\x03\x04": // a spreadsheet package, or a zip archive with a data file inside
			return zipKind(f)
		case "PAR1":
			return FormatParquet
		}
	}
	return ""
}
