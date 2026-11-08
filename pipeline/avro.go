package pipeline

import (
	"bufio"
	"bytes"
	"compress/bzip2"
	"compress/flate"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
)

// FormatAvro is an Avro object container file, the row format Kafka and
// Hadoop pipelines exchange.
const FormatAvro Format = "avro"

var avroMagic = []byte("Obj\x01")

// avroSchema is one node of a parsed Avro schema.
type avroSchema struct {
	typ      string // null boolean int long float double bytes string record enum array map fixed union
	name     string
	fields   []avroField
	items    *avroSchema
	values   *avroSchema
	branches []*avroSchema
	symbols  []string
	size     int
	logical  string
	scale    int
}

type avroField struct {
	name   string
	schema *avroSchema
}

// parseAvroSchema turns the JSON schema from the file header into a tree,
// resolving named types used again later in the schema.
func parseAvroSchema(raw []byte) (*avroSchema, error) {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, fmt.Errorf("avro schema: %w", err)
	}
	named := map[string]*avroSchema{}
	var parse func(v any, ns string) (*avroSchema, error)
	parse = func(v any, ns string) (*avroSchema, error) {
		switch x := v.(type) {
		case string:
			switch x {
			case "null", "boolean", "int", "long", "float", "double", "bytes", "string":
				return &avroSchema{typ: x}, nil
			}
			if s := named[x]; s != nil {
				return s, nil
			}
			if s := named[ns+"."+x]; s != nil {
				return s, nil
			}
			return nil, fmt.Errorf("avro schema: unknown type %q", x)
		case []any:
			u := &avroSchema{typ: "union"}
			for _, b := range x {
				s, err := parse(b, ns)
				if err != nil {
					return nil, err
				}
				u.branches = append(u.branches, s)
			}
			return u, nil
		case map[string]any:
			typ, _ := x["type"].(string)
			s := &avroSchema{typ: typ}
			s.logical, _ = x["logicalType"].(string)
			if sc, ok := x["scale"].(float64); ok {
				s.scale = int(sc)
			}
			if n, ok := x["namespace"].(string); ok {
				ns = n
			}
			if name, ok := x["name"].(string); ok {
				s.name = name
				named[name] = s
				if ns != "" && !strings.Contains(name, ".") {
					named[ns+"."+name] = s
				}
			}
			var err error
			switch typ {
			case "record", "error":
				s.typ = "record"
				fs, _ := x["fields"].([]any)
				for _, f := range fs {
					fm, _ := f.(map[string]any)
					fname, _ := fm["name"].(string)
					fs, err := parse(fm["type"], ns)
					if err != nil {
						return nil, err
					}
					s.fields = append(s.fields, avroField{fname, fs})
				}
			case "array":
				s.items, err = parse(x["items"], ns)
			case "map":
				s.values, err = parse(x["values"], ns)
			case "enum":
				syms, _ := x["symbols"].([]any)
				for _, y := range syms {
					s.symbols = append(s.symbols, fmt.Sprint(y))
				}
			case "fixed":
				if n, ok := x["size"].(float64); ok {
					s.size = int(n)
				}
			case "null", "boolean", "int", "long", "float", "double", "bytes", "string":
			default:
				return parse(x["type"], ns) // {"type": "name"} wrapping a named type
			}
			return s, err
		}
		return nil, fmt.Errorf("avro schema: unexpected %T", v)
	}
	return parse(v, "")
}

// columns flattens a record schema into column names: nested records, and
// nullable nested records, get dotted names.
func (s *avroSchema) columns(prefix string) []string {
	if r := s.recordOf(); r != nil {
		var out []string
		for _, f := range r.fields {
			name := f.name
			if prefix != "" {
				name = prefix + "." + name
			}
			out = append(out, f.schema.columns(name)...)
		}
		return out
	}
	return []string{prefix}
}

// recordOf returns the record schema behind s, looking through a union of
// null and a record, or nil.
func (s *avroSchema) recordOf() *avroSchema {
	if s.typ == "record" {
		return s
	}
	if s.typ == "union" {
		var rec *avroSchema
		for _, b := range s.branches {
			switch {
			case b.typ == "null":
			case b.typ == "record" && rec == nil:
				rec = b
			default:
				return nil
			}
		}
		return rec
	}
	return nil
}

// avroReader decodes Avro binary values from one block.
type avroReader struct {
	buf []byte
	pos int
	err error
}

func (r *avroReader) long() int64 {
	var u uint64
	for shift := uint(0); ; shift += 7 {
		if r.pos >= len(r.buf) {
			r.err = io.ErrUnexpectedEOF
			return 0
		}
		b := r.buf[r.pos]
		r.pos++
		u |= uint64(b&0x7f) << shift
		if b < 0x80 {
			break
		}
	}
	return int64(u>>1) ^ -int64(u&1)
}

func (r *avroReader) take(n int) []byte {
	if n < 0 || r.pos+n > len(r.buf) {
		r.err = io.ErrUnexpectedEOF
		return nil
	}
	b := r.buf[r.pos : r.pos+n]
	r.pos += n
	return b
}

// value decodes one value as text (for the table) or as a nested Go value
// for arrays and maps, which are shown as JSON.
func (r *avroReader) value(s *avroSchema) any {
	switch s.typ {
	case "null":
		return nil
	case "boolean":
		b := r.take(1)
		return len(b) == 1 && b[0] == 1
	case "int", "long":
		n := r.long()
		switch s.logical {
		case "date":
			return time.Unix(n*86400, 0).UTC().Format("2006-01-02")
		case "time-millis":
			return time.Unix(0, n*int64(time.Millisecond)).UTC().Format("15:04:05.000")
		case "time-micros":
			return time.Unix(0, n*int64(time.Microsecond)).UTC().Format("15:04:05.000000")
		case "timestamp-millis", "local-timestamp-millis":
			return time.Unix(0, n*int64(time.Millisecond)).UTC().Format("2006-01-02T15:04:05.000Z")
		case "timestamp-micros", "local-timestamp-micros":
			return time.Unix(0, n*int64(time.Microsecond)).UTC().Format("2006-01-02T15:04:05.000000Z")
		}
		return strconv.FormatInt(n, 10)
	case "float":
		b := r.take(4)
		if b == nil {
			return nil
		}
		return strconv.FormatFloat(float64(math.Float32frombits(binary.LittleEndian.Uint32(b))), 'f', -1, 32)
	case "double":
		b := r.take(8)
		if b == nil {
			return nil
		}
		return strconv.FormatFloat(math.Float64frombits(binary.LittleEndian.Uint64(b)), 'f', -1, 64)
	case "bytes", "fixed":
		var b []byte
		if s.typ == "fixed" {
			b = r.take(s.size)
		} else {
			b = r.take(int(r.long()))
		}
		if s.logical == "decimal" {
			n := new(big.Int).SetBytes(b)
			if len(b) > 0 && b[0]&0x80 != 0 { // two's complement
				n.Sub(n, new(big.Int).Lsh(big.NewInt(1), uint(len(b)*8)))
			}
			return new(big.Rat).SetFrac(n, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(s.scale)), nil)).FloatString(s.scale)
		}
		return base64.StdEncoding.EncodeToString(b)
	case "string":
		b := r.take(int(r.long()))
		if s.logical == "uuid" || true {
			return string(b)
		}
	case "enum":
		i := int(r.long())
		if i >= 0 && i < len(s.symbols) {
			return s.symbols[i]
		}
		return strconv.Itoa(i)
	case "array":
		var out []any
		for {
			n := r.long()
			if n == 0 || r.err != nil {
				break
			}
			if n < 0 {
				n = -n
				r.long() // byte size, not needed
			}
			for i := int64(0); i < n && r.err == nil; i++ {
				out = append(out, r.value(s.items))
			}
		}
		return out
	case "map":
		out := map[string]any{}
		for {
			n := r.long()
			if n == 0 || r.err != nil {
				break
			}
			if n < 0 {
				n = -n
				r.long()
			}
			for i := int64(0); i < n && r.err == nil; i++ {
				k := string(r.take(int(r.long())))
				out[k] = r.value(s.values)
			}
		}
		return out
	case "union":
		i := int(r.long())
		if i < 0 || i >= len(s.branches) {
			r.err = fmt.Errorf("avro: union branch %d out of range", i)
			return nil
		}
		return r.value(s.branches[i])
	case "record":
		out := make(map[string]any, len(s.fields))
		for _, f := range s.fields {
			out[f.name] = r.value(f.schema)
		}
		return out
	}
	r.err = fmt.Errorf("avro: unsupported type %q", s.typ)
	return nil
}

// cell renders a decoded value for a table cell: text as is, nested arrays
// and maps as JSON.
func avroCell(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case bool:
		return strconv.FormatBool(x)
	}
	b, _ := json.Marshal(v)
	return string(b)
}

// fill writes a decoded record into row, following the column order that
// columns produced.
func avroFill(s *avroSchema, v any, row Row, i *int) {
	if r := s.recordOf(); r != nil {
		m, _ := v.(map[string]any)
		for _, f := range r.fields {
			var fv any
			if m != nil {
				fv = m[f.name]
			}
			avroFill(f.schema, fv, row, i)
		}
		return
	}
	row[*i] = avroCell(v)
	*i++
}

// ReadAvro streams the records of an Avro object container file. Nested
// records become dotted columns, arrays and maps JSON text, and the date
// and timestamp logical types ISO text. Codecs: null, deflate, snappy,
// zstd and bzip2.
func ReadAvro(ctx context.Context, r io.Reader) *Table {
	br := bufio.NewReaderSize(r, 1<<20)
	fail := func(err error) *Table {
		t, ch := newRoot(ctx, nil)
		t.fail(err)
		close(ch)
		return t
	}
	magic := make([]byte, 4)
	if _, err := io.ReadFull(br, magic); err != nil || !bytes.Equal(magic, avroMagic) {
		return fail(fmt.Errorf("not an Avro file: bad magic"))
	}
	meta := map[string][]byte{}
	for {
		n, err := readAvroLong(br)
		if err != nil {
			return fail(fmt.Errorf("avro header: %w", err))
		}
		if n == 0 {
			break
		}
		if n < 0 {
			n = -n
			if _, err := readAvroLong(br); err != nil {
				return fail(err)
			}
		}
		for i := int64(0); i < n; i++ {
			k, err := readAvroBytes(br)
			if err != nil {
				return fail(err)
			}
			v, err := readAvroBytes(br)
			if err != nil {
				return fail(err)
			}
			meta[string(k)] = v
		}
	}
	sync := make([]byte, 16)
	if _, err := io.ReadFull(br, sync); err != nil {
		return fail(fmt.Errorf("avro header: %w", err))
	}
	schema, err := parseAvroSchema(meta["avro.schema"])
	if err != nil {
		return fail(err)
	}
	if schema.recordOf() == nil {
		return fail(fmt.Errorf("avro: the top-level schema is %s, not a record", schema.typ))
	}
	codec := string(meta["avro.codec"])
	if codec == "" {
		codec = "null"
	}
	switch codec {
	case "null", "deflate", "snappy", "zstd", "bzip2":
	default:
		return fail(fmt.Errorf("avro: unsupported codec %q", codec))
	}
	t, ch := newRoot(ctx, schema.columns(""))
	go func() {
		defer close(ch)
		var zr *zstd.Decoder
		for {
			count, err := readAvroLong(br)
			if err == io.EOF {
				return
			}
			if err != nil {
				t.fail(fmt.Errorf("avro block: %w", err))
				return
			}
			size, err := readAvroLong(br)
			if err != nil {
				t.fail(fmt.Errorf("avro block: %w", err))
				return
			}
			block := make([]byte, size)
			if _, err := io.ReadFull(br, block); err != nil {
				t.fail(fmt.Errorf("avro block: %w", err))
				return
			}
			switch codec {
			case "deflate":
				block, err = io.ReadAll(flate.NewReader(bytes.NewReader(block)))
			case "snappy":
				if len(block) >= 4 {
					block, err = snappy.Decode(nil, block[:len(block)-4]) // a CRC32 trails the data
				}
			case "zstd":
				if zr == nil {
					zr, err = zstd.NewReader(nil)
				}
				if err == nil {
					block, err = zr.DecodeAll(block, nil)
				}
			case "bzip2":
				block, err = io.ReadAll(bzip2.NewReader(bytes.NewReader(block)))
			}
			if err != nil {
				t.fail(fmt.Errorf("avro %s block: %w", codec, err))
				return
			}
			ar := &avroReader{buf: block}
			for i := int64(0); i < count; i++ {
				v := ar.value(schema)
				if ar.err != nil {
					t.fail(fmt.Errorf("avro record: %w", ar.err))
					return
				}
				row := make(Row, len(t.Header))
				n := 0
				avroFill(schema, v, row, &n)
				if !t.send(ch, row) {
					return
				}
			}
			marker := make([]byte, 16)
			if _, err := io.ReadFull(br, marker); err != nil || !bytes.Equal(marker, sync) {
				t.fail(fmt.Errorf("avro: block sync marker missing; the file is truncated or corrupt"))
				return
			}
		}
	}()
	return t
}

func readAvroLong(r io.ByteReader) (int64, error) {
	var u uint64
	for shift := uint(0); shift < 64; shift += 7 {
		b, err := r.ReadByte()
		if err != nil {
			return 0, err
		}
		u |= uint64(b&0x7f) << shift
		if b < 0x80 {
			return int64(u>>1) ^ -int64(u&1), nil
		}
	}
	return 0, fmt.Errorf("avro: varint too long")
}

func readAvroBytes(r *bufio.Reader) ([]byte, error) {
	n, err := readAvroLong(r)
	if err != nil {
		return nil, err
	}
	if n < 0 || n > 1<<30 {
		return nil, fmt.Errorf("avro: bad length %d", n)
	}
	b := make([]byte, n)
	_, err = io.ReadFull(r, b)
	return b, err
}

// writeAvro writes an Avro object container file with one record per row.
// Columns are typed from the first thousand rows as nullable long, double,
// boolean, date, timestamp or string; blocks are deflate-compressed.
func writeAvro(w io.Writer, t *Table) (int, error) {
	const sample, perBlock = 1000, 1000
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
	fields := make([]string, len(t.Header))
	names := make([]string, len(t.Header))
	for i, h := range t.Header {
		kinds[i] = tallies[i].kind()
		typ := `"string"`
		switch kinds[i] {
		case KindInt:
			typ = `"long"`
		case KindFloat:
			typ = `"double"`
		case KindBool:
			typ = `"boolean"`
		case KindDate:
			typ = `{"type":"int","logicalType":"date"}`
			if timed[i] {
				typ = `{"type":"long","logicalType":"timestamp-millis"}`
			}
		}
		names[i] = avroName(h)
		nameJSON, _ := json.Marshal(names[i])
		fields[i] = fmt.Sprintf(`{"name":%s,"type":["null",%s],"default":null}`, nameJSON, typ)
	}
	uniqueHeaders(names)
	schema := `{"type":"record","name":"row","fields":[` + strings.Join(fields, ",") + `]}`
	sync := make([]byte, 16)
	rand.Read(sync)
	bw := bufio.NewWriterSize(w, 1<<20)
	bw.Write(avroMagic)
	var hdr []byte
	hdr = appendAvroLong(hdr, 2)
	hdr = appendAvroBytes(hdr, []byte("avro.schema"))
	hdr = appendAvroBytes(hdr, []byte(schema))
	hdr = appendAvroBytes(hdr, []byte("avro.codec"))
	hdr = appendAvroBytes(hdr, []byte("deflate"))
	hdr = appendAvroLong(hdr, 0)
	bw.Write(hdr)
	bw.Write(sync)

	var block []byte
	inBlock, n := 0, 0
	flush := func() error {
		if inBlock == 0 {
			return nil
		}
		var zb bytes.Buffer
		fw, _ := flate.NewWriter(&zb, flate.DefaultCompression)
		fw.Write(block)
		fw.Close()
		var b []byte
		b = appendAvroLong(b, int64(inBlock))
		b = appendAvroLong(b, int64(zb.Len()))
		bw.Write(b)
		bw.Write(zb.Bytes())
		_, err := bw.Write(sync)
		block, inBlock = block[:0], 0
		return err
	}
	write := func(r Row) error {
		for i, k := range kinds {
			block = appendAvroValue(block, r.Get(i), k, timed[i])
		}
		inBlock++
		n++
		if inBlock == perBlock {
			return flush()
		}
		return nil
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
	if err := flush(); err != nil {
		return n, err
	}
	if err := bw.Flush(); err != nil {
		return n, err
	}
	return n, t.Err()
}

// appendAvroValue encodes one nullable cell: union index 0 for null, 1 and
// the value otherwise. A value that does not fit its column is null.
func appendAvroValue(b []byte, v string, k Kind, timed bool) []byte {
	if IsNull(v) {
		return appendAvroLong(b, 0)
	}
	switch k {
	case KindInt:
		if f, ok := ParseFloat(v); ok && !isCode(CleanNumber(v)) {
			return appendAvroLong(appendAvroLong(b, 1), int64(f))
		}
	case KindFloat:
		if f, ok := ParseFloat(v); ok {
			return binary.LittleEndian.AppendUint64(appendAvroLong(b, 1), math.Float64bits(f))
		}
	case KindBool:
		switch Normalize(v, KindBool) {
		case "true":
			return append(appendAvroLong(b, 1), 1)
		case "false":
			return append(appendAvroLong(b, 1), 0)
		}
	case KindDate:
		if d, ok := ParseDate(v); ok {
			if timed {
				return appendAvroLong(appendAvroLong(b, 1), d.UnixMilli())
			}
			return appendAvroLong(appendAvroLong(b, 1), d.Unix()/86400)
		}
	default:
		return appendAvroBytes(appendAvroLong(b, 1), []byte(v))
	}
	return appendAvroLong(b, 0)
}

func appendAvroLong(b []byte, n int64) []byte {
	u := uint64(n<<1) ^ uint64(n>>63)
	for u >= 0x80 {
		b = append(b, byte(u)|0x80)
		u >>= 7
	}
	return append(b, byte(u))
}

func appendAvroBytes(b, v []byte) []byte {
	return append(appendAvroLong(b, int64(len(v))), v...)
}

// avroName makes a header a legal Avro field name: letters, digits and
// underscores, not starting with a digit.
func avroName(h string) string {
	var b strings.Builder
	for _, r := range strings.TrimSpace(h) {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	s := b.String()
	if s == "" {
		return "col"
	}
	if s[0] >= '0' && s[0] <= '9' {
		s = "_" + s
	}
	return s
}
