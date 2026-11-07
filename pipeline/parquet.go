package pipeline

import (
	"context"
	"encoding/base64"
	"io"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/deprecated"
	"github.com/parquet-go/parquet-go/format"
)

// FormatParquet is Apache Parquet, read and written through parquet-go,
// the one dependency of this tool: every codec and encoding in the wild
// is covered, which a home-grown reader could not promise.
const FormatParquet Format = "parquet"

// ReadParquet streams a file row group by row group. Nested fields are
// flattened to dotted names and repeated values become JSON arrays.
func ReadParquet(ctx context.Context, r io.Reader) *Table {
	t, ch := newRoot(ctx, nil)
	f, size, cleanup, err := seekable(r)
	if err != nil {
		t.fail(err)
		close(ch)
		return t
	}
	pf, err := parquet.OpenFile(f, size)
	if err != nil {
		cleanup()
		t.fail(err)
		close(ch)
		return t
	}
	schema := pf.Schema()
	paths := schema.Columns()
	hdr := make([]string, len(paths))
	cell := make([]func(parquet.Value) string, len(paths))
	repeated := make([]bool, len(paths))
	for i, path := range paths {
		hdr[i] = columnName(path)
		if lc, ok := schema.Lookup(path...); ok {
			cell[i] = formatter(lc.Node.Type())
			repeated[i] = lc.MaxRepetitionLevel > 0
		}
	}
	uniqueHeaders(hdr)
	t.Header = hdr
	go func() {
		defer close(ch)
		defer cleanup()
		buf := make([]parquet.Row, 256)
		lists := make([][]string, len(paths))
		for _, rg := range pf.RowGroups() {
			rows := rg.Rows()
			for {
				for i := range buf {
					buf[i] = buf[i][:0]
				}
				n, err := rows.ReadRows(buf)
				for _, pr := range buf[:n] {
					row := make(Row, len(paths))
					for _, v := range pr {
						c := v.Column()
						if v.IsNull() || c < 0 || c >= len(paths) {
							continue
						}
						if repeated[c] {
							lists[c] = append(lists[c], cell[c](v))
						} else {
							row[c] = cell[c](v)
						}
					}
					for c, vals := range lists {
						if len(vals) > 0 {
							row[c] = jsonList(vals)
							lists[c] = vals[:0]
						}
					}
					if !t.send(ch, row) {
						rows.Close()
						return
					}
				}
				if err != nil {
					if err != io.EOF {
						t.fail(err)
						rows.Close()
						return
					}
					break
				}
			}
			rows.Close()
		}
	}()
	return t
}

// columnName drops the list/map plumbing from a nested path:
// tags.list.element -> tags, attrs.key_value.value -> attrs.value.
func columnName(path []string) string {
	var parts []string
	for _, p := range path {
		switch p {
		case "list", "element", "key_value", "array", "bag":
			continue
		}
		parts = append(parts, p)
	}
	return strings.Join(parts, ".")
}

func jsonList(vals []string) string {
	b := []byte{'['}
	for i, v := range vals {
		if i > 0 {
			b = append(b, ',')
		}
		b = appendJSONValue(b, v)
	}
	return string(append(b, ']'))
}

// formatter picks how a column's values become text, from its logical
// type (dates, timestamps, decimals, UUIDs) or its physical kind.
func formatter(typ parquet.Type) func(parquet.Value) string {
	if lt := typ.LogicalType(); lt != nil {
		switch x := lt.Value.(type) {
		case *format.DateType:
			return func(v parquet.Value) string { return epochDays(int64(v.Int32())).Format("2006-01-02") }
		case *format.TimestampType:
			div := int64(1) // nanoseconds per unit
			switch x.Unit.Value.(type) {
			case *format.MilliSeconds:
				div = int64(time.Millisecond)
			case *format.MicroSeconds:
				div = int64(time.Microsecond)
			}
			return func(v parquet.Value) string { return stamp(v.Int64() * div) }
		case *format.DecimalType:
			scale := int(x.Scale)
			return func(v parquet.Value) string { return decimal(v, scale) }
		case *format.UUIDType:
			return func(v parquet.Value) string {
				h := strconv.AppendUint(nil, 0, 16)[:0]
				for i, b := range v.ByteArray() {
					if i == 4 || i == 6 || i == 8 || i == 10 {
						h = append(h, '-')
					}
					h = append(h, hexDigits[b>>4], hexDigits[b&0xf])
				}
				return string(h)
			}
		}
	}
	if ct := typ.ConvertedType(); ct != nil {
		switch *ct {
		case deprecated.Date:
			return func(v parquet.Value) string { return epochDays(int64(v.Int32())).Format("2006-01-02") }
		case deprecated.TimestampMillis:
			return func(v parquet.Value) string { return stamp(v.Int64() * int64(time.Millisecond)) }
		case deprecated.TimestampMicros:
			return func(v parquet.Value) string { return stamp(v.Int64() * int64(time.Microsecond)) }
		}
	}
	return plain
}

func plain(v parquet.Value) string {
	switch v.Kind() {
	case parquet.Boolean:
		return strconv.FormatBool(v.Boolean())
	case parquet.Int32:
		return strconv.FormatInt(int64(v.Int32()), 10)
	case parquet.Int64:
		return strconv.FormatInt(v.Int64(), 10)
	case parquet.Int96: // legacy Impala/Hive timestamp: nanos of day + Julian day
		i := v.Int96()
		nanos := int64(i[0]) | int64(i[1])<<32
		return stamp((int64(int32(i[2]))-2440588)*int64(24*time.Hour) + nanos)
	case parquet.Float:
		return strconv.FormatFloat(float64(v.Float()), 'f', -1, 32)
	case parquet.Double:
		return strconv.FormatFloat(v.Double(), 'f', -1, 64)
	}
	b := v.ByteArray()
	if utf8.Valid(b) {
		return string(b)
	}
	return base64.StdEncoding.EncodeToString(b)
}

func epochDays(d int64) time.Time { return time.Unix(d*86400, 0).UTC() }

// stamp renders nanoseconds since the epoch; whole days print as dates.
func stamp(ns int64) string {
	d := time.Unix(0, ns).UTC()
	if ns%int64(24*time.Hour) == 0 {
		return d.Format("2006-01-02")
	}
	if ns%int64(time.Second) == 0 {
		return d.Format(time.RFC3339)
	}
	return d.Format(time.RFC3339Nano)
}

// decimal renders an unscaled integer (int32, int64 or big-endian bytes)
// with scale decimal places.
func decimal(v parquet.Value, scale int) string {
	var n big.Int
	switch v.Kind() {
	case parquet.Int32:
		n.SetInt64(int64(v.Int32()))
	case parquet.Int64:
		n.SetInt64(v.Int64())
	default:
		b := v.ByteArray()
		n.SetBytes(b)
		if len(b) > 0 && b[0]&0x80 != 0 { // two's complement
			n.Sub(&n, new(big.Int).Lsh(big.NewInt(1), uint(8*len(b))))
		}
	}
	s := n.String()
	if scale <= 0 {
		return s
	}
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	for len(s) <= scale {
		s = "0" + s
	}
	s = s[:len(s)-scale] + "." + s[len(s)-scale:]
	if neg {
		s = "-" + s
	}
	return s
}

// writeParquet infers a type per column from the first rows, then writes
// Snappy-compressed row groups of bounded size so memory stays flat.
func writeParquet(w io.Writer, t *Table) (int, error) {
	const sample = 1000
	var head []Row
	kinds := make([]Kind, len(t.Header))
	tallies := make([]tally, len(t.Header))
	timed := make([]bool, len(t.Header)) // dates that carry a time of day
	for r := range t.Rows {
		head = append(head, r)
		for i := 0; i < len(kinds) && i < len(r); i++ {
			k := Infer(r[i])
			tallies[i].add(k)
			timed[i] = timed[i] || k == KindDate && len(strings.TrimSpace(r[i])) > 10
		}
		if len(head) == sample {
			break
		}
	}
	for i := range kinds {
		kinds[i] = tallies[i].kind()
	}
	group := orderedGroup{Group: parquet.Group{}, names: t.Header}
	for i, h := range t.Header {
		var node parquet.Node
		switch kinds[i] {
		case KindInt:
			node = parquet.Int(64)
		case KindFloat:
			node = parquet.Leaf(parquet.DoubleType)
		case KindBool:
			node = parquet.Leaf(parquet.BooleanType)
		case KindDate:
			node = parquet.Date()
			if timed[i] {
				node = parquet.Timestamp(parquet.Millisecond)
			}
		default:
			node = parquet.String()
		}
		group.Group[h] = parquet.Optional(node)
	}
	pw := parquet.NewWriter(w, parquet.NewSchema("datacom", group),
		parquet.Compression(&parquet.Snappy), parquet.MaxRowsPerRowGroup(1<<18))
	n := 0
	batch := make([]parquet.Row, 0, 256)
	flush := func() error {
		_, err := pw.WriteRows(batch)
		batch = batch[:0]
		return err
	}
	write := func(r Row) error {
		pr := make(parquet.Row, len(kinds))
		for i, k := range kinds {
			pr[i] = encode(r.Get(i), k, timed[i]).Level(0, 1, i)
			if pr[i].IsNull() {
				pr[i] = parquet.NullValue().Level(0, 0, i)
			}
		}
		batch = append(batch, pr)
		n++
		if len(batch) == cap(batch) {
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
	return n, pw.Close()
}

// encode converts a cell to a typed value; anything unparseable is null.
func encode(s string, k Kind, timed bool) parquet.Value {
	s = strings.TrimSpace(s)
	if IsNull(s) {
		return parquet.NullValue()
	}
	switch k {
	case KindInt:
		if f, ok := ParseFloat(s); ok {
			return parquet.Int64Value(int64(f))
		}
	case KindFloat:
		if f, ok := ParseFloat(s); ok {
			return parquet.DoubleValue(f)
		}
	case KindBool:
		switch Normalize(s, KindBool) {
		case "true":
			return parquet.BooleanValue(true)
		case "false":
			return parquet.BooleanValue(false)
		}
	case KindDate:
		if d, ok := ParseDate(s); ok {
			if timed {
				return parquet.Int64Value(d.UnixMilli())
			}
			return parquet.Int32Value(int32(d.Unix() / 86400))
		}
	default:
		return parquet.ByteArrayValue([]byte(s))
	}
	return parquet.NullValue()
}

// orderedGroup is a parquet.Group whose columns keep the table's order
// instead of being sorted by name.
type orderedGroup struct {
	parquet.Group
	names []string
}

func (g orderedGroup) Fields() []parquet.Field {
	fields := make([]parquet.Field, len(g.names))
	for i, name := range g.names {
		fields[i] = namedNode{g.Group[name], name}
	}
	return fields
}

type namedNode struct {
	parquet.Node
	name string
}

func (f namedNode) Name() string                      { return f.name }
func (f namedNode) Value(reflect.Value) reflect.Value { return reflect.Value{} }
