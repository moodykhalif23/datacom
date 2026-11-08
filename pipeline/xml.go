package pipeline

import (
	"bufio"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// xmlNode is one element of an XML record held in memory. A record is
// small; the document is streamed one record at a time.
type xmlNode struct {
	name  string
	attrs []xml.Attr
	text  string
	kids  []*xmlNode
}

// ReadXML streams the repeated elements under the document root as rows:
// <rows><row><a>1</a><b>2</b></row>...</rows>. Attributes and child
// elements become columns, nested elements get dotted names (address.city)
// and a repeated child a numbered one (tag_2). The first record fixes the
// columns. One wrapper element around the records (<export><rows>...) is
// looked through without being held in memory.
func ReadXML(ctx context.Context, r io.Reader) *Table {
	t, ch := newRoot(ctx, nil)
	dec := xml.NewDecoder(r)
	dec.Strict = false
	dec.CharsetReader = charsetReader
	first, wrapper, err := firstXMLRecord(dec)
	if err != nil || first == nil {
		if err != nil {
			t.fail(fmt.Errorf("xml: %w", err))
		}
		close(ch)
		return t
	}
	var header []string
	seen := map[string]bool{}
	for _, f := range xmlFields(first) {
		if !seen[f.name] {
			seen[f.name] = true
			header = append(header, f.name)
		}
	}
	t.Header = header
	go func() {
		defer close(ch)
		emit := func(rec *xmlNode) bool {
			vals := map[string]string{}
			for _, f := range xmlFields(rec) {
				if _, dup := vals[f.name]; !dup {
					vals[f.name] = f.value
				}
			}
			row := make(Row, len(header))
			for i, h := range header {
				row[i] = vals[h]
			}
			return t.send(ch, row)
		}
		if !emit(first) {
			return
		}
		inWrapper := wrapper
		for {
			tok, err := dec.Token()
			if err != nil {
				if err != io.EOF {
					t.fail(fmt.Errorf("xml: %w", err))
				}
				return
			}
			switch x := tok.(type) {
			case xml.StartElement:
				if wrapper && !inWrapper {
					inWrapper = true // the next wrapper opens; its children are records
					continue
				}
				n, err := parseXML(dec, x)
				if err != nil {
					t.fail(fmt.Errorf("xml: %w", err))
					return
				}
				if !emit(n) {
					return
				}
			case xml.EndElement:
				if wrapper && inWrapper {
					inWrapper = false
					continue
				}
				return // the root closed
			}
		}
	}()
	return t
}

// firstXMLRecord skips the root element and parses the first record. When
// the root's first child has no attributes or text of its own and its first
// child is itself structured, that child is the first record and wrapper is
// true: the file nests its records one level deeper.
func firstXMLRecord(dec *xml.Decoder) (first *xmlNode, wrapper bool, err error) {
	rootSeen := false
	for {
		tok, err := dec.Token()
		if err != nil {
			if err == io.EOF {
				return nil, false, nil
			}
			return nil, false, err
		}
		switch x := tok.(type) {
		case xml.StartElement:
			if !rootSeen {
				rootSeen = true
				continue
			}
			n := &xmlNode{name: x.Name.Local, attrs: x.Attr}
			var text strings.Builder
			for {
				tok, err := dec.Token()
				if err != nil {
					return nil, false, err
				}
				switch y := tok.(type) {
				case xml.CharData:
					text.Write(y)
				case xml.StartElement:
					gc, err := parseXML(dec, y)
					if err != nil {
						return nil, false, err
					}
					if len(n.attrs) == 0 && strings.TrimSpace(text.String()) == "" && (len(gc.kids) > 0 || len(gc.attrs) > 0) {
						return gc, true, nil
					}
					n.kids = append(n.kids, gc)
					err = parseInto(dec, n, &text)
					return n, false, err
				case xml.EndElement:
					n.text = strings.TrimSpace(text.String())
					return n, false, nil
				}
			}
		case xml.EndElement:
			return nil, false, nil // the root closed with nothing in it
		}
	}
}

// parseXML reads an element and everything inside it.
func parseXML(dec *xml.Decoder, se xml.StartElement) (*xmlNode, error) {
	n := &xmlNode{name: se.Name.Local, attrs: se.Attr}
	var text strings.Builder
	return n, parseInto(dec, n, &text)
}

// parseInto reads the rest of an element that has been opened into n.
func parseInto(dec *xml.Decoder, n *xmlNode, text *strings.Builder) error {
	for {
		tok, err := dec.Token()
		if err != nil {
			return err
		}
		switch x := tok.(type) {
		case xml.StartElement:
			kid, err := parseXML(dec, x)
			if err != nil {
				return err
			}
			n.kids = append(n.kids, kid)
		case xml.CharData:
			text.Write(x)
		case xml.EndElement:
			n.text = strings.TrimSpace(text.String())
			return nil
		}
	}
}

type xmlField struct{ name, value string }

// xmlFields flattens a record into named values.
func xmlFields(n *xmlNode) []xmlField {
	var out []xmlField
	var walk func(prefix string, n *xmlNode, top bool)
	walk = func(prefix string, n *xmlNode, top bool) {
		if !top && (n.text != "" || len(n.kids) == 0 && len(n.attrs) == 0) {
			out = append(out, xmlField{prefix, n.text})
		}
		for _, a := range n.attrs {
			if a.Name.Space == "xmlns" || a.Name.Local == "xmlns" {
				continue
			}
			name := a.Name.Local
			if prefix != "" {
				name = prefix + "." + name
			}
			out = append(out, xmlField{name, a.Value})
		}
		counts := map[string]int{}
		for _, k := range n.kids {
			counts[k.name]++
			name := k.name
			if c := counts[k.name]; c > 1 {
				name += "_" + strconv.Itoa(c)
			}
			if prefix != "" {
				name = prefix + "." + name
			}
			walk(name, k, false)
		}
	}
	walk("", n, true)
	return out
}

// charsetReader lets the decoder read the Latin-1 and Windows-1252 files
// that older systems export; everything else must be UTF-8.
func charsetReader(charset string, input io.Reader) (io.Reader, error) {
	switch strings.ToLower(charset) {
	case "", "utf-8", "utf8", "us-ascii", "ascii":
		return input, nil
	case "iso-8859-1", "latin1", "latin-1", "windows-1252", "cp1252":
		return &latin1Reader{r: bufio.NewReader(input)}, nil
	}
	return nil, fmt.Errorf("unsupported encoding %q", charset)
}

type latin1Reader struct{ r *bufio.Reader }

func (l *latin1Reader) Read(p []byte) (int, error) {
	n := 0
	for n+4 <= len(p) {
		b, err := l.r.ReadByte()
		if err != nil {
			if n > 0 {
				return n, nil
			}
			return 0, err
		}
		if b < 0x80 {
			p[n] = b
			n++
			continue
		}
		r := cp1252[b]
		if r == 0 {
			r = rune(b)
		}
		n += copy(p[n:], string(r))
	}
	return n, nil
}

// cp1252 maps the 0x80-0x9F bytes Windows-1252 uses for punctuation; Latin-1
// has control characters there and the rest of the range is identical.
var cp1252 = map[byte]rune{
	0x80: '€', 0x82: '‚', 0x83: 'ƒ', 0x84: '„', 0x85: '…', 0x86: '†', 0x87: '‡', 0x88: 'ˆ', 0x89: '‰',
	0x8A: 'Š', 0x8B: '‹', 0x8C: 'Œ', 0x8E: 'Ž', 0x91: '‘', 0x92: '’', 0x93: '“', 0x94: '”', 0x95: '•',
	0x96: '–', 0x97: '—', 0x98: '˜', 0x99: '™', 0x9A: 'š', 0x9B: '›', 0x9C: 'œ', 0x9E: 'ž', 0x9F: 'Ÿ',
}

// writeXML writes <rows><row><col>value</col>...</row></rows>, with column
// names made into legal element names.
func writeXML(w *bufio.Writer, t *Table) (int, error) {
	names := make([]string, len(t.Header))
	for i, h := range t.Header {
		names[i] = xmlName(h)
	}
	uniqueHeaders(names)
	w.WriteString(xml.Header + "<rows>\n")
	n := 0
	for r := range t.Rows {
		w.WriteString("  <row>")
		for i, name := range names {
			w.WriteString("<" + name + ">")
			xml.EscapeText(w, []byte(r.Get(i)))
			w.WriteString("</" + name + ">")
		}
		if _, err := w.WriteString("</row>\n"); err != nil {
			return n, err
		}
		n++
	}
	_, err := w.WriteString("</rows>\n")
	return n, err
}

// xmlName turns a header into an element name: letters, digits, '_', '-'
// and '.', not starting with a digit, '-', '.' or "xml".
func xmlName(h string) string {
	var b strings.Builder
	for _, r := range strings.TrimSpace(h) {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_', r == '-', r == '.', r > 0x7f:
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	s := b.String()
	if s == "" {
		return "col"
	}
	if c := s[0]; c >= '0' && c <= '9' || c == '-' || c == '.' || strings.HasPrefix(strings.ToLower(s), "xml") {
		s = "_" + s
	}
	return s
}
