package pipeline

import (
	"archive/zip"
	"bufio"
	"context"
	"encoding/xml"
	"fmt"
	"hash/crc32"
	"io"
	"strconv"
	"strings"
)

// FormatODS is an OpenDocument spreadsheet, what LibreOffice Calc saves.
const FormatODS Format = "ods"

// ReadODS streams one sheet of an OpenDocument spreadsheet: the sheet
// named or numbered (from 1) by sheet, or the first when sheet is "". The
// first row is the header. Typed cells give their value, not their display
// text, so 1,250.00 shown in the sheet is read as 1250.
func ReadODS(ctx context.Context, r io.Reader, sheet string) *Table {
	t, ch := newRoot(ctx, nil)
	f, size, cleanup, err := seekable(r)
	if err != nil {
		t.fail(err)
		close(ch)
		return t
	}
	zr, err := zip.NewReader(f, size)
	var content *zip.File
	if err == nil {
		for _, e := range zr.File {
			if e.Name == "content.xml" {
				content = e
			}
		}
		if content == nil {
			err = fmt.Errorf("not an OpenDocument spreadsheet: no content.xml")
		}
	}
	if err != nil {
		cleanup()
		t.fail(err)
		close(ch)
		return t
	}
	rows := make(chan Row, chanBuf)
	go func() {
		defer cleanup()
		t.fail(streamODS(ctx, content, sheet, rows))
		close(rows)
	}()
	return forward(t, ch, rows)
}

// streamODS sends the chosen sheet's rows, header included, to out.
func streamODS(ctx context.Context, content *zip.File, sheet string, out chan<- Row) error {
	rc, err := content.Open()
	if err != nil {
		return err
	}
	defer rc.Close()
	dec := xml.NewDecoder(bufio.NewReaderSize(rc, 1<<20))
	want, _ := strconv.Atoi(sheet)
	tables, inTable, found := 0, false, false
	var names []string
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("ods: %w", err)
		}
		switch x := tok.(type) {
		case xml.StartElement:
			switch x.Name.Local {
			case "table":
				tables++
				name := attr(x, "name")
				names = append(names, name)
				inTable = sheet == "" && tables == 1 || want > 0 && tables == want || want == 0 && sheet != "" && strings.EqualFold(name, sheet)
				found = found || inTable
			case "table-row":
				if !inTable {
					continue
				}
				repeat := repeatCount(x, "number-rows-repeated")
				row, err := odsRow(dec)
				if err != nil {
					return fmt.Errorf("ods: %w", err)
				}
				if len(row) == 0 {
					continue // padding below the data
				}
				for i := 0; i < repeat && i < 100000; i++ {
					select {
					case out <- append(Row(nil), row...):
					case <-ctx.Done():
						return ctx.Err()
					}
				}
			}
		case xml.EndElement:
			if x.Name.Local == "table" && inTable {
				return nil
			}
		}
	}
	if !found {
		return fmt.Errorf("sheet %q not found (spreadsheet has %d sheets: %s)", sheet, len(names), strings.Join(names, ", "))
	}
	return nil
}

// odsRow reads the cells of one row, expanding repeated cells and dropping
// the empty run at the end.
func odsRow(dec *xml.Decoder) (Row, error) {
	var row Row
	for {
		tok, err := dec.Token()
		if err != nil {
			return nil, err
		}
		switch x := tok.(type) {
		case xml.StartElement:
			if x.Name.Local != "table-cell" && x.Name.Local != "covered-table-cell" {
				continue
			}
			repeat := repeatCount(x, "number-columns-repeated")
			v, err := odsCell(dec, x)
			if err != nil {
				return nil, err
			}
			for i := 0; i < repeat && i < 1024; i++ {
				row = append(row, v)
			}
		case xml.EndElement:
			if x.Name.Local == "table-row" {
				for len(row) > 0 && row[len(row)-1] == "" {
					row = row[:len(row)-1]
				}
				return row, nil
			}
		}
	}
}

// odsCell returns a cell's value: the typed value for numbers, dates, times
// and booleans, otherwise its text with paragraphs joined by newlines.
func odsCell(dec *xml.Decoder, cell xml.StartElement) (string, error) {
	var typed string
	switch attr(cell, "value-type") {
	case "float", "percentage", "currency":
		typed = attr(cell, "value")
	case "date":
		typed = attr(cell, "date-value")
	case "time":
		typed = attr(cell, "time-value")
	case "boolean":
		typed = attr(cell, "boolean-value")
	}
	var text strings.Builder
	paragraphs := 0
	depth := 1
	for depth > 0 {
		tok, err := dec.Token()
		if err != nil {
			return "", err
		}
		switch x := tok.(type) {
		case xml.StartElement:
			depth++
			switch x.Name.Local {
			case "p":
				if paragraphs > 0 {
					text.WriteByte('\n')
				}
				paragraphs++
			case "s":
				n := repeatCount(x, "c")
				text.WriteString(strings.Repeat(" ", n))
			case "tab":
				text.WriteByte('\t')
			case "line-break":
				text.WriteByte('\n')
			}
		case xml.CharData:
			text.Write(x)
		case xml.EndElement:
			depth--
		}
	}
	if typed != "" {
		return typed, nil
	}
	return text.String(), nil
}

func attr(se xml.StartElement, local string) string {
	for _, a := range se.Attr {
		if a.Name.Local == local {
			return a.Value
		}
	}
	return ""
}

func repeatCount(se xml.StartElement, local string) int {
	if n, err := strconv.Atoi(attr(se, local)); err == nil && n > 1 {
		return n
	}
	return 1
}

// writeODS writes a one-sheet OpenDocument spreadsheet. Canonical numbers
// and dates become typed cells so Calc sees numbers, not text.
func writeODS(w io.Writer, t *Table) (int, error) {
	zw := zip.NewWriter(w)
	// The mimetype entry must come first, stored, with its size and checksum
	// in the local header and no data descriptor, or Calc will not open it.
	mt := []byte("application/vnd.oasis.opendocument.spreadsheet")
	mime, err := zw.CreateRaw(&zip.FileHeader{Name: "mimetype", Method: zip.Store, CRC32: crc32.ChecksumIEEE(mt),
		CompressedSize64: uint64(len(mt)), UncompressedSize64: uint64(len(mt))})
	if err != nil {
		return 0, err
	}
	mime.Write(mt)
	manifest, err := zw.Create("META-INF/manifest.xml")
	if err != nil {
		return 0, err
	}
	io.WriteString(manifest, `<?xml version="1.0" encoding="UTF-8"?>
<manifest:manifest xmlns:manifest="urn:oasis:names:tc:opendocument:xmlns:manifest:1.0" manifest:version="1.2">
 <manifest:file-entry manifest:full-path="/" manifest:media-type="application/vnd.oasis.opendocument.spreadsheet"/>
 <manifest:file-entry manifest:full-path="content.xml" manifest:media-type="text/xml"/>
</manifest:manifest>
`)
	content, err := zw.Create("content.xml")
	if err != nil {
		return 0, err
	}
	bw := bufio.NewWriterSize(content, 1<<20)
	bw.WriteString(`<?xml version="1.0" encoding="UTF-8"?>
<office:document-content xmlns:office="urn:oasis:names:tc:opendocument:xmlns:office:1.0" xmlns:table="urn:oasis:names:tc:opendocument:xmlns:table:1.0" xmlns:text="urn:oasis:names:tc:opendocument:xmlns:text:1.0" xmlns:style="urn:oasis:names:tc:opendocument:xmlns:style:1.0" xmlns:number="urn:oasis:names:tc:opendocument:xmlns:datastyle:1.0" office:version="1.2">
<office:automatic-styles>
<number:date-style style:name="N1"><number:year number:style="long"/><number:text>-</number:text><number:month number:style="long"/><number:text>-</number:text><number:day number:style="long"/></number:date-style>
<number:date-style style:name="N2"><number:year number:style="long"/><number:text>-</number:text><number:month number:style="long"/><number:text>-</number:text><number:day number:style="long"/><number:text> </number:text><number:hours number:style="long"/><number:text>:</number:text><number:minutes number:style="long"/><number:text>:</number:text><number:seconds number:style="long"/></number:date-style>
<style:style style:name="ce1" style:family="table-cell" style:data-style-name="N1"/>
<style:style style:name="ce2" style:family="table-cell" style:data-style-name="N2"/>
</office:automatic-styles>
<office:body><office:spreadsheet><table:table table:name="data">
`)
	writeRow := func(r Row, header bool) {
		bw.WriteString("<table:table-row>")
		for i := range t.Header {
			bw.WriteString(odsCellXML(r.Get(i), header))
		}
		bw.WriteString("</table:table-row>\n")
	}
	writeRow(Row(t.Header), true)
	n := 0
	for r := range t.Rows {
		writeRow(r, false)
		n++
	}
	bw.WriteString("</table:table></office:spreadsheet></office:body></office:document-content>\n")
	if err := bw.Flush(); err != nil {
		return n, err
	}
	return n, zw.Close()
}

func odsCellXML(v string, forceText bool) string {
	if v == "" {
		return "<table:table-cell/>"
	}
	var b strings.Builder
	xml.EscapeText(&b, []byte(v))
	text := b.String()
	if !forceText {
		if _, err := strconv.ParseFloat(v, 64); err == nil && !isCode(v) && plainNumber(v, KindFloat) {
			return `<table:table-cell office:value-type="float" office:value="` + text + `"><text:p>` + text + `</text:p></table:table-cell>`
		}
		if d, ok := ParseDate(v); ok {
			iso, style := d.Format("2006-01-02"), "ce1"
			if d.Hour()+d.Minute()+d.Second() > 0 {
				iso, style = d.Format("2006-01-02T15:04:05"), "ce2"
			}
			return `<table:table-cell table:style-name="` + style + `" office:value-type="date" office:date-value="` + iso + `"><text:p>` + text + `</text:p></table:table-cell>`
		}
	}
	return `<table:table-cell office:value-type="string"><text:p>` + text + `</text:p></table:table-cell>`
}
