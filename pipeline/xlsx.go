package pipeline

import (
	"archive/zip"
	"bufio"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

// FormatXLSX is an Excel workbook; reading takes one sheet, writing makes one.
const FormatXLSX Format = "xlsx"

// ReadXLSX streams one sheet of a workbook: by name, by 1-based index, or
// the first sheet when sheet is "".
func ReadXLSX(ctx context.Context, r io.Reader, sheet string) *Table {
	t, ch := newRoot(ctx, nil)
	f, ok := r.(*os.File)
	if !ok {
		tmp, err := os.CreateTemp("", "datacom-*.xlsx")
		if err == nil {
			defer os.Remove(tmp.Name())
			defer tmp.Close()
			_, err = io.Copy(tmp, r)
		}
		if err != nil {
			t.fail(err)
			close(ch)
			return t
		}
		f = tmp
	}
	st, err := f.Stat()
	if err != nil {
		t.fail(err)
		close(ch)
		return t
	}
	zr, err := zip.NewReader(f, st.Size())
	if err != nil {
		t.fail(fmt.Errorf("not an xlsx file: %w", err))
		close(ch)
		return t
	}
	wb, err := loadWorkbook(zr, sheet)
	if err != nil {
		t.fail(err)
		close(ch)
		return t
	}
	rows := make(chan Row, chanBuf)
	go func() { t.fail(wb.stream(ctx, rows)); close(rows) }()
	hdr, ok := <-rows
	if !ok {
		close(ch)
		return t
	}
	t.Header = hdr
	go func() {
		defer close(ch)
		for r := range rows {
			if !t.send(ch, r) {
				return
			}
		}
	}()
	return t
}

type workbook struct {
	sheet   *zip.File
	strings []string
	isDate  []bool // per cell style index
}

func loadWorkbook(zr *zip.Reader, sheet string) (*workbook, error) {
	files := map[string]*zip.File{}
	for _, f := range zr.File {
		files[f.Name] = f
	}
	var wbx struct {
		Sheets []struct {
			Name string `xml:"name,attr"`
			ID   string `xml:"http://schemas.openxmlformats.org/officeDocument/2006/relationships id,attr"`
		} `xml:"sheets>sheet"`
	}
	var rels struct {
		R []struct {
			ID     string `xml:"Id,attr"`
			Target string `xml:"Target,attr"`
		} `xml:"Relationship"`
	}
	if err := decodeXML(files["xl/workbook.xml"], &wbx); err != nil {
		return nil, err
	}
	if err := decodeXML(files["xl/_rels/workbook.xml.rels"], &rels); err != nil {
		return nil, err
	}
	idx := 0
	if n, err := strconv.Atoi(sheet); err == nil && n > 0 {
		idx = n - 1
	} else if sheet != "" {
		idx = -1
		for i, s := range wbx.Sheets {
			if strings.EqualFold(s.Name, sheet) {
				idx = i
			}
		}
	}
	if idx < 0 || idx >= len(wbx.Sheets) {
		return nil, fmt.Errorf("sheet %q not found (workbook has %d sheets)", sheet, len(wbx.Sheets))
	}
	wb := &workbook{}
	for _, r := range rels.R {
		if r.ID == wbx.Sheets[idx].ID {
			wb.sheet = files["xl/"+strings.TrimPrefix(r.Target, "/xl/")]
		}
	}
	if wb.sheet == nil {
		return nil, fmt.Errorf("sheet %q has no data part", wbx.Sheets[idx].Name)
	}
	var sst struct {
		SI []struct {
			T []string `xml:"t"`
			R []string `xml:"r>t"`
		} `xml:"si"`
	}
	if f := files["xl/sharedStrings.xml"]; f != nil {
		if err := decodeXML(f, &sst); err != nil {
			return nil, err
		}
		wb.strings = make([]string, len(sst.SI))
		for i, si := range sst.SI {
			wb.strings[i] = strings.Join(append(si.T, si.R...), "")
		}
	}
	var sty struct {
		Fmts []struct {
			ID   int    `xml:"numFmtId,attr"`
			Code string `xml:"formatCode,attr"`
		} `xml:"numFmts>numFmt"`
		Xfs []struct {
			ID int `xml:"numFmtId,attr"`
		} `xml:"cellXfs>xf"`
	}
	if f := files["xl/styles.xml"]; f != nil {
		if err := decodeXML(f, &sty); err != nil {
			return nil, err
		}
		custom := map[int]bool{}
		for _, nf := range sty.Fmts {
			code := strings.ToLower(nf.Code)
			custom[nf.ID] = strings.ContainsAny(code, "ymdh") && !strings.Contains(code, "#")
		}
		wb.isDate = make([]bool, len(sty.Xfs))
		for i, xf := range sty.Xfs {
			wb.isDate[i] = xf.ID >= 14 && xf.ID <= 22 || xf.ID >= 45 && xf.ID <= 47 || custom[xf.ID]
		}
	}
	return wb, nil
}

func decodeXML(f *zip.File, v any) error {
	if f == nil {
		return fmt.Errorf("xlsx: missing part")
	}
	rc, err := f.Open()
	if err != nil {
		return err
	}
	defer rc.Close()
	return xml.NewDecoder(rc).Decode(v)
}

// stream sends each sheet row, including the header row, to out.
func (wb *workbook) stream(ctx context.Context, out chan<- Row) error {
	rc, err := wb.sheet.Open()
	if err != nil {
		return err
	}
	defer rc.Close()
	dec := xml.NewDecoder(rc)
	var row Row
	var col int
	var typ, style string
	var inRow, inValue bool
	var buf strings.Builder
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		switch el := tok.(type) {
		case xml.StartElement:
			switch el.Name.Local {
			case "row":
				row, inRow = row[:0], true
			case "c":
				typ, style = "", ""
				for _, a := range el.Attr {
					switch a.Name.Local {
					case "r":
						col = colIndex(a.Value)
					case "t":
						typ = a.Value
					case "s":
						style = a.Value
					}
				}
			case "v", "t":
				inValue = inRow
				buf.Reset()
			}
		case xml.CharData:
			if inValue {
				buf.Write(el)
			}
		case xml.EndElement:
			switch el.Name.Local {
			case "v", "t":
				if inValue {
					inValue = false
					for len(row) <= col {
						row = append(row, "")
					}
					row[col] = wb.cell(buf.String(), typ, style)
				}
			case "row":
				inRow = false
				if len(row) > 0 {
					select {
					case out <- append(Row(nil), row...):
					case <-ctx.Done():
						return ctx.Err()
					}
				}
			}
		}
	}
}

// cell converts a raw cell to text: shared strings, booleans and dates.
func (wb *workbook) cell(v, typ, style string) string {
	switch typ {
	case "s":
		if i, err := strconv.Atoi(v); err == nil && i < len(wb.strings) {
			return wb.strings[i]
		}
	case "b":
		return strconv.FormatBool(v == "1")
	case "":
		if s, err := strconv.Atoi(style); err == nil && s < len(wb.isDate) && wb.isDate[s] {
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				d := time.Date(1899, 12, 30, 0, 0, 0, 0, time.UTC).Add(time.Duration(f * 24 * float64(time.Hour)))
				if f == float64(int64(f)) {
					return d.Format("2006-01-02")
				}
				return d.Format("2006-01-02 15:04:05")
			}
		}
	}
	return v
}

// colIndex turns "AB12" into 27.
func colIndex(ref string) int {
	n := 0
	for i := 0; i < len(ref) && ref[i] >= 'A' && ref[i] <= 'Z'; i++ {
		n = n*26 + int(ref[i]-'A'+1)
	}
	return n - 1
}

// writeXLSX streams t as a one-sheet workbook. Numbers and booleans keep
// their types, ISO dates become real Excel dates, everything else is text.
func writeXLSX(w io.Writer, t *Table) (int, error) {
	zw := zip.NewWriter(w)
	static := map[string]string{
		"[Content_Types].xml":        `<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/><Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/></Types>`,
		"_rels/.rels":                `<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>`,
		"xl/workbook.xml":            `<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="data" sheetId="1" r:id="rId1"/></sheets></workbook>`,
		"xl/_rels/workbook.xml.rels": `<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/><Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/></Relationships>`,
		"xl/styles.xml":              `<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><fonts count="1"><font/></fonts><fills count="1"><fill/></fills><borders count="1"><border/></borders><cellXfs count="3"><xf numFmtId="0"/><xf numFmtId="14" applyNumberFormat="1"/><xf numFmtId="22" applyNumberFormat="1"/></cellXfs></styleSheet>`,
	}
	for name, body := range static {
		f, err := zw.Create(name)
		if err == nil {
			_, err = io.WriteString(f, body)
		}
		if err != nil {
			return 0, err
		}
	}
	sheet, err := zw.Create("xl/worksheets/sheet1.xml")
	if err != nil {
		return 0, err
	}
	bw := bufio.NewWriterSize(sheet, 1<<20)
	bw.WriteString(`<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>`)
	writeRow := func(r Row, header bool) {
		bw.WriteString("<row>")
		for _, v := range r {
			bw.WriteString(xlsxCell(v, header))
		}
		bw.WriteString("</row>")
	}
	writeRow(Row(t.Header), true)
	n := 0
	for r := range t.Rows {
		writeRow(r, false)
		n++
	}
	bw.WriteString("</sheetData></worksheet>")
	if err := bw.Flush(); err != nil {
		return n, err
	}
	return n, zw.Close()
}

// xlsxCell encodes one value; a true forceText keeps header numbers as text.
func xlsxCell(v string, forceText bool) string {
	switch {
	case v == "":
		return "<c/>"
	case forceText:
	case v == "true" || v == "false":
		return `<c t="b"><v>` + strconv.FormatBool(v == "true") + "</v></c>"
	case len(v) <= 20 && jsonNumber.MatchString(v):
		return "<c><v>" + v + "</v></c>"
	case len(v) >= 10 && v[4] == '-' && v[7] == '-':
		if d, ok := ParseDate(v); ok {
			serial := d.Sub(time.Date(1899, 12, 30, 0, 0, 0, 0, time.UTC)).Hours() / 24
			style := "1"
			if serial != float64(int64(serial)) {
				style = "2"
			}
			return `<c s="` + style + `"><v>` + strconv.FormatFloat(serial, 'f', -1, 64) + "</v></c>"
		}
	}
	var b strings.Builder
	xml.EscapeText(&b, []byte(v))
	return `<c t="inlineStr"><is><t xml:space="preserve">` + b.String() + "</t></is></c>"
}
