package pipeline

import (
	"html"
	"regexp"
	"strings"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

// TextChange is a set of the repairs CleanText made to one document.
type TextChange uint8

const (
	TextEncoding TextChange = 1 << iota
	TextMojibake
	TextHTML
	TextUnicode
	TextControl
	TextSpace
)

var textChangeNames = []struct {
	change TextChange
	name   string
}{
	{TextEncoding, "text:encoding"},
	{TextMojibake, "text:mojibake"},
	{TextHTML, "text:html"},
	{TextUnicode, "text:unicode"},
	{TextControl, "text:control"},
	{TextSpace, "text:whitespace"},
}

// CleanText readies a document for training or search: bytes that are not
// UTF-8 are read as Windows-1252, text garbled by a wrong decoding
// ("CafÃ©") is repaired, HTML is reduced to its text, Unicode is put in NFC
// form, control and zero-width characters are removed, and spacing is
// tidied without merging paragraphs. It repeats until nothing changes, so
// cleaning clean text is a no-op.
func CleanText(s string) (string, TextChange) {
	var all TextChange
	for pass := 0; pass < 8; pass++ {
		next, c := cleanTextOnce(s)
		if next == s {
			break
		}
		s, all = next, all|c
	}
	return s, all
}

func cleanTextOnce(s string) (string, TextChange) {
	var c TextChange
	if !utf8.ValidString(s) {
		s, c = fromWindows1252(s), c|TextEncoding
	}
	if t := fixMojibake(s); t != s {
		s, c = t, c|TextMojibake
	}
	if t := stripHTML(s); t != s {
		s, c = t, c|TextHTML
	}
	if !norm.NFC.IsNormalString(s) {
		s, c = norm.NFC.String(s), c|TextUnicode
	}
	if t := stripControl(s); t != s {
		s, c = t, c|TextControl
	}
	if t := tidySpace(s); t != s {
		s, c = t, c|TextSpace
	}
	return s, c
}

var cp1252Byte = func() map[rune]byte {
	m := make(map[rune]byte, 64)
	for b := byte(0x80); b < 0xA0; b++ {
		m[rune(b)] = b
	}
	for b, r := range cp1252 {
		m[r] = b
	}
	return m
}()

func win1252(b byte) rune {
	if r := cp1252[b]; r != 0 {
		return r
	}
	return rune(b)
}

func fromWindows1252(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			b.WriteRune(win1252(s[i]))
		} else {
			b.WriteRune(r)
		}
		i += size
	}
	return b.String()
}

// encodable maps a rune back to the Windows-1252 or Latin-1 byte it came
// from when UTF-8 was misread as one of those.
func encodable(r rune) (byte, bool) {
	if r >= 0xA0 && r <= 0xFF {
		return byte(r), true
	}
	b, ok := cp1252Byte[r]
	return b, ok
}

// fixMojibake turns runs like "Ã©" or "â€™" back into the characters whose
// UTF-8 bytes they are. Characters that do not decode are left alone, so
// genuine Latin-1 text such as "café" is not touched.
func fixMojibake(s string) string {
	for {
		t := fixMojibakeOnce(s)
		if t == s {
			return s
		}
		s = t
	}
}

func fixMojibakeOnce(s string) string {
	runes := []rune(s)
	if !mojibakeLikely(runes) {
		return s
	}
	var out strings.Builder
	changed := false
	for i := 0; i < len(runes); {
		j := i
		for j < len(runes) {
			if _, ok := encodable(runes[j]); !ok {
				break
			}
			j++
		}
		if j == i {
			out.WriteRune(runes[i])
			i++
			continue
		}
		seg := make([]byte, j-i)
		for k := i; k < j; k++ {
			seg[k-i], _ = encodable(runes[k])
		}
		for k := 0; k < len(seg); {
			r, size := utf8.DecodeRune(seg[k:])
			if r != utf8.RuneError && size >= 2 && (r < 0x80 || r > 0x9F) {
				out.WriteRune(r)
				changed = true
				k += size
				continue
			}
			out.WriteRune(runes[i+k])
			k++
		}
		i = j
	}
	if !changed {
		return s
	}
	return out.String()
}

// mojibakeLikely reports a UTF-8 lead byte misread as a character (Ã, Ð,
// â, æ and so on) followed by a misread continuation byte.
func mojibakeLikely(runes []rune) bool {
	for i := 0; i+1 < len(runes); i++ {
		if runes[i] < 0xC2 || runes[i] > 0xF4 {
			continue
		}
		if b, ok := encodable(runes[i+1]); ok && b >= 0x80 && b <= 0xBF {
			return true
		}
	}
	return false
}

const htmlTags = `a|abbr|address|article|aside|b|blockquote|body|br|button|caption|center|cite|code|dd|del|details|div|dl|dt|em|figcaption|figure|font|footer|form|h[1-6]|head|header|hr|html|i|iframe|img|input|ins|label|li|link|main|mark|meta|nav|noscript|ol|option|p|pre|q|s|section|select|small|span|strong|sub|summary|sup|table|tbody|td|textarea|tfoot|th|thead|time|title|tr|u|ul`

var (
	htmlTag     = regexp.MustCompile(`(?i)</?(?:` + htmlTags + `)(?:\s[^<>]*)?/?>`)
	htmlDrop    = regexp.MustCompile(`(?is)<(script|style|noscript|template|head)\b[^>]*>.*?</(?:script|style|noscript|template|head)\s*>|<!--.*?-->`)
	htmlBreak   = regexp.MustCompile(`(?i)<br\s*/?>|</(?:p|div|li|tr|h[1-6]|blockquote|pre|section|article|header|footer|table|ul|ol|dl|dt|dd|figcaption|title)\s*>`)
	htmlEntity  = regexp.MustCompile(`&(?:#[0-9]{1,7}|#[xX][0-9a-fA-F]{1,6}|[a-zA-Z][a-zA-Z0-9]{1,31});`)
	blankLines  = regexp.MustCompile(`\n{3,}`)
	innerSpaces = regexp.MustCompile(` {2,}`)
)

func stripHTML(s string) string {
	if strings.IndexByte(s, '<') >= 0 && htmlTag.MatchString(s) {
		s = htmlDrop.ReplaceAllString(s, "")
		s = htmlBreak.ReplaceAllString(s, "\n")
		s = htmlTag.ReplaceAllString(s, "")
		return html.UnescapeString(s)
	}
	if strings.IndexByte(s, '&') >= 0 && htmlEntity.MatchString(s) {
		return html.UnescapeString(s)
	}
	return s
}

func stripControl(s string) string {
	clean := true
	for _, r := range s {
		if r < 0x20 && r != '\n' && r != '\t' || r >= 0x7F && r <= 0x9F || isInvisible(r) || isOddSpace(r) || r == 0x2028 || r == 0x2029 {
			clean = false
			break
		}
	}
	if clean {
		return s
	}
	s = strings.ReplaceAll(s, "\r\n", "\n")
	var b strings.Builder
	for _, r := range s {
		switch {
		case r == '\r' || r == 0x2028 || r == 0x2029:
			b.WriteByte('\n')
		case isOddSpace(r):
			b.WriteByte(' ')
		case r < 0x20 && r != '\n' && r != '\t', r >= 0x7F && r <= 0x9F, isInvisible(r):
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

func isInvisible(r rune) bool {
	return r == 0x200B || r == 0xFEFF || r == 0x00AD || r == 0x2060 || r >= 0x202A && r <= 0x202E || r >= 0x2066 && r <= 0x2069
}

func isOddSpace(r rune) bool {
	return r == 0x00A0 || r == 0x2007 || r == 0x202F || r >= 0x2000 && r <= 0x200A || r == 0x3000
}

func tidySpace(s string) string {
	if !strings.ContainsAny(s, " \t\n") {
		return s
	}
	lines := strings.Split(s, "\n")
	for i, l := range lines {
		body := strings.TrimLeft(l, " \t")
		indent := l[:len(l)-len(body)]
		body = strings.TrimRight(innerSpaces.ReplaceAllString(body, " "), " \t")
		if body == "" {
			indent = ""
		}
		lines[i] = indent + body
	}
	s = strings.Join(lines, "\n")
	s = blankLines.ReplaceAllString(s, "\n\n")
	return strings.TrimSpace(s)
}
