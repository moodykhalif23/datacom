package pipeline

import (
	"strconv"
	"strings"
	"time"
)

var nullWords = map[string]bool{
	"": true, "null": true, "nil": true, "none": true, "na": true, "n/a": true,
	"nan": true, "-": true, "--": true, "?": true, "#n/a": true, "undefined": true,
}

// IsNull reports whether a raw value should be treated as missing.
func IsNull(s string) bool {
	s = strings.TrimSpace(s)
	if len(s) > 9 { // longer than any null word
		return false
	}
	if s == "" {
		return true
	}
	switch s[0] { // every null word starts with one of these
	case 'n', 'N', 'u', 'U', '-', '?', '#':
		return nullWords[strings.ToLower(s)]
	}
	return false
}

// plainNumber reports whether s is already in canonical numeric form, so it
// can be kept as is: an optional '-', up to 15 digits with no leading zero
// (a lone 0 aside) and, for floats, an optional fraction not ending in 0.
func plainNumber(s string, k Kind) bool {
	i := 0
	if i < len(s) && s[i] == '-' {
		i++
	}
	start := i
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	digits := i - start
	if digits == 0 || digits > 15 || digits > 1 && s[start] == '0' {
		return false
	}
	if i == len(s) {
		return true
	}
	if k != KindFloat || s[i] != '.' {
		return false
	}
	i++
	frac := i
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	return i == len(s) && i > frac && s[i-1] != '0' && digits+i-frac <= 15
}

// messySpace reports whether s has a tab, newline or doubled space, the
// cases where whitespace is collapsed.
func messySpace(s string) bool {
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\t', '\n', '\r':
			return true
		case ' ':
			if i+1 < len(s) && s[i+1] == ' ' {
				return true
			}
		}
	}
	return false
}

var dateLayouts = []string{
	time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05", "2006-01-02T15:04:05",
	"2006-01-02 15:04", "2006-01-02", "2006/01/02", "02/01/2006", "01/02/2006",
	"02-01-2006", "2-Jan-2006", "02 Jan 2006", "2 Jan 2006", "2 January 2006", "Jan 2, 2006", "January 2, 2006",
	"2006-01", "20060102",
}

// ParseDate tries common layouts and returns the time in UTC.
func ParseDate(s string) (time.Time, bool) {
	s = strings.TrimSpace(s)
	if y, m, d, ok := isoDate(s); ok {
		return time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC), true
	}
	if len(s) < 6 || len(s) > 40 || !looksLikeDate(s) {
		return time.Time{}, false
	}
	for _, l := range dateLayouts {
		if t, err := time.Parse(l, s); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}

// CleanNumber strips thousands separators, currency symbols, percent signs
// and accounting parentheses, returning a parseable numeric string.
func CleanNumber(s string) string {
	s = stripCurrency(strings.TrimSpace(s))
	neg := false
	if strings.HasPrefix(s, "(") && strings.HasSuffix(s, ")") {
		neg = true
		s = s[1 : len(s)-1]
	}
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		switch {
		case r >= '0' && r <= '9', r == '.', r == '-', r == '+', r == 'e', r == 'E':
			b.WriteRune(r)
		case r == ',', r == ' ', r == '_', r == '$', r == '%', r == '€', r == '£',
			r == '¥', r == '₹', r == '\'':
		default:
			return ""
		}
	}
	out := b.String()
	if neg && !strings.HasPrefix(out, "-") {
		out = "-" + out
	}
	return out
}

func ParseFloat(s string) (float64, bool) {
	c := CleanNumber(s)
	if c == "" {
		return 0, false
	}
	f, err := strconv.ParseFloat(c, 64)
	return f, err == nil
}

// Infer returns the kind of a single raw value.
func Infer(s string) Kind {
	if IsNull(s) {
		return KindEmpty
	}
	t := strings.TrimSpace(s)
	switch strings.ToLower(t) {
	case "true", "false", "yes", "no", "y", "n":
		return KindBool
	}
	if isPhone(t) {
		return KindPhone
	}
	if c := CleanNumber(t); c != "" {
		if _, err := strconv.ParseInt(c, 10, 64); err == nil {
			if isCode(c) {
				return KindText
			}
			return KindInt
		}
		if _, err := strconv.ParseFloat(c, 64); err == nil {
			return KindFloat
		}
	}
	if _, ok := ParseDate(t); ok {
		return KindDate
	}
	return KindText
}

// isCode reports whether an integer-looking string is really an identifier
// that must be kept verbatim: leading zeros ("007", "0712345678"), a leading
// + with phone-length digits ("+254712345678"), or more digits than a float64
// holds exactly. Converting any of these would silently change the data.
func isCode(c string) bool {
	switch {
	case len(c) > 1 && c[0] == '0':
		return true
	case len(c) > 15:
		return true
	case c[0] == '+' && len(c) > 7:
		return true
	}
	return false
}

// Normalize rewrites a raw value into its canonical form for kind.
func Normalize(s string, k Kind) string {
	v, _ := normalize(s, k)
	return v
}

func normalize(s string, k Kind) (string, bool) {
	return normalizeWith(s, k, "")
}

// normalizeWith is normalize with the country calling code used for
// national phone numbers ("" leaves them national).
func normalizeWith(s string, k Kind, country string) (string, bool) {
	if IsNull(s) {
		return "", true
	}
	t := strings.TrimSpace(s)
	switch k {
	case KindPhone:
		if p, ok := NormalizePhone(t, country); ok {
			return p, true
		}
	case KindInt, KindFloat:
		if plainNumber(t, k) {
			return t, true // already canonical
		}
		c := CleanNumber(t)
		if _, err := strconv.ParseInt(c, 10, 64); err == nil && isCode(c) {
			break // a phone or padded ID in a numeric column: keep it as is
		}
		if f, err := strconv.ParseFloat(c, 64); c != "" && err == nil {
			if k == KindInt && f == float64(int64(f)) {
				return strconv.FormatInt(int64(f), 10), true
			}
			return strconv.FormatFloat(f, 'f', -1, 64), true
		}
	case KindBool:
		switch strings.ToLower(t) {
		case "true", "yes", "y", "1":
			return "true", true
		case "false", "no", "n", "0":
			return "false", true
		}
	case KindDate:
		if _, _, _, ok := isoDate(t); ok {
			return t, true // already canonical
		}
		if d, ok := ParseDate(t); ok {
			if d.Hour() == 0 && d.Minute() == 0 && d.Second() == 0 {
				return d.Format("2006-01-02"), true
			}
			return d.Format(time.RFC3339), true
		}
	}
	if messySpace(t) {
		t = strings.Join(strings.Fields(t), " ")
	}
	return t, k == KindText
}

// SnakeHeader turns "Total Amount (KES)" into "total_amount_kes".
func SnakeHeader(h string) string {
	var b strings.Builder
	prevUnderscore := true
	for _, r := range strings.ToLower(strings.TrimSpace(h)) {
		isAlnum := r >= 'a' && r <= 'z' || r >= '0' && r <= '9'
		if isAlnum {
			b.WriteRune(r)
			prevUnderscore = false
		} else if !prevUnderscore {
			b.WriteByte('_')
			prevUnderscore = true
		}
	}
	return strings.TrimRight(b.String(), "_")
}

// isoDate parses the canonical YYYY-MM-DD form by hand: it is the most
// common input by far and time.Parse would try five other layouts first.
func isoDate(s string) (y, m, d int, ok bool) {
	if len(s) != 10 || s[4] != '-' || s[7] != '-' {
		return 0, 0, 0, false
	}
	num := func(a, b int) (int, bool) {
		n := 0
		for i := a; i < b; i++ {
			c := s[i]
			if c < '0' || c > '9' {
				return 0, false
			}
			n = n*10 + int(c-'0')
		}
		return n, true
	}
	var okY, okM, okD bool
	y, okY = num(0, 4)
	m, okM = num(5, 7)
	d, okD = num(8, 10)
	if !okY || !okM || !okD || m < 1 || m > 12 || d < 1 {
		return 0, 0, 0, false
	}
	if t := time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC); t.Day() != d { // e.g. 2024-02-30
		return 0, 0, 0, false
	}
	return y, m, d, true
}

// phoneDigits returns the digits of a phone number written with the usual
// separators, and whether it was in international form (a + or 00 prefix).
func phoneDigits(s string) (digits string, intl, ok bool) {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c >= '0' && c <= '9':
			b.WriteByte(c)
		case c == '+' && i == 0:
			intl = true
		case c == ' ' || c == '-' || c == '.' || c == '(' || c == ')':
		default:
			return "", false, false
		}
	}
	d := b.String()
	if !intl && strings.HasPrefix(d, "00") {
		d, intl = d[2:], true
	}
	if len(d) < 7 || len(d) > 15 {
		return "", false, false
	}
	return d, intl, true
}

// isPhone is the strict test used to infer a column's kind: an international
// form, a national number with its trunk 0, or digit groups with separators.
func isPhone(s string) bool {
	d, intl, ok := phoneDigits(s)
	if !ok || len(d) < 9 {
		return false
	}
	return intl || d[0] == '0' && len(d) >= 10 || strings.ContainsAny(s, " -.()")
}

// NormalizePhone compacts a phone number: "+254 712 345 678" becomes
// "+254712345678" and "0712-345-678" becomes "0712345678". With a country
// calling code, national forms become international: "0712345678",
// "712345678" and "254712345678" all become "+254712345678".
func NormalizePhone(s, country string) (string, bool) {
	d, intl, ok := phoneDigits(strings.TrimSpace(s))
	if !ok {
		return s, false
	}
	switch {
	case intl:
		return "+" + d, true
	case country == "":
		return d, true
	case d[0] == '0':
		return "+" + country + d[1:], true
	case strings.HasPrefix(d, country) && len(d) >= len(country)+8:
		return "+" + d, true
	}
	return "+" + country + d, true
}

func looksLikeDate(s string) bool {
	digit, sep := false, false
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c >= '0' && c <= '9':
			digit = true
		case c == '-' || c == '/' || c == ' ' || c == ':' || c == 'T':
			sep = true
		}
	}
	return digit && (sep || len(s) == 8)
}

var currencyCodes = map[string]bool{
	"kes": true, "ksh": true, "kshs": true, "sh": true, "shs": true, "tsh": true, "ush": true,
	"ugx": true, "tzs": true, "rwf": true, "etb": true, "ngn": true, "zar": true, "ghs": true,
	"usd": true, "eur": true, "gbp": true, "inr": true, "rs": true, "jpy": true, "cny": true,
	"chf": true, "cad": true, "aud": true, "aed": true, "sar": true, "xof": true, "xaf": true,
}

func stripCurrency(s string) string {
	if len(s) < 3 {
		return s
	}
	letters := func(t string) int {
		n := 0
		for n < len(t) && (t[n] >= 'a' && t[n] <= 'z' || t[n] >= 'A' && t[n] <= 'Z') {
			n++
		}
		return n
	}
	if n := letters(s); n > 0 && n < len(s) && currencyCodes[strings.ToLower(s[:n])] {
		return strings.TrimSpace(s[n:])
	}
	i := len(s)
	for i > 0 && (s[i-1] >= 'a' && s[i-1] <= 'z' || s[i-1] >= 'A' && s[i-1] <= 'Z') {
		i--
	}
	if i > 0 && i < len(s) && currencyCodes[strings.ToLower(s[i:])] {
		return strings.TrimSpace(s[:i])
	}
	return s
}

// tally counts the kinds seen in a column sample; kind picks the column
// type by majority so a few stray values do not turn a numeric column
// into text. Those strays are what the monitor reports.
type tally [7]int

func (t *tally) add(k Kind) { t[k]++ }

func (t *tally) kind() Kind {
	numeric := t[KindInt] + t[KindFloat]
	best, n := KindText, t[KindText]
	for _, c := range []struct {
		k Kind
		n int
	}{{KindDate, t[KindDate]}, {KindBool, t[KindBool]}, {KindPhone, t[KindPhone]}, {KindInt, numeric}} {
		if c.n > n {
			best, n = c.k, c.n
		}
	}
	if best == KindInt && t[KindFloat] > 0 {
		return KindFloat
	}
	return best
}
