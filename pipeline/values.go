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
	return nullWords[strings.ToLower(s)]
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
			// drop
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
	if c := CleanNumber(t); c != "" {
		if _, err := strconv.ParseInt(c, 10, 64); err == nil {
			// Leading zeros like "007" or long IDs are codes, keep as text.
			if len(c) > 1 && c[0] == '0' || len(c) > 15 {
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

// Normalize rewrites a raw value into its canonical form for kind.
func Normalize(s string, k Kind) string {
	v, _ := normalize(s, k)
	return v
}

func normalize(s string, k Kind) (string, bool) {
	if IsNull(s) {
		return "", true
	}
	t := strings.TrimSpace(s)
	switch k {
	case KindInt, KindFloat:
		if f, ok := ParseFloat(t); ok {
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
		if d, ok := ParseDate(t); ok {
			if d.Hour() == 0 && d.Minute() == 0 && d.Second() == 0 {
				return d.Format("2006-01-02"), true
			}
			return d.Format(time.RFC3339), true
		}
	}
	if strings.ContainsAny(t, "\t\n\r") || strings.Contains(t, "  ") {
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
type tally [6]int

func (t *tally) add(k Kind) { t[k]++ }

func (t *tally) kind() Kind {
	numeric := t[KindInt] + t[KindFloat]
	best, n := KindText, t[KindText]
	for _, c := range []struct {
		k Kind
		n int
	}{{KindDate, t[KindDate]}, {KindBool, t[KindBool]}, {KindInt, numeric}} {
		if c.n > n {
			best, n = c.k, c.n
		}
	}
	if best == KindInt && t[KindFloat] > 0 {
		return KindFloat
	}
	return best
}
