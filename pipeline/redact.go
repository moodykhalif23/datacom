package pipeline

import (
	"fmt"
	"strings"
	"sync/atomic"
)

// RedactKind is a set of personal-data kinds to mask.
// Redact masks personal data in every cell: emails, phone numbers, ID
// numbers and payment card numbers become [EMAIL], [PHONE], [ID] and
// [CARD]. country is the calling code that marks national phone numbers
// written without a leading 0. In columns whose name says they hold ID or
// phone numbers, every value with enough digits is masked whole.
type RedactKind uint8

const (
	RedactEmail RedactKind = 1 << iota
	RedactPhone
	RedactID
	RedactCard
	RedactAll = RedactEmail | RedactPhone | RedactID | RedactCard
)

var redactNames = []struct {
	kind         RedactKind
	plural       string
	single, noun string
	token        string
}{
	{RedactEmail, "emails", "email", "email", "[EMAIL]"},
	{RedactPhone, "phones", "phone", "phone number", "[PHONE]"},
	{RedactID, "ids", "id", "ID number", "[ID]"},
	{RedactCard, "cards", "card", "card number", "[CARD]"},
}

const (
	maskEmail = iota
	maskPhone
	maskID
	maskCard
)

// ParseRedact reads a -redact list such as "emails,phones" or "all".
func ParseRedact(spec string) (RedactKind, error) {
	var k RedactKind
	for _, part := range strings.Split(spec, ",") {
		part = strings.ToLower(strings.TrimSpace(part))
		if part == "" {
			continue
		}
		if part == "all" {
			k |= RedactAll
			continue
		}
		found := false
		for _, n := range redactNames {
			if part == n.plural || part == n.single {
				k |= n.kind
				found = true
			}
		}
		if !found {
			return 0, fmt.Errorf("unknown -redact kind %q (use emails, phones, ids, cards or all)", part)
		}
	}
	return k, nil
}

func Redact(in *Table, kinds RedactKind, country string) *Table {
	if kinds == 0 {
		return in
	}
	idCols, phoneCols := make([]bool, len(in.Header)), make([]bool, len(in.Header))
	for i, h := range in.Header {
		idCols[i] = kinds&RedactID != 0 && idColumn(h)
		phoneCols[i] = kinds&RedactPhone != 0 && phoneColumn(h)
	}
	mon := in.monitor()
	var counters [4]*atomic.Int64
	for i, n := range redactNames {
		counters[i] = mon.Counter("masked:" + n.single)
	}
	out, ch := in.child(in.Header)
	go func() {
		defer close(ch)
		for r := range in.Rows {
			var n [4]int
			for i, v := range r {
				if v == "" {
					continue
				}
				if i < len(idCols) && idCols[i] && hasDigit(v) {
					r[i] = "[ID]"
					n[maskID]++
					continue
				}
				if i < len(phoneCols) && phoneCols[i] && digitCount(v) >= 7 {
					r[i] = "[PHONE]"
					n[maskPhone]++
					continue
				}
				r[i] = RedactText(v, kinds, country, &n)
			}
			for i, c := range n {
				bump(counters[i], int64(c))
			}
			if !out.send(ch, r) {
				return
			}
		}
	}()
	return out
}

// RedactText masks the personal data in s and adds what it found to n,
// indexed email, phone, id, card.
func RedactText(s string, kinds RedactKind, country string, n *[4]int) string {
	at, letters, digits := false, false, 0
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c == '@':
			at = true
		case c >= '0' && c <= '9':
			digits++
		case c|0x20 >= 'a' && c|0x20 <= 'z':
			letters = true
		}
	}
	if at && kinds&RedactEmail != 0 {
		s = maskEmails(s, n)
	}
	if digits >= 6 && kinds&(RedactPhone|RedactID|RedactCard) != 0 {
		s = maskNumbers(s, kinds, country, n)
	}
	if letters && digits >= 6 && kinds&RedactID != 0 {
		s = maskIDTokens(s, n)
	}
	return s
}

func maskEmails(s string, n *[4]int) string {
	var b strings.Builder
	last := 0
	for i := 0; i < len(s); i++ {
		if s[i] != '@' {
			continue
		}
		st := i
		for st > last && localChar(s[st-1]) {
			st--
		}
		for st < i && s[st] == '.' {
			st++
		}
		if st == i || i+1 >= len(s) || !isAlnum(s[i+1]) {
			continue
		}
		end := i + 1
		for end < len(s) && (isAlnum(s[end]) || s[end] == '.' || s[end] == '-') {
			end++
		}
		for s[end-1] == '.' || s[end-1] == '-' {
			end--
		}
		dot := strings.LastIndexByte(s[i+1:end], '.')
		if dot <= 0 || !tld(s[i+2+dot:end]) {
			continue
		}
		b.WriteString(s[last:st])
		b.WriteString("[EMAIL]")
		n[maskEmail]++
		last = end
		i = end - 1
	}
	if b.Len() == 0 {
		return s
	}
	b.WriteString(s[last:])
	return b.String()
}

func tld(s string) bool {
	if len(s) < 2 {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i]|0x20 < 'a' || s[i]|0x20 > 'z' {
			return false
		}
	}
	return true
}

func localChar(c byte) bool {
	return isAlnum(c) || strings.IndexByte("._%+-'", c) >= 0
}

func isAlnum(c byte) bool {
	return c >= '0' && c <= '9' || c|0x20 >= 'a' && c|0x20 <= 'z'
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

func digitCount(s string) int {
	n := 0
	for i := 0; i < len(s); i++ {
		if isDigit(s[i]) {
			n++
		}
	}
	return n
}

func hasDigit(s string) bool {
	for i := 0; i < len(s); i++ {
		if isDigit(s[i]) {
			return true
		}
	}
	return false
}

// numberRun is a run of digits with the separators people put in phone,
// card and ID numbers. Each group is the byte span of one block of digits.
type numberRun struct {
	start, end int
	groups     [][2]int
	plus       bool
	paren      bool
}

// scanNumber reads the run starting at i, or reports false when s[i] does
// not begin one: a digit, + or ( not glued to a letter, a digit or the end
// of a mask token before it.
func scanNumber(s string, i int) (numberRun, bool) {
	c := s[i]
	if i > 0 && (isAlnum(s[i-1]) || s[i-1] == ']' || isDigit(c) && (s[i-1] == '.' || s[i-1] == ',') && i > 1 && isDigit(s[i-2])) {
		return numberRun{}, false
	}
	if !isDigit(c) && !((c == '+' || c == '(') && i+1 < len(s) && isDigit(s[i+1])) {
		return numberRun{}, false
	}
	r := numberRun{start: i, plus: c == '+', paren: c == '('}
	j := i
	if !isDigit(c) {
		j++
	}
	for {
		g := j
		for j < len(s) && isDigit(s[j]) {
			j++
		}
		r.groups = append(r.groups, [2]int{g, j})
		sep := 0
		switch {
		case j+2 < len(s) && s[j] == ')' && (s[j+1] == ' ' || s[j+1] == '-') && isDigit(s[j+2]):
			sep = 2
		case j+1 < len(s) && strings.IndexByte(" -.)", s[j]) >= 0 && isDigit(s[j+1]):
			sep = 1
		case j+2 < len(s) && s[j] == ' ' && s[j+1] == '(' && isDigit(s[j+2]):
			sep = 2
		}
		if sep == 0 {
			break
		}
		j += sep
	}
	r.end = j
	if j < len(s) && (isAlnum(s[j]) || (s[j] == '.' || s[j] == ',') && j+1 < len(s) && isDigit(s[j+1])) {
		return r, false
	}
	return r, true
}

// uniform reports a run written in equal blocks, like 4111 1111 1111 1111,
// which is one number and is never split.
func (r numberRun) uniform() bool {
	if len(r.groups) < 3 {
		return false
	}
	w := r.groups[0][1] - r.groups[0][0]
	for _, g := range r.groups[1:] {
		if g[1]-g[0] != w {
			return false
		}
	}
	return true
}

// span is the part of a run from group a to group b, as a candidate number.
type span struct {
	start, end int
	digits     string
	groups     []int
	seps       string
	plus       bool
}

func (r numberRun) span(s string, a, b int) span {
	sp := span{start: r.groups[a][0], end: r.groups[b][1]}
	if a == 0 {
		sp.start, sp.plus = r.start, r.plus
		if r.paren {
			sp.seps = "("
		}
	}
	var d strings.Builder
	for k := a; k <= b; k++ {
		g := r.groups[k]
		d.WriteString(s[g[0]:g[1]])
		sp.groups = append(sp.groups, g[1]-g[0])
		if k < b {
			for x := g[1]; x < r.groups[k+1][0]; x++ {
				if strings.IndexByte(sp.seps, s[x]) < 0 {
					sp.seps += string(s[x])
				}
			}
		}
	}
	sp.digits = d.String()
	return sp
}

func classify(sp span, kinds RedactKind, country string) int {
	switch {
	case kinds&RedactID != 0 && isIDNumber(sp):
		return maskID
	case kinds&RedactCard != 0 && isCard(sp):
		return maskCard
	case kinds&RedactPhone != 0 && isPhoneRun(sp, country):
		return maskPhone
	}
	return -1
}

// maskNumbers masks the numbers in each run, trying the longest spans of
// groups first so a card, an ID or a phone number next to other digits is
// still found.
func maskNumbers(s string, kinds RedactKind, country string, n *[4]int) string {
	var b strings.Builder
	last := 0
	for i := 0; i < len(s); i++ {
		r, ok := scanNumber(s, i)
		if !ok {
			if r.end > i {
				i = r.end - 1
			}
			continue
		}
		whole := r.uniform()
		for a := 0; a < len(r.groups); a++ {
			for z := len(r.groups) - 1; z >= a; z-- {
				if whole && (a > 0 || z < len(r.groups)-1) {
					continue
				}
				sp := r.span(s, a, z)
				kind := classify(sp, kinds, country)
				if kind < 0 {
					continue
				}
				b.WriteString(s[last:sp.start])
				b.WriteString(redactNames[kind].token)
				n[kind]++
				last = sp.end
				a = z
				break
			}
		}
		i = r.end - 1
	}
	if b.Len() == 0 {
		return s
	}
	b.WriteString(s[last:])
	return b.String()
}

func onlySeps(r span, allowed string) bool {
	for i := 0; i < len(r.seps); i++ {
		if strings.IndexByte(allowed, r.seps[i]) < 0 {
			return false
		}
	}
	return true
}

func isCard(r span) bool {
	d, n := r.digits, len(r.digits)
	if n < 13 || n > 19 || r.plus || !onlySeps(r, " -") || !luhn(d) {
		return false
	}
	switch {
	case d[0] == '4':
		return n == 13 || n == 16 || n == 19
	case d[:2] >= "51" && d[:2] <= "55", d[:4] >= "2221" && d[:4] <= "2720":
		return n == 16
	case d[:2] == "34", d[:2] == "37":
		return n == 15
	case d[:4] == "6011", d[:2] == "65", d[:3] >= "644" && d[:3] <= "649", d[:2] == "35", d[:2] == "62":
		return n >= 16
	case d[:2] == "36", d[:2] == "38", d[:3] >= "300" && d[:3] <= "305":
		return n >= 14
	}
	return false
}

func isIDNumber(r span) bool {
	d := r.digits
	switch {
	case len(d) == 9 && !r.plus && r.seps == "-" && equalGroups(r.groups, 3, 2, 4):
		return d[:3] != "000" && d[:3] != "666" && d[0] != '9' && d[3:5] != "00" && d[5:] != "0000"
	case len(d) == 12 && !r.plus && onlySeps(r, " -") && equalGroups(r.groups, 4, 4, 4):
		return d[0] >= '2' && verhoeff(d)
	case len(d) == 13 && !r.plus && len(r.groups) == 1:
		return validYYMMDD(d[:6]) && (d[10] == '0' || d[10] == '1') && luhn(d)
	}
	return false
}

func isPhoneRun(r span, country string) bool {
	d, n := r.digits, len(r.digits)
	if n < 8 || n > 15 {
		return false
	}
	switch {
	case r.plus:
		return true
	case strings.HasPrefix(d, "00"):
		return n >= 10
	case d[0] == '0':
		return n >= 9 && n <= 11
	case country != "" && strings.HasPrefix(d, country) && n == len(country)+9:
		return true
	}
	if n < 9 || len(r.groups) < 2 || strings.Contains(r.seps, ".") && len(r.groups) < 3 {
		return false
	}
	if r.groups[0] > 4 {
		return false
	}
	for _, g := range r.groups[1:] {
		if g < 2 || g > 4 {
			return false
		}
	}
	return true
}

func equalGroups(g []int, want ...int) bool {
	if len(g) != len(want) {
		return false
	}
	for i := range g {
		if g[i] != want[i] {
			return false
		}
	}
	return true
}

func luhn(d string) bool {
	sum, alt := 0, false
	for i := len(d) - 1; i >= 0; i-- {
		n := int(d[i] - '0')
		if alt {
			if n *= 2; n > 9 {
				n -= 9
			}
		}
		sum += n
		alt = !alt
	}
	return sum%10 == 0
}

var verhoeffD = [10][10]int{
	{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, {1, 2, 3, 4, 0, 6, 7, 8, 9, 5}, {2, 3, 4, 0, 1, 7, 8, 9, 5, 6},
	{3, 4, 0, 1, 2, 8, 9, 5, 6, 7}, {4, 0, 1, 2, 3, 9, 5, 6, 7, 8}, {5, 9, 8, 7, 6, 0, 4, 3, 2, 1},
	{6, 5, 9, 8, 7, 1, 0, 4, 3, 2}, {7, 6, 5, 9, 8, 2, 1, 0, 4, 3}, {8, 7, 6, 5, 9, 3, 2, 1, 0, 4},
	{9, 8, 7, 6, 5, 4, 3, 2, 1, 0},
}

var verhoeffP = [8][10]int{
	{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, {1, 5, 7, 6, 2, 8, 3, 0, 9, 4}, {5, 8, 0, 3, 7, 9, 6, 1, 4, 2},
	{8, 9, 1, 6, 0, 4, 3, 5, 2, 7}, {9, 4, 5, 3, 1, 2, 6, 8, 7, 0}, {4, 2, 8, 6, 5, 7, 3, 9, 0, 1},
	{2, 7, 9, 3, 8, 0, 6, 4, 1, 5}, {7, 0, 4, 6, 9, 1, 3, 2, 5, 8},
}

func verhoeff(d string) bool {
	c := 0
	for i := 0; i < len(d); i++ {
		c = verhoeffD[c][verhoeffP[i%8][int(d[len(d)-1-i]-'0')]]
	}
	return c == 0
}

func validYYMMDD(s string) bool {
	m := int(s[2]-'0')*10 + int(s[3]-'0')
	d := int(s[4]-'0')*10 + int(s[5]-'0')
	days := [13]int{0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
	return m >= 1 && m <= 12 && d >= 1 && d <= days[m]
}

// maskIDTokens masks Kenyan KRA PINs (A123456789Z) and UK National
// Insurance numbers (AB123456C) written as single words.
func maskIDTokens(s string, n *[4]int) string {
	var b strings.Builder
	last := 0
	for i := 0; i < len(s); {
		if !isAlnum(s[i]) {
			i++
			continue
		}
		j := i
		for j < len(s) && isAlnum(s[j]) {
			j++
		}
		if w := s[i:j]; kraPIN(w) || nino(w) {
			b.WriteString(s[last:i])
			b.WriteString("[ID]")
			n[maskID]++
			last = j
		}
		i = j
	}
	if b.Len() == 0 {
		return s
	}
	b.WriteString(s[last:])
	return b.String()
}

func upper(c byte) bool { return c >= 'A' && c <= 'Z' }

func digitsOnly(s string) bool {
	for i := 0; i < len(s); i++ {
		if !isDigit(s[i]) {
			return false
		}
	}
	return s != ""
}

func kraPIN(w string) bool {
	return len(w) == 11 && (w[0] == 'A' || w[0] == 'P') && digitsOnly(w[1:10]) && upper(w[10])
}

func nino(w string) bool {
	if len(w) != 9 || !upper(w[0]) || !upper(w[1]) || !digitsOnly(w[2:8]) || w[8] < 'A' || w[8] > 'D' {
		return false
	}
	if strings.IndexByte("DFIQUV", w[0]) >= 0 || strings.IndexByte("DFIQUVO", w[1]) >= 0 {
		return false
	}
	switch w[:2] {
	case "BG", "GB", "NK", "KN", "TN", "NT", "ZZ":
		return false
	}
	return true
}

// phoneColumn reports whether a column name says it holds phone numbers.
func phoneColumn(h string) bool {
	for _, w := range strings.FieldsFunc(strings.ToLower(h), func(r rune) bool { return !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9') }) {
		switch w {
		case "phone", "phones", "mobile", "msisdn", "tel", "telephone", "cell", "cellphone", "whatsapp", "simu":
			return true
		}
	}
	return false
}

// idColumn reports whether a column name says it holds ID numbers:
// national_id, ID Number, passport_no, ssn, KRA PIN and the like.
func idColumn(h string) bool {
	words := strings.FieldsFunc(strings.ToLower(h), func(r rune) bool { return !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9') })
	has := func(w string) bool { return contains(words, w) }
	for _, w := range words {
		switch w {
		case "ssn", "nin", "nid", "nino", "passport", "aadhaar", "huduma", "nationalid", "idnumber", "idno", "taxid", "taxpin", "krapin", "passportno":
			return true
		}
	}
	for i := 0; i+1 < len(words); i++ {
		if words[i] == "id" && (words[i+1] == "number" || words[i+1] == "no" || words[i+1] == "num" || words[i+1] == "nr") {
			return true
		}
	}
	return has("national") && (has("id") || has("identity")) ||
		has("tax") && (has("id") || has("pin") || has("number") || has("no")) ||
		has("kra") && has("pin") || has("social") && has("security")
}
