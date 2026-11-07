package pipeline

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
)

// Expr is a compiled expression over a row, used by -where and -derive.
type Expr func(Row) value

type value struct {
	s    string
	f    float64
	kind vkind
}

type vkind uint8

const (
	vNull vkind = iota
	vNum
	vText
	vBool
)

func num(f float64) value { return value{f: f, kind: vNum} }
func text(s string) value {
	if s == "" {
		return value{}
	}
	if f, ok := ParseFloat(s); ok {
		return value{s: s, f: f, kind: vNum}
	}
	return value{s: s, kind: vText}
}
func boolean(b bool) value {
	if b {
		return value{f: 1, kind: vBool}
	}
	return value{kind: vBool}
}

func (v value) truthy() bool {
	switch v.kind {
	case vBool, vNum:
		return v.f != 0
	case vText:
		return true
	}
	return false
}

// String renders a value as a cell.
func (v value) String() string {
	switch v.kind {
	case vNum:
		if v.s != "" {
			return v.s
		}
		return strconv.FormatFloat(v.f, 'f', -1, 64)
	case vBool:
		return strconv.FormatBool(v.f != 0)
	}
	return v.s
}

// Compile parses src against header.
func Compile(header []string, src string) (Expr, error) {
	p := &parser{header: header, src: src, toks: lex(src)}
	e, err := p.or()
	if err == nil && p.peek().kind != tEOF {
		err = fmt.Errorf("unexpected %q", p.peek().s)
	}
	if err != nil {
		return nil, fmt.Errorf("in %q: %w", src, err)
	}
	return e, nil
}

type tkind uint8

const (
	tEOF tkind = iota
	tNum
	tStr
	tIdent
	tOp
)

type token struct {
	kind tkind
	s    string
	pos  int // byte offset in the source
}

var dateRe = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}(T[\dT:.Z+-]*)?`)

func lex(s string) []token {
	var toks []token
	for i := 0; i < len(s); {
		c := s[i]
		start := i
		switch {
		case c == ' ' || c == '\t':
			i++
		case c >= '0' && c <= '9':
			if m := dateRe.FindString(s[i:]); m != "" {
				toks = append(toks, token{tStr, m, start})
				i += len(m)
				continue
			}
			j := i
			for j < len(s) && (s[j] >= '0' && s[j] <= '9' || s[j] == '.') {
				j++
			}
			toks = append(toks, token{tNum, s[i:j], start})
			i = j
		case c == '\'' || c == '"':
			j := strings.IndexByte(s[i+1:], c)
			if j < 0 {
				j = len(s) - i - 1
			}
			toks = append(toks, token{tStr, s[i+1 : i+1+j], start})
			i += j + 2
		case c == '[':
			j := strings.IndexByte(s[i:], ']')
			if j < 0 {
				j = len(s) - i
			}
			toks = append(toks, token{tIdent, s[i+1 : i+j], start})
			i += j + 1
		case c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= 0x80:
			j := i
			for j < len(s) && (s[j] == '_' || s[j] == '.' || s[j] >= '0' && s[j] <= '9' || s[j] >= 'a' && s[j] <= 'z' || s[j] >= 'A' && s[j] <= 'Z' || s[j] >= 0x80) {
				j++
			}
			toks = append(toks, token{tIdent, s[i:j], start})
			i = j
		default:
			op := s[i : i+1]
			if i+1 < len(s) && strings.Contains("== != <= >= =~ !~", s[i:i+2]) {
				op = s[i : i+2]
			}
			toks = append(toks, token{tOp, op, start})
			i += len(op)
		}
	}
	return append(toks, token{tEOF, "", len(s)})
}

type parser struct {
	header []string
	src    string
	toks   []token
	pos    int
}

func (p *parser) peek() token { return p.toks[p.pos] }
func (p *parser) next() token { t := p.toks[p.pos]; p.pos++; return t }

// accept consumes the next token if it is the given operator or keyword.
func (p *parser) accept(s string) bool {
	t := p.peek()
	if (t.kind == tOp || t.kind == tIdent) && strings.EqualFold(t.s, s) {
		p.pos++
		return true
	}
	return false
}

func (p *parser) or() (Expr, error) {
	l, err := p.and()
	for err == nil && p.accept("or") {
		var r Expr
		if r, err = p.and(); err == nil {
			a, b := l, r
			l = func(row Row) value { return boolean(a(row).truthy() || b(row).truthy()) }
		}
	}
	return l, err
}

func (p *parser) and() (Expr, error) {
	l, err := p.not()
	for err == nil && p.accept("and") {
		var r Expr
		if r, err = p.not(); err == nil {
			a, b := l, r
			l = func(row Row) value { return boolean(a(row).truthy() && b(row).truthy()) }
		}
	}
	return l, err
}

func (p *parser) not() (Expr, error) {
	if p.accept("not") {
		e, err := p.not()
		return func(row Row) value { return boolean(!e(row).truthy()) }, err
	}
	return p.cmp()
}

func (p *parser) cmp() (Expr, error) {
	l, err := p.add()
	if err != nil {
		return nil, err
	}
	if p.accept("is") {
		want := !p.accept("not")
		if !p.accept("null") {
			return nil, fmt.Errorf("expected null after is")
		}
		return func(row Row) value { return boolean((l(row).kind == vNull) == want) }, nil
	}
	if p.peek().kind == tIdent && strings.EqualFold(p.peek().s, "not") && p.pos+1 < len(p.toks) && strings.EqualFold(p.toks[p.pos+1].s, "null") {
		p.pos += 2
		return func(row Row) value { return boolean(l(row).kind != vNull) }, nil
	}
	t := p.peek()
	if t.kind != tOp || !strings.Contains("== = != < <= > >= ~ =~ !~", t.s) {
		return l, nil
	}
	p.next()
	if t.s == "=~" || t.s == "!~" {
		re, err := regexp.Compile("(?i)" + p.pattern())
		if err != nil {
			return nil, err
		}
		neg := t.s == "!~"
		return func(row Row) value { return boolean(re.MatchString(l(row).String()) != neg) }, nil
	}
	r, err := p.add()
	if err != nil {
		return nil, err
	}
	if t.s == "~" {
		return func(row Row) value {
			return boolean(strings.Contains(strings.ToLower(l(row).String()), strings.ToLower(r(row).String())))
		}, nil
	}
	op := t.s
	if op == "=" {
		op = "=="
	}
	return func(row Row) value {
		a, b := l(row), r(row)
		if a.kind == vNull || b.kind == vNull {
			return boolean(op == "!=" && a.kind != b.kind)
		}
		c := 0
		if a.kind == vNum && b.kind == vNum {
			c = cmpFloat(a.f, b.f)
		} else {
			c = strings.Compare(strings.ToLower(a.String()), strings.ToLower(b.String()))
		}
		switch op {
		case "==":
			return boolean(c == 0)
		case "!=":
			return boolean(c != 0)
		case "<":
			return boolean(c < 0)
		case "<=":
			return boolean(c <= 0)
		case ">":
			return boolean(c > 0)
		}
		return boolean(c >= 0)
	}, nil
}

func (p *parser) add() (Expr, error) { return p.binary(p.mul, "+", "-") }
func (p *parser) mul() (Expr, error) { return p.binary(p.unary, "*", "/", "%") }

// binary parses a left-associative chain of arithmetic operators.
func (p *parser) binary(operand func() (Expr, error), ops ...string) (Expr, error) {
	l, err := operand()
	for err == nil {
		t := p.peek()
		if t.kind != tOp || !contains(ops, t.s) {
			return l, nil
		}
		p.next()
		var r Expr
		if r, err = operand(); err == nil {
			l = arith(t.s, l, r)
		}
	}
	return l, err
}

func arith(op string, l, r Expr) Expr {
	return func(row Row) value {
		a, b := l(row), r(row)
		if a.kind != vNum || b.kind != vNum {
			return value{}
		}
		switch op {
		case "+":
			return num(a.f + b.f)
		case "-":
			return num(a.f - b.f)
		case "*":
			return num(a.f * b.f)
		case "%":
			if b.f == 0 {
				return value{}
			}
			return num(math.Mod(a.f, b.f))
		}
		if b.f == 0 {
			return value{}
		}
		return num(a.f / b.f)
	}
}

func (p *parser) unary() (Expr, error) {
	if p.accept("-") {
		e, err := p.unary()
		return func(row Row) value {
			if v := e(row); v.kind == vNum {
				return num(-v.f)
			}
			return value{}
		}, err
	}
	return p.primary()
}

func (p *parser) primary() (Expr, error) {
	t := p.next()
	switch t.kind {
	case tNum:
		f, err := strconv.ParseFloat(t.s, 64)
		return func(Row) value { return num(f) }, err
	case tStr:
		v := text(t.s)
		return func(Row) value { return v }, nil
	case tOp:
		if t.s == "(" {
			e, err := p.or()
			if err == nil && !p.accept(")") {
				err = fmt.Errorf("missing )")
			}
			return e, err
		}
	case tIdent:
		switch strings.ToLower(t.s) {
		case "true", "false":
			v := boolean(t.s[0] == 't' || t.s[0] == 'T')
			return func(Row) value { return v }, nil
		case "null":
			return func(Row) value { return value{} }, nil
		}
		if p.accept("(") {
			return p.call(strings.ToLower(t.s))
		}
		if i := Index(p.header, t.s); i >= 0 {
			return func(row Row) value { return text(row.Get(i)) }, nil
		}
		v := text(t.s)
		return func(Row) value { return v }, nil
	}
	return nil, fmt.Errorf("unexpected %q", t.s)
}

func (p *parser) call(name string) (Expr, error) {
	var args []Expr
	for !p.accept(")") {
		if len(args) > 0 && !p.accept(",") {
			return nil, fmt.Errorf("expected , in %s()", name)
		}
		a, err := p.or()
		if err != nil {
			return nil, err
		}
		args = append(args, a)
	}
	fn, ok := funcs[name]
	if !ok {
		return nil, fmt.Errorf("unknown function %s()", name)
	}
	if len(args) < fn.min {
		return nil, fmt.Errorf("%s() needs %d argument(s)", name, fn.min)
	}
	nullable := name == "if" || name == "coalesce" || name == "concat"
	return func(row Row) value {
		vals := make([]value, len(args))
		for i, a := range args {
			if vals[i] = a(row); vals[i].kind == vNull && !nullable {
				return value{} // a missing input gives a missing result
			}
		}
		return fn.f(vals)
	}, nil
}

var funcs = map[string]struct {
	min int
	f   func([]value) value
}{
	"round": {1, func(a []value) value {
		p := 0.0
		if len(a) > 1 {
			p = a[1].f
		}
		m := math.Pow(10, p)
		return num(math.Round(a[0].f*m) / m)
	}},
	"abs":   {1, func(a []value) value { return num(math.Abs(a[0].f)) }},
	"len":   {1, func(a []value) value { return num(float64(len([]rune(a[0].String())))) }},
	"lower": {1, func(a []value) value { return text(strings.ToLower(a[0].String())) }},
	"upper": {1, func(a []value) value { return text(strings.ToUpper(a[0].String())) }},
	"trim":  {1, func(a []value) value { return text(strings.TrimSpace(a[0].String())) }},
	"concat": {1, func(a []value) value {
		var b strings.Builder
		for _, v := range a {
			b.WriteString(v.String())
		}
		return value{s: b.String(), kind: vText}
	}},
	"year":  {1, func(a []value) value { return datePart(a[0], 0) }},
	"month": {1, func(a []value) value { return datePart(a[0], 1) }},
	"day":   {1, func(a []value) value { return datePart(a[0], 2) }},
	"if": {3, func(a []value) value {
		if a[0].truthy() {
			return a[1]
		}
		return a[2]
	}},
	"coalesce": {1, func(a []value) value {
		for _, v := range a {
			if v.kind != vNull {
				return v
			}
		}
		return value{}
	}},
}

func datePart(v value, part int) value {
	d, ok := ParseDate(v.String())
	if !ok {
		return value{}
	}
	return num(float64([]int{d.Year(), int(d.Month()), d.Day()}[part]))
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

// pattern returns a regex operand: a quoted string, or else the raw source
// up to the next " and " / " or ", so metacharacters need no quoting.
func (p *parser) pattern() string {
	if t := p.peek(); t.kind == tStr {
		p.next()
		return t.s
	}
	start, end := p.peek().pos, len(p.src)
	lower := strings.ToLower(p.src[start:])
	for _, kw := range []string{" and ", " or "} {
		if i := strings.Index(lower, kw); i >= 0 && start+i < end {
			end = start + i
		}
	}
	for p.peek().kind != tEOF && p.peek().pos < end {
		p.pos++
	}
	return strings.TrimSpace(p.src[start:end])
}
