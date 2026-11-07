package pipeline

import (
	"fmt"
	"regexp"
	"strings"
)

// Cond is one filter clause: column op value.
// Supported ops: == != > >= < <= ~ (contains, case-insensitive) !~ =~ (regex)
// and the unary "col is null" / "col not null".
type Cond struct {
	col   int
	op    string
	val   string
	num   float64
	isNum bool
	re    *regexp.Regexp
}

var condRe = regexp.MustCompile(`^\s*([^\s=!<>~]+)\s*(==|!=|>=|<=|=~|!~|>|<|~|=)\s*(.*?)\s*$`)

// ParseCond parses "amount > 100", "region == Nairobi", "name ~ ltd",
// "email =~ ^.*@gov\.ke$", "phone is null", "phone not null".
func ParseCond(header []string, expr string) (*Cond, error) {
	e := strings.TrimSpace(expr)
	lower := strings.ToLower(e)
	for _, suf := range []string{" is null", " not null", " is not null"} {
		if strings.HasSuffix(lower, suf) {
			col := strings.TrimSpace(e[:len(e)-len(suf)])
			i := Index(header, col)
			if i < 0 {
				return nil, fmt.Errorf("unknown column %q", col)
			}
			op := "isnull"
			if suf != " is null" {
				op = "notnull"
			}
			return &Cond{col: i, op: op}, nil
		}
	}
	m := condRe.FindStringSubmatch(e)
	if m == nil {
		return nil, fmt.Errorf("cannot parse condition %q", expr)
	}
	i := Index(header, m[1])
	if i < 0 {
		return nil, fmt.Errorf("unknown column %q", m[1])
	}
	c := &Cond{col: i, op: m[2], val: strings.Trim(m[3], `"'`)}
	if c.op == "=" {
		c.op = "=="
	}
	if f, ok := ParseFloat(c.val); ok {
		c.num, c.isNum = f, true
	}
	if c.op == "=~" || c.op == "!~" {
		re, err := regexp.Compile("(?i)" + c.val)
		if err != nil {
			return nil, fmt.Errorf("bad regex in %q: %w", expr, err)
		}
		c.re = re
	}
	return c, nil
}

// Match reports whether row satisfies the condition.
func (c *Cond) Match(r Row) bool {
	v := r.Get(c.col)
	switch c.op {
	case "isnull":
		return v == ""
	case "notnull":
		return v != ""
	case "~":
		return strings.Contains(strings.ToLower(v), strings.ToLower(c.val))
	case "=~":
		return c.re.MatchString(v)
	case "!~":
		return !c.re.MatchString(v)
	}
	if c.isNum {
		if f, ok := ParseFloat(v); ok {
			switch c.op {
			case "==":
				return f == c.num
			case "!=":
				return f != c.num
			case ">":
				return f > c.num
			case ">=":
				return f >= c.num
			case "<":
				return f < c.num
			case "<=":
				return f <= c.num
			}
		}
		if c.op == "!=" {
			return true
		}
		return false
	}
	cmp := strings.Compare(strings.ToLower(v), strings.ToLower(c.val))
	switch c.op {
	case "==":
		return cmp == 0
	case "!=":
		return cmp != 0
	case ">":
		return cmp > 0
	case ">=":
		return cmp >= 0
	case "<":
		return cmp < 0
	case "<=":
		return cmp <= 0
	}
	return false
}

// Filter keeps rows matching every condition (AND).
func Filter(in *Table, conds []*Cond) *Table {
	if len(conds) == 0 {
		return in
	}
	out := &Table{Header: in.Header, Err: in.Err}
	ch := make(chan Row, 1024)
	out.Rows = ch
	go func() {
		defer close(ch)
		for r := range in.Rows {
			ok := true
			for _, c := range conds {
				if !c.Match(r) {
					ok = false
					break
				}
			}
			if ok {
				ch <- r
			}
		}
	}()
	return out
}

// Select keeps only the named columns, in the given order. Names may be
// renamed with "old:new".
func Select(in *Table, cols []string) (*Table, error) {
	if len(cols) == 0 {
		return in, nil
	}
	idx := make([]int, len(cols))
	hdr := make([]string, len(cols))
	for i, c := range cols {
		name, alias, _ := strings.Cut(c, ":")
		j := Index(in.Header, name)
		if j < 0 {
			return nil, fmt.Errorf("unknown column %q", name)
		}
		idx[i] = j
		if alias == "" {
			alias = in.Header[j]
		}
		hdr[i] = strings.TrimSpace(alias)
	}
	out := &Table{Header: hdr, Err: in.Err}
	ch := make(chan Row, 1024)
	out.Rows = ch
	go func() {
		defer close(ch)
		for r := range in.Rows {
			nr := make(Row, len(idx))
			for i, j := range idx {
				nr[i] = r.Get(j)
			}
			ch <- nr
		}
	}()
	return out, nil
}

// Limit passes through at most n rows (n <= 0 means unlimited).
func Limit(in *Table, n int) *Table {
	if n <= 0 {
		return in
	}
	out := &Table{Header: in.Header, Err: in.Err}
	ch := make(chan Row, 1024)
	out.Rows = ch
	go func() {
		defer close(ch)
		i := 0
		for r := range in.Rows {
			if i < n {
				ch <- r
			}
			i++ // keep draining so upstream goroutines finish
		}
	}()
	return out
}
