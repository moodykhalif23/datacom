package pipeline

import (
	"fmt"
	"regexp"
	"strings"
)

type Cond struct {
	col   int
	op    string
	val   string
	num   float64
	isNum bool
	re    *regexp.Regexp
}

var condRe = regexp.MustCompile(`^\s*([^\s=!<>~]+)\s*(==|!=|>=|<=|=~|!~|>|<|~|=)\s*(.*?)\s*$`)

func ParseCond(header []string, expr string) (*Cond, error) {
	e := strings.TrimSpace(expr)
	lower := strings.ToLower(e)
	for suf, op := range map[string]string{" is null": "isnull", " not null": "notnull", " is not null": "notnull"} {
		if strings.HasSuffix(lower, suf) {
			col := e[:len(e)-len(suf)]
			i := Index(header, col)
			if i < 0 {
				return nil, fmt.Errorf("unknown column %q", strings.TrimSpace(col))
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
	c.num, c.isNum = ParseFloat(c.val)
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
	var cmp int
	if c.isNum {
		f, ok := ParseFloat(v)
		if !ok {
			return c.op == "!=" // non-numbers never equal a number
		}
		switch {
		case f < c.num:
			cmp = -1
		case f > c.num:
			cmp = 1
		}
	} else {
		cmp = strings.Compare(strings.ToLower(v), strings.ToLower(c.val))
	}
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
	}
	return cmp <= 0 // "<="
}

// Filter keeps rows matching every condition (AND).
func Filter(in *Table, conds []*Cond) *Table {
	if len(conds) == 0 {
		return in
	}
	out, ch := in.child(in.Header)
	go func() {
		defer close(ch)
	rows:
		for r := range in.Rows {
			for _, c := range conds {
				if !c.Match(r) {
					continue rows
				}
			}
			if !out.send(ch, r) {
				return
			}
		}
	}()
	return out
}

// Select keeps only the named columns, in order. "old:new" renames.
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
		hdr[i] = in.Header[j]
		if alias = strings.TrimSpace(alias); alias != "" {
			hdr[i] = alias
		}
	}
	out, ch := in.child(hdr)
	go func() {
		defer close(ch)
		for r := range in.Rows {
			nr := make(Row, len(idx))
			for i, j := range idx {
				nr[i] = r.Get(j)
			}
			if !out.send(ch, nr) {
				return
			}
		}
	}()
	return out, nil
}

// Limit passes through at most n rows (n <= 0 means unlimited) and then
// stops the upstream stages instead of draining them.
func Limit(in *Table, n int) *Table {
	if n <= 0 {
		return in
	}
	out, ch := in.child(in.Header)
	go func() {
		defer close(ch)
		i := 0
		for r := range in.Rows {
			if !out.send(ch, r) {
				return
			}
			if i++; i == n {
				in.run.cancel()
				return
			}
		}
	}()
	return out
}
