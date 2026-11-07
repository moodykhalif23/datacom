// Package pipeline streams tabular data through clean, filter, aggregate and
// output stages without ever holding a whole file in memory.
package pipeline

import "strings"

// Row is one record. Values line up with Header by index.
type Row []string

// Table is a streaming source: a header plus a channel of rows.
type Table struct {
	Header []string
	Rows   <-chan Row
	Err    *error // set by the producer when the stream ends with an error
}

// Index returns the column position for name, or -1.
func Index(header []string, name string) int {
	name = strings.ToLower(strings.TrimSpace(name))
	for i, h := range header {
		if strings.ToLower(h) == name {
			return i
		}
	}
	return -1
}

// Get returns the value at column i, or "" when the row is short.
func (r Row) Get(i int) string {
	if i < 0 || i >= len(r) {
		return ""
	}
	return r[i]
}

// Kind is an inferred column type.
type Kind int

const (
	KindEmpty Kind = iota
	KindInt
	KindFloat
	KindBool
	KindDate
	KindText
)

func (k Kind) String() string {
	return [...]string{"empty", "int", "float", "bool", "date", "text"}[k]
}

// Widen returns the narrowest kind that holds both a and b.
func Widen(a, b Kind) Kind {
	if a == KindEmpty {
		return b
	}
	if b == KindEmpty || a == b {
		return a
	}
	if (a == KindInt && b == KindFloat) || (a == KindFloat && b == KindInt) {
		return KindFloat
	}
	return KindText
}
