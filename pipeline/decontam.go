package pipeline

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"unicode"
	"unicode/utf8"
)

// Contamination holds the word n-grams of test sets so training text that
// overlaps them can be dropped before it leaks into a model.
type Contamination struct {
	N       int
	Files   []string
	Items   []int64 // test items loaded per file
	Skipped int64   // items shorter than MinItemWords, not used
	grams   map[uint64]int32
	short   map[int]bool
}

// MinItemWords is the shortest test item used: a one-word answer would
// match half of any corpus.
const MinItemWords = 8

// LoadTestSets reads every row of each file, any format Open reads, and
// keeps the hash of each n-word sequence in each field, so a question still
// matches when a document prints its answer differently. Fields shorter
// than n but at least MinItemWords long are kept whole.
func LoadTestSets(ctx context.Context, files []string, n int) (*Contamination, error) {
	if n <= 0 {
		n = 13
	}
	c := &Contamination{N: n, Files: files, Items: make([]int64, len(files)), grams: map[uint64]int32{}, short: map[int]bool{}}
	for fi, path := range files {
		r, f, closeFn, err := Open(ctx, path, FormatAuto)
		if err != nil {
			return nil, fmt.Errorf("test set %s: %w", path, err)
		}
		t := Read(ctx, r, f)
		var words []uint64
		for row := range t.Rows {
			used := false
			for _, cell := range row {
				words = wordHashes(cell, words[:0])
				switch {
				case len(words) >= n:
					for i := 0; i+n <= len(words); i++ {
						c.add(gramHash(words[i:i+n]), fi)
					}
				case len(words) >= MinItemWords:
					c.add(gramHash(words), fi)
					c.short[len(words)] = true
				default:
					continue
				}
				used = true
			}
			if used {
				c.Items[fi]++
			} else {
				c.Skipped++
			}
		}
		err = t.Err()
		closeFn()
		if err != nil {
			return nil, fmt.Errorf("test set %s: %w", path, err)
		}
	}
	return c, nil
}

func (c *Contamination) add(h uint64, file int) {
	if _, ok := c.grams[h]; !ok {
		c.grams[h] = int32(file)
	}
}

// Match returns the index of the first test file text overlaps, or -1.
func (c *Contamination) Match(text string, buf []uint64) (int, []uint64) {
	words := wordHashes(text, buf[:0])
	for i := 0; i+c.N <= len(words); i++ {
		if f, ok := c.grams[gramHash(words[i:i+c.N])]; ok {
			return int(f), words
		}
	}
	for l := range c.short {
		for i := 0; i+l <= len(words); i++ {
			if f, ok := c.grams[gramHash(words[i:i+l])]; ok {
				return int(f), words
			}
		}
	}
	return -1, words
}

// Decontaminate drops rows whose text shares an n-gram with a test set,
// counting the drops per test file.
func Decontaminate(in *Table, c *Contamination, columns []string) (*Table, error) {
	cols, err := textColumns(in.Header, columns, "-decontam")
	if err != nil {
		return nil, err
	}
	mon := in.monitor()
	dropped := make([]*atomic.Int64, len(c.Files))
	for i := range c.Files {
		dropped[i] = mon.Counter("dropped:decontam:" + strconv.Itoa(i+1))
	}
	out, ch := in.child(in.Header)
	go func() {
		defer close(ch)
		ordered(in, func(r Row) int {
			f, _ := c.Match(joinColumns(r, cols), nil)
			return f
		}, func(r Row, f int) bool {
			if f >= 0 {
				bump(dropped[f], 1)
				return true
			}
			return out.send(ch, r)
		})
	}()
	return out, nil
}

// wordHashes appends the hash of each lower-cased run of letters and digits
// in text to buf.
func wordHashes(text string, buf []uint64) []uint64 {
	var w uint64
	in := false
	for i := 0; i < len(text); {
		r, size := utf8.DecodeRuneInString(text[i:])
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			if !in {
				w, in = hashSeed, true
			}
			w = (w ^ uint64(unicode.ToLower(r))) * 1099511628211
		} else if in {
			buf, in = append(buf, w), false
		}
		i += size
	}
	if in {
		buf = append(buf, w)
	}
	return buf
}

func gramHash(words []uint64) uint64 {
	h := hashSeed ^ uint64(len(words))
	for _, w := range words {
		h = (h ^ w) * 1099511628211
	}
	return mix64(h)
}
