package pipeline

import (
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"unicode"
	"unicode/utf8"
)

// QualityRules are the document heuristics of the Gopher paper (Rae et al.,
// 2021). A document fails at the first rule it breaks; zero turns a rule
// off. Stop words are English, so that rule is off by default.
type QualityRules struct {
	MinWords         int
	MaxWords         int
	MinMeanWordLen   float64
	MaxMeanWordLen   float64
	MaxSymbolRatio   float64
	MaxBulletLines   float64
	MaxEllipsisLines float64
	MinAlphaWords    float64
	MaxDupLines      float64
	MinStopwords     int
}

func DefaultQuality() QualityRules {
	return QualityRules{
		MinWords: 50, MaxWords: 100_000, MinMeanWordLen: 3, MaxMeanWordLen: 10, MaxSymbolRatio: 0.1,
		MaxBulletLines: 0.9, MaxEllipsisLines: 0.3, MinAlphaWords: 0.8, MaxDupLines: 0.3,
	}
}

// QualityRuleNames lists the rules in the order they are checked.
var QualityRuleNames = []string{"min-words", "max-words", "word-length", "symbols", "bullets", "ellipsis", "alpha", "dup-lines", "stopwords"}

// ParseQuality reads "default" or a list such as "min-words=20,stopwords=2"
// applied on top of the defaults.
func ParseQuality(spec string) (QualityRules, error) {
	q := DefaultQuality()
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" || part == "default" || part == "on" {
			continue
		}
		key, val, ok := strings.Cut(part, "=")
		if !ok {
			return q, fmt.Errorf("-quality: %q needs a value, like min-words=20", part)
		}
		f, err := strconv.ParseFloat(strings.TrimSpace(val), 64)
		if err != nil || f < 0 {
			return q, fmt.Errorf("-quality: %q is not a number", val)
		}
		switch strings.TrimSpace(key) {
		case "min-words":
			q.MinWords = int(f)
		case "max-words":
			q.MaxWords = int(f)
		case "min-word-length":
			q.MinMeanWordLen = f
		case "max-word-length":
			q.MaxMeanWordLen = f
		case "symbols":
			q.MaxSymbolRatio = f
		case "bullets":
			q.MaxBulletLines = f
		case "ellipsis":
			q.MaxEllipsisLines = f
		case "alpha":
			q.MinAlphaWords = f
		case "dup-lines":
			q.MaxDupLines = f
		case "stopwords":
			q.MinStopwords = int(f)
		default:
			return q, fmt.Errorf("-quality: unknown rule %q (rules: min-words, max-words, min-word-length, max-word-length, symbols, bullets, ellipsis, alpha, dup-lines, stopwords)", key)
		}
	}
	return q, nil
}

var stopwords = map[string]bool{"the": true, "be": true, "to": true, "of": true, "and": true, "that": true, "have": true, "with": true}

// Check returns the name of the first rule text breaks, or "".
func (q QualityRules) Check(text string) string {
	words := strings.Fields(text)
	n := len(words)
	if q.MinWords > 0 && n < q.MinWords {
		return "min-words"
	}
	if q.MaxWords > 0 && n > q.MaxWords {
		return "max-words"
	}
	if n == 0 {
		return ""
	}
	var chars, alpha, stops, symbols int
	for _, w := range words {
		chars += utf8.RuneCountInString(w)
		if strings.IndexFunc(w, unicode.IsLetter) >= 0 {
			alpha++
		}
		if q.MinStopwords > 0 && stopwords[strings.ToLower(strings.Trim(w, ".,;:!?\"'()"))] {
			stops++
		}
	}
	symbols = strings.Count(text, "#") + strings.Count(text, "...") + strings.Count(text, "…")
	mean := float64(chars) / float64(n)
	if q.MinMeanWordLen > 0 && mean < q.MinMeanWordLen || q.MaxMeanWordLen > 0 && mean > q.MaxMeanWordLen {
		return "word-length"
	}
	if q.MaxSymbolRatio > 0 && float64(symbols)/float64(n) > q.MaxSymbolRatio {
		return "symbols"
	}
	var lines, bullets, ellipses, dups int
	seen := map[string]bool{}
	for _, l := range strings.Split(text, "\n") {
		if l = strings.TrimSpace(l); l == "" {
			continue
		}
		lines++
		if r, _ := utf8.DecodeRuneInString(l); strings.ContainsRune("•-*‣◦·▪", r) {
			bullets++
		}
		if strings.HasSuffix(l, "...") || strings.HasSuffix(l, "…") {
			ellipses++
		}
		if seen[l] {
			dups++
		}
		seen[l] = true
	}
	if lines > 0 {
		if q.MaxBulletLines > 0 && float64(bullets)/float64(lines) > q.MaxBulletLines {
			return "bullets"
		}
		if q.MaxEllipsisLines > 0 && float64(ellipses)/float64(lines) > q.MaxEllipsisLines {
			return "ellipsis"
		}
	}
	if q.MinAlphaWords > 0 && float64(alpha)/float64(n) < q.MinAlphaWords {
		return "alpha"
	}
	if lines > 1 && q.MaxDupLines > 0 && float64(dups)/float64(lines) > q.MaxDupLines {
		return "dup-lines"
	}
	if q.MinStopwords > 0 && stops < q.MinStopwords {
		return "stopwords"
	}
	return ""
}

// Quality drops rows whose text breaks a rule, counting each drop under the
// first rule broken.
func Quality(in *Table, q QualityRules, columns []string) (*Table, error) {
	cols, err := textColumns(in.Header, columns, "-quality")
	if err != nil {
		return nil, err
	}
	mon := in.monitor()
	dropped := map[string]*atomic.Int64{}
	for _, name := range QualityRuleNames {
		dropped[name] = mon.Counter("dropped:quality:" + name)
	}
	out, ch := in.child(in.Header)
	go func() {
		defer close(ch)
		ordered(in, nil, func(r Row) string { return q.Check(joinColumns(r, cols)) }, func(r Row, rule string) bool {
			if rule != "" {
				bump(dropped[rule], 1)
				return true
			}
			return out.send(ch, r)
		})
	}()
	return out, nil
}

func textColumns(header, names []string, flag string) ([]int, error) {
	cols := make([]int, 0, len(names))
	for _, c := range names {
		i := Index(header, c)
		if i < 0 {
			return nil, fmt.Errorf("%s: unknown column %q", flag, c)
		}
		cols = append(cols, i)
	}
	if len(cols) == 0 {
		for i := range header {
			cols = append(cols, i)
		}
	}
	return cols, nil
}

func joinColumns(r Row, cols []int) string {
	if len(cols) == 1 {
		return r.Get(cols[0])
	}
	var b strings.Builder
	for i, c := range cols {
		if i > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(r.Get(c))
	}
	return b.String()
}
