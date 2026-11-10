package pipeline

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"runtime"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"
)

func readCSV(ctx context.Context, r io.Reader, comma rune) *Table {
	// Trimming a tab delimiter would merge empty cells in TSV.
	trim := comma != '\t'
	src := &csvPieces{r: r, s: csvSplit{comma: byte(comma), trim: trim}}
	first, err := src.next(1)
	var hdr []string
	var head []Row
	if err == nil {
		head, err = parseCSV(first, comma, trim)
		if len(head) > 0 {
			hdr, head = head[0], head[1:]
			hdr[0] = strings.TrimPrefix(hdr[0], "\uFEFF")
		} else {
			err = io.EOF
		}
	}
	t, ch := newRoot(ctx, hdr)
	if err != nil {
		if err != io.EOF {
			t.fail(fmt.Errorf("read header: %w", err))
		}
		close(ch)
		return t
	}
	go func() {
		defer close(ch)
		for _, row := range head {
			if !t.send(ch, row) {
				return
			}
		}
		type piece struct {
			b    []byte
			rows []Row
			err  error
			done chan struct{}
		}
		workers := runtime.GOMAXPROCS(0)
		if n, ok := ctx.Value(parallelKey{}).(int); ok && n > 0 {
			workers = min(workers, n)
		}
		work := make(chan *piece, workers)
		queue := make(chan *piece, workers*2)
		var wg sync.WaitGroup
		for range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for p := range work {
					p.rows, p.err = parseCSV(p.b, comma, trim)
					src.recycle(p.b)
					close(p.done)
				}
			}()
		}
		defer wg.Wait()
		go func() {
			defer close(queue)
			defer close(work)
			for {
				b, err := src.next(csvPiece)
				p := &piece{b: b, err: err, done: make(chan struct{})}
				if b == nil {
					close(p.done)
					if err != io.EOF {
						offer(t.run.ctx, queue, p)
					}
					return
				}
				if !offer(t.run.ctx, work, p) || !offer(t.run.ctx, queue, p) {
					return
				}
			}
		}()
		for p := range queue {
			<-p.done
			for _, row := range p.rows {
				if !t.send(ch, row) {
					return
				}
			}
			if p.err != nil {
				t.fail(p.err)
				return
			}
		}
	}()
	return t
}

func parseCSV(b []byte, comma rune, trim bool) ([]Row, error) {
	cr := csv.NewReader(bytes.NewReader(b))
	cr.FieldsPerRecord = -1
	cr.LazyQuotes = true
	cr.TrimLeadingSpace = trim
	cr.Comma = comma
	var rows []Row
	for {
		rec, err := cr.Read()
		if err == io.EOF {
			return rows, nil
		}
		if err != nil {
			return rows, err
		}
		rows = append(rows, rec)
	}
}

// csvPiece is roughly how many bytes each parser gets at a time.
const csvPiece = 512 << 10

// csvPieces cuts its input into runs of whole records.
type csvPieces struct {
	r    io.Reader
	s    csvSplit
	buf  []byte
	err  error
	pool sync.Pool
}

// next returns at least want bytes of whole records, or fewer when the input
// pauses, so a live feed is not held back. At the end it returns the rest,
// complete or not, as encoding/csv would; after a read error the unfinished
// record is dropped and the error comes back on the following call.
func (c *csvPieces) next(want int) ([]byte, error) {
	for c.err == nil && c.s.last < want {
		if len(c.buf) == cap(c.buf) {
			nb := c.get(max(2*cap(c.buf), csvPiece+csvPiece/2))
			c.buf = append(nb, c.buf...)
		}
		room := cap(c.buf) - len(c.buf)
		n, err := c.r.Read(c.buf[len(c.buf):cap(c.buf)])
		c.buf = c.buf[:len(c.buf)+n]
		c.err = err
		c.s.scan(c.buf, err != nil)
		if n < room && c.s.last > 0 {
			break
		}
	}
	end := c.s.last
	if c.err == io.EOF {
		end = len(c.buf)
	}
	if end == 0 {
		return nil, c.err
	}
	out := c.buf[:end]
	c.buf = append(c.get(cap(c.buf)), c.buf[end:]...)
	c.s.end -= end
	c.s.last = 0
	return out, nil
}

func (c *csvPieces) get(n int) []byte {
	if b, ok := c.pool.Get().(*[]byte); ok && cap(*b) >= n {
		return (*b)[:0]
	}
	return make([]byte, 0, n)
}

func (c *csvPieces) recycle(b []byte) {
	b = b[:0]
	c.pool.Put(&b)
}

// csvSplit tracks where records end under the rules encoding/csv applies
// with LazyQuotes: a quote opens a quoted field only at the start of a
// field (after leading space when trimming), "" inside one is a literal
// quote, and a closing quote counts only before a delimiter or line end.
type csvSplit struct {
	comma byte
	trim  bool
	state uint8
	end   int // bytes scanned so far
	last  int // offset just past the last complete record
}

const (
	csvStart  = iota // at the start of a field
	csvPlain         // inside an unquoted field
	csvQuoted        // inside a quoted field
	csvQuote         // just after a quote inside a quoted field
)

// scan advances over b from where it stopped. It pauses before bytes it
// cannot judge without more input unless eof is set.
func (s *csvSplit) scan(b []byte, eof bool) {
	i := s.end
	for i < len(b) {
		c := b[i]
		switch s.state {
		case csvStart:
			switch {
			case c == '\n':
				s.last = i + 1
			case c == '"':
				s.state = csvQuoted
			case c == s.comma:
			case s.trim && (c == ' ' || c == '\t' || c == '\r' || c == '\v' || c == '\f'):
			case s.trim && c >= utf8.RuneSelf:
				if !eof && !utf8.FullRune(b[i:]) {
					s.end = i
					return
				}
				r, n := utf8.DecodeRune(b[i:])
				if !unicode.IsSpace(r) {
					s.state = csvPlain
				}
				i += n
				continue
			default:
				s.state = csvPlain
			}
		case csvPlain:
			for i < len(b) && b[i] != s.comma && b[i] != '\n' {
				i++
			}
			if i == len(b) {
				continue
			}
			if b[i] == '\n' {
				s.last = i + 1
			}
			s.state = csvStart
		case csvQuoted:
			j := bytes.IndexByte(b[i:], '"')
			if j < 0 {
				i = len(b)
				continue
			}
			i += j
			s.state = csvQuote
		case csvQuote:
			switch {
			case c == '"':
				s.state = csvQuoted
			case c == s.comma:
				s.state = csvStart
			case c == '\n':
				s.last = i + 1
				s.state = csvStart
			case c == '\r' && i+1 == len(b) && !eof:
				s.end = i
				return
			case c == '\r' && i+1 < len(b) && b[i+1] == '\n':
				s.last = i + 2
				s.state = csvStart
				i++
			default:
				s.state = csvQuoted
				continue
			}
		}
		i++
	}
	s.end = i
}
