package pipeline

import (
	"encoding/json"
	"fmt"
	"io"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Monitor counts what happens to the data as it streams: rows and bytes
// in, rows out, values that could not be typed, rows padded or dropped,
// join misses, and stages that had to buffer. It costs nothing when not
// attached, and only atomic adds when it is.
type Monitor struct {
	start                                    time.Time
	RowsIn, RowsOut, Bytes                   atomic.Int64
	Padded, Trimmed, Empty, Dupes, Unmatched atomic.Int64
	mu                                       sync.Mutex
	bad                                      map[string]*badColumn
	notes                                    []string
	err                                      error
}

type badColumn struct {
	Column   string   `json:"column"`
	Kind     string   `json:"kind"`
	Count    int64    `json:"count"`
	Examples []string `json:"examples"`
}

func NewMonitor() *Monitor {
	return &Monitor{start: time.Now(), bad: map[string]*badColumn{}}
}

// Meter wraps r so bytes read are counted.
func (m *Monitor) Meter(r io.Reader) io.Reader { return &meter{r, &m.Bytes} }

type meter struct {
	r io.Reader
	n *atomic.Int64
}

func (m *meter) Read(p []byte) (int, error) {
	n, err := m.r.Read(p)
	m.n.Add(int64(n))
	return n, err
}

// Bad records a value that did not fit its column's type.
func (m *Monitor) Bad(column string, kind Kind, v string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b := m.bad[column]
	if b == nil {
		b = &badColumn{Column: column, Kind: kind.String()}
		m.bad[column] = b
	}
	b.Count++
	if len(b.Examples) < 3 && !contains(b.Examples, v) {
		b.Examples = append(b.Examples, trunc(v, 40))
	}
}

// Note records a one-off observation such as a stage that buffered rows.
func (m *Monitor) Note(format string, args ...any) {
	m.mu.Lock()
	m.notes = append(m.notes, fmt.Sprintf(format, args...))
	m.mu.Unlock()
}

// Fail records the error that stopped the run, if any.
func (m *Monitor) Fail(err error) {
	m.mu.Lock()
	m.err = err
	m.mu.Unlock()
}

// Live prints a progress line to w every interval until stop is closed.
// On a terminal the line is redrawn in place.
func (m *Monitor) Live(w io.Writer, every time.Duration, terminal bool, stop <-chan struct{}) {
	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		select {
		case <-stop:
			if terminal {
				fmt.Fprint(w, "\r\033[K")
			}
			return
		case <-tick.C:
			line := m.progress()
			if terminal {
				fmt.Fprint(w, "\r\033[K"+line)
			} else {
				fmt.Fprintln(w, line)
			}
		}
	}
}

func (m *Monitor) progress() string {
	secs := time.Since(m.start).Seconds()
	mb := float64(m.Bytes.Load()) / 1e6
	return fmt.Sprintf("%s rows  %.0f MB  %.0f MB/s  %s rows/s  mem %d MB  %.0fs",
		commas(m.RowsIn.Load()), mb, mb/secs, commas(int64(float64(m.RowsIn.Load())/secs)), memMB(), secs)
}

func memMB() uint64 {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.Sys / 1e6
}

func commas(n int64) string {
	s := fmt.Sprint(n)
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
}

// RunReport is the summary of one run.
type RunReport struct {
	Seconds   float64      `json:"seconds"`
	RowsIn    int64        `json:"rows_in"`
	RowsOut   int64        `json:"rows_out"`
	Bytes     int64        `json:"bytes_in"`
	PeakMB    uint64       `json:"peak_mb"`
	Padded    int64        `json:"rows_padded,omitempty"`
	Trimmed   int64        `json:"rows_trimmed,omitempty"`
	Empty     int64        `json:"rows_empty_dropped,omitempty"`
	Dupes     int64        `json:"rows_duplicate_dropped,omitempty"`
	Unmatched int64        `json:"join_unmatched,omitempty"`
	Bad       []*badColumn `json:"untyped_values,omitempty"`
	Notes     []string     `json:"notes,omitempty"`
	Error     string       `json:"error,omitempty"`
}

func (m *Monitor) Report() RunReport {
	m.mu.Lock()
	defer m.mu.Unlock()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	r := RunReport{
		Seconds: time.Since(m.start).Seconds(), RowsIn: m.RowsIn.Load(), RowsOut: m.RowsOut.Load(),
		Bytes: m.Bytes.Load(), PeakMB: ms.Sys / 1e6,
		Padded: m.Padded.Load(), Trimmed: m.Trimmed.Load(), Empty: m.Empty.Load(),
		Dupes: m.Dupes.Load(), Unmatched: m.Unmatched.Load(), Notes: m.notes,
	}
	for _, b := range m.bad {
		r.Bad = append(r.Bad, b)
	}
	sort.Slice(r.Bad, func(i, j int) bool { return r.Bad[i].Count > r.Bad[j].Count })
	if m.err != nil {
		r.Error = m.err.Error()
	}
	return r
}

// WriteText prints the report for people, with what to do about each
// finding.
func (r RunReport) WriteText(w io.Writer) {
	fmt.Fprintf(w, "monitor: %s rows in, %s out, %.0f MB read in %.1fs (%s rows/s), peak memory %d MB\n",
		commas(r.RowsIn), commas(r.RowsOut), float64(r.Bytes)/1e6, r.Seconds,
		commas(int64(float64(r.RowsIn)/max(r.Seconds, 0.001))), r.PeakMB)
	if r.Error != "" {
		fmt.Fprintf(w, "  stopped by error: %s\n", r.Error)
	}
	pct := func(n int64) string {
		if r.RowsIn == 0 {
			return ""
		}
		return fmt.Sprintf(" (%.2f%%)", 100*float64(n)/float64(r.RowsIn))
	}
	for _, b := range r.Bad {
		fmt.Fprintf(w, "  %s: %s values%s are not %s, e.g. %s. They were left as written; fix them at the source or filter them out.\n",
			b.Column, commas(b.Count), pct(b.Count), b.Kind, strings.Join(quoteAll(b.Examples), ", "))
	}
	if r.Padded+r.Trimmed > 0 {
		fmt.Fprintf(w, "  %s rows had too few columns and %s too many; check for stray delimiters or unquoted commas in the source.\n", commas(r.Padded), commas(r.Trimmed))
	}
	if r.Empty > 0 {
		fmt.Fprintf(w, "  %s empty rows dropped%s.\n", commas(r.Empty), pct(r.Empty))
	}
	if r.Dupes > 0 {
		fmt.Fprintf(w, "  %s duplicate rows dropped%s.\n", commas(r.Dupes), pct(r.Dupes))
	}
	if r.Unmatched > 0 {
		fmt.Fprintf(w, "  %s rows%s found no join match; their joined columns are empty. Check key spelling and casing.\n", commas(r.Unmatched), pct(r.Unmatched))
	}
	for _, n := range r.Notes {
		fmt.Fprintf(w, "  %s\n", n)
	}
}

// WriteJSON appends the report as one JSON line, for collecting runs.
func (r RunReport) WriteJSON(w io.Writer) error {
	return json.NewEncoder(w).Encode(r)
}

func quoteAll(s []string) []string {
	out := make([]string, len(s))
	for i, v := range s {
		out[i] = fmt.Sprintf("%q", v)
	}
	return out
}
