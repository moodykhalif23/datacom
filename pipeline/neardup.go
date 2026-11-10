package pipeline

import (
	"fmt"
	"math"
	"sync/atomic"
	"unicode"
	"unicode/utf8"
)

// NearDupOptions configures near-duplicate removal.
type NearDupOptions struct {
	Columns   []string
	Threshold float64
	MemoryMB  int
	Shingle   int
}

// NearIndex is a MinHash LSH index of fixed size, safe to share between
// pipelines that dedupe against each other.
type NearIndex struct {
	bands, rows, shingle int
	seeds, mults         []uint64
	tables               [][]atomic.Uint64
	mask                 uint64
	evicted              atomic.Int64
	sizeMB               int
}

func NewNearIndex(o NearDupOptions) (*NearIndex, error) {
	if o.Threshold <= 0 || o.Threshold > 1 {
		return nil, fmt.Errorf("near-duplicate threshold must be above 0 and at most 1, got %g", o.Threshold)
	}
	if o.MemoryMB <= 0 {
		o.MemoryMB = 256
	}
	if o.Shingle <= 0 {
		o.Shingle = 5
	}
	bands, rows := lshParams(o.Threshold)
	slots := uint64(1)
	for slots*2*8*uint64(bands) <= uint64(o.MemoryMB)<<20 {
		slots *= 2
	}
	ix := &NearIndex{bands: bands, rows: rows, shingle: o.Shingle, mask: slots - 1, sizeMB: o.MemoryMB}
	seed := uint64(0x9E3779B97F4A7C15)
	for i := 0; i < bands*rows; i++ {
		seed = mix64(seed + 0x9E3779B97F4A7C15)
		ix.seeds = append(ix.seeds, seed)
		ix.mults = append(ix.mults, mix64(seed^0xD6E8FEB86659FD93)|1)
	}
	ix.tables = make([][]atomic.Uint64, bands)
	for b := range ix.tables {
		ix.tables[b] = make([]atomic.Uint64, slots)
	}
	return ix, nil
}

func lshParams(t float64) (bands, rows int) {
	best := math.Inf(1)
	for r := 1; r <= 32; r++ {
		b := 128 / r
		if b < 1 {
			break
		}
		if d := math.Abs(math.Pow(1/float64(b), 1/float64(r)) - t); d < best {
			best, bands, rows = d, b, r
		}
	}
	return bands, rows
}

const probe = 8

// Seen reports whether text is a near-duplicate of something already added,
// and adds it when it is not. Empty text is never a duplicate.
func (ix *NearIndex) Seen(text string) bool {
	sig, ok := ix.signature(text)
	return ok && ix.seenSig(sig)
}

func (ix *NearIndex) seenSig(sig []uint64) bool {
	keys := make([]uint64, ix.bands)
	for b := 0; b < ix.bands; b++ {
		h := hashSeed
		for _, m := range sig[b*ix.rows : (b+1)*ix.rows] {
			h = (h ^ m) * 1099511628211
		}
		if h = mix64(h); h == 0 {
			h = 1
		}
		keys[b] = h
		t := ix.tables[b]
		for p := uint64(0); p < probe; p++ {
			if v := t[(h+p)&ix.mask].Load(); v == h {
				return true
			} else if v == 0 {
				break
			}
		}
	}
	for b, h := range keys {
		t, placed := ix.tables[b], false
		for p := uint64(0); p < probe && !placed; p++ {
			slot := &t[(h+p)&ix.mask]
			placed = slot.CompareAndSwap(0, h) || slot.Load() == h
		}
		if !placed {
			t[h&ix.mask].Store(h)
			ix.evicted.Add(1)
		}
	}
	return false
}

func (ix *NearIndex) signature(text string) ([]uint64, bool) {
	k := ix.shingle
	window := make([]uint64, 0, k)
	sig := make([]uint64, len(ix.seeds))
	for i := range sig {
		sig[i] = math.MaxUint64
	}
	words, shingles := 0, 0
	add := func() {
		h := hashSeed
		for _, w := range window {
			h = (h ^ w) * 1099511628211
		}
		x := mix64(h)
		for i, s := range ix.seeds {
			if v := (x ^ s) * ix.mults[i]; v < sig[i] {
				sig[i] = v
			}
		}
		shingles++
	}
	var w uint64
	inWord := false
	for i := 0; i <= len(text); {
		r, size := rune(0), 1
		if i < len(text) {
			r, size = utf8.DecodeRuneInString(text[i:])
		}
		if i < len(text) && (unicode.IsLetter(r) || unicode.IsDigit(r)) {
			if !inWord {
				w, inWord = hashSeed, true
			}
			w = (w ^ uint64(unicode.ToLower(r))) * 1099511628211
		} else if inWord {
			inWord = false
			words++
			if len(window) == k {
				copy(window, window[1:])
				window = window[:k-1]
			}
			if window = append(window, w); len(window) == k {
				add()
			}
		}
		i += size
	}
	if words == 0 {
		return nil, false
	}
	if shingles == 0 {
		add()
	}
	return sig, true
}

func NearDedupe(in *Table, ix *NearIndex, columns []string) (*Table, error) {
	cols, err := textColumns(in.Header, columns, "-near-dupes")
	if err != nil {
		return nil, err
	}
	mon := in.monitor()
	dropped := mon.Counter("dropped:near-dupes")
	out, ch := in.child(in.Header)
	go func() {
		defer close(ch)
		ordered(in, func(r Row) []uint64 {
			sig, _ := ix.signature(joinColumns(r, cols))
			return sig
		}, func(r Row, sig []uint64) bool {
			if sig != nil && ix.seenSig(sig) {
				bump(dropped, 1)
				return true
			}
			return out.send(ch, r)
		})
	}()
	return out, nil
}

// Note reports through mon when the index filled up and overwrote entries.
func (ix *NearIndex) Note(mon *Monitor) {
	if n := ix.evicted.Load(); n > 0 && mon != nil {
		mon.Note("near-duplicate index filled its %d MB; %s older entries were overwritten, so a few far-apart near-duplicates may remain. A larger -near-mem checks more.", ix.sizeMB, commas(n))
	}
}

// Params returns the LSH bands and rows the index uses.
func (ix *NearIndex) Params() (bands, rows int) { return ix.bands, ix.rows }
