package pipeline

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"sync"
	"testing"
)

func randomDoc(r *rand.Rand, words int) []string {
	doc := make([]string, words)
	for i := range doc {
		doc[i] = fmt.Sprintf("w%d", r.Intn(20000))
	}
	return doc
}

func edit(r *rand.Rand, doc []string, changes int) string {
	d := append([]string(nil), doc...)
	for i := 0; i < changes; i++ {
		d[r.Intn(len(d))] = fmt.Sprintf("x%d", r.Intn(1e9))
	}
	return strings.Join(d, " ")
}

func TestLSHParams(t *testing.T) {
	for _, want := range []float64{0.5, 0.7, 0.8, 0.9} {
		b, r := lshParams(want)
		if got := math.Pow(1/float64(b), 1/float64(r)); math.Abs(got-want) > 0.03 || b*r > 128 {
			t.Errorf("lshParams(%g) = %d x %d, threshold %.3f", want, b, r, got)
		}
	}
}

// TestNearDupAccuracy checks that lightly edited copies are caught and that
// unrelated documents are not.
func TestNearDupAccuracy(t *testing.T) {
	ix, err := NewNearIndex(NearDupOptions{Threshold: 0.8, MemoryMB: 64})
	if err != nil {
		t.Fatal(err)
	}
	r := rand.New(rand.NewSource(1))
	var bases [][]string
	falsePos := 0
	for i := 0; i < 2000; i++ {
		d := randomDoc(r, 300)
		bases = append(bases, d)
		if ix.Seen(strings.Join(d, " ")) {
			falsePos++
		}
	}
	caught, far := 0, 0
	for _, d := range bases[:500] {
		if ix.Seen(edit(r, d, 2)) {
			caught++
		}
	}
	for _, d := range bases[500:1000] {
		if ix.Seen(edit(r, d, 120)) {
			far++
		}
	}
	if falsePos > 0 || caught < 490 || far > 5 {
		t.Fatalf("false positives %d of 2000, near copies caught %d of 500, heavily edited flagged %d of 500", falsePos, caught, far)
	}
	if ix.Seen("") || ix.Seen("  ,, ") {
		t.Fatal("empty text must never be a duplicate")
	}
	if !ix.Seen(strings.ToUpper(strings.Join(bases[7], "  "))) {
		t.Fatal("case and spacing must not hide a duplicate")
	}
}

func TestNearDupMemoryBudget(t *testing.T) {
	mon := NewMonitor()
	ix, _ := NewNearIndex(NearDupOptions{Threshold: 0.8, MemoryMB: 1})
	r := rand.New(rand.NewSource(2))
	for i := 0; i < 20000; i++ {
		ix.Seen(strings.Join(randomDoc(r, 30), " "))
	}
	ix.Note(mon)
	if ix.evicted.Load() == 0 || len(mon.Report().Notes) != 1 || !strings.Contains(mon.Report().Notes[0], "filled its 1 MB") {
		t.Fatalf("evicted %d, notes %v", ix.evicted.Load(), mon.Report().Notes)
	}
	var total int
	for _, tb := range ix.tables {
		total += len(tb) * 8
	}
	if total > 1<<20 {
		t.Fatalf("index uses %d bytes, budget 1 MB", total)
	}
	d := randomDoc(r, 40)
	if ix.Seen(strings.Join(d, " ")) || !ix.Seen(edit(r, d, 0)) {
		t.Fatal("a full index must still catch a copy that follows its original")
	}
}

func TestNearDedupeStage(t *testing.T) {
	mon := NewMonitor()
	src := "id,body\n1,the quick brown fox jumps over the lazy dog today\n2,The quick brown fox jumps over the lazy dog today!\n3,\n4,\n5,a completely different sentence about rain in Nairobi\n"
	tb := Read(WithMonitor(context.Background(), mon), strings.NewReader(src), FormatCSV)
	ix, _ := NewNearIndex(NearDupOptions{Threshold: 0.8, MemoryMB: 8})
	out, err := NearDedupe(tb, ix, []string{"body"})
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(collect(out), ";"); got != "1,the quick brown fox jumps over the lazy dog today;3,;4,;5,a completely different sentence about rain in Nairobi" {
		t.Fatalf("rows = %q", got)
	}
	if mon.Report().Counts["dropped:near-dupes"] != 1 {
		t.Fatalf("counts = %v", mon.Report().Counts)
	}
	if _, err := NearDedupe(table(t, "a\n1\n"), ix, []string{"nope"}); err == nil {
		t.Fatal("unknown column must fail")
	}
	if _, err := NewNearIndex(NearDupOptions{Threshold: 1.5}); err == nil {
		t.Fatal("threshold above 1 must fail")
	}
}

func TestNearIndexShared(t *testing.T) {
	ix, _ := NewNearIndex(NearDupOptions{Threshold: 0.8, MemoryMB: 16})
	doc := strings.Join(randomDoc(rand.New(rand.NewSource(3)), 100), " ")
	var wg sync.WaitGroup
	var mu sync.Mutex
	kept := 0
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(g)))
			for i := 0; i < 200; i++ {
				ix.Seen(strings.Join(randomDoc(r, 50), " "))
			}
			if !ix.Seen(doc) {
				mu.Lock()
				kept++
				mu.Unlock()
			}
		}(g)
	}
	wg.Wait()
	if kept != 1 {
		t.Fatalf("kept %d copies", kept)
	}
	if !ix.Seen(doc) {
		t.Fatal("after concurrent inserts the document must be known")
	}
}
