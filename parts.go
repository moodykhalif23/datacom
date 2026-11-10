package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/moodykhalif23/datacom/pipeline"
)

// partsOutput reports an -o that names a directory or a part template with
// a * in it, such as out/ or out/part-*.jsonl.gz.
func partsOutput(out string) bool {
	if out == "" {
		return false
	}
	if strings.HasSuffix(out, "/") || strings.HasSuffix(out, string(os.PathSeparator)) || strings.Contains(filepath.Base(out), "*") {
		return true
	}
	st, err := os.Stat(out)
	return err == nil && st.IsDir()
}

var writable = map[pipeline.Format]bool{
	pipeline.FormatCSV: true, pipeline.FormatTSV: true, pipeline.FormatJSON: true, pipeline.FormatNDJSON: true,
	pipeline.FormatXLSX: true, pipeline.FormatODS: true, pipeline.FormatParquet: true, pipeline.FormatAvro: true,
	pipeline.FormatXML: true, pipeline.FormatHTML: true, pipeline.FormatSQL: true, pipeline.FormatMarkdown: true,
}

// partNames gives each source its output part: the * of the template is
// replaced by a five-digit number, and a bare directory gets part-*.<format>,
// in -f or else the first source's format.
func partNames(o opts, srcs []string) (pipeline.Format, []string, error) {
	tmpl := o.out
	format := pipeline.Format(o.format)
	if !strings.Contains(filepath.Base(tmpl), "*") {
		if format == "" {
			format = pipeline.FormatCSV
			if f, ok := pipeline.FormatFromName(strings.TrimSuffix(srcs[0], ".zst")); ok && writable[f] {
				format = f
			}
		}
		tmpl = filepath.Join(tmpl, "part-*."+string(format))
	}
	if format == "" {
		f, ok := pipeline.FormatFromName(strings.TrimSuffix(tmpl, ".zst"))
		if !ok || !writable[f] {
			return "", nil, fmt.Errorf("name the part format with -f or an extension, like %s", filepath.Join(filepath.Dir(tmpl), "part-*.parquet"))
		}
		format = f
	}
	names := make([]string, len(srcs))
	for i := range srcs {
		names[i] = strings.Replace(tmpl, "*", fmt.Sprintf("%05d", i), 1)
	}
	return format, names, nil
}

// runParts writes one part per source, -workers at a time. Every part
// dedupes against the others: exact duplicates through one shared set,
// near-duplicates through one shared index.
func runParts(ctx context.Context, cmd string, o opts, srcs []string, mon *pipeline.Monitor, rec *runRecord, start time.Time) error {
	switch {
	case cmd != "clean" && cmd != "query" && cmd != "convert":
		return fmt.Errorf("%s writes one report, not parts; give -o a file name", cmd)
	case o.group != "" || o.agg != "" || o.sortKey != "" || o.limit > 0:
		return errors.New("-group, -agg, -sort and -limit need the whole result in one output; give -o a file name instead of parts")
	}
	format, names, err := partNames(o, srcs)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(names[0]), 0o755); err != nil {
		return err
	}
	if o.dedupe {
		rec.seen = pipeline.NewSeenSet()
	}
	workers := o.workers
	if workers <= 0 {
		workers = runtime.NumCPU()
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan int)
	var mu sync.Mutex
	var first error
	var total int64
	var wg sync.WaitGroup
	for w := 0; w < min(workers, len(srcs)); w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				n, err := runPart(ctx, cmd, o, srcs[i], names[i], format, mon, rec)
				mu.Lock()
				total += n
				if err != nil && first == nil && ctx.Err() == nil {
					first = fmt.Errorf("%s: %w", srcs[i], err)
					cancel()
				}
				mu.Unlock()
			}
		}()
	}
feed:
	for i := range srcs {
		select {
		case jobs <- i:
		case <-ctx.Done():
			break feed
		}
	}
	close(jobs)
	wg.Wait()
	if first != nil {
		return first
	}
	if ctx.Err() != nil {
		return errors.New("interrupted")
	}
	if !o.quiet {
		fmt.Fprintf(os.Stderr, "%d rows in %d parts in %s\n", total, len(srcs), time.Since(start).Round(time.Millisecond))
	}
	return nil
}

func runPart(ctx context.Context, cmd string, o opts, src, name string, format pipeline.Format, mon *pipeline.Monitor, rec *runRecord) (int64, error) {
	t, closeIn, err := open(ctx, o, mon, src, false)
	if err != nil {
		return 0, err
	}
	defer closeIn()
	if cmd != "convert" {
		var done func() error
		if t, done, err = shape(ctx, t, o, rec); err != nil {
			return 0, err
		}
		defer done()
	}
	f, err := os.Create(name)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	var base io.Writer = f
	var orec *outputRecord
	if o.manifest != "" {
		orec = &outputRecord{Path: name, Format: string(format), hw: pipeline.NewHashWriter(f)}
		rec.addOutput(orec)
		base = orec.hw
	}
	out := base
	zw := compressedWriter(base, name)
	if zw != nil {
		out = zw
	}
	var n int
	if format == pipeline.FormatSQL {
		n, err = pipeline.WriteSQL(out, t, sqlTable(opts{out: name}))
	} else {
		n, err = pipeline.Write(out, t, format)
	}
	if zw != nil {
		if cerr := zw.Close(); err == nil {
			err = cerr
		}
	}
	if orec != nil {
		orec.Rows = int64(n)
	}
	return int64(n), err
}
