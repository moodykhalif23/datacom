// datacom turns large, messy CSV/JSON files into clean, readable data.
// It streams, so files bigger than memory are fine.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"datacom/pipeline"
)

const usage = `datacom — small, fast data cleaner for big files (CSV, TSV, JSON, NDJSON, gzip)

usage:
  datacom profile [source]                     summarize a dataset
  datacom clean   [source] [-o out] [flags]    normalize and dedupe
  datacom query   [source] [flags]             select, filter, group, sort
  datacom convert [source] -f json|csv|...     change format

source is a file path, an http(s) URL, or stdin when omitted. All commands accept:
  -f  output format: table (default on a terminal), csv, tsv, json, ndjson, md
  -i  input format (default: auto-detect)
  -o  write to file instead of stdout
  -q  quiet: no row count on stderr

clean / query flags:
  -snake        snake_case headers           -no-types   skip type normalization
  -dedupe       drop duplicate rows          -keep-empty keep all-empty rows
  -select a,b   keep columns (old:new renames)
  -where EXPR   filter; repeatable, ANDed    e.g. "amount > 1000", "name ~ ltd",
                "date >= 2025-01-01", "email =~ @gov\.ke$", "phone not null"
  -group a,b    group by columns
  -agg SPEC     count, sum(x), avg(x), min(x), max(x), distinct(x), first(x):alias
  -sort col     sort by column; "-col" for descending (buffers rows)
  -limit n      stop after n rows

profile flags:
  -top n        values to list per column (5)   -json   machine-readable report

examples:
  datacom profile sales.csv.gz
  datacom clean raw.csv -snake -dedupe -o clean.csv
  datacom query sales.csv -where "amount > 1000" -group region -agg "count,sum(amount)" -sort -sum_amount
  datacom convert https://example.org/export.json -f csv > export.csv
`

type opts struct {
	in, out, format, inFormat string
	quiet, snake, dedupe      bool
	noTypes, keepEmpty        bool
	selectCols, group, agg    string
	sortKey                   string
	limit, top                int
	jsonReport                bool
	where                     multi
}

type multi []string

func (m *multi) String() string     { return strings.Join(*m, "; ") }
func (m *multi) Set(s string) error { *m = append(*m, s); return nil }

func main() {
	if len(os.Args) < 2 || os.Args[1] == "-h" || os.Args[1] == "--help" || os.Args[1] == "help" {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	cmd := os.Args[1]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	fs.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	var o opts
	fs.StringVar(&o.out, "o", "", "")
	fs.StringVar(&o.format, "f", "", "")
	fs.StringVar(&o.inFormat, "i", "auto", "")
	fs.BoolVar(&o.quiet, "q", false, "")
	fs.BoolVar(&o.snake, "snake", false, "")
	fs.BoolVar(&o.dedupe, "dedupe", false, "")
	fs.BoolVar(&o.noTypes, "no-types", false, "")
	fs.BoolVar(&o.keepEmpty, "keep-empty", false, "")
	fs.StringVar(&o.selectCols, "select", "", "")
	fs.Var(&o.where, "where", "")
	fs.StringVar(&o.group, "group", "", "")
	fs.StringVar(&o.agg, "agg", "", "")
	fs.StringVar(&o.sortKey, "sort", "", "")
	fs.IntVar(&o.limit, "limit", 0, "")
	fs.IntVar(&o.top, "top", 5, "")
	fs.BoolVar(&o.jsonReport, "json", false, "")

	// Accept the source before or after the flags.
	args := os.Args[2:]
	for len(args) > 0 {
		if !strings.HasPrefix(args[0], "-") || args[0] == "-" {
			if o.in == "" {
				o.in = args[0]
			}
			args = args[1:]
			continue
		}
		fs.Parse(args)
		args = fs.Args()
	}

	// Ctrl-C or a closed pipe stops every stage cleanly.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cmd, o); err != nil {
		fmt.Fprintln(os.Stderr, "datacom:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cmd string, o opts) error {
	switch cmd {
	case "profile", "clean", "query", "convert":
	default:
		return fmt.Errorf("unknown command %q (try: profile, clean, query, convert)", cmd)
	}
	start := time.Now()
	r, inFmt, closeIn, err := pipeline.Open(ctx, o.in, pipeline.Format(o.inFormat))
	if err != nil {
		return err
	}
	defer closeIn()
	t := pipeline.Read(ctx, r, inFmt)

	out := os.Stdout
	if o.out != "" {
		f, err := os.Create(o.out)
		if err != nil {
			return err
		}
		defer f.Close()
		out = f
	}
	outFmt := pipeline.Format(o.format)
	if outFmt == "" {
		var ok bool
		switch {
		case o.out != "":
			outFmt, ok = pipeline.FormatFromName(o.out)
		case isTerminal(os.Stdout) && cmd != "convert":
			outFmt, ok = pipeline.FormatTable, true
		}
		if !ok {
			outFmt = inFmt
		}
		if outFmt == pipeline.FormatJSON {
			outFmt = pipeline.FormatNDJSON
		}
	}

	var schema *pipeline.Schema
	if cmd != "convert" {
		t, schema = pipeline.Clean(t, pipeline.CleanOptions{
			SnakeHeaders: o.snake,
			Dedupe:       o.dedupe,
			DropEmpty:    !o.keepEmpty,
			Types:        !o.noTypes || cmd == "profile",
		})
	}
	if cmd == "profile" {
		rep := pipeline.Profile(t, schema.Kinds, o.top, 0)
		if err := t.Err(); err != nil {
			return err
		}
		if o.jsonReport {
			return rep.WriteJSON(out)
		}
		rep.WriteText(out)
		return nil
	}
	if cmd != "convert" {
		if t, err = shape(t, o); err != nil {
			return err
		}
	}
	n, err := pipeline.Write(out, t, outFmt)
	if err != nil {
		return err
	}
	if ctx.Err() != nil {
		return errors.New("interrupted")
	}
	if !o.quiet {
		fmt.Fprintf(os.Stderr, "%d rows in %s\n", n, time.Since(start).Round(time.Millisecond))
	}
	return nil
}

// shape applies the query stages in order: where, select, group, sort, limit.
func shape(t *pipeline.Table, o opts) (*pipeline.Table, error) {
	var err error
	if len(o.where) > 0 {
		conds := make([]*pipeline.Cond, len(o.where))
		for i, w := range o.where {
			if conds[i], err = pipeline.ParseCond(t.Header, w); err != nil {
				return nil, err
			}
		}
		t = pipeline.Filter(t, conds)
	}
	if o.selectCols != "" {
		if t, err = pipeline.Select(t, splitList(o.selectCols)); err != nil {
			return nil, err
		}
	}
	if o.group != "" || o.agg != "" {
		if o.agg == "" {
			o.agg = "count"
		}
		aggs, err := pipeline.ParseAggs(t.Header, o.agg)
		if err != nil {
			return nil, err
		}
		if t, err = pipeline.Aggregate(t, splitList(o.group), aggs); err != nil {
			return nil, err
		}
	}
	if o.sortKey != "" {
		if t, err = pipeline.Sort(t, o.sortKey); err != nil {
			return nil, err
		}
	}
	return pipeline.Limit(t, o.limit), nil
}

func splitList(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

func isTerminal(f *os.File) bool {
	fi, err := f.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}
