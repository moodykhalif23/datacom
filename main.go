// datacom turns large, messy CSV/JSON files into clean, readable data.
// It streams: files bigger than memory are fine.
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/sozuri/datacom/pipeline"
)

const usage = `datacom — small, fast data cleaner for big files (CSV, TSV, JSON, NDJSON, gzip)

usage:
  datacom profile [file]                     summarize a dataset
  datacom clean   [file] [-o out] [flags]    normalize and dedupe
  datacom query   [file] [flags]             select, filter, group, sort
  datacom convert [file] -f json|csv|...     change format

input is stdin when no file is given. all commands accept:
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
  cat events.ndjson | datacom convert -f csv > events.csv
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

	// Allow "datacom clean file.csv -flags" and "datacom clean -flags file.csv".
	args := os.Args[2:]
	var positional []string
	for len(args) > 0 {
		if !strings.HasPrefix(args[0], "-") {
			positional = append(positional, args[0])
			args = args[1:]
			continue
		}
		if err := fs.Parse(args); err != nil {
			os.Exit(2)
		}
		args = fs.Args()
	}
	if len(positional) > 0 {
		o.in = positional[0]
	}
	if err := run(cmd, o); err != nil {
		fmt.Fprintln(os.Stderr, "datacom:", err)
		os.Exit(1)
	}
}

func run(cmd string, o opts) error {
	switch cmd {
	case "profile", "clean", "query", "convert":
	default:
		return fmt.Errorf("unknown command %q (try: profile, clean, query, convert)", cmd)
	}
	start := time.Now()
	r, inFmt, closeIn, err := pipeline.Open(o.in, pipeline.Format(o.inFormat))
	if err != nil {
		return err
	}
	defer closeIn()
	t := pipeline.Read(r, inFmt)
	if t.Err != nil && *t.Err != nil {
		return *t.Err
	}

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
		switch {
		case o.out != "":
			outFmt = guessFromName(o.out, inFmt)
		case isTerminal(os.Stdout) && cmd != "convert":
			outFmt = pipeline.FormatTable
		default:
			outFmt = inFmt
			if outFmt == pipeline.FormatJSON {
				outFmt = pipeline.FormatNDJSON
			}
		}
	}

	if cmd == "profile" {
		t, schema := pipeline.Clean(t, pipeline.CleanOptions{Types: true, DropEmpty: true})
		rep := pipeline.Profile(t, schema.Kinds, o.top, 0)
		if t.Err != nil && *t.Err != nil {
			return *t.Err
		}
		if o.jsonReport {
			return rep.WriteJSON(out)
		}
		rep.WriteText(out)
		return nil
	}

	if cmd != "convert" {
		t, _ = pipeline.Clean(t, pipeline.CleanOptions{
			SnakeHeaders: o.snake,
			Dedupe:       o.dedupe,
			DropEmpty:    !o.keepEmpty,
			Types:        !o.noTypes,
		})
	}
	if cmd == "query" || cmd == "clean" {
		if len(o.where) > 0 {
			conds := make([]*pipeline.Cond, 0, len(o.where))
			for _, w := range o.where {
				c, err := pipeline.ParseCond(t.Header, w)
				if err != nil {
					return err
				}
				conds = append(conds, c)
			}
			t = pipeline.Filter(t, conds)
		}
		if o.selectCols != "" {
			if t, err = pipeline.Select(t, splitList(o.selectCols)); err != nil {
				return err
			}
		}
		if o.group != "" || o.agg != "" {
			if o.agg == "" {
				o.agg = "count"
			}
			aggs, err := pipeline.ParseAggs(t.Header, o.agg)
			if err != nil {
				return err
			}
			if t, err = pipeline.Aggregate(t, splitList(o.group), aggs); err != nil {
				return err
			}
		}
		if o.sortKey != "" {
			if t, err = pipeline.Sort(t, o.sortKey); err != nil {
				return err
			}
		}
		t = pipeline.Limit(t, o.limit)
	}
	n, err := pipeline.Write(out, t, outFmt)
	if err != nil {
		return err
	}
	if !o.quiet {
		fmt.Fprintf(os.Stderr, "%d rows in %s\n", n, time.Since(start).Round(time.Millisecond))
	}
	return nil
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

func guessFromName(name string, fallback pipeline.Format) pipeline.Format {
	n := strings.ToLower(strings.TrimSuffix(name, ".gz"))
	switch {
	case strings.HasSuffix(n, ".csv"):
		return pipeline.FormatCSV
	case strings.HasSuffix(n, ".tsv"):
		return pipeline.FormatTSV
	case strings.HasSuffix(n, ".json"):
		return pipeline.FormatJSON
	case strings.HasSuffix(n, ".ndjson"), strings.HasSuffix(n, ".jsonl"):
		return pipeline.FormatNDJSON
	case strings.HasSuffix(n, ".md"):
		return pipeline.FormatMarkdown
	}
	if fallback == pipeline.FormatJSON {
		return pipeline.FormatNDJSON
	}
	return fallback
}

func isTerminal(f *os.File) bool {
	fi, err := f.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}
