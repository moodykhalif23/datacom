// datacom turns large, messy CSV/JSON/Excel files into clean, readable data.
// It streams, so files bigger than memory are fine.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/moodykhalif23/datacom/pipeline"
)

const usage = `datacom — small, fast data cleaner for big files (CSV, TSV, JSON, NDJSON, Excel, gzip)

usage:
  datacom profile [source]                     summarize a dataset
  datacom clean   [source] [-o out] [flags]    normalize and dedupe
  datacom query   [source] [flags]             select, filter, derive, join, group, sort
  datacom convert [source] -f json|csv|...     change format
  datacom watch   [source] -every 10s [flags]  follow a growing file; report per window
  datacom ui                                   point-and-click version in your browser
  datacom version                              build details for bug reports

source is a file path, an http(s) URL, or stdin when omitted. All commands accept:
  -f  output format: table (default on a terminal), csv, tsv, json, ndjson, md
  -i  input format (default: auto-detect)      -sheet  Excel sheet name or number
  -o  write to file instead of stdout          -q      quiet: no row count on stderr
  -monitor  live progress on stderr and a report of every hiccup at the end
  -log FILE append the same report as one JSON line per run (errors included)

clean / query / watch flags:
  -snake        snake_case headers           -no-types   skip type normalization
  -dedupe       drop duplicate rows          -keep-empty keep all-empty rows
  -join FILE    left join another file       -on KEY     join key, or "left=right"
  -derive "name = EXPR"   add or overwrite a column; repeatable, e.g. "margin = revenue - cost",
                "band = if(amount > 1000, high, low)", "yr = year(date)", "status = lower(status)"
  -where EXPR   keep rows where EXPR is true; repeatable, ANDed, e.g. "amount > 1000",
                "name ~ ltd", "email =~ @gov\.ke$", "phone not null", "a > 1 and b == x"
  -select a,b   keep columns (old:new renames)
  -group a,b    group by columns
  -agg SPEC     count, sum(x), avg(x), min(x), max(x), distinct(x), first(x):alias
  -sort col     sort by column; "-col" for descending (buffers rows)
  -limit n      stop after n rows
  -every 10s    watch: window length (watch only)

profile flags:
  -top n        values to list per column (5)   -json   machine-readable report

examples:
  datacom profile sales.xlsx -sheet 2
  datacom clean raw.csv -snake -dedupe -o clean.csv
  datacom query sales.csv -join branches.csv -on branch_id -derive "net = amount * 0.84" \
                -where "net > 1000" -group region -agg "count,sum(net)" -sort -sum_net
  datacom watch events.ndjson -every 1m -group status -agg count
`

type opts struct {
	in, out, format, inFormat, sheet string
	logFile                          string
	monitor                          bool
	quiet, snake, dedupe             bool
	noTypes, keepEmpty               bool
	selectCols, group, agg, sortKey  string
	join, on                         string
	limit, top                       int
	jsonReport                       bool
	every                            time.Duration
	where, derive                    multi

	// Set by the browser UI, which polls the monitor for progress and
	// shows the final report instead of printing it.
	mon      *pipeline.Monitor
	onReport func(pipeline.RunReport)
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
	if cmd == "version" {
		fmt.Println(versionString())
		return
	}
	if cmd == "ui" {
		fs := flag.NewFlagSet(cmd, flag.ExitOnError)
		fs.Usage = func() { fmt.Fprint(os.Stderr, usage) }
		addr := fs.String("addr", "127.0.0.1:"+uiPort, "")
		noOpen := fs.Bool("no-open", false, "")
		fs.Parse(os.Args[2:])
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		if err := serveUI(ctx, *addr, !*noOpen); err != nil {
			fmt.Fprintln(os.Stderr, "datacom:", err)
			os.Exit(1)
		}
		return
	}
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	fs.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	var o opts
	fs.StringVar(&o.out, "o", "", "")
	fs.StringVar(&o.format, "f", "", "")
	fs.StringVar(&o.inFormat, "i", "auto", "")
	fs.StringVar(&o.sheet, "sheet", "", "")
	fs.BoolVar(&o.quiet, "q", false, "")
	fs.BoolVar(&o.snake, "snake", false, "")
	fs.BoolVar(&o.dedupe, "dedupe", false, "")
	fs.BoolVar(&o.noTypes, "no-types", false, "")
	fs.BoolVar(&o.keepEmpty, "keep-empty", false, "")
	fs.StringVar(&o.join, "join", "", "")
	fs.StringVar(&o.on, "on", "", "")
	fs.Var(&o.derive, "derive", "")
	fs.Var(&o.where, "where", "")
	fs.StringVar(&o.selectCols, "select", "", "")
	fs.StringVar(&o.group, "group", "", "")
	fs.StringVar(&o.agg, "agg", "", "")
	fs.StringVar(&o.sortKey, "sort", "", "")
	fs.IntVar(&o.limit, "limit", 0, "")
	fs.IntVar(&o.top, "top", 5, "")
	fs.BoolVar(&o.jsonReport, "json", false, "")
	fs.BoolVar(&o.monitor, "monitor", false, "")
	fs.StringVar(&o.logFile, "log", "", "")
	fs.DurationVar(&o.every, "every", 10*time.Second, "")

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
	signal.Ignore(syscall.SIGPIPE)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cmd, o); err != nil {
		if errors.Is(err, syscall.EPIPE) {
			return // the reader has gone away
		}
		fmt.Fprintln(os.Stderr, "datacom:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cmd string, o opts) error {
	switch cmd {
	case "profile", "clean", "query", "convert", "watch":
	default:
		return fmt.Errorf("unknown command %q (try: profile, clean, query, convert, watch)", cmd)
	}
	start := time.Now()
	mon := o.mon
	if mon == nil && (o.monitor || o.logFile != "") {
		mon = pipeline.NewMonitor()
	}
	if mon != nil {
		ctx = pipeline.WithMonitor(ctx, mon)
	}
	t, closeIn, err := open(ctx, o, mon, o.in, cmd == "watch")
	if err != nil {
		return err
	}
	defer closeIn()
	if mon != nil {
		stop := make(chan struct{})
		if o.monitor {
			go mon.Live(os.Stderr, time.Second, isTerminal(os.Stderr), stop)
		}
		defer func() { close(stop); finish(mon, o, start) }()
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
		var ok bool
		switch {
		case o.out != "":
			outFmt, ok = pipeline.FormatFromName(o.out)
		case cmd == "watch" || isTerminal(os.Stdout) && cmd != "convert":
			outFmt, ok = pipeline.FormatTable, true
		}
		if !ok {
			outFmt = pipeline.FormatCSV
		}
	}

	if cmd == "profile" {
		var schema *pipeline.Schema
		t, schema = pipeline.Clean(t, pipeline.CleanOptions{SnakeHeaders: o.snake, DropEmpty: true, Types: true})
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
		var done func() error
		if t, done, err = shape(ctx, t, o); err != nil {
			return err
		}
		defer done()
	}
	if cmd == "watch" {
		return watch(t, o, out, outFmt)
	}
	if t, err = report(t, o); err != nil {
		return err
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

// open reads a source into a raw table (a followed file for watch).
func open(ctx context.Context, o opts, mon *pipeline.Monitor, src string, follow bool) (*pipeline.Table, func() error, error) {
	if follow {
		r, err := pipeline.Follow(ctx, src)
		if err != nil {
			return nil, nil, err
		}
		f, _ := pipeline.FormatFromName(src)
		if f == "" {
			f = pipeline.FormatNDJSON
		}
		return pipeline.Read(ctx, meter(mon, r), f), func() error { return nil }, nil
	}
	r, f, closeFn, err := pipeline.Open(ctx, src, pipeline.Format(o.inFormat))
	if err != nil {
		return nil, nil, err
	}
	if f != pipeline.FormatXLSX && f != pipeline.FormatParquet {
		r = meter(mon, r) // random-access formats bypass the byte meter
	}
	if f == pipeline.FormatXLSX {
		return pipeline.ReadXLSX(ctx, r, o.sheet), closeFn, nil
	}
	return pipeline.Read(ctx, r, f), closeFn, nil
}

// shape applies the streaming stages: clean, join, derive, where, select.
// The returned func releases the joined file once the pipeline is done.
func shape(ctx context.Context, t *pipeline.Table, o opts) (*pipeline.Table, func() error, error) {
	clean := pipeline.CleanOptions{SnakeHeaders: o.snake, Dedupe: o.dedupe, DropEmpty: !o.keepEmpty, Types: !o.noTypes}
	t, _ = pipeline.Clean(t, clean)
	done := func() error { return nil }
	if o.join != "" {
		// The lookup side is not metered: its rows are not "rows in".
		right, closeRight, err := open(pipeline.WithMonitor(ctx, nil), o, nil, o.join, false)
		if err != nil {
			return nil, nil, err
		}
		done = closeRight
		right, _ = pipeline.Clean(right, clean)
		if t, err = pipeline.Join(t, right, o.on); err != nil {
			return nil, nil, err
		}
	}
	var err error
	if t, err = pipeline.Derive(t, o.derive); err != nil {
		return nil, nil, err
	}
	if len(o.where) > 0 {
		conds := make([]pipeline.Expr, len(o.where))
		for i, w := range o.where {
			if conds[i], err = pipeline.Compile(t.Header, w); err != nil {
				return nil, nil, err
			}
		}
		t = pipeline.Filter(t, conds)
	}
	t, err = pipeline.Select(t, splitList(o.selectCols))
	return t, done, err
}

// report applies the stages that need a bounded input: group, sort, limit.
func report(t *pipeline.Table, o opts) (*pipeline.Table, error) {
	var err error
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

// watch prints one report per time window until the input ends or Ctrl-C.
// Grouped reports also show each value's change since the previous window.
func watch(t *pipeline.Table, o opts, out io.Writer, f pipeline.Format) error {
	var prev map[string]pipeline.Row
	for w := range pipeline.Windows(t, o.every) {
		w, err := report(w, o)
		if err != nil {
			return err
		}
		if o.group != "" || o.agg != "" {
			w, prev = pipeline.Trend(w, len(splitList(o.group)), prev)
		}
		fmt.Fprintf(out, "\n%s  (last %s)\n", time.Now().Format("2006-01-02 15:04:05"), o.every)
		if _, err := pipeline.Write(out, w, f); err != nil {
			return err
		}
	}
	return t.Err()
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

// version is set by -ldflags "-X main.version=v1.2.3"; otherwise the module
// version from go install, or "dev".
var version = "dev"

func versionString() string {
	v := version
	if bi, ok := debug.ReadBuildInfo(); ok && v == "dev" && bi.Main.Version != "" && bi.Main.Version != "(devel)" {
		v = bi.Main.Version
	}
	return fmt.Sprintf("datacom %s (%s, %s/%s)", v, runtime.Version(), runtime.GOOS, runtime.GOARCH)
}

func meter(mon *pipeline.Monitor, r io.Reader) io.Reader {
	if mon == nil {
		return r
	}
	return mon.Meter(r)
}

// finish prints the monitor report and appends it to the JSON log, stamped
// with when it ran, which build ran it and the exact arguments.
func finish(mon *pipeline.Monitor, o opts, start time.Time) {
	rep := mon.Report()
	rep.Time, rep.Version, rep.Command = start.Format(time.RFC3339), versionString(), os.Args[1:]
	if o.onReport != nil {
		o.onReport(rep)
	}
	if o.monitor {
		rep.WriteText(os.Stderr)
	}
	if o.logFile != "" {
		if f, err := os.OpenFile(o.logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644); err == nil {
			rep.WriteJSON(f)
			f.Close()
		} else {
			fmt.Fprintln(os.Stderr, "datacom: log:", err)
		}
	}
}
