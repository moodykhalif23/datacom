# datacom

A small, fast, dependency-free Go tool that turns large, messy data into clean, readable data.

- **Streams everything.** Files larger than RAM are fine. Only `-sort` and the `table` / `md` views buffer rows; `-dedupe` keeps one 8-byte hash per distinct row.
- **Reads** CSV, TSV, JSON arrays, NDJSON, plain or gzip, from a file, stdin or an http(s) URL. Format is auto-detected.
- **Survives bad networks.** A URL download that stalls or drops is retried with backoff and resumed from the last byte, so a slow or flaky connection still yields one correct stream.
- **Cannot leak.** Every stage shares one context: an error, Ctrl-C, a closed pipe or `-limit` stops all goroutines, and memory per row stays constant.
- **Cleans** headers, whitespace, `NULL` / `N/A` / `-` markers, numbers like `"1,250.00"`, `$3,400`, `KES 3400`, `(500)`, mixed date formats, `Yes/No/Y/N` booleans, duplicate and empty rows.
- **Answers questions** with select, filter, group-by, aggregates and sort.
- **Profiles** any dataset in one readable report: types, missing values, distinct counts, min / max / mean, top values, and data-quality issues.
- **Zero dependencies.** Standard library only. One static binary. Also usable as a Go library.

## Install

```sh
go install github.com/moodykhalif23/datacom@latest
# or
git clone https://github.com/moodykhalif23/datacom && cd datacom && go build -o datacom .
```

## Usage

```sh
datacom profile sales.csv.gz                 # what is in this file?
datacom clean   raw.csv -snake -dedupe -o clean.csv
datacom query   sales.csv -where "amount > 1000" -group region \
                -agg "count,sum(amount),avg(amount):avg" -sort -sum_amount
datacom convert events.ndjson -f csv > events.csv
cat huge.json | datacom query -where "status == failed" -limit 50
```

Output is a table on a terminal and CSV when piped. Choose with `-f table|csv|tsv|json|ndjson|md`.

### Filters

`-where` is repeatable and the clauses are ANDed.

| Expression                             | Meaning                                  |
| -------------------------------------- | ---------------------------------------- |
| `amount > 1000`                      | numeric compare: `== != > >= < <=`     |
| `region == Nairobi`                  | text compare, case-insensitive           |
| `name ~ ltd`                         | contains                                 |
| `email =~ @.*\.go\.ke$`              | regex (`!~` negates)                   |
| `date >= 2025-01-01`                 | ISO dates compare as text after cleaning |
| `phone not null` / `phone is null` | missing-value checks                     |

### Aggregates

`-agg "count, sum(x), avg(x), min(x), max(x), distinct(x), first(x), last(x)"`. Add `:alias` to name a column.
Memory grows with the number of groups, not the number of rows.

### Profile

```
rows: 8   columns: 7   duplicate rows: 1

amount_kes  (float)
  filled 6   missing 2 (25.0%)   distinct 6
  min -500   max 12000   mean 4166.5   stddev 4697.98
...
issues:
  - region: inconsistent casing for "nairobi"
  - 1 duplicate rows
```

Add `-json` for a machine-readable report.

## As a library

```go
import "github.com/moodykhalif23/datacom/pipeline"

r, format, closeFn, _ := pipeline.Open("big.csv.gz", pipeline.FormatAuto)
defer closeFn()
t := pipeline.Read(r, format)
t, schema := pipeline.Clean(t, pipeline.CleanOptions{SnakeHeaders: true, Dedupe: true, Types: true})
cond, _ := pipeline.ParseCond(t.Header, "amount > 1000")
t = pipeline.Filter(t, []*pipeline.Cond{cond})
pipeline.Write(os.Stdout, t, pipeline.FormatNDJSON)
```

Every stage is a goroutine connected by a channel, so a pipeline of any length still uses constant memory per row.

## Performance and stress testing

`tools/gendata` writes realistic, deliberately messy transaction data: East African names, phones, counties, mixed amount and date formats, assorted null markers, duplicates, and quoted notes with commas and newlines. `tools/stress.sh` generates 10 million rows, runs every command under `GOMEMLIMIT=64MiB`, and checks slow pipes, Ctrl-C and closed pipes:

```sh
tools/stress.sh            # ~1.3 GB of data in ./.stress, results on stdout
```

Results on 10 million rows (1.2 GB, 11 columns, one core, `GOMEMLIMIT=64MiB`):

| command | time | peak memory |
| --- | --- | --- |
| `convert` CSV to NDJSON or JSON | 17 s | 17 MB |
| `clean -snake` to CSV | 19 s | 17 MB |
| `query` two filters, group-by, sum/avg, sort | 23 s | 17 MB |
| `query -limit 10` (stops upstream early) | 0.1 s | 5 MB |
| `profile` (plain or gzip input) | 48 s | 81 MB |
| `clean -dedupe` (one hash per distinct row) | 109 s | 303 MB |
| throttled stdin pipe at 40 MB/s | 53 s | 3 MB |
| Ctrl-C | exits 1 within a second | |
| `\| head -2` | stops in 0.01 s | |

Row counts match between `query -agg count` and `convert` output. Fuzz targets cover the CSV reader, JSON reader and value parsers; `go test -race` passes, including a goroutine-leak test and a network test that drops, stalls and 503s a download mid-stream.

Only three things grow with the data: `-dedupe` keeps one 8-byte hash per distinct row, `-group` keeps one accumulator per group, and `-sort` buffers the rows it sorts. Everything else is constant per row.

## Roadmap

- Parquet and Excel input
- Derived columns (`-derive "margin = revenue - cost"`)
- Join two files on a key
- Watch mode for growing files and realtime tails

## License

MIT

