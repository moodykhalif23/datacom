# datacom

A small, fast, dependency-free Go tool that turns large, messy data into clean, readable data.

- **Streams everything.** Files larger than RAM are fine. Only `-sort` and the `table` / `md` views buffer rows; `-dedupe` keeps one 8-byte hash per distinct row.
- **Reads** CSV, TSV, JSON arrays, NDJSON and Excel (`.xlsx`, one sheet at a time), plain or gzip, from a file, stdin or an http(s) URL. Format is auto-detected.
- **Survives bad networks.** A URL download that stalls or drops is retried with backoff and resumed from the last byte, so a slow or flaky connection still yields one correct stream.
- **Cannot leak.** Every stage shares one context: an error, Ctrl-C, a closed pipe or `-limit` stops all goroutines, and memory per row stays constant.
- **Cleans** headers, whitespace, `NULL` / `N/A` / `-` markers, numbers like `"1,250.00"`, `$3,400`, `KES 3400`, `(500)`, mixed date formats, `Yes/No/Y/N` booleans, duplicate and empty rows.
- **Answers questions** with filters, derived columns, joins, select, group-by, aggregates and sort, all driven by one small expression language.
- **Watches live data.** `watch` follows a growing file or stream and prints a fresh report every window, so you can see trends as they happen.
- **Profiles** any dataset in one readable report: types, missing values, distinct counts, min / max / mean, top values, and data-quality issues.
- **Zero dependencies.** Standard library only. One static binary. Also usable as a Go library.

## Install

```sh
go install github.com/moodykhalif23/datacom@latest
export PATH="$PATH:$(go env GOPATH)/bin"   # where go install puts binaries
# or
git clone https://github.com/moodykhalif23/datacom && cd datacom && go build -o datacom .
go build -ldflags="-s -w" -o datacom .     # smaller binary: symbols stripped
```

## Usage

```sh
datacom profile sales.csv.gz                 # what is in this file?
datacom profile report.xlsx -sheet 2         # Excel, by sheet name or number
datacom clean   raw.csv -snake -dedupe -o clean.csv
datacom query   sales.csv -where "amount > 1000" -group region \
                -agg "count,sum(amount),avg(amount):avg" -sort -sum_amount
datacom query   sales.csv -join branches.csv -on branch_id \
                -derive "net = amount * 0.84" -where "net > 1000 and region == Nairobi"
datacom convert events.ndjson -f csv > events.csv
datacom watch   events.ndjson -every 1m -group status -agg "count,sum(amount)"
cat huge.json | datacom query -where "status == failed" -limit 50
```

Output is a table on a terminal and CSV when piped. Choose with `-f table|csv|tsv|json|ndjson|md`.

### Expressions

`-where` keeps rows where the expression is true (repeatable, ANDed). `-derive "name = expression"` adds a column (repeatable; later ones can use earlier ones).

| Expression | Meaning |
| --- | --- |
| `amount > 1000`, `a != b`, `date >= 2025-01-01` | compare: numeric when both sides are numbers, else case-insensitive text |
| `name ~ ltd` | contains |
| `email =~ @.*\.go\.ke$`, `x !~ ^test` | regex, unquoted is fine |
| `phone is null`, `phone not null` | missing-value checks |
| `a > 1 and (b == x or not c)` | boolean logic |
| `price * qty - discount`, `amount % 7` | arithmetic; a non-number yields null |
| `[Total Amount]` | a column whose name has spaces |
| `round(x, 2)`, `abs(x)`, `len(s)`, `lower(s)`, `upper(s)`, `trim(s)` | numbers and text |
| `concat(a, '-', b)`, `coalesce(a, b, 'none')`, `if(cond, yes, no)` | combine |
| `year(d)`, `month(d)`, `day(d)` | date parts |

A bare word that is not a column is taken as text, so `status == paid` works without quotes.

### Join

`-join other.csv -on key` or `-on left_col=right_col`. It is a left join: every left row is kept, right-hand columns are appended, and several matches yield several rows. The right-hand file is held in memory, so put the smaller one there.

### Watch

`datacom watch file -every 10s [-where ...] [-derive ...] [-group ... -agg ...]` follows the file like `tail -f` (restarting if it is truncated or rotated) and prints one report per window. Stdin works too: `kafka-console-consumer ... | datacom watch -every 30s -group topic -agg count`.

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

| command                                        | time                    | peak memory |
| ---------------------------------------------- | ----------------------- | ----------- |
| `convert` CSV to NDJSON or JSON              | 17 s                    | 17 MB       |
| `clean -snake` to CSV                        | 19 s                    | 17 MB       |
| `query` two filters, group-by, sum/avg, sort | 23 s                    | 17 MB       |
| `query -limit 10` (stops upstream early)     | 0.1 s                   | 5 MB        |
| `profile` (plain or gzip input)              | 48 s                    | 81 MB       |
| `clean -dedupe` (one hash per distinct row)  | 109 s                   | 303 MB      |
| throttled stdin pipe at 40 MB/s                | 53 s                    | 3 MB        |
| Ctrl-C                                         | exits 1 within a second |             |
| `\| head -2`                                  | stops in 0.01 s         |             |

Row counts match between `query -agg count` and `convert` output. Fuzz targets cover the CSV reader, JSON reader and value parsers; `go test -race` passes, including a goroutine-leak test and a network test that drops, stalls and 503s a download mid-stream.

Only three things grow with the data: `-dedupe` keeps one 8-byte hash per distinct row, `-group` keeps one accumulator per group, and `-sort` buffers the rows it sorts. Everything else is constant per row.

## Roadmap

- Parquet input is under discussion: a correct reader needs a dependency or thousands of lines, and this tool stays small.
- Output to Excel, and windowed trend lines (change versus the previous window) for `watch`.

## License

MIT
