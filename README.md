<p align="center"><img src="datacom.png" alt="datacom" width="640"></p>

<p align="center">A small, fast Go tool that turns large, messy data into clean, readable data.</p>

- **Streams everything.** Files larger than RAM are fine. Only `-sort` and the `table` / `md` views buffer rows; `-dedupe` keeps one 8-byte hash per distinct row.
- **Reads** CSV, TSV, JSON arrays, NDJSON and Excel (`.xlsx`, one sheet at a time), plain or gzip, from a file, stdin or an http(s) URL. Format is auto-detected.
- **Survives bad networks.** A URL download that stalls or drops is retried with backoff and resumed from the last byte, so a slow or flaky connection still yields one correct stream.
- **Cleans** headers, whitespace, `NULL` / `N/A` / `-` markers, numbers like `"1,250.00"`, `$3,400`, `KES 3400`, `(500)`, mixed date formats, `Yes/No/Y/N` booleans, duplicate and empty rows.
- **Answers questions** with filters, derived columns, joins, select, group-by, aggregates and sort, all driven by one small expression language.
- **Watches live data.** `watch` follows a growing file or stream and prints a fresh report every window, so you can see trends as they happen.
- **Profiles** any dataset in one readable report: types, missing values, distinct counts, min / max / mean, top values, and data-quality issues.

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
datacom convert export.csv.gz -o export.parquet       # typed columns, Snappy row groups
datacom query   lake/orders.parquet -where "status == failed" -group region -agg count
datacom watch   events.ndjson -every 1m -group status -agg "count,sum(amount)"
cat huge.json | datacom query -where "status == failed" -limit 50
```

Output is a table on a terminal and CSV when piped. Choose with `-f table|csv|tsv|json|ndjson|md|xlsx|parquet`, or by the extension of `-o out.parquet`.

### Join

`-join other.csv -on key` or `-on left_col=right_col`. It is a left join: every left row is kept, right-hand columns are appended, and several matches yield several rows. The right-hand file is held in memory, so put the smaller one there.

### Watch

`datacom watch file -every 10s [-where ...] [-derive ...] [-group ... -agg ...]` follows the file like `tail -f` (restarting if it is truncated or rotated) and prints one report per window. Stdin works too: `kafka-console-consumer ... | datacom watch -every 30s -group topic -agg count`.

### Parquet

Parquet files are read row group by row group and written as Snappy-compressed row groups of at most 262,144 rows, so memory stays bounded either way. Dates, timestamps (including legacy INT96), decimals and UUIDs come out as text you can read; nested fields get dotted names and repeated values become JSON arrays. Parquet and Excel need random access, so a pipe or URL is first spooled to a temporary file.

### Aggregates

`-agg "count, sum(x), avg(x), min(x), max(x), distinct(x), first(x), last(x)"`. Add `:alias` to name a column.
Memory grows with the number of groups, not the number of rows.


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

| command                                        | time            | peak memory |
| ---------------------------------------------- | --------------- | ----------- |
| `convert` CSV to NDJSON or JSON              | 17 s            | 17 MB       |
| `clean -snake` to CSV                        | 19 s            | 17 MB       |
| `query` two filters, group-by, sum/avg, sort | 23 s            | 17 MB       |
| `query -limit 10` (stops upstream early)     | 0.1 s           | 5 MB        |
| `profile` (plain or gzip input)              | 48 s            | 81 MB       |
| `clean -dedupe` (one hash per distinct row)  | 109 s           | 303 MB      |
| throttled stdin pipe at 40 MB/s                | 53 s            | 3 MB        |
| `\| head -2`                                  | stops in 0.01 s |             |

Parquet, on 2 million of the same rows: CSV to Parquet in 5 s at 56 MB peak (226 MB in, 79 MB out), a grouped query on the Parquet file in 4 s at 31 MB, and Parquet back to CSV in 3 s at 28 MB. Row counts match in both directions.

Row counts match between `query -agg count` and `convert` output. Fuzz targets cover the CSV reader, JSON reader and value parsers; `go test -race` passes, including a goroutine-leak test and a network test that drops, stalls and 503s a download mid-stream.

Only three things grow with the data: `-dedupe` keeps one 8-byte hash per distinct row, `-group` keeps one accumulator per group, and `-sort` buffers the rows it sorts. Everything else is constant per row.

## License

MIT
