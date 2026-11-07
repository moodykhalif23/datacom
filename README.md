# datacom

A small, fast, dependency-free Go tool that turns large, messy data into clean, readable data.
Built for finance teams, large organisations and public bodies that receive big CSV / JSON exports and need answers, not spreadsheets that crash.

- **Streams everything.** Files larger than RAM are fine. Only `-sort` and the `table` / `md` views buffer rows; `-dedupe` keeps one 8-byte hash per distinct row.
- **Reads** CSV, TSV, JSON arrays, NDJSON, plain or gzip, from a file or stdin. Format is auto-detected.
- **Cleans** headers, whitespace, `NULL` / `N/A` / `-` markers, numbers like `"1,250.00"`, `$3,400`, `KES 3400`, `(500)`, mixed date formats, `Yes/No/Y/N` booleans, duplicate and empty rows.
- **Answers questions** with select, filter, group-by, aggregates and sort.
- **Profiles** any dataset in one readable report: types, missing values, distinct counts, min / max / mean, top values, and data-quality issues.
- **Zero dependencies.** Standard library only. One static binary. Also usable as a Go library.

## Install

```sh
go install github.com/sozuri/datacom@latest
# or
git clone https://github.com/sozuri/datacom && cd datacom && go build -o datacom .
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

| Expression | Meaning |
| --- | --- |
| `amount > 1000` | numeric compare: `== != > >= < <=` |
| `region == Nairobi` | text compare, case-insensitive |
| `name ~ ltd` | contains |
| `email =~ @.*\.go\.ke$` | regex (`!~` negates) |
| `date >= 2025-01-01` | ISO dates compare as text after cleaning |
| `phone not null` / `phone is null` | missing-value checks |

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
import "github.com/sozuri/datacom/pipeline"

r, format, closeFn, _ := pipeline.Open("big.csv.gz", pipeline.FormatAuto)
defer closeFn()
t := pipeline.Read(r, format)
t, schema := pipeline.Clean(t, pipeline.CleanOptions{SnakeHeaders: true, Dedupe: true, Types: true})
cond, _ := pipeline.ParseCond(t.Header, "amount > 1000")
t = pipeline.Filter(t, []*pipeline.Cond{cond})
pipeline.Write(os.Stdout, t, pipeline.FormatNDJSON)
```

Every stage is a goroutine connected by a channel, so a pipeline of any length still uses constant memory per row.

## Roadmap

- Parquet and Excel input
- Derived columns (`-derive "margin = revenue - cost"`)
- Join two files on a key
- Watch mode for growing files and realtime tails

## License

MIT

## Performance

3 million rows, 161 MB CSV with messy amounts (`"1,250.00"`, `KES 3400.00`), one core, laptop:

| command | time | peak memory |
| --- | --- | --- |
| `query` group-by + sum/avg | 11 s | 13 MB |
| `convert` CSV → NDJSON | 15 s | 12 MB |
| `clean -dedupe` → CSV | 12 s | 161 MB |
| `profile` | 16 s | 124 MB |
