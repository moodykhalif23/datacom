# Benchmarks

How datacom compares with the other command-line tools people reach for when a
CSV is too big or too messy for a spreadsheet. Everything here is reproducible
with `tools/bench.sh`; the numbers below are from one machine and will differ
on yours, but the ratios should hold.

## The tools

| tool | language | what it is | install |
|---|---|---|---|
| datacom | Go, standard library | clean, profile, query and watch messy data, streaming | `go install github.com/moodykhalif23/datacom@latest` |
| [qsv](https://github.com/dathere/qsv) | Rust | 50+ CSV commands, SQL via Polars, very fast | release binary |
| [Miller](https://miller.readthedocs.io/) (`mlr`) | Go | name-indexed record processing with its own DSL | `go install github.com/johnkerl/miller/v6/cmd/mlr@latest` |
| [csvtk](https://bioinf.shenwei.me/csvtk/) | Go | CSV/TSV toolkit from bioinformatics | `go install github.com/shenwei356/csvtk/csvtk@latest` |
| [DuckDB](https://duckdb.org/) | C++ | in-process analytical SQL database with a CSV reader | release binary |
| [csvkit](https://csvkit.readthedocs.io/) | Python | the classic CSV command set | `pipx install csvkit` |
| [xsv](https://github.com/BurntSushi/xsv) | Rust | the original fast CSV toolkit, unmaintained since 2018 | release binary |

## The data

`tools/gendata` writes realistic, deliberately messy transaction data: a UTF-8
byte order mark, headers like `Amount (KES)`, amounts written as `"1,250.00"`,
`KES 3400`, `Ksh 1200` and `(500.00)`, dates as `2024-03-03`, `03/03/2024`,
`3 Mar 2024` and `2024-03-03T09:30:00Z`, null markers `NULL`, `n/a`, `none`
and `-`, whitespace-padded names, mixed-case statuses, duplicate rows, and
notes with commas, quotes and embedded newlines. About 12% of cells are messy
and 2% of rows are duplicates.

Two files take part:

- **messy**: the generator's output as is. Used for the parsing test (count the
  rows) and the cleaning test.
- **clean**: the messy file after `datacom clean -snake`, with status and
  channel lower-cased. Every per-operation test reads this file so all tools
  do the same work on the same typed columns.

## The operations

| op | what it does |
|---|---|
| count-messy | count the rows of the messy file: a parsing test (BOM, quoted newlines, mixed cells) |
| clean-messy | whitespace, null markers, numbers, dates, booleans, empty rows, duplicates, on the messy file |
| profile | per-column type, nulls, distinct values, min, max, mean |
| count | count the rows of the clean file |
| dedupe | drop exact duplicate rows, keeping the first |
| filter | `amount_kes > 5000 and status == paid` |
| group | group by county and channel with count, sum and mean of amount |
| sort | sort by amount descending |
| tojson | convert to newline-delimited JSON |
| freq | count the values of the status column |

The exact command line for every tool and operation is in `tools/bench.sh`.
Each command runs three times with the file in page cache; the table reports
the median wall time and the peak resident memory. datacom runs with
`-no-types` on the clean file, since the columns are already normalized and
the other tools do no normalization either; the cost of leaving normalization
on is reported separately.

Fairness notes, from each tool's documentation:

- qsv's `count` uses the multithreaded Polars reader; `stats` is run with
  `--cardinality --infer-dates` and the stats cache off; `tojsonl` does a
  stats pass first to infer JSON types; `extdedup -s 1-` is the streaming,
  order-preserving dedupe. No `.idx` index files exist during the run.
- DuckDB uses all cores by default and `COPY ... TO '/dev/stdout'` for full
  result sets. `SELECT DISTINCT` does not preserve input order.
- csvkit loads whole files into memory for stats, sort and SQL; it runs once,
  and only on the 1M-row set.
- xsv has no dedupe, filter-by-expression, group-by or JSON output.

## Results

Measured on an 8-core Intel i5-10210U laptop, 31 GB RAM, Linux 6.12, files in page cache.
Median of three runs (csvkit: one run). Peak resident memory in brackets.

### 1M rows (113 MB messy, 111 MB clean)

| operation | datacom | datacom, normalization on | csvkit 2.2.0 |
|---|---:|---:|---:|
| count-messy | 1.1 s (20 MB) |  | 4 s (796 MB) |
| clean-messy | 2.1 s (73 MB) |  | n/a |
| profile | 2.6 s (128 MB) |  | 311 s (1331 MB) |
| count | 1.1 s (21 MB) | 1.5 s (20 MB) | 3 s (782 MB) |
| dedupe | 1.7 s (73 MB) |  | n/a |
| filter | 1.1 s (21 MB) | 1.6 s (20 MB) | 296 s (2511 MB) |
| group | 1.5 s (22 MB) | 1.6 s (22 MB) | 276 s (2511 MB) |
| sort | 3.9 s (411 MB) |  | 268 s (1282 MB) |
| tojson | 2.5 s (20 MB) |  | 15 s (21 MB) |
| freq | 1.2 s (20 MB) |  | 240 s (1259 MB) |

### 10M rows (1.2 GB messy, 1.2 GB clean)

| operation | datacom | datacom, normalization on |
|---|---:|---:|
| count-messy | 13.8 s (20 MB) |  |
| clean-messy | 16.7 s (643 MB) |  |
| profile | 15.8 s (149 MB) |  |
| count | 9.7 s (20 MB) | 12.8 s (20 MB) |
| dedupe | 13.1 s (650 MB) |  |
| filter | 11.2 s (21 MB) | 12.8 s (21 MB) |
| group | 12.3 s (22 MB) | 13.7 s (22 MB) |
| sort | 37.8 s (3922 MB) |  |
| tojson | 23.9 s (20 MB) |  |
| freq | 12.1 s (20 MB) |  |

Reading the table:

- datacom's memory is flat at about 20 MB for everything that streams. `-dedupe`
  holds one 8-byte hash per distinct row (650 MB at 10M rows), `profile` holds
  its sketches and top values, and `-sort` holds every row (3.9 GB at 10M).
- Type normalization on an already-clean file costs 10 to 40 percent on top of
  parsing; `clean` on the messy file, which is the real job, runs at 55 to
  70 MB/s.
- csvkit's streaming commands (count, the `-I` form of `csvjson`) are 3 to 6
  times slower than datacom; everything that loads the file into memory is 70
  to 270 times slower and needs 1.3 to 2.5 GB.
- Parsing throughput is datacom's weak point: 100 to 120 MB/s with Go's CSV
  reader, where qsv and DuckDB manage several hundred on the same hardware.

qsv, Miller, csvtk, DuckDB and xsv have not been measured on this machine
yet; their columns appear when `tools/bench.sh` finds them on PATH. The
numbers that matter most for the comparison are qsv's `count`, `stats` and
`extdedup`, and DuckDB's `SUMMARIZE` and `GROUP BY`, which are expected to
beat datacom on wall time by using every core.


## What "clean" means to each tool

datacom's `clean` is one command. This is what it takes to get the same result
elsewhere, from each tool's documentation:

| change | datacom | qsv | Miller | csvtk | DuckDB | csvkit |
|---|---|---|---|---|---|---|
| strip BOM, normalize headers | `-snake` | `input --trim-headers` + `rename` | `rename -r` | `rename` | SQL aliases | no |
| trim and collapse whitespace | yes | `apply operations trim,squeeze` | `clean-whitespace` | two `replace` passes | `regexp_replace` | no |
| null markers to empty | yes | `replace` with a regex | `put` DSL | `replace` | `nullstr=[...]` | partial, via type inference |
| `"1,250.00"`, `KES 3400` to numbers | yes | `apply operations currencytonum` | `put` DSL with `gsub` | `replace` regex per column, stays text | `regexp_replace` + `try_cast` | no |
| `(500.00)` to -500 | yes | no, sign is lost | `put` DSL | `replace` regex | `CASE WHEN ... LIKE '(%)'` | no |
| mixed date formats to ISO | yes | `datefmt --prefer-dmy` | `put` DSL with `strptime` | `fmtdate` | `try_strptime` with a list | one format only |
| booleans to true/false | yes | no | `put` DSL | `replace` regex | `CASE` | no |
| drop all-empty rows | yes | `luau filter` script | `skip-trivial-records` | `-E` flag | `WHERE coalesce(...)` | no |
| drop duplicate rows | `-dedupe` | `extdedup -s 1-` | `uniq -a` | `uniq -F -f '*'` | `SELECT DISTINCT` | no |
| report what was changed and what was left | `-monitor`, `-log` | no | no | no | no | no |

csvkit's `csvclean` repairs structure only (row lengths, empty columns); it does
not touch cell values. Miller can do everything in one process with its DSL,
which is a small program per dataset. csvtk needs a pipeline of nine processes
that re-parse the CSV at every step. qsv gets closest with built-in commands,
in a pipeline of eight, and still loses the sign of accounting negatives.

## Reproducing

```sh
tools/bench.sh                      # 1M rows into ./.bench, every tool found on PATH
tools/bench.sh .bench 10000000      # 10M rows (csvkit is skipped above 1M)
TOOLS="datacom qsv" tools/bench.sh  # a subset
REPS=5 tools/bench.sh               # more repetitions
```

Put the other tools' binaries on PATH or in `.bench/tools/`. The Go tools come
from `go install`; the release binaries can be dropped straight into `.bench/tools/`:

```sh
mkdir -p .bench/tools && cd .bench/tools
curl -sSLO https://github.com/dathere/qsv/releases/download/28.0.0/qsv-28.0.0-x86_64-unknown-linux-gnu.zip && unzip -o qsv-*.zip qsv && rm qsv-*.zip
curl -sSL https://github.com/duckdb/duckdb/releases/download/v2.0.0/duckdb-cli-linux-amd64.tar.gz | tar -xz duckdb
curl -sSL https://github.com/BurntSushi/xsv/releases/download/0.13.0/xsv-0.13.0-x86_64-unknown-linux-musl.tar.gz | tar -xz xsv
go install github.com/johnkerl/miller/v6/cmd/mlr@latest && go install github.com/shenwei356/csvtk/csvtk@latest
```

Not on Linux x86_64? Pick the matching archive from each project's releases
page instead.

Results append to `.bench/results.tsv` with one line per tool and operation:
median and best wall time, CPU time, peak RSS, exit code and the first line of
stderr on failure.
