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

| operation | datacom | datacom, normalization on | qsv 28.0.0 | Miller 6.22.0 | csvtk 0.39.0 | xsv 0.13.0 | DuckDB 2.0.0 | csvkit 2.2.0 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| count-messy | 0.77 s (21 MB) |  | 0.14 s (412 MB) | 0.60 s (127 MB) | 0.82 s (30 MB) | 0.28 s (8 MB) | 0.21 s (229 MB) | 4 s (796 MB) |
| clean-messy | 1.61 s (80 MB) |  | 9.66 s (264 MB) | 176.88 s (2545 MB) | 18.77 s (369 MB) | n/a | n/a | n/a |
| profile | 1.75 s (133 MB) |  | 2.05 s (530 MB) | 8.39 s (1305 MB) | 1.09 s (253 MB) | 4.98 s (764 MB) | 0.61 s (243 MB) | 311 s (1331 MB) |
| count | 0.93 s (21 MB) | 1.04 s (21 MB) | 0.14 s (410 MB) | 0.74 s (132 MB) | 0.89 s (31 MB) | 0.28 s (8 MB) | 0.23 s (230 MB) | 3 s (782 MB) |
| dedupe | 1.41 s (76 MB) |  | 1.28 s (266 MB) | 5.72 s (2122 MB) | 2.07 s (403 MB) | n/a | 0.92 s (633 MB) | n/a |
| filter | 1.12 s (21 MB) | 1.18 s (21 MB) | 0.28 s (498 MB) | 0.98 s (144 MB) | 2.14 s (31 MB) | n/a | 0.45 s (307 MB) | 296 s (2511 MB) |
| group | 1.24 s (23 MB) | 1.31 s (21 MB) | 0.20 s (372 MB) | 1.06 s (530 MB) | 1.73 s (237 MB) | n/a | 0.31 s (217 MB) | 276 s (2511 MB) |
| sort | 3.23 s (420 MB) |  | 1.70 s (426 MB) | 4.34 s (2248 MB) | 2.83 s (412 MB) | 7.49 s (488 MB) | 0.84 s (866 MB) | 268 s (1282 MB) |
| tojson | 2.06 s (21 MB) |  | 1.37 s (410 MB) | 4.46 s (683 MB) | 6.06 s (31 MB) | n/a | 0.72 s (961 MB) | 15 s (21 MB) |
| freq | 0.83 s (21 MB) |  | 0.31 s (75 MB) | 0.86 s (140 MB) | 0.88 s (32 MB) | 0.75 s (14 MB) | 0.21 s (206 MB) | 240 s (1259 MB) |

### 10M rows gzipped, before and after parallel reading (2026-11-10)

The same 303 MB `big.csv.gz` (10M messy rows, 1.2 GB unpacked), timed with
the build before and after parallel CSV parsing, gzip read-ahead and the
parallel clean and profile stages. The machine was shared, with a load of 5
to 7 on its 8 threads, so these are medians of alternating runs. Decoding the
single gzip stream alone takes about 10 s there, which is now the floor.

| command | before | after |
|---|---:|---:|
| `profile -json` | 19.5 s | 12.3 s |
| `clean -snake -dedupe -f csv` | 20.6 s | 13.3 s |
| `clean -snake -f csv`, peak memory | 22 MB | 35 MB |

### 10M rows (1.2 GB messy, 1.2 GB clean), earlier build, datacom only

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

Reading the tables:

- datacom's memory is flat at about 20 MB for everything that streams (about
  35 MB since reading went parallel). `-dedupe`
  holds one 8-byte hash per distinct row, `profile` holds its sketches and top
  values, and `-sort` holds every row.
- Type normalization on an already-clean file costs 5 to 10 percent on top of
  parsing; `clean` on the messy file, which is the real job, runs at 70 MB/s.
- DuckDB is 1.5 to 4 times faster on wall time by using every core; on CPU time
  the gap is 1.4 to 2.9 times. It holds 206 to 961 MB where datacom holds 20,
  and it has no cleaning step, so the messy file first needs a SQL script.
- xsv parses fastest of the single-threaded tools, 404 MB/s against datacom's
  147, but its `stats --everything` takes 5.0 s and 764 MB where `profile` takes
  1.8 s and 133 MB, and its sort is 2.3 times slower. It has no dedupe, filter,
  group-by or JSON output.
- qsv is 1.1 to 7 times faster on 8 of the 9 per-operation tests, holding 75 to
  530 MB. datacom is faster on profile (1.8 s against 2.0 s). Its eight-command
  clean pipeline takes 9.7 s against `clean`'s 1.6 s, re-parsing the file at
  every step, and still loses the sign of `(500.00)`; the table below has the
  details.
- Miller is level with datacom on filter, group, freq; 1.3 to 5 times slower on
  profile, dedupe, sort, tojson; faster on count-messy, count. It holds 127 to
  2248 MB. Its one-program clean takes 177 s against `clean`'s 1.6 s, most of it
  in strptime tried up to three times per row.
- csvtk is level with datacom on count-messy, count, sort, freq; 1.4 to 3 times
  slower on dedupe, filter, group, tojson; faster on profile. It holds 30 to 412
  MB. Its nine-process clean pipeline takes 18.8 s against `clean`'s 1.6 s; its
  profile row covers numeric stats for two columns only, the nearest it offers.
- csvkit's streaming commands (count, the `-I` form of `csvjson`) are several
  times slower than datacom; everything that loads the file into memory is 70 to
  270 times slower and needs 1.3 to 2.5 GB. Measured once, with an earlier build
  of datacom on the same machine.
- Parsing throughput is datacom's weak point: 147 MB/s with Go's CSV reader.


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
