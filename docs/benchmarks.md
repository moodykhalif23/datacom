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

<!-- RESULTS -->

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

Put the other tools' binaries on PATH or in `.bench/tools/`. Results append to
`.bench/results.tsv` with one line per tool and operation: median and best wall
time, CPU time, peak RSS, exit code and the first line of stderr on failure.
