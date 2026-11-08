# Changelog

## Unreleased

- `datacom ui` opens a browser interface for people who do not use a terminal: drop a file in, choose Summarize, Clean, Pick-filter-total, Convert or Watch, set options built from the file's own columns, and download the result. It is served by the same binary from the standard library, runs offline on Windows, macOS and Linux, and drives the same `run` path as the command line.
- Faster: `-sort` parses each key once instead of on every comparison, and plain `YYYY-MM-DD` dates skip the layout search in type normalization. `clean` on 1M messy rows went from 5.8 s to 2.6 s on the benchmark machine; canonical numbers and null checks take fast paths too.
- `tools/bench.sh` benchmarks datacom against Miller, csvtk, qsv, xsv, DuckDB and csvkit on the same generated data; `docs/benchmarks.md` has the results and how to reproduce them.
- `-sort` keeps empty cells last in descending order too; they used to come first when the key had a leading `-`.
- A reader that stops early (`datacom query big.csv | head`) no longer kills the process with SIGPIPE: the run ends quietly with the `-monitor` / `-log` report written, and the log line records the broken pipe.
- `clean` and `query` keep phone numbers and zero-padded IDs as text (`+254712345678`, `0712345678`, anything over 15 digits) instead of converting them to numbers. A stray one in a numeric column is kept as written and reported by `-monitor`.
- `profile` says how many rows its duplicate scan covered when a file is longer than the one-million-row cap: in the header line, the issues list and the JSON field `rows_checked_for_duplicates`.
- `-derive` overwrites a column when the name already exists, so `-derive "status = lower(status)"` folds casing in place. The casing warning in `profile` shows that command.
- Dependencies: brotli and klauspost/compress bumped past GO-2026-6888 and GO-2026-6865, found by govulncheck.
- CI runs gofmt, vet, the race tests, fuzz smoke tests and govulncheck on every push. Pushing a `v*` tag builds the five platform binaries, attests their provenance and publishes a GitHub release with `SHA256SUMS`.
- New docs: `docs/normalization.md`, every change `clean` makes and how to turn it off, and `docs/operations.md`, verified installs, logging and reconciliation for audited environments.

## v0.1.0-beta.1 

First beta. One static binary that turns large, messy data into clean, readable data, streaming so files bigger than memory are fine.

- Reads and writes CSV, TSV, JSON, NDJSON, Excel (`.xlsx`) and Parquet, plain or gzip, from files, stdin or http(s) URLs. Downloads that stall or drop resume where they left off.
- `profile` summarises any dataset: types, missing values, distinct counts, min, max, mean, top values and quality issues.
- `clean` normalises headers, whitespace, null markers, numbers such as `"1,250.00"`, `KES 3400` and `(500)`, mixed date formats and booleans, and drops empty and duplicate rows. Column types are inferred by majority; strays are kept and reported.
- `query` filters, derives columns, joins, selects, groups, aggregates, sorts and limits, driven by one small expression language.
- `watch` follows a growing file or stream and prints a report per time window with the change since the previous window.
- `-monitor` shows live progress and ends with a report of every hiccup; `-log` appends the same report as JSON per run.
- Every stage shares one context: an error, Ctrl-C, a closed pipe or `-limit` stops all goroutines. Verified on 10 million rows under a 64 MiB Go memory limit, with fuzzing and the race detector.

Known limits: Parquet and Excel from a pipe or URL are spooled to a temporary file; `-dedupe` and `-sort` grow with the data; distinct counts in `profile` are estimates above 100,000 values.
