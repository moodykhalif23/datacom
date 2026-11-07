# Changelog

## v0.1.0-beta.1 — 2026-11-07

First beta. One static binary that turns large, messy data into clean, readable data, streaming so files bigger than memory are fine.

- Reads and writes CSV, TSV, JSON, NDJSON, Excel (`.xlsx`) and Parquet, plain or gzip, from files, stdin or http(s) URLs. Downloads that stall or drop resume where they left off.
- `profile` summarises any dataset: types, missing values, distinct counts, min, max, mean, top values and quality issues.
- `clean` normalises headers, whitespace, null markers, numbers such as `"1,250.00"`, `KES 3400` and `(500)`, mixed date formats and booleans, and drops empty and duplicate rows. Column types are inferred by majority; strays are kept and reported.
- `query` filters, derives columns, joins, selects, groups, aggregates, sorts and limits, driven by one small expression language.
- `watch` follows a growing file or stream and prints a report per time window with the change since the previous window.
- `-monitor` shows live progress and ends with a report of every hiccup; `-log` appends the same report as JSON per run.
- Every stage shares one context: an error, Ctrl-C, a closed pipe or `-limit` stops all goroutines. Verified on 10 million rows under a 64 MiB Go memory limit, with fuzzing and the race detector.

Known limits: Parquet and Excel from a pipe or URL are spooled to a temporary file; `-dedupe` and `-sort` grow with the data; distinct counts in `profile` are estimates above 100,000 values.
