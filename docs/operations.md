# Running datacom where the output has to be defended

For teams in government, parastatals, banks and audit, where someone will
later ask what was done to a file and whether it can be shown. datacom
already produces the evidence; this page says how to keep it.

## 1. Install a binary you can prove

Each release on the [releases page](https://github.com/moodykhalif23/datacom/releases)
ships five binaries, `SHA256SUMS`, and a signed build-provenance attestation
that GitHub Actions generated from the tagged commit.

```sh
sha256sum -c SHA256SUMS --ignore-missing
gh attestation verify datacom-v0.1.0-beta.2-linux-amd64 --owner moodykhalif23
```

The first line proves the file is the one that was published. The second
proves GitHub built it from this repository at that tag, so no one's laptop
was in the chain. Building from source at a tag is equally defensible:
`go install github.com/moodykhalif23/datacom@v0.1.0-beta.2`. Pin the version
in scripts, never run an untagged build in production, and note that
`datacom version` prints what is installed and every run log records it.

## 2. Keep the raw input

Never write output over the source. Keep the raw file, its checksum and the
exact command line together with the output:

```sh
sha256sum export.csv.gz > export.csv.gz.sha256
datacom clean export.csv.gz -snake -dedupe -log runs.jsonl -o export.clean.csv
sha256sum export.clean.csv > export.clean.csv.sha256
```

datacom reads its input once, forwards, and never modifies it.

## 3. Log every run

`-log FILE` appends one JSON object per run, including runs that failed.
This is the line for the sample file that ships with the repo, shown
indented here; in the file it is one line:

```json
{
  "time": "2026-11-07T17:20:23+03:00",
  "version": "datacom v0.1.0-beta.1.0.20261107135004-16dc1e62622d+dirty (go1.25.0, linux/amd64)",
  "command": [
    "clean",
    "messy.csv",
    "-snake",
    "-dedupe",
    "-log",
    "runs.jsonl",
    "-q",
    "-o",
    "messy.clean.csv"
  ],
  "seconds": 0.00030902,
  "rows_in": 9,
  "rows_out": 7,
  "bytes_in": 538,
  "peak_mb": 11,
  "rows_empty_dropped": 1,
  "rows_duplicate_dropped": 1
}
```

Fields: `time` (start, RFC 3339), `version`, `command` (the arguments as
given), `seconds`, `rows_in`, `rows_out`, `bytes_in`, `peak_mb`, and when
non-zero `rows_padded`, `rows_trimmed`, `rows_empty_dropped`,
`rows_duplicate_dropped`, `join_unmatched`, `untyped_values` (per column:
kind, count and up to three examples), `notes` and `error`. Keep the log
next to the outputs. Across many files or a scheduled job it answers
"which runs had problems" with one `grep` or `jq`.

## 4. Reconcile the counts

For `clean`: `rows_in` = `rows_out` + `rows_empty_dropped` +
`rows_duplicate_dropped`. For `convert`: `rows_in` = `rows_out`. If either
does not hold, the run was cut short; check `error`. For `query`, compare
`-agg count` on the raw file with the sum of the groups. For a join,
`join_unmatched` is the number of left rows that gained empty columns.

## 5. Decide what typing you accept

Read `untyped_values` before trusting a typed column. Those values were kept
as written, not guessed, but they now sit among converted ones. Fix the
source, filter them out (`-where "phone not null"`), or run with `-no-types`
for a verbatim pass. [normalization.md](normalization.md) lists every
conversion and the switch that turns it off.

## 6. Reproducibility

The same binary, input and flags give byte-identical output. There is no
randomness and no clock in the data, only in `watch` headers. Column types
are inferred from the first 1,000 rows, so an input delivered in a different
order can type a column differently; `untyped_values` in the log shows it.

## 7. Where the data goes

- No network connection is made except to a URL you pass as the source.
- No telemetry and no update checks.
- Parquet and Excel read from a pipe or URL are spooled to a temporary file
  in the system temp directory and removed when the run ends. Everything
  else streams. Nothing else is written except `-o` and `-log`.
- Memory is bounded per row. On a shared server set `GOMEMLIMIT` to what the
  host allows; `-dedupe`, `-group` and `-sort` are the only stages that grow
  with the data.

## 8. What to keep for an audit

- the raw input and its checksum
- the command line and `runs.jsonl`
- the output and its checksum
- the `datacom version` line, also in the log, and the release checksum you verified
