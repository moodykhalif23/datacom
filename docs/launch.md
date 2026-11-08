# Launch notes

Drafts for the launch morning. Post all of them within the same hour; a
trending day on GitHub is a burst of stars, not a trickle. Replace the numbers
in brackets with the ones from `docs/benchmarks.md` on the day.

Before posting, in this order:

1. `git tag v0.1.0 && git push origin v0.1.0`. The release workflow runs the
   race tests and govulncheck, builds the five binaries, attests provenance
   and publishes the release with `SHA256SUMS`.
2. `vhs docs/demo.tape` and commit `docs/demo.gif`; the README links to it.
3. Run `tools/bench.sh` with qsv, DuckDB, Miller and csvtk installed and paste
   the table into `docs/benchmarks.md`.
4. Enable GitHub Sponsors on the profile; `.github/FUNDING.yml` already points
   at it. Enable Discussions. Add three `good first issue` labels on real
   issues (ideas: Scoop manifest, Homebrew tap, a `-phone` default from the
   locale).

## Show HN

Title (80 characters max):

> Show HN: Datacom – one static binary that turns messy CSV exports into clean data

Text:

> I kept getting the same exports from banks, SACCOs and county offices: a
> BOM at the top, "1,250.00" next to "KES 3400" next to "(500)", three date
> formats in one column, NULL and n/a and "-" for missing, phone numbers in
> four shapes, and duplicates. Every tool I tried either needed a script per
> file (Miller, DuckDB) or could not do it at all (csvkit, xsv).
>
> Datacom is a Go binary with no dependencies that does it in one command and
> tells you what it changed and what it could not: `datacom clean export.csv
> -snake -dedupe -fold-case -phone 254 -monitor`. Column types are decided by
> majority of the first thousand rows; strays are kept as written and listed
> in the report, never silently coerced. It streams, so a 10 GB file needs
> about 20 MB of memory, and it reads CSV, TSV, JSON, NDJSON, Excel and
> Parquet from files, stdin or URLs.
>
> There is also `profile` for a one-page summary of any file, `query` for
> filter/derive/join/group/sort, and `watch` for a growing log. The benchmark
> doc compares it honestly with qsv, Miller, csvtk, DuckDB and csvkit: it is
> slower than qsv and DuckDB at raw parsing and far ahead on what one command
> can do. MIT licensed. I would like to hear which messy export of yours it
> fails on.

## r/golang

Title:

> datacom: a stdlib-only CLI that cleans, profiles and queries messy CSV/JSON/Excel/Parquet, streaming

Body: the Show HN text, plus one paragraph on the Go side: every stage is a
goroutine and a channel sharing one context, so an error, Ctrl-C, a closed
pipe or `-limit` stops the whole chain; `GOMEMLIMIT=64MiB` runs 10M rows; the
race detector and fuzz tests run in CI; the only dependency is parquet-go.

## r/dataengineering

Title:

> I built a one-command cleaner for the exports nobody else wants to touch (CSV, Excel, Parquet; streaming; reports every change)

Body: the Show HN text, with the cleaning comparison table from
`docs/benchmarks.md` pasted in. That table is the argument for this audience.

## Lobsters

Tags: `go`, `show`, `practices`. Title as Show HN without the prefix. One
paragraph: what it does, the link to the benchmark doc, the sentence about
strays being reported rather than coerced.

## Newsletters

- Golang Weekly: submit the repo link with the r/golang title.
- Data Engineering Weekly: submit `docs/benchmarks.md` with the
  r/dataengineering title.
- awesome-go: open a pull request under "Command Line" with the one-line pitch.

## Answering the predictable comments

- "Why not qsv?" It is faster at parsing and has far more commands. It needs
  eight of them in a pipeline to clean this kind of file and loses the sign of
  `(500.00)`. Datacom is one command and a report.
- "Why not DuckDB?" Same answer with SQL instead of a pipeline, and DuckDB is
  the better tool once the data is clean.
- "Go's CSV parser is slow." True; the benchmark doc says so and gives the
  numbers. Batching rows between stages and a custom reader are the next
  work; the architecture already streams and holds 20 MB.
- "Only Kenyan phone numbers?" `-phone` takes any country calling code; the
  default leaves national numbers national because the country is not in
  the data.
