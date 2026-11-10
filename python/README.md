# datacom for Python

Clean, profile and query messy data files, and prepare text for AI training, from Python. The package bundles the datacom binary for your platform, so there is nothing else to install and the results match the command line exactly.

```sh
pip install datacom
```

## Quick start

```python
import datacom

report = datacom.profile("sales.xlsx")
print(report["rows"], report["issues"])

manifest = datacom.clean("raw.csv.gz", "clean.parquet", snake=True, dedupe=True, redact="all")
print(manifest["rows_out"], manifest["outputs"][0]["sha256"])
```

`clean`, `query` and `convert` return the run manifest: input and output SHA-256, every rule applied, and how many rows or values each one dropped or changed.

## Training data

```python
manifest = datacom.clean(
    "s3://lake/crawl/2026-11/*.jsonl.gz",
    "clean/part-*.parquet",
    text=["body"],
    quality="default",
    decontam="evals/mmlu.jsonl",
    near_dupes=0.8,
    redact="all",
    workers=16,
)
for rule in manifest["rules"]:
    print(rule)
```

- `text` cleans those columns as documents: repairs garbled characters, strips HTML, normalizes Unicode and removes control characters.
- `quality` applies the Gopher rules and counts what each rule dropped.
- `decontam` drops documents that share a 13-word sequence with a test set.
- `near_dupes` removes near-duplicates with MinHash in a fixed-size index.
- `redact` masks emails, phone numbers, ID numbers and card numbers.

Sources can be local paths, URLs, `s3://`, `gs://` or `hf://datasets/...`, with globs. Credentials come from the usual places: `AWS_*` variables or `~/.aws/credentials`, `GOOGLE_OAUTH_ACCESS_TOKEN`, and `HF_TOKEN`.

## Options

Keyword arguments become flags: `snake=True` is `-snake`, `near_dupes=0.8` is `-near-dupes 0.8`, `text=["title", "body"]` is `-text title,body`, and lists for `where` and `derive` repeat the flag. `datacom.run(command, *sources, **options)` runs any command and returns the `subprocess.CompletedProcess`; failures raise `datacom.DatacomError` with datacom's message.

With pandas installed, `datacom.clean_frame(df, snake=True, dedupe=True)` returns a cleaned copy of a DataFrame.

The `datacom` command is installed too, so everything in the [main documentation](https://github.com/moodykhalif23/datacom) works from the shell.
