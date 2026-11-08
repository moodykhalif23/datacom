# What `clean` changes, and how to turn each change off

`clean`, `query` and `watch` run every row through the same cleaning stage.
`convert` does not: it only changes the file format and leaves every value as
it was. `profile` cleans in memory to describe the data and writes nothing.

Everything below was checked against the binary with a probe file; the
examples are its actual output.

## How a column's type is decided

The first 1,000 rows are sampled and each value is classified as an integer,
decimal, boolean, date or text. The majority wins for the column, so a few
stray values do not turn a numeric column into text. A value that does not
fit the column's type is **kept exactly as written** and counted by
`-monitor` under "could not be typed", with up to three examples per column.
Nothing is ever replaced with a guess.

`-no-types` skips typing altogether: numbers, dates and booleans stay as
written. Header cleaning, whitespace collapsing and missing-value markers
(below) still apply.

## Headers

| input | output | with `-snake` |
| --- | --- | --- |
| `Header A` (surrounding spaces trimmed) | `Header A` | `header_a` |
| empty header in column 2 | `col_2` | `col_2` |
| second `Header A` | `Header A_2` | `header_a_2` |
| `Amount (KES)` | `Amount (KES)` | `amount_kes` |

Header names are never used to decide types.

## Missing values

These markers become an empty value, case-insensitively, in every column and
even with `-no-types`: empty, `null`, `nil`, `none`, `na`, `n/a`, `nan`, `-`,
`--`, `?`, `#n/a`, `undefined`. Anything else, including `0` and `unknown`,
is kept.

## Whitespace

Leading and trailing spaces are removed. Tabs, newlines and runs of spaces
inside a value collapse to one space: `"two  spaces<tab>and tab"` becomes
`two spaces and tab`. This is the only change ever made to a text value.
Casing is never changed; use `-derive "col = lower(col)"` when you want it.

## Numbers

In an integer or decimal column these are removed before parsing: thousands
separators (`,` `_` `'` and spaces), the symbols `$ € £ ¥ ₹`, a leading or
trailing currency code (KES, KSh, USD, EUR, GBP, UGX, TZS, RWF, ETB, NGN, ZAR,
GHS, INR, AED, SAR, XOF, XAF and a few more), and a trailing `%`. Accounting
parentheses mean negative.

| input | output |
| --- | --- |
| `1,250.00` | `1250` |
| `KES 3,400` | `3400` |
| `(500)` | `-500` |
| `12%` | `12` (not `0.12`) |
| `8 750` | `8750` |

Output is canonical: no separators, no currency, trailing zeros dropped
(`1250.50` becomes `1250.5`). Values are parsed as 64-bit floats, so
integers above 2^53 (about 9 quadrillion) would lose precision; they are
treated as codes instead, below.

**Codes are never converted.** A value that would change meaning as a number
is kept as text, and a column where most values are codes is a text column:

- leading zeros: `007`, `0712345678`
- a leading `+` followed by seven or more digits: `+254712345678`
- more than 15 digits: `12345678901234567`

## Dates

Recognised layouts: ISO 8601 with or without time and zone, `2006-01-02`,
`2006/01/02`, `02/01/2006`, `01/02/2006`, `02-01-2006`, `2-Jan-2006`,
`02 Jan 2006`, `2 January 2006`, `Jan 2, 2006`, `January 2, 2006`, `2006-01`
and `20060102`.

Two rules matter for institutional data:

- **Ambiguous slashed dates are read day-first.** `01/02/2025` is 1 February
  2025. If a source is month-first, use `-no-types` and convert it
  deliberately.
- **Times are converted to UTC.** `2025-06-01T10:05:00+03:00` becomes
  `2025-06-01T07:05:00Z`. A date with no time, or a time at exactly midnight,
  is written as `2025-06-01`; anything else is written in RFC 3339 UTC.

## Booleans

A column where most values are `true`/`false`, `yes`/`no` or `y`/`n` (any
case) is boolean, and `1`/`0` in that column also become `true`/`false`.
A column of only `1` and `0` is an integer column and stays `1` and `0`.

## Phone numbers

A column where most values look like phone numbers is a phone column: an
international form (`+254 712 345 678`, `00254712345678`), a national number
with its leading 0 (`0712 345 678`), or digit groups with spaces, dashes,
dots or brackets (`(020) 221-4000`). Separators are removed, so
`+254 712 345 678` becomes `+254712345678` and `0712 345 678` becomes
`0712345678`. International forms always get their `+`.

With `-phone 254` (any country calling code), national forms become
international too: `0712345678`, `712345678` and `254712345678` all become
`+254712345678`. Without it they stay national, because the country is not
knowable from the data.

A plain 9-digit number without separators is an integer, not a phone, so a
column of IDs is not mistaken for one. In a phone column it is still read as
a phone. Anything in a phone column that is not a phone number (`call me`)
is kept as written and counted by `-monitor`.

## Casing

Nothing is re-cased by default: `Nairobi` and `NAIROBI` stay different
values, and `profile` warns when it sees that. `-fold-case` fixes it for
categorical columns: any text column with at most 100 distinct values in the
first 1000 rows. Each value is respelled the way it is spelled most often in
those rows, so `PAID`, `Paid` and `paid` all become `paid` when `paid` is the
common form, and `nairobi` becomes `Nairobi`. Columns with more distinct
values, such as names or e-mail addresses, are left alone. `-monitor`
reports how many values were respelled in each column.

## Rows

- Rows that are empty after cleaning are dropped and counted. `-keep-empty`
  keeps them.
- A row with fewer fields than the header is padded with empty values; one
  with more is cut to the header width. Both are counted by `-monitor`.
- Duplicates are dropped only with `-dedupe`. Two rows are duplicates when
  every cleaned value matches exactly, so `Nairobi` and `nairobi` are not
  duplicates. The check keeps one 64-bit hash per distinct row; the chance
  that any two different rows in a 10-million-row file share a hash is about
  three in a million.
- Row order is preserved unless you `-sort`.

## Turning it off

| change | switch |
| --- | --- |
| all type conversion (numbers, dates, booleans) | `-no-types` |
| snake_case headers | leave out `-snake` |
| dropping empty rows | `-keep-empty` |
| dropping duplicates | leave out `-dedupe` |
| international phone numbers | leave out `-phone` |
| respelling categorical values | leave out `-fold-case` |
| any cleaning at all | use `convert` |

Missing-value markers, header trimming and whitespace collapsing cannot be
switched off; if a value must survive byte-for-byte, use `convert`.

## Seeing what happened

`-monitor` prints rows in and out, every column with values that could not be
typed (with examples), rows padded or trimmed, empty and duplicate rows
dropped, and join rows without a match. `-log runs.jsonl` appends the same
report as one JSON object per run. See [operations.md](operations.md) for how
to keep that as an audit record.
