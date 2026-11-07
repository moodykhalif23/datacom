# Datacom architecture

Datacom is a Go command-line data tool. Its CLI coordinates file/network I/O and
selects a command; the `pipeline` package reads, transforms, and writes tables.
Rows normally flow through bounded channels, with one goroutine per stage.

## System architecture

```mermaid
flowchart LR
    User["CLI user / shell"] --> CLI["main.go<br/>command, flags, context"]

    subgraph Sources["Input sources"]
        File["Local file"]
        Stdin["stdin"]
        URL["HTTP(S) URL"]
        Follow["Growing file / stdin<br/>watch mode"]
    end
    CLI --> Open["open / pipeline.Open"]
    File --> Open
    Stdin --> Open
    URL --> Remote["remote reader<br/>retry, resume, backoff"]
    Remote --> Open
    CLI --> Follow

    Open --> Detect["format detection<br/>and gzip decoding"]
    Detect --> Decode["pipeline.Read"]
    Follow --> Decode
    Decode --> Decoders["CSV / TSV<br/>JSON / NDJSON<br/>XLSX / Parquet"]
    Decoders --> Raw["Table: header + row channel"]

    subgraph Processing["CLI-selected processing stages"]
        Clean["Clean<br/>headers, types, empty rows, dedupe"]
        Join["Left join<br/>right-side lookup held in memory"]
        Derive["Derive columns"]
        Filter["Where filters"]
        Select["Select / rename columns"]
        Aggregate["Group and aggregate<br/>memory per group"]
        Sort["Sort<br/>buffers rows"]
        Limit["Limit"]
    end
    Raw --> Clean --> Join --> Derive --> Filter --> Select
    Select --> Aggregate --> Sort --> Limit
    Select -. optional stages skipped .-> Limit

    Profile["profile: compute dataset summary"]
    Windows["watch: split into time windows"]
    WatchReport["Per-window group / aggregate<br/>→ sort → limit<br/>→ optional Trend"]
    Convert["convert: read → write"]
    Clean --> Profile
    Select --> Windows --> WatchReport
    Raw --> Convert

    Limit --> Write["pipeline.Write"]
    WatchReport --> Write
    Convert --> Write
    Write --> Dest["stdout or output file<br/>table / CSV / TSV / JSON / NDJSON /<br/>Markdown / XLSX / Parquet"]
    Profile --> ProfileOut["text or JSON report"]
    ProfileOut --> ProfileDest["stdout or output file"]

    subgraph Observability["Optional monitoring"]
        Monitor["Monitor<br/>bytes, rows, data-quality findings,<br/>buffer notes, errors"]
        Log["stderr progress / final report<br/>optional JSONL log"]
    end
    CLI -. attaches to context .-> Monitor
    Decode -. counts input .-> Monitor
    Clean -. reports data-quality findings .-> Monitor
    Join -. reports unmatched keys .-> Monitor
    Aggregate -. reports large group counts .-> Monitor
    Sort -. reports large buffers .-> Monitor
    Write -. records output and error .-> Monitor
    Monitor --> Log

    CLI -. cancellation / first error .-> Raw
    Raw -. shared context and error .-> Clean
    Clean -. shared context and error .-> Join
    Join -. shared context and error .-> Derive
    Derive -. shared context and error .-> Filter
    Filter -. shared context and error .-> Select
    Select -. shared context and error .-> Aggregate
    Aggregate -. shared context and error .-> Sort
    Sort -. shared context and error .-> Limit
```

Each pipeline stage passes rows over a bounded channel (512 rows) and shares
the run's cancellation and first error. This bounds per-row memory for
streaming stages; deduplication retains row hashes, joins retain the right
input, grouping retains group state, and sorting buffers its input. XLSX and
Parquet require random access; non-file inputs are spooled where needed.

## Command flow

```mermaid
flowchart TD
    Start([Start]) --> Args{Command?}
    Args -->|help / no command| Help["Print usage"]
    Args -->|version| Version["Print build details"]
    Args -->|profile, clean, query,<br/>convert, watch| Setup["Parse flags and source<br/>set cancellation context"]
    Args -->|other| Unknown["Return unknown-command error"]

    Setup --> Observe{"Monitor or log<br/>enabled?"}
    Observe -->|yes| Attach[Attach monitor to context]
    Observe -->|no| Open
    Attach --> Open
    Open["Open source and detect format<br/>or follow source for watch"] --> ErrorOpen{Open error?}
    ErrorOpen -->|yes| Fail["Report error and exit"]
    ErrorOpen -->|no| Output["Choose stdout / file<br/>and output format"]

    Output --> IsProfile{profile?}
    IsProfile -->|yes| ProfileClean["Clean and normalize rows"]
    ProfileClean --> ProfileData["Compute column profile,<br/>quality issues, duplicate count"]
    ProfileData --> ProfileFormat{JSON report?}
    ProfileFormat -->|yes| JSONReport[Write JSON report]
    ProfileFormat -->|no| TextReport[Write text report]

    IsProfile -->|no| IsConvert{convert?}
    IsConvert -->|yes| ConvertDirect["Read rows and write<br/>in requested format"]
    IsConvert -->|no| Shape["Clean → optional left join<br/>→ derive → filter → select"]
    Shape --> IsWatch{watch?}
    IsWatch -->|yes| Windows["Split incoming rows into<br/>time windows"]
    Windows --> PerWindow["For each window:<br/>group / aggregate → sort → limit"]
    PerWindow --> Trend{Grouped watch?}
    Trend -->|yes| AddTrend["Add changes since<br/>previous window"]
    Trend -->|no| WriteWindow[Write window report]
    AddTrend --> WriteWindow
    WriteWindow --> MoreWindows{More input / windows?}
    MoreWindows -->|yes| Windows
    MoreWindows -->|no| WatchDone[Finish watch]

    IsWatch -->|no| ReportStages["Optional group / aggregate<br/>→ optional sort → optional limit"]
    ReportStages --> WriteRows["Write rows in output format"]
    WriteRows --> WriteError{"Write or pipeline<br/>error?"}
    WriteError -->|yes| Fail
    WriteError -->|no| RowCount["Print row count unless quiet"]

    JSONReport --> Finish[Finish and emit monitor report / log]
    TextReport --> Finish
    ConvertDirect --> Finish
    WatchDone --> Finish
    RowCount --> Finish
```

For `clean` and `query`, the report stages are only added when requested.
`convert` bypasses cleaning and shaping; `profile` computes a dataset summary;
`watch` follows its source and emits reports repeatedly until input ends or
the run is cancelled.
