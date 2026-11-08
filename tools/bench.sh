#!/usr/bin/env bash
# Benchmark datacom against other CSV tools on the same generated data.
#
#   tools/bench.sh [workdir] [rows]          defaults: ./.bench 1000000
#
# The other tools are optional: whichever of mlr (Miller), csvtk, qsv, xsv,
# duckdb and csvkit are on PATH or in <workdir>/tools take part. Nothing is
# installed for you; docs/benchmarks.md says where each comes from. Every
# command runs REPS times (default 3) and the median wall time, CPU time and
# peak RSS go to <workdir>/results.tsv; a markdown table is printed at the end.
#
#   REPS=5 tools/bench.sh                    more repetitions
#   TOOLS="datacom qsv" tools/bench.sh       only some tools
set -euo pipefail
cd "$(dirname "$0")/.."
WORK=$(mkdir -p "${1:-.bench}" && cd "${1:-.bench}" && pwd); ROWS=${2:-1000000}; REPS=${REPS:-3}
export PATH="$WORK/tools:$HOME/.local/bin:$PATH"
export QSV_NO_UPDATE=1 QSV_STATSCACHE_MODE=none   # no release check over the network, no stats cache between runs
unset AI_AGENT   # DuckDB 2 changes its output format when this is set
mkdir -p "$WORK/tools" "$WORK/out"
go build -o "$WORK/tools/datacom" . && go build -o "$WORK/tools/gendata" ./tools/gendata && go build -o "$WORK/tools/benchrun" ./tools/benchrun

MESSY=$WORK/messy-$ROWS.csv; CLEAN=$WORK/clean-$ROWS.csv; RES=$WORK/results.tsv
[ -s "$MESSY" ] || { echo "generating $ROWS messy rows..."; gendata -rows "$ROWS" -o "$MESSY"; }
[ -s "$CLEAN" ] || { echo "cleaning them once with datacom (the clean file is the input for the per-operation tests)..."
  datacom clean "$MESSY" -snake -derive "status = lower(status)" -derive "channel = lower(channel)" -f csv -o "$CLEAN" -q; }
[ -s "$RES" ] || printf "tool\top\trows\tmedian_s\tbest_s\tcpu_s\trss_mb\texit\tnote\n" > "$RES"

ALL="datacom miller csvtk qsv xsv duckdb csvkit"
declare -A BIN=([datacom]=datacom [miller]=mlr [csvtk]=csvtk [qsv]=qsv [xsv]=xsv [duckdb]=duckdb [csvkit]=csvstat)
have() { [[ " ${TOOLS:-$ALL} " == *" $1 "* ]] && command -v "${BIN[$1]}" >/dev/null 2>&1; }
ver() { case $1 in csvtk) csvtk version ;; datacom) datacom version | head -1 ;; *) "${BIN[$1]}" --version 2>&1 | head -1 ;; esac; }
# run TOOL OP CMD...: time CMD; stdout goes to /dev/null, or to $OUT when set.
run() { have "$1" || return 0; benchrun -n "$REPS" -o "$RES" -tool "$1" -op "$2" -rows "$ROWS" -stdout "${OUT:-/dev/null}" -- "${@:3}"; }
# csvkit is a Python tool that loads whole files; one repetition is enough and 10M rows is too many.
csvkit_reps=1
if [ "$ROWS" -gt 1000000 ]; then TOOLS=${TOOLS:-$ALL}; TOOLS=${TOOLS/csvkit/}; fi

{
  echo "date: $(date -u +%Y-%m-%dT%H:%MZ)   cpu: $(nproc) x $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2- | xargs)   mem: $(free -g | awk '/Mem/{print $2}') GB   kernel: $(uname -r)"
  echo "rows: $ROWS   messy: $(du -h "$MESSY" | cut -f1)   clean: $(du -h "$CLEAN" | cut -f1)"
  for t in $ALL; do if have "$t"; then printf "%-8s %s\n" "$t" "$(ver "$t")"; fi; done
} | tee "$WORK/env.txt"
echo

sql_clean="read_csv('$CLEAN')"

# 1. count the rows of the messy file: a parsing test (BOM, quoted newlines, mixed cells).
for t in $ALL; do have "$t" || continue; export OUT="$WORK/out/count-messy-$t.txt"
  case $t in
    datacom) run $t count-messy datacom query "$MESSY" -no-types -agg count -f csv -q ;;
    miller)  run $t count-messy mlr --icsv --ocsv count "$MESSY" ;;
    csvtk)   run $t count-messy csvtk nrow "$MESSY" ;;
    qsv)     run $t count-messy qsv count "$MESSY" ;;
    xsv)     run $t count-messy xsv count "$MESSY" ;;
    duckdb)  run $t count-messy duckdb -csv -noheader -c "SELECT count(*) FROM read_csv('$MESSY')" ;;
    csvkit)  REPS=$csvkit_reps run $t count-messy csvstat --count -I "$MESSY" ;;
  esac
done; unset OUT

# 2. clean the messy file: whitespace, null markers, numbers, dates, booleans, empty and duplicate rows.
run datacom clean-messy datacom clean "$MESSY" -snake -dedupe -f csv -q
# Miller: whitespace, empty rows and duplicates are verbs; headers, null markers, amounts and dates need a DSL program.
MLR_CLEAN='
  map o = {};
  for (k, v in $*) {
    nk = tolower(gsub(gsub(k, "[^A-Za-z0-9]+", "_"), "^_|_$", ""));
    if (is_string(v) && v =~ "^(null|n/a|none|-)$"i) { v = "" }
    o[nk] = v;
  }
  $* = o;
  if (is_not_empty($amount_kes)) {
    a = gsub($amount_kes, "[^0-9.()-]", "");
    if (a =~ "^\((.*)\)$") { a = "-" . "\1" }
    $amount_kes = float(a);
  }
  if (is_not_empty($txn_date)) {
    t = strptime($txn_date, "%Y-%m-%d");
    if (is_error(t)) { t = strptime($txn_date, "%d/%m/%Y") }
    if (is_error(t)) { t = strptime($txn_date, "%d %b %Y") }
    if (!is_error(t)) { $txn_date = sec2gmtdate(t) }
  }'
run miller  clean-messy mlr --icsv --ocsv clean-whitespace then put "$MLR_CLEAN" then skip-trivial-records then uniq -a "$MESSY"
# csvtk: one process per change, values stay text, headers are not lower-cased.
run csvtk   clean-messy bash -c "csvtk replace -F -f '*' -p '^\s+|\s+\$' -r '' '$MESSY' | csvtk replace -F -f '*' -p '\s+' -r ' ' | csvtk replace -F -f '*' -i -p '^(NULL|n/a|none|-)\$' -r '' | csvtk replace -f 'Amount (KES)' -i -p '^(KES|Ksh)\s*|,' -r '' | csvtk replace -f 'Amount (KES)' -p '^\((.+)\)\$' -r '-\$1' | csvtk fmtdate -f 'Txn Date' --format 'YYYY-MM-DD' -k | csvtk rename2 -F -f '*' -p '[^A-Za-z0-9]+' -r '_' | csvtk uniq -E -F -f '*'"
# qsv: trim, rename, null markers, currency, dates, dedupe. Nothing for booleans, empty rows or (500.00) negatives.
run qsv     clean-messy bash -c "qsv input --trim-headers --trim-fields '$MESSY' | qsv rename txn_id,customer_name,phone,email,county,amount_kes,status,channel,txn_date,age,notes | qsv replace -i --not-one '^(NULL|n/a|none|-)$' '<NULL>' | qsv apply operations trim,squeeze 1- | qsv apply operations currencytonum amount_kes | qsv datefmt txn_date --formatstr '%Y-%m-%d' --prefer-dmy | qsv extdedup -s 1-"
# csvclean only repairs structure (row lengths, empty columns); it does not touch cell values.
run csvkit  clean-messy csvclean -a --omit-error-rows "$MESSY"

# 3. profile every column.
run datacom profile datacom profile "$CLEAN"
run miller  profile mlr --icsv --ocsv summary "$CLEAN"
# csvtk has no profile command; this is the nearest: numeric stats for the two numeric columns only.
run csvtk   profile csvtk summary -i -f amount_kes:count,amount_kes:countn,amount_kes:min,amount_kes:max,amount_kes:mean,amount_kes:stdev,age:min,age:max,age:mean "$CLEAN"
run qsv     profile qsv stats --cardinality --infer-dates --cache-threshold 0 "$CLEAN"
run xsv     profile xsv stats --everything "$CLEAN"
run duckdb  profile duckdb -csv -c "SUMMARIZE SELECT * FROM $sql_clean"
REPS=$csvkit_reps run csvkit profile csvstat "$CLEAN"

# 4. count rows of the clean file.
run datacom count datacom query "$CLEAN" -no-types -agg count -f csv -q
run miller  count mlr --icsv --ocsv count "$CLEAN"
run csvtk   count csvtk nrow "$CLEAN"
run qsv     count qsv count "$CLEAN"
run xsv     count xsv count "$CLEAN"
run duckdb  count duckdb -csv -noheader -c "SELECT count(*) FROM $sql_clean"
REPS=$csvkit_reps run csvkit count csvstat --count -I "$CLEAN"

# 5. drop exact duplicate rows.
run datacom dedupe datacom query "$CLEAN" -no-types -dedupe -f csv -q
run miller  dedupe mlr --icsv --ocsv uniq -a "$CLEAN"
run csvtk   dedupe csvtk uniq -F -f '*' "$CLEAN"
run qsv     dedupe qsv extdedup -s 1- "$CLEAN"
run duckdb  dedupe duckdb -c "COPY (SELECT DISTINCT * FROM $sql_clean) TO '/dev/stdout' (FORMAT csv, HEADER)"

# 6. filter: amount_kes > 5000 and status == paid.
run datacom filter datacom query "$CLEAN" -no-types -where "amount_kes > 5000" -where "status == paid" -f csv -q
run miller  filter mlr --icsv --ocsv filter '$amount_kes > 5000 && $status == "paid"' "$CLEAN"
run csvtk   filter csvtk filter2 -f '$amount_kes > 5000 && $status == "paid"' "$CLEAN"
run qsv     filter qsv sqlp "$CLEAN" "SELECT * FROM _t_1 WHERE amount_kes > 5000 AND status = 'paid'"
run duckdb  filter duckdb -c "COPY (SELECT * FROM $sql_clean WHERE amount_kes > 5000 AND status = 'paid') TO '/dev/stdout' (FORMAT csv, HEADER)"
REPS=$csvkit_reps run csvkit filter csvsql --tables t --query "SELECT * FROM t WHERE amount_kes > 5000 AND status = 'paid'" "$CLEAN"

# 7. group by county, channel with count, sum and mean of amount_kes.
run datacom group datacom query "$CLEAN" -no-types -group county,channel -agg "count,sum(amount_kes),avg(amount_kes)" -f csv -q
run miller  group mlr --icsv --ocsv stats1 -a count,sum,mean -f amount_kes -g county,channel "$CLEAN"
run csvtk   group csvtk summary -i -g county,channel -f amount_kes:count,amount_kes:sum,amount_kes:mean -w 6 "$CLEAN"
run qsv     group qsv sqlp "$CLEAN" "SELECT county, channel, count(*) AS n, sum(amount_kes) AS total, avg(amount_kes) AS mean FROM _t_1 GROUP BY county, channel"
run duckdb  group duckdb -c "COPY (SELECT county, channel, count(*) AS n, sum(amount_kes) AS total, avg(amount_kes) AS mean FROM $sql_clean GROUP BY ALL) TO '/dev/stdout' (FORMAT csv, HEADER)"
REPS=$csvkit_reps run csvkit group csvsql --tables t --query "SELECT county, channel, count(*), sum(amount_kes), avg(amount_kes) FROM t GROUP BY county, channel" "$CLEAN"

# 8. sort by amount_kes descending.
run datacom sort datacom query "$CLEAN" -no-types -sort -amount_kes -f csv -q
run miller  sort mlr --icsv --ocsv sort -nr amount_kes "$CLEAN"
run csvtk   sort csvtk sort -k amount_kes:nr "$CLEAN"
run qsv     sort qsv sort -s amount_kes -N -R "$CLEAN"
run xsv     sort xsv sort -s amount_kes -N -R "$CLEAN"
run duckdb  sort duckdb -c "COPY (SELECT * FROM $sql_clean ORDER BY amount_kes DESC) TO '/dev/stdout' (FORMAT csv, HEADER)"
REPS=$csvkit_reps run csvkit sort csvsort -c amount_kes -r "$CLEAN"

# 9. convert to newline-delimited JSON.
run datacom tojson datacom convert "$CLEAN" -f ndjson -q
run miller  tojson mlr --icsv --ojsonl cat "$CLEAN"
run csvtk   tojson csvtk csv2json -i '' -n all "$CLEAN"   # one JSON array; csvtk has no JSON Lines output
run qsv     tojson qsv tojsonl "$CLEAN"
run duckdb  tojson duckdb -c "COPY (SELECT * FROM $sql_clean) TO '/dev/stdout' (FORMAT json)"
REPS=$csvkit_reps run csvkit tojson csvjson --stream -I --snifflimit 0 "$CLEAN"

# 10. frequency of status values.
run datacom freq datacom query "$CLEAN" -no-types -group status -agg count -sort -count -f csv -q
run miller  freq mlr --icsv --ocsv count-distinct -f status then sort -nr count "$CLEAN"
run csvtk   freq csvtk freq -f status -n -r "$CLEAN"
run qsv     freq qsv frequency -s status "$CLEAN"
run xsv     freq xsv frequency -s status "$CLEAN"
run duckdb  freq duckdb -csv -c "SELECT status, count(*) AS n FROM $sql_clean GROUP BY ALL ORDER BY n DESC"
REPS=$csvkit_reps run csvkit freq csvstat -c status --freq "$CLEAN"

echo; echo "rows counted in the messy file (all should agree):"
for f in "$WORK"/out/count-messy-*.txt; do [ -e "$f" ] || continue; n=$(basename "$f" .txt); printf "  %-8s %s\n" "${n#count-messy-}" "$(tr -s '\n' ' ' < "$f" | cut -c1-100)"; done

echo; echo "## $ROWS rows: median wall seconds (peak RSS MB)"; echo
awk -F'\t' -v rows="$ROWS" -v tools="$ALL" -v ops="count-messy clean-messy profile count dedupe filter group sort tojson freq" '
NR > 1 && $3 == rows { v[$2 SUBSEP $1] = ($8 == 0 ? sprintf("%.2f (%d)", $4, $7) : "fail"); seen[$1] = 1 }
END {
  nt = split(tools, T, " "); no = split(ops, O, " ")
  printf "| operation |"; for (i = 1; i <= nt; i++) if (seen[T[i]]) printf " %s |", T[i]; print ""
  printf "|---|"; for (i = 1; i <= nt; i++) if (seen[T[i]]) printf "---:|"; print ""
  for (j = 1; j <= no; j++) { printf "| %s |", O[j]
    for (i = 1; i <= nt; i++) if (seen[T[i]]) { k = O[j] SUBSEP T[i]; printf " %s |", (k in v) ? v[k] : "n/a" }
    print "" }
}' "$RES"
echo; echo "details: $RES"
