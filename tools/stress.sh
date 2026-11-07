#!/usr/bin/env bash
# Stress datacom on millions of realistic messy rows under a hard Go memory
# limit, and check it survives slow pipes, Ctrl-C and closed pipes.
#
#   tools/stress.sh [workdir] [rows]      defaults: ./.stress 10000000
set -euo pipefail
cd "$(dirname "$0")/.."
WORK=${1:-.stress}; ROWS=${2:-10000000}; LIMIT=${GOMEMLIMIT:-64MiB}
mkdir -p "$WORK"; DATA="$WORK/big.csv"
go build -o "$WORK/datacom" . && go build -o "$WORK/gendata" ./tools/gendata
[ -s "$DATA" ] || { echo "generating $ROWS rows..."; "$WORK/gendata" -rows "$ROWS" -o "$DATA"; }
ls -lh "$DATA" | awk '{print "input:", $5}'

# run LABEL CMD... : wall time and peak RSS sampled from /proc.
run() {
  local label=$1; shift; local s peak=0 v p
  s=$(date +%s.%N); GOMEMLIMIT=$LIMIT "$@" >/dev/null 2>"$WORK/err" & p=$!
  while kill -0 $p 2>/dev/null; do
    v=$(awk '/VmHWM/{print $2}' /proc/$p/status 2>/dev/null || true)
    [ -n "$v" ] && [ "$v" -gt "$peak" ] && peak=$v; sleep 0.05
  done
  local rc=0; wait $p || rc=$?
  printf "%-34s %6.1fs  peak %5d MB  exit %d\n" "$label" "$(echo "$(date +%s.%N) - $s" | bc)" "$((peak/1024))" "$rc"
  [ $rc -eq 0 ] || sed 's/^/    /' "$WORK/err"
}
D="$WORK/datacom"
echo "GOMEMLIMIT=$LIMIT"
run "convert -> ndjson"            $D convert "$DATA" -f ndjson -q
run "clean -snake (types) -> csv"  $D clean "$DATA" -snake -q -f csv
run "clean -dedupe -> csv"         $D clean "$DATA" -snake -dedupe -q -f csv
run "query filter+group+agg"       $D query "$DATA" -snake -where "amount_kes > 5000" -where "status == paid" -group county,channel -agg "count,sum(amount_kes),avg(amount_kes)" -sort -count -q
run "query -limit 10 (early stop)" $D query "$DATA" -snake -limit 10 -q
run "profile"                      $D profile "$DATA"
gzip -kf "$DATA" 2>/dev/null || true
run "profile (gzip input)"         $D profile "$DATA.gz"
run "slow pipe (stdin, 40 MB/s)"      bash -c "size=\$(stat -c %s '$DATA'); for ((i=0;;i++)); do dd if='$DATA' bs=4M skip=\$i count=1 status=none; [ \$(( (i+1)*4194304 )) -ge \$size ] && break; sleep 0.1; done | GOMEMLIMIT=$LIMIT $D query - -i csv -snake -group status -agg count -q"

echo; echo "Ctrl-C after 1s:"
$D convert "$DATA" -f ndjson -q >/dev/null & p=$!; sleep 1; kill -INT $p; wait $p && echo "  exit 0" || echo "  exit $? (interrupted, expected non-zero)"
echo "closed pipe (| head):"
s=$(date +%s.%N); ( set +o pipefail; $D convert "$DATA" -f ndjson -q | head -2 >/dev/null ); printf "  stopped in %.1fs\n" "$(echo "$(date +%s.%N) - $s" | bc)"
echo "done. 4 lines of profile:"; $D profile "$DATA" -top 3 | sed -n '1,4p'
