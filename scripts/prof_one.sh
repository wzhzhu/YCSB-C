#!/usr/bin/env bash
# Profile a single scheme's steady-state transaction phase with perf.
#
# Launches ycsbc (via run_rocksdb_matrix.py, reusing the 0618 golden, skipload),
# waits WARMUP seconds so the DB is open and the cache is warm, samples the
# ycsbc process for DURATION seconds with perf, then stops the run. Produces a
# perf.data plus a --no-children (self%) and a children (cumulative) report.
#
# Usage: prof_one.sh <scheme> <threads> <warmup_s> <duration_s> <tag> [cache_gb]
set -u

SCHEME="${1:?scheme}"
THREADS="${2:-8}"
WARMUP="${3:-70}"
DURATION="${4:-90}"
TAG="${5:-$SCHEME}"
CACHE_GB="${6:-1}"

YCSB=/users/wzhzhu/YCSB-C-master
OUTDIR=/users/wzhzhu/YCSB-C-master/results/prof-bypass-0703
mkdir -p "$OUTDIR"
DATA="$OUTDIR/perf-$TAG.data"
RUNLOG="$OUTDIR/run-$TAG.log"

cd "$YCSB" || exit 1

echo "[prof] launching scheme=$SCHEME threads=$THREADS"
# 200M ops so the transaction phase far outlasts warmup+sampling; we kill it.
setsid python3 scripts/run_rocksdb_matrix.py \
  --workloads A --schemes "$SCHEME" --threads "$THREADS" --cache-gb "$CACHE_GB" \
  --operationcount 200000000 --refill-policy clone_per_case \
  --run-id "prof-$TAG" --db-run-id wlABCDEF-0618 \
  --results-dir "$OUTDIR/matrix-$TAG" >"$RUNLOG" 2>&1 &
RUNNER_PID=$!
echo "[prof] runner pid=$RUNNER_PID (setsid group)"

# Wait for the ycsbc child to appear (clone + DB open take a bit).
YCSBC_PID=""
for _ in $(seq 1 120); do
  YCSBC_PID=$(pgrep -n -f "$YCSB/ycsbc" || true)
  [ -n "$YCSBC_PID" ] && break
  sleep 1
done
if [ -z "$YCSBC_PID" ]; then
  echo "[prof] ERROR: ycsbc never started; see $RUNLOG"
  tail -20 "$RUNLOG"
  pkill -TERM -g "$RUNNER_PID" 2>/dev/null
  exit 2
fi
echo "[prof] ycsbc pid=$YCSBC_PID; warming up ${WARMUP}s"
sleep "$WARMUP"

# Confirm still alive and in transaction phase.
if ! kill -0 "$YCSBC_PID" 2>/dev/null; then
  echo "[prof] ERROR: ycsbc exited during warmup; see $RUNLOG"; tail -20 "$RUNLOG"; exit 3
fi

echo "[prof] recording ${DURATION}s -> $DATA"
sudo perf record -F 999 -p "$YCSBC_PID" --call-graph fp -g \
  -o "$DATA" -- sleep "$DURATION" 2>&1 | tail -3

echo "[prof] stopping run"
sudo pkill -TERM -P "$YCSBC_PID" 2>/dev/null
kill -TERM "$YCSBC_PID" 2>/dev/null
pkill -TERM -g "$RUNNER_PID" 2>/dev/null
sleep 3
pkill -KILL -f "$YCSB/ycsbc" 2>/dev/null
sudo chown "$(id -u):$(id -g)" "$DATA" 2>/dev/null

echo "[prof] self% (top 30):"
sudo perf report -i "$DATA" --no-children --stdio 2>/dev/null \
  | grep -E '^\s+[0-9]' | head -30 | tee "$OUTDIR/self-$TAG.txt"
echo "[prof] cumulative (top 25):"
sudo perf report -i "$DATA" --stdio 2>/dev/null \
  | grep -E '^\s+[0-9]' | head -25 | tee "$OUTDIR/cum-$TAG.txt"
echo "[prof] DONE $TAG"
