#!/usr/bin/env bash
# D-class re-validation (post generator-fix, clean harness).
#
# Stall-adaptive lazy mode was added to recover MLC's fixed per-miss/per-round
# overhead in the LOW-concurrency, read-heavy, converged regime (the header
# claims 1-6% throughput there). That claim was measured BEFORE the YCSB
# ScrambledZipfianGenerator lock-convoy was fixed. This ablation re-measures
# whether lazy mode still buys anything on the clean harness. If lazy_on is
# not meaningfully faster than lazy_off, the overhead it recovers is now
# negligible and lazy mode is unnecessary complexity (-> default it OFF).
#
# Regime: read-heavy (C read-only, D read-latest) so stall ~ 0 and the
# allocation converges -> lazy engages. Low threads (8,16) where the fixed
# overhead is the largest throughput share. Small + large cache (2,8G).
# MLC only; the on/off arm only changes MLC internals. 3 repeats for a small
# % effect. 20M ops: lazy engages after ~2M lookups (20 stable rounds x 100k),
# so ~90% of the run is lazy-engaged.
set -uo pipefail
cd /users/wzhzhu/YCSB-C-master || exit 1

COMMON=(
  --workloads C,D --operationcount 20000000
  --schemes mlc_hcc_all_levels_sharded
  --threads 8,16 --cache-gb 2,8 --repeats 3
  --refill-policy clone_per_case --db-run-id wlABCDEF-0618
  --fstrim-per-run --fstrim-idle-util 5 --fstrim-idle-max-sec 120
)

echo "===== LAZY ON (default)  $(date) ====="
python3 scripts/run_rocksdb_matrix.py "${COMMON[@]}" \
  --extra-prop rocksdb.multi_level_cache_lazy_mode=true \
  --run-id lazy-on-0804 \
  --results-dir results/rocksdb-matrix/lazy-on-0804

echo "===== LAZY OFF  $(date) ====="
python3 scripts/run_rocksdb_matrix.py "${COMMON[@]}" \
  --extra-prop rocksdb.multi_level_cache_lazy_mode=false \
  --run-id lazy-off-0804 \
  --results-dir results/rocksdb-matrix/lazy-off-0804

echo "===== LAZY ABLATION DONE  $(date) ====="
