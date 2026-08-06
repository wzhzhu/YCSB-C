#!/usr/bin/env bash
# D-class re-validation (post generator-fix, clean harness).
#
# The realization credit band (score_credit_frac cap + score_credit_floor_frac
# floor) corrects the ghost's churn under/over-reporting on write-heavy
# workloads: without it, long wlA runs drift to an L5-heavy allocation
# (compaction rewrites L5 files before the promised repeats land) costing
# ~3pt fg hit. That evidence predates the generator fix. This ablation
# re-measures the band's value on the clean harness. If band_on is not
# meaningfully better than band_off (throughput AND fg hit), the band is
# unnecessary complexity.
#
# Regime: wlA (50% writes) is the only place the band's churn argument bites.
# The drift is a LONG-run effect (header: "100M-op runs drifted"), so full
# 100M ops. Mid concurrency (32,64). 2G (the cited cell). MLC only; 3 repeats.
# band_off disables BOTH halves: score_credit_frac<=0 kills the cap,
# score_credit_floor_frac<=0 kills the floor.
set -uo pipefail
cd /users/wzhzhu/YCSB-C-master || exit 1

COMMON=(
  --workloads A --operationcount 100000000
  --schemes mlc_hcc_all_levels_sharded
  --threads 32,64 --cache-gb 2 --repeats 3
  --refill-policy clone_per_case --db-run-id wlABCDEF-0618
  --fstrim-per-run --fstrim-idle-util 5 --fstrim-idle-max-sec 120
)

echo "===== CREDIT BAND ON (default frac=1.0 floor=0.25)  $(date) ====="
python3 scripts/run_rocksdb_matrix.py "${COMMON[@]}" \
  --run-id creditband-on-0804 \
  --results-dir results/rocksdb-matrix/creditband-on-0804

echo "===== CREDIT BAND OFF (frac=0 floor=0)  $(date) ====="
python3 scripts/run_rocksdb_matrix.py "${COMMON[@]}" \
  --extra-prop rocksdb.multi_level_cache_score_credit_frac=0 \
  --extra-prop rocksdb.multi_level_cache_score_credit_floor_frac=0 \
  --run-id creditband-off-0804 \
  --results-dir results/rocksdb-matrix/creditband-off-0804

echo "===== CREDIT BAND ABLATION DONE  $(date) ====="
