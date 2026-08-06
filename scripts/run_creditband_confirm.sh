#!/usr/bin/env bash
# Credit-band wider confirmation (follow-up to creditband-{on,off}-0804).
#
# The first pass (wlA 2G only) found the realization credit band neutral-to-
# negative on the clean harness (geomean ON/OFF -1.85%; t32 band-on lost
# 0.68pt fg + 3.8% tput). 2 cells is thin for removing a core mechanism, so
# widen coverage: two write-heavy workloads (A = update-heavy, F = read-
# modify-write) x larger caches (4/8G) x t64 x 3 repeats, 100M ops (the band's
# drift argument is a long-run effect). lazy_mode pinned OFF on both arms
# (now the default) so the credit band is the only variable.
#
# band_off disables BOTH halves: score_credit_frac<=0 (cap) and
# score_credit_floor_frac<=0 (floor).
set -uo pipefail
cd /users/wzhzhu/YCSB-C-master || exit 1

COMMON=(
  --workloads A,F --operationcount 100000000
  --schemes mlc_hcc_all_levels_sharded
  --threads 64 --cache-gb 4,8 --repeats 3
  --refill-policy clone_per_case --db-run-id wlABCDEF-0618
  --fstrim-per-run --fstrim-idle-util 5 --fstrim-idle-max-sec 120
  --extra-prop rocksdb.multi_level_cache_lazy_mode=false
)

echo "===== CREDIT BAND ON (default frac=1.0 floor=0.25)  $(date) ====="
python3 scripts/run_rocksdb_matrix.py "${COMMON[@]}" \
  --run-id creditband2-on-0805 \
  --results-dir results/rocksdb-matrix/creditband2-on-0805

echo "===== CREDIT BAND OFF (frac=0 floor=0)  $(date) ====="
python3 scripts/run_rocksdb_matrix.py "${COMMON[@]}" \
  --extra-prop rocksdb.multi_level_cache_score_credit_frac=0 \
  --extra-prop rocksdb.multi_level_cache_score_credit_floor_frac=0 \
  --run-id creditband2-off-0805 \
  --results-dir results/rocksdb-matrix/creditband2-off-0805

echo "===== CREDIT BAND CONFIRM DONE  $(date) ====="
