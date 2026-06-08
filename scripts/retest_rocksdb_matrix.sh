#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROCKSDB_HOME="${ROCKSDB_HOME:-/users/wzhzhu/rocksdb}"

cd "${ROOT_DIR}"
make ROCKSDB_HOME="${ROCKSDB_HOME}" \
  LDFLAGS="-L${ROCKSDB_HOME}/build-tests -Wl,-rpath,${ROCKSDB_HOME}/build-tests -lpthread -ltbb -lrocksdb"

python3 "${ROOT_DIR}/scripts/run_rocksdb_matrix.py" \
  --ycsb-root "${ROOT_DIR}" \
  "$@"
