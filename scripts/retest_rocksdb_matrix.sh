#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROCKSDB_HOME="${ROCKSDB_HOME:-/users/wzhzhu/rocksdb}"

cd "${ROOT_DIR}"
if ! make ROCKSDB_HOME="${ROCKSDB_HOME}" \
  LDFLAGS="-Wl,-rpath,${ROCKSDB_HOME}/build-tests ${ROCKSDB_HOME}/build-tests/librocksdb.so -lpthread -ltbb"; then
  make -C core
  make -C db ROCKSDB_HOME="${ROCKSDB_HOME}"
  g++ -std=c++20 -g -Wall -pthread \
    -I./ -I"${ROCKSDB_HOME}" -I"${ROCKSDB_HOME}/include" \
    -DYCSBC_ENABLE_REDIS=0 \
    ycsbc.cc core/core_workload.o db/rocksdb_db.o db/hashtable_db.o db/db_factory.o \
    -Wl,-rpath,"${ROCKSDB_HOME}/build-tests" \
    "${ROCKSDB_HOME}/build-tests/librocksdb.so" -lpthread -ltbb \
    -o ycsbc
fi

python3 "${ROOT_DIR}/scripts/run_rocksdb_matrix.py" \
  --ycsb-root "${ROOT_DIR}" \
  "$@"
