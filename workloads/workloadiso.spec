# YCSB Workload ISO: level-separated isolation workload for MultiLevelCache.
#
# Goal: demonstrate MLC's per-level isolation advantage. Point reads target the
# recently-written keys (latest distribution) which live in shallow LSM levels
# (L0/L1), forming a small hot, highly reusable working set. Scans sweep the old
# bulk via a DECOUPLED uniform start-key distribution over the loaded keyspace,
# streaming low-reuse blocks from the deep levels (L5/L6).
#
# A global cache (LRU/HCC) lets the scan stream evict the hot shallow read set
# (classic non-scan-resistance). MLC caps each level's budget, confining the
# scan pollution to the deep-level sub-caches and protecting the shallow hot
# read set.
#
# Readonly (no inserts/updates) so the LSM is static and every scheme sees an
# identical access sequence; the matrix can reuse one loaded DB across cases.

recordcount=100000
operationcount=100000
workload=com.yahoo.ycsb.workloads.CoreWorkload

readallfields=true

readproportion=0.8
updateproportion=0
scanproportion=0.2
insertproportion=0

# Point reads hit recently written keys (shallow levels).
requestdistribution=latest

# Scans sweep the old bulk (deep levels), decoupled from the read distribution.
scankeydistribution=uniform
maxscanlength=100
scanlengthdistribution=uniform
