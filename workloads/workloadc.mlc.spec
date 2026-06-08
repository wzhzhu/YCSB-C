# Yahoo! Cloud System Benchmark
# Workload C: Read only
#   Application example: user profile cache, where profiles are constructed elsewhere (e.g., Hadoop)
#                        
#   Read/update ratio: 100/0
#   Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
#   Request distribution: zipfian

recordcount=100000
operationcount=100000
workload=com.yahoo.ycsb.workloads.CoreWorkload

readallfields=true

readproportion=1
updateproportion=0
scanproportion=0
insertproportion=0

requestdistribution=zipfian




rocksdb.dir=/tmp/ycsbc-rocksdb-mlc
rocksdb.cache_type=hyper_clock_cache
rocksdb.block_cache_size_bytes=67108864
rocksdb.use_multi_level_cache=true
rocksdb.num_levels=7
rocksdb.multi_level_cache_shared_pool_ratio=0.0
rocksdb.multi_level_cache_auto_adjust=true
rocksdb.multi_level_cache_allocator_mode=model
rocksdb.multi_level_cache_adjust_interval_ms=500
rocksdb.multi_level_cache_adjust_smoothing_ratio=0.5
rocksdb.multi_level_cache_adjust_min_change_bytes=1048576
rocksdb.multi_level_cache_compaction_shift_ratio=0.0
rocksdb.multi_level_cache_compaction_shift_max_total_ratio=0.1
rocksdb.multi_level_cache_alpha_estimator=robust_hit_rate
