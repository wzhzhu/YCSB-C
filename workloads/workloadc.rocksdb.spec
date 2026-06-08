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




rocksdb.dir=/tmp/ycsbc-rocksdb-param
rocksdb.block_cache_size_bytes=67108864
rocksdb.cache_type=hyper_clock_cache
rocksdb.cache_numshardbits=-1
rocksdb.cache_index_and_filter_blocks=true
rocksdb.cache_index_and_filter_blocks_with_high_priority=true
rocksdb.use_multi_level_cache=true
rocksdb.num_levels=7
rocksdb.multi_level_cache_srhcc_start_level=4
rocksdb.multi_level_cache_shared_pool_ratio=0.1
