recordcount=1000000
operationcount=1000000
workload=com.yahoo.ycsb.workloads.CoreWorkload

readallfields=true

readproportion=1
updateproportion=0
scanproportion=0
insertproportion=0

requestdistribution=zipfian

rocksdb.dir=/tmp/ycsbc-rocksdb-hcc64
rocksdb.block_cache_size_bytes=67108864
rocksdb.write_buffer_size=4194304
rocksdb.cache_type=hyper_clock_cache
rocksdb.cache_numshardbits=-1
rocksdb.cache_index_and_filter_blocks=true
rocksdb.cache_index_and_filter_blocks_with_high_priority=true
rocksdb.use_multi_level_cache=false
