# RocksDB Cache Matrix Benchmark

## 默认矩阵（与当前需求一致）

- 工作负载：`A,B,C,D,E,F`
- 预填充：`100GB`（按 `1024B/KV` 近似换算为 `recordcount=104857600`）
- 事务数：`operationcount=1000000`
- block cache：`1GB,2GB,4GB,8GB`
- 线程数：`8,16,32,64,128`
- 对比策略：`lru,hcc,arc,cacheus,mlc_hcc_sr_bottom,mlc_hcc_all_levels`
- 其他固定项：
  - `rocksdb.raw_kv_mode=true`
  - `rocksdb.raw_key_size_bytes=24`
  - `rocksdb.raw_value_size_bytes=1000`
  - `rocksdb.target_file_size_base=64MB`
  - `rocksdb.write_buffer_size=64MB`
  - `rocksdb.bloom_bits_per_key=10`
  - `rocksdb.cache_index_and_filter_blocks=true`
  - `rocksdb.cache_index_and_filter_blocks_with_high_priority=true`
  - `rocksdb.cache_numshardbits=0`（所有方案不分片）

> 说明 1：`mlc_hcc_sr_bottom` 对应“上层 HCC，仅最下层 SR-HCC”，通过  
> `rocksdb.cache_type=hyper_clock_cache` + `rocksdb.use_multi_level_cache=true` + `rocksdb.multi_level_cache_srhcc_start_level=6`（7层）实现。
>
> 说明 1b：`mlc_hcc_all_levels` 对应“MLC 所有层都是 HCC（无 SR-HCC）”，通过  
> `rocksdb.cache_type=hyper_clock_cache` + `rocksdb.use_multi_level_cache=true` + `rocksdb.multi_level_cache_srhcc_start_level=-1` 实现。
>
> 说明 2：`raw_kv_mode=true` 时，RocksDB backend 直接写固定长度 value，且 key 不再拼接 `table` 前缀，从而尽量贴合 `24B key + 1000B value`。

## 一键重测

```bash
./scripts/retest_rocksdb_matrix.sh
```

## 常见复测方式

- 仅跑 workload C，线程 64，缓存 4GB，重复 3 次：

```bash
./scripts/retest_rocksdb_matrix.sh \
  --workloads C \
  --threads 64 \
  --cache-gb 4 \
  --repeats 3
```

- 只看 ARC/Cacheus 两个策略：

```bash
./scripts/retest_rocksdb_matrix.sh \
  --schemes arc,cacheus
```

- 仅预览将执行的任务（不真正运行）：

```bash
./scripts/retest_rocksdb_matrix.sh --dry-run
```

## 输出结果

每次运行会生成独立目录：

- `results/rocksdb-matrix/<run_id>/summary.csv`
- `results/rocksdb-matrix/<run_id>/summary.md`
- `results/rocksdb-matrix/<run_id>/logs/*.log`
- `results/rocksdb-matrix/<run_id>/specs/*.spec`
- `rocksdb.dir` 默认写到 `/mnt/rocksdb_nvme/fio/ycsbc-tmp/<run_id>/...`

如需改数据库路径根目录：

```bash
./scripts/retest_rocksdb_matrix.sh --db-root-dir /path/to/your/db-root
```

默认每个 case 结束会自动删除该 case 的 `rocksdb.dir`，以避免磁盘占用累积。
如需保留数据库文件，添加 `--keep-db`：

```bash
./scripts/retest_rocksdb_matrix.sh --keep-db
```

可通过 `--refill-policy` 控制是否每个 case 都重新 fill：

- `per_case`（默认）：每个 case 都重新 load + run
- `reuse_if_readonly`：只读 workload（如 `C`）首个 case load，后续 case 复用同一 DB 并跳过 load

示例（只读 workload 复用 fill）：

```bash
./scripts/retest_rocksdb_matrix.sh --workloads C --refill-policy reuse_if_readonly
```

可单独设置 fill 阶段线程数（例如统一 128）：

```bash
./scripts/retest_rocksdb_matrix.sh --load-threads 128
```

这样 transaction 阶段仍按 `--threads` 矩阵执行，只有 load/fill 阶段固定为 128 线程。
若不指定 `--load-threads`，默认即为 `128`。

关键指标包含：

- `cache_hit_ratio`（来自 RocksDB 统计）
- `cache_hit_ratio` 在每次 run 前重置统计，仅反映 transaction 阶段
- `throughput_kops`
- `read_throughput_kops`
- `write_throughput_kops`
- `avg_latency_ms` / `read_avg_latency_ms` / `write_avg_latency_ms`
- `mlc_total_hit_ratio` 与 `mlc_metrics`（MLC 各层 lookups/hits/hit_ratio）
