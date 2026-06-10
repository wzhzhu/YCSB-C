# RocksDB Cache Matrix Benchmark

## 默认矩阵（与当前需求一致）

- 工作负载：`A,B,C,D,E,F`
- 预填充：`100GB`（按 `1024B/KV` 近似换算为 `recordcount=104857600`）
- 事务数：`operationcount=20000000`（1M 只能测到冷启动预热瞬态，20M 才能进入稳态）
- block cache：`1GB,2GB,4GB,8GB`
- 线程数：`8,16,32,64,128`
- 对比策略：`lru,hcc,arc,cacheus,mlc_hcc_sr_bottom,mlc_hcc_all_levels,mlc_hcc_dynamic_srhcc,mlc_hcc_sr_bottom_deff,mlc_hcc_all_levels_deff,mlc_hcc_dynamic_srhcc_deff`
- 其他固定项：
  - `insertorder=ordered`（默认固定为顺序 key，以规避当前分支上 hashed load 触发的 compaction 异常）
  - `rocksdb.raw_kv_mode=true`
  - `rocksdb.raw_key_size_bytes=24`
  - `rocksdb.raw_value_size_bytes=1000`
  - `rocksdb.target_file_size_base=64MB`
  - `rocksdb.write_buffer_size=64MB`
  - `rocksdb.bloom_bits_per_key=10`
  - `rocksdb.cache_index_and_filter_blocks=true`
  - `rocksdb.cache_index_and_filter_blocks_with_high_priority=true`
  - `rocksdb.cache_numshardbits=0`（所有方案不分片）
  - `rocksdb.use_direct_reads=true` / `rocksdb.use_direct_io_for_flush_and_compaction=true`
    （O_DIRECT 绕过 page cache：250GB 内存远大于 100GB 数据时，buffered 读会让
    miss 几乎免费，吞吐与 hit ratio 脱钩，且复用 DB 的 case 之间互相焐热缓存）

> 说明 1：`mlc_hcc_sr_bottom` 对应“上层 HCC，仅最下层 SR-HCC”，通过  
> `rocksdb.cache_type=hyper_clock_cache` + `rocksdb.use_multi_level_cache=true` + `rocksdb.multi_level_cache_srhcc_start_level=6`（7层）实现。
>
> 说明 1b：`mlc_hcc_all_levels` 对应“MLC 所有层都是 HCC（无 SR-HCC）”，通过  
> `rocksdb.cache_type=hyper_clock_cache` + `rocksdb.use_multi_level_cache=true` + `rocksdb.multi_level_cache_srhcc_start_level=-1` 实现。
>
> 说明 1c：`mlc_hcc_dynamic_srhcc` 对应“MLC 默认全层 HCC，并按每层 scan 信号动态切换到 SR-HCC 风格（probation_insert）”。
>
> 说明 1d：`mlc_hcc_*_deff` 是对应基础方案的 D_eff 变体
> （`rocksdb.multi_level_cache_use_effective_data_size=true`）：allocator 用
> 每层的有效工作集估计 `D_eff = -(alpha*c)/ln(1-hit)`（带置信度混合与 EMA
> 平滑，截断在 `[0.01*D_raw, D_raw]`）替代原始层数据量 D_raw 参与模型求解。
> 实现移植自 db_bench（`--multi_level_cache_use_effective_data_size`）。
> 当前默认所有 MLC 方案均开启 allocator 自动调节：
> 默认策略参数：
> - `rocksdb.multi_level_cache_auto_adjust=true`
> - `rocksdb.multi_level_cache_allocator_mode=model`
> - `rocksdb.multi_level_cache_adjust_interval_ms=1000`
> - `rocksdb.multi_level_cache_alpha_estimator=robust_hit_rate`（与 db_bench 对齐：
>   按观测 (capacity, hit_rate) 精确反演 `alpha = -(D/c)*ln(1-hit)`，带置信度
>   收缩与 EMA 平滑，截断在 `[alpha_floor=0.1, alpha_max=100]`；
>   默认的 constant_one 均匀模型会在小预算下把数据量巨大的底层容量分配为 0，
>   例如 1-2GB 预算时承载 84% 流量的 L6 被分配 0 字节）
> - `rocksdb.multi_level_cache_dynamic_srhcc_enable=true`
> - `rocksdb.multi_level_cache_dynamic_srhcc_check_interval_ops=4096`
> - `rocksdb.multi_level_cache_dynamic_srhcc_min_samples=12288`
> - `rocksdb.multi_level_cache_dynamic_srhcc_sample_rate_log2=0`（全采样）
> - `rocksdb.multi_level_cache_dynamic_srhcc_poll_interval_ms=100`
> - `rocksdb.multi_level_cache_dynamic_srhcc_unique_ratio_enable_threshold=0.50`
> - `rocksdb.multi_level_cache_dynamic_srhcc_unique_ratio_disable_threshold=0.30`
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

可通过 `--insert-order` 切换 key 顺序（默认 `ordered`）：

```bash
./scripts/retest_rocksdb_matrix.sh --insert-order ordered
```

若确需验证 hashed key，可显式指定：

```bash
./scripts/retest_rocksdb_matrix.sh --insert-order hashed
```

关键指标包含：

- `cache_hit_ratio`（统一主列；对于 `arc/cacheus` 优先采用 wrapper 口径）
- `cache_data_hit_ratio` / `cache_filter_hit_ratio` / `cache_index_hit_ratio`
  （按块类型拆分的命中率，分别来自 `BLOCK_CACHE_{DATA,FILTER,INDEX}_HIT/MISS`；
  filter/index 块极热、命中率接近 1，会抬高混合口径的 `cache_hit_ratio`，
  各策略的真实差异主要体现在 `cache_data_hit_ratio`）
- `cache_data_hit/miss`、`cache_filter_hit/miss`、`cache_index_hit/miss`（原始计数）
- `backing_cache_hit_ratio`（RocksDB 全局 `BLOCK_CACHE_HIT/MISS` 口径）
- `arc_wrapper_hit_ratio` / `cacheus_wrapper_hit_ratio`（wrapper 策略口径）
- 上述命中率统计均在每次 run 前重置，仅反映 transaction 阶段
- `read_attempt_kops` / `write_attempt_kops`
- `read_success_kops` / `write_success_kops`
- `read_ops` / `write_ops`（尝试次数）
- `read_ok_ops` / `write_ok_ops`（成功次数）
- `avg_latency_ms` / `read_avg_latency_ms` / `write_avg_latency_ms`
- `mlc_total_hit_ratio`
- `mlc_l0~l6_hit_ratio`
- `mlc_l0~l6_probation_insert`（0=HCC, 1=SR-HCC 风格）
- `mlc_metrics`（MLC 各层 lookups/hits/hit_ratio/capacity/usage/data_size
  与 mode 的完整键值串，用于诊断 allocator 实际给每层分配的容量）
