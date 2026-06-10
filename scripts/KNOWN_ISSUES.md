# 遗留问题与边界点记录（RocksDB 缓存基准）

> 更新于 2026-06-10。配套文档：`README_rocksdb_matrix.md`。
> 涉及两个仓库：`/users/wzhzhu/rocksdb`（缓存实现）与 `/users/wzhzhu/YCSB-C-master`（基准框架）。

## 一、待验证 / 待办

1. **`insertorder=hashed` 全量回归未做**
   - 历史问题：hashed load + block cache 触发 `Corruption: Compaction sees out-of-order keys`，
     根因是 MLC 旧的 64 位前缀 level 编码造成 cache key 混叠，已用 17 字节扩展 key 方案修复并提交。
   - 现状：矩阵默认仍固定 `insertorder=ordered`（保守起见）。修复后尚未在 100GB 全量 load 下
     验证 hashed 路径，确认后可考虑恢复 hashed 或将其纳入矩阵维度。

2. **正式矩阵尚未在新默认配置下完整跑过**
   - 新默认：O_DIRECT（读 + flush/compaction）、`operationcount=20M`、
     分片 `lru/hcc/arc/cacheus=64`、MLC 不分片、wrapper 计数器跨阶段清零。
   - 之前所有结论（含"hit ratio 与吞吐倒挂"）均基于旧配置，需以新一轮全量矩阵为准。

3. **MLC + ARC/Cacheus 子缓存 + 分片的组合未审查**
   - 当前 MLC 方案只用 HCC 子缓存。若将来给 MLC 配 ARC/Cacheus 子缓存且开分片，
     需复核 `MultiLevelCacheAllocator` 对 `GetUsage()/GetCapacity()` 的消费口径
     （wrapper 的 usage 转发 backing 物理用量，可超逻辑容量 16 倍）。
   - 另外 MLC 的 `ResetStats()` 不会清零子缓存的 wrapper 计数器（当前无此组合，不影响）。

4. **SR-HCC `initial_countdown=0` 的语义近似**
   - 对常见读路径插入（`keep_ref=true`），countdown=0 在首次 Release 后效果等价于
     countdown=1，并未严格实现"零保护期"。当前接受该近似（仍具扫描抗性），
     若需严格语义需要改 ClockCache 的插入/释放协议。
   - 相关修复：`Unref(h, count=0)` 已改为 no-op（修复 `old_meta.GetRefcount() != 0`
     断言误杀，`clock_cache.cc`）。

5. **MLC allocator 的 YCSB 指标提供者在 ResetStats 后首窗失真**
   - `prev_lookups` 等状态存于 lambda 捕获，load→txn 清零 MLC 计数后首个 ~1s 观测窗
     的 delta 无效，`observed_history` 中的 load 残留约 5 轮后老化。影响极小，已接受。

## 二、Wrapper（ARC/Cacheus）分片设计的边界点

实现：`cache/sharded_wrapper_cache.*`、`cache/wrapper_cache_shard.h`，
`num_shard_bits=k>0` 时为 2^k 个策略 shard + 共享 backing（同样 2^k 原生分片）。

1. **析构期回调安全（继承自单实例设计）**
   - backing 的淘汰回调捕获 wrapper/router 的 `this`。安全性依赖于
     HCC/LRU 析构时只走 deleter、不触发 `eviction_callback_`（已核实成立）。
     若未来更换 backing 实现，需重新确认这一点。

2. **`GetCapacity()` 取整**
   - 报告值为 `N x ceil(c/N)`，最多比逻辑容量多 N-1 字节（1GB/64 分片恰好整除）。
     仅影响展示，不影响淘汰。

3. **`StartAsyncLookup`/`WaitAll`（MultiGet 路径）绕过 wrapper 记账**
   - `CacheWrapper` 默认把异步查找直接转发给 backing，ARC/Cacheus 策略层不感知。
     单实例与分片模式行为一致；YCSB 只用 Get，不受影响。若以后用 MultiGet 压测，
     wrapper 命中率口径会偏。

4. **`GetUsage()` 返回 backing 物理用量**
   - backing 池为逻辑容量的 16 倍 + 1MB（`ComputeBackingCapacity`），物理 usage
     可大于逻辑容量。wrapper 通过主动 `Erase` 维持驻留集 ≈ 逻辑容量。

5. **两层哈希相互独立**
   - wrapper shard 路由 seed（`hash_seed>=0 ? hash_seed : 0`）与 backing HCC 内部
     分片 seed 不同，"wrapper shard i" 与 "backing shard i" 无对应关系。
     对正确性无影响（handle 操作直达 backing），且去相关有利于负载均衡。

6. **Cacheus 各 shard 的 RNG seed 偏移**
   - `rng_seed + shard_index`，保证可复现且 shard 间去相关。

7. **锁序不变式（修改 wrapper 时必须维持）**
   - wrapper 持自身 `mu_` 期间，对 backing 只允许无锁操作（当前仅 `GetCharge`）；
     `Insert/Lookup/Erase/SetCapacity` 对 backing 的调用必须在锁外。
     违反会与淘汰回调（backing 上下文 → 取 wrapper 锁）形成死锁环。

## 三、指标口径说明

1. **`cache_hit_ratio` 是全块类型混合口径**
   - 每次 Get 约 3 次块查找，其中 filter/index 块命中率接近 100%，会把混合口径
     抬高约 2/3 权重。策略差异看 `cache_data_hit_ratio`（已拆分输出
     `BLOCK_CACHE_{DATA,FILTER,INDEX}_HIT/MISS`）。

2. **wrapper 计数器口径（已修复）**
   - 2026-06-10 起 `ResetStats()` 会清零 ARC/Cacheus 的 wrapper 计数器
     （按 `Name()` 分发到 `ARCCache`/`CacheusCache`/`ShardedWrapperCache`），
     `arc/cacheus_wrapper_hit_ratio` 仅反映事务阶段。
   - 历史教训：修复前计数器混入 load 阶段 compaction 流量，曾造成
     "Cacheus 分片后 hit ratio 反升、ARC 反降"的假象（修复后 s0/s6 几乎一致）。
     解读旧结果目录时注意此口径差异。

3. **`read_attempt` vs `read_success`**
   - 分别基于尝试次数与 `*_ok_ops`；失败场景下两者会分叉。

4. **O_DIRECT 的覆盖范围**
   - `use_direct_reads`：用户读（block cache miss 后的 SST 读取）；
   - `use_direct_io_for_flush_and_compaction`：flush 写 + compaction 读写；
   - WAL 始终 buffered，不受以上两项控制。

## 四、性能解读备忘

1. **单分片（s0）下 LRU 吞吐被全局锁封顶**（64 线程约 74 kops，与 hit ratio 无关）；
   锁是串行资源、NVMe 是并行资源，瓶颈比较的是吞吐上限而非单次开销。
   64 分片后瓶颈回到 I/O，hit ratio 才能兑现成吞吐。

2. **HCC 1GB→2GB 吞吐超线性提升**的原因：小容量下 filter/index 块也被挤出，
   一次 Get 串行多次 NVMe 往返；2GB 后元数据块稳定驻留。

3. **分片对 MLC 默认关闭**的理由：allocator 给浅层的容量本就小，再切 64 份会放大
   碎片化与元数据开销；每秒 SetCapacity 的调整粒度也会被取整噪声淹没；
   流量已按 level 分散，单层竞争小。需要时可用 `--shard-bits 0,6` 显式对照。

4. **zipfian(0.99) + 1 亿 key 极度倾斜**：1% 容量即可获得较高 data 块命中率（~31%），
   高 hit ratio 不代表测试无效，需结合 `cache_data_hit_ratio` 判断。
