# 遗留问题与边界点记录（RocksDB 缓存基准）

> 更新于 2026-06-10。配套文档：`README_rocksdb_matrix.md`、
> `EXPERIMENT_PLAN.md`（正式实验的双操作点设计与 pilot 计划）。
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

6. **MLC `robust_hit_rate`/`D_eff` 的 alpha 反演基于混合块类型命中率**
   - 每层 `lookups_/hits_`（`multi_level_cache.cc` 219/241 行）不区分
     data/filter/index 块，alpha 反演 `alpha = -(D/c)*ln(1-h)` 中 `D` 是该层
     data 字节数，`h` 却混入了元数据查找——存在模型失配。
   - 偏差对各层不均匀：一次 Get 在多个层探查 bloom filter（常驻后命中率≈1），
     但只在命中层读 data 块，因此浅层（key 大多不在）的混合 `h` 被抬得更高，
     alpha 被高估，allocator 倾向于过配浅层；L6 的 `h` 更接近真实 data 命中率。
   - 可能的细化：利用 Lookup 的 `CacheItemHelper::role` 维护每层分角色计数，
     allocator 用 data-only 命中率建模、元数据字节单独按"全驻留"预留。
   - 处置：先观察新一轮矩阵的 `mlc_level_N_capacity/usage`，若浅层被系统性
     过配再实施细化。

7. **1GB 小容量下分片（s6）疑似比未分片（s0）慢 2~3 倍，待受控 A/B 定性**
   - 现象：`probe-20260610-074612`（s6）中 wlC/t64/1GB 档 LRU≈39、HCC≈34 kops，
     而更早的 `direct-probe-20260610-002926`（s0）同档 LRU≈74、HCC≈120 kops；
     miss 总量仅 +9%（14.7M vs 13.5M），等效 NVMe IOPS 却从 ~50K 掉到 ~29K。
     2GB 及以上无此问题（s6 大幅占优，LRU 412 vs 74 kops）。
   - 注意：这是跨 run 对比，每个 case 独立 fill 100GB，事务阶段与 load 后遗留
     compaction 的重叠程度不可控，尚不能归因于分片本身。
   - 主嫌疑（按字节定量，已取证 table properties）：index block 是巨块
     （monolithic，~590KB/SST；filter ~82KB/SST；1600 SST → index 总量 ~0.92GB）。
     s6-1GB-LRU 的 miss 字节分解：index 1.39M×590KB≈818GB（~1.6GB/s）、
     filter 1.04M×82KB≈86GB、data 12.28M×4KB≈49GB——index miss 仅占 miss
     条数 9.4%，却占读字节 ~86%。设备从 IOPS-bound（s0 纯 4KB 读 ~50K IOPS
     ≈0.2GB/s）翻转为 bandwidth-bound（~1.9GB/s 需求），所有方案被同一带宽墙
     压平在 33-39 kops。机制：64×16MB 分片下单个 index block 占分片 3.7%，
     哈希不均 + 大块淘汰颗粒度加剧本就放不下的 index 工作集颠簸。
   - 保留项：旧 s0 log 无分类型指标（二进制早于该功能），s0 的 index miss 量
     只能反推（约 s6 的一半，带宽差 ~2 倍，与吞吐差量级吻合）。
   - 待办：MLC case 跑完后，单独跑 `--workloads C --threads 64 --cache-gb 1
     --schemes lru,hcc --shard-bits 0,6` 的同 run 对照（新二进制两边都有
     分类型指标）。若确认，可评估 partitioned index/filter（把 miss 颗粒度
     从 ~590KB 降到块级）或元数据高优先级 pool。
   - 背景：1GB 档本身处于"元数据颠簸区"（filter+index 工作集约 1.05GB，
     filter/index 命中率掉到 0.948/0.931，每个 Get 平均 ~0.74 次串行 NVMe 读），
     该档测的是元数据 miss 链下的 I/O 能力，方案间策略差异基本不可比。
   - **缓解决策（2026-06-12）**：主矩阵改为元数据不进 block cache
     （EXPERIMENT_PLAN 二点五），元数据墙消失，1GB 档恢复为有效的
     高压策略对比点；本条受控 A/B 待办仅对 metadata-in-cache 辅助臂
     仍有意义。

8. **ARC 8GB 吞吐（257 kops）反而低于 4GB（296 kops），待确认是否噪声**
   - 同一 run（probe-20260610-074612, s6）内，hit ratio 单调上升
     （0.891→0.901）但吞吐回落。可能是 wrapper 记账（entries/ghost/tombstone）
     随容量增大的开销，也可能是单次噪声，需 repeat 验证。
   - 另注：ARC 的 data_hit_ratio 在同容量下系统性高于 LRU/HCC
     （2GB：0.623 vs 0.581/0.571），策略价值已可见；但吞吐只有 LRU/HCC 的
     60~70%，wrapper 每操作开销仍可观。

9. **Cacheus 大容量段命中率反转为全场最差，待定位**
   - 现象（probe-20260610-074612, wlC/t64/s6 的 data_hit_ratio）：
     1GB 0.429、2GB 0.626 时为四方案最优（高压区自适应优势成立），
     但曲线快速饱和（4GB 0.661 < ARC 0.674），8GB 仅 0.678，
     **低于 LRU 0.704 / HCC 0.708 / ARC 0.705 约 3pp**。
   - "收敛到持平"符合自适应策略规律，"反转为负"超出预期。
   - **机制归因（2026-06-12 代码审查 cacheus_cache.cc）**：小容量赢的
     机器在大容量变成负担——
     (a) SR-LRU 试用区 Q 是准入瓶颈：新块一律进 Q（初始 capacity/100，
     8GB/64shard 下 ≈1.3MB≈320 个 data 块），须在 Q 内被再次命中才晋升 S
     （TouchOnHit→PromoteToS）；zipfian 中尾块复用间隔 > Q 驻留窗口，
     只能走"出 Q→进 history→再 miss 一次借 force_to_s 进 S"，安家成本
     2 次 miss（LRU 仅 1 次），边际容量被长驻 S 的头部 + 空转的 Q 占用；
     (b) 学习信号失明：权重仅在 ghost 命中时更新（纯惩罚制），只能看见
     "逐错了"，看不见大容量下的主要错误"该收留的中尾块没收留"（这类块
     从 history 滚出后再回来，无任何信号），学习器无从纠偏；
     (c) LFU 专家 freq 永不衰减，长驻块几乎不可能被 LFU 侧逐出，
     固化"S 锁死头部、尾部空转"的平衡。
   - 注：命中率结论与编译优化无关，该现象在 Release 构建下会原样保留。
   - 尚未排除：容量利用不足（类似 ARC 旧 bug）。障碍：YCSB 日志只有聚合
     lookups/hits，`ShardedWrapperCache::GetPrintableOptions` 未聚合
     resident usage / LRU-LFU 权重等内部状态。
   - 待办：(a) 给 router 的 stats 聚合补 usage（或跑 8GB 单实例 s0 读
     单体 printable options）；(b) 8GB 档复测定位。

10. **Cacheus wrapper 每操作成本为四方案最重**
    - 单 op 时延（64 线程换算）：Cacheus 310~380µs vs ARC 215~250µs vs
      LRU/HCC ~150µs。64 分片已消除锁争用，剩余为每操作记账本身
      （双历史链表、权重更新、采样决策），比 ARC 贵约 80~130µs。
    - 待办：若需要 Cacheus 在高命中端可比，需 profile 记账热点；
      否则在论文叙事中作为"wrapper 复杂度代价"的数据点呈现。

11. **Cacheus 4GB 吞吐凹陷（169 < 207@2GB，hit 升吞吐降）**
    - 与 ARC 8GB 凹陷（一.8）同款形态，单 repeat + 遗留 compaction
      噪声暂不定论；若正式矩阵复现，怀疑方向：记账结构规模 ∝ 容量
      （历史表/链表随容量增长）带来的每操作成本上升。

12. **【高优先级】MLC level 路由标签在 trivial move 后失效，本轮 MLC 结果
    不代表设计意图**
    - 证据（probe-20260610-074612, sr_bottom/8GB 的 per-level 统计）：
      真实 LSM 形态 L6=96.9GB/L5=9.7GB（占 ~99%），但 L6 仅收到 0.34% 的
      lookup（207K/60M）、被分配 138KB 容量；L0/L3/L4 的 usage 达到
      **本层全部数据量的 4~6 倍**（物理不可能，如 L3 数据 487MB、
      usage 3.08GB）——热数据块实际被标记并路由到了"L3/L4"子缓存。
    - 根因：cache key 的 level tag 取自 `rep_->level`
      （`block_based_table_reader.cc` BuildExtendedCacheLookupKey 调用处），
      该值在 table reader 创建时冻结；`TableCache::FindTable` 按 file number
      缓存 reader，level 仅在首开时生效。**trivial move 改层级不改
      file number、不重开 reader** → 标签停留在文件出生层。
      `insertorder=ordered` 的顺序灌库几乎全走 trivial move，放大到极致。
    - 影响：allocator 用真实 per-level 数据量 × 虚假 per-level 流量建模，
      alpha 失真；SR-HCC 层位放置作用于错误对象；MLC 退化为"按出生层
      切分的几个大缓存"。lookup/insert 两侧标签一致，**正确性无恙**。
      wlC 的 MLC 命中率/吞吐结论（含承重假设检验）全部作废待重测。
    - **修复（2026-06-12 已实施，待全量验证）**：`Rep` 新增
      `std::atomic<int> cache_key_level`（open 时初始化为 level），
      `TableCache::FindTable`（所有 Get/MultiGet/iterator 的公共漏斗，
      含 pinned fast path）在每次访问时用调用方传入的当前 level 惰性刷新
      （load+compare，仅变化时 store；level<0 不更新）；四处
      `BuildExtendedCacheLookupKey` 调用改用 atomic 值。新增
      `TableReader::UpdateCacheKeyLevel` 虚函数（默认 no-op）。
      改动文件：`table/table_reader.h`、`table/block_based/
      block_based_table_reader.{h,cc}`、`db/table_cache.cc`。
    - 冒烟验证：4M 记录小库，(a) 未排空时 L0 占 67% 数据/70% lookup，
      分布自洽且 usage ≤ 本层数据量；(b) `ldb compact` 推平到 L6-only 后
      6.0M/6.0M lookup 全部标记 L6，allocator 容量全数给 L6。
      ~~trivial move 在线刷新场景需全量 100GB ordered 重跑确认~~
      **已确认（release-calib-20260612-020710）：L6 lookup 占比
      0.34%→89.7%（53.8M/60.0M），L5 8.9%，与 LSM 形态（L6≈97GB）自洽；
      allocator 给 L6 89.5% 容量；usage ≤ data_size 各层成立。本条闭环。**
    - 残留边界：tag 变化瞬间，旧 tag 下已缓存的块成为暂态垃圾
      （等待自然淘汰，与修复前行为相同，无正确性问题）——
      **注意：该"暂态"在排空层上会变成永久滞留，已升级为一.15**；
      `EraseFromCache` 用新 tag 找不到旧 tag 条目，同样暂态。
    - 正面副产物：1GB 档 MLC 吞吐 49~81 kops 全面领先单体（34~39），
      index 命中率 0.976 vs 0.931（巨块 miss 少 2.8 倍），独立佐证一.7
      的带宽墙理论。

13. **【高优先级】基准一直链接的是 Debug（-O0 -g、断言开启）版 librocksdb**
    - 发现（2026-06-12）：`ycsbc` 运行时链接
      `rocksdb/build-tests/librocksdb.so.11`，该构建
      `CMAKE_BUILD_TYPE=Debug`（无任何 -O 优化、未定义 NDEBUG、
      RTTI 开启）。此前 clock_cache 断言能触发也源于此。
    - 影响：所有绝对吞吐与标定常数（如软件路径 C≈140µs）都在未优化
      代码上测得，显著偏大；"C 主导、命中率不值钱"的操作点判断在
      -O3 下会向 I/O 侧移动（C 缩小数倍），锁/无锁的相对差距与
      各方案排序也可能变化。趋势性结论方向大概率保持，但
      **所有定量结论需在优化构建下重新标定**。
    - **修复（2026-06-12 已实施）**：Release（-O3 -march=native -DNDEBUG）
      构建放在 `/mnt/rocksdb_nvme/fio/build/rocksdb-release`（根分区 16G 已满，
      放不下第二棵构建树）；YCSB 三个 Makefile 同步加 `-O2 -DNDEBUG`、
      rpath 改指 release 库（默认值已固化进 Makefile）。期间修复
      `multi_level_cache.cc` 两处 `dynamic_cast`（Release 默认关 RTTI 编不过），
      改为 `Name()` strcmp + static_cast 分发。
    - 冒烟（2M 记录小库, t16, 256MB cache, O_DIRECT）：LRU 442 KTPS、
      MLC(dynamic SR-HCC) 417 KTPS，均含 0.91 命中率；单 op 时延
      16/442k ≈ **36µs**（vs Debug 标定 C≈140µs，缩小约 4 倍，量级符合预期）。
      MLC 的 per-level 统计/allocator/动态 SR-HCC 分发在无 RTTI 下工作正常。
    - 残余待办：用 release 构建重跑 100GB 标定（C、r、各档吞吐），
      更新 EXPERIMENT_PLAN 模型常数；正式实验一律用 release 构建
      （`build-tests` 仅留作调试断言用途）。

14. **MLC 吞吐落后单体（probe 轮 8GB：226 vs LRU 425 kops）的归因分解**
    - 单 op 时延差 131µs（282 vs 151µs@t64），其中 **I/O 侧只占 ~5µs**：
      miss 条数几乎相同（6.26M vs 5.93M）；MLC 多付的元数据 miss
      （filter/index 命中 0.9973 vs 0.99999，多 ~107K 次巨块 miss，
      按字节是 LRU 的 2.5 倍读量）只命中 0.54% 的 Get，期望 ~4µs/op。
    - **主体 ~125µs 是软件路径被 Debug(-O0) 放大**：扩展 key 构造、
      level 路由、CacheWrapper 双层虚调用、per-level 原子统计
      （level-tag bug 使 84% 流量集中打 L3/L4 两个计数器缓存行）×
      每 Get ~3 次 lookup。Release 冒烟对照：同 0.91 命中率下 MLC 仅比
      LRU 慢 5.7%（+2.4µs/op），证实该机器本身在 -O3 下只值几 µs。
    - Release 重跑后需盯的残余结构因素：
      (a) allocator 每秒 SetCapacity 震荡赶走热元数据（sr_bottom 自证：
      4GB 元数据命中 0.9997→360kops，8GB 掉到 0.9973→226kops，
      吞吐跟元数据 miss 走而非 data hit）；
      (b) 被饿死层放不下一个 index 块（L6 容量 138KB < index 590KB，
      命中率 0.237）——allocator 需要"至少容纳本层 index+filter"的保底；
      (c) 每层 HCC 单分片 + per-level 统计计数器争用（冒烟仅 t16，
      高吞吐下会重新显现）。
    - 反向佐证：1GB 档 MLC（49~81 kops）领先单体（34~39），因按层切分
      恰好让巨块元数据驻留（index hit 0.976 vs 0.931）。本组实验中
      MLC 吞吐主要由"巨块元数据住在哪"决定。
    - **缓解决策（2026-06-12）**：主矩阵改 `cache_index_and_filter_blocks=
      false`（元数据驻留 reader 堆内存，见 EXPERIMENT_PLAN 二点五），
      (a)(b) 因此对主矩阵不再适用；metadata-in-cache 辅助臂若复测，
      再回头处理 allocator 保底容量。
    - **Release 验证闭环（release-calib-20260612-020710）**：MLC vs HCC
      差距 -47% → **-3%（8GB: 858 vs 884）/ -7%（2GB: 684 vs 756）**，
      证实主体确为 -O0 放大的软件路径；元数据命中率也回升到
      0.9996~0.9999（allocator 在 L6 主导下稳定，震荡消失）。

15. **【正式矩阵前需修】排空层子缓存的陈旧块永久滞留，MLC 实际内存
    超预算**
    - 证据（release-calib, sr_bottom/8GB）：L0 子缓存 capacity 9.7KB、
      本层 data_size=0（已排空），但 **usage=934MB**；各层 usage 总和
      9.52GB vs 总预算 8.59GB，**超 11%（~0.93GB）**。
    - 机制：填库刚结束时 L0 有流量，allocator 曾给过大容量，块灌入后
      L0 经 compaction 排空、tag 刷新（一.12 修复生效），新流量归零；
      而 HCC **只在 insert 路径上兑现淘汰**，SetCapacity 收缩只改数字
      不回收——没有新 insert 的子缓存里的块永远不会被逐出。
    - 影响：(a) 内存口径不公平（MLC 实际用 9.5GB 跑出"8GB"的成绩，
      虽然滞留块 hit 率极低、对命中率几乎无贡献，但对比试验的内存
      预算必须严格）；(b) 这部分预算本可分给 L6。
    - 对照：2GB 档无此现象（L0 仍有文件与流量，usage 5MB=capacity）。
    - 修复方向：allocator 收缩路径主动回收——对收缩幅度大或流量归零的
      子缓存触发 purge-to-capacity（HCC 需要暴露一个"淘汰到目标容量"
      的接口，或粗暴用 EraseUnRefEntries 清排空层）。

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
