# 遗留问题与边界点记录（RocksDB 缓存基准）

> 更新于 2026-06-10。配套文档：`README_rocksdb_matrix.md`、
> `EXPERIMENT_PLAN.md`（正式实验的双操作点设计与 pilot 计划）。
> 涉及两个仓库：`/users/wzhzhu/rocksdb`（缓存实现）与 `/users/wzhzhu/YCSB-C-master`（基准框架）。

## 一、待验证 / 待办

1. **`insertorder` 默认已切回 hashed（YCSB 标准）；100GB 全量 hashed 验证待补**
   - 历史问题：hashed load + block cache 触发 `Corruption: Compaction sees out-of-order keys`，
     根因是 MLC 旧的 64 位前缀 level 编码造成 cache key 混叠，已用 17 字节扩展 key 方案修复并提交。
   - **中等规模验证通过（hashed-smoke-061740，8M 记录、wlC、1GB、开 block cache，
     hcc/all_levels/dynamic 三方案）**：hashed 灌库 + 事务全程无 corruption，
     `out-of-order keys` 未复现 → 扩展 key 修复在 hashed 路径下成立。
   - **`ordered` 是过期 workaround、且系统性扭曲对比**：ordered 顺序灌库几乎全是
     trivial move，产出"过于干净、层间不重叠"的 LSM，data 集中 → 全局 HCC 占优；
     hashed 产出真实重叠 LSM，一次 Get 探多层 → MLC 按层缓存反而获益。同 8M/wlC/1GB：

     | 方案 | ordered hit | hashed hit |
     |---|---|---|
     | hcc | 0.707 | 0.539 |
     | mlc_hcc_all_levels | 0.651 | 0.598 |
     | mlc_hcc_dynamic_srhcc | 0.654 | 0.644 |

     ordered 下 HCC 赢、hashed 下 MLC 赢（dynamic +10pp）。故继续用 ordered 既偏离
     YCSB 默认、又对 MLC 不利且不真实。
   - **处置（2026-06-13）**：`run_rocksdb_matrix.py` 的 `COMMON_PROPS["insertorder"]`
     与 `--insert-order` 默认均改为 `hashed`；`ordered` 保留作受控对照（`--insert-order
     ordered`）。
   - **待补**：100GB 全量 hashed load 的最终闭环验证（中等规模已证伪 corruption，
     全量为彻底确认；耗时较长，稍后做）。

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

9. **[已关闭 2026-06-13] Cacheus 大容量段命中率反转 —— 证伪，系测量噪声**
   - **结论**：确定性多 repeat YCSB 对照（determ-cacheus-20260613，YCSB_RNG_SEED=0，
     wlC/t64/s6，2/4/8GB ×3）证实 Cacheus 在所有容量都 **≥ LRU**，且随容量
     收敛、从不反转：

     | cache | LRU | Cacheus | Δ |
     |---|---|---|---|
     | 2GB | 0.6281 | 0.6703 | +4.2pp |
     | 4GB | 0.6736 | 0.6988 | +2.5pp |
     | 8GB | 0.7186 | 0.7267 | +0.8pp |

     三次 repeat 的 hit_ratio 一致到小数点后 4~5 位（RNG 修复后 trace 完全确定）。
     之前 probe 跑出的 8GB "−3pp 反转"确认为**多线程 RNG 数据竞争**导致不同方案
     看到不同 trace 的测量噪声（见本档"YCSB 随机性"条目，已于 6d9139a 修复）。
     微基准（DISABLED_*Diagnostic）与确定性 YCSB 两条独立证据吻合，Cacheus 策略
     本身无 bug。下方原始排查记录保留备查。
   - 吞吐侧（非本条问题）：Cacheus 命中率更高但吞吐比 LRU 低 10~18%
     （789/767/801 vs 873/933/960 kops），系 wrapper 簿记 + backing 二次查找开销。

   ---
   原始记录（probe-20260610-074612, wlC/t64/s6 的 data_hit_ratio）：
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

   - **结论（2026-06-13，受控微基准排查，rocksdb/cache/cacheus_cache_test.cc
     的 4 个 DISABLED 诊断用例）**：在 zipfian(α=0.99) 合成 trace 上，跨
     【纯策略 / 真实 Lookup-Insert 路径 / 均匀 16KB 块 / 变长 4–32KB 块 /
     s0 单实例 / s6 64 分片 / 0.5×–16× 覆盖】所有维度，**Cacheus 命中率始终
     ≥ LRU**，大容量处收敛为持平（最差 delta = -0.0001，纯噪声），从未出现
     有意义的负回退。
     - 容量利用不足假说被**证伪**：`RealPathDeepDump` 一度观察到影子 usage
       卡在 3071/20000（15%）、零逐出，疑似 bug；但 `BackingIsolation` 证明
       **裸 HCC（cap=1.37MB, charge=1）同样只装 3071 条目、命中 0.666**——
       根因是 HCC 每条目 ~445B 元数据开销，charge=1 时开销完全主导，与
       Cacheus 无关。改用真实块大小（≥8KB，开销占比 <5%）后该假象消失，
       Cacheus 影子容量正常填满、正常逐出。
     - 因此 (a)(b)(c) 三点机制归因**在合成 zipfian 上均未实测到可观测的命中率
       损失**；KNOWN_ISSUES 担心的 "Q 准入瓶颈致大容量反转" 没有被复现。
   - **重新定位 YCSB 8GB 回退（3–4pp）的可疑来源**（按优先级）：
     (1) **测量噪声 / 单次运行**：probe / mlcopt 均为单 repeat。
     (2) **多线程 RNG 数据竞争**（见本档 YCSB 随机性条目）：`utils::RandomDouble`
         共享 `std::default_random_engine`，多线程下 trace 非确定，
         不同方案的 run 可能看到不同请求序列，3–4pp 完全可由 trace 方差解释。
     (3) 真实 RocksDB 块访问序列 ≠ 纯 key zipfian（块内多 key、预取等）。
   - **建议**：先排除 (1)(2)——用单线程或固定 per-thread 种子做一次确定性、
     多 repeat 的 LRU vs Cacheus YCSB 对照；若回退在确定性条件下消失，则确认
     为噪声/trace 方差，Cacheus 策略本身无 bug，本条可关闭。
   - 备注（既有测试债，非本问题）：`cacheus_cache_test.cc` 中既有用例
     （BasicInsertLookup 等）用 4 字节 key，与 HCC backing 要求的 16 字节
     cache key 冲突，Debug 下触发 `clock_cache.h:1134` assert 中止。新增的
     诊断用例已改用 16 字节 key。建议后续统一修正既有用例的 key 长度。

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
    - **优化级别对齐（2026-06-13）**：三个 YCSB Makefile 由 `-O2` 提到 `-O3`，
      与 librocksdb（Release `-O3 -DNDEBUG`）一致。残留可辩护性事项（未做）：
      (a) 内存分配器仍是 glibc malloc（`WITH_JEMALLOC=OFF`），高线程下争用大、
      run 间噪声高（与 HCC 命中率非确定性互相叠加，见五.5），最终评测建议换
      jemalloc/tcmalloc 重标定；(b) release 用了 `-march=native`，跨机不可复现，
      方法学需注明。

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
    - **修复（2026-06-12 已实施，待 100GB 验证）**：HCC 新增
      `PurgeToCapacity()`（`BaseClockTable` 模板方法复用 insert 路径的
      `Evict` 机制，循环逐出至 usage ≤ capacity，封顶 64 轮防止与并发
      insert 病态交替；`ClockCacheShard`/`BaseHyperClockCache` 逐层透出）；
      `MultiLevelCache::ApplyCapacities` 在应用全部新容量后统一对各层 +
      shared pool 调用（先全量 SetCapacity 再 purge，避免误清正在扩容的
      层）。改动：`cache/clock_cache.{h,cc}`、`cache/multi_level_cache.cc`。
    - 冒烟（2M 小库）：L0 排空（0 lookups）时 usage 2.9MB ≤ capacity
      5.3MB，全部层 usage ≤ capacity 成立；squatting 未复现。
    - **100GB 验证通过（dataonly-calib-20260612-052307, sr_bottom/8GB）**：
      L0 capacity 9.3KB / usage 137KB（修复前 934MB，缩小 ~7000 倍；
      残余为 purge 间隙内被引用/新插入的块，占总预算 0.0016%）；
      各层 usage 总和 ≈ 8.589GB 与预算精确吻合。**本条闭环。**
    - 残留边界：purge 只裁到 capacity，低于 capacity 的陈旧块留待
      allocator 进一步收缩或自然淘汰（预算已守住，无伤）；被引用中的
      条目不可逐出，purge 对其放行（freed_count=0 即退出）。

16. **MLC 性能优化四件套（EXPERIMENT_PLAN 三点五，2026-06-12 实施）的
    边界点**
    - **handle 裸透传 + 地址区间反查 owner**：
      (a) 依赖 HCC slot 数组地址区间在 cache 生命周期内稳定——Fixed 表
      数组构造后不重分配（SetCapacity 不扩表，本就是 Fixed HCC 已知
      限制）、Auto 表登记整段保留 mmap（增长在段内），均已核实；
      若未来引入会 realloc 表的缓存实现，须改回包装或更新区间表；
      (b) 指针低位 tag 区分 WrappedHandle 与裸 handle，要求两类指针
      至少 2 字节对齐（堆分配 16B 对齐、slot 数组元素 ≥8B 对齐，成立）；
      (c) standalone（堆分配）handle 与非 HCC 子缓存仍走 WrappedHandle
      堆分配兜底（罕见路径，不在乎）。
    - **采样总开关**：`DrainLookupSamples` 的外部消费方（db_bench）依赖
      "调过 `SetLookupSampleRateLog2` 即开启"；若先 Configure(false) 再
      期待采样会失望（当前无此用法）。thread_local 采样计数使采样率仅
      统计意义成立（每线程独立相位），unique-ratio 用途无影响。
    - **计数器条带化**：stripe 绑定为进程级 thread_local（跨 MLC 实例
      共享 round-robin 计数），多实例场景只是分布不均、无正确性问题；
      读侧求和非原子快照（与旧实现一致的弱一致性）。
    - **allocator 反震荡**：收缩迟滞会让"该让出的容量"晚 3 轮到位，
      compaction 后的容量再平衡变慢（半衰逼近）；自适应间隔退避至
      8×interval 后，工作集突变的响应延迟最坏 ~8s（任一变更即复位）；
      被推迟的 shrink 同步裁掉其资助的 grow 以守预算，极端情况下 grow
      也被推迟 3 轮。参数均在 `MultiLevelAllocationOptions`（YCSB 暂未
      暴露 prop，用默认值）。
    - 冒烟通过（含 Debug 断言库），100GB t64 全量重跑待做——条带化与
      零分配的收益在 t64 高吞吐端才可量化。

17. **【已修复，根因级】MLC 用等分容量构造 Auto HCC 子缓存，mmap 段定死后
    allocator 大幅增容触发表 Grow 越界，堆损坏**
    - 现象（mlcopt-calib-20260612-092825）：sr_bottom 两档 summary 全 0
      （进程 abort）；2GB log 末尾 `corrupted size vs. prev_size in
      fastbins`，8GB log 空。256MB/2GB-data、t64、skipload 高压下稳定复现
      （8 轮崩 6~8 次），报错形态随机：`free(): invalid pointer`、
      `malloc(): smallbin/unsorted double linked list corrupted`、
      `malloc_consolidate(): invalid chunk size`——典型堆元数据被写坏。
    - 定位（A/B 逐项排除）：禁用裸透传 opt1 仍崩、禁用反震荡 opt4 仍崩、
      禁用 PurgeToCapacity 仍崩；唯独 `auto_adjust=false`（不跑 allocator、
      不调 SetCapacity）不崩 → 锁定到 allocator 的 SetCapacity 调用本身。
    - 根因：`AutoHyperClockTable` 的 slot 数组是 `MemMapping::AllocateLazyZeroed
      (sizeof(HandleImpl) * CalcMaxUsableLength(capacity, ...))`，**mmap 段大小
      在构造时由 capacity 定死，表 Grow 永远不能超出该段**。两条建 MLC 子缓存
      的路径都按 `level_capacity = total/7` 建 mmap：
      (a) YCSB `mixed_srhcc` 分支（`rocksdb_db.cc`，sr_bottom 走这条）
          `per_level.capacity = level_capacity`；
      (b) rocksdb `MakeSubCacheWithCapacity(hcc_options, level_capacity)`
          （`multi_level_cache.cc`，dynamic / 非 mixed 走这条）。
      allocator 把承重层 SetCapacity 抬到 ~0.89×total（约 6×起始），该层实际
      插入条目数超过初始 mmap 的 max-usable-length → `GrowIfNeeded`→`Grow`
      写到 mmap 段之外 → 堆损坏。SR-HCC（probation countdown=0）逐出更频繁、
      表周转更快，最先把表撑到越界临界，故 sr_bottom 必崩；dynamic 同 bug 但
      没每轮触到临界，**之前所有 MLC run（dataonly/release-calib 单轮）是侥幸
      没崩，dynamic 的历史数据存在 silent corruption 风险，一并作废重测**。
    - **修复（2026-06-13）**：两条路径都改为「mmap 按 total_capacity 保留，
      构造后 SetCapacity 到起始等分值」。
      (a) `MakeSubCacheWithCapacity(hcc_options, capacity, mmap_capacity)` 新增
          第三参数，mmap 用 total、随后 `cache->SetCapacity(capacity)`；两处
          调用传 total_capacity；
      (b) YCSB mixed_srhcc：`per_level.capacity = cache_capacity` 建 mmap，
          `sub->SetCapacity(level_capacity)` 设起始；shared 同样按 total 建、
          SetCapacity(0)。
      代价：每层 mmap 保留 `CalcMaxUsableLength(total)` 的**虚拟地址**
      （7 层 + shared，lazy-zeroed 物理按需 commit，48 位地址空间充足），起始
      slot 元数据略增，可忽略。
    - 验证：256MB/t64 高压 sr_bottom 8 轮 + dynamic 4 轮（release）+ sr_bottom
      4 轮（Debug 断言库）全部无崩溃（修复前 8 轮崩 6~8 次）。**本条闭环。**
    - 残留：Auto HCC 在「SetCapacity 增大超过构造 capacity」下不安全是
      RocksDB 上游的隐含约束（生产 block cache 很少大幅增容），MLC 是该约束的
      重度违反者；若未来给 MLC 配「运行时整体 SetCapacity 增大」需复核同一点。

18. **【可选，评测质量】把内存分配器从 glibc malloc 换成 jemalloc**
    - 现状：jemalloc 未安装（`ldconfig`/`dpkg` 均无），ycsbc 与 librocksdb 均
      链 glibc malloc（`WITH_JEMALLOC=OFF`）。
    - 收益（对症）：t64/t128 下 glibc arena 争用是吞吐 run 间噪声主源之一，
      jemalloc 多 arena + thread cache 降争用 → 方差更小、所需 repeat 更少；
      碎片更低 → 固定 cache budget 下实际 RSS 更贴近配置容量（可选再上
      `JemallocNodumpAllocator` 给 block cache 专用，容量口径更干净）；且是
      RocksDB 官方推荐配置，绝对数更可辩护。
    - **不能解决**：HCC 命中率非确定性（来自 sharded GCLOCK + 线程交织的算法层，
      见五.5）；也不会翻转策略对比结论（allocator 对所有方案等量施加，是共享
      混淆项，A/B 相对关系稳健）——故属"质量增益"而非"有效性必需"。
    - 成本：装 jemalloc（`apt install libjemalloc-dev` 或源码 build）→ 重链
      ycsbc / `WITH_JEMALLOC=ON` 重建 librocksdb → **换 allocator = rebaseline，
      旧绝对数作废**。
    - 落地路径：(a) 先 `LD_PRELOAD=.../libjemalloc.so ./ycsbc ...` 低成本试水量
      方差收益，不重建；(b) 确认有效再 `WITH_JEMALLOC=ON` 固化为正式评测标准配置。
    - **时机建议**：与"100GB 全量 hashed 重跑"（一.1 待补）合并成一次 rebaseline，
      避免分两次报废历史数据。

19. **YCSB-C 每线程确定性 RNG 改动的方法学辩护 + 多种子待办**
    - 改了什么：原 YCSB-C 是**所有线程共享一个无锁 `static std::default_random_engine`**，
      既是数据竞争（UB），又使 run 间 trace 随线程交织漂移、不可复现。改为每线程
      `thread_local` 独立 engine，种子 = `base_seed + 0x9E3779B97F4A7C15 × stream_id`
      确定性派生（`core/utils.h` `ThreadLocalRng()`，`YCSB_RNG_SEED` 环境变量可换种子）。
    - **可辩护性（这是加分项，非作弊）**：
      (a) 修的是真 UB —— 共享非原子 RNG 无可争议地错；
      (b) **对齐官方 YCSB** —— Java YCSB 本就每个 client 线程各自一个 Random，我们是
          拉回 canonical 设计而非偏离；
      (c) 固定种子是正当的方差削减（common random numbers / paired design），把"抽到
          哪批 key"这一无关变量消掉，使方案间差异只反映 policy 本身。
    - **必须说准的边界（避免 over-claim）**：
      (1) 固定的是**请求 trace（输入）**、与线程调度无关；**不**消除运行时并发效应——
          线程仍真实争用 cache/锁，吞吐照样反映真实并发（相同负载 + 真实并发）。
      (2) 因此 **HCC 命中率的 run 间抖动（五.5）不受此改动掩盖**——那是 HCC 运行时
          sharded GCLOCK + 线程交织的自身非确定性；我们没有把它藏起来。
      (3) 线程→stream 映射按"首次调用顺序"分配、run 间可变，但**全局访问多重集合一致**
          （流集合 {1..N} 与各流序列固定），对共享 cache 命中率而言负载严格对齐成立。
    - **待办（堵住"挑种子"质疑）**：最终结果用 **≥3 个 `YCSB_RNG_SEED`** 跑、报
      均值/区间，证明结论对种子稳健（机制已内置，仅需在矩阵脚本里加种子维度并汇总）。

20. **【已定位根因 + 已修复】MLC "吞吐 inversion"（4–8GB 吞吐腰斩）= compaction
    污染测量窗口，非 allocator/估计器缺陷；修复 = load→txn 边界等 compaction 收敛**
    - **现象（eval-C-seed1-0613，100GB，无收敛）**：MLC 各方案吞吐随缓存先升后崩——
      `all_levels` 2GB→738、**4GB→416、8GB→422** kops（命中率却单调升
      0.617→0.622→0.662）；`dynamic_srhcc` 4GB→407、8GB→387；`sr_bottom` 4/8GB→~503。
    - **根因（统计口径）**：load 阶段产生大量 compaction backlog（实测 ~130 个 L0
      文件），矩阵原先在 load 后立即进入 transaction（无收敛），backlog compaction
      持续跑进测量窗口。而 **compaction 的 block 读会被计入缓存与 allocator 统计**：
      数据块迭代器默认 `use_block_cache_for_lookup=true`（`block_based_table_iterator.cc:413`），
      compaction 读 block 仍调用 `MultiLevelCache::Lookup`（`multi_level_cache.cc:253`），
      无条件 `IncLookupCounter` + `MaybeRecordLookupSample`（260–261 行，**不区分调用者**，
      `Cache::Lookup` 接口拿不到 caller）；compaction 的 `ReadOptions.fill_cache=false`
      （`compaction_job.cc:1465`）→ 探测但不回填 → 大量冷块 miss。这些"读 L5 把 L5→L6"
      的顺序读把**源层的 lookup 量灌大、命中率压低**，污染 allocator 的 per-level
      模型 → 误配/抖动 → inversion。同时也污染各方案上报的 hit_ratio。
    - **修复**：在 `RocksdbDB::ResetStats()`（load→txn 边界，main 单次调用）按 db_bench
      的 `waitforcompaction` 配方等后台 compaction 收敛：**先 sleep 5s + 默认
      `WaitForCompactOptions()`（不 flush、不 purge）**。`rocksdb.wait_for_compact_before_transactions`
      已设为矩阵 COMMON_PROPS 默认 `true`（对所有方案公平、提升测量保真度）。
      - **坑（已避开）**：最初用 `flush=true`+`wait_for_purge=true` 会让 load+WFC
        同进程在 `delete db` 收尾处**死锁**（一个 `rocksdb:low` 线程 100% 自旋、主线程
        阻塞在 futex）。隔离确认：纯 skipload、skipload+WFC 均正常,仅 load+WFC 同进程触发。
        对齐 db_bench 默认 options 后消除（3M 实测 exit=0；100GB 全量重跑亦正常）。
    - **验证（eval-C-seed1-WFC-0614,100GB,带收敛,全量重基线)**：inversion 消失,
      全部恢复单调 scaling：`all_levels` 4GB **416→818（+96%）**、8GB **422→730（+73%）**；
      `dynamic_srhcc` 4GB 407→836、8GB 387→753；`sr_bottom` 4GB 504→817、8GB 502→857。
    - **结论**：(a) inversion 是测量伪象（compaction 污染 + 不稳定 LSM），robust 估计器
      无需"修"；(b) **db_bench 的 shadow_cache 双点估计器移植被放弃**——它本为修
      "robust 误配"而来,但 robust 没坏；且 shadow 自身有**自强化饿死陷阱**（scaled
      影子 LRU 锚定 1.5×当前容量,容量一小就量到边际收益≈0、无法外推,30GB/8GB 实测把
      L5 从 170MB 饿到 13.5KB,吞吐 645→241、命中 0.783→0.745 退步）。已从代码移除。
    - **残留观察（待多种子定性）**：带收敛后 `all_levels`/`dynamic` 在 8GB 仍比 4GB 略低
      （730<818、753<836,非崩溃级；`sr_bottom` 反而单调 817→857），疑为单实例 L6 轻度
      争用或单种子噪声。**同质 wlC 上 MLC 仍低于 HCC**（8GB：hcc 970/lru 951 vs MLC
      730–857,命中率相当 ~0.71–0.72）——诚实"代价"结论,与一.14 及 MLC 主场定位
      （异质/ISO,见五.5）一致。

21. **【已定位根因】残留"命中升、吞吐降"（4→8GB）= 单个未分片 AutoHCC 实例在
    高并发 + 大容量下的争用,与 MLC routing/allocator/估计器无关；治本 = 分片,
    probation 只是治标**
    - **背景**：一.20 用 WFC 消除了 compaction 污染导致的"灾难级 inversion"后,
      `all_levels`/`dynamic_srhcc` 在 100GB 仍残留一个温和凹陷（8GB 略低于 4GB,
      命中率却更高）,而 `sr_bottom` 单调。最初怀疑是单种子噪声或 allocator 残留缺陷。
    - **决定性隔离实验（hcc-shard-probe-0614,100GB,同一共享 DB,只变分片度）**：
      把 MLC 完全拿掉,直接测**全局单层 HCC** 在分片 vs 不分片下的标度：

      | scheme | shard | 4GB | 8GB | 趋势 |
      |---|---|---:|---:|---|
      | hcc **s0（1 shard,未分片）** | 1 | 895 | **677** | **凹陷 −24%** |
      | hcc s6（64 shard） | 64 | 920 | 941 | 单调 ✅ |

      命中率两档几乎相同（~0.674）。即**未分片的普通单层 HCC 自己就复现了凹陷**,
      与 MLC 毫无关系。
    - **根因**：单个 AutoHCC 实例虽"无全局锁",但其 **clock 指针推进、淘汰回收、
      表扩容协调** 在 64 线程并发下仍是集中争用点；容量越大（8GB→单实例管 ~7.6GB
      条目）争用开销越高,超过更高命中率省下的 I/O（且快 NVMe + 64 线程下 miss 的
      磁盘 I/O 被并发隐藏,几乎"免费",而 hit 全压在单实例上）→ 吞吐反降。RocksDB
      默认给全局 HCC 上 64 分片（s6）正是为此。
    - **映射到 MLC**：MLC 在 `COMMON_PROPS` 强制 `cache_numshardbits=0`,**每个子缓存
      只有 1 shard**；承载 ~89% lookup 的热层 L6 因此单 shard 化,得到与 hcc s0 完全
      一致的凹陷（all_levels 818→730）。`sr_bottom` 的 L6 开 `probation_insert=true`
      （SR-HCC,插入 countdown=0,淘汰回收近 O(1)）削减了单实例的淘汰争用,所以单调
      817→857——**这是治标,不是 allocator 修好了**。
      | | 4GB | 8GB | |
      |---|---:|---:|---|
      | MLC all_levels s0 | 818 | 730 | 凹陷（L6 单 shard,同病） |
      | MLC sr_bottom s0（probation） | 817 | 857 | 治标 |
      | hcc s6（分片） | 920 | 941 | 治本 |
    - **更正先前判断**：一.20 残留观察里"疑单种子噪声"被否定（s0 凹陷稳定可复现）；
      "HCC 无锁、分片影响不大"的旧假设也要修正——单实例 AutoHCC 在此并发/容量下分片
      收益显著（s0 677 vs s6 941 @8GB,+39%）。
    - **更深一层：根因是 AutoHCC 特有,不是泛化的"单实例未分片"（FixedHCC 实验,同 DB）**：
      把全局 HCC 从 Auto 换成 **Fixed**（`estimated_entry_charge=4096`）后,**即使不分片
      也单调且最快**：

      | 全局单层 HCC | shard | 4GB | 8GB | 趋势 |
      |---|---|---:|---:|---|
      | AutoHCC s0 | 1 | 895 | 677 | 凹陷 −24% |
      | AutoHCC s6 | 64 | 920 | 941 | 单调 |
      | **FixedHCC s0** | 1 | **947** | **999** | **单调,最快** |

      ⇒ 凹陷源于 **AutoHCC 的动态增长 + 链式表结构**在单实例大容量并发下退化；FixedHCC
      的扁平预分配开放寻址表无此问题。分片只是把退化摊薄（治标的另一形态）。
    - **FixedHCC 不能整体塞进 MLC（已实测 + 代码坐实）**：MLC 为 AutoHCC 的 mmap 增长把
      **每个子缓存表都按 `total_capacity` 建**（`multi_level_cache.cc:MakeSubCacheWithCapacity`）。
      FixedHCC 表构造时定死、`SetCapacity` 只调阈值,于是被 allocator 只分到一小块容量的
      层 → 表**极度稀疏**；FixedHCC `Evict()`（`clock_cache.cc:1161`）逐槽扫描、空槽也扫,
      上限 `3×整表槽数` → 每淘汰一块要扫几十空槽 → 64 线程在淘汰扫描里**自旋,实测慢 ~16×**。
      （全局单实例 FixedHCC 没事,因表≈容量、密集。）
    - **【已实现 + 已验证】修复 = 仅热层 L6 用 FixedHCC,上层留 AutoHCC**（方案
      `mlc_hcc_fixed_bottom`,prop `multi_level_cache_fixed_start_level=6`）：L6 被 allocator
      喂到 ~0.83–0.9×total,其"按 full 建"的 FixedHCC 表密集（load factor ~0.3,快）;
      上层 L0–L5 留 AutoHCC,天然容忍稀疏（惰性 mmap）。同一 100GB DB 实测：

      | MLC all_levels 变体 | 4GB | 8GB | 趋势 |
      |---|---:|---:|---|
      | AutoHCC s0（原默认） | 838 | 773 | 凹陷 −8% |
      | FixedHCC 全层 | 自旋 | 自旋 | 病态(已弃) |
      | **FixedHCC 仅 L6** | 862 | **891** | **单调 +3%；8GB 比 AutoHCC +15%** |

      也优于 `sr_bottom` 的 probation 治标（857@8GB）。
      实现：`rocksdb_db.cc` 的 per-level 构造路径在 `mixed_srhcc || mixed_fixed` 时启用,
      对 `level >= fixed_start_level` 设 `per_level.estimated_entry_charge = fixed_entry_charge`
      （→ FixedHCC）,其余保持 base 的 0（→ AutoHCC）。
    - **【最优方案，已验证】方向 1：每层仍 AutoHCC 但分片（`cache_numshardbits=-1` auto），
      反而全面优于 fixed_bottom**（方案 `mlc_hcc_all_levels_sharded`）。同一 100GB DB 全曲线：

      | MLC all_levels 变体 | 1GB | 2GB | 4GB | 8GB | 趋势 |
      |---|---:|---:|---:|---:|---|
      | AutoHCC s0（原默认） | 741 | 797 | 838 | 773 | 凹陷 |
      | FixedBottom（方向2，仅 L6 Fixed） | 740 | 815 | 862 | 891 | 单调 |
      | **AutoHCC auto-shard（方向1）** | 739 | 818 | 903 | **1029** | **单调,全程最快** |

      参照：全局单 cache hcc s6 @8GB = 941。**auto-shard 的 MLC @8GB 1029 反超全局单
      cache（+9%）**,且比 fixed_bottom +15%；1GB 739≈其他（**无小缓存退化**——`auto(-1)`
      按容量自适应分片数,小预算不会被切成 64 片,规避了一.7 的惩罚）。
      - **为何方向1 > 方向2**：分片作用于**所有层**(含 L6),把 AutoHCC 单实例争用全面摊开,
        且 auto 自适应分片数；fixed_bottom 只修 L6、上层仍未分片 AutoHCC,且 L6 FixedHCC 仍单实例。
      - **更正最初取舍**：当初选方向2 是因为(a)以为分片伤小缓存、(b)全局 FixedHCC 看着最快。
        但 `auto` 分片解决了(a),而 MLC 分层场景下全面分片(方向1)赢过仅底层 FixedHCC(方向2)。
      - **推荐配置 = `mlc_hcc_all_levels_sharded`（AutoHCC + auto 分片）**。fixed_bottom 作为
        次优/对照保留。两者均在 matrix 默认 `--schemes`。**单种子结论,待 100GB 全量多种子复核。**
    - **待办**：100GB 全量重跑（默认 schemes 已含 `all_levels`/`fixed_bottom`/`all_levels_sharded`）
      做正式对照 + 多种子定性,确认 auto-shard 推荐稳健。

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

## 五、MLC 命中率提升排查（2026-06-13）

1. **微基准可复现 ARC > LRU > HCC 的命中率排名**（rocksdb/cache/cacheus_cache_test.cc
   的 `DISABLED_PolicyCountdownDiagnostic`，zipfian α=0.99，真实 8KB 块，s0）：

   | 覆盖率 | LRU | HCC | ARC | Cacheus |
   |---|---|---|---|---|
   | 0.05 | 0.7047 | **0.6798** | 0.7272 | 0.7374 |
   | 0.10 | 0.7653 | 0.7490 | 0.7817 | 0.7901 |
   | 0.20 | 0.8296 | 0.8214 | 0.8363 | 0.8408 |
   | 0.40 | 0.8981 | 0.8970 | 0.8996 | 0.9034 |

   低覆盖区 HCC 不仅落后 ARC ~4.7pp，连纯 LRU 都比它高 ~2.5pp；差距随容量收敛。
   这正是 MLC 各层小预算所处区间，解释了 MLC（HCC 子缓存）在 wlC 上为何贴近
   LRU/HCC 而追不上 ARC。

2. **杠杆 A（加深无锁 GCLOCK countdown）实测基本无效**——已否决。
   把 `kMaxCountdown` 临时改成 env 可调（`ROCKSDB_HCC_MAX_COUNTDOWN`，仅原型，
   已撤销回常量 3），扫 3/5/7/10：

   | 覆盖率 | cd=3 | cd=5 | cd=7 | cd=10 |
   |---|---|---|---|---|
   | 0.05 | 0.6798 | 0.6817 | 0.6821 | 0.6825 |
   | 0.10 | 0.7490 | 0.7508 | 0.7512 | 0.7514 |

   低覆盖区仅 +0.27pp 且 cd=7 即饱和，远不足以补 4.7pp 的 ARC 差距。

3. **差距的机制分解**：
   - HCC < LRU（−2.5pp）：CLOCK 用 2-bit 计数 + 扫描序逐出，是 LRU 精确 recency
     顺序的有损近似；加深计数器属于"频率"轴，补不回"recency 精度"轴的损失。
   - LRU < ARC（−2.2pp）：ARC 的优势来自 B1/B2 幽灵链表对"刚逐出又回来"的块的
     自适应记忆与 recency/frequency 配比，GCLOCK 没有等价的逐出记忆机制。

4. **结论与方向**（同质负载 wlC 上）：
   - HCC/MLC 的命中率天花板≈LRU，结构性低于 ARC，且无法靠 countdown 调参补齐；
     不应继续在频率深度上投入。
   - 真正能补 ARC 差距的唯一无锁杠杆是给子缓存加**无锁幽灵/准入过滤**
     （原子数组实现的 counting-bloom / clock-ghost，复刻 ARC B1/B2 的逐出记忆），
     工程量较大，待评估收益后再决定是否做。
   - 更对路的是杠杆 B：把 MLC 的卖点定位为"无锁并发 + 分层"，命中率优势放到
     **异质负载**（wlD latest、wlE scan、hotspot）上兑现——此时按 level 隔离/特化
     能结构性超过全局缓存，而非在同质 wlC 上硬追 ARC。对应 EXPERIMENT_PLAN.md
     待实现的 hotspot/zipfian_const/request-dist 维度。

5. **杠杆 B 兑现：隔离负载（ISO）上 MLC 反超 HCC +7.7pp**（2026-06-13）
   - 负载 `workloads/workloadiso.spec`：80% read-`latest` + 20% scan，readonly，
     `scankeydistribution=uniform`（解耦 scan 起始键分布，见 e15aa2a）。
     读热集打浅层 L0（高复用），扫描打深层 L6（低复用、污染流）——制造
     "按层分离的污染流"，正是 MLC 的干净主场。
   - **data-size cap（rocksdb `MultiLevelAllocationOptions::cap_at_data_size`，
     默认开）**：指数 MRC 模型会给"热但小"的层（如 L0）远超其数据量的容量，
     surplus 空置而深层饿死。cap 把每层容量上界设为 `data_size × 1.10`
     （空层给 `empty_level_cap_bytes`=64KB），加了上界的注水解再把 surplus
     分给高边际层。改动：`cache/multi_level_cache_allocator.{h,cc}`。
   - 结果（iso-capfix2-053426，同一 DB，1GB/t16，2M ops）：

     | 方案 | hit_ratio | 吞吐 Kops/s |
     |---|---|---|
     | hcc | 0.616 | 145 |
     | mlc_hcc_all_levels | **0.693** | 150 |
     | mlc_hcc_sr_bottom | **0.693** | 143 |
     | mlc_hcc_dynamic_srhcc | **0.695** | 145 |

   - cap 在 per-level 上精确生效（all_levels）：L0 cap=95.5MB≈data(86.9MB)×1.10、
     命中 0.990（read-latest 完全隔离扫描污染）；L1–L4 空层各仅 204B（修复前
     每层浪费 ~143MB）；回收预算精确流向 L5(541MB/data 515MB,0.897)、
     L6(436MB/data 1462MB,0.338)。各层容量和 = 1GB。
   - **受控 A/B 验证（ab-iso-cap-055420 / ab-wlc-cap-055905，YCSB_RNG_SEED=0，
     同一 DB，新增 `rocksdb.multi_level_cache_cap_at_data_size` prop +
     `*_nocap` 方案变体，cap-on/off 两侧仅差此一标志、共用同一 release 库）**：
     - ISO @1GB 2 repeats，**cap 干净贡献 +2.7pp**（两方案一致，吞吐亦升）：

       | 方案 | cap-on | cap-off |
       |---|---|---|
       | all_levels | 0.6905 | 0.6633 |
       | dynamic_srhcc | 0.6922 | 0.6650 |

     - wlC（同质，8M 记录）@1/4GB，**cap-on≈cap-off，差异全在 ±0.0004 噪声内
       → 无回退**（cap 在无"热小层+空置 surplus"的负载上是 no-op）。故
       `cap_at_data_size` 默认开是安全的。
     - **附带发现：HCC 命中率 run 间非确定**——ISO @1GB 同一 DB、同种子，
       HCC 两次 repeat 0.6138 vs 0.6943（**抖 8pp**），系分片 GCLOCK 逐出对
       线程交错敏感；MLC（per-level、unsharded）则确定到小数点后 3 位。
       **教训：MLC-vs-HCC 对比必须给 HCC 多 repeat 取均值/置信区间**，
       单次 HCC 采样会严重误导（之前误判的"DB 方差"实为此）。
     - wlC 上 HCC 命中率仍 ≥ MLC（0.707/0.800 vs 0.651/0.786），与"同质负载
       HCC 全局缓存占优、MLC 主场在异质/隔离"一致，与 cap 无关。
   - **ISO 的方法学定位（呈现时务必如此措辞，避免被质疑挑负载）**：ISO 是
     **"机制演示型"负载**——刻意构造的、对 MLC 的 per-level 隔离有利的异质场景，
     用来证明"MLC 的按层隔离在抗扫描污染上有结构性优势（优势②）"，**不**用于
     声称"MLC 在所有负载都赢"。论文/汇报须**成对呈现**：(a) 同质负载（wlC/A）
     看 MLC 是否不掉队（代价分析，此处 HCC 占优是诚实结论）；(b) 异质/隔离负载
     （ISO）看 MLC 独有优势何时兑现。"主场 + 通用场"成对才站得住。

6. **【教训】所谓"AutoHCC 动态调容堆损坏"= ABI 不匹配，非 AutoHCC bug**
   （2026-06-13，闭环；一.17 的"mmap 越界"是另一独立问题，已闭环）
   - 现象：上 data-size cap 后，`mlc_hcc_all_levels`/`sr_bottom` 启动即
     `corrupted size vs. prev_size`，summary 全 0。曾误判为 AutoHCC 的
     `PurgeToCapacity`/`Evict` 后台并发或动态 SR-HCC 切换的堆损坏。
   - 定位：(a) 并发压力测试（直接 hammer `AdjustCapacities`+purge+真实 deleter）
     反复跑均**干净**；(b) gdb 抓真实崩溃栈落在 **`CoreWorkload::Init`(主线程、
     启动期)**，`executed_ops=0`，事务线程未起——**非事务期并发**；
     (c) ASAN 库精确定位：`heap-buffer-overflow WRITE size 8` 于
     **`MultiLevelCacheAllocator` 构造函数**，写在 344B 对象右侧 0 字节处，
     分配点 `rocksdb_db.cc:539` 的 `make_unique<MultiLevelCacheAllocator>`。
   - 根因：给 `.h` 的 `MultiLevelAllocationOptions` 加字段后
     `sizeof(MultiLevelCacheAllocator)` 变大，但 **`ycsbc` 二进制是改头之前编的**，
     `operator new` 按旧 size 分配、`.so` 构造函数按新 size 写成员 → 越界。
     单测从不复现是因为测试与库同次构建、头/库一致；只有独立编译、头文件
     过期的 `ycsbc` 才崩。
   - 修复：重编 `ycsbc`（让 `rocksdb_db.cc` 用新头）→ ASAN 零报错跑完 2M ops。
   - **铁律**：凡改 `rocksdb` 中被 `rocksdb_db.cc` 直接实例化的公共头
     （尤其 `multi_level_cache_allocator.h` 的 struct/类布局），**必须连带重编
     `ycsbc`**（`make` 不追踪头依赖，需 `touch db/rocksdb_db.cc` 或 `make clean`）。
     否则任何 sizeof 变化都会以"随机堆损坏"形式爆雷，极易误导为缓存并发 bug。

7. **【兑现 五.4 预言】无锁频次准入（W-TinyLFU on CLOCK）让 HCC/MLC 命中率越过
   LRU、在大缓存追平 ARC/Cacheus**（2026-06-14）
   - 五.4 曾断言："真正能补 ARC 差距的唯一无锁杠杆是给子缓存加无锁幽灵/准入
     过滤"。本节即实现并验证之，且**不牺牲 HCC 的无锁特性**。
   - **实现**（`cache/clock_cache.{h,cc}` + `include/rocksdb/cache.h`）：
     - `FrequencySketch`：每 shard 一个无锁 Count-Min sketch（4 行 × w 字节，
       全 relaxed 原子，竞争只丢计数；按 `width×10` 累计触发单线程减半 aging）。
       仅在开启时分配，索引直接取 `hashed_key[0/1]` 高低 32 位。
     - **方案 B（`frequency_aware_admission`，弱）**：只在 insert(miss) 计数，按
       频次选新块初始 CLOCK countdown（冷→0/温→1/热→2）。**Lookup 热路径零改动**。
     - **方案 A（`freq_admission_doorkeeper`，强）**：在 B 之上 (1) 采样(默认 1/2)
       在 Lookup 也计数，使 sketch 反映含命中的真实频率；(2) **容量受压时拒绝
       冷 newcomer**（freq≤cold 阈值且 `usage+charge>capacity`）——走已有 standalone
       路径返回数据但不入表，冷块不再挤掉常驻热块。拒绝走 standalone、sketch 全
       relaxed，**仍是无锁**。
   - **结果（tinylfu-eval-C-seed1-0614，wlC 100GB，t64，seed1，同库）hit_ratio：**

     | 容量 | LRU | hcc | hcc_tinylfu | ARC | Cacheus | mlc_base | mlc_tinylfu |
     |---|---|---|---|---|---|---|---|
     | 1GB | .582 | .581 | **.596** | .635 | .640 | .566 | **.608** |
     | 2GB | .627 | .628 | **.643** | .667 | .672 | .612 | **.649** |
     | 4GB | .677 | .677 | **.691** | .700 | .704 | .660 | **.690** |
     | 8GB | .724 | .724 | **.733** | .733 | .733 | .714 | **.727** |

     - MLC+tinylfu：1GB **+4.2pp**（从低于 LRU 1.6pp → 高于 LRU 2.6pp）；4GB **+3.0pp**
       追平 ARC（差 1pp）；8GB 贴近 ARC/Cacheus（差 0.5pp）。对 ARC 的差距 1GB 收
       61%、4GB 收 75%。
     - HCC+tinylfu：各容量 +0.8~1.5pp，8GB(.733) 直接追平 ARC/Cacheus。
   - **方案 B 单独基本无效（+0.5pp，freqB-eval-C-seed1-0614）**：印证 五.2/五.4——
     "只改 countdown/只数 miss"信号太弱；起作用的是**准入拒绝 + 全访问计数**。
     zipfian 里热 key 多为命中、极少重 insert，insert-only 计数看不到它们；且常驻
     热块本就被 Lookup 顶到 countdown=3，改初值无增益。
   - **吞吐**：MLC tinylfu 吞吐反升（8GB 1053 vs base 783，拒冷块省淘汰）；HCC
     tinylfu 8GB −9.5%（采样查找计数代价）。注意 **Cacheus 大缓存吞吐很差**
     （8GB 644 且递减），故 8GB 处 tinylfu **命中追平 Cacheus 而吞吐高 60%+**。
   - **【已补上】小缓存 2~3pp 缺口 —— 靠"更严准入"而非"更细计数"**
     （tinylfu-sweep-C-seed1-0614，1/2GB 同库扫两杠杆）：
     - **`_s0`（`freq_lookup_sample_log2=0`，每次查找都计数）是负优化**：mlc
       1GB .606→**.563**、2GB .647→**.613**，吞吐也降。原因：zipfian 冷尾在窗口内
       零星重访，全量计数把冷尾频次也抬高 → 门卫放进更多扫描流量 → 抗扫描变弱；
       且累计更快触发 sketch 减半，长期热信号被冲淡。**所以频次"更准"反而更差。**
     - **`_c2`（cold≤2 / warm≤3，被看到 ≥3 次才能挤占常驻块）是干净赢家**：

       | 容量 | mlc_base(1/2) | mlc_tinylfu(默认) | **mlc_tinylfu_c2** | ARC | Cacheus |
       |---|---|---|---|---|---|
       | 1GB | .566 | .606 | **.642** | .628 | .639 |
       | 2GB | .612 | .647 | **.679** | .665 | .670 |

       c2 相比默认 +3.6pp/+3.3pp，**双双反超 ARC、追平/超过 Cacheus**（2GB .679 >
       Cacheus .670），且吞吐全表最高（847/950 Kops/s，命中高→回盘少）。
     - **全容量复核（tinylfu-c2-valid-C-seed1-0615，1/2/4/8GB 同库）—— c2 是全局
       净提升，大缓存非但无回退反而更高：**

       | 容量 | LRU | ARC | Cacheus | hcc_tinylfu(c2) | mlc_tinylfu(c2) |
       |---|---|---|---|---|---|
       | 1GB | .584 | .644 | .648 | .630 | **.646** |
       | 2GB | .635 | .673 | .678 | .674 | **.685** |
       | 4GB | .683 | .705 | .708 | **.713** | **.717** |
       | 8GB | .729 | .737 | .737 | **.743** | **.741** |

       mlc c2 在 2/4/8GB **全面反超 ARC 与 Cacheus**，1GB 平 Cacheus、超 ARC；相比旧
       默认(sample1/cold1：.608/.649/.690/.727)每档 +1.4~3.8pp。吞吐 1/2/4GB 也高于
       arc/cacheus（866/919/963 Kops/s），8GB 794 是已知 MLC 单实例大容量开销、非 c2 引入。
     - **结论 & 落地**：把 `*_tinylfu` 推荐默认改为 `cold_threshold=2, warm_threshold=3`
       （`run_rocksdb_matrix.py` 已生效）。s0/s0c2 路线放弃。
   - **【BUG 修复，2026-06-15】门卫拒绝路径 `usage_` 记账缺失 —— 之前的 tinylfu
     结果（含上面 c2 全部数字）偏乐观,需用修复后二进制重测**：
     - 根因：standalone handle 释放时 FixedHCC/AutoHCC **都会** `usage_.FetchSub`
       (`clock_cache.cc` 的两处 `IsStandalone()` 分支),但门卫拒绝时只调
       `StandaloneInsert`(仅加 `standalone_usage_`,不加 `usage_`),而正常 fallback /
       `CreateStandalone` 都先 `usage_ += charge`。不变量是 `usage_` 必须含 standalone
       charge(见 `GetUsage()=table_pinned+standalone`、`AddShardEvaluation` 里
       `GetUsage()-GetStandaloneUsage()`)。
     - 后果：门卫每拒绝并返回一个块,`usage_` 净减 charge → 持续低估 → 驱逐误判"有
       空间"→ **表实际占用超过配置容量、命中率被人为抬高**。拒绝越多漂移越大,故 c2
       的"优势"里掺了"偷用更多内存"的成分,对 lru/arc/cacheus 不公平。
     - 修复：门卫返回 standalone 前补 `usage_.FetchAddRelaxed(proto.GetTotalCharge())`,
       与 fallback 一致。已重编 librocksdb.so + 重链 ycsbc,5GB 冒烟通过。
     - **行动项**：tinylfu-eval / tinylfu-sweep / tinylfu-c2-valid 均在 bug 二进制上
       跑的,需在修复后二进制上重跑 workload C 全量(wlC-allscheme-fixed-0615)再下结论。
   - **新 prop / 方案**：`rocksdb.hcc_frequency_aware_admission`（B）、
     `rocksdb.hcc_freq_admission_doorkeeper` + `rocksdb.hcc_freq_lookup_sample_log2`（A）、
     `rocksdb.hcc_freq_admission_cold/warm_threshold`；方案 `hcc_freq`/`hcc_tinylfu`、
     `mlc_hcc_all_levels_sharded_{freq,tinylfu}`（及 `_freq_strict`）。
   - **铁律提醒**：本次改了 `include/rocksdb/cache.h` 的 `HyperClockCacheOptions`
     布局，已连带重编 `ycsbc`（见 五.6）。
