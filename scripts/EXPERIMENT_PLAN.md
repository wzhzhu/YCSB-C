# 正式实验规划：双操作点论证 MLC（无锁并发 + 命中率）

> 更新于 2026-06-10。配套文档：`README_rocksdb_matrix.md`、`KNOWN_ISSUES.md`。
> 论证目标：**MLC = HCC 级无锁并发 + 更高命中率**，两个卖点分别需要不同的
> 成本结构操作点，合成一个"hit ratio × 设备速度"的二维谱系叙事。

## 〇、量化模型基础（来自 probe-20260610-074612 标定）

> **2026-06-12 Release 重标定（release-calib-20260612-020710，100GB/wlC/t64，
> metadata-in-cache 旧口径）：操作点已从 CPU-bound 翻转为 NVMe IOPS-bound。**
>
> - 快方案全部顶在 **~300K miss IOPS**（LRU 2GB 306K / LRU 8GB 291K /
>   HCC 2GB 315K），吞吐 ≈ `IOPS_max / m`，验证：300K/0.417=720 vs 实测
>   734；300K/0.290=1036 vs 实测 1004。**命中率在 t64 下已天然 1:1 兑换
>   吞吐**（LRU 2→8GB：data hit +12.7pp → 吞吐 ×1.37）；
> - `C`（Release）≈ 20~40µs（冒烟 t16 36µs/op 含 0.27 miss；Debug 时代
>   140µs 作废），t64 下 `m·r` 主导；
> - **第二组的 io.max 限速臂降级为可选**：自然 NVMe 上限已让 I/O 主导；
>   仅当需要进一步压缩方案间软件路径差异时再启用；
> - 绝对吞吐基线（wlC/t64）：LRU 734/1005、HCC 756/884、MLC sr_bottom
>   684/858 kops（2GB/8GB）。MLC 与 HCC 差距 -3~-7%（Debug 时代 -47%
>   确认为 -O0 放大软件路径，KNOWN_ISSUES 一.14 闭环）。
>
> **新口径（data-only，dataonly-calib-20260612-052307）标定**：
>
> - `C ≈ 39µs`、`r_eff ≈ 93µs`（HCC 2/8GB 对解；r 为 ~280-330K IOPS
>   负载下的排队等效值）；8GB 档 `m·r` 占 L ~40%，为混合区而非纯
>   IOPS 墙；2GB 档 HCC/MLC miss IOPS ~324-330K 接近设备上限；
> - 元数据移出后 data hit 整体上移（2GB 档 0.583→0.628，元数据不再
>   挤占数据空间），且 `hit_ratio` ≡ `data_hit_ratio`；
> - 竞争格局：2GB **MLC(878)≈HCC(871) > LRU(742, -15%)**——miss 重载
>   下 LRU 分片锁在插入/淘汰路径吃亏（其 miss IOPS 仅 276K），第一组
>   叙事的免费预演；8GB MLC -6%（+4.2µs/op 残余：per-level 统计原子
>   计数争用 + 路由，L6 计数器吃 89% 流量）；
> - **dynamic SR-HCC 在 2GB 拿到全场最高 data hit（0.6317，
>   +0.4pp over LRU/HCC）**，MLC 命中率首次出现正信号（幅度小）；
>   但 dynamic 8GB 吞吐 856（-13%），采样/后台 worker 代价在高命中端
>   显形；
> - 一.15（排空层滞留）修复后内存口径已验证精确守住预算。

以下为 Debug 时代旧模型，仅留作形态参考：

固定线程数下每操作时延 `L = C + m·r`：

- `C`：软件路径固定成本，本机（128HT/64核 + NVMe + O_DIRECT + YCSB Get 路径）
  实测 ≈ **140µs**（含 ~3 次 cache 查找、块解析、1KB 值拷贝、客户端开销）；
- `m`：每 op 的块 miss 次数（zipfian 0.99 + 2~8GB cache 时 ≈ 0.30~0.42）；
- `r`：单次 miss 的有效 I/O 成本（NVMe ≈ 32µs 等效值）。

推论（均已实测验证）：

1. NVMe + t64 下 `C` 主导（占 ~90%），命中率差异最多兑换几个百分点吞吐
   —— 这是 LRU(s6)/HCC 在 2~8GB 吞吐几乎并拢的原因；
2. t64 已接近吃满 64 物理核（`C/L ≈ 93%`），CPU 先于 NVMe 饱和，
   单纯加线程无法让设备成为瓶颈；t128 测超线程收益（预期 +20~35%），
   **t256 仅在超订鲁棒性叙事中有意义**；
3. 盘越慢 → `m·r` 越大 → 命中率越值钱、实现效率越不值钱；反之亦然。
   NVMe 是"实现效率"放大器，慢盘/限速是"命中率"放大器。

## 一、第一组：高命中端（无锁并发卖点）

### 设计

- **数据集固定 100GB 不变**（reviewer 防御的核心，见下），24B key + 1000B value；
- **cache 容量保持常规档（2~8GB），高命中率由负载产生而非容量**（拉容量
  既会招致"给了足够大缓存"的质疑，也只抬命中率而不制造热分片——scrambled
  zipfian 把热 key 摊匀到各 shard，分离度无保障）：
  - 主路径：实现标准 YCSB 的 **hotspot 混合分布**（`requestdistribution=
    hotspot` + `hotspotdatafraction` + `hotspotopnfraction`，YCSB-C 需补
    `HotspotGenerator`，~50 行）。如 95% 操作落在 0.01% key（1 万 key ≈
    10MB）→ 2~4GB cache 即 data hit ≈ 0.95；hot set 取 keyspace 连续前缀，
    ordered load 下连续 key 共享少数 data block 与同一两个 SST 的
    filter/index，直接制造"单块热点 → LRU 热分片"；
  - 热度可参数化成曲线：hot set 从 ~100 key（极端档，单块热点）扫到
    ~10 万 key（温和档），展示锁基方案随热度集中的退化轨迹；
  - 参数背书：引用 Twitter 2020 生产 KV trace 统计（多数集群 top 0.01%
    对象占请求量 50%+），防"人造负载"质疑；
  - 加强臂（可选，第二阶段）：真实 trace 回放（首选 Twitter Twemcache
    2020 trace，KV 点查 + 极端倾斜；需给 YCSB-C 加 trace-replay workload：
    按行读 key、映射 24B keyspace、保持到达顺序）；
- 线程扫 `t64 / t128 / t256`（t256 = 超订段，锁车队效应在此展开）；
- 所有方案保持各自最优分片（lru/hcc/arc/cacheus=64 分片，MLC 不分片），
  结论形态是"**分片也救不了锁基方案**"，避免打稻草人。

### 预期（务实版）

- MLC ≈ HCC（持平或略低 1~3%，多一层路由/统计原子操作；"不输"即达成卖点）；
- 64 分片 LRU 在 t256 超订段落后 **20~50% 量级**（不是 Mark 博客的数倍：
  scrambled zipfian 把热 key 打散到不同 block，最热单块仅 ~5% 流量，
  热分片效应比"单块热点 + 全缓存"温和）；
- ARC/Cacheus（wrapper 锁 + 双重记账）显著低于以上所有。

### 分离度不足时的加热手段（按序尝试）

1. 缩小 hotspot 的 hot set（提高单块集中度）或提高 `hotspotopnfraction`；
2. 增加 LRU 默认分片对照（如 RocksDB 默认 vs 64 分片）展示锁敏感性。

### Reviewer 防御点

- 不缩数据集也不拉缓存：100GB 数据 + 常规 cache 档（占数据 2%~8%），高命中
  完全来自负载本身的热点结构，叙事为"同一 100GB 负载，访问呈现生产环境
  常见的热点集中形态（命中率 90%+）时，锁基实现扩展性塌陷"；
- 高命中率是生产常态而非特例（RocksDB HCC 的动机即来自 Meta 生产 block
  cache，命中率普遍 >90%）；"命中越多锁开销越重"的实现自我矛盾——正是图
  要展示的点；
- 第二组主动承认"I/O 主导时锁不重要"，但指出那一端胜负手换成命中率且
  MLC 仍优 —— 两端合起来无回避空间。

## 二、第二组：低命中端（命中率卖点）

### 设计

- **禁用 uniform**：uniform 下一切淘汰策略等价于随机替换（无局部性可利用，
  hit ratio ≈ cache/数据集，对所有方案相同），MLC allocator 亦无从优化
  （各层边际收益相同）——uniform 会把想展示的差异精确归零；
- 用**中等倾斜**最大化策略差异：θ ∈ {0.8, 0.9, 0.99}（需把硬编码的
  `ZipfianGenerator::kZipfianConst=0.99` 暴露成 workload property，
  `core_workload.cc` 构造 `ScrambledZipfianGenerator` 处读取）；
- 让 I/O 项主导（否则差异被 `C` 稀释 ~3 倍）：
  - 首选 cgroup v2 `io.max` 限速臂（如 riops=40~50K），此时吞吐 =
    IOPS上限`/m`，严格 ∝ 1/m，锁差异同时被抹平 —— 单变量最干净；
    所需线程数也小（吞吐 × L ≈ 30~40 线程即可饱和限速设备）；
  - cache 档**避开 1GB 元数据墙**（≥2GB，或开 partitioned index/filter；
    100GB 数据的 index+filter 工作集 ≈ 1.05GB，详见 KNOWN_ISSUES 一.7）；
- 辅战场：workload E（scan 污染，SR-HCC 扫描抗性）与 D（latest 分布,
  层间流量结构变化）—— MLC 三个卖点同时在线的负载。

### 预期排序（修正版）

```
LRU ≈ HCC  <  ARC/Cacheus  ≤  MLC
```

- I/O 主导后锁开销无关紧要：分片 LRU 与 HCC 命中率几乎相同（实测 data hit
  差 ≤1pp），吞吐并拢；
- ARC 会**反超** LRU/HCC（+4~5pp data hit 开始值钱，wrapper 锁不再是瓶颈）；
- **承重假设：wlC 下已干净判负（2026-06-12 release-calib，level tag
  修复后）**：MLC 2GB data_hit 0.5828 与 LRU 0.5836/HCC 0.5829 几乎重合，
  仍低于 Cacheus 0.626。原因明确：wlC 的 zipfian 平铺键空间下层间流量
  无结构差异，allocator 收敛到按比例分配（L6 拿 89.5% 容量、89.7% 流量），
  MLC 行为正确地退化为单体——鲁棒性卖点而非命中率卖点。
  **第二组主战场正式移到 E（scan 污染 + SR-HCC）/ D（latest 分布，
  层间流量结构差异）**；wlC 仅作为"MLC 不输单体"的对照组保留。
- 已知标杆（probe-20260610-074612, wlC/t64 的 data_hit_ratio）：
  - 第二组甜区在**小容量高压段**：2GB 档 Cacheus 0.626 / ARC 0.623 >
    LRU 0.581 / HCC 0.571，MLC 需 **> 0.626** 才登顶；
  - 8GB 档自适应优势消失甚至反转（Cacheus 0.678 全场最差，见
    KNOWN_ISSUES 一.9），第二组容量档应选 2~4GB。

## 二点五、全局设计决策：元数据移出 block cache（2026-06-12 定）

主矩阵统一 `cache_index_and_filter_blocks=false`（已写入
`run_rocksdb_matrix.py` COMMON_PROPS，下一轮 run 生效）：index/filter
固定驻留 table reader 堆内存（`max_open_files=-1` 已确认默认 -1），
block cache 只承载 data block，矩阵纯粹对比数据块策略。

- 动机：一次性消除三个混淆因子——MLC allocator 震荡逐出热元数据
  （KNOWN_ISSUES 一.14a）、饿死层放不下 590KB index 巨块（一.14b）、
  1GB 档元数据墙（一.7，该档重新变为有效高压测试点，恰为第二组甜区）；
  同时 `cache_hit_ratio` ≈ `data_hit_ratio`，每 Get 的 cache lookup
  从 ~3 次降为 ~1 次，ARC/Cacheus 的 wrapper 每操作成本同比例下降。
- 内存口径（论文必须写明）：100GB 数据的 index+filter ≈ 1.05GB 驻留在
  cache 预算之外，所有方案同等支付；`cache_size=X` 的实际总内存 ≈
  X + 1.05GB。
- 保留反向辅助臂：metadata-in-cache（翻回 true）单独一组，承接
  reviewer 的"生产环境 metadata in cache"质疑，并呈现 MLC 1GB 档
  领先单体的元数据驻留优势（49~81 vs 34~39 kops，index hit
  0.976 vs 0.931）作为独立卖点。
- 已知代价：每 case 重开 DB 后首次访问各 SST 付一次元数据加载
  （~1600 文件，O_DIRECT 真 I/O），落在 transaction 早期，20M ops
  下可忽略，repeat 间方差略增。

## 三、Pilot 计划（正式矩阵前）

0. ~~两项前置修复~~ **全部完成并验证（release-calib-20260612-020710）**：
   - MLC level tag：100GB ordered 全量确认，L6 lookup 占比 0.34%→89.7%，
     与 LSM 形态自洽（KNOWN_ISSUES 一.12 闭环）；
   - 优化构建：已重标定，操作点翻转为 IOPS-bound，见〇节；
   - 新发现遗留项：排空层子缓存的陈旧块滞留（KNOWN_ISSUES 一.15，
     8GB MLC 实际内存超预算 ~11%），需 allocator 收缩路径主动回收，
     **MLC 内存口径公平性问题，正式矩阵前应修**；
1. ~~等 probe-20260610-074612 跑完，先看 MLC 命中率~~ 已完成：表面未达标
   但被一.12 阻塞，修复后重测承重假设；
2. 第一组 pilot：hotspot(95% ops → 0.01% keys) × {2, 4GB} ×
   {64, 256 线程} × {lru, hcc, mlc_*}，单 repeat，验证高命中端分离度
   是否达 20%+；不足则缩 hot set 加热；
3. 第二组 pilot：wlC(θ=0.9) × {2, 4GB} × io.max 限速 × 全方案，
   验证吞吐是否严格随 data hit 分层；
4. 1GB 档 s0/s6 受控 A/B（KNOWN_ISSUES 一.7 待办）可与 pilot 合并跑；
5. pilot 通过后正式矩阵 `--repeats 2+`（±3~5% 噪声带需量化）。

## 三点五、MLC 降开销/提命中优化清单（2026-06-12 代码审查定位）

> **1~4 已全部实施（2026-06-12，待 100GB 全量验证）**，实现要点：
>
> 1. WrappedHandle：HCC 暴露各 shard slot 数组地址区间
>    （`HandleAddressRange`/`AppendHandleAddressRanges`，Fixed 用数组、
>    Auto 用整段保留 mmap，生命周期内稳定），MLC 构造时建有序区间表，
>    Lookup/Insert 命中区间的 handle **裸透传零分配**，Release 二分反查
>    owner；standalone（堆分配）与非 HCC 子缓存兜底走 WrappedHandle，
>    用指针低位 tag 区分两类 handle。
> 2. 采样：新增 `lookup_sampling_enabled_` 总开关（仅 dynamic 启用或
>    db_bench 显式设置采样率时打开），关闭时 Lookup 零开销早退；全局
>    `lookup_sample_seq_` 原子序列改为 thread_local 计数（采样率只需
>    统计意义成立）；YCSB dynamic 默认 `sample_rate_log2` 0→4（1/16）、
>    `min_samples` 12288→1024。
> 3. 计数器：per-level lookups/hits 改 16 stripes × levels 的
>    64B 对齐条带（线程 round-robin 绑 stripe），读侧求和。
> 4. allocator 反震荡（`MultiLevelAllocationOptions` 新字段）：
>    总死区 `max(min_total_change_bytes, 总容量×0.5%)`；per-level 死区
>    `max(64KB, 本层×5%)`；收缩迟滞（连续 3 轮确认 + 单轮只关一半，
>    grow 立即）+ 预算再平衡（被推迟的 shrink 同步裁掉它资助的 grow）；
>    稳态自适应间隔（无变更轮指数退避至 8×interval，变更即复位）。
>
> 冒烟（2M 库/256MB/t16/O_DIRECT/metadata-in-cache 旧口径，Release）：
> HCC 402.6 / MLC sr_bottom 379.6 / MLC dynamic 373.8 / LRU 346.5 kops；
> **dynamic 与 sr_bottom 差距 8%→1.5%**（采样优化生效）；MLC 反超 LRU
> ~10%；与 HCC 差 -5.7% 含 0.7pp 命中率差（小库分区统计损失，量级与
> wlC 结构损失一致）。per-level 统计自洽、排空层 usage≈0、容量分配
> 稳定。Debug（断言开启）库下 MLC 冒烟无断言触发，routing 单测通过。
> 条带化/无分配收益主要在 t64 高吞吐端，待 100GB 重跑量化。

8GB 档 +4.2µs/op（69.0 vs HCC 64.9µs）分解与修法，按性价比排序：

1. **消灭 WrappedHandle 堆分配**（每命中 new/delete 一个 16B 对象，
   ~1M malloc/free每秒打 glibc arena 锁；est. 1~3µs/op）：HCC 的
   Value/GetCharge/Ref 只读 handle 不依赖实例可直接转发；仅 Release
   需 owner——MLC 构造时登记各子缓存 handle 数组地址区间（7层+shared
   ≈8 段），Release 按指针二分反查。零分配零新字段。
2. **采样早退 + dynamic 降采样**：`MaybeRecordLookupSample` 的全局
   `lookup_sample_seq_.fetch_add` 在判断前执行，sr_bottom/all_levels
   也白付一次全局原子加；且 `sample_rate_log2=0` 语义为**全采样**
   （YCSB dynamic 配置正是 0），dynamic 每 lookup 多付 std::hash +
   ring 两次原子写 —— 即 dynamic 8GB 比 sr_bottom 慢 5.8µs/op
   （856 vs 927）的来源。修：禁用时 fetch_add 前早退；启用时默认
   log2=4~6（1/16~1/64，unique ratio 统计上足够）。
3. **per-level lookups_/hits_ 计数器条带化**（L6 一对计数器吃 89%
   流量×64 线程；est. 0.3~0.8µs/op）：16 stripes×levels 或采样计数。
4. **allocator 反震荡**（purge 修复后 shrink 从懒变急，误伤温块的
   代价真实化）：死区 1MB → max(1MB, 总容量 0.5~1%)；收缩迟滞
   （grow 立即、shrink 连续 3 轮确认且单轮减半）；稳态自适应拉长
   interval（1s→5~10s 退避）；per-level 死区（<本层 5% 不动）。
   预期 hit +0.1~0.3pp。
5. （可选）大层子缓存内部分片，t256 高命中端才显形。

诚实上限：wlC 下分区有统计复用损失，-0.4pp 已近结构下限，目标是
持平；命中率正收益在 D/E 与 dynamic SR-HCC（2GB 已 +0.4pp）。
做完 1~4 预期 8GB 档进入 HCC -2% 以内，dynamic 与 sr_bottom 并拢；
对第一组（高命中端 C 主导、t256）每 1µs 都直接折算吞吐。

## 四、实施清单（等当前 run 结束）

- [ ] `core/hotspot_generator.h` + `core_workload.cc/h`：hotspot 分布
      （`hotspotdatafraction`/`hotspotopnfraction`，对齐标准 YCSB 语义：
      热集内 uniform、冷集内 uniform）；
- [ ] `core_workload.cc/h`：`zipfian_const` property（默认 0.99 保持兼容，
      第二组中等倾斜扫描用）；
- [ ] `run_rocksdb_matrix.py`：`--request-dist` / `--hotspot-*` /
      `--zipfian-const` 维度（命名进 case/log/CSV）；
- [ ] `run_rocksdb_matrix.py`：`--io-riops` 可选限速臂（cgroup v2 io.max，
      需确认 dm 设备 MAJ:MIN 与 cgroup 权限）；
- [ ] t256 线程档；
- [ ] `make` 重建 ycsbc（原子计数器修复在本轮结束后生效）；
- [ ] （第二阶段，可选）trace-replay workload + Twitter 2020 trace 接入。
