# ShardRoute 设计说明

## 1. 目标与执行模型

ShardRoute 是配置驱动的静态核验器。一次完整运行分成四个只读阶段：

1. `config`：读取并校验旧/新拓扑 JSON，计算规范化配置指纹。
2. `scan`：读取逻辑 CSV 快照、物理分片 CSV、每个分片的 `_manifest.json`。
3. `analyze`：逐行按旧拓扑计算期望落位，与物理落位比对；按旧→新拓扑计算移动矩阵；解释查询裁剪。
4. `archive`：写出 JSON / Markdown / 自包含 HTML，并把结构化结果幂等到 SQLite 台账。

没有数据库驱动、SQL 执行、迁移执行或远程调用。`demo` 生成的是合成 fixture；`plan` 是 dry-run 计划，不会触碰物理文件。

---

## 2. 值规范化

实现：`shardroute/canonical.py`。

- 字符串：去除首尾空白。`case_sensitive=false` 时再执行 Unicode `casefold()`。
- CSV 空单元格：空字符串 `""`，不是 null。空字符串是合法分片键，会参与路由。
- JSON `null`：`None`。只有 JSON/内存值是 null 时才触发 null 策略。
- 数字、布尔值：转为稳定文本（`17`、`true`、`false`）；浮点格式化为 `.15g`。
- 列表、对象等容器：不是合法分片键，抛出 `ROUTE_BAD_VALUE`。
- 缺失分片键字段：抛出 `ROUTE_KEY_MISSING`。
- 复合键：每个分量先按上规则规范化，再用 ASCII Unit Separator `\x1f` 连接，整体参与 hash。
  任一组件为 null 时按 null 策略处理，不进行拼接。

规范化是幂等的；路由 trace 同时保留原始值、规范化值、命中的规则和比较结果。

---

## 3. 路由语义

### Hash

`router=hash` 时：

```text
digest = SHA-256(UTF-8(normalized_key))
number = int.from_bytes(digest[0:8], "big")
bucket = number % buckets
shard  = bucket_map[bucket]
```

- `bucket_map` 可以是长度为 `buckets` 的数组，也可以是键为 `"0"`…`"buckets-1"` 的对象。
- 数组/对象必须覆盖每个 bucket，且不能缺项；重复分片值本身是允许的。
- trace 包含 `digest_prefix`、`bucket`、`rule=hash_bucket` 和目标分片。

### Range

`router=range` 要求单键，日期或数值必须零填充，以便稳定字符串比较。

- 区间为左闭右开 `[lower, upper)`。
- 最后一段 `upper` 可为 `null`，表示无上界；非最后一段必须有限。
- 配置中的区间必须连续、无重叠、无缺口。
- 未命中任何区间时：有默认分片则落默认分片（`rule=range_default`），否则 `ROUTE_NO_MATCH`。
- trace 的 `comparisons` 记录查询值与边界的比较，以及是否回退默认分片。

### List

`router=list` 要求单键。

- 先对值做规范化，再精确查找 `list_map`。`case_sensitive=false` 时 map 键也在配置加载时规范化，大小写折叠后的重复键会报 `CFG_LIST_DUPLICATE`。
- 命中：`rule=list_exact`。
- 未命中但有 `default_shard`：回退默认分片，`rule=list_default`。
- 未命中且没有默认分片：`ROUTE_NO_MATCH`。

### Null 策略

| 策略 | null 行为 |
|---|---|
| `error` | 抛出 `ROUTE_NULL_KEY`，该行无法路由 |
| `route_default` | 落 `default_shard`，`rule=null_route_default` |
| `drop` | 不落任何分片，`dropped=true`，审计中不应出现在物理文件 |

`route_default` 在配置校验阶段强制要求存在默认分片（`CFG_DEFAULT_MISSING`）。

---

## 4. 扫描与输入约定

- 逻辑快照路径：`<workspace>/logical/<table>.csv` 或 `<table>.jsonl`；同一表不能同时存在两者。
- 物理文件路径：`<workspace>/shards/<table>/<shard>/*.csv` 或 `*.jsonl`，可混用。
- CSV 必须有 `row_id` 列；JSONL 每行必须是一个含 `row_id` 的 JSON 对象，空行忽略。逻辑快照中 `row_id` 必须唯一，物理文件允许出现重复行以支持重复审计。JSONL 的 JSON `null` 保留为 null，CSV 的空单元格仍是空字符串。
- 物理目录可含 `_manifest.json`，记录 `table`、`shard`、`row_count`、`files[]`（`name`、`rows`、`sha256`）和 `generated_at`。
- 文件与清单均按文件名稳定排序；物理指纹覆盖数据文件与 `_manifest.json`，基于相对路径和 SHA-256，避免绝对路径泄漏。

manifest 校验会识别：缺失 manifest、解析失败、身份不匹配、清单列出但文件缺失、未列出的孤儿文件、文件 SHA-256 不一致、单文件行数不一致、总行数不一致、重复登记文件。

---

## 5. 审计分类

每一条逻辑行按“行级分类互斥”处理，避免同一异常重复计为多种行问题：

| 代码 | 条件 | 严重度 |
|---|---|---|
| `MISSING_ROW` | 逻辑行在物理分片中完全不存在 | critical |
| `DUPLICATE_ROW` | 同一 `row_id` 出现于两个及以上物理位置 | critical |
| `MISPLACED_ROW` | 恰好一个物理位置，但分片 ≠ 期望分片 | critical |
| `DROPPED_ROW_PRESENT` | null 策略为 drop，但物理仍存在该行 | high |
| `ORPHAN_ROW` | 物理行不在逻辑快照中 | medium |
| `ROUTE_ERROR` | 逻辑行无法按配置路由 | critical |

清单/目录类发现：

| 代码 | 含义 | 严重度 |
|---|---|---|
| `ORPHAN_FILE` | CSV 未出现在 manifest 中 | medium |
| `MANIFEST_MISSING` | 分片目录没有 `_manifest.json` | medium |
| `MANIFEST_PARSE_ERROR` | manifest 无法解析 | high |
| `MANIFEST_IDENTITY` | manifest 的 table/shard 与目录不一致 | high |
| `MANIFEST_FILE_MISSING` | manifest 登记的文件不存在 | high |
| `MANIFEST_DUPLICATE_FILE` | 同一文件重复登记 | high |
| `MANIFEST_HASH` | 文件 SHA-256 与 manifest 不符 | high |
| `MANIFEST_FILE_ROWS` | 单文件行数与 manifest 不符 | high |
| `MANIFEST_ROW_COUNT` | 总行数与磁盘实际行数不符 | high |
| `UNEXPECTED_SHARD` | 分片目录不是该表的合法路由目标 | medium |
| `UNKNOWN_TABLE_DIR` | 物理表目录未在拓扑中声明 | medium |

发现项按 `(severity, table, code, shard, row_id, finding_id)` 稳定排序；`finding_id` 由异常字段的 SHA-256 派生，重复运行不变。任一 critical/high 发现会让审计状态为 `fail`，并成为重分片计划的阻塞项。

---

## 5a. Hash 负载偏斜尾风险诊断

实现：`shardroute/skew.py`，由 `run_audit` 的 `hash_skew_diagnostics` 字段在同一只读链路中产出。它补充而非替代逐行放置审计，不产出 bucket_map、迁移计划或任何执行动作。

### 零假设与适用范围（显式声明）

零假设标签固定为 `independent_canonical_keys_uniform_buckets`：

- 统计单位是**不同规范化键（canonical key）**，不是逻辑行。每条逻辑行经真实 `Router` 轨迹归一化；同一规范化键的全部行必然共桶，合并为一个权重等于其行数的键群。
- 零假设把每个键群建模为一次独立、对桶均匀的类别抽样；键群整体按 `bucket_map` 落 shard。
- 该诊断**不声称 SHA-256 输出是真随机**，尾概率也**不是**对抗性选键或未来容量的保证。每份证书都携带这一 `scope_note`。

### 分母与排除合同

分母字段可逐项复核：`logical_rows`、`eligible_hash_rows`（键群权重之和）、`excluded_rows` 与 `exclusions_by_reason`。以下行绝不偷算为独立 hash 键：

| 排除原因 | 含义 |
|---|---|
| `null_error` | null 键被 `null_policy=error` 拒绝（路由失败） |
| `null_drop` | null 键按 drop 策略丢弃 |
| `null_default` | null 键落 default shard |
| `route_error` | 其他路由失败 |
| `non_hash_route` | range/list 路由决策 |

range/list 表本身标记 `applicable=false, reason=router_not_hash`；没有合格键群的 hash 表标记 `no_eligible_hash_groups`。

### 基线概率与事件

- 基线概率严格按 `bucket_map` 的桶占比计算（如 4 桶中 3 桶指向同一 shard 则概率 0.75），不等权时**不**退化为按 shard 数均分。
- 观测事件固定为：当前 hash 表最大 shard 逻辑行负载 **至少达到观测最大值**（`max_shard_logical_row_load_at_least`），事件定义、各 shard 观测负载与阈值均写入报告。
- 多个原值规范化成同一键时合并为一个加权键群；一个键承载多行时权重 > 1，整群同落，**不**退化为逐行二项检验。

### 证书路线与三态结论

显著性阈值 `alpha` 可配置（CLI `--alpha`，默认 0.01，要求 `0 < alpha < 1`）。

1. `exact_weighted_enumeration`：对加权键群做精确动态规划，维护各 shard 负载向量的概率质量；支持不等桶权与不等群权。8 个键群各 10 行且 80:0 时，群级尾概率为 `2/2^8 = 0.0078125`；逐行独立模型的 `2/2^80` 是错误证据，永远不会被采用。
2. `conservative_hoeffding_bound`：仅当精确路线超出算力预算（状态数或总权重上限）时启用，且只在两个等权 shard、等权重键群的窄条件下严格成立，给出双侧 Hoeffding 上界 `2·exp(-2g(c/g-1/2)^2)`。不适用的形状（不等权 shard、不等群权）返回 `unknown` 并给出原因，绝不伪造精确数。

结论三态：

| decision | 条件 |
|---|---|
| `significant` | 精确尾概率 ≤ α；或保守上界 ≤ α（上界足以证明显著） |
| `not_significant` | 精确尾概率 > α（仅精确路线可给此结论） |
| `unknown` | 精确路线超预算且上界 > α，或上界不适用；不得暗改为通过或异常 |

同一 80:0 事件、α=0.02：精确路线给 `significant`（0.0078125 ≤ 0.02）；Hoeffding 上界 `2e^-4 ≈ 0.03663 > 0.02` 只能给 `unknown`。α=0.005 时精确路线给 `not_significant`。证书类型与结论的差异在 JSON/Markdown/HTML 中显式展示。

### 预算与因果链

- 预算参数 `max_exact_states`、`max_total_weight` 与实际峰值状态数写入证书；超预算时记录 `fallback_reason`，精确值字段为 `null`，不用不可靠近似数伪装精确概率。
- Router 归一化规则或 `bucket_map` 一旦变化，键群划分与基线概率随之变化，尾概率与显著性结论同步改变。诊断从真实 Router 轨迹重建键群并在输出中保留每群的 bucket、routed shard 与 row_ids，使这条因果链可追踪、可复核。

---

## 6. 重分片 dry-run

- 对每条逻辑行分别计算旧拓扑目标与新拓扑目标。
- 仅当两个目标不同且均未 drop 时，计入移动矩阵 `(table, from_shard, to_shard)`。
- 移动矩阵按 table/from/to 排序；批次按 cell 顺序每 80 行切分，批次号从 `batch-001` 开始。
- 阻塞项来自：未修复的 critical/high 审计发现；目标拓扑缺表；分片键变更；路由错误。router 族变化标记为 review。
- 回滚清单固定为 7 步，强调先冻结写入、快照 manifest/校验和、shadow copy、重新审计、双读验证、失败恢复旧拓扑、解冻并归档。
- `safe_to_execute` 只表示静态检查没有 blocking 项；工具自身不会执行任何移动。

---

## 7. 查询裁剪解释

实现：`shardroute/query_explain.py`。支持谓词：`eq`、`in`、`gte`、`gt`、`lte`、`lt`、`ne`、`is_null`、`not_null`。

决策值：

| 决策 | 含义 |
|---|---|
| `pruned` | 可以安全地只访问 `targeted_shards`（包括 0 个分片） |
| `not_prunable` | 存在分片键谓词，但无法证明裁剪安全，保守扫描全部 |
| `full_scan` | 没有分片键谓词，必须扫描全部合法分片 |

- hash：只有所有分片键组件都有等值谓词时才能裁剪；复合键缺组件则不可裁剪。
- range：等值/In 直接路由；区间谓词按左闭右开区间与配置区间求交；完全覆盖所有分片时降级为 `full_scan`。
- list：等值/In 通过 `list_map` 映射；未映射值使用默认分片；存在默认分片时保守加入目标集合。
- null 等值谓词遵循 null 策略；`error` 策略下标记 `not_prunable`。
- 每个结果包含全部合法分片、目标分片、被裁剪分片、覆盖率、原因和结构化 trace。

---

## 8. 确定性与运行标识

- 固定 seed 生成合成数据；固定 `generated_at` 写入 fixture、拓扑和报告。
- 配置指纹：对原始 JSON 对象做排序键、紧凑分隔符的规范化 JSON，再 SHA-256。
- 逻辑/物理数据指纹：基于相对文件名与内容 SHA-256 的规范化 JSON。
- `run_id = "run_" + SHA-256(canonical_json([report_format, old_fp, new_fp, logical_fp, physical_fp, seed]))[:16]`。
- 报告排序键固定；JSON 使用 `sort_keys=True`；Markdown/HTML 从同一报告对象渲染。
- 相同输入、相同 Python 版本、相同 seed 下，fixture 文件、报告文件与发现项字节稳定。
- 报告不嵌入工作区绝对路径；SQLite 使用确定性主键与 `INSERT` 顺序，重复记录同一 run 是幂等替换。

---

## 9. 边界与安全

- 默认只监听 `127.0.0.1`。启动时解析 host，仅接受 `localhost` 或 IP loopback；拒绝 `0.0.0.0`、公网地址和任意主机名。
- HTTP 服务只实现 GET/HEAD，所有响应设置 `Cache-Control: no-store`、`nosniff`、`DENY` 和严格 CSP；不加载外部资源、不执行 JavaScript。
- 路径解析先 URL 解码，拒绝 `..` 与反斜杠；run_id 限定为安全字符集。
- 不读取任何环境密钥、不包含连接串、不发起外部请求。
