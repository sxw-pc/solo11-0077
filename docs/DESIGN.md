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

---

## 10. 金额内容保真审计（amount fidelity）

落位正确只证明 `row_id -> shard`，不证明物理订单金额忠实于逻辑快照。金额审计是叠加在落位审计之上的只读阶段，仅在拓扑表显式声明 `amount_contracts` 时启用；未声明的表与其他表保持原有审计语义不变。

### 10.1 精确十进制词法

实现：`shardroute/decimal_money.py`、`shardroute/scanner.py`。

- 金额一律保留来源词法：CSV 单元格本就是字符串；JSONL 解析使用 `json.loads(..., parse_float=str)`，JSON 数字以原始十进制文本进入扫描结果，二进制 `float` 不进入任何金额路径。
- 词法必须匹配 `^[+-]?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$`：拒绝指数计数（`5e-3`）、空白、下划线、`NaN`、`Infinity`、首尾点号；任何拒绝都是显式 finding，不允许 float 转写后判通过。
- 逻辑金额允许任意小数位，以 `decimal.Decimal`（60 位上下文）精确表示；物理金额最多 2 位小数，超精度以 `AMOUNT_OVER_PRECISION` 拒绝；负金额以 `AMOUNT_NEGATIVE_AMOUNT` 拒绝。
- 所有比较与合计均在整数分（cent）或精确 `Decimal` 上进行；半偶舍入（banker's rounding，`ROUND_HALF_EVEN`）只作用于精确十进制，不作用于二进制近似值。

### 10.2 落位前置与身份证明

实现：`shardroute/amount_audit.py`。一行必须先通过以下全部条件，才允许进入任何金额合计：

1. 逻辑行可路由且未 drop；物理恰好一个落位且分片等于期望分片（缺失、重复、错位、路由错误的行一律排除并保留原因码）。
2. `tenant_field`、`channel_field` 在逻辑/物理两侧都存在且表示同一实体（字符串去空白后相等；禁止 float 参与身份证明）。
3. 两侧金额词法都能精确解析、非负，且物理侧不超精度。

被排除的行不参与合格总额，并使该合同的整体裁定为 `unknown`；金额异常使用独立的 `AMOUNT_*` finding 代码，绝不伪装成 `MISPLACED_ROW` 等路由错误。

### 10.3 两个授权合同

| 合同 | 单行规则 | 交叉边际 |
|---|---|---|
| `row_half_even` | 物理 2 位 cent 必须精确等于逻辑金额按半偶规则单独舍入的值 | 无 |
| `dual_margin` | 每行可在逻辑金额相邻的两个 cent 值（floor/ceil，精确值时退化为单点）中择一 | 每个 `tenant_id` 的物理合计与每个 `channel` 的物理合计，都必须分别等于本组逻辑精确金额之和再半偶舍入的值 |

两个合同独立裁定、独立 finding、独立计划门禁；`dual_margin` 的单行宽松区间不能替代两个交叉边际。仅逐行比较 floor/ceil 会把全零物理误判合格；只按 tenant 贪心补一个 cent 而忽略 channel 维度会违反交叉约束。

### 10.4 只读修复证人与不可行证据

`dual_margin` 失败时，分支限界求解器在显式节点预算（`repair_budget`）内搜索**改动行数最少**的可行 cent 分配：

- 变量按 `row_id` 排序、选项按 cent 升序，结果确定可重放；前向检查用每个后缀分组的可达 `[min, max]` 窗口剪枝。
- 找到证人：返回每行 `from/to` cent、行身份（tenant/channel）、两个维度的证人和边际总额，并标记 `minimum_proven=true`。证人是只读证据，不写物理文件、不产生迁移动作。
- 预算耗尽：只能返回 `budget_exceeded`，合同裁定 `unknown` 并产生 `AMOUNT_DUAL_MARGIN_UNPROVEN`，绝不宣称可修复。
- 穷尽后无解：返回可核验的不可行证据——分组边界违反清单、两个分区的目标总额是否一致、变量顺序与已消耗节点数，配合 `AMOUNT_DUAL_MARGIN_INFEASIBLE`。

### 10.5 同 run 因果链

扫描器金额词法/来源的变化依次改变：允许 cent 区间与两个边际（audit）→ 计划安全结论（planner 的 `amount_gates`）→ 报告证书（report 的 `amount_certificates`）。每个合同 verdict 携带 `scanner_fingerprint`、`allowance_fingerprint`、`margin_fingerprint` 与 `verdict_fingerprint`，使该因果链在同一份 run 证据中可逐环复核。
