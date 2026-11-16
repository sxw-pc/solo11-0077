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

声明了父/子关联的工作区会额外追加一组 `RELATION_*` 发现（见第 6 节）。这些发现同样参与 `fail` 判定与阻塞项；未声明关联时不会产生，报告与旧版本逐字节一致。

---

## 6. 重分片 dry-run

- 对每条逻辑行分别计算旧拓扑目标与新拓扑目标。
- 仅当两个目标不同且均未 drop 时，计入移动矩阵 `(table, from_shard, to_shard)`。
- 移动矩阵按 table/from/to 排序；批次按 cell 顺序每 80 行切分，批次号从 `batch-001` 开始。
- 阻塞项来自：未修复的 critical/high 审计发现；目标拓扑缺表；分片键变更；路由错误；以及声明式父子关联的阻断判定。router 族变化标记为 review。
- 回滚清单固定为 7 步，强调先冻结写入、快照 manifest/校验和、shadow copy、重新审计、双读验证、失败恢复旧拓扑、解冻并归档。
- `safe_to_execute` 只表示静态检查没有 blocking 项；工具自身不会执行任何移动。

### 父/子关联核验

实现：`shardroute/relations.py`，由 `audit.run_audit` 与 `planner.build_plan` 共用同一份判定结果。

单表审计只回答“每行按自己的拓扑落在哪个分片”，不能推出“一条子行能否在本地关联到父行”。父表与子表各自按自己的键落位后，本地关联可能仍拿不到父行。声明式 `relations` 契约补上这一层：

- 对每条被声明的关联，按 `(relation, child_row_id)` 稳定排序逐条判定，输出 `old` 与 `new`（目标拓扑）两份结论，包含结构化证据：子/父逻辑行 ID、规范化引用值、子行物理分片、父行物理分片、父行业务字段指纹、副本清单及其 manifest 证据。
- **引用可用性优先**：引用值缺失、父行在逻辑快照中不存在、父标识不唯一（多行共享同一 `parent_key`）都直接阻断，不进入“可用”分支。父行自身没有可用标识记为 `RELATION_PARENT_KEY_MISSING`。
- **`local_join`**：以物理落位判定。持有子行的每个分片都必须有内容一致（`row_id` 相同且业务字段指纹相同）的父行副本；目标拓扑必须把父、子路由到同一分片，否则 `RELATION_TARGET_SPLIT`。
- **`replicated_parent`**：允许父行在**每个有引用子行的分片**保有一份副本。每份副本都核对业务字段指纹（排除 `row_id`）、`row_id` 身份，并要求其物理文件在分片 manifest 中有匹配的 `name`/`sha256`/`rows` 证据。同名但内容不同、且 `row_id` 相同的父行记为 `RELATION_PARENT_REPLICA_DRIFT`；`row_id` 不同的同键父行视为另一实体，记为 `RELATION_PARENT_CONFLICT`，两者都不会被当作同一父行。
- **目标迁移**：`replicated_parent` 下若目标拓扑把子行移到一个没有父副本的分片，记 `RELATION_TARGET_PARENT_MISSING`；若该分片只有内容漂移的副本，记 `RELATION_TARGET_PARENT_DRIFT`。因此同一个子行、父行与两个拓扑，`local_join` 会阻断迁移，而 `replicated_parent` 在复制证据齐全时允许计划继续。
- 所有 `RELATION_*` critical/high 判定都会进入审计 `findings`，并由 `_relation_blockers` 转成 `PLAN_RELATION_<phase>_<relation>_<code>` blocking 阻塞项，携带与报告一致的 `relation`/`phase`/`relation_code`/`count`/子行证据，使 `safe_to_execute=false`。`replicated_parent` 不因错位而自动放行；它只在有内容一致副本且有 manifest 证据时才通过。
- 未声明 `relations` 的工作区完全保持旧行为：`evaluate_relations` 返回 `None`，审计/计划/报告不出现 `relations` 字段。

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
