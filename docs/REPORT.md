# ShardRoute 报告与台账

## 1. 输出位置与顺序

每次 `demo` 或 `report` 运行会把三份报告写入工作区：

```text
<workspace>/reports/report.json
<workspace>/reports/report.md
<workspace>/reports/report.html
<workspace>/ledger.sqlite3
```

三份报告都从同一个内存报告对象渲染。JSON 使用 `sort_keys=True`、两空格缩进、UTF-8、结尾换行；Markdown 的表格按报告数组顺序输出；HTML 只使用内联样式并转义所有文本。报告不包含工作区绝对路径，因此同一 fixture 在不同目录下生成相同字节。

`write_reports` 返回的 artifact 列表按路径排序：

```text
report.html, report.json, report.md
```

每个 artifact 记录相对路径、原始字节的 SHA-256 和字节数。

---

## 2. run_id 与指纹

所有哈希均为 SHA-256，所有规范化 JSON 都使用排序键和紧凑分隔符。

- **配置指纹**：规范化原始拓扑 JSON 后取 SHA-256。
- **逻辑指纹**：`{"files": [{"name", "sha256", "rows"}]}` 的规范化 JSON 哈希，按表名排序。
- **物理指纹**：`{"files": [{"path", "sha256"}]}` 的规范化 JSON 哈希，覆盖 CSV/JSONL 与 `_manifest.json`，按相对路径稳定排序。
- **finding_id**：`"fnd_" + SHA-256(canonical_json([code, table, shard, row_id, detail]))[:16]`。
- **run_id**：

```text
"run_" + SHA-256(canonical_json([
  "shardroute-report/1",
  old_topology_fingerprint,
  new_topology_fingerprint,
  logical_fingerprint,
  physical_fingerprint,
  seed
]))[:16]
```

同一工作区、同一 seed、同一拓扑和同一 CSV 内容会得到同一 `run_id`。时间戳不参与 run_id；数据、拓扑或 seed 任一变化都会产生新 run。重复运行相同输入时，台账对同一 run 做幂等替换，而不会重复插入。

---

## 3. JSON 报告结构

顶层对象：

| 字段 | 类型 | 说明 |
|---|---|---|
| `format` | 字符串 | 固定为 `shardroute-report/1` |
| `run_id` | 字符串 | 稳定运行标识 |
| `generated_at` | 字符串 | 来自旧拓扑的规范化 UTC 时间 |
| `seed` | 整数 | fixture/运行 seed |
| `status` | 字符串 | `pass`、`attention`、`fail` |
| `fingerprints` | 对象 | `topology_old`、`topology_new`、`logical`、`physical` |
| `topologies` | 对象 | `old` / `new` 的版本、时间、描述、指纹、分片与表摘要 |
| `summary` | 对象 | 发现数、移动数、批次、阻塞、查询裁剪数、是否可安全执行 |
| `audit` | 对象 | 审计状态、汇总、按表统计与发现数组 |
| `plan` | 对象 | 移动矩阵、批次、阻塞项、回滚清单 |
| `queries` | 数组 | 查询裁剪解释 |
| `disclaimer` | 字符串 | 静态离线、不连接数据库、不移动数据的声明 |

状态规则：审计存在 critical/high 发现，或计划存在 blocking 阻塞项，则 `fail`；否则审计为 `attention` 时保持 `attention`；全干净为 `pass`。

### audit 对象

```text
status: "pass" | "attention" | "fail"
summary:
  total_findings, critical_or_high,
  by_severity: {critical, high, medium, low, info},
  by_code: {CODE: count}
tables:
  <table>:
    logical_rows, physical_rows, eligible_shards,
    findings, critical, by_code
findings[]:
  finding_id, code, severity, table, shard, row_id, message, detail
hash_skew_diagnostics:
  <table>:
    hypothesis: "independent_canonical_keys_uniform_buckets"
    scope_note, alpha, applicable, reason?
    denominator:
      logical_rows, eligible_hash_rows, excluded_rows,
      exclusions_by_reason, exclusion_contract
    key_groups:
      count, total_weight,
      groups[]: {canonical_key, weight, bucket, routed_shard, row_ids[]}
    bucket_baseline:
      buckets, bucket_map, shard_probabilities
    event:
      kind, threshold, observed_max_load, observed_max_shard,
      observed_shard_loads, definition
    certificate:
      type: "exact_weighted_enumeration" | "conservative_hoeffding_bound"
      tail_probability | null, tail_probability_upper_bound | null,
      decision: "significant" | "not_significant" | "unknown",
      decision_rule, budget, unknown_reason
```

发现数组按严重度、表名、代码、分片、row_id、finding_id 稳定排序。`hash_skew_diagnostics` 是零假设 `independent_canonical_keys_uniform_buckets` 下的 hash 负载偏斜尾风险诊断，统计单位为规范化键群而非逻辑行；其口径、排除合同、三态结论与预算退化规则见 `docs/DESIGN.md` 第 5a 节。顶层 `summary` 另含 `skew_significant_tables` 与 `skew_unknown_tables` 两个计数。

### plan 对象

```text
old_fingerprint, new_fingerprint, total_rows, moved_rows,
unchanged_rows, dropped_rows, route_errors, blocked_rows,
move_cells[]:  {table, from_shard, to_shard, row_count}
batches[]:     {batch_id, phase, table, source, target, row_count}
blockers[]:    {code, severity, table, count, message}
rollback_checklist[]: {order, action, verification}
safe_to_execute: bool
summary: {tables, moved_by_table, move_cell_count, batch_count, max_batch_rows}
```

`move_cells` 是旧拓扑目标与新拓扑目标不同的行数聚合，按 `(table, from_shard, to_shard)` 排序。批次是 shadow-copy 阶段每 80 行一片，`batch_id` 为 `batch-001` 起的三位序号。blocking 阻塞项使 `safe_to_execute=false`；review 项不会单独阻止计划。

### queries 数组

每个元素：

```text
name, table, description, decision, reason,
key_fields, filters[],
all_shards, targeted_shards, pruned_shards,
coverage_ratio, trace
```

`decision` 只允许：

- `pruned`：目标集合是合法分片的真子集（包括 0 个分片）。
- `full_scan`：没有分片键谓词，或裁剪无法排除任何分片。
- `not_prunable`：存在分片键谓词，但证据不足或路由失败，保守扫描全部。

`coverage_ratio = len(targeted_shards) / len(all_shards)`，保留四位小数。`trace` 记录路由模式、映射值、区间、比较或错误。

---

## 4. Markdown 报告

固定章节顺序：

1. 标题与运行元数据（run_id、UTC 时间、status、seed、拓扑/逻辑指纹）。
2. Executive summary 指标表。
3. Audit tables 按表统计。
4. Findings 表。
5. Hash load-skew diagnostics（每张表的零假设、分母、键群权重、桶基线概率、事件定义与三态证书；无诊断时整节省略）。
6. Reshard move matrix 表。
7. Batch plan 表。
8. Blockers 列表。
9. Query pruning 表。
10. Rollback checklist。
11. Boundary notice 与复现命令。

单元格中的 `|`、回车和换行会被转义为单行文本；空值显示为 `-`。Markdown 不嵌入 HTML 或脚本。

---

## 5. HTML 报告

`report.html` 是单文件自包含页面：

- 只有内联 `<style>`，没有 `<script>`、`<img>`、`<link>`、`<iframe>`、`data:` 或远程 URL。
- 所有动态文本经 HTML 转义。
- 固定展示同样的摘要、发现、Hash load-skew 诊断（`#skew` 锚点，含逐键群表与证书徽章）、计划、查询、回滚清单与免责声明。
- 文件字节由同一报告对象确定性生成。

HTTP 复核台返回页面时额外设置严格 CSP；静态 `report.html` 本身也不加载任何外部资源。

---

## 6. SQLite 台账

`Ledger` 初始化时执行幂等 schema。数据库只用于本地归档，不是数据源；打开后只执行参数化查询，不接纳任意 SQL 字符串。

### runs

| 列 | 说明 |
|---|---|
| `run_id` PK | 稳定运行标识 |
| `format` | 报告格式 |
| `schema_version` | 台账 schema 版本，目前 `1` |
| `generated_at` | UTC 时间 |
| `seed` | seed |
| `status` / `audit_status` | 报告状态与审计状态 |
| `old_fingerprint` / `new_fingerprint` | 拓扑指纹 |
| `logical_fingerprint` / `physical_fingerprint` | 数据指纹 |
| `summary_json` | 报告 summary |
| `tables_json` | 按表审计统计 |
| `plan_json` | 完整计划 |
| `created_at` | 等于报告的 generated_at |

### findings

`run_id` + `finding_id` 为主键；其余列为 `code`、`severity`、`table_name`、`shard`、`row_id`、`message`、`detail_json`。查询接口把 `table_name` 映射回 `table`，`detail_json` 解码为 `detail`。

### moves

`run_id` + `table_name` + `from_shard` + `to_shard` 为主键，`row_count` 为整数。数据来自 `plan.move_cells`。

### queries

`run_id` + `query_name` 为主键；保存 `table_name`、`decision`、`reason`、`targeted_json` 和完整的 `result_json`。读取时按 `query_name` 排序并还原 JSON。

### artifacts

`run_id` + `path` 为主键；保存 `sha256` 与 `bytes`。同一 run 重新归档时，旧的 findings/moves/queries/artifacts 先进删除再插入，保证幂等。

| 表 | 主键 |
|---|---|
| `runs` | `run_id` |
| `findings` | `(run_id, finding_id)` |
| `moves` | `(run_id, table_name, from_shard, to_shard)` |
| `queries` | `(run_id, query_name)` |
| `artifacts` | `(run_id, path)` |

台账查询结果使用稳定 `ORDER BY`：runs 按 `generated_at DESC, run_id DESC`；findings 按严重度和 finding_id；moves 按表/源/目标；queries 按名称；artifacts 按路径。

---

## 7. 只读复核台

`serve` 以 `mode=ro` 和 `PRAGMA query_only=ON` 打开 SQLite，只提供 GET/HEAD。JSON API 直接返回上述结构，HTML 页面提供同一数据的只读展示；任何 POST/PUT/PATCH/DELETE 返回 `405`，不存在写入端点。
