# ShardRoute 报告与台账

## 1. 输出位置与顺序

报告通过可恢复的两阶段发布协议落地。每个 run 拥有不可变的版本目录，上一版 A 的文件在 B 发布前不会被修改或删除：

```text
<workspace>/reports/current.json                       # 当前版本指针（原子替换）
<workspace>/reports/report.json                        # 当前版本的稳定入口（与版本目录同字节）
<workspace>/reports/report.md
<workspace>/reports/report.html
<workspace>/reports/runs/<run_id>/report.json          # 不可变版本目录
<workspace>/reports/runs/<run_id>/report.md
<workspace>/reports/runs/<run_id>/report.html
<workspace>/reports/runs/<run_id>/manifest.json        # run_id + 指纹 + 全部产物哈希
<workspace>/reports/.staging-<run_id>/                 # 暂存目录（一次 rename 原子转正）
<workspace>/reports/quarantine/                        # 被放弃的未发布版本
<workspace>/ledger.sqlite3
```

三份报告都从同一个内存报告对象渲染。JSON 使用 `sort_keys=True`、两空格缩进、UTF-8、结尾换行；Markdown 的表格按报告数组顺序输出；HTML 只使用内联样式并转义所有文本。报告不包含工作区绝对路径，因此同一 fixture 在不同目录下生成相同字节。

### 1.1 发布状态机

`publications` 表为每个 run 记录三种状态，且任意时刻最多一个 `current`（部分唯一索引保证）：

| 状态 | 含义 |
|---|---|
| `staged` | 文件与台账已提交，但尚未成为当前版本；只读入口不把它当作可执行依据 |
| `current` | 唯一当前版本，`current.json` 与顶层报告均指向它 |
| `archived` | 曾经的当前版本，证据完整、仍可查询，但不再是当前预检 |

发布顺序：

1. 渲染到 `reports/.staging-<run_id>/`，再单次原子 `rename` 为 `reports/runs/<run_id>/`，读者永远看不到半写入目录。
2. 一个 SQLite 事务写入 runs/findings/moves/queries/artifacts 与 `staged` 发布行。
3. 交叉校验：版本目录文件哈希 == manifest == 台账 artifacts，且 run_id 与 `safe_to_execute` 完全一致。
4. 原子替换 `reports/current.json` 并刷新顶层三份报告。
5. 同一事务内把发布行翻转为 `current`，旧 current 变为 `archived`。

### 1.2 三个中断切点与恢复

| 切点 | 磁盘表现 | 台账表现 | 允许的恢复 |
|---|---|---|---|
| 1. 部分报告写入 | 残留 `.staging-*` | 无该 run 的发布行 | 只能隔离丢弃并重建，A 保持当前 |
| 2. 报告全部写完、台账未提交 | `runs/<B>/` 完整 | 无 `staged` 行或行不可校验 | 只能隔离丢弃并重建，A 保持当前 |
| 3. 文件与台账均已持久化、指针未切换 | `runs/<B>/` 完整 | `staged` 行且全部校验通过 | 前滚成 B，或回退 A 并隔离 B |

切点 1、2 永远不允许发布未校验的 B；只有切点 3 允许前滚。所有恢复决策（`discard` / `roll_forward` / `rollback`，含原因与隔离路径）写入 `recovery_events`，可经 CLI `status` 与 HTTP `GET /api/recovery` 查询。事件以 run_id+类型+决策派生稳定 event_id，重复恢复幂等、不产生重复记录。

`write_reports` 仍保留为直接写顶层 `reports/` 的低层接口（供单元测试使用）；流水线统一走 `shardroute.publish.publish_run`。

`write_reports` 与 manifest 中的 artifact 列表按路径排序：

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
```

发现数组按严重度、表名、代码、分片、row_id、finding_id 稳定排序。

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
5. Reshard move matrix 表。
6. Batch plan 表。
7. Blockers 列表。
8. Query pruning 表。
9. Rollback checklist。
10. Boundary notice 与复现命令。

单元格中的 `|`、回车和换行会被转义为单行文本；空值显示为 `-`。Markdown 不嵌入 HTML 或脚本。

---

## 5. HTML 报告

`report.html` 是单文件自包含页面：

- 只有内联 `<style>`，没有 `<script>`、`<img>`、`<link>`、`<iframe>`、`data:` 或远程 URL。
- 所有动态文本经 HTML 转义。
- 固定展示同样的摘要、发现、计划、查询、回滚清单与免责声明。
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

### publications

| 列 | 说明 |
|---|---|
| `run_id` PK | 对应 runs 表 |
| `state` | `staged` / `current` / `archived`；部分唯一索引保证至多一个 `current` |
| `manifest_json` | 与磁盘 `manifest.json` 同内容的规范化 JSON |
| `created_at` / `updated_at` | 取报告 `generated_at`，保持确定性 |

### recovery_events

| 列 | 说明 |
|---|---|
| `event_id` PK | `evt_` 前缀的稳定派生 ID，重复恢复不会重复入账 |
| `run_id` | 被恢复的 run |
| `event_type` | `partial_reports` / `ledger_not_committed` / `pointer_not_switched` / `operator_recovery` / `legacy_adoption` |
| `decision` | `discard` / `roll_forward` / `rollback` |
| `reason` / `detail_json` | 人类可读依据与结构化细节（含隔离路径、前后 current） |
| `created_at` | 取报告 `generated_at` |

---

## 7. 只读复核台

`serve` 以 `mode=ro` 和 `PRAGMA query_only=ON` 打开 SQLite，只提供 GET/HEAD。JSON API 直接返回上述结构，HTML 页面提供同一数据的只读展示；任何 POST/PUT/PATCH/DELETE 返回 `405`，不存在写入端点。

- `GET /api/current` 返回经 `resolve_current` 校验的当前版本：台账 current 标记、`current.json` 指针、版本目录哈希、顶层报告哈希必须全部指向同一 run_id 且 `safe_to_execute` 一致，任一不符返回 500 而不是拼凑结论。
- `GET /api/runs` 只返回带发布行的 run，并标注 `publication_state` 与 `is_current`；`staged` run 可作为归档证据查看，但永远不会被标为当前。
- `GET /api/recovery[?run_id=...]` 返回恢复决策流水。
- CLI `shardroute status --workspace ...` 输出与 HTTP 相同的当前版本校验结果；`shardroute recover --run-id ... --roll-forward|--rollback` 仅对切点 3 的 `staged` 候选生效，两条路线都写 `recovery_events`。

## 8. 故障注入与可恢复性验证

`scripts/fault_injection_demo.py` 在独立子进程中分别武装三个切点（环境变量 `SHARDROUTE_FAULT`，仅测试用途），模拟真实崩溃后重启流水线或执行恢复命令，并核对报告文件、台账、指针与只读复核页四个面的 run_id 与 `safe_to_execute` 完全一致。对应单元测试位于 `tests/test_publish.py`。
