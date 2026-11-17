# ShardRoute · 分片路由核验台

ShardRoute 是一个**完全离线、确定性、只用 Python 标准库**的分片路由核验工作台。它回答四个问题：

1. 每一行按声明式拓扑**应该**落在哪个分片？
2. 本地 CSV / JSONL 分片目录中的每一行**实际**落在哪个分片？
3. 从旧拓扑切换到新拓扑会移动多少行，哪些问题必须先修复？
4. 给定查询条件，能否安全裁剪分片，还是必须全分片扫描？

它只读取本地 CSV / JSONL 合成文件与本地 SQLite 台账。逻辑快照可使用 `.csv` 或 `.jsonl`；物理分片目录可混用两种格式，但同一逻辑表不能同时存在两种格式的快照。**不连接数据库、不执行 SQL、不执行迁移、不写物理分片、不发起任何外部网络请求。**

---

## 快速开始

要求 Python 3.10+，无第三方依赖。

```bash
# 1) 生成确定性演示 fixture，跑通校验、审计、计划、查询解释、报告与台账
python -m shardroute demo --workspace .shardroute-demo

# 2) 启动只读本机复核台（默认 127.0.0.1:8765）
python -m shardroute serve --workspace .shardroute-demo --host 127.0.0.1 --port 8765

# 3) 运行全部测试
python -m unittest discover -s tests -v
```

服务启动后访问 <http://127.0.0.1:8765/>。非 loopback 地址（包括 `0.0.0.0`）会在启动时被拒绝。

---

## 演示 fixture

`demo` 使用固定 seed（默认 `20260102`）和固定 UTC 时间生成：

| 逻辑表 | 行数 | 路由 | 分片键 | 演示异常 |
|---|---:|---|---|---|
| `orders` | 200 | hash，16 buckets → 4 shards | `tenant_id` | 2 行错位、2 行跨分片重复、1 行物理缺失 |
| `events` | 120 | range，左闭右开 | `event_day` | 1 行错位、1 个 manifest 行数不一致 |
| `profiles` | 80 | list | `region` | 1 个未登记 manifest 的孤儿 CSV |

生成的工作区结构：

```text
.shardroute-demo/
├── .shardroute-workspace       # 安全再生成标记
├── config/topology_old.json
├── config/topology_new.json
├── logical/{orders,events,profiles}.csv
├── queries/{orders_by_tenant,events_by_day,profiles_by_region}.json
├── shards/<table>/<shard>/part-000.csv
├── shards/<table>/<shard>/_manifest.json
├── reports/report.json
├── reports/report.md
├── reports/report.html
└── ledger.sqlite3
```

仓库内也提交了一份相同的静态 fixture 集合于 `fixtures/`，便于不运行 demo 时直接审阅。所有表名、租户、区域、行 ID 均为合成值。重复运行 `demo` 对同一工作区产生字节一致的数据与报告（SQLite 文件本身不作为字节级复现承诺）。

---

## 命令

```bash
# 配置校验：成功退出码 0，失败退出码 2，并给出稳定错误码和定位
python -m shardroute validate --config fixtures/config/topology_old.json

# 单键路由 trace
python -m shardroute route \
  --config fixtures/config/topology_old.json \
  --table orders --key tenant_demo_007

# 落位审计（使用工作区旧拓扑）
python -m shardroute audit --workspace .shardroute-demo

# 旧→新拓扑移动矩阵、批次、阻塞项、回滚清单（只读 dry-run）
python -m shardroute plan --workspace .shardroute-demo

# 解释全部查询样例；也可只解释一个 --query 文件
python -m shardroute explain --workspace .shardroute-demo
python -m shardroute explain --workspace .shardroute-demo \
  --query .shardroute-demo/queries/orders_by_tenant.json

# 对已存在的工作区重新生成报告与台账
python -m shardroute report --workspace .shardroute-demo

# 为一条已验证的 audit finding 生成最小复现工作区（只读源工作区，写入独立目录）
python -m shardroute repro --workspace .shardroute-demo --out /tmp/repro-ord \
  --mode exact_row --table orders --code MISPLACED_ROW --row-id ord_demo_0018
python -m shardroute repro --workspace .shardroute-demo --out /tmp/repro-hash \
  --mode semantic_family --table events --code MANIFEST_ROW_COUNT

# 只读复核台
python -m shardroute serve --workspace .shardroute-demo \
  --host 127.0.0.1 --port 8765
```

`repro` 把一条 finding 缩减成可交给修复者复跑的最小工作区：可删无关表、文件和行，但保留目标行的旧拓扑路由证据与触发异常的必要成员；每个候选都用真实 scanner+audit 重跑确认，删行后非目标 manifest 字段会一致重建（目标本身的 manifest 因果字段保持原缺陷值）。`--mode exact_row` 保留指定 row_id 的原始记录与路由证据；`--mode semantic_family` 只保留表与异常代码，在多条同类异常中选出包最小的一条作规则级回归例，并在结果中明确标注。`--budget` 限制缩减与最小性证明的审计重跑次数：预算内完成给出 1-minimal 证据（再删任一剩余单元即失去合同或引入旁生高危），预算耗尽只标 `unproven`。包内 `repro.json` 记录合同、保留对象、全部验证与最小性证据；源工作区保持字节不变。

`route` 还支持 `--key-json`，便于传入 JSON 标量或复合键：

```bash
python -m shardroute route --config fixtures/config/topology_old.json \
  --table orders --key-json '"tenant_demo_007"'
```

---

## 输出

每次完整运行会写入：

- **JSON**：机器可读的完整报告，包含配置/逻辑/物理指纹、发现项、移动矩阵、批次、查询解释。
- **Markdown**：便于评审与粘贴到工单的摘要。
- **HTML**：单文件、自包含、内联 CSS，无外部脚本、字体、图片或 CDN。
- **SQLite**：`runs`、`findings`、`moves`、`queries`、`artifacts` 五张表，幂等写入。

控制台命令输出均为确定性排序的 JSON。退出码约定：`0` 成功，`1` 运行时/数据错误，`2` 配置校验失败。

---

## 只读复核台

默认绑定 `127.0.0.1:8765`，只允许 loopback。仅实现 `GET` 和 `HEAD`；其他方法返回 `405`，页面没有写入或迁移按钮。

| 端点 | 用途 |
|---|---|
| `GET /` | 台账中全部 run 的只读概览 |
| `GET /runs/<run_id>` | 单次 run 的发现项、计划、查询解释 |
| `GET /api/health` | 健康与只读标记 |
| `GET /api/runs` | run 列表 JSON |
| `GET /api/runs/<run_id>` | run 详情、findings、moves、queries、artifacts |
| `GET /api/runs/<run_id>/findings` | 可按 `table`、`code`、`severity`、`limit` 过滤 |
| `GET /api/runs/<run_id>/plan` | dry-run 计划与移动矩阵 |
| `GET /api/runs/<run_id>/queries` | 查询裁剪解释 |
| `GET /api/runs/<run_id>/artifacts` | 报告文件指纹 |

服务不加载任何远程资源；CSP 为 `default-src 'none'; style-src 'unsafe-inline'`。

---

## 设计边界

**是：** 静态路由复现、逻辑/物理落位审计、manifest 校验、重分片 dry-run、查询裁剪解释、确定性报告、只读本机复核。

**不是：** 数据库中间件、代理或驱动；SQL 执行器；数据迁移/rebalance 工具；采集/RPA/血缘/湖仓/指标口径平台；报表 CRUD 后台或权限系统。

实现上刻意保持：

- Python 3.10+ 标准库，零第三方依赖。
- 配置只用 JSON，不用 YAML。
- 随机数据只用固定 seed 的 `random.Random`；所有时间固定为 UTC 字面值。
- 报告、发现项、移动矩阵、SQLite 查询结果均稳定排序。
- 文件与报告指纹使用 SHA-256；配置指纹基于规范化 JSON，不受字段顺序影响。
- 所有 fixture 与示例名称明显为合成值，不含真实公司、域名、地址、密钥或个人信息。

详细语义见 [docs/DESIGN.md](docs/DESIGN.md)；配置 schema 与错误码见 [docs/CONFIG.md](docs/CONFIG.md)；报告和台账结构见 [docs/REPORT.md](docs/REPORT.md)。

---

## 测试

```bash
python -m unittest discover -s tests -v
```

测试覆盖规范化、配置错误码、三种路由、扫描与 manifest、异常分类、移动计划、查询裁剪、三种报告、SQLite 幂等、只读 HTTP 服务和完整 demo 的字节级复现。测试只使用临时目录与 `tests/fixtures/` 下的小型 fixture。
