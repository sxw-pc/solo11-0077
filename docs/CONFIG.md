# ShardRoute 配置规范

## 1. 文件与编码

拓扑配置是单个 UTF-8 JSON 对象。工具不接受 YAML、注释、尾随逗号或重复键；重复 JSON 键在解析阶段以 `CFG_SCHEMA` 拒绝。默认文件名为：

```text
<workspace>/config/topology_old.json
<workspace>/config/topology_new.json
```

旧拓扑用于路由复现与落位审计，新拓扑只用于 dry-run 重分片计划。两者 schema 完全相同。

```bash
python -m shardroute validate --config fixtures/config/topology_old.json
```

成功退出码为 `0`，输出 `valid=true` 和配置指纹；校验失败退出码为 `2`。运行期数据错误退出码为 `1`。

---

## 2. 顶层 schema

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `version` | 正整数 | 是 | schema 版本，目前为 `1` |
| `shards` | 字符串数组 | 是 | 全局合法分片 ID，至少一个，不能重复 |
| `tables` | 对象数组 | 是 | 至少一张表，表名不能重复 |
| `generated_at` | UTC 时间字符串 | 否 | ISO-8601；缺省为 `1970-01-01T00:00:00Z` |
| `description` | 字符串 | 否 | 人工说明，会进入报告 |

表名、分片 ID 必须匹配 `^[a-z][a-z0-9_]{0,62}$`：小写字母开头，仅含小写字母、数字、下划线，最长 63 个字符。该规则拒绝 `/`、`\`、`..` 等路径字符。

未知顶层字段和未知表级字段不会改变行为，但会保留在配置对象中并参与指纹。要控制配置身份，请删除无用字段。

---

## 3. 表级公共字段

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `table` | 字符串 | 是 | 逻辑表名，同时决定 CSV 文件名 |
| `router` | 字符串 | 是 | `hash`、`range` 或 `list` |
| `key` | 字符串或字符串数组 | 是 | 分片键字段；单键可写字符串，复合键写数组 |
| `null_policy` | 字符串 | 否 | `error`（默认）、`route_default`、`drop` |
| `default_shard` | 字符串或 null | 否 | 必须引用 `shards` 中的分片 |
| `case_sensitive` | 布尔值 | 否 | 默认 `true`；`false` 时键值做 `casefold()` |

约束：

- `key` 不能为空；分量不能重复，不能为空字符串。
- `router=range` 和 `router=list` 只允许单键。
- `hash` 复合键最多 8 个分量。
- `null_policy=route_default` 时必须提供 `default_shard`。
- `default_shard` 不依赖 null 策略；list/range 的未命中回退也会使用它。
- `case_sensitive=false` 对 hash 规范化、range 边界比较、list 查找以及查询裁剪使用同一套规范化。

---

## 4. `router=hash`

额外字段：

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `buckets` | 正整数 | 是 | bucket 数量 |
| `bucket_map` | 数组或对象 | 是 | 每个 bucket 到分片的映射 |

`bucket_map` 的两种形式：

```json
"bucket_map": ["shard_00", "shard_00", "shard_01", "shard_01"]
```

```json
"bucket_map": {
  "0": "shard_00",
  "1": "shard_00",
  "2": "shard_01",
  "3": "shard_01"
}
```

- 数组长度必须严格等于 `buckets`。
- 对象必须恰好包含 `"0"` 到 `"buckets-1"`，不允许多、少或非数字键。
- 不同 bucket 可以映射到同一分片；这是合法的权重/合并方式。bucket 位置本身必须被完整覆盖。
- 每个目标分片必须存在于顶层 `shards`。

示例：

```json
{
  "table": "orders",
  "router": "hash",
  "key": "tenant_id",
  "null_policy": "route_default",
  "default_shard": "shard_00",
  "case_sensitive": true,
  "buckets": 16,
  "bucket_map": [
    "shard_00", "shard_00", "shard_00", "shard_00",
    "shard_01", "shard_01", "shard_01", "shard_01",
    "shard_02", "shard_02", "shard_02", "shard_02",
    "shard_03", "shard_03", "shard_03", "shard_03"
  ]
}
```

哈希算法固定为：

```text
SHA-256(UTF-8(normalized composite key))
→ 前 8 字节大端整数
→ 对 buckets 取模
→ bucket_map[bucket]
```

---

## 5. `router=range`

额外字段：

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `ranges` | 对象数组 | 是 | 有序、连续的左闭右开区间 |

每个区间包含：

| 字段 | 类型 | 说明 |
|---|---|---|
| `lower` | 非空字符串 | 下界，包含 |
| `upper` | 非空字符串或 null | 上界，不包含；最后一段可为 null |
| `shard` | 字符串 | 目标分片 |

- 区间按数组顺序构成全序，后一段 `lower` 必须等于前一段 `upper`。
- 不允许重叠、逆序、缺口，也不允许 open-ended 区间后再出现区间。
- 所有边界使用字符串字典序比较；日期/数值需要零填充。
- 未命中任何区间时使用 `default_shard`，否则运行期 `ROUTE_NO_MATCH`。

示例：

```json
{
  "table": "events",
  "router": "range",
  "key": "event_day",
  "null_policy": "error",
  "ranges": [
    {"lower": "2024-01-01", "upper": "2024-04-01", "shard": "shard_10"},
    {"lower": "2024-04-01", "upper": "2024-07-01", "shard": "shard_11"},
    {"lower": "2024-07-01", "upper": "2024-10-01", "shard": "shard_12"},
    {"lower": "2024-10-01", "upper": null, "shard": "shard_13"}
  ]
}
```

---

## 6. `router=list`

额外字段：

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `list_map` | 对象 | 是 | 规范化后的键值到分片的精确映射 |

- 键必须是非空 JSON 字符串；值的分片必须存在。
- `case_sensitive=false` 时，加载时对映射键做去空白和 `casefold()`；折叠后重复的键报 `CFG_LIST_DUPLICATE`。
- 未命中 `list_map` 时使用 `default_shard`，没有默认分片则运行期 `ROUTE_NO_MATCH`。

示例：

```json
{
  "table": "profiles",
  "router": "list",
  "key": "region",
  "null_policy": "route_default",
  "default_shard": "shard_23",
  "list_map": {
    "region_alpha": "shard_20",
    "region_beta": "shard_21",
    "region_gamma": "shard_22"
  }
}
```

---

## 7. 校验错误码

一次校验会收集尽可能多的问题，按 `(code, location, message)` 排序。返回对象包含 `valid`、`fingerprint`、`issue_count` 和 `issues[]`；每个 issue 有 `code`、`message`、`location`、`details`。

| 错误码 | 触发条件 |
|---|---|
| `CFG_SCHEMA` | 顶层/字段类型错误、JSON 解析失败、空 tables/shards、重复表、非法分片 ID、未知字段类型 |
| `CFG_TABLE_NAME` | 表名不匹配安全字符集 |
| `CFG_SHARD_DUPLICATE` | 顶层 `shards` 中出现重复 ID |
| `CFG_SHARD_UNKNOWN` | range/list/bucket_map/default_shard 引用未声明分片 |
| `CFG_ROUTER_UNKNOWN` | `router` 不是 `hash`、`range`、`list` |
| `CFG_KEY_MISSING` | `key` 缺失/为空/重复、range/list 非单键、hash 超过 8 个分量 |
| `CFG_BUCKET_RANGE` | `buckets` 非正整数，或 `bucket_map` 未完整覆盖全部 bucket |
| `CFG_RANGE_OVERLAP` | 区间下界不小于上界、重叠、逆序、或 open-ended 区间后仍有区间 |
| `CFG_RANGE_GAP` | 相邻区间首尾不连续 |
| `CFG_LIST_DUPLICATE` | 规范化后 list 键重复 |
| `CFG_NULL_POLICY` | null 策略不是 `error`、`route_default`、`drop` |
| `CFG_DEFAULT_MISSING` | `null_policy=route_default` 但没有合法默认分片 |

配置指纹是原始 JSON 对象经排序键、紧凑分隔符序列化后的 SHA-256。改变字段顺序不会改变指纹；改变任意值会改变指纹。指纹在报告和 run_id 中使用。

---

## 8. 路由期数据错误

配置合法后，逐行路由仍可能产生运行期错误：

| 错误码 | 含义 |
|---|---|
| `ROUTE_KEY_MISSING` | 行/调用缺少分片键字段或分量数量不符 |
| `ROUTE_NULL_KEY` | null 键命中 `null_policy=error` |
| `ROUTE_NO_MATCH` | range/list 未命中且没有默认分片 |
| `ROUTE_BAD_VALUE` | 分片键为对象、数组等非标量 |
| `ROUTE_UNKNOWN_TABLE` | 调用引用未在拓扑中声明的表 |

输入扫描错误使用 `SCAN_IO`、`SCAN_CSV`、`SCAN_JSONL`、`SCAN_ROW_ID` 和 `SCAN_DUPLICATE_ID`。JSONL 的每个非空行必须是含 `row_id` 的 JSON 对象；JSON `null` 保留为 null，以便触发 null 策略。

在 `audit` 中这些会记录为 `ROUTE_ERROR` 发现项；在 `plan` 中计入 `route_errors` 并形成 blocking 阻塞项。
