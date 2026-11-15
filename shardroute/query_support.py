"""Shared parsing and result helpers for query explanations."""

from __future__ import annotations

from typing import Any

from .canonical import normalize_value
from .errors import QUERY_INVALID, QueryError
from .models import QueryExplanation, QueryFilter, TableTopology, Topology

OPS = {"eq", "in", "gte", "gt", "lte", "lt", "ne", "is_null", "not_null"}
RANGE_OPS = {"eq", "in", "gte", "gt", "lte", "lt", "is_null"}

def _parse(topology: Topology, query: Any) -> tuple[str, str, str, list[QueryFilter]]:
    if not isinstance(query, dict):
        raise QueryError(QUERY_INVALID, "query must be a JSON object")
    table = query.get("table")
    if not isinstance(table, str) or table not in topology.tables:
        raise QueryError(QUERY_INVALID, "query references an unknown table", details={"table": table})
    name = query.get("name") if isinstance(query.get("name"), str) else table
    description = query.get("description") if isinstance(query.get("description"), str) else ""
    raw_filters = query.get("filters")
    if not isinstance(raw_filters, list) or not raw_filters:
        raise QueryError(QUERY_INVALID, "query must contain a non-empty filters list", table)
    filters: list[QueryFilter] = []
    for index, item in enumerate(raw_filters):
        location = f"filters[{index}]"
        if not isinstance(item, dict):
            raise QueryError(QUERY_INVALID, "filter must be an object", location)
        field, op = item.get("field"), item.get("op")
        if not isinstance(field, str) or not field:
            raise QueryError(QUERY_INVALID, "filter field must be a non-empty string", location)
        if op not in OPS:
            raise QueryError(QUERY_INVALID, f"unsupported filter op {op!r}", location)
        value = item.get("value")
        if op == "in" and not isinstance(value, list):
            raise QueryError(QUERY_INVALID, "in filter requires a list value", location)
        filters.append(QueryFilter(field, op, value))
    return table, name, description, filters

def _normalized(table: TableTopology, value: Any) -> Any:
    return normalize_value(value, table.case_sensitive)

def _key_filters(table: TableTopology, filters: list[QueryFilter]) -> list[QueryFilter]:
    return [item for item in filters if item.field in table.key_fields]

def _result(
    table: TableTopology,
    name: str,
    description: str,
    filters: list[QueryFilter],
    targeted: set[str],
    decision: str,
    reason: str,
    trace: dict[str, Any],
    *,
    force_not_prunable: bool = False,
) -> QueryExplanation:
    all_shards = table.table_shards()
    allowed = set(all_shards)
    targeted = {shard for shard in targeted if shard in allowed}
    ordered = [shard for shard in all_shards if shard in targeted]
    if force_not_prunable:
        decision, ordered = "not_prunable", list(all_shards)
    elif decision == "full_scan" or len(ordered) == len(all_shards):
        decision, ordered = "full_scan", list(all_shards)
    else:
        decision = "pruned"
    return QueryExplanation(
        name=name,
        table=table.table,
        decision=decision,
        reason=reason,
        key_fields=table.key_fields,
        all_shards=all_shards,
        targeted_shards=ordered,
        filters=filters,
        trace=trace,
        description=description,
    )
