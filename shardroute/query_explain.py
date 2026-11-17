"""Explain whether query predicates can safely prune shards."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from .errors import QUERY_INVALID, QueryError, ShardRouteError
from .models import QueryExplanation, QueryFilter, TableTopology, Topology
from .query_support import OPS, RANGE_OPS, _key_filters, _normalized, _parse, _result
from .router import Router

def _explain_hash(
    router: Router,
    table: TableTopology,
    name: str,
    description: str,
    filters: list[QueryFilter],
    key_filters: list[QueryFilter],
) -> QueryExplanation:
    eqs: dict[str, list[Any]] = {field: [] for field in table.key_fields}
    for item in key_filters:
        if item.op == "eq":
            eqs[item.field].append(_normalized(table, item.value))
    trace: dict[str, Any] = {"router": "hash", "key_equalities": eqs}
    if all(eqs[field] for field in table.key_fields):
        values = []
        for field in table.key_fields:
            distinct = {str(value) for value in eqs[field]}
            if len(distinct) > 1:
                trace["contradiction"] = True
                return _result(table, name, description, filters, set(), "pruned", "contradictory equality filters", trace)
            values.append(eqs[field][0])
        if any(value is None for value in values):
            if table.null_policy == "drop":
                return _result(table, name, description, filters, set(), "pruned", "null keys are dropped", trace)
            if table.null_policy == "route_default" and table.default_shard:
                trace["null_policy"] = "route_default"
                return _result(
                    table, name, description, filters, {table.default_shard}, "pruned",
                    "null key uses the default shard", trace,
                )
            return _result(
                table, name, description, filters, set(), "", "null key is rejected",
                trace, force_not_prunable=True,
            )
        try:
            route = router.route(table.table, values[0] if len(values) == 1 else values)
        except (ShardRouteError, KeyError, IndexError) as exc:
            trace["route_error"] = str(exc)
            return _result(
                table, name, description, filters, set(), "", "equality could not be routed",
                trace, force_not_prunable=True,
            )
        trace["route"] = route.as_dict()
        if route.dropped or not route.target_shard:
            return _result(table, name, description, filters, set(), "pruned", "equality is dropped", trace)
        return _result(
            table, name, description, filters, {route.target_shard}, "pruned",
            "all shard-key components have equality predicates", trace,
        )
    if key_filters:
        reason = (
            "composite hash keys need equality predicates on every component"
            if len(table.key_fields) > 1
            else "hash-key predicate is not an equality test; all shards are required"
        )
        return _result(table, name, description, filters, set(), "", reason, trace, force_not_prunable=True)
    trace["reason"] = "no shard-key predicate"
    return _result(table, name, description, filters, set(), "full_scan", "no shard-key predicate; scan all shards", trace)

def _explain_list(
    router: Router,
    table: TableTopology,
    name: str,
    description: str,
    filters: list[QueryFilter],
    key_filters: list[QueryFilter],
) -> QueryExplanation:
    trace: dict[str, Any] = {"router": "list", "mapped_values": []}
    targets: set[str] = set()
    prune_candidates = [item for item in key_filters if item.op in {"eq", "in", "is_null"}]
    if not prune_candidates:
        reason = (
            "list-key predicates are not equality tests; all shards are required"
            if key_filters
            else "no shard-key predicate; scan all shards"
        )
        return _result(
            table, name, description, filters, set(), "full_scan" if not key_filters else "",
            reason, trace, force_not_prunable=bool(key_filters),
        )
    for item in prune_candidates:
        values = item.value if item.op == "in" else [item.value]
        for value in values:
            normalized = _normalized(table, value)
            try:
                route = router.route(table.table, normalized)
            except ShardRouteError as exc:
                trace.setdefault("route_errors", []).append(exc.as_dict())
                return _result(
                    table, name, description, filters, set(), "", "a list value cannot be routed",
                    trace, force_not_prunable=True,
                )
            trace["mapped_values"].append(
                {"value": normalized, "shard": route.target_shard, "rule": route.rule}
            )
            if route.target_shard:
                targets.add(route.target_shard)
    return _result(
        table, name, description, filters, targets, "pruned",
        "list membership maps to a subset of shards", trace,
    )

def _range_targets(table: TableTopology, lower: str | None, upper: str | None, upper_inclusive: bool) -> tuple[list[str], list[dict[str, Any]]]:
    matched: list[str] = []
    explanations = []
    for index, rule in enumerate(table.ranges):
        rule_upper = _normalized(table, rule.upper) if rule.upper is not None else None
        rule_lower = _normalized(table, rule.lower) or rule.lower
        left_ok = rule_upper is None or lower is None or lower < rule_upper
        right_ok = (
            upper is None
            or upper > rule_lower
            or (upper == rule_lower and upper_inclusive)
        )
        if left_ok and right_ok:
            matched.append(rule.shard)
            explanations.append(
                {
                    "rule": f"range[{index}]",
                    "shard": rule.shard,
                    "lower": rule_lower,
                    "upper": rule_upper,
                    "matched": True,
                }
            )
    return sorted(set(matched)), explanations

def _explain_range(
    router: Router,
    table: TableTopology,
    name: str,
    description: str,
    filters: list[QueryFilter],
    key_filters: list[QueryFilter],
) -> QueryExplanation:
    trace: dict[str, Any] = {"router": "range", "predicates": []}
    eq_values: list[Any] = []
    lower: str | None = None
    upper: str | None = None
    upper_inclusive = False
    interval_used = False
    for item in key_filters:
        if item.op not in RANGE_OPS:
            continue
        if item.op in {"eq", "in", "is_null"}:
            values = item.value if item.op == "in" else [item.value]
            for value in values:
                eq_values.append(_normalized(table, value))
            trace["predicates"].append({"op": item.op, "value": item.value, "mode": "point"})
            continue
        value = _normalized(table, item.value)
        if value is None:
            continue
        interval_used = True
        trace["predicates"].append({"op": item.op, "value": value, "mode": "bound"})
        if item.op in {"gte", "gt"}:
            if lower is None or value > lower or (value == lower and item.op == "gt"):
                lower = value
        if item.op in {"lte", "lt"}:
            if upper is None or value < upper or (value == upper and item.op == "lt"):
                upper = value
                upper_inclusive = item.op == "lte"
    if lower is not None and upper is not None and lower > upper:
        trace["contradiction"] = True
        return _result(table, name, description, filters, set(), "pruned", "range predicates are contradictory", trace)
    if lower is not None and upper is not None and lower == upper and not upper_inclusive:
        trace["contradiction"] = True
        return _result(table, name, description, filters, set(), "pruned", "range predicates match no value", trace)
    if eq_values:
        targets: set[str] = set()
        route_traces = []
        for value in eq_values:
            if value is None:
                if table.null_policy == "route_default" and table.default_shard:
                    targets.add(table.default_shard)
                elif table.null_policy == "error":
                    return _result(
                        table, name, description, filters, set(), "", "null key is rejected",
                        trace, force_not_prunable=True,
                    )
                continue
            try:
                route = router.route(table.table, value)
            except ShardRouteError as exc:
                trace.setdefault("route_errors", []).append(exc.as_dict())
                continue
            if route.target_shard:
                targets.add(route.target_shard)
            route_traces.append(route.as_dict())
        trace["route_points"] = route_traces
        return _result(
            table, name, description, filters, targets,
            "pruned", "equality/in predicates route to known shards", trace,
        )
    if not interval_used:
        reason = (
            "range-key predicate does not define a usable interval"
            if key_filters
            else "no shard-key predicate; scan all shards"
        )
        return _result(
            table, name, description, filters, set(), "full_scan" if not key_filters else "",
            reason, trace, force_not_prunable=bool(key_filters),
        )
    targets, rule_explanations = _range_targets(table, lower, upper, upper_inclusive)
    if not targets and table.default_shard:
        targets = [table.default_shard]
        trace["default_fallback"] = True
    trace.update(
        {
            "interval": {
                "lower": lower,
                "upper": upper,
                "upper_inclusive": upper_inclusive,
                "lower_unbounded": lower is None,
            },
            "rules": rule_explanations,
        }
    )
    decision = "pruned" if targets else "pruned"
    reason = "range interval overlaps a subset of shard intervals"
    return _result(table, name, description, filters, set(targets), decision, reason, trace)

def explain_query(topology: Topology, query: Any) -> QueryExplanation:
    table_name, name, description, filters = _parse(topology, query)
    table = topology.table(table_name)
    key_filters = _key_filters(table, filters)
    router = Router(topology)
    if table.router == "hash":
        return _explain_hash(router, table, name, description, filters, key_filters)
    if table.router == "list":
        return _explain_list(router, table, name, description, filters, key_filters)
    return _explain_range(router, table, name, description, filters, key_filters)

def explain_file(topology: Topology, path: str | Path) -> QueryExplanation:
    try:
        query = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise QueryError(QUERY_INVALID, f"cannot read query JSON: {exc}", str(path)) from exc
    return explain_query(topology, query)

def parse_workload_query(topology: Topology, query: Any) -> tuple[str, str, str, list[QueryFilter]]:
    """Validate one workload query against a topology and return its parsed parts."""
    return _parse(topology, query)

def explain_workload(topology: Topology, workload: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Explain every workload entry through the real query interpreter.

    Billing rule (single source of truth): one query produces exactly one
    request against *each* of its target shards, so an ``in`` query is billed
    once per *distinct* target shard, never once per listed key. Queries with
    no usable shard-key predicate (or a non-prunable predicate) target every
    table shard, and that full-shard fan-out is billed as well.

    Each entry carries ``frequency`` (validated non-negative integer by the
    caller), the real ``QueryExplanation`` decision/reason/trace, the target
    shard set and this entry's per-shard request contributions. A
    ``route_error`` flag marks explanations that are not safe to plan around.
    """
    results: list[dict[str, Any]] = []
    for index, entry in enumerate(workload):
        query = entry["query"]
        frequency = int(entry["frequency"])
        explanation = explain_query(topology, query)
        targets = list(explanation.targeted_shards)
        route_error = bool(explanation.trace.get("route_errors"))
        results.append(
            {
                "index": index,
                "name": explanation.name,
                "table": explanation.table,
                "frequency": frequency,
                "decision": explanation.decision,
                "reason": explanation.reason,
                "targets": targets,
                "target_count": len(targets),
                "requests": frequency * len(targets),
                "shard_contributions": {shard: frequency for shard in targets},
                "route_error": route_error,
                "explanation": explanation,
                "filters": [item.as_dict() for item in explanation.filters],
            }
        )
    return results

def forced_targets(
    table: TableTopology,
    key_filters: list[QueryFilter],
    partial_map: Mapping[str, str],
    open_keys: frozenset[str] | set[str] = frozenset(),
) -> tuple[set[str], bool, bool]:
    """Lower-bound target set for a list query under a partial key assignment.

    ``partial_map`` holds the list_map entries already fixed for the current
    search branch (movable keys assigned so far plus every fixed/undeclared
    key inherited from the old topology). ``open_keys`` are movable keys whose
    branch choice is still open: their old shard must not be assumed, because
    assuming it could overstate the bound. Values that cannot contribute a
    forced target are skipped, so the returned set can only understate the
    final target set — every bound derived from it is a valid lower bound.

    Returns ``(forced_targets, full_scan, complete)`` where ``complete`` is
    ``True`` only when every referenced key is already assigned, i.e. the
    forced set equals the final target set. A partial set is a lower bound
    but cannot support per-shard feasibility conclusions.
    """
    all_shards = set(table.table_shards())
    candidates = [item for item in key_filters if item.op in {"eq", "in", "is_null"}]
    if not candidates:
        return set(all_shards), True, True
    forced: set[str] = set()
    complete = True
    for item in candidates:
        values = item.value if item.op == "in" else [item.value]
        for raw in values:
            normalized = _normalized(table, raw)
            if normalized is None:
                if table.null_policy == "drop":
                    continue
                if table.null_policy == "route_default" and table.default_shard:
                    forced.add(table.default_shard)
                    continue
                return set(all_shards), True, True
            if normalized in open_keys:
                # An open movable key has no fixed target on this branch; its
                # old list_map shard must never be assumed here.
                complete = False
                continue
            if normalized in partial_map:
                forced.add(partial_map[normalized])
            elif normalized in table.list_map:
                forced.add(table.list_map[normalized])
            elif table.default_shard:
                # Undeclared/unmapped values keep the default shard for good.
                forced.add(table.default_shard)
            else:
                # Unroutable in the current partial view: bound with all shards.
                return set(all_shards), True, True
    return forced, False, complete
