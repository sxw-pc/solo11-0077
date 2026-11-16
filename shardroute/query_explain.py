"""Explain whether query predicates can safely prune shards."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .errors import QUERY_INVALID, QueryError, ShardRouteError
from .models import QueryExplanation, QueryFilter, TableTopology, Topology
from .query_support import OPS, RANGE_OPS, _key_filters, _normalized, _parse, _result
from .range_domain import (
    RangeValueError,
    parse_range_value,
    rule_lower_key,
    rule_upper_key,
)
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

def _range_targets(
    table: TableTopology,
    lower: Any,
    upper: Any,
    upper_inclusive: bool,
    *,
    typed: bool = False,
    lower_inclusive: bool = True,
) -> tuple[list[str], list[dict[str, Any]], bool]:
    """Return (targets, rule explanations, default_needed).

    ``default_needed`` is true when the query window reaches values outside
    every configured interval.  Typed tables may legitimately route such
    values to ``default_shard``; ignoring it would let a query prune a shard
    that the Router would still send rows to.
    """
    matched: list[str] = []
    explanations = []
    keys: list[tuple[Any, Any]] = []
    for index, rule in enumerate(table.ranges):
        if typed:
            rule_lower = rule_lower_key(rule, table.range_domain, table.range_alias_policy)
            rule_upper = rule_upper_key(rule, table.range_domain, table.range_alias_policy)
            rule_lower_display: Any = rule.lower
            rule_upper_display: Any = rule.upper
        else:
            rule_upper = _normalized(table, rule.upper) if rule.upper is not None else None
            rule_lower = _normalized(table, rule.lower) or rule.lower
            rule_lower_display = rule_lower
            rule_upper_display = rule_upper
        keys.append((rule_lower, rule_upper))
        left_ok = rule_upper is None or lower is None or lower < rule_upper
        right_ok = (
            upper is None
            or upper > rule_lower
            or (upper == rule_lower and upper_inclusive)
        )
        if left_ok and right_ok:
            matched.append(rule.shard)
            explanation: dict[str, Any] = {
                "rule": f"range[{index}]",
                "shard": rule.shard,
                "lower": rule_lower_display,
                "upper": rule_upper_display,
                "matched": True,
            }
            if typed:
                explanation.update(
                    {
                        "domain": table.range_domain,
                        "parsed_lower": rule_lower,
                        "parsed_upper": rule_upper,
                    }
                )
            explanations.append(explanation)
    default_needed = False
    if typed and table.default_shard and keys:
        first_lower = keys[0][0]
        last_upper = keys[-1][1]
        if lower is None or lower < first_lower:
            default_needed = True
        if last_upper is not None and (
            upper is None
            or upper > last_upper
            or (upper == last_upper and upper_inclusive)
        ):
            default_needed = True
    return sorted(set(matched)), explanations, default_needed


def _explain_range(
    router: Router,
    table: TableTopology,
    name: str,
    description: str,
    filters: list[QueryFilter],
    key_filters: list[QueryFilter],
) -> QueryExplanation:
    typed = table.range_domain != "text"
    trace: dict[str, Any] = {"router": "range", "predicates": []}
    if typed:
        trace["range_domain"] = table.range_domain
        trace["range_alias_policy"] = table.range_alias_policy
    eq_values: list[Any] = []
    lower: Any = None
    upper: Any = None
    lower_inclusive = True
    upper_inclusive = False
    lower_raw: Any = None
    upper_raw: Any = None
    interval_used = False
    value_errors: list[dict[str, Any]] = []
    for item in key_filters:
        if item.op not in RANGE_OPS:
            continue
        if item.op in {"eq", "in", "is_null"}:
            values = item.value if item.op == "in" else [item.value]
            for raw_value in values:
                if typed and raw_value is not None:
                    try:
                        parsed = parse_range_value(
                            raw_value, table.range_domain, table.range_alias_policy
                        )
                    except RangeValueError as exc:
                        value_errors.append({"op": item.op, **exc.as_dict()})
                        trace["predicates"].append(
                            {"op": item.op, "value": raw_value, "mode": "point", "error": exc.as_dict()}
                        )
                        continue
                    eq_values.append(raw_value)
                    trace["predicates"].append(
                        {"op": item.op, "value": raw_value, "parsed": parsed.evidence(),
                         "canonical": parsed.canonical, "alias_used": parsed.alias_used,
                         "mode": "point"}
                    )
                    continue
                eq_values.append(_normalized(table, raw_value))
                trace["predicates"].append({"op": item.op, "value": raw_value, "mode": "point"})
            continue
        if typed:
            if item.value is None:
                continue
            try:
                parsed = parse_range_value(
                    item.value, table.range_domain, table.range_alias_policy
                )
            except RangeValueError as exc:
                value_errors.append({"op": item.op, **exc.as_dict()})
                trace["predicates"].append(
                    {"op": item.op, "value": item.value, "mode": "bound", "error": exc.as_dict()}
                )
                continue
            value = parsed.key
            trace["predicates"].append(
                {"op": item.op, "value": item.value, "parsed": parsed.evidence(),
                 "canonical": parsed.canonical, "alias_used": parsed.alias_used,
                 "mode": "bound"}
            )
        else:
            value = _normalized(table, item.value)
            if value is None:
                continue
            trace["predicates"].append({"op": item.op, "value": value, "mode": "bound"})
        interval_used = True
        if item.op in {"gte", "gt"}:
            if lower is None or value > lower or (value == lower and item.op == "gt"):
                lower = value
                lower_inclusive = item.op == "gte"
                lower_raw = item.value
        if item.op in {"lte", "lt"}:
            if upper is None or value < upper or (value == upper and item.op == "lt"):
                upper = value
                upper_inclusive = item.op == "lte"
                upper_raw = item.value
    if value_errors:
        trace["range_value_errors"] = value_errors
        trace["reason"] = "range predicate value(s) could not be parsed under the declared domain"
        return _result(
            table, name, description, filters, set(), "", trace["reason"], trace,
            force_not_prunable=True,
        )
    if typed:
        empty = (
            lower is not None
            and upper is not None
            and (
                lower > upper
                or (lower == upper and not (lower_inclusive and upper_inclusive))
            )
        )
    else:
        empty = (
            lower is not None
            and upper is not None
            and (
                lower > upper
                or (lower == upper and not upper_inclusive)
            )
        )
    if empty:
        trace["contradiction"] = True
        return _result(table, name, description, filters, set(), "pruned", "range predicates are contradictory", trace)
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
                # ROUTE_NO_MATCH means the Router can place no rows for this
                # key, so it contributes no shard.  Parse-level rejects were
                # already handled above and force not_prunable.
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
    targets, rule_explanations, default_needed = _range_targets(
        table, lower, upper, upper_inclusive, typed=typed
    )
    if table.default_shard and (default_needed or not targets):
        targets = sorted(set(targets) | {table.default_shard})
        trace["default_fallback"] = True
    interval: dict[str, Any] = {
        "lower": lower,
        "upper": upper,
        "upper_inclusive": upper_inclusive,
        "lower_unbounded": lower is None,
    }
    if typed:
        interval.update(
            {
                "domain": table.range_domain,
                "lower_raw": lower_raw,
                "upper_raw": upper_raw,
                "lower_inclusive": lower_inclusive,
            }
        )
    trace.update({"interval": interval, "rules": rule_explanations})
    reason = "range interval overlaps a subset of shard intervals"
    return _result(table, name, description, filters, set(targets), "pruned", reason, trace)

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
