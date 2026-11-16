"""Explain whether query predicates can safely prune shards."""

from __future__ import annotations

import itertools
import json
from pathlib import Path
from typing import Any

from .errors import QUERY_INVALID, QueryError, ShardRouteError
from .models import QueryExplanation, QueryFilter, TableTopology, Topology
from .query_support import (
    RANGE_OPS,
    Atom,
    DnfResult,
    Literal,
    Not,
    ParsedQuery,
    _key_filters,
    _normalized,
    _parse,
    _result,
    expand_dnf,
    parse_boolean_query,
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

def explain_query(topology: Topology, query: Any, branch_budget: int | None = None) -> QueryExplanation:
    """Explain a query against ``topology``.

    The legacy flat ``filters`` list keeps its original single-table router
    logic and result shape. Queries carrying an explicit boolean ``where``
    expression are handled by :func:`explain_boolean_query`.
    """
    if isinstance(query, dict) and "where" in query:
        if branch_budget is not None and "branch_budget" not in query:
            query = {**query, "branch_budget": branch_budget}
        return explain_boolean_query(topology, query)
    table_name, name, description, filters = _parse(topology, query)
    table = topology.table(table_name)
    key_filters = _key_filters(table, filters)
    router = Router(topology)
    if table.router == "hash":
        return _explain_hash(router, table, name, description, filters, key_filters)
    if table.router == "list":
        return _explain_list(router, table, name, description, filters, key_filters)
    return _explain_range(router, table, name, description, filters, key_filters)


# ---------------------------------------------------------------------------
# Explicit boolean (AND / OR / NOT) offline interpreter
# ---------------------------------------------------------------------------

# A sentinel used when a branch can be satisfied by values outside every
# explicitly enumerated domain (unknown non-shard-key behaviour, open
# intervals, negated range predicates, ...). The sentinel is routed through
# the real Router so default-shard and gap behaviour stays honest.
class _Unknown:
    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return "<unknown-key-value>"

_UNKNOWN_KEY = _Unknown()


def _key_literals(table: TableTopology, branch: tuple[Literal, ...]) -> list[Literal]:
    return [literal for literal in branch if literal.field in table.key_fields]


def _route_candidates(
    router: Router,
    table: TableTopology,
    candidates: list[Any],
    trace_rows: list[dict[str, Any]],
) -> tuple[set[str], bool]:
    """Route every candidate point; return (shards, widened?)."""
    targets: set[str] = set()
    widened = False
    for value in candidates:
        if value is _UNKNOWN_KEY:
            trace_rows.append(
                {
                    "value": "<unknown>",
                    "widened": True,
                    "reason": "branch can be satisfied by unenumerated key values",
                }
            )
            widened = True
            continue
        outcome = router.explain_route(table.table, value)
        row = {"value": value, **outcome.as_dict()}
        if outcome.error is not None:
            row["widened"] = "routing error; branch widened conservatively"
            trace_rows.append(row)
            widened = True
            continue
        trace_rows.append(row)
        if outcome.shard is not None:
            targets.add(outcome.shard)
    return targets, widened


# ---- hash ----------------------------------------------------------------

def _solve_hash_branch(
    router: Router,
    table: TableTopology,
    branch: tuple[Literal, ...],
    branch_id: str,
) -> dict[str, Any]:
    all_shards = table.table_shards()
    key = table.key_fields
    evidence: list[dict[str, Any]] = []
    non_key = [literal for literal in branch if literal.field not in key]
    key_lits = _key_literals(table, branch)
    base = {"branch": branch_id, "router": "hash", "literals": [item.as_dict() for item in branch]}
    if non_key:
        evidence.append({"non_shard_key_literals": [item.as_dict() for item in non_key],
                         "effect": "cannot constrain routing; retained in the predicate only"})
    if not key_lits:
        evidence.append({"widened": True, "reason": "branch has no shard-key literal"})
        return {**base, "targets": set(all_shards), "precise": True, "evidence": evidence}

    per_component: dict[str, list[Literal]] = {field: [] for field in key}
    for literal in key_lits:
        per_component[literal.field].append(literal)

    domains: dict[str, list[Any]] = {}
    for field in key:
        domain, contradiction, component_evidence = _hash_component_domain(table, per_component[field])
        evidence.extend(component_evidence)
        if contradiction:
            evidence.append({"component": field, "contradiction": True})
            return {**base, "targets": set(), "precise": True, "evidence": evidence}
        if domain is None:
            evidence.append({"component": field,
                             "reason": "no equality/in pin; hash key cannot be located"})
            return {**base, "targets": set(all_shards), "precise": True, "evidence": evidence}
        domains[field] = domain

    targets: set[str] = set()
    null_components: list[str] = []
    for combo in itertools.product(*(domains[field] for field in key)):
        if any(value is None for value in combo):
            null_components = [field for field, value in zip(key, combo) if value is None]
            null_outcome = _solve_null_key(router, table, combo, evidence)
            if null_outcome is not None:
                targets.add(null_outcome)
            continue
        value = combo[0] if len(combo) == 1 else list(combo)
        outcome = router.explain_route(table.table, value)
        row = {"component_values": list(combo), **outcome.as_dict()}
        if outcome.error is not None:
            row["widened"] = "routing error; branch widened conservatively"
            evidence.append(row)
            return {**base, "targets": set(all_shards), "precise": True, "evidence": evidence}
        evidence.append(row)
        if outcome.shard is not None:
            targets.add(outcome.shard)
    if null_components:
        evidence.append({"null_components": null_components, "policy": table.null_policy})
    return {**base, "targets": targets, "precise": True, "evidence": evidence}


def _hash_component_domain(
    table: TableTopology, literals: list[Literal]
) -> tuple[list[Any] | None, bool, list[dict[str, Any]]]:
    """Finite feasible domain for one hash-key component.

    Returns ``(domain, contradiction, evidence)``. A ``None`` domain means the
    component is not pinned by any equality/in literal, so the hash bucket
    cannot be determined and the branch must widen.
    """
    evidence: list[dict[str, Any]] = []
    pins: set[Any] = set()
    pinned = False
    wants_null = False
    for literal in literals:
        op = literal.op
        if op == "is_null":
            wants_null = True
            continue
        if op == "not_null":
            continue
        if op not in {"eq", "in", "ne", "not_in"}:
            # gte/gt/lte/lt on a hash key gives no bucket information.
            evidence.append({"field": literal.field, "op": op,
                             "reason": "range predicate cannot locate a hash bucket"})
            return None, False, evidence
        values = literal.value if op in {"in", "not_in"} else [literal.value]
        normalized = [_normalized(table, item) for item in values]
        if op in {"eq", "in"}:
            non_null = [item for item in normalized if item is not None]
            if non_null:
                pinned = True
                for item in non_null:
                    pins.add(item)
        # ne / not_in fall through to the 3VL filtering loop below; on their
        # own they cannot pin a bucket, which is handled after this loop.
    if not pinned:
        if wants_null:
            return [None], False, evidence
        if any(literal.op in {"ne", "not_in"} for literal in literals):
            evidence.append({"reason": "inequality cannot pin a hash bucket by itself"})
            return None, False, evidence
        return None, False, evidence
    domain: list[Any] = sorted(pins, key=str)
    if wants_null:
        evidence.append({"contradiction": True, "reason": "is_null conflicts with equality/in pins"})
        return None, True, evidence
    # Apply 3VL-safe filtering of the finite pin set against ne/not_in.
    for literal in literals:
        if literal.op not in {"ne", "not_in"}:
            continue
        raw_values = literal.value if literal.op == "not_in" else [literal.value]
        excluded: set[Any] = set()
        null_member = False
        for item in raw_values:
            normalized = _normalized(table, item)
            if normalized is None:
                null_member = True
            else:
                excluded.add(normalized)
        if null_member:
            # A NULL in a NOT IN / NE list makes the predicate unknown for
            # every value outside the known (definitely-false) members, so no
            # pin can be proven to satisfy the branch.
            evidence.append({"field": literal.field, "op": literal.op,
                             "reason": "NULL negation member makes every surviving pin unprovable"})
            return [], True, evidence
        filtered = [item for item in domain if item is not None and item not in excluded]
        if len(filtered) != len(domain):
            evidence.append({"field": literal.field, "op": literal.op,
                             "removed_values": sorted(set(domain) - set(filtered), key=str)})
        domain = filtered
        if not domain:
            return [], True, evidence
    return domain, False, evidence


def _solve_null_key(router: Router, table: TableTopology, combo: list[Any],
                    evidence: list[dict[str, Any]]) -> str | None:
    outcome = router.explain_route(table.table, list(combo))
    evidence.append({"component_values": list(combo), **outcome.as_dict(), "null_key": True})
    if outcome.error is not None:
        # null_policy=error: the null row is rejected, so it contributes no
        # shard; the error itself is still evidence rather than a contradiction.
        evidence[-1]["note"] = "null rows are rejected by null_policy=error; no shard added"
        return None
    if outcome.dropped or outcome.shard is None:
        return None
    return outcome.shard


# ---- list ----------------------------------------------------------------

def _solve_list_branch(
    router: Router,
    table: TableTopology,
    branch: tuple[Literal, ...],
    branch_id: str,
) -> dict[str, Any]:
    all_shards = table.table_shards()
    key_field = table.key_fields[0]
    key_lits = [literal for literal in branch if literal.field == key_field]
    non_key = [literal for literal in branch if literal.field != key_field]
    base = {"branch": branch_id, "router": "list", "literals": [item.as_dict() for item in branch]}
    evidence: list[dict[str, Any]] = []
    if non_key:
        evidence.append({"non_shard_key_literals": [item.as_dict() for item in non_key],
                         "effect": "cannot constrain routing; retained in the predicate only"})
    if not key_lits:
        evidence.append({"widened": True, "reason": "branch has no shard-key literal"})
        return {**base, "targets": set(all_shards), "precise": True, "evidence": evidence}

    contradiction, wants_null, wants_not_null, points, open_needed, point_evidence = (
        _classify_list_literals(table, key_lits)
    )
    evidence.extend(point_evidence)
    if contradiction:
        evidence.append({"contradiction": True, "reason": "is_null conflicts with non-null key predicates"})
        return {**base, "targets": set(), "precise": True, "evidence": evidence}

    # Finite feasibility filtering of the positive point set against ne/not_in.
    # A NULL member of a not_in/ne list makes the predicate unprovable (3VL
    # unknown) for every value outside the known members; known members are
    # still definitively false, so surviving positive pins decide feasibility.
    excluded: set[str] = set()
    null_in_negation = False
    for literal in key_lits:
        if literal.op in {"ne", "not_in"}:
            values = literal.value if literal.op == "not_in" else [literal.value]
            for value in values:
                normalized = _normalized(table, value)
                if normalized is None:
                    null_in_negation = True
                else:
                    excluded.add(normalized)
    if excluded:
        kept = [item for item in points if item not in excluded]
        removed = sorted(set(points) - set(kept))
        if removed:
            evidence.append({"op": "ne/not_in", "removed_values": removed})
        points = kept
    if null_in_negation and points:
        # Every surviving pin is unprovable because of the NULL negation member.
        evidence.append({"contradiction": True,
                         "reason": "NULL negation member makes every surviving point unprovable"})
        return {**base, "targets": set(), "precise": True, "evidence": evidence}
    if not points and not open_needed and not wants_null:
        if excluded or null_in_negation:
            evidence.append({"contradiction": True,
                             "reason": "every positive point is excluded by ne/not_in"})
            return {**base, "targets": set(), "precise": True, "evidence": evidence}

    candidates: list[Any] = list(points)
    if wants_null and None not in candidates:
        candidates.append(None)
    if open_needed or wants_not_null:
        # An open domain (not_null / range comparisons / negation without
        # positive pins) can contain every mapped value plus values outside
        # the map, which the Router sends to the default shard. A NULL
        # negation member without surviving pins contributes no true value.
        for mapped in sorted(table.list_map):
            if mapped not in excluded and mapped not in candidates:
                candidates.append(mapped)
        candidates.append(_UNKNOWN_KEY)
        evidence.append({"widened": True,
                         "reason": "open-ended/negated/not-null predicate over an unbounded list domain"})

    targets, widened = _route_candidates(router, table, candidates, evidence)
    if widened:
        if table.default_shard:
            targets.add(table.default_shard)
            evidence.append({"default_shard": table.default_shard,
                             "reason": "unmapped/unknown values route through the list default"})
        else:
            evidence.append({"no_default_shard": True,
                             "reason": "unmapped values are rejected; only mapped shards retained"})
    return {**base, "targets": targets, "precise": True, "evidence": evidence}


def _classify_list_literals(
    table: TableTopology, literals: list[Literal]
) -> tuple[bool, bool, bool, list[Any], bool, list[dict[str, Any]]]:
    """Classify list-key literals.

    Returns ``(contradiction, wants_null, wants_not_null, points, open_needed,
    evidence)``. Positive eq/in pins bound the domain; not_null, range
    comparisons and ne/not_in only force an open domain when no positive pins
    exist.
    """
    wants_null = False
    wants_not_null = False
    has_positive_pin = False
    evidence: list[dict[str, Any]] = []
    points: list[Any] = []
    seen: set[Any] = set()
    open_needed = False
    for literal in literals:
        if literal.op == "is_null":
            wants_null = True
        elif literal.op == "not_null":
            wants_not_null = True
        elif literal.op in {"eq", "in"}:
            values = literal.value if literal.op == "in" else [literal.value]
            non_null = [_normalized(table, value) for value in values]
            non_null = [value for value in non_null if value is not None]
            if non_null:
                has_positive_pin = True
                for value in non_null:
                    if value not in seen:
                        seen.add(value)
                        points.append(value)
        else:
            open_needed = True
            evidence.append({"field": literal.field, "op": literal.op,
                             "reason": "negated/open predicate does not bound the list domain by itself"})
    contradiction = wants_null and (wants_not_null or has_positive_pin)
    if wants_null and wants_not_null:
        evidence.append({"contradiction": True, "reason": "is_null conflicts with not_null"})
    return (
        contradiction,
        wants_null,
        wants_not_null,
        points,
        open_needed and not has_positive_pin,
        evidence,
    )


# ---- range ---------------------------------------------------------------

def _solve_range_branch(
    router: Router,
    table: TableTopology,
    branch: tuple[Literal, ...],
    branch_id: str,
) -> dict[str, Any]:
    all_shards = table.table_shards()
    key_field = table.key_fields[0]
    key_lits = [literal for literal in branch if literal.field == key_field]
    non_key = [literal for literal in branch if literal.field != key_field]
    base = {"branch": branch_id, "router": "range", "literals": [item.as_dict() for item in branch]}
    evidence: list[dict[str, Any]] = []
    if non_key:
        evidence.append({"non_shard_key_literals": [item.as_dict() for item in non_key],
                         "effect": "cannot constrain routing; retained in the predicate only"})
    if not key_lits:
        evidence.append({"widened": True, "reason": "branch has no shard-key literal"})
        return {**base, "targets": set(all_shards), "precise": True, "evidence": evidence}

    null_demand = False
    not_null_demanded = False
    null_negation = False
    points: list[Any] = []
    positive_point_count = 0
    excluded: set[Any] = set()
    bounds: dict[str, Any] = {}
    for literal in key_lits:
        op = literal.op
        if op == "is_null":
            null_demand = True
            continue
        if op == "not_null":
            not_null_demanded = True
            evidence.append({"op": op, "reason": "not_null spans every non-null value"})
            continue
        if op in {"ne", "not_in"}:
            values = literal.value if op == "not_in" else [literal.value]
            for value in values:
                normalized = _normalized(table, value)
                if normalized is None:
                    # A NULL negation member makes every non-member unknown,
                    # so the feasible ordered domain cannot be bounded.
                    null_negation = True
                    evidence.append({"op": op,
                                     "reason": "NULL negation member makes non-members unprovable"})
                else:
                    excluded.add(normalized)
            evidence.append({"op": op, "reason": "negation filters feasible values; it bounds nothing by itself"})
            continue
        if op in {"eq", "in"}:
            values = literal.value if op == "in" else [literal.value]
            for value in values:
                normalized = _normalized(table, value)
                if normalized is None:
                    # eq/ne/in against NULL is unknown for every row (3VL);
                    # such literals are handled by the never-true pre-check and
                    # never contribute a routable point.
                    evidence.append({"op": op, "value": value,
                                     "reason": "null comparison is unknown; no routable point"})
                else:
                    points.append(normalized)
                    positive_point_count += 1
            continue
        normalized = _normalized(table, literal.value)
        if normalized is None:
            evidence.append({"op": op,
                             "reason": "bound against NULL is unknown; ignored for routing"})
            continue
        if op in {"gte", "gt"}:
            current = bounds.get("lower")
            if current is None or normalized > current[0] or (
                normalized == current[0] and op == "gt"
            ):
                bounds["lower"] = (normalized, op == "gt")
        else:  # lte / lt
            current = bounds.get("upper")
            if current is None or normalized < current[0] or (
                normalized == current[0] and op == "lt"
            ):
                bounds["upper"] = (normalized, op == "lt")

    if excluded:
        kept = [item for item in points if item not in excluded]
        if len(kept) != len(points):
            evidence.append({"ne/not_in_removed": sorted(set(points) - set(kept))})
        points = kept
    points = sorted(set(points))

    lower = bounds["lower"] if "lower" in bounds else None
    upper = bounds["upper"] if "upper" in bounds else None
    if lower is not None and upper is not None:
        if lower[0] > upper[0] or (lower[0] == upper[0] and (lower[1] or upper[1])):
            evidence.append({"contradiction": True,
                             "reason": "range bounds cannot contain any value",
                             "lower": lower, "upper": upper})
            return {**base, "targets": set(), "precise": True, "evidence": evidence}
    if null_demand and (points or lower is not None or upper is not None):
        evidence.append({"contradiction": True,
                         "reason": "is_null conflicts with non-null key predicates"})
        return {**base, "targets": set(), "precise": True, "evidence": evidence}
    interval_present = lower is not None or upper is not None
    if (
        positive_point_count > 0
        and not points
        and not interval_present
        and not null_demand
    ):
        # Positive eq/in pins existed but ne/not_in removed every one: the
        # conjunction cannot be true for any key value.
        evidence.append({"contradiction": True,
                         "reason": "all feasible point values are excluded by ne/not_in"})
        return {**base, "targets": set(), "precise": True, "evidence": evidence}
    # The feasible domain is bounded when there are surviving eq/in points or
    # an interval. Negation/not_null only opens the domain if nothing positive
    # bounds it.
    bounded = bool(points) or interval_present
    open_ended = (
        null_negation
        or (not bounded and (bool(excluded) or not_null_demanded))
    )

    targets: set[str] = set()
    # Concrete points (eq/in surviving values) are routed through the real
    # Router, so values landing in a gap automatically pick up range_default.
    point_rows: list[dict[str, Any]] = []
    point_targets, _ = _route_candidates(router, table, points, point_rows)
    evidence.append({"point_routes": point_rows})
    targets.update(point_targets)

    if null_demand:
        outcome = router.explain_route(table.table, None)
        evidence.append({"null_key": True, **outcome.as_dict(), "null_policy": table.null_policy})
        if outcome.error is not None:
            evidence[-1]["note"] = "null rows are rejected by null_policy=error; no shard added"
        elif outcome.shard is not None:
            targets.add(outcome.shard)

    interval_used = lower is not None or upper is not None
    if interval_used:
        interval_targets, interval_evidence = _range_interval_targets(table, lower, upper)
        evidence.append(interval_evidence)
        targets.update(interval_targets)
    elif open_ended:
        # ne/not_in/not_null with no bounding interval: the feasible domain is
        # the whole ordered domain, so every reachable shard must be retained.
        targets.update(all_shards)
        evidence.append({"widened": True,
                         "reason": "open-ended branch can reach every range shard"})
    elif not points and not null_demand:
        targets.update(all_shards)
        evidence.append({"widened": True, "reason": "no usable range-key constraint"})
    return {**base, "targets": targets, "precise": True, "evidence": evidence}


def _range_interval_targets(
    table: TableTopology,
    lower: tuple[str, bool] | None,
    upper: tuple[str, bool] | None,
) -> tuple[set[str], dict[str, Any]]:
    """Overlap a feasible half-open interval with configured range rules.

    Bounds are ``(value, exclusive)``. A rule interval overlapping the feasible
    domain contributes its shard. When the feasible domain is not fully covered
    by the configured rules (a gap, or an open side extending past the rules),
    the default shard is included because the real Router sends those values
    to ``range_default``.
    """
    feasible_lower = lower[0] if lower is not None else None
    feasible_upper = upper[0] if upper is not None else None
    lower_exclusive = lower[1] if lower is not None else False
    upper_exclusive = upper[1] if upper is not None else True
    targets: set[str] = set()
    rule_rows = []
    for index, rule in enumerate(table.ranges):
        rule_upper = _normalized(table, rule.upper) if rule.upper is not None else None
        rule_lower = _normalized(table, rule.lower) or rule.lower
        left_ok = rule_upper is None or feasible_lower is None or feasible_lower < rule_upper
        right_ok = (
            feasible_upper is None
            or feasible_upper > rule_lower
            or (feasible_upper == rule_lower and not upper_exclusive)
        )
        overlaps = left_ok and right_ok
        rule_rows.append({"rule": f"range[{index}]", "shard": rule.shard,
                          "lower": rule_lower, "upper": rule_upper, "overlaps": overlaps})
        if overlaps:
            targets.add(rule.shard)
    covered = _interval_fully_covered(table, feasible_lower, feasible_upper,
                                      lower_exclusive, upper_exclusive)
    evidence = {
        "interval": {
            "lower": feasible_lower,
            "lower_exclusive": lower_exclusive,
            "upper": feasible_upper,
            "upper_exclusive": upper_exclusive,
        },
        "rules": rule_rows,
        "fully_covered_by_rules": covered,
    }
    if not covered:
        if table.default_shard:
            targets.add(table.default_shard)
            evidence["default_shard"] = table.default_shard
            evidence["default_reason"] = (
                "feasible values exist outside the configured ranges; Router uses range_default"
            )
        else:
            evidence["default_reason"] = "uncovered values are rejected; no default shard exists"
    return targets, evidence


def _interval_fully_covered(
    table: TableTopology,
    lower: str | None,
    upper: str | None,
    lower_exclusive: bool,
    upper_exclusive: bool,
) -> bool:
    """True when the whole feasible interval lies inside some configured rule."""
    rules = sorted(table.ranges, key=lambda rule: _normalized(table, rule.lower) or rule.lower)
    if lower is None:
        # Feasible domain extends to negative infinity; configured range rules
        # always have a finite lower bound, so values below the first rule fall
        # through to the default shard.
        return False
    cursor = lower
    for rule in rules:
        rule_lower = _normalized(table, rule.lower) or rule.lower
        rule_upper = _normalized(table, rule.upper) if rule.upper is not None else None
        if rule_lower > cursor:
            # Gap between the feasible cursor and this rule: values in the gap
            # are routed to range_default by the real Router.
            return False
        if rule_upper is None:
            # Open-ended rule covers everything from its lower bound onwards.
            return True
        if upper is not None:
            # Rule intervals are half-open [lower, upper). A feasible exclusive
            # upper bound may sit exactly on the rule upper bound; an inclusive
            # bound equal to the rule upper bound cannot (that value is excluded
            # from the rule).
            if upper < rule_upper or (upper == rule_upper and upper_exclusive):
                return True
        cursor = rule_upper
    return False


# ---- boolean driver ------------------------------------------------------

def _literal_never_true(table: TableTopology, literal: Literal) -> bool:
    """Whether a literal is provably never true under three-valued logic.

    These are real contradictions even when the field is not the shard key:
    SQL comparisons against NULL evaluate to *unknown* for every row (a NULL
    column compared to NULL is still unknown), ``x IN (empty)`` is false, and
    any ``NOT IN``/``NE`` list containing a NULL member is unprovable for
    every value outside the known (definitely false) members.
    """
    op = literal.op
    if op == "in":
        return all(_normalized(table, item) is None for item in literal.value)
    if op == "not_in":
        if not literal.value:
            # NOT IN () is true for every non-null row.
            return False
        # A NULL member makes the test unprovable for every non-excluded value.
        return any(_normalized(table, item) is None for item in literal.value)
    if op == "ne":
        return _normalized(table, literal.value) is None
    if op in {"eq", "gte", "gt", "lte", "lt"}:
        return _normalized(table, literal.value) is None
    return False


def _expression_touches_key(table: TableTopology, expression: Any) -> bool:
    return any(atom.field in table.key_fields for atom in _all_atoms(expression))


def _all_atoms(node: Any):
    if isinstance(node, Atom):
        yield node
        return
    if isinstance(node, Not):
        yield from _all_atoms(node.child)
        return
    for child in node.children:
        yield from _all_atoms(child)


def explain_boolean_query(topology: Topology, query: Any) -> QueryExplanation:
    parsed: ParsedQuery = parse_boolean_query(topology, query)
    table = topology.table(parsed.table)
    router = Router(topology)
    all_shards = table.table_shards()
    filters = list(parsed.filters)
    solver = {
        "hash": _solve_hash_branch,
        "list": _solve_list_branch,
        "range": _solve_range_branch,
    }[table.router]

    trace: dict[str, Any] = {
        "mode": "boolean",
        "router": table.router,
        "null_semantics": "three_valued_logic",
        "null_rule": "only predicates evaluating to true return rows; unknown is never treated as true",
        "key_fields": list(table.key_fields),
        "expression": parsed.expression.as_dict(),
        "branch_budget": parsed.branch_budget,
        "default_shard": table.default_shard,
        "null_policy": table.null_policy,
    }

    if not _expression_touches_key(table, parsed.expression):
        trace["branches"] = []
        trace["precision"] = {"precise": True, "mode": "no_shard_key_predicate"}
        trace["notes"] = ["no branch constrains the shard key; non-shard-key predicates cannot prove pruning"]
        return _result(
            table, parsed.name, parsed.description, filters, set(all_shards),
            "full_scan", "no shard-key predicate in any branch; scan all shards", trace,
        )

    dnf: DnfResult = expand_dnf(parsed.expression, parsed.branch_budget)
    if not dnf.precise:
        trace["dnf"] = {"precise": False, "branch_budget": parsed.branch_budget,
                        "branch_estimate": dnf.raw_branch_estimate, "reason": dnf.reason}
        trace["branches"] = []
        trace["precision"] = {"precise": False, "mode": "budget_full_scan", "reason": dnf.reason}
        trace["notes"] = [
            dnf.reason,
            "conservatively scanning every shard; no branch was dropped, so no true row can be missed",
        ]
        return _result(
            table, parsed.name, parsed.description, filters, set(all_shards),
            "not_prunable",
            "branch budget exhausted; exact pruning cannot be proven, all shards retained",
            trace,
            force_not_prunable=True,
        )

    branch_traces = []
    union: set[str] = set()
    contradictions = 0
    for index, branch in enumerate(dnf.branches):
        branch_id = f"branch-{index + 1:03d}"
        never_true = next(
            (literal for literal in branch if _literal_never_true(table, literal)),
            None,
        )
        if never_true is not None:
            # A NULL comparison or an empty IN cannot be true for any row, so
            # the whole conjunction is unsatisfiable without touching routing.
            branch_traces.append({
                "branch": branch_id,
                "router": table.router,
                "literals": [item.as_dict() for item in branch],
                "target_shards": [],
                "contradiction": True,
                "evidence": [{
                    "literal": never_true.as_dict(),
                    "reason": "three-valued logic makes this literal unknown/false for every row",
                }],
            })
            contradictions += 1
            continue
        solved = solver(router, table, branch, branch_id)
        targets = solved.pop("targets")
        branch_traces.append({**solved, "target_shards": sorted(targets)})
        if not targets:
            contradictions += 1
        union.update(targets)

    trace["dnf"] = {"precise": True, "branch_count": len(dnf.branches),
                    "branch_budget": parsed.branch_budget}
    trace["branches"] = branch_traces
    trace["precision"] = {"precise": True, "mode": "exact_dnf_union"}
    trace["notes"] = _boolean_notes(table, branch_traces, contradictions)

    if not union:
        reason = "every DNF branch is contradictory on the shard key; no shard can hold a true row"
        trace["contradiction"] = True
        return _result(table, parsed.name, parsed.description, filters, set(),
                       "pruned", reason, trace)
    if len(union) == len(all_shards):
        reason = "the union of feasible branches covers every legal shard"
        return _result(table, parsed.name, parsed.description, filters, union,
                       "full_scan", reason, trace)
    reason = f"union of {len(dnf.branches)} DNF branch(es) routes to a safe subset of shards"
    return _result(table, parsed.name, parsed.description, filters, union, "pruned", reason, trace)


def _boolean_notes(table: TableTopology, branch_traces: list[dict[str, Any]], contradictions: int) -> list[str]:
    notes: list[str] = []
    if contradictions:
        notes.append(f"{contradictions} branch(es) reduced to zero shards by shard-key contradictions; "
                     "non-shard-key assertions were never treated as contradictions")
    if table.default_shard:
        notes.append(f"default shard {table.default_shard} retained wherever a feasible value could "
                     "miss every configured rule")
    notes.append("null-valued keys follow null_policy and are evaluated with three-valued logic")
    return notes

def explain_file(
    topology: Topology, path: str | Path, branch_budget: int | None = None
) -> QueryExplanation:
    try:
        query = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise QueryError(QUERY_INVALID, f"cannot read query JSON: {exc}", str(path)) from exc
    return explain_query(topology, query, branch_budget=branch_budget)
