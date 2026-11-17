"""Build a dry-run resharding matrix, batches, blockers and rollback checklist."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, replace
from typing import Any, Mapping

from .canonical import fingerprint, normalize_value
from .errors import SEARCH_INVALID, ShardRouteError
from .models import LogicalRow, Topology
from .query_explain import explain_workload, forced_targets, parse_workload_query
from .query_support import _key_filters
from .router import Router

MAX_BATCH_ROWS = 80
DEFAULT_EVALUATION_LIMIT = 100_000
HARD_EVALUATION_LIMIT = 1_000_000

def _audit_blockers(audit: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    if not audit:
        return []
    grouped: Counter[tuple[str, str]] = Counter()
    for finding in audit.get("findings", []):
        if finding.get("severity") in {"critical", "high"}:
            grouped[(finding.get("table", ""), finding.get("code", ""))] += 1
    return [
        {
            "code": f"PLAN_AUDIT_{code}",
            "severity": "blocking",
            "table": table,
            "count": count,
            "message": f"{count} unresolved {code} finding(s) must be repaired before a move",
        }
        for (table, code), count in sorted(
            grouped.items(), key=lambda item: (item[0][0], item[0][1])
        )
    ]

def _compatibility_blockers(old: Topology, new: Topology) -> list[dict[str, Any]]:
    blockers: list[dict[str, Any]] = []
    for table in sorted(set(old.tables) - set(new.tables)):
        blockers.append(
            {
                "code": "PLAN_TABLE_MISSING",
                "severity": "blocking",
                "table": table,
                "count": 1,
                "message": "table does not exist in the target topology",
            }
        )
    for table in sorted(set(old.tables) & set(new.tables)):
        before, after = old.table(table), new.table(table)
        if before.key_fields != after.key_fields:
            blockers.append(
                {
                    "code": "PLAN_KEY_CHANGED",
                    "severity": "blocking",
                    "table": table,
                    "count": 1,
                    "message": "shard key changed; a data rewrite contract is required",
                }
            )
        if before.router != after.router:
            blockers.append(
                {
                    "code": "PLAN_ROUTER_CHANGED",
                    "severity": "review",
                    "table": table,
                    "count": 1,
                    "message": "router family changed; validate the shadow plan carefully",
                }
            )
    return blockers

def _make_batches(cells: list[dict[str, Any]]) -> list[dict[str, Any]]:
    batches: list[dict[str, Any]] = []
    for cell in cells:
        remaining = cell["row_count"]
        while remaining:
            take = min(remaining, MAX_BATCH_ROWS)
            batches.append(
                {
                    "batch_id": f"batch-{len(batches) + 1:03d}",
                    "phase": "shadow-copy",
                    "table": cell["table"],
                    "source": cell["from_shard"],
                    "target": cell["to_shard"],
                    "row_count": take,
                }
            )
            remaining -= take
    return batches

def _rollback_checklist() -> list[dict[str, Any]]:
    return [
        {
            "order": 1,
            "action": "Freeze writes against the old topology and record the fence token.",
            "verification": "Topology fingerprint and fence token are stored in the run ledger.",
        },
        {
            "order": 2,
            "action": "Snapshot every source manifest and CSV checksum.",
            "verification": "Physical fingerprint matches the pre-plan audit artifact.",
        },
        {
            "order": 3,
            "action": "Copy (never move) batches into a shadow target namespace.",
            "verification": "Per-batch source and target row counts are equal.",
        },
        {
            "order": 4,
            "action": "Re-run the offline audit with the target topology.",
            "verification": "Zero critical or high findings and no duplicate row_id.",
        },
        {
            "order": 5,
            "action": "Switch reads, then writes, behind the new topology.",
            "verification": "Dual-read samples agree for one full verification window.",
        },
        {
            "order": 6,
            "action": "On any checksum or count drift, restore old manifests and topology.",
            "verification": "Restored physical fingerprint equals the snapshot from step 2.",
        },
        {
            "order": 7,
            "action": "Unfreeze writes and archive the rollback decision.",
            "verification": "Ledger run records the final topology and artifact hashes.",
        },
    ]

def build_plan(
    old: Topology,
    new: Topology,
    logical_rows: Mapping[str, list[LogicalRow]],
    audit: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    old_router, new_router = Router(old), Router(new)
    move_counts: Counter[tuple[str, str, str]] = Counter()
    table_totals: Counter[str] = Counter()
    table_moved: Counter[str] = Counter()
    route_errors = 0
    dropped = 0
    for table in sorted(old.tables):
        row_list = logical_rows.get(table, [])
        table_totals[table] = len(row_list)
        if table not in new.tables:
            continue
        for row in row_list:
            try:
                before = old_router.route_values(table, row.values)
                after = new_router.route_values(table, row.values)
            except ShardRouteError:
                route_errors += 1
                continue
            if before.dropped or after.dropped:
                dropped += 1
                continue
            if before.target_shard != after.target_shard:
                move_counts[(table, before.target_shard or "", after.target_shard or "")] += 1
                table_moved[table] += 1
    cells = [
        {"table": table, "from_shard": source, "to_shard": target, "row_count": count}
        for (table, source, target), count in sorted(move_counts.items())
    ]
    blockers = _audit_blockers(audit) + _compatibility_blockers(old, new)
    if route_errors:
        blockers.append(
            {
                "code": "PLAN_ROUTE_ERRORS",
                "severity": "blocking",
                "table": "",
                "count": route_errors,
                "message": "rows could not be routed in both topologies",
            }
        )
    blockers.sort(key=lambda item: (item["table"], item["code"], item["message"]))
    total_rows = sum(table_totals.values())
    moved_rows = sum(cell["row_count"] for cell in cells)
    blocked_tables = sorted(set(old.tables) - set(new.tables))
    blocked_rows = sum(table_totals[table] for table in blocked_tables)
    batches = _make_batches(cells)
    return {
        "old_fingerprint": old.fingerprint,
        "new_fingerprint": new.fingerprint,
        "total_rows": total_rows,
        "moved_rows": moved_rows,
        "unchanged_rows": total_rows - moved_rows - route_errors - dropped - blocked_rows,
        "dropped_rows": dropped,
        "route_errors": route_errors,
        "blocked_rows": blocked_rows,
        "move_cells": cells,
        "batches": batches,
        "blockers": blockers,
        "rollback_checklist": _rollback_checklist(),
        "safe_to_execute": not any(item["severity"] == "blocking" for item in blockers),
        "summary": {
            "tables": dict(sorted(table_totals.items())),
            "moved_by_table": dict(sorted(table_moved.items())),
            "move_cell_count": len(cells),
            "batch_count": len(batches),
            "max_batch_rows": MAX_BATCH_ROWS,
        },
    }

# ---------------------------------------------------------------------------
# Offline list_map layout search
# ---------------------------------------------------------------------------

OBJECTIVES = ("request_economy", "hotspot_relief")

@dataclass(frozen=True)
class _KeyDomain:
    """One declared movable list key and its finite candidate-shard domain."""
    key: str
    choices: tuple[str, ...]
    row_ids: tuple[str, ...]
    group_size: int

def _search_error(message: str, details: Mapping[str, Any] | None = None) -> ShardRouteError:
    return ShardRouteError(SEARCH_INVALID, message, "layout_search", dict(details or {}))

def _validate_search_inputs(
    old: Topology,
    table_name: str,
    domains: Mapping[str, list[str]],
    fixed_keys: list[str],
    move_budget: int,
    shard_caps: Mapping[str, int],
    workload: list[Mapping[str, Any]],
    max_evaluations: int,
) -> None:
    if not isinstance(table_name, str) or table_name not in old.tables:
        raise _search_error("search table is not present in the old topology", {"table": table_name})
    table = old.table(table_name)
    if table.router != "list":
        raise _search_error(
            "layout search only supports list-routed tables",
            {"table": table_name, "router": table.router},
        )
    if not isinstance(domains, Mapping) or not domains:
        raise _search_error("at least one movable key domain must be declared")
    known_shards = set(old.shards)
    for key, choices in domains.items():
        if not isinstance(key, str) or key == "":
            raise _search_error("movable key must be a non-empty string")
        normalized = normalize_value(key, table.case_sensitive)
        if normalized is None:
            raise _search_error("movable key must not be null", {"key": key})
        if normalized not in table.list_map:
            raise _search_error(
                "movable key is not present in the old list_map", {"key": normalized}
            )
        if not isinstance(choices, list) or not choices:
            raise _search_error("key domain must be a non-empty shard list", {"key": normalized})
        if any(not isinstance(shard, str) or shard not in known_shards for shard in choices):
            raise _search_error(
                "key domain contains an unknown shard", {"key": normalized, "choices": choices}
            )
    if not isinstance(fixed_keys, list):
        raise _search_error("fixed_keys must be a list")
    normalized_fixed = []
    for key in fixed_keys:
        if not isinstance(key, str):
            raise _search_error("fixed key must be a string")
        normalized = normalize_value(key, table.case_sensitive)
        if normalized is None or normalized not in table.list_map:
            raise _search_error("fixed key must exist in the old list_map", {"key": key})
        normalized_fixed.append(normalized)
    movable = {normalize_value(key, table.case_sensitive) for key in domains}
    overlap = movable.intersection(normalized_fixed)
    if overlap:
        raise _search_error("a key cannot be both movable and fixed", {"keys": sorted(overlap)})
    if isinstance(move_budget, bool) or not isinstance(move_budget, int) or move_budget < 0:
        raise _search_error("move_budget_rows must be a non-negative integer")
    if not isinstance(shard_caps, Mapping) or not shard_caps:
        raise _search_error("shard_read_caps must map every shard to a non-negative limit")
    missing = sorted(known_shards - set(shard_caps))
    if missing:
        raise _search_error("a read cap is missing for some shards", {"shards": missing})
    for shard, cap in shard_caps.items():
        if shard not in known_shards or isinstance(cap, bool) or not isinstance(cap, int) or cap < 0:
            raise _search_error(
                "each shard cap must be a non-negative integer for a known shard", {"shard": shard}
            )
    if not isinstance(workload, list):
        raise _search_error("workload must be a list of query entries")
    for index, entry in enumerate(workload):
        if not isinstance(entry, Mapping) or "query" not in entry:
            raise _search_error("workload entry must contain a query object", {"entry": index})
        frequency = entry.get("frequency", 1)
        if isinstance(frequency, bool) or not isinstance(frequency, int) or frequency < 0:
            raise _search_error(
                "workload frequency must be a non-negative integer", {"entry": index}
            )
        parsed_table, _, _, _ = parse_workload_query(old, entry["query"])
        if parsed_table != table_name:
            raise _search_error(
                "every workload query must target the search table",
                {"entry": index, "table": parsed_table, "expected": table_name},
            )
    if (
        isinstance(max_evaluations, bool)
        or not isinstance(max_evaluations, int)
        or max_evaluations < 1
        or max_evaluations > HARD_EVALUATION_LIMIT
    ):
        raise _search_error(
            "max_evaluations must be an integer in [1, 1_000_000]", {"value": max_evaluations}
        )

def _candidate_topology(old: Topology, table_name: str, new_list_map: Mapping[str, str]) -> Topology:
    """Build a candidate topology that only rewrites one table's list_map."""
    table = old.table(table_name)
    candidate_table = replace(table, list_map=dict(new_list_map))
    tables = dict(old.tables)
    tables[table_name] = candidate_table
    raw = {
        "version": old.version,
        "generated_at": old.generated_at,
        "description": old.description,
        "shards": list(old.shards),
        "tables": [candidate.as_dict() for name, candidate in sorted(tables.items())],
    }
    return replace(old, tables=tables, fingerprint=fingerprint(raw))

def _layout_lex_key(list_map: Mapping[str, str], movable_keys: tuple[str, ...]) -> tuple[tuple[str, str], ...]:
    """Stable layout ordering: full sorted list_map, then movable assignment."""
    return tuple(sorted(list_map.items())) + tuple((key, list_map[key]) for key in movable_keys)

def _migration_matrix(
    old_router: Router,
    candidate_router: Router,
    rows: list[LogicalRow],
    table_name: str,
) -> tuple[list[dict[str, Any]], list[str], int, int]:
    """Recompute moves row-by-row with the real old and candidate Routers."""
    counts: Counter[tuple[str, str]] = Counter()
    moved_rows: list[str] = []
    route_errors = 0
    dropped = 0
    for row in rows:
        try:
            before = old_router.route_values(table_name, row.values)
            after = candidate_router.route_values(table_name, row.values)
        except ShardRouteError:
            route_errors += 1
            continue
        if before.dropped or after.dropped:
            dropped += 1
            continue
        if before.target_shard != after.target_shard:
            counts[(before.target_shard or "", after.target_shard or "")] += 1
            moved_rows.append(row.row_id)
    cells = [
        {"table": table_name, "from_shard": source, "to_shard": target, "row_count": count}
        for (source, target), count in sorted(counts.items())
    ]
    return cells, sorted(moved_rows), route_errors, dropped

def _bound_requests(
    table: Any,
    all_topology_shards: tuple[str, ...],
    workload_parsed: list[tuple[int, list[Any]]],
    partial_map: Mapping[str, str],
    open_keys: frozenset[str],
) -> tuple[int, dict[str, int], bool]:
    """Lower bounds on total requests and per-shard load for a branch.

    The third value tells whether every query's target set is already fully
    determined. Only a complete per-shard bound can justify a hard-cap prune;
    an incomplete bound remains a valid (loose) lower bound for the objective
    functions but cannot prove the subtree infeasible.
    """
    reachable = set(table.table_shards())
    # Seed every topology shard: caps apply shard-wide even when the table
    # currently has no rule reaching one of them.
    load_lb: dict[str, int] = {shard: 0 for shard in all_topology_shards}
    total_lb = 0
    # The bound is complete only when no movable key remains open. Even an
    # unqueried open key can reroute rows, so its subtree's true per-shard
    # load is not fixed until that key is assigned.
    complete = len(open_keys) == 0
    for frequency, key_filters in workload_parsed:
        forced, full_scan, query_complete = forced_targets(
            table, key_filters, partial_map, open_keys
        )
        targets = reachable if full_scan else forced
        if not query_complete:
            complete = False
        total_lb += frequency * len(targets)
        for shard in targets:
            load_lb[shard] = load_lb.get(shard, 0) + frequency
    return total_lb, load_lb, complete

def search_layouts(
    old: Topology,
    table_name: str,
    logical_rows: Mapping[str, list[LogicalRow]],
    domains: Mapping[str, list[str]],
    *,
    fixed_keys: list[str] | None = None,
    move_budget_rows: int = 0,
    shard_read_caps: Mapping[str, int] | None = None,
    workload: list[Mapping[str, Any]] | None = None,
    audit: Mapping[str, Any] | None = None,
    max_evaluations: int = DEFAULT_EVALUATION_LIMIT,
) -> dict[str, Any]:
    """Search feasible list_map layouts for a real query workload.

    The search is offline and read-only: the old topology and every physical
    file are left untouched, no migration is executed and no query is sent to
    a datastore. Candidate target sets come from the real query interpreter
    in :mod:`shardroute.query_explain`; migration matrices come from the real
    old/new :class:`Router` pair.
    """
    fixed_keys = list(fixed_keys or [])
    shard_read_caps = dict(shard_read_caps or {})
    workload = list(workload or [])
    _validate_search_inputs(
        old, table_name, domains, fixed_keys, move_budget_rows,
        shard_read_caps, workload, max_evaluations,
    )
    table = old.table(table_name)
    rows = list(logical_rows.get(table_name, []))
    old_router = Router(old)

    # --- Normalize the declared domain -------------------------------------
    movable_keys = tuple(sorted(normalize_value(key, table.case_sensitive) for key in domains))
    choices_by_key = {
        normalize_value(key, table.case_sensitive): tuple(sorted(domains[key]))
        for key in domains
    }
    fixed_normalized = tuple(sorted(
        normalize_value(key, table.case_sensitive) for key in fixed_keys
    ))
    movable_set = frozenset(movable_keys)

    # --- Row groups: whole key groups move atomically ----------------------
    group_rows: dict[str, list[str]] = defaultdict(list)
    pre_route_errors = 0
    pre_dropped = 0
    for row in rows:
        try:
            trace = old_router.route_values(table_name, row.values)
        except ShardRouteError:
            pre_route_errors += 1
            continue
        if trace.dropped:
            pre_dropped += 1
            continue
        if trace.normalized_value in movable_set:
            group_rows[trace.normalized_value].append(row.row_id)
    key_domains: list[_KeyDomain] = []
    for key in movable_keys:
        row_ids = tuple(sorted(group_rows.get(key, [])))
        key_domains.append(_KeyDomain(key, choices_by_key[key], row_ids, len(row_ids)))

    # --- Parse the workload once; keep real filters for the explainer ------
    workload_parsed: list[tuple[int, list[Any], Mapping[str, Any]]] = []
    for entry in workload:
        parsed_table, _, _, filters = parse_workload_query(old, entry["query"])
        assert parsed_table == table_name
        workload_parsed.append(
            (int(entry["frequency"]), _key_filters(table, filters), entry["query"])
        )

    audit_blockers = _audit_blockers(audit)
    global_blockers: list[dict[str, Any]] = list(audit_blockers)
    if pre_route_errors:
        global_blockers.append(
            {
                "code": "SEARCH_ROUTE_ERRORS",
                "severity": "blocking",
                "table": table_name,
                "count": pre_route_errors,
                "message": "logical rows could not be routed with the old topology",
            }
        )
    global_blockers.sort(key=lambda item: (item["table"], item["code"], item["message"]))

    base_list_map = dict(table.list_map)
    old_shard_by_key = {key: table.list_map[key] for key in movable_keys}
    caps = {shard: int(shard_read_caps[shard]) for shard in old.shards}
    # Entries that never change: fixed and undeclared keys only. Movable keys
    # must be excluded from bounding partial maps while still open, otherwise
    # their old shard would be assumed and the lower bound would be unsound.
    fixed_list_map = {
        key: shard for key, shard in base_list_map.items() if key not in movable_set
    }

    # --- Branch and bound over the finite, explicitly declared domain ------
    feasible: list[dict[str, Any]] = []
    infeasible: list[dict[str, Any]] = []
    reason_counts: Counter[str] = Counter()
    pruned_branches: Counter[str] = Counter()
    evaluations = 0
    limit_reached = False
    best_request: dict[str, Any] | None = None
    best_peak: dict[str, Any] | None = None
    request_frontier: int | None = None
    peak_frontier: int | None = None

    def better(candidate: dict[str, Any], incumbent: dict[str, Any] | None, primary: str) -> bool:
        if incumbent is None:
            return True
        candidate_key = (
            candidate[primary],
            candidate["moved_rows"],
            _layout_lex_key(candidate["list_map"], movable_keys),
        )
        incumbent_key = (
            incumbent[primary],
            incumbent["moved_rows"],
            _layout_lex_key(incumbent["list_map"], movable_keys),
        )
        return candidate_key < incumbent_key

    def evaluate_leaf(assignment: dict[str, str], move_count: int) -> dict[str, Any] | None:
        nonlocal evaluations
        evaluations += 1
        candidate_map = dict(base_list_map)
        candidate_map.update(assignment)
        candidate = _candidate_topology(old, table_name, candidate_map)
        candidate_router = Router(candidate)
        cells, moved_row_ids, leaf_route_errors, leaf_dropped = _migration_matrix(
            old_router, candidate_router, rows, table_name
        )
        explanations = explain_workload(candidate, workload)
        # Seed every topology shard, not only table_shards(): a default shard
        # that no list_map entry points to is still a valid query target.
        leaf_load: dict[str, int] = {shard: 0 for shard in old.shards}
        total_requests = 0
        query_records = []
        unsafe = leaf_route_errors > 0
        for record in explanations:
            total_requests += record["requests"]
            for shard, contribution in record["shard_contributions"].items():
                leaf_load[shard] += contribution
            if record["route_error"]:
                unsafe = True
            query_records.append(
                {
                    "name": record["name"],
                    "frequency": record["frequency"],
                    "decision": record["decision"],
                    "reason": record["reason"],
                    "targets": record["targets"],
                    "target_count": record["target_count"],
                    "requests": record["requests"],
                    "shard_contributions": (
                        dict(sorted(record["shard_contributions"].items()))
                        if record["frequency"]
                        else {}
                    ),
                    "filters": record["filters"],
                }
            )
        peak = max(leaf_load.values()) if leaf_load else 0
        over_budget = move_count > move_budget_rows
        over_cap = any(leaf_load.get(shard, 0) > caps[shard] for shard in caps)
        matrix_move_total = sum(cell["row_count"] for cell in cells)
        # The incremental group counter must agree with the Router matrix.
        assert matrix_move_total == move_count, (matrix_move_total, move_count)
        result = {
            "list_map": dict(sorted(candidate_map.items())),
            "fingerprint": candidate.fingerprint,
            "moved_rows": move_count,
            "budget_remaining": move_budget_rows - move_count,
            "move_cells": cells,
            "moved_row_ids": moved_row_ids,
            "route_errors": leaf_route_errors,
            "dropped_rows": leaf_dropped,
            "queries": query_records,
            "shard_load": dict(sorted(leaf_load.items())),
            "shard_caps": dict(sorted(caps.items())),
            "total_requests": total_requests,
            "peak_shard_requests": peak,
            "unsafe": unsafe,
        }
        if unsafe:
            reason = "route_error"
        elif over_budget:
            reason = "move_budget"
        elif over_cap:
            reason = "read_cap"
        else:
            return result
        violations = []
        if unsafe:
            violations.append("rows or list values could not be routed/explained safely")
        if over_budget:
            violations.append(f"moves {move_count} exceed budget {move_budget_rows}")
        if over_cap:
            violations.extend(
                f"shard {shard} load {leaf_load.get(shard, 0)} exceeds cap {caps[shard]}"
                for shard in sorted(caps)
                if leaf_load.get(shard, 0) > caps[shard]
            )
        infeasible.append(
            {
                "list_map": dict(sorted(candidate_map.items())),
                "reason": reason,
                "moved_rows": move_count,
                "total_requests": total_requests,
                "peak_shard_requests": peak,
                "violations": violations,
            }
        )
        reason_counts[reason] += 1
        return None

    def search(
        depth: int,
        assignment: dict[str, str],
        move_count: int,
    ) -> None:
        nonlocal best_request, best_peak, request_frontier, peak_frontier, limit_reached
        if limit_reached:
            return
        if depth == len(key_domains):
            if evaluations >= max_evaluations:
                # Leaf budget spent before this layout could be evaluated.
                limit_reached = True
                return
            leaf = evaluate_leaf(assignment, move_count)
            if leaf is None:
                # A hard-constraint rejection recorded in ``infeasible``; the
                # budget was not tripped, so traversal continues normally.
                return
            feasible.append(leaf)
            if better(leaf, best_request, "total_requests"):
                best_request = leaf
            if better(leaf, best_peak, "peak_shard_requests"):
                best_peak = leaf
            return
        domain = key_domains[depth]
        # This node's bound runs before ``key_domains[depth]`` is chosen, so
        # every key from ``depth`` onward is still open. The branch loop below
        # assigns ``domain.key`` before recursing, where it leaves the set.
        open_keys = frozenset(item.key for item in key_domains[depth:])
        # Bounding map: immutable fixed/undeclared entries plus the keys
        # assigned on this branch. Open movable keys are deliberately absent.
        partial_map = dict(fixed_list_map)
        partial_map.update(assignment)
        total_lb, load_lb, bound_complete = _bound_requests(
            table, old.shards,
            [(f, kf) for f, kf, _ in workload_parsed], partial_map, open_keys,
        )
        # Hard-cap pruning is sound only when every query's target set is
        # fully determined; otherwise an open key could move load away. A cap
        # prune is a feasibility proof for the whole subtree, so it never
        # defeats an exhausted/infeasible certification.
        if bound_complete and any(load_lb.get(shard, 0) > caps[shard] for shard in caps):
            pruned_branches["read_cap"] += 1
            return
        # Record the weakest (smallest) bound seen anywhere, including nodes
        # that are never expanded into leaves. These frontiers are what allow
        # an incumbent to be certified optimal once traversal stops early.
        if request_frontier is None or total_lb < request_frontier:
            request_frontier = total_lb
        branch_peak_lb = max(load_lb.values(), default=0)
        if peak_frontier is None or branch_peak_lb < peak_frontier:
            peak_frontier = branch_peak_lb
        for shard in domain.choices:
            next_assignment = dict(assignment)
            next_assignment[domain.key] = shard
            extra = domain.group_size if shard != old_shard_by_key[domain.key] else 0
            next_moves = move_count + extra
            # Whole key groups count atomically; partial groups cannot fit.
            if next_moves > move_budget_rows:
                infeasible.append(
                    {
                        "list_map": dict(sorted({**base_list_map, **next_assignment}.items())),
                        "reason": "move_budget",
                        "moved_rows": next_moves,
                        "violations": [
                            f"key group {domain.key!r} needs {domain.group_size} row(s); "
                            f"running total {next_moves} exceeds budget {move_budget_rows}"
                        ],
                    }
                )
                reason_counts["move_budget"] += 1
                continue
            search(depth + 1, next_assignment, next_moves)

    search(0, {}, 0)

    # --- Stable ordering of every evaluated layout -------------------------
    feasible.sort(
        key=lambda item: (
            item["total_requests"],
            item["peak_shard_requests"],
            item["moved_rows"],
            _layout_lex_key(item["list_map"], movable_keys),
        )
    )
    infeasible.sort(
        key=lambda item: (
            item["reason"], item.get("moved_rows", 0), tuple(sorted(item["list_map"].items()))
        )
    )

    # Exhausted means the evaluation budget was never reached, so every leaf
    # was either evaluated or removed by a sound hard-constraint proof (a
    # complete read-cap bound or an atomic move-budget rejection). Such prunes
    # establish feasibility/optimality over the whole declared domain.
    exhausted = not limit_reached
    domain_size = 1
    for domain in key_domains:
        domain_size *= len(domain.choices)

    # --- Select one winner per business objective --------------------------
    selections: dict[str, Any] = {}
    for objective, winner, bound_key in (
        ("request_economy", best_request, "total_requests"),
        ("hotspot_relief", best_peak, "peak_shard_requests"),
    ):
        if winner is None:
            selections[objective] = None
            continue
        if exhausted:
            status = "optimal"
            evidence = (
                f"exhaustive enumeration of {domain_size} layout(s); every feasible layout was "
                "evaluated with the real query interpreter and Routers"
            )
            bound = None
        else:
            frontier = request_frontier if objective == "request_economy" else peak_frontier
            if frontier is not None and frontier == winner[bound_key]:
                status = "optimal"
                evidence = (
                    f"search stopped after {max_evaluations} evaluations but the branch-and-bound "
                    f"lower bound {frontier} equals the incumbent objective value, proving optimality"
                )
                bound = frontier
            else:
                status = "unknown"
                evidence = (
                    f"evaluation budget {max_evaluations} exhausted before optimality could be "
                    "proven; the incumbent is feasible but not certified optimal"
                )
                bound = frontier
        selections[objective] = {
            "status": status,
            "optimality_evidence": evidence,
            "lower_bound": bound,
            "layout": _public_layout(winner, table_name, caps, move_budget_rows),
        }

    if not feasible:
        overall_status = "infeasible" if exhausted else "unknown"
    elif limit_reached and any(
        selections[obj]["status"] == "unknown" for obj in OBJECTIVES if selections[obj]
    ):
        overall_status = "unknown"
    else:
        overall_status = "optimal"
    safe = overall_status == "optimal" and not global_blockers

    infeasibility_evidence: list[dict[str, Any]] = []
    if overall_status == "infeasible":
        infeasibility_evidence = _infeasibility_evidence(
            table_name, key_domains, old_shard_by_key, move_budget_rows, caps,
            reason_counts, infeasible, pruned_branches,
        )

    return {
        "status": overall_status,
        "safe_to_execute": safe,
        "table": table_name,
        "objectives": list(OBJECTIVES),
        "constraints": {
            "move_budget_rows": move_budget_rows,
            "shard_read_caps": dict(sorted(caps.items())),
        },
        "declared_domain": {
            "movable_keys": [
                {
                    "key": item.key,
                    "candidate_shards": list(item.choices),
                    "current_shard": old_shard_by_key[item.key],
                    "group_rows": item.group_size,
                    "group_row_ids": list(item.row_ids),
                }
                for item in key_domains
            ],
            "fixed_keys": [
                {"key": key, "shard": table.list_map[key]} for key in fixed_normalized
            ],
            "preserved": [
                "key_fields", "normalization", "null_policy", "default_shard",
                "undeclared list_map entries",
            ],
        },
        "enumeration": {
            "domain_size": domain_size,
            "evaluated_feasible": len(feasible),
            "evaluated_infeasible": len(infeasible),
            "pruned_branches": dict(sorted(pruned_branches.items())),
            "max_evaluations": max_evaluations,
            "exhausted": exhausted,
            "infeasible_reasons": dict(sorted(reason_counts.items())),
        },
        "workload": [
            {"name": _query_name(query), "frequency": frequency, "query": query}
            for frequency, _, query in workload_parsed
        ],
        "selections": selections,
        "feasible_layouts": [
            _public_layout(item, table_name, caps, move_budget_rows) for item in feasible
        ],
        "rejected_layouts": infeasible,
        "infeasibility_evidence": infeasibility_evidence,
        "blockers": global_blockers,
        "row_accounting": {
            "total_rows": len(rows),
            "route_errors": pre_route_errors,
            "dropped_rows": pre_dropped,
            "movable_group_rows": sum(item.group_size for item in key_domains),
        },
        "disclaimer": (
            "Static offline layout search. The old topology and physical files were not modified, "
            "no migration was executed and no query was sent to a datastore."
        ),
    }

def _query_name(query: Mapping[str, Any]) -> str:
    name = query.get("name")
    return name if isinstance(name, str) else str(query.get("table", "query"))

def _public_layout(item: Mapping[str, Any], table_name: str, caps: Mapping[str, int], budget: int) -> dict[str, Any]:
    """Serialize one evaluated layout with independently recomputable evidence."""
    return {
        "table": table_name,
        "list_map": item["list_map"],
        "fingerprint": item["fingerprint"],
        "moved_rows": item["moved_rows"],
        "budget_remaining": budget - item["moved_rows"],
        "move_cells": item["move_cells"],
        "moved_row_ids": item["moved_row_ids"],
        "route_errors": item["route_errors"],
        "dropped_rows": item["dropped_rows"],
        "queries": item["queries"],
        "shard_load": item["shard_load"],
        "shard_caps": dict(sorted(caps.items())),
        "total_requests": item["total_requests"],
        "peak_shard_requests": item["peak_shard_requests"],
    }

def _infeasibility_evidence(
    table_name: str,
    key_domains: list[_KeyDomain],
    old_shard_by_key: Mapping[str, str],
    budget: int,
    caps: Mapping[str, int],
    reason_counts: Counter[str],
    infeasible: list[dict[str, Any]],
    pruned_branches: Counter[str],
) -> list[dict[str, Any]]:
    """Explain which hard constraint makes the whole declared domain infeasible."""
    evidence: list[dict[str, Any]] = []
    minimum_moves = 0
    cheapest: dict[str, str] = {}
    for domain in key_domains:
        current = old_shard_by_key[domain.key]
        choice = current if current in domain.choices else domain.choices[0]
        cheapest[domain.key] = choice
        if choice != current:
            minimum_moves += domain.group_size
    if minimum_moves > budget:
        evidence.append(
            {
                "constraint": "move_budget_rows",
                "message": (
                    f"even the cheapest assignment moves {minimum_moves} row(s), "
                    f"which exceeds the budget of {budget}"
                ),
                "minimum_moves": minimum_moves,
                "budget": budget,
                "assignment": dict(sorted(cheapest.items())),
            }
        )
    cap_hits = reason_counts.get("read_cap", 0) + pruned_branches.get("read_cap", 0)
    if cap_hits and not reason_counts.get("route_error"):
        sample = next((item for item in infeasible if item["reason"] == "read_cap"), None)
        evidence.append(
            {
                "constraint": "shard_read_caps",
                "message": "every budget-feasible assignment overloads at least one shard",
                "caps": dict(sorted(caps.items())),
                "leaf_violations": cap_hits,
                "sample_violations": sample["violations"] if sample else [],
            }
        )
    if reason_counts.get("route_error"):
        evidence.append(
            {
                "constraint": "routing_safety",
                "message": "every candidate contains rows or list values that cannot be safely routed/explained",
                "count": reason_counts["route_error"],
            }
        )
    if not evidence:
        evidence.append(
            {
                "constraint": "combined",
                "message": "no layout satisfies the move budget and every shard read cap simultaneously",
                "reason_counts": dict(sorted(reason_counts.items())),
                "pruned_branches": dict(sorted(pruned_branches.items())),
            }
        )
    return evidence
