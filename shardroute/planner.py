"""Build a dry-run resharding matrix, batches, blockers and rollback checklist."""

from __future__ import annotations

from collections import Counter
from typing import Any, Mapping

from .errors import ShardRouteError
from .models import LogicalRow, ScanResult, Topology
from .relations import evaluate_relations
from .router import Router

MAX_BATCH_ROWS = 80

def _audit_blockers(audit: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    if not audit:
        return []
    grouped: Counter[tuple[str, str]] = Counter()
    for finding in audit.get("findings", []):
        code = str(finding.get("code", ""))
        if code.startswith("RELATION_"):
            # Declared-relation verdicts are emitted by ``_relation_blockers``
            # with the full old/new evidence, so they are not double-counted.
            continue
        if finding.get("severity") in {"critical", "high"}:
            grouped[(finding.get("table", ""), code)] += 1
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

def _relation_blockers(audit: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    """Turn declared-relation findings into plan blockers.

    Each ``(relation, phase, code)`` group becomes exactly one blocking entry
    whose count and row evidence come from the same finding list the report
    shows, so a reviewer can trace every blocker back to specific rows and
    shards.  Parent-key findings that are not attached to a child row are
    included here as well; they must not silently disappear from the plan.
    """
    if not audit:
        return []
    relations = audit.get("relations")
    if not relations:
        return []
    grouped: dict[tuple[str, str, str], dict[str, Any]] = {}
    child_tables = {
        str(item.get("name", "")): str(item.get("child_table", ""))
        for item in relations.get("declared", [])
    }
    for finding in relations.get("findings", []):
        code = str(finding.get("code", ""))
        if not code.startswith("RELATION_"):
            continue
        detail = finding.get("detail", {})
        relation = str(detail.get("relation", ""))
        phase = str(detail.get("phase", ""))
        if not relation or phase not in {"old", "new"}:
            continue
        key = (relation, phase, code)
        bucket = grouped.setdefault(
            key,
            {
                "table": finding.get("table", ""),
                "child_rows": set(),
                "parent_rows": set(),
            },
        )
        row_id = str(finding.get("row_id", ""))
        if not row_id:
            continue
        if finding.get("table", "") == child_tables.get(relation, ""):
            bucket["child_rows"].add(row_id)
        else:
            bucket["parent_rows"].add(row_id)
    blockers: list[dict[str, Any]] = []
    for (relation, phase, code), bucket in sorted(grouped.items()):
        child_rows = sorted(bucket["child_rows"])
        parent_rows = sorted(bucket["parent_rows"])
        count = len(child_rows) or len(parent_rows) or 1
        blockers.append(
            {
                "code": f"PLAN_RELATION_{phase.upper()}_{relation.upper()}_{code}",
                "severity": "blocking",
                "table": bucket["table"],
                "count": count,
                "relation": relation,
                "phase": phase,
                "relation_code": code,
                "evidence": {
                    "relation": relation,
                    "phase": phase,
                    "child_row_ids": child_rows[:20],
                    "child_row_total": len(child_rows),
                    "parent_row_ids": parent_rows[:20],
                    "parent_row_total": len(parent_rows),
                },
                "message": (
                    f"{count} row(s) fail the declared relation {relation!r} "
                    f"under the {phase} topology ({code})"
                ),
            }
        )
    return blockers


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
    scan: ScanResult | None = None,
    relation: Any | None = None,
) -> dict[str, Any]:
    old_router, new_router = Router(old), Router(new)
    move_counts: Counter[tuple[str, str, str]] = Counter()
    table_totals: Counter[str] = Counter()
    table_moved: Counter[str] = Counter()
    relation_report: dict[str, Any] | None = (
        relation.report if relation is not None else None
    )
    if relation_report is None and scan is not None and old.relations:
        recomputed = evaluate_relations(old, scan, target=new)
        if recomputed is not None:
            relation_report = recomputed.report
    if relation_report is None and audit and isinstance(audit.get("relations"), dict):
        relation_report = audit["relations"]
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
    blockers = (
        _audit_blockers(audit)
        + _compatibility_blockers(old, new)
        + _relation_blockers({"relations": relation_report} if relation_report else None)
    )
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
    plan = {
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
    if relation_report is not None:
        plan["relations"] = relation_report
        plan["summary"]["relation_blockers"] = sum(
            1 for item in blockers if item["code"].startswith("PLAN_RELATION_")
        )
    return plan
