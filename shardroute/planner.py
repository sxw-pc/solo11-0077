"""Build a dry-run resharding matrix, batches, blockers and rollback checklist."""

from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Mapping

from .errors import ShardRouteError
from .models import LogicalRow, Topology
from .router import Router

MAX_BATCH_ROWS = 80

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
    route_error_details: list[dict[str, Any]] = []
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
            except ShardRouteError as exc:
                route_errors += 1
                route_error_details.append(
                    {
                        "table": table,
                        "row_id": row.row_id,
                        "code": exc.code,
                        "location": exc.location,
                        "message": exc.message,
                        "details": exc.details,
                    }
                )
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
    result = {
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
    if route_error_details:
        # Only present when something actually failed to route, so plans over
        # fully valid data keep their historical representation.
        result["route_error_details"] = route_error_details
    return result
