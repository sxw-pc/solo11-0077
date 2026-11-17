"""Build a dry-run resharding matrix, batches, blockers and rollback checklist."""

from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Mapping

from .errors import ShardRouteError
from .models import LogicalRow, Topology
from .router import Router

MAX_BATCH_ROWS = 80

# Amount fidelity gate codes, kept distinct from placement audit blockers so
# the two authorization contracts are never conflated with routing errors.
AMOUNT_GATE_PREFIX = "PLAN_AMOUNT"
# Amount finding codes.  Contract-specific codes carry their contract in the
# finding detail; row-level quality codes are contract-independent and gate
# every contract declared on their table.
_CONTRACT_SPECIFIC_AMOUNT_CODES = {
    "AMOUNT_ROW_HALF_EVEN_MISMATCH",
    "AMOUNT_DUAL_MARGIN_VIOLATION",
    "AMOUNT_DUAL_MARGIN_INFEASIBLE",
    "AMOUNT_DUAL_MARGIN_UNPROVEN",
}
_QUALITY_AMOUNT_CODES = {
    "AMOUNT_DECIMAL_INVALID",
    "AMOUNT_NEGATIVE_AMOUNT",
    "AMOUNT_OVER_PRECISION",
    "AMOUNT_FIELD_UNTRUSTED",
}
_AMOUNT_FINDING_CODES = _CONTRACT_SPECIFIC_AMOUNT_CODES | _QUALITY_AMOUNT_CODES

# Tables that declare each contract, derived from the audit verdicts.
def _declared_contracts(audit: Mapping[str, Any]) -> dict[str, list[str]]:
    declared: dict[str, list[str]] = {}
    for verdict in audit.get("amount_contracts", []):
        declared.setdefault(verdict["table"], []).append(verdict["contract"])
    return declared

def _amount_gates(audit: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    """Turn amount findings into explicit executable gates per contract."""

    if not audit:
        return []
    declared = _declared_contracts(audit)
    # (table, contract, code) -> count.
    grouped: Counter[tuple[str, str, str]] = Counter()
    for finding in audit.get("findings", []):
        code = finding.get("code", "")
        if code not in _AMOUNT_FINDING_CODES:
            continue
        table = finding.get("table", "")
        if code in _QUALITY_AMOUNT_CODES:
            for contract in declared.get(table, []):
                grouped[(table, contract, code)] += 1
        else:
            contract = finding.get("detail", {}).get("contract", "unknown")
            grouped[(table, contract, code)] += 1
    gates = [
        {
            "code": f"{AMOUNT_GATE_PREFIX}_{code[len('AMOUNT_'):]}",
            "severity": "blocking",
            "table": table,
            "contract": contract,
            "count": count,
            "message": (
                f"{count} {code} finding(s) under {contract}; amount fidelity "
                "is not certified, blocking execution"
            ),
        }
        for (table, contract, code), count in sorted(grouped.items())
    ]
    # Contract-level UNKNOWN verdicts gate even when their only problem was an
    # exclusion that already raised a finding; this keeps unknowns auditable.
    for verdict in audit.get("amount_contracts", []):
        if verdict.get("status") == "unknown":
            gates.append(
                {
                    "code": f"{AMOUNT_GATE_PREFIX}_{verdict['contract'].upper()}_UNKNOWN",
                    "severity": "blocking",
                    "table": verdict["table"],
                    "contract": verdict["contract"],
                    "count": verdict.get("ineligible_rows", 0),
                    "message": (
                        f"{verdict['contract']} verdict is UNKNOWN for {verdict['ineligible_rows']} "
                        "ineligible row(s); no fidelity certificate can be issued"
                    ),
                }
            )
    gates.sort(key=lambda item: (item["table"], item["contract"], item["code"]))
    return gates

def _audit_blockers(audit: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    if not audit:
        return []
    grouped: Counter[tuple[str, str]] = Counter()
    for finding in audit.get("findings", []):
        if finding.get("severity") in {"critical", "high"}:
            code = finding.get("code", "")
            # Amount findings have their own explicitly separated gate list.
            if code in _AMOUNT_FINDING_CODES:
                continue
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
    amount_gates = _amount_gates(audit)
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
        "amount_gates": amount_gates,
        "rollback_checklist": _rollback_checklist(),
        "safe_to_execute": not any(item["severity"] == "blocking" for item in blockers)
        and not any(item["severity"] == "blocking" for item in amount_gates),
        "summary": {
            "tables": dict(sorted(table_totals.items())),
            "moved_by_table": dict(sorted(table_moved.items())),
            "move_cell_count": len(cells),
            "batch_count": len(batches),
            "max_batch_rows": MAX_BATCH_ROWS,
        },
    }
