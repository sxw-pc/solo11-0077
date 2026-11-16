"""Shared compact fixtures for the unittest suite."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

from shardroute.audit import run_audit
from shardroute.config import load_topology
from shardroute.config_validation import validate_document
from shardroute.ledger import Ledger
from shardroute.models import LogicalRow, ScanResult, make_finding
from shardroute.planner import build_plan
from shardroute.publish import publish_run
from shardroute.query_explain import explain_query
from shardroute.report import build_report, write_reports

ROOT = Path(__file__).resolve().parent
FIXTURES = ROOT / "fixtures"

def base_doc() -> dict[str, Any]:
    return json.loads((FIXTURES / "config" / "mini_topology.json").read_text(encoding="utf-8"))

def mini_topology():
    return load_topology(FIXTURES / "config" / "mini_topology.json")

def topology_from(doc: dict[str, Any]):
    topology, issues, _ = validate_document(doc)
    assert topology is not None and not issues, [i.as_dict() for i in issues]
    return topology

def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n", encoding="utf-8")

def logical(table: str, row_id: str, values: dict[str, str]) -> LogicalRow:
    return LogicalRow(table, row_id, {"row_id": row_id, **values}, f"logical/{table}.csv", 2)

def empty_audit() -> dict[str, Any]:
    return {
        "status": "pass",
        "summary": {"total_findings": 0, "critical_or_high": 0},
        "tables": {},
        "findings": [],
    }

def empty_plan(old, new) -> dict[str, Any]:
    return {
        "old_fingerprint": old.fingerprint,
        "new_fingerprint": new.fingerprint,
        "total_rows": 0,
        "moved_rows": 0,
        "unchanged_rows": 0,
        "dropped_rows": 0,
        "route_errors": 0,
        "blocked_rows": 0,
        "move_cells": [],
        "batches": [],
        "blockers": [],
        "rollback_checklist": [{"order": 1, "action": "freeze", "verification": "token"}],
        "safe_to_execute": True,
        "summary": {},
    }

def test_report(with_finding: bool = True) -> dict[str, Any]:
    old = mini_topology()
    new = old
    query = explain_query(
        old,
        {
            "name": "orders_by_tenant",
            "table": "orders",
            "filters": [{"field": "tenant_id", "op": "eq", "value": "tenant_demo_001"}],
        },
    )
    audit = empty_audit()
    if with_finding:
        finding = make_finding(
            "MISPLACED_ROW", "critical", "orders", "synthetic test finding",
            shard="shard_b", row_id="ord_t_0002", detail={"expected_shard": "shard_a"},
        ).as_dict()
        audit = {
            "status": "fail",
            "summary": {"total_findings": 1, "critical_or_high": 1},
            "tables": {
                "orders": {
                    "logical_rows": 4, "physical_rows": 4, "eligible_shards": ["shard_a", "shard_b"],
                    "findings": 1, "critical": 1, "by_code": {"MISPLACED_ROW": 1},
                }
            },
            "findings": [finding],
        }
    plan = empty_plan(old, new)
    return build_report(
        old=old,
        new=new,
        seed=7,
        logical_fingerprint="a" * 64,
        physical_fingerprint="b" * 64,
        audit=audit,
        plan=plan,
        queries=[query],
    )

def write_test_run(root: Path, with_finding: bool = True) -> dict[str, Any]:
    report = test_report(with_finding)
    ledger = Ledger(root / "ledger.sqlite3")
    ledger.initialize()
    publish_run(root, report, ledger)
    ledger.close()
    return report

def empty_scan(topology, rows_by_table=None) -> ScanResult:
    return ScanResult(
        logical_rows=rows_by_table or {name: [] for name in topology.table_names()},
        physical_rows={name: [] for name in topology.table_names()},
        files=[],
        manifests={},
        manifest_errors=[],
        physical_dirs=[],
        logical_fingerprint="0" * 64,
        physical_fingerprint="0" * 64,
    )

def plan_for(rows: dict[str, list[LogicalRow]], audit: dict[str, Any] | None = None) -> dict[str, Any]:
    old = mini_topology()
    return build_plan(old, old, rows, audit)

def audit_for(topology, scan: ScanResult) -> dict[str, Any]:
    return run_audit(topology, scan)

__all__ = [
    "ROOT", "FIXTURES", "base_doc", "mini_topology", "topology_from", "write_json",
    "logical", "empty_audit", "empty_plan", "test_report", "write_test_run", "empty_scan",
    "plan_for", "audit_for", "copy", "run_audit", "build_plan", "explain_query",
]
