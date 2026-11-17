"""Build the deterministic demo workspace and run the complete offline pipeline."""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

from .audit import run_audit
from .config import load_topology
from .errors import WORKSPACE_INVALID, ShardRouteError
from .fixturegen import DEMO_SEED, build_fixtures
from .ledger import Ledger
from .planner import build_plan
from .query_explain import explain_file
from .report import build_report, write_reports
from .scanner import scan_workspace

SENTINEL = ".shardroute-workspace"
SENTINEL_BODY = {"format": 1, "tool": "shardroute", "note": "Synthetic demo workspace; safe to regenerate."}

def prepare_workspace(workspace: str | Path) -> Path:
    root = Path(workspace)
    if root.exists():
        if not root.is_dir():
            raise ShardRouteError(WORKSPACE_INVALID, "workspace path is not a directory", str(root))
        entries = list(root.iterdir())
        if entries and not (root / SENTINEL).is_file():
            raise ShardRouteError(
                WORKSPACE_INVALID,
                "refusing to overwrite a non-ShardRoute directory",
                str(root),
                {"hint": f"choose an empty directory or remove the existing one"},
            )
        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)
    (root / SENTINEL).write_bytes(
        (json.dumps(SENTINEL_BODY, sort_keys=True) + "\n").encode("utf-8")
    )
    return root

def _query_files(workspace: Path) -> list[Path]:
    return sorted((workspace / "queries").glob("*.json"), key=lambda path: path.name)

def run_pipeline(workspace: str | Path, seed: int = DEMO_SEED) -> dict[str, Any]:
    root = Path(workspace)
    required = [root / "config" / "topology_old.json", root / "config" / "topology_new.json"]
    missing = [str(path) for path in required if not path.is_file()]
    if missing:
        raise ShardRouteError(WORKSPACE_INVALID, "topology configuration is missing", str(root), {"missing": missing})
    old = load_topology(required[0])
    new = load_topology(required[1])
    scan = scan_workspace(root / "logical", root / "shards", old.table_names())
    audit = run_audit(old, scan)
    plan = build_plan(old, new, scan.logical_rows, audit)
    certificate = plan["compatibility_certificate"]
    queries = [explain_file(old, path) for path in _query_files(root)]
    report = build_report(
        old=old,
        new=new,
        seed=seed,
        logical_fingerprint=scan.logical_fingerprint,
        physical_fingerprint=scan.physical_fingerprint,
        audit=audit,
        plan=plan,
        queries=queries,
        certificate=certificate,
    )
    artifacts = write_reports(report, root)
    ledger = Ledger(root / "ledger.sqlite3")
    ledger.initialize()
    try:
        ledger.record_run(report, artifacts)
    finally:
        ledger.close()
    return {
        "run_id": report["run_id"],
        "status": report["status"],
        "seed": seed,
        "logical_rows": sum(len(rows) for rows in scan.logical_rows.values()),
        "findings": audit["summary"]["total_findings"],
        "critical_or_high": audit["summary"]["critical_or_high"],
        "moved_rows": plan["moved_rows"],
        "batches": len(plan["batches"]),
        "blockers": len(plan["blockers"]),
        "safe_to_execute": plan["safe_to_execute"],
        "compatibility_status": certificate["status"],
        "future_write_permission": certificate["permission"],
        "future_writes_allowed": certificate["future_writes_allowed"],
        "compatibility_counterexample_tables": certificate["summary"]["counterexample_tables"],
        "pruned_queries": sum(1 for item in queries if item.decision == "pruned"),
        "query_count": len(queries),
        "artifacts": artifacts,
        "ledger": str(root / "ledger.sqlite3"),
    }

def run_demo(workspace: str | Path, seed: int = DEMO_SEED) -> dict[str, Any]:
    root = prepare_workspace(workspace)
    fixture = build_fixtures(root, seed)
    summary = run_pipeline(root, seed)
    summary["fixture_fingerprint"] = fixture["fixture_fingerprint"]
    summary["fixture_rows"] = fixture["logical_rows"]
    summary["manifest_count"] = fixture["manifest_count"]
    return summary
