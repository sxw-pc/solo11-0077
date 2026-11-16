"""Assemble and render deterministic JSON, Markdown and self-contained HTML reports."""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

from . import RUN_FORMAT
from .canonical import sha256_bytes, stable_id
from .errors import WORKSPACE_INVALID, ShardRouteError
from .html_report import render_html
from .models import QueryExplanation, Topology

REPORT_FILES = ("report.html", "report.json", "report.md")
MANIFEST_NAME = "manifest.json"
STAGING_PREFIX = ".staging-"
RUNS_DIRNAME = "runs"

def compute_run_id(
    old: Topology, new: Topology, logical_fingerprint: str, physical_fingerprint: str, seed: int
) -> str:
    return stable_id(
        "run",
        RUN_FORMAT,
        old.fingerprint,
        new.fingerprint,
        logical_fingerprint,
        physical_fingerprint,
        seed,
    )

def _topology_summary(topology: Topology) -> dict[str, Any]:
    return {
        "version": topology.version,
        "generated_at": topology.generated_at,
        "description": topology.description,
        "fingerprint": topology.fingerprint,
        "shards": list(topology.shards),
        "tables": [topology.tables[name].as_dict() for name in topology.table_names()],
    }

def build_report(
    *,
    old: Topology,
    new: Topology,
    seed: int,
    logical_fingerprint: str,
    physical_fingerprint: str,
    audit: dict[str, Any],
    plan: dict[str, Any],
    queries: list[QueryExplanation],
) -> dict[str, Any]:
    run_id = compute_run_id(old, new, logical_fingerprint, physical_fingerprint, seed)
    query_dicts = [item.as_dict() for item in queries]
    pruned = sum(1 for item in query_dicts if item["decision"] == "pruned")
    safe = bool(plan.get("safe_to_execute"))
    status = "fail" if audit["status"] == "fail" or not safe else audit["status"]
    report = {
        "format": RUN_FORMAT,
        "run_id": run_id,
        "generated_at": old.generated_at,
        "seed": seed,
        "status": status,
        "fingerprints": {
            "topology_old": old.fingerprint,
            "topology_new": new.fingerprint,
            "logical": logical_fingerprint,
            "physical": physical_fingerprint,
        },
        "topologies": {"old": _topology_summary(old), "new": _topology_summary(new)},
        "summary": {
            "audit_findings": audit["summary"]["total_findings"],
            "critical_or_high": audit["summary"]["critical_or_high"],
            "rows_moved": plan["moved_rows"],
            "rows_unchanged": plan["unchanged_rows"],
            "move_cells": len(plan["move_cells"]),
            "batches": len(plan["batches"]),
            "blockers": len(plan["blockers"]),
            "queries": len(query_dicts),
            "pruned_queries": pruned,
            "safe_to_execute": safe,
        },
        "audit": audit,
        "plan": plan,
        "queries": query_dicts,
        "disclaimer": (
            "Static, offline verification only. No database connection was opened and "
            "no shard was moved, rewritten, rebalanced or queried."
        ),
    }
    return report

def _cell(value: Any) -> str:
    text = str(value).replace("\r", " ").replace("\n", " ").replace("|", "\\|")
    return text or "-"

def render_markdown(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "# ShardRoute routing verification report",
        "",
        f"- run_id: `{report['run_id']}`",
        f"- generated_at (UTC): `{report['generated_at']}`",
        f"- status: **{report['status']}**",
        f"- seed: `{report['seed']}`",
        f"- old topology: `{report['fingerprints']['topology_old']}`",
        f"- new topology: `{report['fingerprints']['topology_new']}`",
        f"- logical snapshot: `{report['fingerprints']['logical']}`",
        "",
        "## Executive summary",
        "",
        "| Metric | Value |",
        "|---|---:|",
        f"| Audit findings | {summary['audit_findings']} |",
        f"| Critical or high findings | {summary['critical_or_high']} |",
        f"| Logical rows moved | {summary['rows_moved']} |",
        f"| Rows unchanged | {summary['rows_unchanged']} |",
        f"| Move cells | {summary['move_cells']} |",
        f"| Shadow batches | {summary['batches']} |",
        f"| Blockers | {summary['blockers']} |",
        f"| Queries pruned | {summary['pruned_queries']} / {summary['queries']} |",
        f"| Safe to execute | {'yes' if summary['safe_to_execute'] else 'no'} |",
        "",
        "## Audit tables",
        "",
        "| Table | Logical rows | Physical rows | Findings | Critical |",
        "|---|---:|---:|---:|---:|",
    ]
    for table, stats in sorted(report["audit"]["tables"].items()):
        lines.append(
            f"| {_cell(table)} | {stats['logical_rows']} | {stats['physical_rows']} | "
            f"{stats['findings']} | {stats['critical']} |"
        )
    lines.extend(
        [
            "",
            "## Findings",
            "",
            "| Severity | Code | Table | Shard | row_id | Message |",
            "|---|---|---|---|---|---|",
        ]
    )
    findings = report["audit"]["findings"]
    if not findings:
        lines.append("| - | - | - | - | - | No findings |")
    for finding in findings:
        lines.append(
            "| {severity} | {code} | {table} | {shard} | {row_id} | {message} |".format(
                severity=_cell(finding["severity"]),
                code=_cell(finding["code"]),
                table=_cell(finding["table"]),
                shard=_cell(finding["shard"]),
                row_id=_cell(finding["row_id"]),
                message=_cell(finding["message"]),
            )
        )
    lines.extend(
        [
            "",
            "## Reshard move matrix",
            "",
            "| Table | From | To | Rows |",
            "|---|---|---|---:|",
        ]
    )
    for cell in report["plan"]["move_cells"]:
        lines.append(
            f"| {_cell(cell['table'])} | {_cell(cell['from_shard'])} | "
            f"{_cell(cell['to_shard'])} | {cell['row_count']} |"
        )
    if not report["plan"]["move_cells"]:
        lines.append("| - | - | - | 0 |")
    lines.extend(["", "### Batch plan", "", "| Batch | Phase | Table | Source | Target | Rows |", "|---|---|---|---|---|---:|"])
    for batch in report["plan"]["batches"]:
        lines.append(
            "| {batch} | {phase} | {table} | {source} | {target} | {rows} |".format(
                batch=batch["batch_id"],
                phase=batch["phase"],
                table=_cell(batch["table"]),
                source=_cell(batch["source"]),
                target=_cell(batch["target"]),
                rows=batch["row_count"],
            )
        )
    if not report["plan"]["batches"]:
        lines.append("| - | - | - | - | - | 0 |")
    lines.extend(["", "### Blockers", ""])
    if report["plan"]["blockers"]:
        for blocker in report["plan"]["blockers"]:
            lines.append(
                f"- **{blocker['severity']}** `{blocker['code']}` "
                f"({_cell(blocker['table'])}): {blocker['message']} [count={blocker['count']}]"
            )
    else:
        lines.append("- No blockers. This remains a dry-run plan only.")
    lines.extend(["", "## Query pruning", "", "| Query | Table | Decision | Target shards | Reason |", "|---|---|---|---|---|"])
    for query in report["queries"]:
        targets = ", ".join(query["targeted_shards"]) or "none"
        lines.append(
            f"| {_cell(query['name'])} | {_cell(query['table'])} | {query['decision']} | "
            f"{_cell(targets)} | {_cell(query['reason'])} |"
        )
    lines.extend(["", "## Rollback checklist", ""])
    for item in report["plan"]["rollback_checklist"]:
        lines.append(f"{item['order']}. {item['action']} Verification: {item['verification']}")
    lines.extend(
        [
            "",
            "## Boundary notice",
            "",
            report["disclaimer"],
            "",
            "Reproduce with:",
            "",
            "```bash",
            "python -m shardroute demo --workspace .shardroute-demo",
            "```",
            "",
        ]
    )
    return "\n".join(lines)

def report_payloads(report: dict[str, Any]) -> dict[str, bytes]:
    """Render the three deterministic report payloads keyed by file name."""
    return {
        "report.json": (json.dumps(report, sort_keys=True, indent=2, ensure_ascii=False) + "\n").encode("utf-8"),
        "report.md": render_markdown(report).encode("utf-8"),
        "report.html": render_html(report).encode("utf-8"),
    }

def build_manifest(report: dict[str, Any], artifacts: list[dict[str, Any]]) -> dict[str, Any]:
    """Bind the run_id, fingerprints and every artifact hash into one manifest."""
    return {
        "format": "shardroute-publication/1",
        "run_id": report["run_id"],
        "report_format": report["format"],
        "generated_at": report["generated_at"],
        "status": report["status"],
        "safe_to_execute": bool(report["summary"]["safe_to_execute"]),
        "fingerprints": dict(report["fingerprints"]),
        "artifacts": sorted(
            (dict(item) for item in artifacts), key=lambda item: item["path"]
        ),
    }

def _write_payload_set(payloads: dict[str, bytes], directory: Path) -> list[dict[str, Any]]:
    directory.mkdir(parents=True, exist_ok=True)
    artifacts = []
    for name in sorted(payloads):
        data = payloads[name]
        (directory / name).write_bytes(data)
        artifacts.append(
            {"path": f"reports/{name}", "sha256": sha256_bytes(data), "bytes": len(data)}
        )
    return artifacts

def stage_run(report: dict[str, Any], workspace: str | Path) -> dict[str, Any]:
    """Render one run into an immutable version directory.

    Files are first written to a sibling staging directory and then moved
    with a single atomic ``rename``; readers can therefore never observe a
    half-written version directory.  Re-staging an identical run is a
    no-op; re-staging a run id whose bytes differ replaces the directory
    atomically.
    """
    root = Path(workspace) / "reports"
    run_id = report["run_id"]
    target = root / RUNS_DIRNAME / run_id
    staging = root / f"{STAGING_PREFIX}{run_id}"
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True, exist_ok=True)
    payloads = report_payloads(report)
    artifacts = _write_payload_set(payloads, staging)
    manifest = build_manifest(report, artifacts)
    (staging / MANIFEST_NAME).write_bytes(
        (json.dumps(manifest, sort_keys=True, indent=2, ensure_ascii=False) + "\n").encode("utf-8")
    )
    if target.exists():
        shutil.rmtree(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    staging.rename(target)
    return {"run_id": run_id, "directory": target, "artifacts": artifacts, "manifest": manifest}

def write_reports(report: dict[str, Any], workspace: str | Path) -> list[dict[str, Any]]:
    """Write the three reports directly under ``<workspace>/reports``.

    Legacy entry point used by the low-level tests; the publication pipeline
    uses :func:`stage_run` so versions are isolated and atomically swapped.
    """
    root = Path(workspace) / "reports"
    root.mkdir(parents=True, exist_ok=True)
    payloads = report_payloads(report)
    artifacts = []
    for name in sorted(payloads):
        data = payloads[name]
        (root / name).write_bytes(data)
        artifacts.append({"path": f"reports/{name}", "sha256": sha256_bytes(data), "bytes": len(data)})
    return artifacts

def run_directory(workspace: str | Path, run_id: str) -> Path:
    return Path(workspace) / "reports" / RUNS_DIRNAME / run_id

def load_run_report(workspace: str | Path, run_id: str) -> dict[str, Any]:
    """Read the canonical JSON report stored in a version directory."""
    path = run_directory(workspace, run_id) / "report.json"
    if not path.is_file():
        raise ShardRouteError(WORKSPACE_INVALID, "version report.json is missing", str(path))
    return json.loads(path.read_text(encoding="utf-8"))

def load_manifest(workspace: str | Path, run_id: str) -> dict[str, Any]:
    path = run_directory(workspace, run_id) / MANIFEST_NAME
    if not path.is_file():
        raise ShardRouteError(WORKSPACE_INVALID, "publication manifest is missing", str(path))
    return json.loads(path.read_text(encoding="utf-8"))

def verify_run_files(workspace: str | Path, run_id: str) -> dict[str, Any]:
    """Verify the on-disk version directory and return its parsed manifest.

    Every report file must exist, its bytes must match the manifest hash,
    ``report.json`` must parse and carry the same run_id, and every
    artifact recorded in the manifest must be present.  Any mismatch raises
    :class:`ShardRouteError`; the caller must never promote such a run.
    """
    directory = run_directory(workspace, run_id)
    if not directory.is_dir():
        raise ShardRouteError(WORKSPACE_INVALID, "run directory is missing", str(directory))
    manifest = load_manifest(workspace, run_id)
    if manifest.get("run_id") != run_id:
        raise ShardRouteError(
            WORKSPACE_INVALID, "manifest run_id does not match its directory", str(directory)
        )
    recorded = {item["path"]: item for item in manifest.get("artifacts", [])}
    expected_paths = {f"reports/{name}" for name in REPORT_FILES}
    if set(recorded) != expected_paths:
        raise ShardRouteError(
            WORKSPACE_INVALID, "manifest artifact set is incomplete", str(directory),
            {"expected": sorted(expected_paths), "found": sorted(recorded)},
        )
    for relative, item in sorted(recorded.items()):
        # The manifest stores logical workspace-relative paths; the
        # authoritative bytes of an unpublished version live inside its own
        # version directory, not at the top-level report paths.
        path = directory / Path(relative).name
        if not path.is_file():
            raise ShardRouteError(WORKSPACE_INVALID, "reported artifact is missing", str(path))
        data = path.read_bytes()
        digest = sha256_bytes(data)
        if digest != item["sha256"] or len(data) != item["bytes"]:
            raise ShardRouteError(
                WORKSPACE_INVALID, "artifact hash or size mismatch", str(path),
                {"expected_sha256": item["sha256"], "actual_sha256": digest},
            )
    report_path = directory / "report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    if report.get("run_id") != run_id:
        raise ShardRouteError(
            WORKSPACE_INVALID, "report.json run_id does not match the version", str(report_path)
        )
    if bool(report["summary"]["safe_to_execute"]) != bool(manifest["safe_to_execute"]):
        raise ShardRouteError(
            WORKSPACE_INVALID, "manifest and report disagree on safe_to_execute", str(report_path)
        )
    return manifest
