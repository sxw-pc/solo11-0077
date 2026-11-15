"""Assemble and render deterministic JSON, Markdown and self-contained HTML reports."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from . import RUN_FORMAT
from .canonical import sha256_bytes, stable_id
from .html_report import render_html
from .models import QueryExplanation, Topology

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

def write_reports(report: dict[str, Any], workspace: str | Path) -> list[dict[str, Any]]:
    root = Path(workspace) / "reports"
    root.mkdir(parents=True, exist_ok=True)
    payloads = {
        "report.json": (json.dumps(report, sort_keys=True, indent=2, ensure_ascii=False) + "\n").encode("utf-8"),
        "report.md": render_markdown(report).encode("utf-8"),
        "report.html": render_html(report).encode("utf-8"),
    }
    artifacts = []
    for name in sorted(payloads):
        data = payloads[name]
        (root / name).write_bytes(data)
        artifacts.append({"path": f"reports/{name}", "sha256": sha256_bytes(data), "bytes": len(data)})
    return artifacts
