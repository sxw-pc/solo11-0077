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
    summary: dict[str, Any] = {
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
    }
    if plan.get("relations"):
        summary["relation_contracts"] = (
            plan["relations"].get("summary", {}).get("contracts", 0)
        )
        summary["relation_blockers"] = sum(
            1 for item in plan["blockers"] if item["code"].startswith("PLAN_RELATION_")
        )
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
        "summary": summary,
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
    relations = report["plan"].get("relations") or report["audit"].get("relations")
    if relations:
        lines.extend(_render_relations_markdown(relations))
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

def _render_relations_markdown(relations: dict[str, Any]) -> list[str]:
    summary = relations.get("summary", {})
    lines = [
        "",
        "## Declared relation verification",
        "",
        f"- contracts: {summary.get('contracts', 0)} "
        f"(target: {summary.get('target_contracts', 0)})",
        f"- old topology verdict: **{summary.get('old_status', 'pass')}** "
        f"({summary.get('old_blocked', 0)} blocked of {summary.get('evaluations', 0)})",
        f"- target topology verdict: **{summary.get('new_status', 'not_evaluated')}** "
        f"({summary.get('new_blocked', 0)} blocked)",
        "",
        "| Relation | Mode | Parent | Child | Child row | Parent row | "
        "Old | Old shards (child→parent) | New | New shards (child→parent) |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    for item in relations.get("evaluations", []):
        old = item.get("old", {})
        new = item.get("new")
        old_shards = f"{_cell(old.get('child_routed_shard') or '-')} → " \
            f"{_cell(old.get('parent_routed_shard') or '-')}"
        if new is not None:
            new_status = "blocked" if new.get("status") == "blocked" else "satisfied"
            new_shards = f"{_cell(new.get('child_target_shard') or '-')} → " \
                f"{_cell(new.get('parent_target_shard') or '-')}"
        else:
            new_status = "not_evaluated"
            new_shards = "-"
        lines.append(
            "| {relation} | {mode} | {parent} | {child} | {child_row} | {parent_row} | "
            "{old} | {old_shards} | {new} | {new_shards} |".format(
                relation=_cell(item.get("relation")),
                mode=_cell(item.get("mode")),
                parent=_cell(item.get("parent_table")),
                child=_cell(item.get("child_table")),
                child_row=_cell(item.get("child_row_id") or "-"),
                parent_row=_cell(item.get("parent_row_id") or "-"),
                old=_cell(old.get("status") or "-"),
                old_shards=old_shards,
                new=_cell(new_status),
                new_shards=new_shards,
            )
        )
    lines.extend(
        [
            "",
            "### Relation findings",
            "",
            "| Severity | Code | Phase | Relation | Child row | Parent row | Shard | Message |",
            "|---|---|---|---|---|---|---|---|",
        ]
    )
    if not relations.get("findings"):
        lines.append("| - | - | - | - | - | - | - | No relation findings |")
    for finding in relations.get("findings", []):
        detail = finding.get("detail", {})
        lines.append(
            "| {severity} | {code} | {phase} | {relation} | {child} | {parent} | "
            "{shard} | {message} |".format(
                severity=_cell(finding.get("severity")),
                code=_cell(finding.get("code")),
                phase=_cell(detail.get("phase") or "-"),
                relation=_cell(detail.get("relation") or "-"),
                child=_cell(finding.get("row_id") or "-"),
                parent=_cell(detail.get("parent_row_id") or "-"),
                shard=_cell(finding.get("shard") or "-"),
                message=_cell(finding.get("message")),
            )
        )
    return lines


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
