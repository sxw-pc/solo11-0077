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
    skew = audit.get("hash_skew_diagnostics", {})
    skew_significant = sum(
        1
        for diagnostic in skew.values()
        if diagnostic.get("applicable")
        and diagnostic["certificate"]["decision"] == "significant"
    )
    skew_unknown = sum(
        1
        for diagnostic in skew.values()
        if diagnostic.get("applicable")
        and diagnostic["certificate"]["decision"] == "unknown"
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
            "skew_significant_tables": skew_significant,
            "skew_unknown_tables": skew_unknown,
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


def _format_probability(value: Any) -> str:
    return "-" if value is None else f"{float(value):.6g}"


def render_skew_markdown(report: dict[str, Any]) -> list[str]:
    """Render the hash load-skew diagnostic section, tolerating absent diagnostics."""
    diagnostics = report.get("audit", {}).get("hash_skew_diagnostics", {})
    if not diagnostics:
        return []
    lines = ["", "## Hash load-skew diagnostics", ""]
    for table_name in sorted(diagnostics):
        diagnostic = diagnostics[table_name]
        lines.append(f"### Table `{_cell(table_name)}`")
        lines.append("")
        if not diagnostic.get("applicable"):
            lines.append(f"- Applicable: no (`{diagnostic.get('reason', 'not_applicable')}`)")
            lines.append("")
            continue
        certificate = diagnostic["certificate"]
        denominator = diagnostic["denominator"]
        baseline = diagnostic["bucket_baseline"]["shard_probabilities"]
        event = diagnostic["event"]
        lines.extend(
            [
                f"- Null hypothesis: `{diagnostic['hypothesis']}`",
                f"- Significance threshold alpha: `{diagnostic['alpha']}`",
                "- Denominator (reviewable):",
                f"  - logical rows: {denominator['logical_rows']}",
                f"  - eligible hash rows (group total weight): {denominator['eligible_hash_rows']}",
                f"  - excluded rows: {denominator['excluded_rows']}",
                f"  - exclusions by reason: {denominator['exclusions_by_reason'] or '{}'}",
                f"  - {denominator['exclusion_contract']}",
                f"- Canonical key groups: {diagnostic['key_groups']['count']} "
                f"(total weight {diagnostic['key_groups']['total_weight']})",
                "- Baseline shard probabilities from bucket_map "
                f"({diagnostic['bucket_baseline']['buckets']} buckets): "
                + ", ".join(
                    f"{_cell(shard)}={_format_probability(probability)}"
                    for shard, probability in sorted(baseline.items())
                ),
                f"- Observed event: max shard logical-row load >= {event['threshold']} "
                f"(observed {event['observed_max_load']} on "
                f"`{event['observed_max_shard']}`; loads {event['observed_shard_loads']})",
                f"- Certificate type: `{certificate['type']}`",
            ]
        )
        if certificate["tail_probability"] is not None:
            lines.append(
                "- Exact group-weighted tail probability: "
                f"`{_format_probability(certificate['tail_probability'])}`"
            )
        if certificate["tail_probability_upper_bound"] is not None:
            lines.append(
                "- Conservative tail probability upper bound (not an exact value): "
                f"`{_format_probability(certificate['tail_probability_upper_bound'])}`"
            )
        lines.extend(
            [
                f"- Decision: **{certificate['decision'].upper()}** "
                f"({certificate['decision_rule']})",
                "- Compute budget:",
                f"  - {certificate['budget']}",
            ]
        )
        if certificate.get("unknown_reason"):
            lines.append(f"- UNKNOWN reason: `{certificate['unknown_reason']}`")
        lines.extend(["", f"> {diagnostic['scope_note']}", ""])
    return lines

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
    lines.extend(render_skew_markdown(report))
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
