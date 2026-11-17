"""Compare logical routes with physical placement and manifest contents."""

from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any

from .errors import ShardRouteError
from .models import ScanResult, Topology, make_finding
from .manifest_audit import _audit_manifests
from .router import Router
from .skew import DEFAULT_ALPHA, diagnose_hash_table

SEVERITY_ORDER = ("critical", "high", "medium", "low", "info")

def _placement_rows(scan: ScanResult) -> dict[str, dict[str, list[Any]]]:
    grouped: dict[str, dict[str, list[Any]]] = defaultdict(lambda: defaultdict(list))
    for table, rows in scan.physical_rows.items():
        for row in rows:
            grouped[table][row.row_id].append(row)
    return {table: dict(by_id) for table, by_id in grouped.items()}

def _audit_placements(topology: Topology, scan: ScanResult) -> list[Any]:
    router = Router(topology)
    findings = []
    placements = _placement_rows(scan)
    for table_name in topology.table_names():
        table = topology.table(table_name)
        logical = scan.logical_rows.get(table_name, [])
        logical_ids = {row.row_id for row in logical}
        physical = placements.get(table_name, {})
        for row in logical:
            try:
                trace = router.route_values(table_name, row.values)
            except ShardRouteError as exc:
                findings.append(
                    make_finding(
                        "ROUTE_ERROR",
                        "critical",
                        table_name,
                        f"logical row cannot be routed: {exc.message}",
                        row_id=row.row_id,
                        detail={"error_code": exc.code, "error": exc.as_dict()},
                    )
                )
                continue
            places = sorted(physical.get(row.row_id, []), key=lambda p: (p.shard, p.source, p.line_no))
            if trace.dropped:
                if places:
                    findings.append(
                        make_finding(
                            "DROPPED_ROW_PRESENT",
                            "high",
                            table_name,
                            "row routed to drop still exists in a physical shard",
                            shard=places[0].shard,
                            row_id=row.row_id,
                            detail={"placements": [p.source for p in places]},
                        )
                    )
                continue
            expected = trace.target_shard or ""
            if not places:
                findings.append(
                    make_finding(
                        "MISSING_ROW",
                        "critical",
                        table_name,
                        f"logical row is absent from physical shards; expected {expected}",
                        shard=expected,
                        row_id=row.row_id,
                        detail={"expected_shard": expected, "logical_source": row.source},
                    )
                )
            elif len(places) > 1:
                shards = sorted({place.shard for place in places})
                findings.append(
                    make_finding(
                        "DUPLICATE_ROW",
                        "critical",
                        table_name,
                        f"row appears {len(places)} times across {len(shards)} shard(s)",
                        shard=expected,
                        row_id=row.row_id,
                        detail={
                            "expected_shard": expected,
                            "shards": shards,
                            "placements": [
                                {"shard": p.shard, "path": p.source, "line": p.line_no} for p in places
                            ],
                        },
                    )
                )
            elif places[0].shard != expected:
                findings.append(
                    make_finding(
                        "MISPLACED_ROW",
                        "critical",
                        table_name,
                        f"row is in {places[0].shard} but routes to {expected}",
                        shard=places[0].shard,
                        row_id=row.row_id,
                        detail={
                            "expected_shard": expected,
                            "actual_shard": places[0].shard,
                            "path": places[0].source,
                            "line": places[0].line_no,
                            "trace": trace.as_dict(),
                        },
                    )
                )
        for row_id, places in sorted(physical.items()):
            if row_id not in logical_ids:
                for place in places:
                    findings.append(
                        make_finding(
                            "ORPHAN_ROW",
                            "medium",
                            table_name,
                            "physical row is not present in the logical snapshot",
                            shard=place.shard,
                            row_id=row_id,
                            detail={"path": place.source, "line": place.line_no},
                        )
                    )
        known = set(table.table_shards())
        for physical_table, shard in sorted(scan.physical_dirs):
            if physical_table == table_name and shard not in known:
                findings.append(
                    make_finding(
                        "UNEXPECTED_SHARD",
                        "medium",
                        table_name,
                        f"physical shard {shard!r} is not a routing target for this table",
                        shard=shard,
                    )
                )
    return findings

def run_hash_skew_diagnostics(
    topology: Topology,
    scan: ScanResult,
    alpha: float = DEFAULT_ALPHA,
    **skew_budget: Any,
) -> dict[str, dict[str, Any]]:
    """Per-table hash load-skew tail diagnostics, purely read-only.

    Only hash tables carry a full diagnostic; other tables get an explicit
    not-applicable stub so the evidence chain never silently omits a table.
    Key groups are rebuilt from logical rows through the same Router used by
    the placement audit, keeping normalization and bucket_map changes on the
    causal path into the tail probability and its significance decision.
    """
    router = Router(topology)
    diagnostics: dict[str, dict[str, Any]] = {}
    for table_name in topology.table_names():
        table = topology.table(table_name)
        diagnostics[table_name] = diagnose_hash_table(
            table,
            scan.logical_rows.get(table_name, []),
            router,
            alpha,
            **skew_budget,
        )
    return diagnostics


def run_audit(
    topology: Topology,
    scan: ScanResult,
    alpha: float = DEFAULT_ALPHA,
    **skew_budget: Any,
) -> dict[str, Any]:
    findings = _audit_placements(topology, scan) + _audit_manifests(topology, scan)
    unique = {finding.finding_id: finding for finding in findings}
    ordered = sorted(unique.values(), key=lambda finding: finding.sort_key())
    by_code = Counter(finding.code for finding in ordered)
    by_severity = Counter(finding.severity for finding in ordered)
    by_table: dict[str, dict[str, int]] = {}
    for table in topology.table_names():
        table_findings = [finding for finding in ordered if finding.table == table]
        detail = Counter(finding.code for finding in table_findings)
        by_table[table] = {
            "logical_rows": len(scan.logical_rows.get(table, [])),
            "physical_rows": len(scan.physical_rows.get(table, [])),
            "eligible_shards": topology.table(table).table_shards(),
            "findings": len(table_findings),
            "critical": sum(1 for finding in table_findings if finding.severity == "critical"),
            "by_code": dict(sorted(detail.items())),
        }
    critical = sum(1 for finding in ordered if finding.severity in {"critical", "high"})
    status = "fail" if critical else ("attention" if ordered else "pass")
    return {
        "status": status,
        "summary": {
            "total_findings": len(ordered),
            "critical_or_high": critical,
            "by_severity": {level: by_severity.get(level, 0) for level in SEVERITY_ORDER},
            "by_code": dict(sorted(by_code.items())),
        },
        "tables": by_table,
        "findings": [finding.as_dict() for finding in ordered],
        "hash_skew_diagnostics": run_hash_skew_diagnostics(
            topology, scan, alpha, **skew_budget
        ),
    }
