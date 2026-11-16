"""Verify declarative parent/child join contracts against physical shards.

A relation contract answers one offline question: given a child row and a
declared execution mode, can the referenced parent row be obtained from the
physical shards that the topology declares?  Two execution modes are
supported:

``local_join``
    The child row and its unique parent row must live in the same shard, and
    the target topology must still route both rows to the same shard.

``replicated_parent``
    A content-identical copy of the parent row may live in every shard that
    holds a referencing child row.  Every replica is compared with the
    canonical business-field fingerprint of the logical parent row, and the
    shard manifest must provide evidence for the file that actually holds the
    replica.  A same-key row with different content is never treated as the
    same parent entity.

This module is read-only.  It never moves, rewrites or queries a datastore.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any, Mapping

from .canonical import fingerprint, normalize_value
from .errors import ROUTE_UNKNOWN_TABLE, ShardRouteError
from .models import (
    Finding,
    LogicalRow,
    PhysicalRow,
    RelationContract,
    ScanResult,
    Topology,
    make_finding,
)
from .router import Router

RELATION_REFERENCE_MISSING = "RELATION_REFERENCE_MISSING"
RELATION_PARENT_KEY_MISSING = "RELATION_PARENT_KEY_MISSING"
RELATION_PARENT_MISSING = "RELATION_PARENT_MISSING"
RELATION_PARENT_DUPLICATE = "RELATION_PARENT_DUPLICATE"
RELATION_PARENT_CONFLICT = "RELATION_PARENT_CONFLICT"
RELATION_CHILD_COPY_MISSING = "RELATION_CHILD_COPY_MISSING"
RELATION_PARENT_COPY_MISSING = "RELATION_PARENT_COPY_MISSING"
RELATION_SHARD_SPLIT = "RELATION_SHARD_SPLIT"
RELATION_PARENT_REPLICA_MISSING = "RELATION_PARENT_REPLICA_MISSING"
RELATION_PARENT_REPLICA_DRIFT = "RELATION_PARENT_REPLICA_DRIFT"
RELATION_MANIFEST_EVIDENCE_MISSING = "RELATION_MANIFEST_EVIDENCE_MISSING"
RELATION_TARGET_SPLIT = "RELATION_TARGET_SPLIT"
RELATION_TARGET_PARENT_MISSING = "RELATION_TARGET_PARENT_MISSING"
RELATION_TARGET_PARENT_DRIFT = "RELATION_TARGET_PARENT_DRIFT"
RELATION_TARGET_TABLE_MISSING = "RELATION_TARGET_TABLE_MISSING"
RELATION_ROUTE_ERROR = "RELATION_ROUTE_ERROR"

RELATION_SEVERITY = {
    RELATION_REFERENCE_MISSING: "critical",
    RELATION_PARENT_KEY_MISSING: "high",
    RELATION_PARENT_MISSING: "critical",
    RELATION_PARENT_DUPLICATE: "critical",
    RELATION_PARENT_CONFLICT: "critical",
    RELATION_CHILD_COPY_MISSING: "critical",
    RELATION_PARENT_COPY_MISSING: "critical",
    RELATION_SHARD_SPLIT: "critical",
    RELATION_PARENT_REPLICA_MISSING: "critical",
    RELATION_PARENT_REPLICA_DRIFT: "critical",
    RELATION_MANIFEST_EVIDENCE_MISSING: "high",
    RELATION_TARGET_SPLIT: "critical",
    RELATION_TARGET_PARENT_MISSING: "critical",
    RELATION_TARGET_PARENT_DRIFT: "critical",
    RELATION_TARGET_TABLE_MISSING: "critical",
    RELATION_ROUTE_ERROR: "critical",
}


@dataclass
class RelationEvaluation:
    """Serializable relation report plus the findings for every phase.

    ``allowed_replicas`` lists ``(table, row_id, shard)`` parent copies that a
    ``replicated_parent`` contract has proven to be content-identical to the
    logical parent and backed by manifest evidence.  The single-table
    placement audit uses it to avoid mislabelling those copies as duplicates;
    every other extra placement is still reported normally.
    """

    report: dict[str, Any]
    findings: list[Finding]
    allowed_replicas: frozenset[tuple[str, str, str]] = frozenset()


def business_fingerprint(values: Mapping[str, Any]) -> str:
    """Fingerprint the payload of a row, ignoring its physical ``row_id``."""
    return fingerprint({key: value for key, value in values.items() if key != "row_id"})


def _normalized_key(values: Mapping[str, Any], field: str) -> str | None:
    if field not in values:
        return None
    try:
        key = normalize_value(values[field])
    except ShardRouteError:
        return None
    return key or None


def _place_index(rows: list[PhysicalRow]) -> dict[str, list[PhysicalRow]]:
    grouped: dict[str, list[PhysicalRow]] = defaultdict(list)
    for row in rows:
        grouped[row.row_id].append(row)
    return {
        row_id: sorted(places, key=lambda item: (item.shard, item.source, item.line_no))
        for row_id, places in grouped.items()
    }


def _key_index(rows: list[PhysicalRow], field: str) -> dict[str, list[PhysicalRow]]:
    grouped: dict[str, list[PhysicalRow]] = defaultdict(list)
    for row in sorted(rows, key=lambda item: (item.shard, item.source, item.line_no)):
        key = _normalized_key(row.values, field)
        if key is not None:
            grouped[key].append(row)
    return grouped


def _logical_parent_index(rows: list[LogicalRow], field: str) -> dict[str, list[LogicalRow]]:
    grouped: dict[str, list[LogicalRow]] = defaultdict(list)
    for row in sorted(rows, key=lambda item: (item.row_id, item.source, item.line_no)):
        key = _normalized_key(row.values, field)
        if key is not None:
            grouped[key].append(row)
    return grouped


def _manifest_evidence(
    scan: ScanResult,
    file_index: Mapping[tuple[str, str, str], Any],
    row: PhysicalRow,
) -> dict[str, Any]:
    """Prove that a shard manifest vouches for the file holding a replica."""
    name = PurePosixPath(row.source).name
    manifest = scan.manifests.get((row.table, row.shard))
    scanned = file_index.get((row.table, row.shard, row.source))
    evidence: dict[str, Any] = {
        "ok": False,
        "reason": "",
        "manifest": manifest.relative_path if manifest is not None else "",
        "file": name,
        "path": row.source,
        "sha256": scanned.sha256 if scanned is not None else "",
    }
    if manifest is None:
        evidence["reason"] = "manifest_missing"
    elif manifest.table != row.table or manifest.shard != row.shard:
        evidence["reason"] = "manifest_identity"
    elif scanned is None:
        evidence["reason"] = "file_not_scanned"
    else:
        entry = next((item for item in manifest.files if item.name == name), None)
        if entry is None:
            evidence["reason"] = "file_not_listed"
        elif entry.sha256 != scanned.sha256:
            evidence["reason"] = "hash_mismatch"
        elif entry.rows != scanned.row_count:
            evidence["reason"] = "row_count_mismatch"
        else:
            on_disk = sum(
                item.row_count
                for item in scan.files
                if item.table == row.table and item.shard == row.shard
            )
            if manifest.row_count != on_disk:
                evidence["reason"] = "manifest_row_count"
            else:
                evidence["ok"] = True
                evidence["reason"] = "verified"
    return evidence


def _replica_detail(
    row: PhysicalRow,
    canonical: str,
    parent_row_id: str,
    scan: ScanResult,
    file_index: Mapping[tuple[str, str, str], Any],
    *,
    with_evidence: bool,
) -> dict[str, Any]:
    stored = business_fingerprint(row.values)
    same_payload = stored == canonical
    same_identity = row.row_id == parent_row_id
    detail: dict[str, Any] = {
        "row_id": row.row_id,
        "shard": row.shard,
        "path": row.source,
        "line": row.line_no,
        "business_fingerprint": stored,
        "matches_logical_parent": same_payload and same_identity,
        "same_row_id": same_identity,
    }
    if with_evidence:
        detail["manifest_evidence"] = _manifest_evidence(scan, file_index, row)
    return detail


def _finding(
    code: str,
    contract: RelationContract,
    child: LogicalRow,
    phase: str,
    message: str,
    *,
    shard: str = "",
    detail: dict[str, Any],
) -> Finding:
    return make_finding(
        code,
        RELATION_SEVERITY[code],
        contract.child_table,
        message,
        shard=shard,
        row_id=child.row_id,
        detail={
            "phase": phase,
            "relation": contract.name,
            "mode": contract.mode,
            "parent_table": contract.parent_table,
            "parent_key": contract.parent_key,
            "child_key": contract.child_key,
            **detail,
        },
    )


def _record(
    codes: list[str],
    findings: list[Finding],
    code: str,
    contract: RelationContract,
    child: LogicalRow,
    phase: str,
    message: str,
    *,
    shard: str = "",
    detail: dict[str, Any],
) -> None:
    if code not in codes:
        codes.append(code)
    findings.append(_finding(code, contract, child, phase, message, shard=shard, detail=detail))


def _route_rows(
    router: Router,
    contract: RelationContract,
    child: LogicalRow,
    parent: LogicalRow,
) -> tuple[str, str, list[dict[str, Any]]]:
    """Route both rows, returning their target shards and any routing issues."""
    expected_child = expected_parent = ""
    issues: list[dict[str, Any]] = []
    for table, row, label in (
        (contract.child_table, child, "child"),
        (contract.parent_table, parent, "parent"),
    ):
        try:
            trace = router.route_values(table, row.values)
        except ShardRouteError as exc:
            issues.append({"label": label, "table": table, "error": exc.as_dict()})
            continue
        if trace.dropped or not trace.target_shard:
            issues.append(
                {"label": label, "table": table, "trace": trace.as_dict()}
            )
            continue
        if label == "child":
            expected_child = trace.target_shard
        else:
            expected_parent = trace.target_shard
    return expected_child, expected_parent, issues


def _route_evidence(
    router: Router,
    contract: RelationContract,
    child: LogicalRow,
    parent: LogicalRow,
    codes: list[str],
    findings: list[Finding],
    phase: str,
) -> tuple[str, str]:
    """Route both rows and record a finding when either cannot be placed."""
    expected_child, expected_parent, issues = _route_rows(
        router, contract, child, parent
    )
    for issue in issues:
        label, table = issue["label"], issue["table"]
        error = issue.get("error")
        missing_table = (
            isinstance(error, dict)
            and error.get("code") == ROUTE_UNKNOWN_TABLE
        )
        code = (
            RELATION_TARGET_TABLE_MISSING
            if phase == "new" and missing_table
            else RELATION_ROUTE_ERROR
        )
        message = (
            f"{label} table is missing from the target topology"
            if code == RELATION_TARGET_TABLE_MISSING
            else f"{label} row cannot be placed under the {phase} topology"
        )
        _record(
            codes, findings, code, contract, child, phase, message,
            detail={"table": table, **issue},
        )
    return expected_child, expected_parent


def _key_conflicts(
    parent_physical: list[PhysicalRow],
    parent_row_id: str,
    shards: set[str],
) -> list[dict[str, Any]]:
    """Physical parent rows under the same key that are a different entity.

    A row with a different ``row_id`` is a stale or unrelated entity that
    merely shares the reference value; it must never be accepted as the
    declared parent.  A same ``row_id`` row with a different payload is a
    drifted replica and is reported by the replica checks instead.
    """
    conflicts: list[dict[str, Any]] = []
    for row in parent_physical:
        if row.shard not in shards or row.row_id == parent_row_id:
            continue
        conflicts.append(
            {
                "row_id": row.row_id,
                "shard": row.shard,
                "path": row.source,
                "line": row.line_no,
                "business_fingerprint": business_fingerprint(row.values),
            }
        )
    return conflicts


def _old_side(
    contract: RelationContract,
    child: LogicalRow,
    parent: LogicalRow,
    canonical: str,
    child_places: list[PhysicalRow],
    parent_places: list[PhysicalRow],
    router: Router,
    scan: ScanResult,
    file_index: Mapping[tuple[str, str, str], Any],
) -> tuple[dict[str, Any], list[Finding]]:
    codes: list[str] = []
    findings: list[Finding] = []
    expected_child, expected_parent = _route_evidence(
        router, contract, child, parent, codes, findings, "old"
    )
    child_shards = sorted({place.shard for place in child_places})
    with_evidence = contract.mode == "replicated_parent"
    replicas = [
        _replica_detail(
            place, canonical, parent.row_id, scan, file_index,
            with_evidence=with_evidence,
        )
        for place in parent_places
    ]
    conflicts = _key_conflicts(parent_places, parent.row_id, set(child_shards))
    if conflicts:
        _record(
            codes, findings, RELATION_PARENT_CONFLICT, contract, child, "old",
            "a same-key physical parent row in the child shard is a different entity",
            shard=conflicts[0]["shard"],
            detail={
                "parent_row_id": parent.row_id,
                "canonical_fingerprint": canonical,
                "conflicts": conflicts,
            },
        )
    if not child_places:
        _record(
            codes, findings, RELATION_CHILD_COPY_MISSING, contract, child, "old",
            "child row is absent from every physical shard",
            detail={
                "child_shards": [],
                "reference": _normalized_key(child.values, contract.child_key),
            },
        )
    elif expected_child and not any(place.shard == expected_child for place in child_places):
        _record(
            codes, findings, RELATION_CHILD_COPY_MISSING, contract, child, "old",
            f"child row is absent from its routed shard {expected_child}",
            shard=expected_child,
            detail={"expected_shard": expected_child, "child_shards": child_shards},
        )
    if contract.mode == "local_join":
        # Old side is judged by physical placement: every shard that holds the
        # child must also hold the unique parent.  A parent copy anywhere else
        # does not make the join local, and is reported as a placement defect
        # by the single-table audit when it is undeclared.
        if not parent_places:
            _record(
                codes, findings, RELATION_PARENT_COPY_MISSING, contract, child, "old",
                "parent row has no physical copy in any shard",
                detail={"parent_row_id": parent.row_id, "child_shards": child_shards},
            )
        for shard in child_shards:
            in_shard = [item for item in replicas if item["shard"] == shard]
            if not in_shard:
                _record(
                    codes, findings, RELATION_SHARD_SPLIT, contract, child, "old",
                    f"local_join child shard {shard} has no parent copy",
                    shard=shard,
                    detail={
                        "child_shard": shard,
                        "parent_row_id": parent.row_id,
                        "parent_shards": sorted({item["shard"] for item in replicas}),
                    },
                )
            elif not any(item["matches_logical_parent"] for item in in_shard):
                _record(
                    codes, findings, RELATION_PARENT_REPLICA_DRIFT, contract, child, "old",
                    f"parent copy in child shard {shard} differs from the logical parent",
                    shard=shard,
                    detail={
                        "parent_row_id": parent.row_id,
                        "canonical_fingerprint": canonical,
                        "replicas": in_shard,
                    },
                )
    else:
        if not parent_places:
            _record(
                codes, findings, RELATION_PARENT_REPLICA_MISSING, contract, child, "old",
                "parent row has no physical replica in any shard",
                detail={"parent_row_id": parent.row_id, "child_shards": child_shards},
            )
        for shard in child_shards:
            in_shard = [item for item in replicas if item["shard"] == shard]
            if not in_shard:
                _record(
                    codes, findings, RELATION_PARENT_REPLICA_MISSING, contract, child, "old",
                    f"replicated_parent has no replica in child shard {shard}",
                    shard=shard,
                    detail={
                        "child_shard": shard,
                        "parent_row_id": parent.row_id,
                        "parent_shards": sorted({item["shard"] for item in replicas}),
                    },
                )
            elif not any(item["matches_logical_parent"] for item in in_shard):
                _record(
                    codes, findings, RELATION_PARENT_REPLICA_DRIFT, contract, child, "old",
                    f"replica in child shard {shard} is not the same parent entity",
                    shard=shard,
                    detail={
                        "parent_row_id": parent.row_id,
                        "canonical_fingerprint": canonical,
                        "replicas": in_shard,
                    },
                )
        drifted = [item for item in replicas if not item["matches_logical_parent"]]
        if drifted:
            _record(
                codes, findings, RELATION_PARENT_REPLICA_DRIFT, contract, child, "old",
                "one or more parent replicas differ from the logical parent payload",
                shard=drifted[0]["shard"],
                detail={
                    "parent_row_id": parent.row_id,
                    "canonical_fingerprint": canonical,
                    "replicas": drifted,
                },
            )
        unproven = [
            item
            for item in replicas
            if item["matches_logical_parent"] and not item["manifest_evidence"]["ok"]
        ]
        if unproven:
            _record(
                codes, findings, RELATION_MANIFEST_EVIDENCE_MISSING, contract, child, "old",
                "parent replica lacks a matching shard manifest entry",
                shard=unproven[0]["shard"],
                detail={
                    "parent_row_id": parent.row_id,
                    "canonical_fingerprint": canonical,
                    "replicas": unproven,
                },
            )
    return {
        "status": "blocked" if codes else "satisfied",
        "codes": codes,
        "child_routed_shard": expected_child,
        "parent_routed_shard": expected_parent,
        "child_shards": child_shards,
        "parent_shards": sorted({item["shard"] for item in replicas}),
        "parent_business_fingerprint": canonical,
        "replicas": replicas,
    }, findings


def _target_side(
    contract: RelationContract,
    child: LogicalRow,
    parent: LogicalRow,
    canonical: str,
    replicas: list[dict[str, Any]],
    router: Router,
    scan: ScanResult,
    file_index: Mapping[tuple[str, str, str], Any],
    physical_parent_by_key: Mapping[str, list[PhysicalRow]],
) -> tuple[dict[str, Any], list[Finding]]:
    codes: list[str] = []
    findings: list[Finding] = []
    expected_child, expected_parent = _route_evidence(
        router, contract, child, parent, codes, findings, "new"
    )
    reference = _normalized_key(child.values, contract.child_key) or ""
    target_replicas = [
        _replica_detail(
            item, canonical, parent.row_id, scan, file_index, with_evidence=True
        )
        for item in physical_parent_by_key.get(reference, [])
        if expected_child and item.shard == expected_child
    ]
    if not expected_child or not expected_parent:
        return {
            "status": "blocked" if codes else "satisfied",
            "codes": codes,
            "child_target_shard": expected_child,
            "parent_target_shard": expected_parent,
            "target_replica": target_replicas,
        }, findings
    split = expected_child != expected_parent
    if contract.mode == "local_join" and split:
        _record(
            codes, findings, RELATION_TARGET_SPLIT, contract, child, "new",
            "target routing splits the parent and child of a local_join",
            shard=expected_child,
            detail={
                "child_shard": expected_child,
                "parent_shard": expected_parent,
                "parent_row_id": parent.row_id,
            },
        )
    elif contract.mode == "replicated_parent" and split:
        matching = [item for item in target_replicas if item["matches_logical_parent"]]
        if not target_replicas:
            _record(
                codes, findings, RELATION_TARGET_PARENT_MISSING, contract, child, "new",
                f"target child shard {expected_child} has no parent replica",
                shard=expected_child,
                detail={
                    "child_shard": expected_child,
                    "parent_shard": expected_parent,
                    "parent_row_id": parent.row_id,
                    "parent_shards": sorted({item["shard"] for item in replicas}),
                },
            )
        elif not matching:
            _record(
                codes, findings, RELATION_TARGET_PARENT_DRIFT, contract, child, "new",
                f"target child shard {expected_child} only holds a divergent replica",
                shard=expected_child,
                detail={
                    "child_shard": expected_child,
                    "parent_shard": expected_parent,
                    "parent_row_id": parent.row_id,
                    "canonical_fingerprint": canonical,
                    "replicas": target_replicas,
                },
            )
        else:
            unproven = [item for item in matching if not item["manifest_evidence"]["ok"]]
            if unproven:
                _record(
                    codes, findings, RELATION_MANIFEST_EVIDENCE_MISSING, contract, child, "new",
                    f"target replica in {expected_child} lacks manifest evidence",
                    shard=expected_child,
                    detail={
                        "parent_row_id": parent.row_id,
                        "canonical_fingerprint": canonical,
                        "replicas": unproven,
                    },
                )
    elif not split:
        # The target keeps both rows together.  The existing placement in that
        # shard must still hold a content-consistent copy of the parent.
        matching = [item for item in target_replicas if item["matches_logical_parent"]]
        if not target_replicas:
            _record(
                codes, findings, RELATION_TARGET_PARENT_MISSING, contract, child, "new",
                f"target child shard {expected_child} has no parent replica",
                shard=expected_child,
                detail={
                    "child_shard": expected_child,
                    "parent_shard": expected_parent,
                    "parent_row_id": parent.row_id,
                    "parent_shards": sorted({item["shard"] for item in replicas}),
                },
            )
        elif not matching:
            _record(
                codes, findings, RELATION_TARGET_PARENT_DRIFT, contract, child, "new",
                f"target child shard {expected_child} only holds a divergent replica",
                shard=expected_child,
                detail={
                    "child_shard": expected_child,
                    "parent_shard": expected_parent,
                    "parent_row_id": parent.row_id,
                    "canonical_fingerprint": canonical,
                    "replicas": target_replicas,
                },
            )
        elif contract.mode == "replicated_parent":
            unproven = [item for item in matching if not item["manifest_evidence"]["ok"]]
            if unproven:
                _record(
                    codes, findings, RELATION_MANIFEST_EVIDENCE_MISSING, contract, child, "new",
                    f"target replica in {expected_child} lacks manifest evidence",
                    shard=expected_child,
                    detail={
                        "parent_row_id": parent.row_id,
                        "canonical_fingerprint": canonical,
                        "replicas": unproven,
                    },
                )
    return {
        "status": "blocked" if codes else "satisfied",
        "codes": codes,
        "child_target_shard": expected_child,
        "parent_target_shard": expected_parent,
        "target_replica": target_replicas,
    }, findings


def _contract_dict(contract: RelationContract) -> dict[str, Any]:
    return contract.as_dict()


def evaluate_relations(
    old: Topology,
    scan: ScanResult,
    target: Topology | None = None,
) -> RelationEvaluation | None:
    """Evaluate every contract declared by ``old``.

    Returns ``None`` when the old topology declares no relation, in which case
    callers keep the existing single-table audit/report unchanged.
    """
    if not old.relations:
        return None
    file_index = {
        (item.table, item.shard, item.relative_path): item for item in scan.files
    }
    target_contracts = {item.name: item for item in (target.relations if target else ())}
    old_router = Router(old)
    evaluations: list[dict[str, Any]] = []
    findings: list[Finding] = []
    relation_summaries: list[dict[str, Any]] = []
    for contract in sorted(old.relations, key=lambda item: item.name):
        parent_rows = scan.logical_rows.get(contract.parent_table, [])
        child_rows = scan.logical_rows.get(contract.child_table, [])
        parent_by_key = _logical_parent_index(parent_rows, contract.parent_key)
        physical_parent_by_key = _key_index(
            scan.physical_rows.get(contract.parent_table, []), contract.parent_key
        )
        child_places = _place_index(scan.physical_rows.get(contract.child_table, []))
        new_contract = target_contracts.get(contract.name, contract)
        target_router = Router(target) if target is not None else None
        for parent_row in parent_rows:
            if _normalized_key(parent_row.values, contract.parent_key) is None:
                findings.append(
                    make_finding(
                        RELATION_PARENT_KEY_MISSING,
                        RELATION_SEVERITY[RELATION_PARENT_KEY_MISSING],
                        contract.parent_table,
                        f"{contract.parent_table} row has no usable "
                        f"{contract.parent_key!r} identity",
                        row_id=parent_row.row_id,
                        detail={
                            "phase": "old",
                            "relation": contract.name,
                            "mode": contract.mode,
                            "parent_table": contract.parent_table,
                            "parent_key": contract.parent_key,
                            "child_key": contract.child_key,
                            "parent_row_id": parent_row.row_id,
                            "path": parent_row.source,
                            "line": parent_row.line_no,
                        },
                    )
                )
        old_results: list[dict[str, Any]] = []
        new_results: list[dict[str, Any]] = []
        for child in sorted(
            child_rows, key=lambda item: (item.row_id, item.source, item.line_no)
        ):
            reference = _normalized_key(child.values, contract.child_key)
            candidates = parent_by_key.get(reference, []) if reference else []
            parent = candidates[0] if len(candidates) == 1 else None
            canonical = business_fingerprint(parent.values) if parent else ""
            child_physical = child_places.get(child.row_id, [])
            parent_physical = physical_parent_by_key.get(reference or "", [])
            old_result: dict[str, Any] | None = None
            new_result: dict[str, Any] | None = None
            old_codes: list[str] = []
            new_codes: list[str] = []
            if not reference:
                _record(
                    old_codes, findings, RELATION_REFERENCE_MISSING, contract, child, "old",
                    f"child row has no usable reference value in {contract.child_key!r}",
                    detail={"path": child.source, "line": child.line_no},
                )
                new_codes.append(RELATION_REFERENCE_MISSING)
            elif not candidates:
                _record(
                    old_codes, findings, RELATION_PARENT_MISSING, contract, child, "old",
                    f"no {contract.parent_table} row has "
                    f"{contract.parent_key}={reference!r}",
                    detail={
                        "reference": reference,
                        "path": child.source,
                        "line": child.line_no,
                        "parent_shards": sorted({item.shard for item in parent_physical}),
                    },
                )
                new_codes.append(RELATION_PARENT_MISSING)
            elif len(candidates) > 1:
                _record(
                    old_codes, findings, RELATION_PARENT_DUPLICATE, contract, child, "old",
                    f"{len(candidates)} {contract.parent_table} rows share "
                    f"{contract.parent_key}={reference!r}",
                    detail={
                        "reference": reference,
                        "parent_row_ids": [item.row_id for item in candidates],
                    },
                )
                new_codes.append(RELATION_PARENT_DUPLICATE)
            elif parent is not None:
                old_result, old_findings = _old_side(
                    contract, child, parent, canonical, child_physical,
                    parent_physical, old_router, scan, file_index,
                )
                findings.extend(old_findings)
                old_codes = old_result["codes"]
                if target is not None and target_router is not None:
                    new_result, new_findings = _target_side(
                        new_contract, child, parent, canonical,
                        old_result["replicas"], target_router, scan, file_index,
                        physical_parent_by_key,
                    )
                    findings.extend(new_findings)
                    new_codes = new_result["codes"]
            if old_result is None:
                old_result = {
                    "status": "blocked",
                    "codes": old_codes,
                    "child_routed_shard": "",
                    "parent_routed_shard": "",
                    "child_shards": sorted({item.shard for item in child_physical}),
                    "parent_shards": sorted({item.shard for item in parent_physical}),
                    "parent_business_fingerprint": canonical,
                    "replicas": [
                        _replica_detail(
                            item, canonical,
                            parent.row_id if parent else "",
                            scan, file_index,
                            with_evidence=contract.mode == "replicated_parent",
                        )
                        for item in parent_physical
                    ],
                }
            old_result.setdefault("parent_row_id", parent.row_id if parent else "")
            old_result.setdefault("reference", reference or "")
            if target is not None and target_router is not None and new_result is None:
                new_result = {
                    "status": "blocked",
                    "codes": new_codes,
                    "child_target_shard": "",
                    "parent_target_shard": "",
                    "target_replica": [],
                }
            if new_result is not None:
                new_result.setdefault("parent_row_id", parent.row_id if parent else "")
                new_result.setdefault("reference", reference or "")
            evaluation: dict[str, Any] = {
                "relation": contract.name,
                "mode": contract.mode,
                "parent_table": contract.parent_table,
                "parent_key": contract.parent_key,
                "child_table": contract.child_table,
                "child_key": contract.child_key,
                "child_row_id": child.row_id,
                "child_source": child.source,
                "parent_row_id": parent.row_id if parent else "",
                "reference": reference or "",
                "old": old_result,
            }
            if new_result is not None:
                evaluation["new"] = new_result
            evaluations.append(evaluation)
            old_results.append(old_result)
            if new_result is not None:
                new_results.append(new_result)
        relation_summaries.append(
            {
                **_contract_dict(contract),
                "old": {
                    "status": "fail" if any(item["status"] == "blocked" for item in old_results) else "pass",
                    "evaluations": len(old_results),
                    "blocked": sum(1 for item in old_results if item["status"] == "blocked"),
                },
                "new": (
                    {
                        "status": (
                            "fail"
                            if any(item["status"] == "blocked" for item in new_results)
                            else "pass"
                        ),
                        "evaluations": len(new_results),
                        "blocked": sum(
                            1 for item in new_results if item["status"] == "blocked"
                        ),
                    }
                    if target is not None
                    else {"status": "not_evaluated", "evaluations": 0, "blocked": 0}
                ),
            }
        )
    unique = {item.finding_id: item for item in findings}
    ordered = sorted(unique.values(), key=lambda item: item.sort_key())
    # For replicated_parent, a content-identical copy with manifest evidence is
    # a legitimate replica wherever it lives: the contract's guarantee is about
    # content consistency, not placement.  Any copy that differs is still a
    # conflict/drift finding, and at most one copy per shard is excused by the
    # placement audit below.
    allowed_shards: set[tuple[str, str, str]] = set()
    for evaluation in evaluations:
        if evaluation["mode"] != "replicated_parent":
            continue
        for phase in ("old", "new"):
            side = evaluation.get(phase)
            if not side:
                continue
            for replica in side.get("replicas", []) + side.get("target_replica", []):
                if replica["matches_logical_parent"] and replica["manifest_evidence"]["ok"]:
                    allowed_shards.add(
                        (evaluation["parent_table"], replica["row_id"], replica["shard"])
                    )
    old_findings = [item for item in ordered if item.detail.get("phase") == "old"]
    new_findings = [item for item in ordered if item.detail.get("phase") == "new"]
    old_blocked = sum(
        1 for item in evaluations if item["old"]["status"] == "blocked"
    )
    new_blocked = sum(
        1 for item in evaluations if item.get("new", {}).get("status") == "blocked"
    )
    report = {
        "declared": [_contract_dict(item) for item in sorted(old.relations, key=lambda rel: rel.name)],
        "target_declared": (
            [_contract_dict(item) for item in sorted(target.relations, key=lambda rel: rel.name)]
            if target is not None
            else []
        ),
        "summary": {
            "contracts": len(old.relations),
            "target_contracts": len(target.relations) if target is not None else 0,
            "evaluations": len(evaluations),
            "old_blocked": old_blocked,
            "new_blocked": new_blocked,
            "old_status": "fail" if old_blocked or old_findings else "pass",
            "new_status": (
                "not_evaluated"
                if target is None
                else ("fail" if new_blocked or new_findings else "pass")
            ),
            "old_findings": len(old_findings),
            "new_findings": len(new_findings),
            "findings": len(ordered),
        },
        "relations": relation_summaries,
        "evaluations": evaluations,
        "findings": [item.as_dict() for item in ordered],
    }
    return RelationEvaluation(
        report=report,
        findings=ordered,
        allowed_replicas=frozenset(allowed_shards),
    )
