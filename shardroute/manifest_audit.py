"""Manifest-to-disk consistency checks for physical shard directories."""

from __future__ import annotations

from typing import Any

from .models import ScanResult, Topology, make_finding

def _audit_manifests(topology: Topology, scan: ScanResult) -> list[Any]:
    findings = []
    files = {(item.table, item.shard, item.name): item for item in scan.files}
    error_dirs = {(item["table"], item["shard"]) for item in scan.manifest_errors}
    for error in scan.manifest_errors:
        findings.append(
            make_finding(
                "MANIFEST_PARSE_ERROR",
                "high",
                error["table"],
                "shard manifest could not be parsed",
                shard=error["shard"],
                detail={"path": error["path"], "error": error["error"]},
            )
        )
    dirs = sorted(set(scan.physical_dirs))
    for table, shard in dirs:
        if table not in topology.tables:
            findings.append(
                make_finding(
                    "UNKNOWN_TABLE_DIR",
                    "medium",
                    table,
                    "physical table directory is not declared in the topology",
                    shard=shard,
                )
            )
            continue
        manifest = scan.manifests.get((table, shard))
        if manifest is None:
            if (table, shard) not in error_dirs:
                findings.append(
                    make_finding(
                        "MANIFEST_MISSING",
                        "medium",
                        table,
                        "shard directory has no _manifest.json",
                        shard=shard,
                    )
                )
            continue
        if manifest.table != table or manifest.shard != shard:
            findings.append(
                make_finding(
                    "MANIFEST_IDENTITY",
                    "high",
                    table,
                    "manifest table/shard identity does not match its directory",
                    shard=shard,
                    detail={
                        "manifest_table": manifest.table,
                        "manifest_shard": manifest.shard,
                        "path": manifest.relative_path,
                    },
                )
            )
        listed: set[str] = set()
        for entry in manifest.files:
            if entry.name in listed:
                findings.append(
                    make_finding(
                        "MANIFEST_DUPLICATE_FILE",
                        "high",
                        table,
                        f"manifest lists {entry.name!r} more than once",
                        shard=shard,
                        detail={"file": entry.name},
                    )
                )
                continue
            listed.add(entry.name)
            actual = files.get((table, shard, entry.name))
            if actual is None:
                findings.append(
                    make_finding(
                        "MANIFEST_FILE_MISSING",
                        "high",
                        table,
                        f"manifest lists missing file {entry.name!r}",
                        shard=shard,
                        detail={"file": entry.name},
                    )
                )
                continue
            if entry.sha256 != actual.sha256:
                findings.append(
                    make_finding(
                        "MANIFEST_HASH",
                        "high",
                        table,
                        f"manifest checksum differs for {entry.name!r}",
                        shard=shard,
                        detail={
                            "file": entry.name,
                            "declared": entry.sha256,
                            "actual": actual.sha256,
                        },
                    )
                )
            if entry.rows != actual.row_count:
                findings.append(
                    make_finding(
                        "MANIFEST_FILE_ROWS",
                        "high",
                        table,
                        f"manifest row count differs for {entry.name!r}",
                        shard=shard,
                        detail={"file": entry.name, "declared": entry.rows, "actual": actual.row_count},
                    )
                )
        for item in scan.files:
            if item.table == table and item.shard == shard and item.name not in listed:
                findings.append(
                    make_finding(
                        "ORPHAN_FILE",
                        "medium",
                        table,
                        f"data file {item.name!r} is not listed in the manifest",
                        shard=shard,
                        detail={"file": item.name, "path": item.relative_path, "rows": item.row_count},
                    )
                )
        actual_total = sum(
            item.row_count for item in scan.files if item.table == table and item.shard == shard
        )
        if manifest.row_count != actual_total:
            findings.append(
                make_finding(
                    "MANIFEST_ROW_COUNT",
                    "high",
                    table,
                    "manifest row_count differs from the rows on disk",
                    shard=shard,
                    detail={
                        "declared": manifest.row_count,
                        "actual": actual_total,
                        "path": manifest.relative_path,
                    },
                )
            )
    return findings
