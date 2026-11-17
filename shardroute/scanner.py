"""Read logical snapshots, physical CSV/JSONL shard files and manifests."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from .canonical import file_sha256, fingerprint
from .config import NAME_RE
from .errors import (
    SCAN_CSV,
    SCAN_DUPLICATE_ID,
    SCAN_IO,
    SCAN_JSONL,
    SCAN_ROW_ID,
    ScanError,
)
from .models import (
    LogicalRow,
    ManifestEntry,
    ManifestRecord,
    PhysicalRow,
    ScanResult,
    ScannedFile,
)

ROW_ID = "row_id"
SUFFIXES = (".csv", ".jsonl")

def _row(raw: dict[str, Any], source: str, line_no: int, unique: bool, seen: set[str]) -> dict[str, Any]:
    value = raw.get(ROW_ID)
    if isinstance(value, bool) or not isinstance(value, (str, int, float)) or not str(value).strip():
        raise ScanError(SCAN_ROW_ID, "row_id must be a non-empty scalar", f"{source}:{line_no}")
    row_id = str(value).strip()
    if unique and row_id in seen:
        raise ScanError(
            SCAN_DUPLICATE_ID,
            f"duplicate logical row_id {row_id!r}",
            f"{source}:{line_no}",
            {"row_id": row_id},
        )
    seen.add(row_id)
    return {"row_id": row_id, "values": {**raw, ROW_ID: row_id}, "line_no": line_no}

def _csv_rows(path: Path, source: str, unique: bool) -> list[dict[str, Any]]:
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            if not reader.fieldnames or ROW_ID not in reader.fieldnames:
                raise ScanError(
                    SCAN_CSV,
                    f"CSV is missing the required {ROW_ID!r} column",
                    source,
                    {"columns": reader.fieldnames or []},
                )
            if len(set(reader.fieldnames)) != len(reader.fieldnames):
                raise ScanError(SCAN_CSV, "CSV contains duplicate column names", source)
            rows: list[dict[str, Any]] = []
            seen: set[str] = set()
            for line_no, raw in enumerate(reader, start=2):
                if None in raw or any(value is None for value in raw.values()):
                    raise ScanError(SCAN_CSV, "CSV row has a different column count", f"{source}:{line_no}")
                rows.append(_row(raw, source, line_no, unique, seen))
            return rows
    except ScanError:
        raise
    except (OSError, UnicodeError) as exc:
        raise ScanError(SCAN_IO, f"cannot read CSV: {exc}", source) from exc
    except csv.Error as exc:
        raise ScanError(SCAN_CSV, f"invalid CSV: {exc}", source) from exc

def _jsonl_rows(path: Path, source: str, unique: bool) -> list[dict[str, Any]]:
    try:
        rows: list[dict[str, Any]] = []
        seen: set[str] = set()
        with path.open("r", encoding="utf-8") as handle:
            for line_no, line in enumerate(handle, start=1):
                if not line.strip():
                    continue
                try:
                    raw = json.loads(line)
                except ValueError as exc:
                    raise ScanError(SCAN_JSONL, f"invalid JSONL row: {exc}", f"{source}:{line_no}") from exc
                if not isinstance(raw, dict):
                    raise ScanError(SCAN_JSONL, "each JSONL row must be an object", f"{source}:{line_no}")
                rows.append(_row(raw, source, line_no, unique, seen))
        return rows
    except ScanError:
        raise
    except (OSError, UnicodeError) as exc:
        raise ScanError(SCAN_IO, f"cannot read JSONL: {exc}", source) from exc

def _rows(path: Path, source: str, unique: bool) -> list[dict[str, Any]]:
    if path.suffix.lower() == ".csv":
        return _csv_rows(path, source, unique)
    if path.suffix.lower() == ".jsonl":
        return _jsonl_rows(path, source, unique)
    raise ScanError(SCAN_CSV, "only CSV and JSONL data files are supported", source)

def _snapshot(root: Path, table: str) -> Path:
    paths = [root / f"{table}{suffix}" for suffix in SUFFIXES if (root / f"{table}{suffix}").is_file()]
    if not paths:
        raise ScanError(SCAN_IO, "logical snapshot file is missing", str(root / f"{table}.csv"))
    if len(paths) > 1:
        raise ScanError(SCAN_IO, "logical snapshot is ambiguous: use CSV or JSONL, not both", str(root))
    return paths[0]

def load_logical(logical_dir: str | Path, tables: list[str]) -> tuple[dict[str, list[LogicalRow]], str]:
    root = Path(logical_dir)
    if not root.is_dir():
        raise ScanError(SCAN_IO, "logical snapshot directory does not exist", str(root))
    result: dict[str, list[LogicalRow]] = {}
    parts: list[dict[str, Any]] = []
    for table in sorted(tables):
        path = _snapshot(root, table)
        rows: list[LogicalRow] = []
        source = f"logical/{path.name}"
        for item in _rows(path, source, unique=True):
            rows.append(LogicalRow(table, item["row_id"], item["values"], source, item["line_no"]))
        result[table] = rows
        parts.append({"name": path.name, "sha256": file_sha256(path), "rows": len(rows)})
    return result, fingerprint({"files": parts})

def _manifest(path: Path, table: str, shard: str, source: str) -> ManifestRecord:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ScanError(SCAN_IO, f"invalid manifest JSON: {exc}", source) from exc
    if not isinstance(raw, dict) or not isinstance(raw.get("files"), list):
        raise ScanError(SCAN_IO, "manifest must contain a files list", source)
    entries: list[ManifestEntry] = []
    for index, item in enumerate(raw["files"]):
        if (
            not isinstance(item, dict)
            or not isinstance(item.get("name"), str)
            or isinstance(item.get("rows"), bool)
            or not isinstance(item.get("rows"), int)
            or not isinstance(item.get("sha256"), str)
        ):
            raise ScanError(SCAN_IO, f"manifest files[{index}] is malformed", source)
        entries.append(ManifestEntry(item["name"], item["rows"], item["sha256"]))
    declared_count = raw.get("row_count")
    if isinstance(declared_count, bool) or not isinstance(declared_count, int):
        raise ScanError(SCAN_IO, "manifest row_count must be an integer", source)
    declared_table = raw.get("table") if isinstance(raw.get("table"), str) else table
    declared_shard = raw.get("shard") if isinstance(raw.get("shard"), str) else shard
    generated_at = raw.get("generated_at") if isinstance(raw.get("generated_at"), str) else ""
    return ManifestRecord(
        declared_table,
        declared_shard,
        source,
        declared_count,
        tuple(entries),
        generated_at,
    )

def load_physical(
    shards_dir: str | Path, tables: list[str]
) -> tuple[
    dict[str, list[PhysicalRow]],
    list[ScannedFile],
    dict[tuple[str, str], ManifestRecord],
    list[dict[str, str]],
    list[tuple[str, str]],
    str,
]:
    root = Path(shards_dir)
    if not root.is_dir():
        raise ScanError(SCAN_IO, "physical shard directory does not exist", str(root))
    rows_by_table: dict[str, list[PhysicalRow]] = {table: [] for table in tables}
    files: list[ScannedFile] = []
    manifests: dict[tuple[str, str], ManifestRecord] = {}
    manifest_errors: list[dict[str, str]] = []
    manifest_parts: list[dict[str, str]] = []
    dirs: list[tuple[str, str]] = []
    for table_dir in sorted(root.iterdir(), key=lambda p: p.name):
        if not table_dir.is_dir() or not NAME_RE.match(table_dir.name):
            continue
        table = table_dir.name
        rows_by_table.setdefault(table, [])
        for shard_dir in sorted(table_dir.iterdir(), key=lambda p: p.name):
            if not shard_dir.is_dir() or not NAME_RE.match(shard_dir.name):
                continue
            shard = shard_dir.name
            dirs.append((table, shard))
            for path in sorted(shard_dir.iterdir(), key=lambda p: p.name):
                if path.name == "_manifest.json" and path.is_file():
                    relative = path.relative_to(root).as_posix()
                    manifest_parts.append({"path": relative, "sha256": file_sha256(path)})
                    try:
                        manifests[(table, shard)] = _manifest(path, table, shard, relative)
                    except ScanError as exc:
                        manifest_errors.append(
                            {"table": table, "shard": shard, "path": relative, "error": str(exc)}
                        )
                    continue
                if not path.is_file() or path.suffix.lower() not in SUFFIXES:
                    continue
                relative = path.relative_to(root).as_posix()
                digest = file_sha256(path)
                raw_rows = _rows(path, relative, unique=False)
                for item in raw_rows:
                    rows_by_table[table].append(
                        PhysicalRow(table, shard, item["row_id"], item["values"], relative, item["line_no"])
                    )
                files.append(ScannedFile(table, shard, path.name, relative, digest, len(raw_rows)))
    digest_parts = [{"path": item.relative_path, "sha256": item.sha256} for item in files]
    digest_parts = sorted(digest_parts + manifest_parts, key=lambda item: item["path"])
    return (
        rows_by_table,
        files,
        manifests,
        manifest_errors,
        dirs,
        fingerprint({"files": digest_parts}),
    )

def snapshot_tree(root: str | Path) -> dict[str, str]:
    """Map every file under ``root`` to its SHA-256, keyed by POSIX relative path.

    Used to prove that a read-only operation (for example building a minimal
    reproduction workspace) left the source tree byte-identical.
    """
    root = Path(root)
    if not root.is_dir():
        raise ScanError(SCAN_IO, "directory does not exist", str(root))
    result: dict[str, str] = {}
    for path in sorted(root.rglob("*")):
        if path.is_file():
            result[path.relative_to(root).as_posix()] = file_sha256(path)
    return result

def scan_workspace(
    logical_dir: str | Path,
    shards_dir: str | Path,
    table_names: list[str],
) -> ScanResult:
    logical_rows, logical_fp = load_logical(logical_dir, table_names)
    physical_rows, files, manifests, manifest_errors, dirs, physical_fp = load_physical(
        shards_dir, table_names
    )
    return ScanResult(
        logical_rows=logical_rows,
        physical_rows=physical_rows,
        files=files,
        manifests=manifests,
        manifest_errors=manifest_errors,
        physical_dirs=dirs,
        logical_fingerprint=logical_fp,
        physical_fingerprint=physical_fp,
    )
