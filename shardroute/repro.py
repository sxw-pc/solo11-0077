"""Minimal offline reproduction workspaces for verified audit findings.

Given one finding from a workspace placement audit and a reproduction
contract, shrink the workspace into an independent output directory that still
reproduces the finding under the real scanner + audit pipeline. The source
workspace is only read; every reduction candidate is written to a temporary
directory and confirmed by re-running :func:`scanner.scan_workspace` and
:func:`audit.run_audit`.

Two contracts are supported:

``exact_row``
    Preserve the exact finding instance: table, row_id, anomaly code and the
    original routing evidence (expected/actual shard, declared checksums...).
    Suited to handing one real record to a fixer.
``semantic_family``
    Preserve only table + anomaly code. When the source audit reports several
    findings of the same family, the instance yielding the smallest package is
    kept as a rule-level regression example and is labeled as such; it is
    never presented as the reproduction of a specific original record.

Manifest fields are rebuilt after row/file deletion so the package stays
self-consistent (no MANIFEST_HASH / MANIFEST_FILE_ROWS / MANIFEST_ROW_COUNT
side findings are manufactured) — except the causal fields of a
manifest-target finding (for example the stale declared SHA-256 of a
MANIFEST_HASH finding), which are preserved verbatim so the target defect
survives the rebuild.
"""

from __future__ import annotations

import csv
import json
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .audit import HIGH_SEVERITIES, finding_evidence, finding_signature, run_audit
from .canonical import file_sha256, fingerprint
from .config import load_topology
from .errors import (
    CLI_USAGE,
    REPRO_AMBIGUOUS,
    REPRO_NO_TARGET,
    REPRO_OUTPUT,
    REPRO_UNSUPPORTED,
    ShardRouteError,
)
from .models import Topology
from .scanner import scan_workspace, snapshot_tree

EXACT_ROW = "exact_row"
SEMANTIC_FAMILY = "semantic_family"
CONTRACT_MODES = (EXACT_ROW, SEMANTIC_FAMILY)

DEFAULT_BUDGET = 250
FAMILY_INSTANCE_LIMIT = 16

# Finding codes whose reproduction depends on the shard directory itself (or
# its manifest as a whole) rather than on individual rows or files.
_SHARD_CAUSAL_CODES = frozenset(
    {
        "MANIFEST_ROW_COUNT",
        "MANIFEST_IDENTITY",
        "MANIFEST_PARSE_ERROR",
        "MANIFEST_MISSING",
        "UNEXPECTED_SHARD",
    }
)

@dataclass(frozen=True)
class ReproContract:
    """Which finding to reproduce and at which fidelity."""

    mode: str
    table: str
    code: str
    row_id: str = ""

    def __post_init__(self) -> None:
        if self.mode not in CONTRACT_MODES:
            raise ShardRouteError(
                CLI_USAGE,
                f"unknown repro contract mode {self.mode!r}",
                details={"modes": list(CONTRACT_MODES)},
            )
        if not self.table or not self.code:
            raise ShardRouteError(CLI_USAGE, "repro contract requires a table and a finding code")

    def as_dict(self) -> dict[str, Any]:
        return {"mode": self.mode, "table": self.table, "code": self.code, "row_id": self.row_id}

@dataclass
class _Source:
    """Faithful in-memory model of the source workspace."""

    root: Path
    tables: list[str]
    topology_bytes: bytes
    logical_suffix: dict[str, str]
    logical_columns: dict[str, list[str] | None]
    logical_rows: dict[str, list[dict[str, Any]]]
    logical_by_id: dict[str, dict[str, dict[str, Any]]]
    file_rows: dict[tuple[str, str, str], list[dict[str, Any]]]
    file_columns: dict[tuple[str, str, str], list[str] | None]
    manifest_docs: dict[tuple[str, str], dict[str, Any]]
    manifest_raw: dict[tuple[str, str], bytes]
    manifest_broken: set[tuple[str, str]]
    audit: dict[str, Any]

@dataclass
class _State:
    """A reduction candidate: which logical rows and physical rows survive."""

    logical: dict[str, list[str]]
    files: dict[tuple[str, str, str], list[int]]

    def clone(self) -> "_State":
        return _State(
            {table: list(ids) for table, ids in self.logical.items()},
            {key: list(indices) for key, indices in self.files.items()},
        )

class _EvalCounter:
    """Budget bookkeeping: candidate evaluations are capped, structural ones are not."""

    def __init__(self, budget: int) -> None:
        self.budget = budget
        self.candidate_evals = 0
        self.structural_evals = 0

    def remaining(self) -> int:
        return self.budget - self.candidate_evals

def _csv_columns(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.reader(handle):
            return row
    return []

def _read_source(root: Path, topology: Topology, topology_bytes: bytes) -> _Source:
    tables = topology.table_names()
    scan = scan_workspace(root / "logical", root / "shards", tables)
    audit = run_audit(topology, scan)
    logical_suffix: dict[str, str] = {}
    logical_columns: dict[str, list[str] | None] = {}
    logical_rows: dict[str, list[dict[str, Any]]] = {}
    logical_by_id: dict[str, dict[str, dict[str, Any]]] = {}
    for table in tables:
        suffix = ".csv" if (root / "logical" / f"{table}.csv").is_file() else ".jsonl"
        logical_suffix[table] = suffix
        path = root / "logical" / f"{table}{suffix}"
        logical_columns[table] = _csv_columns(path) if suffix == ".csv" else None
        rows = [dict(row.values) for row in scan.logical_rows[table]]
        logical_rows[table] = rows
        logical_by_id[table] = {row["row_id"]: row for row in rows}
    file_rows: dict[tuple[str, str, str], list[dict[str, Any]]] = {
        (item.table, item.shard, item.name): [] for item in scan.files
    }
    for table_rows in scan.physical_rows.values():
        for row in table_rows:
            parts = row.source.split("/")
            key = (parts[0], parts[1], "/".join(parts[2:]))
            file_rows.setdefault(key, []).append(dict(row.values))
    file_columns: dict[tuple[str, str, str], list[str] | None] = {}
    for table, shard, name in file_rows:
        path = root / "shards" / table / shard / name
        file_columns[(table, shard, name)] = (
            _csv_columns(path) if name.lower().endswith(".csv") else None
        )
    manifest_docs: dict[tuple[str, str], dict[str, Any]] = {}
    manifest_raw: dict[tuple[str, str], bytes] = {}
    manifest_broken: set[tuple[str, str]] = set()
    error_dirs = {(item["table"], item["shard"]) for item in scan.manifest_errors}
    for table, shard in scan.physical_dirs:
        path = root / "shards" / table / shard / "_manifest.json"
        if not path.is_file():
            continue
        raw = path.read_bytes()
        manifest_raw[(table, shard)] = raw
        if (table, shard) in error_dirs:
            manifest_broken.add((table, shard))
        else:
            manifest_docs[(table, shard)] = json.loads(raw.decode("utf-8"))
    return _Source(
        root=root,
        tables=tables,
        topology_bytes=topology_bytes,
        logical_suffix=logical_suffix,
        logical_columns=logical_columns,
        logical_rows=logical_rows,
        logical_by_id=logical_by_id,
        file_rows=file_rows,
        file_columns=file_columns,
        manifest_docs=manifest_docs,
        manifest_raw=manifest_raw,
        manifest_broken=manifest_broken,
        audit=audit,
    )

# --- candidate writing -------------------------------------------------------

def _write_data_file(path: Path, columns: list[str] | None, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix.lower() == ".csv":
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=columns or [], lineterminator="\n")
            writer.writeheader()
            for row in rows:
                writer.writerow({column: row.get(column, "") for column in columns or []})
    else:
        with path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")

def _shard_rule(source: _Source, target: dict[str, Any], table: str, shard: str) -> str:
    """How to treat one shard's manifest: rebuild, copy raw, or keep absent."""
    if (table, shard) == (target["table"], target["shard"]):
        code = target["code"]
        if code == "MANIFEST_PARSE_ERROR":
            return "raw"
        if code == "MANIFEST_MISSING":
            return "absent"
        return "rebuild"
    if (table, shard) in source.manifest_broken:
        return "raw"
    return "rebuild"

def _rebuild_manifest(
    source: _Source,
    target: dict[str, Any],
    table: str,
    shard: str,
    written: dict[str, tuple[int, str]],
) -> dict[str, Any]:
    """Rebuild one manifest consistently with the reduced files on disk.

    Every field follows the reduced reality, except the causal fields of the
    target finding, which are pinned to their original (defective) values so
    the target defect survives while no new manifest defect is manufactured.
    """
    original = source.manifest_docs[(table, shard)]
    pin_sha256: set[str] = set()
    pin_rows: set[str] = set()
    pin_row_count = False
    keep_ghosts: set[str] = set()
    keep_duplicates = False
    pin_identity = False
    if (table, shard) == (target["table"], target["shard"]):
        detail = target.get("detail") or {}
        code = target["code"]
        if code == "MANIFEST_HASH":
            pin_sha256 = {str(detail.get("file", ""))}
        elif code == "MANIFEST_FILE_ROWS":
            pin_rows = {str(detail.get("file", ""))}
        elif code == "MANIFEST_ROW_COUNT":
            pin_row_count = True
        elif code == "MANIFEST_FILE_MISSING":
            keep_ghosts = {str(detail.get("file", ""))}
        elif code == "MANIFEST_DUPLICATE_FILE":
            keep_duplicates = True
        elif code == "MANIFEST_IDENTITY":
            pin_identity = True
    doc = {
        key: value
        for key, value in original.items()
        if key not in ("files", "row_count", "table", "shard")
    }
    doc["table"] = original.get("table", table) if pin_identity else table
    doc["shard"] = original.get("shard", shard) if pin_identity else shard
    entries: list[dict[str, Any]] = []
    seen: set[str] = set()
    for entry in original.get("files", []):
        if not isinstance(entry, dict) or not isinstance(entry.get("name"), str):
            continue
        name = entry["name"]
        if name in seen and not keep_duplicates:
            continue
        if name in written:
            row_count, digest = written[name]
            new_entry: dict[str, Any] = {"name": name, "rows": row_count, "sha256": digest}
            if name in pin_sha256:
                new_entry["sha256"] = entry.get("sha256", "")
            if name in pin_rows:
                new_entry["rows"] = entry.get("rows", 0)
            entries.append(new_entry)
            seen.add(name)
        elif name in keep_ghosts:
            entries.append(dict(entry))
            seen.add(name)
    doc["files"] = entries
    if pin_row_count:
        doc["row_count"] = original.get("row_count", 0)
    else:
        doc["row_count"] = sum(row_count for row_count, _ in written.values())
    return doc

def _write_candidate(
    source: _Source,
    target: dict[str, Any],
    state: _State,
    dest: Path,
) -> None:
    """Materialize a reduction candidate (or the final package) on disk."""
    dest.mkdir(parents=True, exist_ok=True)
    config_dir = dest / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    (config_dir / "topology_old.json").write_bytes(source.topology_bytes)
    logical_dir = dest / "logical"
    logical_dir.mkdir(parents=True, exist_ok=True)
    for table in source.tables:
        rows = [source.logical_by_id[table][row_id] for row_id in state.logical.get(table, [])]
        suffix = source.logical_suffix[table]
        _write_data_file(logical_dir / f"{table}{suffix}", source.logical_columns[table], rows)
    shards_dir = dest / "shards"
    shards_dir.mkdir(parents=True, exist_ok=True)
    by_shard: dict[tuple[str, str], list[tuple[str, str, str]]] = {}
    for key in sorted(state.files):
        by_shard.setdefault((key[0], key[1]), []).append(key)
    shard_dirs = set(by_shard)
    if target["code"] in _SHARD_CAUSAL_CODES:
        shard_dirs.add((target["table"], target["shard"]))
    for table, shard in sorted(shard_dirs):
        directory = shards_dir / table / shard
        directory.mkdir(parents=True, exist_ok=True)
        written: dict[str, tuple[int, str]] = {}
        for key in by_shard.get((table, shard), []):
            name = key[2]
            rows = [source.file_rows[key][index] for index in state.files[key]]
            path = directory / name
            _write_data_file(path, source.file_columns[key], rows)
            written[name] = (len(rows), file_sha256(path))
        rule = _shard_rule(source, target, table, shard)
        if rule == "absent":
            continue
        if rule == "raw":
            (directory / "_manifest.json").write_bytes(source.manifest_raw[(table, shard)])
            continue
        if (table, shard) not in source.manifest_docs:
            continue  # the source shard had no manifest; keep it that way
        manifest = _rebuild_manifest(source, target, table, shard, written)
        text = json.dumps(manifest, sort_keys=True, indent=2, ensure_ascii=False) + "\n"
        (directory / "_manifest.json").write_text(text, encoding="utf-8")

# --- target matching ---------------------------------------------------------

def _same_instance(finding: dict[str, Any], target: dict[str, Any]) -> bool:
    left = (finding.get("code"), finding.get("table"), finding.get("shard"), finding.get("row_id"))
    right = (target.get("code"), target.get("table"), target.get("shard"), target.get("row_id"))
    if left != right:
        return False
    return (finding.get("detail") or {}).get("file", "") == (target.get("detail") or {}).get(
        "file", ""
    )

def _target_present(audit: dict[str, Any], target: dict[str, Any], contract: ReproContract) -> bool:
    for finding in audit["findings"]:
        if not _same_instance(finding, target):
            continue
        if contract.mode == EXACT_ROW and finding_evidence(finding) != finding_evidence(target):
            continue
        return True
    return False

def _locate_targets(source: _Source, contract: ReproContract) -> list[dict[str, Any]]:
    matches = [
        finding
        for finding in source.audit["findings"]
        if finding["code"] == contract.code
        and finding["table"] == contract.table
        and (contract.mode == SEMANTIC_FAMILY or finding["row_id"] == contract.row_id)
    ]
    if not matches:
        available = sorted(
            {f"{finding['code']}:{finding['table']}" for finding in source.audit["findings"]}
        )
        raise ShardRouteError(
            REPRO_NO_TARGET,
            f"no {contract.code!r} finding for table {contract.table!r}"
            + (f" and row_id {contract.row_id!r}" if contract.mode == EXACT_ROW else ""),
            details={"available": available},
        )
    if contract.mode == EXACT_ROW:
        if len(matches) > 1:
            raise ShardRouteError(
                REPRO_AMBIGUOUS,
                f"contract matches {len(matches)} findings; the target cannot be identified reliably",
                details={
                    "candidates": [finding_signature(finding) for finding in matches],
                },
            )
        return matches
    ordered = sorted(
        matches,
        key=lambda finding: (_support_estimate(source, finding), finding_signature(finding)),
    )
    return ordered[:FAMILY_INSTANCE_LIMIT]

def _support_estimate(source: _Source, finding: dict[str, Any]) -> int:
    """Rough byte size of the rows a reproduction of this finding must keep."""
    table = finding["table"]
    row_id = finding["row_id"]
    total = 0
    if row_id:
        row = source.logical_by_id.get(table, {}).get(row_id)
        if row is not None:
            total += len(json.dumps(row, ensure_ascii=False))
        for key, rows in source.file_rows.items():
            if key[0] != table:
                continue
            total += sum(
                len(json.dumps(item, ensure_ascii=False)) for item in rows if item["row_id"] == row_id
            )
    else:
        key = (table, finding["shard"], str((finding.get("detail") or {}).get("file", "")))
        total += sum(len(json.dumps(item, ensure_ascii=False)) for item in source.file_rows.get(key, []))
    return total

# --- reduction units ---------------------------------------------------------

def _causal_files(source: _Source, target: dict[str, Any]) -> set[tuple[str, str, str]]:
    """Files that must never silently disappear: they anchor the target."""
    files: set[tuple[str, str, str]] = set()
    table, shard = target["table"], target["shard"]
    detail = target.get("detail") or {}
    name = detail.get("file")
    if isinstance(name, str) and name and table and shard:
        files.add((table, shard, name))
    row_id = target["row_id"]
    if row_id and table:
        for key, rows in source.file_rows.items():
            if key[0] == table and any(item["row_id"] == row_id for item in rows):
                files.add(key)
    return files

def _initial_state(source: _Source) -> _State:
    return _State(
        logical={
            table: [row["row_id"] for row in source.logical_rows[table]] for table in source.tables
        },
        files={key: list(range(len(rows))) for key, rows in source.file_rows.items()},
    )

def _state_row_ids(source: _Source, state: _State, table: str) -> list[str]:
    ids = list(state.logical.get(table, []))
    seen = set(ids)
    extra: set[str] = set()
    for key, indices in state.files.items():
        if key[0] != table:
            continue
        for index in indices:
            row_id = source.file_rows[key][index]["row_id"]
            if row_id not in seen:
                extra.add(row_id)
    return ids + sorted(extra)

def _remove_table(state: _State, table: str) -> _State:
    new = state.clone()
    new.logical[table] = []
    new.files = {key: indices for key, indices in new.files.items() if key[0] != table}
    return new

def _remove_file(state: _State, key: tuple[str, str, str]) -> _State:
    new = state.clone()
    new.files.pop(key, None)
    return new

def _remove_rows(
    source: _Source,
    state: _State,
    causal_files: set[tuple[str, str, str]],
    table: str,
    row_ids: set[str],
) -> _State:
    new = state.clone()
    new.logical[table] = [row_id for row_id in new.logical.get(table, []) if row_id not in row_ids]
    for key in list(new.files):
        if key[0] != table:
            continue
        kept = [
            index
            for index in new.files[key]
            if source.file_rows[key][index]["row_id"] not in row_ids
        ]
        if kept:
            new.files[key] = kept
        elif key in causal_files:
            new.files[key] = []
        else:
            del new.files[key]
    return new

def _apply_unit(
    source: _Source,
    state: _State,
    causal_files: set[tuple[str, str, str]],
    kind: str,
    payload: Any,
) -> _State:
    if kind == "table":
        return _remove_table(state, payload)
    if kind == "file":
        return _remove_file(state, payload)
    table, row_id = payload
    return _remove_rows(source, state, causal_files, table, {row_id})

def _units(source: _Source, state: _State, target: dict[str, Any]) -> list[tuple[str, Any, str]]:
    """Every remaining deletable unit: whole tables, whole files, single rows."""
    units: list[tuple[str, Any, str]] = []
    for table in source.tables:
        if table == target["table"]:
            continue
        if state.logical.get(table) or any(key[0] == table for key in state.files):
            units.append(("table", table, f"table:{table}"))
    for key in sorted(state.files):
        units.append(("file", key, f"file:{key[0]}/{key[1]}/{key[2]}"))
    for table in source.tables:
        for row_id in _state_row_ids(source, state, table):
            units.append(("row", (table, row_id), f"row:{table}/{row_id}"))
    return units

def _chunks(items: list[str], count: int) -> list[list[str]]:
    count = max(1, min(count, len(items)))
    base, extra = divmod(len(items), count)
    chunks: list[list[str]] = []
    start = 0
    for index in range(count):
        size = base + (1 if index < extra else 0)
        chunk = items[start : start + size]
        start += size
        if chunk:
            chunks.append(chunk)
    return chunks

class _Minimizer:
    """Evaluates reduction candidates by re-running the real scanner + audit."""

    def __init__(
        self,
        source: _Source,
        topology: Topology,
        target: dict[str, Any],
        contract: ReproContract,
        counter: _EvalCounter,
    ) -> None:
        self.source = source
        self.topology = topology
        self.target = target
        self.contract = contract
        self.counter = counter
        self.causal_files = _causal_files(source, target)
        self.allowed_highs = {
            finding_signature(finding)
            for finding in source.audit["findings"]
            if finding["severity"] in HIGH_SEVERITIES
        }

    def remaining(self) -> int:
        return self.counter.remaining()

    def audit_dir(self, root: Path) -> dict[str, Any]:
        scan = scan_workspace(root / "logical", root / "shards", self.source.tables)
        return run_audit(self.topology, scan)

    def audit_state(self, state: _State) -> dict[str, Any]:
        with tempfile.TemporaryDirectory(prefix="shardroute-repro-") as temporary:
            _write_candidate(self.source, self.target, state, Path(temporary))
            return self.audit_dir(Path(temporary))

    def acceptable(self, audit: dict[str, Any]) -> bool:
        """A candidate keeps the contract and introduces no new critical/high."""
        if not _target_present(audit, self.target, self.contract):
            return False
        return all(
            finding_signature(finding) in self.allowed_highs
            for finding in audit["findings"]
            if finding["severity"] in HIGH_SEVERITIES
        )

    def rejection(self, audit: dict[str, Any]) -> str:
        if not _target_present(audit, self.target, self.contract):
            return "target_lost"
        new_highs = sorted(
            finding_signature(finding)
            for finding in audit["findings"]
            if finding["severity"] in HIGH_SEVERITIES
            and finding_signature(finding) not in self.allowed_highs
        )
        return "new_high_findings:" + ",".join(new_highs)

    def evaluate(self, state: _State) -> tuple[bool, dict[str, Any]]:
        self.counter.candidate_evals += 1
        audit = self.audit_state(state)
        return self.acceptable(audit), audit

def _reduce_rows(minimizer: _Minimizer, state: _State, table: str, ids: list[str]) -> _State:
    source = minimizer.source
    width = 2
    while len(ids) > 1 and minimizer.remaining() > 0:
        progressed = False
        for chunk in _chunks(ids, width):
            if minimizer.remaining() <= 0:
                break
            candidate = _remove_rows(source, state, minimizer.causal_files, table, set(chunk))
            if minimizer.evaluate(candidate)[0]:
                state = candidate
                ids = _state_row_ids(source, state, table)
                width = 2
                progressed = True
                break
        if not progressed:
            if width >= len(ids):
                break
            width = min(len(ids), width * 2)
    if len(ids) == 1 and minimizer.remaining() > 0:
        candidate = _remove_rows(source, state, minimizer.causal_files, table, {ids[0]})
        if minimizer.evaluate(candidate)[0]:
            state = candidate
    return state

def _reduce(minimizer: _Minimizer, state: _State) -> _State:
    source = minimizer.source
    changed = True
    while changed and minimizer.remaining() > 0:
        changed = False
        for table in source.tables:
            if table == minimizer.target["table"] or minimizer.remaining() <= 0:
                continue
            if not state.logical.get(table) and not any(key[0] == table for key in state.files):
                continue
            candidate = _remove_table(state, table)
            if minimizer.evaluate(candidate)[0]:
                state, changed = candidate, True
        for key in sorted(state.files):
            if minimizer.remaining() <= 0:
                break
            if key not in state.files:
                continue
            candidate = _remove_file(state, key)
            if minimizer.evaluate(candidate)[0]:
                state, changed = candidate, True
        for table in source.tables:
            if minimizer.remaining() <= 0:
                break
            ids = _state_row_ids(source, state, table)
            if not ids:
                continue
            new_state = _reduce_rows(minimizer, state, table, ids)
            if new_state is not state:
                state, changed = new_state, True
    return state

def _prove(
    minimizer: _Minimizer, state: _State
) -> tuple[_State, str, list[dict[str, str]]]:
    """1-minimality proof: try deleting every remaining unit once more.

    Returns the (possibly further reduced) state, ``1-minimal`` when every
    remaining unit was evaluated and rejected, or ``unproven`` when the
    budget ran out first.
    """
    source = minimizer.source
    evidence: list[dict[str, str]] = []
    while True:
        progressed = False
        for kind, payload, label in _units(source, state, minimizer.target):
            if minimizer.remaining() <= 0:
                evidence.append(
                    {
                        "unit": "(remaining units)",
                        "removal": "skipped",
                        "reason": "budget_exhausted",
                    }
                )
                return state, "unproven", evidence
            candidate = _apply_unit(source, state, minimizer.causal_files, kind, payload)
            accepted, audit = minimizer.evaluate(candidate)
            if accepted:
                state = candidate
                evidence.append(
                    {
                        "unit": label,
                        "removal": "accepted",
                        "reason": "contract still satisfied; proof restarted",
                    }
                )
                progressed = True
                break
            evidence.append(
                {"unit": label, "removal": "rejected", "reason": minimizer.rejection(audit)}
            )
        if not progressed:
            return state, "1-minimal", evidence

# --- package -----------------------------------------------------------------

def _tree_bytes(root: Path) -> int:
    return sum(path.stat().st_size for path in root.rglob("*") if path.is_file())

def _check_output(workspace: Path, out_dir: Path) -> None:
    if out_dir == workspace or workspace in out_dir.parents:
        raise ShardRouteError(
            REPRO_OUTPUT,
            "output directory must be outside the source workspace",
            str(out_dir),
        )
    if out_dir.exists():
        if not out_dir.is_dir():
            raise ShardRouteError(
                REPRO_OUTPUT, "output path exists and is not a directory", str(out_dir)
            )
        if any(out_dir.iterdir()):
            raise ShardRouteError(
                REPRO_OUTPUT, "output directory is not empty", str(out_dir)
            )

def build_minimal_repro(
    workspace: str | Path,
    out_dir: str | Path,
    contract: ReproContract,
    *,
    budget: int = DEFAULT_BUDGET,
    config: str | Path | None = None,
) -> dict[str, Any]:
    """Build a minimal reproduction workspace for one verified audit finding.

    The source workspace is only read. The package is written to ``out_dir``
    together with a ``repro.json`` manifest describing the contract, the
    preserved object, the verification runs and the minimality evidence.
    """
    if budget < 0:
        raise ShardRouteError(CLI_USAGE, "budget must be a non-negative integer")
    workspace = Path(workspace).resolve()
    out_dir = Path(out_dir).resolve()
    _check_output(workspace, out_dir)
    topology_path = Path(config).resolve() if config else workspace / "config" / "topology_old.json"
    topology = load_topology(topology_path)
    before = snapshot_tree(workspace)
    source = _read_source(workspace, topology, topology_path.read_bytes())
    targets = _locate_targets(source, contract)
    counter = _EvalCounter(budget)
    runs: list[dict[str, Any]] = []
    for target in targets:
        minimizer = _Minimizer(source, topology, target, contract, counter)
        initial = _initial_state(source)
        counter.structural_evals += 1
        if not minimizer.acceptable(minimizer.audit_state(initial)):
            raise ShardRouteError(
                REPRO_UNSUPPORTED,
                "a faithful rewrite of the workspace does not reproduce the target "
                "finding; reliable reduction is not supported for this input",
                details={"target": finding_signature(target)},
            )
        state = _reduce(minimizer, initial)
        runs.append({"target": target, "state": state, "minimizer": minimizer})
    for run in runs:
        with tempfile.TemporaryDirectory(prefix="shardroute-repro-") as temporary:
            _write_candidate(source, run["target"], run["state"], Path(temporary))
            run["package_bytes"] = _tree_bytes(Path(temporary))
    runs.sort(key=lambda run: (run["package_bytes"], finding_signature(run["target"])))
    winner = runs[0]
    target = winner["target"]
    minimizer = winner["minimizer"]
    state, minimality, evidence = _prove(minimizer, winner["state"])
    out_dir.mkdir(parents=True, exist_ok=True)
    _write_candidate(source, target, state, out_dir)
    counter.structural_evals += 1
    final_audit = minimizer.audit_dir(out_dir)
    if not minimizer.acceptable(final_audit):
        raise ShardRouteError(
            REPRO_UNSUPPORTED,
            "the written package failed its final audit verification",
            str(out_dir),
        )
    after = snapshot_tree(workspace)
    if before != after:
        raise ShardRouteError(
            REPRO_UNSUPPORTED,
            "source workspace changed during reproduction; refusing to report success",
            str(workspace),
        )
    final_target = next(
        finding for finding in final_audit["findings"] if _same_instance(finding, target)
    )
    remaining_highs = sorted(
        {
            finding_signature(finding)
            for finding in final_audit["findings"]
            if finding["severity"] in HIGH_SEVERITIES
        }
    )
    new_highs = [sig for sig in remaining_highs if sig not in minimizer.allowed_highs]
    package_files = sorted(
        path.relative_to(out_dir).as_posix() for path in out_dir.rglob("*") if path.is_file()
    )
    if contract.mode == EXACT_ROW:
        note = (
            "exact_row contract: this package reproduces the original finding "
            "instance with its routing evidence preserved."
        )
    else:
        note = (
            "semantic_family contract: rule-level regression example. The kept "
            f"instance was selected as the smallest of {len(runs)} same-code "
            "candidate(s); it is not the reproduction of a specific original record."
        )
    result: dict[str, Any] = {
        "status": "ok",
        "contract": contract.as_dict(),
        "target_finding": target,
        "preserved": {
            "mode": contract.mode,
            "table": target["table"],
            "code": target["code"],
            "row_id": target["row_id"] or None,
            "shard": target["shard"] or None,
            "evidence": finding_evidence(target),
            "claim": "original_record" if contract.mode == EXACT_ROW else "rule_level_example",
            "note": note,
        },
        "candidates_considered": [
            {
                "signature": finding_signature(run["target"]),
                "row_id": run["target"]["row_id"] or None,
                "shard": run["target"]["shard"] or None,
                "package_bytes": run["package_bytes"],
                "selected": run is winner,
            }
            for run in runs
        ],
        "package": {
            "path": str(out_dir),
            "bytes": sum((out_dir / name).stat().st_size for name in package_files),
            "files": package_files,
            "data_files": sorted(
                name
                for name in package_files
                if name.startswith("shards/") and not name.endswith("_manifest.json")
            ),
            "manifests": sorted(
                name for name in package_files if name.endswith("_manifest.json")
            ),
            "logical_rows": {
                table: len(state.logical.get(table, [])) for table in source.tables
            },
            "physical_rows": sum(len(indices) for indices in state.files.values()),
        },
        "verification": {
            "budget": budget,
            "budget_exhausted": counter.remaining() <= 0,
            "candidate_audits": counter.candidate_evals,
            "structural_audits": counter.structural_evals,
            "audits_run": counter.candidate_evals + counter.structural_evals,
            "target_present": True,
            "original_finding_id": target["finding_id"],
            "package_finding_id": final_target["finding_id"],
            "new_high_findings": new_highs,
            "remaining_high_findings": remaining_highs,
            "minimality": minimality,
            "minimality_evidence": evidence,
            "audit_summary": final_audit["summary"],
            "audit_tables": final_audit["tables"],
        },
        "source_unchanged": True,
        "source_tree_fingerprint": fingerprint(before),
    }
    (out_dir / "repro.json").write_text(
        json.dumps(result, sort_keys=True, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return result
