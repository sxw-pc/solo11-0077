"""Small domain model shared by configuration, routing, auditing and reporting."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .canonical import stable_id

SEVERITY_RANK = {"critical": 0, "high": 1, "medium": 2, "low": 3, "info": 4}

@dataclass(frozen=True)
class RangeRule:
    lower: str
    upper: str | None
    shard: str

@dataclass(frozen=True)
class TableTopology:
    table: str
    router: str
    key_fields: tuple[str, ...]
    null_policy: str
    default_shard: str | None
    buckets: int | None = None
    bucket_map: tuple[str, ...] = ()
    ranges: tuple[RangeRule, ...] = ()
    list_map: dict[str, str] = field(default_factory=dict)
    case_sensitive: bool = True
    def table_shards(self) -> list[str]:
        """Every shard this table can route to, sorted for deterministic output."""
        targets: set[str] = set()
        if self.router == "hash":
            targets.update(self.bucket_map)
        elif self.router == "range":
            targets.update(rule.shard for rule in self.ranges)
        else:
            targets.update(self.list_map.values())
        if self.default_shard:
            targets.add(self.default_shard)
        return sorted(targets)
    def as_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "router": self.router,
            "key": list(self.key_fields),
            "null_policy": self.null_policy,
            "default_shard": self.default_shard,
            "buckets": self.buckets,
            "bucket_map": list(self.bucket_map),
            "ranges": [
                {"lower": rule.lower, "upper": rule.upper, "shard": rule.shard}
                for rule in self.ranges
            ],
            "list_map": dict(sorted(self.list_map.items())),
            "case_sensitive": self.case_sensitive,
        }

@dataclass(frozen=True)
class Topology:
    version: int
    shards: tuple[str, ...]
    tables: dict[str, TableTopology]
    generated_at: str
    description: str
    fingerprint: str
    def table(self, name: str) -> TableTopology:
        return self.tables[name]
    def table_names(self) -> list[str]:
        return sorted(self.tables)

@dataclass(frozen=True)
class RouteTrace:
    table: str
    key_fields: tuple[str, ...]
    raw_value: Any
    normalized_value: Any
    target_shard: str | None
    rule: str
    null_action: str
    bucket: int | None = None
    digest_prefix: str | None = None
    comparisons: tuple[str, ...] = ()
    dropped: bool = False
    def as_dict(self) -> dict[str, Any]:
        return {"table": self.table, "key_fields": list(self.key_fields), "raw_value": self.raw_value,
                "normalized_value": self.normalized_value, "bucket": self.bucket,
                "target_shard": self.target_shard, "rule": self.rule, "null_action": self.null_action,
                "digest_prefix": self.digest_prefix, "comparisons": list(self.comparisons),
                "dropped": self.dropped}

@dataclass(frozen=True)
class LogicalRow:
    table: str
    row_id: str
    values: dict[str, Any]
    source: str
    line_no: int

@dataclass(frozen=True)
class PhysicalRow:
    table: str
    shard: str
    row_id: str
    values: dict[str, Any]
    source: str
    line_no: int

@dataclass(frozen=True)
class ScannedFile:
    table: str
    shard: str
    name: str
    relative_path: str
    sha256: str
    row_count: int

@dataclass(frozen=True)
class ManifestEntry:
    name: str
    rows: int
    sha256: str

@dataclass(frozen=True)
class ManifestRecord:
    table: str
    shard: str
    relative_path: str
    row_count: int
    files: tuple[ManifestEntry, ...]
    generated_at: str

@dataclass
class ScanResult:
    logical_rows: dict[str, list[LogicalRow]]
    physical_rows: dict[str, list[PhysicalRow]]
    files: list[ScannedFile]
    manifests: dict[tuple[str, str], ManifestRecord]
    manifest_errors: list[dict[str, str]]
    physical_dirs: list[tuple[str, str]]
    logical_fingerprint: str
    physical_fingerprint: str

@dataclass(frozen=True)
class Finding:
    finding_id: str
    code: str
    severity: str
    table: str
    message: str
    shard: str = ""
    row_id: str = ""
    detail: dict[str, Any] = field(default_factory=dict)
    def as_dict(self) -> dict[str, Any]:
        return {"finding_id": self.finding_id, "code": self.code, "severity": self.severity,
                "table": self.table, "shard": self.shard, "row_id": self.row_id,
                "message": self.message, "detail": self.detail}
    def sort_key(self) -> tuple[Any, ...]:
        return (
            SEVERITY_RANK.get(self.severity, 9),
            self.table,
            self.code,
            self.shard,
            self.row_id,
            self.finding_id,
        )

def make_finding(
    code: str,
    severity: str,
    table: str,
    message: str,
    *,
    shard: str = "",
    row_id: str = "",
    detail: dict[str, Any] | None = None,
) -> Finding:
    payload = detail or {}
    finding_id = stable_id("fnd", code, table, shard, row_id, payload)
    return Finding(finding_id, code, severity, table, message, shard, row_id, payload)

@dataclass(frozen=True)
class MoveCell:
    table: str
    from_shard: str
    to_shard: str
    row_count: int
    def as_dict(self) -> dict[str, Any]:
        return {"table": self.table, "from_shard": self.from_shard,
                "to_shard": self.to_shard, "row_count": self.row_count}

@dataclass(frozen=True)
class PlanBatch:
    batch_id: str
    phase: str
    table: str
    source: str
    target: str
    row_count: int
    def as_dict(self) -> dict[str, Any]:
        return {"batch_id": self.batch_id, "phase": self.phase, "table": self.table,
                "source": self.source, "target": self.target, "row_count": self.row_count}

@dataclass(frozen=True)
class QueryFilter:
    field: str
    op: str
    value: Any
    def as_dict(self) -> dict[str, Any]:
        return {"field": self.field, "op": self.op, "value": self.value}

@dataclass
class QueryExplanation:
    name: str
    table: str
    decision: str
    reason: str
    key_fields: tuple[str, ...]
    all_shards: list[str]
    targeted_shards: list[str]
    filters: list[QueryFilter]
    trace: dict[str, Any]
    description: str = ""
    def as_dict(self) -> dict[str, Any]:
        coverage = round(len(self.targeted_shards) / len(self.all_shards), 4) if self.all_shards else 0.0
        return {"name": self.name, "table": self.table, "description": self.description,
                "decision": self.decision, "reason": self.reason, "key_fields": list(self.key_fields),
                "filters": [item.as_dict() for item in self.filters], "all_shards": self.all_shards,
                "targeted_shards": self.targeted_shards, "pruned_shards":
                [s for s in self.all_shards if s not in self.targeted_shards],
                "coverage_ratio": coverage, "trace": self.trace}
