"""Stable error codes and exception types used by every ShardRoute command."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# Configuration errors (returned by ``validate`` with exit code 2).
CFG_SCHEMA = "CFG_SCHEMA"
CFG_TABLE_NAME = "CFG_TABLE_NAME"
CFG_SHARD_DUPLICATE = "CFG_SHARD_DUPLICATE"
CFG_SHARD_UNKNOWN = "CFG_SHARD_UNKNOWN"
CFG_ROUTER_UNKNOWN = "CFG_ROUTER_UNKNOWN"
CFG_KEY_MISSING = "CFG_KEY_MISSING"
CFG_BUCKET_RANGE = "CFG_BUCKET_RANGE"
CFG_RANGE_OVERLAP = "CFG_RANGE_OVERLAP"
CFG_RANGE_GAP = "CFG_RANGE_GAP"
CFG_LIST_DUPLICATE = "CFG_LIST_DUPLICATE"
CFG_NULL_POLICY = "CFG_NULL_POLICY"
CFG_DEFAULT_MISSING = "CFG_DEFAULT_MISSING"

# Runtime and data errors.
ROUTE_NULL_KEY = "ROUTE_NULL_KEY"
ROUTE_NO_MATCH = "ROUTE_NO_MATCH"
ROUTE_KEY_MISSING = "ROUTE_KEY_MISSING"
ROUTE_BAD_VALUE = "ROUTE_BAD_VALUE"
SCAN_IO = "SCAN_IO"
SCAN_CSV = "SCAN_CSV"
SCAN_JSONL = "SCAN_JSONL"
SCAN_ROW_ID = "SCAN_ROW_ID"
SCAN_DUPLICATE_ID = "SCAN_DUPLICATE_ID"
QUERY_INVALID = "QUERY_INVALID"
WORKSPACE_INVALID = "WORKSPACE_INVALID"
LEDGER_INVALID = "LEDGER_INVALID"
SERVER_HOST = "SERVER_HOST"
CLI_USAGE = "CLI_USAGE"

# Minimal-reproduction errors (returned by ``repro`` with exit code 1).
REPRO_NO_TARGET = "REPRO_NO_TARGET"
REPRO_AMBIGUOUS = "REPRO_AMBIGUOUS"
REPRO_UNSUPPORTED = "REPRO_UNSUPPORTED"
REPRO_OUTPUT = "REPRO_OUTPUT"

@dataclass
class Issue:
    """A single validation or runtime problem with a stable code."""
    code: str
    message: str
    location: str = ""
    details: dict[str, Any] = field(default_factory=dict)
    def as_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "location": self.location,
            "details": dict(sorted(self.details.items())),
        }

class ShardRouteError(Exception):
    """Base exception carrying a stable code and structured context."""
    def __init__(
        self,
        code: str,
        message: str,
        location: str = "",
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.location = location
        self.details = details or {}
    def as_dict(self) -> dict[str, Any]:
        return Issue(self.code, self.message, self.location, self.details).as_dict()
    def __str__(self) -> str:
        where = f" at {self.location}" if self.location else ""
        return f"[{self.code}] {self.message}{where}"

class ConfigValidationError(ShardRouteError):
    """Aggregates all topology validation issues instead of stopping at one."""
    def __init__(self, issues: list[Issue], fingerprint: str = "") -> None:
        self.issues = sorted(issues, key=lambda i: (i.code, i.location, i.message))
        message = f"{len(self.issues)} configuration issue(s)"
        super().__init__(CFG_SCHEMA, message)
        self.fingerprint = fingerprint
    def as_dict(self) -> dict[str, Any]:
        return {
            "valid": False,
            "fingerprint": self.fingerprint,
            "issue_count": len(self.issues),
            "issues": [issue.as_dict() for issue in self.issues],
        }

class RoutingError(ShardRouteError):
    """Raised when a row cannot be assigned to a shard."""

class ScanError(ShardRouteError):
    """Raised when source files cannot be parsed safely."""

class QueryError(ShardRouteError):
    """Raised for malformed query explanations."""
