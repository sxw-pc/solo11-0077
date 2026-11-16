"""Load JSON topology files and expose validation results for the CLI."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .config_validation import NAME_RE, NULL_POLICIES, ROUTERS, validate_document
from .errors import CFG_SCHEMA, SCAN_IO, ConfigValidationError, ShardRouteError
from .models import Topology

__all__ = [
    "NAME_RE",
    "NULL_POLICIES",
    "ROUTERS",
    "read_json",
    "load_topology",
    "validate_document",
    "validation_report",
]

class _DuplicateJSONKey(ValueError):
    pass

def _no_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateJSONKey(f"duplicate JSON key: {key}")
        result[key] = value
    return result

def read_json(path: Path) -> Any:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ShardRouteError(SCAN_IO, f"cannot read {path}", str(path), {"error": str(exc)}) from exc
    try:
        return json.loads(text, object_pairs_hook=_no_duplicate_keys)
    except (json.JSONDecodeError, _DuplicateJSONKey) as exc:
        raise ShardRouteError(CFG_SCHEMA, f"invalid JSON: {exc}", str(path)) from exc

def load_topology(path: str | Path) -> Topology:
    path = Path(path)
    raw = read_json(path)
    topology, issues, fp = validate_document(raw)
    if issues:
        raise ConfigValidationError(issues, fp)
    assert topology is not None
    return topology

def validation_report(path: str | Path) -> dict[str, Any]:
    path = Path(path)
    try:
        raw = read_json(path)
    except ShardRouteError as exc:
        return {"valid": False, "fingerprint": "", "issue_count": 1, "issues": [exc.as_dict()]}
    topology, issues, fp = validate_document(raw)
    if issues:
        return {
            "valid": False,
            "fingerprint": fp,
            "issue_count": len(issues),
            "issues": [issue.as_dict() for issue in sorted(issues, key=lambda i: (i.code, i.location, i.message))],
        }
    assert topology is not None
    return {
        "valid": True,
        "fingerprint": fp,
        "issue_count": 0,
        "issues": [],
        "tables": [
            {
                "table": table.table,
                "router": table.router,
                "key": list(table.key_fields),
                "target_shards": table.table_shards(),
            }
            for table in (topology.tables[name] for name in topology.table_names())
        ],
    }
