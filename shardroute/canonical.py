"""Deterministic normalization, hashing, stable identifiers and JSON helpers."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Iterable

from .errors import ROUTE_BAD_VALUE, ShardRouteError

SEPARATOR = "\x1f"

def normalize_value(value: Any, case_sensitive: bool = True) -> str | None:
    """Normalize one shard-key component.
    Strings are trimmed; ``casefold`` is applied when the table declares
    case-insensitive routing. JSON null stays null. Scalars are converted to
    a stable textual representation. Containers are rejected.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        text = "true" if value else "false"
    elif isinstance(value, int):
        text = str(value)
    elif isinstance(value, float):
        text = format(value, ".15g")
    elif isinstance(value, str):
        text = value.strip()
    else:
        raise ShardRouteError(
            ROUTE_BAD_VALUE, "shard key must be a scalar value", details={"type": type(value).__name__}
        )
    return text if case_sensitive else text.casefold()

def normalize_components(values: Iterable[Any], case_sensitive: bool = True) -> tuple[str | None, ...]:
    return tuple(normalize_value(value, case_sensitive) for value in values)

def composite_key(parts: Iterable[str]) -> str:
    """Join already-normalized components using the documented unit separator."""
    return SEPARATOR.join(parts)

def canonical_json(value: Any) -> str:
    """Stable JSON text: sorted keys, compact separators, UTF-8 friendly."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)

def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(65536), b""):
            digest.update(block)
    return digest.hexdigest()

def fingerprint(value: Any) -> str:
    """SHA-256 over canonical JSON, independent of object key order."""
    return sha256_text(canonical_json(value))

def stable_id(prefix: str, *parts: Any, length: int = 16) -> str:
    """Create a deterministic identifier from stable parts."""
    digest = sha256_text(canonical_json(list(parts)))[:length]
    return f"{prefix}_{digest}"

def format_utc(value: str) -> str:
    """Validate the compact UTC timestamp notation used by fixtures."""
    from datetime import datetime, timezone
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid UTC timestamp: {value!r}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
