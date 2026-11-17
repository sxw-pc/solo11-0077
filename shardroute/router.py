"""Deterministic hash, range and list routing with an inspectable trace."""

from __future__ import annotations

import hashlib
from typing import Any, Mapping

from .canonical import composite_key, normalize_components, normalize_value
from .errors import (
    ROUTE_KEY_MISSING,
    ROUTE_NO_MATCH,
    ROUTE_NULL_KEY,
    RoutingError,
)
from .models import RouteTrace, TableTopology, Topology

_MISSING = object()

def hash_prefix_integer(normalized: str) -> int:
    """Return the deterministic SHA-256 prefix integer used for hash routing.

    ``router=hash`` assigns ``int.from_bytes(SHA-256(key)[0:8], "big") % buckets``.
    Exposing the integer (rather than the bucket) lets compatibility proofs
    compare two topologies' bucket assignments over the same digest space.
    """
    digest = hashlib.sha256(normalized.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big")

def hash_bucket_assignment(
    buckets: int, bucket_map: tuple[str, ...] | list[str], number: int
) -> tuple[int, str]:
    """Map a SHA-prefix integer to ``(bucket, shard)`` for one topology."""
    bucket = number % buckets
    return bucket, bucket_map[bucket]

class Router:
    """Routes key values against an already validated topology."""
    def __init__(self, topology: Topology) -> None:
        self.topology = topology
    def route_values(self, table_name: str, values: Mapping[str, Any]) -> RouteTrace:
        table = self._table(table_name)
        missing = [field for field in table.key_fields if field not in values]
        if missing:
            raise RoutingError(
                ROUTE_KEY_MISSING,
                f"row is missing routing key field(s): {', '.join(missing)}",
                location=table_name,
                details={"missing_fields": missing, "table": table_name},
            )
        raw = [values[field] for field in table.key_fields]
        return self.route(table_name, raw[0] if len(raw) == 1 else raw)
    def route(self, table_name: str, value: Any = _MISSING) -> RouteTrace:
        table = self._table(table_name)
        if value is _MISSING:
            raise RoutingError(ROUTE_KEY_MISSING, "a shard key value is required", table_name)
        raw = value if isinstance(value, (list, tuple)) else [value]
        raw_list = list(raw)
        if len(table.key_fields) == 1 and len(raw_list) == 1:
            raw_reported: Any = raw_list[0]
        else:
            raw_reported = raw_list
        if len(raw_list) != len(table.key_fields):
            raise RoutingError(
                ROUTE_KEY_MISSING,
                "component count does not match the configured key",
                table_name,
                {"expected": len(table.key_fields), "actual": len(raw_list)},
            )
        normalized_values = normalize_components(raw_list, table.case_sensitive)
        normalized = composite_key([v for v in normalized_values if v is not None])
        if any(v is None for v in normalized_values):
            return self._route_null(table, raw_reported)
        if table.router == "hash":
            return self._route_hash(table, raw_reported, normalized)
        if table.router == "range":
            return self._route_range(table, raw_reported, normalized_values[0] or "")
        return self._route_list(table, raw_reported, normalized_values[0] or "")
    def _table(self, table_name: str) -> TableTopology:
        try:
            return self.topology.table(table_name)
        except KeyError as exc:
            raise RoutingError(
                "ROUTE_UNKNOWN_TABLE",
                f"table {table_name!r} is not present in the topology",
                table_name,
            ) from exc
    def _route_null(self, table: TableTopology, raw: Any) -> RouteTrace:
        if table.null_policy == "error":
            raise RoutingError(
                ROUTE_NULL_KEY,
                "null shard key rejected by null_policy=error",
                table.table,
                {"key_fields": list(table.key_fields)},
            )
        if table.null_policy == "drop":
            return RouteTrace(
                table=table.table,
                key_fields=table.key_fields,
                raw_value=raw,
                normalized_value=None,
                target_shard=None,
                rule="null_drop",
                null_action="drop",
                dropped=True,
            )
        assert table.default_shard is not None
        return RouteTrace(
            table=table.table,
            key_fields=table.key_fields,
            raw_value=raw,
            normalized_value=None,
            target_shard=table.default_shard,
            rule="null_route_default",
            null_action="route_default",
            comparisons=(f"null -> {table.default_shard}",),
        )
    def _route_hash(self, table: TableTopology, raw: Any, normalized: str) -> RouteTrace:
        assert table.buckets is not None
        digest = hashlib.sha256(normalized.encode("utf-8")).digest()
        number = int.from_bytes(digest[:8], "big")
        bucket, shard = hash_bucket_assignment(table.buckets, table.bucket_map, number)
        return RouteTrace(
            table=table.table,
            key_fields=table.key_fields,
            raw_value=raw,
            normalized_value=normalized,
            target_shard=shard,
            rule="hash_bucket",
            null_action="reject",
            bucket=bucket,
            digest_prefix=digest.hex()[:16],
            comparisons=(f"sha256[0:8] % {table.buckets} = {bucket}",),
        )
    def _route_range(self, table: TableTopology, raw: Any, normalized: str) -> RouteTrace:
        checked: list[str] = []
        for index, rule in enumerate(table.ranges):
            lower = normalize_value(rule.lower, table.case_sensitive) or rule.lower
            upper = (
                normalize_value(rule.upper, table.case_sensitive)
                if rule.upper is not None
                else None
            )
            lower_ok = normalized >= lower
            upper_ok = upper is None or normalized < upper
            checked.append(f"{normalized} >= {lower}: {str(lower_ok).lower()}")
            if upper is not None:
                checked.append(f"{normalized} < {upper}: {str(upper_ok).lower()}")
            if lower_ok and upper_ok:
                return RouteTrace(
                    table=table.table,
                    key_fields=table.key_fields,
                    raw_value=raw,
                    normalized_value=normalized,
                    target_shard=rule.shard,
                    rule=f"range[{index}]",
                    null_action="reject",
                    comparisons=tuple(checked),
                )
        if table.default_shard:
            return RouteTrace(
                table=table.table,
                key_fields=table.key_fields,
                raw_value=raw,
                normalized_value=normalized,
                target_shard=table.default_shard,
                rule="range_default",
                null_action="reject",
                comparisons=tuple(checked + [f"no interval matched -> {table.default_shard}"]),
            )
        raise RoutingError(
            ROUTE_NO_MATCH,
            "value did not match any configured range",
            table.table,
            {"value": normalized},
        )
    def _route_list(self, table: TableTopology, raw: Any, normalized: str) -> RouteTrace:
        shard = table.list_map.get(normalized)
        if shard:
            return RouteTrace(
                table=table.table,
                key_fields=table.key_fields,
                raw_value=raw,
                normalized_value=normalized,
                target_shard=shard,
                rule="list_exact",
                null_action="reject",
                comparisons=(f"{normalized} in list_map",),
            )
        if table.default_shard:
            return RouteTrace(
                table=table.table,
                key_fields=table.key_fields,
                raw_value=raw,
                normalized_value=normalized,
                target_shard=table.default_shard,
                rule="list_default",
                null_action="reject",
                comparisons=(f"{normalized} not in list_map -> {table.default_shard}",),
            )
        raise RoutingError(
            ROUTE_NO_MATCH,
            "value did not match list_map and no default shard exists",
            table.table,
            {"value": normalized},
        )
