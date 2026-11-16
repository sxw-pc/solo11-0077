"""Validation rules for declarative shard topologies."""

from __future__ import annotations

import re
from typing import Any

from .canonical import fingerprint, format_utc, normalize_value
from .errors import (
    CFG_BUCKET_RANGE,
    CFG_DEFAULT_MISSING,
    CFG_KEY_MISSING,
    CFG_LIST_DUPLICATE,
    CFG_NULL_POLICY,
    CFG_RANGE_DOMAIN,
    CFG_RANGE_GAP,
    CFG_RANGE_OVERLAP,
    CFG_RANGE_VALUE,
    CFG_ROUTER_UNKNOWN,
    CFG_SCHEMA,
    CFG_SHARD_DUPLICATE,
    CFG_SHARD_UNKNOWN,
    CFG_TABLE_NAME,
    Issue,
)
from .models import RangeRule, TableTopology, Topology
from .range_domain import (
    RANGE_ALIAS_POLICIES,
    RANGE_DOMAINS,
    RangeValueError,
    parse_range_value,
)

NAME_RE = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
NULL_POLICIES = {"error", "route_default", "drop"}
ROUTERS = {"hash", "range", "list"}

def _issue(code: str, message: str, location: str, **details: Any) -> Issue:
    return Issue(code, message, location, details)

def _is_bool(value: Any) -> bool:
    return isinstance(value, bool)

def _validate_shards(raw: Any, issues: list[Issue]) -> list[str]:
    if not isinstance(raw, list) or not raw:
        issues.append(_issue(CFG_SCHEMA, "shards must be a non-empty list", "shards"))
        return []
    shards: list[str] = []
    seen: set[str] = set()
    for index, shard in enumerate(raw):
        location = f"shards[{index}]"
        if not isinstance(shard, str) or not NAME_RE.match(shard):
            issues.append(
                _issue(CFG_SCHEMA, "shard id must match ^[a-z][a-z0-9_]{0,62}$", location)
            )
            continue
        if shard in seen:
            issues.append(_issue(CFG_SHARD_DUPLICATE, f"duplicate shard id {shard!r}", location))
            continue
        seen.add(shard)
        shards.append(shard)
    return shards

def _bucket_map(raw: Any, buckets: int, location: str, issues: list[Issue]) -> tuple[str, ...]:
    if not isinstance(raw, (list, dict)):
        issues.append(_issue(CFG_BUCKET_RANGE, "bucket_map must be a list or object", location))
        return ()
    if isinstance(raw, dict):
        expected = {str(index) for index in range(buckets)}
        if set(raw) != expected:
            issues.append(
                _issue(
                    CFG_BUCKET_RANGE,
                    "bucket_map must cover every bucket exactly once",
                    location,
                    expected=sorted(expected),
                    actual=sorted(raw),
                )
            )
        ordered = [raw.get(str(index)) for index in range(buckets)]
    else:
        ordered = raw
        if len(ordered) != buckets:
            issues.append(
                _issue(
                    CFG_BUCKET_RANGE,
                    "bucket_map length must equal buckets",
                    location,
                    buckets=buckets,
                    length=len(ordered),
                )
            )
    result: list[str] = []
    for index, shard in enumerate(ordered):
        if not isinstance(shard, str) or not shard:
            issues.append(_issue(CFG_BUCKET_RANGE, "bucket_map entry must be a shard id", f"{location}[{index}]"))
        else:
            result.append(shard)
    return tuple(result)

def _range_bound_is_valid(value: Any, domain: str, *, required: bool) -> bool:
    """Structural bound check; semantic parsing happens in the domain layer.

    The text domain keeps the historical contract (a non-empty string).  Typed
    domains accept any JSON scalar through to the parser so that floats,
    booleans, empty strings and oversized integers are reported with the
    domain-specific ``CFG_RANGE_VALUE`` evidence rather than a generic schema
    error.  Containers and a missing required bound stay ``CFG_SCHEMA``.
    """
    if value is None:
        return not required
    if domain == "text":
        return isinstance(value, str) and bool(value.strip())
    if isinstance(value, (list, tuple, dict, set)):
        return False
    return True


def _range_value_issue(error: RangeValueError, location: str) -> Issue:
    """Convert a typed range parse failure into stable configuration evidence."""
    return _issue(
        CFG_RANGE_VALUE,
        error.message,
        location,
        domain=error.domain,
        raw_value=error.raw,
        reason=error.reason,
        **({"canonical": error.canonical} if error.canonical is not None else {}),
    )

def _validate_range(
    raw: Any,
    location: str,
    shards: set[str],
    domain: str,
    alias_policy: str,
    issues: list[Issue],
) -> tuple[RangeRule, ...]:
    if not isinstance(raw, list) or not raw:
        issues.append(_issue(CFG_SCHEMA, "ranges must be a non-empty list", location))
        return ()
    rules: list[RangeRule] = []
    parsed: list[tuple[RangeRule, Any, Any]] = []
    parsed_all = True
    for index, item in enumerate(raw):
        loc = f"{location}[{index}]"
        if not isinstance(item, dict):
            issues.append(_issue(CFG_SCHEMA, "range entry must be an object", loc))
            parsed_all = False
            continue
        lower, upper, shard = item.get("lower"), item.get("upper"), item.get("shard")
        if not _range_bound_is_valid(lower, domain, required=True):
            issues.append(
                _issue(
                    CFG_SCHEMA,
                    "range lower must be a non-empty string"
                    + (" or base-10 integer" if domain == "integer" else ""),
                    f"{loc}.lower",
                )
            )
            parsed_all = False
            continue
        if upper is not None and not _range_bound_is_valid(upper, domain, required=False):
            issues.append(
                _issue(
                    CFG_SCHEMA,
                    "range upper must be a string, base-10 integer or null"
                    if domain == "integer"
                    else "range upper must be a string or null",
                    f"{loc}.upper",
                )
            )
            parsed_all = False
            continue
        if not isinstance(shard, str):
            issues.append(_issue(CFG_SHARD_UNKNOWN, "range shard must be named", f"{loc}.shard"))
            parsed_all = False
            continue
        if shard not in shards:
            issues.append(_issue(CFG_SHARD_UNKNOWN, f"unknown shard {shard!r}", f"{loc}.shard"))
        lower_key: Any = None
        upper_key: Any = None
        lower_error = upper_error = None
        if domain == "text":
            lower_key, upper_key = lower, upper
        else:
            try:
                parsed_lower = parse_range_value(lower, domain, alias_policy)
                lower_key = parsed_lower.key
            except RangeValueError as error:
                lower_error = error
            if upper is not None:
                try:
                    parsed_upper = parse_range_value(upper, domain, alias_policy)
                    upper_key = parsed_upper.key
                except RangeValueError as error:
                    upper_error = error
        if lower_error is not None:
            issues.append(_range_value_issue(lower_error, f"{loc}.lower"))
            parsed_all = False
        if upper_error is not None:
            issues.append(_range_value_issue(upper_error, f"{loc}.upper"))
            parsed_all = False
        if lower_error is not None or upper_error is not None:
            continue
        if upper is not None and lower_key >= upper_key:
            details = {"lower": lower, "upper": upper}
            if domain != "text":
                details.update(
                    {
                        "domain": domain,
                        "parsed_lower": lower_key,
                        "parsed_upper": upper_key,
                    }
                )
            issues.append(
                _issue(
                    CFG_RANGE_OVERLAP,
                    "range lower must be strictly less than upper",
                    loc,
                    **details,
                )
            )
        rule = RangeRule(
            lower,
            upper,
            shard,
            lower_key=lower_key if domain != "text" else None,
            upper_key=upper_key if domain != "text" else None,
        )
        rules.append(rule)
        parsed.append((rule, lower_key, upper_key))
    if not parsed_all:
        return tuple(rules)
    for index in range(1, len(parsed)):
        previous, _, previous_upper = parsed[index - 1]
        current, current_lower, _ = parsed[index]
        loc = f"{location}[{index}]"
        if previous.upper is None:
            issues.append(_issue(CFG_RANGE_OVERLAP, "no range may follow an open-ended range", loc))
        elif previous_upper > current_lower:
            details = {"previous_upper": previous.upper, "lower": current.lower}
            if domain != "text":
                details.update(
                    {"domain": domain, "parsed_previous_upper": previous_upper,
                     "parsed_lower": current_lower}
                )
            issues.append(
                _issue(CFG_RANGE_OVERLAP, "range overlaps the previous range", loc, **details)
            )
        elif previous_upper < current_lower:
            details = {"previous_upper": previous.upper, "lower": current.lower}
            if domain != "text":
                details.update(
                    {"domain": domain, "parsed_previous_upper": previous_upper,
                     "parsed_lower": current_lower}
                )
            issues.append(
                _issue(
                    CFG_RANGE_GAP,
                    "range leaves a gap after the previous range",
                    loc,
                    **details,
                )
            )
    return tuple(rules)

def _validate_list_map(
    raw: Any, location: str, shards: set[str], case_sensitive: bool, issues: list[Issue]
) -> dict[str, str]:
    if not isinstance(raw, dict) or not raw:
        issues.append(_issue(CFG_SCHEMA, "list_map must be a non-empty object", location))
        return {}
    result: dict[str, str] = {}
    seen: set[str] = set()
    for key, shard in raw.items():
        loc = f"{location}.{key}"
        normalized = normalize_value(key, case_sensitive)
        assert normalized is not None
        if normalized in seen:
            issues.append(_issue(CFG_LIST_DUPLICATE, f"duplicate normalized list key {key!r}", loc))
        seen.add(normalized)
        if not isinstance(shard, str) or shard not in shards:
            issues.append(_issue(CFG_SHARD_UNKNOWN, f"unknown shard {shard!r}", loc))
        else:
            result[normalized] = shard
    return result

def validate_document(raw: Any) -> tuple[Topology | None, list[Issue], str]:
    issues: list[Issue] = []
    fp = fingerprint(raw) if raw is not None else ""
    if not isinstance(raw, dict):
        return None, [_issue(CFG_SCHEMA, "top-level configuration must be an object", "$")], fp
    version = raw.get("version")
    if not isinstance(version, int) or _is_bool(version) or version < 1:
        issues.append(_issue(CFG_SCHEMA, "version must be a positive integer", "version"))
    generated_at = raw.get("generated_at", "1970-01-01T00:00:00Z")
    if not isinstance(generated_at, str):
        issues.append(_issue(CFG_SCHEMA, "generated_at must be a UTC ISO-8601 timestamp", "generated_at"))
        generated_at = "1970-01-01T00:00:00Z"
    else:
        try:
            generated_at = format_utc(generated_at)
        except (TypeError, ValueError):
            issues.append(_issue(CFG_SCHEMA, "generated_at must be a UTC ISO-8601 timestamp", "generated_at"))
    description = raw.get("description", "")
    if not isinstance(description, str):
        issues.append(_issue(CFG_SCHEMA, "description must be a string", "description"))
    shard_list = _validate_shards(raw.get("shards"), issues)
    known_shards = set(shard_list)
    raw_tables = raw.get("tables")
    if not isinstance(raw_tables, list) or not raw_tables:
        issues.append(_issue(CFG_SCHEMA, "tables must be a non-empty list", "tables"))
        raw_tables = []
    tables: dict[str, TableTopology] = {}
    for table_index, item in enumerate(raw_tables):
        loc = f"tables[{table_index}]"
        if not isinstance(item, dict):
            issues.append(_issue(CFG_SCHEMA, "table entry must be an object", loc))
            continue
        table_name = item.get("table")
        if not isinstance(table_name, str) or not NAME_RE.match(table_name):
            issues.append(
                _issue(CFG_TABLE_NAME, "table must match ^[a-z][a-z0-9_]{0,62}$", f"{loc}.table")
            )
            continue
        if table_name in tables:
            issues.append(_issue(CFG_SCHEMA, f"duplicate table {table_name!r}", f"{loc}.table"))
            continue
        router = item.get("router")
        if not isinstance(router, str) or router not in ROUTERS:
            issues.append(
                _issue(CFG_ROUTER_UNKNOWN, f"router must be one of {sorted(ROUTERS)}", f"{loc}.router")
            )
            continue
        raw_key = item.get("key")
        keys = [raw_key] if isinstance(raw_key, str) else raw_key
        if (
            not isinstance(keys, list)
            or not keys
            or any(not isinstance(key, str) or not key.strip() for key in keys)
            or len(set(keys)) != len(keys)
        ):
            issues.append(_issue(CFG_KEY_MISSING, "key must be a field or non-empty field list", f"{loc}.key"))
            continue
        null_policy = item.get("null_policy", "error")
        if not isinstance(null_policy, str) or null_policy not in NULL_POLICIES:
            issues.append(
                _issue(CFG_NULL_POLICY, f"null_policy must be one of {sorted(NULL_POLICIES)}", f"{loc}.null_policy")
            )
        default_shard = item.get("default_shard")
        if default_shard is not None and (
            not isinstance(default_shard, str) or default_shard not in known_shards
        ):
            issues.append(_issue(CFG_SHARD_UNKNOWN, f"unknown default shard {default_shard!r}", f"{loc}.default_shard"))
            default_shard = None
        if null_policy == "route_default" and not default_shard:
            issues.append(_issue(CFG_DEFAULT_MISSING, "route_default requires default_shard", loc))
        case_sensitive = item.get("case_sensitive", True)
        if not _is_bool(case_sensitive):
            issues.append(_issue(CFG_SCHEMA, "case_sensitive must be boolean", f"{loc}.case_sensitive"))
            case_sensitive = True
        buckets: int | None = None
        bucket_map: tuple[str, ...] = ()
        ranges: tuple[RangeRule, ...] = ()
        list_map: dict[str, str] = {}
        range_domain = "text"
        range_alias_policy = "strict"
        if router == "hash":
            buckets = item.get("buckets")
            if not isinstance(buckets, int) or _is_bool(buckets) or buckets < 1:
                issues.append(_issue(CFG_BUCKET_RANGE, "buckets must be a positive integer", f"{loc}.buckets"))
                buckets = None
            else:
                bucket_map = _bucket_map(item.get("bucket_map"), buckets, f"{loc}.bucket_map", issues)
                for index, shard in enumerate(bucket_map):
                    if shard not in known_shards:
                        issues.append(
                            _issue(CFG_SHARD_UNKNOWN, f"unknown shard {shard!r}", f"{loc}.bucket_map[{index}]")
                        )
            if len(keys) > 8:
                issues.append(_issue(CFG_KEY_MISSING, "hash composite key supports at most 8 fields", f"{loc}.key"))
        elif router == "range":
            if len(keys) != 1:
                issues.append(_issue(CFG_KEY_MISSING, "range router requires exactly one key", f"{loc}.key"))
            raw_domain = item.get("range_domain", "text")
            if not isinstance(raw_domain, str) or raw_domain not in RANGE_DOMAINS:
                issues.append(
                    _issue(
                        CFG_RANGE_DOMAIN,
                        f"range_domain must be one of {list(RANGE_DOMAINS)}",
                        f"{loc}.range_domain",
                        actual=raw_domain,
                        allowed=list(RANGE_DOMAINS),
                    )
                )
                raw_domain = "text"
            range_domain = raw_domain
            raw_alias_policy = item.get("range_alias_policy", "strict")
            if not isinstance(raw_alias_policy, str) or raw_alias_policy not in RANGE_ALIAS_POLICIES:
                issues.append(
                    _issue(
                        CFG_RANGE_DOMAIN,
                        f"range_alias_policy must be one of {list(RANGE_ALIAS_POLICIES)}",
                        f"{loc}.range_alias_policy",
                        actual=raw_alias_policy,
                        allowed=list(RANGE_ALIAS_POLICIES),
                    )
                )
                raw_alias_policy = "strict"
            range_alias_policy = raw_alias_policy
            ranges = _validate_range(
                item.get("ranges"),
                f"{loc}.ranges",
                known_shards,
                range_domain,
                range_alias_policy,
                issues,
            )
        else:
            if len(keys) != 1:
                issues.append(_issue(CFG_KEY_MISSING, "list router requires exactly one key", f"{loc}.key"))
            list_map = _validate_list_map(
                item.get("list_map"), f"{loc}.list_map", known_shards, case_sensitive, issues
            )
        tables[table_name] = TableTopology(
            table=table_name,
            router=router,
            key_fields=tuple(keys),
            null_policy=null_policy if isinstance(null_policy, str) and null_policy in NULL_POLICIES else "error",
            default_shard=default_shard if default_shard in known_shards else None,
            buckets=buckets,
            bucket_map=bucket_map,
            ranges=ranges,
            list_map=list_map,
            case_sensitive=case_sensitive,
            range_domain=range_domain,
            range_alias_policy=range_alias_policy,
        )
    if issues:
        return None, issues, fp
    topology = Topology(
        version=version,
        shards=tuple(shard_list),
        tables=tables,
        generated_at=generated_at,
        description=description,
        fingerprint=fp,
    )
    return topology, [], fp
