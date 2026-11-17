"""Offline, read-only compatibility certificate for *unseen* future routing keys.

The resharding plan in :mod:`shardroute.planner` answers a different question:
"which rows that exist *today* would move?".  A plan with ``moved_rows == 0``
says nothing about keys that have never been observed.  This module answers the
narrower, but genuinely stronger, question:

    If future writes keep using the old routing contract, will the new
    topology send the same key to the same shard as the old one?

A certificate has exactly three honest outcomes per table:

``proven_equivalent``
    The two routing functions agree over the *declared key domain*.  For hash
    routing that is a finite periodic residue proof over every possible
    SHA-256 prefix integer; for range routing it is a partition of the whole
    string domain by the union of both topologies' boundaries; for list routing
    it is the finite union of map keys plus the default fall-through class.

``counterexample``
    An *actual raw key* was routed through both :class:`~shardroute.router.Router`
    instances and produced different shards (or one routed while the other
    raised).  The raw key, normalized value, bucket / comparison trace and both
    shard decisions are recorded so the result can be replayed.

``unknown``
    Neither equivalence nor a real counterexample could be established.  This
    covers contract changes (key fields or normalization semantics differ),
    non-finite inverse images (a digest residue is split across shards but no
    raw key witness exists in the budget), a changed ``null_policy`` that could
    not be demonstrated with a real key, and exhausted budgets.

Important: a divergent digest residue is only *potential* divergence.  It is
never reported as proven drift unless a real raw key is found and both routers
are invoked.  Symmetrically, a bounded witness search that found nothing is
never reported as proof.

The certificate is deliberately kept separate from ``plan["safe_to_execute"]``:
that flag describes moving the rows that exist today, while
``certificate["future_writes_allowed"]`` (and ``permission``) describes whether
future writes may switch to the new topology.
"""

from __future__ import annotations

from math import gcd
from typing import Any, Mapping

from .canonical import canonical_json, composite_key, normalize_components, normalize_value, sha256_text, stable_id
from .config_validation import routing_contract
from .errors import ROUTE_NO_MATCH, ROUTE_NULL_KEY, ShardRouteError
from .models import TableTopology, Topology
from .router import Router, hash_bucket_assignment, hash_prefix_integer

CERTIFICATE_FORMAT = "shardroute-compat-certificate/1"

PROVEN_EQUIVALENT = "proven_equivalent"
COUNTEREXAMPLE = "counterexample"
UNKNOWN = "unknown"

PERMISSION_ALLOW = "allow_future_writes"
PERMISSION_BLOCK = "block_future_writes"
PERMISSION_SHADOW = "shadow_observation_only"

DEFAULT_HASH_RESIDUE_BUDGET = 1_000_000
DEFAULT_WITNESS_PROBE_BUDGET = 10_000
DEFAULT_MAX_COUNTEREXAMPLES = 3
DEFAULT_MAX_REPORTED_RESIDUES = 20


def _permission(status: str) -> str:
    if status == PROVEN_EQUIVALENT:
        return PERMISSION_ALLOW
    if status == COUNTEREXAMPLE:
        return PERMISSION_BLOCK
    return PERMISSION_SHADOW


def _normalization_premise(old: TableTopology, new: TableTopology) -> dict[str, Any]:
    """Describe the key/normalization contract that any proof is conditioned on."""
    same_keys = old.key_fields == new.key_fields
    same_case = old.case_sensitive == new.case_sensitive
    return {
        "key_fields_old": list(old.key_fields),
        "key_fields_new": list(new.key_fields),
        "case_sensitive_old": old.case_sensitive,
        "case_sensitive_new": new.case_sensitive,
        "normalization_equivalent": same_keys and same_case,
        "null_policy_old": old.null_policy,
        "null_policy_new": new.null_policy,
        "default_shard_old": old.default_shard,
        "default_shard_new": new.default_shard,
        "contract_old": routing_contract(old),
        "contract_new": routing_contract(new),
    }


def _observe(router: Router, table: str, raw: Any) -> dict[str, Any]:
    """Route one raw key and return a comparison-friendly observation."""
    try:
        trace = router.route(table, raw)
    except ShardRouteError as exc:
        return {
            "kind": "error",
            "target_shard": None,
            "rule": None,
            "bucket": None,
            "digest_prefix": None,
            "dropped": False,
            "error_code": exc.code,
            "comparisons": [],
            "trace": exc.as_dict(),
        }
    if trace.dropped:
        return {
            "kind": "dropped",
            "target_shard": None,
            "rule": trace.rule,
            "bucket": trace.bucket,
            "digest_prefix": trace.digest_prefix,
            "dropped": True,
            "error_code": None,
            "comparisons": list(trace.comparisons),
            "trace": trace.as_dict(),
        }
    return {
        "kind": "route",
        "target_shard": trace.target_shard,
        "rule": trace.rule,
        "bucket": trace.bucket,
        "digest_prefix": trace.digest_prefix,
        "dropped": False,
        "error_code": None,
        "comparisons": list(trace.comparisons),
        "trace": trace.as_dict(),
    }


def _observations_differ(old: Mapping[str, Any], new: Mapping[str, Any]) -> bool:
    if old["kind"] != new["kind"]:
        return True
    if old["kind"] == "route":
        return old["target_shard"] != new["target_shard"]
    return False


def _make_witness(
    table: str,
    raw: Any,
    normalized: Any,
    old: Mapping[str, Any],
    new: Mapping[str, Any],
    *,
    reason: str,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    witness: dict[str, Any] = {
        "table": table,
        "reason": reason,
        "raw_key": raw,
        "normalized_value": normalized,
        "old": dict(old),
        "new": dict(new),
        "comparison_trace": [
            f"old {old['kind']} -> {old['target_shard']} ({old['rule']})",
            f"new {new['kind']} -> {new['target_shard']} ({new['rule']})",
        ],
    }
    witness.update(dict(extra or {}))
    return witness


def _normalized_hash_key(table: TableTopology, raw: Any) -> str | None:
    raw_list = list(raw) if isinstance(raw, (list, tuple)) else [raw]
    if len(raw_list) != len(table.key_fields):
        return None
    values = normalize_components(raw_list, table.case_sensitive)
    if any(value is None for value in values):
        return None
    return composite_key([value for value in values if value is not None])


def _hash_probe(table: TableTopology, index: int) -> Any:
    token = f"compat_probe_{index:06d}"
    if len(table.key_fields) == 1:
        return token
    return [token] + [f"compat_part_{part:02d}" for part in range(1, len(table.key_fields))]


def _range_boundaries(table: TableTopology) -> set[str]:
    boundaries: set[str] = set()
    for rule in table.ranges:
        boundaries.add(normalize_value(rule.lower, table.case_sensitive) or rule.lower)
        if rule.upper is not None:
            upper = normalize_value(rule.upper, table.case_sensitive)
            if upper is not None:
                boundaries.add(upper)
    return boundaries


def _range_target_normalized(table: TableTopology, value: str) -> tuple[str, str | None]:
    """Route an already-normalized range key, mirroring ``Router._route_range``."""
    for rule in table.ranges:
        lower = normalize_value(rule.lower, table.case_sensitive) or rule.lower
        upper = normalize_value(rule.upper, table.case_sensitive) if rule.upper is not None else None
        if value >= lower and (upper is None or value < upper):
            return ("route", rule.shard)
    if table.default_shard:
        return ("route", table.default_shard)
    return ("error", ROUTE_NO_MATCH)


def _list_target_normalized(table: TableTopology, value: str) -> tuple[str, str | None]:
    shard = table.list_map.get(value)
    if shard:
        return ("route", shard)
    if table.default_shard:
        return ("route", table.default_shard)
    return ("error", ROUTE_NO_MATCH)


def _unmapped_list_key(table: TableTopology, *others: TableTopology) -> str:
    known: set[str] = set(table.list_map)
    for other in others:
        known.update(other.list_map)
    index = 0
    while True:
        candidate = f"compat_unmapped_{index:06d}"
        if candidate not in known:
            return candidate
        index += 1


def _null_outcome(table: TableTopology) -> tuple[str, str | None]:
    if table.null_policy == "error":
        return ("error", ROUTE_NULL_KEY)
    if table.null_policy == "drop":
        return ("dropped", None)
    return ("route", table.default_shard)


def _null_raw(table: TableTopology) -> Any:
    if len(table.key_fields) == 1:
        return None
    return [None] + [f"compat_null_part_{part:02d}" for part in range(1, len(table.key_fields))]


def _unknown(reason: str, **extra: Any) -> dict[str, Any]:
    record = {"status": UNKNOWN, "unknown_reasons": [reason]}
    record.update(extra)
    return record


def _contract_unknown(old: TableTopology, new: TableTopology) -> list[str]:
    reasons: list[str] = []
    if old.router != new.router:
        reasons.append(
            f"router family changed ({old.router!r} -> {new.router!r}); routing functions are not comparable"
        )
    if old.key_fields != new.key_fields:
        reasons.append(
            f"shard key changed ({list(old.key_fields)!r} -> {list(new.key_fields)!r}); a rewrite contract is required"
        )
    if old.case_sensitive != new.case_sensitive:
        reasons.append(
            "case sensitivity / normalization semantics differ; identical raw keys may normalize differently"
        )
    return reasons


def _null_record(old: TableTopology, new: TableTopology, old_router: Router, new_router: Router) -> dict[str, Any]:
    before, after = _null_outcome(old), _null_outcome(new)
    record: dict[str, Any] = {
        "old": {"policy": old.null_policy, "outcome": before[0], "target_shard": before[1]},
        "new": {"policy": new.null_policy, "outcome": after[0], "target_shard": after[1]},
    }
    if before == after:
        record.update({"status": PROVEN_EQUIVALENT, "counterexamples": []})
        return record
    if old.key_fields != new.key_fields:
        # A raw null key cannot be shaped to match both key arities, so it cannot
        # serve as a replayable witness for the null domain.
        record.update(
            {
                "status": UNKNOWN,
                "counterexamples": [],
                "unknown_reasons": [
                    "shard key fields differ; the same raw null key cannot be routed through both topologies"
                ],
            }
        )
        return record
    raw = _null_raw(old)
    old_obs = _observe(old_router, old.table, raw)
    new_obs = _observe(new_router, new.table, raw)
    record["candidate_reason"] = "null_policy"
    if _observations_differ(old_obs, new_obs):
        record.update(
            {
                "status": COUNTEREXAMPLE,
                "counterexamples": [
                    _make_witness(
                        old.table, raw, None, old_obs, new_obs, reason="null_policy"
                    )
                ],
            }
        )
    else:
        record.update(
            {
                "status": UNKNOWN,
                "counterexamples": [],
                "unknown_reasons": [
                    "null behavior differs by configuration but the real null key did not reproduce a routing difference"
                ],
            }
        )
    return record


def _hash_certificate(
    old: Topology,
    new: Topology,
    old_router: Router,
    new_router: Router,
    old_t: TableTopology,
    new_t: TableTopology,
    *,
    hash_residue_budget: int,
    witness_probe_budget: int,
    max_counterexamples: int,
    max_reported_residues: int,
) -> dict[str, Any]:
    premise = _normalization_premise(old_t, new_t)
    reasons = _contract_unknown(old_t, new_t)
    if reasons:
        return _unknown(reasons[0], unknown_reasons=reasons, normalization_premise=premise)

    assert old_t.buckets and new_t.buckets
    old_buckets, new_buckets = old_t.buckets, new_t.buckets
    old_map, new_map = old_t.bucket_map, new_t.bucket_map
    period = old_buckets * new_buckets // gcd(old_buckets, new_buckets)

    proof: dict[str, Any] = {
        "method": "hash_periodic_residue_scan",
        "domain": (
            "all normalized composite keys under the shared normalization contract: the "
            "SHA-256 prefix integer ranges over every value, and bucket assignment is "
            "periodic modulo lcm(old_buckets, new_buckets), so every possible key is covered "
            "by the residue classes below"
        ),
        "budget": {
            "residue_classes": period,
            "residue_scan_limit": hash_residue_budget,
            "witness_probe_limit": witness_probe_budget,
            "max_counterexamples": max_counterexamples,
        },
        "buckets_old": old_buckets,
        "buckets_new": new_buckets,
        "assignment_note": (
            "both maps are compared as functions of the SHA-256 prefix integer, not by "
            "comparing equal bucket indices; a bucket-count change is therefore handled"
        ),
        "consumed": {"residues_scanned": 0, "witness_probes": 0},
        "exhaustive": False,
    }

    if period > hash_residue_budget:
        return _unknown(
            "hash residue budget exhausted before the periodic assignment could be enumerated",
            normalization_premise=premise,
            proof=proof,
            potential_divergences={"count": 0, "residues": [], "note": "scan did not run"},
        )

    divergent: list[dict[str, Any]] = []
    divergent_residues: set[int] = set()
    for residue in range(period):
        old_bucket, old_shard = hash_bucket_assignment(old_buckets, old_map, residue)
        new_bucket, new_shard = hash_bucket_assignment(new_buckets, new_map, residue)
        if old_shard == new_shard:
            continue
        divergent_residues.add(residue)
        if len(divergent) < max_reported_residues:
            divergent.append(
                {
                    "residue_mod": period,
                    "residue": residue,
                    "old_bucket": old_bucket,
                    "new_bucket": new_bucket,
                    "old_shard": old_shard,
                    "new_shard": new_shard,
                }
            )
    proof["consumed"]["residues_scanned"] = period

    potential = {
        "count": len(divergent_residues),
        "residue_mod": period,
        "residues": divergent,
        "truncated": len(divergent_residues) > len(divergent),
    }

    if not divergent_residues:
        proof["exhaustive"] = True
        return {
            "status": PROVEN_EQUIVALENT,
            "normalization_premise": premise,
            "proof": proof,
            "potential_divergences": potential,
            "counterexamples": [],
        }

    witnesses: list[dict[str, Any]] = []
    probes = 0
    for index in range(witness_probe_budget):
        probes += 1
        raw = _hash_probe(old_t, index)
        normalized = _normalized_hash_key(old_t, raw)
        if normalized is None:
            continue
        number = hash_prefix_integer(normalized)
        if number % period not in divergent_residues:
            continue
        old_obs = _observe(old_router, old_t.table, raw)
        new_obs = _observe(new_router, new_t.table, raw)
        if not _observations_differ(old_obs, new_obs):
            continue
        witnesses.append(
            _make_witness(
                old_t.table,
                raw,
                normalized,
                old_obs,
                new_obs,
                reason="hash_residue_split_across_shards",
                extra={
                    "residue_mod": period,
                    "residue": number % period,
                    "sha256_prefix_integer": number,
                },
            )
        )
        if len(witnesses) >= max_counterexamples:
            break
    proof["consumed"]["witness_probes"] = probes

    if witnesses:
        return {
            "status": COUNTEREXAMPLE,
            "normalization_premise": premise,
            "proof": proof,
            "potential_divergences": potential,
            "counterexamples": witnesses,
        }

    proof["exhaustive"] = False
    return _unknown(
        (
            "digest residues map to different shards, but no raw key witness was found within "
            "the witness probe budget; this is potential divergence, not proven drift or equivalence"
        ),
        normalization_premise=premise,
        proof=proof,
        potential_divergences=potential,
        counterexamples=[],
    )


def _range_certificate(
    old: Topology,
    new: Topology,
    old_router: Router,
    new_router: Router,
    old_t: TableTopology,
    new_t: TableTopology,
    *,
    witness_probe_budget: int,
    max_counterexamples: int,
) -> dict[str, Any]:
    premise = _normalization_premise(old_t, new_t)
    reasons = _contract_unknown(old_t, new_t)
    if reasons:
        return _unknown(reasons[0], unknown_reasons=reasons, normalization_premise=premise)

    boundaries = sorted(_range_boundaries(old_t) | _range_boundaries(new_t))
    # Class representatives in normalized space: [""..b0), [b0..b1), ...
    representatives: list[str] = [""] + boundaries
    candidates: list[dict[str, Any]] = []
    for value in representatives:
        before = _range_target_normalized(old_t, value)
        after = _range_target_normalized(new_t, value)
        if before != after:
            candidates.append(
                {
                    "normalized_value": value,
                    "old": {"outcome": before[0], "target_shard": before[1]},
                    "new": {"outcome": after[0], "target_shard": after[1]},
                }
            )

    proof = {
        "method": "range_boundary_partition",
        "domain": (
            "all scalar strings after normalization, partitioned into intervals by the union of "
            "both topologies' normalized range boundaries; includes values below the first range, "
            "values above a finite final range, explicit gaps, default_shard fallback and "
            "ROUTE_NO_MATCH outcomes"
        ),
        "budget": {
            "classes": len(representatives),
            "witness_probe_limit": witness_probe_budget,
            "max_counterexamples": max_counterexamples,
        },
        "consumed": {"classes_checked": len(representatives), "witness_probes": 0},
        "exhaustive": not candidates,
    }
    potential = {
        "count": len(candidates),
        "classes": len(representatives),
        "boundaries": boundaries,
        "classes_with_different_outcomes": candidates[:max_counterexamples],
        "truncated": len(candidates) > max_counterexamples,
    }
    if not candidates:
        return {
            "status": PROVEN_EQUIVALENT,
            "normalization_premise": premise,
            "proof": proof,
            "potential_divergences": potential,
            "counterexamples": [],
        }

    witnesses: list[dict[str, Any]] = []
    probes = 0
    for candidate in candidates[:witness_probe_budget]:
        probes += 1
        raw = candidate["normalized_value"]
        old_obs = _observe(old_router, old_t.table, raw)
        new_obs = _observe(new_router, new_t.table, raw)
        if not _observations_differ(old_obs, new_obs):
            continue
        witnesses.append(
            _make_witness(
                old_t.table,
                raw,
                normalize_value(raw, old_t.case_sensitive),
                old_obs,
                new_obs,
                reason="range_boundary_or_default_differs",
                extra={
                    "normalized_partition_value": candidate["normalized_value"],
                },
            )
        )
        if len(witnesses) >= max_counterexamples:
            break
    proof["consumed"]["witness_probes"] = probes

    if witnesses:
        return {
            "status": COUNTEREXAMPLE,
            "normalization_premise": premise,
            "proof": proof,
            "potential_divergences": potential,
            "counterexamples": witnesses,
        }

    proof["exhaustive"] = False
    return _unknown(
        (
            "range partition classes differ, but no raw key witness reproduced the difference; "
            "this is potential divergence, not proven drift or equivalence"
        ),
        normalization_premise=premise,
        proof=proof,
        potential_divergences=potential,
        counterexamples=[],
    )


def _list_certificate(
    old: Topology,
    new: Topology,
    old_router: Router,
    new_router: Router,
    old_t: TableTopology,
    new_t: TableTopology,
    *,
    witness_probe_budget: int,
    max_counterexamples: int,
) -> dict[str, Any]:
    premise = _normalization_premise(old_t, new_t)
    reasons = _contract_unknown(old_t, new_t)
    if reasons:
        return _unknown(reasons[0], unknown_reasons=reasons, normalization_premise=premise)

    known_keys = sorted(set(old_t.list_map) | set(new_t.list_map))
    unmapped = _unmapped_list_key(old_t, new_t)
    candidates: list[dict[str, Any]] = []
    for key in known_keys:
        before = _list_target_normalized(old_t, key)
        after = _list_target_normalized(new_t, key)
        if before != after:
            candidates.append(
                {
                    "kind": "listed_key",
                    "normalized_value": key,
                    "old": {"outcome": before[0], "target_shard": before[1]},
                    "new": {"outcome": after[0], "target_shard": after[1]},
                }
            )
    before_default = _list_target_normalized(old_t, unmapped)
    after_default = _list_target_normalized(new_t, unmapped)
    if before_default != after_default:
        candidates.append(
            {
                "kind": "unlisted_key",
                "normalized_value": unmapped,
                "old": {"outcome": before_default[0], "target_shard": before_default[1]},
                "new": {"outcome": after_default[0], "target_shard": after_default[1]},
            }
        )

    proof = {
        "method": "list_finite_map_plus_default_fallthrough",
        "domain": (
            "all scalar strings after normalization: every key in the union of both list maps is "
            "checked individually, and every unlisted key shares the default_shard fall-through or "
            "ROUTE_NO_MATCH outcome"
        ),
        "budget": {
            "listed_keys": len(known_keys),
            "unlisted_probes": 1,
            "witness_probe_limit": witness_probe_budget,
            "max_counterexamples": max_counterexamples,
        },
        "consumed": {"listed_keys_checked": len(known_keys), "witness_probes": 0},
        "exhaustive": not candidates,
    }
    potential = {
        "count": len(candidates),
        "classes": len(known_keys) + 1,
        "classes_with_different_outcomes": candidates[:max_counterexamples],
        "truncated": len(candidates) > max_counterexamples,
    }
    if not candidates:
        return {
            "status": PROVEN_EQUIVALENT,
            "normalization_premise": premise,
            "proof": proof,
            "potential_divergences": potential,
            "counterexamples": [],
        }

    witnesses: list[dict[str, Any]] = []
    probes = 0
    for candidate in candidates[:witness_probe_budget]:
        probes += 1
        raw = candidate["normalized_value"]
        old_obs = _observe(old_router, old_t.table, raw)
        new_obs = _observe(new_router, new_t.table, raw)
        if not _observations_differ(old_obs, new_obs):
            continue
        witnesses.append(
            _make_witness(
                old_t.table,
                raw,
                normalize_value(raw, old_t.case_sensitive),
                old_obs,
                new_obs,
                reason=(
                    "list_map_value_differs"
                    if candidate["kind"] == "listed_key"
                    else "unlisted_key_default_differs"
                ),
                extra={"class_kind": candidate["kind"]},
            )
        )
        if len(witnesses) >= max_counterexamples:
            break
    proof["consumed"]["witness_probes"] = probes

    if witnesses:
        return {
            "status": COUNTEREXAMPLE,
            "normalization_premise": premise,
            "proof": proof,
            "potential_divergences": potential,
            "counterexamples": witnesses,
        }

    proof["exhaustive"] = False
    return _unknown(
        (
            "list routing classes differ, but no raw key witness reproduced the difference; "
            "this is potential divergence, not proven drift or equivalence"
        ),
        normalization_premise=premise,
        proof=proof,
        potential_divergences=potential,
        counterexamples=[],
    )


def _table_certificate(
    old: Topology,
    new: Topology,
    old_router: Router,
    new_router: Router,
    old_t: TableTopology,
    new_t: TableTopology,
    *,
    hash_residue_budget: int,
    witness_probe_budget: int,
    max_counterexamples: int,
    max_reported_residues: int,
) -> dict[str, Any]:
    common = {
        "table": old_t.table,
        "router_old": old_t.router,
        "router_new": new_t.router,
    }
    if old_t.router == "hash":
        body = _hash_certificate(
            old, new, old_router, new_router, old_t, new_t,
            hash_residue_budget=hash_residue_budget,
            witness_probe_budget=witness_probe_budget,
            max_counterexamples=max_counterexamples,
            max_reported_residues=max_reported_residues,
        )
    elif old_t.router == "range":
        body = _range_certificate(
            old, new, old_router, new_router, old_t, new_t,
            witness_probe_budget=witness_probe_budget,
            max_counterexamples=max_counterexamples,
        )
    else:
        body = _list_certificate(
            old, new, old_router, new_router, old_t, new_t,
            witness_probe_budget=witness_probe_budget,
            max_counterexamples=max_counterexamples,
        )

    # Null keys are a separate domain and can independently invalidate a proof.
    null_record = _null_record(old_t, new_t, old_router, new_router)
    body["null_policy"] = null_record
    counterexamples = list(body.get("counterexamples", [])) + list(null_record.get("counterexamples", []))
    unknown_reasons = list(body.get("unknown_reasons", [])) + list(null_record.get("unknown_reasons", []))
    if counterexamples:
        status = COUNTEREXAMPLE
    elif unknown_reasons:
        status = UNKNOWN
    else:
        status = body["status"]
    body.update(common)
    body["status"] = status
    body["counterexamples"] = counterexamples
    if unknown_reasons:
        body["unknown_reasons"] = unknown_reasons
    body["permission"] = _permission(status)
    return body


def build_compatibility_certificate(
    old: Topology,
    new: Topology,
    *,
    hash_residue_budget: int = DEFAULT_HASH_RESIDUE_BUDGET,
    witness_probe_budget: int = DEFAULT_WITNESS_PROBE_BUDGET,
    max_counterexamples: int = DEFAULT_MAX_COUNTEREXAMPLES,
    max_reported_residues: int = DEFAULT_MAX_REPORTED_RESIDUES,
) -> dict[str, Any]:
    """Build an offline certificate for unseen future-key routing compatibility."""
    if hash_residue_budget < 1 or witness_probe_budget < 0 or max_counterexamples < 1:
        raise ValueError("compatibility budgets must be non-negative and meaningful")

    old_router, new_router = Router(old), Router(new)
    tables: list[dict[str, Any]] = []
    old_names, new_names = set(old.tables), set(new.tables)

    for table in sorted(old_names - new_names):
        tables.append(
            {
                "table": table,
                "router_old": old.tables[table].router,
                "router_new": None,
                "status": UNKNOWN,
                "permission": _permission(UNKNOWN),
                "counterexamples": [],
                "unknown_reasons": ["table does not exist in the new topology"],
            }
        )
    for table in sorted(old_names & new_names):
        tables.append(
            _table_certificate(
                old, new, old_router, new_router, old.tables[table], new.tables[table],
                hash_residue_budget=hash_residue_budget,
                witness_probe_budget=witness_probe_budget,
                max_counterexamples=max_counterexamples,
                max_reported_residues=max_reported_residues,
            )
        )

    by_status: dict[str, int] = {PROVEN_EQUIVALENT: 0, COUNTEREXAMPLE: 0, UNKNOWN: 0}
    for record in tables:
        by_status[record["status"]] += 1
    if by_status[COUNTEREXAMPLE]:
        status = COUNTEREXAMPLE
    elif by_status[UNKNOWN]:
        status = UNKNOWN
    else:
        status = PROVEN_EQUIVALENT
    permission = _permission(status)

    counterexample_tables = sorted(r["table"] for r in tables if r["status"] == COUNTEREXAMPLE)
    unknown_tables = sorted(r["table"] for r in tables if r["status"] == UNKNOWN)
    potential_tables = sorted(
        r["table"]
        for r in tables
        if r.get("potential_divergences", {}).get("count", 0)
    )
    core: dict[str, Any] = {
        "format": CERTIFICATE_FORMAT,
        "status": status,
        "permission": permission,
        "future_writes_allowed": status == PROVEN_EQUIVALENT,
        "old_fingerprint": old.fingerprint,
        "new_fingerprint": new.fingerprint,
        "budget": {
            "hash_residue_budget": hash_residue_budget,
            "witness_probe_budget": witness_probe_budget,
            "max_counterexamples": max_counterexamples,
            "max_reported_residues": max_reported_residues,
        },
        "proof_scope": (
            "Unseen future-key routing only. This certificate is independent of plan.moved_rows "
            "and plan.safe_to_execute; a zero-row move matrix does not by itself prove future-key "
            "equivalence, and this certificate does not by itself prove current rows are placed "
            "correctly."
        ),
        "summary": {
            "tables_checked": len(tables),
            "proven_equivalent": by_status[PROVEN_EQUIVALENT],
            "counterexample": by_status[COUNTEREXAMPLE],
            "unknown": by_status[UNKNOWN],
            "counterexample_tables": counterexample_tables,
            "unknown_tables": unknown_tables,
            "potential_divergence_tables": potential_tables,
            "new_only_tables": sorted(new_names - old_names),
        },
        "tables": tables,
    }
    certificate_id = stable_id("compat", core, length=16)
    core["certificate_id"] = certificate_id
    core["digest"] = sha256_text(canonical_json(core))
    return core


__all__ = [
    "CERTIFICATE_FORMAT",
    "PROVEN_EQUIVALENT",
    "COUNTEREXAMPLE",
    "UNKNOWN",
    "PERMISSION_ALLOW",
    "PERMISSION_BLOCK",
    "PERMISSION_SHADOW",
    "build_compatibility_certificate",
]
