"""Hash-table load-skew tail diagnostics under an explicit null hypothesis.

Scope contract (this module never claims otherwise):

* The null hypothesis, labelled ``independent_canonical_keys_uniform_buckets``,
  models each *distinct canonical routing key* as one independent categorical
  draw that is uniform over the table's buckets. Rows sharing a canonical key
  move together: a key group carrying ``w`` logical rows contributes all ``w``
  rows to the shard resolved through ``bucket_map``.
* This is a statistical model for audit evidence. It does **not** claim that
  SHA-256 output is truly random, and a tail probability here is not a
  guarantee against adversarially chosen keys or against future capacity
  changes.
* Null/drop/default rows, rows that fail routing and rows handled by non-hash
  routers are never counted as independent hash keys. They are reported as
  exclusions with a reason code.

Two certificate routes are supported:

1. ``exact_weighted_enumeration``: exact dynamic programming over weighted key
   groups, valid for unequal bucket weights and unequal group weights. It
   never degrades into a per-row binomial test.
2. ``conservative_hoeffding_bound``: a strict upper bound used only when the
   exact route exceeds its compute budget. It is issued only for two
   equal-weight shards and equal group weights, where a group-level
   two-sided Hoeffding inequality rigorously bounds the weighted load event.
   Whenever the bound cannot be derived or the bound is larger than alpha,
   the decision is ``unknown`` -- it is never silently converted into a
   pass/fail verdict.
"""

from __future__ import annotations

import math
from collections import Counter
from typing import Any, Sequence

from .errors import ROUTE_NULL_KEY, ShardRouteError
from .models import LogicalRow, TableTopology
from .router import Router

# Public hypothesis label and verdict vocabulary.
HYPOTHESIS_LABEL = "independent_canonical_keys_uniform_buckets"
DECISION_SIGNIFICANT = "significant"
DECISION_NOT_SIGNIFICANT = "not_significant"
DECISION_UNKNOWN = "unknown"

EXACT_CERTIFICATE = "exact_weighted_enumeration"
HOEFFDING_CERTIFICATE = "conservative_hoeffding_bound"

# Default evidence budget. The exact route is DP over reachable load states;
# these caps bound both time (state count) and memory (total carried weight).
DEFAULT_ALPHA = 0.01
DEFAULT_MAX_EXACT_STATES = 2_000_000
DEFAULT_MAX_EXACT_TOTAL_WEIGHT = 200_000

_FLOAT_EPSILON = 1e-12

SCOPE_NOTE = (
    "Statistical audit evidence only. Under the explicit null hypothesis "
    "'independent canonical keys, uniform buckets', distinct canonical keys "
    "are modelled as independent uniform bucket draws and all rows sharing a "
    "canonical key move as one weighted group. This does not claim SHA-256 "
    "output is truly random and gives no guarantee against adversarially "
    "chosen keys or future capacity changes."
)


class SkewBudgetExceeded(RuntimeError):
    """Raised when exact enumeration cannot complete inside the evidence budget."""

    def __init__(self, reason: str, **context: Any) -> None:
        super().__init__(reason)
        self.reason = reason
        self.context = context


class SkewCertificateUnavailable(RuntimeError):
    """Raised when the conservative bound does not apply to the input shape."""

    def __init__(self, reason: str, **context: Any) -> None:
        super().__init__(reason)
        self.reason = reason
        self.context = context


def _validate_alpha(alpha: float) -> float:
    if not isinstance(alpha, (int, float)) or isinstance(alpha, bool):
        raise TypeError("alpha must be a number")
    alpha = float(alpha)
    if not 0.0 < alpha < 1.0:
        raise ValueError("alpha must satisfy 0 < alpha < 1")
    return alpha


def shard_baseline_probabilities(table: TableTopology) -> dict[str, float]:
    """Baseline probability of each shard from its bucket_map share.

    Unequal bucket maps stay unequal here: probabilities are bucket counts
    divided by the bucket count, never a flat 1/number-of-shards split.
    """
    assert table.router == "hash" and table.buckets is not None
    counts = Counter(table.bucket_map)
    return {shard: counts[shard] / table.buckets for shard in sorted(counts)}


def build_hash_key_groups(
    table: TableTopology, logical_rows: Sequence[LogicalRow], router: Router
) -> tuple[dict[str, dict[str, Any]], Counter, Counter]:
    """Group logical rows by canonical key using real Router traces.

    Returns ``(groups, exclusions, observed_loads)`` where each group carries
    its row-count weight, the bucket/shard the Router actually resolved and
    the contributing row ids. Excluded rows are bucketed by reason:

    * ``null_error``: null key rejected by ``null_policy=error`` (routing fail)
    * ``null_drop``: null key dropped by policy
    * ``null_default``: null key routed to the default shard
    * ``route_error``: any other routing failure
    * ``non_hash_route``: range/list rule (not a hash decision)
    """
    groups: dict[str, dict[str, Any]] = {}
    exclusions: Counter = Counter()
    observed_loads: Counter = Counter()
    for row in logical_rows:
        try:
            trace = router.route_values(table.table, row.values)
        except ShardRouteError as exc:
            exclusions["null_error" if exc.code == ROUTE_NULL_KEY else "route_error"] += 1
            continue
        if trace.rule == "hash_bucket":
            key = trace.normalized_value
            assert isinstance(key, str)
            entry = groups.get(key)
            if entry is None:
                groups[key] = {
                    "canonical_key": key,
                    "weight": 1,
                    "bucket": trace.bucket,
                    "routed_shard": trace.target_shard,
                    "row_ids": [row.row_id],
                }
            else:
                # Router determinism: one canonical key must keep one bucket.
                if entry["bucket"] != trace.bucket or entry["routed_shard"] != trace.target_shard:
                    raise AssertionError(
                        f"canonical key {key!r} resolved to two destinations: "
                        f"{entry['bucket']}/{entry['routed_shard']} vs "
                        f"{trace.bucket}/{trace.target_shard}"
                    )
                entry["weight"] += 1
                entry["row_ids"].append(row.row_id)
            observed_loads[trace.target_shard] += 1
            continue
        if trace.dropped:
            exclusions["null_drop"] += 1
        elif trace.rule == "null_route_default":
            exclusions["null_default"] += 1
        else:
            exclusions["non_hash_route"] += 1
    for entry in groups.values():
        entry["row_ids"].sort()
    return groups, exclusions, observed_loads


def exact_tail_probability(
    group_weights: Sequence[int],
    shard_probabilities: Sequence[float],
    observed_max: int,
    *,
    max_states: int = DEFAULT_MAX_EXACT_STATES,
    max_total_weight: int = DEFAULT_MAX_EXACT_TOTAL_WEIGHT,
) -> tuple[float, int]:
    """Exact probability that the largest shard load is >= ``observed_max``.

    The sample space is one bucket draw per key group; a group of weight ``w``
    moves all ``w`` rows together. Dynamic programming keeps the probability
    mass of every still-live vector of shard loads; any state whose maximum
    already reaches the observed threshold is folded into the tail mass.

    Returns ``(tail_probability, peak_states)``. Raises
    :class:`SkewBudgetExceeded` when the state table or total carried weight
    would exceed the declared budget. No approximation is ever returned from
    this function.
    """
    if observed_max <= 0:
        return 1.0, 1
    total_weight = sum(group_weights)
    if total_weight < observed_max:
        return 0.0, 1
    if total_weight > max_total_weight:
        raise SkewBudgetExceeded(
            "total_weight_budget",
            total_weight=total_weight,
            max_total_weight=max_total_weight,
        )
    shard_count = len(shard_probabilities)
    states: dict[tuple[int, ...], float] = {(0,) * shard_count: 1.0}
    tail_mass = 0.0
    peak_states = 1
    for weight in group_weights:
        next_states: dict[tuple[int, ...], float] = {}
        for state, mass in states.items():
            for shard_index, probability in enumerate(shard_probabilities):
                updated = list(state)
                updated[shard_index] += weight
                new_state = tuple(updated)
                contribution = mass * probability
                if max(new_state) >= observed_max:
                    tail_mass += contribution
                else:
                    next_states[new_state] = next_states.get(new_state, 0.0) + contribution
        if len(next_states) > max_states:
            raise SkewBudgetExceeded(
                "state_count_budget",
                states_required=len(next_states),
                max_states=max_states,
            )
        peak_states = max(peak_states, len(next_states))
        states = next_states
    return tail_mass, peak_states


def conservative_hoeffding_bound(
    group_weights: Sequence[int],
    shard_probabilities: Sequence[float],
    observed_max: int,
) -> float:
    """Strict two-sided Hoeffding upper bound for the narrow balanced case.

    Rigorous only when there are exactly two shards of equal bucket weight
    (0.5 each) and every key group has the same weight. Under the null, each
    group is then an independent Bernoulli(1/2) draw and the weighted load is
    a constant rescaling of the group count, so Hoeffding's two-sided bound
    on the group fraction bounds the observed max-load event exactly::

        B = 2 * exp(-2 * g * (c/g - 1/2)^2)

    where ``g`` is the group count and ``c`` the number of groups observed in
    the heaviest shard. Any other shape raises
    :class:`SkewCertificateUnavailable`; callers must answer ``unknown``.
    """
    if len(shard_probabilities) != 2 or any(
        abs(probability - 0.5) > _FLOAT_EPSILON for probability in shard_probabilities
    ):
        raise SkewCertificateUnavailable(
            "requires_two_equal_weight_shards",
            shard_probabilities=list(shard_probabilities),
        )
    if not group_weights:
        raise SkewCertificateUnavailable("no_key_groups")
    distinct_weights = set(group_weights)
    if len(distinct_weights) != 1:
        raise SkewCertificateUnavailable(
            "requires_equal_group_weights", distinct_weights=sorted(distinct_weights)
        )
    group_weight = next(iter(distinct_weights))
    group_count = len(group_weights)
    if observed_max % group_weight != 0:
        raise SkewCertificateUnavailable(
            "observed_load_not_composed_of_equal_weights",
            observed_max=observed_max,
            group_weight=group_weight,
        )
    heaviest_groups = observed_max // group_weight
    deviation = heaviest_groups / group_count - 0.5
    if deviation <= 0.0:
        return 1.0
    return min(1.0, 2.0 * math.exp(-2.0 * group_count * deviation * deviation))


def _group_records(groups: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    return [groups[key] for key in sorted(groups)]


def diagnose_hash_table(
    table: TableTopology,
    logical_rows: Sequence[LogicalRow],
    router: Router,
    alpha: float = DEFAULT_ALPHA,
    *,
    max_exact_states: int = DEFAULT_MAX_EXACT_STATES,
    max_total_weight: int = DEFAULT_MAX_EXACT_TOTAL_WEIGHT,
) -> dict[str, Any]:
    """Build the full reviewable diagnostic for one table.

    The output exposes the denominator, exclusion counts, per-group weights
    with their Router-resolved buckets, the bucket_map-derived baseline
    probabilities, the exact event definition and the certificate (exact tail
    probability or conservative upper bound) with a three-state decision.
    """
    alpha = _validate_alpha(alpha)
    common = {
        "hypothesis": HYPOTHESIS_LABEL,
        "scope_note": SCOPE_NOTE,
        "alpha": alpha,
        "table": table.table,
    }
    if table.router != "hash":
        return {
            **common,
            "applicable": False,
            "reason": "router_not_hash",
            "router": table.router,
        }

    groups, exclusions, observed_loads = build_hash_key_groups(table, logical_rows, router)
    eligible_rows = sum(entry["weight"] for entry in groups.values())
    excluded_rows = sum(exclusions.values())
    baseline = shard_baseline_probabilities(table)
    shard_names = sorted(baseline)
    denominator = {
        "logical_rows": len(logical_rows),
        "eligible_hash_rows": eligible_rows,
        "excluded_rows": excluded_rows,
        "exclusions_by_reason": dict(sorted(exclusions.items())),
        "exclusion_contract": (
            "null/drop/default rows, routing failures and non-hash router "
            "decisions are excluded and are never counted as hash keys"
        ),
    }
    if not groups:
        return {
            **common,
            "applicable": False,
            "reason": "no_eligible_hash_groups",
            "denominator": denominator,
            "bucket_baseline": {
                "buckets": table.buckets,
                "bucket_map": list(table.bucket_map),
                "shard_probabilities": baseline,
            },
        }

    records = _group_records(groups)
    group_weights = [entry["weight"] for entry in records]
    # Normalise so the categorical distribution sums to exactly 1.0 even when
    # bucket-share floats do not; baseline dict keeps the reviewable raw shares.
    probability_values = [baseline[shard] for shard in shard_names]
    probability_sum = sum(probability_values)
    probability_tuple = tuple(value / probability_sum for value in probability_values)
    loads = {shard: observed_loads.get(shard, 0) for shard in shard_names}
    observed_max = max(loads.values())
    observed_max_shard = max(shard_names, key=lambda shard: loads[shard])
    event = {
        "kind": "max_shard_logical_row_load_at_least",
        "threshold": observed_max,
        "observed_max_load": observed_max,
        "observed_max_shard": observed_max_shard,
        "observed_shard_loads": loads,
        "definition": (
            "event = max over shards of logical-row load is at least the "
            "observed maximum load, evaluated over independent canonical key "
            "group draws (rows of one group always land together)"
        ),
    }
    budget = {
        "max_exact_states": max_exact_states,
        "max_total_weight": max_total_weight,
        "group_count": len(records),
        "total_weight": eligible_rows,
        "exact_attempted": True,
        "exact_completed": False,
        "peak_states": None,
        "fallback_reason": None,
    }

    try:
        tail, peak_states = exact_tail_probability(
            group_weights,
            probability_tuple,
            observed_max,
            max_states=max_exact_states,
            max_total_weight=max_total_weight,
        )
    except SkewBudgetExceeded as exceeded:
        budget["fallback_reason"] = exceeded.reason
        certificate = _conservative_certificate(
            group_weights, probability_tuple, observed_max, alpha, budget, exceeded
        )
    else:
        budget["exact_completed"] = True
        budget["peak_states"] = peak_states
        certificate = {
            "type": EXACT_CERTIFICATE,
            "alpha": alpha,
            "tail_probability": tail,
            "tail_probability_upper_bound": None,
            "decision": (
                DECISION_SIGNIFICANT if tail <= alpha else DECISION_NOT_SIGNIFICANT
            ),
            "decision_rule": (
                "exact group-weighted tail probability compared directly with alpha"
            ),
            "budget": budget,
        }

    return {
        **common,
        "applicable": True,
        "denominator": denominator,
        "key_groups": {
            "count": len(records),
            "total_weight": eligible_rows,
            "groups": records,
        },
        "bucket_baseline": {
            "buckets": table.buckets,
            "bucket_map": list(table.bucket_map),
            "shard_probabilities": baseline,
        },
        "event": event,
        "certificate": certificate,
    }


def _conservative_certificate(
    group_weights: Sequence[int],
    probability_tuple: tuple[float, ...],
    observed_max: int,
    alpha: float,
    budget: dict[str, Any],
    exceeded: SkewBudgetExceeded,
) -> dict[str, Any]:
    """Build the fallback certificate; never fake an exact probability."""
    try:
        bound = conservative_hoeffding_bound(group_weights, probability_tuple, observed_max)
    except SkewCertificateUnavailable as unavailable:
        return {
            "type": HOEFFDING_CERTIFICATE,
            "alpha": alpha,
            "tail_probability": None,
            "tail_probability_upper_bound": None,
            "decision": DECISION_UNKNOWN,
            "decision_rule": (
                "exact enumeration exceeded its budget and no conservative "
                "bound applies to this input shape"
            ),
            "budget": {**budget, "bound_unavailable_reason": unavailable.reason},
            "unknown_reason": f"budget:{exceeded.reason};bound:{unavailable.reason}",
        }
    return {
        "type": HOEFFDING_CERTIFICATE,
        "alpha": alpha,
        "tail_probability": None,
        "tail_probability_upper_bound": bound,
        "decision": (
            DECISION_SIGNIFICANT if bound <= alpha else DECISION_UNKNOWN
        ),
        "decision_rule": (
            "strict upper bound below alpha proves significance; an upper "
            "bound above alpha cannot decide, so the verdict is unknown"
        ),
        "budget": budget,
        "unknown_reason": None if bound <= alpha else "upper_bound_above_alpha",
    }
