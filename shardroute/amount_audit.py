"""Read-only amount content-fidelity audit for correctly placed rows.

Placement correctness (row_id -> expected shard, no missing/duplicate/misplaced
rows) is a *precondition*: only rows whose identity is proven are admitted to
any amount total.  Two independently authorized precision contracts are
supported:

* ``row_half_even`` - every physical row must equal the half-to-even cent
  rounding of its exact logical amount.
* ``dual_margin`` - each row may pick either cent adjacent to its logical
  amount, but every per-tenant and per-channel physical margin must equal the
  half-to-even rounding of that group's exact logical sum.

All arithmetic is exact (decimal lexemes -> integer cents); binary float is
never used.  Nothing here writes a file or emits a migration action.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from .canonical import fingerprint, stable_id
from .decimal_money import (
    adjacent_cents,
    format_cents,
    is_negative,
    parse_decimal_lexeme,
    quantize_cents,
    sum_decimals,
    to_cents_exact,
)
from .errors import ShardRouteError
from .models import (
    AMOUNT_CONTRACT_DUAL_MARGIN,
    AMOUNT_CONTRACT_ROW_HALF_EVEN,
    AmountContract,
    LogicalRow,
    PhysicalRow,
    ScanResult,
    Topology,
    make_finding,
)
from .router import Router

# Finding codes.  Data-quality codes describe why a row cannot be certified at
# all; contract codes describe contract-specific failures.  They stay separate
# from the placement codes (MISSING_ROW, DUPLICATE_ROW, MISPLACED_ROW ...) so
# an amount discrepancy is never disguised as a routing error.
AMOUNT_DECIMAL_INVALID = "AMOUNT_DECIMAL_INVALID"
AMOUNT_NEGATIVE_AMOUNT = "AMOUNT_NEGATIVE_AMOUNT"
AMOUNT_OVER_PRECISION = "AMOUNT_OVER_PRECISION"
AMOUNT_FIELD_UNTRUSTED = "AMOUNT_FIELD_UNTRUSTED"
AMOUNT_ROW_HALF_EVEN_MISMATCH = "AMOUNT_ROW_HALF_EVEN_MISMATCH"
AMOUNT_DUAL_MARGIN_VIOLATION = "AMOUNT_DUAL_MARGIN_VIOLATION"
AMOUNT_DUAL_MARGIN_INFEASIBLE = "AMOUNT_DUAL_MARGIN_INFEASIBLE"
AMOUNT_DUAL_MARGIN_UNPROVEN = "AMOUNT_DUAL_MARGIN_UNPROVEN"

VERDICT_PASS = "pass"
VERDICT_FAIL = "fail"
VERDICT_UNKNOWN = "unknown"

# Exclusion reasons shared by both contracts.
EX_DROPPED = "dropped_row"
EX_ROUTE_ERROR = "route_error"
EX_MISSING = "missing_row"
EX_DUPLICATE = "duplicate_row"
EX_MISPLACED = "misplaced_row"
EX_FIELD_UNTRUSTED = "identity_field_untrusted"
EX_AMOUNT_INVALID = "amount_untrusted"


@dataclass(frozen=True)
class _Amount:
    lexeme: str
    decimal: Decimal
    cents: int | None  # physical only; None when not cent-exact


@dataclass
class _RowState:
    row_id: str
    eligible: bool
    reason: str
    logical: LogicalRow | None = None
    physical: PhysicalRow | None = None
    logical_amount: _Amount | None = None
    physical_amount: _Amount | None = None
    tenant: str = ""
    channel: str = ""
    allowed_cents: tuple[int, ...] = ()
    expected_half_even_cents: int | None = None
    detail: dict[str, Any] = field(default_factory=dict)


def _scalar_lexeme(value: Any) -> str:
    """Return the exact textual token for an amount cell.

    CSV cells are strings already.  A JSONL integer arrives as an ``int`` and
    its ``str()`` form is its exact lexeme.  A ``float`` here would mean the
    scanner lost lexical precision, which is rejected rather than transcribed.
    """

    if isinstance(value, bool):
        raise ValueError("amount must be a decimal text token, not boolean")
    if isinstance(value, str):
        return value
    if isinstance(value, int):
        return str(value)
    raise ValueError("amount must be a decimal text token")


def _parse_amount(value: Any, source: str, line_no: int, *, physical: bool) -> _Amount:
    lexeme = _scalar_lexeme(value)
    parsed = parse_decimal_lexeme(lexeme, source, line_no)
    cents: int | None
    if physical:
        cents = to_cents_exact(parsed.value)  # raises on sub-cent precision
    else:
        cents = None
    return _Amount(lexeme, parsed.value, cents)


def _identity_equal(logical_value: Any, physical_value: Any) -> bool:
    """Prove a grouping/identity field means the same entity on both sides."""

    if logical_value is None or physical_value is None:
        return False
    if isinstance(logical_value, bool) or isinstance(physical_value, bool):
        return logical_value is physical_value
    if isinstance(logical_value, str) and isinstance(physical_value, str):
        return logical_value.strip() == physical_value.strip()
    if isinstance(logical_value, (int, float)) and isinstance(physical_value, (int, float)):
        if isinstance(logical_value, float) or isinstance(physical_value, float):
            return False  # no float transcription in an identity proof
        return logical_value == physical_value
    return False


def _placement_map(topology: Topology, scan: ScanResult, table: str) -> dict[str, dict[str, Any]]:
    """Classify every logical row's physical placement, reusing router traces."""

    router = Router(topology)
    by_id: dict[str, list[PhysicalRow]] = {}
    for row in scan.physical_rows.get(table, []):
        by_id.setdefault(row.row_id, []).append(row)
    result: dict[str, dict[str, Any]] = {}
    for logical in scan.logical_rows.get(table, []):
        try:
            trace = router.route_values(table, logical.values)
        except ShardRouteError as exc:
            result[logical.row_id] = {"state": EX_ROUTE_ERROR, "error": exc}
            continue
        if trace.dropped:
            result[logical.row_id] = {"state": EX_DROPPED}
            continue
        places = sorted(by_id.get(logical.row_id, []), key=lambda p: (p.shard, p.source, p.line_no))
        if not places:
            result[logical.row_id] = {"state": EX_MISSING, "expected": trace.target_shard}
        elif len(places) > 1:
            result[logical.row_id] = {"state": EX_DUPLICATE, "places": places}
        elif places[0].shard != trace.target_shard:
            result[logical.row_id] = {"state": EX_MISPLACED, "place": places[0]}
        else:
            result[logical.row_id] = {"state": "ok", "place": places[0]}
    return result


def _build_row_states(
    topology: Topology, scan: ScanResult, table: str, contract: AmountContract
) -> tuple[list[_RowState], list[Any]]:
    placement = _placement_map(topology, scan, table)
    states: list[_RowState] = []
    findings: list[Any] = []

    def untrusted_finding(row_id: str, reason: str, detail: dict[str, Any]) -> None:
        # Row-level data quality is contract-independent; the finding must not
        # carry the contract name so the same fact is deduplicated when several
        # contracts are declared on one table.
        findings.append(
            make_finding(
                AMOUNT_FIELD_UNTRUSTED,
                "high",
                table,
                f"row cannot participate in amount totals: {reason}",
                row_id=row_id,
                detail=dict(detail),
            )
        )

    def amount_finding(code: str, row_id: str, message: str, detail: dict[str, Any]) -> None:
        findings.append(
            make_finding(
                code,
                "high",
                table,
                message,
                row_id=row_id,
                detail=dict(detail),
            )
        )

    for logical in scan.logical_rows.get(table, []):
        verdict = placement.get(logical.row_id, {"state": EX_MISSING})
        state = _RowState(row_id=logical.row_id, eligible=False, reason=verdict["state"], logical=logical)
        if verdict["state"] != "ok":
            states.append(state)
            continue
        physical = verdict["place"]
        state.physical = physical

        # Identity proof first: tenant/channel group fields must be present on
        # both sides and denote the same entity.
        identity: dict[str, Any] = {}
        identity_bad = ""
        for field_name in (contract.tenant_field, contract.channel_field):
            left = logical.values.get(field_name)
            right = physical.values.get(field_name)
            if field_name not in logical.values or field_name not in physical.values:
                identity_bad = f"missing group field {field_name}"
                identity[field_name] = {"present": False}
                break
            if not _identity_equal(left, right):
                identity_bad = f"group field {field_name} disagrees between logical and physical"
                identity[field_name] = {"logical": left, "physical": right}
                break
            identity[field_name] = left
        if identity_bad:
            state.reason = EX_FIELD_UNTRUSTED
            state.detail = identity
            untrusted_finding(logical.row_id, identity_bad, identity)
            states.append(state)
            continue
        state.tenant = str(identity[contract.tenant_field]).strip()
        state.channel = str(identity[contract.channel_field]).strip()

        # Exact amount parsing on both sides; no float, explicit reject reasons.
        amount_bad = False
        for side, row, physical_flag in (
            ("logical", logical, False),
            ("physical", physical, True),
        ):
            raw = row.values.get(contract.amount_field)
            if contract.amount_field not in row.values or raw is None:
                state.reason = EX_FIELD_UNTRUSTED
                untrusted_finding(
                    logical.row_id,
                    f"missing {side} amount field {contract.amount_field}",
                    {"side": side, "field": contract.amount_field},
                )
                amount_bad = True
                break
            try:
                amount = _parse_amount(raw, row.source, row.line_no, physical=physical_flag)
            except ValueError as exc:
                message = str(exc)
                if "sub-cent precision" in message:
                    amount_finding(
                        AMOUNT_OVER_PRECISION,
                        logical.row_id,
                        f"{side} amount {raw!r} carries sub-cent precision",
                        {"side": side, "lexeme": str(raw), "max_fraction_digits": 2},
                    )
                    state.reason = EX_AMOUNT_INVALID
                else:
                    amount_finding(
                        AMOUNT_DECIMAL_INVALID,
                        logical.row_id,
                        f"{side} amount {raw!r} is not a legal fixed-point decimal",
                        {"side": side, "lexeme": str(raw)},
                    )
                    state.reason = EX_AMOUNT_INVALID
                amount_bad = True
                break
            if is_negative(amount.decimal):
                amount_finding(
                    AMOUNT_NEGATIVE_AMOUNT,
                    logical.row_id,
                    f"{side} amount {amount.lexeme} is negative and cannot enter a money total",
                    {"side": side, "lexeme": amount.lexeme},
                )
                state.reason = EX_AMOUNT_INVALID
                amount_bad = True
                break
            if side == "logical":
                state.logical_amount = amount
            else:
                state.physical_amount = amount
        if amount_bad:
            states.append(state)
            continue

        assert state.logical_amount is not None and state.physical_amount is not None
        low, high = adjacent_cents(state.logical_amount.decimal)
        state.allowed_cents = (low, high) if low != high else (low,)
        state.expected_half_even_cents = quantize_cents(state.logical_amount.decimal)
        state.eligible = True
        state.reason = "eligible"
        states.append(state)

    return states, findings


def _row_record(state: _RowState) -> dict[str, Any]:
    record: dict[str, Any] = {
        "row_id": state.row_id,
        "eligible": state.eligible,
        "reason": state.reason,
    }
    if state.logical is not None:
        record["logical_source"] = {"path": state.logical.source, "line": state.logical.line_no}
    if state.physical is not None:
        record["physical_source"] = {"path": state.physical.source, "line": state.physical.line_no}
    if state.logical_amount is not None:
        record["logical_lexeme"] = state.logical_amount.lexeme
    if state.physical_amount is not None and state.physical_amount.cents is not None:
        record["physical_lexeme"] = state.physical_amount.lexeme
        record["physical_cents"] = state.physical_amount.cents
    if state.eligible:
        record.update(
            {
                "tenant_id": state.tenant,
                "channel": state.channel,
                "allowed_cents": list(state.allowed_cents),
                "half_even_cents": state.expected_half_even_cents,
            }
        )
    return record


def _scanner_evidence(
    states: list[_RowState], contract: AmountContract
) -> dict[str, Any]:
    """Exact lexical provenance: every amount token with its source location."""

    tokens = []
    for state in states:
        entry: dict[str, Any] = {"row_id": state.row_id}
        if state.logical is not None:
            entry["logical"] = {
                "path": state.logical.source,
                "line": state.logical.line_no,
                "field": contract.amount_field,
                "lexeme": state.logical.values.get(contract.amount_field),
            }
        if state.physical is not None:
            entry["physical"] = {
                "path": state.physical.source,
                "line": state.physical.line_no,
                "field": contract.amount_field,
                "lexeme": state.physical.values.get(contract.amount_field),
            }
        tokens.append(entry)
    return {"tokens": tokens, "fingerprint": fingerprint(tokens)}


# ---------------------------------------------------------------------------
# row_half_even contract
# ---------------------------------------------------------------------------

def _audit_row_half_even(table: str, contract: AmountContract, states: list[_RowState]) -> dict[str, Any]:
    eligible = [state for state in states if state.eligible]
    mismatches: list[dict[str, Any]] = []
    for state in eligible:
        assert state.physical_amount is not None and state.expected_half_even_cents is not None
        if state.physical_amount.cents != state.expected_half_even_cents:
            mismatches.append(
                {
                    "row_id": state.row_id,
                    "logical_lexeme": state.logical_amount.lexeme,
                    "physical_lexeme": state.physical_amount.lexeme,
                    "expected_cents": state.expected_half_even_cents,
                    "expected_amount": format_cents(state.expected_half_even_cents),
                    "actual_cents": state.physical_amount.cents,
                    "actual_amount": state.physical_amount.lexeme,
                    "physical_source": {"path": state.physical.source, "line": state.physical.line_no},
                }
            )
    findings = [
        make_finding(
            AMOUNT_ROW_HALF_EVEN_MISMATCH,
            "high",
            table,
            (
                f"physical amount {item['physical_lexeme']} does not equal half-even rounding "
                f"{item['expected_amount']} of logical amount {item['logical_lexeme']}"
            ),
            row_id=item["row_id"],
            detail={"contract": contract.contract, **{k: v for k, v in item.items() if k != "row_id"}},
        )
        for item in mismatches
    ]
    status = VERDICT_FAIL if mismatches else (VERDICT_UNKNOWN if states != eligible else VERDICT_PASS)
    if mismatches and states != eligible:
        # Failures among certified rows are reported, but the contract as a
        # whole cannot be settled while some rows were excluded.
        status = VERDICT_UNKNOWN
    allowance = [
        {
            "row_id": state.row_id,
            "logical_lexeme": state.logical_amount.lexeme,
            "half_even_cents": state.expected_half_even_cents,
            "physical_cents": state.physical_amount.cents,
        }
        for state in eligible
    ]
    return {
        "contract": contract.contract,
        "table": table,
        "fields": contract.as_dict(),
        "status": status,
        "eligible_rows": len(eligible),
        "ineligible_rows": len(states) - len(eligible),
        "rows": [_row_record(state) for state in states],
        "mismatches": mismatches,
        "allowance_fingerprint": fingerprint(allowance),
        "findings": findings,
        "repair": None,
    }


# ---------------------------------------------------------------------------
# dual_margin contract + bounded minimum-change witness solver
# ---------------------------------------------------------------------------

@dataclass
class _Var:
    row_id: str
    tenant: str
    channel: str
    options: tuple[int, ...]
    current: int
    logical_lexeme: str
    physical_lexeme: str
    source: dict[str, Any]


def _margin_table(states: list[_RowState], dimension_field: str) -> list[dict[str, Any]]:
    groups: dict[str, list[_RowState]] = {}
    for state in states:
        groups.setdefault(getattr(state, dimension_field), []).append(state)
    table_rows = []
    for key in sorted(groups):
        members = groups[key]
        logical_sum = sum_decimals([state.logical_amount.decimal for state in members])  # type: ignore[union-attr]
        expected = quantize_cents(logical_sum)
        actual = sum(state.physical_amount.cents for state in members)  # type: ignore[union-attr]
        min_possible = sum(state.allowed_cents[0] for state in members)
        max_possible = sum(state.allowed_cents[-1] for state in members)
        table_rows.append(
            {
                "dimension": dimension_field,
                "key": key,
                "row_ids": [state.row_id for state in members],
                "logical_exact_sum": format(logical_sum, "f"),
                "expected_cents": expected,
                "expected_amount": format_cents(expected),
                "actual_cents": actual,
                "actual_amount": format_cents(actual),
                "min_cents": min_possible,
                "max_cents": max_possible,
                "matches": actual == expected,
            }
        )
    return table_rows


def _solve_witness(variables: list[_Var], tenant_target: dict[str, int], channel_target: dict[str, int],
                   budget: int) -> dict[str, Any]:
    """Branch-and-bound search for the minimum-change feasible cent assignment.

    Deterministic: variables are visited in row_id order, option values in
    ascending order.  ``budget`` bounds the number of search nodes; when the
    tree is not fully explored the result is UNKNOWN, never a repair claim.
    """

    variables = sorted(variables, key=lambda var: var.row_id)
    nodes = 0
    exhausted = True
    best: list[int] | None = None
    best_cost: int | None = None

    # Remaining per-group option min/max over unvisited suffixes, for pruning.
    # Each position is computed independently so a group that disappears from
    # a suffix is simply absent from that position's dicts; the forward check
    # then treats only genuinely absent groups as having an empty window.
    tenants = sorted(tenant_target)
    channels = sorted(channel_target)
    suffix_min_t: list[dict[str, int]] = [{} for _ in range(len(variables) + 1)]
    suffix_max_t: list[dict[str, int]] = [{} for _ in range(len(variables) + 1)]
    suffix_min_c: list[dict[str, int]] = [{} for _ in range(len(variables) + 1)]
    suffix_max_c: list[dict[str, int]] = [{} for _ in range(len(variables) + 1)]
    for start in range(len(variables)):
        for var in variables[start:]:
            suffix_min_t[start][var.tenant] = (
                suffix_min_t[start].get(var.tenant, 0) + var.options[0]
            )
            suffix_max_t[start][var.tenant] = (
                suffix_max_t[start].get(var.tenant, 0) + var.options[-1]
            )
            suffix_min_c[start][var.channel] = (
                suffix_min_c[start].get(var.channel, 0) + var.options[0]
            )
            suffix_max_c[start][var.channel] = (
                suffix_max_c[start].get(var.channel, 0) + var.options[-1]
            )

    chosen: list[int] = [0] * len(variables)
    sum_t: dict[str, int] = {key: 0 for key in tenants}
    sum_c: dict[str, int] = {key: 0 for key in channels}

    def visit(index: int, cost: int) -> None:
        nonlocal nodes, exhausted, best, best_cost
        nodes += 1
        if nodes > budget:
            exhausted = False
            return
        if best_cost is not None and cost >= best_cost:
            return
        if index == len(variables):
            if all(sum_t[key] == tenant_target[key] for key in tenants) and all(
                sum_c[key] == channel_target[key] for key in channels
            ):
                best = list(chosen)
                best_cost = cost
            return
        var = variables[index]
        # Forward-check at entry: the suffix window covers every row from
        # this position onward (the current row has not been chosen yet), so
        # a group absent from suffix[index] truly has no remaining row and its
        # current sum must already equal the target.  Final equality is also
        # re-checked at leaves.
        rest_min_t, rest_max_t = suffix_min_t[index], suffix_max_t[index]
        rest_min_c, rest_max_c = suffix_min_c[index], suffix_max_c[index]
        for key in tenants:
            low = sum_t[key] + rest_min_t.get(key, 0)
            high = sum_t[key] + rest_max_t.get(key, 0)
            if not low <= tenant_target[key] <= high:
                return
        for key in channels:
            low = sum_c[key] + rest_min_c.get(key, 0)
            high = sum_c[key] + rest_max_c.get(key, 0)
            if not low <= channel_target[key] <= high:
                return
        for cent in var.options:
            chosen[index] = cent
            sum_t[var.tenant] += cent
            sum_c[var.channel] += cent
            visit(index + 1, cost + (cent != var.current))
            sum_t[var.tenant] -= cent
            sum_c[var.channel] -= cent
            if not exhausted:
                return

    visit(0, 0)
    if not exhausted:
        return {
            "status": "budget_exceeded",
            "nodes_used": nodes,
            "budget": budget,
            "message": "search exhausted the explicit node budget before optimality could be proven",
        }
    if best is None:
        return {"status": "infeasible", "nodes_used": nodes, "budget": budget}
    changes = []
    for var, cent in zip(variables, best):
        if cent != var.current:
            changes.append(
                {
                    "row_id": var.row_id,
                    "tenant_id": var.tenant,
                    "channel": var.channel,
                    "from_cents": var.current,
                    "from_amount": format_cents(var.current),
                    "to_cents": cent,
                    "to_amount": format_cents(cent),
                    "allowed_cents": list(var.options),
                    "logical_lexeme": var.logical_lexeme,
                    "physical_source": var.source,
                }
            )
    witness_margins_t = {key: 0 for key in tenants}
    witness_margins_c = {key: 0 for key in channels}
    for var, cent in zip(variables, best):
        witness_margins_t[var.tenant] += cent
        witness_margins_c[var.channel] += cent
    return {
        "status": "witness",
        "nodes_used": nodes,
        "budget": budget,
        "minimum_proven": True,
        "changed_rows": len(changes),
        "changes": changes,
        "witness_margins": {
            "tenant": [
                {"key": key, "cents": witness_margins_t[key], "expected_cents": tenant_target[key]}
                for key in tenants
            ],
            "channel": [
                {"key": key, "cents": witness_margins_c[key], "expected_cents": channel_target[key]}
                for key in channels
            ],
        },
    }


def _infeasibility_evidence(
    variables: list[_Var],
    tenant_target: dict[str, int],
    channel_target: dict[str, int],
    margins: list[dict[str, Any]],
    search: dict[str, Any],
) -> dict[str, Any]:
    """Numerically checkable proof that no allowed assignment can satisfy."""

    bound_violations = [
        {
            "dimension": row["dimension"],
            "key": row["key"],
            "target_cents": row["expected_cents"],
            "min_cents": row["min_cents"],
            "max_cents": row["max_cents"],
        }
        for row in margins
        if not row["min_cents"] <= row["expected_cents"] <= row["max_cents"]
    ]
    tenant_total = sum(tenant_target.values())
    channel_total = sum(channel_target.values())
    fixed_total = sum(var.options[0] for var in variables if len(var.options) == 1)
    return {
        "bound_violations": bound_violations,
        "partition_totals": {
            "tenant_target_total_cents": tenant_total,
            "channel_target_total_cents": channel_total,
            "partitions_agree": tenant_total == channel_total,
            "fixed_row_total_cents": fixed_total,
        },
        "exhausted_search": {
            "nodes_used": search.get("nodes_used"),
            "budget": search.get("budget"),
            "variable_order": [var.row_id for var in variables],
        },
        "replay": "re-run the bounded solver with the same variable order and budget",
    }


def _audit_dual_margin(table: str, contract: AmountContract, states: list[_RowState]) -> dict[str, Any]:
    eligible = [state for state in states if state.eligible]
    margins = _margin_table(eligible, "tenant") + _margin_table(eligible, "channel")
    findings: list[Any] = []
    violations = [row for row in margins if not row["matches"]]
    for row in violations:
        findings.append(
            make_finding(
                AMOUNT_DUAL_MARGIN_VIOLATION,
                "high",
                table,
                (
                    f"{row['dimension']} {row['key']!r} physical margin {row['actual_amount']} "
                    f"does not match required {row['expected_amount']}"
                ),
                detail={"contract": contract.contract, **{k: v for k, v in row.items() if k != "dimension"}},
            )
        )

    variables = [
        _Var(
            row_id=state.row_id,
            tenant=state.tenant,
            channel=state.channel,
            options=state.allowed_cents,
            current=state.physical_amount.cents,  # type: ignore[union-attr]
            logical_lexeme=state.logical_amount.lexeme,  # type: ignore[union-attr]
            physical_lexeme=state.physical_amount.lexeme,  # type: ignore[union-attr]
            source={"path": state.physical.source, "line": state.physical.line_no},
        )
        for state in eligible
    ]
    tenant_target = {row["key"]: row["expected_cents"] for row in margins if row["dimension"] == "tenant"}
    channel_target = {row["key"]: row["expected_cents"] for row in margins if row["dimension"] == "channel"}
    allowance = [
        {
            "row_id": state.row_id,
            "logical_lexeme": state.logical_amount.lexeme,
            "allowed_cents": list(state.allowed_cents),
            "physical_cents": state.physical_amount.cents,
        }
        for state in eligible
    ]

    if not violations:
        repair: dict[str, Any] | None = None
        status = VERDICT_PASS if states == eligible else VERDICT_UNKNOWN
    else:
        search = _solve_witness(variables, tenant_target, channel_target, contract.repair_budget)
        if search["status"] == "witness":
            repair = search
            # Margins fail among the admitted rows; excluded rows still keep
            # the overall verdict UNKNOWN rather than a clean fail.
            status = VERDICT_FAIL if states == eligible else VERDICT_UNKNOWN
        elif search["status"] == "infeasible":
            repair = {
                "status": "infeasible",
                "evidence": _infeasibility_evidence(variables, tenant_target, channel_target, margins, search),
            }
            findings.append(
                make_finding(
                    AMOUNT_DUAL_MARGIN_INFEASIBLE,
                    "high",
                    table,
                    "no selection of per-row adjacent cent values can satisfy both cross margins",
                    detail={
                        "contract": contract.contract,
                        "evidence": repair["evidence"],
                    },
                )
            )
            status = VERDICT_FAIL if states == eligible else VERDICT_UNKNOWN
        else:
            repair = search
            findings.append(
                make_finding(
                    AMOUNT_DUAL_MARGIN_UNPROVEN,
                    "high",
                    table,
                    "margin repair could not be decided within the explicit search budget",
                    detail={"contract": contract.contract, **search},
                )
            )
            status = VERDICT_UNKNOWN
    return {
        "contract": contract.contract,
        "table": table,
        "fields": contract.as_dict(),
        "status": status,
        "eligible_rows": len(eligible),
        "ineligible_rows": len(states) - len(eligible),
        "rows": [_row_record(state) for state in states],
        "margins": margins,
        "allowance_fingerprint": fingerprint(allowance),
        "findings": findings,
        "repair": repair,
    }


def audit_amount_contracts(topology: Topology, scan: ScanResult) -> tuple[list[dict[str, Any]], list[Any]]:
    """Evaluate every declared amount contract; returns (verdicts, findings)."""

    verdicts: list[dict[str, Any]] = []
    findings: list[Any] = []
    for table in topology.table_names():
        contracts = topology.table(table).amount_contracts
        if not contracts:
            continue
        for contract in contracts:
            states, quality_findings = _build_row_states(topology, scan, table, contract)
            findings.extend(quality_findings)
            if contract.contract == AMOUNT_CONTRACT_ROW_HALF_EVEN:
                verdict = _audit_row_half_even(table, contract, states)
            elif contract.contract == AMOUNT_CONTRACT_DUAL_MARGIN:
                verdict = _audit_dual_margin(table, contract, states)
            else:  # pragma: no cover - configuration validation guards this
                continue
            scanner_evidence = _scanner_evidence(states, contract)
            margin_payload = verdict.get("margins")
            verdict["evidence_chain"] = {
                "scanner": scanner_evidence,
                "scanner_fingerprint": scanner_evidence["fingerprint"],
                "allowance_fingerprint": verdict["allowance_fingerprint"],
                "margin_fingerprint": fingerprint(margin_payload) if margin_payload is not None else "",
            }
            verdict["verdict_fingerprint"] = stable_id(
                "amt",
                table,
                contract.contract,
                verdict["status"],
                verdict["evidence_chain"]["scanner_fingerprint"],
                verdict["evidence_chain"]["allowance_fingerprint"],
                verdict["evidence_chain"]["margin_fingerprint"],
                verdict.get("repair"),
            )
            findings.extend(verdict.pop("findings"))
            verdicts.append(verdict)
    return verdicts, findings
