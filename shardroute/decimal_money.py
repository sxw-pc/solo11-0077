"""Exact decimal money parsing and cent-scale integer arithmetic.

Every amount in ShardRoute is handled as an original textual lexeme plus an
exact :class:`decimal.Decimal`; binary ``float`` is never used.  Physical
amounts must be fixed-point money with at most two fractional digits and are
represented as integer cents.  Logical amounts may carry arbitrary precision
and are rounded to cents with banker's rounding (half to even) per declared
contract.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_EVEN, localcontext

# A strict fixed-point lexical grammar: optional sign, digits, optional
# decimal point with at least one digit on each side once a point appears.
# Exponents, whitespace, underscores, NaN and Infinity are all rejected so a
# value can never pass through a binary or scientific transcription.
_DECIMAL_RE = re.compile(r"^[+-]?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$")

CENT = Decimal("0.01")
CENT_PLACES = 2


@dataclass(frozen=True)
class DecimalLexeme:
    """An amount captured exactly as written in the source file."""

    lexeme: str
    value: Decimal
    source: str
    line_no: int


def parse_decimal_lexeme(lexeme: Any, source: str = "", line_no: int = 0) -> DecimalLexeme:
    """Parse an original CSV/JSONL amount token without using ``float``.

    JSONL numeric tokens arrive as their exact JSON number text (the scanner
    installs a ``parse_float=str`` hook); CSV cells are strings already.
    """

    if isinstance(lexeme, bool) or not isinstance(lexeme, str):
        raise ValueError("amount must be a decimal text token")
    text = lexeme
    if not _DECIMAL_RE.match(text):
        raise ValueError(f"illegal decimal lexeme {lexeme!r}")
    try:
        with localcontext() as context:
            context.prec = 60
            value = Decimal(text)
    except InvalidOperation as exc:  # pragma: no cover - grammar guards this
        raise ValueError(f"illegal decimal lexeme {lexeme!r}") from exc
    return DecimalLexeme(text, value, source, line_no)


def is_negative(decimal: Decimal) -> bool:
    return decimal.is_signed() and decimal != 0


def decimal_places(decimal: Decimal) -> int:
    exponent = decimal.as_tuple().exponent
    return max(0, -int(exponent))


def quantize_cents(decimal: Decimal) -> int:
    """Round an exact decimal to integer cents using half-to-even."""

    with localcontext() as context:
        context.prec = 60
        rounded = decimal.quantize(CENT, rounding=ROUND_HALF_EVEN)
    return int(rounded.scaleb(CENT_PLACES))


def adjacent_cents(decimal: Decimal) -> tuple[int, int]:
    """Return the two cent values bracketing a logical amount.

    For an exact two-cent value both endpoints are equal.  Otherwise the
    tuple is ``(floor_cent, ceil_cent)`` in ascending order.
    """

    with localcontext() as context:
        context.prec = 60
        scaled = decimal * 100
    if scaled == scaled.to_integral_value():
        exact = int(scaled)
        return exact, exact
    floor = int(scaled.to_integral_value(rounding="ROUND_FLOOR"))
    return floor, floor + 1


def to_cents_exact(decimal: Decimal) -> int:
    """Convert a physical money decimal that must already be cent exact."""

    if decimal_places(decimal) > CENT_PLACES:
        raise ValueError("physical amount carries sub-cent precision")
    return int(decimal.scaleb(CENT_PLACES))


def format_cents(cents: int) -> str:
    """Render integer cents as a fixed two-place money string (no float)."""

    sign = "-" if cents < 0 else ""
    magnitude = abs(cents)
    return f"{sign}{magnitude // 100}.{magnitude % 100:02d}"


def sum_decimals(values: list[Decimal]) -> Decimal:
    with localcontext() as context:
        context.prec = 60
        total = Decimal(0)
        for value in values:
            total += value
        return total
