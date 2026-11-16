"""Typed range-key domains for ``router=range`` tables.

The default domain is ``text``, which keeps the historical lexicographic
comparison behaviour.  ``integer`` and ``date`` add an explicit, optional
domain whose parsing and comparison rules are shared by configuration
validation, the router, query explanation and the reshard planner.

All typed values are compared through an exact sort key:

* integer -> Python ``int`` (no floats are ever used)
* date    -> proleptic Gregorian ordinal (no datetime or timezone is involved)

Bounds remain left-closed / right-open.  The original representation of a
value is always retained so that errors and traces never lose it.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Any

RANGE_DOMAINS = ("text", "integer", "date")
RANGE_ALIAS_POLICIES = ("strict", "canonicalize")

INTEGER_MIN = -(2**63)
INTEGER_MAX = 2**63 - 1
# |INTEGER_MIN| and INTEGER_MAX both have 19 decimal digits.
_MAX_INTEGER_DIGITS = 19

_INTEGER_RE = re.compile(r"[+-]?[0-9]+\Z")
_DATE_RE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}\Z")


class RangeValueError(ValueError):
    """A typed range key could not be parsed under the declared domain."""

    def __init__(
        self,
        reason: str,
        message: str,
        raw: Any,
        domain: str,
        canonical: str | None = None,
    ) -> None:
        super().__init__(message)
        self.reason = reason
        self.message = message
        self.raw = raw
        self.domain = domain
        self.canonical = canonical

    def details(self) -> dict[str, Any]:
        details: dict[str, Any] = {
            "domain": self.domain,
            "raw_value": self.raw,
            "reason": self.reason,
        }
        if self.canonical is not None:
            details["canonical"] = self.canonical
        return details

    def as_dict(self) -> dict[str, Any]:
        return {"message": self.message, **self.details()}


@dataclass(frozen=True)
class RangeValue:
    """A parsed typed range key.

    ``raw`` keeps the exact input representation; ``canonical`` is the stable
    textual form used in traces and reports; ``key`` is the exact comparison
    key.
    """

    raw: Any
    canonical: str
    key: Any
    domain: str
    alias_used: bool = False

    def evidence(self) -> Any:
        """JSON-safe representation of the parsed business value."""
        return self.key if self.domain == "integer" else self.canonical

    def as_dict(self) -> dict[str, Any]:
        return {
            "raw_value": self.raw,
            "canonical": self.canonical,
            "parsed_value": self.evidence(),
            "domain": self.domain,
            "alias_used": self.alias_used,
        }


def _wrong_type(value: Any, domain: str, expected: str) -> RangeValueError:
    return RangeValueError(
        "wrong_type",
        f"{domain} range key must be {expected}, not {type(value).__name__}",
        value,
        domain,
    )


def _parse_integer(value: Any, alias_policy: str) -> RangeValue:
    alias = False
    if isinstance(value, bool):
        raise _wrong_type(value, "integer", "a base-10 integer or integer string")
    if isinstance(value, int):
        canonical = str(value)
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            raise RangeValueError(
                "empty", "integer range key must not be empty", value, "integer"
            )
        if not _INTEGER_RE.fullmatch(text):
            raise RangeValueError(
                "invalid_syntax",
                "integer range key must be a base-10 integer without fraction or exponent",
                value,
                "integer",
            )
        significant = text.lstrip("+-").lstrip("0") or "0"
        if len(significant) > _MAX_INTEGER_DIGITS:
            raise RangeValueError(
                "out_of_range",
                f"integer range key exceeds the signed 64-bit range "
                f"[{INTEGER_MIN}, {INTEGER_MAX}]",
                value,
                "integer",
            )
        number = int(text)
        canonical = str(number)
        # Surrounding whitespace is trimmed like the legacy text path; the
        # alias policy only distinguishes the numeric spelling itself.
        alias = text != canonical
        if alias and alias_policy == "strict":
            raise RangeValueError(
                "non_canonical",
                "non-canonical integer spelling rejected by range_alias_policy=strict",
                value,
                "integer",
                canonical,
            )
    else:
        raise _wrong_type(value, "integer", "a base-10 integer or integer string")

    number = int(canonical)
    if number < INTEGER_MIN or number > INTEGER_MAX:
        raise RangeValueError(
            "out_of_range",
            f"integer range key exceeds the signed 64-bit range "
            f"[{INTEGER_MIN}, {INTEGER_MAX}]",
            value,
            "integer",
            canonical,
        )
    return RangeValue(value, canonical, number, "integer", alias)


def _parse_date(value: Any, alias_policy: str) -> RangeValue:
    if not isinstance(value, str):
        raise _wrong_type(value, "date", "a strict ISO calendar date string")
    text = value.strip()
    if not text:
        raise RangeValueError("empty", "date range key must not be empty", value, "date")
    if not _DATE_RE.fullmatch(text):
        raise RangeValueError(
            "invalid_format",
            "date range key must use the strict ISO calendar form YYYY-MM-DD",
            value,
            "date",
        )
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise RangeValueError(
            "invalid_calendar_date",
            "date range key is not a valid calendar date",
            value,
            "date",
        ) from exc
    canonical = parsed.isoformat()
    alias = text != canonical
    if alias and alias_policy == "strict":
        raise RangeValueError(
            "non_canonical",
            "non-canonical date spelling rejected by range_alias_policy=strict",
            value,
            "date",
            canonical,
        )
    return RangeValue(value, canonical, parsed.toordinal(), "date", alias)


def parse_range_value(
    value: Any, domain: str, alias_policy: str = "strict"
) -> RangeValue:
    """Parse one value under a typed range domain.

    The caller is responsible for handling JSON null through ``null_policy``
    before calling this function.
    """
    if domain == "integer":
        return _parse_integer(value, alias_policy)
    if domain == "date":
        return _parse_date(value, alias_policy)
    raise ValueError(f"unsupported typed range domain: {domain!r}")


def rule_lower_key(rule: Any, domain: str, alias_policy: str) -> Any:
    """Return the parsed lower sort key for a configured rule."""
    if rule.lower_key is not None:
        return rule.lower_key
    return parse_range_value(rule.lower, domain, alias_policy).key


def rule_upper_key(rule: Any, domain: str, alias_policy: str) -> Any:
    """Return the parsed upper sort key, or ``None`` for an open interval."""
    if rule.upper is None:
        return None
    if rule.upper_key is not None:
        return rule.upper_key
    return parse_range_value(rule.upper, domain, alias_policy).key
