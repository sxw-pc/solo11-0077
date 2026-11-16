"""Shared parsing and result helpers for query explanations.

Two input dialects are supported:

* the legacy flat ``filters`` list, which is an implicit conjunction of atomic
  predicates and keeps its original behaviour and result shape;
* an explicit boolean expression supplied in the ``where`` field, built from
  ``and`` / ``or`` / ``not`` nodes and the same atomic predicates.

Atomic predicates are evaluated with SQL-style three-valued logic (3VL): a
predicate over a missing/``null`` field yields ``unknown`` and only rows whose
whole expression evaluates to ``true`` are considered matches.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .canonical import normalize_value
from .errors import QUERY_INVALID, QueryError
from .models import QueryExplanation, QueryFilter, TableTopology, Topology

# Legacy predicates accepted inside a flat ``filters`` list.
OPS = {"eq", "in", "gte", "gt", "lte", "lt", "ne", "is_null", "not_null"}
RANGE_OPS = {"eq", "in", "gte", "gt", "lte", "lt", "is_null"}
# Predicates accepted inside an explicit boolean ``where`` expression.
ATOM_OPS = OPS | {"not_in"}
BOOLEANS = {"and", "or", "not"}# Default cap on the number of DNF branches the exact solver is allowed to
# enumerate. Queries may override this with ``branch_budget``.
DEFAULT_BRANCH_BUDGET = 64
# Hard defensive ceiling so a pathological expression cannot blow up memory
# even when a caller configures a very large explicit budget.
HARD_BRANCH_CAP = 100_000
TRUE, FALSE, UNKNOWN = True, False, None

def _parse(topology: Topology, query: Any) -> tuple[str, str, str, list[QueryFilter]]:
    if not isinstance(query, dict):
        raise QueryError(QUERY_INVALID, "query must be a JSON object")
    table = query.get("table")
    if not isinstance(table, str) or table not in topology.tables:
        raise QueryError(QUERY_INVALID, "query references an unknown table", details={"table": table})
    name = query.get("name") if isinstance(query.get("name"), str) else table
    description = query.get("description") if isinstance(query.get("description"), str) else ""
    raw_filters = query.get("filters")
    if not isinstance(raw_filters, list) or not raw_filters:
        raise QueryError(QUERY_INVALID, "query must contain a non-empty filters list", table)
    filters: list[QueryFilter] = []
    for index, item in enumerate(raw_filters):
        location = f"filters[{index}]"
        if not isinstance(item, dict):
            raise QueryError(QUERY_INVALID, "filter must be an object", location)
        field_name, op = item.get("field"), item.get("op")
        if not isinstance(field_name, str) or not field_name:
            raise QueryError(QUERY_INVALID, "filter field must be a non-empty string", location)
        if op not in OPS:
            raise QueryError(QUERY_INVALID, f"unsupported filter op {op!r}", location)
        value = item.get("value")
        if op == "in" and not isinstance(value, list):
            raise QueryError(QUERY_INVALID, "in filter requires a list value", location)
        filters.append(QueryFilter(field_name, op, value))
    return table, name, description, filters

def _normalized(table: TableTopology, value: Any) -> Any:
    return normalize_value(value, table.case_sensitive)

def _key_filters(table: TableTopology, filters: list[QueryFilter]) -> list[QueryFilter]:
    return [item for item in filters if item.field in table.key_fields]

def _result(
    table: TableTopology,
    name: str,
    description: str,
    filters: list[QueryFilter],
    targeted: set[str],
    decision: str,
    reason: str,
    trace: dict[str, Any],
    *,
    force_not_prunable: bool = False,
) -> QueryExplanation:
    all_shards = table.table_shards()
    allowed = set(all_shards)
    targeted = {shard for shard in targeted if shard in allowed}
    ordered = [shard for shard in all_shards if shard in targeted]
    if force_not_prunable:
        decision, ordered = "not_prunable", list(all_shards)
    elif decision == "full_scan" or len(ordered) == len(all_shards):
        decision, ordered = "full_scan", list(all_shards)
    else:
        decision = "pruned"
    return QueryExplanation(
        name=name,
        table=table.table,
        decision=decision,
        reason=reason,
        key_fields=table.key_fields,
        all_shards=all_shards,
        targeted_shards=ordered,
        filters=filters,
        trace=trace,
        description=description,
    )


# ---------------------------------------------------------------------------
# Explicit boolean expression model (AND / OR / NOT over atomic predicates)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Atom:
    """An atomic predicate.

    Negation never lives on the atom itself: pushing a NOT down to a literal
    swaps the op for its 3VL inverse (``eq`` -> ``ne``, ``in`` -> ``not_in``,
    ``is_null`` -> ``not_null`` ...), so an atom always carries plain
    semantics.
    """

    field: str
    op: str
    value: Any = None

    def as_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"field": self.field, "op": self.op}
        if self.op not in {"is_null", "not_null"}:
            payload["value"] = self.value
        return payload

@dataclass(frozen=True)
class Not:
    child: "Expr"
    def as_dict(self) -> dict[str, Any]:
        return {"not": _node_dict(self.child)}

@dataclass(frozen=True)
class And:
    children: tuple["Expr", ...]
    def as_dict(self) -> dict[str, Any]:
        return {"and": [_node_dict(child) for child in self.children]}

@dataclass(frozen=True)
class Or:
    children: tuple["Expr", ...]
    def as_dict(self) -> dict[str, Any]:
        return {"or": [_node_dict(child) for child in self.children]}

Expr = Atom | And | Or | Not

def _node_dict(node: Expr) -> dict[str, Any]:
    return node.as_dict()

def _parse_atom(raw: Any, location: str) -> Atom:
    if not isinstance(raw, dict):
        raise QueryError(QUERY_INVALID, "predicate must be an object", location)
    field_name, op = raw.get("field"), raw.get("op")
    if not isinstance(field_name, str) or not field_name:
        raise QueryError(QUERY_INVALID, "predicate field must be a non-empty string", location)
    if op not in ATOM_OPS:
        raise QueryError(QUERY_INVALID, f"unsupported predicate op {op!r}", location)
    value = raw.get("value")
    if op in {"in", "not_in"}:
        if not isinstance(value, list):
            raise QueryError(QUERY_INVALID, f"{op} predicate requires a list value", location)
    elif op not in {"is_null", "not_null", "eq", "ne"} and value is None:
        raise QueryError(QUERY_INVALID, f"{op} predicate requires a non-null value", location)
    return Atom(field=field_name, op=op, value=value)

def _parse_node(raw: Any, location: str) -> Expr:
    if not isinstance(raw, dict):
        raise QueryError(QUERY_INVALID, "expression node must be an object", location)
    if "field" in raw:
        return _parse_atom(raw, location)
    keys = set(raw)
    boolean_keys = keys & BOOLEANS
    if len(boolean_keys) != 1 or len(keys) != 1:
        raise QueryError(
            QUERY_INVALID,
            "expression node must be exactly one of and/or/not or an atomic predicate",
            location,
        )
    kind = next(iter(boolean_keys))
    if kind == "not":
        return Not(_parse_node(raw["not"], f"{location}.not"))
    children_raw = raw[kind]
    if not isinstance(children_raw, list) or len(children_raw) < 2:
        raise QueryError(QUERY_INVALID, f"{kind} requires a list of at least two expressions", location)
    children = tuple(
        _parse_node(child, f"{location}.{kind}[{index}]")
        for index, child in enumerate(children_raw)
    )
    return And(children) if kind == "and" else Or(children)

def _parse_expression(raw: Any, location: str = "where") -> Expr:
    if not isinstance(raw, dict):
        raise QueryError(QUERY_INVALID, "where must be an expression object", location)
    if "field" in raw:
        return _parse_atom(raw, location)
    return _parse_node(raw, location)

def parse_expression(raw: Any) -> Expr:
    """Parse a serialized boolean expression (e.g. one stored in a trace)."""
    return _parse_expression(raw)

@dataclass(frozen=True)
class ParsedQuery:
    table: str
    name: str
    description: str
    expression: Expr
    filters: tuple[QueryFilter, ...]
    branch_budget: int

def _resolve_budget(query: dict[str, Any]) -> int:
    if "branch_budget" not in query:
        return DEFAULT_BRANCH_BUDGET
    budget = query["branch_budget"]
    if not isinstance(budget, int) or isinstance(budget, bool) or budget < 1:
        raise QueryError(QUERY_INVALID, "branch_budget must be a positive integer", "branch_budget")
    return min(budget, HARD_BRANCH_CAP)

def parse_boolean_query(topology: Topology, query: Any) -> ParsedQuery:
    """Validate a query carrying an explicit boolean ``where`` expression."""
    if not isinstance(query, dict):
        raise QueryError(QUERY_INVALID, "query must be a JSON object")
    table = query.get("table")
    if not isinstance(table, str) or table not in topology.tables:
        raise QueryError(QUERY_INVALID, "query references an unknown table", details={"table": table})
    if "filters" in query:
        raise QueryError(
            QUERY_INVALID,
            "query cannot mix the legacy filters list with a boolean where expression",
            table,
        )
    name = query.get("name") if isinstance(query.get("name"), str) else table
    description = query.get("description") if isinstance(query.get("description"), str) else ""
    expression = _parse_expression(query.get("where"))
    atoms = list(_iter_atoms(expression))
    filters = tuple(QueryFilter(atom.field, atom.op, atom.value) for atom in atoms)
    return ParsedQuery(table, name, description, expression, filters, _resolve_budget(query))


# ---------------------------------------------------------------------------
# Negation normalisation and disjunctive normal form (DNF)
# ---------------------------------------------------------------------------

_INVERSE = {
    "eq": "ne",
    "ne": "eq",
    "in": "not_in",
    "not_in": "in",
    "gte": "lt",
    "gt": "lte",
    "lte": "gt",
    "lt": "gte",
    "is_null": "not_null",
    "not_null": "is_null",
}

def invert_atom(atom: Atom) -> Atom:
    """Return the 3VL negation of an atomic predicate via an inverse op."""
    return Atom(atom.field, _INVERSE[atom.op], atom.value)

def _negate(node: Expr) -> Expr:
    if isinstance(node, Atom):
        return invert_atom(node)
    if isinstance(node, Not):
        return node.child
    if isinstance(node, And):
        return Or(tuple(_negate(child) for child in node.children))
    return And(tuple(_negate(child) for child in node.children))

def _nnf(node: Expr) -> Expr:
    if isinstance(node, Atom):
        return node
    if isinstance(node, Not):
        return _nnf(_negate(node.child))
    if isinstance(node, And):
        return And(tuple(_nnf(child) for child in node.children))
    return Or(tuple(_nnf(child) for child in node.children))

def _iter_atoms(node: Expr):
    if isinstance(node, Atom):
        yield node
        return
    if isinstance(node, Not):
        stack = [node.child]
    else:
        stack = list(node.children)
    while stack:
        current = stack.pop()
        if isinstance(current, Atom):
            yield current
        elif isinstance(current, Not):
            stack.append(current.child)
        else:
            stack.extend(current.children)

def _distribute(left: list[list[Literal]], right: list[list[Literal]]) -> list[list[Literal]]:
    return [a + b for a in left for b in right]

@dataclass(frozen=True)
class Literal:
    """A signed atomic predicate inside a DNF branch."""

    atom: Atom
    @property
    def field(self) -> str:
        return self.atom.field
    @property
    def op(self) -> str:
        return self.atom.op
    @property
    def value(self) -> Any:
        return self.atom.value
    def as_dict(self) -> dict[str, Any]:
        return self.atom.as_dict()

@dataclass(frozen=True)
class DnfResult:
    branches: tuple[tuple[Literal, ...], ...]
    precise: bool
    reason: str = ""
    raw_branch_estimate: int = 0

def _dnf_nnf(node: Expr, budget: int, hard_cap: int) -> tuple[list[list[Literal]] | None, int]:
    """Return DNF branches plus the current enumerated branch count.

    ``None`` branches mean expansion exceeded the budget and the caller must
    fall back to a conservative all-shard scan.
    """
    if isinstance(node, Atom):
        return [[Literal(node)]], 1
    if isinstance(node, And):
        combined: list[list[Literal]] = [[]]
        total = 0
        for child in node.children:
            child_branches, total = _dnf_nnf(child, budget, hard_cap)
            if child_branches is None or total > hard_cap:
                return None, total
            combined = _distribute(combined, child_branches)
            total = len(combined)
            if total > budget:
                return None, total
        return combined, total
    # OR: union the branches, still bounded by the configured budget.
    union: list[list[Literal]] = []
    total = 0
    for child in node.children:
        child_branches, total = _dnf_nnf(child, budget, hard_cap)
        if child_branches is None:
            return None, total
        union.extend(child_branches)
        total = len(union)
        if total > budget:
            return None, total
    return union, total

def expand_dnf(expression: Expr, budget: int) -> DnfResult:
    """Convert an expression to DNF branches within ``budget``.

    A branch is a conjunction (AND) of signed literals; the whole result is a
    disjunction (OR) of branches. When the exact branch count would exceed the
    budget, ``precise`` is False and the caller must conservatively scan every
    shard rather than silently dropping branches.
    """
    nnf = _nnf(expression)
    hard_cap = max(HARD_BRANCH_CAP, budget)
    branches, total = _dnf_nnf(nnf, budget, hard_cap)
    if branches is None:
        return DnfResult(
            branches=(),
            precise=False,
            reason=(
                f"boolean expression requires more than {budget} DNF branch(es); "
                "exact pruning cannot be proven within the branch budget"
            ),
            raw_branch_estimate=total,
        )
    normalized = tuple(tuple(branch) for branch in branches)
    return DnfResult(branches=normalized, precise=True, raw_branch_estimate=len(normalized))


# ---------------------------------------------------------------------------
# Three-valued logic (3VL) evaluation used for exhaustive safety checks
# ---------------------------------------------------------------------------

def eval_atom(atom: Atom, row: dict[str, Any], case_sensitive: bool = True) -> bool | None:
    """Evaluate one atomic predicate against a row using SQL 3VL.

    A field absent from the row is treated as SQL NULL. Comparisons against
    NULL yield ``unknown`` (represented by ``None``); only ``True`` matches.
    The atom's op already carries any negation (e.g. ``ne`` / ``not_in``), so
    no extra sign is applied.
    """
    present = atom.field in row
    raw = row.get(atom.field)
    if atom.op in {"is_null", "not_null"}:
        result = (not present) or (raw is None)
        if atom.op == "not_null":
            result = not result
        return result
    if raw is None:
        return UNKNOWN
    op = atom.op
    if op in {"eq", "ne"}:
        left = normalize_value(raw, case_sensitive)
        right = normalize_value(atom.value, case_sensitive)
        if right is None:
            return UNKNOWN
        result: bool | None = left == right
        if op == "ne":
            result = not result
    elif op in {"in", "not_in"}:
        left = normalize_value(raw, case_sensitive)
        members = {normalize_value(item, case_sensitive) for item in atom.value}
        known = members - {None}
        if None not in members:
            result = left in members
            if op == "not_in":
                result = not result
        elif op == "in":
            # x IN (known..., NULL): true on a known hit, otherwise unknown.
            result = True if left in known else UNKNOWN
        else:
            # x NOT IN (known..., NULL): false on a known hit, otherwise
            # unknown (the NULL member makes the test unprovable).
            result = False if left in known else UNKNOWN
    else:
        left = normalize_value(raw, case_sensitive)
        right = normalize_value(atom.value, case_sensitive)
        if right is None:
            return UNKNOWN
        if op == "gte":
            result = left >= right
        elif op == "gt":
            result = left > right
        elif op == "lte":
            result = left <= right
        else:  # lt
            result = left < right
    return result

def _not3(value: bool | None) -> bool | None:
    return UNKNOWN if value is UNKNOWN else not value

def _and3(left: bool | None, right: bool | None) -> bool | None:
    if left is FALSE or right is FALSE:
        return FALSE
    if left is UNKNOWN or right is UNKNOWN:
        return UNKNOWN
    return TRUE

def _or3(left: bool | None, right: bool | None) -> bool | None:
    if left is TRUE or right is TRUE:
        return TRUE
    if left is UNKNOWN or right is UNKNOWN:
        return UNKNOWN
    return FALSE

def eval_expression(node: Expr, row: dict[str, Any], case_sensitive: bool = True) -> bool | None:
    """Evaluate a boolean expression against a row, returning true/false/unknown."""
    if isinstance(node, Atom):
        return eval_atom(node, row, case_sensitive)
    if isinstance(node, Not):
        return _not3(eval_expression(node.child, row, case_sensitive))
    combiner = _and3 if isinstance(node, And) else _or3
    result = TRUE if isinstance(node, And) else FALSE
    for child in node.children:
        result = combiner(result, eval_expression(child, row, case_sensitive))
    return result
