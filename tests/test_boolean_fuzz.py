"""Randomized property tests: explanations must never miss a true row's shard.

Generates small finite domains and many random boolean expressions, evaluates
each expression with three-valued logic, routes every true row through the
real Router and asserts the explanation's targeted shards cover the true-row
shards -- for every router type and under both branch-budget routes.
"""

from __future__ import annotations

import random
import unittest

from shardroute.query_explain import explain_query
from shardroute.query_support import parse_expression

from test_boolean_explain import (
    actual_shards,
    assert_covers,
    composite_hash_topology,
    list_topology,
    range_gap_topology,
)


KEY_OPS = ["eq", "ne", "in", "not_in", "gte", "gt", "lte", "lt", "is_null", "not_null"]
NONKEY_OPS = ["eq", "ne", "is_null", "not_null"]


def _random_atom(rng: random.Random, field: str, values: list, ops: list[str]) -> dict:
    op = rng.choice(ops)
    if op in {"is_null", "not_null"}:
        return {"field": field, "op": op}
    if op in {"in", "not_in"}:
        size = rng.randint(0, 3)
        sample = rng.sample(values, min(size, len(values))) if values else []
        # Sprinkle SQL NULL members so three-valued IN/NOT IN paths are exercised.
        if rng.random() < 0.35 and None not in sample:
            sample = sample + [None]
        return {"field": field, "op": op, "value": sample}
    if op in {"eq", "ne"} and rng.random() < 0.2:
        return {"field": field, "op": op, "value": None}
    return {"field": field, "op": op, "value": rng.choice(values)}


def _random_expr(rng: random.Random, depth: int, key_values: list, tag_values: list) -> dict:
    if depth == 0 or rng.random() < 0.45:
        if rng.random() < 0.75:
            return _random_atom(rng, "key", key_values, KEY_OPS)
        atom = _random_atom(rng, "tag", tag_values, NONKEY_OPS)
        if rng.random() < 0.3:
            return {"not": atom}
        return atom
    kind = rng.choice(["and", "or", "not"])
    if kind == "not":
        return {"not": _random_expr(rng, depth - 1, key_values, tag_values)}
    children = [_random_expr(rng, depth - 1, key_values, tag_values)
                for _ in range(rng.randint(2, 3))]
    return {kind: children}


def _adapt(where: dict, key_field: str, tag_field: str) -> dict:
    if isinstance(where, dict):
        out = {}
        for k, v in where.items():
            if k in {"and", "or"}:
                out[k] = [_adapt(child, key_field, tag_field) for child in v]
            elif k == "not":
                out[k] = _adapt(v, key_field, tag_field)
            elif k == "field":
                out[k] = key_field if v == "key" else tag_field
            else:
                out[k] = v
        return out
    return where


class FuzzSafetyTests(unittest.TestCase):
    def _run_case(self, topology, table_name, domains, tag_values, key_values, rng, iterations):
        table = topology.table(table_name)
        for _ in range(iterations):
            if len(table.key_fields) > 1:
                where = self._composite_expr(rng)
            else:
                where_generic = _random_expr(rng, rng.randint(1, 4), key_values, tag_values)
                where = _adapt(where_generic, table.key_fields[0], "tag")
            budget = rng.choice([1, 2, 3, 8, 32, 64])
            query = {"table": table_name, "where": where, "branch_budget": budget}
            result = explain_query(topology, query)
            expression = parse_expression(where)
            actual, _ = actual_shards(
                topology, table_name, expression, domains, non_key_rows=tag_values
            )
            assert_covers(self, result, actual)
            if result.decision == "not_prunable":
                self.assertFalse(result.trace["precision"]["precise"])
            else:
                self.assertTrue(result.trace["precision"]["precise"])

    def _composite_expr(self, rng: random.Random) -> dict:
        parts = []
        for field, values in (("tenant", ["t1", "t2", "t3"]), ("kind", ["k1", "k2"])):
            op = rng.choice(["eq", "in", "ne", "is_null", "not_null"])
            if op in {"is_null", "not_null"}:
                parts.append({"field": field, "op": op})
            elif op == "in":
                pool = ["t1", "t2", "t3"] if field == "tenant" else ["k1", "k2"]
                sample = rng.sample(pool, rng.randint(1, len(pool)))
                if rng.random() < 0.3:
                    sample = sample + [None]
                parts.append({"field": field, "op": "in", "value": sample})
            else:
                parts.append({"field": field, "op": op, "value": rng.choice(values)})
        if rng.random() < 0.4:
            parts.append({"field": "tag", "op": "eq", "value": "x"})
        return {"and": parts} if len(parts) > 1 else parts[0]

    def test_fuzz_range_topology(self):
        topology = range_gap_topology()
        rng = random.Random(20261116)
        days = [None, "2023-12-31", "2024-01-01", "2024-02-15", "2024-03-01",
                "2024-04-15", "2024-06-01", "2024-08-31", "2024-09-01", "2025-01-01"]
        self._run_case(topology, "events", {"day": days}, [None, "x", "y"],
                       [d for d in days if d is not None], rng, 250)

    def test_fuzz_list_topology(self):
        topology = list_topology()
        rng = random.Random(20261117)
        regions = [None, "alpha", "beta", "gamma", "delta"]
        self._run_case(topology, "profiles", {"region": regions}, [None, "x", "y"],
                       [r for r in regions if r is not None], rng, 250)

    def test_fuzz_composite_hash_topology(self):
        topology = composite_hash_topology(null_policy="route_default")
        rng = random.Random(20261118)
        tenants = [None, "t1", "t2", "t3", "t4"]
        kinds = [None, "k1", "k2", "k3"]
        self._run_case(
            topology, "orders",
            {"tenant": tenants, "kind": kinds},
            [None, "x", "y"], ["t1", "t2", "t3"], rng, 250,
        )

    def test_fuzz_or_nesting_stays_within_budget_or_bails_safely(self):
        # A deliberately small budget forces the conservative route often.
        topology = range_gap_topology()
        rng = random.Random(42)
        days = [None, "2024-01-15", "2024-04-15", "2024-07-15", "2024-10-01"]
        for _ in range(120):
            where = {"or": [
                _random_expr(rng, 3, [d for d in days if d], [None, "x"]),
                _random_expr(rng, 3, [d for d in days if d], [None, "x"]),
            ]}
            where = _adapt(where, "day", "tag")
            result = explain_query(
                topology, {"table": "events", "where": where, "branch_budget": 4}
            )
            actual, _ = actual_shards(
                topology, "events", parse_expression(where), {"day": days},
                non_key_rows=[None, "x"],
            )
            assert_covers(self, result, actual)


if __name__ == "__main__":
    unittest.main()
