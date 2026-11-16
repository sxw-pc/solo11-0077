"""Exhaustive safety tests for the explicit boolean query interpreter.

For every synthetic topology the tests enumerate a small domain of shard-key
values (including NULL and values landing in range gaps / list defaults),
evaluate the boolean expression with three-valued logic and route every true
row through the real :class:`Router`. The explanation's targeted shard set must
cover every shard that actually holds a true row -- under both solving routes
(exact DNF union within the branch budget, and conservative full scan when the
budget is exhausted).
"""

from __future__ import annotations

import itertools
import json
import tempfile
import unittest
from pathlib import Path
from typing import Any

from shardroute.errors import QueryError, RoutingError
from shardroute.models import RangeRule, TableTopology, Topology
from shardroute.query_explain import explain_file, explain_query
from shardroute.query_support import eval_expression, parse_expression
from shardroute.report import build_report, write_reports
from shardroute.ledger import Ledger
from shardroute.router import Router

from support import empty_audit, empty_plan


def make_topology(tables: list[TableTopology]) -> Topology:
    shards = tuple(sorted({shard for table in tables for shard in table.table_shards()}))
    return Topology(
        version=1,
        shards=shards,
        tables={table.table: table for table in tables},
        generated_at="2026-01-02T00:00:00Z",
        description="synthetic boolean-explain topology",
        fingerprint="0" * 64,
    )


# ---------------------------------------------------------------------------
# Synthetic topologies
# ---------------------------------------------------------------------------

def range_gap_topology() -> Topology:
    # Rules leave a gap (2024-03-01 .. 2024-06-01) and the right side is bounded,
    # so values >= 2024-09-01 also fall through to the default shard.
    table = TableTopology(
        table="events",
        router="range",
        key_fields=("day",),
        null_policy="route_default",
        default_shard="shard_d",
        ranges=(
            RangeRule("2024-01-01", "2024-03-01", "shard_a"),
            RangeRule("2024-06-01", "2024-09-01", "shard_b"),
        ),
    )
    return make_topology([table])


def range_no_default_topology() -> Topology:
    table = TableTopology(
        table="events",
        router="range",
        key_fields=("day",),
        null_policy="error",
        default_shard=None,
        ranges=(RangeRule("2024-01-01", "2024-03-01", "shard_a"),),
    )
    return make_topology([table])


def list_topology() -> Topology:
    table = TableTopology(
        table="profiles",
        router="list",
        key_fields=("region",),
        null_policy="route_default",
        default_shard="shard_d",
        list_map={"alpha": "shard_a", "beta": "shard_b"},
    )
    return make_topology([table])


def composite_hash_topology(null_policy: str = "drop") -> Topology:
    table = TableTopology(
        table="orders",
        router="hash",
        key_fields=("tenant", "kind"),
        null_policy=null_policy,
        default_shard="shard_d" if null_policy == "route_default" else None,
        buckets=4,
        bucket_map=("shard_a", "shard_a", "shard_b", "shard_b"),
    )
    return make_topology([table])


# ---------------------------------------------------------------------------
# Exhaustive small-domain safety checker
# ---------------------------------------------------------------------------

def actual_shards(
    topology: Topology,
    table_name: str,
    expression: Any,
    domains: dict[str, list[Any]],
    non_key_rows: list[Any] | None = None,
    non_key_field: str = "tag",
) -> tuple[set[str], list[dict[str, Any]]]:
    """Route every row whose expression is true under 3VL.

    Returns the set of shards holding true rows and the matching rows. NULL
    keys follow the Router's null policy; rejected/dropped rows contribute no
    shard.
    """
    table = topology.table(table_name)
    router = Router(topology)
    if isinstance(expression, dict):
        expression = parse_expression(expression)
    non_key_values = non_key_rows if non_key_rows is not None else [None, "x"]
    fields = list(domains)
    shards: set[str] = set()
    matches: list[dict[str, Any]] = []
    key_combos = itertools.product(*(domains[field] for field in fields))
    for combo in key_combos:
        for tag in non_key_values:
            row = dict(zip(fields, combo))
            row[non_key_field] = tag
            truth = eval_expression(expression, row, table.case_sensitive)
            if truth is not True:
                continue  # false and unknown rows must never be returned
            matches.append(row)
            key_values = [row[field] for field in table.key_fields]
            try:
                trace = router.route(table_name, key_values[0] if len(key_values) == 1 else key_values)
            except RoutingError:
                continue  # rejected (e.g. null_policy=error): no physical row
            if not trace.dropped and trace.target_shard:
                shards.add(trace.target_shard)
    return shards, matches


def assert_covers(testcase: unittest.TestCase, result, actual: set[str]) -> None:
    targeted = set(result.targeted_shards)
    testcase.assertTrue(
        actual <= targeted,
        f"targeted {sorted(targeted)} misses true-row shards {sorted(actual - targeted)}",
    )
    testcase.assertEqual(set(result.all_shards) >= targeted, True)


class BooleanExplainSafetyTests(unittest.TestCase):
    # -- range with gap + default -------------------------------------------

    def test_range_gap_interval_includes_default_and_is_exhaustive(self):
        topology = range_gap_topology()
        days = [
            None, "2023-12-31", "2024-01-01", "2024-02-15", "2024-03-01",
            "2024-04-15", "2024-05-31", "2024-06-01", "2024-08-31",
            "2024-09-01", "2024-12-31",
        ]
        where = {"and": [
            {"field": "day", "op": "gte", "value": "2024-02-01"},
            {"field": "day", "op": "lt", "value": "2024-07-01"},
        ]}
        query = {"table": "events", "where": where}
        result = explain_query(topology, query)
        parsed = result.trace["expression"]
        actual, matches = actual_shards(topology, "events", parsed, {"day": days})
        # Gap values (March-May) inside the interval force the default shard.
        self.assertIn("shard_d", actual)
        assert_covers(self, result, actual)
        self.assertEqual(set(result.targeted_shards), {"shard_a", "shard_b", "shard_d"})
        self.assertEqual(result.decision, "full_scan")  # union covers every legal shard
        evidence = result.trace["branches"][0]["evidence"]
        interval = next(item for item in evidence if "interval" in item)
        self.assertFalse(interval["fully_covered_by_rules"])
        self.assertEqual(interval["default_shard"], "shard_d")
        self.assertTrue(matches)

    def test_range_tight_interval_prunes_and_exhaustive(self):
        topology = range_gap_topology()
        days = [None, "2024-01-15", "2024-02-01", "2024-03-01", "2024-04-01"]
        where = {"and": [
            {"field": "day", "op": "gte", "value": "2024-01-01"},
            {"field": "day", "op": "lt", "value": "2024-03-01"},
        ]}
        result = explain_query(topology, {"table": "events", "where": where})
        actual, _ = actual_shards(topology, "events", result.trace["expression"], {"day": days})
        assert_covers(self, result, actual)
        self.assertEqual(set(result.targeted_shards), {"shard_a"})
        self.assertEqual(result.decision, "pruned")

    def test_range_equality_in_gap_routes_default(self):
        topology = range_gap_topology()
        days = [None, "2024-04-15", "2024-06-01"]
        where = {"field": "day", "op": "eq", "value": "2024-04-15"}
        result = explain_query(topology, {"table": "events", "where": where})
        actual, _ = actual_shards(topology, "events", result.trace["expression"], {"day": days})
        assert_covers(self, result, actual)
        self.assertEqual(result.targeted_shards, ["shard_d"])
        point_routes = result.trace["branches"][0]["evidence"][0]["point_routes"]
        self.assertEqual(point_routes[0]["rule"], "range_default")

    def test_range_or_branches_and_negation_exhaustive(self):
        topology = range_gap_topology()
        days = [None, "2024-01-15", "2024-04-15", "2024-07-01", "2025-01-01"]
        where = {"or": [
            {"field": "day", "op": "eq", "value": "2024-01-15"},
            {"not": {"field": "day", "op": "eq", "value": "2024-07-01"}},
        ]}
        result = explain_query(topology, {"table": "events", "where": where})
        actual, matches = actual_shards(topology, "events", result.trace["expression"], {"day": days})
        assert_covers(self, result, actual)
        # NOT(eq) over an unbounded ordered domain can reach every shard.
        self.assertEqual(set(result.targeted_shards), set(result.all_shards))
        self.assertTrue(matches)

    def test_range_null_three_valued_logic(self):
        topology = range_gap_topology()
        # is_null alone: only NULL rows match and they route via route_default.
        where = {"field": "day", "op": "is_null"}
        result = explain_query(topology, {"table": "events", "where": where})
        actual, matches = actual_shards(
            topology, "events", result.trace["expression"], {"day": [None, "2024-01-01"]}
        )
        assert_covers(self, result, actual)
        self.assertEqual(result.targeted_shards, ["shard_d"])
        self.assertEqual({row["day"] for row in matches}, {None})

        # eq against NULL is unknown, never true.
        where = {"field": "day", "op": "eq", "value": None}
        result = explain_query(topology, {"table": "events", "where": where})
        actual, matches = actual_shards(
            topology, "events", result.trace["expression"], {"day": [None, "2024-01-01"]}
        )
        self.assertEqual(matches, [])
        assert_covers(self, result, actual)

    def test_range_contradictory_and_yields_empty_shards(self):
        topology = range_gap_topology()
        where = {"and": [
            {"field": "day", "op": "gte", "value": "2024-09-01"},
            {"field": "day", "op": "lt", "value": "2024-06-01"},
        ]}
        result = explain_query(topology, {"table": "events", "where": where})
        self.assertEqual(result.decision, "pruned")
        self.assertEqual(result.targeted_shards, [])
        actual, matches = actual_shards(
            topology, "events", result.trace["expression"],
            {"day": [None, "2024-01-01", "2024-07-01"]},
        )
        self.assertEqual(matches, [])
        self.assertEqual(actual, set())
        self.assertTrue(result.trace["contradiction"])

    def test_range_null_policy_error_rejects_instead_of_widening(self):
        topology = range_no_default_topology()
        where = {"field": "day", "op": "is_null"}
        result = explain_query(topology, {"table": "events", "where": where})
        self.assertEqual(result.targeted_shards, [])
        actual, matches = actual_shards(
            topology, "events", result.trace["expression"], {"day": [None, "2024-02-01"]}
        )
        self.assertEqual(actual, set())
        self.assertEqual({row["day"] for row in matches}, {None})

    # -- list null_route_default --------------------------------------------

    def test_list_null_and_membership_are_exhaustive(self):
        topology = list_topology()
        regions = [None, "alpha", "beta", "gamma"]
        where = {"or": [
            {"field": "region", "op": "eq", "value": "alpha"},
            {"field": "region", "op": "is_null"},
        ]}
        result = explain_query(topology, {"table": "profiles", "where": where})
        actual, matches = actual_shards(
            topology, "profiles", result.trace["expression"], {"region": regions}
        )
        assert_covers(self, result, actual)
        self.assertEqual(set(result.targeted_shards), {"shard_a", "shard_d"})
        self.assertEqual({row["region"] for row in matches}, {None, "alpha"})

    def test_list_negation_keeps_default_and_is_exhaustive(self):
        topology = list_topology()
        regions = [None, "alpha", "beta", "gamma"]
        where = {"not": {"field": "region", "op": "eq", "value": "alpha"}}
        result = explain_query(topology, {"table": "profiles", "where": where})
        actual, matches = actual_shards(
            topology, "profiles", result.trace["expression"], {"region": regions}
        )
        assert_covers(self, result, actual)
        # NULL comparisons are unknown so NULL rows are excluded; beta and the
        # unmapped gamma (default) remain.
        self.assertEqual({row["region"] for row in matches}, {"beta", "gamma"})
        self.assertEqual(set(result.targeted_shards), {"shard_b", "shard_d"})

    def test_list_in_and_not_in_combo(self):
        topology = list_topology()
        regions = [None, "alpha", "beta", "gamma"]
        where = {"and": [
            {"field": "region", "op": "in", "value": ["alpha", "beta", "gamma"]},
            {"field": "region", "op": "not_in", "value": ["beta"]},
        ]}
        result = explain_query(topology, {"table": "profiles", "where": where})
        actual, matches = actual_shards(
            topology, "profiles", result.trace["expression"], {"region": regions}
        )
        assert_covers(self, result, actual)
        self.assertEqual({row["region"] for row in matches}, {"alpha", "gamma"})
        self.assertEqual(set(result.targeted_shards), {"shard_a", "shard_d"})

    # -- composite hash ------------------------------------------------------

    def test_composite_hash_in_plus_equality_is_exhaustive(self):
        topology = composite_hash_topology()
        tenants = [None, "t1", "t2", "t3", "t4"]
        kinds = [None, "k1", "k2"]
        where = {"and": [
            {"field": "tenant", "op": "in", "value": ["t1", "t2"]},
            {"field": "kind", "op": "eq", "value": "k1"},
        ]}
        result = explain_query(topology, {"table": "orders", "where": where})
        actual, matches = actual_shards(
            topology, "orders", result.trace["expression"],
            {"tenant": tenants, "kind": kinds},
        )
        assert_covers(self, result, actual)
        # Exactly the two concrete composite keys are enumerated.
        self.assertEqual(len(result.trace["branches"]), 1)
        routed = {
            Router(topology).route("orders", ["t1", "k1"]).target_shard,
            Router(topology).route("orders", ["t2", "k1"]).target_shard,
        }
        self.assertEqual(set(result.targeted_shards), routed)
        self.assertEqual({(row["tenant"], row["kind"]) for row in matches},
                         {("t1", "k1"), ("t2", "k1")})

    def test_composite_hash_missing_component_widens(self):
        topology = composite_hash_topology(null_policy="route_default")
        tenants, kinds = [None, "t1", "t2"], [None, "k1", "k2"]
        where = {"field": "tenant", "op": "eq", "value": "t1"}
        result = explain_query(topology, {"table": "orders", "where": where})
        actual, _ = actual_shards(
            topology, "orders", result.trace["expression"],
            {"tenant": tenants, "kind": kinds},
        )
        assert_covers(self, result, actual)
        # kind is unconstrained: every bucket shard plus the null-default shard.
        self.assertEqual(set(result.targeted_shards), set(result.all_shards))

    def test_composite_hash_null_component_follows_policy(self):
        topology = composite_hash_topology(null_policy="drop")
        where = {"and": [
            {"field": "tenant", "op": "eq", "value": "t1"},
            {"field": "kind", "op": "is_null"},
        ]}
        result = explain_query(topology, {"table": "orders", "where": where})
        actual, matches = actual_shards(
            topology, "orders", result.trace["expression"],
            {"tenant": [None, "t1"], "kind": [None, "k1"]},
        )
        self.assertEqual({(row["tenant"], row["kind"]) for row in matches}, {("t1", None)})
        self.assertEqual(actual, set())  # dropped null key
        self.assertEqual(result.targeted_shards, [])

    def test_hash_not_in_with_null_member_makes_survivors_unknown(self):
        topology = composite_hash_topology(null_policy="drop")
        where = {"and": [
            {"field": "tenant", "op": "in", "value": ["t1", "t2"]},
            {"field": "tenant", "op": "not_in", "value": ["t1", None]},
            {"field": "kind", "op": "eq", "value": "k1"},
        ]}
        result = explain_query(topology, {"table": "orders", "where": where})
        actual, matches = actual_shards(
            topology, "orders", result.trace["expression"],
            {"tenant": [None, "t1", "t2"], "kind": [None, "k1", "k2"]},
        )
        # t1 NOT IN (t1, NULL) is false; t2 NOT IN (t1, NULL) is unknown because
        # the NULL dominates. No row can be true, so the branch is contradictory.
        self.assertEqual(matches, [])
        self.assertEqual(actual, set())
        self.assertEqual(result.targeted_shards, [])
        self.assertTrue(result.trace["contradiction"])
    # -- OR with a non-shard-key branch -------------------------------------

    def test_or_non_shard_key_branch_keeps_every_shard(self):
        topology = range_gap_topology()
        days = [None, "2024-01-15", "2024-04-15", "2024-07-01"]
        where = {"or": [
            {"field": "day", "op": "eq", "value": "2024-01-15"},
            {"field": "tag", "op": "eq", "value": "x"},
        ]}
        result = explain_query(topology, {"table": "events", "where": where})
        actual, matches = actual_shards(
            topology, "events", result.trace["expression"],
            {"day": days}, non_key_rows=[None, "x", "y"],
        )
        assert_covers(self, result, actual)
        self.assertEqual(set(result.targeted_shards), set(result.all_shards))
        # Rows matching only via the non-key branch exist in the true set.
        self.assertTrue(any(row["tag"] == "x" and row["day"] != "2024-01-15" for row in matches))
        branch_evidence = result.trace["branches"]
        self.assertTrue(
            any(
                any("non_shard_key_literals" in item for item in branch["evidence"])
                for branch in branch_evidence
            )
        )

    def test_non_shard_key_assertion_is_not_a_contradiction(self):
        topology = range_gap_topology()
        # An impossible-looking AND on a non-key field must NOT prune shards.
        where = {"and": [
            {"field": "tag", "op": "eq", "value": "x"},
            {"field": "tag", "op": "eq", "value": "y"},
        ]}
        result = explain_query(topology, {"table": "events", "where": where})
        self.assertEqual(result.decision, "full_scan")
        self.assertEqual(set(result.targeted_shards), set(result.all_shards))
        self.assertNotIn("contradiction", result.trace)

    def test_mixed_key_contradiction_and_non_key_branch(self):
        topology = range_gap_topology()
        where = {"or": [
            {"and": [
                {"field": "day", "op": "gte", "value": "2024-09-01"},
                {"field": "day", "op": "lt", "value": "2024-01-01"},
            ]},
            {"field": "tag", "op": "eq", "value": "x"},
        ]}
        result = explain_query(topology, {"table": "events", "where": where})
        # First branch is a genuine key contradiction (zero shards); the second
        # still forces a full scan.
        self.assertEqual(set(result.targeted_shards), set(result.all_shards))
        empty_branches = [b for b in result.trace["branches"] if not b["target_shards"]]
        self.assertEqual(len(empty_branches), 1)

    # -- branch budget: both routes safe ------------------------------------

    def test_budget_exhaustion_scans_all_and_both_routes_cover_true_rows(self):
        topology = range_gap_topology()
        days = [None, "2024-01-15", "2024-04-15", "2024-07-15", "2024-10-01"]
        conditions = [{"field": "day", "op": "eq", "value": day} for day in days if day]
        where = {"or": conditions}
        domains = {"day": days}

        precise = explain_query(topology, {"table": "events", "where": where, "branch_budget": 16})
        self.assertTrue(precise.trace["precision"]["precise"])
        actual, _ = actual_shards(topology, "events", precise.trace["expression"], domains)
        assert_covers(self, precise, actual)

        limited = explain_query(topology, {"table": "events", "where": where, "branch_budget": 2})
        self.assertFalse(limited.trace["precision"]["precise"])
        self.assertEqual(limited.decision, "not_prunable")
        self.assertEqual(set(limited.targeted_shards), set(limited.all_shards))
        # The conservative route still covers every true-row shard.
        assert_covers(self, limited, actual)
        # The precise route never scans more shards than the conservative one.
        self.assertTrue(set(precise.targeted_shards) <= set(limited.targeted_shards))
        self.assertIn("branch_budget", limited.trace["dnf"])

    def test_budget_zero_and_override_path(self):
        topology = range_gap_topology()
        where = {"or": [
            {"field": "day", "op": "eq", "value": "2024-01-15"},
            {"field": "day", "op": "eq", "value": "2024-07-01"},
        ]}
        with self.assertRaises(QueryError):
            explain_query(topology, {"table": "events", "where": where, "branch_budget": 0})
        # explain_query-level budget override applies without editing the query.
        result = explain_query(
            topology, {"table": "events", "where": where}, branch_budget=1
        )
        self.assertEqual(result.decision, "not_prunable")

    # -- NOT / NULL 3VL ------------------------------------------------------

    def test_nested_not_normalisation_and_3vl(self):
        topology = list_topology()
        regions = [None, "alpha", "beta", "gamma"]
        where = {"not": {"not": {"field": "region", "op": "eq", "value": "alpha"}}}
        result = explain_query(topology, {"table": "profiles", "where": where})
        actual, matches = actual_shards(
            topology, "profiles", result.trace["expression"], {"region": regions}
        )
        assert_covers(self, result, actual)
        self.assertEqual({row["region"] for row in matches}, {"alpha"})

    def test_not_null_is_unknown_not_true(self):
        topology = list_topology()
        where = {"field": "region", "op": "ne", "value": "alpha"}
        result = explain_query(topology, {"table": "profiles", "where": where})
        actual, matches = actual_shards(
            topology, "profiles", result.trace["expression"],
            {"region": [None, "alpha", "beta", "gamma"]},
        )
        assert_covers(self, result, actual)
        self.assertNotIn(None, {row["region"] for row in matches})

    def test_ne_removing_every_positive_pin_is_a_contradiction(self):
        for topology, table, domain, key in (
            (range_gap_topology(), "events",
             {"day": [None, "2024-01-15", "2024-04-15"]}, "day"),
            (list_topology(), "profiles",
             {"region": [None, "alpha", "beta"]}, "region"),
        ):
            where = {"and": [
                {"field": key, "op": "eq", "value": domain[key][1]},
                {"field": key, "op": "ne", "value": domain[key][1]},
            ]}
            result = explain_query(topology, {"table": table, "where": where})
            actual, matches = actual_shards(
                topology, table, result.trace["expression"], domain
            )
            self.assertEqual(matches, [])
            self.assertEqual(actual, set())
            self.assertEqual(result.targeted_shards, [])

    def test_ne_without_positive_pin_stays_open_and_covers_all(self):
        topology = range_gap_topology()
        where = {"field": "day", "op": "ne", "value": "2024-01-15"}
        result = explain_query(topology, {"table": "events", "where": where})
        actual, matches = actual_shards(
            topology, "events", result.trace["expression"],
            {"day": [None, "2024-01-15", "2024-04-15", "2024-10-01"]},
        )
        assert_covers(self, result, actual)
        self.assertEqual(set(result.targeted_shards), set(result.all_shards))
        # NULL comparisons are unknown, so the NULL row is not returned.
        self.assertNotIn(None, {row["day"] for row in matches})

    def test_three_valued_in_with_null_members(self):
        topology = list_topology()
        # IN (alpha, NULL): alpha rows are true, NULL rows stay unknown.
        where = {"field": "region", "op": "in", "value": ["alpha", None]}
        result = explain_query(topology, {"table": "profiles", "where": where})
        actual, matches = actual_shards(
            topology, "profiles", result.trace["expression"],
            {"region": [None, "alpha", "beta"]},
        )
        assert_covers(self, result, actual)
        self.assertEqual({row["region"] for row in matches}, {"alpha"})
        # NOT IN (alpha, NULL): SQL evaluates alpha to false (a known member)
        # and every other non-null value to unknown; NULL rows stay unknown.
        # No row can be true, so the branch is unsatisfiable.
        where = {"field": "region", "op": "not_in", "value": ["alpha", None]}
        result = explain_query(topology, {"table": "profiles", "where": where})
        actual, matches = actual_shards(
            topology, "profiles", result.trace["expression"],
            {"region": [None, "alpha", "beta", "gamma"]},
        )
        self.assertEqual(matches, [])
        self.assertEqual(actual, set())
        self.assertEqual(result.targeted_shards, [])

    def test_not_null_literal_is_open_domain_for_every_router(self):
        for topology, table, domains in (
            (range_gap_topology(), "events", {"day": [None, "2024-01-15"]}),
            (list_topology(), "profiles", {"region": [None, "alpha"]}),
            (composite_hash_topology("route_default"), "orders",
             {"tenant": [None, "t1"], "kind": [None, "k1"]}),
        ):
            key = topology.table(table).key_fields[0]
            where = {"field": key, "op": "not_null"}
            result = explain_query(topology, {"table": table, "where": where})
            actual, _ = actual_shards(
                topology, table, result.trace["expression"], domains
            )
            assert_covers(self, result, actual)

    # -- legacy compatibility -----------------------------------------------

    def test_legacy_filters_format_unchanged(self):
        topology = list_topology()
        legacy = explain_query(
            topology,
            {"table": "profiles", "filters": [{"field": "region", "op": "in", "value": ["alpha"]}]},
        )
        self.assertEqual(legacy.decision, "pruned")
        self.assertEqual(legacy.targeted_shards, ["shard_a"])
        self.assertNotIn("mode", legacy.trace)
        with self.assertRaises(QueryError):
            explain_query(
                topology,
                {"table": "profiles", "filters": [{"field": "region", "op": "in", "value": ["alpha"]}],
                 "where": {"field": "region", "op": "eq", "value": "alpha"}},
            )

    def test_invalid_boolean_queries_rejected(self):
        topology = list_topology()
        bad_queries = [
            {"table": "profiles", "where": {"and": [{"field": "region", "op": "eq", "value": "a"}]}},
            {"table": "profiles", "where": {"and": [
                {"field": "region", "op": "eq", "value": "a"},
                {"field": "region", "op": "between", "value": "a"},
            ]}},
            {"table": "profiles", "where": {"not": {}}},
            {"table": "profiles", "where": {"field": "region", "op": "in", "value": "nope"}},
            {"table": "unknown", "where": {"field": "region", "op": "eq", "value": "a"}},
            {"table": "profiles", "where": {"field": "", "op": "eq", "value": "a"}},
            {"table": "profiles", "where": {"field": "region", "op": "gte"}},
            {"table": "profiles", "where": [{"field": "region", "op": "eq", "value": "a"}]},
        ]
        for query in bad_queries:
            with self.assertRaises(QueryError):
                explain_query(topology, query)

    def test_explain_file_round_trip(self):
        topology = range_gap_topology()
        payload = {
            "name": "gap_window",
            "table": "events",
            "where": {"field": "day", "op": "eq", "value": "2024-04-15"},
        }
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "query.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            result = explain_file(topology, path)
        self.assertEqual(result.name, "gap_window")
        self.assertEqual(result.targeted_shards, ["shard_d"])

    # -- report and ledger evidence -----------------------------------------

    def test_report_and_ledger_carry_boolean_evidence(self):
        topology = range_gap_topology()
        precise_query = explain_query(
            topology,
            {"name": "tight", "table": "events",
             "where": {"field": "day", "op": "eq", "value": "2024-01-15"}},
        )
        imprecise_query = explain_query(
            topology,
            {"name": "wide", "table": "events", "branch_budget": 1,
             "where": {"or": [
                 {"field": "day", "op": "eq", "value": "2024-01-15"},
                 {"field": "day", "op": "eq", "value": "2024-07-01"},
             ]}},
        )
        report = build_report(
            old=topology,
            new=topology,
            seed=11,
            logical_fingerprint="a" * 64,
            physical_fingerprint="b" * 64,
            audit=empty_audit(),
            plan=empty_plan(topology, topology),
            queries=[precise_query, imprecise_query],
        )
        self.assertEqual(report["summary"]["not_precise_queries"], 1)
        self.assertEqual(report["summary"]["pruned_queries"], 1)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            artifacts = write_reports(report, root)
            markdown = (root / "reports" / "report.md").read_text(encoding="utf-8")
            self.assertIn("Imprecise queries", markdown)
            self.assertIn("wide", markdown)
            ledger = Ledger(root / "ledger.sqlite3")
            ledger.initialize()
            ledger.record_run(report, artifacts)
            stored = ledger.get_queries(report["run_id"])
            self.assertEqual(len(stored), 2)
            by_name = {item["name"]: item for item in stored}
            self.assertEqual(by_name["tight"]["targeted_shards"], ["shard_a"])
            self.assertFalse(by_name["wide"]["trace"]["precision"]["precise"])
            ledger.close()

    def test_router_explain_route_surfaces_default_and_errors(self):
        topology = range_gap_topology()
        router = Router(topology)
        gap = router.explain_route("events", "2024-04-15")
        self.assertEqual(gap.shard, "shard_d")
        self.assertEqual(gap.rule, "range_default")
        null_outcome = router.explain_route("events", None)
        self.assertEqual(null_outcome.shard, "shard_d")
        error_topology = range_no_default_topology()
        rejected = Router(error_topology).explain_route("events", None)
        self.assertIsNone(rejected.shard)
        self.assertEqual(rejected.error, "ROUTE_NULL_KEY")


if __name__ == "__main__":
    unittest.main()
