"""Typed range-domain semantics: integer and strict ISO date keys.

Covers the four acceptance dimensions:

1. legacy text-domain topologies are unchanged;
2. typed boundaries are trustworthy (left-closed/right-open on parsed values);
3. alias policy (``02``) is consistent across validate, route and explain;
4. query pruning and the reshard plan never contradict the Router.
"""

import unittest

from shardroute.config_validation import validate_document
from shardroute.errors import RoutingError
from shardroute.models import LogicalRow
from shardroute.planner import build_plan
from shardroute.query_explain import explain_query
from shardroute.router import Router

from support import base_doc, mini_topology, topology_from


def typed_doc(domain, ranges, alias_policy="strict", shards=None, table="nums",
              key="k", default_shard=None, null_policy="error"):
    return {
        "version": 1,
        "generated_at": "2026-01-02T00:00:00Z",
        "shards": shards or ["s0", "s1", "s2", "s3"],
        "tables": [
            {
                "table": table,
                "router": "range",
                "key": key,
                "null_policy": null_policy,
                "default_shard": default_shard,
                "range_domain": domain,
                "range_alias_policy": alias_policy,
                "ranges": ranges,
            }
        ],
    }


INT_RANGES = [
    {"lower": "2", "upper": "10", "shard": "s0"},
    {"lower": "10", "upper": "20", "shard": "s1"},
    {"lower": "20", "upper": None, "shard": "s2"},
]

DATE_RANGES = [
    {"lower": "2024-02-01", "upper": "2024-03-01", "shard": "s0"},
    {"lower": "2024-03-01", "upper": "2024-04-01", "shard": "s1"},
    {"lower": "2024-04-01", "upper": None, "shard": "s2"},
]


def issue_codes(doc):
    _, issues, _ = validate_document(doc)
    return {issue.code for issue in issues}


def issues_of(doc):
    _, issues, _ = validate_document(doc)
    return issues


def row(table, row_id, **values):
    return LogicalRow(table, row_id, {"row_id": row_id, **values}, f"logical/{table}.csv", 2)


class LegacyTextDomainTests(unittest.TestCase):
    """Acceptance 1: undeclared topologies keep the historical lexicographic topology."""

    def test_fixture_declares_no_domain_and_remains_text(self):
        topology = mini_topology()
        events = topology.table("events")
        self.assertEqual(events.range_domain, "text")
        self.assertEqual(events.range_alias_policy, "strict")
        self.assertNotIn("range_domain", events.as_dict())
        self.assertNotIn("range_alias_policy", events.as_dict())

    def test_lexicographic_order_is_preserved_without_a_domain(self):
        # "10" sorts before "2" under text comparison; that is the legacy
        # behaviour and must not silently change.
        doc = typed_doc(
            "text",
            [
                {"lower": "10", "upper": "20", "shard": "s0"},
                {"lower": "20", "upper": "21", "shard": "s1"},
            ],
        )
        del doc["tables"][0]["range_domain"]
        del doc["tables"][0]["range_alias_policy"]
        doc["shards"] = ["s0", "s1", "s2"]
        doc["tables"][0]["default_shard"] = "s2"
        topology = topology_from(doc)
        router = Router(topology)
        # "10" <= "2" < "20" textually, so integer 2 lands in [10, 20).
        self.assertEqual(router.route("nums", "2").target_shard, "s0")
        self.assertEqual(router.route("nums", "10").target_shard, "s0")
        self.assertEqual(router.route("nums", "30").target_shard, "s2")

    def test_text_domain_still_compares_strings(self):
        doc = typed_doc(
            "text",
            [{"lower": "10", "upper": "20", "shard": "s0"}],
            default_shard="s1",
        )
        doc["tables"][0].pop("range_domain")
        doc["tables"][0].pop("range_alias_policy")
        doc["shards"] = ["s0", "s1"]
        router = Router(topology_from(doc))
        # Text order: "10" <= "2" < "20" and "02" < "10".
        self.assertEqual(router.route("nums", "2").target_shard, "s0")
        self.assertEqual(router.route("nums", "02").target_shard, "s1")
        self.assertEqual(router.route("nums", "15").target_shard, "s0")
        self.assertEqual(router.route("nums", "3").target_shard, "s1")

    def test_text_domain_traces_are_unchanged(self):
        router = Router(mini_topology())
        trace = router.route("events", "2024-07-01")
        self.assertIsNone(trace.range_domain)
        self.assertNotIn("range_domain", trace.as_dict())
        self.assertNotIn("parsed_value", trace.as_dict())
        self.assertEqual(trace.normalized_value, "2024-07-01")

    def test_old_fixture_validation_has_no_domain_issue(self):
        self.assertEqual(issue_codes(base_doc()), set())


class IntegerDomainTests(unittest.TestCase):
    def setUp(self):
        self.topology = topology_from(typed_doc("integer", INT_RANGES))
        self.router = Router(self.topology)

    def test_left_closed_right_open_on_parsed_values(self):
        self.assertEqual(self.router.route("nums", 2).target_shard, "s0")
        self.assertEqual(self.router.route("nums", 9).target_shard, "s0")
        self.assertEqual(self.router.route("nums", 10).target_shard, "s1")
        self.assertEqual(self.router.route("nums", 19).target_shard, "s1")
        self.assertEqual(self.router.route("nums", 20).target_shard, "s2")
        self.assertEqual(self.router.route("nums", 999).target_shard, "s2")

    def test_numeric_order_not_lexicographic(self):
        # The headline defect: under text order "10" < "2", so 10 would land
        # in the [2, 10) shard. Typed comparison must place it in [10, 20).
        self.assertEqual(self.router.route("nums", 10).rule, "range[1]")
        self.assertEqual(self.router.route("nums", "10").target_shard, "s1")
        self.assertNotEqual(
            self.router.route("nums", "10").target_shard,
            self.router.route("nums", "2").target_shard,
        )

    def test_accepts_json_int_and_clean_integer_strings(self):
        for value in (2, "2", " 2 "):
            trace = self.router.route("nums", value)
            self.assertEqual(trace.target_shard, "s0")
            self.assertEqual(trace.normalized_value, "2")

    def test_trace_exposes_parsed_comparison_and_raw_key(self):
        trace = self.router.route("nums", 7)
        self.assertEqual(trace.raw_value, 7)
        self.assertEqual(trace.parsed_value, 7)
        self.assertEqual(trace.range_domain, "integer")
        self.assertEqual(trace.target_shard, "s0")
        self.assertTrue(any("7 >= 2: true" in line for line in trace.comparisons))
        self.assertTrue(any("7 < 10: true" in line for line in trace.comparisons))
        # The raw representation of an alias survives in canonicalize mode.
        loose = Router(topology_from(typed_doc("integer", INT_RANGES, "canonicalize")))
        alias_trace = loose.route("nums", " 007 ")
        self.assertEqual(alias_trace.raw_value, " 007 ")
        self.assertEqual(alias_trace.parsed_value, 7)
        self.assertTrue(alias_trace.range_alias_used)

    def test_trace_shows_parsed_bound_for_non_canonical_config_spelling(self):
        loose = Router(
            topology_from(
                typed_doc(
                    "integer",
                    [
                        {"lower": "002", "upper": "010", "shard": "s0"},
                        {"lower": "010", "upper": None, "shard": "s1"},
                    ],
                    "canonicalize",
                )
            )
        )
        trace = loose.route("nums", 7)
        self.assertTrue(any("7 >= 002=2: true" in line for line in trace.comparisons))
        self.assertTrue(any("7 < 010=10: true" in line for line in trace.comparisons))

    def test_rejects_float_and_scientific_spellings(self):
        for value in ("2.0", "2,0", "1e3", "1E3", "0x10", "2.5"):
            with self.assertRaises(RoutingError) as caught:
                self.router.route("nums", value)
            self.assertEqual(caught.exception.code, "ROUTE_RANGE_VALUE")
            self.assertEqual(caught.exception.details["reason"], "invalid_syntax")
            self.assertEqual(caught.exception.details["raw_value"], value)

    def test_rejects_wrong_types_and_empty_keys(self):
        for value in (True, False, {}, 2.0):
            with self.assertRaises(RoutingError) as caught:
                self.router.route("nums", value)
            self.assertEqual(caught.exception.code, "ROUTE_RANGE_VALUE")
            self.assertEqual(caught.exception.details["reason"], "wrong_type")
            self.assertEqual(caught.exception.details["raw_value"], value)
        for value in ("", "   "):
            with self.assertRaises(RoutingError) as caught:
                self.router.route("nums", value)
            self.assertEqual(caught.exception.code, "ROUTE_RANGE_VALUE")
            self.assertEqual(caught.exception.details["reason"], "empty")

    def test_rejects_out_of_range_integers(self):
        for value in (2**63, -(2**63) - 1, "9" * 40, str(2**63)):
            with self.assertRaises(RoutingError) as caught:
                self.router.route("nums", value)
            self.assertEqual(caught.exception.code, "ROUTE_RANGE_VALUE")
            self.assertEqual(caught.exception.details["reason"], "out_of_range")

    def test_negative_and_large_int64_bounds_are_valid(self):
        doc = typed_doc(
            "integer",
            [
                {"lower": str(-(2**63)), "upper": "0", "shard": "s0"},
                {"lower": "0", "upper": None, "shard": "s1"},
            ],
        )
        topology = topology_from(doc)
        router = Router(topology)
        self.assertEqual(router.route("nums", -(2**63)).target_shard, "s0")
        self.assertEqual(router.route("nums", -1).target_shard, "s0")
        self.assertEqual(router.route("nums", 0).target_shard, "s1")
        self.assertEqual(router.route("nums", 2**63 - 1).target_shard, "s1")
        with self.assertRaises(RoutingError) as caught:
            router.route("nums", 2**63)
        self.assertEqual(caught.exception.details["reason"], "out_of_range")

    def test_accepts_json_integer_bounds_in_config(self):
        doc = typed_doc(
            "integer",
            [{"lower": 2, "upper": 10, "shard": "s0"}, {"lower": 10, "upper": None, "shard": "s1"}],
        )
        router = Router(topology_from(doc))
        self.assertEqual(router.route("nums", 2).target_shard, "s0")
        self.assertEqual(router.route("nums", 10).target_shard, "s1")


class IntegerAliasStrictTests(unittest.TestCase):
    """Acceptance 3a: strict mode rejects non-canonical spellings everywhere."""

    def setUp(self):
        self.topology = topology_from(typed_doc("integer", INT_RANGES, "strict"))
        self.router = Router(self.topology)

    def test_route_rejects_non_canonical_spelling(self):
        for value in ("02", "007", "+2", " 02 "):
            with self.assertRaises(RoutingError) as caught:
                self.router.route("nums", value)
            self.assertEqual(caught.exception.code, "ROUTE_RANGE_VALUE")
            self.assertEqual(caught.exception.details["reason"], "non_canonical")
            self.assertEqual(caught.exception.details["raw_value"], value)
            self.assertIn("canonical", caught.exception.details)

    def test_config_rejects_non_canonical_bounds(self):
        doc = typed_doc(
            "integer",
            [
                {"lower": "02", "upper": "010", "shard": "s0"},
                {"lower": "10", "upper": None, "shard": "s1"},
            ],
            "strict",
        )
        issues = issues_of(doc)
        self.assertEqual({issue.code for issue in issues}, {"CFG_RANGE_VALUE"})
        self.assertEqual(
            {(issue.location, issue.details["reason"]) for issue in issues},
            {
                ("tables[0].ranges[0].lower", "non_canonical"),
                ("tables[0].ranges[0].upper", "non_canonical"),
            },
        )
        self.assertEqual(issues[0].details["raw_value"], "02")

    def test_query_rejects_alias_and_cannot_prune(self):
        result = explain_query(
            self.topology,
            {"table": "nums", "filters": [{"field": "k", "op": "eq", "value": "02"}]},
        )
        self.assertEqual(result.decision, "not_prunable")
        self.assertEqual(result.targeted_shards, ["s0", "s1", "s2"])
        self.assertEqual(result.trace["range_value_errors"][0]["reason"], "non_canonical")
        self.assertEqual(result.trace["range_value_errors"][0]["raw_value"], "02")


class IntegerAliasCanonicalizeTests(unittest.TestCase):
    """Acceptance 3b: canonicalize mode treats 02 and 2 as one business value."""

    def setUp(self):
        self.topology = topology_from(typed_doc("integer", INT_RANGES, "canonicalize"))
        self.router = Router(self.topology)

    def test_route_treats_alias_as_the_same_business_value(self):
        canonical = self.router.route("nums", 2)
        alias = self.router.route("nums", "02")
        self.assertEqual(alias.target_shard, canonical.target_shard)
        self.assertEqual(alias.normalized_value, canonical.normalized_value)
        self.assertEqual(alias.parsed_value, 2)
        self.assertEqual(alias.raw_value, "02")
        self.assertTrue(alias.range_alias_used)
        self.assertFalse(canonical.range_alias_used)
        self.assertEqual(alias.rule, canonical.rule)

    def test_config_normalizes_bounds_when_canonicalizing(self):
        doc = typed_doc(
            "integer",
            [
                {"lower": "002", "upper": "010", "shard": "s0"},
                {"lower": "0010", "upper": None, "shard": "s1"},
            ],
            "canonicalize",
        )
        topology = topology_from(doc)
        router = Router(topology)
        self.assertEqual(router.route("nums", 2).target_shard, "s0")
        self.assertEqual(router.route("nums", 10).target_shard, "s1")

    def test_queries_prune_to_the_same_shard(self):
        for value in (2, "2", "02", "002"):
            result = explain_query(
                self.topology,
                {"table": "nums", "filters": [{"field": "k", "op": "eq", "value": value}]},
            )
            self.assertEqual(result.decision, "pruned", value)
            self.assertEqual(result.targeted_shards, ["s0"], value)

    def test_modes_differ_on_pruned_cardinality_but_are_self_consistent(self):
        strict_doc = typed_doc("integer", INT_RANGES, "strict")
        canonical_doc = typed_doc("integer", INT_RANGES, "canonicalize")
        strict = topology_from(strict_doc)
        canonical = topology_from(canonical_doc)
        # Both configs accept the topology itself (no alias in bounds).
        self.assertEqual(issue_codes(strict_doc), set())
        self.assertEqual(issue_codes(canonical_doc), set())
        strict_query = explain_query(
            strict, {"table": "nums", "filters": [{"field": "k", "op": "eq", "value": "02"}]}
        )
        canonical_query = explain_query(
            canonical, {"table": "nums", "filters": [{"field": "k", "op": "eq", "value": "02"}]}
        )
        self.assertEqual(strict_query.decision, "not_prunable")
        self.assertEqual(canonical_query.decision, "pruned")
        self.assertEqual(canonical_query.targeted_shards, ["s0"])
        # The router agrees with whichever policy the topology declared.
        with self.assertRaises(RoutingError):
            Router(strict).route("nums", "02")
        self.assertEqual(Router(canonical).route("nums", "02").target_shard, "s0")


class DateDomainTests(unittest.TestCase):
    def setUp(self):
        self.topology = topology_from(typed_doc("date", DATE_RANGES, table="days", key="day"))
        self.router = Router(self.topology)

    def test_leap_day_is_valid_and_boundaries_are_typed(self):
        self.assertEqual(self.router.route("days", "2024-02-29").target_shard, "s0")
        self.assertEqual(self.router.route("days", "2024-03-01").target_shard, "s1")
        self.assertEqual(self.router.route("days", "2024-04-01").target_shard, "s2")

    def test_invalid_leap_day_and_malformed_dates_are_rejected(self):
        cases = {
            "2024-02-30": "invalid_calendar_date",
            "2023-02-29": "invalid_calendar_date",
            "2024-13-01": "invalid_calendar_date",
            "2024-2-9": "invalid_format",
            "20240209": "invalid_format",
            "2024-02-09T00:00:00": "invalid_format",
            "": "empty",
        }
        for value, reason in cases.items():
            with self.assertRaises(RoutingError) as caught:
                self.router.route("days", value)
            self.assertEqual(caught.exception.code, "ROUTE_RANGE_VALUE", value)
            self.assertEqual(caught.exception.details["reason"], reason, value)
            self.assertEqual(caught.exception.details["raw_value"], value)
            self.assertEqual(caught.exception.details["domain"], "date")

    def test_rejects_non_string_types(self):
        for value in (20240229, True, 2024.0, {"day": "2024-02-09"}):
            with self.assertRaises(RoutingError) as caught:
                self.router.route("days", value)
            self.assertEqual(caught.exception.code, "ROUTE_RANGE_VALUE")
            self.assertEqual(caught.exception.details["reason"], "wrong_type")
            self.assertEqual(caught.exception.details["raw_value"], value)

    def test_config_rejects_invalid_bound_dates(self):
        doc = typed_doc(
            "date",
            [
                {"lower": "2024-02-30", "upper": "2024-03-01", "shard": "s0"},
                {"lower": "2024-03-01", "upper": None, "shard": "s1"},
            ],
            table="days",
            key="day",
        )
        issues = issues_of(doc)
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0].code, "CFG_RANGE_VALUE")
        self.assertEqual(issues[0].location, "tables[0].ranges[0].lower")
        self.assertEqual(issues[0].details["reason"], "invalid_calendar_date")
        self.assertEqual(issues[0].details["raw_value"], "2024-02-30")

    def test_date_boundary_gap_and_overlap_detected_by_value(self):
        gap = typed_doc(
            "date",
            [
                {"lower": "2024-02-01", "upper": "2024-03-01", "shard": "s0"},
                {"lower": "2024-03-02", "upper": None, "shard": "s1"},
            ],
            table="days",
            key="day",
        )
        self.assertIn("CFG_RANGE_GAP", issue_codes(gap))
        overlap = typed_doc(
            "date",
            [
                {"lower": "2024-02-01", "upper": "2024-03-01", "shard": "s0"},
                {"lower": "2024-02-28", "upper": None, "shard": "s1"},
            ],
            table="days",
            key="day",
        )
        self.assertIn("CFG_RANGE_OVERLAP", issue_codes(overlap))

    def test_open_ended_last_range_and_infinite_upper_bound(self):
        self.assertEqual(self.router.route("days", "2099-12-31").target_shard, "s2")
        trace = self.router.route("days", "2099-12-31")
        self.assertEqual(trace.rule, "range[2]")
        # The open interval has only a lower comparison, never an upper one.
        self.assertEqual(len(trace.comparisons), 5)
        self.assertTrue(trace.comparisons[-1].startswith("2099-12-31 >= "))


class TypedTopologyValidationTests(unittest.TestCase):
    def test_unknown_domain_and_alias_policy_are_rejected(self):
        doc = typed_doc("decimal", INT_RANGES)
        self.assertIn("CFG_RANGE_DOMAIN", issue_codes(doc))
        doc = typed_doc("integer", INT_RANGES, "fuzzy")
        self.assertIn("CFG_RANGE_DOMAIN", issue_codes(doc))

    def test_bound_spellings_are_validated_by_the_domain(self):
        cases = {
            2.0: "wrong_type",
            True: "wrong_type",
            "2.0": "invalid_syntax",
            "1e3": "invalid_syntax",
            "": "empty",
            str(2**63): "out_of_range",
        }
        for bound, reason in cases.items():
            doc = typed_doc(
                "integer",
                [{"lower": bound, "upper": "20", "shard": "s0"}],
            )
            issues = issues_of(doc)
            self.assertEqual(
                [(issue.code, issue.location, issue.details["reason"]) for issue in issues],
                [("CFG_RANGE_VALUE", "tables[0].ranges[0].lower", reason)],
                bound,
            )
            self.assertEqual(issues[0].details["raw_value"], bound)
        bad_date = typed_doc(
            "date", [{"lower": "2024-02-30", "upper": None, "shard": "s0"}], table="days", key="day"
        )
        self.assertEqual(issues_of(bad_date)[0].details["reason"], "invalid_calendar_date")
        container = typed_doc(
            "integer", [{"lower": [2], "upper": "20", "shard": "s0"}]
        )
        self.assertIn("CFG_SCHEMA", issue_codes(container))

    def test_typed_ranges_still_reject_overlap_gap_and_trailing_range(self):
        overlap = typed_doc(
            "integer",
            [
                {"lower": "2", "upper": "10", "shard": "s0"},
                {"lower": "9", "upper": None, "shard": "s1"},
            ],
        )
        self.assertIn("CFG_RANGE_OVERLAP", issue_codes(overlap))
        gap = typed_doc(
            "integer",
            [
                {"lower": "2", "upper": "10", "shard": "s0"},
                {"lower": "11", "upper": None, "shard": "s1"},
            ],
        )
        self.assertIn("CFG_RANGE_GAP", issue_codes(gap))
        empty = typed_doc("integer", [{"lower": "10", "upper": "10", "shard": "s0"}])
        self.assertIn("CFG_RANGE_OVERLAP", issue_codes(empty))
        trailing = typed_doc(
            "integer",
            [
                {"lower": "2", "upper": None, "shard": "s0"},
                {"lower": "10", "upper": None, "shard": "s1"},
            ],
        )
        self.assertIn("CFG_RANGE_OVERLAP", issue_codes(trailing))

    def test_typed_overlap_is_by_value_not_text(self):
        # Textually "10" < "2", but numerically [10, 20) follows [2, 10).
        doc = typed_doc(
            "integer",
            [
                {"lower": "2", "upper": "10", "shard": "s0"},
                {"lower": "10", "upper": "20", "shard": "s1"},
            ],
        )
        self.assertEqual(issue_codes(doc), set())
        # In canonicalize mode "009" is the same business value as "9", so
        # [009, 20) overlaps [2, 10).
        bad = typed_doc(
            "integer",
            [
                {"lower": "2", "upper": "10", "shard": "s0"},
                {"lower": "009", "upper": "20", "shard": "s1"},
            ],
            "canonicalize",
        )
        issues = issues_of(bad)
        self.assertEqual({issue.code for issue in issues}, {"CFG_RANGE_OVERLAP"})
        self.assertEqual(issues[0].details["parsed_lower"], 9)
        self.assertEqual(issues[0].details["parsed_previous_upper"], 10)

    def test_null_default_fallback_uses_typed_domain(self):
        doc = typed_doc(
            "integer", INT_RANGES, default_shard="s3", null_policy="route_default"
        )
        topology = topology_from(doc)
        router = Router(topology)
        self.assertEqual(router.route("nums", 1).target_shard, "s3")
        self.assertEqual(router.route("nums", 1).rule, "range_default")
        self.assertEqual(router.route("nums", None).target_shard, "s3")

    def test_empty_key_with_route_default(self):
        doc = typed_doc(
            "integer",
            INT_RANGES,
            default_shard="s3",
            null_policy="route_default",
            table="nums",
        )
        topology = topology_from(doc)
        router = Router(topology)
        # An empty CSV cell is an empty string, not null; typed parsing
        # rejects it rather than silently falling back to the default shard.
        with self.assertRaises(RoutingError) as caught:
            router.route("nums", "")
        self.assertEqual(caught.exception.code, "ROUTE_RANGE_VALUE")
        self.assertEqual(caught.exception.details["reason"], "empty")
        trace = router.route("nums", None)
        self.assertEqual(trace.rule, "null_route_default")
        self.assertEqual(trace.target_shard, "s3")

    def test_empty_key_default_is_used_for_a_gap_outside_the_domain(self):
        # A numeric value below every configured interval is a legitimate
        # typed miss, and default_shard catches it as the documented fallback.
        doc = typed_doc(
            "integer", INT_RANGES, default_shard="s3", null_policy="route_default"
        )
        trace = Router(topology_from(doc)).route("nums", 1)
        self.assertEqual(trace.rule, "range_default")
        self.assertEqual(trace.target_shard, "s3")
        self.assertTrue(trace.comparisons[-1].startswith("no interval matched"))

    def test_empty_key_without_default_is_an_error(self):
        doc = typed_doc("integer", INT_RANGES)
        router = Router(topology_from(doc))
        with self.assertRaises(RoutingError) as caught:
            router.route("nums", "")
        self.assertEqual(caught.exception.code, "ROUTE_RANGE_VALUE")
        with self.assertRaises(RoutingError) as caught:
            router.route("nums", None)
        self.assertEqual(caught.exception.code, "ROUTE_NULL_KEY")


class TypedQueryExplainTests(unittest.TestCase):
    def setUp(self):
        self.topology = topology_from(typed_doc("integer", INT_RANGES))
        self.date_topology = topology_from(
            typed_doc("date", DATE_RANGES, table="days", key="day")
        )

    def explain(self, topology, table, *filters):
        return explain_query(topology, {"table": table, "filters": list(filters)})

    def test_interval_pruning_uses_parsed_values(self):
        result = self.explain(
            self.topology,
            "nums",
            {"field": "k", "op": "gte", "value": 2},
            {"field": "k", "op": "lt", "value": 10},
        )
        self.assertEqual(result.decision, "pruned")
        self.assertEqual(result.targeted_shards, ["s0"])
        result = self.explain(
            self.topology,
            "nums",
            {"field": "k", "op": "gte", "value": 9},
            {"field": "k", "op": "lte", "value": 20},
        )
        self.assertEqual(result.targeted_shards, ["s0", "s1", "s2"])
        self.assertEqual(result.trace["interval"]["domain"], "integer")

    def test_equality_pruning_agrees_with_router(self):
        for value in (2, 10, 20):
            result = self.explain(
                self.topology, "nums", {"field": "k", "op": "eq", "value": value}
            )
            self.assertEqual(result.targeted_shards, [Router(self.topology).route("nums", value).target_shard])

    def test_date_interval_excludes_gap_correctly(self):
        result = self.explain(
            self.date_topology,
            "days",
            {"field": "day", "op": "gte", "value": "2024-02-29"},
            {"field": "day", "op": "lt", "value": "2024-04-01"},
        )
        self.assertEqual(result.targeted_shards, ["s0", "s1"])
        self.assertTrue(all("parsed_upper" in rule for rule in result.trace["rules"]))
        self.assertEqual(
            [rule["shard"] for rule in result.trace["rules"]], ["s0", "s1"]
        )

    def test_invalid_query_value_never_silently_prunes(self):
        result = self.explain(
            self.date_topology, "days", {"field": "day", "op": "eq", "value": "2024-02-30"}
        )
        self.assertEqual(result.decision, "not_prunable")
        self.assertEqual(result.targeted_shards, ["s0", "s1", "s2"])
        self.assertEqual(
            result.trace["range_value_errors"][0]["reason"], "invalid_calendar_date"
        )
        self.assertEqual(result.trace["range_value_errors"][0]["raw_value"], "2024-02-30")

    def test_query_includes_default_shard_when_window_reaches_outside_union(self):
        # With default_shard=s3 the Router would send such values to s3.
        doc = typed_doc("integer", INT_RANGES, default_shard="s3")
        topology = topology_from(doc)
        router = Router(topology)
        result = explain_query(
            topology,
            {"table": "nums", "filters": [{"field": "k", "op": "lt", "value": 2}]},
        )
        self.assertIn("s3", result.targeted_shards)
        # Every value the window admits routes into the targeted set.
        for value in range(-3, 2):
            self.assertIn(router.route("nums", value).target_shard, result.targeted_shards)
        # A window wholly inside the union does not drag in the default.
        inside = explain_query(
            topology,
            {"table": "nums", "filters": [{"field": "k", "op": "gte", "value": 2}]},
        )
        self.assertNotIn("s3", inside.targeted_shards)
        self.assertEqual(inside.targeted_shards, ["s0", "s1", "s2"])

    def test_contract_with_router_over_a_grid_of_windows(self):
        doc = typed_doc("integer", INT_RANGES, default_shard="s3")
        topology = topology_from(doc)
        router = Router(topology)
        bounds = [0, 2, 5, 9, 10, 19, 20, 30]
        windows = [(None, high) for high in bounds]
        windows += [(low, None) for low in bounds]
        windows += [(low, high) for low in bounds for high in bounds if low < high]
        for low, high in windows:
            if low is not None and high is not None and low >= high:
                continue
            filters = []
            if low is not None:
                filters.append({"field": "k", "op": "gte", "value": low})
            if high is not None:
                filters.append({"field": "k", "op": "lt", "value": high})
            result = explain_query(topology, {"table": "nums", "filters": filters})
            for value in range(0, 40):
                if (low is None or value >= low) and (high is None or value < high):
                    self.assertIn(
                        router.route("nums", value).target_shard,
                        result.targeted_shards,
                        (low, high, value),
                    )

        router = Router(self.topology)
        for low, high in [(2, 10), (0, 5), (15, 100), (20, 25)]:
            result = self.explain(
                self.topology,
                "nums",
                {"field": "k", "op": "gte", "value": low},
                {"field": "k", "op": "lt", "value": high},
            )
            allowed = set(self.topology.table("nums").table_shards())
            self.assertTrue(set(result.targeted_shards) <= allowed)
            # Every routable value in the window stays inside the pruned set.
            reachable = set()
            for value in range(max(low, 0), high):
                try:
                    reachable.add(router.route("nums", value).target_shard)
                except RoutingError:
                    # Values below the first finite interval have no default.
                    continue
            self.assertTrue(reachable <= set(result.targeted_shards))
            # And each targeted shard is actually reachable from the window.
            self.assertEqual(set(result.targeted_shards), reachable)


class TypedPlanTests(unittest.TestCase):
    """Acceptance 4: build_plan's move_cells/moved_rows agree with the Router."""

    def rows(self):
        return [
            row("nums", f"r{index}", k=index)
            for index in (2, 5, 9, 10, 15, 19, 20, 25)
        ]

    def test_legacy_text_plan_unchanged(self):
        old = mini_topology()
        self.assertEqual(build_plan(old, old, {})["moved_rows"], 0)

    def test_typed_topology_move_counts_match_router(self):
        old_doc = typed_doc("integer", INT_RANGES)
        new_doc = typed_doc(
            "integer",
            [
                {"lower": "2", "upper": "10", "shard": "s0"},
                {"lower": "10", "upper": "15", "shard": "s1"},
                {"lower": "15", "upper": None, "shard": "s2"},
            ],
        )
        old = topology_from(old_doc)
        new = topology_from(new_doc)
        rows = {"nums": self.rows()}
        plan = build_plan(old, new, rows)
        old_router, new_router = Router(old), Router(new)
        expected = {}
        for item in rows["nums"]:
            before = old_router.route_values("nums", item.values)
            after = new_router.route_values("nums", item.values)
            if before.target_shard != after.target_shard:
                key = (before.target_shard, after.target_shard)
                expected[key] = expected.get(key, 0) + 1
        cells = {(cell["from_shard"], cell["to_shard"]): cell["row_count"] for cell in plan["move_cells"]}
        self.assertEqual(cells, expected)
        self.assertEqual(plan["moved_rows"], sum(expected.values()))
        self.assertEqual(plan["moved_rows"] + plan["unchanged_rows"], plan["total_rows"])
        self.assertEqual(plan["route_errors"], 0)

    def test_legacy_vs_typed_topology_move_cells(self):
        # Same synthetic rows, evaluated once under text and once under typed
        # semantics; the moved counts must follow each topology's Router.
        rows = {"nums": [row("nums", str(index), k=index) for index in range(0, 25)]}

        def layout(domain, first_shard, second_shard):
            doc = typed_doc(
                domain,
                [
                    {"lower": "2", "upper": "3", "shard": first_shard},
                    {"lower": "3", "upper": None, "shard": second_shard},
                ],
                default_shard="s3",
            )
            if domain == "text":
                for table in doc["tables"]:
                    table.pop("range_domain")
                    table.pop("range_alias_policy")
            return doc

        old_doc, new_doc = layout("text", "s0", "s1"), layout("text", "s1", "s0")
        typed_old_doc, typed_new_doc = layout("integer", "s0", "s1"), layout("integer", "s1", "s0")
        text_plan = build_plan(topology_from(old_doc), topology_from(new_doc), rows)
        typed_plan = build_plan(
            topology_from(typed_old_doc), topology_from(typed_new_doc), rows
        )
        # Under text order "10" < "3", so integer 10 misses every interval and
        # falls back to s3; under typed order 10 lands in [3, inf) -> s1.
        text_old = Router(topology_from(old_doc))
        typed_old = Router(topology_from(typed_old_doc))
        self.assertEqual(text_old.route("nums", 10).target_shard, "s3")
        self.assertEqual(typed_old.route("nums", 10).target_shard, "s1")
        self.assertNotEqual(text_plan["moved_rows"], typed_plan["moved_rows"])
        for plan in (text_plan, typed_plan):
            self.assertEqual(plan["route_errors"], 0)
            self.assertEqual(
                plan["moved_rows"] + plan["unchanged_rows"], plan["total_rows"]
            )
    def test_alias_policy_changes_routable_rows_consistently(self):
        rows = {"nums": [row("nums", "a", k="02"), row("nums", "b", k="2")]}
        strict = typed_doc("integer", INT_RANGES, "strict")
        loose = typed_doc("integer", INT_RANGES, "canonicalize")
        strict_plan = build_plan(topology_from(strict), topology_from(strict), rows)
        loose_plan = build_plan(topology_from(loose), topology_from(loose), rows)
        self.assertEqual(strict_plan["route_errors"], 1)
        self.assertEqual(loose_plan["route_errors"], 0)
        self.assertIn("route_error_details", strict_plan)
        self.assertEqual(
            strict_plan["route_error_details"][0]["code"], "ROUTE_RANGE_VALUE"
        )
        self.assertEqual(
            strict_plan["route_error_details"][0]["details"]["raw_value"], "02"
        )
        self.assertNotIn("route_error_details", loose_plan)

    def test_plan_route_error_details_carry_raw_key(self):
        doc = typed_doc("date", DATE_RANGES, table="days", key="day")
        rows = {"days": [row("days", "bad", day="2024-02-30")]}
        plan = build_plan(topology_from(doc), topology_from(doc), rows)
        self.assertEqual(plan["route_errors"], 1)
        detail = plan["route_error_details"][0]
        self.assertEqual(detail["code"], "ROUTE_RANGE_VALUE")
        self.assertEqual(detail["details"]["raw_value"], "2024-02-30")


class CompositeKeysUnaffectedTests(unittest.TestCase):
    def test_hash_and_list_tables_ignore_range_domain_fields(self):
        doc = base_doc()
        orders = next(item for item in doc["tables"] if item["table"] == "orders")
        orders["range_domain"] = "integer"
        orders["range_alias_policy"] = "canonicalize"
        topology = topology_from(doc)
        # Unknown fields do not change behaviour; hash routing is unaffected.
        self.assertEqual(topology.table("orders").range_domain, "text")
        Router(topology).route("orders", "tenant_demo_001")
        list_table = next(item for item in doc["tables"] if item["table"] == "profiles")
        self.assertEqual(topology.table("profiles").range_domain, "text")


if __name__ == "__main__":
    unittest.main()
