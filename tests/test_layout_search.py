import copy
import io
import json
import tempfile
import unittest
from pathlib import Path

from shardroute.cli import main
from shardroute.config_validation import validate_document
from shardroute.errors import ShardRouteError
from shardroute.planner import build_plan, search_layouts
from shardroute.query_explain import explain_query, explain_workload, forced_targets
from shardroute.report import (
    build_layout_search_report,
    render_layout_search_markdown,
    write_layout_search_report,
)
from shardroute.router import Router

from support import logical, write_json

SHARDS = ["a", "b"]

def list_doc(list_map, *, null_policy="route_default", default_shard="a", case_sensitive=True):
    return {
        "version": 1,
        "generated_at": "2026-01-01T00:00:00Z",
        "shards": list(SHARDS),
        "tables": [
            {
                "table": "items",
                "router": "list",
                "key": "k",
                "null_policy": null_policy,
                "default_shard": default_shard,
                "list_map": copy.deepcopy(list_map),
                "case_sensitive": case_sensitive,
            }
        ],
    }

def topology(doc):
    topo, issues, _ = validate_document(doc)
    assert topo is not None and not issues, [i.as_dict() for i in issues]
    return topo

def q(name, op, value, field="k"):
    return {"name": name, "table": "items", "filters": [{"field": field, "op": op, "value": value}]}

def rows_for(groups):
    """groups: dict key -> number of logical rows with that key."""
    out = []
    line = 1
    for key, count in sorted(groups.items()):
        for index in range(count):
            out.append(logical("items", f"{key}-{index}", {"k": key}))
            line += 1
    return out

class CounterexampleTests(unittest.TestCase):
    """The mandated two-shard, two-key counterexample from the contract."""

    def setUp(self):
        self.topo = topology(list_doc({"x": "a", "y": "b"}))
        self.rows = {"items": rows_for({"x": 2, "y": 3})}
        self.workload = [
            {"frequency": 100, "query": q("eq_x", "eq", "x")},
            {"frequency": 100, "query": q("eq_y", "eq", "y")},
            {"frequency": 10, "query": q("in_xy", "in", ["x", "y"])},
        ]
        self.caps = {"a": 220, "b": 220}

    def search(self, **overrides):
        params = dict(
            fixed_keys=["x"],
            move_budget_rows=3,
            shard_read_caps=self.caps,
            workload=self.workload,
        )
        params.update(overrides)
        return search_layouts(self.topo, "items", self.rows, {"y": ["a", "b"]}, **params)

    def test_two_objectives_pick_different_layouts(self):
        result = self.search()
        self.assertEqual(result["status"], "optimal")
        self.assertTrue(result["safe_to_execute"])
        economy = result["selections"]["request_economy"]["layout"]
        hotspot = result["selections"]["hotspot_relief"]["layout"]
        # Co-location wins request economy: 100 + 100 + 10 = 210.
        self.assertEqual(economy["list_map"], {"x": "a", "y": "a"})
        self.assertEqual(economy["total_requests"], 210)
        self.assertEqual(economy["peak_shard_requests"], 210)
        self.assertEqual(economy["moved_rows"], 3)
        self.assertEqual(economy["budget_remaining"], 0)
        # Split layout wins hotspot relief: peak 110, total 220.
        self.assertEqual(hotspot["list_map"], {"x": "a", "y": "b"})
        self.assertEqual(hotspot["total_requests"], 220)
        self.assertEqual(hotspot["peak_shard_requests"], 110)
        self.assertEqual(hotspot["moved_rows"], 0)
        self.assertNotEqual(economy["list_map"], hotspot["list_map"])
        # Each objective reports both metrics, without cross-claiming optima.
        self.assertEqual(
            result["selections"]["request_economy"]["status"], "optimal"
        )
        self.assertEqual(
            result["selections"]["hotspot_relief"]["status"], "optimal"
        )

    def test_in_query_billed_once_per_target_not_per_key(self):
        result = self.search()
        layout = result["selections"]["request_economy"]["layout"]
        in_record = next(item for item in layout["queries"] if item["name"] == "in_xy")
        # Co-located: one distinct target shard -> 10 requests, not 20.
        self.assertEqual(in_record["target_count"], 1)
        self.assertEqual(in_record["requests"], 10)
        split = result["selections"]["hotspot_relief"]["layout"]
        split_in = next(item for item in split["queries"] if item["name"] == "in_xy")
        self.assertEqual(split_in["target_count"], 2)
        self.assertEqual(split_in["requests"], 20)

    def test_request_math_is_recomputable_from_query_records(self):
        result = self.search()
        for selection in result["selections"].values():
            layout = selection["layout"]
            total = sum(item["requests"] for item in layout["queries"])
            self.assertEqual(total, layout["total_requests"])
            recomputed_load = {shard: 0 for shard in SHARDS}
            for item in layout["queries"]:
                for shard, contribution in item["shard_contributions"].items():
                    recomputed_load[shard] += contribution
            self.assertEqual(recomputed_load, layout["shard_load"])
            self.assertEqual(max(recomputed_load.values()), layout["peak_shard_requests"])

    def test_migration_matches_real_routers_and_build_plan(self):
        result = self.search()
        economy = result["selections"]["request_economy"]["layout"]
        candidate_doc = list_doc(economy["list_map"])
        candidate = topology(candidate_doc)
        old_router, new_router = Router(self.topo), Router(candidate)
        recomputed = []
        for row in self.rows["items"]:
            before = old_router.route_values("items", row.values)
            after = new_router.route_values("items", row.values)
            if before.target_shard != after.target_shard:
                recomputed.append(row.row_id)
        self.assertEqual(sorted(recomputed), economy["moved_row_ids"])
        self.assertEqual(len(recomputed), economy["moved_rows"])
        plan = build_plan(self.topo, candidate, self.rows)
        self.assertEqual(plan["moved_rows"], economy["moved_rows"])
        self.assertEqual(plan["move_cells"], economy["move_cells"])

    def test_candidate_targets_match_real_interpreter(self):
        result = self.search()
        for layout in result["feasible_layouts"]:
            candidate = topology(list_doc(layout["list_map"]))
            for record in layout["queries"]:
                query = next(
                    item["query"] for item in result["workload"] if item["name"] == record["name"]
                )
                explanation = explain_query(candidate, query)
                self.assertEqual(explanation.targeted_shards, record["targets"])

    def test_optimality_evidence_and_all_alternatives_listed(self):
        result = self.search()
        self.assertTrue(result["enumeration"]["exhausted"])
        self.assertEqual(result["enumeration"]["domain_size"], 2)
        self.assertEqual(result["enumeration"]["evaluated_feasible"], 2)
        maps = {tuple(sorted(item["list_map"].items())) for item in result["feasible_layouts"]}
        self.assertEqual(
            maps,
            {
                (("x", "a"), ("y", "a")),
                (("x", "a"), ("y", "b")),
            },
        )
        for objective in result["objectives"]:
            self.assertIn(
                "exhaustive enumeration",
                result["selections"][objective]["optimality_evidence"],
            )

class PreservationAndNullTests(unittest.TestCase):
    def test_undeclared_key_and_default_behavior_preserved(self):
        topo = topology(list_doc({"x": "a", "y": "b", "z": "b"}))
        rows = {"items": rows_for({"x": 1, "y": 1, "z": 1, "other": 1})}
        workload = [
            {"frequency": 5, "query": q("eq_y", "eq", "y")},
            {"frequency": 7, "query": q("eq_other", "eq", "other")},
        ]
        result = search_layouts(
            topo, "items", rows, {"y": ["a", "b"]},
            fixed_keys=["x"], move_budget_rows=1,
            shard_read_caps={"a": 100, "b": 100}, workload=workload,
        )
        for layout in result["feasible_layouts"]:
            self.assertEqual(layout["list_map"]["z"], "b")  # undeclared key untouched
            self.assertEqual(layout["list_map"]["x"], "a")  # fixed key untouched
        # 'other' is unmapped: default shard 'a' under every candidate.
        record = result["selections"]["request_economy"]["layout"]["queries"][1]
        self.assertEqual(record["targets"], ["a"])
        self.assertEqual(record["requests"], 7)

    def test_null_key_drop_policy_prunes_to_no_targets(self):
        topo = topology(list_doc({"x": "a", "y": "b"}, null_policy="drop", default_shard=None))
        rows = {"items": rows_for({"x": 1, "y": 1})}
        workload = [
            {"frequency": 4, "query": q("is_null", "is_null", None)},
            {"frequency": 3, "query": q("eq_y", "eq", "y")},
        ]
        result = search_layouts(
            topo, "items", rows, {"y": ["a", "b"]},
            move_budget_rows=1, shard_read_caps={"a": 50, "b": 50}, workload=workload,
        )
        layout = result["selections"]["request_economy"]["layout"]
        null_record = layout["queries"][0]
        self.assertEqual(null_record["targets"], [])
        self.assertEqual(null_record["requests"], 0)

    def test_null_key_default_policy_bills_default_shard(self):
        topo = topology(list_doc({"x": "a", "y": "b"}))
        rows = {"items": rows_for({"x": 1, "y": 1})}
        workload = [{"frequency": 6, "query": q("is_null", "is_null", None)}]
        result = search_layouts(
            topo, "items", rows, {"y": ["a", "b"]},
            move_budget_rows=1, shard_read_caps={"a": 50, "b": 50}, workload=workload,
        )
        layout = result["selections"]["request_economy"]["layout"]
        self.assertEqual(layout["queries"][0]["targets"], ["a"])
        self.assertEqual(layout["queries"][0]["requests"], 6)
        self.assertEqual(layout["shard_load"]["a"], 6)

    def test_non_key_filter_counts_full_shard_access(self):
        # Keys map into both shards and there is no default, so both physical
        # shards are reachable and a non-prunable predicate fans out to both.
        topo = topology(list_doc(
            {"x": "a", "y": "b", "z": "b"}, null_policy="error", default_shard=None
        ))
        rows = {"items": rows_for({"x": 1, "y": 1, "z": 1})}
        workload = [
            {"frequency": 9, "query": q("non_key", "eq", "v", field="note")},
            {"frequency": 2, "query": q("ne_key", "ne", "x")},
        ]
        result = search_layouts(
            topo, "items", rows, {"y": ["a", "b"]}, fixed_keys=["x", "z"],
            move_budget_rows=1, shard_read_caps={"a": 100, "b": 100}, workload=workload,
        )
        layout = result["selections"]["request_economy"]["layout"]
        decisions = [record["decision"] for record in layout["queries"]]
        # A non-key equality has no key predicate (full_scan); a key 'ne'
        # predicate is explicitly not prunable. Both must still hit every shard.
        self.assertEqual(decisions, ["full_scan", "not_prunable"])
        for record in layout["queries"]:
            self.assertEqual(record["targets"], ["a", "b"])
        # 9 * 2 + 2 * 2 = 22; each shard carries 9 + 2 = 11.
        self.assertEqual(layout["total_requests"], 22)
        self.assertEqual(layout["shard_load"], {"a": 11, "b": 11})

    def test_full_scan_on_default_only_table_targets_single_shard(self):
        topo = topology(list_doc({"x": "a", "y": "b"}))
        rows = {"items": rows_for({"x": 1, "y": 1})}
        workload = [{"frequency": 9, "query": q("non_key", "eq", "v", field="note")}]
        result = search_layouts(
            topo, "items", rows, {"y": ["a", "b"]}, fixed_keys=["x"],
            move_budget_rows=1, shard_read_caps={"a": 100, "b": 100}, workload=workload,
        )
        layout = result["selections"]["request_economy"]["layout"]
        # Only shard 'a' is reachable (default), so the fan-out touches it alone.
        self.assertEqual(layout["queries"][0]["targets"], ["a"])
        self.assertEqual(layout["queries"][0]["requests"], 9)
        self.assertEqual(layout["shard_load"], {"a": 9, "b": 0})

class GroupingAndFrequencyTests(unittest.TestCase):
    def test_same_key_multiple_rows_move_as_one_atomic_group(self):
        topo = topology(list_doc({"x": "a", "y": "b"}))
        rows = {"items": rows_for({"x": 1, "y": 4})}
        workload = [{"frequency": 1, "query": q("eq_y", "eq", "y")}]
        # Budget fits the whole group of 4.
        ok = search_layouts(
            topo, "items", rows, {"y": ["a", "b"]}, fixed_keys=["x"],
            move_budget_rows=4, shard_read_caps={"a": 50, "b": 50}, workload=workload,
        )
        self.assertEqual(ok["status"], "optimal")
        moved = next(
            item for item in ok["feasible_layouts"] if item["list_map"]["y"] == "a"
        )
        self.assertEqual(moved["moved_rows"], 4)
        self.assertEqual(len(moved["moved_row_ids"]), 4)
        self.assertEqual(moved["move_cells"][0]["row_count"], 4)
        # Budget 3 cannot take a partial group: moving is rejected wholesale.
        tight = search_layouts(
            topo, "items", rows, {"y": ["a", "b"]}, fixed_keys=["x"],
            move_budget_rows=3, shard_read_caps={"a": 50, "b": 50}, workload=workload,
        )
        self.assertEqual(tight["status"], "optimal")
        self.assertEqual(tight["selections"]["request_economy"]["layout"]["moved_rows"], 0)
        self.assertTrue(
            any(item["reason"] == "move_budget" for item in tight["rejected_layouts"])
        )
        rejection = next(item for item in tight["rejected_layouts"] if item["reason"] == "move_budget")
        self.assertEqual(rejection["moved_rows"], 4)
        self.assertIn("group", rejection["violations"][0])

    def test_zero_frequency_still_explained_but_bills_nothing(self):
        topo = topology(list_doc({"x": "a", "y": "b"}))
        rows = {"items": rows_for({"x": 1, "y": 1})}
        workload = [
            {"frequency": 0, "query": q("eq_y", "eq", "y")},
            {"frequency": 5, "query": q("eq_x", "eq", "x")},
        ]
        result = search_layouts(
            topo, "items", rows, {"y": ["a", "b"]},
            move_budget_rows=1, shard_read_caps={"a": 50, "b": 50}, workload=workload,
        )
        layout = result["selections"]["request_economy"]["layout"]
        zero = next(item for item in layout["queries"] if item["name"] == "eq_y")
        self.assertEqual(zero["requests"], 0)
        self.assertEqual(zero["shard_contributions"], {})
        self.assertEqual(layout["total_requests"], 5)

    def test_cap_equal_to_load_is_feasible_but_one_more_is_not(self):
        topo = topology(list_doc({"x": "a", "y": "b"}, null_policy="error", default_shard=None))
        rows = {"items": rows_for({"x": 1, "y": 1})}
        workload = [{"frequency": 10, "query": q("eq_x", "eq", "x")}]
        equal = search_layouts(
            topo, "items", rows, {"y": ["a", "b"]},
            move_budget_rows=1, shard_read_caps={"a": 10, "b": 10}, workload=workload,
        )
        self.assertEqual(equal["status"], "optimal")
        self.assertEqual(
            equal["selections"]["request_economy"]["layout"]["shard_load"]["a"], 10
        )
        over = search_layouts(
            topo, "items", rows, {"y": ["a", "b"]},
            move_budget_rows=1, shard_read_caps={"a": 9, "b": 10}, workload=workload,
        )
        self.assertEqual(over["status"], "infeasible")
        self.assertFalse(over["safe_to_execute"])
        cap_evidence = next(
            item for item in over["infeasibility_evidence"]
            if item["constraint"] == "shard_read_caps"
        )
        self.assertEqual(cap_evidence["caps"]["a"], 9)
        # The overload can be proven at the branch bound before any leaf runs.
        self.assertGreaterEqual(
            cap_evidence["leaf_violations"] + over["enumeration"]["pruned_branches"].get("read_cap", 0),
            1,
        )

class InfeasibleAndUnknownTests(unittest.TestCase):
    def setUp(self):
        self.topo = topology(list_doc({"x": "a", "y": "b"}))
        self.rows = {"items": rows_for({"x": 2, "y": 3})}
        self.workload = [{"frequency": 5, "query": q("eq_y", "eq", "y")}]

    def test_budget_infeasible_when_cheapest_assignment_too_expensive(self):
        # y can only move to a; moving 3 rows exceeds budget 2.
        result = search_layouts(
            self.topo, "items", self.rows, {"y": ["a"]},
            move_budget_rows=2, shard_read_caps={"a": 50, "b": 50},
            workload=self.workload,
        )
        self.assertEqual(result["status"], "infeasible")
        self.assertFalse(result["safe_to_execute"])
        evidence = result["infeasibility_evidence"]
        self.assertEqual(evidence[0]["constraint"], "move_budget_rows")
        self.assertEqual(evidence[0]["minimum_moves"], 3)
        self.assertEqual(result["feasible_layouts"], [])
        self.assertIsNone(result["selections"]["request_economy"])
        self.assertIsNone(result["selections"]["hotspot_relief"])

    def test_unknown_when_evaluation_budget_exhausted(self):
        # Three binary keys -> 8 layouts; one leaf evaluation cannot finish.
        # Separate per-key queries keep the partial-assignment lower bound
        # loose enough that at least one objective cannot be certified.
        topo = topology(list_doc(
            {"x": "a", "y": "b", "z": "b"}, null_policy="error", default_shard=None
        ))
        rows = {"items": rows_for({"x": 2, "y": 3, "z": 1})}
        workload = [
            {"frequency": 5, "query": q("eq_x", "eq", "x")},
            {"frequency": 5, "query": q("eq_y", "eq", "y")},
            {"frequency": 5, "query": q("eq_z", "eq", "z")},
        ]
        result = search_layouts(
            topo, "items", rows,
            {"x": ["a", "b"], "y": ["a", "b"], "z": ["a", "b"]},
            move_budget_rows=6, shard_read_caps={"a": 100, "b": 100},
            workload=workload, max_evaluations=1,
        )
        self.assertEqual(result["status"], "unknown")
        self.assertFalse(result["safe_to_execute"])
        self.assertFalse(result["enumeration"]["exhausted"])
        self.assertEqual(result["enumeration"]["evaluated_feasible"], 1)
        statuses = {
            objective: result["selections"][objective]["status"]
            for objective in result["objectives"]
        }
        self.assertIn("unknown", statuses.values())
        for objective in result["objectives"]:
            selection = result["selections"][objective]
            self.assertIn(selection["status"], {"unknown", "optimal"})
            if selection["status"] == "unknown":
                self.assertIn("not certified optimal", selection["optimality_evidence"])
                self.assertIsNotNone(selection["lower_bound"])
            else:
                self.assertIn("lower bound", selection["optimality_evidence"])
        # The incumbent feasible candidate is still reported, never as a safe optimum.
        self.assertEqual(len(result["feasible_layouts"]), 1)

    def test_large_domain_cutoff_keeps_incumbent_without_safe_claim(self):
        # 8 binary keys (256 layouts) with per-key queries keep bounds loose;
        # a one-evaluation budget must return UNKNOWN with one incumbent.
        keys = {"k0": "a"}
        keys.update({f"k{i}": "b" for i in range(1, 8)})
        topo = topology(list_doc(keys, null_policy="error", default_shard=None))
        rows = {"items": rows_for({f"k{i}": 1 for i in range(8)})}
        workload = [
            {"frequency": 5, "query": q(f"eq_k{i}", "eq", f"k{i}")} for i in range(8)
        ]
        result = search_layouts(
            topo, "items", rows,
            {f"k{i}": ["a", "b"] for i in range(8)},
            move_budget_rows=8, shard_read_caps={"a": 1000, "b": 1000},
            workload=workload, max_evaluations=1,
        )
        self.assertEqual(result["status"], "unknown")
        self.assertFalse(result["safe_to_execute"])
        self.assertFalse(result["enumeration"]["exhausted"])
        self.assertEqual(len(result["feasible_layouts"]), 1)
        self.assertIn(
            "unknown",
            {result["selections"][o]["status"] for o in result["objectives"]},
        )

    def test_unknown_never_claims_safe_to_execute(self):
        topo = topology(list_doc(
            {"x": "a", "y": "b", "z": "b"}, null_policy="error", default_shard=None
        ))
        rows = {"items": rows_for({"x": 2, "y": 3, "z": 1})}
        workload = [
            {"frequency": 5, "query": q("eq_x", "eq", "x")},
            {"frequency": 5, "query": q("eq_y", "eq", "y")},
            {"frequency": 5, "query": q("eq_z", "eq", "z")},
        ]
        result = search_layouts(
            topo, "items", rows,
            {"x": ["a", "b"], "y": ["a", "b"], "z": ["a", "b"]},
            move_budget_rows=6, shard_read_caps={"a": 100, "b": 100},
            workload=workload, max_evaluations=1,
        )
        self.assertFalse(result["safe_to_execute"])

    def test_enough_budget_certifies_optimal_with_evidence(self):
        result = search_layouts(
            self.topo, "items", self.rows, {"y": ["a", "b"]},
            move_budget_rows=3, shard_read_caps={"a": 50, "b": 50},
            workload=self.workload,
        )
        self.assertEqual(result["status"], "optimal")
        for objective in result["objectives"]:
            selection = result["selections"][objective]
            self.assertEqual(selection["status"], "optimal")
            self.assertIsNone(selection["lower_bound"])
            self.assertIn("exhaustive enumeration", selection["optimality_evidence"])

class ValidationAndSafetyTests(unittest.TestCase):
    def setUp(self):
        self.topo = topology(list_doc({"x": "a", "y": "b"}))
        self.rows = {"items": rows_for({"x": 1, "y": 1})}
        self.workload = [{"frequency": 1, "query": q("eq_y", "eq", "y")}]
        self.caps = {"a": 50, "b": 50}

    def test_rejects_unknown_table_non_list_router_bad_domains(self):
        with self.assertRaises(ShardRouteError) as caught:
            search_layouts(
                self.topo, "nope", self.rows, {"y": ["a"]},
                move_budget_rows=1, shard_read_caps=self.caps, workload=self.workload,
            )
        self.assertEqual(caught.exception.code, "SEARCH_INVALID")
        with self.assertRaises(ShardRouteError):
            search_layouts(
                self.topo, "items", self.rows, {"ghost": ["a"]},
                move_budget_rows=1, shard_read_caps=self.caps, workload=self.workload,
            )
        with self.assertRaises(ShardRouteError):
            search_layouts(
                self.topo, "items", self.rows, {"y": ["c"]},
                move_budget_rows=1, shard_read_caps=self.caps, workload=self.workload,
            )
        with self.assertRaises(ShardRouteError):
            search_layouts(
                self.topo, "items", self.rows, {"y": ["a", "b"]},
                fixed_keys=["y"], move_budget_rows=1,
                shard_read_caps=self.caps, workload=self.workload,
            )
        with self.assertRaises(ShardRouteError):
            search_layouts(
                self.topo, "items", self.rows, {"y": ["a", "b"]},
                move_budget_rows=-1, shard_read_caps=self.caps, workload=self.workload,
            )
        with self.assertRaises(ShardRouteError):
            search_layouts(
                self.topo, "items", self.rows, {"y": ["a", "b"]},
                move_budget_rows=1, shard_read_caps={"a": 50}, workload=self.workload,
            )
        with self.assertRaises(ShardRouteError):
            search_layouts(
                self.topo, "items", self.rows, {"y": ["a", "b"]},
                move_budget_rows=1, shard_read_caps=self.caps,
                workload=[{"frequency": -1, "query": q("eq_y", "eq", "y")}],
            )

    def test_blocking_audit_findings_prevent_safe_to_execute(self):
        audit = {"findings": [{"severity": "critical", "table": "items", "code": "MISSING_ROW"}]}
        result = search_layouts(
            self.topo, "items", self.rows, {"y": ["a", "b"]},
            move_budget_rows=1, shard_read_caps=self.caps,
            workload=self.workload, audit=audit,
        )
        self.assertEqual(result["status"], "optimal")
        self.assertFalse(result["safe_to_execute"])
        self.assertTrue(any(item["code"] == "PLAN_AUDIT_MISSING_ROW" for item in result["blockers"]))

    def test_old_topology_is_not_mutated(self):
        before = dict(self.topo.table("items").list_map)
        fingerprint_before = self.topo.fingerprint
        search_layouts(
            self.topo, "items", self.rows, {"y": ["a", "b"]},
            move_budget_rows=1, shard_read_caps=self.caps, workload=self.workload,
        )
        self.assertEqual(self.topo.table("items").list_map, before)
        self.assertEqual(self.topo.fingerprint, fingerprint_before)

    def test_unexplainable_query_makes_every_candidate_unsafe(self):
        # No default shard and an unmapped value: every candidate explanation
        # carries a route error, so no executable recommendation is allowed.
        topo = topology(list_doc(
            {"x": "a", "y": "b"}, null_policy="error", default_shard=None
        ))
        rows = {"items": rows_for({"x": 1, "y": 1})}
        result = search_layouts(
            topo, "items", rows, {"y": ["a", "b"]},
            move_budget_rows=5, shard_read_caps={"a": 100, "b": 100},
            workload=[{"frequency": 4, "query": q("ghost", "eq", "ghost")}],
        )
        self.assertEqual(result["status"], "infeasible")
        self.assertFalse(result["safe_to_execute"])
        self.assertEqual(result["feasible_layouts"], [])
        self.assertEqual(result["enumeration"]["infeasible_reasons"], {"route_error": 2})
        self.assertTrue(all(
            any("could not be routed" in v for v in item["violations"])
            for item in result["rejected_layouts"]
        ))

class TieBreakAndWorkloadHelperTests(unittest.TestCase):
    def test_lexicographic_layout_tie_break(self):
        # y and z carry no rows: all four assignments tie on requests and
        # moves, so ties must resolve on the stable sorted list_map order.
        topo = topology(list_doc({"x": "a", "y": "b", "z": "b"}, null_policy="error", default_shard=None))
        rows = {"items": rows_for({"x": 1})}
        workload = [{"frequency": 3, "query": q("eq_x", "eq", "x")}]
        result = search_layouts(
            topo, "items", rows,
            {"y": ["a", "b"], "z": ["a", "b"]},
            fixed_keys=["x"], move_budget_rows=0,
            shard_read_caps={"a": 50, "b": 50}, workload=workload,
        )
        self.assertEqual(result["status"], "optimal")
        winner = result["selections"]["request_economy"]["layout"]
        # Smallest lexicographic full list_map: y=a, z=a win the tie.
        self.assertEqual(winner["list_map"], {"x": "a", "y": "a", "z": "a"})
        self.assertEqual(winner["moved_rows"], 0)
        maps = [tuple(sorted(item["list_map"].items())) for item in result["feasible_layouts"]]
        self.assertEqual(maps, sorted(maps))
        self.assertEqual(len(maps), 4)
        # Both objectives must resolve to the same deterministic layout here.
        self.assertEqual(
            result["selections"]["hotspot_relief"]["layout"]["list_map"],
            winner["list_map"],
        )

    def test_explain_workload_billing_and_lower_bound_helper(self):
        topo = topology(list_doc({"x": "a", "y": "b"}))
        workload = [
            {"frequency": 2, "query": q("in_xy", "in", ["x", "y"])},
            {"frequency": 3, "query": q("eq_x", "eq", "x")},
        ]
        explained = explain_workload(topo, workload)
        self.assertEqual(explained[0]["requests"], 4)  # 2 freqs * 2 targets
        self.assertEqual(explained[1]["requests"], 3)
        table = topo.table("items")
        filters_a = [explained[0]["explanation"].filters[0]]
        forced, full, complete = forced_targets(
            table, filters_a, partial_map={"x": "a"}, open_keys=frozenset({"y"})
        )
        self.assertFalse(full)
        self.assertFalse(complete)  # y is still open
        self.assertEqual(forced, {"a"})  # y's old shard must not be assumed
        full_forced, full_scan, complete2 = forced_targets(
            table, [explained[1]["explanation"].filters[0]],
            partial_map={}, open_keys=frozenset({"x"}),
        )
        self.assertEqual(full_forced, set())
        self.assertFalse(full_scan)
        self.assertFalse(complete2)
        non_key = q("scan", "eq", "v", field="note")
        from shardroute.query_explain import parse_workload_query
        _, _, _, scan_filters = parse_workload_query(topo, non_key)
        from shardroute.query_support import _key_filters
        scan_forced, is_full, scan_complete = forced_targets(
            table, _key_filters(table, scan_filters), partial_map={}, open_keys=frozenset()
        )
        self.assertTrue(is_full)
        self.assertTrue(scan_complete)
        self.assertEqual(scan_forced, {"a", "b"})

class ReportTests(unittest.TestCase):
    def test_report_rendering_and_artifacts(self):
        topo = topology(list_doc({"x": "a", "y": "b"}))
        rows = {"items": rows_for({"x": 1, "y": 1})}
        workload = [
            {"frequency": 100, "query": q("eq_x", "eq", "x")},
            {"frequency": 100, "query": q("eq_y", "eq", "y")},
            {"frequency": 10, "query": q("in_xy", "in", ["x", "y"])},
        ]
        search = search_layouts(
            topo, "items", rows, {"y": ["a", "b"]}, fixed_keys=["x"],
            move_budget_rows=1, shard_read_caps={"a": 220, "b": 220}, workload=workload,
        )
        report = build_layout_search_report(old=topo, search=search, seed=9)
        self.assertEqual(report["status"], "optimal")
        self.assertTrue(report["run_id"].startswith("layoutsearch_"))
        markdown = render_layout_search_markdown(report)
        self.assertIn("list_map layout search", markdown)
        self.assertIn("request_economy", markdown)
        self.assertIn("hotspot_relief", markdown)
        self.assertIn("210", markdown)
        with tempfile.TemporaryDirectory() as temporary:
            artifacts = write_layout_search_report(report, temporary)
            names = [item["path"] for item in artifacts]
            self.assertEqual(
                names, ["reports/layout_search.json", "reports/layout_search.md"]
            )
            for item in artifacts:
                path = Path(temporary) / item["path"]
                self.assertTrue(path.is_file())
                self.assertEqual(len(path.read_bytes()), item["bytes"])
            loaded = json.loads(
                (Path(temporary) / "reports" / "layout_search.json").read_text(encoding="utf-8")
            )
            self.assertEqual(loaded["run_id"], report["run_id"])
            # Deterministic bytes across runs.
            again = write_layout_search_report(
                build_layout_search_report(old=topo, search=search, seed=9), temporary
            )
            self.assertEqual(artifacts, again)

class CliLayoutSearchTests(unittest.TestCase):
    def _workspace(self, root: Path) -> Path:
        config = root / "config"
        logical_dir = root / "logical"
        shard_dir = root / "shards" / "items"
        queries_dir = root / "queries"
        for path in (config, logical_dir, shard_dir, queries_dir):
            path.mkdir(parents=True, exist_ok=True)
        doc = list_doc({"x": "a", "y": "b"})
        write_json(config / "topology_old.json", doc)
        write_json(config / "topology_new.json", doc)
        (logical_dir / "items.csv").write_text(
            "row_id,k\n" + "\n".join(f"{key}-{i},{key}" for key, count in (("x", 2), ("y", 3)) for i in range(count)) + "\n",
            encoding="utf-8",
        )
        for name, value in (("eq_x", "x"), ("eq_y", "y")):
            write_json(queries_dir / f"{name}.json", q(name, "eq", value))
        spec = {
            "table": "items",
            "domains": {"y": ["a", "b"]},
            "fixed_keys": ["x"],
            "move_budget_rows": 3,
            "shard_read_caps": {"a": 220, "b": 220},
            "workload": [
                {"frequency": 100, "query": q("eq_x", "eq", "x")},
                {"frequency": 100, "query": q("eq_y", "eq", "y")},
                {"frequency": 10, "query": q("in_xy", "in", ["x", "y"])},
            ],
        }
        write_json(root / "layout_spec.json", spec)
        return root

    def test_cli_layout_search_emits_result_and_writes_report(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = self._workspace(Path(temporary) / "ws")
            buffer = io.StringIO()
            import contextlib
            with contextlib.redirect_stdout(buffer):
                code = main([
                    "layout-search", "--workspace", str(root),
                    "--spec", str(root / "layout_spec.json"), "--write-report",
                ])
            self.assertEqual(code, 0)
            payload = json.loads(buffer.getvalue())
            result = payload["result"]
            self.assertEqual(result["status"], "optimal")
            economy = result["selections"]["request_economy"]["layout"]
            hotspot = result["selections"]["hotspot_relief"]["layout"]
            self.assertEqual(economy["total_requests"], 210)
            self.assertEqual(hotspot["peak_shard_requests"], 110)
            self.assertTrue((root / "reports" / "layout_search.json").is_file())
            self.assertTrue((root / "reports" / "layout_search.md").is_file())

    def test_cli_workload_falls_back_to_workspace_query_files(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = self._workspace(Path(temporary) / "ws")
            spec_path = root / "no_workload_spec.json"
            write_json(spec_path, {
                "table": "items",
                "domains": {"y": ["a", "b"]},
                "fixed_keys": ["x"],
                "move_budget_rows": 3,
                "shard_read_caps": {"a": 220, "b": 220},
            })
            buffer = io.StringIO()
            import contextlib
            with contextlib.redirect_stdout(buffer):
                code = main([
                    "layout-search", "--workspace", str(root), "--spec", str(spec_path),
                ])
            self.assertEqual(code, 0)
            result = json.loads(buffer.getvalue())
            # Two query files, each frequency 1.
            self.assertEqual(len(result["workload"]), 2)
            self.assertTrue(all(entry["frequency"] == 1 for entry in result["workload"]))

    def test_cli_legacy_plan_command_unchanged(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = self._workspace(Path(temporary) / "ws")
            buffer = io.StringIO()
            import contextlib
            with contextlib.redirect_stdout(buffer):
                code = main(["plan", "--workspace", str(root)])
            self.assertEqual(code, 0)
            plan = json.loads(buffer.getvalue())
            for key in (
                "moved_rows", "move_cells", "batches", "blockers",
                "rollback_checklist", "safe_to_execute", "summary",
            ):
                self.assertIn(key, plan)


if __name__ == "__main__":
    unittest.main()
