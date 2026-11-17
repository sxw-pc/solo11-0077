import math
import unittest

from shardroute.audit import run_audit
from shardroute.models import ScanResult
from shardroute.router import Router
from shardroute.skew import (
    DECISION_NOT_SIGNIFICANT,
    DECISION_SIGNIFICANT,
    DECISION_UNKNOWN,
    EXACT_CERTIFICATE,
    HOEFFDING_CERTIFICATE,
    HYPOTHESIS_LABEL,
    SkewBudgetExceeded,
    conservative_hoeffding_bound,
    exact_tail_probability,
)
from shardroute.report import build_report, render_markdown
from shardroute.html_report import render_html

from support import empty_plan, logical, topology_from

TABLE = "t_skew"


def skew_doc(bucket_map, shards=("s0", "s1"), null_policy="error",
             default_shard=None, case_sensitive=True, key="k"):
    table = {
        "table": TABLE,
        "router": "hash",
        "key": key,
        "null_policy": null_policy,
        "buckets": len(bucket_map),
        "bucket_map": list(bucket_map),
        "case_sensitive": case_sensitive,
    }
    if default_shard is not None:
        table["default_shard"] = default_shard
    return {
        "version": 1,
        "generated_at": "2026-01-02T00:00:00Z",
        "description": "skew diagnostic synthetic topology",
        "shards": list(shards),
        "tables": [table],
    }


def balanced_topology():
    # Two shards, each owns half of four buckets.
    return topology_from(skew_doc(["s0", "s0", "s1", "s1"]))


def keys_routing_to(topology, shard, count, table=TABLE):
    router = Router(topology)
    found = []
    index = 0
    while len(found) < count:
        candidate = f"key_{index:06d}"
        index += 1
        if router.route(table, candidate).target_shard == shard:
            found.append(candidate)
    return found


def rows_for_keys(keys, rows_per_key, table=TABLE, field="k"):
    rows = []
    row_index = 0
    for key in keys:
        for _ in range(rows_per_key):
            row_index += 1
            rows.append(logical(table, f"row_{row_index:06d}", {field: key}))
    return rows


def skew_scan(topology, logical_rows):
    return ScanResult(
        logical_rows={TABLE: logical_rows},
        physical_rows={TABLE: []},
        files=[],
        manifests={},
        manifest_errors=[],
        physical_dirs=[],
        logical_fingerprint="0" * 64,
        physical_fingerprint="0" * 64,
    )


def diagnostic_for(topology, rows, alpha=0.01, **budget):
    result = run_audit(topology, skew_scan(topology, rows), alpha=alpha, **budget)
    return result["hash_skew_diagnostics"][TABLE]


class EightGroupCounterexampleTests(unittest.TestCase):
    """The headline case: 8 canonical keys x 10 rows, all on one shard (80:0)."""

    def setUp(self):
        self.topology = balanced_topology()
        self.keys = keys_routing_to(self.topology, "s0", 8)
        self.rows = rows_for_keys(self.keys, 10)
        self.scan = skew_scan(self.topology, self.rows)

    def test_group_level_tail_is_two_over_256_not_two_over_2_80(self):
        diagnostic = diagnostic_for(self.topology, self.rows, alpha=0.02)
        certificate = diagnostic["certificate"]
        self.assertEqual(certificate["type"], EXACT_CERTIFICATE)
        self.assertAlmostEqual(certificate["tail_probability"], 2 / 2 ** 8)
        self.assertEqual(certificate["tail_probability"], 0.0078125)
        # The per-row independent model 2/2^80 must never appear.
        self.assertNotAlmostEqual(certificate["tail_probability"], 2 / 2 ** 80)

    def test_alpha_thresholds_three_state_gate(self):
        at_005 = diagnostic_for(self.topology, self.rows, alpha=0.005)
        self.assertEqual(
            at_005["certificate"]["decision"], DECISION_NOT_SIGNIFICANT
        )
        at_02 = diagnostic_for(self.topology, self.rows, alpha=0.02)
        self.assertEqual(at_02["certificate"]["decision"], DECISION_SIGNIFICANT)

    def test_reviewable_denominator_groups_and_baseline(self):
        diagnostic = diagnostic_for(self.topology, self.rows)
        denominator = diagnostic["denominator"]
        self.assertEqual(denominator["logical_rows"], 80)
        self.assertEqual(denominator["eligible_hash_rows"], 80)
        self.assertEqual(denominator["excluded_rows"], 0)
        self.assertEqual(denominator["exclusions_by_reason"], {})
        self.assertEqual(diagnostic["key_groups"]["count"], 8)
        self.assertEqual(diagnostic["key_groups"]["total_weight"], 80)
        for group in diagnostic["key_groups"]["groups"]:
            self.assertEqual(group["weight"], 10)
            self.assertEqual(len(group["row_ids"]), 10)
            self.assertEqual(group["routed_shard"], "s0")
        self.assertEqual(
            diagnostic["bucket_baseline"]["shard_probabilities"],
            {"s0": 0.5, "s1": 0.5},
        )
        event = diagnostic["event"]
        self.assertEqual(event["threshold"], 80)
        self.assertEqual(event["observed_max_load"], 80)
        self.assertEqual(event["observed_max_shard"], "s0")
        self.assertEqual(event["observed_shard_loads"], {"s0": 80, "s1": 0})
        self.assertEqual(diagnostic["hypothesis"], HYPOTHESIS_LABEL)
        self.assertIn("does not claim SHA-256", diagnostic["scope_note"])

    def test_deterministic_across_runs(self):
        first = run_audit(self.topology, self.scan)["hash_skew_diagnostics"]
        second = run_audit(self.topology, self.scan)["hash_skew_diagnostics"]
        self.assertEqual(first, second)


class ExactMathTests(unittest.TestCase):
    def test_unequal_bucket_map_probabilities_are_kept(self):
        # s0 owns 3/4 buckets, s1 owns 1/4.
        topology = topology_from(skew_doc(["s0", "s0", "s0", "s1"]))
        diagnostic = diagnostic_for(topology, [])
        self.assertEqual(
            diagnostic["bucket_baseline"]["shard_probabilities"],
            {"s0": 0.75, "s1": 0.25},
        )
        # Two unit-weight groups, event max load >= 2 means both groups land
        # on the same shard: 0.75^2 + 0.25^2 = 0.625 (no flat 1/2 split).
        tail, _ = exact_tail_probability([1, 1], (0.75, 0.25), 2)
        self.assertAlmostEqual(tail, 0.625)
        # Three unit groups, all on the light shard: event max >= 3 means
        # all three agree: 0.75^3 + 0.25^3 = 0.4375.
        tail_three, _ = exact_tail_probability([1, 1, 1], (0.75, 0.25), 3)
        self.assertAlmostEqual(tail_three, 0.4375)

    def test_unequal_group_weights_move_together(self):
        # Groups of weight 3 and 1, balanced shards. Max load >= 4 only when
        # both groups take the same shard: probability 0.5.
        tail, _ = exact_tail_probability([3, 1], (0.5, 0.5), 4)
        self.assertAlmostEqual(tail, 0.5)
        # Max load >= 3 is a certainty: the weight-3 group always lands
        # somewhere, so the tail is exactly 1.
        tail_three, _ = exact_tail_probability([3, 1], (0.5, 0.5), 3)
        self.assertAlmostEqual(tail_three, 1.0)

    def test_unequal_weights_end_to_end(self):
        topology = balanced_topology()
        heavy = keys_routing_to(topology, "s0", 1)[0]
        light = keys_routing_to(topology, "s0", 2)[1]
        rows = rows_for_keys([heavy], 3) + rows_for_keys([light], 1)
        diagnostic = diagnostic_for(topology, rows, alpha=0.02)
        weights = sorted(
            group["weight"] for group in diagnostic["key_groups"]["groups"]
        )
        self.assertEqual(weights, [1, 3])
        self.assertAlmostEqual(diagnostic["certificate"]["tail_probability"], 0.5)
        self.assertEqual(
            diagnostic["certificate"]["decision"], DECISION_NOT_SIGNIFICANT
        )
        with self.assertRaises(Exception):
            conservative_hoeffding_bound([3, 1], (0.5, 0.5), 4)

    def test_impossible_event_has_zero_tail(self):
        tail, _ = exact_tail_probability([1, 1], (0.5, 0.5), 5)
        self.assertEqual(tail, 0.0)

    def test_dp_matches_exhaustive_enumeration(self):
        import itertools

        cases = [
            ([1] * 6, (0.5, 0.5)),
            ([2, 1, 3, 1], (0.5, 0.5)),
            ([1] * 5, (0.7, 0.3)),
            ([2, 2, 1], (0.6, 0.25, 0.15)),
            ([3, 1, 2], (0.34, 0.33, 0.33)),
        ]
        for weights, probabilities in cases:
            shard_count = len(probabilities)
            total = sum(weights)
            for threshold in range(1, total + 1):
                expected = 0.0
                for assignment in itertools.product(range(shard_count), repeat=len(weights)):
                    loads = [0] * shard_count
                    mass = 1.0
                    for group_index, shard_index in enumerate(assignment):
                        loads[shard_index] += weights[group_index]
                        mass *= probabilities[shard_index]
                    if max(loads) >= threshold:
                        expected += mass
                actual, _ = exact_tail_probability(weights, probabilities, threshold)
                self.assertAlmostEqual(
                    actual,
                    expected,
                    places=12,
                    msg=f"weights={weights} probs={probabilities} threshold={threshold}",
                )


class CanonicalizationTests(unittest.TestCase):
    def test_case_and_trim_collapse_into_one_group(self):
        topology = topology_from(
            skew_doc(["s0", "s0", "s1", "s1"], case_sensitive=False)
        )
        rows = (
            rows_for_keys(["Alpha"], 5)
            + rows_for_keys([" alpha "], 5)
            + rows_for_keys(["ALPHA"], 4)
        )
        diagnostic = diagnostic_for(topology, rows)
        groups = diagnostic["key_groups"]["groups"]
        self.assertEqual(len(groups), 1)
        self.assertEqual(groups[0]["canonical_key"], "alpha")
        self.assertEqual(groups[0]["weight"], 14)
        self.assertEqual(len(groups[0]["row_ids"]), 14)

    def test_composite_key_collapses_components(self):
        doc = skew_doc(
            ["s0", "s0", "s1", "s1"], key=["k1", "k2"]
        )
        topology = topology_from(doc)
        rows = [
            logical(TABLE, "r1", {"k1": "a", "k2": "b"}),
            logical(TABLE, "r2", {"k1": " a ", "k2": "b"}),
        ]
        diagnostic = diagnostic_for(topology, rows)
        groups = diagnostic["key_groups"]["groups"]
        self.assertEqual(len(groups), 1)
        self.assertEqual(groups[0]["weight"], 2)
        self.assertIn("\x1f", groups[0]["canonical_key"])


class ExclusionTests(unittest.TestCase):
    def _two_table_topology(self, null_policy, default_shard):
        doc = skew_doc(
            ["s0", "s0", "s1", "s1"],
            shards=("s0", "s1", "s2"),
            null_policy=null_policy,
            default_shard=default_shard,
        )
        doc["tables"].append(
            {
                "table": "t_list",
                "router": "list",
                "key": "region",
                "null_policy": "route_default",
                "default_shard": "s2",
                "list_map": {"region_alpha": "s0"},
            }
        )
        return topology_from(doc)

    def test_null_drop_default_error_and_non_hash_are_excluded(self):
        topology = self._two_table_topology("route_default", "s0")
        hash_rows = [
            logical(TABLE, "h1", {"k": None}),       # null -> default
            logical(TABLE, "h2", {"k": "key_one"}),
            logical(TABLE, "h3", {"k": "key_one"}),
            logical(TABLE, "h4", {}),                # missing key -> route_error
        ]
        list_rows = [
            logical("t_list", "l1", {"region": "region_alpha"}),
            logical("t_list", "l2", {"region": "other"}),
        ]
        scan = ScanResult(
            logical_rows={TABLE: hash_rows, "t_list": list_rows},
            physical_rows={TABLE: [], "t_list": []},
            files=[], manifests={}, manifest_errors=[], physical_dirs=[],
            logical_fingerprint="0" * 64, physical_fingerprint="0" * 64,
        )
        diagnostics = run_audit(topology, scan)["hash_skew_diagnostics"]
        hash_diag = diagnostics[TABLE]
        self.assertTrue(hash_diag["applicable"])
        self.assertEqual(hash_diag["denominator"]["logical_rows"], 4)
        self.assertEqual(hash_diag["denominator"]["eligible_hash_rows"], 2)
        self.assertEqual(hash_diag["denominator"]["excluded_rows"], 2)
        self.assertEqual(
            hash_diag["denominator"]["exclusions_by_reason"],
            {"null_default": 1, "route_error": 1},
        )
        self.assertEqual(hash_diag["key_groups"]["count"], 1)
        self.assertEqual(hash_diag["key_groups"]["groups"][0]["weight"], 2)
        self.assertFalse(diagnostics["t_list"]["applicable"])
        self.assertEqual(diagnostics["t_list"]["reason"], "router_not_hash")

    def test_null_drop_and_null_error_exclusions(self):
        topology = self._two_table_topology("drop", None)
        rows = [
            logical(TABLE, "h1", {"k": None}),
            logical(TABLE, "h2", {"k": "key_one"}),
        ]
        diagnostic = diagnostic_for(topology, rows)
        self.assertEqual(
            diagnostic["denominator"]["exclusions_by_reason"], {"null_drop": 1}
        )
        topology_error = self._two_table_topology("error", None)
        diagnostic_error = diagnostic_for(topology_error, rows)
        self.assertEqual(
            diagnostic_error["denominator"]["exclusions_by_reason"],
            {"null_error": 1},
        )
        self.assertEqual(diagnostic_error["key_groups"]["count"], 1)


class ConservativeCertificateTests(unittest.TestCase):
    def setUp(self):
        self.topology = balanced_topology()
        self.keys = keys_routing_to(self.topology, "s0", 8)
        self.rows = rows_for_keys(self.keys, 10)

    def test_hoeffding_bound_matches_known_constant(self):
        bound = conservative_hoeffding_bound([10] * 8, (0.5, 0.5), 80)
        self.assertAlmostEqual(bound, 2 * math.exp(-4))
        self.assertAlmostEqual(bound, 0.03663127777746836)

    def test_fallback_returns_unknown_when_bound_crosses_alpha(self):
        diagnostic = diagnostic_for(
            self.topology, self.rows, alpha=0.02, max_exact_states=2
        )
        certificate = diagnostic["certificate"]
        self.assertEqual(certificate["type"], HOEFFDING_CERTIFICATE)
        self.assertIsNone(certificate["tail_probability"])
        self.assertAlmostEqual(
            certificate["tail_probability_upper_bound"], 2 * math.exp(-4)
        )
        self.assertEqual(certificate["decision"], DECISION_UNKNOWN)
        self.assertEqual(certificate["unknown_reason"], "upper_bound_above_alpha")
        self.assertFalse(certificate["budget"]["exact_completed"])
        self.assertIn("budget", certificate["budget"]["fallback_reason"])

    def test_exact_and_conservative_certificates_can_differ_on_same_event(self):
        exact = diagnostic_for(self.topology, self.rows, alpha=0.02)
        self.assertEqual(exact["certificate"]["type"], EXACT_CERTIFICATE)
        self.assertEqual(exact["certificate"]["decision"], DECISION_SIGNIFICANT)
        conservative = diagnostic_for(
            self.topology, self.rows, alpha=0.02, max_exact_states=2
        )
        self.assertEqual(
            conservative["certificate"]["type"], HOEFFDING_CERTIFICATE
        )
        self.assertEqual(
            conservative["certificate"]["decision"], DECISION_UNKNOWN
        )
        # Same alpha, same observed event: only the certificate strength differs.
        self.assertEqual(exact["event"], conservative["event"])

    def test_bound_below_alpha_can_still_prove_significance(self):
        diagnostic = diagnostic_for(
            self.topology, self.rows, alpha=0.05, max_exact_states=2
        )
        certificate = diagnostic["certificate"]
        self.assertEqual(certificate["type"], HOEFFDING_CERTIFICATE)
        self.assertEqual(certificate["decision"], DECISION_SIGNIFICANT)

    def test_unequal_shapes_honestly_return_unknown(self):
        heavy = keys_routing_to(self.topology, "s0", 1)[0]
        light = keys_routing_to(self.topology, "s0", 2)[1]
        rows = rows_for_keys([heavy], 3) + rows_for_keys([light], 1)
        diagnostic = diagnostic_for(
            self.topology, rows, alpha=0.02, max_exact_states=1
        )
        self.assertEqual(diagnostic["certificate"]["decision"], DECISION_UNKNOWN)
        self.assertIn(
            "requires_equal_group_weights",
            diagnostic["certificate"]["unknown_reason"],
        )
        unequal_doc = topology_from(skew_doc(["s0", "s0", "s0", "s1"]))
        unequal_keys = keys_routing_to(unequal_doc, "s1", 4)
        unequal_rows = rows_for_keys(unequal_keys, 1)
        diagnostic_unbalanced = diagnostic_for(
            unequal_doc, unequal_rows, alpha=0.02, max_exact_states=1
        )
        self.assertEqual(
            diagnostic_unbalanced["certificate"]["decision"], DECISION_UNKNOWN
        )
        self.assertIn(
            "requires_two_equal_weight_shards",
            diagnostic_unbalanced["certificate"]["unknown_reason"],
        )


class BudgetTests(unittest.TestCase):
    def test_total_weight_budget_is_enforced_not_approximated(self):
        with self.assertRaises(SkewBudgetExceeded) as caught:
            exact_tail_probability(
                [10] * 8, (0.5, 0.5), 80, max_total_weight=50
            )
        self.assertEqual(caught.exception.reason, "total_weight_budget")

    def test_state_budget_is_enforced(self):
        with self.assertRaises(SkewBudgetExceeded) as caught:
            exact_tail_probability(
                [1] * 20, (0.5, 0.5), 15, max_states=4
            )
        self.assertEqual(caught.exception.reason, "state_count_budget")

    def test_no_exact_value_is_fabricated_under_fallback(self):
        topology = balanced_topology()
        keys = keys_routing_to(topology, "s0", 8)
        rows = rows_for_keys(keys, 10)
        diagnostic = diagnostic_for(
            topology, rows, alpha=0.02, max_exact_states=1
        )
        certificate = diagnostic["certificate"]
        self.assertIsNone(certificate["tail_probability"])
        self.assertIsNotNone(certificate["tail_probability_upper_bound"])


class AlphaValidationTests(unittest.TestCase):
    def test_alpha_must_be_in_open_unit_interval(self):
        topology = balanced_topology()
        rows = rows_for_keys(keys_routing_to(topology, "s0", 2), 1)
        for bad_alpha in (0.0, 1.0, -0.1, 1.5):
            with self.assertRaises(ValueError):
                diagnostic_for(topology, rows, alpha=bad_alpha)


class CausalChainTests(unittest.TestCase):
    """Router normalization and bucket_map drive groups, baseline and tail."""

    def test_bucket_map_change_moves_baseline_tail_and_decision(self):
        rows = rows_for_keys(
            keys_routing_to(balanced_topology(), "s0", 6), 1
        )
        balanced = balanced_topology()
        skewed_map = topology_from(skew_doc(["s0", "s0", "s0", "s1"]))
        balanced_diag = diagnostic_for(balanced, rows, alpha=0.05)
        skewed_diag = diagnostic_for(skewed_map, rows, alpha=0.05)
        self.assertEqual(
            balanced_diag["bucket_baseline"]["shard_probabilities"],
            {"s0": 0.5, "s1": 0.5},
        )
        self.assertEqual(
            skewed_diag["bucket_baseline"]["shard_probabilities"],
            {"s0": 0.75, "s1": 0.25},
        )
        # Same logical rows, same six groups, but different tail evidence.
        self.assertEqual(balanced_diag["key_groups"]["count"], 6)
        self.assertEqual(skewed_diag["key_groups"]["count"], 6)
        self.assertNotAlmostEqual(
            balanced_diag["certificate"]["tail_probability"],
            skewed_diag["certificate"]["tail_probability"],
        )

    def test_normalization_change_merges_groups_and_changes_tail(self):
        sensitive = topology_from(
            skew_doc(["s0", "s0", "s1", "s1"], case_sensitive=True)
        )
        insensitive = topology_from(
            skew_doc(["s0", "s0", "s1", "s1"], case_sensitive=False)
        )
        # Three case variants plus one separate key: case-sensitive routing
        # sees 4 groups; case-insensitive routing folds the variants into one
        # weight-30 group, leaving 2 groups with a different tail structure.
        rows = (
            rows_for_keys(["Alpha"], 10)
            + rows_for_keys(["alpha"], 10)
            + rows_for_keys(["ALPHA"], 10)
            + rows_for_keys(["other_key"], 10)
        )
        case_diag = diagnostic_for(sensitive, rows)
        fold_diag = diagnostic_for(insensitive, rows)
        self.assertEqual(case_diag["key_groups"]["count"], 4)
        self.assertEqual(fold_diag["key_groups"]["count"], 2)
        folded_weights = sorted(
            group["weight"] for group in fold_diag["key_groups"]["groups"]
        )
        self.assertEqual(folded_weights, [10, 30])
        self.assertNotEqual(
            case_diag["certificate"]["tail_probability"],
            fold_diag["certificate"]["tail_probability"],
        )

    def test_certificate_contract_is_fully_serializable(self):
        import json

        topology = balanced_topology()
        rows = rows_for_keys(keys_routing_to(topology, "s0", 8), 10)
        result = run_audit(topology, skew_scan(topology, rows))
        # Everything the audit emits must round-trip through JSON for reports.
        encoded = json.dumps(result, sort_keys=True)
        decoded = json.loads(encoded)
        self.assertEqual(
            decoded["hash_skew_diagnostics"][TABLE],
            result["hash_skew_diagnostics"][TABLE],
        )


class ReportIntegrationTests(unittest.TestCase):
    def test_certificate_flows_into_all_report_formats(self):
        topology = balanced_topology()
        keys = keys_routing_to(topology, "s0", 8)
        rows = rows_for_keys(keys, 10)
        audit = run_audit(topology, skew_scan(topology, rows), alpha=0.02)
        report = build_report(
            old=topology,
            new=topology,
            seed=7,
            logical_fingerprint="a" * 64,
            physical_fingerprint="b" * 64,
            audit=audit,
            plan=empty_plan(topology, topology),
            queries=[],
        )
        self.assertEqual(report["summary"]["skew_significant_tables"], 1)
        self.assertEqual(report["summary"]["skew_unknown_tables"], 0)
        markdown = render_markdown(report)
        self.assertIn("## Hash load-skew diagnostics", markdown)
        self.assertIn("independent_canonical_keys_uniform_buckets", markdown)
        self.assertIn("SIGNIFICANT", markdown)
        html = render_html(report)
        self.assertIn('id="skew"', html)
        self.assertIn("exact_weighted_enumeration", html)
        self.assertNotIn("<script", html.lower())

    def test_unknown_certificate_is_rendered_as_unknown(self):
        topology = balanced_topology()
        keys = keys_routing_to(topology, "s0", 8)
        rows = rows_for_keys(keys, 10)
        audit = run_audit(
            topology,
            skew_scan(topology, rows),
            alpha=0.02,
        )
        # Re-run with a crushing budget to force the conservative route.
        audit_unknown = run_audit(
            topology, skew_scan(topology, rows), alpha=0.02, max_exact_states=1
        )
        report = build_report(
            old=topology,
            new=topology,
            seed=7,
            logical_fingerprint="a" * 64,
            physical_fingerprint="b" * 64,
            audit=audit_unknown,
            plan=empty_plan(topology, topology),
            queries=[],
        )
        self.assertEqual(report["summary"]["skew_significant_tables"], 0)
        self.assertEqual(report["summary"]["skew_unknown_tables"], 1)
        markdown = render_markdown(report)
        self.assertIn("UNKNOWN", markdown)
        self.assertIn("upper_bound_above_alpha", markdown)
        self.assertEqual(
            audit["hash_skew_diagnostics"][TABLE]["event"],
            audit_unknown["hash_skew_diagnostics"][TABLE]["event"],
        )


if __name__ == "__main__":
    unittest.main()
