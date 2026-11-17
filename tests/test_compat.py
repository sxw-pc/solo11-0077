import copy
import unittest

from shardroute.compat import (
    CERTIFICATE_FORMAT,
    COUNTEREXAMPLE,
    PERMISSION_ALLOW,
    PERMISSION_BLOCK,
    PERMISSION_SHADOW,
    PROVEN_EQUIVALENT,
    UNKNOWN,
    build_compatibility_certificate,
)
from shardroute.errors import ShardRouteError
from shardroute.planner import build_plan
from shardroute.router import Router

from support import base_doc, logical, topology_from


def table(doc, name):
    return next(item for item in doc["tables"] if item["table"] == name)


def certificate_for(record):
    return next(item for item in record["tables"] if item["table"] == record["_table"])


def replay(topology, table_name, raw_key):
    """Route a witness's raw key through a real Router and return a comparable observation."""
    try:
        trace = Router(topology).route(table_name, raw_key)
    except ShardRouteError as exc:
        return {"kind": "error", "target_shard": None, "error_code": exc.code}
    if trace.dropped:
        return {"kind": "dropped", "target_shard": None, "error_code": None}
    return {"kind": "route", "target_shard": trace.target_shard, "error_code": None}


class CertificateTests(unittest.TestCase):
    def build(self, new_doc, **kwargs):
        old = topology_from(base_doc())
        new = topology_from(new_doc)
        return old, new, build_compatibility_certificate(old, new, **kwargs)

    def orders_new(self, mutate):
        doc = copy.deepcopy(base_doc())
        mutate(table(doc, "orders"))
        return doc

    def test_identical_topology_is_proven_for_every_router(self):
        _, _, cert = self.build(base_doc())
        self.assertEqual(cert["format"], CERTIFICATE_FORMAT)
        self.assertEqual(cert["status"], PROVEN_EQUIVALENT)
        self.assertEqual(cert["permission"], PERMISSION_ALLOW)
        self.assertTrue(cert["future_writes_allowed"])
        self.assertEqual(cert["summary"]["proven_equivalent"], 3)
        methods = {item["table"]: item["proof"]["method"] for item in cert["tables"]}
        self.assertEqual(methods["orders"], "hash_periodic_residue_scan")
        self.assertEqual(methods["events"], "range_boundary_partition")
        self.assertEqual(methods["profiles"], "list_finite_map_plus_default_fallthrough")
        for item in cert["tables"]:
            self.assertTrue(item["proof"]["exhaustive"])
            self.assertTrue(item["normalization_premise"]["normalization_equivalent"])
        self.assertTrue(cert["certificate_id"].startswith("compat_"))
        self.assertEqual(len(cert["digest"]), 64)

    def test_bucket2_change_zero_current_moves_but_blocks_future_writes(self):
        # Old bucket 2 -> shard_b. New bucket 2 -> shard_c. Current rows only hit bucket 0.
        doc = self.orders_new(lambda orders: orders.update(bucket_map=["shard_a", "shard_a", "shard_c", "shard_b"]))
        old, new, _ = self.build(doc)
        rows = {
            "orders": [
                logical("orders", "o1", {"tenant_id": "tenant_demo_006"}),  # bucket 0
                logical("orders", "o2", {"tenant_id": "tenant_demo_034"}),  # bucket 0
            ]
        }
        plan = build_plan(old, new, rows)
        self.assertEqual(plan["moved_rows"], 0)
        self.assertEqual(plan["move_cells"], [])
        self.assertTrue(plan["safe_to_execute"])  # current rows are safe to execute
        self.assertEqual(plan["compatibility_certificate"]["status"], COUNTEREXAMPLE)
        self.assertFalse(plan["future_writes_allowed"])
        self.assertEqual(plan["future_write_permission"], PERMISSION_BLOCK)
        self.assertIn(
            "PLAN_FUTURE_WRITE_DRIFT", {item["code"] for item in plan["future_write_blockers"]}
        )
        orders = certificate_for({**plan["compatibility_certificate"], "_table": "orders"})
        self.assertGreaterEqual(len(orders["counterexamples"]), 1)
        witness = orders["counterexamples"][0]
        self.assertNotEqual(witness["old"]["target_shard"], witness["new"]["target_shard"])
        # The potential divergence is only concrete because a raw key was found.
        self.assertIsNotNone(witness["raw_key"])
        self.assertEqual(witness["old"]["bucket"], 2)
        self.assertEqual(witness["new"]["bucket"], 2)

    def test_real_witness_replays_through_both_routers(self):
        doc = self.orders_new(lambda orders: orders.update(bucket_map=["shard_a", "shard_a", "shard_c", "shard_b"]))
        old, new, cert = self.build(doc)
        orders = certificate_for({**cert, "_table": "orders"})
        self.assertEqual(orders["status"], COUNTEREXAMPLE)
        for witness in orders["counterexamples"]:
            old_obs = replay(old, "orders", witness["raw_key"])
            new_obs = replay(new, "orders", witness["raw_key"])
            self.assertEqual(old_obs["kind"], witness["old"]["kind"])
            self.assertEqual(new_obs["kind"], witness["new"]["kind"])
            self.assertEqual(old_obs["target_shard"], witness["old"]["target_shard"])
            self.assertEqual(new_obs["target_shard"], witness["new"]["target_shard"])
            self.assertNotEqual(old_obs["target_shard"], new_obs["target_shard"])
            self.assertEqual(
                witness["normalized_value"],
                Router(old).route("orders", witness["raw_key"]).normalized_value,
            )

    def test_bucket_count_change_compares_alignment_not_same_index(self):
        # 4 -> 8 buckets. Replicating each group (indices mod 4) is genuinely equivalent
        # even though bucket 2 has a different index range.
        doc = self.orders_new(
            lambda orders: orders.update(
                buckets=8,
                bucket_map=["shard_a", "shard_a", "shard_b", "shard_b", "shard_a", "shard_a", "shard_b", "shard_b"],
            )
        )
        _, _, cert = self.build(doc)
        orders = certificate_for({**cert, "_table": "orders"})
        self.assertEqual(orders["status"], PROVEN_EQUIVALENT)
        self.assertEqual(orders["proof"]["consumed"]["residues_scanned"], 8)
        self.assertEqual(orders["potential_divergences"]["count"], 0)

        # A bucket-count change that assigns an index range differently is caught with a witness.
        doc = self.orders_new(
            lambda orders: orders.update(
                buckets=8,
                bucket_map=["shard_a"] * 4 + ["shard_b"] * 4,
            )
        )
        _, _, cert = self.build(doc)
        orders = certificate_for({**cert, "_table": "orders"})
        self.assertEqual(orders["status"], COUNTEREXAMPLE)
        self.assertGreater(orders["potential_divergences"]["count"], 0)

    def test_divergent_residue_without_witness_is_unknown_not_drift(self):
        doc = self.orders_new(lambda orders: orders.update(bucket_map=["shard_a", "shard_a", "shard_c", "shard_b"]))
        _, _, cert = self.build(doc, witness_probe_budget=0)
        orders = certificate_for({**cert, "_table": "orders"})
        self.assertEqual(orders["status"], UNKNOWN)
        self.assertEqual(orders["permission"], PERMISSION_SHADOW)
        self.assertEqual(orders["counterexamples"], [])
        self.assertGreater(orders["potential_divergences"]["count"], 0)
        self.assertTrue(
            any("potential divergence" in reason for reason in orders["unknown_reasons"])
        )
        # An unproven certificate cannot authorize a write switch.
        self.assertFalse(cert["future_writes_allowed"])

    def test_hash_residue_budget_exhaustion_is_unknown(self):
        doc = self.orders_new(
            lambda orders: orders.update(buckets=5, bucket_map=["shard_a"] * 5)
        )
        _, _, cert = self.build(doc, hash_residue_budget=5)
        orders = certificate_for({**cert, "_table": "orders"})
        self.assertEqual(orders["status"], UNKNOWN)
        self.assertIn("budget exhausted", orders["unknown_reasons"][0])
        self.assertEqual(orders["proof"]["consumed"]["residues_scanned"], 0)
        self.assertEqual(orders["permission"], PERMISSION_SHADOW)

    def test_range_gap_and_default_change_finds_real_key(self):
        def events_doc(default_shard):
            doc = base_doc()
            events = table(doc, "events")
            events["default_shard"] = default_shard
            events["ranges"] = [
                {"lower": "2024-01-01", "upper": "2024-07-01", "shard": "shard_b"},
                {"lower": "2024-07-01", "upper": "2024-12-01", "shard": "shard_c"},
            ]
            return doc

        old = topology_from(events_doc("shard_a"))
        new = topology_from(events_doc("shard_b"))  # values outside all ranges now go elsewhere
        cert = build_compatibility_certificate(old, new)
        events = certificate_for({**cert, "_table": "events"})
        self.assertEqual(events["status"], COUNTEREXAMPLE)
        self.assertGreaterEqual(len(events["counterexamples"]), 1)
        # The below-first and above-last gaps are part of the proven key domain.
        self.assertEqual(events["potential_divergences"]["count"], 2)
        for witness in events["counterexamples"]:
            old_obs = replay(old, "events", witness["raw_key"])
            new_obs = replay(new, "events", witness["raw_key"])
            self.assertNotEqual(old_obs["target_shard"], new_obs["target_shard"])

    def test_list_unlisted_key_default_change_finds_real_key(self):
        doc = copy.deepcopy(base_doc())
        table(doc, "profiles")["default_shard"] = "shard_c"
        old = topology_from(base_doc())
        new = topology_from(doc)
        cert = build_compatibility_certificate(old, new)
        profiles = certificate_for({**cert, "_table": "profiles"})
        self.assertEqual(profiles["status"], COUNTEREXAMPLE)
        reasons = {witness["reason"] for witness in profiles["counterexamples"]}
        self.assertIn("unlisted_key_default_differs", reasons)
        witness = next(
            item for item in profiles["counterexamples"]
            if item["reason"] == "unlisted_key_default_differs"
        )
        self.assertNotIn(witness["normalized_value"], old.table("profiles").list_map)
        old_obs = replay(old, "profiles", witness["raw_key"])
        new_obs = replay(new, "profiles", witness["raw_key"])
        self.assertEqual((old_obs["target_shard"], new_obs["target_shard"]), ("shard_b", "shard_c"))

    def test_null_policy_change_finds_real_null_key(self):
        doc = copy.deepcopy(base_doc())
        orders = table(doc, "orders")
        orders["null_policy"] = "drop"
        old = topology_from(base_doc())
        new = topology_from(doc)
        cert = build_compatibility_certificate(old, new)
        record = certificate_for({**cert, "_table": "orders"})
        self.assertEqual(record["status"], COUNTEREXAMPLE)
        witness = next(item for item in record["counterexamples"] if item["reason"] == "null_policy")
        self.assertIsNone(witness["raw_key"])
        old_obs = replay(old, "orders", None)
        new_obs = replay(new, "orders", None)
        self.assertEqual(old_obs["kind"], "route")
        self.assertEqual(new_obs["kind"], "dropped")
        self.assertEqual(record["null_policy"]["status"], COUNTEREXAMPLE)

    def test_case_normalization_mismatch_is_unknown(self):
        doc = copy.deepcopy(base_doc())
        table(doc, "orders")["case_sensitive"] = False
        _, _, cert = self.build(doc)
        record = certificate_for({**cert, "_table": "orders"})
        self.assertEqual(record["status"], UNKNOWN)
        self.assertEqual(record["permission"], PERMISSION_SHADOW)
        self.assertTrue(
            any("normalization" in reason for reason in record["unknown_reasons"])
        )
        self.assertFalse(record["normalization_premise"]["normalization_equivalent"])
        self.assertEqual(cert["status"], UNKNOWN)
        self.assertFalse(cert["future_writes_allowed"])

    def test_key_field_change_is_unknown(self):
        doc = copy.deepcopy(base_doc())
        table(doc, "orders")["key"] = ["tenant_id", "region"]
        _, _, cert = self.build(doc)
        record = certificate_for({**cert, "_table": "orders"})
        self.assertEqual(record["status"], UNKNOWN)
        self.assertTrue(any("shard key changed" in reason for reason in record["unknown_reasons"]))

    def test_certificate_is_deterministic(self):
        doc = self.orders_new(lambda orders: orders.update(bucket_map=["shard_a", "shard_a", "shard_c", "shard_b"]))
        old, new = topology_from(base_doc()), topology_from(doc)
        first = build_compatibility_certificate(old, new)
        second = build_compatibility_certificate(old, new)
        self.assertEqual(first, second)

    def test_plan_and_report_keep_two_verdicts_separate(self):
        doc = self.orders_new(lambda orders: orders.update(bucket_map=["shard_a", "shard_a", "shard_c", "shard_b"]))
        old, new, _ = self.build(doc)
        plan = build_plan(
            old,
            new,
            {"orders": [logical("orders", "o1", {"tenant_id": "tenant_demo_006"})]},
        )
        self.assertTrue(plan["safe_to_execute"])
        self.assertFalse(plan["future_writes_allowed"])
        self.assertEqual(plan["summary"]["compatibility_status"], COUNTEREXAMPLE)

        from shardroute.report import build_report

        report = build_report(
            old=old,
            new=new,
            seed=1,
            logical_fingerprint="a" * 64,
            physical_fingerprint="b" * 64,
            audit={
                "status": "pass",
                "summary": {"total_findings": 0, "critical_or_high": 0},
                "tables": {},
                "findings": [],
            },
            plan=plan,
            queries=[],
        )
        self.assertTrue(report["summary"]["safe_to_execute"])
        self.assertFalse(report["summary"]["future_writes_allowed"])
        self.assertEqual(report["summary"]["compatibility_status"], COUNTEREXAMPLE)
        self.assertEqual(report["compatibility_certificate"]["certificate_id"], plan["compatibility_certificate"]["certificate_id"])

    def test_zero_witness_budget_never_fabricates_a_witness(self):
        # Range and list domains are finite, but a zero probe budget must still
        # report unknown rather than silently probing once. null_policy=error on
        # both sides isolates the unlisted-key class from the null domain.
        doc = copy.deepcopy(base_doc())
        profiles = table(doc, "profiles")
        profiles["default_shard"] = "shard_c"
        profiles["null_policy"] = "error"
        old_doc = base_doc()
        table(old_doc, "profiles")["null_policy"] = "error"
        old = topology_from(old_doc)
        new = topology_from(doc)
        cert = build_compatibility_certificate(old, new, witness_probe_budget=0)
        record = certificate_for({**cert, "_table": "profiles"})
        self.assertEqual(record["status"], UNKNOWN)
        self.assertEqual(record["counterexamples"], [])
        self.assertEqual(record["proof"]["consumed"]["witness_probes"], 0)
        self.assertGreater(record["potential_divergences"]["count"], 0)

    def test_key_field_change_with_null_policy_change_is_unknown_not_fake_witness(self):
        doc = copy.deepcopy(base_doc())
        orders = table(doc, "orders")
        orders["key"] = ["tenant_id", "region"]
        orders["null_policy"] = "error"
        orders["default_shard"] = None
        old = topology_from(base_doc())
        new = topology_from(doc)
        cert = build_compatibility_certificate(old, new)
        record = certificate_for({**cert, "_table": "orders"})
        self.assertEqual(record["status"], UNKNOWN)
        self.assertEqual(record["counterexamples"], [])
        self.assertTrue(
            any("cannot be routed through both" in reason for reason in record["unknown_reasons"])
        )

    def test_missing_table_is_unknown(self):
        doc = copy.deepcopy(base_doc())
        doc["tables"] = [item for item in doc["tables"] if item["table"] != "events"]
        old, new, cert = self.build(doc)
        record = certificate_for({**cert, "_table": "events"})
        self.assertEqual(record["status"], UNKNOWN)
        self.assertIn("table does not exist", record["unknown_reasons"][0])


if __name__ == "__main__":
    unittest.main()
