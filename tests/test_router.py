import hashlib
import unittest

from shardroute.errors import RoutingError
from shardroute.router import Router

from support import base_doc, mini_topology, topology_from

class RouterTests(unittest.TestCase):
    def setUp(self):
        self.topology = mini_topology()
        self.router = Router(self.topology)
    def test_hash_bucket_and_trace(self):
        trace = self.router.route("orders", "  tenant_demo_001  ")
        expected = int.from_bytes(
            hashlib.sha256(b"tenant_demo_001").digest()[:8], "big"
        ) % 4
        self.assertEqual(trace.bucket, expected)
        self.assertEqual(trace.normalized_value, "tenant_demo_001")
        self.assertEqual(trace.target_shard, "shard_a")
        self.assertEqual(trace.rule, "hash_bucket")
        self.assertTrue(trace.digest_prefix)
    def test_range_boundaries_are_left_closed(self):
        self.assertEqual(self.router.route("events", "2024-01-01").target_shard, "shard_b")
        self.assertEqual(self.router.route("events", "2024-06-30").target_shard, "shard_b")
        self.assertEqual(self.router.route("events", "2024-07-01").target_shard, "shard_c")
        trace = self.router.route("events", "2024-07-01")
        self.assertEqual(trace.rule, "range[1]")
        self.assertTrue(any(">= 2024-07-01" in line for line in trace.comparisons))
    def test_list_exact_default_and_case_insensitive(self):
        self.assertEqual(self.router.route("profiles", "region_alpha").rule, "list_exact")
        fallback = self.router.route("profiles", "region_omega")
        self.assertEqual(fallback.target_shard, "shard_b")
        self.assertEqual(fallback.rule, "list_default")
        doc = base_doc()
        profiles = next(item for item in doc["tables"] if item["table"] == "profiles")
        profiles["case_sensitive"] = False
        router = Router(topology_from(doc))
        self.assertEqual(router.route("profiles", "REGION_ALPHA").target_shard, "shard_a")
    def test_null_policies(self):
        route_default = self.router.route("orders", None)
        self.assertEqual(route_default.target_shard, "shard_a")
        self.assertEqual(route_default.rule, "null_route_default")
        with self.assertRaises(RoutingError) as caught:
            self.router.route("events", None)
        self.assertEqual(caught.exception.code, "ROUTE_NULL_KEY")
        doc = base_doc()
        orders = next(item for item in doc["tables"] if item["table"] == "orders")
        orders["null_policy"] = "drop"
        dropped = Router(topology_from(doc)).route("orders", None)
        self.assertTrue(dropped.dropped)
        self.assertIsNone(dropped.target_shard)
    def test_missing_component_and_composite_key(self):
        with self.assertRaises(RoutingError) as caught:
            self.router.route_values("orders", {"row_id": "x"})
        self.assertEqual(caught.exception.code, "ROUTE_KEY_MISSING")
        doc = base_doc()
        orders = next(item for item in doc["tables"] if item["table"] == "orders")
        orders["key"] = ["tenant_id", "region"]
        trace = Router(topology_from(doc)).route("orders", ["tenant_demo_001", "region_alpha"])
        self.assertIn("\x1f", trace.normalized_value)
        self.assertEqual(trace.key_fields, ("tenant_id", "region"))
