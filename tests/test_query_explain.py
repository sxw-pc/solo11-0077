import unittest

from shardroute.errors import QueryError
from shardroute.query_explain import explain_query

from support import mini_topology

class QueryExplainTests(unittest.TestCase):
    def setUp(self):
        self.topology = mini_topology()
    def explain(self, table, *filters):
        return explain_query(self.topology, {"table": table, "filters": list(filters)})
    def test_hash_equality_prunes_and_absent_key_scans_all(self):
        result = self.explain("orders", {"field": "tenant_id", "op": "eq", "value": "tenant_demo_001"})
        self.assertEqual(result.decision, "pruned")
        self.assertEqual(result.targeted_shards, ["shard_a"])
        full = self.explain("orders", {"field": "amount", "op": "eq", "value": "10.00"})
        self.assertEqual(full.decision, "full_scan")
        self.assertEqual(full.targeted_shards, ["shard_a", "shard_b"])
        unsupported = self.explain("orders", {"field": "tenant_id", "op": "ne", "value": "x"})
        self.assertEqual(unsupported.decision, "not_prunable")
        self.assertEqual(unsupported.targeted_shards, ["shard_a", "shard_b"])
    def test_range_interval_and_equality(self):
        result = self.explain(
            "events",
            {"field": "event_day", "op": "gte", "value": "2024-06-01"},
            {"field": "event_day", "op": "lt", "value": "2024-06-30"},
        )
        self.assertEqual(result.decision, "pruned")
        self.assertEqual(result.targeted_shards, ["shard_b"])
        equality = self.explain("events", {"field": "event_day", "op": "eq", "value": "2024-07-01"})
        self.assertEqual(equality.targeted_shards, ["shard_c"])
        full = self.explain("events", {"field": "actor_id", "op": "eq", "value": "actor_demo_001"})
        self.assertEqual(full.decision, "full_scan")
        self.assertIn("interval", self.explain(
            "events", {"field": "event_day", "op": "gte", "value": "2024-01-01"}
        ).trace)
    def test_list_membership_and_default(self):
        one = self.explain("profiles", {"field": "region", "op": "in", "value": ["region_alpha"]})
        self.assertEqual(one.targeted_shards, ["shard_a"])
        fallback = self.explain("profiles", {"field": "region", "op": "eq", "value": "region_omega"})
        self.assertEqual(fallback.targeted_shards, ["shard_b"])
        both = self.explain("profiles", {"field": "region", "op": "in", "value": ["region_alpha", "region_beta"]})
        self.assertEqual(both.decision, "pruned")
        self.assertEqual(both.targeted_shards, ["shard_a", "shard_c"])
        unsupported = self.explain("profiles", {"field": "region", "op": "ne", "value": "region_alpha"})
        self.assertEqual(unsupported.decision, "not_prunable")
    def test_invalid_queries_are_rejected(self):
        with self.assertRaises(QueryError):
            explain_query(self.topology, {"table": "unknown", "filters": [{"field": "x", "op": "eq"}]})
        with self.assertRaises(QueryError):
            self.explain("orders", {"field": "tenant_id", "op": "between", "value": "x"})
        with self.assertRaises(QueryError):
            self.explain("orders", {"field": "tenant_id", "op": "in", "value": "not-a-list"})
