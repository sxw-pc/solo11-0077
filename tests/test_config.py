import tempfile
import unittest
from pathlib import Path

from shardroute.config import load_topology, validation_report
from shardroute.config_validation import validate_document

from support import FIXTURES, base_doc

class ConfigTests(unittest.TestCase):
    def codes(self, doc):
        _, issues, _ = validate_document(doc)
        return {issue.code for issue in issues}
    def table(self, doc, name):
        return next(item for item in doc["tables"] if item["table"] == name)
    def test_fixture_is_valid_and_fingerprint_is_order_independent(self):
        doc = base_doc()
        topology, issues, fp = validate_document(doc)
        self.assertFalse(issues)
        self.assertEqual(topology.fingerprint, fp)
        reversed_doc = dict(reversed(list(doc.items())))
        self.assertEqual(validate_document(reversed_doc)[2], fp)
        loaded = load_topology(FIXTURES / "config" / "mini_topology.json")
        self.assertEqual(loaded.fingerprint, fp)
    def test_safe_table_names_and_duplicate_shards(self):
        doc = base_doc()
        doc["tables"][0]["table"] = "../orders"
        self.assertIn("CFG_TABLE_NAME", self.codes(doc))
        doc = base_doc()
        doc["shards"].append("shard_a")
        self.assertIn("CFG_SHARD_DUPLICATE", self.codes(doc))
    def test_unknown_router_key_shard_and_null_policy(self):
        doc = base_doc()
        self.table(doc, "orders")["router"] = "magic"
        self.assertIn("CFG_ROUTER_UNKNOWN", self.codes(doc))
        doc = base_doc()
        del self.table(doc, "orders")["key"]
        self.assertIn("CFG_KEY_MISSING", self.codes(doc))
        doc = base_doc()
        self.table(doc, "profiles")["list_map"]["region_alpha"] = "shard_missing"
        self.assertIn("CFG_SHARD_UNKNOWN", self.codes(doc))
        doc = base_doc()
        self.table(doc, "orders")["null_policy"] = "guess"
        self.assertIn("CFG_NULL_POLICY", self.codes(doc))
        doc = base_doc()
        self.table(doc, "orders")["router"] = []
        self.assertIn("CFG_ROUTER_UNKNOWN", self.codes(doc))
        doc = base_doc()
        self.table(doc, "orders")["null_policy"] = []
        self.assertIn("CFG_NULL_POLICY", self.codes(doc))
    def test_bucket_coverage(self):
        doc = base_doc()
        self.table(doc, "orders")["bucket_map"] = ["shard_a", "shard_b"]
        self.assertIn("CFG_BUCKET_RANGE", self.codes(doc))
        doc = base_doc()
        self.table(doc, "orders")["buckets"] = 0
        self.assertIn("CFG_BUCKET_RANGE", self.codes(doc))
        doc = base_doc()
        self.table(doc, "orders")["bucket_map"] = {
            "0": "shard_a", "1": "shard_a", "2": "shard_b"
        }
        self.assertIn("CFG_BUCKET_RANGE", self.codes(doc))
    def test_range_gap_overlap_and_open_end(self):
        doc = base_doc()
        self.table(doc, "events")["ranges"][1]["lower"] = "2024-07-02"
        self.assertIn("CFG_RANGE_GAP", self.codes(doc))
        doc = base_doc()
        self.table(doc, "events")["ranges"][1]["lower"] = "2024-06-30"
        self.assertIn("CFG_RANGE_OVERLAP", self.codes(doc))
        doc = base_doc()
        self.table(doc, "events")["ranges"][0]["upper"] = "2024-07-02"
        self.assertIn("CFG_RANGE_OVERLAP", self.codes(doc))
    def test_list_duplicate_default_and_key_count(self):
        doc = base_doc()
        profiles = self.table(doc, "profiles")
        profiles["case_sensitive"] = False
        profiles["list_map"]["REGION_ALPHA"] = "shard_b"
        self.assertIn("CFG_LIST_DUPLICATE", self.codes(doc))
        doc = base_doc()
        profiles = self.table(doc, "profiles")
        profiles["default_shard"] = None
        self.assertIn("CFG_DEFAULT_MISSING", self.codes(doc))
        doc = base_doc()
        self.table(doc, "events")["key"] = ["event_day", "actor_id"]
        self.assertIn("CFG_KEY_MISSING", self.codes(doc))
    def test_validation_report_and_bad_json(self):
        report = validation_report(FIXTURES / "config" / "mini_topology.json")
        self.assertTrue(report["valid"])
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "bad.json"
            path.write_text('{"version": 1, "version": 2}', encoding="utf-8")
            broken = validation_report(path)
            self.assertFalse(broken["valid"])
            self.assertEqual(broken["issues"][0]["code"], "CFG_SCHEMA")

class AmountContractConfigTests(unittest.TestCase):
    def codes(self, doc):
        _, issues, _ = validate_document(doc)
        return {issue.code for issue in issues}
    def table(self, doc, name):
        return next(item for item in doc["tables"] if item["table"] == name)
    def test_valid_contracts_are_loaded(self):
        doc = base_doc()
        self.table(doc, "orders")["amount_contracts"] = [
            {"contract": "row_half_even"},
            {"contract": "dual_margin", "repair_budget": 250,
             "amount_field": "amount", "tenant_field": "tenant_id", "channel_field": "channel"},
        ]
        topology, issues, _ = validate_document(doc)
        self.assertFalse(issues)
        names = [c.contract for c in topology.table("orders").amount_contracts]
        self.assertEqual(names, ["row_half_even", "dual_margin"])
        self.assertEqual(topology.table("orders").amount_contracts[1].repair_budget, 250)
        self.assertIn("amount_contracts", topology.table("orders").as_dict())
    def test_unknown_contract_duplicate_and_bad_budget(self):
        doc = base_doc()
        self.table(doc, "orders")["amount_contracts"] = [{"contract": "magic"}]
        self.assertIn("CFG_AMOUNT_CONTRACT", self.codes(doc))
        doc = base_doc()
        self.table(doc, "orders")["amount_contracts"] = [
            {"contract": "dual_margin"}, {"contract": "dual_margin"}
        ]
        self.assertIn("CFG_AMOUNT_CONTRACT", self.codes(doc))
        doc = base_doc()
        self.table(doc, "orders")["amount_contracts"] = [
            {"contract": "dual_margin", "repair_budget": 0}
        ]
        self.assertIn("CFG_AMOUNT_CONTRACT", self.codes(doc))
        doc = base_doc()
        self.table(doc, "orders")["amount_contracts"] = []
        self.assertIn("CFG_AMOUNT_CONTRACT", self.codes(doc))
        doc = base_doc()
        self.table(doc, "orders")["amount_contracts"] = [
            {"contract": "dual_margin", "tenant_field": "  "}
        ]
        self.assertIn("CFG_AMOUNT_CONTRACT", self.codes(doc))
