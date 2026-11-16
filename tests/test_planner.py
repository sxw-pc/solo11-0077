import copy
import unittest

from shardroute.planner import build_plan

from support import base_doc, logical, topology_from

def table(doc, name):
    return next(item for item in doc["tables"] if item["table"] == name)

class PlannerTests(unittest.TestCase):
    def setUp(self):
        old_doc = base_doc()
        new_doc = copy.deepcopy(old_doc)
        table(new_doc, "orders")["buckets"] = 8
        table(new_doc, "orders")["bucket_map"] = [
            "shard_a", "shard_a", "shard_a", "shard_a",
            "shard_b", "shard_b", "shard_b", "shard_b",
        ]
        table(new_doc, "events")["ranges"] = [
            {"lower": "2024-01-01", "upper": "2024-08-01", "shard": "shard_b"},
            {"lower": "2024-08-01", "upper": None, "shard": "shard_c"},
        ]
        table(new_doc, "profiles")["list_map"] = {
            "region_alpha": "shard_c", "region_beta": "shard_a"
        }
        self.old, self.new = topology_from(old_doc), topology_from(new_doc)
        self.rows = {
            "orders": [
                logical("orders", f"o{index}", {"tenant_id": f"tenant_demo_00{index}"})
                for index in range(1, 5)
            ],
            "events": [
                logical("events", "e1", {"event_day": "2024-06-30"}),
                logical("events", "e2", {"event_day": "2024-09-01"}),
            ],
            "profiles": [
                logical("profiles", "p1", {"region": "region_alpha"}),
                logical("profiles", "p2", {"region": "region_omega"}),
            ],
        }
    def test_move_matrix_batches_and_rollback(self):
        plan = build_plan(self.old, self.new, self.rows)
        self.assertGreater(plan["moved_rows"], 0)
        self.assertEqual(plan["total_rows"], 8)
        self.assertEqual(
            plan["moved_rows"] + plan["unchanged_rows"], plan["total_rows"]
        )
        self.assertEqual(
            plan["move_cells"], sorted(
                plan["move_cells"], key=lambda cell: (cell["table"], cell["from_shard"], cell["to_shard"])
            )
        )
        self.assertTrue(all(cell["row_count"] > 0 for cell in plan["move_cells"]))
        self.assertTrue(plan["safe_to_execute"])
        self.assertEqual(len(plan["rollback_checklist"]), 7)
        self.assertEqual(plan["summary"]["batch_count"], len(plan["batches"]))
        self.assertTrue(all(item["row_count"] <= 80 for item in plan["batches"]))
        self.assertEqual([item["batch_id"] for item in plan["batches"]][0], "batch-001")
    def test_audit_findings_block_plan(self):
        audit = {
            "findings": [
                {"severity": "critical", "table": "orders", "code": "MISSING_ROW"},
                {"severity": "high", "table": "orders", "code": "MISSING_ROW"},
                {"severity": "medium", "table": "orders", "code": "ORPHAN_FILE"},
            ]
        }
        plan = build_plan(self.old, self.new, self.rows, audit)
        self.assertFalse(plan["safe_to_execute"])
        self.assertEqual(plan["blockers"][0]["code"], "PLAN_AUDIT_MISSING_ROW")
        self.assertEqual(plan["blockers"][0]["count"], 2)
    def test_missing_target_table_is_blocking(self):
        new_doc = copy.deepcopy(base_doc())
        new_doc["tables"] = [item for item in new_doc["tables"] if item["table"] != "events"]
        plan = build_plan(self.old, topology_from(new_doc), self.rows)
        self.assertIn("PLAN_TABLE_MISSING", {item["code"] for item in plan["blockers"]})
        self.assertFalse(plan["safe_to_execute"])
