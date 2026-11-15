import unittest

from shardroute.audit import run_audit
from shardroute.models import (
    ManifestEntry,
    ManifestRecord,
    PhysicalRow,
    ScanResult,
    ScannedFile,
)

from support import logical, mini_topology

def physical(row_id, shard, tenant, line):
    return PhysicalRow(
        "orders", shard, row_id, {"row_id": row_id, "tenant_id": tenant},
        f"orders/{shard}/part-000.csv", line,
    )

class AuditTests(unittest.TestCase):
    def setUp(self):
        self.topology = mini_topology()
        ids = ["ord_t_0001", "ord_t_0002", "ord_t_0003", "ord_t_0004"]
        tenants = [f"tenant_demo_00{index}" for index in range(1, 5)]
        logical_rows = [logical("orders", ids[i], {"tenant_id": tenants[i]}) for i in range(4)]
        self.scan = ScanResult(
            logical_rows={"orders": logical_rows},
            physical_rows={
                "orders": [
                    physical(ids[0], "shard_a", tenants[0], 2),
                    physical(ids[1], "shard_a", tenants[1], 3),
                    physical(ids[2], "shard_a", tenants[2], 4),
                    physical(ids[2], "shard_b", tenants[2], 2),
                ]
            },
            files=[
                ScannedFile("orders", "shard_a", "part-000.csv", "orders/shard_a/part-000.csv", "a" * 64, 3),
                ScannedFile("orders", "shard_b", "part-000.csv", "orders/shard_b/part-000.csv", "b" * 64, 1),
                ScannedFile("orders", "shard_a", "orphan.csv", "orders/shard_a/orphan.csv", "c" * 64, 0),
            ],
            manifests={
                ("orders", "shard_a"): ManifestRecord(
                    "orders", "shard_a", "orders/shard_a/_manifest.json", 3,
                    (ManifestEntry("part-000.csv", 3, "a" * 64),), "2026-01-02T00:00:00Z",
                ),
                ("orders", "shard_b"): ManifestRecord(
                    "orders", "shard_b", "orders/shard_b/_manifest.json", 5,
                    (ManifestEntry("part-000.csv", 1, "b" * 64),), "2026-01-02T00:00:00Z",
                ),
            },
            manifest_errors=[],
            physical_dirs=[("orders", "shard_a"), ("orders", "shard_b")],
            logical_fingerprint="0" * 64,
            physical_fingerprint="1" * 64,
        )
    def test_placement_and_manifest_classification(self):
        result = run_audit(self.topology, self.scan)
        by_code = result["summary"]["by_code"]
        self.assertEqual(by_code["MISPLACED_ROW"], 1)
        self.assertEqual(by_code["DUPLICATE_ROW"], 1)
        self.assertEqual(by_code["MISSING_ROW"], 1)
        self.assertEqual(by_code["ORPHAN_FILE"], 1)
        self.assertEqual(by_code["MANIFEST_ROW_COUNT"], 1)
        self.assertEqual(result["status"], "fail")
        self.assertEqual(result["summary"]["total_findings"], 5)
        self.assertEqual(result["tables"]["orders"]["logical_rows"], 4)
        self.assertEqual(result["tables"]["orders"]["physical_rows"], 4)
        self.assertEqual(
            [finding["severity"] for finding in result["findings"]][:3],
            ["critical", "critical", "critical"],
        )
    def test_finding_ids_and_detail_are_stable(self):
        first = run_audit(self.topology, self.scan)
        second = run_audit(self.topology, self.scan)
        self.assertEqual(
            [item["finding_id"] for item in first["findings"]],
            [item["finding_id"] for item in second["findings"]],
        )
        missing = next(item for item in first["findings"] if item["code"] == "MISSING_ROW")
        self.assertEqual(missing["row_id"], "ord_t_0004")
        duplicate = next(item for item in first["findings"] if item["code"] == "DUPLICATE_ROW")
        self.assertEqual(duplicate["detail"]["shards"], ["shard_a", "shard_b"])
