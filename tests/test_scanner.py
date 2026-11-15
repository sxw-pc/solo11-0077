import shutil
import tempfile
import unittest
from pathlib import Path

from shardroute.errors import ScanError
from shardroute.scanner import load_logical, scan_workspace

from support import FIXTURES

class ScannerTests(unittest.TestCase):
    def test_scan_fixture_and_stable_fingerprints(self):
        first = scan_workspace(FIXTURES / "logical", FIXTURES / "shards", ["orders"])
        second = scan_workspace(FIXTURES / "logical", FIXTURES / "shards", ["orders"])
        self.assertEqual(len(first.logical_rows["orders"]), 4)
        self.assertEqual(len(first.physical_rows["orders"]), 4)
        self.assertEqual(len(first.files), 2)
        self.assertEqual(len(first.manifests), 2)
        self.assertEqual(first.logical_fingerprint, second.logical_fingerprint)
        self.assertEqual(first.physical_fingerprint, second.physical_fingerprint)
        self.assertEqual(first.manifests[("orders", "shard_a")].row_count, 2)
    def test_orphan_and_parse_errors(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            shutil.copytree(FIXTURES / "logical", root / "logical")
            shutil.copytree(FIXTURES / "shards", root / "shards")
            orphan = root / "shards" / "orders" / "shard_a" / "orphan.csv"
            orphan.write_text("row_id,tenant_id,amount\n", encoding="utf-8")
            scan = scan_workspace(root / "logical", root / "shards", ["orders"])
            self.assertEqual(len(scan.files), 3)
            manifest = root / "shards" / "orders" / "shard_b" / "_manifest.json"
            manifest.write_text("{", encoding="utf-8")
            broken = scan_workspace(root / "logical", root / "shards", ["orders"])
            self.assertEqual(len(broken.manifest_errors), 1)
            self.assertEqual(broken.manifest_errors[0]["table"], "orders")
            self.assertNotEqual(scan.physical_fingerprint, broken.physical_fingerprint)
    def test_jsonl_snapshot_and_shard(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "logical").mkdir()
            shard = root / "shards" / "orders" / "shard_a"
            shard.mkdir(parents=True)
            (root / "logical" / "orders.jsonl").write_text(
                '{"row_id":"r1","tenant_id":"tenant_demo_001"}\n\n', encoding="utf-8"
            )
            (shard / "part.jsonl").write_text(
                '{"row_id":"r1","tenant_id":"tenant_demo_001"}\n'
                '{"row_id":"r2","tenant_id":null}\n',
                encoding="utf-8",
            )
            scan = scan_workspace(root / "logical", root / "shards", ["orders"])
            self.assertEqual(len(scan.logical_rows["orders"]), 1)
            self.assertEqual(len(scan.physical_rows["orders"]), 2)
            self.assertIsNone(scan.physical_rows["orders"][1].values["tenant_id"])
    def test_bad_logical_csv_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "bad.csv").write_text("tenant_id,amount\nx,1\n", encoding="utf-8")
            with self.assertRaises(ScanError) as caught:
                load_logical(root, ["bad"])
            self.assertEqual(caught.exception.code, "SCAN_CSV")
            (root / "dupes.csv").write_text(
                "row_id,tenant_id,amount\nr1,x,1\nr1,y,2\n", encoding="utf-8"
            )
            with self.assertRaises(ScanError) as caught:
                load_logical(root, ["dupes"])
            self.assertEqual(caught.exception.code, "SCAN_DUPLICATE_ID")
