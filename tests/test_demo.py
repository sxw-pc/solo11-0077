import contextlib
import hashlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from shardroute.cli import main
from shardroute.demo import prepare_workspace, run_demo
from shardroute.errors import ShardRouteError

def tree_hashes(root):
    return {
        path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(root.rglob("*"))
        if path.is_file() and path.name != "ledger.sqlite3"
    }

class DemoTests(unittest.TestCase):
    def test_demo_builds_expected_audit_and_is_reproducible(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "demo"
            summary = run_demo(root)
            self.assertEqual(summary["logical_rows"], 400)
            self.assertEqual(summary["query_count"], 3)
            self.assertEqual(summary["pruned_queries"], 3)
            self.assertEqual(summary["findings"], 8)
            self.assertFalse(summary["safe_to_execute"])
            report = json.loads((root / "reports" / "report.json").read_text(encoding="utf-8"))
            codes = report["audit"]["summary"]["by_code"]
            self.assertEqual(codes["DUPLICATE_ROW"], 2)
            self.assertEqual(codes["MISSING_ROW"], 1)
            self.assertEqual(codes["MISPLACED_ROW"], 3)
            self.assertEqual(codes["ORPHAN_FILE"], 1)
            self.assertNotIn(str(root), json.dumps(report))
            first = tree_hashes(root)
            second_summary = run_demo(root)
            second = tree_hashes(root)
            self.assertEqual(first, second)
            self.assertEqual(summary["run_id"], second_summary["run_id"])
            validate_code = main([
                "validate", "--config", str(root / "config" / "topology_old.json")
            ])
            self.assertEqual(validate_code, 0)
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(main(["explain", "--workspace", str(root)]), 0)
    def test_workspace_overwrite_guard(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "unsafe"
            root.mkdir()
            (root / "keep.txt").write_text("do not delete", encoding="utf-8")
            with self.assertRaises(ShardRouteError) as caught:
                prepare_workspace(root)
            self.assertEqual(caught.exception.code, "WORKSPACE_INVALID")
            self.assertTrue((root / "keep.txt").is_file())
