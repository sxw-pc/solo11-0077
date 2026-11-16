import json
import tempfile
import unittest
from pathlib import Path

from shardroute.report import build_report, render_markdown, write_reports

from support import mini_topology, test_report

class ReportTests(unittest.TestCase):
    def test_write_all_formats_and_hash_artifacts(self):
        report = test_report()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            artifacts = write_reports(report, root)
            self.assertEqual(
                [item["path"] for item in artifacts],
                ["reports/report.html", "reports/report.json", "reports/report.md"],
            )
            self.assertTrue((root / "reports" / "report.json").is_file())
            self.assertTrue((root / "reports" / "report.md").is_file())
            self.assertTrue((root / "reports" / "report.html").is_file())
            for item in artifacts:
                data = (root / item["path"]).read_bytes()
                self.assertEqual(len(data), item["bytes"])
            loaded = json.loads((root / "reports" / "report.json").read_text(encoding="utf-8"))
            self.assertEqual(loaded["run_id"], report["run_id"])
            html = (root / "reports" / "report.html").read_text(encoding="utf-8")
            self.assertTrue(html.startswith("<!doctype html>"))
            self.assertIn("MISPLACED_ROW", html)
            self.assertNotIn("<script", html.lower())
            self.assertNotIn("http://", html)
            self.assertNotIn("https://", html)
            markdown = (root / "reports" / "report.md").read_text(encoding="utf-8")
            self.assertIn("## Findings", markdown)
            self.assertIn("## Rollback checklist", markdown)
    def test_reports_are_byte_stable(self):
        report = test_report()
        with tempfile.TemporaryDirectory() as left, tempfile.TemporaryDirectory() as right:
            first = write_reports(report, Path(left))
            second = write_reports(report, Path(right))
            self.assertEqual(first, second)
            for item in first:
                self.assertEqual(
                    (Path(left) / item["path"]).read_bytes(),
                    (Path(right) / item["path"]).read_bytes(),
                )
        self.assertEqual(render_markdown(report), render_markdown(report))
    def test_run_id_changes_with_fingerprints(self):
        report = test_report()
        topology = mini_topology()
        other = build_report(
            old=topology,
            new=topology,
            seed=7,
            logical_fingerprint="a" * 64,
            physical_fingerprint="c" * 64,
            audit=report["audit"],
            plan=report["plan"],
            queries=[],
        )
        self.assertEqual(report["status"], "fail")
        self.assertNotEqual(report["run_id"], other["run_id"])
