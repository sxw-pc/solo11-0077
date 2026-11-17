import tempfile
import unittest
from pathlib import Path

from shardroute.ledger import Ledger
from shardroute.report import write_reports

from support import write_test_run

class LedgerTests(unittest.TestCase):
    def test_record_query_and_idempotency(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            report = write_test_run(root)
            ledger = Ledger(root / "ledger.sqlite3")
            ledger.initialize()
            artifacts = write_reports(report, root)
            ledger.record_run(report, artifacts)
            ledger.record_run(report, artifacts)
            runs = ledger.list_runs()
            self.assertEqual(len(runs), 1)
            self.assertEqual(runs[0]["run_id"], report["run_id"])
            self.assertEqual(len(ledger.get_findings(report["run_id"])), 1)
            self.assertEqual(
                len(ledger.get_findings(report["run_id"], severity="critical")), 1
            )
            self.assertEqual(
                ledger.get_findings(report["run_id"], code="ORPHAN_FILE"), []
            )
            self.assertEqual(len(ledger.get_queries(report["run_id"])), 1)
            self.assertEqual(len(ledger.get_artifacts(report["run_id"])), 3)
            self.assertEqual(ledger.get_moves(report["run_id"]), [])
            self.assertEqual(ledger.get_run("run_missing"), None)
            run = ledger.get_run(report["run_id"])
            self.assertEqual(run["status"], "fail")
            self.assertIn("plan", run)
            ledger.close()
    def test_read_only_ledger(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            report = write_test_run(root)
            with Ledger(root / "ledger.sqlite3", readonly=True) as ledger:
                self.assertEqual(ledger.list_runs()[0]["run_id"], report["run_id"])
                with self.assertRaises(Exception):
                    ledger.record_run(report)
