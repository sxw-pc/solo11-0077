"""Recoverable publication protocol: cut-point injection, recovery and verification."""

import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path

from shardroute.demo import run_demo, run_pipeline
from shardroute.errors import ShardRouteError
from shardroute.ledger import Ledger
from shardroute.publish import (
    FAULT_LEDGER_NOT_COMMITTED,
    FAULT_PARTIAL_REPORTS,
    FAULT_POINTER_NOT_SWITCHED,
    POINTER_NAME,
    list_published_runs,
    resolve_current,
    rollback_candidate,
    roll_forward_candidate,
)

FAULT_ENV = "SHARDROUTE_FAULT"


def mutate_one_shard(root: Path) -> None:
    """Change one physical shard file so a new run_id (snapshot B) is produced."""
    target = next((root / "shards").rglob("part-000.csv"))
    target.write_bytes(target.read_bytes() + b"ord_demo_9999,tenant_demo_001,1.00,web_demo,mutation\n")
    manifest = target.parent / "_manifest.json"
    doc = json.loads(manifest.read_text(encoding="utf-8"))
    doc["row_count"] += 1
    manifest.write_text(json.dumps(doc, sort_keys=True, indent=2) + "\n", encoding="utf-8")


class PublicationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name) / "ws"
        self.summary_a = run_demo(self.root)
        self.run_a = self.summary_a["run_id"]

    def tearDown(self):
        self.temporary.cleanup()

    def _current_view(self):
        """Return the same facts every read-only entry point must agree on."""
        with Ledger(self.root / "ledger.sqlite3", readonly=True) as ledger:
            current = resolve_current(self.root, ledger)
            run = ledger.get_run(current["run_id"])
            pointer = json.loads((self.root / "reports" / POINTER_NAME).read_text())
            disk_report = json.loads((self.root / "reports" / "report.json").read_text())
            return {
                "run_id": current["run_id"],
                "safe_to_execute": current["safe_to_execute"],
                "ledger_safe": run["summary"]["safe_to_execute"],
                "pointer_safe": pointer["safe_to_execute"],
                "disk_safe": disk_report["summary"]["safe_to_execute"],
                "disk_run_id": disk_report["run_id"],
                "pointer_run_id": pointer["run_id"],
            }

    def _attempt_b(self, fault: str):
        mutate_one_shard(self.root)
        os.environ[FAULT_ENV] = json.dumps({fault: 1})
        with self.assertRaises(ShardRouteError) as caught:
            run_pipeline(self.root)
        os.environ.pop(FAULT_ENV, None)
        return caught.exception.code

    def test_current_version_is_complete_and_self_consistent(self):
        view = self._current_view()
        self.assertEqual(view["run_id"], self.run_a)
        self.assertEqual(view["disk_run_id"], self.run_a)
        self.assertEqual(view["pointer_run_id"], self.run_a)
        self.assertFalse(view["safe_to_execute"])
        self.assertEqual(view["ledger_safe"], view["pointer_safe"])
        self.assertEqual(view["ledger_safe"], view["disk_safe"])
        with Ledger(self.root / "ledger.sqlite3", readonly=True) as ledger:
            published = list_published_runs(ledger)
        self.assertEqual(len(published), 1)
        self.assertTrue(published[0]["is_current"])

    def test_cut_point_1_partial_reports_keeps_a_and_recovers(self):
        code = self._attempt_b(FAULT_PARTIAL_REPORTS)
        self.assertEqual(code, "WORKSPACE_INVALID")
        # A is still the only current, complete version.
        view = self._current_view()
        self.assertEqual(view["run_id"], self.run_a)
        staging = list((self.root / "reports").glob(".staging-*"))
        self.assertEqual(len(staging), 1)
        # Restart: interrupted staging is quarantined, B rebuilt and published.
        summary = run_pipeline(self.root)
        run_b = summary["run_id"]
        self.assertNotEqual(run_b, self.run_a)
        view = self._current_view()
        self.assertEqual(view["run_id"], run_b)
        self.assertEqual(view["disk_run_id"], run_b)
        self.assertEqual(view["pointer_run_id"], run_b)
        self.assertEqual(view["ledger_safe"], view["disk_safe"])
        self.assertEqual([], list((self.root / "reports").glob(".staging-*")))
        quarantine = list((self.root / "reports" / "quarantine").iterdir())
        self.assertEqual(len(quarantine), 1)
        with Ledger(self.root / "ledger.sqlite3", readonly=True) as ledger:
            events = ledger.list_recovery_events()
        self.assertTrue(events)
        self.assertEqual(events[0]["decision"], "discard")
        self.assertEqual(events[0]["event_type"], FAULT_PARTIAL_REPORTS)
        # A's files and ledger evidence remain available.
        self.assertTrue((self.root / "reports" / "runs" / self.run_a / "report.json").is_file())
        with Ledger(self.root / "ledger.sqlite3", readonly=True) as ledger:
            self.assertIsNotNone(ledger.get_run(self.run_a))

    def test_cut_point_2_ledger_not_committed_never_publishes_b(self):
        self._attempt_b(FAULT_LEDGER_NOT_COMMITTED)
        # The complete B directory exists but has no ledger publication.
        run_dirs = sorted(path.name for path in (self.root / "reports" / "runs").iterdir())
        self.assertEqual(len(run_dirs), 2)
        run_b = next(name for name in run_dirs if name != self.run_a)
        with Ledger(self.root / "ledger.sqlite3", readonly=True) as ledger:
            self.assertIsNone(ledger.get_run(run_b))
            self.assertEqual(ledger.get_current_run_id(), self.run_a)
        view = self._current_view()
        self.assertEqual(view["run_id"], self.run_a)
        # Restart recovery: unverified B is quarantined and rebuilt fresh.
        summary = run_pipeline(self.root)
        self.assertEqual(summary["run_id"], run_b)
        view = self._current_view()
        self.assertEqual(view["run_id"], run_b)
        with Ledger(self.root / "ledger.sqlite3", readonly=True) as ledger:
            events = ledger.list_recovery_events(run_b)
        self.assertTrue(any(event["decision"] == "discard" for event in events))
        self.assertTrue(
            list((self.root / "reports" / "quarantine").glob(f"{run_b}-*"))
        )

    def test_cut_point_3_roll_forward_publishes_verified_b(self):
        self._attempt_b(FAULT_POINTER_NOT_SWITCHED)
        with Ledger(self.root / "ledger.sqlite3") as ledger:
            staged = ledger.list_publications("staged")
            self.assertEqual(len(staged), 1)
            run_b = staged[0]["run_id"]
            self.assertNotEqual(run_b, self.run_a)
            # Before recovery the current version is still A.
            self.assertEqual(ledger.get_current_run_id(), self.run_a)
            result = roll_forward_candidate(self.root, ledger, run_b)
        self.assertEqual(result["current_run_id"], run_b)
        view = self._current_view()
        self.assertEqual(view["run_id"], run_b)
        self.assertEqual(view["disk_run_id"], run_b)
        self.assertEqual(view["pointer_run_id"], run_b)
        self.assertEqual(view["ledger_safe"], view["pointer_safe"])
        self.assertEqual(view["ledger_safe"], view["disk_safe"])
        with Ledger(self.root / "ledger.sqlite3", readonly=True) as ledger:
            events = ledger.list_recovery_events(run_b)
        self.assertEqual(events[0]["decision"], "roll_forward")
        # Idempotent: a second recovery attempt is rejected, state unchanged.
        with Ledger(self.root / "ledger.sqlite3") as ledger:
            with self.assertRaises(ShardRouteError):
                roll_forward_candidate(self.root, ledger, run_b)

    def test_cut_point_3_rollback_keeps_a_and_quarantines_b(self):
        self._attempt_b(FAULT_POINTER_NOT_SWITCHED)
        with Ledger(self.root / "ledger.sqlite3") as ledger:
            run_b = ledger.list_publications("staged")[0]["run_id"]
            result = rollback_candidate(self.root, ledger, run_b)
        self.assertEqual(result["current_run_id"], self.run_a)
        view = self._current_view()
        self.assertEqual(view["run_id"], self.run_a)
        self.assertEqual(view["disk_run_id"], self.run_a)
        # B is removed from the version store and quarantined; ledger rows retained.
        self.assertFalse((self.root / "reports" / "runs" / run_b).exists())
        self.assertTrue(
            list((self.root / "reports" / "quarantine").glob(f"{run_b}-*"))
        )
        with Ledger(self.root / "ledger.sqlite3", readonly=True) as ledger:
            events = ledger.list_recovery_events(run_b)
            self.assertIsNotNone(ledger.get_run(run_b))
        self.assertEqual(events[0]["decision"], "rollback")

    def test_cut_point_3_auto_recovery_on_restart_rolls_forward(self):
        self._attempt_b(FAULT_POINTER_NOT_SWITCHED)
        with Ledger(self.root / "ledger.sqlite3", readonly=True) as ledger:
            run_b = ledger.list_publications("staged")[0]["run_id"]
        # A plain restart recovers the staged candidate automatically.
        summary = run_pipeline(self.root)
        self.assertEqual(summary["run_id"], run_b)
        view = self._current_view()
        self.assertEqual(view["run_id"], run_b)
        with Ledger(self.root / "ledger.sqlite3", readonly=True) as ledger:
            events = ledger.list_recovery_events(run_b)
            self.assertEqual(ledger.get_current_run_id(), run_b)
        self.assertTrue(any(event["decision"] == "roll_forward" for event in events))
        # Re-running the same snapshot again must not create a second event/version.
        run_pipeline(self.root)
        with Ledger(self.root / "ledger.sqlite3", readonly=True) as ledger:
            self.assertEqual(len(ledger.list_publications()), 2)
            roll_forwards = [
                event for event in ledger.list_recovery_events(run_b)
                if event["decision"] == "roll_forward"
            ]
        self.assertEqual(len(roll_forwards), 1)

    def test_same_snapshot_rerun_never_mixes_versions(self):
        run_pipeline(self.root)
        run_pipeline(self.root)
        view = self._current_view()
        self.assertEqual(view["run_id"], self.run_a)
        # Every fingerprint and the safe flag come from the same run.
        with Ledger(self.root / "ledger.sqlite3", readonly=True) as ledger:
            run = ledger.get_run(self.run_a)
            current = resolve_current(self.root, ledger)
        self.assertEqual(
            run["logical_fingerprint"], current["fingerprints"]["logical"]
        )
        self.assertEqual(
            run["summary"]["safe_to_execute"], current["safe_to_execute"]
        )
        # Only one version directory exists; no stray staging or quarantine data.
        self.assertEqual(
            [p.name for p in (self.root / "reports" / "runs").iterdir()], [self.run_a]
        )
        self.assertFalse((self.root / "reports" / "quarantine").exists())

    def test_tampered_current_artifact_fails_closed(self):
        report_path = self.root / "reports" / "runs" / self.run_a / "report.json"
        report_path.write_bytes(report_path.read_bytes() + b" ")
        with Ledger(self.root / "ledger.sqlite3", readonly=True) as ledger:
            with self.assertRaises(ShardRouteError):
                resolve_current(self.root, ledger)

    def test_legacy_flat_layout_is_adopted_after_reverification(self):
        # Build a second workspace in the pre-protocol shape: flat reports
        # plus ledger rows, but no version directory or pointer.
        legacy = Path(self.temporary.name) / "legacy"
        run_demo(legacy)
        run_id = json.loads((legacy / "reports" / "report.json").read_text())["run_id"]
        with Ledger(legacy / "ledger.sqlite3") as ledger:
            ledger.delete_publication(run_id)
        shutil.rmtree(legacy / "reports" / "runs")
        (legacy / "reports" / "current.json").unlink()
        self.assertFalse((legacy / "reports" / "runs").exists())

        summary = run_pipeline(legacy)
        self.assertEqual(summary["run_id"], run_id)
        with Ledger(legacy / "ledger.sqlite3", readonly=True) as ledger:
            current = resolve_current(legacy, ledger)
            events = ledger.list_recovery_events(run_id)
        self.assertEqual(current["run_id"], run_id)
        self.assertEqual(events[0]["event_type"], "legacy_adoption")
        self.assertTrue((legacy / "reports" / "runs" / run_id / "manifest.json").is_file())


if __name__ == "__main__":
    unittest.main()
