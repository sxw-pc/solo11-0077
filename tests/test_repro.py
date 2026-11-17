import contextlib
import hashlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from shardroute.audit import run_audit
from shardroute.cli import main
from shardroute.config import load_topology
from shardroute.errors import ShardRouteError
from shardroute.fixturegen import (
    REPRO_DUP_ROW,
    REPRO_HASH_FILE,
    REPRO_HASH_SHARD,
    REPRO_LONG_ROW,
    REPRO_SHORT_ROW,
    build_repro_fixtures,
)
from shardroute.repro import ReproContract, build_minimal_repro
from shardroute.scanner import scan_workspace, snapshot_tree

class ReproTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.tmp.cleanup)
        cls.root = Path(cls.tmp.name)
        cls.src = cls.root / "src"
        cls.fixture = build_repro_fixtures(cls.src)
        cls.topology = load_topology(cls.src / "config" / "topology_old.json")
        cls.baseline = cls.audit(cls.src)

    @classmethod
    def audit(cls, workspace):
        scan = scan_workspace(
            workspace / "logical", workspace / "shards", cls.topology.table_names()
        )
        return run_audit(cls.topology, scan)

    def out_dir(self, name):
        return self.root / name

    def test_fixture_baseline_findings(self):
        by_code = self.baseline["summary"]["by_code"]
        self.assertEqual(by_code.get("MISPLACED_ROW"), 2)
        self.assertEqual(by_code.get("DUPLICATE_ROW"), 1)
        self.assertEqual(by_code.get("MANIFEST_HASH"), 1)
        self.assertEqual(self.baseline["summary"]["critical_or_high"], 4)
        misplaced = {
            finding["row_id"]
            for finding in self.baseline["findings"]
            if finding["code"] == "MISPLACED_ROW"
        }
        self.assertEqual(misplaced, {REPRO_LONG_ROW, REPRO_SHORT_ROW})
        # The synthetic workspace is deterministic.
        again = self.root / "src-again"
        second = build_repro_fixtures(again)
        self.assertEqual(self.fixture["fixture_fingerprint"], second["fixture_fingerprint"])

    def test_exact_row_preserves_long_row_and_routing_evidence(self):
        out = self.out_dir("pkg-exact")
        result = build_minimal_repro(
            self.src,
            out,
            ReproContract("exact_row", "orders", "MISPLACED_ROW", REPRO_LONG_ROW),
            budget=300,
        )
        verification = result["verification"]
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["preserved"]["claim"], "original_record")
        self.assertEqual(result["preserved"]["row_id"], REPRO_LONG_ROW)
        # The target defect genuinely re-fires in the package, with the
        # original routing evidence intact.
        package_audit = self.audit(out)
        targets = [
            finding
            for finding in package_audit["findings"]
            if finding["code"] == "MISPLACED_ROW"
        ]
        self.assertEqual(len(targets), 1)
        original = result["target_finding"]
        self.assertEqual(targets[0]["row_id"], REPRO_LONG_ROW)
        self.assertEqual(
            targets[0]["detail"]["expected_shard"], original["detail"]["expected_shard"]
        )
        self.assertEqual(
            targets[0]["detail"]["actual_shard"], original["detail"]["actual_shard"]
        )
        # Reduction manufactured no fake high-severity findings.
        self.assertEqual(package_audit["summary"]["critical_or_high"], 1)
        self.assertEqual(verification["new_high_findings"], [])
        self.assertEqual(len(verification["remaining_high_findings"]), 1)
        self.assertIn("MISPLACED_ROW", verification["remaining_high_findings"][0])
        # Unrelated tables, files and rows are gone.
        self.assertEqual(result["package"]["logical_rows"], {"events": 0, "orders": 1})
        self.assertEqual(result["package"]["physical_rows"], 1)
        self.assertFalse((out / "shards" / "events").exists())
        # In-budget run carries honest 1-minimal evidence.
        self.assertEqual(verification["minimality"], "1-minimal")
        evidence = verification["minimality_evidence"]
        self.assertTrue(evidence)
        self.assertTrue(all(entry["removal"] == "rejected" for entry in evidence))
        units = {entry["unit"] for entry in evidence}
        self.assertIn(f"row:orders/{REPRO_LONG_ROW}", units)
        # The package manifest agrees with the file on disk.
        manifests = list((out / "shards").rglob("_manifest.json"))
        self.assertEqual(len(manifests), 1)
        manifest = json.loads(manifests[0].read_text(encoding="utf-8"))
        data_file = manifests[0].parent / manifest["files"][0]["name"]
        self.assertEqual(
            manifest["files"][0]["sha256"],
            hashlib.sha256(data_file.read_bytes()).hexdigest(),
        )
        self.assertEqual(manifest["files"][0]["rows"], 1)
        self.assertEqual(manifest["row_count"], 1)

    def test_semantic_family_selects_smaller_instance(self):
        out = self.out_dir("pkg-semantic")
        result = build_minimal_repro(
            self.src,
            out,
            ReproContract("semantic_family", "orders", "MISPLACED_ROW"),
            budget=300,
        )
        preserved = result["preserved"]
        self.assertEqual(preserved["claim"], "rule_level_example")
        self.assertEqual(preserved["row_id"], REPRO_SHORT_ROW)
        self.assertIn("not the reproduction of a specific original record", preserved["note"])
        considered = result["candidates_considered"]
        self.assertEqual(len(considered), 2)
        self.assertEqual(
            {candidate["row_id"] for candidate in considered},
            {REPRO_LONG_ROW, REPRO_SHORT_ROW},
        )
        selected = [candidate for candidate in considered if candidate["selected"]]
        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0]["row_id"], REPRO_SHORT_ROW)
        package_audit = self.audit(out)
        self.assertEqual(package_audit["summary"]["critical_or_high"], 1)
        kept = [
            finding["row_id"]
            for finding in package_audit["findings"]
            if finding["code"] == "MISPLACED_ROW"
        ]
        self.assertEqual(kept, [REPRO_SHORT_ROW])

    def test_two_contracts_keep_different_rows(self):
        exact = build_minimal_repro(
            self.src,
            self.out_dir("pkg-exact-2"),
            ReproContract("exact_row", "orders", "MISPLACED_ROW", REPRO_LONG_ROW),
            budget=300,
        )
        semantic = build_minimal_repro(
            self.src,
            self.out_dir("pkg-semantic-2"),
            ReproContract("semantic_family", "orders", "MISPLACED_ROW"),
            budget=300,
        )
        self.assertEqual(exact["preserved"]["row_id"], REPRO_LONG_ROW)
        self.assertEqual(semantic["preserved"]["row_id"], REPRO_SHORT_ROW)
        self.assertNotEqual(
            exact["preserved"]["claim"], semantic["preserved"]["claim"]
        )
        # The semantic example is the smaller package.
        self.assertLess(semantic["package"]["bytes"], exact["package"]["bytes"])

    def test_manifest_hash_target_survives_consistent_rebuild(self):
        out = self.out_dir("pkg-hash")
        result = build_minimal_repro(
            self.src,
            out,
            ReproContract("exact_row", "orders", "MANIFEST_HASH"),
            budget=300,
        )
        original_declared = result["target_finding"]["detail"]["declared"]
        package_audit = self.audit(out)
        by_code = package_audit["summary"]["by_code"]
        # The target manifest defect is preserved...
        self.assertEqual(by_code.get("MANIFEST_HASH"), 1)
        target = next(
            finding
            for finding in package_audit["findings"]
            if finding["code"] == "MANIFEST_HASH"
        )
        self.assertEqual(target["shard"], REPRO_HASH_SHARD)
        self.assertEqual(target["detail"]["file"], REPRO_HASH_FILE)
        self.assertEqual(target["detail"]["declared"], original_declared)
        # ...while the rebuilt manifest stays consistent everywhere else:
        # deleting rows did not manufacture new manifest defects.
        for code in (
            "MANIFEST_FILE_ROWS",
            "MANIFEST_ROW_COUNT",
            "MANIFEST_FILE_MISSING",
            "MANIFEST_MISSING",
            "ORPHAN_FILE",
        ):
            self.assertNotIn(code, by_code)
        self.assertEqual(package_audit["summary"]["critical_or_high"], 1)
        manifest = json.loads(
            (out / "shards" / "orders" / REPRO_HASH_SHARD / "_manifest.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(manifest["files"][0]["sha256"], original_declared)
        self.assertEqual(manifest["files"][0]["rows"], 0)
        self.assertEqual(manifest["row_count"], 0)
        self.assertEqual(result["verification"]["minimality"], "1-minimal")

    def test_duplicate_row_keeps_both_cross_file_placements(self):
        out = self.out_dir("pkg-dup")
        result = build_minimal_repro(
            self.src,
            out,
            ReproContract("exact_row", "orders", "DUPLICATE_ROW", REPRO_DUP_ROW),
            budget=300,
        )
        package_audit = self.audit(out)
        duplicates = [
            finding
            for finding in package_audit["findings"]
            if finding["code"] == "DUPLICATE_ROW"
        ]
        self.assertEqual(len(duplicates), 1)
        duplicate = duplicates[0]
        self.assertEqual(duplicate["row_id"], REPRO_DUP_ROW)
        original = result["target_finding"]
        self.assertEqual(duplicate["detail"]["shards"], original["detail"]["shards"])
        self.assertEqual(len(duplicate["detail"]["placements"]), 2)
        paths = {placement["path"] for placement in duplicate["detail"]["placements"]}
        self.assertEqual(len(paths), 2)
        for relative in paths:
            self.assertTrue((out / "shards" / relative).is_file())
        self.assertEqual(package_audit["summary"]["critical_or_high"], 1)
        self.assertEqual(result["verification"]["minimality"], "1-minimal")

    def test_source_workspace_stays_byte_identical(self):
        before = snapshot_tree(self.src)
        result = build_minimal_repro(
            self.src,
            self.out_dir("pkg-readonly"),
            ReproContract("exact_row", "orders", "MANIFEST_HASH"),
            budget=300,
        )
        self.assertTrue(result["source_unchanged"])
        self.assertEqual(before, snapshot_tree(self.src))

    def test_budget_exhaustion_marks_unproven_not_minimal(self):
        out = self.out_dir("pkg-budget")
        result = build_minimal_repro(
            self.src,
            out,
            ReproContract("exact_row", "orders", "MISPLACED_ROW", REPRO_LONG_ROW),
            budget=3,
        )
        verification = result["verification"]
        self.assertEqual(verification["minimality"], "unproven")
        self.assertTrue(verification["budget_exhausted"])
        self.assertLessEqual(verification["candidate_audits"], 3)
        # The package is still a valid reproduction of the target.
        self.assertTrue(verification["target_present"])
        package_audit = self.audit(out)
        misplaced = {
            finding["row_id"]
            for finding in package_audit["findings"]
            if finding["code"] == "MISPLACED_ROW"
        }
        self.assertIn(REPRO_LONG_ROW, misplaced)
        self.assertEqual(verification["new_high_findings"], [])

    def test_package_summary_is_self_consistent(self):
        out = self.out_dir("pkg-consistency")
        result = build_minimal_repro(
            self.src,
            out,
            ReproContract("exact_row", "orders", "MISPLACED_ROW", REPRO_LONG_ROW),
            budget=300,
        )
        rerun = self.audit(out)
        self.assertEqual(result["verification"]["audit_summary"], rerun["summary"])
        self.assertEqual(result["verification"]["audit_tables"], rerun["tables"])
        document = json.loads((out / "repro.json").read_text(encoding="utf-8"))
        self.assertEqual(document["verification"]["audit_summary"], rerun["summary"])
        self.assertEqual(
            document["package"]["logical_rows"],
            {table: rerun["tables"][table]["logical_rows"] for table in rerun["tables"]},
        )
        self.assertEqual(
            document["package"]["physical_rows"],
            sum(rerun["tables"][table]["physical_rows"] for table in rerun["tables"]),
        )

    def test_missing_target_reports_clear_reason(self):
        with self.assertRaises(ShardRouteError) as caught:
            build_minimal_repro(
                self.src,
                self.out_dir("pkg-missing"),
                ReproContract("exact_row", "orders", "MISPLACED_ROW", "ord_9999"),
            )
        self.assertEqual(caught.exception.code, "REPRO_NO_TARGET")
        self.assertIn("available", caught.exception.details)

    def test_output_directory_validation(self):
        with self.assertRaises(ShardRouteError) as caught:
            build_minimal_repro(
                self.src,
                self.src / "inside",
                ReproContract("exact_row", "orders", "MISPLACED_ROW", REPRO_LONG_ROW),
            )
        self.assertEqual(caught.exception.code, "REPRO_OUTPUT")
        occupied = self.out_dir("pkg-occupied")
        occupied.mkdir()
        (occupied / "keep.txt").write_text("do not delete", encoding="utf-8")
        with self.assertRaises(ShardRouteError) as caught:
            build_minimal_repro(
                self.src,
                occupied,
                ReproContract("exact_row", "orders", "MISPLACED_ROW", REPRO_LONG_ROW),
            )
        self.assertEqual(caught.exception.code, "REPRO_OUTPUT")
        self.assertTrue((occupied / "keep.txt").is_file())

    def test_cli_repro_and_standard_audit_rerun(self):
        out = self.out_dir("pkg-cli")
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            code = main(
                [
                    "repro",
                    "--workspace",
                    str(self.src),
                    "--out",
                    str(out),
                    "--mode",
                    "exact_row",
                    "--table",
                    "orders",
                    "--code",
                    "MISPLACED_ROW",
                    "--row-id",
                    REPRO_LONG_ROW,
                    "--budget",
                    "300",
                ]
            )
        self.assertEqual(code, 0)
        result = json.loads(buffer.getvalue())
        self.assertEqual(result["preserved"]["row_id"], REPRO_LONG_ROW)
        self.assertTrue((out / "repro.json").is_file())
        # The package is a normal workspace: the standard audit command
        # re-runs on it and still reports the target finding.
        audit_buffer = io.StringIO()
        with contextlib.redirect_stdout(audit_buffer):
            self.assertEqual(main(["audit", "--workspace", str(out)]), 0)
        rerun = json.loads(audit_buffer.getvalue())
        self.assertEqual(rerun["summary"]["by_code"].get("MISPLACED_ROW"), 1)
        self.assertEqual(rerun["summary"]["critical_or_high"], 1)
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(
                main(["validate", "--config", str(out / "config" / "topology_old.json")]),
                0,
            )

if __name__ == "__main__":
    unittest.main()
