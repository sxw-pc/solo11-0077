"""Amount content-fidelity tests for the two authorized precision contracts.

The placement precondition is proven row-by-row before any amount total is
admitted.  Every check below uses exact decimal lexemes and integer cents;
binary float never appears in an assertion.
"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from shardroute.amount_audit import (
    AMOUNT_DECIMAL_INVALID,
    AMOUNT_DUAL_MARGIN_INFEASIBLE,
    AMOUNT_DUAL_MARGIN_UNPROVEN,
    AMOUNT_DUAL_MARGIN_VIOLATION,
    AMOUNT_FIELD_UNTRUSTED,
    AMOUNT_NEGATIVE_AMOUNT,
    AMOUNT_OVER_PRECISION,
    AMOUNT_ROW_HALF_EVEN_MISMATCH,
)
from shardroute.audit import run_audit
from shardroute.config_validation import validate_document
from shardroute.models import LogicalRow, PhysicalRow, ScanResult
from shardroute.planner import build_plan
from shardroute.report import build_report
from shardroute.scanner import scan_workspace

from support import base_doc, mini_topology

TENANTS = ("ta", "tb")
CHANNELS = ("wa", "wb")
# Four cross cells, each one logical order of exactly 0.005.
CELLS = [
    ("r1", "ta", "wa"),
    ("r2", "ta", "wb"),
    ("r3", "tb", "wa"),
    ("r4", "tb", "wb"),
]


def contract_doc(contracts):
    doc = base_doc()
    orders = next(item for item in doc["tables"] if item["table"] == "orders")
    orders["amount_contracts"] = [
        {"contract": name, "repair_budget": 1000} for name in contracts
    ]
    topology, issues, _ = validate_document(doc)
    assert topology is not None and not issues, [i.as_dict() for i in issues]
    return topology


def logical_rows(amounts):
    rows = []
    for (row_id, tenant, channel), amount in zip(CELLS, amounts):
        rows.append(
            LogicalRow(
                "orders",
                row_id,
                {"row_id": row_id, "tenant_id": tenant, "channel": channel, "amount": amount},
                "logical/orders.csv",
                2 + CELLS.index((row_id, tenant, channel)),
            )
        )
    return rows


def physical_rows(amounts, *, shard="shard_a"):
    rows = []
    for (row_id, tenant, channel), amount in zip(CELLS, amounts):
        rows.append(
            PhysicalRow(
                "orders",
                shard,
                row_id,
                {"row_id": row_id, "tenant_id": tenant, "channel": channel, "amount": amount},
                f"orders/{shard}/part-000.csv",
                2 + CELLS.index((row_id, tenant, channel)),
            )
        )
    return rows


def scan(amounts_logical, amounts_physical, *, physical_override=None,
         logical_override=None, shard="shard_a"):
    logical = logical_override if logical_override is not None else logical_rows(amounts_logical)
    physical = (
        physical_override
        if physical_override is not None
        else physical_rows(amounts_physical, shard=shard)
    )
    return ScanResult(
        logical_rows={"orders": logical, "events": [], "profiles": []},
        physical_rows={"orders": physical, "events": [], "profiles": []},
        files=[],
        manifests={},
        manifest_errors=[],
        physical_dirs=[("orders", shard)],
        logical_fingerprint="0" * 64,
        physical_fingerprint="1" * 64,
    )


def verdict(audit, contract):
    return next(item for item in audit["amount_contracts"] if item["contract"] == contract)


def codes(audit):
    return [finding["code"] for finding in audit["findings"]]


class FourRowContractTests(unittest.TestCase):
    LOGICAL = ["0.005", "0.005", "0.005", "0.005"]

    def test_all_zero_passes_row_half_even_but_fails_dual_margin(self):
        topology = contract_doc(["row_half_even", "dual_margin"])
        result = run_audit(topology, scan(self.LOGICAL, ["0.00", "0.00", "0.00", "0.00"]))
        he = verdict(result, "row_half_even")
        dm = verdict(result, "dual_margin")
        self.assertEqual(he["status"], "pass")
        self.assertEqual(he["mismatches"], [])
        self.assertEqual(dm["status"], "fail")
        # Every tenant and every channel margin must be 0.01 but is 0.00.
        self.assertEqual(len(dm["margins"]), 4)
        self.assertTrue(all(row["expected_cents"] == 1 for row in dm["margins"]))
        self.assertTrue(all(row["actual_cents"] == 0 for row in dm["margins"]))
        margin_codes = [item for item in codes(result) if item == AMOUNT_DUAL_MARGIN_VIOLATION]
        self.assertEqual(len(margin_codes), 4)
        self.assertNotIn(AMOUNT_ROW_HALF_EVEN_MISMATCH, codes(result))

    def test_diagonal_split_passes_dual_margin_but_fails_row_half_even(self):
        topology = contract_doc(["row_half_even", "dual_margin"])
        result = run_audit(topology, scan(self.LOGICAL, ["0.01", "0.00", "0.00", "0.01"]))
        he = verdict(result, "row_half_even")
        dm = verdict(result, "dual_margin")
        self.assertEqual(he["status"], "fail")
        # The two 0.01 rows violate half-even (logical 0.005 rounds to 0.00);
        # the two 0.00 rows are exactly the half-even result.
        self.assertEqual(len(he["mismatches"]), 2)
        self.assertEqual({item["row_id"] for item in he["mismatches"]}, {"r1", "r4"})
        self.assertEqual(dm["status"], "pass")
        for row in dm["margins"]:
            self.assertTrue(row["matches"], row)
        self.assertEqual(dm["repair"], None)

    def test_dual_margin_witness_is_minimum_change_and_replayable(self):
        topology = contract_doc(["dual_margin"])
        result = run_audit(topology, scan(self.LOGICAL, ["0.00", "0.00", "0.00", "0.00"]))
        dm = verdict(result, "dual_margin")
        repair = dm["repair"]
        self.assertEqual(repair["status"], "witness")
        self.assertTrue(repair["minimum_proven"])
        # Two tenants and two channels each need one cent => at least two rows.
        self.assertEqual(repair["changed_rows"], 2)
        changed = {item["row_id"] for item in repair["changes"]}
        self.assertEqual(len(changed), 2)
        for item in repair["changes"]:
            self.assertEqual(item["from_cents"], 0)
            self.assertEqual(item["to_cents"], 1)
            self.assertEqual(item["allowed_cents"], [0, 1])
            # Row identity is preserved on the witness.
            cell = next(c for c in CELLS if c[0] == item["row_id"])
            self.assertEqual(item["tenant_id"], cell[1])
            self.assertEqual(item["channel"], cell[2])
        # Both witness margin dimensions reconcile exactly.
        for dimension in ("tenant", "channel"):
            for margin in repair["witness_margins"][dimension]:
                self.assertEqual(margin["cents"], margin["expected_cents"])
        # Either diagonal is a valid minimum witness: the other diagonal is
        # itself feasible, so check the returned change set is one of them.
        diagonals = [{"r1", "r4"}, {"r2", "r3"}]
        self.assertIn(changed, diagonals)

    def test_same_column_greedy_assignment_still_violates_cross_margin(self):
        # Both tenant cent needs greedily satisfied inside channel wa:
        # wa totals 0.02, wb 0.00; both channel margins are wrong.
        topology = contract_doc(["dual_margin"])
        result = run_audit(topology, scan(self.LOGICAL, ["0.01", "0.00", "0.01", "0.00"]))
        dm = verdict(result, "dual_margin")
        self.assertEqual(dm["status"], "fail")
        channel_margins = {row["key"]: row for row in dm["margins"] if row["dimension"] == "channel"}
        self.assertEqual(channel_margins["wa"]["actual_cents"], 2)
        self.assertEqual(channel_margins["wb"]["actual_cents"], 0)
        self.assertFalse(channel_margins["wa"]["matches"])
        self.assertFalse(channel_margins["wb"]["matches"])
        # The solver still finds the cross-feasible diagonal two-row witness.
        self.assertEqual(dm["repair"]["status"], "witness")
        self.assertEqual(dm["repair"]["changed_rows"], 2)

    def test_correct_route_but_changed_amount_is_not_a_routing_finding(self):
        topology = contract_doc(["row_half_even", "dual_margin"])
        result = run_audit(
            topology,
            scan(["1.23", "2.34", "3.45", "4.56"], ["1.24", "2.34", "9.99", "4.56"]),
        )
        placement_codes = {
            "MISSING_ROW", "DUPLICATE_ROW", "MISPLACED_ROW", "ORPHAN_ROW", "ROUTE_ERROR"
        }
        self.assertFalse(placement_codes.intersection(codes(result)))
        he = verdict(result, "row_half_even")
        bad_ids = {item["row_id"] for item in he["mismatches"]}
        self.assertEqual(bad_ids, {"r1", "r3"})
        detail = next(item for item in he["mismatches"] if item["row_id"] == "r1")
        self.assertEqual(detail["expected_amount"], "1.23")
        self.assertEqual(detail["actual_amount"], "1.24")

    def test_half_even_uses_bankers_rounding_per_row(self):
        # 0.015 half-even -> 0.02; 0.025 half-even -> 0.02; 0.035 -> 0.04.
        topology = contract_doc(["row_half_even"])
        logical = ["0.015", "0.025", "0.035", "1.005"]
        result = run_audit(topology, scan(logical, ["0.02", "0.02", "0.04", "1.00"]))
        self.assertEqual(verdict(result, "row_half_even")["status"], "pass")
        result_bad = run_audit(topology, scan(logical, ["0.01", "0.03", "0.03", "1.01"]))
        self.assertEqual(verdict(result_bad, "row_half_even")["status"], "fail")
        self.assertEqual(len(verdict(result_bad, "row_half_even")["mismatches"]), 4)


class BadInputTests(unittest.TestCase):
    LOGICAL = ["0.005", "0.005", "0.005", "0.005"]

    def _run(self, logical_amounts, physical_amounts, contracts=("row_half_even", "dual_margin")):
        topology = contract_doc(list(contracts))
        return run_audit(topology, scan(logical_amounts, physical_amounts))

    def test_illegal_decimal_lexemes_are_rejected_not_float_passed(self):
        result = self._run(["1e-2", "0.005", "0.005", "0.005"], self._physical_zeros())
        self.assertIn(AMOUNT_DECIMAL_INVALID, codes(result))
        for contract in ("row_half_even", "dual_margin"):
            self.assertEqual(verdict(result, contract)["status"], "unknown")
        # The rejected row never enters a margin total.
        dm = verdict(result, "dual_margin")
        admitted = {row["row_id"] for row in dm["rows"] if row["eligible"]}
        self.assertNotIn("r1", admitted)

    def test_negative_amounts_are_rejected(self):
        result = self._run(["-0.01", "0.005", "0.005", "0.005"], self._physical_zeros())
        self.assertIn(AMOUNT_NEGATIVE_AMOUNT, codes(result))
        result2 = self._run(self.LOGICAL, ["-0.01", "0.00", "0.00", "0.00"])
        self.assertIn(AMOUNT_NEGATIVE_AMOUNT, codes(result2))

    def test_sub_cent_physical_amount_is_over_precision(self):
        result = self._run(self.LOGICAL, ["0.001", "0.00", "0.00", "0.00"])
        self.assertIn(AMOUNT_OVER_PRECISION, codes(result))
        self.assertEqual(verdict(result, "dual_margin")["status"], "unknown")

    def test_missing_group_field_is_untrusted(self):
        physical = physical_rows(self._physical_zeros())
        del physical[0].values["channel"]
        result = run_audit(contract_doc(["dual_margin"]), scan(self.LOGICAL, None, physical_override=physical))
        self.assertIn(AMOUNT_FIELD_UNTRUSTED, codes(result))
        dm = verdict(result, "dual_margin")
        self.assertEqual(dm["status"], "unknown")
        self.assertEqual(dm["ineligible_rows"], 1)

    def test_disagreeing_group_field_is_untrusted(self):
        physical = physical_rows(self._physical_zeros())
        physical[0] = PhysicalRow(
            "orders", "shard_a", "r1",
            {"row_id": "r1", "tenant_id": "ta", "channel": "wb", "amount": "0.00"},
            physical[0].source, physical[0].line_no,
        )
        result = run_audit(contract_doc(["dual_margin"]), scan(self.LOGICAL, None, physical_override=physical))
        self.assertIn(AMOUNT_FIELD_UNTRUSTED, codes(result))
        self.assertEqual(verdict(result, "dual_margin")["status"], "unknown")

    @staticmethod
    def _physical_zeros():
        return ["0.00", "0.00", "0.00", "0.00"]


class PlacementPreconditionTests(unittest.TestCase):
    LOGICAL = ["0.005", "0.005", "0.005", "0.005"]

    def test_missing_and_duplicate_rows_never_enter_amount_totals(self):
        topology = contract_doc(["dual_margin", "row_half_even"])
        physical = physical_rows(["0.01", "0.01", "0.01", "0.01"])
        # Drop r4 (missing) and duplicate r2 into the same shard.
        physical = [row for row in physical if row.row_id != "r4"]
        duplicate = physical[1]
        physical.append(
            PhysicalRow("orders", "shard_a", duplicate.row_id, dict(duplicate.values),
                        duplicate.source, duplicate.line_no + 10)
        )
        result = run_audit(topology, scan(self.LOGICAL, None, physical_override=physical))
        self.assertIn("MISSING_ROW", codes(result))
        self.assertIn("DUPLICATE_ROW", codes(result))
        dm = verdict(result, "dual_margin")
        eligible = {row["row_id"] for row in dm["rows"] if row["eligible"]}
        self.assertEqual(eligible, {"r1", "r3"})
        for excluded in ("r2", "r4"):
            record = next(row for row in dm["rows"] if row["row_id"] == excluded)
            self.assertFalse(record["eligible"])
        self.assertEqual(dm["status"], "unknown")

    def test_misplaced_row_is_amount_ineligible(self):
        topology = contract_doc(["row_half_even"])
        physical = physical_rows(["0.00", "0.00", "0.00", "0.00"], shard="shard_b")
        result = run_audit(topology, scan(self.LOGICAL, None, physical_override=physical))
        self.assertIn("MISPLACED_ROW", codes(result))
        he = verdict(result, "row_half_even")
        self.assertEqual(he["eligible_rows"], 0)
        self.assertEqual(he["status"], "unknown")


class InfeasibilityTests(unittest.TestCase):
    def _vars(self):
        from shardroute.amount_audit import _Var

        specs = [
            ("r1", "ta", "wa"),
            ("r2", "ta", "wb"),
            ("r3", "tb", "wa"),
            ("r4", "tb", "wb"),
        ]
        return [
            _Var(
                row_id=rid, tenant=t, channel=c, options=(0, 1), current=0,
                logical_lexeme="0.005", physical_lexeme="0.00",
                source={"path": "orders/shard_a/part-000.csv", "line": 2 + index},
            )
            for index, (rid, t, c) in enumerate(specs)
        ]

    def test_contradictory_cross_margins_are_proven_infeasible(self):
        # Classic binary 2x2 contradiction: both tenant ta rows and both
        # channel wa rows are forced to 1, which forces tb row r3 to 1 while
        # the tb margin demands 0.  Every single group target lies inside its
        # own reachable [0, 2] bound, so only the joint cross-margin structure
        # exposes the impossibility.
        from shardroute.amount_audit import _solve_witness

        variables = self._vars()
        search = _solve_witness(
            variables,
            tenant_target={"ta": 2, "tb": 0},
            channel_target={"wa": 2, "wb": 0},
            budget=5000,
        )
        self.assertEqual(search["status"], "infeasible")
        self.assertLessEqual(search["nodes_used"], 5000)

    def test_infeasibility_evidence_is_checkable_and_replayable(self):
        from shardroute.amount_audit import _infeasibility_evidence, _solve_witness

        variables = self._vars()
        tenant_target = {"ta": 2, "tb": 0}
        channel_target = {"wa": 2, "wb": 0}
        search = _solve_witness(variables, tenant_target, channel_target, 5000)
        margins = [
            {"dimension": "tenant", "key": "ta", "expected_cents": 2, "min_cents": 0, "max_cents": 2},
            {"dimension": "tenant", "key": "tb", "expected_cents": 0, "min_cents": 0, "max_cents": 2},
            {"dimension": "channel", "key": "wa", "expected_cents": 2, "min_cents": 0, "max_cents": 2},
            {"dimension": "channel", "key": "wb", "expected_cents": 0, "min_cents": 0, "max_cents": 2},
        ]
        evidence = _infeasibility_evidence(
            variables, tenant_target, channel_target, margins, search
        )
        # No single-group bound explains it: the contradiction is cross-margin.
        self.assertEqual(evidence["bound_violations"], [])
        totals = evidence["partition_totals"]
        self.assertEqual(totals["tenant_target_total_cents"], 2)
        self.assertEqual(totals["channel_target_total_cents"], 2)
        self.assertTrue(totals["partitions_agree"])
        self.assertEqual(evidence["exhausted_search"]["variable_order"],
                         ["r1", "r2", "r3", "r4"])

    def test_out_of_bound_target_is_flagged_directly(self):
        from shardroute.amount_audit import _infeasibility_evidence, _solve_witness

        variables = self._vars()
        tenant_target = {"ta": 5, "tb": 0}
        channel_target = {"wa": 2, "wb": 0}
        search = _solve_witness(variables, tenant_target, channel_target, 5000)
        self.assertEqual(search["status"], "infeasible")
        margins = [
            {"dimension": "tenant", "key": "ta", "expected_cents": 5, "min_cents": 0, "max_cents": 2},
            {"dimension": "tenant", "key": "tb", "expected_cents": 0, "min_cents": 0, "max_cents": 2},
            {"dimension": "channel", "key": "wa", "expected_cents": 2, "min_cents": 0, "max_cents": 2},
            {"dimension": "channel", "key": "wb", "expected_cents": 0, "min_cents": 0, "max_cents": 2},
        ]
        evidence = _infeasibility_evidence(
            variables, tenant_target, channel_target, margins, search
        )
        self.assertEqual(len(evidence["bound_violations"]), 1)
        self.assertEqual(evidence["bound_violations"][0]["key"], "ta")
        self.assertFalse(evidence["partition_totals"]["partitions_agree"])

    def test_derived_margins_on_clean_data_always_admit_a_witness(self):
        # The matrix-rounding property: targets derived as half-even sums of
        # exact cell values with per-cell adjacent options are always jointly
        # satisfiable.  The all-zero physical four-row fixture therefore gets
        # a witness, never an infeasibility verdict.
        topology = contract_doc(["dual_margin"])
        result = run_audit(topology, scan(["0.005"] * 4, ["0.00"] * 4))
        dm = verdict(result, "dual_margin")
        self.assertEqual(dm["repair"]["status"], "witness")
        self.assertNotIn(AMOUNT_DUAL_MARGIN_INFEASIBLE, codes(result))

    def test_budget_exceeded_is_unknown_never_a_repair_claim(self):
        topology = contract_doc(["dual_margin"])
        # Shrink the budget through a bespoke topology document.
        doc = base_doc()
        orders = next(item for item in doc["tables"] if item["table"] == "orders")
        orders["amount_contracts"] = [{"contract": "dual_margin", "repair_budget": 1}]
        topology, issues, _ = validate_document(doc)
        assert topology is not None and not issues
        result = run_audit(
            topology,
            scan(["0.005"] * 4, ["0.00"] * 4),
        )
        dm = verdict(result, "dual_margin")
        self.assertEqual(dm["repair"]["status"], "budget_exceeded")
        self.assertEqual(dm["status"], "unknown")
        self.assertIn(AMOUNT_DUAL_MARGIN_UNPROVEN, codes(result))


class ScannerLexemeTests(unittest.TestCase):
    def _workspace(self, logical_text, physical_text):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        shard_dir = root / "shards" / "orders" / "shard_a"
        shard_dir.mkdir(parents=True)
        logical_dir = root / "logical"
        logical_dir.mkdir()
        (logical_dir / "orders.csv").write_text(logical_text, encoding="utf-8")
        (shard_dir / "part-000.csv").write_text(physical_text, encoding="utf-8")
        return root

    def test_csv_and_jsonl_decimal_lexemes_are_exact(self):
        root = self._workspace(
            "row_id,tenant_id,channel,amount\n"
            "r1,ta,wa,0.005\nr2,ta,wb,0.005\nr3,tb,wa,0.005\nr4,tb,wb,0.005\n",
            "row_id,tenant_id,channel,amount\n"
            "r1,ta,wa,0.00\nr2,ta,wb,0.01\nr3,tb,wa,0.01\nr4,tb,wb,0.00\n",
        )
        topology = contract_doc(["row_half_even", "dual_margin"])
        scanned = scan_workspace(root / "logical", root / "shards", ["orders"])
        result = run_audit(topology, scanned)
        self.assertEqual(verdict(result, "dual_margin")["status"], "pass")
        self.assertEqual(verdict(result, "row_half_even")["status"], "fail")
        # Lexeme provenance points at the exact file and line.
        dm = verdict(result, "dual_margin")
        token = next(
            item for item in dm["evidence_chain"]["scanner"]["tokens"] if item["row_id"] == "r1"
        )
        self.assertEqual(token["logical"]["lexeme"], "0.005")
        self.assertEqual(token["physical"]["lexeme"], "0.00")
        self.assertEqual(token["logical"]["path"], "logical/orders.csv")

    def test_jsonl_float_token_retains_lexeme(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        shard_dir = root / "shards" / "orders" / "shard_a"
        shard_dir.mkdir(parents=True)
        logical_dir = root / "logical"
        logical_dir.mkdir()
        (logical_dir / "orders.jsonl").write_text(
            '{"row_id":"r1","tenant_id":"ta","channel":"wa","amount":0.005}\n'
            '{"row_id":"r2","tenant_id":"ta","channel":"wb","amount":0.005}\n'
            '{"row_id":"r3","tenant_id":"tb","channel":"wa","amount":0.005}\n'
            '{"row_id":"r4","tenant_id":"tb","channel":"wb","amount":0.005}\n',
            encoding="utf-8",
        )
        (shard_dir / "part.jsonl").write_text(
            '{"row_id":"r1","tenant_id":"ta","channel":"wa","amount":0.00}\n'
            '{"row_id":"r2","tenant_id":"ta","channel":"wb","amount":0.01}\n'
            '{"row_id":"r3","tenant_id":"tb","channel":"wa","amount":0.01}\n'
            '{"row_id":"r4","tenant_id":"tb","channel":"wb","amount":0.00}\n',
            encoding="utf-8",
        )
        topology = contract_doc(["dual_margin"])
        scanned = scan_workspace(root / "logical", root / "shards", ["orders"])
        self.assertIsInstance(scanned.logical_rows["orders"][0].values["amount"], str)
        result = run_audit(topology, scanned)
        self.assertEqual(verdict(result, "dual_margin")["status"], "pass")

    def test_jsonl_exponent_notation_is_rejected(self):
        root = self._workspace(
            "row_id,tenant_id,channel,amount\nr1,ta,wa,0.005\nr2,ta,wb,0.005\n"
            "r3,tb,wa,0.005\nr4,tb,wb,0.005\n",
            "row_id,tenant_id,channel,amount\nr1,ta,wa,0.00\nr2,ta,wb,0.00\n"
            "r3,tb,wa,0.00\nr4,tb,wb,0.00\n",
        )
        # Replace the physical file with JSONL whose amount uses exponent
        # notation: parse_float preserves the "5e-3" lexeme and the strict
        # decimal grammar rejects it instead of float-transcribing it.
        (root / "shards" / "orders" / "shard_a" / "part-000.csv").unlink()
        jsonl = root / "shards" / "orders" / "shard_a" / "part-000.jsonl"
        jsonl.write_text(
            '{"row_id":"r1","tenant_id":"ta","channel":"wa","amount":5e-3}\n'
            '{"row_id":"r2","tenant_id":"ta","channel":"wb","amount":0.00}\n'
            '{"row_id":"r3","tenant_id":"tb","channel":"wa","amount":0.00}\n'
            '{"row_id":"r4","tenant_id":"tb","channel":"wb","amount":0.00}\n',
            encoding="utf-8",
        )
        topology = contract_doc(["dual_margin"])
        scanned = scan_workspace(root / "logical", root / "shards", ["orders"])
        result = run_audit(topology, scanned)
        self.assertIn(AMOUNT_DECIMAL_INVALID, codes(result))
        dm = verdict(result, "dual_margin")
        self.assertEqual(dm["status"], "unknown")
        self.assertFalse(any(row["row_id"] == "r1" and row["eligible"] for row in dm["rows"]))


class GateAndReportTests(unittest.TestCase):
    LOGICAL = ["0.005"] * 4

    def test_plan_gates_are_separate_and_blocking(self):
        topology = contract_doc(["row_half_even", "dual_margin"])
        sc = scan(self.LOGICAL, ["0.00"] * 4)
        audit = run_audit(topology, sc)
        plan = build_plan(topology, topology, sc.logical_rows, audit)
        gate_codes = {gate["code"] for gate in plan["amount_gates"]}
        self.assertIn("PLAN_AMOUNT_DUAL_MARGIN_VIOLATION", gate_codes)
        # row_half_even passes, so it must not produce a gate.
        self.assertFalse(any("ROW_HALF_EVEN" in code for code in gate_codes))
        self.assertTrue(all(gate["severity"] == "blocking" for gate in plan["amount_gates"]))
        self.assertFalse(plan["safe_to_execute"])
        # Placement blockers remain a separate list, free of amount codes.
        self.assertFalse(any("AMOUNT" in blocker["code"] for blocker in plan["blockers"]))

    def test_unknown_verdict_gates_even_without_violation_findings_on_margins(self):
        topology = contract_doc(["dual_margin"])
        physical = physical_rows(["0.01", "0.00", "0.00", "0.01"])
        del physical[0].values["channel"]
        sc = scan(self.LOGICAL, None, physical_override=physical)
        audit = run_audit(topology, sc)
        plan = build_plan(topology, topology, sc.logical_rows, audit)
        self.assertIn("PLAN_AMOUNT_DUAL_MARGIN_UNKNOWN",
                      {gate["code"] for gate in plan["amount_gates"]})
        self.assertFalse(plan["safe_to_execute"])

    def test_report_certificate_carries_full_causal_chain(self):
        topology = contract_doc(["row_half_even", "dual_margin"])
        sc = scan(self.LOGICAL, ["0.00"] * 4)
        audit = run_audit(topology, sc)
        plan = build_plan(topology, topology, sc.logical_rows, audit)
        report = build_report(
            old=topology, new=topology, seed=7,
            logical_fingerprint=sc.logical_fingerprint,
            physical_fingerprint=sc.physical_fingerprint,
            audit=audit, plan=plan, queries=[],
        )
        certs = {item["contract"]: item for item in report["amount_certificates"]["certificates"]}
        dm = certs["dual_margin"]
        self.assertTrue(dm["scanner_fingerprint"])
        self.assertTrue(dm["allowance_fingerprint"])
        self.assertTrue(dm["margin_fingerprint"])
        self.assertTrue(dm["verdict_fingerprint"])
        gate_codes_on_cert = {gate["code"] for gate in dm["plan_gates"]}
        self.assertIn("PLAN_AMOUNT_DUAL_MARGIN_VIOLATION", gate_codes_on_cert)
        self.assertEqual(dm["repair"]["changed_rows"], 2)
        self.assertEqual(report["summary"]["amount_contracts"], 2)
        self.assertGreaterEqual(report["summary"]["amount_gates"], 1)

    def test_lexeme_change_propagates_through_every_stage(self):
        topology = contract_doc(["dual_margin"])
        first_scan = scan(self.LOGICAL, ["0.01", "0.00", "0.00", "0.01"])
        first_audit = run_audit(topology, first_scan)
        # Mutate one physical lexeme (same route, different amount text).
        changed_physical = physical_rows(["0.01", "0.00", "0.01", "0.01"])
        second_scan = scan(self.LOGICAL, None, physical_override=changed_physical)
        second_audit = run_audit(topology, second_scan)
        f1 = verdict(first_audit, "dual_margin")["evidence_chain"]["scanner_fingerprint"]
        f2 = verdict(second_audit, "dual_margin")["evidence_chain"]["scanner_fingerprint"]
        self.assertNotEqual(f1, f2)
        self.assertNotEqual(
            verdict(first_audit, "dual_margin")["allowance_fingerprint"],
            verdict(second_audit, "dual_margin")["allowance_fingerprint"],
        )
        first_plan = build_plan(topology, topology, first_scan.logical_rows, first_audit)
        second_plan = build_plan(topology, topology, second_scan.logical_rows, second_audit)
        self.assertNotEqual(
            json.dumps(first_plan["amount_gates"], sort_keys=True),
            json.dumps(second_plan["amount_gates"], sort_keys=True),
        )
        r1 = build_report(old=topology, new=topology, seed=7,
                          logical_fingerprint="0" * 64, physical_fingerprint="1" * 64,
                          audit=first_audit, plan=first_plan, queries=[])
        r2 = build_report(old=topology, new=topology, seed=7,
                          logical_fingerprint="0" * 64, physical_fingerprint="2" * 64,
                          audit=second_audit, plan=second_plan, queries=[])
        self.assertNotEqual(
            r1["amount_certificates"]["certificate_fingerprint"],
            r2["amount_certificates"]["certificate_fingerprint"],
        )


class EdgeCaseTests(unittest.TestCase):
    LOGICAL = ["0.005"] * 4

    def test_all_rows_ineligible_is_unknown_not_a_pass(self):
        topology = contract_doc(["row_half_even", "dual_margin"])
        # Every physical row missing: nothing can be certified or totaled.
        sc = ScanResult(
            logical_rows={"orders": logical_rows(self.LOGICAL), "events": [], "profiles": []},
            physical_rows={"orders": [], "events": [], "profiles": []},
            files=[], manifests={}, manifest_errors=[], physical_dirs=[],
            logical_fingerprint="0" * 64, physical_fingerprint="1" * 64,
        )
        result = run_audit(topology, sc)
        self.assertIn("MISSING_ROW", codes(result))
        for verdict in result["amount_contracts"]:
            self.assertEqual(verdict["status"], "unknown")
            self.assertEqual(verdict["eligible_rows"], 0)

    def test_jsonl_integer_amount_lexeme_is_exact(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        shard_dir = root / "shards" / "orders" / "shard_a"
        shard_dir.mkdir(parents=True)
        logical_dir = root / "logical"
        logical_dir.mkdir()
        # Integer JSON amount (no fraction) must parse as an exact lexeme.
        (logical_dir / "orders.jsonl").write_text(
            '{"row_id":"r1","tenant_id":"ta","channel":"wa","amount":1}\n'
            '{"row_id":"r2","tenant_id":"ta","channel":"wb","amount":2}\n'
            '{"row_id":"r3","tenant_id":"tb","channel":"wa","amount":3}\n'
            '{"row_id":"r4","tenant_id":"tb","channel":"wb","amount":4}\n',
            encoding="utf-8",
        )
        (shard_dir / "part.jsonl").write_text(
            '{"row_id":"r1","tenant_id":"ta","channel":"wa","amount":1.00}\n'
            '{"row_id":"r2","tenant_id":"ta","channel":"wb","amount":2.00}\n'
            '{"row_id":"r3","tenant_id":"tb","channel":"wa","amount":3.00}\n'
            '{"row_id":"r4","tenant_id":"tb","channel":"wb","amount":4.00}\n',
            encoding="utf-8",
        )
        topology = contract_doc(["row_half_even", "dual_margin"])
        scanned = scan_workspace(logical_dir, root / "shards", ["orders"])
        # JSON integers stay ints; the amount parser stringifies them exactly
        # without any float involvement.
        self.assertEqual(scanned.logical_rows["orders"][0].values["amount"], 1)
        self.assertNotIsInstance(scanned.logical_rows["orders"][0].values["amount"], float)
        result = run_audit(topology, scanned)
        self.assertEqual(verdict(result, "row_half_even")["status"], "pass")
        self.assertEqual(verdict(result, "dual_margin")["status"], "pass")


class NoContractTests(unittest.TestCase):
    def test_tables_without_contract_keep_original_audit_semantics(self):
        topology = mini_topology()
        self.assertEqual(topology.table("orders").amount_contracts, ())
        rows = [
            LogicalRow("orders", f"o{i}",
                       {"row_id": f"o{i}", "tenant_id": f"tenant_demo_00{i}", "amount": "0.005"},
                       "logical/orders.csv", i + 1)
            for i in range(1, 5)
        ]
        sc = ScanResult(
            logical_rows={"orders": rows},
            physical_rows={"orders": []},
            files=[], manifests={}, manifest_errors=[], physical_dirs=[],
            logical_fingerprint="0" * 64, physical_fingerprint="1" * 64,
        )
        result = run_audit(topology, sc)
        self.assertEqual(result["amount_contracts"], [])
        self.assertIn("MISSING_ROW", codes(result))
        plan = build_plan(topology, topology, sc.logical_rows, result)
        self.assertEqual(plan["amount_gates"], [])
