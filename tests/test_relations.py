import copy
import unittest

from shardroute.audit import run_audit
from shardroute.config_validation import validate_document
from shardroute.models import (
    LogicalRow,
    ManifestEntry,
    ManifestRecord,
    PhysicalRow,
    ScanResult,
    ScannedFile,
)
from shardroute.planner import build_plan
from shardroute.relations import (
    RELATION_CHILD_COPY_MISSING,
    RELATION_MANIFEST_EVIDENCE_MISSING,
    RELATION_PARENT_CONFLICT,
    RELATION_PARENT_COPY_MISSING,
    RELATION_PARENT_DUPLICATE,
    RELATION_PARENT_MISSING,
    RELATION_PARENT_REPLICA_DRIFT,
    RELATION_PARENT_REPLICA_MISSING,
    RELATION_REFERENCE_MISSING,
    RELATION_SHARD_SPLIT,
    RELATION_TARGET_PARENT_DRIFT,
    RELATION_TARGET_PARENT_MISSING,
    RELATION_TARGET_SPLIT,
    business_fingerprint,
    evaluate_relations,
)

from support import base_doc, topology_from

ACCOUNT = "account_code"
REGION = "region"


def relation_doc(mode="local_join", name="orders_accounts"):
    doc = base_doc()
    doc["relations"] = [
        {
            "name": name,
            "parent_table": "accounts",
            "parent_key": ACCOUNT,
            "child_table": "orders",
            "child_key": "account_ref",
            "mode": mode,
        }
    ]
    orders = next(item for item in doc["tables"] if item["table"] == "orders")
    orders.clear()
    orders.update(
        {
            "table": "orders",
            "router": "list",
            "key": REGION,
            "null_policy": "route_default",
            "default_shard": "shard_a",
            "list_map": {"region_alpha": "shard_a", "region_beta": "shard_b"},
            "case_sensitive": True,
        }
    )
    doc["tables"].append(
        {
            "table": "accounts",
            "router": "list",
            "key": REGION,
            "null_policy": "route_default",
            "default_shard": "shard_b",
            "list_map": {"region_alpha": "shard_a", "region_beta": "shard_b"},
            "case_sensitive": True,
        }
    )
    return doc


def row(table, row_id, values):
    return LogicalRow(table, row_id, {"row_id": row_id, **values}, f"logical/{table}.csv", 2)


def place(table, shard, row_id, values, line=2):
    return PhysicalRow(
        table, shard, row_id, {"row_id": row_id, **values},
        f"{table}/{shard}/part-000.csv", line,
    )


def scan_for(accounts, orders, account_physical, order_physical, *, manifests=True):
    files = []
    manifest_records = {}
    for table, physical in (("accounts", account_physical), ("orders", order_physical)):
        by_shard = {}
        for item in physical:
            by_shard.setdefault(item.shard, []).append(item)
        for shard, rows in by_shard.items():
            source = f"{table}/{shard}/part-000.csv"
            digest = (table + shard).encode().hex()[:64].ljust(64, "0")
            files.append(ScannedFile(table, shard, "part-000.csv", source, digest, len(rows)))
            if manifests:
                manifest_records[(table, shard)] = ManifestRecord(
                    table, shard, f"{table}/{shard}/_manifest.json", len(rows),
                    (ManifestEntry("part-000.csv", len(rows), digest),),
                    "2026-01-02T00:00:00Z",
                )
    return ScanResult(
        logical_rows={"accounts": accounts, "orders": orders, "profiles": [], "events": []},
        physical_rows={
            "accounts": list(account_physical),
            "orders": list(order_physical),
            "profiles": [],
            "events": [],
        },
        files=files,
        manifests=manifest_records,
        manifest_errors=[],
        physical_dirs=sorted({(item.table, item.shard) for item in account_physical + order_physical}),
        logical_fingerprint="0" * 64,
        physical_fingerprint="1" * 64,
    )


def scenario(mode="local_join", *, account_region="region_alpha", order_region="region_alpha",
             accounts=None, account_shards=None, order_shard="shard_a", extra_account_rows=None):
    """One child row referencing one parent row, with explicit placements."""
    accounts = accounts or [row("accounts", "acc_1", {ACCOUNT: "ACC_1", REGION: account_region, "name": "Acme"})]
    orders = [row("orders", "ord_1", {REGION: order_region, "account_ref": "ACC_1", "amount": "1.00"})]
    regions = {"region_alpha": "shard_a", "region_beta": "shard_b"}
    account_shards = account_shards or [regions[account_region]]
    account_physical = [
        place("accounts", shard, "acc_1", {ACCOUNT: "ACC_1", REGION: account_region, "name": "Acme"}, line=2 + index)
        for index, shard in enumerate(account_shards)
    ]
    account_physical.extend(extra_account_rows or [])
    order_physical = [place("orders", order_shard, "ord_1", {REGION: order_region, "account_ref": "ACC_1", "amount": "1.00"})]
    return topology_from(relation_doc(mode)), scan_for(accounts, orders, account_physical, order_physical)


class RelationConfigTests(unittest.TestCase):
    def test_relation_is_parsed_and_missing_declaration_keeps_old_shape(self):
        legacy, issues, _ = validate_document(base_doc())
        self.assertFalse(issues)
        self.assertEqual(legacy.relations, ())
        declared = topology_from(relation_doc())
        self.assertEqual(len(declared.relations), 1)
        self.assertEqual(declared.relations[0].parent_key, ACCOUNT)
        self.assertEqual(declared.relations[0].child_key, "account_ref")
        self.assertEqual(declared.relations[0].mode, "local_join")

    def test_invalid_relation_declarations_are_rejected(self):
        cases = []
        doc = relation_doc()
        doc["relations"][0]["mode"] = "guess"
        cases.append(("CFG_RELATION_INVALID", doc))
        doc = relation_doc()
        doc["relations"][0]["parent_table"] = "missing_table"
        cases.append(("CFG_RELATION_UNKNOWN_TABLE", doc))
        doc = relation_doc()
        doc["relations"].append(copy.deepcopy(doc["relations"][0]))
        cases.append(("CFG_RELATION_DUPLICATE", doc))
        doc = relation_doc()
        doc["relations"][0]["child_key"] = "row_id"
        cases.append(("CFG_RELATION_INVALID", doc))
        for expected, document in cases:
            _, issues, _ = validate_document(document)
            self.assertIn(expected, {issue.code for issue in issues})

    def test_no_contract_keeps_single_table_report(self):
        topology = topology_from(base_doc())
        accounts = [row("accounts", "acc_1", {ACCOUNT: "ACC_1", REGION: "region_alpha"})]
        orders = [row("orders", "ord_1", {REGION: "region_alpha", "account_ref": "ACC_1"})]
        scan = scan_for(accounts, orders, [], [])
        self.assertIsNone(evaluate_relations(topology, scan))
        audit = run_audit(topology, scan)
        self.assertNotIn("relations", audit)


class LocalJoinTests(unittest.TestCase):
    def test_same_shard_parent_and_child_satisfies(self):
        topology, scan = scenario()
        result = evaluate_relations(topology, scan)
        evaluation = result.report["evaluations"][0]
        self.assertEqual(evaluation["old"]["status"], "satisfied")
        self.assertEqual(evaluation["old"]["codes"], [])
        self.assertEqual(evaluation["old"]["child_routed_shard"], "shard_a")
        self.assertEqual(evaluation["old"]["parent_routed_shard"], "shard_a")
        self.assertEqual(evaluation["old"]["parent_business_fingerprint"],
                         business_fingerprint(topology and scan.logical_rows["accounts"][0].values))
        self.assertEqual(result.report["summary"]["old_status"], "pass")

    def test_parent_in_other_shard_blocks(self):
        topology, scan = scenario(account_region="region_beta", account_shards=["shard_b"])
        codes = evaluate_relations(topology, scan).report["evaluations"][0]["old"]["codes"]
        self.assertIn(RELATION_SHARD_SPLIT, codes)

    def test_missing_parent_row_blocks(self):
        topology, scan = scenario()
        bad_orders = [row("orders", "ord_1", {REGION: "region_alpha", "account_ref": "ABSENT"})]
        scan.logical_rows["orders"] = bad_orders
        result = evaluate_relations(topology, scan)
        codes = result.report["evaluations"][0]["old"]["codes"]
        self.assertIn(RELATION_PARENT_MISSING, codes)
        self.assertEqual(
            [finding.code for finding in result.findings], [RELATION_PARENT_MISSING]
        )

    def test_duplicate_parent_identity_blocks(self):
        topology, scan = scenario(
            accounts=[
                row("accounts", "acc_1", {ACCOUNT: "ACC_1", REGION: "region_alpha", "name": "Acme"}),
                row("accounts", "acc_2", {ACCOUNT: "ACC_1", REGION: "region_alpha", "name": "Acme"}),
            ]
        )
        codes = evaluate_relations(topology, scan).report["evaluations"][0]["old"]["codes"]
        self.assertIn(RELATION_PARENT_DUPLICATE, codes)

    def test_missing_reference_value_blocks(self):
        topology, scan = scenario()
        scan.logical_rows["orders"] = [row("orders", "ord_1", {REGION: "region_alpha", "account_ref": ""})]
        codes = evaluate_relations(topology, scan).report["evaluations"][0]["old"]["codes"]
        self.assertIn(RELATION_REFERENCE_MISSING, codes)

    def test_parent_key_missing_is_reported(self):
        topology, scan = scenario()
        scan.logical_rows["accounts"] = [row("accounts", "acc_1", {REGION: "region_alpha", "name": "Acme"})]
        result = evaluate_relations(topology, scan)
        self.assertIn(
            "RELATION_PARENT_KEY_MISSING", {finding.code for finding in result.findings}
        )

    def test_child_absent_from_physical_tree_blocks(self):
        topology, scan = scenario()
        scan.physical_rows["orders"] = []
        codes = evaluate_relations(topology, scan).report["evaluations"][0]["old"]["codes"]
        self.assertIn(RELATION_CHILD_COPY_MISSING, codes)

    def test_same_key_but_different_content_is_not_the_parent(self):
        topology, scan = scenario(
            extra_account_rows=[
                place(
                    "accounts", "shard_a", "acc_stale",
                    {ACCOUNT: "ACC_1", REGION: "region_alpha", "name": "Different Entity"},
                    line=9,
                )
            ]
        )
        codes = evaluate_relations(topology, scan).report["evaluations"][0]["old"]["codes"]
        self.assertIn(RELATION_PARENT_CONFLICT, codes)

    def test_target_split_blocks_local_join(self):
        topology, scan = scenario()
        new_doc = relation_doc()
        accounts_table = next(item for item in new_doc["tables"] if item["table"] == "accounts")
        accounts_table["list_map"] = {"region_alpha": "shard_b", "region_beta": "shard_b"}
        new = topology_from(new_doc)
        result = evaluate_relations(topology, scan, target=new)
        codes = result.report["evaluations"][0]["new"]["codes"]
        self.assertIn(RELATION_TARGET_SPLIT, codes)
        self.assertEqual(result.report["evaluations"][0]["new"]["child_target_shard"], "shard_a")
        self.assertEqual(result.report["evaluations"][0]["new"]["parent_target_shard"], "shard_b")

    def test_target_split_blocks_plan_but_same_shard_does_not(self):
        topology, scan = scenario()
        legacy_audit = run_audit(topology, scan)
        legacy_plan = build_plan(
            topology, topology, scan.logical_rows, legacy_audit, scan=scan,
        )
        self.assertTrue(legacy_plan["safe_to_execute"])

        new_doc = relation_doc()
        next(item for item in new_doc["tables"] if item["table"] == "accounts")["list_map"] = {
            "region_alpha": "shard_b", "region_beta": "shard_b",
        }
        new = topology_from(new_doc)
        relation = evaluate_relations(topology, scan, target=new)
        audit = run_audit(topology, scan, relation=relation)
        plan = build_plan(topology, new, scan.logical_rows, audit, scan=scan, relation=relation)
        self.assertFalse(plan["safe_to_execute"])
        codes = {item["code"] for item in plan["blockers"]}
        self.assertIn("PLAN_RELATION_NEW_ORDERS_ACCOUNTS_RELATION_TARGET_SPLIT", codes)
        self.assertEqual(plan["relations"]["summary"]["new_blocked"], 1)


class ReplicatedParentTests(unittest.TestCase):
    def replicated_scenario(self, *, extra_shards=None, drifted=None, evidence=True):
        accounts = [row("accounts", "acc_1", {ACCOUNT: "ACC_1", REGION: "region_alpha", "name": "Acme"})]
        orders = [
            row("orders", "ord_1", {REGION: "region_alpha", "account_ref": "ACC_1"}),
            row("orders", "ord_2", {REGION: "region_beta", "account_ref": "ACC_1"}),
        ]
        account_physical = [
            place("accounts", "shard_a", "acc_1", {ACCOUNT: "ACC_1", REGION: "region_alpha", "name": "Acme"}),
            place("accounts", "shard_b", "acc_1", {ACCOUNT: "ACC_1", REGION: "region_alpha", "name": "Acme"}),
        ]
        for shard, payload in (extra_shards or []):
            account_physical.append(place("accounts", shard, "acc_1", payload, line=7))
        for item in drifted or []:
            account_physical.append(item)
        order_physical = [
            place("orders", "shard_a", "ord_1", {REGION: "region_alpha", "account_ref": "ACC_1"}),
            place("orders", "shard_b", "ord_2", {REGION: "region_beta", "account_ref": "ACC_1"}),
        ]
        scan = scan_for(accounts, orders, account_physical, order_physical, manifests=evidence)
        return topology_from(relation_doc("replicated_parent")), scan

    def test_identical_replica_in_each_child_shard_satisfies(self):
        topology, scan = self.replicated_scenario()
        result = evaluate_relations(topology, scan)
        self.assertEqual(result.report["summary"]["old_status"], "pass")
        for evaluation in result.report["evaluations"]:
            self.assertEqual(evaluation["old"]["status"], "satisfied")
            for replica in evaluation["old"]["replicas"]:
                self.assertTrue(replica["matches_logical_parent"])
                self.assertTrue(replica["manifest_evidence"]["ok"])
        self.assertIn(("accounts", "acc_1", "shard_b"), result.allowed_replicas)

    def test_missing_replica_in_child_shard_blocks(self):
        topology, scan = self.replicated_scenario()
        scan.physical_rows["accounts"] = [
            item for item in scan.physical_rows["accounts"] if item.shard != "shard_b"
        ]
        codes = evaluate_relations(topology, scan).report["evaluations"][1]["old"]["codes"]
        self.assertIn(RELATION_PARENT_REPLICA_MISSING, codes)

    def test_divergent_replica_content_blocks(self):
        topology, scan = self.replicated_scenario(
            drifted=[
                place(
                    "accounts", "shard_b", "acc_1",
                    {ACCOUNT: "ACC_1", REGION: "region_alpha", "name": "Acme stale"},
                    line=8,
                )
            ]
        )
        result = evaluate_relations(topology, scan)
        codes = result.report["evaluations"][1]["old"]["codes"]
        self.assertIn(RELATION_PARENT_REPLICA_DRIFT, codes)
        drift = next(
            finding for finding in result.findings
            if finding.code == RELATION_PARENT_REPLICA_DRIFT
        )
        fingerprints = {
            item["business_fingerprint"] for item in drift.detail["replicas"]
        }
        self.assertIn(drift.detail["canonical_fingerprint"], {
            item["business_fingerprint"]
            for item in result.report["evaluations"][1]["old"]["replicas"]
        })
        self.assertEqual(len(fingerprints), 1)

    def test_missing_manifest_evidence_blocks(self):
        topology, scan = self.replicated_scenario(evidence=False)
        result = evaluate_relations(topology, scan)
        codes = result.report["evaluations"][0]["old"]["codes"]
        self.assertIn(RELATION_MANIFEST_EVIDENCE_MISSING, codes)
        evidence = next(
            finding for finding in result.findings
            if finding.code == RELATION_MANIFEST_EVIDENCE_MISSING
        ).detail["replicas"][0]["manifest_evidence"]
        self.assertEqual(evidence["reason"], "manifest_missing")

    def test_same_name_different_content_is_not_same_entity(self):
        # Same row_id, different payload: a drifted replica, not the parent.
        drifted_topology, drifted_scan = self.replicated_scenario(
            extra_shards=[
                ("shard_a", {ACCOUNT: "ACC_1", REGION: "region_alpha", "name": "Impostor"})
            ]
        )
        drifted_codes = evaluate_relations(drifted_topology, drifted_scan).report[
            "evaluations"
        ][0]["old"]["codes"]
        self.assertIn(RELATION_PARENT_REPLICA_DRIFT, drifted_codes)

        # Different row_id under the same key: a foreign entity, a conflict.
        foreign_topology, foreign_scan = self.replicated_scenario(
            extra_shards=[
                ("shard_a", {ACCOUNT: "ACC_1", REGION: "region_alpha", "name": "Impostor"})
            ]
        )
        foreign_scan.physical_rows["accounts"] = [
            item for item in foreign_scan.physical_rows["accounts"]
            if not (item.shard == "shard_a" and item.row_id == "acc_1")
        ]
        foreign_scan.physical_rows["accounts"].append(
            place(
                "accounts", "shard_a", "acc_impostor",
                {ACCOUNT: "ACC_1", REGION: "region_alpha", "name": "Acme"},
                line=11,
            )
        )
        foreign_codes = evaluate_relations(foreign_topology, foreign_scan).report[
            "evaluations"
        ][0]["old"]["codes"]
        self.assertIn(RELATION_PARENT_CONFLICT, foreign_codes)

    def test_target_move_without_parent_replica_blocks(self):
        topology, scan = self.replicated_scenario()
        new_doc = relation_doc("replicated_parent")
        next(item for item in new_doc["tables"] if item["table"] == "orders")["list_map"] = {
            "region_alpha": "shard_c", "region_beta": "shard_c",
        }
        new = topology_from(new_doc)
        result = evaluate_relations(topology, scan, target=new)
        codes = result.report["evaluations"][0]["new"]["codes"]
        self.assertIn(RELATION_TARGET_PARENT_MISSING, codes)

    def test_target_move_into_divergent_replica_blocks(self):
        topology, scan = self.replicated_scenario()
        scan.logical_rows["orders"] = [
            row("orders", "ord_1", {REGION: "region_alpha", "account_ref": "ACC_1"}),
        ]
        scan.physical_rows["orders"] = [
            place("orders", "shard_a", "ord_1", {REGION: "region_alpha", "account_ref": "ACC_1"}),
        ]
        scan.physical_rows["accounts"].append(
            place(
                "accounts", "shard_c", "acc_1",
                {ACCOUNT: "ACC_1", REGION: "region_alpha", "name": "Stale"},
                line=9,
            )
        )
        # add the manifest and scanned file entry for shard_c so evidence is not
        # the reason the check fails
        scan.files.append(
            ScannedFile("accounts", "shard_c", "part-000.csv", "accounts/shard_c/part-000.csv", "c" * 64, 1)
        )
        scan.manifests[("accounts", "shard_c")] = ManifestRecord(
            "accounts", "shard_c", "accounts/shard_c/_manifest.json", 1,
            (ManifestEntry("part-000.csv", 1, "c" * 64),), "2026-01-02T00:00:00Z",
        )
        new_doc = relation_doc("replicated_parent")
        next(item for item in new_doc["tables"] if item["table"] == "orders")["list_map"] = {
            "region_alpha": "shard_c", "region_beta": "shard_c",
        }
        new = topology_from(new_doc)
        codes = evaluate_relations(topology, scan, target=new).report["evaluations"][0]["new"]["codes"]
        self.assertIn(RELATION_TARGET_PARENT_DRIFT, codes)

    def test_replicated_parent_allows_migration_local_join_blocks_it(self):
        """Same child, parent and two topologies: only evidence permits the move.

        Old topology keeps the child in shard_a next to the parent.  The target
        topology moves the child to shard_b while the parent stays in shard_a.
        Under ``local_join`` that is a hard split.  Under ``replicated_parent``
        the move is allowed only because shard_b already holds a content
        identical, manifest-backed copy of the parent.
        """
        accounts = [row("accounts", "acc_1", {ACCOUNT: "ACC_1", REGION: "region_alpha", "name": "Acme"})]
        orders = [row("orders", "ord_1", {REGION: "region_alpha", "account_ref": "ACC_1"})]
        order_physical = [place("orders", "shard_a", "ord_1", {REGION: "region_alpha", "account_ref": "ACC_1"})]
        canonical_parent = {ACCOUNT: "ACC_1", REGION: "region_alpha", "name": "Acme"}

        def new_doc(mode):
            document = relation_doc(mode)
            # The target moves the child from shard_a to shard_b; the parent
            # keeps routing to shard_a.
            next(item for item in document["tables"] if item["table"] == "orders")["list_map"] = {
                "region_alpha": "shard_b", "region_beta": "shard_b",
            }
            return topology_from(document)

        # local_join: the parent lives only in shard_a, so the target split is
        # the sole blocking relation verdict.
        local_parents = [place("accounts", "shard_a", "acc_1", canonical_parent)]
        local_scan = scan_for(accounts, orders, local_parents, order_physical)
        local_top = topology_from(relation_doc("local_join"))
        local_new = new_doc("local_join")
        local_relation = evaluate_relations(local_top, local_scan, target=local_new)
        local_audit = run_audit(local_top, local_scan, relation=local_relation)
        local_plan = build_plan(
            local_top, local_new, local_scan.logical_rows, local_audit,
            scan=local_scan, relation=local_relation,
        )

        # replicated_parent: shard_b holds an identical, manifest-backed copy,
        # so the same move has evidence to proceed.
        rep_parents = [
            place("accounts", "shard_a", "acc_1", canonical_parent),
            place("accounts", "shard_b", "acc_1", canonical_parent, line=3),
        ]
        rep_scan = scan_for(accounts, orders, rep_parents, order_physical)
        rep_top = topology_from(relation_doc("replicated_parent"))
        rep_new = new_doc("replicated_parent")
        rep_relation = evaluate_relations(rep_top, rep_scan, target=rep_new)
        rep_audit = run_audit(rep_top, rep_scan, relation=rep_relation)
        rep_plan = build_plan(
            rep_top, rep_new, rep_scan.logical_rows, rep_audit,
            scan=rep_scan, relation=rep_relation,
        )

        self.assertFalse(local_plan["safe_to_execute"])
        self.assertIn(
            "PLAN_RELATION_NEW_ORDERS_ACCOUNTS_RELATION_TARGET_SPLIT",
            {item["code"] for item in local_plan["blockers"]},
        )
        self.assertNotIn(
            "PLAN_AUDIT_DUPLICATE_ROW",
            {item["code"] for item in local_plan["blockers"]},
        )
        self.assertEqual(rep_relation.report["summary"]["new_status"], "pass")
        self.assertTrue(rep_plan["safe_to_execute"])
        self.assertIn(("accounts", "acc_1", "shard_b"), rep_relation.allowed_replicas)


class RelationReportAndOrderingTests(unittest.TestCase):
    def test_many_children_same_parent_are_order_independent(self):
        topology, scan = scenario()
        orders = [
            row("orders", f"ord_{index:02d}", {REGION: "region_alpha", "account_ref": "ACC_1"})
            for index in range(6)
        ]
        order_physical = [
            place("orders", "shard_a", f"ord_{index:02d}", {REGION: "region_alpha", "account_ref": "ACC_1"}, line=index + 2)
            for index in range(6)
        ]
        scan.logical_rows["orders"] = orders
        scan.physical_rows["orders"] = order_physical
        forward = evaluate_relations(topology, scan)
        scan.logical_rows["orders"] = list(reversed(orders))
        scan.physical_rows["orders"] = list(reversed(order_physical))
        backward = evaluate_relations(topology, scan)
        self.assertEqual(forward.report, backward.report)
        self.assertEqual(
            [finding.finding_id for finding in forward.findings],
            [finding.finding_id for finding in backward.findings],
        )
        parent_ids = {item["parent_row_id"] for item in forward.report["evaluations"]}
        self.assertEqual(parent_ids, {"acc_1"})

    def test_plan_blockers_match_audit_relation_report(self):
        topology, scan = scenario(account_region="region_beta", account_shards=["shard_b"])
        new_doc = relation_doc()
        next(item for item in new_doc["tables"] if item["table"] == "accounts")["list_map"] = {
            "region_alpha": "shard_a", "region_beta": "shard_a",
        }
        new = topology_from(new_doc)
        relation = evaluate_relations(topology, scan, target=new)
        audit = run_audit(topology, scan, relation=relation)
        plan = build_plan(topology, new, scan.logical_rows, audit, scan=scan, relation=relation)
        self.assertFalse(plan["safe_to_execute"])
        relation_blockers = [
            item for item in plan["blockers"] if item["code"].startswith("PLAN_RELATION_")
        ]
        self.assertTrue(relation_blockers)
        for blocker in relation_blockers:
            self.assertEqual(blocker["severity"], "blocking")
            self.assertEqual(blocker["relation"], "orders_accounts")
            self.assertIn(blocker["phase"], {"old", "new"})
            self.assertGreaterEqual(blocker["count"], 1)
            self.assertIn("child_row_ids", blocker["evidence"])
            self.assertLessEqual(len(blocker["evidence"]["child_row_ids"]), 20)
        audit_codes = {finding["code"] for finding in audit["findings"]}
        for blocker in relation_blockers:
            suffix = blocker["code"].rsplit("RELATION_", 1)[-1]
            self.assertIn(f"RELATION_{suffix}", audit_codes)

    def test_audit_report_carries_relation_evidence_for_both_topologies(self):
        topology, scan = scenario(account_region="region_beta", account_shards=["shard_b"])
        new_doc = relation_doc()
        next(item for item in new_doc["tables"] if item["table"] == "accounts")["list_map"] = {
            "region_alpha": "shard_a", "region_beta": "shard_a",
        }
        relation = evaluate_relations(topology, scan, target=topology_from(new_doc))
        audit = run_audit(topology, scan, relation=relation)
        self.assertIn("relations", audit)
        relations = audit["relations"]
        self.assertEqual(relations["declared"][0]["mode"], "local_join")
        self.assertEqual(relations["summary"]["target_contracts"], 1)
        for evaluation in relations["evaluations"]:
            self.assertIn("old", evaluation)
            self.assertIn("new", evaluation)
            self.assertEqual(evaluation["old"]["parent_row_id"], "acc_1")
            self.assertIn("child_routed_shard", evaluation["old"])
            self.assertIn("child_target_shard", evaluation["new"])

    def test_repeated_planted_duplicate_replicas_do_not_crash_placement_audit(self):
        topology, scan = scenario()
        sibling = place(
            "accounts", "shard_a", "acc_1",
            {ACCOUNT: "ACC_1", REGION: "region_alpha", "name": "Acme"},
            line=9,
        )
        scan.physical_rows["accounts"].append(sibling)
        audit = run_audit(topology, scan)
        self.assertIn(
            "DUPLICATE_ROW", {finding["code"] for finding in audit["findings"]}
        )

    def test_parent_key_missing_becomes_plan_blocker(self):
        topology, scan = scenario()
        scan.logical_rows["accounts"] = [
            row("accounts", "acc_1", {REGION: "region_alpha", "name": "Acme"})
        ]
        relation = evaluate_relations(topology, scan)
        audit = run_audit(topology, scan, relation=relation)
        plan = build_plan(topology, topology, scan.logical_rows, audit, scan=scan, relation=relation)
        blocker = next(
            item for item in plan["blockers"]
            if item["code"].endswith("RELATION_PARENT_KEY_MISSING")
        )
        self.assertEqual(blocker["severity"], "blocking")
        self.assertEqual(blocker["evidence"]["parent_row_ids"], ["acc_1"])
        self.assertFalse(plan["safe_to_execute"])


class CommittedFixtureTests(unittest.TestCase):
    """Reproduce the contract checks from committed CSV/JSONL + manifests."""

    def setUp(self):
        from pathlib import Path
        from shardroute.config import load_topology
        from shardroute.scanner import scan_workspace
        from support import FIXTURES

        self.root = Path(FIXTURES) / "relations"
        self.old = load_topology(self.root / "config" / "topology_old.json")
        self.new = load_topology(self.root / "config" / "topology_new.json")
        self.scan = scan_workspace(
            self.root / "logical", self.root / "shards", self.old.table_names()
        )

    def test_fixture_covers_every_required_failure_class(self):
        relation = evaluate_relations(self.old, self.scan, target=self.new)
        codes = {finding.code for finding in relation.findings}
        self.assertEqual(len(relation.report["declared"]), 2)
        self.assertEqual(
            {item["mode"] for item in relation.report["declared"]},
            {"local_join", "replicated_parent"},
        )
        self.assertIn(RELATION_REFERENCE_MISSING, codes)
        self.assertIn(RELATION_PARENT_MISSING, codes)
        self.assertIn(RELATION_PARENT_DUPLICATE, codes)
        self.assertIn(RELATION_PARENT_REPLICA_DRIFT, codes)
        self.assertIn(RELATION_TARGET_PARENT_MISSING, codes)
        self.assertIn(RELATION_TARGET_SPLIT, codes)

    def test_fixture_positive_paths_are_satisfied(self):
        relation = evaluate_relations(self.old, self.scan, target=self.new)
        by_child = {
            (item["relation"], item["child_row_id"]): item
            for item in relation.report["evaluations"]
        }
        self.assertEqual(by_child[("items_catalogs", "it_alpha")]["old"]["status"], "satisfied")
        self.assertEqual(by_child[("items_catalogs", "it_peer")]["old"]["status"], "satisfied")
        self.assertEqual(by_child[("items_catalogs", "it_replicated")]["old"]["status"], "satisfied")
        self.assertEqual(by_child[("items_catalogs", "it_replicated")]["new"]["status"], "satisfied")
        self.assertEqual(by_child[("orders_accounts", "ord_ok")]["old"]["status"], "satisfied")
        self.assertEqual(by_child[("orders_accounts", "ord_repeat_1")]["old"]["status"], "satisfied")
        self.assertEqual(by_child[("orders_accounts", "ord_repeat_2")]["old"]["status"], "satisfied")

    def test_fixture_plan_blockers_trace_to_report_rows(self):
        relation = evaluate_relations(self.old, self.scan, target=self.new)
        audit = run_audit(self.old, self.scan, relation=relation)
        plan = build_plan(
            self.old, self.new, self.scan.logical_rows, audit,
            scan=self.scan, relation=relation,
        )
        self.assertFalse(plan["safe_to_execute"])
        evaluations = relation.report["evaluations"]
        for blocker in plan["blockers"]:
            if not blocker["code"].startswith("PLAN_RELATION_"):
                continue
            self.assertEqual(blocker["severity"], "blocking")
            matching = [
                item for item in evaluations
                if item["relation"] == blocker["relation"]
                and blocker["relation_code"] in item[blocker["phase"]]["codes"]
            ]
            self.assertTrue(matching, blocker)
            self.assertEqual(blocker["count"], len(matching))
            self.assertLessEqual(
                set(blocker["evidence"]["child_row_ids"]),
                {item["child_row_id"] for item in matching},
            )

    def test_fixture_report_contains_relation_section(self):
        from shardroute.report import build_report, render_markdown

        relation = evaluate_relations(self.old, self.scan, target=self.new)
        audit = run_audit(self.old, self.scan, relation=relation)
        plan = build_plan(
            self.old, self.new, self.scan.logical_rows, audit,
            scan=self.scan, relation=relation,
        )
        report = build_report(
            old=self.old, new=self.new, seed=20260102,
            logical_fingerprint=self.scan.logical_fingerprint,
            physical_fingerprint=self.scan.physical_fingerprint,
            audit=audit, plan=plan, queries=[],
        )
        self.assertEqual(report["summary"]["relation_contracts"], 2)
        self.assertGreater(report["summary"]["relation_blockers"], 0)
        self.assertEqual(report["audit"]["relations"], report["plan"]["relations"])
        markdown = render_markdown(report)
        self.assertIn("## Declared relation verification", markdown)
        self.assertIn("items_catalogs", markdown)
        self.assertIn("replicated_parent", markdown)

    def test_fixture_evaluation_is_input_order_independent(self):
        import copy as _copy
        import random

        forward = evaluate_relations(self.old, self.scan, target=self.new)
        shuffled = _copy.deepcopy(self.scan)
        rng = random.Random(7)
        for rows in shuffled.logical_rows.values():
            rng.shuffle(rows)
        for rows in shuffled.physical_rows.values():
            rng.shuffle(rows)
        backward = evaluate_relations(self.old, shuffled, target=self.new)
        self.assertEqual(forward.report, backward.report)
        self.assertEqual(
            [finding.finding_id for finding in forward.findings],
            [finding.finding_id for finding in backward.findings],
        )


if __name__ == "__main__":
    unittest.main()
