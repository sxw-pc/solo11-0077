#!/usr/bin/env python3
"""Inject each publication cut point into a synthetic workspace and restart recovery.

The harness never migrates shards or touches an external database: it only
mutates synthetic fixture files, runs the offline precheck pipeline in a
subprocess (simulating a real crash/restart), then reconciles the workspace
through the same public CLI/publish entry points an operator would use.

Usage:
    python3 scripts/fault_injection_demo.py [--output-dir DIR]
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shardroute.demo import run_demo, run_pipeline  # noqa: E402
from shardroute.ledger import Ledger  # noqa: E402
from shardroute.publish import (  # noqa: E402
    FAULT_LEDGER_NOT_COMMITTED,
    FAULT_PARTIAL_REPORTS,
    FAULT_POINTER_NOT_SWITCHED,
    POINTER_NAME,
    resolve_current,
    rollback_candidate,
    roll_forward_candidate,
)

FAULT_ENV = "SHARDROUTE_FAULT"


def mutate_one_shard(root: Path) -> None:
    target = next((root / "shards").rglob("part-000.csv"))
    target.write_bytes(
        target.read_bytes() + b"ord_demo_9999,tenant_demo_001,1.00,web_demo,injected mutation\n"
    )
    manifest = target.parent / "_manifest.json"
    doc = json.loads(manifest.read_text(encoding="utf-8"))
    doc["row_count"] += 1
    manifest.write_text(
        json.dumps(doc, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )


def crash_pipeline(workspace: Path, fault: str) -> dict[str, Any]:
    """Run the pipeline in a subprocess with one fault armed; expect exit 1."""
    env = dict(os.environ)
    env[FAULT_ENV] = json.dumps({fault: 1})
    code = (
        "import sys;"
        f"sys.path.insert(0, {str(ROOT)!r});"
        "import json;"
        "from shardroute.cli import main;"
        f"sys.exit(main(['report', '--workspace', {str(workspace)!r}]))"
    )
    completed = subprocess.run(
        [sys.executable, "-I", "-c", code],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    try:
        error = json.loads(completed.stderr)
    except ValueError:
        raise AssertionError(
            f"subprocess did not emit a structured error\nstdout={completed.stdout}\nstderr={completed.stderr}"
        )
    return {"exit_code": completed.returncode, "error_code": error["code"], "fault": fault}


def current_facts(workspace: Path) -> dict[str, Any]:
    """The four observable surfaces, reduced to run_id + safe_to_execute."""
    with Ledger(workspace / "ledger.sqlite3", readonly=True) as ledger:
        current = resolve_current(workspace, ledger)
        run = ledger.get_run(current["run_id"])
        events = ledger.list_recovery_events()
        publications = [
            (item["run_id"], item["state"]) for item in ledger.list_publications()
        ]
    pointer = json.loads((workspace / "reports" / POINTER_NAME).read_text())
    disk = json.loads((workspace / "reports" / "report.json").read_text())
    return {
        "resolver_run_id": current["run_id"],
        "resolver_safe_to_execute": current["safe_to_execute"],
        "ledger_run_id": run["run_id"],
        "ledger_safe_to_execute": run["summary"]["safe_to_execute"],
        "pointer_run_id": pointer["run_id"],
        "pointer_safe_to_execute": pointer["safe_to_execute"],
        "disk_report_run_id": disk["run_id"],
        "disk_report_safe_to_execute": disk["summary"]["safe_to_execute"],
        "publications": publications,
        "recovery_events": [
            {"run_id": event["run_id"], "type": event["event_type"],
             "decision": event["decision"], "reason": event["reason"]}
            for event in events
        ],
    }


def assert_aligned(facts: dict[str, Any], expected_run: str) -> None:
    run_ids = {
        facts["resolver_run_id"], facts["ledger_run_id"],
        facts["pointer_run_id"], facts["disk_report_run_id"],
    }
    safes = {
        facts["resolver_safe_to_execute"], facts["ledger_safe_to_execute"],
        facts["pointer_safe_to_execute"], facts["disk_report_safe_to_execute"],
    }
    assert run_ids == {expected_run}, f"mixed run ids: {run_ids}"
    assert len(safes) == 1, f"mixed safe_to_execute: {safes}"


def scenario(workspace: Path, fault: str, restart: str) -> dict[str, Any]:
    run_a = current_facts(workspace)["resolver_run_id"]
    crash = crash_pipeline(workspace, fault)
    assert crash["exit_code"] == 1 and crash["error_code"] == "WORKSPACE_INVALID"

    # Between crash and recovery every read surface must still show complete A.
    if fault in {FAULT_PARTIAL_REPORTS, FAULT_LEDGER_NOT_COMMITTED}:
        assert_aligned(current_facts(workspace), run_a)

    if fault == FAULT_POINTER_NOT_SWITCHED and restart == "rollback":
        with Ledger(workspace / "ledger.sqlite3") as ledger:
            run_b = ledger.list_publications("staged")[0]["run_id"]
            rollback_candidate(workspace, ledger, run_b)
        expected = run_a
        # After rollback the inputs still describe snapshot B, so the
        # operator must be able to keep reviewing complete A: a plain
        # status read is stable and B's evidence remains only in the ledger.
        assert_aligned(current_facts(workspace), expected)
        with Ledger(workspace / "ledger.sqlite3", readonly=True) as ledger:
            assert ledger.get_run(run_b) is not None
            assert ledger.get_current_run_id() == run_a
        rerun_summary = None
    elif fault == FAULT_POINTER_NOT_SWITCHED and restart == "roll-forward":
        with Ledger(workspace / "ledger.sqlite3") as ledger:
            run_b = ledger.list_publications("staged")[0]["run_id"]
            roll_forward_candidate(workspace, ledger, run_b)
        expected = run_b
        # Same snapshot rerun: idempotent, current pointer must not move.
        rerun_summary = run_pipeline(workspace)
        assert rerun_summary["published"] is False
        assert_aligned(current_facts(workspace), expected)
    else:
        # Restart the pipeline; staged leftovers are reconciled automatically
        # and the fresh build becomes the current version.
        run_pipeline(workspace)
        expected = current_facts(workspace)["resolver_run_id"]
        assert_aligned(current_facts(workspace), expected)
        # Same snapshot rerun: no new version, no mixing.
        rerun_summary = run_pipeline(workspace)
        assert rerun_summary["published"] is False
        assert_aligned(current_facts(workspace), expected)

    facts = current_facts(workspace)
    assert_aligned(facts, expected)
    return {"fault": fault, "restart": restart, "crash": crash,
            "expected_run_id": expected, "after_recovery": facts}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default="/tmp/shardroute-fault-demo")
    args = parser.parse_args()
    base = Path(args.output_dir)
    if base.exists():
        shutil.rmtree(base)
    base.mkdir(parents=True)

    cases = [
        (FAULT_PARTIAL_REPORTS, "auto-discard-and-rebuild"),
        (FAULT_LEDGER_NOT_COMMITTED, "auto-discard-and-rebuild"),
        (FAULT_POINTER_NOT_SWITCHED, "roll-forward"),
        (FAULT_POINTER_NOT_SWITCHED, "rollback"),
    ]
    results = []
    for index, (fault, restart) in enumerate(cases, start=1):
        workspace = base / f"case-{index}-{fault}-{restart}"
        run_demo(workspace)  # published, readable version A
        mutate_one_shard(workspace)  # logical/physical change -> snapshot B
        results.append(scenario(workspace, fault, restart))

    print(json.dumps(results, sort_keys=True, indent=2, ensure_ascii=False))
    print("\nAll four surfaces aligned in every case:")
    for item in results:
        facts = item["after_recovery"]
        print(
            f"- {item['fault']:28s} {item['restart']:28s} -> "
            f"current={facts['resolver_run_id']} "
            f"safe_to_execute={facts['resolver_safe_to_execute']} "
            f"events={len(facts['recovery_events'])}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
