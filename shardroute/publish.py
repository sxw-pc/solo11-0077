"""Recoverable, atomic publication of a precheck run.

A run becomes the *current* precheck only after a two-phase commit across
the filesystem and the SQLite ledger:

1. The three reports and a manifest are rendered into
   ``reports/runs/<run_id>/`` via an atomic staging rename.  Previously
   published versions are never modified or deleted.
2. The ledger transaction writes findings/moves/queries/artifacts together
   with a ``publications`` row in state ``staged``.
3. Every artifact hash is cross-checked between files, manifest and ledger.
4. ``reports/current.json`` is swapped atomically and the top-level report
   files are refreshed from the version directory.
5. The ledger flips the publication to ``current`` (exactly one row).

Interruptions leave one of three cut points:

* partial reports on disk,
* a complete version directory without a committed ledger publication,
* everything persisted but the current pointer not yet switched.

Only the third cut point may be rolled forward.  The others are quarantined
and discarded; the previous current version stays authoritative.  Every
recovery decision is written to ``recovery_events`` and can be queried.
"""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path
from typing import Any

from .canonical import file_sha256, sha256_bytes, sha256_text, stable_id
from .errors import WORKSPACE_INVALID, ShardRouteError
from .ledger import (
    PUBLICATION_ARCHIVED,
    PUBLICATION_CURRENT,
    PUBLICATION_STAGED,
    Ledger,
)
from .report import (
    MANIFEST_NAME,
    REPORT_FILES,
    RUNS_DIRNAME,
    STAGING_PREFIX,
    build_manifest,
    load_run_report,
    report_payloads,
    stage_run,
    verify_run_files,
)

POINTER_NAME = "current.json"
QUARANTINE_DIRNAME = "quarantine"
POINTER_FORMAT = "shardroute-current/1"

# Test-only fault injection: name of a fault point -> number of times it
# must still fire.  Consumed one unit at a time, so a value of ``1`` breaks
# exactly the next publication attempt and later reruns stay clean.
FAULT_ENV = "SHARDROUTE_FAULT"

# Cut point 1: some report files have been written, nothing is committed.
FAULT_PARTIAL_REPORTS = "partial_reports"
# Cut point 2: all report files (and the manifest) are on disk but the
# ledger transaction has not been committed.
FAULT_LEDGER_NOT_COMMITTED = "ledger_not_committed"
# Cut point 3: files and ledger are fully persisted, only the current
# pointer has not been switched.
FAULT_POINTER_NOT_SWITCHED = "pointer_not_switched"
FAULT_POINTS = frozenset(
    {FAULT_PARTIAL_REPORTS, FAULT_LEDGER_NOT_COMMITTED, FAULT_POINTER_NOT_SWITCHED}
)


class PublicationError(ShardRouteError):
    """Raised when a run cannot be verified or promoted safely."""


def _reports_root(workspace: str | Path) -> Path:
    return Path(workspace) / "reports"


def pointer_path(workspace: str | Path) -> Path:
    return _reports_root(workspace) / POINTER_NAME


def _quarantine_root(workspace: str | Path) -> Path:
    return _reports_root(workspace) / QUARANTINE_DIRNAME


def _staging_dirs(workspace: str | Path) -> list[Path]:
    root = _reports_root(workspace)
    return sorted(path for path in root.glob(f"{STAGING_PREFIX}*") if path.is_dir())


def _fault_active(name: str) -> bool:
    raw = os.environ.get(FAULT_ENV, "")
    if not raw:
        return False
    try:
        configured = json.loads(raw)
    except ValueError:
        configured = {}
    if not isinstance(configured, dict) or configured.get(name, 0) <= 0:
        return False
    remaining = int(configured[name]) - 1
    if remaining <= 0:
        configured.pop(name, None)
    else:
        configured[name] = remaining
    os.environ[FAULT_ENV] = json.dumps(configured, sort_keys=True) if configured else ""
    return True


def _atomic_write(path: Path, payload: bytes) -> None:
    """Write ``path`` via a sibling temp file and an atomic rename."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    temporary.write_bytes(payload)
    temporary.rename(path)


def _write_partial_staging(workspace: Path, report: dict[str, Any]) -> Path:
    """Simulate cut point 1: render only one report into the staging dir."""
    root = _reports_root(workspace)
    staging = root / f"{STAGING_PREFIX}{report['run_id']}"
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True, exist_ok=True)
    payloads = report_payloads(report)
    (staging / "report.html").write_bytes(payloads["report.html"])
    return staging


def _quarantine(path: Path, workspace: Path, reason: str) -> str:
    """Move an unpublished, unverifiable directory into the quarantine area."""
    if not path.exists():
        return ""
    root = _quarantine_root(workspace)
    root.mkdir(parents=True, exist_ok=True)
    digest = sha256_text(f"{path.name}:{reason}")[:12]
    destination = root / f"{path.name}-{digest}"
    if destination.exists():
        shutil.rmtree(destination)
    path.rename(destination)
    return destination.relative_to(workspace).as_posix()


def _event_id(run_id: str, event_type: str, decision: str) -> str:
    return stable_id("evt", run_id, event_type, decision, length=16)


def _record_event(
    ledger: Ledger,
    *,
    run_id: str,
    event_type: str,
    decision: str,
    reason: str,
    detail: dict[str, Any],
    created_at: str,
) -> dict[str, Any]:
    event_id = _event_id(run_id, event_type, decision)
    stored = ledger.record_recovery_event(
        event_id=event_id,
        run_id=run_id,
        event_type=event_type,
        decision=decision,
        reason=reason,
        detail=detail,
        created_at=created_at,
    )
    return {
        "event_id": event_id,
        "run_id": run_id,
        "event_type": event_type,
        "decision": decision,
        "reason": reason,
        "detail": detail,
        "created_at": created_at,
        "recorded": stored,
    }


def verify_ledger_run(
    ledger: Ledger, run_id: str, manifest: dict[str, Any], report: dict[str, Any] | None = None
) -> None:
    """Cross-check the committed ledger rows against the on-disk manifest."""
    run = ledger.get_run(run_id)
    if run is None:
        raise PublicationError(WORKSPACE_INVALID, "ledger has no rows for run", run_id)
    if run["run_id"] != manifest["run_id"]:
        raise PublicationError(WORKSPACE_INVALID, "ledger/manifest run_id mismatch", run_id)
    if bool(run["summary"]["safe_to_execute"]) != bool(manifest["safe_to_execute"]):
        raise PublicationError(
            WORKSPACE_INVALID, "ledger and manifest disagree on safe_to_execute", run_id
        )
    artifacts = ledger.get_artifacts(run_id)
    ledger_map = {item["path"]: item for item in artifacts}
    manifest_map = {item["path"]: item for item in manifest["artifacts"]}
    if set(ledger_map) != set(manifest_map):
        raise PublicationError(
            WORKSPACE_INVALID,
            "ledger/manifest artifact set mismatch",
            run_id,
            {"ledger": sorted(ledger_map), "manifest": sorted(manifest_map)},
        )
    for path, item in manifest_map.items():
        stored = ledger_map[path]
        if stored["sha256"] != item["sha256"] or stored["bytes"] != item["bytes"]:
            raise PublicationError(
                WORKSPACE_INVALID, "ledger artifact hash does not match manifest", path
            )
    if report is not None:
        # The child tables must carry exactly the report's evidence rows;
        # a half-written run would fail here and can never be promoted.
        actual = {
            "findings": len(ledger.get_findings(run_id)),
            "moves": len(ledger.get_moves(run_id)),
            "queries": len(ledger.get_queries(run_id)),
        }
        expected = {
            "findings": len(report["audit"]["findings"]),
            "moves": len(report["plan"]["move_cells"]),
            "queries": len(report["queries"]),
        }
        if actual != expected:
            raise PublicationError(
                WORKSPACE_INVALID,
                "ledger child-table row counts do not match the report",
                run_id,
                {"expected": expected, "actual": actual},
            )


def _pointer_payload(run_id: str, manifest: dict[str, Any]) -> bytes:
    pointer = {
        "format": POINTER_FORMAT,
        "run_id": run_id,
        "generated_at": manifest["generated_at"],
        "status": manifest["status"],
        "safe_to_execute": bool(manifest["safe_to_execute"]),
        "fingerprints": dict(manifest["fingerprints"]),
        "manifest": f"reports/{RUNS_DIRNAME}/{run_id}/{MANIFEST_NAME}",
        "artifacts": [
            {"path": item["path"], "sha256": item["sha256"], "bytes": item["bytes"]}
            for item in sorted(manifest["artifacts"], key=lambda item: item["path"])
        ],
    }
    return (json.dumps(pointer, sort_keys=True, indent=2, ensure_ascii=False) + "\n").encode("utf-8")


def _refresh_top_level_reports(workspace: Path, run_id: str) -> None:
    """Copy the current version's reports to the stable top-level paths."""
    source = workspace / "reports" / RUNS_DIRNAME / run_id
    for name in REPORT_FILES:
        _atomic_write(workspace / "reports" / name, (source / name).read_bytes())


def _promote(workspace: Path, ledger: Ledger, run_id: str, manifest: dict[str, Any]) -> None:
    """Switch the workspace pointer and then the ledger current marker."""
    _atomic_write(pointer_path(workspace), _pointer_payload(run_id, manifest))
    _refresh_top_level_reports(workspace, run_id)
    ledger.mark_publication_current(run_id, manifest)


def publish_run(workspace: str | Path, report: dict[str, Any], ledger: Ledger) -> dict[str, Any]:
    """Publish one report through the recoverable two-phase protocol."""
    root = Path(workspace)
    run_id = report["run_id"]
    recovery = recover_interrupted(workspace, ledger)

    existing = ledger.get_publication(run_id)
    if existing is not None and existing["state"] == PUBLICATION_CURRENT:
        # Idempotent rerun of the exact current snapshot: re-verify and stop.
        manifest = verify_run_files(root, run_id)
        verify_ledger_run(ledger, run_id, manifest, report)
        return {
            "run_id": run_id,
            "published": False,
            "reason": "already_current",
            "recovery": recovery,
            "manifest": manifest,
        }

    if _fault_active(FAULT_PARTIAL_REPORTS):
        staging = _write_partial_staging(root, report)
        raise PublicationError(
            WORKSPACE_INVALID,
            "injected failure while writing report files",
            str(staging),
            {"fault": FAULT_PARTIAL_REPORTS},
        )

    staged = stage_run(report, root)
    manifest = staged["manifest"]
    artifacts = staged["artifacts"]

    if _fault_active(FAULT_LEDGER_NOT_COMMITTED):
        raise PublicationError(
            WORKSPACE_INVALID,
            "injected failure before the ledger publication committed",
            str(root),
            {"fault": FAULT_LEDGER_NOT_COMMITTED, "run_id": run_id},
        )

    ledger.begin_publication(report, artifacts, manifest)
    verify_run_files(root, run_id)
    verify_ledger_run(ledger, run_id, manifest, report)

    if _fault_active(FAULT_POINTER_NOT_SWITCHED):
        raise PublicationError(
            WORKSPACE_INVALID,
            "injected failure after ledger commit but before pointer switch",
            str(pointer_path(root)),
            {"fault": FAULT_POINTER_NOT_SWITCHED, "run_id": run_id},
        )

    _promote(root, ledger, run_id, manifest)
    return {
        "run_id": run_id,
        "published": True,
        "reason": "promoted",
        "recovery": recovery,
        "manifest": manifest,
    }


def _adopt_legacy_layout(workspace: Path, ledger: Ledger) -> dict[str, Any] | None:
    """Adopt a workspace written by the pre-protocol release.

    Old workspaces have flat ``reports/report.{json,md,html}`` files and
    ledger rows but no version directory, pointer or publication marker.
    The bytes are hashed and re-verified before adoption, so an adopted
    version satisfies the same binding contract as a new publication.
    """
    root_reports = _reports_root(workspace)
    flat_json = root_reports / "report.json"
    if not flat_json.is_file() or (root_reports / RUNS_DIRNAME).is_dir():
        return None
    if ledger.get_current_run_id() is not None:
        return None
    report = json.loads(flat_json.read_text(encoding="utf-8"))
    run_id = report["run_id"]
    if ledger.get_run(run_id) is None:
        return None
    run_dir = root_reports / RUNS_DIRNAME / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    artifacts = []
    for name in REPORT_FILES:
        source = root_reports / name
        data = source.read_bytes()
        (run_dir / name).write_bytes(data)
        artifacts.append(
            {"path": f"reports/{name}", "sha256": sha256_bytes(data), "bytes": len(data)}
        )
    manifest = build_manifest(report, artifacts)
    (run_dir / MANIFEST_NAME).write_bytes(
        (json.dumps(manifest, sort_keys=True, indent=2, ensure_ascii=False) + "\n").encode("utf-8")
    )
    verify_run_files(workspace, run_id)
    # Bind the archived run rows and the manifest in one staged transaction.
    ledger.begin_publication(report, ledger.get_artifacts(run_id) or artifacts, manifest)
    verify_ledger_run(ledger, run_id, manifest, report)
    _atomic_write(pointer_path(workspace), _pointer_payload(run_id, manifest))
    ledger.mark_publication_current(run_id, manifest)
    event = _record_event(
        ledger,
        run_id=run_id,
        event_type="legacy_adoption",
        decision="roll_forward",
        reason="adopted the only archived run from a pre-protocol workspace after re-verification",
        detail={"pointer": f"reports/{POINTER_NAME}"},
        created_at=manifest["generated_at"],
    )
    return event


def recover_interrupted(workspace: str | Path, ledger: Ledger) -> list[dict[str, Any]]:
    """Reconcile every interrupted publication before starting a new one.

    Cut points 1 and 2 can never be promoted: their directories are
    quarantined and the previous current version remains authoritative.
    Cut point 3 is a complete, verified candidate and is rolled forward.
    """
    root = Path(workspace)
    events: list[dict[str, Any]] = []

    adopted = _adopt_legacy_layout(root, ledger)
    if adopted is not None:
        events.append(adopted)

    # Cut point 1: leftover staging directories are partial by construction.
    for staging in _staging_dirs(root):
        run_id = staging.name[len(STAGING_PREFIX):]
        quarantined = _quarantine(staging, root, FAULT_PARTIAL_REPORTS)
        publication = ledger.get_publication(run_id)
        detail: dict[str, Any] = {"quarantined": quarantined}
        if publication is not None and publication["state"] == PUBLICATION_STAGED:
            # The staging directory never reached verification; any staged
            # marker left behind is orphaned state rather than evidence.
            ledger.delete_publication(run_id)
            detail["ledger_marker"] = "removed_staged"
        events.append(
            _record_event(
                ledger,
                run_id=run_id,
                event_type=FAULT_PARTIAL_REPORTS,
                decision="discard",
                reason="partial report files never reached the ledger; publishing is forbidden",
                detail=detail,
                created_at=_event_timestamp(ledger, run_id),
            )
        )

    # Cut points 2 and 3: complete version directories without a current marker.
    current_id = ledger.get_current_run_id()
    runs_root = root / "reports" / RUNS_DIRNAME
    if runs_root.is_dir():
        for directory in sorted(path for path in runs_root.iterdir() if path.is_dir()):
            run_id = directory.name
            publication = ledger.get_publication(run_id)
            if publication is not None and publication["state"] in (
                PUBLICATION_CURRENT,
                PUBLICATION_ARCHIVED,
            ):
                continue
            try:
                manifest = verify_run_files(root, run_id)
                if publication is None or publication["state"] != PUBLICATION_STAGED:
                    raise PublicationError(
                        WORKSPACE_INVALID,
                        "complete files without a committed ledger publication",
                        run_id,
                    )
                verify_ledger_run(ledger, run_id, manifest, load_run_report(root, run_id))
            except (ShardRouteError, json.JSONDecodeError, OSError) as exc:
                # Cut point 2 (or corrupted candidate): never promote.
                quarantined = _quarantine(directory, root, FAULT_LEDGER_NOT_COMMITTED)
                detail = {"quarantined": quarantined, "error": str(exc)}
                if publication is not None:
                    detail["ledger_state"] = publication["state"]
                    if publication["state"] == PUBLICATION_STAGED:
                        # Staged marker without verifiable files: drop the
                        # marker; archived run rows remain for audit.
                        ledger.delete_publication(run_id)
                        detail["ledger_marker"] = "removed_staged"
                events.append(
                    _record_event(
                        ledger,
                        run_id=run_id,
                        event_type=FAULT_LEDGER_NOT_COMMITTED,
                        decision="discard",
                        reason="ledger publication missing or unverified; previous version stays current",
                        detail=detail,
                        created_at=_event_timestamp(ledger, run_id),
                    )
                )
                continue
            # Cut point 3: full and verified candidate -> roll forward.
            _promote(root, ledger, run_id, manifest)
            events.append(
                _record_event(
                    ledger,
                    run_id=run_id,
                    event_type=FAULT_POINTER_NOT_SWITCHED,
                    decision="roll_forward",
                    reason="all files and ledger rows verified; switching current pointer",
                    detail={
                        "previous_current": current_id,
                        "pointer": f"reports/{POINTER_NAME}",
                    },
                    created_at=manifest["generated_at"],
                )
            )
            current_id = run_id
    return events


def rollback_candidate(
    workspace: str | Path, ledger: Ledger, run_id: str
) -> dict[str, Any]:
    """Operator-driven cut point 3 recovery: keep A, discard unpublished B."""
    root = Path(workspace)
    publication = ledger.get_publication(run_id)
    if publication is None or publication["state"] != PUBLICATION_STAGED:
        raise PublicationError(
            WORKSPACE_INVALID, "run is not a staged recovery candidate", run_id
        )
    manifest = verify_run_files(root, run_id)
    verify_ledger_run(ledger, run_id, manifest, load_run_report(root, run_id))
    current_id = ledger.get_current_run_id()
    if current_id == run_id:
        raise PublicationError(
            WORKSPACE_INVALID, "run is already current; nothing to roll back", run_id
        )
    directory = root / "reports" / RUNS_DIRNAME / run_id
    quarantined = _quarantine(directory, root, "operator_rollback")
    event = _record_event(
        ledger,
        run_id=run_id,
        event_type="operator_recovery",
        decision="rollback",
        reason="operator chose to retain the previous current version",
        detail={"quarantined": quarantined, "retained_current": current_id},
        created_at=manifest["generated_at"],
    )
    return {"event": event, "current_run_id": current_id, "quarantined": quarantined}


def roll_forward_candidate(
    workspace: str | Path, ledger: Ledger, run_id: str
) -> dict[str, Any]:
    """Operator-driven cut point 3 recovery: verify B and make it current."""
    root = Path(workspace)
    publication = ledger.get_publication(run_id)
    if publication is None or publication["state"] != PUBLICATION_STAGED:
        raise PublicationError(
            WORKSPACE_INVALID, "run is not a staged recovery candidate", run_id
        )
    manifest = verify_run_files(root, run_id)
    verify_ledger_run(ledger, run_id, manifest, load_run_report(root, run_id))
    previous = ledger.get_current_run_id()
    _promote(root, ledger, run_id, manifest)
    event = _record_event(
        ledger,
        run_id=run_id,
        event_type="operator_recovery",
        decision="roll_forward",
        reason="operator verified the staged candidate and promoted it",
        detail={"previous_current": previous, "pointer": f"reports/{POINTER_NAME}"},
        created_at=manifest["generated_at"],
    )
    return {"event": event, "current_run_id": run_id}


def _event_timestamp(ledger: Ledger, run_id: str) -> str:
    run = ledger.get_run(run_id)
    if run is not None:
        return str(run["generated_at"])
    return "1970-01-01T00:00:00Z"


def read_pointer(workspace: str | Path) -> dict[str, Any]:
    """Parse the current-version pointer without touching the ledger."""
    path = pointer_path(workspace)
    if not path.is_file():
        raise PublicationError(WORKSPACE_INVALID, "current version pointer is missing", str(path))
    try:
        pointer = json.loads(path.read_text(encoding="utf-8"))
    except ValueError as exc:
        raise PublicationError(WORKSPACE_INVALID, "current pointer is not valid JSON", str(path)) from exc
    if pointer.get("format") != POINTER_FORMAT or not pointer.get("run_id"):
        raise PublicationError(WORKSPACE_INVALID, "current pointer has an unknown format", str(path))
    return pointer


def resolve_current(workspace: str | Path, ledger: Ledger) -> dict[str, Any]:
    """Single read-only entry point for "which precheck is current?".

    The ledger marker, the pointer file and the on-disk artifacts must all
    name the same run_id and carry matching hashes.  A failure here means
    no trustworthy current version exists; callers must fail closed rather
    than combine evidence from different runs.
    """
    pointer = read_pointer(workspace)
    run_id = pointer["run_id"]
    current_id = ledger.get_current_run_id()
    if current_id != run_id:
        raise PublicationError(
            WORKSPACE_INVALID,
            "ledger current marker and workspace pointer disagree",
            str(pointer_path(workspace)),
            {"ledger_current": current_id, "pointer_run_id": run_id},
        )
    manifest = verify_run_files(workspace, run_id)
    if manifest["safe_to_execute"] != pointer["safe_to_execute"]:
        raise PublicationError(
            WORKSPACE_INVALID, "pointer and manifest disagree on safe_to_execute", run_id
        )
    verify_ledger_run(
        ledger, run_id, manifest, load_run_report(workspace, run_id)
    )
    run = ledger.get_run(run_id)
    assert run is not None
    for name in REPORT_FILES:
        top_level = workspace / "reports" / name
        archived = workspace / "reports" / RUNS_DIRNAME / run_id / name
        if not top_level.is_file() or file_sha256(top_level) != file_sha256(archived):
            raise PublicationError(
                WORKSPACE_INVALID, "top-level report does not match the current version", str(top_level)
            )
    return {
        "run_id": run_id,
        "status": run["status"],
        "safe_to_execute": bool(run["summary"]["safe_to_execute"]),
        "generated_at": run["generated_at"],
        "fingerprints": {
            "topology_old": run["old_fingerprint"],
            "topology_new": run["new_fingerprint"],
            "logical": run["logical_fingerprint"],
            "physical": run["physical_fingerprint"],
        },
        "manifest": manifest,
        "pointer": pointer,
    }


def list_published_runs(ledger: Ledger) -> list[dict[str, Any]]:
    """Return ledger runs annotated with publication state, current first."""
    current_id = ledger.get_current_run_id()
    result = []
    for publication in ledger.list_publications():
        run = ledger.get_run(publication["run_id"])
        if run is None:
            continue
        run["publication_state"] = publication["state"]
        run["is_current"] = publication["run_id"] == current_id
        result.append(run)
    result.sort(key=lambda item: (not item["is_current"], item["run_id"]))
    return result
