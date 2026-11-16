"""Local SQLite ledger with deterministic, idempotent report writes.

The ledger participates in the two-phase publication protocol implemented in
``shardroute.publish``:

1. ``begin_publication`` records a run in state ``staged`` in the same
   transaction that writes its findings/moves/queries/artifacts.
2. ``mark_publication_current`` flips the single current pointer inside one
   transaction once the workspace pointer file has been swapped.

A run that never leaves ``staged`` is not a published version and is hidden
from every read-only entry point.  Every automatic or operator-driven
recovery decision is appended to ``recovery_events`` for later inspection.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from .canonical import canonical_json
from .errors import LEDGER_INVALID, ShardRouteError
from .models import SEVERITY_RANK

SCHEMA_VERSION = 1
PUBLICATION_STAGED = "staged"
PUBLICATION_CURRENT = "current"
PUBLICATION_ARCHIVED = "archived"
SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
 run_id TEXT PRIMARY KEY, format TEXT NOT NULL, schema_version INTEGER NOT NULL,
 generated_at TEXT NOT NULL, seed INTEGER NOT NULL, status TEXT NOT NULL,
 audit_status TEXT NOT NULL, old_fingerprint TEXT NOT NULL, new_fingerprint TEXT NOT NULL,
 logical_fingerprint TEXT NOT NULL, physical_fingerprint TEXT NOT NULL,
 summary_json TEXT NOT NULL, tables_json TEXT NOT NULL, plan_json TEXT NOT NULL,
 created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS findings (
 run_id TEXT NOT NULL, finding_id TEXT NOT NULL, code TEXT NOT NULL, severity TEXT NOT NULL,
 table_name TEXT NOT NULL, shard TEXT NOT NULL, row_id TEXT NOT NULL, message TEXT NOT NULL,
 detail_json TEXT NOT NULL, PRIMARY KEY (run_id, finding_id)
);
CREATE TABLE IF NOT EXISTS moves (
 run_id TEXT NOT NULL, table_name TEXT NOT NULL, from_shard TEXT NOT NULL,
 to_shard TEXT NOT NULL, row_count INTEGER NOT NULL,
 PRIMARY KEY (run_id, table_name, from_shard, to_shard)
);
CREATE TABLE IF NOT EXISTS queries (
 run_id TEXT NOT NULL, query_name TEXT NOT NULL, table_name TEXT NOT NULL,
 decision TEXT NOT NULL, reason TEXT NOT NULL, targeted_json TEXT NOT NULL,
 result_json TEXT NOT NULL, PRIMARY KEY (run_id, query_name)
);
CREATE TABLE IF NOT EXISTS artifacts (
 run_id TEXT NOT NULL, path TEXT NOT NULL, sha256 TEXT NOT NULL, bytes INTEGER NOT NULL,
 PRIMARY KEY (run_id, path)
);
CREATE TABLE IF NOT EXISTS publications (
 run_id TEXT PRIMARY KEY, state TEXT NOT NULL, manifest_json TEXT NOT NULL,
 created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS recovery_events (
 event_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, event_type TEXT NOT NULL,
 decision TEXT NOT NULL, reason TEXT NOT NULL, detail_json TEXT NOT NULL,
 created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS findings_run_severity ON findings(run_id, severity, finding_id);
CREATE INDEX IF NOT EXISTS runs_created ON runs(generated_at, run_id);
CREATE UNIQUE INDEX IF NOT EXISTS publications_one_current
 ON publications(state) WHERE state='current';
CREATE INDEX IF NOT EXISTS recovery_events_run ON recovery_events(run_id, event_id);
"""


class Ledger:
    def __init__(self, path: str | Path, readonly: bool = False) -> None:
        self.path = Path(path)
        self.readonly = readonly
        self._connection: sqlite3.Connection | None = None

    @property
    def connection(self) -> sqlite3.Connection:
        if self._connection is None:
            if not self.readonly:
                self.path.parent.mkdir(parents=True, exist_ok=True)
            elif not self.path.is_file():
                raise ShardRouteError(LEDGER_INVALID, "ledger does not exist", str(self.path))
            if self.readonly:
                uri = self.path.resolve().as_uri() + "?mode=ro"
                self._connection = sqlite3.connect(uri, uri=True)
                self._connection.execute("PRAGMA query_only=ON")
            else:
                self._connection = sqlite3.connect(str(self.path))
                self._connection.execute("PRAGMA foreign_keys=ON")
            self._connection.row_factory = sqlite3.Row
        return self._connection

    def initialize(self) -> None:
        if self.readonly:
            raise ShardRouteError(LEDGER_INVALID, "cannot initialize a read-only ledger", str(self.path))
        self.connection.executescript(SCHEMA)
        self.connection.commit()

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def __enter__(self) -> "Ledger":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def _write_run_rows(
        self,
        conn: sqlite3.Connection,
        report: dict[str, Any],
        artifacts: list[dict[str, Any]] | None,
    ) -> str:
        """Insert (or idempotently replace) every row belonging to one run."""
        try:
            run_id = report["run_id"]
            audit = report["audit"]
            plan = report["plan"]
            fps = report["fingerprints"]
        except (KeyError, TypeError) as exc:
            raise ShardRouteError(LEDGER_INVALID, f"report is missing {exc}") from exc
        conn.execute("DELETE FROM artifacts WHERE run_id=?", (run_id,))
        conn.execute("DELETE FROM queries WHERE run_id=?", (run_id,))
        conn.execute("DELETE FROM moves WHERE run_id=?", (run_id,))
        conn.execute("DELETE FROM findings WHERE run_id=?", (run_id,))
        conn.execute("DELETE FROM runs WHERE run_id=?", (run_id,))
        conn.execute(
            """INSERT INTO runs(run_id,format,schema_version,generated_at,seed,status,audit_status,
            old_fingerprint,new_fingerprint,logical_fingerprint,physical_fingerprint,
            summary_json,tables_json,plan_json,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                run_id, report["format"], SCHEMA_VERSION, report["generated_at"], report["seed"],
                report["status"], audit["status"], fps["topology_old"], fps["topology_new"],
                fps["logical"], fps["physical"], canonical_json(report["summary"]),
                canonical_json(audit["tables"]), canonical_json(plan), report["generated_at"],
            ),
        )
        for finding in audit["findings"]:
            conn.execute(
                """INSERT INTO findings(run_id,finding_id,code,severity,table_name,shard,row_id,
                message,detail_json) VALUES(?,?,?,?,?,?,?,?,?)""",
                (
                    run_id, finding["finding_id"], finding["code"], finding["severity"],
                    finding["table"], finding["shard"], finding["row_id"], finding["message"],
                    canonical_json(finding["detail"]),
                ),
            )
        for cell in plan["move_cells"]:
            conn.execute(
                "INSERT INTO moves(run_id,table_name,from_shard,to_shard,row_count) VALUES(?,?,?,?,?)",
                (run_id, cell["table"], cell["from_shard"], cell["to_shard"], cell["row_count"]),
            )
        for query in report["queries"]:
            conn.execute(
                """INSERT INTO queries(run_id,query_name,table_name,decision,reason,
                targeted_json,result_json) VALUES(?,?,?,?,?,?,?)""",
                (
                    run_id, query["name"], query["table"], query["decision"], query["reason"],
                    canonical_json(query["targeted_shards"]), canonical_json(query),
                ),
            )
        for artifact in artifacts or []:
            conn.execute(
                "INSERT INTO artifacts(run_id,path,sha256,bytes) VALUES(?,?,?,?)",
                (run_id, artifact["path"], artifact["sha256"], artifact["bytes"]),
            )
        return run_id

    def record_run(self, report: dict[str, Any], artifacts: list[dict[str, Any]] | None = None) -> None:
        """Idempotently archive one run without publishing it.

        Retained for direct/legacy use; the publication pipeline calls
        :meth:`begin_publication` so run rows and the ``staged`` publication
        state are committed in one transaction.
        """
        conn = self.connection
        with conn:
            self._write_run_rows(conn, report, artifacts)

    def begin_publication(
        self,
        report: dict[str, Any],
        artifacts: list[dict[str, Any]],
        manifest: dict[str, Any],
    ) -> str:
        """Phase one: persist run rows together with a ``staged`` publication."""
        conn = self.connection
        with conn:
            run_id = self._write_run_rows(conn, report, artifacts)
            timestamp = report["generated_at"]
            conn.execute("DELETE FROM publications WHERE run_id=?", (run_id,))
            conn.execute(
                """INSERT INTO publications(run_id,state,manifest_json,created_at,updated_at)
                VALUES(?,?,?,?,?)""",
                (run_id, PUBLICATION_STAGED, canonical_json(manifest), timestamp, timestamp),
            )
        return run_id

    def get_publication(self, run_id: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT * FROM publications WHERE run_id=?", (run_id,)
        ).fetchone()
        if row is None:
            return None
        item = dict(row)
        item["manifest"] = json.loads(item.pop("manifest_json"))
        return item

    def list_publications(self, state: str | None = None) -> list[dict[str, Any]]:
        if state:
            rows = self.connection.execute(
                "SELECT * FROM publications WHERE state=? ORDER BY run_id", (state,)
            ).fetchall()
        else:
            rows = self.connection.execute(
                "SELECT * FROM publications ORDER BY state, run_id"
            ).fetchall()
        items = []
        for row in rows:
            item = dict(row)
            item["manifest"] = json.loads(item.pop("manifest_json"))
            items.append(item)
        return items

    def get_current_run_id(self) -> str | None:
        row = self.connection.execute(
            "SELECT run_id FROM publications WHERE state=?", (PUBLICATION_CURRENT,)
        ).fetchone()
        return row["run_id"] if row is not None else None

    def delete_publication(self, run_id: str) -> None:
        """Drop an orphaned publication marker (run rows are left untouched)."""
        with self.connection:
            self.connection.execute("DELETE FROM publications WHERE run_id=?", (run_id,))

    def mark_publication_current(self, run_id: str, manifest: dict[str, Any]) -> None:
        """Phase two: make exactly one publication current in one transaction.

        The previous current run becomes ``archived``: it stays a complete,
        queryable published version, while ``staged`` is reserved for runs
        that have never finished the protocol.
        """
        conn = self.connection
        with conn:
            row = conn.execute(
                "SELECT runs.generated_at AS generated_at FROM runs WHERE runs.run_id=?",
                (run_id,),
            ).fetchone()
            if row is None:
                raise ShardRouteError(
                    LEDGER_INVALID, "cannot publish a run without archived run rows", run_id
                )
            conn.execute(
                "UPDATE publications SET state=? WHERE state=?",
                (PUBLICATION_ARCHIVED, PUBLICATION_CURRENT),
            )
            conn.execute(
                "UPDATE publications SET state=?, manifest_json=?, updated_at=? WHERE run_id=?",
                (PUBLICATION_CURRENT, canonical_json(manifest), row["generated_at"], run_id),
            )

    def record_recovery_event(
        self,
        *,
        event_id: str,
        run_id: str,
        event_type: str,
        decision: str,
        reason: str,
        detail: dict[str, Any] | None = None,
        created_at: str,
    ) -> bool:
        """Append a recovery decision; returns False when the event already exists."""
        conn = self.connection
        with conn:
            cursor = conn.execute(
                """INSERT OR IGNORE INTO recovery_events(event_id,run_id,event_type,decision,
                reason,detail_json,created_at) VALUES(?,?,?,?,?,?,?)""",
                (
                    event_id, run_id, event_type, decision, reason,
                    canonical_json(detail or {}), created_at,
                ),
            )
            return cursor.rowcount > 0

    def list_recovery_events(self, run_id: str | None = None) -> list[dict[str, Any]]:
        if run_id:
            rows = self.connection.execute(
                "SELECT * FROM recovery_events WHERE run_id=? ORDER BY created_at, event_id",
                (run_id,),
            ).fetchall()
        else:
            rows = self.connection.execute(
                "SELECT * FROM recovery_events ORDER BY created_at, event_id"
            ).fetchall()
        items = []
        for row in rows:
            item = dict(row)
            item["detail"] = json.loads(item.pop("detail_json"))
            items.append(item)
        return items

    @staticmethod
    def _decode(row: sqlite3.Row, *fields: str) -> dict[str, Any]:
        result = dict(row)
        for field in fields:
            if field in result:
                result[field] = json.loads(result[field])
        return result

    def list_runs(self) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT * FROM runs ORDER BY generated_at DESC, run_id DESC"
        ).fetchall()
        result = []
        for row in rows:
            item = self._decode(row, "summary_json", "tables_json", "plan_json")
            item["summary"] = item.pop("summary_json")
            item["tables"] = item.pop("tables_json")
            item["plan"] = item.pop("plan_json")
            result.append(item)
        return result

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        row = self.connection.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
        if row is None:
            return None
        item = self._decode(row, "summary_json", "tables_json", "plan_json")
        item["summary"] = item.pop("summary_json")
        item["tables"] = item.pop("tables_json")
        item["plan"] = item.pop("plan_json")
        return item

    def get_findings(
        self,
        run_id: str,
        *,
        table: str | None = None,
        code: str | None = None,
        severity: str | None = None,
    ) -> list[dict[str, Any]]:
        clauses, params = ["run_id=?"], [run_id]
        for column, value in (("table_name", table), ("code", code), ("severity", severity)):
            if value:
                clauses.append(f"{column}=?")
                params.append(value)
        rows = self.connection.execute(
            f"SELECT * FROM findings WHERE {' AND '.join(clauses)}", tuple(params)
        ).fetchall()
        items = [self._decode(row, "detail_json") for row in rows]
        for item in items:
            item["detail"] = item.pop("detail_json")
            item["table"] = item.pop("table_name")
        items.sort(key=lambda item: (SEVERITY_RANK.get(item["severity"], 9), item["finding_id"]))
        return items

    def get_moves(self, run_id: str) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            """SELECT table_name,from_shard,to_shard,row_count FROM moves WHERE run_id=?
            ORDER BY table_name,from_shard,to_shard""",
            (run_id,),
        ).fetchall()
        moves = []
        for row in rows:
            item = dict(row)
            item["table"] = item.pop("table_name")
            moves.append(item)
        return moves

    def get_queries(self, run_id: str) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT result_json FROM queries WHERE run_id=? ORDER BY query_name", (run_id,)
        ).fetchall()
        return [json.loads(row["result_json"]) for row in rows]

    def get_artifacts(self, run_id: str) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT path,sha256,bytes FROM artifacts WHERE run_id=? ORDER BY path", (run_id,)
        ).fetchall()
        return [dict(row) for row in rows]
