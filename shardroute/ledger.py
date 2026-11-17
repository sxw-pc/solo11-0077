"""Local SQLite ledger with deterministic, idempotent report writes."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from .canonical import canonical_json
from .errors import LEDGER_INVALID, ShardRouteError
from .models import SEVERITY_RANK

SCHEMA_VERSION = 1
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
CREATE INDEX IF NOT EXISTS findings_run_severity ON findings(run_id, severity, finding_id);
CREATE INDEX IF NOT EXISTS runs_created ON runs(generated_at, run_id);
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

    def record_run(self, report: dict[str, Any], artifacts: list[dict[str, Any]] | None = None) -> None:
        try:
            run_id = report["run_id"]
            audit = report["audit"]
            plan = report["plan"]
            fps = report["fingerprints"]
        except (KeyError, TypeError) as exc:
            raise ShardRouteError(LEDGER_INVALID, f"report is missing {exc}") from exc
        conn = self.connection
        with conn:
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
