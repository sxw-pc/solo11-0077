"""Read-only HTTP review desk bound strictly to loopback addresses."""

from __future__ import annotations

import ipaddress
import json
import re
import socket
import sqlite3
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from . import __version__
from .errors import SERVER_HOST, WORKSPACE_INVALID, ShardRouteError
from .ledger import Ledger
from .server_ui import render_index, render_run

RUN_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,80}$")
MAX_LIMIT = 500

def validate_bind(host: str, port: int) -> None:
    if not isinstance(host, str) or not host:
        raise ShardRouteError(SERVER_HOST, "host must be a non-empty string")
    if isinstance(port, bool) or not isinstance(port, int) or not 0 <= port <= 65535:
        raise ShardRouteError(SERVER_HOST, "port must be an integer between 0 and 65535")
    if host == "localhost":
        return
    try:
        address = ipaddress.ip_address(host)
    except ValueError as exc:
        raise ShardRouteError(SERVER_HOST, f"host {host!r} is not a loopback address", details={"host": host}) from exc
    if not address.is_loopback:
        raise ShardRouteError(
            SERVER_HOST,
            "refusing to bind a non-loopback address; use 127.0.0.1",
            details={"host": host},
        )

class ShardRouteServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True
    def __init__(self, address: tuple[str, int], ledger_path: Path) -> None:
        self.ledger_path = ledger_path
        super().__init__(address, ReviewHandler)

class ShardRouteServerV6(ShardRouteServer):
    address_family = socket.AF_INET6

def _json_bytes(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, indent=2, ensure_ascii=False) + "\n").encode("utf-8")

class ReviewHandler(BaseHTTPRequestHandler):
    server_version = f"ShardRoute/{__version__}"
    protocol_version = "HTTP/1.1"
    def log_message(self, format: str, *args: Any) -> None:
        return
    def do_GET(self) -> None:  # noqa: N802
        self._dispatch(head=False)
    def do_HEAD(self) -> None:  # noqa: N802
        self._dispatch(head=True)
    def _method_not_allowed(self) -> None:
        body = _json_bytes({"error": "read_only", "message": "Only GET and HEAD are supported."})
        self._send(405, body, "application/json; charset=utf-8", head=False, extra={"Allow": "GET, HEAD"})
    do_POST = _method_not_allowed
    do_PUT = _method_not_allowed
    do_PATCH = _method_not_allowed
    do_DELETE = _method_not_allowed
    def _send(
        self,
        status: int,
        body: bytes,
        content_type: str,
        *,
        head: bool,
        extra: dict[str, str] | None = None,
    ) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header(
            "Content-Security-Policy",
            "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; base-uri 'none'",
        )
        for key, value in (extra or {}).items():
            self.send_header(key, value)
        self.end_headers()
        if not head:
            self.wfile.write(body)
    def _error(self, status: int, code: str, message: str, head: bool) -> None:
        self._send(status, _json_bytes({"error": code, "message": message}), "application/json; charset=utf-8", head=head)
    @staticmethod
    def _first(query: dict[str, list[str]], name: str) -> str | None:
        values = query.get(name)
        return values[0] if values else None
    @staticmethod
    def _limit(query: dict[str, list[str]]) -> int:
        raw = ReviewHandler._first(query, "limit") or "200"
        try:
            limit = int(raw)
        except ValueError:
            limit = 200
        return max(1, min(limit, MAX_LIMIT))
    def _dispatch(self, *, head: bool) -> None:
        parsed = urlparse(self.path)
        path = unquote(parsed.path)
        if "\\" in path or ".." in path:
            self._error(400, "BAD_PATH", "path contains forbidden segments", head)
            return
        parts = [part for part in path.split("/") if part]
        query = parse_qs(parsed.query, keep_blank_values=False)
        try:
            with Ledger(self.server.ledger_path, readonly=True) as ledger:  # type: ignore[attr-defined]
                self._route(parts, query, ledger, head)
        except sqlite3.Error as exc:
            self._error(500, "LEDGER_ERROR", f"ledger read failed: {exc}", head)
        except ShardRouteError as exc:
            self._error(500, exc.code, exc.message, head)
    def _route(
        self, parts: list[str], query: dict[str, list[str]], ledger: Ledger, head: bool
    ) -> None:
        if parts == ["favicon.ico"]:
            self._send(204, b"", "image/x-icon", head=head)
            return
        if parts == ["api", "health"]:
            runs = ledger.list_runs()
            self._send(
                200,
                _json_bytes({"status": "ok", "read_only": True, "version": __version__, "runs": len(runs)}),
                "application/json; charset=utf-8",
                head=head,
            )
            return
        if parts == ["api", "runs"]:
            self._send(200, _json_bytes(ledger.list_runs()), "application/json; charset=utf-8", head=head)
            return
        if not parts:
            self._send(200, render_index(ledger.list_runs()).encode("utf-8"), "text/html; charset=utf-8", head=head)
            return
        if parts[0] == "runs" and len(parts) == 2 and RUN_ID_RE.match(parts[1]):
            run = ledger.get_run(parts[1])
            if run is None:
                self._error(404, "RUN_NOT_FOUND", "run not found", head)
                return
            findings = ledger.get_findings(
                run["run_id"],
                table=self._first(query, "table"),
                code=self._first(query, "code"),
                severity=self._first(query, "severity"),
            )[: self._limit(query)]
            run["_artifacts"] = ledger.get_artifacts(run["run_id"])
            queries = ledger.get_queries(run["run_id"])
            self._send(200, render_run(run, findings, queries).encode("utf-8"), "text/html; charset=utf-8", head=head)
            return
        if parts[0] == "api" and len(parts) >= 3 and parts[1] == "runs" and RUN_ID_RE.match(parts[2]):
            self._api_run(parts[2], parts[3:], query, ledger, head)
            return
        self._error(404, "NOT_FOUND", "endpoint not found", head)
    def _api_run(
        self, run_id: str, tail: list[str], query: dict[str, list[str]], ledger: Ledger, head: bool
    ) -> None:
        run = ledger.get_run(run_id)
        if run is None:
            self._error(404, "RUN_NOT_FOUND", "run not found", head)
            return
        if not tail:
            payload = {
                **run,
                "findings": ledger.get_findings(run_id)[: self._limit(query)],
                "moves": ledger.get_moves(run_id),
                "queries": ledger.get_queries(run_id),
                "artifacts": ledger.get_artifacts(run_id),
            }
        elif tail == ["findings"]:
            payload = ledger.get_findings(
                run_id,
                table=self._first(query, "table"),
                code=self._first(query, "code"),
                severity=self._first(query, "severity"),
            )[: self._limit(query)]
        elif tail == ["plan"]:
            payload = {"run_id": run_id, "plan": run["plan"], "moves": ledger.get_moves(run_id)}
        elif tail == ["queries"]:
            payload = ledger.get_queries(run_id)
        elif tail == ["artifacts"]:
            payload = ledger.get_artifacts(run_id)
        else:
            self._error(404, "NOT_FOUND", "endpoint not found", head)
            return
        self._send(200, _json_bytes(payload), "application/json; charset=utf-8", head=head)

def create_server(workspace: str | Path, host: str = "127.0.0.1", port: int = 8765) -> ShardRouteServer:
    validate_bind(host, port)
    root = Path(workspace)
    ledger_path = root / "ledger.sqlite3"
    if not ledger_path.is_file():
        raise ShardRouteError(
            WORKSPACE_INVALID, "ledger not found; run the demo first", str(ledger_path)
        )
    with Ledger(ledger_path, readonly=True) as ledger:
        ledger.list_runs()
    server_class = ShardRouteServerV6 if ":" in host else ShardRouteServer
    return server_class((host, port), ledger_path)

def serve(workspace: str | Path, host: str = "127.0.0.1", port: int = 8765) -> None:
    server = create_server(workspace, host, port)
    actual_host, actual_port = server.server_address[:2]
    sys.stdout.write(
        f"ShardRoute read-only UI: http://{actual_host}:{actual_port}/ (loopback only, Ctrl+C to stop)\n"
    )
    sys.stdout.flush()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
