"""Read-only HTTP review desk bound strictly to loopback addresses.

Local access is enforced at three independent boundaries:

1. Startup resolution.  A name is resolved exactly once through an injectable
   resolver and every answer is classified.  Only *verified numeric loopback
   addresses* are ever handed to the socket layer; a checked hostname is never
   passed to ``bind`` for a second, mutable system resolution.
2. Socket verification.  After ``bind`` the actual ``getsockname`` result is
   re-checked to be loopback and IPv6 dual-stack behaviour is recorded.
3. Request authority gate.  Before the ledger is opened, every GET/HEAD request
   must present a single syntactically valid Host whose port matches the real
   bound port, the absolute-form authority (if any) must match Host, and the
   connected peer must itself be loopback.  No DNS is resolved while serving.

Two independently selectable authorization contracts are offered:

* ``pinned_endpoint`` (default): numeric loopback is pinned even when the local
  resolver also returns non-loopback answers; only numeric URLs are advertised.
* ``pure_local_name``: the literal name ``localhost`` is treated as local only
  when *every* resolver answer is loopback, otherwise startup is refused.
"""

from __future__ import annotations

import ipaddress
import json
import logging
import re
import socket
import sqlite3
import sys
import threading
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qs, unquote, urlparse, urlsplit

from . import __version__
from .errors import SERVER_HOST, WORKSPACE_INVALID, ShardRouteError
from .ledger import Ledger
from .server_ui import render_index, render_run

LOGGER = logging.getLogger("shardroute.server")

RUN_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,80}$")
MAX_LIMIT = 500

PINNED_ENDPOINT = "pinned_endpoint"
PURE_LOCAL_NAME = "pure_local_name"
BIND_CONTRACTS = (PINNED_ENDPOINT, PURE_LOCAL_NAME)
LOCALHOST_NAME = "localhost"

# RFC 3986 unreserved + sub-delims (the legal characters of an RFC 3986
# reg-name apart from percent-encoded octets).
_NAME_CHARS = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~!$&'()*+,;=")
_HEX_CHARS = set("0123456789abcdefABCDEF")

# Stable machine-readable denial reasons; the bodies sent to clients stay
# generic and never contain ledger data or the rejected authority.
REASON_PEER_NON_LOOPBACK = "peer_non_loopback"
REASON_HOST_MISSING = "host_missing"
REASON_HOST_DUPLICATE = "host_duplicate"
REASON_HOST_INVALID = "host_invalid"
REASON_HOST_PORT_MISMATCH = "host_port_mismatch"
REASON_HOST_UNAUTHORIZED = "host_unauthorized"
REASON_TARGET_INVALID = "request_target_invalid"
REASON_TARGET_SCHEME = "request_target_scheme_forbidden"
REASON_TARGET_CONFLICT = "absolute_target_authority_conflict"


@dataclass(frozen=True)
class ResolvedCandidate:
    """One classified resolver answer or a literal address supplied directly."""

    family: int
    literal: str
    address: ipaddress._BaseAddress
    loopback: bool
    source: str = "resolved"

    def as_record(self, *, selected: bool = False) -> dict[str, Any]:
        return {
            "family": "AF_INET6" if self.family == socket.AF_INET6 else "AF_INET",
            "literal": self.literal,
            "normalized": str(self.address),
            "loopback": self.loopback,
            "source": self.source,
            "selected": selected,
        }


@dataclass(frozen=True)
class BindPlan:
    """The immutable outcome of startup authorization, used by the request gate."""

    contract: str
    requested_host: str
    family: int
    bind_host: str
    advertised_host: str
    allowed_names: tuple[str, ...]
    candidates: tuple[ResolvedCandidate, ...]
    rejections: tuple[dict[str, Any], ...]
    selection_reason: str
    dns_used: bool

    @property
    def allowed_authorities(self) -> list[str]:
        authorities = [self.advertised_host]
        authorities.extend(self.allowed_names)
        return authorities


@dataclass
class ParsedAuthority:
    kind: str  # "ipv4" | "ipv6" | "name"
    port: int | None
    address: ipaddress._BaseAddress | None = None
    name: str | None = None

    @property
    def normalized(self) -> str:
        if self.kind == "ipv6":
            return f"[{self.address}]"
        if self.kind == "name":
            return self.name or ""
        return str(self.address)


@dataclass
class GateDecision:
    allowed: bool
    reason: str | None = None
    host_kind: str = "missing"
    host_normalized: str | None = None
    port_matches: bool | None = None
    target_form: str = "origin"
    path_and_query: str = "/"
    peer: str = ""
    peer_loopback: bool = False

    def as_event(self, method: str) -> dict[str, Any]:
        return {
            "type": "request",
            "method": method,
            "allowed": self.allowed,
            "reason": self.reason,
            "target_form": self.target_form,
            "host_kind": self.host_kind,
            "host_normalized": self.host_normalized,
            "port_matches": self.port_matches,
            "peer": self.peer,
            "peer_loopback": self.peer_loopback,
        }


def _deny(reason: str, **fields: Any) -> GateDecision:
    return GateDecision(allowed=False, reason=reason, **fields)


def _validate_port(port: int) -> None:
    if isinstance(port, bool) or not isinstance(port, int) or not 0 <= port <= 65535:
        raise ShardRouteError(
            SERVER_HOST, "port must be an integer between 0 and 65535", details={"port": port}
        )


def validate_bind(host: str, port: int) -> None:
    """Static, network-free check of the caller's bind arguments.

    Resolution-time enforcement happens in :func:`plan_bind`; this keeps the
    historical validation entry point while never trusting the literal name
    ``localhost`` without classified resolver answers.
    """
    if not isinstance(host, str) or not host:
        raise ShardRouteError(SERVER_HOST, "host must be a non-empty string")
    _validate_port(port)
    if host == LOCALHOST_NAME:
        return
    try:
        address = ipaddress.ip_address(host)
    except ValueError as exc:
        raise ShardRouteError(
            SERVER_HOST,
            f"host {host!r} is not a loopback address or the literal name 'localhost'",
            details={"host": host},
        ) from exc
    if not address.is_loopback:
        raise ShardRouteError(
            SERVER_HOST,
            "refusing to bind a non-loopback address; use 127.0.0.1",
            details={"host": host},
        )


def system_resolve(host: str) -> list[ResolvedCandidate]:
    """Resolve ``host`` once and classify every answer without name re-binding."""
    infos = socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
    candidates: list[ResolvedCandidate] = []
    seen: set[tuple[int, str]] = set()
    for family, _socket_type, _proto, _canonname, sockaddr in infos:
        if family not in (socket.AF_INET, socket.AF_INET6):
            continue
        literal = sockaddr[0]
        try:
            address = ipaddress.ip_address(literal)
        except ValueError:
            continue
        key = (family, str(address))
        if key in seen:
            continue
        seen.add(key)
        candidates.append(
            ResolvedCandidate(family, literal, address, address.is_loopback, source="resolved")
        )
    return candidates


def _literal_candidate(host: str) -> ResolvedCandidate:
    address = ipaddress.ip_address(host)
    family = socket.AF_INET if isinstance(address, ipaddress.IPv4Address) else socket.AF_INET6
    return ResolvedCandidate(family, str(address), address, address.is_loopback, source="literal")


def _preferred_loopback(candidates: list[ResolvedCandidate]) -> ResolvedCandidate | None:
    """Prefer IPv4 loopback (broadest client support), then IPv6 loopback."""
    ordered = [
        candidate
        for candidate in candidates
        if candidate.loopback and candidate.family == socket.AF_INET
    ] + [
        candidate
        for candidate in candidates
        if candidate.loopback and candidate.family == socket.AF_INET6
    ]
    return ordered[0] if ordered else None


def plan_bind(
    host: str,
    port: int,
    contract: str = PINNED_ENDPOINT,
    resolver: Callable[[str], list[ResolvedCandidate]] | None = None,
) -> BindPlan:
    """Resolve and authorize the bind, returning a numeric-only bind plan.

    The plan's ``bind_host`` is always a verified numeric loopback literal, so
    the socket layer never re-resolves a checked hostname.
    """
    if not isinstance(host, str) or not host:
        raise ShardRouteError(SERVER_HOST, "host must be a non-empty string")
    _validate_port(port)
    if contract not in BIND_CONTRACTS:
        raise ShardRouteError(
            SERVER_HOST,
            f"unknown local access contract {contract!r}; expected one of {sorted(BIND_CONTRACTS)}",
            details={"contract": contract},
        )
    resolve = resolver or system_resolve
    rejections: list[dict[str, Any]] = []

    if host == LOCALHOST_NAME:
        try:
            candidates = list(resolve(host))
        except OSError as exc:
            raise ShardRouteError(
                SERVER_HOST,
                f"could not resolve {host!r}: {exc}",
                details={"host": host, "contract": contract},
            ) from exc
        dns_used = True
        if not candidates:
            raise ShardRouteError(
                SERVER_HOST,
                "'localhost' resolved to no usable addresses",
                details={"host": host, "contract": contract},
            )
        non_loopback = [candidate for candidate in candidates if not candidate.loopback]
        if contract == PURE_LOCAL_NAME and non_loopback:
            details = {
                "host": host,
                "contract": contract,
                "candidates": [candidate.as_record() for candidate in candidates],
            }
            raise ShardRouteError(
                SERVER_HOST,
                "pure_local_name contract requires every 'localhost' answer to be loopback; "
                f"{len(non_loopback)} non-loopback answer(s) observed",
                details=details,
            )
        for candidate in non_loopback:
            rejections.append(
                {"literal": candidate.literal, "reason": "non_loopback_answer_skipped"}
            )
        chosen = _preferred_loopback(candidates)
        if chosen is None:
            raise ShardRouteError(
                SERVER_HOST,
                "'localhost' returned no loopback address to pin",
                details={
                    "host": host,
                    "contract": contract,
                    "candidates": [candidate.as_record() for candidate in candidates],
                },
            )
        selection_reason = (
            "pinned_first_loopback_among_mixed_resolver_answers"
            if non_loopback
            else "all_resolver_answers_loopback"
        )
        allowed_names = (LOCALHOST_NAME,) if contract == PURE_LOCAL_NAME else ()
    else:
        try:
            candidate = _literal_candidate(host)
        except ValueError as exc:
            # Any other DNS name is refused outright: no external resolution,
            # no trust relationship with arbitrary names.
            raise ShardRouteError(
                SERVER_HOST,
                f"host {host!r} is not a loopback address or the literal name 'localhost'",
                details={"host": host, "contract": contract},
            ) from exc
        candidates = [candidate]
        dns_used = False
        allowed_names = ()
        if not candidate.loopback:
            raise ShardRouteError(
                SERVER_HOST,
                "refusing to bind a non-loopback address; use 127.0.0.1",
                details={"host": host, "contract": contract},
            )
        chosen = candidate
        selection_reason = "literal_numeric_loopback"

    advertised = f"[{chosen.address}]" if chosen.family == socket.AF_INET6 else str(chosen.address)
    return BindPlan(
        contract=contract,
        requested_host=host,
        family=chosen.family,
        bind_host=str(chosen.address),
        advertised_host=advertised,
        allowed_names=allowed_names,
        candidates=tuple(candidates),
        rejections=tuple(rejections),
        selection_reason=selection_reason,
        dns_used=dns_used,
    )


_NO_PORT = -1


def _port_tail(tail: str) -> int | None:
    """Parse ``""`` (no port) or ``":12345"``; return ``None`` when malformed."""
    if tail == "":
        return _NO_PORT
    if not tail.startswith(":"):
        return None
    port_text = tail[1:]
    if not port_text or not port_text.isdigit() or len(port_text) > 5:
        return None
    port = int(port_text)
    if port > 65535:
        return None
    return port


def _valid_reg_name(text: str) -> bool:
    if not text:
        return False
    index = 0
    while index < len(text):
        char = text[index]
        if char == "%":
            if index + 2 >= len(text):
                return False
            if text[index + 1] not in _HEX_CHARS or text[index + 2] not in _HEX_CHARS:
                return False
            index += 3
            continue
        if char not in _NAME_CHARS:
            return False
        index += 1
    return True


def parse_authority(raw: str | None) -> ParsedAuthority | None:
    """Strictly parse an RFC 3986 host[:port] authority, or return ``None``.

    Numeric hosts are normalized through :mod:`ipaddress` so IPv6 compression
    and casing spellings collapse onto one identity.  No DNS is performed.
    """
    if raw is None:
        return None
    text = raw.strip()
    if not text:
        return None
    if text.startswith("["):
        end = text.find("]")
        if end < 2:
            return None
        inside = text[1:end]
        try:
            address = ipaddress.ip_address(inside)
        except ValueError:
            return None
        if not isinstance(address, ipaddress.IPv6Address):
            return None
        port = _port_tail(text[end + 1 :])
        if port is None:
            return None
        return ParsedAuthority("ipv6", None if port == _NO_PORT else port, address=address)
    if ":" in text:
        host_part, _, port_text = text.rpartition(":")
        port = _port_tail(":" + port_text)
        if port is None or not host_part:
            return None
    else:
        host_part = text
        port = _NO_PORT
    try:
        address = ipaddress.ip_address(host_part)
    except ValueError:
        if not _valid_reg_name(host_part):
            return None
        name = unquote(host_part).lower()
        # Decoded form must remain printable reg-name characters: encoded
        # separators or NUL bytes cannot smuggle a different identity.
        if not name or any(char not in _NAME_CHARS for char in name):
            return None
        return ParsedAuthority("name", None if port == _NO_PORT else port, name=name)
    if isinstance(address, ipaddress.IPv6Address):
        # A bare IPv6 literal without brackets is not a valid authority.
        return None
    return ParsedAuthority("ipv4", None if port == _NO_PORT else port, address=address)


def review_request_gate(
    *,
    host_headers: list[str] | None,
    request_target: str,
    peer_host: str,
    server_family: int,
    bound_literal: str,
    bound_port: int,
    allowed_names: tuple[str, ...],
) -> GateDecision:
    """Authorize one request before any ledger or UI call may happen.

    Checks the connected peer, the single Host header, its port and any
    absolute-form request-target authority.  Everything is compared against
    the pinned numeric loopback identity; name resolution is never used.
    """
    try:
        peer_address = ipaddress.ip_address(peer_host)
        peer_loopback = bool(peer_address.is_loopback)
        peer_normalized = str(peer_address)
    except ValueError:
        return _deny(
            REASON_PEER_NON_LOOPBACK,
            host_kind="missing",
            target_form="origin",
            peer=peer_host,
            peer_loopback=False,
        )
    if not peer_loopback:
        return _deny(
            REASON_PEER_NON_LOOPBACK,
            host_kind="missing",
            target_form="origin",
            peer=peer_normalized,
            peer_loopback=False,
        )

    target_form = "origin"
    path_and_query = "/"
    if request_target.startswith(("http://", "https://")):
        target_form = "absolute"
        try:
            split = urlsplit(request_target)
        except ValueError:
            return _deny(
                REASON_TARGET_INVALID, peer=peer_normalized, peer_loopback=True
            )
        if split.scheme != "http" or not split.netloc or split.username is not None:
            return _deny(
                REASON_TARGET_SCHEME,
                target_form=target_form,
                peer=peer_normalized,
                peer_loopback=True,
            )
        target_authority = parse_authority(split.netloc)
        if target_authority is None:
            return _deny(
                REASON_TARGET_INVALID,
                target_form=target_form,
                peer=peer_normalized,
                peer_loopback=True,
            )
        path_and_query = split.path or "/"
        if split.query:
            path_and_query = f"{path_and_query}?{split.query}"
        if split.fragment:
            path_and_query = f"{path_and_query}#{split.fragment}"
    elif request_target.startswith("/"):
        path_and_query = request_target
    else:
        return _deny(
            REASON_TARGET_INVALID, peer=peer_normalized, peer_loopback=True
        )

    if not host_headers:
        return _deny(
            REASON_HOST_MISSING,
            target_form=target_form,
            peer=peer_normalized,
            peer_loopback=True,
        )
    if len(host_headers) != 1:
        # RFC 9112: a recipient that parses multiple Host headers MUST reject.
        return _deny(
            REASON_HOST_DUPLICATE,
            target_form=target_form,
            peer=peer_normalized,
            peer_loopback=True,
        )
    host = parse_authority(host_headers[0])
    if host is None:
        return _deny(
            REASON_HOST_INVALID,
            host_kind="invalid",
            target_form=target_form,
            peer=peer_normalized,
            peer_loopback=True,
        )

    effective_port = host.port if host.port is not None else 80
    port_matches = effective_port == bound_port
    if not port_matches:
        return _deny(
            REASON_HOST_PORT_MISMATCH,
            host_kind=host.kind,
            host_normalized=host.normalized,
            port_matches=False,
            target_form=target_form,
            peer=peer_normalized,
            peer_loopback=True,
        )

    if host.kind == "name":
        authorized = host.name in allowed_names
    else:
        if host.kind == "ipv6" and server_family != socket.AF_INET6:
            authorized = False
        elif host.kind == "ipv4" and server_family != socket.AF_INET:
            authorized = False
        else:
            authorized = host.address is not None and str(host.address) == bound_literal
    if not authorized:
        return _deny(
            REASON_HOST_UNAUTHORIZED,
            host_kind=host.kind,
            host_normalized=host.normalized,
            port_matches=True,
            target_form=target_form,
            peer=peer_normalized,
            peer_loopback=True,
        )

    if target_form == "absolute":
        if target_authority.normalized != host.normalized or target_authority.port != host.port:
            return _deny(
                REASON_TARGET_CONFLICT,
                host_kind=host.kind,
                host_normalized=host.normalized,
                port_matches=True,
                target_form=target_form,
                peer=peer_normalized,
                peer_loopback=True,
            )

    return GateDecision(
        allowed=True,
        host_kind=host.kind,
        host_normalized=host.normalized,
        port_matches=True,
        target_form=target_form,
        path_and_query=path_and_query,
        peer=peer_normalized,
        peer_loopback=True,
    )


class ShardRouteServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self,
        address: tuple[str, int],
        ledger_path: Path,
        *,
        bind_plan: BindPlan,
    ) -> None:
        self.ledger_path = Path(ledger_path)
        self.bind_plan = bind_plan
        self.ledger_factory: Callable[..., Any] = Ledger
        self.security_events: list[dict[str, Any]] = []
        self.bind_audit: dict[str, Any] = {}
        self._events_lock = threading.Lock()
        super().__init__(address, ReviewHandler)
        self._verify_bound_socket(bind_plan)

    def record_event(self, event: dict[str, Any]) -> None:
        with self._events_lock:
            self.security_events.append(event)
        if event.get("allowed"):
            LOGGER.info("security event: %s", event)
        else:
            LOGGER.info("denied request: %s", event)

    def _verify_bound_socket(self, plan: BindPlan) -> None:
        socket_name = self.socket.getsockname()
        actual_literal = socket_name[0]
        actual_port = socket_name[1]
        try:
            actual_address = ipaddress.ip_address(actual_literal)
        except ValueError as exc:
            self.server_close()
            raise ShardRouteError(
                SERVER_HOST,
                f"bound socket returned non-IP address {actual_literal!r}",
                details={"requested_host": plan.requested_host},
            ) from exc
        if not actual_address.is_loopback or str(actual_address) != plan.bind_host:
            self.server_close()
            raise ShardRouteError(
                SERVER_HOST,
                "post-bind verification failed: socket is not the pinned loopback address",
                details={
                    "requested_host": plan.requested_host,
                    "planned_address": plan.bind_host,
                    "actual_address": str(actual_address),
                },
            )
        v6_only: int | None = None
        dual_stack = "not_applicable_ipv4"
        if plan.family == socket.AF_INET6:
            try:
                v6_only = int(
                    self.socket.getsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY)
                )
            except OSError:
                v6_only = None
            dual_stack = "ipv6_only" if v6_only == 1 else "dual_stack_ipv4_mapped"
        self.bind_audit = {
            "contract": plan.contract,
            "requested_host": plan.requested_host,
            "family": "AF_INET6" if plan.family == socket.AF_INET6 else "AF_INET",
            "planned_address": plan.bind_host,
            "actual_address": str(actual_address),
            "actual_port": actual_port,
            "loopback_verified": True,
            "ipv6_v6only": v6_only,
            "dual_stack_behavior": dual_stack,
            "allowed_authorities": list(plan.allowed_authorities),
            "advertised_url": f"http://{plan.advertised_host}:{actual_port}/",
            "candidates": [
                candidate.as_record(selected=candidate.literal == plan.bind_host)
                for candidate in plan.candidates
            ],
            "rejections": list(plan.rejections),
            "selection_reason": plan.selection_reason,
            "dns_used": plan.dns_used,
        }
        self.record_event(
            {
                "type": "bind",
                "allowed": True,
                "contract": plan.contract,
                "actual_address": str(actual_address),
                "actual_port": actual_port,
                "dual_stack_behavior": dual_stack,
                "allowed_authorities": list(plan.allowed_authorities),
            }
        )


class ShardRouteServerV6(ShardRouteServer):
    address_family = socket.AF_INET6

    def server_bind(self) -> None:
        # Pin to IPv6-only: IPv4 clients use the separately verified 127.0.0.1
        # identity instead of relying on platform dual-stack defaults.
        try:
            self.socket.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
        except OSError:
            pass
        super().server_bind()


def _json_bytes(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, indent=2, ensure_ascii=False) + "\n").encode("utf-8")


class ReviewHandler(BaseHTTPRequestHandler):
    server_version = f"ShardRoute/{__version__}"
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: Any) -> None:
        return

    def do_GET(self) -> None:  # noqa: N802
        self._handle_read(head=False)

    def do_HEAD(self) -> None:  # noqa: N802
        self._handle_read(head=True)

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

    def _handle_read(self, *, head: bool) -> None:
        server: ShardRouteServer = self.server  # type: ignore[assignment]
        plan = server.bind_plan
        decision = review_request_gate(
            host_headers=self.headers.get_all("Host"),
            request_target=self.path,
            peer_host=self.client_address[0],
            server_family=plan.family,
            bound_literal=plan.bind_host,
            bound_port=server.server_address[1],
            allowed_names=plan.allowed_names,
        )
        server.record_event(decision.as_event(self.command))
        if not decision.allowed:
            # Generic body: no ledger content, no reflected authority.
            self._error(
                400,
                "BAD_REQUEST",
                "The request was not authorized for this local read-only service.",
                head,
            )
            return

        parsed = urlparse(decision.path_and_query)
        path = unquote(parsed.path)
        if "\\" in path or ".." in path:
            self._error(400, "BAD_PATH", "path contains forbidden segments", head)
            return
        parts = [part for part in path.split("/") if part]
        query = parse_qs(parsed.query, keep_blank_values=False)
        try:
            with server.ledger_factory(server.ledger_path, readonly=True) as ledger:
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


def create_server(
    workspace: str | Path,
    host: str = "127.0.0.1",
    port: int = 8765,
    *,
    contract: str = PINNED_ENDPOINT,
    resolver: Callable[[str], list[ResolvedCandidate]] | None = None,
) -> ShardRouteServer:
    plan = plan_bind(host, port, contract=contract, resolver=resolver)
    root = Path(workspace)
    ledger_path = root / "ledger.sqlite3"
    if not ledger_path.is_file():
        raise ShardRouteError(
            WORKSPACE_INVALID, "ledger not found; run the demo first", str(ledger_path)
        )
    with Ledger(ledger_path, readonly=True) as ledger:
        ledger.list_runs()
    server_class = ShardRouteServerV6 if plan.family == socket.AF_INET6 else ShardRouteServer
    return server_class((plan.bind_host, port), ledger_path, bind_plan=plan)


def serve(
    workspace: str | Path,
    host: str = "127.0.0.1",
    port: int = 8765,
    *,
    contract: str = PINNED_ENDPOINT,
) -> None:
    server = create_server(workspace, host, port, contract=contract)
    audit = server.bind_audit
    sys.stdout.write(f"ShardRoute read-only UI: {audit['advertised_url']}\n")
    sys.stdout.write(
        f"local access contract: {audit['contract']} "
        f"(bound {audit['actual_address']}:{audit['actual_port']}, loopback verified; "
        f"allowed authorities: {', '.join(audit['allowed_authorities'])})\n"
    )
    sys.stdout.flush()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
