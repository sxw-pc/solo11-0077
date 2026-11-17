"""Synthetic tests for the loopback-only local access contracts.

The resolver is always scripted, so no real DNS is involved.  Attacker Host
headers and absolute-form targets are sent over real loopback sockets.
"""

from __future__ import annotations

import http.client
import ipaddress
import socket
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

from shardroute import server as server_module
from shardroute.errors import SERVER_HOST, ShardRouteError
from shardroute.ledger import Ledger
from shardroute.server import (
    PINNED_ENDPOINT,
    PURE_LOCAL_NAME,
    REASON_HOST_DUPLICATE,
    REASON_HOST_INVALID,
    REASON_HOST_MISSING,
    REASON_HOST_PORT_MISMATCH,
    REASON_HOST_UNAUTHORIZED,
    REASON_PEER_NON_LOOPBACK,
    REASON_TARGET_CONFLICT,
    REASON_TARGET_INVALID,
    REASON_TARGET_SCHEME,
    ResolvedCandidate,
    ShardRouteServerV6,
    create_server,
    parse_authority,
    plan_bind,
    review_request_gate,
    system_resolve,
    validate_bind,
)

from support import write_test_run


def make_candidate(literal: str) -> ResolvedCandidate:
    address = ipaddress.ip_address(literal)
    family = socket.AF_INET if isinstance(address, ipaddress.IPv4Address) else socket.AF_INET6
    return ResolvedCandidate(family, literal, address, address.is_loopback)


class ScriptedResolver:
    """Replays scripted answer lists; each call advances one scripted round."""

    def __init__(self, rounds: list[list[str]]) -> None:
        self.rounds = [[make_candidate(item) for item in round_] for round_ in rounds]
        self.calls: list[str] = []

    def __call__(self, host: str) -> list[ResolvedCandidate]:
        self.calls.append(host)
        index = min(len(self.calls) - 1, len(self.rounds) - 1)
        # Return copies so callers can never mutate the script.
        return list(self.rounds[index])


MIXED_ANSWERS = ["127.0.0.1", "10.20.30.40"]
SWAP_ROUNDS = [MIXED_ANSWERS, ["10.20.30.40", "127.0.0.1"]]


class SpyLedger(Ledger):
    instances: list["SpyLedger"] = []
    method_names = ("list_runs", "get_run", "get_findings", "get_moves", "get_queries", "get_artifacts")

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.calls = {name: 0 for name in self.method_names}
        SpyLedger.instances.append(self)

    def list_runs(self):
        self.calls["list_runs"] += 1
        return super().list_runs()

    def get_run(self, run_id):
        self.calls["get_run"] += 1
        return super().get_run(run_id)

    def get_findings(self, run_id, **kwargs):
        self.calls["get_findings"] += 1
        return super().get_findings(run_id, **kwargs)

    def get_moves(self, run_id):
        self.calls["get_moves"] += 1
        return super().get_moves(run_id)

    def get_queries(self, run_id):
        self.calls["get_queries"] += 1
        return super().get_queries(run_id)

    def get_artifacts(self, run_id):
        self.calls["get_artifacts"] += 1
        return super().get_artifacts(run_id)


class PlanBindTests(unittest.TestCase):
    def test_pinned_contract_starts_under_mixed_resolution_without_name_rebind(self):
        resolver = ScriptedResolver([MIXED_ANSWERS])
        plan = plan_bind("localhost", 8765, contract=PINNED_ENDPOINT, resolver=resolver)
        self.assertEqual(plan.bind_host, "127.0.0.1")
        self.assertEqual(plan.family, socket.AF_INET)
        self.assertEqual(plan.allowed_names, ())
        self.assertEqual(plan.selection_reason, "pinned_first_loopback_among_mixed_resolver_answers")
        self.assertTrue(plan.dns_used)
        skipped = [item["literal"] for item in plan.rejections]
        self.assertIn("10.20.30.40", skipped)
        self.assertEqual(plan.candidates[1].loopback, False)
        self.assertEqual(plan.advertised_host, "127.0.0.1")
        self.assertEqual(resolver.calls, ["localhost"])

    def test_second_resolution_round_changes_preference_but_pinned_address_holds(self):
        resolver = ScriptedResolver(SWAP_ROUNDS)
        plan = plan_bind("localhost", 8765, contract=PINNED_ENDPOINT, resolver=resolver)
        self.assertEqual(plan.bind_host, "127.0.0.1")
        # A bind that trusted the name would re-resolve here and could pick
        # 10.20.30.40 first; the plan already carries a numeric literal.
        second_round = resolver("localhost")
        self.assertEqual(second_round[0].literal, "10.20.30.40")
        self.assertEqual(plan.bind_host, "127.0.0.1")

    def test_pinned_contract_with_real_server_never_re_resolves(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        write_test_run(root)
        resolver = ScriptedResolver(SWAP_ROUNDS)
        server = create_server(root, "localhost", 0, contract=PINNED_ENDPOINT, resolver=resolver)
        self.addCleanup(server.server_close)
        self.assertEqual(resolver.calls, ["localhost"])
        self.assertEqual(server.server_address[0], "127.0.0.1")
        self.assertEqual(server.socket.getsockname()[0], "127.0.0.1")
        self.assertTrue(server.bind_audit["loopback_verified"])
        # A further (mutated) resolution must not influence the live socket.
        resolver("localhost")
        self.assertEqual(server.socket.getsockname()[0], "127.0.0.1")
        self.assertEqual(resolver.calls.count("localhost"), 2)

    def test_pure_local_name_refuses_mixed_resolution(self):
        resolver = ScriptedResolver([MIXED_ANSWERS])
        with self.assertRaises(ShardRouteError) as caught:
            plan_bind("localhost", 8765, contract=PURE_LOCAL_NAME, resolver=resolver)
        self.assertEqual(caught.exception.code, SERVER_HOST)
        literals = [item["literal"] for item in caught.exception.details["candidates"]]
        self.assertEqual(set(literals), {"127.0.0.1", "10.20.30.40"})
        self.assertEqual(caught.exception.details["contract"], PURE_LOCAL_NAME)

    def test_pure_local_name_accepts_all_loopback_answers_and_keeps_name_identity(self):
        resolver = ScriptedResolver([["127.0.0.1", "::1"]])
        plan = plan_bind("localhost", 8765, contract=PURE_LOCAL_NAME, resolver=resolver)
        self.assertEqual(plan.bind_host, "127.0.0.1")
        self.assertEqual(plan.allowed_names, ("localhost",))
        self.assertEqual(plan.selection_reason, "all_resolver_answers_loopback")
        self.assertIn("localhost", plan.allowed_authorities)
        self.assertIn("127.0.0.1", plan.allowed_authorities)

    def test_pinned_prefers_ipv4_loopback_over_ipv6(self):
        resolver = ScriptedResolver([["::1", "127.0.0.1"]])
        plan = plan_bind("localhost", 8765, contract=PINNED_ENDPOINT, resolver=resolver)
        self.assertEqual(plan.bind_host, "127.0.0.1")
        self.assertEqual(plan.family, socket.AF_INET)

    def test_pinned_falls_back_to_ipv6_loopback_and_records_dual_stack_behavior(self):
        resolver = ScriptedResolver([["::1", "10.20.30.40"]])
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        write_test_run(root)
        server = create_server(root, "localhost", 0, contract=PINNED_ENDPOINT, resolver=resolver)
        self.addCleanup(server.server_close)
        self.assertIsInstance(server, ShardRouteServerV6)
        self.assertEqual(server.server_address[0], "::1")
        self.assertEqual(ipaddress.ip_address(server.socket.getsockname()[0]).is_loopback, True)
        self.assertEqual(server.bind_audit["ipv6_v6only"], 1)
        self.assertEqual(server.bind_audit["dual_stack_behavior"], "ipv6_only")
        self.assertEqual(server.bind_audit["advertised_url"], f"http://[::1]:{server.server_address[1]}/")

    def test_no_loopback_answer_refuses_startup(self):
        resolver = ScriptedResolver([["10.20.30.40", "192.168.1.9"]])
        with self.assertRaises(ShardRouteError) as caught:
            plan_bind("localhost", 8765, contract=PINNED_ENDPOINT, resolver=resolver)
        self.assertEqual(caught.exception.code, SERVER_HOST)

    def test_empty_resolution_refuses_startup(self):
        resolver = ScriptedResolver([[]])
        with self.assertRaises(ShardRouteError):
            plan_bind("localhost", 8765, contract=PINNED_ENDPOINT, resolver=resolver)

    def test_literal_non_loopback_and_arbitrary_names_are_rejected(self):
        for host in ("0.0.0.0", "10.20.30.40", "224.0.0.1", "db.internal", "example.com"):
            with self.subTest(host=host):
                with self.assertRaises(ShardRouteError) as caught:
                    plan_bind(host, 8765, contract=PINNED_ENDPOINT)
                self.assertEqual(caught.exception.code, SERVER_HOST)

    def test_literal_loopback_addresses_bind_numerically(self):
        plan_v4 = plan_bind("127.0.0.1", 8765)
        self.assertEqual(plan_v4.bind_host, "127.0.0.1")
        self.assertFalse(plan_v4.dns_used)
        plan_v6 = plan_bind("::1", 8765)
        self.assertEqual(plan_v6.bind_host, "::1")
        self.assertEqual(plan_v6.family, socket.AF_INET6)

    def test_unknown_contract_and_bad_port_are_rejected(self):
        with self.assertRaises(ShardRouteError):
            plan_bind("localhost", 8765, contract="mystery")
        for bad_port in (-1, 65536, "80", True):
            with self.subTest(bad_port=bad_port):
                with self.assertRaises(ShardRouteError):
                    plan_bind("127.0.0.1", bad_port)  # type: ignore[arg-type]

    def test_static_validate_bind_keeps_legacy_contract(self):
        validate_bind("localhost", 8765)
        validate_bind("127.0.0.1", 0)
        with self.assertRaises(ShardRouteError):
            validate_bind("0.0.0.0", 8765)
        with self.assertRaises(ShardRouteError):
            validate_bind("", 8765)

    def test_system_resolver_classifies_real_loopback(self):
        candidates = system_resolve("127.0.0.1")
        self.assertTrue(candidates)
        self.assertTrue(all(candidate.loopback for candidate in candidates))
        self.assertEqual(candidates[0].source, "resolved")

    def test_resolver_failure_surfaces_as_server_host_error(self):
        def broken(_host):
            raise OSError("resolver unavailable")

        with self.assertRaises(ShardRouteError) as caught:
            plan_bind("localhost", 8765, resolver=broken)
        self.assertEqual(caught.exception.code, SERVER_HOST)

    def test_post_bind_verification_rejects_a_wrong_socket(self):
        # Simulate a socket whose getsockname does not match the pinned plan.
        plan = plan_bind("127.0.0.1", 0)
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        write_test_run(root)
        server = object.__new__(server_module.ShardRouteServer)
        fake_sock = mock.Mock()
        fake_sock.getsockname.return_value = ("10.20.30.40", 4444)
        server.socket = fake_sock
        closed = []
        server.server_close = lambda: closed.append(True)
        with self.assertRaises(ShardRouteError) as caught:
            server_module.ShardRouteServer._verify_bound_socket(server, plan)
        self.assertEqual(caught.exception.code, SERVER_HOST)
        self.assertEqual(caught.exception.details["actual_address"], "10.20.30.40")
        self.assertEqual(closed, [True])


class AuthorityParsingTests(unittest.TestCase):
    def test_numeric_normalization_collapses_spellings(self):
        self.assertEqual(parse_authority("127.0.0.1:8765").normalized, "127.0.0.1")
        upper = parse_authority("[0:0:0:0:0:0:0:1]:8765")
        self.assertEqual(upper.kind, "ipv6")
        self.assertEqual(upper.normalized, "[::1]")
        self.assertEqual(upper.port, 8765)
        compressed = parse_authority("[::1]:8765")
        self.assertEqual(compressed.normalized, upper.normalized)

    def test_invalid_authorities(self):
        for raw in (
            "", " ", "[]", "[127.0.0.1]", "::1", "127.0.0.1:", "127.0.0.1:99999",
            "127.0.0.1:abc", "attacker example", "127.0.0.1:80:90", "[::1",
            "[vFe.x]:80", "127.0.0.1%00",
        ):
            with self.subTest(raw=raw):
                self.assertIsNone(parse_authority(raw))

    def test_name_percent_decoding_and_case_folding(self):
        parsed = parse_authority("LOCAL%68OST")
        self.assertEqual(parsed.kind, "name")
        self.assertEqual(parsed.name, "localhost")
        self.assertEqual(parse_authority("AtTaCkEr.Example").name, "attacker.example")


class RequestGateTests(unittest.TestCase):
    def gate(self, **overrides):
        fields = {
            "host_headers": ["127.0.0.1:8765"],
            "request_target": "/api/runs",
            "peer_host": "127.0.0.1",
            "server_family": socket.AF_INET,
            "bound_literal": "127.0.0.1",
            "bound_port": 8765,
            "allowed_names": (),
        }
        fields.update(overrides)
        return review_request_gate(**fields)

    def test_legitimate_loopback_request_passes_and_preserves_target(self):
        decision = self.gate()
        self.assertTrue(decision.allowed)
        self.assertEqual(decision.path_and_query, "/api/runs")
        self.assertTrue(decision.peer_loopback)
        head_decision = self.gate(request_target="/runs/abc?limit=2")
        self.assertEqual(head_decision.path_and_query, "/runs/abc?limit=2")

    def test_non_loopback_peer_is_denied_before_host_matters(self):
        decision = self.gate(peer_host="10.9.9.9")
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.reason, REASON_PEER_NON_LOOPBACK)
        self.assertEqual(
            self.gate(peer_host="not-an-ip").reason, REASON_PEER_NON_LOOPBACK
        )

    def test_missing_duplicate_and_invalid_host(self):
        self.assertEqual(self.gate(host_headers=[]).reason, REASON_HOST_MISSING)
        self.assertEqual(
            self.gate(host_headers=["127.0.0.1:8765", "127.0.0.1:8765"]).reason,
            REASON_HOST_DUPLICATE,
        )
        self.assertEqual(self.gate(host_headers=["attacker example:8765"]).reason, REASON_HOST_INVALID)
        self.assertEqual(self.gate(host_headers=["127.0.0.1:99999"]).reason, REASON_HOST_INVALID)

    def test_wrong_port_and_default_port_handling(self):
        self.assertEqual(
            self.gate(host_headers=["127.0.0.1:9000"]).reason, REASON_HOST_PORT_MISMATCH
        )
        # No explicit port means the HTTP default (80), which differs from 8765.
        self.assertEqual(
            self.gate(host_headers=["127.0.0.1"]).reason, REASON_HOST_PORT_MISMATCH
        )
        self.assertTrue(self.gate(bound_port=80, host_headers=["127.0.0.1"]).allowed)

    def test_untrusted_names_and_family_mismatch(self):
        self.assertEqual(
            self.gate(host_headers=["attacker.example:8765"]).reason,
            REASON_HOST_UNAUTHORIZED,
        )
        # The local name is only an authority under the pure_local_name contract.
        self.assertEqual(
            self.gate(host_headers=["localhost:8765"]).reason, REASON_HOST_UNAUTHORIZED
        )
        self.assertTrue(
            self.gate(
                host_headers=["LOCALHOST:8765"], allowed_names=("localhost",)
            ).allowed
        )
        # IPv6 authority against an IPv4 socket is a different permission set.
        self.assertEqual(
            self.gate(host_headers=["[::1]:8765"]).reason, REASON_HOST_UNAUTHORIZED
        )
        # Equivalent IPv6 spellings still resolve to the same allowed identity.
        ok = self.gate(
            host_headers=["[0:0:0:0:0:0:0:1]:8765"],
            server_family=socket.AF_INET6,
            bound_literal="::1",
        )
        self.assertTrue(ok.allowed)

    def test_absolute_form_targets(self):
        ok = self.gate(request_target="http://127.0.0.1:8765/api/runs")
        self.assertTrue(ok.allowed)
        self.assertEqual(ok.target_form, "absolute")
        self.assertEqual(ok.path_and_query, "/api/runs")
        conflict = self.gate(
            host_headers=["127.0.0.1:8765"],
            request_target="http://attacker.example:8765/api/runs",
        )
        self.assertEqual(conflict.reason, REASON_TARGET_CONFLICT)
        bad_target = self.gate(request_target="http://127.0.0.1:8765:extra/api")
        self.assertEqual(bad_target.reason, REASON_TARGET_INVALID)
        https = self.gate(request_target="https://127.0.0.1:8765/api/runs")
        self.assertEqual(https.reason, REASON_TARGET_SCHEME)
        userinfo = self.gate(request_target="http://user@127.0.0.1:8765/api/runs")
        self.assertEqual(userinfo.reason, REASON_TARGET_SCHEME)

    def test_asterisk_and_relative_targets_are_rejected(self):
        self.assertEqual(self.gate(request_target="*").reason, REASON_TARGET_INVALID)
        self.assertEqual(
            self.gate(request_target="ftp://127.0.0.1:8765/").reason,
            REASON_TARGET_INVALID,
        )


class LiveServerSecurityTests(unittest.TestCase):
    def setUp(self):
        SpyLedger.instances = []
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.report = write_test_run(self.root)
        self.resolver = ScriptedResolver([MIXED_ANSWERS])
        self.server = create_server(
            self.root, "localhost", 0, contract=PINNED_ENDPOINT, resolver=self.resolver
        )
        self.server.ledger_factory = SpyLedger
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=3)
        self.temporary.cleanup()

    @property
    def port(self) -> int:
        return self.server.server_address[1]

    def client_request(self, method: str, path: str, host: str | None = None):
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        headers = {"Host": host} if host is not None else {}
        connection.request(method, path, headers=headers)
        response = connection.getresponse()
        body = response.read()
        headers_out = dict(response.getheaders())
        connection.close()
        return response.status, headers_out, body

    def raw_request(
        self,
        target: str,
        host_headers: list[str],
        *,
        method: str = "GET",
        family: int = socket.AF_INET,
        address: str = "127.0.0.1",
    ) -> bytes:
        sock = socket.socket(family, socket.SOCK_STREAM)
        sock.settimeout(5)
        sock.connect((address, self.port))
        host_lines = "".join(f"Host: {value}\r\n" for value in host_headers)
        raw = (
            f"{method} {target} HTTP/1.1\r\n{host_lines}Connection: close\r\n\r\n"
        ).encode("utf-8")
        sock.sendall(raw)
        chunks = []
        while True:
            chunk = sock.recv(4096)
            if not chunk:
                break
            chunks.append(chunk)
        sock.close()
        return b"".join(chunks)

    def test_socket_is_loopback_and_only_numeric_url_is_advertised(self):
        self.assertEqual(self.server.server_address[0], "127.0.0.1")
        self.assertTrue(
            ipaddress.ip_address(self.server.socket.getsockname()[0]).is_loopback
        )
        audit = self.server.bind_audit
        self.assertEqual(audit["contract"], PINNED_ENDPOINT)
        self.assertEqual(audit["actual_address"], "127.0.0.1")
        self.assertEqual(audit["allowed_authorities"], ["127.0.0.1"])
        self.assertNotIn("localhost", audit["advertised_url"])
        self.assertEqual(self.resolver.calls, ["localhost"])
        bind_events = [event for event in self.server.security_events if event["type"] == "bind"]
        self.assertEqual(bind_events[0]["actual_address"], "127.0.0.1")

    def test_legitimate_browser_and_cli_reads_still_work(self):
        status, _, body = self.client_request("GET", "/api/runs")
        self.assertEqual(status, 200)
        self.assertIn(self.report["run_id"].encode(), body)
        status, headers, body = self.client_request("GET", "/")
        self.assertEqual(status, 200)
        self.assertIn(b"ShardRoute review desk", body)
        self.assertIn("default-src 'none'", headers["Content-Security-Policy"])
        status, _, body = self.client_request(
            "GET", f"/runs/{self.report['run_id']}"
        )
        self.assertEqual(status, 200)
        self.assertIn(b"Read-only archive", body)
        status, _, body = self.client_request("HEAD", "/api/health")
        self.assertEqual(status, 200)
        self.assertEqual(body, b"")
        status, _, _ = self.client_request("POST", "/api/runs")
        self.assertEqual(status, 405)

    def test_attacker_host_gets_generic_400_and_never_touches_ledger_or_ui(self):
        with mock.patch.object(server_module, "render_index") as index_spy, mock.patch.object(
            server_module, "render_run"
        ) as run_spy:
            response = self.raw_request("/api/runs", [f"attacker.example:{self.port}"])
            self.assertTrue(response.startswith(b"HTTP/1.1 400"))
            self.assertNotIn(self.report["run_id"].encode(), response)
            self.assertNotIn(b"finding", response.lower())
            self.assertNotIn(b"attacker.example", response)
            html_response = self.raw_request("/", [f"attacker.example:{self.port}"])
            self.assertTrue(html_response.startswith(b"HTTP/1.1 400"))
            self.assertNotIn(b"ShardRoute review desk", html_response)
        self.assertEqual(SpyLedger.instances, [])
        index_spy.assert_not_called()
        run_spy.assert_not_called()
        denied = [event for event in self.server.security_events if not event.get("allowed")]
        self.assertTrue(denied)
        self.assertEqual(denied[0]["reason"], REASON_HOST_UNAUTHORIZED)
        self.assertEqual(denied[0]["host_normalized"], "attacker.example")

    def test_authorized_request_does_query_the_same_ledger_and_renders(self):
        status, _, body = self.client_request("GET", "/api/runs")
        self.assertEqual(status, 200)
        self.assertEqual(len(SpyLedger.instances), 1)
        self.assertGreaterEqual(SpyLedger.instances[0].calls["list_runs"], 1)
        self.assertIn(self.report["run_id"].encode(), body)

    def test_missing_duplicate_malformed_and_wrong_port_host_over_real_socket(self):
        missing = self.raw_request("/api/runs", [])
        self.assertTrue(missing.startswith(b"HTTP/1.1 400"))
        duplicate = self.raw_request(
            "/api/runs", [f"127.0.0.1:{self.port}", f"127.0.0.1:{self.port}"]
        )
        self.assertTrue(duplicate.startswith(b"HTTP/1.1 400"))
        malformed = self.raw_request("/api/runs", ["127.0.0.1:99999"])
        self.assertTrue(malformed.startswith(b"HTTP/1.1 400"))
        wrong_port = self.raw_request("/api/runs", ["127.0.0.1:9000"])
        self.assertTrue(wrong_port.startswith(b"HTTP/1.1 400"))
        self.assertEqual(SpyLedger.instances, [])
        reasons = {
            event["reason"]
            for event in self.server.security_events
            if not event.get("allowed")
        }
        self.assertIn(REASON_HOST_MISSING, reasons)
        self.assertIn(REASON_HOST_DUPLICATE, reasons)
        self.assertIn(REASON_HOST_INVALID, reasons)
        self.assertIn(REASON_HOST_PORT_MISMATCH, reasons)

    def test_localhost_name_cannot_access_pinned_endpoint(self):
        response = self.raw_request("/api/runs", [f"localhost:{self.port}"])
        self.assertTrue(response.startswith(b"HTTP/1.1 400"))
        response_upper = self.raw_request("/api/runs", [f"LOCALHOST:{self.port}"])
        self.assertTrue(response_upper.startswith(b"HTTP/1.1 400"))
        self.assertEqual(SpyLedger.instances, [])

    def test_absolute_form_conflict_and_matching_authority(self):
        evil = self.raw_request(
            "http://attacker.example:%d/api/runs" % self.port,
            [f"127.0.0.1:{self.port}"],
        )
        self.assertTrue(evil.startswith(b"HTTP/1.1 400"))
        good = self.raw_request(
            "http://127.0.0.1:%d/api/runs" % self.port,
            [f"127.0.0.1:{self.port}"],
        )
        self.assertTrue(good.startswith(b"HTTP/1.1 200"))
        self.assertIn(self.report["run_id"].encode(), good)
        self.assertEqual(len(SpyLedger.instances), 1)

    def test_request_phase_performs_no_dns(self):
        original_getaddrinfo = socket.getaddrinfo

        def refusing_getaddrinfo(*args, **kwargs):
            raise OSError("name resolution disabled during request handling")

        with mock.patch.object(socket, "getaddrinfo", refusing_getaddrinfo):
            # Raw numeric-socket request must succeed without any resolver call.
            response = self.raw_request("/api/runs", [f"127.0.0.1:{self.port}"])
        self.assertTrue(response.startswith(b"HTTP/1.1 200"))
        self.assertEqual(self.resolver.calls, ["localhost"])
        self.assertIs(socket.getaddrinfo, original_getaddrinfo)


class LiveV6ServerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.report = write_test_run(self.root)
        self.server = create_server(self.root, "::1", 0)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=3)
        self.temporary.cleanup()

    def raw_v6(self, target: str, host_headers: list[str]) -> bytes:
        sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        sock.settimeout(5)
        sock.connect(("::1", self.server.server_address[1]))
        host_lines = "".join(f"Host: {value}\r\n" for value in host_headers)
        sock.sendall(f"GET {target} HTTP/1.1\r\n{host_lines}Connection: close\r\n\r\n".encode())
        chunks = []
        while True:
            chunk = sock.recv(4096)
            if not chunk:
                break
            chunks.append(chunk)
        sock.close()
        return b"".join(chunks)

    def test_ipv6_spellings_share_one_permission_set(self):
        port = self.server.server_address[1]
        good = self.raw_v6("/api/runs", [f"[::1]:{port}"])
        self.assertTrue(good.startswith(b"HTTP/1.1 200"))
        equivalent = self.raw_v6("/api/runs", [f"[0:0:0:0:0:0:0:1]:{port}"])
        self.assertTrue(equivalent.startswith(b"HTTP/1.1 200"))
        evil = self.raw_v6("/api/runs", [f"attacker.example:{port}"])
        self.assertTrue(evil.startswith(b"HTTP/1.1 400"))
        v4_authority = self.raw_v6("/api/runs", [f"127.0.0.1:{port}"])
        self.assertTrue(v4_authority.startswith(b"HTTP/1.1 400"))


class PureLocalNameLiveTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.report = write_test_run(self.root)

    def tearDown(self):
        if getattr(self, "server", None) is not None:
            self.server.shutdown()
            self.server.server_close()
            self.temporary.cleanup()

    def start(self, answers):
        resolver = ScriptedResolver([answers])
        self.server = create_server(
            self.root, "localhost", 0, contract=PURE_LOCAL_NAME, resolver=resolver
        )
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        return resolver

    def raw(self, host_header: str) -> bytes:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        sock.connect(("127.0.0.1", self.server.server_address[1]))
        sock.sendall(
            f"GET /api/runs HTTP/1.1\r\nHost: {host_header}\r\nConnection: close\r\n\r\n".encode()
        )
        chunks = []
        while True:
            chunk = sock.recv(4096)
            if not chunk:
                break
            chunks.append(chunk)
        sock.close()
        return b"".join(chunks)

    def test_mixed_resolution_refuses_to_start(self):
        resolver = ScriptedResolver([MIXED_ANSWERS])
        with self.assertRaises(ShardRouteError) as caught:
            create_server(self.root, "localhost", 0, contract=PURE_LOCAL_NAME, resolver=resolver)
        self.assertEqual(caught.exception.code, SERVER_HOST)

    def test_all_loopback_answers_start_and_name_authority_works(self):
        self.start(["127.0.0.1", "::1"])
        port = self.server.server_address[1]
        self.assertEqual(self.server.bind_audit["allowed_authorities"], ["127.0.0.1", "localhost"])
        by_name = self.raw(f"localhost:{port}")
        self.assertTrue(by_name.startswith(b"HTTP/1.1 200"))
        self.assertIn(self.report["run_id"].encode(), by_name)
        by_number = self.raw(f"127.0.0.1:{port}")
        self.assertTrue(by_number.startswith(b"HTTP/1.1 200"))
        evil = self.raw(f"attacker.example:{port}")
        self.assertTrue(evil.startswith(b"HTTP/1.1 400"))
        wrong_port = self.raw("localhost:9000")
        self.assertTrue(wrong_port.startswith(b"HTTP/1.1 400"))


if __name__ == "__main__":
    unittest.main()
