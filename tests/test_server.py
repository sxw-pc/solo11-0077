import http.client
import json
import tempfile
import threading
import unittest
from pathlib import Path

from shardroute.errors import ShardRouteError
from shardroute.server import create_server

from support import write_test_run

class ServerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.report = write_test_run(self.root)
        self.server = create_server(self.root, "127.0.0.1", 0)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=3)
        self.temporary.cleanup()
    def request(self, method, path):
        connection = http.client.HTTPConnection("127.0.0.1", self.server.server_port, timeout=5)
        connection.request(method, path)
        response = connection.getresponse()
        body = response.read()
        headers = dict(response.getheaders())
        connection.close()
        return response.status, headers, body
    def test_health_index_and_apis(self):
        status, _, body = self.request("GET", "/api/health")
        self.assertEqual(status, 200)
        self.assertTrue(json.loads(body)["read_only"])
        status, headers, body = self.request("GET", "/")
        self.assertEqual(status, 200)
        self.assertIn("ShardRoute review desk", body.decode())
        self.assertIn("default-src 'none'", headers["Content-Security-Policy"])
        status, _, body = self.request("GET", "/api/runs")
        runs = json.loads(body)
        self.assertEqual(status, 200)
        self.assertEqual(runs[0]["run_id"], self.report["run_id"])
        status, _, body = self.request(
            "GET", f"/api/runs/{self.report['run_id']}/findings?severity=critical"
        )
        self.assertEqual(status, 200)
        self.assertEqual(len(json.loads(body)), 1)
        status, _, body = self.request("GET", f"/runs/{self.report['run_id']}")
        self.assertIn("Read-only archive", body.decode())
        self.assertEqual(status, 200)
    def test_read_only_methods_and_missing_resources(self):
        status, headers, _ = self.request("POST", "/api/runs")
        self.assertEqual(status, 405)
        self.assertEqual(headers["Allow"], "GET, HEAD")
        status, _, _ = self.request("GET", "/api/runs/run_missing")
        self.assertEqual(status, 404)
        status, _, body = self.request("HEAD", "/api/health")
        self.assertEqual(status, 200)
        self.assertEqual(body, b"")
        status, _, _ = self.request("GET", "/../etc/passwd")
        self.assertEqual(status, 400)
    def test_non_loopback_host_is_rejected(self):
        with self.assertRaises(ShardRouteError) as caught:
            create_server(self.root, "0.0.0.0", 0)
        self.assertEqual(caught.exception.code, "SERVER_HOST")
        with self.assertRaises(ShardRouteError):
            create_server(self.root / "missing", "127.0.0.1", 0)
