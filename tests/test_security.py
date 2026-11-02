"""
Security regression tests for server.py's HTTP front door.

Stdlib only: `websockets` is stubbed, since process_request does not use it,
and nothing here opens a socket or talks to Plex or Tautulli.

Run:  python3 -m unittest discover -s tests
"""

import asyncio
import importlib.util
import logging
import os
import sys
import tempfile
import types
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

sys.modules.setdefault("websockets", types.ModuleType("websockets"))
_spec = importlib.util.spec_from_file_location("plexonauts_server", ROOT / "server.py")
srv = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = srv  # dataclasses looks the module up by name
_spec.loader.exec_module(srv)
srv.log.setLevel(logging.ERROR)  # rejected requests are expected here


def req(path="/", host="127.0.0.1:8787", **headers):
    if host is not None:
        headers["Host"] = host
    return asyncio.run(srv.process_request(path, headers))


def status(resp):
    return None if resp is None else int(resp[0])


class StaticFiles(unittest.TestCase):
    def test_viewer_served(self):
        self.assertEqual(status(req("/")), 200)
        self.assertEqual(status(req("/viewer.html")), 200)

    def test_app_dir_files_not_served(self):
        for p in ("/.env", "/server.py", "/.git/config", "/run.pid", "/run.log",
                  "/viewer.html.before-lobby-roaming", "/README.md",
                  "/../../../../etc/passwd"):
            with self.subTest(p=p):
                self.assertEqual(status(req(p)), 404)

    def test_viewer_security_headers(self):
        hdrs = dict(req("/", host="plex.lan:8787")[1])
        self.assertIn("frame-ancestors 'none'", hdrs["Content-Security-Policy"])
        self.assertIn("ws://plex.lan:8787", hdrs["Content-Security-Policy"])
        self.assertEqual(hdrs["X-Content-Type-Options"], "nosniff")


class ArtRoute(unittest.TestCase):
    def test_unknown_or_malformed_keys(self):
        for p in ("/art/999999", "/art/abc", "/art/..%2fetc", "/art/../.env"):
            with self.subTest(p=p):
                self.assertEqual(status(req(p)), 404)


class HostCheck(unittest.TestCase):
    def tearDown(self):
        srv.CFG.allowed_hosts = ""

    def test_local_names_allowed(self):
        for h in ("127.0.0.1:8787", "192.168.1.50:8787", "[::1]:8787",
                  "localhost:8787", "plex:8787", "plex.lan:8787",
                  "plex.local", "plex.home.arpa:8787"):
            with self.subTest(h=h):
                self.assertEqual(status(req("/healthz", host=h)), 200)

    def test_rebinding_names_rejected(self):
        for h in ("attacker.example", "plex.lan.attacker.example:8787",
                  "", None, "1.2.3.4:80; script-src *"):
            with self.subTest(h=h):
                self.assertEqual(status(req("/.env", host=h)), 403)
                self.assertEqual(status(req("/", host=h)), 403)

    def test_allowed_hosts_config(self):
        self.assertEqual(status(req("/healthz", host="plex.example.com")), 403)
        srv.CFG.allowed_hosts = "plex.example.com:443, other.example"
        self.assertEqual(status(req("/healthz", host="plex.example.com")), 200)


class OriginCheck(unittest.TestCase):
    WS = {"Upgrade": "websocket"}

    def tearDown(self):
        srv.CFG.allowed_origins = ""

    def test_same_origin_upgrade_allowed(self):
        for host in ("192.168.1.50:8787", "plex.lan:8787", "localhost:9000"):
            with self.subTest(host=host):
                self.assertIsNone(req("/", host=host, Origin=f"http://{host}", **self.WS))

    def test_no_origin_allowed(self):
        self.assertIsNone(req("/", **self.WS))

    def test_foreign_origin_rejected(self):
        for o in ("https://evil.example", "http://localhost:8787", "null"):
            with self.subTest(o=o):
                self.assertEqual(status(req("/", Origin=o, **self.WS)), 403)

    def test_upgrade_with_bad_host_rejected(self):
        self.assertEqual(status(req("/", host="evil.example",
                                    Origin="http://evil.example", **self.WS)), 403)

    def test_allowed_origins_config(self):
        srv.CFG.allowed_origins = "null,https://plex.example.com"
        self.assertIsNone(req("/", Origin="null", **self.WS))
        self.assertIsNone(req("/", Origin="https://plex.example.com", **self.WS))


class DotEnv(unittest.TestCase):
    KEYS = ("TAUTULLI_API_KEY", "PLEX_TOKEN", "BIND_HOST", "BIND_PORT",
            "BROWSE_TTL", "COUCHES", "PE_QUOTED", "PE_HASH", "PE_COMMENT")

    def setUp(self):
        self.saved = {k: os.environ.pop(k) for k in self.KEYS if k in os.environ}

    def tearDown(self):
        for k in self.KEYS:
            os.environ.pop(k, None)
        os.environ.update(self.saved)

    def test_example_file(self):
        srv._load_dotenv(ROOT / ".env.example")
        self.assertEqual(os.environ["BIND_HOST"], "127.0.0.1")
        self.assertEqual(os.environ["TAUTULLI_API_KEY"], "")
        self.assertEqual(int(os.environ["BROWSE_TTL"]), 45)

    def test_inline_comments(self):
        with tempfile.TemporaryDirectory() as d:
            f = Path(d) / ".env"
            f.write_text('PE_QUOTED="a # b"  # c\nPE_HASH=abc#def\n'
                         "PE_COMMENT=   # nothing\nBIND_PORT=8787  # port\n")
            srv._load_dotenv(f)
        self.assertEqual(os.environ["PE_QUOTED"], "a # b")
        self.assertEqual(os.environ["PE_HASH"], "abc#def")
        self.assertEqual(os.environ["PE_COMMENT"], "")
        self.assertEqual(os.environ["BIND_PORT"], "8787")


if __name__ == "__main__":
    unittest.main()
