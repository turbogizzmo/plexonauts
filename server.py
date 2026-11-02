#!/usr/bin/env python3
"""
Plexonauts - backend feed.

Publishes a normalized per-user activity stream over a plain WebSocket for the
top-down "room" frontend.

Two signals, deliberately different in confidence:

  streams  - authoritative, from Tautulli's get_activity API. Tautulli already
             maintains a live connection to Plex and does the session
             bookkeeping; polling Plex /status/sessions separately would
             duplicate a service already running on this host.

  browsing - best effort, from tailing the Plex Media Server debug log. Only
             genuine navigation counts. Idle Plex clients poll the server
             constantly, so naive "any request means active" detection marks
             every connected client as permanently busy. See BROWSE_PATHS /
             IGNORE_PATHS below.

Runs unprivileged: the Plex log is world-readable (plex:plex, mode 644).
Stdlib only except `websockets` (apt: python3-websockets).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import re
import signal
import sys
import threading
import time
import http
import ipaddress
import mimetypes
import socket
import urllib.parse
import urllib.request
from collections import OrderedDict
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any

try:
    import websockets
except ImportError:
    sys.exit("missing dependency: sudo apt install -y python3-websockets")

log = logging.getLogger("activity")

# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------


def _load_dotenv(path: Path) -> None:
    if not path.is_file():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        v = v.strip()
        if v[:1] in ('"', "'") and v[0] in v[1:]:
            v = v[1:v.index(v[0], 1)]
        else:
            # unquoted: a "#" at the start or after whitespace begins a comment
            v = re.sub(r"(^|\s)#.*$", "", v).strip()
        os.environ.setdefault(k.strip(), v)


_load_dotenv(Path(__file__).with_name(".env"))


@dataclass
class Config:
    tautulli_url: str = os.environ.get("TAUTULLI_URL", "http://localhost:8181")
    tautulli_key: str = os.environ.get("TAUTULLI_API_KEY", "")
    plex_url: str = os.environ.get("PLEX_URL", "http://localhost:32400")
    plex_token: str = os.environ.get("PLEX_TOKEN", "")
    log_path: str = os.environ.get(
        "PLEX_LOG",
        "/var/lib/plexmediaserver/Library/Application Support/"
        "Plex Media Server/Logs/Plex Media Server.log",
    )
    bind_host: str = os.environ.get("BIND_HOST", "127.0.0.1")
    bind_port: int = int(os.environ.get("BIND_PORT", "8787"))
    poll_seconds: float = float(os.environ.get("POLL_SECONDS", "4"))
    browse_ttl: float = float(os.environ.get("BROWSE_TTL", "45"))
    couches: int = int(os.environ.get("COUCHES", "8"))
    demo: bool = os.environ.get("DEMO", "").lower() in ("1", "true", "yes")
    # Extra Host names / browser Origins to accept, comma separated. See
    # _host_allowed and _origin_allowed for what is accepted without them.
    allowed_hosts: str = os.environ.get("ALLOWED_HOSTS", "")
    allowed_origins: str = os.environ.get("ALLOWED_ORIGINS", "")


CFG = Config()

# --------------------------------------------------------------------------
# Log parsing
# --------------------------------------------------------------------------

# PMS stamps the account on every request line, e.g.
#   Request: [192.168.1.100:54321 (WAN)] GET /library/sections/1/all ... Signed-in Token (alice)
# Use that instead of harvesting X-Plex-Token values out of the log and mapping
# them to users: the token IS the user's credential, and caching other people's
# tokens to render a dashboard is an unnecessary liability.
RE_REQUEST = re.compile(
    r"Request: \[(?P<ip>[\d.]+):\d+[^\]]*\]\s+"
    r"(?P<method>GET|POST|PUT)\s+(?P<path>/\S*)"
)
RE_USER = re.compile(r"Signed-in Token \((?P<user>[^)]*)\)")

# Genuine navigation. Anything not matched here is not browsing.
BROWSE_PATHS = (
    re.compile(r"^/library/sections/(?P<sid>\d+)/(all|folder|collections|categories)"),
    re.compile(r"^/library/sections/(?P<sid>\d+)/(recentlyAdded|onDeck|firstCharacter)"),
    re.compile(r"^/library/metadata/\d+/children"),
    re.compile(r"^/hubs/sections/(?P<sid>\d+)"),
    re.compile(r"^/hubs/search"),
    re.compile(r"^/search"),
)

# Chatter from idle clients. Measured on this server: a single idle app produced
# 77 /media/providers hits and a steady /:/eventsource/notifications long-poll
# while nobody was watching anything. Counting these as activity is what makes
# an idle-timeout approach never fire.
IGNORE_PATHS = (
    "/:/eventsource/notifications",
    "/:/websockets/notifications",
    "/:/timeline",
    "/:/metadata/notify",
    "/:/prefs",
    "/media/providers",
    "/library/changestamp",
    "/photo/:/transcode",
    "/status/sessions",
    "/identity",
    "/player/",
)

SECTION_ZONE = {"movie": "movies", "show": "tv", "artist": "music", "photo": "movies"}
ZONE_LABEL = {"movies": "Movies", "tv": "TV", "music": "Music"}


def classify(path: str) -> tuple[str, str | None] | None:
    """Return (kind, section_id) for a browse-worthy path, else None."""
    for prefix in IGNORE_PATHS:
        if path.startswith(prefix):
            return None
    for rx in BROWSE_PATHS:
        m = rx.match(path)
        if m:
            sid = m.groupdict().get("sid") if m.groupdict() else None
            return ("browse", sid)
    return None


# --------------------------------------------------------------------------
# HTTP helpers (stdlib; no requests dependency)
# --------------------------------------------------------------------------


def _get_json(url: str, headers: dict[str, str] | None = None, timeout: int = 15) -> Any:
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8", "replace"))


class Tautulli:
    def __init__(self, base: str, key: str):
        self.base, self.key = base.rstrip("/"), key

    def _cmd(self, cmd: str, **params) -> Any:
        q = {"apikey": self.key, "cmd": cmd, **params}
        url = f"{self.base}/api/v2?" + urllib.parse.urlencode(q)
        data = _get_json(url)
        resp = (data or {}).get("response") or {}
        if resp.get("result") != "success":
            raise RuntimeError(f"tautulli {cmd}: {resp.get('message')}")
        return resp.get("data")

    def activity(self) -> list[dict]:
        return (self._cmd("get_activity") or {}).get("sessions", []) or []


def plex_sections(base: str, token: str) -> dict[str, str]:
    """section id -> zone. Best effort; browsing still works without it."""
    try:
        data = _get_json(
            f"{base.rstrip('/')}/library/sections",
            {"X-Plex-Token": token, "Accept": "application/json"},
        )
        out = {}
        for d in data["MediaContainer"].get("Directory", []):
            out[str(d.get("key"))] = SECTION_ZONE.get(d.get("type", ""), "movies")
        return out
    except Exception as e:  # noqa: BLE001
        log.warning("could not fetch library sections (%s); browse zones default to movies", e)
        return {}


# --------------------------------------------------------------------------
# State
# --------------------------------------------------------------------------


# rating_key -> Plex image path, populated only from live sessions. The HTTP
# art route will serve nothing that is not in here, so it cannot be used to
# proxy arbitrary Plex paths.
ART_PATHS: dict[str, str] = {}
ART_CACHE: OrderedDict[str, tuple[str, bytes]] = OrderedDict()
ART_CACHE_MAX = 120


@dataclass
class UserState:
    user: str
    zone: str = "idle"
    detail: str = "Idle"
    art: str | None = None
    # bookkeeping, not serialized to clients
    streaming: bool = field(default=False, repr=False)
    last_browse: float = field(default=0.0, repr=False)
    browse_zone: str = field(default="movies", repr=False)

    def payload(self) -> dict:
        return {"user": self.user, "zone": self.zone,
                "detail": self.detail, "art": self.art}


class Room:
    """Owns per-user state and couch assignment."""

    def __init__(self, couches: int, browse_ttl: float):
        self.users: dict[str, UserState] = {}
        self.couches = couches
        self.browse_ttl = browse_ttl
        self._seats: dict[str, str] = {}  # user -> tvN

    def _seat_for(self, user: str) -> str:
        if user in self._seats:
            return self._seats[user]
        taken = set(self._seats.values())
        for i in range(1, self.couches + 1):
            z = f"tv{i}"
            if z not in taken:
                self._seats[user] = z
                return z
        # Overflow: more concurrent streams than couches. Peak observed on this
        # server is 7, so this is rare but real - park them in the lounge rather
        # than dropping them off the map entirely.
        self._seats[user] = "tv_overflow"
        return "tv_overflow"

    def _release(self, user: str) -> None:
        self._seats.pop(user, None)

    def apply_streams(self, sessions: list[dict]) -> None:
        live = {}
        for s in sessions:
            name = (s.get("friendly_name") or s.get("user") or "").strip()
            if not name:
                continue
            state = (s.get("state") or "").lower()
            title = s.get("grandparent_title") or ""
            sub = s.get("title") or ""
            media = f"{title} - {sub}".strip(" -") if title else sub
            verb = {"playing": "Streaming", "paused": "Paused", "buffering": "Buffering"}.get(
                state, "Streaming"
            )
            # prefer the 16:9 backdrop over the poster - it fits a cinema screen
            key = str(s.get("rating_key") or "")
            img = s.get("art") or s.get("grandparent_thumb") or s.get("thumb") or ""
            art = None
            if key.isdigit() and img:
                ART_PATHS[key] = img
                art = f"/art/{key}"
            live[name] = (f"{verb}: {media}" if media else verb, art)

        for name, (detail, art) in live.items():
            st = self.users.setdefault(name, UserState(name))
            st.streaming = True
            st.zone = self._seat_for(name)
            st.detail = detail
            st.art = art

        for name, st in self.users.items():
            if st.streaming and name not in live:
                st.streaming = False
                self._release(name)

    def note_browse(self, user: str, zone: str) -> None:
        st = self.users.setdefault(user, UserState(user))
        st.last_browse = time.time()
        st.browse_zone = zone

    def settle(self) -> None:
        """Resolve non-streaming users into browsing or idle."""
        now = time.time()
        for st in self.users.values():
            if st.streaming:
                continue
            st.art = None
            if now - st.last_browse <= self.browse_ttl:
                st.zone = st.browse_zone
                st.detail = f"Browsing {ZONE_LABEL.get(st.browse_zone, st.browse_zone)}"
            else:
                st.zone = "idle"
                st.detail = "Idle"

    def snapshot(self) -> list[dict]:
        return [u.payload() for u in self.users.values()]


# --------------------------------------------------------------------------
# Log tailer (blocking; runs in a thread)
# --------------------------------------------------------------------------


def tail_log(path: str, queue: asyncio.Queue, loop: asyncio.AbstractEventLoop, stop: Any) -> None:
    """
    Follow the Plex log, surviving rotation.

    Plex rotates its log every time the server restarts - which now happens
    automatically via the nightly updater - so a tailer that holds a stale fd goes
    permanently deaf without any error. Re-stat the path and compare inodes.
    """
    fh = None
    inode = None
    pending_path: str | None = None

    while not stop.is_set():
        try:
            if fh is None:
                if not os.path.exists(path):
                    time.sleep(2)
                    continue
                fh = open(path, "r", encoding="utf-8", errors="replace")
                fh.seek(0, os.SEEK_END)
                inode = os.fstat(fh.fileno()).st_ino
                log.info("tailing %s (inode %s)", path, inode)

            line = fh.readline()
            if not line:
                try:
                    if os.stat(path).st_ino != inode:
                        log.info("log rotated; reopening")
                        fh.close()
                        fh = None
                        continue
                except FileNotFoundError:
                    fh.close()
                    fh = None
                    continue
                time.sleep(0.25)
                continue

            m = RE_REQUEST.search(line)
            if m:
                pending_path = m.group("path")
                u = RE_USER.search(line)
                if u and pending_path:
                    hit = classify(pending_path)
                    if hit:
                        loop.call_soon_threadsafe(
                            queue.put_nowait, (u.group("user"), hit[1])
                        )
                    pending_path = None
        except Exception as e:  # noqa: BLE001
            log.warning("tailer error: %s", e)
            try:
                if fh:
                    fh.close()
            except Exception:  # noqa: BLE001
                pass
            fh = None
            time.sleep(2)

    if fh:
        fh.close()


# --------------------------------------------------------------------------
# Server
# --------------------------------------------------------------------------


STATIC_DIR = Path(__file__).parent
STATIC_FILES = {"/viewer.html": "viewer.html"}

# Demo mode needs titles and artwork that actually belong together, otherwise
# it shows one film's backdrop under another film's name. Seed a handful of real
# library items so the demo exercises the live code path honestly.
DEMO_CATALOG: list[dict] = []
# After a failed seed, wait this long before letting a request try again, so
# repeated /demo/catalog or /art/demoN hits can't keep Plex calls queued.
SEED_RETRY_SECONDS = 60.0
_seed_lock = asyncio.Lock()
_seed_retry_at = 0.0


def _seed_demo_catalog(n: int = 48) -> None:
    """
    Build a varied demo dataset: a random sample drawn across every movie and
    show section, not the first few titles of one library. Each entry pairs a
    real title with that title's own artwork, so a demo screen never shows one
    film's backdrop under another film's name.
    """
    if DEMO_CATALOG:
        return
    try:
        hdr = {"X-Plex-Token": CFG.plex_token, "Accept": "application/json"}
        base = CFG.plex_url.rstrip("/")
        secs = _get_json(f"{base}/library/sections", hdr)["MediaContainer"]["Directory"]
        pools = [d for d in secs if d.get("type") in ("movie", "show")]
        if not pools:
            return
        per = max(4, n // len(pools) + 2)
        # built locally and published at the end: this runs in a worker thread
        catalog: list[dict] = []
        paths: dict[str, str] = {}
        for d in pools:
            key = d["key"]
            # totalSize comes back even with a zero-size page, so we can pick a
            # random window instead of always reading the alphabetical head
            head = _get_json(
                f"{base}/library/sections/{key}/all"
                f"?X-Plex-Container-Start=0&X-Plex-Container-Size=0", hdr)["MediaContainer"]
            total = int(head.get("totalSize") or head.get("size") or 0)
            if total <= 0:
                continue
            start = random.randint(0, max(0, total - per))
            items = _get_json(
                f"{base}/library/sections/{key}/all"
                f"?X-Plex-Container-Start={start}&X-Plex-Container-Size={per}", hdr
            )["MediaContainer"].get("Metadata", [])
            for it in items:
                img = it.get("art") or it.get("thumb")
                if not img:
                    continue
                title = it.get("title", "Unknown")
                if it.get("type") == "show":
                    title = f"{title} - S1"
                slot = f"demo{len(catalog)}"
                paths[slot] = img
                catalog.append({"title": title, "art": f"/art/{slot}"})
        random.shuffle(catalog)
        if catalog:
            paths.setdefault("demo", paths[catalog[0]["art"].rsplit("/", 1)[1]])
        ART_PATHS.update(paths)
        DEMO_CATALOG[:] = catalog
        log.info("demo catalog seeded with %d titles from %d sections",
                 len(DEMO_CATALOG), len(pools))
    except Exception as e:  # noqa: BLE001
        log.warning("demo catalog seed failed: %s", e)


async def _ensure_demo_catalog() -> None:
    """Seed off the event loop, one attempt at a time, with a retry backoff."""
    global _seed_retry_at
    if DEMO_CATALOG or time.monotonic() < _seed_retry_at:
        return
    async with _seed_lock:
        if DEMO_CATALOG or time.monotonic() < _seed_retry_at:
            return
        await asyncio.to_thread(_seed_demo_catalog)
        if not DEMO_CATALOG:
            _seed_retry_at = time.monotonic() + SEED_RETRY_SECONDS


def _fetch_art(key: str) -> tuple[str, bytes]:
    q = urllib.parse.urlencode({
        "width": 640, "height": 360, "minSize": 1, "upscale": 1,
        "url": ART_PATHS[key]})
    # token as a header, not a query parameter, so it stays out of access logs
    req = urllib.request.Request(
        f"{CFG.plex_url.rstrip('/')}/photo/:/transcode?{q}",
        headers={"X-Plex-Token": CFG.plex_token})
    with urllib.request.urlopen(req, timeout=15) as r:
        return r.headers.get("Content-Type", "image/jpeg"), r.read()


# Host header: a name, IPv4 or bracketed IPv6, optional port. Anything else is
# rejected before it can be echoed into a response header.
RE_HOST = re.compile(r"^(?P<name>[a-z0-9.-]+|\[[0-9a-f:.]+\])(?::\d{1,5})?$")


def _csv(value: str) -> set[str]:
    return {x.strip().lower() for x in value.split(",") if x.strip()}


def _hostname(hostport: str) -> str | None:
    m = RE_HOST.match(hostport.strip().lower())
    return m.group("name").strip("[]").rstrip(".") if m else None


# Names that public DNS can't hand out, so a rebinding attacker can't own them:
# single-label names (plex) and these LAN-only suffixes (plex.lan, plex.local).
LAN_SUFFIXES = (".localhost", ".local", ".lan", ".home", ".home.arpa",
                ".internal", ".localdomain")

_OWN_NAMES: set[str] = set()


def _own_names() -> set[str]:
    """This machine's own hostname and FQDN, looked up once."""
    if not _OWN_NAMES:
        _OWN_NAMES.add("localhost")
        for n in (socket.gethostname(), socket.getfqdn()):
            n = n.strip().lower().rstrip(".")
            if n:
                _OWN_NAMES.add(n)
    return _OWN_NAMES


def _host_allowed(hostport: str) -> bool:
    """
    DNS-rebinding defence. A rebinding page reaches us under a public domain
    the attacker controls, so accept only names that can't be one: IP literals
    (the LAN IP or 127.0.0.1), single-label and LAN_SUFFIXES names, this
    machine's hostname/FQDN, and ALLOWED_HOSTS ("*" turns the check off).
    """
    host = _hostname(hostport)
    if not host:
        return False
    extra = {_hostname(h) or h for h in _csv(CFG.allowed_hosts)}
    if "*" in extra or host in extra:
        return True
    try:
        ipaddress.ip_address(host)
        return True
    except ValueError:
        pass
    return ("." not in host or host.endswith(LAN_SUFFIXES)
            or host in _own_names())


def _origin_allowed(origin: str | None, hostport: str) -> bool:
    """
    Cross-site WebSocket hijacking defence. Browsers always send Origin on a
    WebSocket upgrade, so a page may connect only from the origin it was served
    from (the Host it asked for) or an origin listed in ALLOWED_ORIGINS. A
    missing Origin means a non-browser client and is let through. file://
    pages send "null"; add null to ALLOWED_ORIGINS to allow them.
    """
    if origin is None:
        return True
    origin = origin.strip().lower()
    extra = _csv(CFG.allowed_origins)
    if "*" in extra or origin in extra:
        return True
    u = urllib.parse.urlsplit(origin)
    return u.scheme in ("http", "https") and u.netloc == hostport.strip().lower()


# viewer.html is one self-contained page: inline script and style, Google
# Fonts, artwork from /art and its feed on the same host. connect-src names
# ws:// explicitly because older Safari does not match it with 'self'.
VIEWER_CSP = (
    "default-src 'none'; script-src 'unsafe-inline'; "
    "style-src 'unsafe-inline' https://fonts.googleapis.com; "
    "font-src https://fonts.gstatic.com; img-src 'self'; "
    "connect-src 'self' ws://{host} wss://{host}; "
    "base-uri 'none'; form-action 'none'; frame-ancestors 'none'"
)

_REJECTED: set[str] = set()


def _forbidden(what: str, value: str):
    if value not in _REJECTED and len(_REJECTED) < 64:
        _REJECTED.add(value)
        log.warning("rejected request with %s %r (see ALLOWED_HOSTS / "
                    "ALLOWED_ORIGINS)", what, value)
    return (http.HTTPStatus.FORBIDDEN, [("Content-Type", "text/plain")],
            f"{what} not allowed\n".encode())


async def process_request(path: str, request_headers):
    """
    Serve the viewer over HTTP on the same port the WebSocket uses.

    Returning None lets the request continue to the WebSocket handshake, so
    ws:// and http:// share one port - one service to run, one port to open,
    and the page can always reach its own feed with no cross-origin setup.
    """
    hostport = request_headers.get("Host", "")
    if not _host_allowed(hostport):
        return _forbidden("host", hostport)

    if request_headers.get("Upgrade", "").lower() == "websocket":
        origin = request_headers.get("Origin")
        if not _origin_allowed(origin, hostport):
            return _forbidden("origin", origin)
        return None

    route = urllib.parse.urlparse(path).path
    if route in ("/", "/index.html"):
        route = "/viewer.html"
    if route.startswith("/art/"):
        key = route[5:]
        if key.startswith("demo") and key not in ART_PATHS:
            await _ensure_demo_catalog()
        if (not key.isdigit() and not key.startswith("demo")) or key not in ART_PATHS:
            return (http.HTTPStatus.NOT_FOUND, [("Content-Type", "text/plain")], b"no art\n")
        if key in ART_CACHE:
            ART_CACHE.move_to_end(key)
            ctype, body = ART_CACHE[key]
        else:
            try:
                ctype, body = await asyncio.to_thread(_fetch_art, key)
            except Exception as e:  # noqa: BLE001
                log.warning("art fetch failed for %s: %s", key, e)
                return (http.HTTPStatus.BAD_GATEWAY,
                        [("Content-Type", "text/plain")], b"art unavailable\n")
            ART_CACHE[key] = (ctype, body)
            while len(ART_CACHE) > ART_CACHE_MAX:
                ART_CACHE.popitem(last=False)
        return (http.HTTPStatus.OK,
                [("Content-Type", ctype), ("Content-Length", str(len(body))),
                 ("Cache-Control", "public, max-age=3600"),
                 ("X-Content-Type-Options", "nosniff")], body)

    if route == "/demo/catalog":
        await _ensure_demo_catalog()
        body = json.dumps(DEMO_CATALOG).encode()
        return (http.HTTPStatus.OK,
                [("Content-Type", "application/json"),
                 ("Content-Length", str(len(body))),
                 ("X-Content-Type-Options", "nosniff")], body)

    if route == "/healthz":
        return (http.HTTPStatus.OK, [("Content-Type", "text/plain")], b"ok\n")

    # Only files listed here are served. The app directory also holds .env
    # (Plex token, Tautulli key), server.py and backups, so never fall back to
    # serving arbitrary paths from it.
    name = STATIC_FILES.get(route)
    if name is None:
        return (http.HTTPStatus.NOT_FOUND, [("Content-Type", "text/plain")], b"not found\n")

    target = STATIC_DIR / name
    ctype = mimetypes.guess_type(str(target))[0] or "application/octet-stream"
    body = target.read_bytes()
    return (
        http.HTTPStatus.OK,
        [("Content-Type", ctype), ("Content-Length", str(len(body))),
         ("Cache-Control", "no-store"),
         # hostport already passed RE_HOST, so it is safe to echo here
         ("Content-Security-Policy", VIEWER_CSP.format(host=hostport.strip())),
         ("X-Content-Type-Options", "nosniff"),
         ("Referrer-Policy", "no-referrer")],
        body,
    )


class Hub:
    def __init__(self):
        self.clients: set = set()

    async def register(self, ws, room: Room):
        self.clients.add(ws)
        await ws.send(json.dumps({"type": "snapshot", "users": room.snapshot()}))

    def unregister(self, ws):
        self.clients.discard(ws)

    async def broadcast(self, msg: dict):
        if not self.clients:
            return
        data = json.dumps(msg)
        dead = []
        for ws in list(self.clients):
            try:
                await ws.send(data)
            except Exception:  # noqa: BLE001
                dead.append(ws)
        for ws in dead:
            self.clients.discard(ws)


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)-5s %(message)s"
    )
    room = Room(CFG.couches, CFG.browse_ttl)
    hub = Hub()
    loop = asyncio.get_running_loop()
    browse_q: asyncio.Queue = asyncio.Queue()

    sections = {} if CFG.demo else plex_sections(CFG.plex_url, CFG.plex_token)
    tau = Tautulli(CFG.tautulli_url, CFG.tautulli_key)

    stop = asyncio.Event()

    class _Stop:
        def is_set(self_inner):  # noqa: N805
            return stop.is_set()

    # A daemon thread, not run_in_executor: the default executor's threads are
    # non-daemon and joined at interpreter exit, which left the process alive
    # but no longer serving after shutdown.
    if not CFG.demo:
        threading.Thread(
            target=tail_log,
            args=(CFG.log_path, browse_q, loop, _Stop()),
            name="log-tailer",
            daemon=True,
        ).start()

    async def drain_browse():
        while not stop.is_set():
            try:
                user, sid = await asyncio.wait_for(browse_q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            room.note_browse(user, sections.get(str(sid), "movies"))

    async def poll_streams():
        last: dict[str, dict] = {}
        while not stop.is_set():
            try:
                sessions = [] if CFG.demo else await asyncio.to_thread(tau.activity)
                room.apply_streams(sessions)
            except Exception as e:  # noqa: BLE001
                log.warning("tautulli poll failed: %s", e)
            room.settle()

            for p in room.snapshot():
                if last.get(p["user"]) != p:
                    last[p["user"]] = p
                    await hub.broadcast({"type": "activity", **p})
            await asyncio.sleep(CFG.poll_seconds)

    # websockets<11 calls handler(ws, path); >=11 calls handler(ws).
    async def handler(ws, path=None):  # noqa: ARG001
        await hub.register(ws, room)
        try:
            async for _ in ws:
                pass
        finally:
            hub.unregister(ws)

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    _own_names()  # resolve once now, not inside the first request

    try:
        server = await websockets.serve(
            handler, CFG.bind_host, CFG.bind_port,
            process_request=process_request, reuse_port=False
        )
    except OSError as e:
        log.error("cannot bind %s:%s - %s", CFG.bind_host, CFG.bind_port, e)
        raise SystemExit(1)

    async with server:
        shown = "<server-ip>" if CFG.bind_host == "0.0.0.0" else CFG.bind_host
        log.info("viewer   http://%s:%s/", shown, CFG.bind_port)
        log.info("feed     ws://%s:%s/", shown, CFG.bind_port)
        if CFG.bind_host == "0.0.0.0":
            log.warning("bound to all interfaces - this feed shows real usernames "
                        "and what they are watching")
        try:
            await asyncio.gather(poll_streams(), drain_browse())
        finally:
            log.info("shutting down")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
