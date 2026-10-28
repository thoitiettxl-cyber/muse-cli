"""A warm, shared, self-healing gateway connection.

One multiplexed Noise connection carries every request, download and
subscription of a process (the Telegram bot, or one `muse-cli wait`). A
heartbeat thread pings it when nothing has arrived for a while and replaces
it when the ping fails or the connection drops; callers get a SharedConn
that always uses the current connection and repeats a request on a new one
when that is safe. Connecting is bounded in time, so a server that accepts
the socket and then goes silent cannot hang the caller.
"""

import logging
import threading
import time
from contextlib import contextmanager

from . import cli
from .gateway import AuthError

log = logging.getLogger("muse_cli.conn")

CONNECT_TIMEOUT = 60   # seconds for a whole connect (HTTPS token fetches + WS + Noise)


def bounded_connect(connector, cfg, timeout=None):
    """connector(cfg) with an upper bound on time. A connect that hangs (e.g.
    the server stops answering mid-handshake) raises TimeoutError; if it
    completes later, that connection is closed instead of leaking."""
    timeout = CONNECT_TIMEOUT if timeout is None else timeout
    box = {}
    done = threading.Event()
    abandoned = threading.Event()
    lock = threading.Lock()
    claimed = [False]

    def run():
        try:
            box["gw"] = connector(cfg)
        except BaseException as e:  # noqa: BLE001 - handed to the caller
            box["err"] = e
        done.set()
        # Ownership under the lock: the worker closes a late gateway only
        # if the caller did not claim it in the same instant (the old
        # unlocked check could close a gateway the caller just took).
        with lock:
            if abandoned.is_set() and "gw" in box and not claimed[0]:
                try:
                    box["gw"].close()
                except Exception:  # noqa: BLE001
                    pass
                del box["gw"]
    t = threading.Thread(target=run, daemon=True, name="muse-connect")
    t.start()
    if not done.wait(timeout):
        abandoned.set()
        with lock:
            if done.is_set() and "gw" in box:
                claimed[0] = True
                return box["gw"]
        raise TimeoutError(f"gateway connect timed out after {timeout:.0f}s")
    if "err" in box:
        raise box["err"]
    return box["gw"]


class Muse:
    """The Muse calls the bot makes, on the active muse-cli profile.

    All calls share one warm, multiplexed gateway connection (requests,
    downloads and subscriptions from any thread run as separate streams on
    it), kept alive with the web app's heartbeat and reopened transparently
    when it drops. `connector(cfg)` defaults to `cli.connect`; tests pass a
    fake gateway. A connector whose gateways cannot be shared (no `lease()`)
    gets one connection per call, as before.

    A dropped connection is invisible to callers: connect() hands out a
    SharedConn that always uses the current connection, and repeats a request
    on a new one when that is safe (nothing was sent yet, or a read). Only a
    connection that stays down past DOWN_NOTICE seconds (or DOWN_FAILS failed
    reconnects) is reported, once, through on_down; on_up follows on recovery.
    """

    HEARTBEAT = 25     # seconds of idleness before a connection.ping
    DOWN_NOTICE = 60   # seconds without a working connection before on_down
    DOWN_FAILS = 5     # ... or this many failed reconnects in a row
    RETRY_FOR = 30     # seconds a request keeps retrying across a reconnect
    FAIL_SPACING = 2   # failed reconnects closer than this count once

    def __init__(self, connector=None, on_auth_error=None, on_auth_ok=None,
                 on_down=None, on_up=None, cfg=None, log=None, connect_timeout=None):
        self.connector = connector
        self.cfg = cfg
        self.log = log or globals()["log"]
        self.connect_timeout = connect_timeout
        self.on_auth_error = on_auth_error
        self.on_auth_ok = on_auth_ok
        self.on_down = on_down
        self.on_up = on_up
        self.shared = None
        self.shared_lock = threading.Lock()
        self.connects = 0          # real connections opened (for tests / logs)
        self.drops = 0             # connections lost (for tests / logs)
        self.down_since = None     # first moment without a working connection
        self.down_fails = 0
        self.down_notified = False
        self._last_fail = 0.0
        self.down_lock = threading.Lock()
        self._hb = None
        self._hb_stop = threading.Event()
        self._hb_lock = threading.Lock()
        self._hb_last_ping = 0.0  # monotonic time of the last ping attempt

    def reload(self):
        old, self.cfg = self.cfg, cli.load_config()
        ident = lambda c: c and (c.get("cookies_file"), c.get("vm_id"), c.get("node_id"))
        if old is not None and ident(old) != ident(self.cfg):
            self.reset()       # another profile / VM: the warm connection is the wrong one
        return self.cfg

    def config(self):
        return self.cfg if self.cfg is not None else self.reload()

    # -- health: quiet on a blip, one notice when it stays down -------------------
    # down_since / _last_fail are monotonic: they measure durations, and a
    # wall-clock jump must neither hide an outage nor invent one.
    def mark_down(self, err, failed_connect=False):
        notify = False
        with self.down_lock:
            if self.down_since is None:
                self.down_since = time.monotonic()
            if failed_connect and time.monotonic() - self._last_fail >= self.FAIL_SPACING:
                # several threads failing at the same moment count as one attempt
                self.down_fails += 1
                self._last_fail = time.monotonic()
            if not self.down_notified and (time.monotonic() - self.down_since >= self.DOWN_NOTICE
                                           or self.down_fails >= self.DOWN_FAILS):
                self.down_notified = notify = True
            since = self.down_since
        if notify:
            self.log.warning("gateway down for %.0fs (%d failed reconnects): %s",
                        time.monotonic() - since, self.down_fails, err)
            if self.on_down:
                try:
                    self.on_down(err, time.monotonic() - since)
                except Exception as e:  # noqa: BLE001
                    self.log.warning("down notice: %s", e)

    def mark_up(self):
        with self.down_lock:
            if self.down_since is None:
                return
            was, since = self.down_notified, self.down_since
            self.down_since, self.down_fails, self.down_notified = None, 0, False
            self._last_fail = 0.0
        if was and self.on_up:
            try:
                self.on_up(time.monotonic() - since)
            except Exception as e:  # noqa: BLE001
                self.log.warning("up notice: %s", e)

    def _open(self, cfg):
        try:
            gw = bounded_connect(self.connector or cli.connect, cfg or self.config(),
                                 self.connect_timeout)
        except AuthError as e:
            if self.on_auth_error:
                self.on_auth_error(e)
            raise
        except Exception as e:
            self.mark_down(e, failed_connect=True)
            raise
        self.connects += 1
        if self.on_auth_ok:
            self.on_auth_ok()
        return gw

    def _gw_age(self, gw, mono_attr, wall_attr):
        """Seconds since GW's activity timestamp, or 0 when unknown.

        Monotonic stamps (real Gateway) are preferred; wall-clock stamps
        (fakes, which set last_recv/last_used from time.time()) fall back
        to a wall-clock difference. A missing stamp means 'just now'."""
        v = getattr(gw, mono_attr, None)
        if v is not None:
            return time.monotonic() - v
        v = getattr(gw, wall_attr, None)
        if v is not None:
            return time.time() - v
        return 0.0

    def _probe_stale(self, g):
        """Ping a shared connection that claims to be alive but has neither
        sent nor received anything for a heartbeat interval: `alive` alone
        cannot prove a silently dead socket. Returns False when the probe
        fails; the caller drops and reopens. Never drops itself: live()
        calls it both outside and under shared_lock."""
        if self._gw_age(g, "last_recv_mono", "last_recv") <= self.HEARTBEAT:
            return True
        if self._gw_age(g, "last_used_mono", "last_used") <= self.HEARTBEAT:
            return True
        ping = getattr(g, "ping", None)
        if ping is None:
            return True
        self._hb_last_ping = time.monotonic()
        try:
            ping(timeout=5)
        except Exception as e:  # noqa: BLE001 - stale: caller reopens
            self.log.info("gateway liveness probe failed (%s); reconnecting", e)
            return False
        return True

    def live(self, cfg=None):
        """The shared connection, (re)opened if it is missing or dead.

        A connection that only *claims* to be alive is probed first when
        it has been quiet for a heartbeat interval (see _probe_stale)."""
        g = self.shared
        if g is not None and getattr(g, "alive", False):
            if self._probe_stale(g):
                return g
            self.drop(g)
        with self.shared_lock:
            g = self.shared
            old = None
            if g is not None:
                if getattr(g, "alive", False) and self._probe_stale(g):
                    return g
                old = g
                self.shared = None
                self.drops += 1
                self.mark_down(getattr(g, "dead", None) or "closed")
                self.log.info("gateway connection dropped (%s); reconnecting",
                              getattr(g, "dead", None))
            t0 = time.monotonic()
            gw = self._open(cfg)
            if hasattr(gw, "lease"):
                self.shared = gw
            else:
                # Cannot be shared: the caller owns (and closes) it. Forget
                # any dead shared gateway so the next call opens fresh.
                self.shared = None
            self.log.info("gateway connected in %.2fs", time.monotonic() - t0)
        self.mark_up()
        if old is not None:
            old.close()
        return gw

    def connect(self, cfg=None):
        """A gateway to use and close(): a SharedConn on the shared connection
        (close() leaves it up), or a fresh connection if it cannot be shared."""
        g = self.live(cfg)
        if not hasattr(g, "lease"):
            return g
        return SharedConn(self, cfg)

    def drop(self, g):
        """Forget a connection that failed its heartbeat; the next call reconnects."""
        with self.shared_lock:
            if self.shared is g:
                self.shared = None
                self.drops += 1
        self.mark_down("heartbeat failed")
        g.close()

    def adopt(self, gw):
        """Use an already open connection as the shared one."""
        with self.shared_lock:
            old, self.shared = self.shared, gw
            self.connects += 1
        if old is not None and old is not gw:
            old.close()

    def warm(self):
        """Open the shared connection now so the first message does not pay for it."""
        try:
            self.connect().close()
        except Exception as e:  # noqa: BLE001
            self.log.warning("gateway warm-up failed: %s", e)

    def reset(self):
        """Drop the shared connection (new cookies / profile)."""
        with self.shared_lock:
            old, self.shared = self.shared, None
        if old is not None:
            old.close()

    def start_heartbeat(self, interval=None):
        """Start the heartbeat thread, restarting it when it died and after
        a stop: a thread reference alone never proves it is alive, and a
        set stop event from an earlier run must not kill the new thread."""
        with self._hb_lock:
            if self._hb is not None and self._hb.is_alive():
                return
            self._hb_stop = threading.Event()
            interval = interval or self.HEARTBEAT

            def loop():
                while not self._hb_stop.wait(min(5, interval)):
                    try:
                        self._hb_tick(interval)
                    except Exception as e:  # noqa: BLE001 - one bad tick
                        # must not kill the supervisor thread
                        self.log.debug("heartbeat tick failed: %s", e)
            self._hb = threading.Thread(target=loop, daemon=True, name="muse-heartbeat")
            self._hb.start()

    def _hb_tick(self, interval):
        g = self.shared
        if g is None or not getattr(g, "alive", True):
            if g is None and self.down_since is None and self.connects == 0:
                return      # never connected yet: the first call opens it
            try:
                self.live()   # keep trying while down (quietly; see mark_down)
            except Exception as e:  # noqa: BLE001
                self.log.info("gateway reconnect failed: %s", e)
            return
        quiet = self._gw_age(g, "last_recv_mono", "last_recv")
        used = self._gw_age(g, "last_used_mono", "last_used")
        if used < interval and quiet < interval:
            return      # in use and the server is talking: alive
        # Idle, or nothing has arrived for a while (a connection can
        # die silently: no close, no error): a ping settles it.
        self._hb_last_ping = time.monotonic()
        try:
            g.ping()
        except Exception as e:  # noqa: BLE001
            self.log.info("gateway heartbeat failed (%s); reconnecting", e)
            self.drop(g)
            try:
                self.live()
            except Exception as e2:  # noqa: BLE001
                self.log.info("gateway reconnect failed: %s", e2)

    def stop_heartbeat(self):
        with self._hb_lock:
            self._hb_stop.set()
            self._hb = None

    def close(self):
        """Stop the heartbeat and close the connection (a CLI command is done)."""
        self.stop_heartbeat()
        self.reset()

    @contextmanager
    def gw(self):
        g = self.connect()
        try:
            yield g
        finally:
            g.close()

    def call(self, method, **kw):
        with self.gw() as g:
            return g.call_json(method, **kw)

    def download(self, ref):
        with self.gw() as g:
            return g.download_file(ref)

    def history(self, session_id=None, limit=20):
        body = {"limit": limit}
        if session_id:
            body["session_id"] = session_id
        evs = self.call("chat.history", body=body).get("chat_events", [])
        return sorted(evs, key=lambda e: e.get("seq") or 0)

    def fs(self, fn):
        from .fs import FS
        with self.gw() as g:
            return fn(FS(g))


_READ_WORDS = ("history", "list", "read", "stat", "get", "identity", "status", "unread",
               "ping", "approvals", "usage", "session.info")


def _safe_to_repeat(method, route=None):
    """A request that only reads, so repeating it after a lost reply is harmless."""
    from .gateway import ROUTES
    r = route or ROUTES.get(method) or {}
    if str(r.get("http") or "").upper() == "GET":
        return True
    return method != "chat.stream" and any(w in method for w in _READ_WORDS)


class SharedConn:
    """What Muse.connect() hands out: the shared connection as it is at each
    call. A request on a connection that just dropped is repeated on the new
    one when nothing was sent yet or it only reads; subscriptions are opened
    on the current connection (their callers resubscribe on a drop)."""

    def __init__(self, muse, cfg=None):
        self._muse = muse
        self._cfg = cfg

    def _gw(self):
        return self._muse.live(self._cfg)

    def __getattr__(self, name):
        return getattr(self._gw(), name)

    @property
    def alive(self):
        g = self._muse.shared
        return g is not None and getattr(g, "alive", False)

    def lease(self):
        return self

    def close(self):
        pass

    def _retry(self, what, safe, fn, deadline=None):
        """Repeat FN on a new connection while SAFE and time remains.

        DEADLINE is the caller's remaining monotonic deadline: the retry
        budget is the sooner of RETRY_FOR and the caller's deadline, so a
        retry storm cannot push a bounded wait past its own bound."""
        from .gateway import ConnectionDown
        own_deadline = time.monotonic() + self._muse.RETRY_FOR
        deadline = own_deadline if deadline is None else min(own_deadline, deadline)
        pause = 0.25
        while True:
            try:
                g = self._gw()
            except AuthError:
                raise
            except Exception as e:  # noqa: BLE001 - the reconnect failed: keep trying a while
                if time.monotonic() + pause > deadline:
                    raise
                self._muse.log.info("%s: reconnect failed (%s); retrying", what, e)
            else:
                try:
                    return fn(g)
                except ConnectionError as e:
                    if not (isinstance(e, ConnectionDown) or safe) \
                            or time.monotonic() + pause > deadline:
                        raise
                    self._muse.log.info("%s: %s; retrying on a new connection", what, e)
            time.sleep(pause)
            pause = min(pause * 2, 4)

    def _open(self, method, path_params=None, body=None, query=None, route=None, track=True):
        return self._retry(method, False, lambda g: g._open(
            method, path_params=path_params, body=body, query=query, route=route, track=track))

    def call_json(self, method, *a, **kw):
        deadline = kw.pop("_deadline", None)
        return self._retry(method, _safe_to_repeat(method),
                           lambda g: g.call_json(method, *a, **kw), deadline=deadline)

    def request(self, method, *a, **kw):
        deadline = kw.pop("_deadline", None)
        return self._retry(method, _safe_to_repeat(method, kw.get("route")),
                           lambda g: g.request(method, *a, **kw), deadline=deadline)

    def download_file(self, ref, *a, **kw):
        deadline = kw.pop("_deadline", None)
        return self._retry("download", True, lambda g: g.download_file(ref, *a, **kw),
                           deadline=deadline)

    def subscribe_raw(self, method, *a, **kw):
        # only a connection that is down before the stream opens is retried here
        from .gateway import ConnectionDown
        for attempt in (0, 1, 2):
            try:
                return self._gw().subscribe_raw(method, *a, **kw)
            except ConnectionDown:
                if attempt == 2:
                    raise
                time.sleep(0.25)
