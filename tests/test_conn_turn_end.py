"""D1 bounded I/O + monotonic clocks (T1/T2) and D2 supervised heartbeat (T3).

Seams: Gateway._write below the curl seam, SharedConn._retry, Muse
heartbeat lifecycle, bounded_connect ownership. Fakes never touch the
network; clock tests jump the WALL clock and require the monotonic
deadlines to be unaffected.
"""
import threading
import time

import pytest

from muse_cli import cli, conn, gateway
from muse_cli.gateway import ConnectionDown, Gateway
from curl_cffi.const import CurlECode
from curl_cffi.curl import CurlError
from test_wait_reconnect import FakeGW, World


# -- T1: _write ----------------------------------------------------------------

class FakeCurl:
    def __init__(self, behavior):
        self.behavior = behavior  # callable(data) -> bytes sent, or raises
        self.sent = bytearray()

    def getinfo(self, _):
        return -1

    def ws_send(self, data, _flag):
        n = self.behavior(bytes(data))
        self.sent += bytes(data[:n])
        return n


class FakeWS:
    def __init__(self, curl):
        self.curl = curl


def _bare_gw(curl):
    g = object.__new__(Gateway)
    g.ws = FakeWS(curl)
    g._io_lock = threading.Lock()
    return g


def test_write_zero_byte_send_is_bounded():
    g = _bare_gw(FakeCurl(lambda data: 0))
    t0 = time.monotonic()
    with pytest.raises(ConnectionError):
        g._write(b"x" * 64, timeout=0.3)
    assert time.monotonic() - t0 < 2


def test_write_again_forever_is_bounded(monkeypatch):
    def raiser(data):
        raise CurlError("again", CurlECode.AGAIN)

    def never_writable(r, w, x, t):
        time.sleep(min(t, 0.02))
        return [], [], []
    monkeypatch.setattr("select.select", never_writable)
    g = _bare_gw(FakeCurl(raiser))
    t0 = time.monotonic()
    with pytest.raises(ConnectionError):
        g._write(b"x" * 64, timeout=0.3)
    assert time.monotonic() - t0 < 2


def test_write_partial_then_complete():
    calls = {"n": 0}

    def partial(data):
        calls["n"] += 1
        return 3 if calls["n"] == 1 else len(data)

    curl = FakeCurl(partial)
    g = _bare_gw(curl)
    g._write(b"abcdefghij", timeout=5)
    assert bytes(curl.sent) == b"abcdefghij"


def test_write_success_returns_promptly():
    curl = FakeCurl(lambda data: len(data))
    g = _bare_gw(curl)
    t0 = time.monotonic()
    g._write(b"hello", timeout=5)
    assert time.monotonic() - t0 < 1
    assert bytes(curl.sent) == b"hello"


# -- T2: wall-clock jumps -------------------------------------------------------

class _JumpingTime:
    """time.time that jumps by OFFSET once REAL seconds have passed."""

    def __init__(self, offset, after=0.5):
        self.offset = offset
        self.after = after
        self.t0 = time.monotonic()
        self.real = time.time

    def __call__(self):
        now = self.real()
        if time.monotonic() - self.t0 > self.after:
            return now + self.offset
        return now


def _run_wait(world, timeout, **kw):
    out = {}
    stop_at = time.monotonic() + min(timeout + 5, 40)
    kw.setdefault("should_stop", lambda: time.monotonic() > stop_at)

    def go():
        out["r"] = cli.run_wait({}, None, 6324, timeout, auto_approve=False, **kw)
    t = threading.Thread(target=go, daemon=True)
    t0 = time.monotonic()
    t.start()
    t.join(45)
    assert not t.is_alive(), "wait hung"
    return out["r"], time.monotonic() - t0


@pytest.fixture
def world(monkeypatch):
    w = World()
    monkeypatch.setattr(cli, "connect", w.connect)
    monkeypatch.setattr(cli, "RESUB_PAUSE", 0.05)
    monkeypatch.setattr(conn.Muse, "HEARTBEAT", 0.2)
    return w


class TimeoutGW(FakeGW):
    """FakeGW that honors subscribe overall_timeout like the real gateway:
    the stream returns when the overall budget elapses, even if the
    caller never asks it to stop."""

    def subscribe_raw(self, *a, **kw):
        overall = kw.get("overall_timeout", 120)
        box = {}

        def run():
            try:
                box["r"] = super(TimeoutGW, self).subscribe_raw(*a, **kw)
            except Exception as e:  # noqa: BLE001
                box["e"] = e
        t = threading.Thread(target=run, daemon=True)
        t.start()
        t.join(overall)
        if "e" in box:
            raise box["e"]
        return box.get("r", [])


class TimeoutWorld(World):
    def connect(self, cfg):
        g = TimeoutGW(self, len(self.conns) + 1)
        self.conns.append(g)
        return g


@pytest.fixture
def tworld(monkeypatch):
    w = TimeoutWorld()
    monkeypatch.setattr(cli, "connect", w.connect)
    monkeypatch.setattr(cli, "RESUB_PAUSE", 0.05)
    monkeypatch.setattr(conn.Muse, "HEARTBEAT", 0.2)
    return w


def test_backward_wall_jump_does_not_extend_the_bound(tworld, monkeypatch):
    """The wall clock jumping back an hour must not extend a 2 s wait."""
    tworld.state = "running"  # never finishes
    monkeypatch.setattr(time, "time", _JumpingTime(-3600))
    (_code, _res), secs = _run_wait(tworld, 2)
    assert secs < 5


def test_forward_wall_jump_revalidates_before_timeout_exit(tworld, monkeypatch):
    """The wall clock jumping forward must not end the wait early: the
    turn that finishes at 1 s is still reported done, with its reply."""
    tworld.state = "running"
    monkeypatch.setattr(time, "time", _JumpingTime(3600))

    def finish():
        time.sleep(1.0)
        tworld.reply(6325, "Late but done.")
        tworld.state = "completed"
    threading.Thread(target=finish, daemon=True).start()
    (code, res), secs = _run_wait(tworld, 20)
    assert code == 0 and [r["text"] for r in res["replies"]] == ["Late but done."]
    assert secs < 5


# -- T3: heartbeat supervision, bounded_connect race, _retry deadline ---------

def test_heartbeat_restarts_after_stop_and_after_death(monkeypatch):
    w = World()
    muse = conn.Muse(connector=w.connect)
    muse.start_heartbeat(interval=0.05)
    first = muse._hb
    assert first is not None and first.is_alive()
    muse.stop_heartbeat()
    muse.start_heartbeat(interval=0.05)
    assert muse._hb is not None and muse._hb.is_alive() and muse._hb is not first
    assert not muse._hb_stop.is_set()
    muse.stop_heartbeat()
    dead = threading.Thread(target=lambda: None)
    dead.start()
    dead.join()
    muse._hb = dead
    muse.start_heartbeat(interval=0.05)
    assert muse._hb.is_alive() and muse._hb is not dead
    muse.stop_heartbeat()


def test_bounded_connect_never_returns_a_closed_gateway():
    """At the timeout boundary, a late-finishing connect is either claimed
    (and never closed by the worker) or timed out (and closed exactly
    once): never both."""
    results = []
    for _ in range(60):
        closed = []

        class GW:
            def close(self):
                closed.append(1)

        def slow(cfg, _closed=closed):
            time.sleep(0.03)
            return GW()
        try:
            gw = conn.bounded_connect(slow, {}, timeout=0.03)
            results.append(("ok", gw, closed))
        except TimeoutError:
            results.append(("timeout", None, closed))
        time.sleep(0.06)  # let the worker finish / close
    for kind, gw, closed in results:
        if kind == "ok":
            assert closed == [], "claimed gateway was closed by the worker"
        else:
            assert closed == [1], "timed-out gateway was not closed exactly once"


class _FlakyGW:
    def __init__(self):
        self.calls = 0
        self.closed = False

    @property
    def alive(self):
        return not self.closed

    def lease(self):
        return self

    def close(self):
        self.closed = True

    def call_json(self, method, *a, **kw):
        self.calls += 1
        if self.calls <= 2:
            raise ConnectionDown("down")
        return {"ok": True}


def test_retry_respects_the_callers_remaining_deadline():
    muse = conn.Muse(connector=lambda cfg: _FlakyGW())
    sc = conn.SharedConn(muse)
    gw = _FlakyGW()
    muse.shared = gw
    past = time.monotonic() - 1
    with pytest.raises(ConnectionDown):
        sc._retry("test", True, lambda g: g.call_json("x"), deadline=past)
    assert gw.calls == 1  # no retry once the deadline has passed
    gw.calls = 0
    assert sc._retry("test", True, lambda g: g.call_json("x"),
                     deadline=time.monotonic() + 30) == {"ok": True}
    assert gw.calls == 3


class _StaleGW:
    def __init__(self, ping_fails):
        self.ping_fails = ping_fails
        self.pings = 0
        self.closed = False
        self.last_recv = time.time() - 1000
        self.last_used = time.time() - 1000

    @property
    def alive(self):
        return not self.closed

    def lease(self):
        return self

    def ping(self, timeout=10):
        self.pings += 1
        if self.ping_fails:
            raise TimeoutError("no response for connection.ping")

    def close(self):
        self.closed = True


def test_live_probes_a_quiet_connection_before_reusing_it():
    """A shared gateway that claims alive but has been quiet for a
    heartbeat interval is pinged before reuse: a failed probe reopens,
    a passed probe keeps the connection."""
    fresh = _StaleGW(ping_fails=False)
    muse = conn.Muse(connector=lambda cfg: fresh)
    muse.shared = fresh
    assert muse.live() is fresh and fresh.pings == 1
    stale = _StaleGW(ping_fails=True)
    replacement = _StaleGW(ping_fails=False)
    muse2 = conn.Muse(connector=lambda cfg: replacement)
    muse2.shared = stale
    assert muse2.live() is replacement
    assert stale.closed and muse2.drops >= 1


def test_close_does_not_hang_on_a_wedged_io_lock():
    """close() waits at most ~2 s for the I/O lock, then closes the
    socket anyway, even while another thread holds the lock."""
    g = object.__new__(Gateway)
    closed = []
    lock = threading.Lock()
    lock.acquire()

    class WS:
        def close(self):
            closed.append(1)
    g.ws = WS()
    g._io_lock = lock
    g.closed = False
    g._reader = None
    t0 = time.monotonic()
    g.close()
    assert time.monotonic() - t0 < 5
    assert closed == [1]
    lock.release()
