"""Review-fix round: derive fail-closed, settle-per-replay, final-read
floor + lag window, server_check guard, backoff cap, last-status check.

Seam: _run_wait via the World/FakeGW harness from test_wait_reconnect.
"""
import json
import threading
import time

import pytest

from muse_cli import cli, conn
from muse_cli.gateway import GatewayError
from muse_cli.wait import Waiter
from test_wait_reconnect import FakeGW, World, status

AFTER = 6324


def run(world, **kw):
    out = {}
    timeout = kw.get("timeout", 20)
    stop_at = time.monotonic() + min(timeout + 5, 40)
    kw.setdefault("should_stop", lambda: time.monotonic() > stop_at)

    def go():
        out["r"] = cli.run_wait({}, None, kw.pop("after_seq", AFTER), kw.pop("timeout", 20),
                                auto_approve=False, **kw)
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


def status_seq(s, seq, thread=None):
    return json.dumps({"event": "task.status", "seq": seq, "payload": {
        "status": s, "agent_id": "r", "root_agent_id": "r", "is_thread": bool(thread),
        "session_id": thread}}).encode()


# -- Fix 1: derive_after_seq fail-closed --------------------------------------

class FlakyHistoryGW(FakeGW):
    """chat.history fails exactly once, then succeeds."""

    def call_json(self, method, body=None, **kw):
        self.w.history_calls += 1
        if self.w.history_calls == 1:
            raise GatewayError(502, "transient history failure")
        return super().call_json(method, body, **kw)


class FlakyHistoryWorld(World):
    def __init__(self):
        super().__init__()
        self.history_calls = 0

    def connect(self, cfg):
        g = FlakyHistoryGW(self, len(self.conns) + 1)
        self.conns.append(g)
        return g


def test_derive_retries_once_then_excludes_stale_reply(monkeypatch):
    """Safety B1: a transient history error during derive is retried once;
    the retried boundary excludes the stale reply, so a replayed
    completed must NOT exit 0 delivering it."""
    w = FlakyHistoryWorld()
    monkeypatch.setattr(cli, "connect", w.connect)
    monkeypatch.setattr(cli, "RESUB_PAUSE", 0.05)
    monkeypatch.setattr(conn.Muse, "HEARTBEAT", 0.2)
    w.history = [{"seq": 100, "event_name": "message.assistant",
                  "payload": {"display_text": "OLD REPLY", "message_id": "m100",
                              "status": "completed"}},
                 {"seq": AFTER, "event_name": "message.user",
                  "payload": {"display_text": "hi"}}]
    w.state = "completed"
    (code, res), _ = run(w, after_seq=None, timeout=2, check_every=0)
    assert code == 124
    assert res["replies"] == []
    assert res["after_seq"] == AFTER
    assert w.history_calls >= 2


class ExhaustedHistoryGW(FakeGW):
    def call_json(self, method, body=None, **kw):
        return {"chat_events": list(self.w.history), "has_more": True}


class ExhaustedHistoryWorld(World):
    def connect(self, cfg):
        g = ExhaustedHistoryGW(self, len(self.conns) + 1)
        self.conns.append(g)
        return g


def test_derive_window_exhaustion_uses_seen_max(monkeypatch):
    """Window exhaustion (has_more still true at limit 1000, no user
    event) falls back to the max seq seen, not 0."""
    w = ExhaustedHistoryWorld()
    monkeypatch.setattr(cli, "connect", w.connect)
    monkeypatch.setattr(cli, "RESUB_PAUSE", 0.05)
    monkeypatch.setattr(conn.Muse, "HEARTBEAT", 0.2)
    w.history = [{"seq": 9000, "event_name": "message.assistant",
                  "payload": {"display_text": "old", "message_id": "m9",
                              "status": "completed"}}]
    w.state = "completed"
    (code, res), _ = run(w, after_seq=None, timeout=2, check_every=0)
    assert code == 124
    assert res["after_seq"] == 9000
    assert res["replies"] == []


class FailingHistoryGW(FakeGW):
    """chat.history fails for the first 6 calls (derive: 3 limits x 2
    attempts), then succeeds."""

    def call_json(self, method, body=None, **kw):
        self.w.history_calls += 1
        if self.w.history_calls <= 6:
            raise GatewayError(502, "history unreadable")
        return super().call_json(method, body, **kw)


class FailingHistoryWorld(World):
    def __init__(self):
        super().__init__()
        self.history_calls = 0

    def connect(self, cfg):
        g = FailingHistoryGW(self, len(self.conns) + 1)
        self.conns.append(g)
        return g


def test_derive_all_reads_failing_disables_replay_settle(monkeypatch):
    """No window ever succeeds: settle-from-replay is disabled and the
    wait reports timeout with a notice, never the stale reply."""
    w = FailingHistoryWorld()
    monkeypatch.setattr(cli, "connect", w.connect)
    monkeypatch.setattr(cli, "RESUB_PAUSE", 0.05)
    monkeypatch.setattr(conn.Muse, "HEARTBEAT", 0.2)
    w.history = [{"seq": AFTER, "event_name": "message.user",
                  "payload": {"display_text": "hi"}},
                 {"seq": AFTER + 1, "event_name": "message.assistant",
                  "payload": {"display_text": "STALE", "message_id": "m1",
                              "status": "completed"}}]
    w.state = "completed"
    (code, res), _ = run(w, after_seq=None, timeout=2, check_every=0)
    assert code == 124
    assert res["replies"] == []
    assert res["after_seq"] == 0
    assert "boundary" in (res.get("note") or "")


class RatchetGW(FailingHistoryGW):
    """Derive fails, but the turn then runs live: running -> completed,
    with the reply landing after the running event."""

    def subscribe_raw(self, method, body=None, max_records=100, on_record=None,
                      should_stop=None, **kw):
        if max_records == 50:
            return super().subscribe_raw(method, body, max_records, on_record,
                                         should_stop, **kw)
        if on_record(status_seq("running", 50)):
            return []
        if on_record(status_seq("completed", 60)):
            return []
        return []


class RatchetWorld(FailingHistoryWorld):
    def connect(self, cfg):
        g = RatchetGW(self, len(self.conns) + 1)
        self.conns.append(g)
        return g


def test_boundary_unknown_live_running_ratchets_boundary(monkeypatch):
    """After a live root running clears boundary-unknown, the running
    event's seq becomes the boundary: the reply after it is delivered."""
    w = RatchetWorld()
    monkeypatch.setattr(cli, "connect", w.connect)
    monkeypatch.setattr(cli, "RESUB_PAUSE", 0.05)
    monkeypatch.setattr(conn.Muse, "HEARTBEAT", 0.2)
    w.history = [{"seq": 5, "event_name": "message.user",
                  "payload": {"display_text": "hi"}},
                 {"seq": 55, "event_name": "message.assistant",
                  "payload": {"display_text": "live reply", "message_id": "m55",
                              "status": "completed"}}]
    (code, res), _ = run(w, after_seq=None, timeout=5, check_every=0)
    assert code == 0
    assert [r["text"] for r in res["replies"]] == ["live reply"]


# -- Fix 3: settle retry per resubscribe (Red Probe A) -------------------------

class DropOnceGW(FakeGW):
    """First subscribe replays completed, then drops; later subscribes
    behave normally."""

    def subscribe_raw(self, method, body=None, max_records=100, on_record=None,
                      should_stop=None, **kw):
        if max_records == 50:
            return super().subscribe_raw(method, body, max_records, on_record,
                                         should_stop, **kw)
        if self.n == 1:
            if on_record(status(self.w.state)):
                return []
            time.sleep(0.2)
            raise ConnectionError("gateway connection lost: drop once")
        return super().subscribe_raw(method, body, max_records, on_record,
                                     should_stop, **kw)


class DropOnceWorld(World):
    def connect(self, cfg):
        g = DropOnceGW(self, len(self.conns) + 1)
        self.conns.append(g)
        return g


def test_settle_retried_after_resubscribe(monkeypatch):
    """Red Probe A: the reply lands 0.3 s after the first empty settle
    read; a resubscribe retries the settle and delivers it. Safety net
    disabled: only the resubscribe may retry."""
    w = DropOnceWorld()
    monkeypatch.setattr(cli, "connect", w.connect)
    monkeypatch.setattr(cli, "RESUB_PAUSE", 0.05)
    monkeypatch.setattr(conn.Muse, "HEARTBEAT", 0.2)
    w.state = "completed"
    threading.Timer(0.3, lambda: w.reply(AFTER + 1, "late reply")).start()
    stop_at = time.monotonic() + 4
    (code, res), _ = run(w, timeout=10, check_every=0, recheck_after=60,
                         should_stop=lambda: time.monotonic() > stop_at)
    assert code == 0
    assert [r["text"] for r in res["replies"]] == ["late reply"]


# -- Fix 4: final-read floor + lag window --------------------------------------

class LiveTermGW(FakeGW):
    """The turn runs live (running -> completed) and returns at once;
    the reply lands in history 0.3 s later."""

    def subscribe_raw(self, method, body=None, max_records=100, on_record=None,
                      should_stop=None, **kw):
        if max_records == 50:
            return super().subscribe_raw(method, body, max_records, on_record,
                                         should_stop, **kw)
        if on_record(status("running")):
            return []
        if on_record(status("completed")):
            return []
        return []


class LiveTermWorld(World):
    def connect(self, cfg):
        g = LiveTermGW(self, len(self.conns) + 1)
        self.conns.append(g)
        return g


def test_final_read_lag_window_delivers_late_reply(monkeypatch):
    """Red Probe D: a reply landing 0.3 s after the terminal is delivered
    by the lag window, not reported as done-with-no-reply."""
    w = LiveTermWorld()
    monkeypatch.setattr(cli, "connect", w.connect)
    monkeypatch.setattr(cli, "RESUB_PAUSE", 0.05)
    monkeypatch.setattr(conn.Muse, "HEARTBEAT", 0.2)
    threading.Timer(0.3, lambda: w.reply(AFTER + 1, "lagged reply")).start()
    (code, res), _ = run(w, timeout=10, check_every=0, recheck_after=60)
    assert code == 0
    assert [r["text"] for r in res["replies"]] == ["lagged reply"]


class TimeoutHonoringGW(FakeGW):
    """chat.history honours its timeout: a read given less than 0.6 s
    fails; the subscribe itself takes 0.9 s, so the final read starts
    with the caller deadline nearly expired."""

    def call_json(self, method, body=None, **kw):
        timeout = kw.get("timeout", 10)
        if timeout < 0.6:
            time.sleep(timeout)
            raise TimeoutError("no response for chat.history")
        time.sleep(0.6)
        return super().call_json(method, body, **kw)

    def subscribe_raw(self, method, body=None, max_records=100, on_record=None,
                      should_stop=None, **kw):
        if max_records == 50:
            return super().subscribe_raw(method, body, max_records, on_record,
                                         should_stop, **kw)
        if on_record(status("running")):
            return []
        time.sleep(0.9)
        if on_record(status("completed")):
            return []
        return []


class TimeoutHonoringWorld(World):
    def connect(self, cfg):
        g = TimeoutHonoringGW(self, len(self.conns) + 1)
        self.conns.append(g)
        return g


def test_final_read_floor_survives_expired_deadline(monkeypatch):
    """The final history read gets a floored budget independent of the
    expired caller deadline: a 0.6 s read past a 1 s timeout succeeds."""
    w = TimeoutHonoringWorld()
    monkeypatch.setattr(cli, "connect", w.connect)
    monkeypatch.setattr(cli, "RESUB_PAUSE", 0.05)
    monkeypatch.setattr(conn.Muse, "HEARTBEAT", 0.2)
    w.reply(AFTER + 1, "slow final reply")
    (code, res), secs = run(w, timeout=1, check_every=0, recheck_after=60)
    assert code == 0
    assert [r["text"] for r in res["replies"]] == ["slow final reply"]
    assert secs < 10


# -- Fix 6: server_check settle guard ------------------------------------------

class GateHistoryGW(FakeGW):
    """The second history read (server_check's) blocks on a gate; all
    other reads are empty. The first read is on_record's settle attempt
    during the replay; gating the second isolates server_check."""

    def call_json(self, method, body=None, **kw):
        self.w.history_calls += 1
        if self.w.history_calls == 2:
            self.w.gate_started.set()
            self.w.gate_release.wait(5)
            return {"chat_events": list(self.w.history)}
        return {"chat_events": []}


class GateHistoryWorld(World):
    def __init__(self):
        super().__init__()
        self.history_calls = 0
        self.gate_started = threading.Event()
        self.gate_release = threading.Event()

    def connect(self, cfg):
        g = GateHistoryGW(self, len(self.conns) + 1)
        self.conns.append(g)
        return g


def test_server_check_does_not_settle_swapped_terminal(monkeypatch):
    """A history read started for terminal A must not settle terminal B
    when the waiter swaps terminals mid-read."""
    w = GateHistoryWorld()
    monkeypatch.setattr(cli, "connect", w.connect)
    monkeypatch.setattr(cli, "RESUB_PAUSE", 0.05)
    monkeypatch.setattr(conn.Muse, "HEARTBEAT", 0.2)
    w.state = "completed"
    w.reply(AFTER + 1, "gated reply")
    instances = []

    class SpyWaiter(Waiter):
        def __init__(self, *a, **k):
            super().__init__(*a, **k)
            instances.append(self)

    monkeypatch.setattr("muse_cli.wait.Waiter", SpyWaiter)

    def swapper():
        assert w.gate_started.wait(5)
        time.sleep(0.05)
        instances[0].feed(status_seq("idle", 99))
        w.gate_release.set()

    threading.Thread(target=swapper, daemon=True).start()
    stop_at = time.monotonic() + 2.5
    (code, res), _ = run(w, timeout=10, check_every=0.05,
                         should_stop=lambda: time.monotonic() > stop_at)
    assert code == 124
    assert res["replies"] == []


# -- Fix 5: backoff capped by remaining deadline --------------------------------

def test_resubscribe_backoff_capped_by_deadline(monkeypatch):
    """A storm of failed connects must end near the caller deadline, not
    overshoot it by whole backoff sleeps."""
    w = World()
    w.fail_connects = 100
    monkeypatch.setattr(cli, "connect", w.connect)
    monkeypatch.setattr(cli, "RESUB_PAUSE", 1.0)
    monkeypatch.setattr(conn.Muse, "HEARTBEAT", 0.2)
    (code, _res), secs = run(w, timeout=1.2, check_every=0)
    assert code == 124
    assert secs < 2.0


# -- F7: task_state_info last status --------------------------------------------

class TwoStatusGW(FakeGW):
    def subscribe_raw(self, method, body=None, max_records=100, on_record=None,
                      should_stop=None, **kw):
        if max_records == 50:
            on_record(status("running"))
            on_record(status("completed"))
            return []
        return super().subscribe_raw(method, body, max_records, on_record,
                                     should_stop, **kw)


def test_task_state_info_prefers_last_status_when_asked():
    """A replay carrying running then completed reports the freshest
    status when prefer_last is set, the first otherwise."""
    from muse_cli.wait import task_status_info
    gw = TwoStatusGW(World(), 1)
    first = cli.task_state_info(gw)
    assert task_status_info(first)["status"] == "running"
    last = cli.task_state_info(gw, prefer_last=True)
    assert task_status_info(last)["status"] == "completed"
