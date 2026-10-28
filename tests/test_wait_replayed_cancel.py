"""A replayed cancel (`interrupted`) ends a wait with exit 2 only when it
provably belongs to the awaited turn: a standalone wait whose boundary was
read from history, with no user message newer than that boundary.

Live muse.ai: the seq on a replayed task.status is a stream counter
(observed 5988, 5991, 5994 on three subscribes) unrelated to chat.history
seqs (~5196), so the two must never be compared."""
import json
import time

import pytest

from muse_cli import cli, conn
from muse_cli.wait import Waiter
from test_wait_reconnect import AFTER, FakeGW, World, status

STREAM_SEQ = 99999  # a live-shaped stream counter, far above any chat seq


def replay(s, thread=None, seq=STREAM_SEQ):
    return json.dumps({"event": "task.status", "seq": seq, "payload": {
        "status": s, "agent_id": "r", "root_agent_id": "r", "is_thread": bool(thread),
        "session_id": thread}}).encode()


class CancelGW(FakeGW):
    """Replays the stored terminal with a stream seq, then stays quiet."""

    def subscribe_raw(self, method, body=None, max_records=100, on_record=None,
                      should_stop=None, **kw):
        self._check()
        thread = (body or {}).get("session_id")
        if on_record(replay(self.w.state, thread)) or max_records == 50:
            return []
        while True:
            if should_stop and should_stop():
                return []
            time.sleep(0.02)


class CancelWorld(World):
    def __init__(self):
        super().__init__()
        self.state = "interrupted"

    def connect(self, cfg):
        g = CancelGW(self, len(self.conns) + 1)
        self.conns.append(g)
        return g


@pytest.fixture
def cw(monkeypatch):
    w = CancelWorld()
    monkeypatch.setattr(cli, "connect", w.connect)
    monkeypatch.setattr(cli, "RESUB_PAUSE", 0.05)
    monkeypatch.setattr(conn.Muse, "HEARTBEAT", 0.2)
    return w


def wait(after_seq, timeout, **kw):
    t0 = time.monotonic()
    kw.setdefault("should_stop", lambda: time.monotonic() > t0 + timeout + 0.3)
    code, res = cli.run_wait({}, None, after_seq, timeout, auto_approve=False,
                             check_every=0, **kw)
    return code, res, time.monotonic() - t0


def test_standalone_wait_right_after_cancel_exits_2(cw):
    code, res, secs = wait(None, 5)
    assert code == 2 and res["outcome"] == "failed" and res["status"] == "interrupted"
    assert res["after_seq"] == AFTER and res["replies"] == []
    assert secs < 3


def test_explicit_after_seq_beyond_cancelled_turn_does_not_exit_2(cw):
    """Reproduced live: `wait --after-seq 5300` on a chat whose last turn
    (user seq 5196) was cancelled exited 2 at once. It must keep waiting."""
    code, res, secs = wait(AFTER + 100, 1.5)
    assert code == 124 and res["outcome"] == "timeout"
    assert secs >= 1.2


def test_explicit_after_seq_equal_to_cancelled_turn_does_not_exit_2(cw):
    """send --wait / Telegram follow pass the user seq explicitly: a replay
    cannot prove the cancel is this turn's (the new turn may not have
    started yet), so no exit 2 from a replay."""
    code, _res, _secs = wait(AFTER, 1.5)
    assert code == 124


def test_standalone_wait_with_newer_user_message_does_not_exit_2(cw, monkeypatch):
    """The boundary is the newest user message; a user message arriving
    after the boundary read means the replayed cancel is older."""
    calls = {"n": 0}
    real = FakeGW.call_json

    def call_json(self, method, body=None, **kw):
        calls["n"] += 1
        if calls["n"] > 1 and not any(e["seq"] == AFTER + 5 for e in cw.history):
            cw.history.append({"seq": AFTER + 5, "event_name": "message.user",
                               "payload": {"display_text": "new"}})
        return real(self, method, body, **kw)
    monkeypatch.setattr(FakeGW, "call_json", call_json)
    code, _res, _secs = wait(None, 1.5)
    assert code == 124


def test_live_cancel_after_running_still_exits_2_with_explicit_after_seq(monkeypatch):
    class LiveGW(FakeGW):
        def subscribe_raw(self, method, body=None, max_records=100, on_record=None,
                          should_stop=None, **kw):
            self._check()
            if max_records == 50:
                on_record(status("running"))
                return []
            for s in ("running", "interrupted"):
                if on_record(status(s)):
                    return []
            while not (should_stop and should_stop()):
                time.sleep(0.02)
            return []

    class LiveWorld(World):
        def connect(self, cfg):
            g = LiveGW(self, len(self.conns) + 1)
            self.conns.append(g)
            return g
    w = LiveWorld()
    monkeypatch.setattr(cli, "connect", w.connect)
    code, res, _ = wait(AFTER, 5)
    assert code == 2 and res["status"] == "interrupted"


def _replayed(seq=STREAM_SEQ):
    w = Waiter()
    w.feed({"event": "task.status", "seq": seq, "payload": {
        "status": "interrupted", "agent_id": "r", "root_agent_id": "r",
        "is_thread": False, "session_id": None}})
    assert w.ignored_terminal == "interrupted"
    return w


def test_waiter_accepts_only_derived_boundary_with_no_newer_user():
    w = _replayed()
    w.boundary_derived, w.after_seq = True, 50
    assert w.accept_replayed_failed(51) is None   # newer user message
    assert w.accept_replayed_failed(None) is None  # unprovable
    assert w.accept_replayed_failed(50) == ("failed", "interrupted")


def test_waiter_never_compares_stream_seq_with_history_seq():
    # terminal stream seq lower than the user seq: irrelevant to the rule
    w = _replayed(seq=3)
    w.boundary_derived, w.after_seq = True, 50
    assert w.accept_replayed_failed(50) == ("failed", "interrupted")


def test_waiter_explicit_boundary_never_accepts_replay():
    w = _replayed()
    w.boundary_derived, w.after_seq = False, 50
    assert w.accept_replayed_failed(50) is None


def test_waiter_unknown_boundary_never_accepts_replay():
    w = _replayed()
    w.boundary_derived, w.after_seq, w.boundary_unknown = True, 50, True
    assert w.accept_replayed_failed(50) is None


def test_last_chance_cancel_check_uses_final_read_floor(cw, monkeypatch):
    """At timeout the deadline is gone: the last-chance read must get the
    FINAL_READ_FLOOR budget, not 0.1 s."""
    budgets = []
    real = FakeGW.call_json

    def call_json(self, method, body=None, **kw):
        budgets.append(kw.get("timeout"))
        return real(self, method, body, **kw)
    monkeypatch.setattr(FakeGW, "call_json", call_json)
    # explicit boundary: the replay never settles, so the wait times out and
    # the last-chance check runs (and still must not exit 2).
    code, _res, _ = wait(AFTER, 0.5)
    assert code == 124
    # the last-chance read precedes the final replies read; both floored
    assert budgets and min(budgets[-2:]) >= cli.FINAL_READ_FLOOR - 0.01
