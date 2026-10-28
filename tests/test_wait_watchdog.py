"""D5 watchdog / state checks / reconnect budget (T6/T7/T8) + the D4
after_seq fast-turn regression, at the _run_wait seam.

Fakes reuse the World/FakeGW harness from test_wait_reconnect.py.
Constants under test: watchdog 20 s (here shortened via recheck_after),
state-check budget 8 s, shared reconnect budget 30 s.
"""
import json
import threading
import time

import pytest

from muse_cli import cli, conn
from muse_cli.wait import describe_approval
from test_wait_reconnect import FakeGW, World, status

AFTER = 6324


@pytest.fixture
def world(monkeypatch):
    w = World()
    monkeypatch.setattr(cli, "connect", w.connect)
    monkeypatch.setattr(cli, "RESUB_PAUSE", 0.05)
    monkeypatch.setattr(conn.Muse, "HEARTBEAT", 0.2)
    return w


def run(world, **kw):
    out = {}
    timeout = kw.get("timeout", 20)
    # FakeGW ignores subscribe overall_timeout, so bound the wait with a
    # safety should_stop (the real gateway returns at overall_timeout).
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


# -- D4 after_seq: the fast-turn regression -----------------------------------

def test_fast_turn_reply_before_wait_starts_is_delivered(world):
    """A reply that landed before a standalone wait (no --after-seq) must
    be delivered, not excluded by a latest-event snapshot of after_seq."""
    world.history = [
        {"seq": AFTER, "event_name": "message.user",
         "payload": {"display_text": "hi"}},
        {"seq": AFTER + 1, "event_name": "message.assistant",
         "payload": {"display_text": "fast reply", "message_id": "m1",
                     "status": "completed"}}]
    world.state = "completed"
    (code, res), _ = run(world, after_seq=None)
    assert code == 0 and [r["text"] for r in res["replies"]] == ["fast reply"]


def test_no_user_event_falls_back_to_zero_boundary(world):
    world.history = [
        {"seq": 5, "event_name": "message.assistant",
         "payload": {"display_text": "only reply", "message_id": "m1",
                     "status": "completed"}}]
    world.state = "completed"
    (code, res), _ = run(world, after_seq=None)
    assert code == 0 and [r["text"] for r in res["replies"]] == ["only reply"]


# -- T6: slow history / hung state check ---------------------------------------

class SlowHistoryGW(FakeGW):
    def call_json(self, method, body=None, **kw):
        if method == "chat.history":
            time.sleep(1.5)
        return super().call_json(method, body, **kw)


class SlowHistoryWorld(World):
    def connect(self, cfg):
        g = SlowHistoryGW(self, len(self.conns) + 1)
        self.conns.append(g)
        return g


def test_slow_history_does_not_hang_the_wait(monkeypatch):
    w = SlowHistoryWorld()
    monkeypatch.setattr(cli, "connect", w.connect)
    monkeypatch.setattr(cli, "RESUB_PAUSE", 0.05)
    monkeypatch.setattr(conn.Muse, "HEARTBEAT", 0.2)
    w.state = "completed"
    w.reply(AFTER + 1, "Slow but done.")
    (code, res), secs = run(w)
    assert code == 0 and [r["text"] for r in res["replies"]] == ["Slow but done."]
    assert secs < 10


class HungCheckGW(FakeGW):
    def subscribe_raw(self, method, body=None, max_records=100, on_record=None,
                      should_stop=None, **kw):
        if max_records == 50:  # task_state(): hang far past the check cadence
            time.sleep(3)
            return super().subscribe_raw(method, body, max_records, on_record,
                                         should_stop, **kw)
        return super().subscribe_raw(method, body, max_records, on_record,
                                     should_stop, **kw)


class HungCheckWorld(World):
    def connect(self, cfg):
        g = HungCheckGW(self, len(self.conns) + 1)
        self.conns.append(g)
        return g


def test_hung_state_check_does_not_suppress_later_checks(monkeypatch):
    """A state check stuck in its subscribe must not stop the next cadence
    check from starting: the wait still ends on its own timeout, and more
    than one state check ran."""
    w = HungCheckWorld()
    monkeypatch.setattr(cli, "connect", w.connect)
    monkeypatch.setattr(cli, "RESUB_PAUSE", 0.05)
    monkeypatch.setattr(conn.Muse, "HEARTBEAT", 0.2)
    w.state = "running"
    (code, _res), secs = run(w, timeout=3, check_every=0.3, recheck_after=0)
    assert code == 124
    checks = [m for _n, m in w.subscribes if m == 50]
    assert len(checks) >= 3


# -- T7: reconnect storm ---------------------------------------------------------

class StormGW(FakeGW):
    def subscribe_raw(self, method, body=None, max_records=100, on_record=None,
                      should_stop=None, **kw):
        if max_records != 50:
            time.sleep(1)
            # A subscribe that raises ConnectionError kills the gateway:
            # the shared connection must be reopened, not reused.
            self.dead = ConnectionError("gateway connection lost: storm")
            raise self.dead
        return super().subscribe_raw(method, body, max_records, on_record,
                                     should_stop, **kw)


class StormWorld(World):
    def connect(self, cfg):
        g = StormGW(self, len(self.conns) + 1)
        self.conns.append(g)
        return g


def test_reconnect_storm_ends_via_state_check_within_budget(monkeypatch):
    """Thirty seconds of failed resubscribes must trigger a bounded state
    check, which settles the already-finished turn instead of storming
    until the caller's timeout."""
    w = StormWorld()
    monkeypatch.setattr(cli, "connect", w.connect)
    monkeypatch.setattr(cli, "RESUB_PAUSE", 0.05)
    monkeypatch.setattr(conn.Muse, "HEARTBEAT", 0.2)
    w.state = "completed"
    w.reply(AFTER + 1, "Done in the storm.")
    (code, res), secs = run(w, timeout=120)
    assert code == 0 and [r["text"] for r in res["replies"]] == ["Done in the storm."]
    assert secs < 45


class FlakyGW(FakeGW):
    drops = 3

    def subscribe_raw(self, method, body=None, max_records=100, on_record=None,
                      should_stop=None, **kw):
        if max_records != 50 and self.n <= self.drops:
            self.dead = ConnectionError("gateway connection lost: flaky")
            raise self.dead
        return super().subscribe_raw(method, body, max_records, on_record,
                                     should_stop, **kw)


class FlakyWorld(World):
    def connect(self, cfg):
        g = FlakyGW(self, len(self.conns) + 1)
        self.conns.append(g)
        return g


def test_brief_outage_then_recovery_exits_promptly(monkeypatch):
    w = FlakyWorld()
    monkeypatch.setattr(cli, "connect", w.connect)
    monkeypatch.setattr(cli, "RESUB_PAUSE", 0.05)
    monkeypatch.setattr(conn.Muse, "HEARTBEAT", 0.2)
    w.state = "completed"
    w.reply(AFTER + 1, "Back.")
    (code, res), secs = run(w)
    assert code == 0 and [r["text"] for r in res["replies"]] == ["Back."]
    assert secs < 10


# -- D5 watchdog: only scoped progress resets it ---------------------------------

class NoiseGW(FakeGW):
    def subscribe_raw(self, method, body=None, max_records=100, on_record=None,
                      should_stop=None, **kw):
        if max_records == 50:
            return super().subscribe_raw(method, body, max_records, on_record,
                                         should_stop, **kw)
        thread = (body or {}).get("session_id")
        self.w.subscribes.append((self.n, max_records))
        if on_record(status(self.w.state, thread)):
            return []
        while True:
            if should_stop and should_stop():
                return []
            if self.w.state != "running":
                if on_record(status(self.w.state, thread)):
                    return []
            for line in (
                    json.dumps({"event": "sessions.updated", "payload": {}}),
                    json.dumps({"event": "tasks.updated", "payload": {}}),
                    json.dumps({"event": "task.status", "payload": {
                        "status": "running", "agent_id": "other",
                        "root_agent_id": "other", "session_id": "other",
                        "is_thread": True}})):
                if on_record(line.encode()):
                    return []
            time.sleep(0.02)


class NoiseWorld(World):
    def connect(self, cfg):
        g = NoiseGW(self, len(self.conns) + 1)
        self.conns.append(g)
        return g


def test_unscoped_noise_does_not_reset_the_watchdog(monkeypatch):
    """sessions.updated / tasks.updated / other-thread statuses are not
    progress: the watchdog still fires, resubscribes, and the finished
    turn is reported instead of hanging on a noisy-but-deaf stream."""
    w = NoiseWorld()
    monkeypatch.setattr(cli, "connect", w.connect)
    monkeypatch.setattr(cli, "RESUB_PAUSE", 0.05)
    monkeypatch.setattr(conn.Muse, "HEARTBEAT", 0.2)

    def finish():
        time.sleep(0.3)
        w.reply(AFTER + 1, "Finished quietly.")
        w.state = "completed"
    threading.Thread(target=finish, daemon=True).start()
    (code, res), secs = run(w, recheck_after=0.5)
    assert code == 0 and [r["text"] for r in res["replies"]] == ["Finished quietly."]
    assert secs < 5


# -- T8: approver ordering / no retry ---------------------------------------------

class DecideGW:
    def __init__(self):
        self.decides = []
        self.fail_first = set()

    def call_json(self, method, path_params=None, body=None, **kw):
        assert method == "egress.approval.decide", method
        aid = (path_params or {})["approval_id"]
        self.decides.append(aid)
        if aid in self.fail_first:
            self.fail_first.discard(aid)
            from muse_cli.gateway import GatewayError
            raise GatewayError(502, "decide lost")
        return {"ok": True}

    def close(self):
        pass


def _described(aid):
    return describe_approval({"approval_id": aid,
                              "display": {"summary_title": aid},
                              "decision_options": [{"kind": "allow_always"},
                                                   {"kind": "deny"}]})


def test_auto_approver_decides_in_offer_order():
    gw = DecideGW()
    approved, errors = [], []
    approve = cli.make_auto_approver({}, approved, errors, connector=lambda cfg: gw)
    done = approve([_described("a2"), _described("a1")])
    assert done == ["a2", "a1"] and gw.decides == ["a2", "a1"]


def test_auto_approver_never_retries_a_failed_decide():
    gw = DecideGW()
    gw.fail_first = {"a1"}
    approved, errors = [], []
    approve = cli.make_auto_approver({}, approved, errors, connector=lambda cfg: gw)
    done = approve([_described("a1")])
    assert done == [] and gw.decides == ["a1"] and len(errors) == 1
