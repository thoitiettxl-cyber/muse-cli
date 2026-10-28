"""PR #2 review fixes (items 1-3): AuthError propagation, replayed FAILED
exit 2, self-referential reply filter. Seams: _run_wait/run_wait with the
World/FakeGW harness, Waiter unit tests, replies_after/is_reply fixtures.
"""
import json
import threading
import time

import pytest

from muse_cli import cli, conn
from muse_cli.gateway import AuthError
from muse_cli.wait import Waiter, replies_after
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


# -- Item 1: AuthError must propagate out of _run_wait -------------------------

class AuthFailWorld(World):
    def connect(self, cfg):
        raise AuthError("auth/check -> 401 (cookies expired? re-export)")


def test_run_wait_reraises_auth_error(world, monkeypatch):
    """An expired-cookie failure inside the wait must surface as AuthError,
    not as exit 124 'run wait again' (which loops forever on retry)."""
    w = AuthFailWorld()
    monkeypatch.setattr(cli, "connect", w.connect)
    with pytest.raises(AuthError):
        cli.run_wait({}, None, AFTER, timeout=5, auto_approve=False)


def test_run_wait_preserves_keyboard_interrupt(monkeypatch):
    """KeyboardInterrupt/SystemExit are BaseExceptions: no wrapper may
    convert them into a timeout result."""

    def boom(cfg):
        raise KeyboardInterrupt

    monkeypatch.setattr(cli, "connect", boom)
    with pytest.raises(KeyboardInterrupt):
        cli.run_wait({}, None, AFTER, timeout=5, auto_approve=False)


def test_main_maps_auth_error_to_exit_2(monkeypatch, capsys):
    """The CLI exit-code contract: AuthError from a wait reaches main()'s
    handler and exits 2 with the auth-error message."""
    monkeypatch.setattr(cli, "activate_profile", lambda *a, **k: None)
    monkeypatch.setattr(cli, "resolve_profile", lambda *a, **k: "default")
    monkeypatch.setattr(cli, "start_update_check", lambda: None)
    monkeypatch.setattr(cli, "finish_update_check", lambda notice: None)
    monkeypatch.setattr(cli, "load_config", lambda: {})
    monkeypatch.setattr(cli, "run_wait", lambda *a, **k: (_ for _ in ()).throw(
        AuthError("auth/check -> 401 (cookies expired? re-export)")))
    with pytest.raises(SystemExit) as exc:
        cli.main(["wait", "--timeout", "1"])
    assert exc.value.code == 2
    assert "auth error" in capsys.readouterr().err


# -- Item 2: replayed FAILED terminal belonging to the boundary turn ---------

def status_seq(s, seq, thread=None):
    return json.dumps({"event": "task.status", "seq": seq, "payload": {
        "status": s, "agent_id": "r", "root_agent_id": "r", "is_thread": bool(thread),
        "session_id": thread}}).encode()


class ReplayGW(FakeGW):
    """Replay one seq-bearing terminal, then stay quiet until stopped."""

    def subscribe_raw(self, method, body=None, max_records=100, on_record=None,
                      should_stop=None, **kw):
        self._check()
        thread = (body or {}).get("session_id")
        self.w.subscribes.append((self.n, max_records))
        if max_records == 50:
            on_record(status(self.w.state, thread))
            return []
        if on_record(status_seq(self.w.state, self.w.term_seq, thread)):
            return []
        while True:
            if should_stop and should_stop():
                return []
            time.sleep(0.02)


class ReplayWorld(World):
    def __init__(self):
        super().__init__()
        self.term_seq = AFTER + 1

    def connect(self, cfg):
        g = ReplayGW(self, len(self.conns) + 1)
        self.conns.append(g)
        return g


def test_replayed_interrupted_after_boundary_turn_exits_2(world, monkeypatch):
    """A standalone wait started after a cancel: the replayed `interrupted`
    belongs to the boundary (latest user) turn, so it exits 2 at once
    instead of blocking for the full timeout."""
    w = ReplayWorld()
    monkeypatch.setattr(cli, "connect", w.connect)
    monkeypatch.setattr(cli, "RESUB_PAUSE", 0.05)
    monkeypatch.setattr(conn.Muse, "HEARTBEAT", 0.2)
    w.state = "interrupted"
    w.term_seq = AFTER + 1  # stream seq: never compared with chat seqs
    # standalone: the boundary (AFTER, the latest user message) is derived
    (code, res), secs = run(w, timeout=5, check_every=0, after_seq=None)
    assert code == 2 and res["outcome"] == "failed"
    assert res["replies"] == []
    assert secs < 4


def test_replayed_interrupted_from_older_turn_does_not_exit_2(world, monkeypatch):
    """The FAILED terminal predates a newer user message: it belongs to an
    older turn, so the wait must keep waiting for the live turn."""
    w = ReplayWorld()
    monkeypatch.setattr(cli, "connect", w.connect)
    monkeypatch.setattr(cli, "RESUB_PAUSE", 0.05)
    monkeypatch.setattr(conn.Muse, "HEARTBEAT", 0.2)
    w.history = [{"seq": AFTER, "event_name": "message.user",
                  "payload": {"display_text": "old turn"}},
                 {"seq": AFTER + 6, "event_name": "message.user",
                  "payload": {"display_text": "new turn"}}]
    w.state = "interrupted"
    w.term_seq = AFTER + 1  # terminal before the newer user message
    # Bound explicitly just past the deadline: FakeGW ignores subscribe
    # overall_timeout, so the run() safety stop (timeout+5) would cost 7 s.
    t0 = time.monotonic()
    (code, res), secs = run(w, timeout=2, check_every=0,
                            should_stop=lambda: time.monotonic() > t0 + 2.3)
    assert code == 124
    assert secs >= 1.5


def test_live_interrupted_after_running_still_exits_2(world, monkeypatch):
    """Existing behaviour preserved: a live cancel after running exits 2."""

    class LiveFailGW(FakeGW):
        def subscribe_raw(self, method, body=None, max_records=100, on_record=None,
                          should_stop=None, **kw):
            self._check()
            thread = (body or {}).get("session_id")
            self.w.subscribes.append((self.n, max_records))
            if max_records == 50:
                on_record(status("completed", thread))
                return []
            if on_record(status("running", thread)):
                return []
            if on_record(status("interrupted", thread)):
                return []
            while True:
                if should_stop and should_stop():
                    return []
                time.sleep(0.02)

    class LiveFailWorld(World):
        def connect(self, cfg):
            g = LiveFailGW(self, len(self.conns) + 1)
            self.conns.append(g)
            return g

    w = LiveFailWorld()
    monkeypatch.setattr(cli, "connect", w.connect)
    (code, res), _ = run(w, timeout=5, check_every=0)
    assert code == 2 and res["outcome"] == "failed"


def test_replayed_failed_with_missing_root_id_does_not_exit_2(world, monkeypatch):
    """A replayed FAILED terminal with no root id is suspect: it must not
    exit 2 without the settle discipline (which never accepts FAILED)."""
    w = ReplayWorld()
    monkeypatch.setattr(cli, "connect", w.connect)
    monkeypatch.setattr(cli, "RESUB_PAUSE", 0.05)
    monkeypatch.setattr(conn.Muse, "HEARTBEAT", 0.2)
    w.state = "interrupted"
    w.term_seq = AFTER + 1
    orig_subscribe = ReplayGW.subscribe_raw

    def no_root_subscribe(self, method, body=None, max_records=100, on_record=None,
                          should_stop=None, **kw):
        if max_records == 50:
            return orig_subscribe(self, method, body, max_records, on_record, should_stop, **kw)
        thread = (body or {}).get("session_id")
        ev = json.dumps({"event": "task.status", "seq": self.w.term_seq, "payload": {
            "status": self.w.state, "agent_id": "r",
            "is_thread": bool(thread), "session_id": thread}}).encode()
        if on_record(ev):
            return []
        while True:
            if should_stop and should_stop():
                return []
            time.sleep(0.02)
    monkeypatch.setattr(ReplayGW, "subscribe_raw", no_root_subscribe)
    t0 = time.monotonic()
    (code, _res), _ = run(w, timeout=2, check_every=0,
                          should_stop=lambda: time.monotonic() > t0 + 2.3)
    assert code == 124


def test_waiter_accept_replayed_failed_unit():
    """Waiter-level rule: FAILED + root-confirmed + reason clear + boundary
    derived from history and known + no user message newer than it -> exit 2."""
    w = Waiter()
    w.feed({"event": "task.status", "seq": 10, "payload": {
        "status": "interrupted", "agent_id": "r", "root_agent_id": "r",
        "is_thread": False, "session_id": None}})
    assert w.ignored_terminal == "interrupted"
    w.boundary_derived, w.after_seq = True, 9
    assert w.accept_replayed_failed(9) == ("failed", "interrupted")
    assert w.result == ("failed", "interrupted")


def test_waiter_accept_replayed_failed_rejects_stale_and_unknown():
    w = Waiter()
    w.feed({"event": "task.status", "seq": 10, "payload": {
        "status": "interrupted", "agent_id": "r", "root_agent_id": "r",
        "is_thread": False, "session_id": None}})
    w.boundary_derived, w.after_seq = True, 9
    assert w.accept_replayed_failed(11) is None  # newer user turn exists
    assert w.result is None
    w.boundary_unknown = True
    assert w.accept_replayed_failed(9) is None
    w.boundary_unknown = False
    assert w.accept_replayed_failed(None) is None  # unprovable: fail closed


# -- Item 3: self-referential filter only -------------------------------------

def assistant(seq, text, mid, reply_to=None, level="payload", status="completed"):
    p = {"display_text": text, "message_id": mid, "status": status}
    e = {"seq": seq, "event_name": "message.assistant", "payload": p}
    if reply_to is not None:
        if level == "payload":
            p["reply_to_message_id"] = reply_to
        else:
            e["reply_to_message_id"] = reply_to
    return e


def test_self_referential_push_at_payload_level_dropped():
    ev = assistant(11, "push", "m11", reply_to="m11")
    assert replies_after([ev], after_seq=10) == []


def test_self_referential_push_at_event_level_dropped():
    ev = assistant(11, "push", "m11", reply_to="m11", level="event")
    assert replies_after([ev], after_seq=10) == []


def test_genuine_reply_with_reply_to_user_message_kept():
    ev = assistant(11, "answer", "m11", reply_to="u9")
    out = replies_after([ev], after_seq=10)
    assert [r["text"] for r in out] == ["answer"]


def test_genuine_reply_with_reply_to_other_assistant_kept():
    ev = assistant(11, "follow-up", "m11", reply_to="m5")
    out = replies_after([ev], after_seq=10)
    assert [r["text"] for r in out] == ["follow-up"]


def test_reply_without_ids_cannot_prove_self_reference_kept():
    """No message_id anywhere: self-reference cannot be proved, so the
    reply is kept (fail open, logged by the status rule only)."""
    ev = {"seq": 11, "event_name": "message.assistant",
          "payload": {"display_text": "reply", "status": "completed",
                      "reply_to_message_id": "x"}}
    out = replies_after([ev], after_seq=10)
    assert [r["text"] for r in out] == ["reply"]


def test_is_reply_agrees_with_replies_after():
    """cli.is_reply applies the same exact-match rule as replies_after."""
    from muse_cli.cli import is_reply
    push = assistant(11, "push", "m11", reply_to="m11")
    genuine = assistant(12, "answer", "m12", reply_to="u9")
    assert is_reply(push, 10) is False
    assert is_reply(genuine, 10) is True
