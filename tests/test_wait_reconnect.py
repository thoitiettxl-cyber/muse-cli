"""`muse-cli wait` survives a gateway connection that dies silently (0.6.0).

The CLI wait runs on conn.Muse: a heartbeat notices a connection that
stopped answering, replaces it, the subscription is reopened and the turn
state re-read, so a turn that ended during the gap is reported at once."""
import json
import threading
import time

import pytest

from muse_cli import cli, conn

AFTER = 6324


def status(s, thread=None):
    return json.dumps({"event": "task.status", "payload": {
        "status": s, "agent_id": "r", "root_agent_id": "r", "is_thread": bool(thread),
        "session_id": thread}}).encode()


def keepalive():
    return json.dumps({"event": "keepalive", "payload": {}}).encode()


class World:
    """The server: turn state, history, and how each connection behaves."""

    def __init__(self):
        self.state = "running"
        self.history = [{"seq": AFTER, "event_name": "message.user", "payload": {"display_text": "hi"}}]
        self.conns = []
        self.fail_connects = 0
        self.silent = set()          # connection numbers that go silent after the first records
        self.chatter = set()         # connection numbers that only send keep-alives
        self.subscribes = []

    def reply(self, seq, text):
        self.history.append({"seq": seq, "event_name": "message.assistant",
                             "payload": {"display_text": text, "message_id": f"m{seq}",
                                         "status": "completed"}})

    def connect(self, cfg):
        if self.fail_connects:
            self.fail_connects -= 1
            raise ConnectionError("curl: (28) Connection timed out after 20003 ms")
        g = FakeGW(self, len(self.conns) + 1)
        self.conns.append(g)
        return g


class FakeGW:
    def __init__(self, world, n):
        self.w, self.n = world, n
        self.closed = False
        self.dead = None
        self.last_used = self.last_recv = time.time()
        self.vm_id = "vm"

    @property
    def alive(self):
        return not self.closed and self.dead is None

    def lease(self):
        return self

    def _check(self):
        if self.closed or self.dead:
            raise conn_down("gateway connection is down")

    def ping(self, timeout=10):
        self._check()
        if self.n in self.w.silent:
            raise TimeoutError("no response for connection.ping")
        self.last_recv = time.time()
        return {}

    def call_json(self, method, body=None, **_kw):
        self._check()
        if self.n in self.w.silent:
            time.sleep(0.05)
            self.dead = ConnectionError("silent")
            raise ConnectionError("gateway connection lost: silent")
        assert method == "chat.history", method
        return {"chat_events": list(self.w.history)[-int((body or {}).get("limit", 10)):]}

    def subscribe_raw(self, method, body=None, max_records=100, on_record=None, should_stop=None,
                      **_kw):
        self._check()
        thread = (body or {}).get("session_id")
        self.w.subscribes.append((self.n, max_records))
        if max_records == 50:                           # task_state(): replay and stop
            on_record(status(self.w.state, thread))
            return []
        if on_record(status(self.w.state, thread)):     # the replayed current state
            return []
        self.last_recv = time.time()
        while True:                                     # then: live, silent, or chatter
            if should_stop and should_stop():
                return []
            if self.closed or self.dead:
                raise ConnectionError("gateway connection lost: gateway connection closed")
            if self.n in self.w.chatter:
                self.last_recv = time.time()
                if on_record(keepalive()):
                    return []
            elif self.n not in self.w.silent and self.w.state != "running":
                if on_record(status(self.w.state, thread)):
                    return []
            time.sleep(0.02)

    def close(self):
        self.closed = True


def conn_down(msg):
    from muse_cli.gateway import ConnectionDown
    return ConnectionDown(msg)


@pytest.fixture
def world(monkeypatch):
    w = World()
    monkeypatch.setattr(cli, "connect", w.connect)
    monkeypatch.setattr(cli, "RESUB_PAUSE", 0.05)
    monkeypatch.setattr(conn.Muse, "HEARTBEAT", 0.2)
    return w


def run(world, **kw):
    out = {}

    def go():
        out["r"] = cli.run_wait({}, None, AFTER, kw.pop("timeout", 20), auto_approve=False, **kw)
    t = threading.Thread(target=go, daemon=True)
    t0 = time.time()
    t.start()
    t.join(15)
    assert not t.is_alive(), "wait hung"
    return out["r"], time.time() - t0


def test_silent_dead_connection_is_replaced_and_the_turn_end_seen(world):
    world.silent.add(1)            # connection 1: answers the subscribe, then nothing at all

    def finish():                  # Muse finishes while we are cut off
        time.sleep(0.3)
        world.reply(AFTER + 44, "Done: the final reply.")
        world.state = "completed"
    threading.Thread(target=finish, daemon=True).start()
    (code, res), secs = run(world)
    assert code == 0 and res["outcome"] == "done"
    assert [r["text"] for r in res["replies"]] == ["Done: the final reply."]   # once, not lost
    assert len(world.conns) == 2 and world.conns[0].closed
    assert secs < 8


def test_turn_that_ended_during_the_gap_before_running_was_seen(world):
    world.silent.add(1)
    world.state = "queued"

    def finish():
        time.sleep(0.2)
        world.reply(AFTER + 2, "Fast answer.")
        world.state = "completed"
    threading.Thread(target=finish, daemon=True).start()
    (code, res), _ = run(world, require_running=True)
    assert code == 0 and [r["text"] for r in res["replies"]] == ["Fast answer."]


def test_reconnect_failures_then_recovery(world, monkeypatch, caplog):
    import logging
    monkeypatch.setattr(conn.Muse, "DOWN_FAILS", 3)
    monkeypatch.setattr(conn.Muse, "FAIL_SPACING", 0)
    world.fail_connects = 4        # the box's flaky network: curl 28 four times
    world.state = "completed"
    world.reply(AFTER + 1, "All done.")
    with caplog.at_level(logging.WARNING, logger="muse_cli.wait"):
        (code, res), _ = run(world)
    assert code == 0 and [r["text"] for r in res["replies"]] == ["All done."]
    msgs = [r.getMessage() for r in caplog.records if r.name == "muse_cli.wait"]
    assert sum("can't reach Muse" in m for m in msgs) == 1          # one notice, not one per try
    assert sum("reconnected to Muse" in m for m in msgs) == 1


def test_periodic_check_catches_a_missed_turn_end(world, monkeypatch):
    monkeypatch.setattr(cli, "CHECK_EVERY", 0.3)
    world.chatter.add(1)           # the stream stays "alive" (keep-alives) but never says done

    def finish():
        time.sleep(0.2)
        world.reply(AFTER + 5, "Finished quietly.")
        world.state = "completed"
    threading.Thread(target=finish, daemon=True).start()
    (code, res), secs = run(world)
    assert code == 0 and [r["text"] for r in res["replies"]] == ["Finished quietly."]
    assert any(m == 50 for _n, m in world.subscribes)               # the separate state check
    assert secs < 5


def test_hung_connect_is_bounded(monkeypatch):
    gate = threading.Event()
    closed = []

    class Late:
        def close(self):
            closed.append(1)

    def hang(cfg):
        gate.wait(5)
        return Late()
    t0 = time.time()
    with pytest.raises(TimeoutError):
        conn.bounded_connect(hang, {}, timeout=0.2)
    assert time.time() - t0 < 1
    gate.set()
    time.sleep(0.2)
    assert closed == [1]           # a connect that finishes after we gave up is closed


def test_noise_handshake_has_a_timeout(monkeypatch):
    """The server accepts the websocket and never answers the handshake."""
    from muse_cli import gateway
    from curl_cffi.const import CurlECode
    from curl_cffi.curl import CurlError

    class Curl:
        def getinfo(self, _):
            return -1

        def ws_recv(self):
            raise CurlError("again", CurlECode.AGAIN)

    class WS:
        curl = Curl()
        closed = False

        def close(self):
            WS.closed = True
    monkeypatch.setattr("select.select", lambda r, w, x, t: time.sleep(min(t, 0.05)) or ([], [], []))
    g = object.__new__(gateway.Gateway)
    g.ws = WS()
    t0 = time.time()
    with pytest.raises(TimeoutError):
        g._recv_handshake(0.3)
    assert time.time() - t0 < 2 and WS.closed
