"""D6 Telegram bridge turn-end fixes (T9): part keying, media claim rollback,
watermark-after-success, monitor watchdog, bounded waits.

Evidence: muse01 probes in ~/workspace/tmp/muse-cli-wait-design/
2b-probe-results-muse01.md (D6 follow-ups) + static findings in 1b/1c.
"""
import concurrent.futures
import json
import threading
import time
import types

import pytest

from muse_cli import cli, tgapi, tgbot
from muse_cli.tgapi import TelegramAPI, TelegramError
from test_telegram import FakeTelegram, Server, ev

OWNER = 1001
TOKEN = "123456:" + "A" * 35


@pytest.fixture
def home(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv(cli.PROFILE_ENV, raising=False)
    monkeypatch.delenv("MUSE_AUTO_APPROVE", raising=False)
    for attr in ("PROFILE", "CONFIG_DIR", "CONFIG_FILE", "COOKIES_FILE"):
        monkeypatch.setattr(cli, attr, getattr(cli, attr))
    cli.activate_profile(cli.DEFAULT_PROFILE)
    cli.ensure_config_dir()
    tgbot.write_private_json(tgbot.config_path(), {"token": TOKEN, "owner_id": OWNER,
                                                   "allowed_user_ids": [OWNER]})
    monkeypatch.setattr(cli, "POLL", 0)
    monkeypatch.setattr(cli, "RESUB_PAUSE", 0, raising=False)
    monkeypatch.setattr(tgbot, "QUEUE_RECHECK", 0.3, raising=False)
    return tmp_path


@pytest.fixture
def env(home):
    server = Server()
    tg = FakeTelegram()
    bot = tgbot.Bot(tg, muse=tgbot.Muse(connector=server.connect), sync=True)
    bot.edit_interval = 0
    return types.SimpleNamespace(bot=bot, tg=tg, s=server, home=home)


def _turn(bot):
    return tgbot.Turn(types.SimpleNamespace(clock=time.time, edit_interval=1.0),
                      OWNER, None, "Main")


def _line(name, payload):
    return json.dumps({"event": name, "payload": payload})


def test_part_key_unifies_message_id_and_message_seq():
    """A message whose start carries message_id and whose appends/done carry
    message_seq is ONE part: no duplicate, no truncation on resubscription."""
    turn = _turn(None)
    turn.on_event(_line("delta.message_start", {"message_id": "m1", "message_seq": 9}))
    turn.on_event(_line("delta.text_append", {"message_id": "m1", "text": "he"}))
    turn.on_event(_line("delta.text_append", {"message_seq": 9, "text": "llo"}))
    turn.on_event(_line("delta.message_done", {"message_seq": 9, "content": "hello"}))
    assert turn.body() == "hello"
    # A resubscription restart of the same message starts it over, once.
    turn.on_event(_line("delta.message_start", {"message_id": "m1", "message_seq": 9}))
    turn.on_event(_line("delta.text_append", {"message_seq": 9, "text": "hello"}))
    assert turn.body() == "hello"


def test_part_key_namespaces_ids_by_field():
    """F14: a message_id of "9" and a message_seq of 9 are DIFFERENT
    messages (a reused seq after a resubscribe must not merge into an
    earlier message that happens to share the bare string)."""
    turn = _turn(None)
    turn.on_event(_line("delta.message_start", {"message_id": "9"}))
    turn.on_event(_line("delta.text_append", {"message_id": "9", "text": "first"}))
    turn.on_event(_line("delta.message_start", {"message_seq": 9}))
    turn.on_event(_line("delta.text_append", {"message_seq": 9, "text": "second"}))
    assert turn.body() == "first\n\nsecond"


def test_failed_live_send_leaves_file_for_end_of_turn_retry(env):
    """Fix 7: a live file whose download fails is removed from
    live_order (send_media returns True even on failure), so the
    end-of-turn pass re-sends it instead of it vanishing from both."""
    r = {"path": "sandbox://x.png", "byte_len": 10, "mime": "image/png"}
    turn = _turn(env.bot)
    turn.user_seq = 1
    env.s.download_error = True
    ev = json.loads(_line("delta.presentation", {
        "resources": [dict(r)], "chat_event_seq": 5}))
    env.bot.live_media(turn, ev)
    assert "sandbox://x.png" not in turn.live_order
    assert env.bot.media_sent(OWNER, None, 5, r) is False
    env.s.download_error = False
    res = {"replies": [{"seq": 6, "text": "here it is"}]}
    evs = [{"seq": 5, "event_name": "delta.presentation",
            "payload": {"resources": [dict(r)]}}]
    env.bot.finish_split(turn, res, evs, [], None)
    assert len(env.tg.docs) == 1


def test_await_claim_churn_is_capped(env):
    """F12: a chain of takeovers cannot keep a waiter following claims
    forever: after CLAIM_CHAIN_MAX hops it takes over, bounded."""
    bot = env.bot
    bot.claim_wait_seconds = 0.05
    aid = "a-churn"
    first = tgbot._InFlightClaim()
    with bot.lock:
        bot._inflight_claims[aid] = first
    stop = threading.Event()

    def churn():
        while not stop.is_set():
            with bot.lock:
                bot._inflight_claims[aid] = tgbot._InFlightClaim()
            time.sleep(0.02)

    t = threading.Thread(target=churn, daemon=True)
    t.start()
    out = {}

    def go():
        out["r"] = bot._await_claim({"approval_id": aid}, first)

    waiter = threading.Thread(target=go, daemon=True)
    waiter.start()
    try:
        waiter.join(2)
        assert not waiter.is_alive(), "_await_claim followed the claim chain unbounded"
        done, takeover = out["r"]
        assert done is False and takeover is not None
        assert bot._inflight_claims[aid] is takeover
    finally:
        stop.set()
        t.join(2)


def test_wait_media_total_budget_bounds_all_jobs():
    """wait_media's budget is shared across jobs: three stuck downloads end
    in ~the budget, not 3x the per-job timeout."""
    turn = _turn(None)
    gate = threading.Event()
    ex = concurrent.futures.ThreadPoolExecutor(max_workers=3)
    try:
        turn.media_jobs = [ex.submit(gate.wait) for _ in range(3)]
        t0 = time.monotonic()
        turn.wait_media(timeout=0.3)
        assert time.monotonic() - t0 < 0.6
    finally:
        gate.set()
        ex.shutdown(wait=True)


def test_send_media_failure_releases_claim_for_retry(env):
    """A failed download/send must not poison the claim: the next path
    (background/finish) can send the same file, and success then dedupes."""
    r = {"path": "/tmp/x.png", "byte_len": 10, "mime": "image/png"}
    env.s.download_error = True
    assert env.bot.send_media(OWNER, None, 1, r) is True
    assert any("could not fetch" in (t or "") for t in env.tg.texts())
    assert env.bot.media_sent(OWNER, None, 1, r) is False
    env.s.download_error = False
    assert env.bot.send_media(OWNER, None, 1, r) is True
    assert len(env.tg.docs) == 1
    assert env.bot.media_sent(OWNER, None, 1, r) is True
    assert env.bot.send_media(OWNER, None, 1, r) is False
    assert len(env.tg.docs) == 1


def test_check_background_watermark_advances_only_after_send(env):
    """A failed background send must not advance the watermark past the
    undelivered reply: the next check re-forwards it exactly once."""
    env.s.add(None, "message.assistant", "late reply")
    env.bot.set_watermark(None, 100)
    calls = {"n": 0}
    orig = env.tg.call

    def flaky(method, _http_timeout=None, _retries=None, **params):
        if method == "sendMessage":
            calls["n"] += 1
            if calls["n"] == 1:
                raise TelegramError(method, 500, "boom")
        return orig(method, _http_timeout, _retries, **params)

    env.tg.call = flaky
    with pytest.raises(TelegramError):
        env.bot.check_background(None)
    assert env.bot.watermark(None) == 100
    env.bot.check_background(None)
    assert env.bot.watermark(None) == 101
    assert sum(1 for t in env.tg.texts() if "late reply" in (t or "")) == 1


def test_monitor_scoped_progress_and_watchdog():
    """sessions.updated/tasks.updated never count as progress; a scoped
    turn signal does. 60s without scoped progress ends the subscribe so
    the monitor resubscribes and catches up from history."""
    calls = []

    class Stub:
        def check_background(self, sid, limit=15):
            calls.append((sid, limit))

        def flush_watch(self):
            pass

        def on_pending(self, pend):
            pass

        def watch_event(self, ev):
            pass

    mon = tgbot.Monitor(Stub(), "s1")
    mon.on_record(json.dumps({"event": "sessions.updated",
                              "payload": {"session_id": "s1"}}))
    assert mon.dirty_at in (0.0, None)
    assert mon.last_progress is None
    mon.on_record(json.dumps({"event": "tasks.updated",
                              "payload": {"session_id": "s1"}}))
    assert mon.last_progress is None
    mon.on_record(_line("task.status", {"status": "running", "agent_id": "r",
                                        "root_agent_id": "r", "session_id": "s1"}))
    assert mon.last_progress is not None
    mon.last_progress = time.monotonic() - 61
    mon.dirty_at = None
    assert mon.tick() is True
    assert calls and calls[0][0] == "s1"


def test_session_busy_until_finish_turn_delivery_completes(env):
    """The monitor must not forward a turn while finish_turn is still
    delivering it: session_busy stays true until delivery returns."""
    bot = env.bot
    turn = tgbot.Turn(bot, OWNER, None, "Main")
    bot.turns[OWNER] = turn
    turn.user_seq = 100
    turn.finished = True
    seen = []

    def delivering(chat_id, msg_id, text, markup=None, stem="muse-reply"):
        seen.append(bot.session_busy(None))
        return None

    bot.deliver_reply = delivering
    bot.turn_history = lambda t: []
    bot.finish_turn(turn, {"outcome": "done", "replies": [{"text": "hi", "seq": 101}]})
    assert seen == [True]
    assert turn.delivering is False
    assert bot.session_busy(None) is False


def test_after_turn_watermark_capped_to_delivered(env):
    """after_turn must not skip events the turn did not deliver: the caller
    caps the watermark at the last delivered seq."""
    turn = tgbot.Turn(env.bot, OWNER, None, "Main")
    evs = [ev(101, "message.assistant", "reply"), ev(105, "message.assistant", "later")]
    env.bot.after_turn(turn, evs, upto=101)
    assert env.bot.watermark(None) == 101


def test_tgapi_pool_post_is_bounded(monkeypatch):
    """A hung Bot API worker thread cannot hang the caller forever:
    _post returns (TimeoutError) within its HTTP timeout + grace."""
    monkeypatch.setattr(tgapi, "_POST_GRACE", 0.2)
    api = TelegramAPI(TOKEN)
    monkeypatch.setattr(api, "_post_now",
                        lambda method, timeout, **kw: time.sleep(20))
    t0 = time.monotonic()
    with pytest.raises(concurrent.futures.TimeoutError):
        api.call("sendMessage", _http_timeout=1, chat_id=1, text="x")
    assert time.monotonic() - t0 < 5
