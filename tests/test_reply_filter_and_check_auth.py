"""Shared self-referential reply filter (live muse.ai shapes) across
wait.replies_after / cli.is_reply / chat.new_replies, and AuthError from
the periodic server_check thread surfacing out of the wait."""
import threading
import time

import pytest

from muse_cli import chat, cli
from muse_cli.gateway import AuthError
from muse_cli.wait import is_self_referential, replies_after


def live_push(seq, exec_id, root=None, text="push"):
    """A proactive push as muse.ai history returns it: message_id is
    'assistant-msg-<exec id>', reply_to is the bare execution root id."""
    root = root or exec_id
    mid = f"assistant-msg-{exec_id}"
    return {"seq": seq, "event_name": "message.assistant", "message_id": mid,
            "reply_to_message_id": root,
            "payload": {"display_text": text, "content": text, "status": "completed",
                        "message_id": mid, "reply_to_message_id": root,
                        "metadata": {"root_message_execution_id": root,
                                     "message_kind": "message"}}}


def live_reply(seq, exec_id, text="answer"):
    mid = f"assistant-msg-{exec_id}"
    return {"seq": seq, "event_name": "message.assistant", "message_id": mid,
            "reply_to_message_id": "",
            "payload": {"display_text": text, "content": text, "status": "completed",
                        "message_id": mid,
                        "metadata": {"root_message_execution_id": exec_id,
                                     "message_kind": "message"}}}


def test_live_shaped_push_is_self_referential():
    assert is_self_referential(live_push(11, "235c53a0"))[0] is True


def test_live_shaped_second_push_message_points_at_execution_root():
    ev = live_push(12, "fa7cc1a5", root="93a810d8")
    assert is_self_referential(ev)[0] is True


def test_live_shaped_genuine_reply_kept_everywhere():
    evs = [live_push(11, "p1"), live_reply(12, "r1"), live_push(13, "p2", root="p0")]
    assert [r["text"] for r in replies_after(evs, after_seq=10)] == ["answer"]
    assert [cli.is_reply(e, 10) for e in evs] == [False, True, False]


def test_reply_to_other_message_kept():
    ev = live_reply(12, "r1")
    ev["payload"]["reply_to_message_id"] = "user-msg-9"
    assert is_self_referential(ev)[0] is False


class _HistGW:
    def __init__(self, evs):
        self.evs = evs

    def call_json(self, method, body=None, **_kw):
        return {"chat_events": list(self.evs)}


def _chat(evs):
    c = chat.Chat.__new__(chat.Chat)
    c.cfg, c.st, c.gw, c.listing = {}, {"session_id": None}, _HistGW(evs), []
    return c


def test_chat_new_replies_uses_shared_self_referential_rule():
    push = live_push(11, "p1")
    genuine = live_reply(12, "r1")
    answers_user = live_reply(13, "r2", text="re: you")
    answers_user["payload"]["reply_to_message_id"] = "user-msg-9"
    got = _chat([push, genuine, answers_user]).new_replies(10)
    assert [e["seq"] for e in got] == [12, 13]
