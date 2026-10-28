"""Turn-end detection, D3/D4/D7 (test-first, T4/T5/T8).

Seams under test: Waiter.feed/settle and replies_after in muse_cli.wait
(public behaviour: what a waiter decides from live events + history).
Expected values come from the muse01 measurements in
~/workspace/tmp/muse-cli-wait-design/2b-probe-results-muse01.md, not from
the implementation: DONE=completed only (N=30 turns), FAILED=interrupted
only (N=2 cancels), running is the only in-progress value, pending_init
never progresses. (The first T10 report, 2-probe-results.md, is polluted
by the default/self profile and must not be cited.)
"""
import json
import time

import pytest

from muse_cli.wait import (DONE, FAILED, Waiter, describe_approval, exit_code,
                           replies_after, task_status_info)

ROOT = "root-agent"
SUB = "sub-agent"


def ts(status, **kw):
    p = {"status": status, "agent_id": ROOT, "root_agent_id": ROOT,
         "is_thread": False, "session_id": "main"}
    p.update(kw)
    return {"event": "task.status", "payload": p, "seq": 1, "type": "event"}


def hist(seq, text="reply", status="completed", reply_to=None, event_reply_to=None,
         name="message.assistant", **payload_kw):
    p = {"display_text": text, "message_id": f"m{seq}", "status": status}
    if reply_to is not None:
        p["reply_to_message_id"] = reply_to
    p.update(payload_kw)
    ev = {"seq": seq, "event_name": name, "payload": p}
    if event_reply_to is not None:
        ev["reply_to_message_id"] = event_reply_to
    return ev


REPLY = [hist(10, "final reply")]


# -- D3 vocabulary / scope -------------------------------------------------

def test_proven_vocabulary_sets():
    assert DONE == {"completed"}
    assert FAILED == {"interrupted"}


def test_completed_without_running_never_ends_wait_alone():
    for require in (True, False):
        w = Waiter(require_running=require)
        assert w.feed(ts("completed")) is None
        assert w.result is None
        assert w.settle([]) is None
        assert w.settle(REPLY) == ("done", "completed")


def test_pending_init_never_sets_seen_running():
    w = Waiter(require_running=True)
    assert w.feed(ts("pending_init")) is None
    assert w.seen_running is False
    assert w.feed(ts("completed")) is None
    assert w.settle([]) is None
    # Subagent pending_init likewise never progresses the root turn.
    w2 = Waiter(require_running=True)
    assert w2.feed(ts("pending_init", agent_id=SUB)) is None
    assert w2.seen_running is False


def test_only_running_sets_seen_running():
    w = Waiter(require_running=True)
    for s in ("queued", "starting", "working", "pending_init", "idle"):
        assert w.feed(ts(s)) is None
        assert w.seen_running is False, s
    assert w.feed(ts("running")) is None
    assert w.seen_running is True
    assert w.feed(ts("completed")) == ("done", "completed")


def test_subagent_completed_is_ignored():
    w = Waiter()
    assert w.feed(ts("completed", agent_id=SUB)) is None
    assert w.feed(ts("running")) is None
    assert w.feed(ts("completed", agent_id=SUB)) is None
    assert w.result is None
    assert w.feed(ts("completed")) == ("done", "completed")


def test_missing_root_id_is_suspect():
    w = Waiter()
    assert w.feed(ts("running", root_agent_id=None)) is None
    assert w.seen_running is False
    assert w.feed(ts("completed", root_agent_id=None)) is None
    assert w.settle(REPLY) == ("done", "completed")


def test_terminal_for_a_different_root_is_ignored():
    w = Waiter(require_running=True)
    assert w.feed(ts("running")) is None
    other = ts("completed", agent_id="other", root_agent_id="other")
    assert w.feed(other) is None
    assert w.result is None
    assert w.feed(ts("completed")) == ("done", "completed")


def test_root_terminal_with_subagents_running_is_suspect():
    w = Waiter()
    assert w.feed(ts("running", active_subagent_count=1)) is None
    assert w.feed(ts("completed", active_subagent_count=1)) is None
    assert w.result is None
    assert w.settle(REPLY) is None  # kept, not cleared: re-check required
    assert w.ignored_terminal == "completed"
    assert w.feed(ts("completed", active_subagent_count=0)) == ("done", "completed")


def test_interrupted_after_running_is_failed_without_history():
    w = Waiter(require_running=True)
    assert w.feed(ts("running")) is None
    assert w.feed(ts("interrupted")) == ("failed", "interrupted")
    assert exit_code("failed") == 2


def test_interrupted_without_running_never_settles():
    w = Waiter()
    assert w.feed(ts("interrupted")) is None
    assert w.settle(REPLY) is None
    assert w.settle([]) is None
    assert w.result is None


@pytest.mark.parametrize("s", ["idle", "done", "finished", "succeeded", "success", "complete"])
def test_suspect_done_strings_require_history_proof(s):
    w = Waiter()
    assert w.feed(ts("running")) is None
    assert w.feed(ts(s)) is None
    assert w.settle([]) is None
    assert w.settle(REPLY) == ("done", s)


@pytest.mark.parametrize("s", ["failed", "failure", "error", "errored", "cancelled",
                               "canceled", "aborted", "timed_out"])
def test_suspect_failed_strings_require_history_proof(s):
    w = Waiter()
    assert w.feed(ts("running")) is None
    assert w.feed(ts(s)) is None
    assert w.settle([]) is None
    assert w.settle(REPLY) == ("failed", s)


def test_settle_clears_ignored_terminal_only_on_success():
    w = Waiter()
    assert w.feed(ts("completed")) is None
    assert w.settle([]) is None
    assert w.ignored_terminal == "completed"
    assert w.settle(REPLY) == ("done", "completed")
    assert w.ignored_terminal is None


def test_task_status_info_reports_scope():
    info = task_status_info(ts("running"))
    assert info["status"] == "running" and info["root_confirmed"] is True
    assert info["active_subagent_count"] is None
    assert task_status_info(ts("completed", agent_id=SUB)) is None
    suspect = task_status_info(ts("running", root_agent_id=None))
    assert suspect is not None and suspect["root_confirmed"] is False


# -- D4 history settle / reply filtering ------------------------------------

def test_replies_after_requires_status_and_empty_reply_to():
    evs = [
        hist(1, "old"),
        hist(7, "genuine"),
        hist(8, "push", reply_to="m8"),
        hist(9, "push-ev", event_reply_to="m9"),
        hist(10, "writing", status=None),
        hist(11, "delta", name="delta.presentation"),
        hist(12, "other", name="message.user"),
    ]
    r = replies_after(evs, 6)
    assert [x["seq"] for x in r] == [7]


def test_replies_after_dedups_and_applies_upto():
    evs = [hist(7, "a"), hist(7, "a"), hist(9, "b"), hist(12, "c")]
    assert [x["seq"] for x in replies_after(evs, 6)] == [7, 9, 12]
    assert [x["seq"] for x in replies_after(evs, 6, upto=9)] == [7, 9]


def test_replies_after_keeps_media_only_and_widget_only_entries():
    media = hist(7, "", data={"images": [{"path": "sandbox://workspace/pic.png"}]})
    widget = hist(8, "", widgets=[{"widget_id": "w1", "kind": "option",
                                    "display_text": "pick", "data": {"options": ["A"]},
                                    "state": {"version": 0}}])
    r = replies_after([media, widget], 6)
    assert [x["seq"] for x in r] == [7, 8]
    assert r[0].get("media") is True and r[0]["text"] == ""
    assert r[1]["widgets"][0]["widget_id"] == "w1"


def test_replies_after_ignores_delta_presentation_events():
    ev = {"seq": 7, "event_name": "delta.presentation",
          "payload": {"status": "completed",
                      "data": {"images": [{"path": "sandbox://workspace/pic.png"}]}}}
    assert replies_after([ev], 6) == []


def test_replies_after_excludes_interrupted_partials():
    """An interrupted-status assistant event is a partial, not a reply,
    even when its text is long: only completed events deliver."""
    evs = [hist(7, "partial text", status="interrupted"),
           hist(8, "final", status="completed")]
    assert [x["seq"] for x in replies_after(evs, 6)] == [8]


# -- D7 defensive approval parsing / expiry, D5 approver safety -------------

def test_describe_approval_never_crashes_on_garbage():
    assert describe_approval("not-a-dict")["approval_id"] is None
    assert describe_approval({"display": "not-a-dict"})["approval_id"] is None
    assert describe_approval(None)["approval_id"] is None


def test_feed_never_crashes_on_garbage_events():
    w = Waiter()
    assert w.feed(["not", "a", "dict"]) is None
    assert w.feed({"event": "approvals.snapshot",
                   "payload": {"pending_approvals": ["garbage", {"display": "x"}]}}) is not None
    assert w.feed(b"not json") is None
    assert w.result is not None


def test_expired_approvals_are_ignored():
    past = {"approval_id": "a1", "request_expires_at_ms": 1,
            "display": {"summary_title": "old"}}
    w = Waiter()
    assert w.feed({"event": "approvals.snapshot",
                   "payload": {"pending_approvals": [past]}}) is None
    assert w.result is None
    future = {"approval_id": "a2",
              "request_expires_at_ms": int((time.time() + 3600) * 1000),
              "display": {"summary_title": "new"}}
    assert w.feed({"event": "approvals.snapshot",
                   "payload": {"pending_approvals": [future]}}) == (
        "approval", w.result[1])


def test_approver_exception_never_redecides():
    """decide is not idempotent: an approver that raises must not be called
    again for the same id. The wait ends as approval for a human instead
    of crashing or silently re-deciding a possibly-decided approval."""
    calls = []

    def approver(items):
        calls.append([d["approval_id"] for d in items])
        raise ConnectionError("gateway dropped mid-decide")

    snap = {"event": "approvals.snapshot",
            "payload": {"pending_approvals": [{"approval_id": "a1",
                                               "display": {"summary_title": "t"}}]}}
    w = Waiter(approver=approver)
    kind, det = w.feed(snap)
    assert kind == "approval" and det[0]["approval_id"] == "a1"
    assert w.feed(snap)[0] == "approval"
    assert len(calls) == 1


def test_recent_decision_marks_approval_handled_without_reoffer():
    """A decide whose reply was lost shows up in recent_approvals: the
    waiter must treat it as handled, not offer it to the approver again."""
    calls = []

    def approver(items):
        calls.append(items)
        return [d["approval_id"] for d in items]

    snap = {"event": "approvals.snapshot",
            "payload": {"pending_approvals": [{"approval_id": "a1",
                                               "display": {"summary_title": "t"}}],
                        "recent_approvals": [{"approval_id": "a1",
                                              "decision": "allow_always"}]}}
    w = Waiter(approver=approver)
    assert w.feed(snap) is None
    assert w.result is None
    assert calls == []


def test_exit_code_matrix():
    assert exit_code("done", [{"seq": 1, "text": "OK"}]) == 0
    assert exit_code("failed") == 2
    assert exit_code("approval") == 4
    assert exit_code("done", [{"seq": 1, "text": "Should I push?"}]) == 4
    assert exit_code("timeout") == 124
    assert exit_code("bogus-kind") == 124
