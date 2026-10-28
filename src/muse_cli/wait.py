"""Block until the agent's turn is really over.

Live `chat.subscribe` events that matter (observed on muse01):
  task.status        payload {status: running|completed|..., agent_id,
                     root_agent_id, task_id, session_id, is_thread,
                     active_subagent_count}
  approvals.snapshot payload {pending_approvals: [...], recent_approvals: [...]}
  message.assistant / delta.*  partial or full reply text (ignored here;
                     the final text is re-read from chat.history)
On subscribe the server first replays the current task.status and
approvals.snapshot, so a stale "completed" can arrive before the turn we
care about starts. Only `completed` (DONE) and `interrupted` (FAILED) are
proven terminal values on muse01, with `cancelled`/`canceled` retained as
FAILED from the pre-existing cancel flow; every other status string is
suspect and needs a chat.history proof (see settle). `task_id` is agent
identity, not a turn epoch: scope by agent/root id + session +
running-before-terminal.
"""
import json
import logging
import re
import time

log = logging.getLogger("muse_cli.wait")

# Proven on muse01 (N=30 turns completed, N=2 cancels interrupted). Every
# other string is suspect: it may end the wait only with history proof.
# `cancelled`/`canceled` are RETAINED as FAILED from the pre-existing
# cancel flow (the base FAILED set below, plus the tgbot.py comment "The
# turn ends on Muse's cancelled status"): muse01 never observed them, so
# they are NOT muse01-measured, but dropping them would leave a stopped
# turn waiting until its timeout. They still need running-seen and never
# settle from a replay.
DONE = {"completed"}
FAILED = {"interrupted", "cancelled", "canceled"}
SUSPECT_DONE = {"idle", "complete", "done", "finished", "succeeded", "success"}
SUSPECT_FAILED = {"failed", "failure", "error", "errored", "aborted", "timed_out"}
TERMINAL = DONE | FAILED | SUSPECT_DONE | SUSPECT_FAILED

EXIT_OK, EXIT_FAILED, EXIT_HUMAN, EXIT_TIMEOUT = 0, 2, 4, 124

WIDGET_RE = re.compile(r"\[\[hatch_widget:[^\]]+\]\]")


def _payload(ev):
    if not isinstance(ev, dict):
        return {}
    p = ev.get("payload")
    return p if isinstance(p, dict) else {}


def event_name(ev):
    if not isinstance(ev, dict):
        return None
    return ev.get("event") or ev.get("event_name") or ev.get("type")


def in_scope(ev, thread=None):
    p = _payload(ev)
    if thread:
        return p.get("session_id") in (None, thread)
    return not p.get("is_thread", False)


def task_status_info(ev, thread=None):
    """Scoped task.status facts, or None when the event is not the root
    agent's task status for THREAD.

    root_confirmed is False when root_agent_id is missing (suspect: the
    event cannot be proved to be the root agent's) or the agent is a
    subagent (then the event is ignored entirely, info None)."""
    if not isinstance(ev, dict) or event_name(ev) != "task.status" or not in_scope(ev, thread):
        return None
    p = _payload(ev)
    root = p.get("root_agent_id")
    agent = p.get("agent_id")
    if root and agent and agent != root:
        return None  # a subagent finishing is not the turn finishing
    s = p.get("status")
    return {"status": str(s).lower() if s else None,
            "root_confirmed": bool(root and agent == root),
            "root_agent_id": root, "agent_id": agent,
            "active_subagent_count": p.get("active_subagent_count")}


def task_status(ev, thread=None):
    """Lower-cased status of the root agent's task, or None."""
    info = task_status_info(ev, thread)
    return info["status"] if info else None


def pending_approvals(ev):
    if not str(event_name(ev) or "").startswith("approvals"):
        return None
    p = _payload(ev)
    pend = p.get("pending_approvals")
    if pend is None:
        pend = p.get("pending")
    return pend if isinstance(pend, list) else None


def describe_approval(a):
    """Describe one approval; never crashes on malformed payloads.

    A non-dict approval, display, question or payload degrades to empty
    fields (approval_id None) instead of raising into the waiter: a
    malformed snapshot must not kill a wait."""
    if not isinstance(a, dict):
        log.debug("describe_approval: non-dict approval %r", type(a).__name__)
        return {"approval_id": None, "title": None, "question": None, "kind": None,
                "scope": None, "why": None, "expires_at_ms": None, "options": None}
    d = a.get("display") if isinstance(a.get("display"), dict) else {}
    q = d.get("permission_question") if isinstance(d.get("permission_question"), dict) else {}
    payload = a.get("payload") if isinstance(a.get("payload"), dict) else {}
    rows = {r.get("label"): r.get("value") for r in d.get("detail_rows") or [] if isinstance(r, dict)}
    return {"approval_id": a.get("approval_id"),
            "title": d.get("summary_title") or d.get("display_name"),
            "question": (q.get("text") or "").replace("{assistant}", "Muse") or None,
            "kind": q.get("kind") or payload.get("type"),
            "scope": q.get("scope") or rows.get("Host"),
            "why": d.get("short_explanation") or d.get("reason") or a.get("reason"),
            "expires_at_ms": a.get("request_expires_at_ms"),
            # The choices Muse offers in the app, e.g. allow_once ("Allow
            # once"), allow_always ("Always allow this site" = forever), deny.
            "options": [o.get("kind") for o in a.get("decision_options") or []
                        if isinstance(o, dict) and o.get("kind")] or None}


FOREVER = "allow_always"  # Muse's "forever" / "Always allow" choice
ONCE = "allow_once"     # Muse's "Allow once" choice


def offers_forever(described):
    """True when the approval can be decided with Muse's forever choice.
    Approvals that list no options (older servers) are assumed to accept it."""
    opts = described.get("options")
    return not opts or FOREVER in opts


def offers_once(described):
    """True only when allow_once is explicitly listed in the options."""
    opts = described.get("options")
    return bool(opts) and ONCE in opts


def choose_auto_decision(described, auto_approve, allow_once=False):
    """The decision an auto-approver may send, or None to leave it for a human.

    allow_always wins when offered. Otherwise allow_once, and only when the
    caller explicitly enabled the fallback (allow_once=True) and auto-approve
    is on. Never picks options[0], never allow_session/allow_today, and
    never restricts by kind: the caller decides the policy, this helper
    only reads the offered options. An approval listing no options keeps
    the offers_forever assumption (allow_always), it is not turned into
    allow_once.
    """
    if not auto_approve:
        return None
    if offers_forever(described):
        return FOREVER
    if allow_once and offers_once(described):
        return ONCE
    return None


def open_widgets(ev):
    """Unanswered hatch widgets (questions for the human) in a history event."""
    p = _payload(ev)
    out = []
    for w in p.get("widgets") or []:
        if not isinstance(w, dict):
            continue
        st = w.get("state") if isinstance(w.get("state"), dict) else {}
        answered = any(st.get(k) not in (None, "", [], {}) for k in
                       ("value", "selected", "selection", "answer", "submitted", "response"))
        if not answered:
            data = w.get("data") if isinstance(w.get("data"), dict) else {}
            out.append({"widget_id": w.get("widget_id"), "kind": w.get("kind"),
                        "text": w.get("display_text") or w.get("fallback_text"),
                        "options": data.get("options")})
    return out


def _event_has_media(ev):
    """True when a history event carries a downloadable file, by the same
    path rule as cli.event_resources (sandbox:// or /media/raw/)."""
    p = _payload(ev)
    data = p.get("data") if isinstance(p.get("data"), dict) else {}
    items = []
    for val in data.values():
        if isinstance(val, list):
            items.extend(val)
    for src in (ev.get("resources"), p.get("resources")):
        if isinstance(src, list):
            items.extend(src)
    for item in items:
        if isinstance(item, str):
            item = {"path": item}
        if not isinstance(item, dict):
            continue
        variants = item.get("variants")
        path = item.get("path") or (variants.get("original")
                                    if isinstance(variants, dict) else None)
        if path and (str(path).startswith("sandbox://") or "/media/raw/" in str(path)):
            return True
    return False


def clean_text(text):
    return WIDGET_RE.sub("", text or "").strip()


class Waiter:
    """Feed live events; `decide()` says whether to stop and why.

    require_running is kept for callers; replay protection no longer
    depends on it: a terminal status ends the wait directly only after a
    root `running` was seen, whatever the flag. Anything earlier is a
    replayed stale state and must be settled against chat.history.
    approver: optional callable(list of describe_approval dicts) that decides
    new pending approvals (auto-approve) and returns the ids it handled.
    Handled approvals do not stop the wait; anything it could not handle
    still ends it with ("approval", ...). Each id is offered once, and an
    approver that raises is never called again for that id: decide is not
    idempotent, so a lost reply must never be re-decided blindly.
    """
    def __init__(self, thread=None, require_running=False, approver=None):
        self.thread = thread
        self.require_running = require_running
        self.approver = approver
        self.handled = set()    # approval ids the approver already decided
        self.offered = set()    # approval ids already passed to the approver
        self.approver_failed = set()  # ids the approver raised on: never re-offer
        self.seen_running = False
        self.result = None      # ("done"|"failed"|"approval", detail)
        self.status = None
        # A terminal status that did not end the wait (replay, suspect
        # string, or subagents still running). The caller settles it with
        # chat.history (see settle); it is cleared only on success.
        self.ignored_terminal = None
        self.ignored_reason = None  # "subagents" when the root terminal arrived
        # while subagents were still running: settle must not accept it.
        self.ignored_terminal_seq = None  # seq of the ignored terminal record
        # (None when the record carried no seq: accept_replayed_failed
        # cannot prove the terminal belongs to the boundary turn then).
        self.root_id = None  # learned ONLY from a root-confirmed `running`
        # (a subagent's own task.status carries agent_id == its own
        # root_agent_id, a different root value: learning from any
        # passing task.status would learn the wrong root)
        self.boundary_unknown = False  # history boundary unreadable: no
        # settle-from-replay until a live root `running` is seen
        self.running_seq = None  # seq of the first root-confirmed running
        # The wait's boundary, and whether it was derived from history (a
        # standalone wait) rather than given by the caller: only a derived
        # boundary lets a replayed cancel end the wait (accept_replayed_failed).
        self.after_seq = None
        self.boundary_derived = False

    def _expired(self, described):
        exp = described.get("expires_at_ms")
        return isinstance(exp, (int, float)) and exp <= time.time() * 1000

    def feed(self, ev):
        if isinstance(ev, (bytes, str)):
            try:
                ev = json.loads(ev)
            except json.JSONDecodeError:
                return None
        if not isinstance(ev, dict):
            return None
        pend = pending_approvals(ev)
        if pend:
            p = _payload(ev)
            recent = set()
            for lst in (p.get("recent_approvals"), p.get("recent")):
                if isinstance(lst, list):
                    for e in lst:
                        if isinstance(e, dict) and e.get("approval_id") and e.get("decision"):
                            recent.add(e["approval_id"])
            self.handled.update(recent & {d["approval_id"] for d in
                                          (describe_approval(a) for a in pend)})
            described = [d for d in (describe_approval(a) for a in pend)
                         if not self._expired(d)]
            if self.approver:
                new = [d for d in described
                       if d["approval_id"] is not None
                       and d["approval_id"] not in self.offered
                       and d["approval_id"] not in self.approver_failed
                       and d["approval_id"] not in self.handled]
                if new:
                    self.offered.update(d["approval_id"] for d in new)
                    try:
                        self.handled.update(self.approver(new) or ())
                    except Exception as e:  # noqa: BLE001 - decide is not
                        # idempotent: never re-offer after a failed decide.
                        log.warning("approver failed, leaving for a human: %s", e)
                        self.approver_failed.update(d["approval_id"] for d in new)
                left = [d for d in described if d["approval_id"] not in self.handled]
                if not left:
                    return None  # all auto-approved; keep waiting for the turn
                described = left
            elif described:
                pass
            else:
                return None  # every pending approval is expired: keep waiting
            self.result = ("approval", described)
            return self.result
        info = task_status_info(ev, self.thread)
        if info is None or not info["status"]:
            return None
        root = info["root_agent_id"]
        # F8: a later record showing no subagents running re-arms a
        # terminal stored while subagents ran (the terminal itself is
        # kept; only its "subagents" reason is cleared, so the next
        # settle may accept it).
        if (self.ignored_reason == "subagents"
                and info["active_subagent_count"] == 0):
            self.ignored_reason = None
        s = info["status"]
        if s == "running":
            if info["root_confirmed"]:
                if self.root_id is None:
                    self.root_id = root
                if root == self.root_id:
                    self.seen_running = True
                    self.ignored_terminal = None
                    self.ignored_reason = None
                    self.ignored_terminal_seq = None
                    self.boundary_unknown = False
                    if self.running_seq is None:
                        self.running_seq = ev.get("seq")
            self.status = s
            return None
        if s not in TERMINAL:
            return None  # queued/starting/working/pending_init: never progress
        # A terminal from a different root (another agent sharing the
        # session scope) is not this turn; it teaches nothing.
        if self.root_id is not None and root and root != self.root_id:
            return None
        self.status = s
        if info["active_subagent_count"]:
            # Root says done while subagents still run (muse01: a subagent
            # `completed` arrived 4.5 s before the root's): suspect, re-check.
            self.ignored_terminal = s
            self.ignored_terminal_seq = ev.get("seq")
            self.ignored_reason = "subagents"
            return None
        if not info["root_confirmed"]:
            # Missing root id (absent, or a subagent id with no root id):
            # suspect, never a direct end. Settle decides with history.
            self.ignored_terminal = s
            self.ignored_terminal_seq = ev.get("seq")
            self.ignored_reason = "missing_root"
            return None
        if s in DONE or s in FAILED:
            if self.seen_running:
                self.result = ("done" if s in DONE else "failed", s)
                return self.result
            self.ignored_terminal = s
            self.ignored_terminal_seq = ev.get("seq")
            self.ignored_reason = None
            return None
        # Suspect string (idle/done/failed/...): needs history proof.
        self.ignored_terminal = s
        self.ignored_terminal_seq = ev.get("seq")
        self.ignored_reason = None
        return None

    def settle(self, replies):
        """Accept the ignored terminal status when chat.history already holds
        a reply to our message: the turn ran and ended before the
        subscription started, or under a suspect status string. Returns
        the result or None. The ignored terminal is cleared only on
        success; `interrupted` never settles here (a replayed FAILED
        terminal is handled by accept_replayed_failed, which proves it
        belongs to the boundary turn from the terminal's seq), and a
        terminal stored while subagents ran needs a re-check first."""
        s = self.ignored_terminal
        if self.result is not None or not s:
            return None
        if self.boundary_unknown:
            # The history boundary could not be read: a history proof
            # cannot be trusted not to be stale. Disabled until a live
            # root `running` clears the flag (see feed).
            return None
        if self.ignored_reason == "subagents" or s in FAILED:
            return None
        if not replies:
            return None
        self.ignored_terminal = None
        self.ignored_reason = None
        self.ignored_terminal_seq = None
        self.result = ("failed" if s in SUSPECT_FAILED else "done", s)
        return self.result

    def accept_replayed_failed(self, latest_user_seq):
        """Exit 2 for a replayed FAILED terminal that provably ends the
        awaited turn: a standalone wait started after a cancel.

        Proof, all required:
        - the terminal is FAILED (not SUSPECT_FAILED) and root-confirmed
          (ignored_reason None: not missing-root/subagents-suspect);
        - the boundary was DERIVED from history (boundary_derived: a
          standalone `wait`, no explicit --after-seq). An explicit
          boundary (`wait --after-seq`, `send --wait`, the Telegram
          follower) names a turn that may not have started yet, so a
          replayed cancel cannot be proved to be its own: never accepted;
        - the boundary is known and LATEST_USER_SEQ == after_seq: no user
          message newer than the boundary exists.
        The replayed task.status seq is never used: live it is a stream
        counter (e.g. 5988 while chat.history seqs were ~5196), not a
        chat seq, and the event carries no turn marker (task_id is agent
        identity). Exits with 0 replies: a cancelled turn's partials are
        never final replies."""
        s = self.ignored_terminal
        if self.result is not None or not s or s not in FAILED:
            return None
        if self.boundary_unknown or self.ignored_reason is not None:
            return None
        if not self.boundary_derived or self.after_seq is None:
            return None
        if latest_user_seq is None or latest_user_seq != self.after_seq:
            return None
        self.ignored_terminal = None
        self.ignored_reason = None
        self.ignored_terminal_seq = None
        self.result = ("failed", s)
        return self.result


_ASSISTANT_PREFIX = "assistant-msg-"


def is_self_referential(ev):
    """Return (True, level, matched_id) when the event's reply_to_message_id
    (event level or payload level) equals the message's OWN id, or
    (False, None, None) otherwise.

    Only an exact match proves a proactive push: the push's reply_to
    points at itself. A genuine reply carries reply_to pointing at the
    message it answers (a user message, or another assistant message),
    and must be kept. When the event and payload ids differ, a match to
    either own id drops it. When no id exists at either level,
    self-reference cannot be proved and the event is kept (fail open).
    "Own id" covers the live forms: the message_id itself, the message_id
    without its "assistant-msg-" prefix, and payload.metadata
    .root_message_execution_id (a push's 2nd+ message points at its
    execution root). Observed on the default muse.ai VM: all 17 pushes
    with a reply_to in a 300-event window matched one of these, none
    matched the bare message_id."""
    p = _payload(ev)
    own = set()
    for mid in (p.get("message_id"), ev.get("message_id")):
        if mid:
            own.add(mid)
            # Live shape (muse.ai history): message_id is
            # "assistant-msg-<execution id>" while a push's reply_to is the
            # bare execution id, so the prefix is stripped for the match.
            if isinstance(mid, str) and mid.startswith(_ASSISTANT_PREFIX):
                own.add(mid[len(_ASSISTANT_PREFIX):])
    meta = p.get("metadata") if isinstance(p.get("metadata"), dict) else {}
    if meta.get("root_message_execution_id"):
        # Later messages of one push execution point at the execution's
        # root id, which differs from their own message_id suffix.
        own.add(meta["root_message_execution_id"])
    for level, ref in (("event", ev.get("reply_to_message_id")),
                       ("payload", p.get("reply_to_message_id"))):
        if ref and ref in own:
            return True, level, ref
    return False, None, None


def replies_after(events, after_seq, upto=None):
    """Complete assistant messages with seq > after_seq (and <= UPTO when
    given), oldest first, deduped by (seq, message_id).

    Only `completed` message.assistant events count: an event still
    being written has no status, an `interrupted` one is a partial, and
    a self-referential event (reply_to_message_id equal to the
    message's own message_id, at either level) is a proactive push, not
    a reply to our message. A reply_to pointing at another id (the user
    message it answers, or another assistant message) is kept. Entries
    with no text are kept when they carry media or an unanswered widget
    (media=True)."""
    out = []
    seen = set()
    dropped = []
    for e in sorted(events, key=lambda e: e.get("seq") or 0):
        if not isinstance(e, dict):
            continue
        seq = e.get("seq") or 0
        if seq <= after_seq or (upto is not None and seq > upto):
            continue
        if e.get("event_name") != "message.assistant":
            continue
        p = _payload(e)
        if p.get("status") != "completed":
            # Text-bearing events dropped for lacking a completed status
            # are logged, not lost silently (F15): a status-less partial
            # must be visible in the log when a wait reports no reply.
            if p.get("display_text") or p.get("content") or e.get("display_text"):
                dropped.append(seq)
            continue
        self_ref, level, matched = is_self_referential(e)
        if self_ref:
            # A proactive push points at itself; a genuine reply points
            # at another id and is kept. Drops are logged, never silent.
            log.debug("replies_after: dropping self-referential push seq=%s "
                      "level=%s reply_to=%r", seq, level, matched)
            continue
        mid = p.get("message_id") or e.get("message_id")
        if (seq, mid) in seen:
            continue
        seen.add((seq, mid))
        text = p.get("display_text") or p.get("content") or e.get("display_text") or ""
        w = open_widgets(e)
        has_media = _event_has_media(e)
        if not text and not w and not has_media:
            continue
        r = {"seq": seq, "message_id": mid, "text": clean_text(text)}
        if w:
            r["widgets"] = w
        if has_media:
            r["media"] = True
        out.append(r)
    if dropped:
        log.warning("replies_after: dropping %d text-bearing assistant event(s) "
                    "without payload.status='completed' (seqs %s)",
                    len(dropped), dropped)
    return out


_WAIT_PHRASES = [
    # Vietnamese
    "chờ boss", "chờ bạn", "tôi chờ", "mình chờ", "đợi boss", "đợi bạn", "boss muốn",
    "bạn muốn", "boss có muốn", "bạn có muốn", "trước khi tiếp tục", "trước khi làm tiếp",
    "boss xác nhận", "bạn xác nhận", "cần boss", "cần bạn", "boss quyết", "bạn quyết",
    "hay không?", "được không?", "nhé?",
    # English
    "should i", "do you want", "would you like", "waiting for your", "let me know if",
    "let me know whether", "please confirm", "can you confirm", "before i continue",
    "before i proceed", "shall i", "which do you prefer", "your call",
]
_SAFETY_RE = re.compile(r"safety check|kiểm tra an toàn|cơ chế an toàn|tripped .{0,30}safety|"
                        r"refus(e|ed) to (run|do|continue)|từ chối (chạy|làm)", re.I)
_GATE_RE = re.compile(r"(gate|policy)[ _-]?questions?\s*[:=]\s*(?!none\b)\S", re.I)


def needs_input_reason(text):
    """Why the final reply is waiting on a human, or None. Conservative:
    a question at the end, a known waiting phrase, a non-empty gate/policy
    question field, or a stop because of a safety check."""
    t = clean_text(text or "")
    if not t:
        return None
    low = t.lower()
    if _GATE_RE.search(t):
        return "gate/policy question"
    if _SAFETY_RE.search(t):
        return "stopped by a safety check"
    for ph in _WAIT_PHRASES:
        if ph in low:
            return f"waiting phrase: {ph!r}"
    tail = t.rstrip(" \n*_`)\"'").splitlines()[-1] if t.strip() else ""
    if tail.rstrip().endswith("?") or tail.rstrip().endswith("？"):
        return "ends with a question"
    return None


def question_of(text):
    """The sentence(s) that carry the question, for printing."""
    t = clean_text(text or "")
    qs = [s.strip() for s in re.split(r"(?<=[.!?。])\s+", t) if "?" in s]
    return " ".join(qs[-2:]) if qs else t.splitlines()[-1] if t else ""


def outcome_of(kind, replies=()):
    """done | needs_input | needs_approval | failed | timeout."""
    if kind == "approval":
        return "needs_approval"
    if kind == "failed":
        return "failed"
    if kind == "done":
        last = replies[-1] if replies else None
        if last and (last.get("widgets") or needs_input_reason(last.get("text"))):
            return "needs_input"
        return "done"
    return "timeout"


def exit_code(kind, replies=()):
    return {"done": EXIT_OK, "needs_input": EXIT_HUMAN, "needs_approval": EXIT_HUMAN,
            "failed": EXIT_FAILED}.get(outcome_of(kind, replies), EXIT_TIMEOUT)


def user_ack(events, baseline, text=None):
    """seq of our message.user event (> baseline), or None."""
    for e in sorted(events, key=lambda e: e.get("seq") or 0):
        if (e.get("seq") or 0) > baseline and e.get("event_name") == "message.user":
            p = _payload(e)
            t = p.get("display_text") or p.get("content") or e.get("display_text") or ""
            if text is None or text.strip() in t or t.strip() in text:
                return e.get("seq")
    return None
