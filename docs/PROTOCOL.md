# Protocol notes

How `muse` talks to the muse.ai personal gateway. Derived from the web app's
own client bundle and verified live. Internal APIs are unversioned and can
change without notice; if calls start failing, re-derive from a fresh bundle
(HAR capture + the `derive-client` workflow).

## Layers

```
muse CLI
  │  HTTPS (browser TLS fingerprint via curl-impersonated requests)
  ▼
muse.ai            cookies → POST /api/auth/check → access_token
                   POST /api/hatch/token {vmAddress, vmName} → hatch token
                   GET /api/session → assigned VM id (auto-discovered)
  │  wss://hatch.metaaivm.com/v1/noise?vm_id=..&auth_token=..
  │  Noise_XX_25519_AESGCM_SHA256, empty payloads both ways
  ▼
personal VM gateway
  │  protobuf envelopes (descriptors in desc0.bin / desc1.bin)
  │  request/response + newline-JSON event subscriptions
  ▼
chat / feed / goals / ideas / sessions / ...
```

## Details

- **Gateway host:** the shared `hatch.metaaivm.com`, not the per-VM hostname.
  Plain TLS to the per-VM host is reset at the edge; a Chrome fingerprint
  (curl_cffi `impersonate="chrome"`) is required.
- **Handshake:** standard Noise XX. This account negotiates standard mode
  (empty msg3). An attested/RV mode with SNP attestation and HMAC proofs
  exists in the client for other configurations.
- **Framing:** `NoiseTransportFrame{chunk_id(i64), chunk_index, total_chunks,
  payload}` protobuf → Noise transport encrypt → one WS binary frame each.
  Large responses (e.g. `/approvals` with many recent entries) are split
  across several frames sharing a `chunk_id`; join payloads in `chunk_index`
  order once `total_chunks` have arrived, then parse the `ServiceResponse`.
- **Request:** `ServiceRequest{service, payload =
  ServiceFrame{stream_id, request =
  ApplicationRequest{verb, path, headers, body, end_body}}}`. Stream ids start
  at 1. Services: daemon 0, sentinel 1, vault 2, authd 3.
- **Response:** `ServiceResponse{payload = ServiceFrame{... response =
  ApplicationResponse{status, headers, body, end_body}}}` plus `body_chunk`
  frames until `end_body`.
- **Subscriptions** (`chat.subscribe`, `chat.stream`, ...): same envelope, the
  server streams `application/x-ndjson` records in body chunks.
- **Method table:** `routes.json` (258 methods extracted from the web bundle).

## Server quirks learned from live errors

- `chat.history`: GET query params only; `transcript_mode` is rejected, omit
  it; `session_id` scopes to a side chat; `limit` works. History events use
  `event_name` (`message.user` / `message.assistant`), carry the text in both
  top-level and `payload.display_text`, and are the source of truth.
- Genuine assistant replies have an **empty `reply_to_message_id`**; proactive
  pushes (Telegram drafts, background task updates) are self-referential
  there. That is how `send` tells a reply apart from background chatter.
- Live `chat.subscribe` events use **different shapes** (`delta.text_append`
  with `message_seq` + text chunks, `delta.message_done` with the full
  transcript) and only cover main-chat traffic: threaded replies never arrive
  as live events, just a `sessions.updated` snippet. `send` therefore polls
  history instead of watching the stream. The `chat.stream` response stream
  itself carries only the send echo, then ends.
- `/api/session` can return 200 `{"status":"unavailable",
  "vm_resolution_issue":{"kind":"retryable"}}` while the VM restarts. The
  client raises a clean error (not KeyError); retry, or wake a known VM id
  via `MUSE_VM_ID=<id> muse-cli wake`.
- Concurrent `_read_frame` calls from two threads split frames and corrupt
  the stateful Noise decrypt (fatal BAD_DECRYPT). `Gateway` serializes
  receives with a lock as a backstop, but callers must still keep exactly
  one frame consumer at a time (`send` makes sequential unary calls only).
- **Agent files** (`sandbox://workspace/<p>` in `payload.data.videos[]` /
  `images[]` / `fallback_text`): each entry carries `mime`, `byte_len` and
  `variants.original = https://<vm>.metaaivm.com/media/raw/workspace/<p>`.
  The bytes come from `GET /media/raw/workspace/<p>` on the VM. That route is
  not in `routes.json`; sent as a raw-path daemon request over the Noise
  tunnel it returns the file (body chunks). `Gateway.download_file()` wraps
  this. `artifacts.fetch_url` is unrelated (needs `project_dir`).
- **Plan usage** (Settings > Usage) is not a REST route or a gateway method.
  It is the Next.js server action `fetchSubscriptionAction`: `POST
  https://muse.ai/settings` with cookies, `Next-Action: <id>`, `Accept:
  text/x-component`, body `[{"includeAgreement":false}]`. The RSC reply is
  `0:{"a":"$@1",...}` then `1:{"success":true,"subscription":{tier, usage:
  {state, percentUsed, resetsAt (unix s), quotaStatus}, statusSubtitle,
  usageRowLabel, topupBalance, topupTotal, topupRowLabel,
  entryPointCtaLabel, ...}}`. The bonus token % is the web app's own
  `round(100*(total-clamp(balance))/total)`. `topupBalance`/`topupTotal` are NOT
  tokens (120M units showed as "3B tokens left"); the web app never converts
  them and prints `topupRowValueLabel` verbatim, so the CLI parses tokens
  left from that label. The action id is a build hash
  (`4029ac5c...` as of 2026-10-23); when it stops working the client looks it
  up again by scanning `/_next/static/chunks/*.js` for
  `createServerReference)("<id>",...,"fetchSubscriptionAction")`, then caches
  it as `subscription_action_id` in config.json.
- **Invite friends** is a plain REST route on muse.ai (cookies only):
  `GET /api/hatch/invite` returns `{invite_state, code, invite_link,
  max_uses, use_count, uses_remaining, share_text, reward{title, body},
  subtitle, has_redeemed_invite_code, redeem_invite_code_screen, ...}`. The
  earned stats come only as display text in `reward`. `POST` (no body) to the same URL
  creates the code and returns `{code, max_uses (30), use_count, invite_link
  (https://muse.ai/join), share_text}`; `invite --create` sends it only when
  GET shows `code: null`; codes match
  `^[A-Z0-9]{6,16}$`. Found via `useHatchInvitePageQuery` in the bundle.
- **Redeem a code:** `POST /api/hatch/invite-code/redeem` `{code,
  supportsRedemptionStatus: true}`. Success: `{success: true,
  redemptionStatus: "redeemed"}`. Failure (400): `{success: false, reason,
  detail}`, e.g. `window_expired` ("Your 48-hour window to redeem a code has
  passed."), `already_redeemed`, `invalid`.
- `session.start`: `{method: "/api/session/start", params: {origin: "fresh",
  lifecycle: "persistent", title?}}`.
- `session.rename`: flat `{session_id, title}`. pin/unpin/archive/unarchive/
  delete: `{method: "/api/session/<op>", session_id}`.
- `api.idea-cards.execute`: `{ideaCardId, mode: "full"}` plus path param.
- POSTs to muse.ai need browser `Sec-Fetch-*` headers or they return 403.
- `auth export` reads cookies from the user's own Google Chrome through
  agent-browser `--auto-connect`. Chrome 144+ shares that profile only after
  remote debugging is enabled at `chrome://inspect/#remote-debugging`. The
  export follows the active tab, so a muse.ai tab has to be focused first,
  and it must never overwrite a working jar without a `hatch_sess` in the
  new one. A freshly launched browser has no login; do not fall back to one.

## System Files (fs.*)

All POST with JSON bodies; paths relative to the agent home (System Files
root); absolute paths -> 400 "absolute paths are not allowed", `..` -> 400
"path traversal is not allowed". Every field listed is required (serde 422
otherwise). Responses are `{"ok":true,"result":{...}}` except `fs.list`.

| method | body | result |
|---|---|---|
| `fs.list` | `{path}` (`""` = root) | `{path, entries:[{name,type:file|directory,size?,modifiedAt}]}` (not wrapped) |
| `fs.stat` | `{path}` | `{path, kind:file|dir, size, modified_ms}` |
| `fs.read` | `{path, offset, len}` | `{path, offset, len, eof, data_base64}` |
| `fs.reads` | `{entries:[...]}` | batch read (unused) |
| `fs.search` | `{name:{contains}}` or an extensions filter | `{entries:[{name,path,size,modified_ms}], total, has_more}`; index lags new files |
| `fs.write` | `{path, overwrite, append, create_parent, data_base64 \| text}` | `{path, bytes_written, append}` |
| `fs.mkdir` | `{path, recursive}` | `{path, created}` |
| `fs.rename` | `{source, destination, overwrite, create_parent}` | `{source, destination}` |
| `fs.delete` | `{path, recursive}` | `{path}` |

`fs.read` of some home files (IDENTITY.md ...) returns a longer body than
`fs.list`'s size: the server prepends an "About this file" note. muse-cli
reads in 4 MiB windows until `eof` and writes in 4 MiB append chunks.

## Turn completion and approvals

Live `chat.subscribe` replays current state on connect, then streams:

- `task.status` payload `{status, agent_id, root_agent_id, task_id, session_id,
  is_thread, active_subagent_count}`. The full vocabulary is measured in
  "Turn-end semantics" below: only the root agent's `completed` ends a
  turn, and only after `running` was seen from that root; other strings
  are suspect and need history proof. `task_id` is the agent's identity,
  constant per agent and equal to `root_agent_id` — never a turn epoch.
- `approvals.snapshot` payload `{emitted_at_ms, pending_approvals[], recent_approvals[]}`;
  each approval has `approval_id`, `display.{summary_title, short_explanation,
  permission_question{kind,scope,text}, detail_rows}`, `payload.network_payload`,
  `request_expires_at_ms`, and once decided `decision`. Pending ones carry
  `decision_options[{kind, label, label_text}]`, the app's buttons:
  `allow_once` "Allow once", `allow_always` "Always allow this site"
  (the forever choice; recent entries then show `always_scope: destination`,
  outcome "Site always allowed"), `deny` "Deny".
- Widgets: assistant messages embed `[[hatch_widget:<id>]]` in text and carry
  `payload.widgets[{widget_id, kind:"option", data.options, display_text, state}]`.

Routes: `egress.approvals` GET /approvals (`pending`, `pending_approvals`,
`recent`); `egress.approval.decide` POST /approvals/{id}/decide `{decision}` with
decision in `allow_once|allow_session|allow_today|allow_always|deny|deny_always`
(server also lists `timed_out`, `auto_allow`).

## Turn-end semantics (measured live, 2026-10-28)

Measured against a live account over 30 turns plus 2 cancels, driving
`chat.subscribe` and `chat.history` directly. Where this section differs
from older notes, it wins. Counts below are those measurements; anything
not measured is marked unresolved.

### `task.status` vocabulary

| Value | Meaning for `wait` |
|---|---|
| `completed`, root agent, `running` seen first | DONE (exit 0) AT ONCE, no history proof: a live terminal after running ends the wait; the final replies are re-read from history afterwards |
| `interrupted`, root agent, `running` seen first | FAILED (exit 2); no history proof required |
| `cancelled`/`canceled`, root agent, `running` seen first | FAILED (exit 2), RETAINED from the pre-existing cancel flow (base FAILED set + the tgbot.py comment "The turn ends on Muse's cancelled status"), NOT muse01-measured; never accepted from a replay |
| `completed`/`interrupted`/any terminal replayed BEFORE `running` was seen in this wait | Suspect: stored, settled against history (below); never ends the wait alone |
| Terminal with a missing `root_agent_id` (absent, or a subagent id with no root id) | Suspect IN CODE, never a direct end — even after `running` was seen; settled against history only. The root id is learned ONLY from a root-confirmed `running` (a subagent's own `task.status` carries `agent_id == root_agent_id` with a DIFFERENT root value) |
| `running`, root agent only | The only in-progress value; the only value that may set `seen_running` |
| `pending_init` | Suspect, never progress, for any agent |
| `idle`, `complete`, `done`, `finished`, `succeeded`, `success`, and every other DONE/FAILED-looking string | Suspect-and-require-settle: stored, settled against history, accepted only with history proof + root scope + `running` seen; with no proof the wait keeps waiting, never exits 0 empty |
| Any `task.status` from a non-root agent | Never terminal, whatever the string |
| Root terminal with `active_subagent_count > 0` | Suspect, re-check (provisional: one observation, non-root `completed` 4.532 s before root `completed`) |

Scope: filter `agent_id == root_agent_id` when `root_agent_id` is present;
a missing root id makes the event suspect IN CODE (see the table). `sessions.updated` /
`tasks.updated` arrive on scoped subscribes and must never count as
progress; scope-check every event kind by `session_id` when present.
A fresh subscribe replays the last terminal state (`completed` on a fresh
thread, `interrupted` after a cancel): a replay terminal alone never ends
a wait — it needs `running` seen in this wait first, or history proof via
settle. Settle never accepts FAILED: a standalone wait started AFTER a
cancel times out (124) instead of exiting 2 — known limitation.

### History settle

A history event counts as a delivered reply only if ALL hold:
`event_name = message.assistant`, `payload.status` exactly `completed`,
`reply_to_message_id` empty at BOTH the event and the payload level,
`seq > after_seq`, `seq <=` the upper bound captured at terminal /
state-check time. Dedup by (`seq`, `message_id`). `interrupted`-status
assistant events are partials (two observed, 1998/2464 chars) and are
excluded from replies, while the `interrupted` terminal itself needs no
history proof. Media/widget-carrying completed assistant events count as
replies even with empty text. `delta.presentation` and non-assistant
events are excluded. Never use `transcript_revision` as a bound (observed
constant 4993 against `seq_max` 4994, unresolved). Window: `limit=200`
plus growth while `has_more` is set — `routes.json` has no cursor. The
settled terminal is cleared only on a successful settle. One settle
attempt per (re)subscribe: a resubscribe retries the settle once with a
fresh read; a deaf stream replaying the same terminal does not pay a
history fetch per record.
`upto` semantics (as implemented): each history read caps at the newest
seq in ITS OWN window; there is NO freeze of the bound at the first
settle read — freezing would reintroduce the fast-turn loss (a reply
landing after the freeze, before the terminal, would be excluded
forever). A reply landing after a settle read is picked up by the next
resubscribe settle retry or by the final-read lag window: after a `done`
terminal whose final read returns empty, history is polled for ~2 s
before reporting done. The final history read has a floored 5 s budget
independent of the expired caller deadline (a collapsed 0.1 s timeout is
what the gateway reports as "no response" even when healthy); a final
read FAILURE still fails closed to 124, never a silent exit 0.

### The `after_seq` boundary

A standalone `wait` without an explicit `--after-seq` derives its
boundary from the LAST `message.user` seq in a history window (or from a
caller-supplied `user_seq`), never from the latest event's seq: the old
`limit=1` latest-event snapshot made `after_seq` equal the reply's own seq
whenever the reply had already landed, excluding it forever (split
send+wait: 11 exit 0 / 9 exit 124 over 20 turns, perfectly separated by
completion time). When no user event is found, the boundary depends on
WHY (see below): only a genuinely empty window yields 0. Integrated
`send --wait` passes the delivery `user_seq` and was immune.

Three cases are distinguished, and only the third yields boundary 0:

- Read ERROR: each window read is retried once, bounded. If a window
  succeeds, its result is used. If NO window ever succeeds, the
  boundary is UNKNOWN, not 0: settle-from-replay is disabled until a
  live root `running` is seen, and the wait reports timeout with a
  notice rather than delivering stale replies. When a live `running`
  is then seen, its event seq ratchets the boundary up (a reply to
  this turn necessarily follows the running event).
- Window EXHAUSTION (`has_more` still true at limit 1000, no user
  event): the boundary is the max seq seen in any successful window
  (the old snapshot behaviour), not 0.
- Genuinely EMPTY (successful read, `has_more` false, no user event):
  the boundary is 0. STALE RISK, documented: with boundary 0 the wait
  relies on terminal + scope checks, and a stale reply already in
  history can be delivered as this turn's if a replayed terminal
  settles against it. The risk is confined to threads whose history
  predates the wait with no user event in the window.

### Not turn signals

`chat.stream_events` (global snapshot, no `task.status`),
`tasks.subscribe` (no records while idle), `subagents.status` (400/404),
`tasks.runs` (scheduled background jobs only). Do not use them to detect
turn end.

### Unresolved (do not assume)

FAILED strings other than `interrupted`; the `idle` value; the
approval-pending `task.status` value (0 pending in 3 elicitation
attempts); whether a decide can safely be retried after a lost reply
(`_safe_to_repeat("egress.approval.decide")` is False, no idempotency key
— never retry a decide blindly). `approvals.snapshot` carries no
`session_id`, so approval filtering cannot be proved session-scoped:
auto-approve may decide an approval Muse did not scope to this session.
Expired-but-listed approvals are ignored (the waiter filters by
`request_expires_at_ms`): a clock running ahead hides them — known
limitation, no surfacing. A standalone wait started after a cancel
times out (124) instead of exiting 2 (settle never accepts FAILED).
Watermark granularity: a partially delivered batch retried by the
monitor can duplicate the delivered part (per-item watermarking is a
follow-up, not implemented). Same-path media in different turns can
collide on the path-only dedup key (pre-existing). Single-flight
`server_check` is a follow-up: bounding check threads would break the
pinned property that a hung check never suppresses the next cadence
check. A reply with no terminal ever still times out (124) by design
(R4, Boss decision open). `transcript_revision` semantics unresolved.

### Wait constants (0.6.1)

Watchdog 20 s without scoped progress (provisional — no progress-gap
measurement supports tightening it); state-check budget 8 s subscribe
(warm `task_state()` max 0.184 s over 5 calls, cold connect up to
3.272 s); reconnect: each wait-path connect capped at 30 s
(`connect_timeout=30` on `_wait_connection` → `bounded_connect`), the
connect itself moved out from under `shared_lock` in `live()` so a slow
handshake cannot serialise other threads, a storm of failed resubscribes
switches to one bounded state check after 30 s, and resubscribe backoff
sleeps are capped by the remaining caller deadline; heartbeat 25 s;
state check only on a suspected gap; final history read floored at 5 s;
lag window ~2 s after an empty `done` final read.
Evidence-based hang bound: caller timeout + 65 s worst overshoot (30 s
in-flight wait-path connect + 35 s final phase: 30 s connect + 5 s
read; concurrent state checks do not delay the return, backoff cannot
overshoot the deadline). A 20 s harness therefore ends by ~85 s, inside
the 90 s Floor; the 35 s Target is unreachable by design (one hung
connect alone can consume 30 s).

M2 honesty: live terminal-to-exit latency is UNMEASURED post-fix. Do not
cite the pre-fix exiting-subset numbers (median 0.469 s, p95 0.541 s,
N=11) for the fixed code. The post-fix live evidence is 20/20 exit +
exact replies on split send+wait — after_seq-fix evidence only, not a
latency measurement.
