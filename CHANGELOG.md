# Changelog

All notable changes to this project will be documented here.

## [0.1.20] - 2026-10-24 (UTC)

### Fixed

- Keep Talk lifecycle generation, task and completion-waiter state private to the
  original event rather than passing it as native admission metadata. Ordinary
  Talk inputs now satisfy Hermes cores with a closed metadata schema without
  weakening native validation or sender authorization.
- Preserve exact queued-event ownership and completion tracking; reconstructed
  native inputs do not inherit live tasks, waiters or stale generations.
- Add real native admission and restoration regressions, authorization and unknown
  metadata controls, busy-queue waiter identity, persisted ACK and replay checks.
  Existing image-batch delivery and cancellation fixes from 0.1.19 are retained.

## [0.1.19] - 2026-10-24 (UTC)

### Fixed

- Return an explicit delivery result from image batches, so successful image-only
  replies are acknowledged instead of running the original command again. A partial
  batch failure remains a failure even if another image or the text was delivered.
- Propagate image-batch cancellation instead of swallowing it and sending later images.
- Add real-Hermes regressions for image-only delivery, persisted acknowledgement,
  replay suppression, partial failures, empty batches and cancellation.

## [0.1.18] - 2026-10-22 (UTC)

### Added

- Automatic discovery of private group rooms (Talk type 2), guarded by a non-empty,
  restrictive group sender allowlist. `NEXTCLOUD_TALK_AUTO_DISCOVER_GROUP_ROOMS`
  (`extra.auto_discover_group_rooms`) defaults to true; false retains DM-only
  discovery. The master `auto_discover_rooms` switch still disables all discovery.
  Wildcard allowlists, missing group allowlists, and `ALLOW_ALL_USERS=true` never
  enable automatic group intake, even with the new switch explicitly enabled.
  Existing per-message stable user-ID authorization, explicit room behavior,
  room-count limits, initialization, and durable acknowledgement lifecycle are
  unchanged. Public, system, note-to-self, and unknown types remain excluded.
- Native execution-approval reactions: ✅ once, 🌀 session, ♾️ always and ❌ deny,
  seeded only for choices offered by Hermes. Reactions are consumed through the existing
  chat poll, never dispatched as agent turns, and acknowledged through the prefix-floor
  ledger, including while an approval-blocked turn remains in flight.
- Approval-card expiry uses `NEXTCLOUD_APPROVAL_TIMEOUT_SECONDS` / YAML
  `approval_timeout_seconds`, then Hermes `approvals.timeout`, then a 3600-second fallback.
  Seeding failure retracts attempted seeds and returns failure for the core text fallback.

### Security and compatibility

- Reactions must target a registered card in the same room and pass that room's effective
  user allowlist; bot seeds, removed reactions and unoffered choices never approve.
- Released Hermes does not supply requester identity or request ID to this adapter hook.
  Any allowlisted participant in the room can answer; the response resolves the oldest
  pending request in that session (FIFO), not necessarily the newest displayed card.
  Optional request-ID metadata is honored when provided by a compatible caller.
- Preserve v0.1.17 owner/session-isolated slash-command routing, pending-approval polling
  after card failure/expiry, and current cursor semantics. Prune cards on every room poll.
  ACK-store failures retry only a reaction's ACK in process, using the existing bounded
  settlement cache; restart or eviction before persistence retains the documented replay risk.
- Keep current scoped configuration validation and routing; omit the stale contributor
  standalone cron sender and client-directory additions, which are unrelated to approvals.

## [0.1.17] - 2026-10-10 (UTC)

### Fixed

- `/approve`, `/deny`, `/stop` and the other Hermes slash commands now reach a running turn.
  While a turn ran, the adapter kept every later room message on Talk until the turn ended,
  so an approval prompt always timed out and `/stop` arrived only after the work it was
  meant to stop. A recognized slash command for the busy session, sent by the participant
  who owns the running turn and carrying no attachment, now passes; all other messages
  still wait and are handled after the turn, as before. Hermes core then handles the
  command as it does on other platforms.
- Such a command is settled at most once: it is acknowledged as soon as Hermes has handled
  it, even if its reply could not be sent, because commands such as `/queue`, `/bg` or a
  later `/approve` must never run twice. If storing that acknowledgement fails, the gateway
  remembers the command in memory and later retries only the acknowledgement; after a
  restart before it is stored, Talk may hand the command over once more. This also applies
  while a `clarify` question is pending.
- A turn that Hermes cancelled for the user's `/stop`, `/new` or `/reset` is acknowledged
  rather than left retryable, so it is not run again on the next poll. A turn whose stop
  command failed stays retryable as before.

### Changed

- While a turn runs, its room is still polled, but only for messages newer than the
  newest one already seen (short 5 s long poll, no re-read of older held messages).
  Messages that are not commands for the busy session are neither acknowledged nor
  downloaded during the turn (unless a `clarify` question is pending, when the room is swept
  as before), so a flood from other participants cannot fill the
  acknowledgement capacity; a burst is read in bounded pages within one cycle, and an
  oversized page is retried smaller. With several rooms, a command
  is seen within one polling cycle (at most the configured poll timeout, 30 s by default).

## [0.1.16] - 2026-10-09 (UTC)

### Added

- Message editing and deletion (`edit_message` / `delete_message`). Hermes now updates one
  progress or "still working" message in place, edits streamed replies as they grow (when
  streaming is enabled), removes stale previews, and can auto-delete ephemeral replies and
  progress bubbles (`display.ephemeral_system_ttl`, `display.cleanup_progress`).
- Only messages this bot posted in that room since the gateway started are edited or deleted
  (bounded registry), so a moderator bot never changes other people's messages. Talk refusals
  (24-hour edit window, permissions, size) are final failures, and Hermes falls back to a new
  message. A timed-out edit may be repeated with the same complete text (unlike a
  non-idempotent send), so transient failures do not turn progress into a flood
  of separate messages.
- Streamed answers notify on their initial post: Talk does not notify on edits.
  Bot-authored edit/delete system events are skipped without filling the durable
  exact-ACK ledger behind an unfinished turn.
- Exhausted exact-ACK capacity now holds new intake rather than moving the prefix
  past a retryable message; the oldest unresolved ID can use bounded recovery
  headroom, and an operator can raise the limit if that also fills.
- Hold a profile-scoped cursor-writer lease for the life of the connection so
  concurrent gateways cannot independently advance a floor past unseen work.

### Changed

- Tool progress now appears in Talk. Hermes skips tool progress on platforms that cannot edit,
  so with editing available the `display.tool_progress` setting (global or
  `display.platforms.nextcloud_talk`) takes effect: one silent progress message per turn,
  edited in place. Set `display.platforms.nextcloud_talk.tool_progress: off` for the previous
  behaviour.
- Status reactions are failure-only: no more 👀 while working and ✅ on success, only ❌ when a
  turn fails. Talk cannot post a reaction silently and notifies the author about each one, so
  👀/✅ caused a notification on every message; the read marker still shows that a message was
  picked up. `reactions` keeps its values (`groups`, `all`, `off`) and governs where ❌ appears.

## [0.1.15] - 2026-10-09 (UTC)

### Added

- Status reactions: 👀 while a turn runs, then ✅ or ❌ (Hermes' processing hooks). Default
  `reactions: groups`, because Talk notifies the author about every reaction in one-to-one
  rooms; `all` / `off` switch it. Reaction and read-marker calls run detached from the turn
  on a small dedicated thread pool with a short HTTP time-out and a bounded queue, so a slow
  server never delays a reply and never ties up the threads that polls and sends use.
- Read marker: an accepted message marks the room read up to its ID (monotonic, best effort,
  `read_marker`). With it on, polls ask Talk not to move the marker itself, so messages the bot
  skips (not allowed, system) no longer count as read; `read_marker: false` keeps Talk's own
  marker handling.
- Silent sends: in the default `notifications: important` mode, only sends Hermes marks as a
  final reply (`notify`) or as an approval/clarify prompt notify; other chat messages use Talk's
  `silent` flag, including the main-chat retry of a rejected thread send. The caption or link
  message that accompanies a final attachment inherits its `notify`. File shares follow the
  same rule through Talk's `silent` share metadata (`media-caption` capability); a server that
  refuses the metadata gets a plain, notifying share. The turn-error notice carries no `notify`
  and is silent.
- Voice replies in MP3/WAV (auto-TTS and `[[audio_as_voice]]`) are shared as Talk voice
  messages (`messageType: voice-message`); Talk keeps that type only for those formats (probed
  on Talk 24).

### Changed

- Replies keep their Markdown (code blocks, links, lists, tables), which Talk renders; only
  image embeds become their URL. `markdown: false` restores the plain-text conversion. The
  platform hint tells the model that Markdown is rendered. Long replies are split without
  breaking code blocks: a chunk ending inside a fence closes it and the next one reopens it, and
  indentation at chunk edges is kept.
- Default notification behaviour: progress, status and tool messages are now silent. Set
  `notifications: all` for the previous behaviour.

### Documentation

- `THREAT_MODEL.md`: assets, hostile inputs, required security properties, scope, and the severity
  rubric for reviewers and automated scanners; linked from `SECURITY.md`.

## [0.1.14] - 2026-10-08 (UTC)

### Added

- Talk threads map to Hermes topics. A message Talk marks with `isThread: true` carries the
  thread ID as the Hermes `thread_id` (own session per thread) and the sanitized thread title
  as the chat topic. Messages without that exact marker stay in the room session, because
  Talk gives every message a `threadId`.
- Outbound `thread_id` metadata becomes Talk `threadId` for text, split chunks, file shares
  (`talkMetaData.threadId`), non-atomic captions and link follow-ups. A definite HTTP 400
  for the thread falls back to the main chat once, without the reply anchor (it points into the
  rejected thread); an ambiguous send is still never repeated. A file the server could not tag
  with the thread is logged as shared to the main chat.
- `thread_topics` (YAML): bot-created topic threads with their own skill(s) and prompt. The
  adapter reuses its persisted thread IDs, adopts only threads the bot itself started, never
  creates a duplicate on an inconclusive lookup, and skips rooms it does not poll or servers
  without the `threads` capability. Setup runs in the background with a time limit; each
  binding is saved as soon as it is made, and a lost thread is forgotten only after the room's
  thread list has been read (a missing room or lost access keeps the binding).

### Changed

- Inside a thread, Talk's implicit parent (the thread root) is no longer passed to Hermes as a
  quoted reply.

## [0.1.13] - 2026-10-02 (UTC)

### Fixed

- Images and files sent while the bot was waiting in a long poll reached Hermes as
  Talk's "{actor} shared a file which is no longer available" stand-in without the
  attachment. Talk resolves the share inside the long-poll request whose filesystem
  view predates the new share, fails, and drops the `file` parameter. The adapter now
  recognises that stand-in (an emphasized first line containing `{actor}`, an `actor`
  parameter and no `file` parameter, in any server language) and re-reads the same
  message with a short ordinary history request (1 s, 2 s, 4 s backoff, each attempt
  capped at 10 s so a hung server cannot stall the other rooms for long). A replacement is accepted only when it has the same
  message ID, the same actor and valid file metadata; then the attachment is downloaded
  as usual. If every attempt fails, the message is delivered as text with the actor name
  filled in. The refetch runs only after authorization checks.

## [0.1.12] - 2026-09-30 (UTC)

### Changed

- Room ACK ledgers use a bounded **prefix floor**: the floor advances only over IDs
  seen in the room's own history that are acknowledged and have been visible for a
  short settle window (`NEXTCLOUD_TALK_FLOOR_SETTLE_SECONDS`, 30 s). Exact ACKs are
  kept only above the floor. An unacknowledged ID now stays retryable no matter how
  much newer traffic follows it; 0.1.11 dropped it from its overlap window after 199
  newer IDs.
- Polling catches up from the floor without waiting, follows full pages, resumes a
  long sweep across poll cycles, and long-polls only once caught up.
- A turn whose handler or tools fail, or a message that fails before dispatch (e.g. an
  attachment download that keeps failing), is retried with backoff and given up with
  a warning after `NEXTCLOUD_TALK_MAX_DISPATCH_ATTEMPTS` (5), without blocking newer
  messages. 0.1.11 retried it on every poll forever and a failing attachment muted
  the room. A turn whose reply Talk refused gets a separate, larger budget
  (`NEXTCLOUD_TALK_MAX_DELIVERY_ATTEMPTS`, 8, about an hour).
- A poll page larger than `NEXTCLOUD_TALK_MAX_JSON_BYTES` (a few huge messages) is
  re-fetched with smaller pages; in 0.1.11 it muted the room.
- A short poll page (Talk hides invisible messages after applying the page limit) no
  longer ends the catch-up sweep early.

### Fixed

- A startup-backlog turn that outlives `NEXTCLOUD_TALK_PROCESSING_TIMEOUT` counts toward
  the give-up budget; in 0.1.11 the room never finished initializing and the turn
  (and its reply) re-ran on every retry.
- `NEXTCLOUD_TALK_INITIAL_BACKLOG_LIMIT=0` really starts a new room at the latest
  message, and a restart never replays history below the persisted floor.
- With `NEXTCLOUD_TALK_PROCESS_HISTORY=true`, history older than the capped backfill
  (`NEXTCLOUD_TALK_MAX_BACKLOG_MESSAGES`) is not replayed.

### Known limits

- The floor passes an ID only after a complete rescan from the floor that started at
  least the settle window (30 s) after the ID was first seen; a lower ID revealed
  later than that is not dispatched.
- A single message larger than `NEXTCLOUD_TALK_MAX_JSON_BYTES` still blocks its room
  (not reachable with the 4 MiB default), as in 0.1.11.

### Compatibility

- 0.1.11 cursor files load unchanged: their floors are adopted once with the 0.1.11
  overlap rule and rewritten in prefix mode. No Hermes core changes are required.

### Tests

- `test_prefix_ledger`: real Hermes dispatch path; first-seen backlog window, backlog 0,
  6,000 unauthorized messages without a stall or growing state, bounded retries of a
  failing turn, bounded reply-delivery retries, the settle window
  against a later sweep, single accounting of awaited failures, pre-dispatch errors, oversized pages,
  capped history backfill, short pages, no starvation beyond one cycle of pages,
  0.1.11 cursor adoption, and restart without replay.

## [0.1.11] - 2026-09-19 (UTC)

### Fixed

- Connect after room metadata validation without waiting for backlog replies. New-room
  history now runs in owned background tasks, preventing gateway connect timeouts and
  duplicate execution when a reply exceeds the connection deadline.
- Keep initialized rooms polling while another room initializes. Retry partial backlog
  failures with bounded backoff; require durable acknowledgement before advancing.
- Cancel and await initialization when a discovered room disappears or the adapter
  disconnects. Reclaim completed tasks and preserve exact ownership on room re-entry.

### Tests

- Add real-Hermes startup, partial-retry, cross-room progress, disconnect, discovery
  removal, task cleanup and in-flight acknowledgement regressions to CI.

## [0.1.10] - 2026-09-15

### Fixed

- The launcher-environment probes in `test_compat_shim.py` and `test_profile_scope.py` are now
  hermetic. They seeded only the keys their own fixtures named, while importing the adapter pulls in
  the core modules that hydrate the active profile's env file into the process environment: on a host
  whose profile defines a Talk key the fixture does not name (the singular room-token variable, for
  instance) the real settings of that host leaked into the assertions, and the unscoped default-lane
  probe compared a two-element token list against its single fixture value. CI never saw it because
  its runner unsets credential variables; the documented `python3 -m unittest` run does not. The
  helper drops every `NEXTCLOUD_TALK*` key before applying the fixture's launcher values and
  restores the host's values afterwards, so the probes depend only on the fixture.

## [0.1.9] - 2026-09-15

### Fixed

- `hermes plugins install` no longer trips the plugin-guard scan. The guard pinned this tree at a
  `dangerous` verdict — un-overridable, `--force` included — on two content-level false positives:
  the SSRF-scheme fixture in `test_adapter.py` carried the literal system password-file path
  (`system_passwd_access`, critical; the guard caps a critical found under a top-level `tests/`
  directory, and these modules live at the repo root), and prose in `CHANGELOG.md`, `README.md`
  and `SECURITY.md` named the environment accessors verbatim (`python_os_environ`, exempt on code
  files only, applied in full to docs). The fixture now rejects a neutral `file:///tmp/...`
  target — the same scheme / userinfo / host / port contract, still asserted before any network
  call — and the docs say "the plain process environment". Verdict is now `safe`, so installs and
  `plugins update` work without `--force`.

## [0.1.8] - 2026-09-14

### Fixed

- Read every adapter setting through the profile-scoped readers
  (`gateway.platforms._shared.get_scoped_secret` / `extra_or_secret` / `platform_gate_env`)
  instead of a raw `os.getenv`. Under `gateway.multiplex_profiles` a secondary profile's lane
  inherited the LAUNCHER's values — the default profile's bot credentials, room list and
  allowlist — so it connected as the default profile's bot and answered in that profile's
  rooms. A secondary lane now uses its own credentials, its own rooms and its own allowlist,
  and a secondary profile without Talk credentials of its own stays unconfigured
  (`validate_config` is scope-aware, and the core's `platform_registry.create_adapter`
  consults it) instead of borrowing another profile's.
- Keep profile isolation on runtimes without `gateway.platforms._shared`: the compatibility
  shim now inlines the core reader on top of `agent.secret_scope` (Hermes 0.20.x). Only a
  runtime with no per-profile secret scope at all falls back to the plain process
  environment, which is documented as unsupported for a secondary lane.
- Report the real plugin version in the Talk client `User-Agent` (it was frozen at 0.1.7);
  it is now derived from the neighbouring `plugin.yaml`, with hardening so a missing, unreadable,
  non-UTF-8, comment-laden or implausible manifest falls back to the released constant instead of
  failing the plugin import or poisoning every request header.

### Changed

- A present-but-blank `NEXTCLOUD_TALK_*` value now counts as UNSET, matching the core
  contract: the profile's own `config.yaml` `extra` key (or the setting default) applies.
  The previous reader treated a blank value as a hard `false`/empty, which could silently
  disable room discovery (`NEXTCLOUD_TALK_AUTO_DISCOVER_ROOMS=`) or the mention requirement
  (`NEXTCLOUD_TALK_REQUIRE_MENTION=`). Blank YAML strings are likewise unset.

### Added

- `test_profile_scope.py`: real-core probes for secondary-profile isolation, the fail-closed
  uncredentialed case (through `platform_registry.create_adapter` on a dedicated probe
  registration that carries THIS module's `validate_config`, so the gate under test can never be
  a foreign registration), default and single-profile env semantics, the blank-env rule, the
  isolation path on a runtime with `agent.secret_scope` but no shared readers, and the legacy
  fallback. An anti-regression probe spies on both attribute-style and direct environment lookups.
- `test_compat_shim.py`: probes the compat shim's own branch (scope-aware credentials,
  fail-closed without profile credentials, unscoped default lane) without importing
  `gateway.platforms._shared`, so the compatibility CI job exercises it on the real 0.20.6
  runtime instead of skipping.
- CI: the main job runs against the earliest Hermes `main` commit whose `extra_or_secret` is
  env-first (`3dedb71f2f`); the compatibility job covers the previously tested 0.20.6 runtime,
  where the shim probes run for real and the profile-scope module reports as skipped
  (discovery invocation, `pipefail`, exact `OK (skipped=` gate).

## [0.1.7] - 2026-09-04

### Fixed

- Continue Talk polling while an in-flight Hermes turn is waiting for an interactive `clarify` response, allowing typed choices to reach the blocking waiter instead of deadlocking behind the room lock.
- Match the bypass to the exact Hermes session so a prompt owned by one group participant cannot release the room lock for another participant.
- Treat already-resolved clarify entries as non-pending, preventing a rapid second reply from being associated with the previous question before waiter cleanup completes.

## [0.1.6] - 2026-08-30

### Fixed

- Reconcile confirmed text-only delivery during graceful shutdown without
  acknowledging mixed-media work before every attachment reaches a terminal
  successful outcome.
- Keep document, video, voice, TTS, image, and media-only delivery failures
  generation-scoped and retryable, including replies drained later from a
  busy-session queue.
- Observe and sanitize cursor-commit failures raised by completion watchdogs so
  private paths cannot leak through unhandled task tracebacks.
- Count each failed attachment once while preserving visible failure notices.

## [0.1.5] - 2026-08-29

### Fixed

- Keep a Talk message inflight while its real Hermes background handler task is
  still running or while the exact event remains in Hermes' busy-session queue,
  preventing the processing watchdog from replaying long and queued turns.
- Preserve retry behavior when neither a live handler task nor an owned queued
  event remains and lifecycle completion is genuinely lost.

## [0.1.4] - 2026-08-21

### Fixed

- Ship a scanner-safe runtime bundle under `plugin/` so immutable community
  installs scan only executable plugin files, not adversarial tests or CI.
- Document and test the subdirectory install path while keeping the reviewed
  root runtime files byte-identical to the published bundle.

## [0.1.3] - 2026-08-21

### Fixed

- Prevent ACK-overlap replay from bypassing Talk long polling and driving a
  tight PHP-FPM request loop. ACK-only overlap pages now advance to a live
  long-poll anchored at the highest returned acknowledged message ID.
- Keep the live anchor ephemeral so subsequent cycles recheck the durable
  overlap window and can recover retryable gaps.

## [0.1.2] - 2026-08-15

### Fixed

- Honor explicitly configured ACK overlap values from 0 through 31 instead of
  silently raising them to the legacy minimum of 32.
- Centralize poll-page and ACK-overlap normalization across configuration,
  client construction, and legacy runtime state.

## [0.1.1] - 2026-08-15

### Fixed

- Accept the empty `messageParameters: []` shape emitted by some Talk versions
  for ordinary text messages while continuing to reject malformed non-empty metadata.
- Prevent newer-message starvation by capping polling pages at Talk's 200-message
  protocol maximum and clamping the durable ACK overlap below that page size.

## [0.1.0] - 2026-08-14

### Added

- Native Hermes Gateway platform adapter for Nextcloud Talk.
- OCS long polling with independent message cursors per room.
- Explicit multi-room support and automatic discovery of one-to-one chats.
- User allowlists and optional mention gating.
- Inbound image, audio, and document downloads into the Hermes media pipeline.
- Correct handling of files attached to ordinary text captions without a `{file}` marker.
- Native outbound file/image sharing through streamed WebDAV uploads and Talk shares.
- Same-origin-only authenticated attachment downloads with SSRF/credential-leak protection.
- Profile-aware persistent cursors, bounded first-run backlog, paginated history, and retry-safe dispatch.
- Conservative configurable inbound/outbound file-size limits.
- Public-link fallback as an explicit, privacy-sensitive opt-in (off by default).
- Stable actor-ID authorization, DM classification, and complete MessageEvent actor metadata.
- Attachment caption/link delivery failure propagation and correct Talk message-ID semantics.
- Standalone regression tests.
