# Threat model

This file tells reviewers (people and automated scanners) what this plugin defends, which inputs
are hostile, what is out of scope, and how to rate findings. It describes the code as released;
if the code and this file disagree, report the disagreement.

## What the plugin is

A Hermes Gateway platform adapter. It runs inside the gateway process with the gateway user's
rights, logs in to one Nextcloud server as one Nextcloud account (the bot), long-polls the Talk
chat API of the rooms it serves, turns each accepted message into a Hermes turn, and posts the
answer back (text, files via WebDAV upload and a Talk share). There is no listening socket: all
traffic is outbound to the configured Nextcloud origin.

## Assets

1. The bot's Nextcloud credentials (ideally an app password) and the Hermes host they live on.
2. The agent's authority: which people can start a turn, and therefore use the agent's tools.
3. Files on the gateway host: the Hermes document cache that receives attachments, the plugin's
   cursor and topic caches, and anything the gateway user can read.
4. Room confidentiality: an answer, a file, or a public link must not reach a room, thread, or
   person it was not meant for.
5. Delivery integrity: every accepted message is processed exactly once in effect; nothing is
   silently dropped, and no reply or turn is duplicated by retries.
6. Availability of polling across all rooms.

## Trust boundaries and adversarial inputs

Treat every one of these as attacker-controlled:

- **Room participants**, including users not on the allowlist, guests, and federated actors: message
  text, display names, `messageParameters` (any shape, size, depth), replies/parents, reactions,
  thread markers and titles (`isThread`, `threadId`, `threadTitle`), file names, MIME types, sizes.
- **The Nextcloud server's responses**: OCS envelopes, JSON size and shape, status codes, redirects,
  WebDAV paths and download URLs, share objects, capability lists, thread listings, message IDs.
  A compromised or misbehaving server must not get the bot's credentials sent to another origin,
  must not write outside the cache directories, and must not make the adapter spin, grow memory
  without bound, or stall other rooms.
- **Persisted cache files** (cursor ledger, thread topics, attachment manifests): they may be
  stale, truncated, from another release, from another account or server, or hand-edited.
- **The plugin directory itself at import time** (manifest read for the version string): it may
  contain a non-regular file or undecodable bytes.

Trusted: the Hermes core and its configuration (operator-owned), the Python runtime, and the
operator who configures rooms, allowlists, and `thread_topics`.

## Security properties the code must keep

Report any way to break one of these:

- **Authorization fails closed.** Only `users` and `guests` actors are considered; the bot's own
  messages, system messages, bots, and empty actor IDs are acknowledged and skipped. The allowlist
  matches the stable actor ID, never the display name; DMs and group rooms have separate
  allowlists. A guest is never allowed by name, only by an explicit wildcard or allow-all.
- **Credentials stay on one origin.** Authenticated requests go only to the configured scheme,
  host, and port; cross-origin redirects are refused; attachment URLs must resolve to the same
  origin.
- **Native approval reactions are an explicit system-message exception.** A reaction
  must target a registered card in the same room, have a stable authorized user/guest
  actor, and select an offered choice; bot seeds and reaction removals cannot approve.
  Released Hermes provides no requester or request ID in the adapter hook, so the
  effective room user allowlist is the authority: another allowlisted room participant
  may answer. An unbound tap resolves the session's oldest pending approval (FIFO),
  not necessarily the newest card. Exact request binding applies only when a caller
  supplies optional request-ID metadata. Do not claim requester isolation.
- **Filesystem confinement.** Downloaded files land only in the Hermes document cache directory
  with a sanitized name; WebDAV paths are decoded a bounded number of times and refused if absolute,
  ambiguous, containing traversal components, or escaping the user's namespace.
- **Bounded resources.** Response bodies, JSON, error bodies, poll pages, attachments (per file,
  per message, in total), the cache (bytes and files), rooms, metadata (keys, values, depth,
  nodes), thread titles, topic prompts, and retries all have explicit limits. Every extra request
  made while handling one message is bounded by a time-out so one room cannot stall the others.
- **No silent loss, no duplicate side effects.** A message is acknowledged only after a
  deterministic skip or a confirmed outcome. A send whose outcome is unknown (time-out, reset) is
  never repeated; only a definite refusal (for example HTTP 400 for a thread) may fall back.
- **Routing.** A reply goes to the room (and Talk thread) of the message that caused it. A message
  counts as being in a thread only with the exact `isThread: true` marker and a valid thread ID that
  is not newer than the message. Each thread has its own session. A thread's configured skill and
  prompt apply only to the thread this bot created or adopted, never to another thread with the
  same title.
- **The busy-room lock opens only for the turn's owner.** While a turn runs, later room
  messages stay on Talk. The only exceptions are a typed answer to that session's pending
  `clarify` and a recognized Hermes slash command for the session that is actually busy, both
  from the participant who owns the running turn; a slash command carrying an attachment never
  passes as a command and causes no download (while a `clarify` is pending, the owner's message
  with an attachment is still treated as a clarify answer, as in earlier releases). Another
  participant, or a session that is not busy, cannot use the exception to reach or stop someone
  else's turn. While a turn runs without a pending `clarify`, messages that do not qualify are
  neither acknowledged nor downloaded, so they cannot exhaust the acknowledgement capacity; while
  a `clarify` is pending the room is swept as in earlier releases (a flood can hold intake until
  the clarify times out, a recoverable stall). A passed command is handed to Hermes at most once
  per gateway process; known exceptions are a restart before its acknowledgement is stored,
  more than 1024 commands whose acknowledgement could not be stored, and a turn that ends just
  before the command is dispatched (then it runs as a normal turn with normal retries). A turn
  Hermes cancelled for the owner's stop/reset command is acknowledged, while one whose stop
  command failed stays retryable.
  Native approval-card reactions also pass the hold under the separate room/allowlist
  policy above, not the slash command's owner policy. Their consumed IDs use the same
  prefix ACK ledger and bounded in-process settlement cache; restart or eviction before
  an ACK is stored can replay an unbound tap against a later FIFO request.
- **Presentation calls are side effects only.** Status reactions and read markers are best
  effort and time-limited; their failure or delay must never change, delay, or duplicate a
  reply, and never change what is acknowledged.
- **Edits and deletions touch only the bot's own recent messages.** The adapter edits or
  deletes a message only when it posted that message ID in that room during the running
  process (bounded registry), even when the bot is a moderator who Talk would let change other
  people's messages. A timed-out edit may be repeated with the same complete
  text, but an ambiguous new-message POST is not blindly repeated.
- **No public links by default.** Public share links are created only when the operator enabled
  the public-share fallback explicitly.
- **Profile isolation.** Under the multiplexing gateway, each profile's settings and credentials
  come from that profile's secret scope (see SECURITY.md for the runtime matrix).
- **Log hygiene.** Logs show shortened room tokens and sanitized text, never credentials,
  full tokens, or raw server error bodies.

## Out of scope

- What the agent decides to do once an authorized user starts a turn (tool use, which local file a
  reply attaches, prompt injection inside an authorized conversation). That is Hermes core policy;
  the plugin delivers what core hands it. Report core issues to the Hermes project.
- A malicious Nextcloud administrator or a fully compromised Nextcloud server reading or forging
  room content. The plugin cannot defend data that the server itself serves; it only has to keep
  the properties above (credentials, filesystem, resource bounds) intact against it.
- A local attacker who already runs code as the gateway user.
- Denial of service against oneself (for example, a participant choosing the bot's own login as
  their display name so their messages are skipped).
- Talk features the server does not support (threads in federated rooms).
- Third-party dependencies of Hermes and Python themselves.

## Severity rubric

- **Critical**: a person who is not on the allowlist can start a turn; the bot's credentials are sent
  to another origin; a write outside the cache directories; remote code execution in the gateway.
- **High**: a reply, file, or public link reaches the wrong room, thread, or person; a user can mute
  a room or the whole adapter permanently (cursor or ACK starvation, persistent stall); state
  that grows without bound from outside input; a duplicated tool-running turn.
- **Medium**: a recoverable stall of one room; a lost reply or file with a log line; a skill or
  prompt applied to the wrong thread; unbounded logging.
- **Low**: log content issues without secrets; a misleading error message; hardening gaps with no
  demonstrated impact.

## Reports that help most

A failing `unittest` in the repository's style (fake client or real Hermes core, temporary
`HERMES_HOME`) that passes once fixed, plus the smallest patch. Please also state which Hermes
commit you tested against; the CI matrix pins several.
