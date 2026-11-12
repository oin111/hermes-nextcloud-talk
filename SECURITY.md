# Security Policy

## Reporting a vulnerability

Please do not open a public issue for credential disclosure, authentication bypass, unsafe file handling, or unintended public file sharing.

Report the issue privately through GitHub's **Security → Report a vulnerability** feature for this repository. Include affected versions, reproduction steps, and impact when possible.

## Threat model

See [THREAT_MODEL.md](THREAT_MODEL.md) for the assets, the inputs treated as hostile, the
properties the adapter must keep, what is out of scope, and the severity rubric used for reports.

## Mention-triggered history context

When mention gating is enabled, an addressed group question may disclose prior
eligible room discussion to the configured model provider and normal Hermes
transcript. The adapter fetches text only, strictly before the trigger ID, within
24 hours, four pages and a bounded added-text budget. Each participant must pass
the existing stable-ID group authorization and any denying core callback; history
never widens the trigger's own authorization. Room/thread/profile session keys
isolate quote deduplication. DMs and command requests do not fetch context.

Quotes are untrusted user-level JSON, not system prompts, slash commands, approval
choices or synthetic turns. No history attachment is downloaded and backward
reads explicitly disable read-marker updates. The adapter writes no passive text
cache or independent inclusion ledger. Once included, quotes follow the ordinary
transcript's persistence, retention and model disclosure policy.

Deduplication reads trusted persisted platform-ID columns for ordinary user turns.
Quote receipts are domain-separated HMAC-SHA256, using the configured app password,
and bind the exact record to the owning server/account/routing scope, session ID
and trusted trigger platform ID. Forged prose and exact receipt replay under
another turn ID do not suppress history. No raw endpoint, bot account or session
scope is added to model input; only opaque MACs accompany the quoted data. Use a
strong app password; rotating it invalidates old quote receipts. No native event
metadata or core codec change is made. Resolve the actual SessionStore route
before dedup, including suspension/new-session transitions.

Routing, transcript I/O and receipt scans run off the event loop. Bounded
read-only SQLite reads of the resolved owning database replace the unbounded
transcript loader: 512 active user rows, 64 KiB per row, 1 MiB aggregate, 50-ms
lock timeout and SQL progress deadline. Unsupported APIs/schema, pending writes,
overflow and read failures skip enrichment rather than trusting partial evidence.
The request's deadline includes that work and API waiting. One outstanding
shielded task per adapter retains its permit until real completion even after
timeout, preventing replacement-thread accumulation. An OS-stalled worker cannot
be killed by asyncio; other questions continue without enrichment.

Compaction/rewind/editing or a pre-persistence crash can permit re-inclusion;
retained-transcript dedup is not an independent exactly-once delivery ledger.
Concurrent reset and handler routing have no plugin/core transaction. The current
question is kept on enrichment failure. Only explicit ordinary comment rows with
string display names pass the history type/author gate. Quoting is not a
prompt-injection sandbox: keep participant allowlists restrictive and require
approval for sensitive tool actions. See README for defaults/opt-out.

## Deployment notes

This plugin runs inside Hermes Gateway and uses a Nextcloud user's credentials. Operators should use a dedicated account, an app password, restrictive Talk room membership, and `NEXTCLOUD_TALK_ALLOWED_USERS`.

### Multiple profiles on one gateway

With `gateway.multiplex_profiles: true` one gateway process serves several profiles. The adapter resolves every `NEXTCLOUD_TALK_*` setting from the **owning profile's** secret scope, so a secondary profile's lane uses its own credentials, rooms and allowlist, and a secondary profile without Talk credentials of its own is left unconfigured (the core's `platform_registry.create_adapter` consults `validate_config`, which is scope-aware).

Runtime matrix (Hermes version strings, tags are date-based so commits are cited):

- `gateway/platforms/_shared.py` present with all three readers (`get_scoped_secret` added 2026-09-02 in `661fc669a0bb`; `extra_or_secret`/`platform_gate_env` and the env-first precedence since `de114b3a`/`3dedb71f2f`, 2026-09-13 UTC) — isolation through the shared readers.
- `agent.secret_scope` present but the shared module absent or missing those two readers (released 0.20.x; 0.21.1/0.21.2 ship only `get_scoped_secret`) — isolation through the adapter's compat shim, which inlines the core readers.
- Neither present (no per-profile secret scope at all) — no isolation is possible, so a secondary profile's Talk lane would use the launcher's (default profile's) credentials. Keep Talk on the default profile only on such a host.

Isolation also presumes that multiplexing is active, because a scoped miss falls back to the plain process environment whenever the platform is not multiplexing (that is the unscoped default lane's own environment). The gateway sets that flag in `GatewayRunner.__init__` from the same `gateway.multiplex_profiles` key that gates secondary startup, before any adapter is built, so no in-tree path constructs a secondary lane outside it.

One caveat is worth auditing when extending the adapter: a settings read that happens **outside** any profile scope falls back to the plain process environment (that is the default profile's own environment, which is what the unscoped default lane needs). Any new read must therefore happen inside the scope — the adapter is constructed and connected inside `_profile_runtime_scope`, and it starts no bare threads that read settings.
