# Security Policy

## Reporting a vulnerability

Please do not open a public issue for credential disclosure, authentication bypass, unsafe file handling, or unintended public file sharing.

Report the issue privately through GitHub's **Security → Report a vulnerability** feature for this repository. Include affected versions, reproduction steps, and impact when possible.

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
