# Hermes Nextcloud Talk

A native [Hermes Agent](https://github.com/NousResearch/hermes-agent) Gateway platform plugin for [Nextcloud Talk](https://nextcloud.com/talk/).

It connects Hermes directly to Talk through the Spreed OCS API. It does **not** require a public webhook endpoint, a separate bridge service, or changes to Hermes core.

## Features

- Native `nextcloud_talk` platform inside Hermes Gateway.
- Standard Hermes sessions, memory, tools, approvals, slash commands, cron delivery, and reply routing.
- Concurrent polling of multiple Talk rooms with independent message cursors.
- Automatic discovery of new one-to-one conversations.
- Explicit group/public room configuration.
- User allowlists, open-access mode, and optional bot-mention gating.
- Inbound images, audio, and documents downloaded to the Hermes media pipeline.
- Captioned attachments work even when Talk does not include a `{file}` placeholder.
- Native outbound image/document sharing through WebDAV and Nextcloud Talk shares.
- Optional public-link fallback (disabled by default) if native Talk sharing is unavailable.
- Python standard library only at runtime.

## Requirements

- Hermes Agent with the user plugin system and `ctx.register_platform()` support.
- Python 3.11 or newer.
- Nextcloud with the Talk/Spreed app.
- A dedicated Nextcloud user and an app password are strongly recommended.

## Install

```bash
hermes plugins install oin111/hermes-nextcloud-talk --enable
```

For a reproducible installation, pin a full commit SHA:

```bash
hermes plugins install oin111/hermes-nextcloud-talk \
  --ref <full-40-character-commit-sha> --enable
```

Alternatively, clone or copy this repository to:

```text
~/.hermes/plugins/nextcloud_talk/
```

Then enable and restart the gateway:

```bash
hermes plugins enable nextcloud-talk-platform
hermes gateway restart
```

## Configure

Copy `.env.example` values into the Hermes environment file or run the plugin setup through Hermes:

```env
NEXTCLOUD_TALK_URL=https://cloud.example.com
NEXTCLOUD_TALK_USERNAME=hermes-bot
NEXTCLOUD_TALK_PASSWORD=replace-with-a-nextcloud-app-password
```

### Rooms

Group and public rooms must be configured explicitly with comma-separated Talk room tokens:

```env
NEXTCLOUD_TALK_ROOM_TOKENS=roomtoken1,roomtoken2
```

The legacy single-room variable is also supported:

```env
NEXTCLOUD_TALK_ROOM_TOKEN=roomtoken1
```

One-to-one conversations are discovered automatically by default:

```env
NEXTCLOUD_TALK_AUTO_DISCOVER_ROOMS=true
NEXTCLOUD_TALK_DISCOVERY_INTERVAL=30
```

A room token is the final segment of a Talk URL such as:

```text
https://cloud.example.com/call/roomtoken1
```

### Access control

Restrict access to trusted, stable Nextcloud `actorId` values (normally login IDs):

```env
NEXTCLOUD_TALK_ALLOWED_USERS=alice,bob
NEXTCLOUD_TALK_ALLOW_ALL_USERS=false
```

To accept every user in every configured/discovered room:

```env
NEXTCLOUD_TALK_ALLOW_ALL_USERS=true
```

Access is denied by default: when `NEXTCLOUD_TALK_ALLOW_ALL_USERS=false`, the
allowlist must contain the sender's non-empty, stable `actorId`. An empty allowlist
does **not** enable open access. Guests without a stable ID are denied unless
`NEXTCLOUD_TALK_ALLOW_ALL_USERS=true` is explicitly configured.

Open access gives every participant conversational access to the same Hermes tools available on that platform. Use it only in trusted rooms.

Display names are deliberately **not** used for authorization: display names may change
and need not be unique. `actorDisplayName` is retained only for message attribution.

To require a leading bot name/mention:

```env
NEXTCLOUD_TALK_REQUIRE_MENTION=true
NEXTCLOUD_TALK_BOT_NAME=Hermes
```

### Other settings

```env
NEXTCLOUD_TALK_POLL_TIMEOUT=30
NEXTCLOUD_TALK_PROCESS_HISTORY=false
NEXTCLOUD_TALK_INITIAL_BACKLOG_LIMIT=50
NEXTCLOUD_TALK_MAX_MESSAGE_LENGTH=32000
NEXTCLOUD_TALK_UPLOAD_FOLDER=/Hermes Uploads
NEXTCLOUD_TALK_MAX_INBOUND_FILE_BYTES=26214400
NEXTCLOUD_TALK_MAX_OUTBOUND_FILE_BYTES=26214400
NEXTCLOUD_TALK_ALLOW_PUBLIC_SHARE_FALLBACK=false
```

Room cursors are persisted under the active profile's Hermes cache and advance only
after a message is deterministically ignored or Hermes accepts it successfully.
Handler failures remain retryable. On a first-ever installation/new room, the newest
`NEXTCLOUD_TALK_INITIAL_BACKLOG_LIMIT` messages are processed (50 by default), avoiding
both silent offline loss and an unbounded ancient replay. Set the limit to `0` only to
explicitly skip existing history (a warning is logged), or set legacy
`NEXTCLOUD_TALK_PROCESS_HISTORY=true` for an unlimited, paginated first-run backfill.
Subsequent restarts resume from the persisted cursor and process messages received
while the gateway was offline.

## How media works

### Inbound

Talk file metadata is read from `messageParameters`. The plugin accepts attachment
links only when they resolve to the exact HTTP(S) origin (scheme, host, and effective
port) configured by `NEXTCLOUD_TALK_URL`; userinfo, cross-origin links, and
cross-origin redirects are rejected before credentials can be sent. Same-origin DAV
downloads remain authenticated. Downloads are streamed into the profile-aware Hermes
document cache and bounded by `NEXTCLOUD_TALK_MAX_INBOUND_FILE_BYTES`.

Hermes core then handles each media type normally:

- images are attached to a vision-capable main model or pre-analyzed with `vision_analyze`;
- voice/audio follows the configured Hermes transcription path;
- documents are exposed as local cached files.

### Outbound

Local images and documents are size-checked and streamed to the bot user's Nextcloud
storage, then shared natively to the destination Talk room. Native-share failures fail
delivery by default. Public fallback requires the explicit opt-in
`NEXTCLOUD_TALK_ALLOW_PUBLIC_SHARE_FALLBACK=true` and is attempted only for recognized
capability or permission failures, never transient network or generic failures. If an
attachment/share succeeds but its caption or public-link message fails, the plugin
reports partial delivery as non-retryable to prevent a gateway retry from duplicating
the attachment.

**Privacy warning:** public fallback creates an anonymous Files share. Anyone who
obtains its URL may be able to download the file, independently of Talk room
membership. Enable it only when that exposure matches your data policy and review
Nextcloud's share expiration/revocation settings.

The bot account therefore needs normal Files/WebDAV access. Uploaded files are stored under `/Hermes Uploads` by default.

## Verify

```bash
python -m py_compile adapter.py test_adapter.py
python test_adapter.py -v
```

The suite includes credential-boundary, cursor retry, DM classification, attachment
delivery, fallback-policy, and byte-limit regressions.

After gateway startup, check the log for messages similar to:

```text
Connecting to nextcloud_talk...
[nextcloud_talk] Connected to rooms: abc...xyz
nextcloud_talk connected
```

Room tokens are redacted in plugin logs.

## Architecture

This project is a Hermes **platform plugin**, not a standalone Talk webhook bot:

```text
Nextcloud Talk
    │ OCS long polling / WebDAV
    ▼
nextcloud-talk-platform plugin
    │ MessageEvent / SendResult
    ▼
Hermes Gateway and normal Hermes session
```

That design is useful when Hermes already runs continuously and should behave in Talk like its built-in messaging platforms without exposing a new public service.

## Security

- Use a dedicated Nextcloud account and an app password.
- Never commit the Hermes environment file.
- Keep `NEXTCLOUD_TALK_ALLOWED_USERS` restrictive unless all room members are trusted.
- Treat group membership as authorization to interact with the tools enabled for that Hermes platform.
- The plugin stores downloaded attachments in the local Hermes cache; protect the Hermes host accordingly.
- Public-link fallback is disabled by default because it can expose an outbound file to anyone with the generated URL.

Please report security issues privately as described in [SECURITY.md](SECURITY.md).

## Compatibility

The plugin uses Hermes' public plugin/platform adapter interfaces, but those interfaces may evolve. The initial release was tested with Hermes Agent 0.20.x and Python 3.11.

## License

MIT — see [LICENSE](LICENSE).
