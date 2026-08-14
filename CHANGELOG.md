# Changelog

All notable changes to this project will be documented here.

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
