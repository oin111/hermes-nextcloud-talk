# Changelog

All notable changes to this project will be documented here.

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
