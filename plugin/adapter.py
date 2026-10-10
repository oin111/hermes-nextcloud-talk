"""
Nextcloud Talk platform adapter for Hermes Agent.

Multi-room adapter that polls and responds in multiple Talk rooms simultaneously.

Configuration via ~/.hermes/.env:
    NEXTCLOUD_TALK_URL=https://cloud.example.com
    NEXTCLOUD_TALK_USERNAME=bot-user
    NEXTCLOUD_TALK_PASSWORD=<app password recommended>
    NEXTCLOUD_TALK_ROOM_TOKENS=token1,token2,token3   # comma-separated room tokens
    NEXTCLOUD_TALK_ROOM_TOKEN=<single token>            # kept for backward compat; merged

Optional:
    NEXTCLOUD_TALK_ALLOWED_USERS=alice,bob
    NEXTCLOUD_TALK_ALLOW_ALL_USERS=false
    NEXTCLOUD_TALK_REQUIRE_MENTION=false
    NEXTCLOUD_TALK_BOT_NAME=Hermes
    NEXTCLOUD_TALK_POLL_TIMEOUT=30
    NEXTCLOUD_TALK_INITIAL_BACKLOG_LIMIT=50
    NEXTCLOUD_TALK_PROCESS_HISTORY=false  # legacy true = unlimited first-run backfill
    NEXTCLOUD_TALK_UPLOAD_FOLDER=/Hermes Uploads
    NEXTCLOUD_TALK_MAX_INBOUND_FILE_BYTES=26214400
    NEXTCLOUD_TALK_MAX_ATTACHMENTS_PER_MESSAGE=8
    NEXTCLOUD_TALK_MAX_ATTACHMENT_TOTAL_BYTES=52428800
    NEXTCLOUD_TALK_MAX_CACHE_BYTES=536870912
    NEXTCLOUD_TALK_MAX_CACHE_FILES=2048
    NEXTCLOUD_TALK_MAX_OUTBOUND_FILE_BYTES=26214400
    NEXTCLOUD_TALK_MAX_JSON_BYTES=4194304
    NEXTCLOUD_TALK_MAX_BODY_BYTES=1048576
    NEXTCLOUD_TALK_MAX_POLL_BATCH=200
    NEXTCLOUD_TALK_MAX_ROOMS=200
    NEXTCLOUD_TALK_MAX_ACK_ROOMS=800
    NEXTCLOUD_TALK_MAX_BACKLOG_MESSAGES=10000
    NEXTCLOUD_TALK_ACK_RETENTION_COUNT=4096
    NEXTCLOUD_TALK_ACK_OVERLAP_IDS=199
    NEXTCLOUD_TALK_PROCESSING_TIMEOUT=300
    NEXTCLOUD_TALK_ALLOW_INSECURE_HTTP=false
    NEXTCLOUD_TALK_ALLOW_PUBLIC_SHARE_FALLBACK=false
    NEXTCLOUD_TALK_AUTO_DISCOVER_ROOMS=true
    NEXTCLOUD_TALK_DISCOVERY_INTERVAL=30
"""

from __future__ import annotations

import asyncio
import base64
import collections
try:
    import fcntl  # POSIX writer lease; never permit two gateways on one cursor file.
except ImportError:  # pragma: no cover - an unguarded platform must fail closed.
    fcntl = None
import concurrent.futures
import hashlib
import ipaddress
import inspect
import json
import logging
import mimetypes
import os
import re
import stat
import tempfile
import threading
import time
import unicodedata
import uuid
import weakref
from contextvars import ContextVar
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set
from urllib import error, parse, request

try:
    from hermes_constants import get_hermes_home
except ImportError:  # Hermes 0.20.x/source-tree fallback
    def get_hermes_home() -> Path:
        configured = (os.getenv("HERMES_HOME") or "").strip()
        return Path(configured).expanduser() if configured else Path.home() / ".hermes"

try:  # Hermes with the shared profile-scope-aware readers: gateway/platforms/_shared.py was added
      # to main on 2026-09-02 (661fc669a0bb), extra_or_secret/platform_gate_env on 2026-09-13
      # (de114b3a), and it has been shipped in released trees (e.g. 0.21.1) since.
    from gateway.platforms._shared import get_scoped_secret as _scoped_env
except ImportError:  # runtime without the shared readers, but with the profile secret scope itself
    try:
        from agent.secret_scope import UnscopedSecretError as _UnscopedSecretError
        from agent.secret_scope import get_secret as _scope_get_secret

        def _scoped_env(name, default=None, **_kwargs):
            """The core reader's own body: an installed scope is authoritative, a scoped miss is
            the default, and only the unscoped default-profile path falls back to os.environ.

            The core's optional ``external_fallback`` rung is NOT reproduced here: this adapter
            never passes it, and ignoring it narrows the read rather than widening it."""
            try:
                value = _scope_get_secret(name, None)
            except _UnscopedSecretError:
                value = os.getenv(name)
            return value if value is not None else default
    except ImportError:  # no per-profile secret scope exists at all: os.environ is the profile's own
        def _scoped_env(name, default=None, **_kwargs):
            return os.getenv(name, default)

try:
    from gateway.platforms._shared import extra_or_secret as _extra_or_secret
except ImportError:  # runtimes that ship only the credential reader
    def _extra_or_secret(extra, key, env, default="", *, blank_is_unset=True):
        """env -> YAML ``extra[key]`` -> ``default``, mirroring the core reader."""
        env_value = _scoped_env(env, None)
        if env_value is not None and str(env_value).strip():
            return env_value
        value = (extra or {}).get(key)
        if value is None or (blank_is_unset and isinstance(value, str) and not value.strip()):
            return default
        return value

try:
    from gateway.platforms._shared import platform_gate_env as _gate_env
except ImportError:  # runtimes that ship only the credential reader
    def _gate_env(name, default=""):
        """Allow/deny gate read, mirroring the core reader's body: an installed scope counts only
        while multiplexing is active, a scoped miss returns the default (never the launcher's
        value), and values are stringified the same way."""
        if not name:
            return default
        try:
            from agent.secret_scope import current_secret_scope, is_multiplex_active

            scope = current_secret_scope()
            if scope is not None and is_multiplex_active():
                value = scope.get(name)
                return default if value is None else str(value).strip()
        except Exception:  # no per-profile secret scope at all on this runtime
            pass
        return (os.getenv(name) or default).strip()


def _cfg(extra: Optional[dict], env: str, key: str, default: Any = "") -> Any:
    """This profile's setting: its own ``env`` -> its own YAML ``extra[key]`` -> ``default``.

    A raw ``os.getenv`` here reads the LAUNCH process's values. Under
    ``gateway.multiplex_profiles`` those belong to the default profile, so a secondary
    profile's adapter connected with the default profile's bot credentials and room list
    and answered in the default profile's rooms under the default profile's account.
    """
    return _extra_or_secret(extra, key, env, default)


from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    ProcessingOutcome,
    SendResult,
)

logger = logging.getLogger(__name__)


_RELEASED_VERSION = "0.1.17"
_VERSION_RE = re.compile(r"[A-Za-z0-9._+-]{1,64}")


def _plugin_version() -> str:
    """The plugin version reported to the Talk server, taken from the manifest beside this file so
    it cannot drift from plugin.yaml. Anything unreadable or implausible (missing, a directory, a
    FIFO, invalid UTF-8, a stray comment, a non-header-safe character) falls back to the released
    constant instead of failing the plugin import or poisoning every request's User-Agent."""
    manifest = Path(__file__).with_name("plugin.yaml")
    try:
        if not manifest.is_file():  # a directory, a FIFO (read_text would block forever), a dangling link
            return _RELEASED_VERSION
        text = manifest.read_text(encoding="utf-8")
    except (OSError, ValueError):  # unreadable, or not valid UTF-8
        return _RELEASED_VERSION
    for line in text.splitlines():
        if line.startswith("version:"):
            value = line.split(":", 1)[1].split("#", 1)[0].strip().strip("'\"")
            if _VERSION_RE.fullmatch(value):
                return value
    return _RELEASED_VERSION


_PLUGIN_VERSION = _plugin_version()

_DEFAULT_POLL_TIMEOUT = 30
# While a turn runs, the room is still long-polled so slash commands for the
# busy session (/approve, /deny, /stop) reach Hermes immediately.  Held
# follow-ups are dispatched when the turn ends, so keep this wait short: it
# bounds the extra delay before they are picked up.
_INFLIGHT_POLL_TIMEOUT = 5
_INFLIGHT_MAX_PAGES = 32
# In-memory guard for commands core ran whose durable ACK failed.
_SETTLED_COMMAND_LIMIT = 1024
_DEFAULT_MAX_MESSAGE_LENGTH = 32000
_DEFAULT_MAX_DOWNLOAD_BYTES = 25 * 1024 * 1024
_DEFAULT_MAX_UPLOAD_BYTES = 25 * 1024 * 1024
_DEFAULT_INITIAL_BACKLOG = 50
_DEFAULT_MAX_ATTACHMENTS_PER_MESSAGE = 8
_DEFAULT_MAX_ATTACHMENT_TOTAL_BYTES = 50 * 1024 * 1024
_DEFAULT_MAX_CACHE_BYTES = 512 * 1024 * 1024
_DEFAULT_MAX_CACHE_FILES = 2048
_DEFAULT_MAX_JSON_BYTES = 4 * 1024 * 1024
_DEFAULT_MAX_BODY_BYTES = 1024 * 1024
_MAX_TALK_POLL_BATCH = 200
_DEFAULT_MAX_POLL_BATCH = _MAX_TALK_POLL_BATCH
_DEFAULT_MAX_ROOMS = 200
_ACK_ROOM_LIMIT_MULTIPLIER = 4
_DEFAULT_MAX_BACKLOG_MESSAGES = 10000
_DEFAULT_ACK_RETENTION_COUNT = 4096
_DEFAULT_ACK_OVERLAP_IDS = _DEFAULT_MAX_POLL_BATCH - 1
_DEFAULT_PROCESSING_TIMEOUT = 300.0
_ACK_STATE_VERSION = 2
# Cursor files written by this ledger mark their floors as contiguous-prefix
# floors. Files without the marker (0.1.11 and earlier) are normalized once on
# load with the historical overlap rule, which is what those releases ran.
_ACK_FLOOR_MODE = "prefix"
# A message ID joins the floor only after it has been visible this long, so a
# lower ID committed a moment late by the Talk database is not skipped.
_DEFAULT_FLOOR_SETTLE_SECONDS = 30.0
# A dispatched message that keeps failing is retried with backoff and then
# given up with a warning, so one failing turn neither re-runs forever nor
# blocks the room's floor.
_DEFAULT_MAX_DISPATCH_ATTEMPTS = 5
# Reply delivery failures get a longer, still finite budget: GET polling works
# (otherwise nothing is retried), so repeated POST failures are usually
# deterministic (read-only room, lost chat permission) or a short rate limit.
# With the backoff below 8 attempts span about an hour.
_DEFAULT_MAX_DELIVERY_ATTEMPTS = 8


def _ledger_clock() -> float:
    """Clock for settle/backoff bookkeeping (patchable in tests)."""
    return time.monotonic()
_RETRY_BACKOFF_SECONDS = (0.0, 60.0, 300.0, 900.0)
_STREAM_CHUNK_SIZE = 64 * 1024
_MAX_ERROR_BYTES = 4096
_MAX_DAV_PATH_LENGTH = 2048
_MAX_DAV_COMPONENTS = 64
_MAX_DAV_DECODE_ROUNDS = 3
_MAX_METADATA_PARAMETERS = 64
_MAX_METADATA_KEY = 128
_MAX_METADATA_VALUE = 2048
_MAX_METADATA_DEPTH = 8
_MAX_METADATA_NODES = 512
_MAX_ROOM_TOKEN_LENGTH = 128
_MAX_TALK_MESSAGE_ID = (1 << 63) - 1
_MAX_THREAD_TITLE_LENGTH = 200
_MAX_THREAD_TOPICS = 50
_MAX_TOPIC_PROMPT_LENGTH = 4000
_MAX_TOPIC_SKILLS = 16
_THREAD_TOPICS_TIMEOUT = 120.0
_THREAD_TOPICS_LOOKUP_LIMIT = 50  # Talk rejects a larger page with HTTP 400
_TALK_SHARE_MESSAGE_TYPES = frozenset({"voice-message"})
# Talk 24 keeps ``voice-message`` only for these container types (probed live:
# Ogg/Opus and MP4 audio silently become an ordinary file share).
_VOICE_MESSAGE_SUFFIXES = frozenset({".mp3", ".wav"})
# Edits and deletions are limited to chat messages this adapter posted in the
# running process.  A moderator bot could otherwise edit or delete other
# people's messages if Hermes ever handed it a foreign message ID.
_SENT_MESSAGE_RETENTION = 4096
# Presentation calls (reactions, read marker) run detached from the turn on a
# small dedicated pool with a short HTTP time-out, so a slow server can neither
# delay a reply nor occupy the default executor that polls and sends share.
# The outer asyncio guards are larger than the worst queued wait
# (queue / workers * HTTP time-out + own call), so a call is only abandoned
# after its worker thread has finished: no late POST can overtake a later call.
_PRESENTATION_HTTP_TIMEOUT = 5.0
_PRESENTATION_WORKERS = 2
_PRESENTATION_MAX_PENDING = 4
_STATUS_REACTION_TIMEOUT = 20.0
_READ_MARKER_TIMEOUT = 20.0
_NOTIFICATION_MODES = frozenset({"all", "important"})
# Talk renders a file share whose node cannot be resolved inside the current
# request as "*{actor} shared a file which is no longer available*" and drops
# the ``file`` parameter.  A long-poll request that set up the bot's filesystem
# before the share existed hits this for every fresh upload, so the message is
# re-read with short ordinary history requests before falling back to text.
_UNAVAILABLE_FILE_REFETCH_DELAYS = (1.0, 2.0, 4.0)
_UNAVAILABLE_FILE_REFETCH_TIMEOUT = 10.0
_MIN_CONVERSATION_TYPE = 1
_MAX_CONVERSATION_TYPE = 6
_ATTACHMENT_CACHE_MANIFEST_VERSION = 1
_MAX_ATTACHMENT_CACHE_MANIFEST_BYTES = 1024
_PERMANENT_ATTACHMENT_HTTP_STATUSES = {400, 404, 410, 413, 414, 415, 416, 422}


def _normalize_poll_batch(value: Any) -> int:
    """Clamp a requested page size to Talk's protocol-supported range."""
    return min(_MAX_TALK_POLL_BATCH, max(1, int(value)))


def _normalize_ack_overlap(value: Any, poll_batch: Any) -> int:
    """Keep ACK overlap non-negative and strictly smaller than one page."""
    return max(0, min(int(value), _normalize_poll_batch(poll_batch) - 1))


def _safe_log_text(value: Any, limit: int = 500) -> str:
    text = str(value or "")
    return re.sub(r"[\x00-\x1f\x7f-\x9f]", "?", text)[:limit]


def _contains_unicode_control(value: str) -> bool:
    return any(unicodedata.category(char) == "Cc" for char in value)


def _strict_talk_message_id(value: Any) -> Optional[int]:
    """Accept only provider-safe positive signed-64-bit Talk message IDs."""
    if type(value) is int and 0 < value <= _MAX_TALK_MESSAGE_ID:
        return value
    return None


def _inbound_thread_id(msg: Dict[str, Any], numeric_id: int) -> Optional[int]:
    """Return the Talk thread of a message only when Talk marks it a real thread.

    Every Talk message carries ``threadId`` (its own ID when it is not in a
    thread), so only an exact ``isThread: true`` makes it a topic.  The root
    of a thread is never newer than a message inside it.
    """
    if msg.get("isThread") is not True:
        return None
    thread_id = _strict_talk_message_id(msg.get("threadId"))
    if thread_id is None or thread_id > numeric_id:
        return None
    return thread_id


def _outbound_thread_id(metadata: Any) -> Optional[int]:
    """Map Hermes ``metadata["thread_id"]`` to a validated Talk ``threadId``."""
    if not isinstance(metadata, dict):
        return None
    raw = metadata.get("thread_id")
    if type(raw) is int:
        return _strict_talk_message_id(raw)
    if isinstance(raw, str) and 0 < len(raw) <= 19 and raw.isascii() and raw.isdigit():
        return _strict_talk_message_id(int(raw))
    return None


def _thread_title(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    title = _sanitize_reply_field(" ".join(value[: _MAX_THREAD_TITLE_LENGTH * 4].split()), _MAX_THREAD_TITLE_LENGTH)
    return title[:_MAX_THREAD_TITLE_LENGTH] or None


def _parse_thread_topics(raw: Any) -> List[Dict[str, Any]]:
    """Validate ``extra.thread_topics``; invalid entries are dropped with a warning."""
    if raw in (None, "", []):
        return []
    if not isinstance(raw, list):
        logger.warning("[nextcloud_talk] thread_topics must be a list; ignoring it")
        return []
    topics: List[Dict[str, Any]] = []
    seen: Set[tuple[str, str]] = set()
    for index, entry in enumerate(raw):
        if len(topics) >= _MAX_THREAD_TOPICS:
            logger.warning(
                "[nextcloud_talk] thread_topics has more than %d entries; ignoring the rest",
                _MAX_THREAD_TOPICS,
            )
            break
        if not isinstance(entry, dict):
            logger.warning("[nextcloud_talk] thread_topics[%d] is not a mapping; ignored", index)
            continue
        room = entry.get("room")
        title = _thread_title(entry.get("title") or entry.get("name"))
        if (
            not isinstance(room, str)
            or not re.fullmatch(rf"[A-Za-z0-9]{{1,{_MAX_ROOM_TOKEN_LENGTH}}}", room)
            or not title
        ):
            logger.warning(
                "[nextcloud_talk] thread_topics[%d] needs a room token and a title; ignored",
                index,
            )
            continue
        if (room, title.casefold()) in seen:
            logger.warning("[nextcloud_talk] thread_topics[%d] duplicates a title; ignored", index)
            continue
        raw_skills = entry.get("skills", entry.get("skill"))
        if isinstance(raw_skills, str):
            raw_skills = [raw_skills]
        skills: List[str] = []
        if isinstance(raw_skills, list):
            for name in raw_skills[:_MAX_TOPIC_SKILLS]:
                if isinstance(name, str) and re.fullmatch(r"[A-Za-z0-9_.:/-]{1,128}", name.strip()):
                    if name.strip() not in skills:
                        skills.append(name.strip())
        prompt = entry.get("prompt")
        prompt = (
            _sanitize_prompt_text(prompt)[:_MAX_TOPIC_PROMPT_LENGTH].strip() or None
            if isinstance(prompt, str) else None
        )
        message = entry.get("message")
        message = message.strip()[:2000] if isinstance(message, str) and message.strip() else title
        seen.add((room, title.casefold()))
        topics.append({
            "room": room, "title": title, "skills": skills or None,
            "prompt": prompt, "message": message,
        })
    return topics


def _sanitize_prompt_text(value: str) -> str:
    """Keep newlines/tabs in an operator prompt; drop other control code points."""
    return "".join(
        character for character in value
        if character in "\n\t" or unicodedata.category(character) not in {"Cc", "Cf", "Cs"}
    )


def _valid_message_parameters(parameters: Any) -> bool:
    """Validate untrusted JSON metadata without recursive traversal."""
    if not isinstance(parameters, dict) or len(parameters) > _MAX_METADATA_PARAMETERS:
        return False
    stack: List[tuple[Any, int]] = []
    for key, value in parameters.items():
        if not isinstance(key, str) or len(key) > _MAX_METADATA_KEY or _contains_unicode_control(key):
            return False
        if not isinstance(value, dict):
            return False
        stack.append((value, 1))
    nodes = 0
    while stack:
        value, depth = stack.pop()
        nodes += 1
        if nodes > _MAX_METADATA_NODES or depth > _MAX_METADATA_DEPTH:
            return False
        if isinstance(value, dict):
            if len(value) > _MAX_METADATA_PARAMETERS:
                return False
            for key, child in value.items():
                if not isinstance(key, str) or len(key) > _MAX_METADATA_KEY or _contains_unicode_control(key):
                    return False
                stack.append((child, depth + 1))
        elif isinstance(value, list):
            if len(value) > _MAX_METADATA_PARAMETERS:
                return False
            stack.extend((child, depth + 1) for child in value)
        elif isinstance(value, str):
            if len(value) > _MAX_METADATA_VALUE or _contains_unicode_control(value):
                return False
        elif value is not None and not isinstance(value, (bool, int, float)):
            return False
    return True


def _is_unavailable_file_placeholder(text: str, parameters: Dict[str, Any]) -> bool:
    """Detect Talk's server-rendered stand-in for an unresolved file share.

    The rendered sentence is localized, so match its structure instead: it is
    an emphasized first line containing the ``{actor}`` placeholder, and the parameters
    carry the system ``actor`` entry but no ``file`` entry.  Ordinary user
    comments have no ``actor`` parameter, so typed text cannot trigger this.
    """
    actor = parameters.get("actor")
    if not isinstance(actor, dict) or actor.get("type") != "user":
        return False
    if any(isinstance(p, dict) and p.get("type") == "file" for p in parameters.values()):
        return False
    # The caption, if any, follows a blank line; translations place {actor}
    # anywhere in the sentence (ja "*{actor}は...", el "*Ο/Η {actor} ...").
    first = text.split("\n\n", 1)[0].strip()
    return len(first) > 2 and first.startswith("*") and first.endswith("*") and "{actor}" in first


def _validated_file_share_replacement(
    candidate: Any, numeric_id: int, actor_id: str, actor_type: str
) -> Optional[tuple]:
    if not isinstance(candidate, dict):
        return None
    if _strict_talk_message_id(candidate.get("id")) != numeric_id:
        return None
    if candidate.get("actorId") != actor_id or str(candidate.get("actorType") or "") != actor_type:
        return None
    if candidate.get("systemMessage"):
        return None
    parameters = candidate.get("messageParameters")
    if not _valid_message_parameters(parameters):
        return None
    files = [p for p in parameters.values() if p.get("type") == "file"]
    if not files:
        return None
    for param in files:
        if any(not isinstance(param.get(field, ""), str)
               for field in ("name", "path", "link", "mimetype")):
            return None
    text = candidate.get("message")
    if not isinstance(text, str):
        return None
    return candidate, text.strip(), dict(parameters)


def _truthy(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _csv_set(value: str) -> set[str]:
    return {part.strip() for part in (value or "").split(",") if part.strip()}


def _sanitize_reply_field(value: str, limit: int) -> str:
    """Replace Unicode control/format/surrogate code points and bound metadata."""
    return "".join(
        "?" if unicodedata.category(character) in {"Cc", "Cf", "Cs"} else character
        for character in value
    )[:limit]


def _reaction_mode(value: Any) -> str:
    """``all``, ``groups`` (default) or ``off`` for processing-status reactions.

    Talk notifies a message's author about every reaction in one-to-one rooms,
    so the default keeps status reactions to group rooms, where Talk notifies
    about reactions only at the "all messages" level.
    """
    if value is None or (isinstance(value, str) and not value.strip()):
        return "groups"
    text = str(value).strip().lower()
    if text in {"groups", "group"}:
        return "groups"
    if text in {"all", "true", "1", "yes", "on"} or value is True:
        return "all"
    if text in {"off", "false", "0", "no", "none"} or value is False:
        return "off"
    logger.warning(
        "[nextcloud_talk] Unknown reactions mode %r; using 'groups' (valid: all, groups, off)",
        _safe_log_text(text, 32),
    )
    return "groups"


def _talk_text(text: str, *, markdown: bool = True) -> str:
    """Prepare Hermes text for Talk.

    Talk renders chat messages as extended Markdown (code blocks, links, lists,
    tables), so only image embeds, which Talk does not display, become their
    URL (Talk previews it).  ``markdown=False`` keeps the plain-text conversion.
    """
    if not markdown:
        return _strip_rich_text(text)
    text = re.sub(r"!\[([^\]]*)\]\(([^)\s]+)\)", r"\2", text or "")
    # Keep leading indentation: it can be an indented code block.
    return text.strip("\r\n").rstrip()


def _open_code_fence(text: str) -> Optional[str]:
    """The opening line of a ``` fence still open at the end of ``text``."""
    opener: Optional[str] = None
    for line in text.split("\n"):
        stripped = line.strip()
        if stripped.startswith("```"):
            opener = None if opener is not None else stripped[:64]
    return opener


def _strip_rich_text(text: str) -> str:
    """Convert the small markdown-ish subset Hermes often emits to Talk-friendly text."""
    text = re.sub(r"```\w*\n?", "", text or "")
    text = text.replace("```", "")
    text = re.sub(r"!\[([^\]]*)\]\(([^)]+)\)", r"\2", text)
    text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"\1 (\2)", text)
    return text.strip()


def _secure_base_url(url: str, allow_insecure: bool = False) -> bool:
    try:
        scheme, hostname, _port = NextcloudTalkClient._origin(str(url or ""))
        if scheme == "https":
            return True
        try:
            loopback = ipaddress.ip_address(hostname).is_loopback
        except ValueError:
            loopback = hostname == "localhost"
        return loopback or allow_insecure
    except (TypeError, ValueError):
        return False


class _OutcomeAlreadyRecorded(RuntimeError):
    """A waited-for turn failed; on_processing_complete already accounted it."""


class NextcloudTalkAPIError(RuntimeError):
    def __init__(self, message: str, *, category: str = "generic", status_code: Optional[int] = None):
        super().__init__(message)
        self.category = category
        self.status_code = status_code


class CursorCacheError(OSError):
    """Sanitized cursor persistence failure safe for propagation/logging."""

    def __init__(self, message: str, *, preserve_memory: bool = False):
        super().__init__(message)
        self.preserve_memory = preserve_memory


def _error_class_name(exc: BaseException) -> str:
    name = type(exc).__name__
    return name if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", name) else "Exception"


def _log_cursor_cache_warning(operation: str, exc: BaseException) -> None:
    logger.warning(
        "[nextcloud_talk] Cursor cache %s failed (%s)",
        operation,
        _error_class_name(exc),
    )


class AttachmentDownloadError(NextcloudTalkAPIError):
    """Inbound attachment failure that must block cursor commit."""


class _AttachmentCacheLease:
    """Pin one complete cache entry until its immediate consumer is done."""

    def __init__(
        self, client: "NextcloudTalkClient", path: str, digest: str, target_dir: Path
    ) -> None:
        self.path = path
        self._client = client
        self._digest = digest
        self._target_dir = target_dir
        self._released = False
        self._release_guard = threading.Lock()

    def release(self) -> None:
        with self._release_guard:
            if self._released:
                return
            self._released = True
        self._client._release_cache_lease(self._digest, self._target_dir)

    def __enter__(self) -> "_AttachmentCacheLease":
        return self

    def __exit__(self, *_exc: Any) -> None:
        self.release()


_NETWORK_ERROR_TEXT = "network request failed"
_SAFE_ERROR_CATEGORY_TEXT = {
    "network": _NETWORK_ERROR_TEXT,
    "permission": "request permission denied",
    "capability": "request capability unavailable",
    "security": "unsafe request refused",
    "overflow": "request size limit exceeded",
    "aggregate_overflow": "request aggregate size limit exceeded",
    "content": "invalid response content",
    "shape": "invalid response shape",
    "protocol": "invalid server response",
    "io": "I/O operation failed",
    "generic": "request failed",
}


def _network_api_error(
    error_type: type[NextcloudTalkAPIError] = NextcloudTalkAPIError,
) -> NextcloudTalkAPIError:
    """Build a fixed, non-reflective network error for outward propagation."""
    return error_type(_NETWORK_ERROR_TEXT, category="network")


def _safe_outward_error_text(exc: BaseException) -> str:
    """Return fixed text without rendering exception messages or nested reasons."""
    if isinstance(exc, NextcloudTalkAPIError):
        text = _SAFE_ERROR_CATEGORY_TEXT.get(exc.category, "request failed")
        if exc.category != "network" and isinstance(exc.status_code, int):
            return f"{text} (status {exc.status_code})"
        return text
    if isinstance(exc, (error.URLError, TimeoutError, ConnectionError)):
        return _NETWORK_ERROR_TEXT
    return f"operation failed ({_error_class_name(exc)})"


def _attachment_file_size(path: str) -> int:
    try:
        return Path(path).stat().st_size
    except OSError as exc:
        raise AttachmentDownloadError(
            f"attachment cache I/O failed ({_error_class_name(exc)})", category="io"
        ) from exc


class _AttachmentLeaseHandoff:
    """Choose exactly one owner for a lease returned by an executor worker."""

    def __init__(self, download: Callable[..., Any], args: tuple[Any, ...], kwargs: Dict[str, Any]):
        self._download = download
        self._args = args
        self._kwargs = kwargs
        self._guard = threading.Lock()
        self._cancelled = False
        self._offered: Optional[_AttachmentCacheLease] = None

    def run(self) -> Any:
        result = self._download(*self._args, **self._kwargs)
        release = False
        if isinstance(result, _AttachmentCacheLease):
            with self._guard:
                if self._cancelled:
                    release = True
                else:
                    self._offered = result
        if release:
            result.release()
        return result

    def cancel(self) -> None:
        release: Optional[_AttachmentCacheLease] = None
        with self._guard:
            self._cancelled = True
            if self._offered is not None:
                release = self._offered
                self._offered = None
        if release is not None:
            release.release()

    def accept(self, result: Any) -> Any:
        with self._guard:
            if isinstance(result, _AttachmentCacheLease):
                if self._cancelled or self._offered is not result:
                    raise RuntimeError("attachment lease handoff lost ownership")
                self._offered = None
        return result


async def _download_attachment_with_lease_handoff(
    download: Callable[..., Any], *args: Any, **kwargs: Any
) -> Any:
    """Transfer a worker-created lease to the caller or release it after cancellation."""
    handoff = _AttachmentLeaseHandoff(download, args, kwargs)
    worker = asyncio.create_task(asyncio.to_thread(handoff.run))
    try:
        result = await asyncio.shield(worker)
    except asyncio.CancelledError:
        handoff.cancel()

        def consume_late_failure(completed: "asyncio.Task[Any]") -> None:
            try:
                completed.exception()
            except BaseException:
                pass

        worker.add_done_callback(consume_late_failure)
        raise
    return handoff.accept(result)


class _ExactSizeReader:
    """Expose at most a fixed snapshot length from one open descriptor."""

    def __init__(self, stream: Any, size: int):
        self._stream = stream
        self.remaining = size

    def read(self, size: int = -1) -> bytes:
        if self.remaining <= 0:
            return b""
        amount = self.remaining if size is None or size < 0 else min(size, self.remaining)
        chunk = self._stream.read(amount)
        self.remaining -= len(chunk)
        return chunk


def _parse_room_tokens(value: Optional[str]) -> List[str]:
    """Parse a comma-separated list of room tokens, returning non-empty unique tokens."""
    if not value:
        return []
    seen: Set[str] = set()
    result: List[str] = []
    for part in value.split(","):
        t = part.strip()
        if t and t not in seen:
            seen.add(t)
            result.append(t)
    return result


def _resolve_room_tokens() -> List[str]:
    """Resolve the full set of room tokens from env vars."""
    tokens = _parse_room_tokens(_scoped_env("NEXTCLOUD_TALK_ROOM_TOKENS", "") or "")
    legacy = _parse_room_tokens(_scoped_env("NEXTCLOUD_TALK_ROOM_TOKEN", "") or "")
    # Merge: legacy token first if not already in tokens list
    for t in legacy:
        if t not in tokens:
            tokens.insert(0, t)
    return tokens


class _UnsafeRedirectError(error.HTTPError):
    """Authenticated redirect rejected by the local same-origin policy."""


class _SameOriginRedirectHandler(request.HTTPRedirectHandler):
    """Refuse redirects that could leak the authenticated request."""

    def __init__(self, allowed_origin: tuple[str, str, int]):
        self.allowed_origin = allowed_origin

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        resolved = parse.urljoin(req.full_url, newurl)
        if NextcloudTalkClient._origin(resolved) != self.allowed_origin:
            raise _UnsafeRedirectError(newurl, code, "cross-origin redirect refused", headers, fp)
        return super().redirect_request(req, fp, code, msg, headers, resolved)


class NextcloudTalkClient:
    """Tiny stdlib OCS client for the Nextcloud Talk chat endpoint."""

    def __init__(self, base_url: str, username: str, password: str, *, timeout: int = 35,
                 max_download_bytes: int = _DEFAULT_MAX_DOWNLOAD_BYTES,
                 max_upload_bytes: int = _DEFAULT_MAX_UPLOAD_BYTES,
                 allow_public_share_fallback: bool = False,
                 max_json_bytes: int = _DEFAULT_MAX_JSON_BYTES,
                 max_body_bytes: int = _DEFAULT_MAX_BODY_BYTES,
                 max_poll_batch: int = _DEFAULT_MAX_POLL_BATCH,
                 max_rooms: int = _DEFAULT_MAX_ROOMS,
                 max_cache_bytes: int = _DEFAULT_MAX_CACHE_BYTES,
                 max_cache_files: int = _DEFAULT_MAX_CACHE_FILES):
        if not _secure_base_url(
            base_url, _truthy(_scoped_env("NEXTCLOUD_TALK_ALLOW_INSECURE_HTTP"), False)
        ):
            raise ValueError("NEXTCLOUD_TALK_URL must use HTTPS (HTTP is limited to loopback or explicit opt-in)")
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.timeout = timeout
        self.max_download_bytes = max_download_bytes
        self.max_upload_bytes = max_upload_bytes
        self.allow_public_share_fallback = allow_public_share_fallback
        self.max_json_bytes = max(1, int(max_json_bytes))
        self.max_body_bytes = max(1, int(max_body_bytes))
        self.max_poll_batch = _normalize_poll_batch(max_poll_batch)
        # True: polls tell Talk not to move the read marker (the adapter sets
        # it itself, only for messages it accepted).
        self.explicit_read_marker = False
        self.max_rooms = max(1, int(max_rooms))
        self.max_cache_bytes = max(0, int(max_cache_bytes))
        self.max_cache_files = max(0, int(max_cache_files))
        self._download_locks_guard = threading.Lock()
        self._download_locks: Dict[str, List[Any]] = {}
        self._cache_guard = threading.RLock()
        self._cache_inflight: Dict[str, int] = {}
        self._cache_leases: Dict[str, int] = {}
        self._presentation_guard = threading.Lock()
        self._presentation_pool: Optional[concurrent.futures.ThreadPoolExecutor] = None
        self._presentation_pending = 0
        self._base_origin = self._origin(self.base_url)
        token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
        self._headers = {
            "Authorization": f"Basic {token}",
            "OCS-APIRequest": "true",
            "Accept": "application/json",
            "User-Agent": f"Hermes-Agent-Nextcloud-Talk/{_PLUGIN_VERSION}",
        }

    @staticmethod
    def _origin(url: str) -> tuple[str, str, int]:
        try:
            parsed = parse.urlsplit(url)
        except ValueError as exc:
            raise ValueError("malformed URL") from exc
        if parsed.scheme.lower() not in {"http", "https"} or not parsed.hostname:
            raise ValueError("only HTTP(S) URLs are accepted")
        if parsed.username is not None or parsed.password is not None:
            raise ValueError("URL userinfo is not accepted")
        hostname = parsed.hostname
        if any(char.isspace() for char in hostname) or "\\" in parsed.netloc:
            raise ValueError("malformed URL host")
        try:
            normalized_host = ipaddress.ip_address(hostname).compressed.lower()
        except ValueError:
            try:
                normalized_host = hostname.encode("idna").decode("ascii").lower()
            except UnicodeError as exc:
                raise ValueError("malformed URL host") from exc
            if len(normalized_host) > 253 or not normalized_host:
                raise ValueError("malformed URL host")
            labels = normalized_host.split(".")
            if any(
                not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", label)
                for label in labels
            ):
                raise ValueError("malformed URL host")
        authority = parsed.netloc.rsplit("@", 1)[-1]
        if authority.startswith("["):
            closing = authority.find("]")
            suffix = authority[closing + 1:] if closing >= 0 else ""
            if suffix == ":":
                raise ValueError("empty URL port")
            if suffix and (not suffix.startswith(":") or not suffix[1:].isdigit()):
                raise ValueError("invalid URL port")
        elif authority.endswith(":"):
            raise ValueError("empty URL port")
        try:
            port = parsed.port
        except ValueError as exc:
            raise ValueError("invalid URL port") from exc
        scheme = parsed.scheme.lower()
        if port == 0:
            raise ValueError("URL port 0 is not accepted")
        effective_port = port if port is not None else (443 if scheme == "https" else 80)
        return scheme, normalized_host, effective_port

    def _authenticated_url(self, untrusted_url: str) -> Optional[str]:
        try:
            resolved = parse.urljoin(self.base_url + "/", str(untrusted_url or ""))
            return resolved if self._origin(resolved) == self._base_origin else None
        except (TypeError, ValueError):
            return None

    def _open_authenticated(self, req: request.Request, *, timeout: Optional[float] = None):
        try:
            initial_origin = self._origin(req.full_url)
        except (TypeError, ValueError) as exc:
            raise NextcloudTalkAPIError(
                "refused malformed authenticated request URL", category="security"
            ) from exc
        if initial_origin != self._base_origin:
            raise NextcloudTalkAPIError(
                "refused cross-origin authenticated request", category="security"
            )
        opener = request.build_opener(_SameOriginRedirectHandler(self._base_origin))
        return opener.open(req, timeout=self.timeout if timeout is None else timeout)

    @staticmethod
    def _read_bounded(stream: Any, limit: int) -> bytes:
        chunks: List[bytes] = []
        total = 0
        while True:
            chunk = stream.read(min(_STREAM_CHUNK_SIZE, limit - total + 1))
            if not chunk:
                return b"".join(chunks)
            total += len(chunk)
            if total > limit:
                raise NextcloudTalkAPIError("upstream response exceeds size limit", category="overflow")
            chunks.append(chunk)

    def _read_json(self, stream: Any) -> Any:
        payload = self._read_bounded(stream, self.max_json_bytes)
        try:
            return json.loads(payload.decode("utf-8")) if payload else {}
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise NextcloudTalkAPIError("invalid JSON response", category="protocol") from exc

    @staticmethod
    def _ocs_data(parsed: Any, *, expected_types: Optional[tuple[type, ...]] = None) -> Any:
        if not isinstance(parsed, dict) or not isinstance(parsed.get("ocs"), dict):
            raise NextcloudTalkAPIError("malformed OCS envelope", category="protocol")
        ocs = parsed["ocs"]
        meta = ocs.get("meta")
        if not isinstance(meta, dict):
            raise NextcloudTalkAPIError("malformed OCS metadata", category="protocol")
        status_text = meta.get("status")
        if not isinstance(status_text, str) or not status_text.strip():
            raise NextcloudTalkAPIError("malformed OCS status", category="protocol")
        normalized_status = status_text.strip().lower()
        raw_status_code = meta.get("statuscode")
        if isinstance(raw_status_code, bool) or not isinstance(raw_status_code, (int, str)):
            raise NextcloudTalkAPIError("malformed OCS status code", category="protocol")
        if isinstance(raw_status_code, str) and not raw_status_code.strip().isdigit():
            raise NextcloudTalkAPIError("malformed OCS status code", category="protocol")
        status = int(raw_status_code)
        if normalized_status not in {"ok", "success", "failure", "error"}:
            raise NextcloudTalkAPIError(
                f"malformed OCS status (status {status})",
                category="protocol",
                status_code=status,
            )
        if normalized_status in {"failure", "error"}:
            category = "permission" if status in {401, 403} else "capability" if status in {404, 405, 501} else "generic"
            raise NextcloudTalkAPIError(
                f"OCS request failed (status {status})",
                category=category,
                status_code=status,
            )
        if status < 200 or status > 299:
            raise NextcloudTalkAPIError("invalid OCS success status code", category="protocol")
        if "data" not in ocs:
            raise NextcloudTalkAPIError("malformed OCS envelope: missing data", category="protocol")
        data = ocs["data"]
        if expected_types is not None and not isinstance(data, expected_types):
            raise NextcloudTalkAPIError("unexpected OCS data type", category="protocol")
        return data

    def _url(self, room_token: str, query: Optional[Dict[str, Any]] = None,
             suffix: str = "") -> str:
        encoded_token = parse.quote(str(room_token).strip("/"), safe="")
        url = f"{self.base_url}/ocs/v2.php/apps/spreed/api/v1/chat/{encoded_token}{suffix}"
        if query:
            cleaned = {k: v for k, v in query.items() if v is not None}
            url += "?" + parse.urlencode(cleaned)
        return url

    def _request(self, method: str, room_token: str, *, query: Optional[Dict[str, Any]] = None,
                 data: Optional[Dict[str, Any]] = None,
                 expected_types: Optional[tuple[type, ...]] = None,
                 suffix: str = "") -> Any:
        body = None
        headers = dict(self._headers)
        if data is not None:
            body = parse.urlencode(data).encode("utf-8")
            headers["Content-Type"] = "application/x-www-form-urlencoded"

        req = request.Request(
            self._url(room_token, query, suffix),
            data=body, headers=headers, method=method,
        )
        try:
            with self._open_authenticated(req) as resp:
                parsed_payload = self._read_json(resp)
        except error.HTTPError as exc:
            if exc.code == 304:
                return []
            self._read_bounded(exc, _MAX_ERROR_BYTES)
            raise NextcloudTalkAPIError(f"HTTP {exc.code}", status_code=exc.code) from exc
        except (error.URLError, TimeoutError) as exc:
            raise _network_api_error() from exc

        return self._ocs_data(parsed_payload, expected_types=expected_types)

    async def get_messages(self, room_token: str, *, last_known_id: Optional[int],
                           look_into_future: bool, timeout: int, limit: Optional[int] = None) -> List[dict]:
        query = {
            "lookIntoFuture": 1 if look_into_future else 0,
            "timeout": timeout,
            "lastKnownMessageId": last_known_id,
            "limit": limit,
            "setReadMarker": 0 if getattr(self, "explicit_read_marker", False) else None,
        }
        result = await asyncio.to_thread(
            self._request, "GET", room_token, query=query, expected_types=(list,)
        )
        if not isinstance(result, list):
            raise NextcloudTalkAPIError("chat response data is not a list", category="protocol")
        if len(result) > self.max_poll_batch:
            raise NextcloudTalkAPIError("chat response exceeds poll batch limit", category="overflow")
        return result

    def _ocs_get(self, endpoint: str, query: Optional[Dict[str, Any]] = None,
                 *, expected_types: Optional[tuple[type, ...]] = None) -> Any:
        url = f"{self.base_url}{endpoint}"
        if query:
            cleaned = {key: value for key, value in query.items() if value is not None}
            if cleaned:
                url += "?" + parse.urlencode(cleaned)
        req = request.Request(url, headers=self._headers, method="GET")
        try:
            with self._open_authenticated(req) as resp:
                parsed = self._read_json(resp)
        except error.HTTPError as exc:
            self._read_bounded(exc, _MAX_ERROR_BYTES)
            raise NextcloudTalkAPIError(f"HTTP {exc.code}", status_code=exc.code) from exc
        except (error.URLError, TimeoutError) as exc:
            raise _network_api_error() from exc
        return self._ocs_data(parsed, expected_types=expected_types)

    async def list_conversations(self) -> List[dict]:
        conversations = await asyncio.to_thread(
            self._ocs_get, "/ocs/v2.php/apps/spreed/api/v4/room", expected_types=(list,)
        )
        if not isinstance(conversations, list):
            raise NextcloudTalkAPIError("OCS conversation data is not a list", category="protocol")
        if len(conversations) > self.max_rooms:
            raise NextcloudTalkAPIError("conversation count exceeds configured limit", category="overflow")
        seen: Set[str] = set()
        result: List[dict] = []
        for conversation in conversations if isinstance(conversations, list) else []:
            if not isinstance(conversation, dict):
                raise NextcloudTalkAPIError("OCS conversation entry is not an object", category="protocol")
            token = conversation.get("token")
            conversation_type = conversation.get("type")
            if (
                not isinstance(token, str)
                or not token
                or len(token) > _MAX_ROOM_TOKEN_LENGTH
                or re.fullmatch(r"[A-Za-z0-9]+", token) is None
            ):
                raise NextcloudTalkAPIError(
                    "malformed OCS conversation token", category="protocol"
                )
            if (
                isinstance(conversation_type, bool)
                or not isinstance(conversation_type, int)
                or not _MIN_CONVERSATION_TYPE
                <= conversation_type
                <= _MAX_CONVERSATION_TYPE
            ):
                raise NextcloudTalkAPIError(
                    "malformed OCS conversation type", category="protocol"
                )
            if token not in seen:
                seen.add(token)
                result.append({"token": token, "type": conversation_type})
        return result

    async def list_conversation_tokens(self) -> List[str]:
        return [room["token"] for room in await self.list_conversations() if room["type"] == 1]

    async def send_message(self, room_token: str, message: str, *, reply_to: Optional[str] = None,
                           reference_id: Optional[str] = None,
                           thread_id: Optional[int] = None,
                           thread_title: Optional[str] = None,
                           silent: bool = False) -> Any:
        data = {"message": message}
        if reference_id:
            data["referenceId"] = reference_id
        if reply_to:
            data["replyTo"] = reply_to
        if silent:
            # Talk posts the message without notifying the participants.
            data["silent"] = "true"
        if thread_id is not None:
            data["threadId"] = str(int(thread_id))
        elif thread_title:
            # Talk creates a new thread rooted at this message.
            data["threadTitle"] = thread_title
        return await asyncio.to_thread(
            self._request, "POST", room_token, data=data, expected_types=(dict,)
        )

    def _thread_url(self, room_token: str, suffix: str) -> str:
        return self._url(room_token) + suffix

    def _get_thread_sync(self, room_token: str, thread_id: int) -> Any:
        endpoint = self._thread_url(room_token, f"/threads/{int(thread_id)}")[len(self.base_url):]
        return self._ocs_get(endpoint, expected_types=(dict,))

    async def get_thread(self, room_token: str, thread_id: int) -> Optional[dict]:
        """Return one thread, or ``None`` when Talk reports it does not exist."""
        try:
            return await asyncio.to_thread(self._get_thread_sync, room_token, thread_id)
        except NextcloudTalkAPIError as exc:
            if exc.status_code == 404:
                return None
            raise

    async def list_recent_threads(self, room_token: str, limit: int) -> List[dict]:
        endpoint = self._thread_url(room_token, "/threads/recent")[len(self.base_url):]
        data = await asyncio.to_thread(
            self._ocs_get, endpoint, {"limit": int(limit)}, expected_types=(list,)
        )
        return [item for item in data if isinstance(item, dict)]

    async def edit_message(self, room_token: str, message_id: int, message: str) -> Any:
        """Replace the text of a chat message (Talk: own messages, up to 24 hours old)."""
        return await asyncio.to_thread(
            self._request, "PUT", room_token, data={"message": message},
            expected_types=(dict,), suffix=f"/{int(message_id)}",
        )

    async def delete_message(self, room_token: str, message_id: int) -> Any:
        """Delete a chat message (Talk leaves a "message deleted" placeholder)."""
        return await asyncio.to_thread(
            self._request, "DELETE", room_token, expected_types=(dict,),
            suffix=f"/{int(message_id)}",
        )

    async def supports_threads(self) -> bool:
        data = await asyncio.to_thread(
            self._ocs_get, "/ocs/v2.php/cloud/capabilities", expected_types=(dict,)
        )
        spreed = (data.get("capabilities") or {}).get("spreed") if isinstance(data, dict) else None
        features = spreed.get("features") if isinstance(spreed, dict) else None
        return isinstance(features, list) and "threads" in features

    def _ocs_form_sync(self, method: str, endpoint: str, data: Dict[str, Any]) -> Any:
        """Authenticated form request to an OCS endpoint; 4xx raise with the status code."""
        body = parse.urlencode(data).encode("utf-8")
        headers = dict(self._headers)
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        req = request.Request(f"{self.base_url}{endpoint}", data=body, headers=headers, method=method)
        try:
            with self._open_authenticated(req, timeout=_PRESENTATION_HTTP_TIMEOUT) as resp:
                parsed = self._read_json(resp)
        except error.HTTPError as exc:
            self._read_bounded(exc, _MAX_ERROR_BYTES)
            raise NextcloudTalkAPIError(f"HTTP {exc.code}", status_code=exc.code) from exc
        except (error.URLError, TimeoutError, OSError) as exc:
            raise _network_api_error() from exc
        return self._ocs_data(parsed)

    _PRESENTATION_INIT_LOCK = threading.Lock()

    def _presentation_lock(self) -> Any:
        """The pool guard; created lazily for wrappers that skip ``__init__``."""
        guard = getattr(self, "_presentation_guard", None)
        if guard is None:
            with NextcloudTalkClient._PRESENTATION_INIT_LOCK:
                guard = getattr(self, "_presentation_guard", None)
                if guard is None:
                    guard = self._presentation_guard = threading.Lock()
                    self._presentation_pool = None
                    self._presentation_pending = 0
        return guard

    async def _presentation_call(self, method: str, endpoint: str, data: Dict[str, Any]) -> Any:
        """Run a best-effort presentation request on the dedicated small pool.

        A full queue refuses the call instead of waiting, so a stalled server
        costs at most ``_PRESENTATION_WORKERS`` threads and never delays polls
        or sends on the default executor.
        """
        guard = self._presentation_lock()
        with guard:
            if self._presentation_pending >= _PRESENTATION_MAX_PENDING:
                raise NextcloudTalkAPIError("presentation queue full", category="overflow")
            if self._presentation_pool is None:
                self._presentation_pool = concurrent.futures.ThreadPoolExecutor(
                    max_workers=_PRESENTATION_WORKERS,
                    thread_name_prefix="nextcloud-talk-presentation",
                )
            self._presentation_pending += 1
            try:
                future = self._presentation_pool.submit(
                    self._ocs_form_sync, method, endpoint, data
                )
            except BaseException:
                self._presentation_pending -= 1
                raise

        def _release(_future: Any) -> None:
            with guard:
                self._presentation_pending -= 1

        future.add_done_callback(_release)
        return await asyncio.wrap_future(future)

    def close_presentation(self) -> None:
        """Drop queued presentation calls; running ones end at their HTTP time-out."""
        with self._presentation_lock():
            pool = getattr(self, "_presentation_pool", None)
            self._presentation_pool = None
        if pool is not None:
            pool.shutdown(wait=False, cancel_futures=True)

    @staticmethod
    def _spreed_endpoint(kind: str, room_token: str, suffix: str = "") -> str:
        encoded_token = parse.quote(str(room_token).strip("/"), safe="")
        return f"/ocs/v2.php/apps/spreed/api/v1/{kind}/{encoded_token}{suffix}"

    async def add_reaction(self, room_token: str, message_id: int, reaction: str) -> None:
        await self._presentation_call(
            "POST",
            self._spreed_endpoint("reaction", room_token, f"/{int(message_id)}"),
            {"reaction": reaction},
        )

    async def remove_reaction(self, room_token: str, message_id: int, reaction: str) -> None:
        try:
            await self._presentation_call(
                "DELETE",
                self._spreed_endpoint("reaction", room_token, f"/{int(message_id)}"),
                {"reaction": reaction},
            )
        except NextcloudTalkAPIError as exc:
            if exc.status_code != 404:  # 404: the reaction is already gone
                raise

    async def mark_read(self, room_token: str, message_id: int) -> None:
        await self._presentation_call(
            "POST",
            self._spreed_endpoint("chat", room_token, "/read"),
            {"lastReadMessage": int(message_id)},
        )

    def _dav_url(self, remote_path: str) -> str:
        if not isinstance(remote_path, str) or not remote_path or len(remote_path) > _MAX_DAV_PATH_LENGTH:
            raise NextcloudTalkAPIError("invalid DAV path", category="security")
        if remote_path.startswith(("/", "\\")) or parse.urlsplit(remote_path).scheme:
            raise NextcloudTalkAPIError("absolute DAV path refused", category="security")
        validation_path = remote_path
        decoded = remote_path
        for round_index in range(_MAX_DAV_DECODE_ROUNDS):
            try:
                canonical = parse.unquote(
                    validation_path,
                    encoding="utf-8",
                    errors="strict" if round_index == 0 else "replace",
                )
            except UnicodeDecodeError as exc:
                raise NextcloudTalkAPIError(
                    "invalid percent-encoded DAV path", category="security"
                ) from exc
            if round_index == 0:
                # Build the DAV URL from exactly one decode so legitimate names
                # containing percent escapes retain their filename semantics.
                decoded = canonical
            if canonical.startswith(("/", "\\")) or parse.urlsplit(canonical).scheme:
                raise NextcloudTalkAPIError("absolute DAV path refused", category="security")
            if _contains_unicode_control(canonical) or "\\" in canonical:
                raise NextcloudTalkAPIError("unsafe DAV path characters", category="security")
            if any(part in {".", ".."} for part in canonical.split("/")):
                raise NextcloudTalkAPIError("unsafe DAV path components", category="security")
            if canonical == validation_path:
                break
            validation_path = canonical
        else:
            # A remaining encoded percent, separator, dot, or control byte can
            # become traversal after more decoding. Refuse it rather than loop.
            if re.search(
                r"%(?:25|2e|2f|5c|0[0-9A-Fa-f]|1[0-9A-Fa-f]|7f|c2%[89][0-9A-Fa-f])",
                canonical,
                re.IGNORECASE,
            ):
                raise NextcloudTalkAPIError(
                    "ambiguous percent-encoded DAV path", category="security"
                )
        raw_parts = decoded.split("/")
        if len(raw_parts) > _MAX_DAV_COMPONENTS or any(
            not part or part in {".", ".."} or len(part) > 255 for part in raw_parts
        ):
            raise NextcloudTalkAPIError("unsafe DAV path components", category="security")
        parts = [parse.quote(part, safe="") for part in raw_parts]
        user = parse.quote(self.username, safe="")
        namespace = f"{self.base_url}/remote.php/dav/files/{user}/"
        result = namespace + "/".join(parts)
        if not result.startswith(namespace):
            raise NextcloudTalkAPIError("DAV namespace escape refused", category="security")
        return result

    def _raw_request(self, method: str, url: str, *, data: Any = None,
                     headers: Optional[Dict[str, str]] = None) -> tuple[int, bytes, Dict[str, str]]:
        req_headers = dict(self._headers)
        req_headers.update(headers or {})
        req = request.Request(url, data=data, headers=req_headers, method=method)
        try:
            with self._open_authenticated(req) as resp:
                return resp.status, self._read_bounded(resp, self.max_body_bytes), dict(resp.headers)
        except error.HTTPError as exc:
            self._read_bounded(exc, _MAX_ERROR_BYTES)
            raise NextcloudTalkAPIError(f"HTTP {exc.code}", status_code=exc.code) from exc
        except (error.URLError, TimeoutError) as exc:
            raise _network_api_error() from exc

    def _ensure_folder_sync(self, folder: str) -> None:
        current = ""
        for part in [p for p in folder.strip("/").split("/") if p]:
            current = f"{current}/{part}"
            req = request.Request(self._dav_url(current.lstrip("/")), headers=self._headers, method="MKCOL")
            try:
                with self._open_authenticated(req) as resp:
                    if resp.status not in (201, 405):
                        raise NextcloudTalkAPIError(
                            "WebDAV folder creation failed",
                            status_code=resp.status,
                        )
            except error.HTTPError as exc:
                if exc.code != 405:
                    self._read_bounded(exc, _MAX_ERROR_BYTES)
                    raise NextcloudTalkAPIError(f"MKCOL failed: HTTP {exc.code}", status_code=exc.code) from exc
            except (error.URLError, TimeoutError) as exc:
                raise _network_api_error() from exc

    @staticmethod
    def _safe_filename(name: str) -> str:
        name = Path(name or "file").name
        name = re.sub(r"[^A-Za-z0-9._ -]+", "_", name).strip(" .")
        return name or "file"

    def _download_file(
        self, url: str, download_dir: str, *, cache_identity: Optional[str] = None,
        max_bytes: Optional[int] = None, lease: bool = False,
    ) -> Any:
        resolved = self._authenticated_url(url)
        if not resolved:
            raise AttachmentDownloadError(
                "refused non-same-origin attachment URL", category="security"
            )
        if self.max_cache_files < 1:
            raise AttachmentDownloadError(
                "attachment cache file quota is zero", category="overflow"
            )
        identity = cache_identity or resolved
        protected_digest = hashlib.sha256(
            identity.encode("utf-8", errors="replace")
        ).hexdigest()[:24]
        with self._cache_guard:
            self._cache_inflight[protected_digest] = self._cache_inflight.get(protected_digest, 0) + 1
        with self._download_locks_guard:
            entry = self._download_locks.get(identity)
            if entry is None:
                entry = [threading.Lock(), 0]
                self._download_locks[identity] = entry
            entry[1] += 1
            identity_lock = entry[0]
        identity_lock.acquire()
        try:
            path = self._download_file_locked(
                resolved, download_dir, identity=identity, max_bytes=max_bytes
            )
            if lease:
                target_dir = Path(download_dir).expanduser()
                with self._cache_guard:
                    self._cache_leases[protected_digest] = (
                        self._cache_leases.get(protected_digest, 0) + 1
                    )
                return _AttachmentCacheLease(self, path, protected_digest, target_dir)
            return path
        finally:
            identity_lock.release()
            with self._download_locks_guard:
                entry[1] -= 1
                if entry[1] == 0 and self._download_locks.get(identity) is entry:
                    del self._download_locks[identity]
            with self._cache_guard:
                remaining = self._cache_inflight.get(protected_digest, 1) - 1
                if remaining > 0:
                    self._cache_inflight[protected_digest] = remaining
                else:
                    self._cache_inflight.pop(protected_digest, None)
                try:
                    self._enforce_cache_quota(Path(download_dir).expanduser())
                except OSError:
                    logger.warning("[nextcloud_talk] Attachment cache quota check failed")

    def _release_cache_lease(self, digest: str, target_dir: Path) -> None:
        with self._cache_guard:
            remaining = self._cache_leases.get(digest, 1) - 1
            if remaining > 0:
                self._cache_leases[digest] = remaining
            else:
                self._cache_leases.pop(digest, None)
            try:
                self._enforce_cache_quota(target_dir)
            except OSError:
                logger.warning("[nextcloud_talk] Attachment cache quota check failed")

    def _enforce_cache_quota(self, target_dir: Path) -> None:
        """Evict complete attachment pairs oldest-first under one cache lock."""
        entries: List[tuple[int, str, Path, Path, int]] = []
        for manifest_path in sorted(target_dir.glob("*.complete.json")):
            digest = manifest_path.name.removesuffix(".complete.json")
            try:
                raw = manifest_path.read_bytes()
                if len(raw) > _MAX_ATTACHMENT_CACHE_MANIFEST_BYTES:
                    continue
                manifest = json.loads(raw.decode("utf-8"))
                filename = manifest.get("filename") if isinstance(manifest, dict) else None
                size = manifest.get("size") if isinstance(manifest, dict) else None
                if (
                    not isinstance(filename, str) or Path(filename).name != filename
                    or type(size) is not int or size < 0
                ):
                    continue
                data_path = target_dir / filename
                data_stat = data_path.stat()
                if not data_path.is_file() or data_path.is_symlink() or data_stat.st_size != size:
                    continue
                entries.append((manifest_path.stat().st_mtime_ns, manifest_path.name,
                                manifest_path, data_path, size))
            except (OSError, UnicodeError, ValueError, TypeError, json.JSONDecodeError):
                continue
        total_bytes = sum(entry[4] for entry in entries)
        total_files = len(entries)
        for _mtime, name, manifest_path, data_path, size in sorted(entries):
            if total_files <= self.max_cache_files and total_bytes <= self.max_cache_bytes:
                break
            digest = name.removesuffix(".complete.json")
            if digest in self._cache_inflight or digest in self._cache_leases:
                continue
            try:
                data_path.unlink(missing_ok=True)
                manifest_path.unlink(missing_ok=True)
            except OSError:
                logger.warning("[nextcloud_talk] Attachment cache eviction failed")
                continue
            total_files -= 1
            total_bytes -= size

    def _download_file_locked(self, resolved: str, download_dir: str, *, identity: str,
                              max_bytes: Optional[int] = None) -> str:
        target_dir = Path(download_dir)
        headers = dict(self._headers)
        headers["Accept"] = "*/*"
        temp_path: Optional[Path] = None
        manifest_temp_path: Optional[Path] = None
        finalized_path: Optional[Path] = None
        per_file_limit = max(0, int(self.max_download_bytes))
        cache_limit = max(0, int(self.max_cache_bytes))
        effective_limit = min(per_file_limit, cache_limit)
        aggregate_limited = False
        if max_bytes is not None:
            aggregate_limit = int(max_bytes)
            if aggregate_limit < 0:
                raise AttachmentDownloadError(
                    "attachment aggregate limit exceeded", category="aggregate_overflow"
                )
            aggregate_limited = aggregate_limit < effective_limit
            effective_limit = min(effective_limit, aggregate_limit)
        identity_digest = hashlib.sha256(
            identity.encode("utf-8", errors="replace")
        ).hexdigest()
        digest = identity_digest[:24]

        def data_entries() -> List[Path]:
            return sorted(
                candidate for candidate in target_dir.glob(f"{digest}-*")
                if candidate.is_file() and not candidate.is_symlink()
            )

        def remove_cache_path(path: Path) -> None:
            try:
                path.unlink(missing_ok=True)
            except OSError:
                logger.warning("[nextcloud_talk] Could not remove invalid attachment cache entry")

        def fsync_cache_directory() -> None:
            flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
            directory_fd = os.open(target_dir, flags)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)

        manifest_path = target_dir / f"{digest}.complete.json"

        def verified_cache_entry() -> Optional[Path]:
            candidates = data_entries()
            if not manifest_path.is_file() or manifest_path.is_symlink():
                for candidate in candidates:
                    remove_cache_path(candidate)
                remove_cache_path(manifest_path)
                return None
            try:
                if manifest_path.stat().st_size > _MAX_ATTACHMENT_CACHE_MANIFEST_BYTES:
                    raise ValueError("oversized cache manifest")
                with manifest_path.open("rb") as manifest_stream:
                    raw_manifest = manifest_stream.read(
                        _MAX_ATTACHMENT_CACHE_MANIFEST_BYTES + 1
                    )
                if len(raw_manifest) > _MAX_ATTACHMENT_CACHE_MANIFEST_BYTES:
                    raise ValueError("oversized cache manifest")
                manifest = json.loads(raw_manifest.decode("utf-8"))
                if not isinstance(manifest, dict):
                    raise ValueError("invalid cache manifest")
                filename = manifest.get("filename")
                size = manifest.get("size")
                content_digest = manifest.get("sha256")
                if (
                    set(manifest) != {
                        "version", "identity_sha256", "filename", "size", "sha256"
                    }
                    or type(manifest.get("version")) is not int
                    or manifest.get("version") != _ATTACHMENT_CACHE_MANIFEST_VERSION
                    or manifest.get("identity_sha256") != identity_digest
                    or not isinstance(filename, str)
                    or not filename
                    or Path(filename).name != filename
                    or _contains_unicode_control(filename)
                    or type(size) is not int
                    or size < 0
                    or size > effective_limit
                    or not isinstance(content_digest, str)
                    or re.fullmatch(r"[0-9a-f]{64}", content_digest) is None
                ):
                    raise ValueError("invalid cache manifest")
                matching = [candidate for candidate in candidates if candidate.name == filename]
                if len(matching) != 1:
                    raise ValueError("cache file missing")
                candidate = matching[0]
                hasher = hashlib.sha256()
                total = 0
                with candidate.open("rb") as cached_stream:
                    opened_stat = os.fstat(cached_stream.fileno())
                    if not stat.S_ISREG(opened_stat.st_mode) or opened_stat.st_size != size:
                        raise ValueError("invalid cache file")
                    while True:
                        chunk = cached_stream.read(_STREAM_CHUNK_SIZE)
                        if not chunk:
                            break
                        total += len(chunk)
                        if total > size:
                            raise ValueError("invalid cache file")
                        hasher.update(chunk)
                    if os.fstat(cached_stream.fileno()).st_size != size:
                        raise ValueError("invalid cache file")
                if total != size or hasher.hexdigest() != content_digest:
                    raise ValueError("invalid cache file")
                for stale in candidates:
                    if stale != candidate:
                        remove_cache_path(stale)
                return candidate
            except (OSError, UnicodeError, ValueError, TypeError, json.JSONDecodeError):
                for candidate in candidates:
                    remove_cache_path(candidate)
                remove_cache_path(manifest_path)
                return None

        try:
            target_dir = target_dir.expanduser()
            manifest_path = target_dir / f"{digest}.complete.json"
            target_dir.mkdir(parents=True, exist_ok=True)
            cached = verified_cache_entry()
            if cached is not None:
                return str(cached)
            req = request.Request(resolved, headers=headers)
            with self._open_authenticated(req) as resp:
                final_url = getattr(resp, "geturl", lambda: resolved)()
                if self._origin(final_url) != self._base_origin:
                    raise AttachmentDownloadError("cross-origin redirect refused", category="security")
                content_type = resp.headers.get("Content-Type", "")
                if "text/html" in content_type.lower():
                    raise AttachmentDownloadError("received HTML instead of attachment", category="content")
                declared = resp.headers.get("Content-Length")
                try:
                    declared_size = int(declared) if declared else None
                except (TypeError, ValueError) as exc:
                    raise AttachmentDownloadError(
                        "invalid attachment Content-Length", category="content"
                    ) from exc
                if declared_size is not None and (declared_size < 0 or declared_size > effective_limit):
                    category = "aggregate_overflow" if aggregate_limited else "overflow"
                    raise AttachmentDownloadError("inbound file exceeds size limit", category=category)
                fname = self._safe_filename(self._extract_filename(resolved, resp.headers))
                local = target_dir / f"{digest}-{fname}"
                fd, temp_name = tempfile.mkstemp(prefix=f".{digest}-", suffix=".part", dir=target_dir)
                temp_path = Path(temp_name)
                total = 0
                content_hasher = hashlib.sha256()
                with os.fdopen(fd, "wb") as output:
                    while True:
                        chunk = resp.read(_STREAM_CHUNK_SIZE)
                        if not chunk:
                            break
                        total += len(chunk)
                        if total > effective_limit:
                            category = "aggregate_overflow" if aggregate_limited else "overflow"
                            raise AttachmentDownloadError(
                                "inbound file exceeds size limit", category=category
                            )
                        output.write(chunk)
                        content_hasher.update(chunk)
                    if declared_size is not None and total != declared_size:
                        if total < declared_size:
                            raise AttachmentDownloadError(
                                "attachment body ended before declared length", category="network"
                            )
                        raise AttachmentDownloadError(
                            "attachment body exceeded declared length", category="content"
                        )
                    output.flush()
                    os.fsync(output.fileno())
                os.replace(temp_path, local)
                temp_path = None
                finalized_path = local
                manifest = {
                    "version": _ATTACHMENT_CACHE_MANIFEST_VERSION,
                    "identity_sha256": identity_digest,
                    "filename": local.name,
                    "size": total,
                    "sha256": content_hasher.hexdigest(),
                }
                manifest_bytes = json.dumps(
                    manifest, sort_keys=True, separators=(",", ":")
                ).encode("utf-8")
                if len(manifest_bytes) > _MAX_ATTACHMENT_CACHE_MANIFEST_BYTES:
                    raise OSError("cache manifest exceeded internal limit")
                manifest_fd, manifest_temp_name = tempfile.mkstemp(
                    prefix=f".{digest}-manifest-", suffix=".part", dir=target_dir
                )
                manifest_temp_path = Path(manifest_temp_name)
                with os.fdopen(manifest_fd, "wb") as manifest_stream:
                    manifest_stream.write(manifest_bytes)
                    manifest_stream.flush()
                    os.fsync(manifest_stream.fileno())
                with self._cache_guard:
                    os.replace(manifest_temp_path, manifest_path)
                    manifest_temp_path = None
                    fsync_cache_directory()
                    self._enforce_cache_quota(target_dir)
                finalized_path = None
                logger.info(
                    "[nextcloud_talk] Downloaded attachment %s (%d bytes)",
                    _safe_log_text(local.name, 300), total,
                )
                return str(local)
        except (error.HTTPError, error.URLError, TimeoutError, OSError,
                NextcloudTalkAPIError, ValueError) as exc:
            if isinstance(exc, error.HTTPError):
                try:
                    exc.read(_MAX_ERROR_BYTES + 1)
                except Exception:
                    pass
                finally:
                    exc.close()
            for partial in (temp_path, manifest_temp_path, finalized_path):
                if partial:
                    try:
                        partial.unlink(missing_ok=True)
                    except OSError:
                        logger.warning("[nextcloud_talk] Could not remove partial attachment")
            if finalized_path is not None:
                remove_cache_path(manifest_path)
            if isinstance(exc, AttachmentDownloadError):
                raise
            if isinstance(exc, NextcloudTalkAPIError):
                category = exc.category
                status_code = exc.status_code
            elif isinstance(exc, error.HTTPError):
                status_code = exc.code if isinstance(exc.code, int) else None
                if isinstance(exc, _UnsafeRedirectError):
                    category = "security"
                elif status_code in _PERMANENT_ATTACHMENT_HTTP_STATUSES:
                    category = "content"
                else:
                    category = "network"
            elif isinstance(exc, (error.URLError, TimeoutError)):
                category = "network"
                status_code = None
            else:
                category = "io"
                status_code = None
            if category == "network":
                raise AttachmentDownloadError(
                    _NETWORK_ERROR_TEXT, category="network", status_code=status_code
                ) from exc
            if isinstance(exc, error.HTTPError):
                raise AttachmentDownloadError(
                    "attachment request was deterministically rejected",
                    category=category,
                    status_code=status_code,
                ) from exc
            raise AttachmentDownloadError(
                f"attachment download failed ({_error_class_name(exc)})",
                category=category,
            ) from exc

    def _extract_filename(self, url: str, headers: dict) -> str:
        cd = headers.get("Content-Disposition", "")
        m = re.search(r'filename="?([^";]+)"?', cd)
        if m:
            return m.group(1)
        m = re.search(r"filename\*=UTF-8''([^;\s]+)", cd)
        if m:
            from urllib.parse import unquote
            return unquote(m.group(1))
        fname = url.rstrip("/").split("/")[-1]
        if "?" in fname:
            fname = fname.split("?")[0]
        return fname or "downloaded_file"

    def _upload_file_sync(self, local_path: str, upload_folder: str) -> str:
        path = Path(local_path).expanduser()
        try:
            stream = path.open("rb")
        except OSError as exc:
            raise NextcloudTalkAPIError("outbound file could not be opened", category="io") from exc
        with stream:
            try:
                opened_stat = os.fstat(stream.fileno())
                if not stat.S_ISREG(opened_stat.st_mode):
                    raise NextcloudTalkAPIError("outbound source is not a regular file", category="io")
                size = opened_stat.st_size
                if size > self.max_upload_bytes:
                    raise NextcloudTalkAPIError("outbound file exceeds size limit", category="overflow")
                folder = "/" + upload_folder.strip("/") if upload_folder else "/Hermes Uploads"
                self._ensure_folder_sync(folder)
                current_stat = os.fstat(stream.fileno())
                path_stat = path.stat()
                identity = (opened_stat.st_dev, opened_stat.st_ino)
                if (
                    current_stat.st_size != size
                    or (current_stat.st_dev, current_stat.st_ino) != identity
                    or (path_stat.st_dev, path_stat.st_ino) != identity
                ):
                    raise NextcloudTalkAPIError("outbound file changed before upload", category="io")
            except NextcloudTalkAPIError:
                raise
            except OSError as exc:
                raise NextcloudTalkAPIError("outbound file validation failed", category="io") from exc

            filename = self._safe_filename(path.name)
            stamp = time.strftime("%Y%m%d-%H%M%S")
            remote_path = f"{folder.rstrip('/')}/{stamp}-{filename}"
            content_type = mimetypes.guess_type(str(path))[0] or "application/octet-stream"
            headers = {"Content-Type": content_type, "Content-Length": str(size)}
            bounded = _ExactSizeReader(stream, size)
            try:
                status, _body, _headers = self._raw_request(
                    "PUT", self._dav_url(remote_path.lstrip("/")), data=bounded, headers=headers
                )
            except NextcloudTalkAPIError as exc:
                if exc.category == "network":
                    raise NextcloudTalkAPIError(
                        "attachment upload outcome unknown", category="ambiguous_upload"
                    ) from exc
                raise
            try:
                final_stat = os.fstat(stream.fileno())
                final_path_stat = path.stat()
            except OSError as exc:
                raise NextcloudTalkAPIError("outbound file validation failed", category="io") from exc
            if (
                bounded.remaining != 0
                or final_stat.st_size != size
                or (final_stat.st_dev, final_stat.st_ino) != identity
                or (final_path_stat.st_dev, final_path_stat.st_ino) != identity
            ):
                raise NextcloudTalkAPIError("outbound file changed during upload", category="io")
        if status not in (200, 201, 204):
            raise NextcloudTalkAPIError(f"PUT {remote_path}: HTTP {status}")
        return remote_path

    def _ocs_post(self, endpoint: str, data: Dict[str, Any]) -> Any:
        body = parse.urlencode(data).encode("utf-8")
        headers = dict(self._headers)
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        url = f"{self.base_url}{endpoint}"
        req = request.Request(url, data=body, headers=headers, method="POST")
        try:
            with self._open_authenticated(req) as resp:
                parsed = self._read_json(resp)
        except error.HTTPError as exc:
            self._read_bounded(exc, _MAX_ERROR_BYTES)
            category = (
                "permission" if exc.code in {401, 403}
                else "capability" if exc.code in {404, 405, 501}
                else "generic"
            )
            raise NextcloudTalkAPIError(
                f"HTTP {exc.code}", category=category, status_code=exc.code
            ) from exc
        except (error.URLError, TimeoutError) as exc:
            raise _network_api_error() from exc
        return self._ocs_data(parsed, expected_types=(dict,))

    def _share_file_to_talk_sync(self, remote_path: str, room_token: str, *,
                                 caption: Optional[str] = None,
                                 reply_to: Optional[str] = None,
                                 thread_id: Optional[int] = None,
                                 message_type: Optional[str] = None,
                                 silent: bool = False) -> Any:
        form: Dict[str, Any] = {
            "path": remote_path, "shareType": 10, "shareWith": room_token
        }
        talk_metadata: Dict[str, Any] = {}
        if caption:
            talk_metadata["caption"] = _sanitize_reply_field(caption, 2000)
        if reply_to:
            talk_metadata["replyTo"] = _sanitize_reply_field(reply_to, 64)
        if thread_id is not None:
            talk_metadata["threadId"] = int(thread_id)
        if message_type in _TALK_SHARE_MESSAGE_TYPES:
            talk_metadata["messageType"] = message_type
        if silent:
            # Talk posts the share without chat notifications (needs the
            # ``media-caption`` capability; older servers ignore the field).
            talk_metadata["silent"] = True
        if talk_metadata:
            form["talkMetaData"] = json.dumps(
                talk_metadata, sort_keys=True, separators=(",", ":")
            )
        return self._ocs_post(
            "/ocs/v2.php/apps/files_sharing/api/v1/shares", form
        )

    def _create_public_share_sync(self, remote_path: str) -> Any:
        return self._ocs_post(
            "/ocs/v2.php/apps/files_sharing/api/v1/shares",
            {"path": remote_path, "shareType": 3},
        )

    async def upload_and_share_file(self, local_path: str, room_token: str,
                                    upload_folder: str, *, caption: Optional[str] = None,
                                    reply_to: Optional[str] = None,
                                    thread_id: Optional[int] = None,
                                    message_type: Optional[str] = None,
                                    silent: bool = False) -> tuple[str, Any, bool, bool]:
        remote_path = await asyncio.to_thread(self._upload_file_sync, local_path, upload_folder)
        try:
            if caption or reply_to or thread_id is not None or message_type or silent:
                share = await asyncio.to_thread(
                    self._share_file_to_talk_sync, remote_path, room_token,
                    caption=caption, reply_to=reply_to,
                    **({"thread_id": thread_id} if thread_id is not None else {}),
                    **({"message_type": message_type} if message_type else {}),
                    **({"silent": True} if silent else {}),
                )
            else:
                share = await asyncio.to_thread(
                    self._share_file_to_talk_sync, remote_path, room_token
                )
            return remote_path, share, True, True
        except NextcloudTalkAPIError as exc:
            if (
                caption or reply_to or thread_id is not None or message_type or silent
            ) and exc.category == "capability":
                # A structured capability response is the only safe proof that
                # this server cannot attach Talk metadata atomically.
                share = await asyncio.to_thread(
                    self._share_file_to_talk_sync, remote_path, room_token
                )
                return remote_path, share, True, False
            if not self.allow_public_share_fallback:
                raise
            if exc.category not in {"capability", "permission"}:
                raise
            logger.warning(
                "[nextcloud_talk] Native Talk share failed; explicit public-share fallback is enabled"
            )
            share = await asyncio.to_thread(self._create_public_share_sync, remote_path)
            return remote_path, share, False, False


class NextcloudTalkAdapter(BasePlatformAdapter):
    MAX_MESSAGE_LENGTH = _DEFAULT_MAX_MESSAGE_LENGTH
    splits_long_messages = True
    # Failure-only status reaction. Talk has no silent reaction: the server
    # notifies the message author about each new one, so 👀/✅ on every turn
    # would ping on every message. Base's completion hook adds ❌ on failure;
    # with no in-progress emoji its "remove" step sends no request.
    _ACK_EMOJI = None
    _OK_EMOJI = None
    _FAIL_EMOJI = "❌"

    _DETERMINISTIC_ATTACHMENT_CATEGORIES = {
        "security", "path", "overflow", "aggregate_overflow", "content", "shape", "protocol"
    }

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform("nextcloud_talk"))
        extra = getattr(config, "extra", {}) or {}

        self.base_url = _cfg(extra, "NEXTCLOUD_TALK_URL", "url", "")
        self.username = _cfg(extra, "NEXTCLOUD_TALK_USERNAME", "username", "")
        self.password = _cfg(extra, "NEXTCLOUD_TALK_PASSWORD", "password", "")
        self.bot_name = _cfg(extra, "NEXTCLOUD_TALK_BOT_NAME", "bot_name", "Hermes")
        self.poll_timeout = int(_cfg(extra, "NEXTCLOUD_TALK_POLL_TIMEOUT", "poll_timeout", _DEFAULT_POLL_TIMEOUT))
        self.require_mention = _truthy(
            _cfg(extra, "NEXTCLOUD_TALK_REQUIRE_MENTION", "require_mention", None), False
        )
        self.markdown = _truthy(_cfg(extra, "NEXTCLOUD_TALK_MARKDOWN", "markdown", None), True)
        self.status_reactions = _reaction_mode(
            _cfg(extra, "NEXTCLOUD_TALK_REACTIONS", "reactions", None)
        )
        self.read_marker = _truthy(
            _cfg(extra, "NEXTCLOUD_TALK_READ_MARKER", "read_marker", None), True
        )
        notifications = str(
            _cfg(extra, "NEXTCLOUD_TALK_NOTIFICATIONS", "notifications", None) or "important"
        ).strip().lower()
        if notifications not in _NOTIFICATION_MODES:
            logger.warning(
                "[nextcloud_talk] Unknown notifications mode %r; using 'important' (valid: all, important)",
                _safe_log_text(notifications, 32),
            )
            notifications = "important"
        self.notifications = notifications
        self._read_marker_pending: Dict[str, int] = {}
        self._read_marker_done: Dict[str, int] = {}
        self._read_marker_tasks: Dict[str, asyncio.Task] = {}
        self._presentation_tasks: Set[asyncio.Task] = set()
        self._sent_message_ids: "collections.OrderedDict[tuple[str, int], None]" = (
            collections.OrderedDict()
        )
        # Legacy PROCESS_HISTORY=true means unlimited first-run backfill.  The
        # safe default is a documented bounded backlog, never "skip to latest".
        self.process_history = _truthy(
            _cfg(extra, "NEXTCLOUD_TALK_PROCESS_HISTORY", "process_history", None), False
        )
        backlog_value = _cfg(extra, "NEXTCLOUD_TALK_INITIAL_BACKLOG_LIMIT", "initial_backlog_limit", _DEFAULT_INITIAL_BACKLOG)
        self.initial_backlog_limit: Optional[int] = None if self.process_history else max(0, int(backlog_value))
        self.upload_folder = _cfg(extra, "NEXTCLOUD_TALK_UPLOAD_FOLDER", "upload_folder", "/Hermes Uploads")
        self.max_download_bytes = int(_cfg(extra, "NEXTCLOUD_TALK_MAX_INBOUND_FILE_BYTES", "max_inbound_file_bytes", _DEFAULT_MAX_DOWNLOAD_BYTES))
        self.max_upload_bytes = int(_cfg(extra, "NEXTCLOUD_TALK_MAX_OUTBOUND_FILE_BYTES", "max_outbound_file_bytes", _DEFAULT_MAX_UPLOAD_BYTES))
        self.max_attachments_per_message = max(0, int(
            _cfg(extra, "NEXTCLOUD_TALK_MAX_ATTACHMENTS_PER_MESSAGE", "max_attachments_per_message", _DEFAULT_MAX_ATTACHMENTS_PER_MESSAGE)
        ))
        self.max_attachment_total_bytes = max(0, int(
            _cfg(extra, "NEXTCLOUD_TALK_MAX_ATTACHMENT_TOTAL_BYTES", "max_attachment_total_bytes", _DEFAULT_MAX_ATTACHMENT_TOTAL_BYTES)
        ))
        self.max_cache_bytes = max(0, int(
            _cfg(extra, "NEXTCLOUD_TALK_MAX_CACHE_BYTES", "max_cache_bytes", _DEFAULT_MAX_CACHE_BYTES)
        ))
        self.max_cache_files = max(0, int(
            _cfg(extra, "NEXTCLOUD_TALK_MAX_CACHE_FILES", "max_cache_files", _DEFAULT_MAX_CACHE_FILES)
        ))
        self.max_json_bytes = max(1, int(_cfg(extra, "NEXTCLOUD_TALK_MAX_JSON_BYTES", "max_json_bytes", _DEFAULT_MAX_JSON_BYTES)))
        self.max_body_bytes = max(1, int(_cfg(extra, "NEXTCLOUD_TALK_MAX_BODY_BYTES", "max_body_bytes", _DEFAULT_MAX_BODY_BYTES)))
        configured_poll_batch = max(1, int(
            _cfg(extra, "NEXTCLOUD_TALK_MAX_POLL_BATCH", "max_poll_batch", _DEFAULT_MAX_POLL_BATCH)
        ))
        self.max_poll_batch = _normalize_poll_batch(configured_poll_batch)
        if configured_poll_batch > _MAX_TALK_POLL_BATCH:
            logger.warning(
                "[nextcloud_talk] Poll batch %d exceeds Talk's protocol limit; clamping to %d",
                configured_poll_batch, _MAX_TALK_POLL_BATCH,
            )
        self.max_rooms = max(1, int(_cfg(extra, "NEXTCLOUD_TALK_MAX_ROOMS", "max_rooms", _DEFAULT_MAX_ROOMS)))
        configured_ack_rooms = _cfg(extra, "NEXTCLOUD_TALK_MAX_ACK_ROOMS", "max_ack_rooms", None)
        self.max_ack_rooms = max(
            self.max_rooms,
            int(configured_ack_rooms) if configured_ack_rooms is not None
            else self.max_rooms * _ACK_ROOM_LIMIT_MULTIPLIER,
        )
        self.max_backlog_messages = max(1, int(_cfg(extra, "NEXTCLOUD_TALK_MAX_BACKLOG_MESSAGES", "max_backlog_messages", _DEFAULT_MAX_BACKLOG_MESSAGES)))
        self.allow_public_share_fallback = _truthy(
            _cfg(extra, "NEXTCLOUD_TALK_ALLOW_PUBLIC_SHARE_FALLBACK", "allow_public_share_fallback", None),
            False,
        )
        self.auto_discover_rooms = _truthy(
            _cfg(extra, "NEXTCLOUD_TALK_AUTO_DISCOVER_ROOMS", "auto_discover_rooms", None),
            True,
        )
        self.discovery_interval = max(
            5,
            int(_cfg(extra, "NEXTCLOUD_TALK_DISCOVERY_INTERVAL", "discovery_interval", 30)),
        )

        # Resolve room tokens: support both legacy NEXTCLOUD_TALK_ROOM_TOKEN (single)
        # and new NEXTCLOUD_TALK_ROOM_TOKENS (comma-separated multi)
        extra_room_token = extra.get("room_token", "")
        extra_room_tokens = extra.get("room_tokens", "")
        self.room_tokens = _resolve_room_tokens()
        # Merge tokens from config 'extra' if provided
        extra_tokens = _parse_room_tokens(extra_room_token) + _parse_room_tokens(extra_room_tokens)
        seen = set(self.room_tokens)
        for t in extra_tokens:
            if t not in seen:
                seen.add(t)
                self.room_tokens.append(t)
        self._configured_room_tokens = list(self.room_tokens)
        self._discovered_room_tokens: Set[str] = set()
        self._room_types: Dict[str, int] = {}

        # Allow/deny gates go through the core's profile-isolated reader: under a multiplexed
        # secondary profile a scoped miss must return the default, never the launcher's allowlist.
        allowed_env = _gate_env("NEXTCLOUD_TALK_ALLOWED_USERS", "")
        allowed_cfg = extra.get("allow_from", [])
        if allowed_env:
            self.allowed_users = _csv_set(allowed_env)
        elif isinstance(allowed_cfg, list):
            self.allowed_users = {str(u).strip() for u in allowed_cfg if str(u).strip()}
        else:
            self.allowed_users = _csv_set(str(allowed_cfg or ""))
        group_allowed_cfg = extra.get("group_allow_from", [])
        if allowed_env:
            # Registry metadata gives core one platform-wide env allowlist,
            # including group traffic. Intake must enforce the same scope.
            self.group_allowed_users = _csv_set(allowed_env)
        elif isinstance(group_allowed_cfg, list):
            self.group_allowed_users = {
                str(user).strip() for user in group_allowed_cfg if str(user).strip()
            }
        else:
            self.group_allowed_users = _csv_set(str(group_allowed_cfg or ""))
        self.allow_all = _truthy(_gate_env("NEXTCLOUD_TALK_ALLOW_ALL_USERS", ""), False)

        max_len = _cfg(extra, "NEXTCLOUD_TALK_MAX_MESSAGE_LENGTH", "max_message_length", None)
        self.max_message_length = int(max_len or _DEFAULT_MAX_MESSAGE_LENGTH)

        self._client: Optional[NextcloudTalkClient] = None
        self._poll_task: Optional[asyncio.Task] = None
        self._initialization_tasks: Dict[str, asyncio.Task] = {}
        # Durable successful-message ledger plus process-local dispatch guards.
        self._last_message_ids: Dict[str, int] = {}
        self._ack_rooms: Dict[str, Dict[str, Any]] = {}
        self._inflight_message_ids: Dict[str, Set[int]] = {}
        self._initializing_rooms: Set[str] = set()
        self._ack_seen_counter = 0
        self._ack_persistence_blocked = False
        self._generation_counter = 0
        self._current_generations: Dict[tuple[str, int], int] = {}
        self._inflight_generations: Dict[tuple[str, int, int], MessageEvent] = {}
        self._generation_outcomes: Dict[tuple[str, int, int], Dict[str, Any]] = {}
        self._dispatch_generation_context: ContextVar[Optional[tuple[str, int, int]]] = (
            ContextVar(f"nextcloud_talk_dispatch_generation_{id(self)}", default=None)
        )
        self._completion_watchdogs: Dict[tuple[str, int, int], asyncio.Task] = {}
        self.processing_timeout = max(1.0, float(
            _cfg(extra, "NEXTCLOUD_TALK_PROCESSING_TIMEOUT", "processing_timeout", _DEFAULT_PROCESSING_TIMEOUT)
        ))
        self.ack_retention_count = max(32, int(
            _cfg(extra, "NEXTCLOUD_TALK_ACK_RETENTION_COUNT", "ack_retention_count", _DEFAULT_ACK_RETENTION_COUNT)
        ))
        configured_ack_overlap = int(
            _cfg(extra, "NEXTCLOUD_TALK_ACK_OVERLAP_IDS", "ack_overlap_ids", _DEFAULT_ACK_OVERLAP_IDS)
        )
        self.ack_overlap_ids = _normalize_ack_overlap(
            configured_ack_overlap, self.max_poll_batch
        )
        if configured_ack_overlap > self.ack_overlap_ids:
            logger.warning(
                "[nextcloud_talk] ACK overlap %d exceeds one poll page; clamping to %d "
                "to prevent newer-message starvation",
                configured_ack_overlap, self.ack_overlap_ids,
            )
        if self.ack_retention_count < self.ack_overlap_ids:
            logger.warning(
                "[nextcloud_talk] ACK retention %d is below overlap %d; clamping retention to %d",
                self.ack_retention_count, self.ack_overlap_ids, self.ack_overlap_ids,
            )
            self.ack_retention_count = self.ack_overlap_ids
        self.max_dispatch_attempts = max(1, int(_cfg(
            extra, "NEXTCLOUD_TALK_MAX_DISPATCH_ATTEMPTS", "max_dispatch_attempts",
            _DEFAULT_MAX_DISPATCH_ATTEMPTS,
        )))
        self.max_delivery_attempts = max(1, int(_cfg(
            extra, "NEXTCLOUD_TALK_MAX_DELIVERY_ATTEMPTS", "max_delivery_attempts",
            _DEFAULT_MAX_DELIVERY_ATTEMPTS,
        )))
        self.floor_settle_seconds = max(0.0, float(_cfg(
            extra, "NEXTCLOUD_TALK_FLOOR_SETTLE_SECONDS", "floor_settle_seconds",
            _DEFAULT_FLOOR_SETTLE_SECONDS,
        )))
        self._cursor_path = get_hermes_home() / "cache" / "nextcloud_talk" / "cursors.json"
        self._topics_path = get_hermes_home() / "cache" / "nextcloud_talk" / "thread_topics.json"
        self.thread_topics = _parse_thread_topics(extra.get("thread_topics"))
        self._topic_threads: Dict[tuple[str, str], int] = {}
        self._topics_task: Optional[asyncio.Task] = None
        self._last_discovery_at = 0.0
        self._running = False
        self._cursor_lock = threading.RLock()

    @property
    def name(self) -> str:
        return "Nextcloud Talk"

    def set_message_handler(self, handler) -> None:
        """Record inline control-handler outcomes hidden by Base's void API."""
        async def tracked(event: MessageEvent):
            metadata = getattr(event, "metadata", None)
            if isinstance(metadata, dict):
                metadata["nextcloud_talk_handler_state"] = "running"
                metadata["nextcloud_talk_processing_task"] = asyncio.current_task()
            try:
                result = await handler(event)
            except BaseException:
                if isinstance(metadata, dict):
                    metadata["nextcloud_talk_handler_state"] = "failure"
                raise
            if isinstance(metadata, dict):
                metadata["nextcloud_talk_handler_state"] = "success"
                try:
                    reply_text, _ttl = self._unwrap_ephemeral(result)
                except Exception:
                    reply_text = result
                metadata["nextcloud_talk_reply_required"] = bool(reply_text)
                text_required = bool(reply_text)
                media_expected = False
                if isinstance(reply_text, str):
                    try:
                        media_files, remaining = self.extract_media(reply_text)
                        images, remaining = self.extract_images(remaining)
                        local_files, remaining = self.extract_local_files(remaining)
                        media_expected = bool(media_files or images or local_files)
                        text_required = bool(remaining.strip())
                    except Exception:
                        # Fail closed: an unclassifiable non-empty reply must not
                        # be treated as text-only during disconnect reconciliation.
                        media_expected = bool(reply_text)
                metadata["nextcloud_talk_text_delivery_required"] = text_required
                metadata["nextcloud_talk_media_expected"] = media_expected
            return result

        base_setter = getattr(super(), "set_message_handler", None)
        if callable(base_setter):
            base_setter(tracked)
        else:
            self._message_handler = tracked

    async def _run_processing_hook(self, hook_name: str, *args: Any, **kwargs: Any) -> None:
        if hook_name == "on_processing_complete" and len(args) >= 2:
            event, outcome = args[0], args[1]
            metadata = getattr(event, "metadata", None)
            if isinstance(metadata, dict):
                metadata["nextcloud_talk_terminal_outcome"] = getattr(
                    outcome, "value", str(outcome)
                )
        await super()._run_processing_hook(hook_name, *args, **kwargs)

    async def _process_message_background(
        self, event: MessageEvent, session_key: str
    ) -> None:
        """Bind delivery tracking to the exact Talk generation being drained."""
        self._ensure_ack_runtime()
        generation_key = self._event_generation_key(event)
        if generation_key is None or not self._generation_is_current(generation_key):
            generation_key = None
        context_token = self._dispatch_generation_context.set(generation_key)
        try:
            await super()._process_message_background(event, session_key)
        finally:
            self._dispatch_generation_context.reset(context_token)

    def set_busy_session_handler(self, handler) -> None:
        """Track whether Base synchronously consumed or deferred this event."""
        if handler is None:
            tracked = None
        else:
            async def tracked(event: MessageEvent, session_key: str):
                metadata = getattr(event, "metadata", None)
                if isinstance(metadata, dict):
                    metadata["nextcloud_talk_busy_session_key"] = session_key
                try:
                    handled = await handler(event, session_key)
                except BaseException:
                    if isinstance(metadata, dict):
                        metadata["nextcloud_talk_busy_state"] = "failure"
                    raise
                if isinstance(metadata, dict):
                    metadata["nextcloud_talk_busy_state"] = (
                        "consumed" if handled else "declined"
                    )
                return handled

        base_setter = getattr(super(), "set_busy_session_handler", None)
        if callable(base_setter):
            base_setter(tracked)
        else:
            self._busy_session_handler = tracked

    def _reactions_enabled(self) -> bool:
        return getattr(self, "status_reactions", "off") in {"all", "groups"} and self._client is not None

    def _reactions_enabled_for(self, chat_id: str) -> bool:
        if not self._reactions_enabled():
            return False
        if getattr(self, "status_reactions", "off") == "groups":
            return getattr(self, "_room_types", {}).get(chat_id) not in {None, 1}
        return True

    async def _status_reaction(self, action: str, chat_id: Any, message_id: Any,
                               emoji: Optional[str]) -> None:
        """Best-effort, bounded reaction call that can never affect delivery or ACKs."""
        numeric_id = _strict_talk_message_id(
            int(message_id) if isinstance(message_id, str) and message_id.isascii()
            and message_id.isdigit() and len(message_id) <= 19 else message_id
        )
        client = self._client
        if (
            client is None or numeric_id is None
            or not isinstance(chat_id, str) or not chat_id or not emoji
            or not self._reactions_enabled_for(chat_id)
        ):
            return
        call = getattr(client, "add_reaction" if action == "add" else "remove_reaction", None)
        if not callable(call):
            return
        try:
            await asyncio.wait_for(call(chat_id, numeric_id, emoji), _STATUS_REACTION_TIMEOUT)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.debug(
                "[nextcloud_talk] Status reaction %s failed in room %s: %s",
                action, self._safe_room_token(chat_id), _safe_outward_error_text(exc),
            )

    async def _add_reaction(self, chat_id: str, message_id: str, emoji: str) -> None:
        await self._status_reaction("add", chat_id, message_id, emoji)

    async def _remove_reaction(self, chat_id: str, message_id: str) -> None:
        await self._status_reaction("remove", chat_id, message_id, self._ACK_EMOJI)

    def _spawn_presentation(self, coro: Any) -> asyncio.Task:
        """Detached presentation task: never awaited by a turn, cancelled on disconnect.

        Kept out of Base's ``_background_tasks`` so it can never be mistaken
        for a deferred handler of the message being dispatched.
        """
        tasks = getattr(self, "_presentation_tasks", None)
        if tasks is None:
            tasks = self._presentation_tasks = set()
        task = asyncio.create_task(coro)
        tasks.add(task)
        task.add_done_callback(tasks.discard)
        return task

    async def on_processing_start(self, event: MessageEvent) -> None:
        await super().on_processing_start(event)
        metadata = getattr(event, "metadata", None)
        if not isinstance(metadata, dict) or metadata.get("nextcloud_talk_message_id") is None:
            return  # only real Talk messages get a status reaction
        chat_id = getattr(event.source, "chat_id", None)
        if (not self._ACK_EMOJI or not isinstance(chat_id, str)
                or not self._reactions_enabled_for(chat_id)):
            return
        # Never awaited here: Base runs this hook before the handler, so an
        # awaited slow reaction would delay the reply itself.
        metadata["nextcloud_talk_reaction_task"] = self._spawn_presentation(
            self._add_reaction(chat_id, getattr(event, "message_id", None), self._ACK_EMOJI)
        )

    async def _swap_status_reaction(self, base_hook: Any, event: MessageEvent,
                                    outcome: ProcessingOutcome,
                                    start_task: Optional[asyncio.Task]) -> None:
        """Run Base's outcome reaction (❌ on failure) after any in-progress reaction call."""
        if start_task is not None and not start_task.done():
            # The add call is bounded by its own guard; waiting for it keeps
            # the later remove from overtaking it on the server.
            await asyncio.wait({start_task}, timeout=_STATUS_REACTION_TIMEOUT + 1.0)
        try:
            await base_hook(event, outcome)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.debug("[nextcloud_talk] Status reaction swap failed: %s",
                         _safe_outward_error_text(exc))

    def _queue_read_marker(self, room_token: str, message_id: int) -> None:
        """Mark the room read up to ``message_id`` in the background (monotonic, best effort)."""
        if not getattr(self, "read_marker", False) or self._client is None:
            return
        pending = self._read_marker_pending
        done = self._read_marker_done
        if message_id <= max(pending.get(room_token, 0), done.get(room_token, 0)):
            return
        pending[room_token] = message_id
        task = self._read_marker_tasks.get(room_token)
        if task is None or task.done():
            self._read_marker_tasks[room_token] = asyncio.create_task(
                self._flush_read_marker(room_token)
            )

    async def _flush_read_marker(self, room_token: str) -> None:
        pending = self._read_marker_pending
        done = self._read_marker_done
        while True:
            target = pending.get(room_token)
            client = self._client
            if target is None or client is None or target <= done.get(room_token, 0):
                pending.pop(room_token, None)
                return
            try:
                mark_read = getattr(client, "mark_read", None)
                if not callable(mark_read):
                    pending.pop(room_token, None)
                    return
                await asyncio.wait_for(mark_read(room_token, target), _READ_MARKER_TIMEOUT)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.debug(
                    "[nextcloud_talk] Read marker failed in room %s: %s",
                    self._safe_room_token(room_token), _safe_outward_error_text(exc),
                )
                if pending.get(room_token) == target:
                    pending.pop(room_token, None)
                    return
                continue  # a newer message arrived meanwhile: try that one
            done[room_token] = max(done.get(room_token, 0), target)

    async def _send_with_retry(self, *args, **kwargs) -> SendResult:
        """Capture Base's real inline delivery result for the active generation."""
        self._ensure_ack_runtime()
        generation_key = self._dispatch_generation_context.get()
        if generation_key is not None and self._generation_is_current(generation_key):
            state = self._generation_outcomes.setdefault(generation_key, {})
            state["delivery_attempted"] = True
        try:
            result = await super()._send_with_retry(*args, **kwargs)
        except BaseException as exc:
            if generation_key is not None and self._generation_is_current(generation_key):
                state = self._generation_outcomes.setdefault(generation_key, {})
                state["delivery_attempted"] = True
                state["delivery_exception"] = exc
                state["delivery_succeeded"] = False
            raise
        if generation_key is not None and self._generation_is_current(generation_key):
            state = self._generation_outcomes.setdefault(generation_key, {})
            state["delivery_result"] = result
            state["delivery_succeeded"] = (
                isinstance(result, SendResult) and result.success is True
            )
        return result

    async def send_multiple_images(self, chat_id: str, images: List[Any],
                                   metadata: Optional[Dict[str, Any]] = None,
                                   human_delay: float = 0.0) -> None:
        """Mirror Hermes image delivery while retaining failures for ACK gating."""
        self._ensure_ack_runtime()
        generation_key = self._dispatch_generation_context.get()
        for image_url, alt_text in images:
            if human_delay > 0:
                await asyncio.sleep(human_delay)
            failed = False
            try:
                if image_url.startswith("file://"):
                    result = await self.send_image_file(
                        chat_id=chat_id, image_path=parse.unquote(image_url[7:]),
                        caption=alt_text or None, metadata=metadata,
                    )
                elif self._is_animation_url(image_url):
                    result = await self.send_animation(
                        chat_id=chat_id, animation_url=image_url,
                        caption=alt_text or None, metadata=metadata,
                    )
                else:
                    result = await self.send_image(
                        chat_id=chat_id, image_url=image_url,
                        caption=alt_text or None, metadata=metadata,
                    )
                failed = not (isinstance(result, SendResult) and result.success is True)
            except BaseException:
                failed = True
            if failed and generation_key is not None and self._generation_is_current(generation_key):
                state = self._generation_outcomes.setdefault(generation_key, {})
                state["media_delivery_failed"] = True
                state["media_failure_count"] = min(
                    1024, int(state.get("media_failure_count", 0)) + 1
                )

    async def _notify_media_delivery_failure(
        self,
        chat_id: str,
        media_path: str,
        *,
        is_voice: bool = False,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        await super()._notify_media_delivery_failure(
            chat_id,
            media_path,
            is_voice=is_voice,
            metadata=metadata,
        )

    async def connect(self, **kwargs) -> bool:
        if self._poll_task is not None and not self._poll_task.done():
            return True  # an existing connection owns both poller and lease
        if getattr(self, "_cursor_writer_fd", None) is not None:
            return False  # a failed poller must be disconnected before reconnect
        if not validate_config(self.config):
            self._set_fatal_error(
                "config_missing",
                "NEXTCLOUD_TALK_URL, NEXTCLOUD_TALK_USERNAME, NEXTCLOUD_TALK_PASSWORD, "
                "are required; configure room tokens or enable automatic room discovery",
                retryable=False,
            )
            return False

        self._client = NextcloudTalkClient(
            self.base_url,
            self.username,
            self.password,
            timeout=max(self.poll_timeout + 10, 35),
            max_download_bytes=self.max_download_bytes,
            max_upload_bytes=self.max_upload_bytes,
            allow_public_share_fallback=self.allow_public_share_fallback,
            max_json_bytes=self.max_json_bytes,
            max_body_bytes=self.max_body_bytes,
            max_poll_batch=self.max_poll_batch,
            max_rooms=self.max_rooms,
            max_cache_bytes=self.max_cache_bytes,
            max_cache_files=self.max_cache_files,
        )
        self._client.explicit_read_marker = bool(self.read_marker)

        if len(self.room_tokens) > self.max_rooms:
            self._set_fatal_error(
                "config_limit",
                f"Configured room count exceeds NEXTCLOUD_TALK_MAX_ROOMS ({self.max_rooms})",
                retryable=False,
            )
            return False

        if not self.room_tokens and not self.auto_discover_rooms:
            self._set_fatal_error(
                "config_missing",
                "No room tokens configured. Set NEXTCLOUD_TALK_ROOM_TOKENS or NEXTCLOUD_TALK_ROOM_TOKEN.",
                retryable=False,
            )
            return False

        try:
            self._claim_cursor_writer()
            self._load_cursors()
            # Room metadata is useful even when DM auto-discovery is disabled:
            # explicit type-1 rooms must still be classified as DMs.
            await self._refresh_discovered_rooms(force=True, process_new_messages=False)
            self._running = True
            self._poll_task = asyncio.create_task(self._poll_loop())
            if self.thread_topics:
                previous_topics_task = getattr(self, "_topics_task", None)
                if previous_topics_task is not None and not previous_topics_task.done():
                    # A repeated connect() must not leave an orphaned setup task.
                    previous_topics_task.cancel()
                    await asyncio.gather(previous_topics_task, return_exceptions=True)
                self._load_topic_threads()
                self._topics_task = asyncio.create_task(self._ensure_thread_topics_bounded())
            rooms_safe = ", ".join(self._safe_room_token(t) for t in self.room_tokens)
            logger.info("[nextcloud_talk] Connected to rooms: %s", rooms_safe)
            self._mark_connected()
            return True
        except BaseException as exc:
            self._running = False
            topics_task = getattr(self, "_topics_task", None)
            if topics_task is not None:
                topics_task.cancel()
                await asyncio.gather(topics_task, return_exceptions=True)
                self._topics_task = None
            poll_task = getattr(self, "_poll_task", None)
            if poll_task is not None:
                poll_task.cancel()
                await asyncio.gather(poll_task, return_exceptions=True)
                self._poll_task = None
            self._release_cursor_writer()
            if not isinstance(exc, Exception):
                raise  # cancellation must not leave a stale writer lease
            safe_error = _safe_outward_error_text(exc)
            logger.error("[nextcloud_talk] Failed to connect: %s", safe_error)
            self._set_fatal_error("connect_failed", safe_error, retryable=True)
            return False

    async def disconnect(self) -> None:
        self._running = False
        if self._poll_task and not self._poll_task.done():
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
        self._poll_task = None
        topics_task = getattr(self, "_topics_task", None)
        if topics_task is not None and not topics_task.done():
            topics_task.cancel()
            await asyncio.gather(topics_task, return_exceptions=True)
        self._topics_task = None
        read_tasks = list(getattr(self, "_read_marker_tasks", {}).values())
        for task in read_tasks:
            task.cancel()
        if read_tasks:
            await asyncio.gather(*read_tasks, return_exceptions=True)
        getattr(self, "_read_marker_tasks", {}).clear()
        getattr(self, "_read_marker_pending", {}).clear()
        presentation_tasks = list(getattr(self, "_presentation_tasks", set()))
        for task in presentation_tasks:
            task.cancel()
        if presentation_tasks:
            await asyncio.gather(*presentation_tasks, return_exceptions=True)
        getattr(self, "_presentation_tasks", set()).clear()
        close_presentation = getattr(self._client, "close_presentation", None)
        if callable(close_presentation):
            close_presentation()
        initialization_tasks = list(getattr(self, "_initialization_tasks", {}).values())
        for task in initialization_tasks:
            task.cancel()
        if initialization_tasks:
            await asyncio.gather(*initialization_tasks, return_exceptions=True)
        self._initialization_tasks = {}
        self._ensure_ack_runtime()
        for key, event in list(self._inflight_generations.items()):
            if not self._generation_has_confirmed_success(event, key):
                continue
            try:
                await self.on_processing_complete(event, ProcessingOutcome.SUCCESS)
            except Exception as exc:
                logger.warning(
                    "[nextcloud_talk] Could not commit confirmed processing during "
                    "disconnect: %s",
                    _safe_outward_error_text(exc),
                )
        watchdogs = list(self._completion_watchdogs.values())
        for task in watchdogs:
            if not task.done():
                task.cancel()
        if watchdogs:
            await asyncio.gather(*watchdogs, return_exceptions=True)
        self._completion_watchdogs.clear()
        failure_value = getattr(
            ProcessingOutcome.FAILURE, "value", str(ProcessingOutcome.FAILURE)
        )
        for event in list(self._inflight_generations.values()):
            metadata = getattr(event, "metadata", None) or {}
            metadata["nextcloud_talk_processing_outcome"] = failure_value
            completion_event = metadata.get("nextcloud_talk_completion_event")
            if isinstance(completion_event, asyncio.Event):
                completion_event.set()
        self._current_generations.clear()
        self._inflight_generations.clear()
        self._generation_outcomes.clear()
        self._inflight_message_ids.clear()
        self._initializing_rooms.clear()
        self._release_cursor_writer()
        self._mark_disconnected()
        logger.info("[nextcloud_talk] Disconnected")

    def _topics_identity(self) -> str:
        return f"{str(self.base_url).rstrip('/')}|{str(self.username).lower()}"

    def _load_topic_threads(self) -> None:
        """Load bot-created topic thread IDs persisted by an earlier start."""
        self._topic_threads = {}
        path = getattr(self, "_topics_path", None)
        try:
            if path is None or not path.is_file() or path.stat().st_size > 256 * 1024:
                return
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            _log_cursor_cache_warning("load thread topics", exc)
            return
        if not isinstance(data, dict) or data.get("identity") != self._topics_identity():
            return
        rooms = data.get("rooms")
        if not isinstance(rooms, dict):
            return
        wanted = {(topic["room"], topic["title"].casefold()) for topic in self.thread_topics}
        for room, titles in rooms.items():
            if not isinstance(titles, dict):
                continue
            for title, thread_id in titles.items():
                key = (room, title) if isinstance(title, str) else None
                if key in wanted and _strict_talk_message_id(thread_id) is not None:
                    self._topic_threads[key] = thread_id

    def _persist_topic_threads(self) -> None:
        path = getattr(self, "_topics_path", None)
        if path is None:
            return
        rooms: Dict[str, Dict[str, int]] = {}
        for (room, title), thread_id in sorted(self._topic_threads.items()):
            rooms.setdefault(room, {})[title] = thread_id
        payload = {"version": 1, "identity": self._topics_identity(), "rooms": rooms}
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
            tmp.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
            os.replace(tmp, path)
        except OSError as exc:
            _log_cursor_cache_warning("persist thread topics", exc)

    def _topic_for_thread(self, room_token: str, thread_id: Optional[int]) -> Optional[Dict[str, Any]]:
        if thread_id is None:
            return None
        for topic in getattr(self, "thread_topics", None) or []:
            if topic["room"] != room_token:
                continue
            known = getattr(self, "_topic_threads", None) or {}
            if known.get((room_token, topic["title"].casefold())) == thread_id:
                return topic
        return None

    async def _ensure_thread_topics_bounded(self) -> None:
        try:
            await asyncio.wait_for(self._ensure_thread_topics(), timeout=_THREAD_TOPICS_TIMEOUT)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(
                "[nextcloud_talk] Could not prepare thread topics: %s",
                _safe_outward_error_text(exc),
            )

    async def _ensure_thread_topics(self) -> None:
        """Find or create one bot-owned Talk thread per configured topic."""
        if not self.thread_topics or self._client is None:
            return
        if not await self._client.supports_threads():
            logger.warning(
                "[nextcloud_talk] thread_topics configured, but this Talk server has no "
                "threads capability; topics were not created"
            )
            return
        polled = set(self.room_tokens)
        recent_by_room: Dict[str, List[dict]] = {}
        for topic in self.thread_topics:
            room, title = topic["room"], topic["title"]
            key = (room, title.casefold())
            if room not in polled:
                logger.warning(
                    "[nextcloud_talk] Topic %r skipped: room %s is not polled by this bot",
                    _safe_log_text(title, 80), self._safe_room_token(room),
                )
                continue
            try:
                known = self._topic_threads.get(key)
                if known is not None and await self._client.get_thread(room, known) is not None:
                    continue
                # Talk answers 404 for a deleted thread, but also when the room
                # itself is gone or the bot lost access.  Only a successful
                # thread listing for the room proves the room is reachable, so
                # the binding is dropped after that call, never before it.
                if room not in recent_by_room:
                    recent_by_room[room] = await self._client.list_recent_threads(
                        room, _THREAD_TOPICS_LOOKUP_LIMIT
                    )
                if known is not None:
                    self._topic_threads.pop(key, None)
                    self._persist_topic_threads()
                adopted = self._find_own_thread(recent_by_room[room], title)
                if adopted is None:
                    result = await self._client.send_message(
                        room, topic["message"], reference_id=str(uuid.uuid4()),
                        thread_title=title,
                    )
                    adopted = _strict_talk_message_id(
                        result.get("id") if isinstance(result, dict) else None
                    )
                    if adopted is None:
                        raise NextcloudTalkAPIError(
                            "Talk thread creation returned an invalid message ID",
                            category="protocol",
                        )
                    logger.info(
                        "[nextcloud_talk] Created topic %r in room %s",
                        _safe_log_text(title, 80), self._safe_room_token(room),
                    )
                self._topic_threads[key] = adopted
                # Persist each binding at once: a time-out or cancellation
                # later in the loop must not forget a thread already created.
                self._persist_topic_threads()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "[nextcloud_talk] Topic %r in room %s not prepared: %s",
                    _safe_log_text(title, 80), self._safe_room_token(room),
                    _safe_outward_error_text(exc),
                )

    def _find_own_thread(self, threads: List[dict], title: str) -> Optional[int]:
        """Adopt an existing thread only if this bot created it with the same title."""
        own = str(self.username).lower()
        for item in threads:
            thread = item.get("thread") if isinstance(item, dict) else None
            first = item.get("first") if isinstance(item, dict) else None
            if not isinstance(thread, dict) or not isinstance(first, dict):
                continue
            thread_id = _strict_talk_message_id(thread.get("id"))
            if (
                thread_id is not None
                and (_thread_title(thread.get("title")) or "").casefold() == title.casefold()
                and first.get("actorType") == "users"
                and isinstance(first.get("actorId"), str)
                and first["actorId"].lower() == own
                and first.get("id") == thread_id
            ):
                return thread_id
        return None

    def _load_cursors(self) -> None:
        self._ensure_ack_runtime()
        try:
            payload = json.loads(self._cursor_path.read_text(encoding="utf-8"))
            rooms = (
                payload.get("rooms")
                if isinstance(payload, dict) and payload.get("version") == _ACK_STATE_VERSION
                else None
            )
            if isinstance(rooms, dict):
                for token, state in rooms.items():
                    if not isinstance(token, str) or not isinstance(state, dict):
                        continue
                    try:
                        raw_floor = state.get("floor", 0)
                        floor = 0 if raw_floor == 0 and type(raw_floor) is int else _strict_talk_message_id(raw_floor)
                        raw_successful = state.get("successful", [])
                        if floor is None or not isinstance(raw_successful, list):
                            raise ValueError
                        successful_values = [_strict_talk_message_id(value) for value in raw_successful]
                        if any(value is None for value in successful_values):
                            raise ValueError
                        successful = set(successful_values)
                        raw_last_seen = state.get("last_seen", 0)
                        if type(raw_last_seen) is not int or raw_last_seen < 0:
                            raise ValueError
                        last_seen = raw_last_seen
                    except (TypeError, ValueError):
                        logger.warning("[nextcloud_talk] Ignoring invalid ACK state for room %s", self._safe_room_token(str(token)))
                        continue
                    self._ack_rooms[token] = {
                        "floor": floor,
                        "successful": successful,
                        "initialized": state.get("initialized") is True,
                        "last_seen": last_seen,
                        "active": state.get("active") is True,
                    }
                    self._ack_seen_counter = max(self._ack_seen_counter, last_seen)
                    legacy = payload.get("floor_mode") != _ACK_FLOOR_MODE
                    if self._normalize_ack_state(self._ack_rooms[token], legacy_overlap=legacy):
                        self._ack_state_dirty = True
                    if legacy:
                        self._ack_state_dirty = True
                    self._sync_legacy_cursor(token)
            elif (
                isinstance(payload, dict)
                and "version" not in payload
                and "rooms" not in payload
            ):
                configured = set(getattr(self, "_configured_room_tokens", []))
                for token, value in payload.items():
                    try:
                        if not isinstance(token, str):
                            raise ValueError
                        cursor = _strict_talk_message_id(value)
                        if cursor is None:
                            raise ValueError
                    except (TypeError, ValueError, OverflowError):
                        logger.warning(
                            "[nextcloud_talk] Ignoring invalid legacy cursor entry"
                        )
                        continue
                    self._ack_rooms[token] = {
                        "floor": cursor,
                        "successful": set(),
                        "initialized": False,
                        "last_seen": 0,
                        "active": token in configured,
                    }
                    self._sync_legacy_cursor(token)
            else:
                self._ack_persistence_blocked = True
                logger.warning(
                    "[nextcloud_talk] Cursor cache has unsupported ACK state version or shape; "
                    "preserving it unchanged until explicit recovery"
                )
        except FileNotFoundError:
            return
        except Exception as exc:
            _log_cursor_cache_warning("load", exc)

    def _ensure_ack_runtime(self) -> None:
        if not hasattr(self, "_ack_rooms"):
            # Compatibility for safely constructed legacy/test instances that
            # only carry the old scalar cursor map.
            self._ack_rooms = {}
            for token, value in getattr(self, "_last_message_ids", {}).items():
                message_id = _strict_talk_message_id(value)
                if isinstance(token, str) and message_id is not None:
                    self._ack_rooms[token] = {
                        "floor": message_id,
                        "successful": set(),
                        "initialized": False,
                    }
        if not hasattr(self, "_room_seen_ids"):
            self._room_seen_ids = {}
        if not hasattr(self, "_scan_resume"):
            self._scan_resume = {}
        if not hasattr(self, "_poll_page_limits"):
            self._poll_page_limits = {}
        if not hasattr(self, "_sweep_completed_from"):
            self._sweep_completed_from = {}
        if not hasattr(self, "_dispatch_attempts"):
            self._dispatch_attempts = {}
        if not hasattr(self, "_retry_not_before"):
            self._retry_not_before = {}
        if not hasattr(self, "_inflight_message_ids"):
            self._inflight_message_ids = {}
        if not hasattr(self, "_initializing_rooms"):
            self._initializing_rooms = set()
        if not hasattr(self, "_completion_watchdogs"):
            self._completion_watchdogs = {}
        if not hasattr(self, "_generation_counter"):
            self._generation_counter = 0
        if not hasattr(self, "_current_generations"):
            self._current_generations = {}
        if not hasattr(self, "_inflight_generations"):
            self._inflight_generations = {}
        if not hasattr(self, "_live_anchors"):
            # Newest Talk ID observed per room: the in-flight command poll
            # reads only messages after it instead of re-sweeping the floor.
            self._live_anchors = {}
        if not hasattr(self, "_user_stop_sessions"):
            # Sessions whose /stop, /new or /reset is being dispatched inline.
            self._user_stop_sessions = set()
        if not hasattr(self, "_settled_commands"):
            # Inline commands core already handled whose durable ACK failed:
            # never hand them to core again in this process (bounded).
            self._settled_commands = collections.OrderedDict()
        if not hasattr(self, "_user_stopped_tasks"):
            # Turn tasks that Hermes core cancelled for such a command.
            self._user_stopped_tasks = weakref.WeakSet()
        if not hasattr(self, "_generation_outcomes"):
            self._generation_outcomes = {}
        if not hasattr(self, "_dispatch_generation_context"):
            self._dispatch_generation_context = ContextVar(
                f"nextcloud_talk_dispatch_generation_{id(self)}", default=None
            )
        if not hasattr(self, "processing_timeout"):
            self.processing_timeout = _DEFAULT_PROCESSING_TIMEOUT
        if not hasattr(self, "_last_message_ids"):
            self._last_message_ids = {}
        if not hasattr(self, "ack_retention_count"):
            self.ack_retention_count = _DEFAULT_ACK_RETENTION_COUNT
        if not hasattr(self, "ack_overlap_ids"):
            self.ack_overlap_ids = _DEFAULT_ACK_OVERLAP_IDS
        if not hasattr(self, "max_poll_batch"):
            self.max_poll_batch = _DEFAULT_MAX_POLL_BATCH
        self.max_poll_batch = _normalize_poll_batch(self.max_poll_batch)
        self.ack_overlap_ids = _normalize_ack_overlap(
            self.ack_overlap_ids, self.max_poll_batch
        )
        self.ack_retention_count = max(
            32, int(self.ack_retention_count), self.ack_overlap_ids
        )
        if not hasattr(self, "_ack_state_dirty"):
            self._ack_state_dirty = False
        if not hasattr(self, "_ack_persistence_blocked"):
            self._ack_persistence_blocked = False
        if not hasattr(self, "_ack_seen_counter"):
            self._ack_seen_counter = max(
                (int(state.get("last_seen", 0)) for state in self._ack_rooms.values()),
                default=0,
            )
        if not hasattr(self, "max_rooms"):
            self.max_rooms = _DEFAULT_MAX_ROOMS
        if not hasattr(self, "max_ack_rooms"):
            self.max_ack_rooms = max(1, int(self.max_rooms)) * _ACK_ROOM_LIMIT_MULTIPLIER

    def _next_generation(self, room_token: str, numeric_id: int) -> tuple[str, int, int]:
        self._ensure_ack_runtime()
        self._generation_counter += 1
        generation = self._generation_counter
        self._current_generations[(room_token, numeric_id)] = generation
        return room_token, numeric_id, generation

    def _generation_is_current(self, key: tuple[str, int, int]) -> bool:
        room_token, numeric_id, generation = key
        return self._current_generations.get((room_token, numeric_id)) == generation

    @staticmethod
    def _event_generation_key(event: MessageEvent) -> Optional[tuple[str, int, int]]:
        metadata = getattr(event, "metadata", None) or {}
        room_token = metadata.get("nextcloud_talk_room_token")
        numeric_id = metadata.get("nextcloud_talk_message_id")
        generation = metadata.get("nextcloud_talk_generation")
        if (
            isinstance(room_token, str)
            and _strict_talk_message_id(numeric_id) is not None
            and type(generation) is int
            and generation > 0
        ):
            return room_token, numeric_id, generation
        return None

    def _touch_ack_room(self, room_token: str, *, active: Optional[bool] = None) -> None:
        state = self._ack_rooms.get(room_token)
        if state is None:
            return
        self._ack_seen_counter += 1
        state["last_seen"] = self._ack_seen_counter
        if active is not None:
            state["active"] = active

    def _protected_ack_rooms(self) -> Set[str]:
        configured = set(getattr(self, "_configured_room_tokens", []))
        discovered = set(getattr(self, "_discovered_room_tokens", set()))
        initializing = set(getattr(self, "_initializing_rooms", set()))
        inflight = {
            token for token, ids in getattr(self, "_inflight_message_ids", {}).items() if ids
        }
        incomplete = {
            token for token, state in self._ack_rooms.items()
            if state.get("initialized") is not True
        }
        active = {
            token for token, state in self._ack_rooms.items() if state.get("active") is True
        }
        return configured | discovered | initializing | inflight | incomplete | active

    def _prune_ack_rooms(self) -> None:
        """Bound inactive historical ledgers with deterministic last-seen LRU."""
        self._ensure_ack_runtime()
        limit = max(int(self.max_rooms), int(self.max_ack_rooms), 1)
        excess = len(self._ack_rooms) - limit
        if excess <= 0:
            return
        protected = self._protected_ack_rooms()
        candidates = sorted(
            (token for token in self._ack_rooms if token not in protected),
            key=lambda token: (int(self._ack_rooms[token].get("last_seen", 0)), token),
        )
        for token in candidates[:excess]:
            self._ack_rooms.pop(token, None)
            self._last_message_ids.pop(token, None)

    def _normalize_ack_state(self, state: Dict[str, Any], *, legacy_overlap: bool = False) -> bool:
        """Drop exact ACKs the floor already covers; bound the exact set.

        ``legacy_overlap`` applies the 0.1.11 rule (floor = highest ACK minus
        the overlap window) once, to cursor files that release wrote: that was
        the contract they ran under, so it neither replays nor skips anything
        0.1.11 would not have. The prefix ledger itself only advances the floor
        over IDs proven contiguous in the room's own history.
        """
        floor = int(state.get("floor", 0))
        successful = set(state.get("successful", set()))
        original_floor = floor
        original_successful = set(successful)
        if legacy_overlap and successful:
            floor = max(floor, max(successful) - self.ack_overlap_ids)
        successful = {value for value in successful if value > floor}
        if len(successful) > self.ack_retention_count:
            # A historic cursor may exceed a newly configured limit. Keep its
            # exact ACKs; capacity pressure cannot justify crossing a gap.
            logger.warning(
                "[nextcloud_talk] Exact ACK retention exceeded; new IDs held until "
                "the prefix settles or the limit is raised",
            )
        state["floor"] = floor
        state["successful"] = successful
        return floor != original_floor or successful != original_successful

    def _sync_legacy_cursor(self, room_token: str) -> None:
        state = self._ack_rooms.get(room_token, {"floor": 0, "successful": set()})
        successful = state.get("successful", set())
        self._last_message_ids[room_token] = max([int(state.get("floor", 0)), *successful])

    def _ack_payload(self) -> Dict[str, Any]:
        self._ensure_ack_runtime()
        self._prune_ack_rooms()
        return {
            "version": _ACK_STATE_VERSION,
            "floor_mode": _ACK_FLOOR_MODE,
            "rooms": {
                token: {
                    "floor": int(state.get("floor", 0)),
                    "successful": sorted(int(value) for value in state.get("successful", set())),
                    "initialized": state.get("initialized") is True,
                    "last_seen": max(0, int(state.get("last_seen", 0))),
                    "active": state.get("active") is True,
                }
                for token, state in sorted(self._ack_rooms.items())
            },
        }

    def _claim_cursor_writer(self) -> None:
        """Hold an exclusive OS lease for this profile's cursor across polling.

        The prefix relies on this writer's observed history. A second gateway
        with an independent volatile view cannot safely merge numeric floors.
        """
        if getattr(self, "_cursor_writer_fd", None) is not None:
            return
        if fcntl is None:
            raise CursorCacheError("OS cursor writer lease unavailable")
        path = self._cursor_path.with_name("cursors.writer.lock")
        path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(path, os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0), 0o600)
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                raise CursorCacheError("cursor writer lease is not a regular file")
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise CursorCacheError("another gateway owns the Talk cursor") from exc
            self._cursor_writer_fd = fd
        except BaseException:
            os.close(fd)
            raise

    def _release_cursor_writer(self) -> None:
        fd = getattr(self, "_cursor_writer_fd", None)
        if fd is not None:
            self._cursor_writer_fd = None
            os.close(fd)  # closing the handle releases flock even after cancellation

    def _persist_cursors(self) -> None:
        try:
            self._persist_cursors_atomic()
        except CursorCacheError as exc:
            _log_cursor_cache_warning("persist", exc)
            raise
        except Exception as exc:
            _log_cursor_cache_warning("persist", exc)
            raise CursorCacheError(
                f"cursor cache persist failed ({_error_class_name(exc)})"
            ) from exc

    def _persist_cursors_atomic(self) -> None:
        self._ensure_ack_runtime()
        if self._ack_persistence_blocked:
            raise CursorCacheError("cursor cache persistence blocked by unsupported ACK state")
        path = self._cursor_path
        path.parent.mkdir(parents=True, exist_ok=True)
        lock = getattr(self, "_cursor_lock", None)
        if lock is None:
            lock = self._cursor_lock = threading.RLock()
        temp_path: Optional[Path] = None
        backup_path: Optional[Path] = None
        with lock:
            try:
                fd, temp_name = tempfile.mkstemp(
                    prefix=".cursors-", suffix=".tmp", dir=path.parent
                )
                temp_path = Path(temp_name)
                os.fchmod(fd, 0o600)
                with os.fdopen(fd, "w", encoding="utf-8") as output:
                    json.dump(self._ack_payload(), output, sort_keys=True)
                    output.flush()
                    os.fsync(output.fileno())

                def fsync_directory() -> None:
                    directory_fd = os.open(path.parent, os.O_RDONLY)
                    try:
                        os.fsync(directory_fd)
                    finally:
                        os.close(directory_fd)

                # Fail before replacement if this filesystem cannot provide
                # directory durability, preserving the prior cursor unchanged.
                fsync_directory()
                if path.exists():
                    backup_path = Path(f"{temp_name}.previous")
                    os.link(path, backup_path)
                    fsync_directory()

                replaced = False
                try:
                    os.replace(temp_path, path)
                    temp_path = None
                    replaced = True
                    os.chmod(path, 0o600)
                    fsync_directory()
                except Exception:
                    if replaced:
                        if backup_path is not None:
                            try:
                                os.replace(backup_path, path)
                                backup_path = None
                            except Exception as rollback_exc:
                                # The newly replaced cursor is still the actual disk state.
                                # Keep the prior hard-link as a recovery artifact and make
                                # memory agree with the readable cursor before propagating.
                                try:
                                    payload = json.loads(path.read_text(encoding="utf-8"))
                                    rooms = payload.get("rooms")
                                    if payload.get("version") != _ACK_STATE_VERSION or not isinstance(rooms, dict):
                                        raise ValueError("invalid cursor payload")
                                    reconciled: Dict[str, Dict[str, Any]] = {}
                                    for token, state in rooms.items():
                                        raw_floor = state.get("floor", 0)
                                        floor = (
                                            0 if type(raw_floor) is int and raw_floor == 0
                                            else _strict_talk_message_id(raw_floor)
                                        )
                                        raw_successful = state.get("successful", [])
                                        if floor is None or not isinstance(raw_successful, list):
                                            raise ValueError("invalid cursor value")
                                        successful_values = [
                                            _strict_talk_message_id(value)
                                            for value in raw_successful
                                        ]
                                        if any(value is None for value in successful_values):
                                            raise ValueError("invalid cursor value")
                                        successful = set(successful_values)
                                        reconciled[str(token)] = {
                                            "floor": floor,
                                            "successful": successful,
                                            "initialized": state.get("initialized") is True,
                                        }
                                    self._ack_rooms = reconciled
                                    self._last_message_ids = {}
                                    for token in reconciled:
                                        self._sync_legacy_cursor(token)
                                except Exception:
                                    # The temp file was fsynced and atomically replaced; if
                                    # rereading fails, retain that same intended state in memory.
                                    pass
                                backup_path = None  # deliberately preserve recovery artifact
                                raise CursorCacheError(
                                    f"cursor cache rollback failed ({_error_class_name(rollback_exc)})",
                                    preserve_memory=True,
                                ) from rollback_exc
                        else:
                            path.unlink(missing_ok=True)
                    raise

                if backup_path is not None:
                    backup_path.unlink(missing_ok=True)
                    backup_path = None
            finally:
                if temp_path is not None:
                    temp_path.unlink(missing_ok=True)
                if backup_path is not None:
                    backup_path.unlink(missing_ok=True)

    def _is_recovery_barrier(self, room_token: str, numeric_id: int,
                             state: Dict[str, Any]) -> bool:
        """Allow only the oldest observed gap to resolve a full exact ledger."""
        successful = state.get("successful", set())
        if len(successful) >= self.ack_retention_count + self.max_poll_batch:
            return False  # bounded recovery headroom; operator can raise limit
        floor = int(state.get("floor", 0))
        seen = self._room_seen_ids.get(room_token, {})
        return numeric_id == next(
            (mid for mid in sorted(seen) if mid > floor and mid not in successful),
            None,
        )

    def _commit_cursor(self, room_token: str, numeric_id: Optional[int]) -> None:
        """Durably acknowledge one deterministic/successfully processed ID."""
        numeric_id = _strict_talk_message_id(numeric_id)
        if numeric_id is None:
            return
        self._ensure_ack_runtime()
        lock = getattr(self, "_cursor_lock", None)
        if lock is None:
            lock = self._cursor_lock = threading.RLock()
        with lock:
            room_existed = room_token in self._ack_rooms
            state = self._ack_rooms.setdefault(
                room_token, {
                    "floor": 0, "successful": set(), "initialized": False,
                    "last_seen": 0,
                    "active": room_token in set(getattr(self, "room_tokens", [])),
                }
            )
            floor = int(state.get("floor", 0))
            old_successful = set(state.get("successful", set()))
            if (numeric_id <= floor or numeric_id in old_successful) and room_existed:
                if self._ack_state_dirty:
                    self._persist_cursors()
                    self._ack_state_dirty = False
                return
            if (len(old_successful) >= self.ack_retention_count
                    and not self._is_recovery_barrier(room_token, numeric_id, state)):
                raise CursorCacheError("exact ACK capacity exhausted; raise retention limit")
            snapshot = {
                "floor": floor,
                "successful": set(old_successful),
                "initialized": state.get("initialized") is True,
                "last_seen": int(state.get("last_seen", 0)),
                "active": state.get("active") is True,
            }
            successful = set(old_successful)
            successful.add(numeric_id)
            state["floor"] = floor
            state["successful"] = successful
            self._advance_floor(room_token, state)
            self._normalize_ack_state(state)
            self._prune_retry_state(room_token, int(state["floor"]))
            self._dispatch_attempts.get(room_token, {}).pop(numeric_id, None)
            self._retry_not_before.get(room_token, {}).pop(numeric_id, None)
            self._touch_ack_room(
                room_token,
                active=room_token in set(getattr(self, "room_tokens", [])),
            )
            self._sync_legacy_cursor(room_token)
            try:
                self._persist_cursors()
                self._ack_state_dirty = False
            except Exception as exc:
                if isinstance(exc, CursorCacheError) and exc.preserve_memory:
                    raise
                if room_existed:
                    self._ack_rooms[room_token] = snapshot
                    self._sync_legacy_cursor(room_token)
                else:
                    self._ack_rooms.pop(room_token, None)
                    self._last_message_ids.pop(room_token, None)
                raise

    def _observe_room_ids(self, room_token: str, ids: Any) -> None:
        """Record message IDs the server has shown for this room (volatile).

        Talk IDs are global and sparse; a room's own history is the only proof
        that no unseen ID lies between two ACKed ones. The first-seen time lets
        the floor wait out IDs the database commits slightly out of order.
        """
        seen = self._room_seen_ids.setdefault(room_token, {})
        now = _ledger_clock()
        for value in ids:
            message_id = _strict_talk_message_id(value)
            if message_id is not None and message_id not in seen:
                seen[message_id] = now
        limit = max(4 * int(self.ack_retention_count), 1024)
        if len(seen) > limit:
            floor = int(self._ack_rooms.get(room_token, {}).get("floor", 0))
            for message_id in [m for m in seen if m <= floor]:
                del seen[message_id]
            if len(seen) > limit:
                state = self._ack_rooms.get(room_token, {})
                handled = state.get("successful", set())
                # The oldest *unresolved* observed ID is a floor barrier.
                # Evict only already-handled observations; their exact ACK is
                # still available for the next sweep.
                for message_id in sorted(seen):
                    if len(seen) <= limit:
                        break
                    if message_id in handled:
                        del seen[message_id]
                if len(seen) > limit:
                    # A page with too many unresolved IDs cannot be safely
                    # remembered. Stop intake instead of losing its barriers.
                    for message_id in sorted(seen, reverse=True)[: len(seen) - limit]:
                        del seen[message_id]
                    raise NextcloudTalkAPIError(
                        "room observation capacity exhausted; unresolved IDs retained",
                        category="overflow",
                    )

    def _advance_floor(self, room_token: str, state: Dict[str, Any]) -> bool:
        """Advance the floor over the contiguous ACKed prefix of observed history.

        A room ID above the floor joins it only when it is exactly ACKed (or
        given up after bounded retries) AND every lower observed ID already
        did, AND it has been visible for the settle window. An unacknowledged
        ID stops the walk, so it stays retryable; nothing unseen is skipped.
        """
        seen = self._room_seen_ids.get(room_token)
        if not seen:
            return False
        floor = int(state.get("floor", 0))
        successful = state.get("successful", set())
        settle = float(getattr(self, "floor_settle_seconds", _DEFAULT_FLOOR_SETTLE_SECONDS))
        if settle > 0:
            # An ID is settled only once a sweep that started from the floor
            # at least ``settle`` after the ID was first seen has completed:
            # that sweep would have shown any lower ID committed late.
            horizon = self._sweep_completed_from.get(room_token)
            if horizon is None:
                return False
            horizon -= settle
        else:
            horizon = float("inf")
        new_floor = floor
        for message_id in sorted(m for m in seen if m > floor):
            if message_id not in successful:
                break
            if seen[message_id] > horizon:
                break
            new_floor = message_id
        if new_floor == floor:
            return False
        state["floor"] = new_floor
        state["successful"] = {m for m in successful if m > new_floor}
        self._prune_retry_state(room_token, new_floor)
        return True

    def _prune_retry_state(self, room_token: str, floor: int) -> None:
        for name in ("_dispatch_attempts", "_retry_not_before"):
            per_room = getattr(self, name, {}).get(room_token)
            if per_room:
                for message_id in [m for m in per_room if m <= floor]:
                    del per_room[message_id]

    def _settle_room_floor(self, room_token: str) -> None:
        """Persist a floor advance that became possible only as time passed."""
        state = self._ack_rooms.get(room_token)
        if not state:
            return
        lock = getattr(self, "_cursor_lock", None)
        if lock is None:
            lock = self._cursor_lock = threading.RLock()
        with lock:
            snapshot = (int(state.get("floor", 0)), set(state.get("successful", set())))
            if not self._advance_floor(room_token, state):
                return
            self._sync_legacy_cursor(room_token)
            try:
                self._persist_cursors()
                self._ack_state_dirty = False
            except Exception:
                state["floor"], state["successful"] = snapshot
                self._sync_legacy_cursor(room_token)
                raise

    def _record_dispatch_failure(
        self, room_token: str, numeric_id: int, *, delivery: bool = False
    ) -> None:
        """Back off a failed turn; give it up after bounded attempts.

        Giving up acknowledges the ID with a warning: one message whose
        handler, tools or attachments keep failing must neither re-run the
        agent forever nor pin the floor. A failed reply delivery
        (``delivery=True``) gets the separate, larger
        ``max_delivery_attempts`` budget: the user has not been answered, so a
        short POST outage must not drop the message, yet a reply Talk will
        never accept must not re-run the agent forever.
        """
        attempts = self._dispatch_attempts.setdefault(room_token, {})
        count = attempts.get(numeric_id, 0) + 1
        attempts[numeric_id] = count
        if delivery:
            limit = int(getattr(self, "max_delivery_attempts", _DEFAULT_MAX_DELIVERY_ATTEMPTS))
        else:
            limit = int(getattr(self, "max_dispatch_attempts", _DEFAULT_MAX_DISPATCH_ATTEMPTS))
        if count >= limit:
            logger.warning(
                "[nextcloud_talk] Giving up message %s in room %s after %d failed attempts",
                numeric_id, self._safe_room_token(room_token), count,
            )
            attempts.pop(numeric_id, None)
            self._retry_not_before.get(room_token, {}).pop(numeric_id, None)
            self._commit_cursor(room_token, numeric_id)
            return
        backoff = tuple(getattr(self, "retry_backoff_seconds", _RETRY_BACKOFF_SECONDS)) or (0.0,)
        delay = float(backoff[min(count - 1, len(backoff) - 1)])
        if delay > 0:
            self._retry_not_before.setdefault(room_token, {})[numeric_id] = _ledger_clock() + delay

    def _drop_volatile_room_state(self, room_token: str) -> None:
        for name in ("_room_seen_ids", "_scan_resume", "_dispatch_attempts",
                     "_retry_not_before", "_poll_page_limits", "_sweep_completed_from",
                     "_read_marker_done", "_read_marker_pending"):
            getattr(self, name, {}).pop(room_token, None)

    def _is_own_presentation_event(self, msg: Any) -> bool:
        """Talk's bot-authored edit/delete events are safe to ignore on replay."""
        return (
            isinstance(msg, dict)
            and isinstance(msg.get("systemMessage"), str)
            and msg["systemMessage"] in {"message_edited", "message_deleted"}
            and isinstance(msg.get("actorId"), str)
            and msg["actorId"].lower() == self.username.lower()
        )

    def _retry_deferred(self, room_token: str, numeric_id: int) -> bool:
        due = self._retry_not_before.get(room_token, {}).get(numeric_id)
        return due is not None and _ledger_clock() < due

    def _is_acknowledged(self, room_token: str, numeric_id: int) -> bool:
        numeric_id = _strict_talk_message_id(numeric_id)
        if numeric_id is None:
            return False
        self._ensure_ack_runtime()
        state = self._ack_rooms.get(room_token)
        return bool(state and (numeric_id <= int(state.get("floor", 0)) or numeric_id in state.get("successful", set())))

    def _poll_anchor(self, room_token: str) -> Optional[int]:
        self._ensure_ack_runtime()
        state = self._ack_rooms.get(room_token)
        if not state:
            return None
        raw_floor = state.get("floor", 0)
        floor = 0 if type(raw_floor) is int and raw_floor == 0 else _strict_talk_message_id(raw_floor)
        if floor is None:
            return None
        # Rescan from the proven prefix; exact ACKs above it are deduplicated.
        # Scanning stays bounded because the floor follows the room's own
        # contiguous history (see _advance_floor).
        return floor

    def _is_room_initialized(self, room_token: str) -> bool:
        self._ensure_ack_runtime()
        return self._ack_rooms.get(room_token, {}).get("initialized") is True

    def _mark_room_initialized(self, room_token: str) -> None:
        """Persist startup completion independently from successful message IDs."""
        self._ensure_ack_runtime()
        state = self._ack_rooms.setdefault(
            room_token, {
                "floor": 0, "successful": set(), "initialized": False,
                "last_seen": 0,
                "active": room_token in set(getattr(self, "room_tokens", [])),
            }
        )
        if state.get("initialized") is True:
            return
        state["initialized"] = True
        self._touch_ack_room(
            room_token,
            active=room_token in set(getattr(self, "room_tokens", [])),
        )
        self._sync_legacy_cursor(room_token)
        try:
            self._persist_cursors()
            self._ack_state_dirty = False
        except BaseException:
            state["initialized"] = False
            self._sync_legacy_cursor(room_token)
            raise

    async def _fetch_initial_backlog(self, room_token: str, limit: Optional[int]) -> List[dict]:
        """Fetch newest bounded history (or all history) in backward pages."""
        assert self._client is not None
        remaining = limit
        hard_limit = getattr(self, "max_backlog_messages", _DEFAULT_MAX_BACKLOG_MESSAGES)
        cursor: Optional[int] = None
        collected: Dict[int, dict] = {}
        while remaining is None or remaining > 0:
            page_limit = 100 if remaining is None else min(100, remaining)
            page = await self._client.get_messages(
                room_token, last_known_id=cursor, look_into_future=False, timeout=0, limit=page_limit
            )
            valid: List[int] = []
            added = 0
            for message in page:
                if not isinstance(message, dict):
                    continue
                message_id = _strict_talk_message_id(message.get("id"))
                if message_id is None:
                    continue
                if message_id not in collected:
                    added += 1
                collected[message_id] = message
                valid.append(message_id)
                if len(collected) >= hard_limit:
                    logger.warning(
                        "[nextcloud_talk] Initial backlog reached hard limit %d for room %s; stopping",
                        hard_limit, self._safe_room_token(room_token),
                    )
                    break
            if len(collected) >= hard_limit:
                break
            if not valid:
                if page:
                    logger.warning(
                        "[nextcloud_talk] Initial backlog page made no ID progress for room %s",
                        self._safe_room_token(room_token),
                    )
                break
            next_cursor = min(valid)
            if cursor is not None and (next_cursor >= cursor or added == 0):
                logger.warning(
                    "[nextcloud_talk] Initial backlog repeated/no-progress page for room %s; stopping",
                    self._safe_room_token(room_token),
                )
                break
            cursor = next_cursor
            if remaining is not None:
                remaining -= added
            if len(page) < page_limit:
                break
        ordered_ids = sorted(collected)
        if limit is not None:
            ordered_ids = ordered_ids[-limit:]
        return [collected[message_id] for message_id in ordered_ids]

    def _establish_backlog_floor(self, room_token: str, backlog_ids: List[int]) -> None:
        """First sight of a room: history older than the backlog window is handled.

        Without this the prefix floor would start at 0 and the whole room
        history would be dispatched as new turns.
        """
        state = self._ack_rooms.get(room_token)
        if state is not None and (int(state.get("floor", 0)) or state.get("successful")):
            return
        if not backlog_ids:
            return
        lock = getattr(self, "_cursor_lock", None)
        if lock is None:
            lock = self._cursor_lock = threading.RLock()
        with lock:
            existed = state is not None
            state = self._ack_rooms.setdefault(room_token, {
                "floor": 0, "successful": set(), "initialized": False, "last_seen": 0,
                "active": room_token in set(getattr(self, "room_tokens", [])),
            })
            state["floor"] = max(int(state.get("floor", 0)), min(backlog_ids) - 1)
            self._touch_ack_room(room_token, active=room_token in set(getattr(self, "room_tokens", [])))
            self._sync_legacy_cursor(room_token)
            try:
                self._persist_cursors()
            except Exception:
                if existed:
                    state["floor"] = 0
                else:
                    self._ack_rooms.pop(room_token, None)
                    self._last_message_ids.pop(room_token, None)
                raise

    async def _initialize_room(self, room_token: str) -> None:
        self._ensure_ack_runtime()
        self._initializing_rooms.add(room_token)
        try:
            messages = await self._fetch_initial_backlog(room_token, self.initial_backlog_limit)
            if self.initial_backlog_limit == 0:
                # Explicit opt-out: establish a cursor at latest, with a visible warning.
                latest = await self._fetch_initial_backlog(room_token, 1)
                if latest:
                    latest_id = _strict_talk_message_id(latest[-1].get("id"))
                    if latest_id is not None:
                        self._establish_backlog_floor(room_token, [latest_id + 1])
                logger.warning("[nextcloud_talk] Initial backlog disabled for room %s", self._safe_room_token(room_token))
                self._mark_room_initialized(room_token)
                return
            backlog_ids = [
                message_id for message in messages
                if isinstance(message, dict)
                and (message_id := _strict_talk_message_id(message.get("id"))) is not None
            ]
            # The fetched window (bounded backlog, or the capped full backfill
            # of PROCESS_HISTORY) is everything this room will ever dispatch
            # from before its first sight; older history is handled.
            self._establish_backlog_floor(room_token, backlog_ids)
            self._observe_room_ids(room_token, backlog_ids)
            for message in messages:
                message_id = _strict_talk_message_id(message.get("id"))
                try:
                    await self._handle_talk_message(message, room_token, await_completion=True)
                except asyncio.CancelledError:
                    raise
                except _OutcomeAlreadyRecorded:
                    pass
                except Exception:
                    # Not accounted by on_processing_complete (e.g. the awaited
                    # turn timed out): count it so a backlog turn that never
                    # finishes is given up instead of blocking the room forever.
                    if message_id is not None:
                        self._record_dispatch_failure(room_token, message_id)
                    if message_id is None or not self._is_acknowledged(room_token, message_id):
                        raise
                if message_id is not None and not self._is_acknowledged(room_token, message_id):
                    # A removed/re-added room may still own an accepted handler.
                    # Deduplication alone is not proof that this backlog item finished.
                    raise RuntimeError("backlog message is not acknowledged")
            self._mark_room_initialized(room_token)
        finally:
            self._initializing_rooms.discard(room_token)

    def _schedule_room_initialization(self, room_token: str) -> None:
        task = self._initialization_tasks.get(room_token)
        if task is None or task.done():
            task = asyncio.create_task(self._initialize_room_background(room_token))
            self._initialization_tasks[room_token] = task
            def completed(done: asyncio.Task) -> None:
                if self._initialization_tasks.get(room_token) is done:
                    self._initialization_tasks.pop(room_token, None)
            task.add_done_callback(completed)

    async def _initialize_room_background(self, room_token: str) -> None:
        # Readiness precedes processing: startup may queue handlers until connect returns.
        backoff = 1.0
        while self._running and room_token in self.room_tokens:
            try:
                await self._initialize_room(room_token)
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "[nextcloud_talk] Initialize room %s failed: %s",
                    self._safe_room_token(room_token), _safe_outward_error_text(exc),
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    async def _poll_loop(self) -> None:
        assert self._client is not None
        backoff = 1.0
        while self._running:
            try:
                if self.auto_discover_rooms:
                    await self._refresh_discovered_rooms(process_new_messages=False)
                # Poll all rooms concurrently
                tasks = []
                for token in self.room_tokens:
                    if not self._is_room_initialized(token):
                        self._schedule_room_initialization(token)
                    else:
                        tasks.append(self._poll_room(token))
                await asyncio.gather(*tasks)
                backoff = 1.0
                # While a room has unacknowledged work, _poll_room intentionally
                # avoids another Talk request. Use a full-second breather rather
                # than spinning at the ordinary post-long-poll cadence.
                self._ensure_ack_runtime()
                in_flight = any(self._inflight_message_ids.values())
                await asyncio.sleep(1.0 if in_flight else 0.2)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "[nextcloud_talk] Poll loop error: %s",
                    _safe_outward_error_text(exc),
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    async def _refresh_discovered_rooms(
        self, *, force: bool = False, process_new_messages: bool = True
    ) -> None:
        if not self._client:
            return
        now = time.monotonic()
        if not force and now - self._last_discovery_at < self.discovery_interval:
            return

        conversations = await self._client.list_conversations()
        self._last_discovery_at = now
        metadata = {room["token"]: room["type"] for room in conversations}
        self._room_types.update(metadata)
        configured = list(self._configured_room_tokens)
        configured_set = set(configured)
        discovered = [room["token"] for room in conversations if room["type"] == 1] if self.auto_discover_rooms else []
        if len(configured) + len([token for token in discovered if token not in configured_set]) > self.max_rooms:
            raise NextcloudTalkAPIError("combined room count exceeds configured limit", category="overflow")
        discovered_set = set(discovered) - configured_set
        previous_discovered = set(self._discovered_room_tokens)
        new_tokens = [token for token in discovered if token in discovered_set and token not in previous_discovered]
        removed_tokens = previous_discovered - discovered_set

        # Stop intake for departed rooms before admitting replacements. Already
        # accepted handlers retain their generation ownership until completion.
        obsolete_tasks = []
        for token in removed_tokens:
            task = getattr(self, "_initialization_tasks", {}).get(token)
            if task is not None:
                task.cancel()
                obsolete_tasks.append(task)
        if obsolete_tasks:
            await asyncio.gather(*obsolete_tasks, return_exceptions=True)

        self._discovered_room_tokens.difference_update(removed_tokens)
        self.room_tokens = configured + [
            token for token in discovered
            if token not in configured_set and token in self._discovered_room_tokens
        ]
        ack_activity_changed = False
        for token in configured:
            state = self._ack_rooms.get(token)
            if state is not None and state.get("active") is not True:
                self._touch_ack_room(token, active=True)
                ack_activity_changed = True
        for token in removed_tokens:
            self._room_types.pop(token, None)
            self._drop_volatile_room_state(token)
            state = self._ack_rooms.get(token)
            if state is not None and state.get("active") is not False:
                self._touch_ack_room(token, active=False)
                ack_activity_changed = True
        for token in new_tokens:
            if process_new_messages and not self._is_room_initialized(token):
                await self._initialize_room(token)
            self._discovered_room_tokens.add(token)
            if token not in self.room_tokens:
                self.room_tokens.append(token)
            state = self._ack_rooms.get(token)
            if state is not None:
                self._touch_ack_room(token, active=True)
                ack_activity_changed = True
            logger.info("[nextcloud_talk] Auto-discovered DM %s", self._safe_room_token(token))
        before_prune = len(self._ack_rooms)
        self._prune_ack_rooms()
        if ack_activity_changed or len(self._ack_rooms) != before_prune:
            self._persist_cursors()

    async def _poll_room(self, room_token: str) -> None:
        assert self._client is not None
        try:
            self._ensure_ack_runtime()
            room_inflight = bool(self._inflight_message_ids.get(room_token))
            if room_inflight and not self._room_has_pending_clarify(room_token):
                if self._room_has_busy_session(room_token):
                    await self._poll_inflight_commands(room_token)
                return
            max_batch = getattr(self, "max_poll_batch", _DEFAULT_MAX_POLL_BATCH)
            poll_anchor = self._poll_anchor(room_token)

            def normalize_batch(batch: Any) -> tuple[Dict[int, dict], int, List[int]]:
                if not isinstance(batch, list):
                    raise NextcloudTalkAPIError("chat response data is not a list", category="protocol")
                if len(batch) > max_batch:
                    raise NextcloudTalkAPIError("chat response exceeds poll batch limit", category="overflow")
                normalized_batch: Dict[int, dict] = {}
                malformed_count = 0
                valid_ids: List[int] = []
                observed_ids: List[int] = []
                for msg in batch:
                    if not isinstance(msg, dict):
                        malformed_count += 1
                        continue
                    message_id = _strict_talk_message_id(msg.get("id"))
                    if message_id is None:
                        malformed_count += 1
                        continue
                    valid_ids.append(message_id)
                    if self._is_own_presentation_event(msg):
                        # These are re-identifiable after a restart and never
                        # need a volatile observation or a durable exact ACK.
                        continue
                    observed_ids.append(message_id)
                    if (not self._is_acknowledged(room_token, message_id)
                            and not self._retry_deferred(room_token, message_id)):
                        normalized_batch[message_id] = msg
                self._observe_room_ids(room_token, observed_ids)
                return normalized_batch, malformed_count, valid_ids

            cycle_dispatched: set[int] = set()

            async def dispatch(batch: Dict[int, dict]) -> None:
                for message_id in sorted(batch):
                    # One dispatch per ID per poll cycle, even if a page repeats.
                    if message_id in cycle_dispatched:
                        continue
                    state = self._ack_rooms.get(room_token, {})
                    if len(state.get("successful", ())) >= self.ack_retention_count:
                        self._settle_room_floor(room_token)
                        state = self._ack_rooms.get(room_token, {})
                        if (len(state.get("successful", ())) >= self.ack_retention_count
                                and not self._is_recovery_barrier(room_token, message_id, state)):
                            logger.warning(
                                "[nextcloud_talk] Room %s intake held: exact ACK capacity reached",
                                self._safe_room_token(room_token),
                            )
                            return
                    cycle_dispatched.add(message_id)
                    try:
                        await self._handle_talk_message(batch[message_id], room_token)
                    except asyncio.CancelledError:
                        raise
                    except _OutcomeAlreadyRecorded:
                        continue
                    except Exception as exc:
                        # e.g. an attachment download that keeps failing. It
                        # must not abort the page (newer messages would starve)
                        # nor retry forever; count it like a failed turn.
                        logger.warning(
                            "[nextcloud_talk] Message %s in room %s failed before dispatch: %s",
                            message_id, self._safe_room_token(room_token),
                            _safe_outward_error_text(exc),
                        )
                        self._record_dispatch_failure(room_token, message_id)

            async def fetch(last_known_id: Optional[int], timeout: int) -> List[dict]:
                # A page larger than max_json_bytes (a few huge messages) is
                # retried with a smaller page instead of failing the room.
                limit = self._poll_page_limits.get(room_token, max_batch)
                while True:
                    try:
                        page = await self._client.get_messages(
                            room_token, last_known_id=last_known_id, look_into_future=True,
                            timeout=timeout, limit=limit,
                        )
                    except NextcloudTalkAPIError as exc:
                        if exc.category != "overflow" or limit <= 1:
                            raise
                        limit = max(1, limit // 2)
                        self._poll_page_limits[room_token] = limit
                        logger.warning(
                            "[nextcloud_talk] Poll page too large in room %s; retrying with %d messages",
                            self._safe_room_token(room_token), limit,
                        )
                        continue
                    if limit < max_batch and len(page) < limit:
                        # Recover the normal page size once past the large messages.
                        self._poll_page_limits.pop(room_token, None)
                    return page

            # Catch up from the proven prefix without waiting: acknowledged or
            # backed-off IDs above the floor are skipped, and full pages are
            # followed (bounded) so newer messages are never starved behind them.
            # A sweep longer than one cycle's page budget resumes where it
            # stopped (volatile); only a completed sweep restarts at the floor,
            # so a backed-off lower ID is revisited and nothing is starved.
            resume = self._scan_resume.get(room_token)
            if resume is None or (poll_anchor or 0) >= resume[0]:
                anchor = poll_anchor
                sweep_started = _ledger_clock()
            else:
                anchor, sweep_started = resume
            highest_seen = anchor
            dispatched = False
            malformed_total = 0
            pages = 0
            for pages in range(1, 33):
                messages = await fetch(anchor, 0)
                normalized, malformed, valid_ids = normalize_batch(messages)
                malformed_total += malformed
                if valid_ids:
                    highest_seen = max(valid_ids) if highest_seen is None else max(highest_seen, max(valid_ids))
                if valid_ids and anchor is not None and max(valid_ids) <= anchor:
                    logger.warning("[nextcloud_talk] Poll page made no progress in room %s",
                                   self._safe_room_token(room_token))
                    self._scan_resume.pop(room_token, None)
                    break
                if normalized:
                    # Dispatch and keep sweeping: ending the sweep here would
                    # mean a room with steady traffic never completes one, so
                    # its floor would never settle.
                    await dispatch(normalized)
                    dispatched = True
                if not valid_ids:
                    # Only an empty page proves the sweep ended: Talk drops
                    # invisible messages after applying the page limit, so a
                    # short page can still have newer messages behind it.
                    self._scan_resume.pop(room_token, None)
                    self._sweep_completed_from[room_token] = sweep_started
                    break
                anchor = max(valid_ids)
            else:
                self._scan_resume[room_token] = (anchor, sweep_started)
            if room_token not in self._scan_resume:
                # Caught up (sweep completed or dispatched to its end): long-poll
                # for genuinely new messages from the newest ID seen, so an idle
                # room waits instead of rescanning.
                messages = await fetch(highest_seen, self.poll_timeout)
                normalized, malformed, live_ids = normalize_batch(messages)
                if live_ids:
                    highest_seen = max([highest_seen or 0, *live_ids])
                malformed_total += malformed
                await dispatch(normalized)
            if malformed_total:
                logger.warning(
                    "[nextcloud_talk] Ignored %d poll message(s) with malformed/missing IDs in room %s",
                    malformed_total, self._safe_room_token(room_token),
                )
            if highest_seen is not None:
                self._note_live_anchor(room_token, highest_seen)
            self._settle_room_floor(room_token)
            if pages > 1 or len(messages) > 5:
                await asyncio.sleep(0.5)
        except NextcloudTalkAPIError as exc:
            if exc.status_code == 304:
                pass  # No new messages — normal
            else:
                logger.warning("[nextcloud_talk] Poll room %s failed: %s",
                               self._safe_room_token(room_token),
                               _safe_outward_error_text(exc))
        except Exception as exc:
            logger.warning("[nextcloud_talk] Poll room %s failed: %s",
                           self._safe_room_token(room_token),
                           _safe_outward_error_text(exc))

    def _event_is_deferred(self, event: MessageEvent) -> bool:
        if any(candidate is event for candidate in getattr(self, "_pending_messages", {}).values()):
            return True
        for state in getattr(self, "_text_debounce", {}).values():
            if getattr(state, "event", None) is event:
                return True
        metadata = getattr(event, "metadata", None) or {}
        session_key = metadata.get("nextcloud_talk_busy_session_key")
        runner = getattr(self, "gateway_runner", None)
        peek = getattr(runner, "_peek_session_state", None)
        if session_key and callable(peek):
            try:
                session_state = peek(session_key)
                queued = getattr(getattr(session_state, "conversation", None), "queued_events", [])
                if any(candidate is event for candidate in queued):
                    return True
            except Exception:
                pass
        return False

    def _room_has_pending_clarify(self, room_token: str) -> bool:
        """Return whether an in-flight turn is blocked on a clarify response."""
        return any(
            key[0] == room_token
            and self._source_has_pending_clarify(getattr(event, "source", None))
            for key, event in self._inflight_generations.items()
        )

    def _note_live_anchor(self, room_token: str, message_id: Any) -> None:
        numeric = _strict_talk_message_id(message_id)
        if numeric is None:
            return
        anchors = self._live_anchors
        if numeric > anchors.get(room_token, 0):
            anchors[room_token] = numeric

    async def _poll_inflight_commands(self, room_token: str) -> None:
        """Read new room messages during a running turn; pass only commands.

        Hermes core expects ``/approve``, ``/deny``, ``/stop`` and the other
        slash commands to reach a busy session at once.  This poll reads only
        messages after the newest ID already observed (no floor re-sweep), and
        hands a message to the normal intake only when it is a slash command
        the turn's owner may send now.  Every other message is left untouched
        (no ACK, no download): the normal sweep handles it after the turn.
        """
        assert self._client is not None
        anchor = self._live_anchors.get(room_token)
        inflight_ids = self._inflight_message_ids.get(room_token) or set()
        if inflight_ids:
            anchor = max([anchor or 0, *inflight_ids])
        if anchor is None:
            return
        max_batch = getattr(self, "max_poll_batch", _DEFAULT_MAX_POLL_BATCH)
        timeout = min(self.poll_timeout, _INFLIGHT_POLL_TIMEOUT)
        # Bounded catch-up: a burst of other posts must not keep the owner's
        # command many cycles away. Later pages do not wait.
        for _page in range(_INFLIGHT_MAX_PAGES):
            page, limit = await self._fetch_inflight_page(
                room_token, anchor, timeout, max_batch
            )
            timeout = 0
            highest = await self._dispatch_inflight_page(room_token, anchor, page)
            if highest is None or highest <= anchor or len(page) < limit:
                return
            anchor = highest

    async def _fetch_inflight_page(
        self, room_token: str, anchor: int, timeout: int, max_batch: int
    ) -> tuple:
        """One in-flight page; an oversized page is retried smaller (as the sweep does)."""
        limit = min(self._poll_page_limits.get(room_token, max_batch), max_batch)
        while True:
            try:
                page = await self._client.get_messages(
                    room_token, last_known_id=anchor, look_into_future=True,
                    timeout=timeout, limit=limit,
                )
            except NextcloudTalkAPIError as exc:
                if exc.category != "overflow" or limit <= 1:
                    raise
                limit = max(1, limit // 2)
                self._poll_page_limits[room_token] = limit
                logger.warning(
                    "[nextcloud_talk] Poll page too large in room %s; retrying with %d messages",
                    self._safe_room_token(room_token), limit,
                )
                continue
            if not isinstance(page, list):
                raise NextcloudTalkAPIError("chat response data is not a list", category="protocol")
            if len(page) > max_batch:
                raise NextcloudTalkAPIError("chat response exceeds poll batch limit", category="overflow")
            if limit < max_batch and len(page) < limit:
                # Past the large messages: recover the normal page size.
                self._poll_page_limits.pop(room_token, None)
            return page, limit

    async def _dispatch_inflight_page(
        self, room_token: str, anchor: int, page: List[Any]
    ) -> Optional[int]:
        """Pass this page's busy-session commands to intake; return its newest ID."""
        highest: Optional[int] = None
        candidates: Dict[int, dict] = {}
        for msg in page:
            if not isinstance(msg, dict):
                continue
            message_id = _strict_talk_message_id(msg.get("id"))
            if message_id is None or message_id <= anchor:
                continue
            highest = message_id if highest is None else max(highest, message_id)
            self._note_live_anchor(room_token, message_id)
            if self._is_acknowledged(room_token, message_id):
                continue
            if self._inflight_command_candidate(msg, room_token, message_id):
                candidates[message_id] = msg
        for message_id in sorted(candidates):
            try:
                await self._handle_talk_message(candidates[message_id], room_token)
            except asyncio.CancelledError:
                raise
            except _OutcomeAlreadyRecorded:
                continue
            except Exception as exc:
                logger.warning(
                    "[nextcloud_talk] Message %s in room %s failed before dispatch: %s",
                    message_id, self._safe_room_token(room_token),
                    _safe_outward_error_text(exc),
                )
                self._record_dispatch_failure(room_token, message_id)
        return highest

    def _inflight_command_candidate(
        self, msg: Dict[str, Any], room_token: str, numeric_id: int
    ) -> bool:
        """Side-effect-free check: may this message pass the busy-room hold?"""
        actor_id = msg.get("actorId")
        if not isinstance(actor_id, str) or not actor_id.strip():
            return False
        actor_name = str(msg.get("actorDisplayName") or actor_id)
        if str(msg.get("actorType") or "").lower() not in {"users", "guests"}:
            return False
        if msg.get("systemMessage"):
            return False
        username = self.username.lower()
        if actor_id.lower() == username or actor_name.lower() == username:
            return False
        if self._is_own_presentation_event(msg):
            return False
        text = msg.get("message")
        if not isinstance(text, str) or not text.strip():
            return False
        params = msg.get("messageParameters", {})
        if params is None or params == []:
            params = {}
        if not isinstance(params, dict) or not _valid_message_parameters(params):
            return False
        chat_type = "dm" if self._room_types.get(room_token) == 1 else "group"
        if not self._is_allowed(actor_id, actor_name, chat_type=chat_type):
            return False
        thread_fields: Dict[str, Any] = {}
        thread_id = _inbound_thread_id(msg, numeric_id)
        if thread_id is not None:
            thread_fields["thread_id"] = str(thread_id)
        source = self.build_source(
            chat_id=room_token,
            chat_name=f"Nextcloud Talk {self._safe_room_token(room_token)}",
            chat_type=chat_type,
            user_id=actor_id,
            user_name=actor_name,
            **thread_fields,
        )
        return self._room_lock_bypass_command(source, text.strip(), params) is not None

    def _room_has_busy_session(self, room_token: str) -> bool:
        """Return whether an in-flight event of this room holds the busy guard."""
        active = getattr(self, "_active_sessions", {}) or {}
        if not active:
            return False
        for key, event in list(self._inflight_generations.items()):
            if key[0] != room_token:
                continue
            try:
                session_key = self._source_session_key(getattr(event, "source", None))
            except Exception:
                continue
            if session_key and session_key in active:
                return True
        return False

    def _room_lock_bypass_command(
        self, source: Any, text: str, file_refs: Dict[str, Any]
    ) -> Optional[str]:
        """Return the canonical slash command that may pass the room lock.

        Mirrors Hermes core's busy-session rule (every recognized command is
        dispatched mid-run, never queued) for the session that is actually
        busy, and only for the participant who owns the running turn.  Messages
        with attachments never pass, so no download happens for held input.
        """
        if source is None or not isinstance(text, str):
            return None
        if any(
            isinstance(param, dict) and param.get("type") == "file"
            for param in file_refs.values()
        ):
            return None
        stripped = self._strip_bot_mention(text)
        if not stripped or not stripped.startswith("/"):
            return None
        try:
            from hermes_cli.commands import resolve_command, should_bypass_active_session
        except Exception:
            return None
        try:
            command = MessageEvent(text=stripped, source=source).get_command()
        except Exception:
            return None
        if not command or not should_bypass_active_session(command):
            return None
        resolved = resolve_command(command)
        canonical = getattr(resolved, "name", None) or command
        try:
            session_key = self._source_session_key(source)
        except Exception:
            return None
        if not session_key or session_key not in (getattr(self, "_active_sessions", {}) or {}):
            return None
        if not self._source_matches_pending_participant(source, session_key):
            return None
        return canonical

    def _source_session_key(self, source: Any) -> Optional[str]:
        """Build the Hermes session key for one Talk message source."""
        if source is None:
            return None
        from gateway.session import build_session_key

        options = {
            "group_sessions_per_user": self.config.extra.get(
                "group_sessions_per_user", True
            ),
            "thread_sessions_per_user": self.config.extra.get(
                "thread_sessions_per_user", False
            ),
        }
        profile_resolver = getattr(self, "_session_key_profile", None)
        if (
            "profile" in inspect.signature(build_session_key).parameters
            and callable(profile_resolver)
        ):
            options["profile"] = profile_resolver(source)
        return build_session_key(source, **options)

    def _source_matches_pending_participant(
        self, source: Any, session_key: str
    ) -> bool:
        """Bind a shared group session's clarify to its oldest in-flight actor."""
        if getattr(source, "chat_type", "") != "group":
            return True
        candidate_user_id = getattr(source, "user_id", None)
        if not candidate_user_id:
            return False
        for event in self._inflight_generations.values():
            owner_source = getattr(event, "source", None)
            if owner_source is None:
                continue
            try:
                owner_key = self._source_session_key(owner_source)
            except Exception:
                return False
            if owner_key != session_key:
                continue
            owner_user_id = getattr(owner_source, "user_id", None)
            return bool(owner_user_id) and owner_user_id == candidate_user_id
        return False

    def _source_has_pending_clarify(
        self, source: Any, *, require_participant_owner: bool = False
    ) -> bool:
        """Match an unresolved clarify to one exact Hermes session source."""
        if source is None:
            return False
        try:
            from tools import clarify_gateway
        except Exception:
            return False
        try:
            session_key = self._source_session_key(source)
        except Exception:
            return False
        if not session_key:
            return False
        entry = clarify_gateway.get_pending_for_session(
            session_key, include_choice_prompts=True
        )
        if entry is None or entry.event.is_set():
            return False
        return not require_participant_owner or self._source_matches_pending_participant(
            source, session_key
        )

    def _finish_without_ack(
        self,
        event: MessageEvent,
        room_token: str,
        numeric_id: int,
        generation: Optional[int] = None,
    ) -> bool:
        key = self._event_generation_key(event)
        if key is None and isinstance(generation, int):
            key = (room_token, numeric_id, generation)
        if key is None or not self._generation_is_current(key):
            return False
        watchdog = self._completion_watchdogs.pop(key, None)
        if (
            watchdog is not None
            and watchdog is not asyncio.current_task()
            and not watchdog.done()
        ):
            watchdog.cancel()
        self._current_generations.pop((room_token, numeric_id), None)
        self._inflight_generations.pop(key, None)
        self._generation_outcomes.pop(key, None)
        self._inflight_message_ids.get(room_token, set()).discard(numeric_id)
        metadata = getattr(event, "metadata", None) or {}
        metadata.pop("nextcloud_talk_processing_task", None)
        metadata["nextcloud_talk_processing_outcome"] = getattr(
            ProcessingOutcome.FAILURE, "value", str(ProcessingOutcome.FAILURE)
        )
        completion_event = metadata.get("nextcloud_talk_completion_event")
        if isinstance(completion_event, asyncio.Event):
            completion_event.set()
        return True

    def _generation_has_confirmed_success(
        self, event: MessageEvent, key: tuple[str, int, int]
    ) -> bool:
        """Return true only when handler and required delivery both completed."""
        if not self._generation_is_current(key):
            return False
        metadata = getattr(event, "metadata", None) or {}
        if metadata.get("nextcloud_talk_handler_state") != "success":
            return False
        state = self._generation_outcomes.get(key, {})
        if state.get("media_delivery_failed") is True:
            return False
        success_value = getattr(
            ProcessingOutcome.SUCCESS, "value", str(ProcessingOutcome.SUCCESS)
        )
        if (
            metadata.get("nextcloud_talk_media_expected") is True
            and metadata.get("nextcloud_talk_terminal_outcome") != success_value
        ):
            return False
        return not (
            metadata.get(
                "nextcloud_talk_text_delivery_required",
                metadata.get("nextcloud_talk_reply_required") is True,
            ) is True
            and state.get("delivery_succeeded") is not True
        )

    async def _completion_watchdog(
        self, event: MessageEvent, room_token: str, numeric_id: int, generation: int
    ) -> None:
        key = (room_token, numeric_id, generation)
        try:
            while self._generation_is_current(key):
                await asyncio.sleep(self.processing_timeout)
                if not self._generation_is_current(key):
                    return
                metadata = getattr(event, "metadata", None) or {}
                processing_task = metadata.get("nextcloud_talk_processing_task")
                if isinstance(processing_task, asyncio.Task) and not processing_task.done():
                    logger.info(
                        "[nextcloud_talk] Processing timeout reached for room %s; "
                        "handler is still active, extending watchdog",
                        self._safe_room_token(room_token),
                    )
                    continue
                if self._event_is_deferred(event):
                    logger.info(
                        "[nextcloud_talk] Processing timeout reached for room %s; "
                        "event is still queued, extending watchdog",
                        self._safe_room_token(room_token),
                    )
                    continue
                session_key = metadata.get("nextcloud_talk_busy_session_key")
                session_task = getattr(self, "_session_tasks", {}).get(session_key)
                if isinstance(session_task, asyncio.Task) and not session_task.done():
                    logger.info(
                        "[nextcloud_talk] Processing timeout reached for room %s; "
                        "busy-session handoff is still active, extending watchdog",
                        self._safe_room_token(room_token),
                    )
                    continue
                if self._generation_has_confirmed_success(event, key):
                    logger.info(
                        "[nextcloud_talk] Completion hook was not observed for room %s; "
                        "committing confirmed successful processing",
                        self._safe_room_token(room_token),
                    )
                    try:
                        await self.on_processing_complete(event, ProcessingOutcome.SUCCESS)
                    except Exception as exc:
                        logger.warning(
                            "[nextcloud_talk] Could not commit confirmed processing: %s",
                            _safe_outward_error_text(exc),
                        )
                    return
                logger.warning(
                    "[nextcloud_talk] Processing completion timed out for room %s; "
                    "leaving message retryable",
                    self._safe_room_token(room_token),
                )
                self._finish_without_ack(event, room_token, numeric_id, generation)
                return
        except asyncio.CancelledError:
            raise
        finally:
            if self._completion_watchdogs.get(key) is asyncio.current_task():
                self._completion_watchdogs.pop(key, None)

    def _schedule_completion_watchdog(
        self, event: MessageEvent, room_token: str, numeric_id: int, generation: int
    ) -> None:
        self._ensure_ack_runtime()
        key = (room_token, numeric_id, generation)
        if not self._generation_is_current(key):
            return
        old = self._completion_watchdogs.get(key)
        if old is not None and not old.done():
            return
        task = asyncio.create_task(
            self._completion_watchdog(event, room_token, numeric_id, generation)
        )
        self._completion_watchdogs[key] = task

    async def _dispatch_bypass_command(
        self,
        event: MessageEvent,
        command: str,
        room_token: str,
        numeric_id: int,
        generation: int,
    ) -> None:
        """Hand a busy-session slash command to core and settle it at most once.

        Core runs such a command inline (``_handle_message_while_active``) and
        reports neither success nor failure.  Commands are not idempotent
        (``/queue``, ``/bg``, ``/approve`` of a later prompt), so an inline
        command is acknowledged once core returns, whatever its outcome.  If the
        turn ended meanwhile, core starts the command as a normal turn and the
        usual completion lifecycle applies.
        """
        key = (room_token, numeric_id, generation)
        session_key = None
        owner_task = None
        interrupts = False
        try:
            from hermes_cli.commands import is_interrupt_then_dispatch
            interrupts = bool(is_interrupt_then_dispatch(command))
        except Exception:
            interrupts = False
        if interrupts:
            try:
                session_key = self._source_session_key(event.source)
            except Exception:
                session_key = None
            if session_key:
                owner_task = (getattr(self, "_session_tasks", {}) or {}).get(session_key)
                self._user_stop_sessions.add(session_key)
        background_before = set(getattr(self, "_background_tasks", set()))
        context_token = self._dispatch_generation_context.set(key)
        dispatch_task = asyncio.current_task()
        try:
            await self.handle_message(event)
        except BaseException:
            metadata = getattr(event, "metadata", None) or {}
            if metadata.get("nextcloud_talk_processing_task") is dispatch_task:
                # Core already ran the command inline; never run it again.
                self._remember_settled_command(room_token, numeric_id)
            self._finish_without_ack(event, room_token, numeric_id, generation)
            raise
        finally:
            self._dispatch_generation_context.reset(context_token)
            if session_key:
                self._user_stop_sessions.discard(session_key)
                expected = getattr(self, "_expected_cancelled_tasks", None) or ()
                if owner_task is not None and owner_task in expected:
                    # Core really cancelled the turn for this command.
                    self._user_stopped_tasks.add(owner_task)
        metadata = getattr(event, "metadata", None) or {}
        if metadata.get("nextcloud_talk_processing_task") is dispatch_task:
            # Handled inline: settle exactly once, never replay the command.
            if not self._generation_is_current(key):
                return
            self._generation_outcomes.get(key, {}).pop("media_delivery_failed", None)
            metadata["nextcloud_talk_handler_state"] = "success"
            metadata.pop("nextcloud_talk_text_delivery_required", None)
            try:
                await self.on_processing_complete(event, ProcessingOutcome.SUCCESS)
            except Exception as exc:
                self._remember_settled_command(room_token, numeric_id)
                logger.warning(
                    "[nextcloud_talk] Command %s in room %s ran but its ACK was not stored: %s",
                    numeric_id, self._safe_room_token(room_token),
                    _safe_outward_error_text(exc),
                )
                raise _OutcomeAlreadyRecorded("command settled in memory") from exc
            if not self._is_acknowledged(room_token, numeric_id):
                self._remember_settled_command(room_token, numeric_id)
            return
        await self._reconcile_base_dispatch(
            event, room_token, numeric_id, generation, background_before
        )

    def _remember_settled_command(self, room_token: str, numeric_id: int) -> None:
        settled = self._settled_commands
        settled[(room_token, numeric_id)] = True
        settled.move_to_end((room_token, numeric_id))
        while len(settled) > _SETTLED_COMMAND_LIMIT:
            settled.popitem(last=False)

    def _is_user_stopped_turn(self, event: MessageEvent) -> bool:
        """A CANCELLED turn that core cancelled for the user's /stop, /new or /reset."""
        task = asyncio.current_task()
        if task is None:
            return False
        if task in self._user_stopped_tasks:
            return True
        expected = getattr(self, "_expected_cancelled_tasks", None) or ()
        if task not in expected:
            return False
        try:
            session_key = self._source_session_key(getattr(event, "source", None))
        except Exception:
            return False
        return bool(session_key) and session_key in self._user_stop_sessions

    async def _reconcile_base_dispatch(
        self,
        event: MessageEvent,
        room_token: str,
        numeric_id: int,
        generation: int,
        background_before: Set[asyncio.Task],
    ) -> None:
        key = (room_token, numeric_id, generation)
        if not self._generation_is_current(key):
            return
        metadata = getattr(event, "metadata", None) or {}
        if "nextcloud_talk_processing_outcome" in metadata:
            return
        background_after = set(getattr(self, "_background_tasks", set()))
        deferred = (
            self._event_is_deferred(event)
            or bool(background_after - background_before)
            or metadata.get("nextcloud_talk_handler_state") == "running"
        )
        if deferred:
            self._schedule_completion_watchdog(event, room_token, numeric_id, generation)
            return
        if (
            metadata.get("nextcloud_talk_busy_state") == "consumed"
            or metadata.get("nextcloud_talk_handler_state") == "success"
        ):
            reply_required = metadata.get(
                "nextcloud_talk_text_delivery_required",
                metadata.get("nextcloud_talk_reply_required") is True,
            ) is True
            delivery_succeeded = self._generation_outcomes.get(key, {}).get(
                "delivery_succeeded"
            ) is True
            if not reply_required or delivery_succeeded:
                await self.on_processing_complete(event, ProcessingOutcome.SUCCESS)
            else:
                self._finish_without_ack(event, room_token, numeric_id, generation)
            return
        self._finish_without_ack(event, room_token, numeric_id, generation)

    async def _refetch_unavailable_file(
        self, room_token: str, numeric_id: int,
        actor_id: str, actor_type: str,
    ) -> Optional[tuple]:
        """Re-read a file-share message whose file Talk failed to resolve.

        Returns ``(msg, text, file_refs)`` from a fresh request that carries a
        valid ``file`` parameter, or ``None`` when every attempt failed.  The
        replacement must be the same message from the same actor.
        """
        client = self._client
        fetch = getattr(client, "get_messages", None) if client is not None else None
        if fetch is None or numeric_id >= _MAX_TALK_MESSAGE_ID:
            return None
        for delay in _UNAVAILABLE_FILE_REFETCH_DELAYS:
            if delay > 0:
                await asyncio.sleep(delay)
            try:
                # Bound each attempt: the poll loop gathers every room, so a
                # hung server must not stall the other rooms for long.
                page = await asyncio.wait_for(
                    fetch(
                        room_token,
                        last_known_id=numeric_id + 1,
                        look_into_future=False,
                        timeout=0,
                        limit=1,
                    ),
                    _UNAVAILABLE_FILE_REFETCH_TIMEOUT,
                )
            except (NextcloudTalkAPIError, OSError, asyncio.TimeoutError) as exc:
                logger.debug(
                    "[nextcloud_talk] Refetch of message %s failed: %s",
                    numeric_id, _error_class_name(exc),
                )
                continue
            for candidate in page if isinstance(page, list) else ():
                parsed = _validated_file_share_replacement(
                    candidate, numeric_id, actor_id, actor_type
                )
                if parsed is not None:
                    logger.info(
                        "[nextcloud_talk] Resolved unavailable file share in message %s on refetch",
                        numeric_id,
                    )
                    return parsed
        logger.warning(
            "[nextcloud_talk] File share in message %s in room %s stayed unavailable after refetch",
            numeric_id, self._safe_room_token(room_token),
        )
        return None

    async def _handle_talk_message(
        self, msg: Dict[str, Any], room_token: str, *, await_completion: bool = False
    ) -> None:
        attachment_leases: List[_AttachmentCacheLease] = []
        try:
            await self._handle_talk_message_with_attachment_leases(
                msg,
                room_token,
                attachment_leases=attachment_leases,
                await_completion=await_completion,
            )
        finally:
            for attachment_lease in reversed(attachment_leases):
                attachment_lease.release()

    async def _handle_talk_message_with_attachment_leases(
        self, msg: Dict[str, Any], room_token: str, *,
        attachment_leases: List[_AttachmentCacheLease],
        await_completion: bool = False,
    ) -> None:
        msg_id = msg.get("id")
        numeric_id = _strict_talk_message_id(msg_id)
        if numeric_id is None:
            return

        self._ensure_ack_runtime()
        if self._is_acknowledged(room_token, numeric_id):
            return
        if self._retry_deferred(room_token, numeric_id):
            return
        inflight = self._inflight_message_ids.setdefault(room_token, set())
        if numeric_id in inflight:
            return
        if (room_token, numeric_id) in self._settled_commands:
            # Core already ran this command but its ACK was not stored: retry
            # only the ACK, never the command.
            try:
                self._commit_cursor(room_token, numeric_id)
            except Exception as exc:
                logger.warning(
                    "[nextcloud_talk] ACK retry for command %s in room %s failed: %s",
                    numeric_id, self._safe_room_token(room_token),
                    _safe_outward_error_text(exc),
                )
            else:
                self._settled_commands.pop((room_token, numeric_id), None)
            return

        raw_actor_id = msg.get("actorId")
        if not isinstance(raw_actor_id, str) or not raw_actor_id.strip():
            self._commit_cursor(room_token, numeric_id)
            return
        actor_id = raw_actor_id
        actor_name = str(msg.get("actorDisplayName") or actor_id or "")
        actor_type = str(msg.get("actorType") or "")
        system_message = str(msg.get("systemMessage") or "")
        text = str(msg.get("message") or "").strip()

        raw_file_refs = msg.get("messageParameters", {})
        if raw_file_refs is None:
            raw_file_refs = {}
        # Talk serializes an empty parameter map as [] on some server versions.
        # A non-empty list is still malformed and must fail closed.
        if raw_file_refs == []:
            raw_file_refs = {}
        if not isinstance(raw_file_refs, dict):
            logger.warning(
                "[nextcloud_talk] Ignoring message %s in room %s: malformed messageParameters",
                _safe_log_text(msg_id, 64), self._safe_room_token(room_token),
            )
            self._commit_cursor(room_token, numeric_id)
            return
        if len(raw_file_refs) > _MAX_METADATA_PARAMETERS:
            logger.warning(
                "[nextcloud_talk] Ignoring message %s in room %s: too many messageParameters",
                _safe_log_text(msg_id, 64), self._safe_room_token(room_token),
            )
            self._commit_cursor(room_token, numeric_id)
            return
        if not _valid_message_parameters(raw_file_refs):
            logger.warning(
                "[nextcloud_talk] Ignoring message %s in room %s: malformed messageParameters metadata",
                _safe_log_text(msg_id, 64), self._safe_room_token(room_token),
            )
            self._commit_cursor(room_token, numeric_id)
            return
        file_refs: Dict[str, Any] = dict(raw_file_refs)
        for param in file_refs.values():
            if param.get("type") == "file" and any(
                not isinstance(param.get(field, ""), str)
                for field in ("name", "path", "link", "mimetype")
            ):
                logger.warning(
                    "[nextcloud_talk] Ignoring message %s in room %s: malformed file metadata",
                    _safe_log_text(msg_id, 64), self._safe_room_token(room_token),
                )
                self._commit_cursor(room_token, numeric_id)
                return
        if self._is_own_presentation_event(msg):
            # Direct calls share the poller's deterministic skip contract.
            return
        if (not text and not file_refs) or system_message:
            self._commit_cursor(room_token, numeric_id)
            return
        if actor_id and actor_id.lower() == self.username.lower():
            self._commit_cursor(room_token, numeric_id)
            return
        if actor_name and actor_name.lower() == self.username.lower():
            self._commit_cursor(room_token, numeric_id)
            return
        if actor_type.lower() not in {"users", "guests"}:
            self._commit_cursor(room_token, numeric_id)
            return
        if not actor_id.strip():
            self._commit_cursor(room_token, numeric_id)
            return
        chat_type = "dm" if self._room_types.get(room_token) == 1 else "group"
        if not self._is_allowed(actor_id, actor_name, chat_type=chat_type):
            logger.debug(
                "[nextcloud_talk] Ignoring unauthorized user %s",
                _safe_log_text(actor_id or actor_name, 256),
            )
            self._commit_cursor(room_token, numeric_id)
            return

        thread_id = _inbound_thread_id(msg, numeric_id)
        thread_fields: Dict[str, Any] = {}
        if thread_id is not None:
            thread_fields["thread_id"] = str(thread_id)
            thread_title = _thread_title(msg.get("threadTitle"))
            if thread_title:
                thread_fields["chat_topic"] = thread_title
        source = self.build_source(
            chat_id=room_token,
            chat_name=f"Nextcloud Talk {self._safe_room_token(room_token)}",
            chat_type=chat_type,
            user_id=actor_id or actor_name,
            user_name=actor_name,
            **thread_fields,
        )
        # Apply the exact-session room-lock exception before attachment I/O.
        # A message that cannot answer the unresolved clarify must remain on
        # Talk without triggering downloads or any other external work.
        bypass_command: Optional[str] = None
        if inflight:
            # A slash command for the busy session passes (core expects
            # /approve, /deny, /stop mid-run, before any clarify intercept) and
            # is settled at most once. Otherwise only a clarify answer passes;
            # everything else waits on Talk.
            bypass_command = self._room_lock_bypass_command(source, text, file_refs)
            if bypass_command is None and not self._source_has_pending_clarify(
                source, require_participant_owner=True
            ):
                return
        # A bypass command is handled inline by core (or, if the turn just
        # ended, as a normal turn); never block the poll waiting for it.
        await_completion = await_completion or (bool(inflight) and bypass_command is None)

        if _is_unavailable_file_placeholder(text, file_refs):
            replacement = await self._refetch_unavailable_file(
                room_token, numeric_id, actor_id, actor_type
            )
            if replacement is not None:
                msg, text, file_refs = replacement
            else:
                text = text.replace("{actor}", actor_name or actor_id)

        # Resolve file attachments from messageParameters.  Talk captions do
        # not necessarily contain a ``{file}`` placeholder, so carry every
        # downloaded attachment on MessageEvent as well as replacing any
        # placeholder that is present.  The gateway uses media_urls/media_types
        # to route images through native vision or vision_analyze.
        download_dir = str(get_hermes_home() / "cache" / "documents")
        media_urls: List[str] = []
        media_types: List[str] = []
        attachments = [param for param in file_refs.values() if param.get("type") == "file"]
        max_count = getattr(self, "max_attachments_per_message", _DEFAULT_MAX_ATTACHMENTS_PER_MESSAGE)
        if len(attachments) > max_count:
            logger.warning(
                "[nextcloud_talk] Ignoring message %s in room %s: attachment count exceeds limit",
                _safe_log_text(msg_id, 64), self._safe_room_token(room_token),
            )
            self._commit_cursor(room_token, numeric_id)
            return
        aggregate_limit = getattr(
            self, "max_attachment_total_bytes", _DEFAULT_MAX_ATTACHMENT_TOTAL_BYTES
        )
        aggregate_bytes = 0
        for key, param in file_refs.items():
            if isinstance(param, dict) and param.get("type") == "file":
                fname = param.get("name", "file")
                mimetype = str(param.get("mimetype") or mimetypes.guess_type(fname)[0] or "application/octet-stream")
                file_link = param.get("link", "")
                file_path = param.get("path", "")
                cache_identity = f"{room_token}:{numeric_id}:{key}:{file_path or file_link}"
                remaining = aggregate_limit - aggregate_bytes
                if file_path and self._client:
                    try:
                        dav_url = self._client._dav_url(file_path)
                        downloaded = await _download_attachment_with_lease_handoff(
                            self._client._download_file, dav_url, download_dir,
                            cache_identity=cache_identity, max_bytes=remaining, lease=True,
                        )
                    except NextcloudTalkAPIError as exc:
                        if exc.category not in self._DETERMINISTIC_ATTACHMENT_CATEGORIES:
                            raise
                        logger.warning(
                            "[nextcloud_talk] Quarantining message %s in room %s: rejected attachment metadata",
                            _safe_log_text(msg_id, 64), self._safe_room_token(room_token),
                        )
                        self._commit_cursor(room_token, numeric_id)
                        return
                    local_path = (
                        downloaded.path
                        if isinstance(downloaded, _AttachmentCacheLease)
                        else downloaded
                    )
                    if isinstance(downloaded, _AttachmentCacheLease):
                        attachment_leases.append(downloaded)
                    if local_path:
                        aggregate_bytes += _attachment_file_size(local_path)
                        media_urls.append(local_path)
                        media_types.append(mimetype)
                        text = text.replace(
                            "{" + key + "}",
                            f"[{fname}](file://{local_path})",
                        )
                        continue
                if file_link:
                    try:
                        downloaded = await _download_attachment_with_lease_handoff(
                            self._client._download_file, file_link, download_dir,
                            cache_identity=cache_identity, max_bytes=remaining, lease=True,
                        ) if self._client else None
                    except NextcloudTalkAPIError as exc:
                        if exc.category not in self._DETERMINISTIC_ATTACHMENT_CATEGORIES:
                            raise
                        logger.warning(
                            "[nextcloud_talk] Quarantining message %s in room %s: rejected attachment metadata",
                            _safe_log_text(msg_id, 64), self._safe_room_token(room_token),
                        )
                        self._commit_cursor(room_token, numeric_id)
                        return
                    local_path = (
                        downloaded.path
                        if isinstance(downloaded, _AttachmentCacheLease)
                        else downloaded
                    )
                    if isinstance(downloaded, _AttachmentCacheLease):
                        attachment_leases.append(downloaded)
                    if local_path:
                        aggregate_bytes += _attachment_file_size(local_path)
                        media_urls.append(local_path)
                        media_types.append(mimetype)
                        text = text.replace(
                            "{" + key + "}",
                            f"[{fname}](file://{local_path})",
                        )
                    else:
                        text = text.replace(
                            "{" + key + "}",
                            f"[{fname}]({file_link})",
                        )
                else:
                    text = text.replace("{" + key + "}", fname)

        text = self._strip_bot_mention(text)
        if self.require_mention and not text:
            self._commit_cursor(room_token, numeric_id)
            return

        parent = msg.get("parent")
        reply_fields: Dict[str, Any] = {}
        if isinstance(parent, dict) and thread_id is not None and parent.get("id") == thread_id:
            # Talk sets every thread message's parent to the thread root; that
            # root is the topic itself, not a quote the user chose.
            parent = None
        if isinstance(parent, dict):
            parent_id = parent.get("id")
            parent_text = parent.get("message")
            if parent_id is not None and isinstance(parent_text, str):
                parent_actor_id = parent.get("actorId")
                parent_actor_name = parent.get("actorDisplayName")
                sanitized_parent = _sanitize_reply_field(parent_text, 500)
                reply_fields = {
                    "reply_to_message_id": _sanitize_reply_field(str(parent_id), 64),
                    "reply_to_text": sanitized_parent,
                    "reply_to_author_id": _sanitize_reply_field(parent_actor_id, 256) if isinstance(parent_actor_id, str) else None,
                    "reply_to_author_name": _sanitize_reply_field(parent_actor_name, 256) if isinstance(parent_actor_name, str) else None,
                    "reply_to_is_own_message": bool(
                        (isinstance(parent_actor_id, str) and parent_actor_id.lower() == self.username.lower())
                        or (isinstance(parent_actor_name, str) and parent_actor_name.lower() in {self.username.lower(), self.bot_name.lower()})
                    ),
                }
        completion_event = asyncio.Event() if await_completion else None
        generation_key = (
            self._next_generation(room_token, numeric_id)
            if numeric_id is not None
            else None
        )
        generation = generation_key[2] if generation_key is not None else None
        event = MessageEvent(
            text=text,
            user_id=actor_id or None,
            user_name=actor_name or None,
            source=source,
            message_type=MessageType.TEXT,
            raw_message=msg,
            message_id=str(msg_id) if msg_id is not None else None,
            media_urls=media_urls,
            media_types=media_types,
            timestamp=self._timestamp_from_message(msg),
            metadata={
                "nextcloud_talk_room_token": room_token,
                "nextcloud_talk_message_id": numeric_id,
                "nextcloud_talk_generation": generation,
                "nextcloud_talk_completion_event": completion_event,
                **({"nextcloud_talk_thread_id": thread_id} if thread_id is not None else {}),
            },
            **reply_fields,
        )
        if numeric_id is not None:
            # Show the sender that the bot has picked the message up.
            self._queue_read_marker(room_token, numeric_id)
        topic = self._topic_for_thread(room_token, thread_id)
        if topic is not None:
            if topic.get("skills") and hasattr(event, "auto_skill"):
                event.auto_skill = list(topic["skills"])
            if topic.get("prompt") and hasattr(event, "channel_prompt"):
                event.channel_prompt = topic["prompt"]
        if generation_key is not None:
            self._inflight_message_ids.setdefault(room_token, set()).add(numeric_id)
            self._inflight_generations[generation_key] = event
            self._generation_outcomes[generation_key] = {}
        if bypass_command is not None and generation_key is not None:
            await self._dispatch_bypass_command(
                event, bypass_command, room_token, numeric_id, generation
            )
            return
        background_before = set(getattr(self, "_background_tasks", set()))
        context_token = (
            self._dispatch_generation_context.set(generation_key)
            if generation_key is not None
            else None
        )
        try:
            await self.handle_message(event)
        except BaseException:
            if generation_key is not None:
                self._finish_without_ack(event, room_token, numeric_id, generation)
            raise
        finally:
            if context_token is not None:
                self._dispatch_generation_context.reset(context_token)
        if generation_key is not None:
            await self._reconcile_base_dispatch(
                event, room_token, numeric_id, generation, background_before
            )
        if completion_event is not None:
            try:
                await asyncio.wait_for(
                    completion_event.wait(), timeout=self.processing_timeout + 1.0
                )
            except asyncio.TimeoutError as exc:
                if generation_key is not None:
                    self._finish_without_ack(event, room_token, numeric_id, generation)
                # on_processing_complete did not run: let the caller count it.
                raise RuntimeError(
                    "Nextcloud Talk startup message processing timed out"
                ) from exc
            expected_success = getattr(
                ProcessingOutcome.SUCCESS, "value", str(ProcessingOutcome.SUCCESS)
            )
            if event.metadata.get("nextcloud_talk_processing_outcome") != expected_success:
                raise _OutcomeAlreadyRecorded("Nextcloud Talk startup message processing failed")

    async def on_processing_complete(self, event: MessageEvent, outcome: ProcessingOutcome) -> None:
        """Commit Talk input only for the authoritative dispatch generation."""
        self._ensure_ack_runtime()
        key = self._event_generation_key(event)
        if key is None or not self._generation_is_current(key):
            return
        room_token, numeric_id, _generation = key
        metadata = getattr(event, "metadata", None) or {}
        hook_error: Optional[BaseException] = None
        effective_outcome = outcome
        generation_state = self._generation_outcomes.get(key, {})
        if (
            outcome == ProcessingOutcome.SUCCESS
            and generation_state.get("media_delivery_failed") is True
        ):
            effective_outcome = ProcessingOutcome.FAILURE
        watchdog = self._completion_watchdogs.pop(key, None)
        if (
            watchdog is not None
            and watchdog is not asyncio.current_task()
            and not watchdog.done()
        ):
            watchdog.cancel()
        metadata.pop("nextcloud_talk_processing_task", None)
        self._current_generations.pop((room_token, numeric_id), None)
        self._inflight_generations.pop(key, None)
        self._generation_outcomes.pop(key, None)
        self._inflight_message_ids.get(room_token, set()).discard(numeric_id)
        if effective_outcome == ProcessingOutcome.SUCCESS:
            try:
                self._commit_cursor(room_token, numeric_id)
            except BaseException as exc:
                effective_outcome = ProcessingOutcome.FAILURE
                hook_error = exc
        elif (
            effective_outcome == ProcessingOutcome.CANCELLED
            and self._is_user_stopped_turn(event)
        ):
            # The user stopped (or reset) this turn with a slash command:
            # acknowledge it, or the next poll would run it again.
            try:
                self._commit_cursor(room_token, numeric_id)
            except BaseException as exc:
                hook_error = exc
        elif effective_outcome == ProcessingOutcome.FAILURE:
            # Only a turn that actually ran and failed counts; cancellation,
            # busy deferral and watchdog release stay plain retryable. A turn
            # whose handler succeeded but whose reply/attachment delivery
            # failed uses the larger delivery budget.
            handler_ok = metadata.get("nextcloud_talk_handler_state") == "success"
            try:
                self._record_dispatch_failure(room_token, numeric_id, delivery=handler_ok)
            except Exception as exc:
                logger.warning("[nextcloud_talk] Could not record failed attempt: %s",
                               _safe_outward_error_text(exc))
        metadata["nextcloud_talk_processing_outcome"] = getattr(
            effective_outcome, "value", str(effective_outcome)
        )
        completion_event = metadata.get("nextcloud_talk_completion_event")
        if isinstance(completion_event, asyncio.Event):
            completion_event.set()
        start_task = metadata.pop("nextcloud_talk_reaction_task", None)
        chat_id = getattr(getattr(event, "source", None), "chat_id", None)
        if isinstance(chat_id, str) and self._reactions_enabled_for(chat_id):
            # The swap is presentation only: run it detached so the queued
            # follow-up, typing stop and post-delivery callback are not held
            # behind two reaction round-trips.
            self._spawn_presentation(self._swap_status_reaction(
                super().on_processing_complete, event, effective_outcome,
                start_task if isinstance(start_task, asyncio.Task) else None,
            ))
        else:
            await super().on_processing_complete(event, effective_outcome)
        if hook_error is not None:
            raise hook_error

    def _is_allowed(self, actor_id: str, actor_name: str, *, chat_type: str = "dm") -> bool:
        stable_id = str(actor_id or "")
        if not stable_id:
            return False
        if self.allow_all:
            return True
        allowed = self.group_allowed_users if chat_type in {"group", "forum", "channel"} else self.allowed_users
        return "*" in allowed or stable_id in allowed

    def _strip_bot_mention(self, text: str) -> str:
        raw = text.strip()
        if not self.bot_name:
            return raw if not self.require_mention else ""
        escaped = re.escape(self.bot_name.strip())
        patterns = [
            rf"^@?{escaped}[:,\s]+(.+)$",
            rf"^\s*<[^>]*>{escaped}</[^>]*>[:,\s]*(.+)$",
        ]
        for pattern in patterns:
            match = re.match(pattern, raw, flags=re.IGNORECASE | re.DOTALL)
            if match:
                return match.group(1).strip()
        return "" if self.require_mention else raw

    @staticmethod
    def _timestamp_from_message(msg: Dict[str, Any]) -> datetime:
        try:
            ts = int(msg.get("timestamp") or 0)
            if ts > 0:
                return datetime.fromtimestamp(ts)
        except Exception:
            pass
        return datetime.now()

    def _safe_room_token(self, token: Optional[str] = None) -> str:
        t = _safe_log_text(
            str(token or "") or str(self.room_tokens[0] if self.room_tokens else ""), 256
        )
        if len(t) <= 6:
            return "***"
        return f"{t[:3]}...{t[-3:]}"

    async def send(self, chat_id: str, content: str, reply_to: Optional[str] = None,
                   metadata: Optional[Dict[str, Any]] = None) -> SendResult:
        if not self._client:
            return SendResult(success=False, error="Not connected")
        target_room = chat_id or (self.room_tokens[0] if self.room_tokens else "")
        if not target_room:
            return SendResult(success=False, error="No room token configured")
        chunks = self._split_message(
            _talk_text(content, markdown=getattr(self, "markdown", True))
        )
        message_ids: List[str] = []
        thread_id = _outbound_thread_id(metadata)
        silent_kwargs = {"silent": True} if self._send_silently(metadata) else {}
        try:
            for chunk in chunks:
                reference_id = str(uuid.uuid4())
                if thread_id is None:
                    result = await self._client.send_message(
                        target_room, chunk, reply_to=reply_to, reference_id=reference_id,
                        **silent_kwargs,
                    )
                else:
                    try:
                        result = await self._client.send_message(
                            target_room, chunk, reply_to=reply_to,
                            reference_id=reference_id, thread_id=thread_id,
                            **silent_kwargs,
                        )
                    except NextcloudTalkAPIError as exc:
                        if exc.status_code != 400:
                            raise
                        # A definite 400 means nothing was posted: the thread is
                        # gone (deleted, or unknown to an older Talk). Deliver to
                        # the main chat instead of losing the reply.  The reply
                        # anchor is dropped too: inside a thread it points at a
                        # message of that same thread, which Talk would reject
                        # again (or re-route into the dead thread).
                        logger.warning(
                            "[nextcloud_talk] Thread %s rejected in room %s; sending to main chat",
                            thread_id, self._safe_room_token(target_room),
                        )
                        thread_id = None
                        reply_to = None
                        result = await self._client.send_message(
                            target_room, chunk, reference_id=str(uuid.uuid4()),
                            **silent_kwargs,
                        )
                talk_id = _strict_talk_message_id(
                    result.get("id") if isinstance(result, dict) else None
                )
                if talk_id is None:
                    raise NextcloudTalkAPIError(
                        "Talk send returned an invalid message ID", category="protocol"
                    )
                message_ids.append(str(talk_id))
                self._remember_sent_message(target_room, talk_id)
                await asyncio.sleep(0.15)
            return SendResult(
                success=True,
                message_id=message_ids[-1] if message_ids else None,
                continuation_message_ids=tuple(message_ids[:-1]),
            )
        except Exception as exc:
            if isinstance(exc, NextcloudTalkAPIError) and exc.category == "network":
                return SendResult(
                    success=False,
                    # Hermes 0.20.1 treats explicit timeout wording as ambiguous
                    # and performs neither a retry nor a formatting fallback.
                    error="delivery outcome unknown (write timed out)",
                    retryable=False,
                    message_id=message_ids[-1] if message_ids else None,
                    continuation_message_ids=tuple(message_ids[:-1]),
                    raw_response={
                        "category": "ambiguous_delivery",
                        **({"partial_delivery": True} if message_ids else {}),
                    },
                )
            return SendResult(
                success=False,
                error=_safe_outward_error_text(exc),
                # A Talk POST can have reached the server before a timeout or
                # malformed response. Never invite a duplicate visible send.
                retryable=False,
                message_id=message_ids[-1] if message_ids else None,
                continuation_message_ids=tuple(message_ids[:-1]),
                raw_response={"partial_delivery": bool(message_ids), "delivered_message_ids": message_ids},
            )

    def _remember_sent_message(self, room_token: str, message_id: int) -> None:
        sent = getattr(self, "_sent_message_ids", None)
        if sent is None:
            sent = self._sent_message_ids = collections.OrderedDict()
        key = (str(room_token), int(message_id))
        sent[key] = None
        sent.move_to_end(key)
        while len(sent) > _SENT_MESSAGE_RETENTION:
            sent.popitem(last=False)

    def _own_message_id(self, chat_id: Any, message_id: Any) -> Optional[int]:
        """The numeric ID when this adapter posted ``message_id`` in ``chat_id``."""
        if not isinstance(chat_id, str) or not chat_id:
            return None
        if isinstance(message_id, str) and message_id.isascii() and message_id.isdigit() \
                and len(message_id) <= 19:
            message_id = int(message_id)
        numeric_id = _strict_talk_message_id(message_id)
        if numeric_id is None:
            return None
        sent = getattr(self, "_sent_message_ids", None) or {}
        return numeric_id if (chat_id, numeric_id) in sent else None

    def max_message_length_for_chat(self, chat_id: str) -> int:
        """Expose the configured Talk limit to Hermes' progress/stream splitters."""
        return self.max_message_length

    async def edit_message(self, chat_id: str, message_id: str, content: str, *,
                           finalize: bool = False, **_kwargs: Any) -> SendResult:
        """Replace the text of a message this adapter sent (progress, streaming previews).

        Talk allows edits of one's own messages for 24 hours; every refusal is
        reported as a non-retryable failure, so Hermes falls back to a new message.
        A timed-out edit may be repeated with the same full text (unlike a POST);
        this lets Hermes keep updating a single tool-progress bubble.
        """
        client = self._client
        if client is None or not callable(getattr(client, "edit_message", None)):
            return SendResult(success=False, error="Not supported", retryable=False)
        numeric_id = self._own_message_id(chat_id, message_id)
        if numeric_id is None:
            return SendResult(
                success=False, error="Message was not sent by this adapter", retryable=False
            )
        text = _talk_text(content, markdown=getattr(self, "markdown", True))
        if not text.strip():
            return SendResult(success=False, error="Edited message is empty", retryable=False)
        if len(text) > self.max_message_length:
            return SendResult(success=False, error="Edited message is too long", retryable=False)
        try:
            await client.edit_message(chat_id, numeric_id, text)
        except NextcloudTalkAPIError as exc:
            if exc.category == "network":
                return SendResult(
                    success=False, error="edit outcome unknown (write timed out)",
                    retryable=True, raw_response={"category": "ambiguous_delivery"},
                )
            return SendResult(
                success=False, error=_safe_outward_error_text(exc), retryable=False
            )
        except Exception as exc:
            return SendResult(
                success=False, error=_safe_outward_error_text(exc), retryable=False
            )
        return SendResult(success=True, message_id=str(numeric_id))

    async def delete_message(self, chat_id: str, message_id: str) -> bool:
        """Delete a message this adapter sent; True when it is gone."""
        client = self._client
        if client is None or not callable(getattr(client, "delete_message", None)):
            return False
        numeric_id = self._own_message_id(chat_id, message_id)
        if numeric_id is None:
            return False
        try:
            await client.delete_message(chat_id, numeric_id)
        except NextcloudTalkAPIError as exc:
            if exc.status_code != 404:  # 404: already gone
                logger.debug(
                    "[nextcloud_talk] Delete failed in room %s: %s",
                    self._safe_room_token(chat_id), _safe_outward_error_text(exc),
                )
                return False
        except Exception as exc:
            logger.debug(
                "[nextcloud_talk] Delete failed in room %s: %s",
                self._safe_room_token(chat_id), _safe_outward_error_text(exc),
            )
            return False
        self._sent_message_ids.pop((chat_id, numeric_id), None)
        return True

    def _send_silently(self, metadata: Any) -> bool:
        """In ``important`` mode only final replies and prompts notify.

        Hermes marks a final reply with ``metadata["notify"]`` (cron delivery
        and background-task notices too) and approval/clarify prompts with
        ``is_approval_prompt``; progress, status and tool messages carry
        neither and are posted silently, like Telegram's default mode.
        """
        if getattr(self, "notifications", "all") != "important":
            return False
        if isinstance(metadata, dict) and (
            metadata.get("notify") or metadata.get("is_approval_prompt")
            or (metadata.get("expect_edits") and not metadata.get("_interim_send"))
        ):
            return False
        return True

    def _split_message(self, content: str) -> List[str]:
        """Split at line/word boundaries without breaking Markdown code fences.

        A chunk that ends inside a ``` fence is closed and the next chunk
        reopens it with the same info string, so every chunk renders on its
        own; indentation at chunk edges is preserved.
        """
        content = content or ""
        limit = max(1, int(self.max_message_length))
        if len(content) <= limit:
            return [content]
        closer = "\n```"
        # Fence repair needs room for an opener, a closer and some content.
        repair = limit >= 128
        chunks: List[str] = []
        remaining = content
        reopen = ""
        while remaining:
            if len(reopen) + len(remaining) <= limit:
                chunks.append(reopen + remaining)
                break
            budget = limit - len(reopen) - (len(closer) if repair else 0)
            piece = remaining[:budget]
            split_at = piece.rfind("\n")
            if split_at <= budget // 3:
                split_at = max(split_at, piece.rfind(" "))
            if split_at > budget // 3:
                piece = remaining[:split_at]
                remaining = remaining[split_at + 1:]  # drop exactly the separator
            else:
                remaining = remaining[len(piece):]
            body = reopen + piece
            opener = _open_code_fence(body) if repair else None
            if opener is not None:
                chunks.append(body.rstrip("\n") + closer)
                # Keep the reopened info string short enough for the budget.
                reopen = opener[: max(3, limit // 4)] + "\n"
            else:
                chunks.append(body.rstrip())
                reopen = ""
        return [chunk for chunk in chunks if chunk.strip()] or [""]

    async def send_typing(self, chat_id: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        return None

    def _record_media_delivery_result(self, result: Optional[SendResult]) -> None:
        if isinstance(result, SendResult) and getattr(result, "success", False) is True:
            return
        generation_key = self._dispatch_generation_context.get()
        if generation_key is not None and self._generation_is_current(generation_key):
            state = self._generation_outcomes.setdefault(generation_key, {})
            state["media_delivery_failed"] = True
            state["media_failure_count"] = min(
                1024, int(state.get("media_failure_count", 0)) + 1
            )

    async def _send_tracked_media_attachment(
        self,
        chat_id: str,
        local_path: str,
        *,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        message_type: Optional[str] = None,
    ) -> SendResult:
        # Always forward the metadata: besides the thread it carries ``notify``,
        # which decides whether a caption/link follow-up is posted silently.
        thread_kwargs: Dict[str, Any] = {"metadata": metadata} if metadata else {}
        if message_type:
            thread_kwargs["message_type"] = message_type
        try:
            result = await self._send_file_attachment(
                chat_id, local_path, caption=caption, reply_to=reply_to, **thread_kwargs
            )
        except BaseException:
            self._record_media_delivery_result(None)
            raise
        self._record_media_delivery_result(result)
        return result

    async def _send_file_attachment(self, chat_id: str, local_path: str, caption: Optional[str] = None,
                                    reply_to: Optional[str] = None,
                                    metadata: Optional[Dict[str, Any]] = None,
                                    message_type: Optional[str] = None) -> SendResult:
        if not self._client:
            return SendResult(success=False, error="Not connected")
        path = Path(str(local_path)).expanduser()
        if not path.is_file():
            return SendResult(success=False, error="Outbound file was not found")
        size = path.stat().st_size
        if size > self.max_upload_bytes:
            return SendResult(
                success=False,
                error="Outbound file exceeds configured size limit",
            )
        target_room = chat_id or (self.room_tokens[0] if self.room_tokens else "")
        if not target_room:
            return SendResult(success=False, error="No room token configured")
        attachment_delivered = False
        remote_path: Optional[str] = None
        share: Any = None
        try:
            thread_id = _outbound_thread_id(metadata)
            upload_result = await self._client.upload_and_share_file(
                str(path), target_room, self.upload_folder,
                caption=caption, reply_to=reply_to,
                **({"thread_id": thread_id} if thread_id is not None else {}),
                **({"message_type": message_type} if message_type else {}),
                **({"silent": True} if self._send_silently(metadata) else {}),
            )
            if len(upload_result) == 4:
                remote_path, share, native_talk_share, atomic_metadata = upload_result
            else:  # compatibility with older test/provider wrappers
                remote_path, share, native_talk_share = upload_result
                atomic_metadata = False
            attachment_delivered = True
            link = share.get("url") if isinstance(share, dict) else None
            delivery_result: Optional[SendResult] = None
            if (
                native_talk_share and not atomic_metadata and not (caption or reply_to)
                and _outbound_thread_id(metadata) is not None
            ):
                # The server refused Talk metadata, so the share was posted
                # without it and landed in the main chat.  Say so instead of
                # losing the thread silently; there is no caption to follow up.
                logger.warning(
                    "[nextcloud_talk] Talk could not attach thread metadata to a file in room %s; "
                    "the file was shared to the main chat",
                    self._safe_room_token(target_room),
                )
            if native_talk_share and (caption or reply_to) and not atomic_metadata:
                delivery_result = await self.send(
                    target_room, caption, reply_to=reply_to, metadata=metadata
                )
            elif not native_talk_share:
                if not link:
                    return SendResult(
                        success=False,
                        error="Attachment fallback was incomplete (partial delivery)",
                        retryable=False,
                        raw_response={
                            "partial_delivery": True,
                            "attachment_delivered": True,
                            "category": "provider_protocol",
                        },
                    )
                delivery_result = await self.send(
                    target_room, f"{caption or path.name}\n{link}", reply_to=reply_to,
                    metadata=metadata,
                )
            if delivery_result is not None and not delivery_result.success:
                return SendResult(
                    success=False,
                    error="Attachment follow-up delivery failed (partial delivery)",
                    retryable=False,
                    message_id=getattr(delivery_result, "message_id", None),
                    continuation_message_ids=getattr(
                        delivery_result, "continuation_message_ids", ()
                    ),
                    raw_response={
                        "partial_delivery": True,
                        "attachment_delivered": True,
                        "category": "followup_delivery",
                    },
                )
            raw_response = {
                "native_talk_share": native_talk_share,
                "atomic_metadata": atomic_metadata,
                "category": "attachment_delivery",
            }
            # Files share IDs/tokens are not Talk chat message IDs.  Only a
            # follow-up caption/link send can supply a real message ID.
            message_id = getattr(delivery_result, "message_id", None) if delivery_result else None
            continuation_ids = (
                getattr(delivery_result, "continuation_message_ids", ())
                if delivery_result else ()
            )
            return SendResult(
                success=True,
                message_id=message_id,
                continuation_message_ids=continuation_ids,
                raw_response=raw_response,
            )
        except Exception as exc:
            if (
                isinstance(exc, NextcloudTalkAPIError)
                and exc.category == "ambiguous_upload"
                and not attachment_delivered
            ):
                return SendResult(
                    success=False,
                    # Hermes 0.20.1 recognizes this as an ambiguous timeout and
                    # therefore performs neither a retry nor a formatting fallback.
                    error="attachment upload outcome unknown (write timed out)",
                    retryable=False,
                    raw_response={"category": "ambiguous_upload"},
                )
            error_message = _safe_outward_error_text(exc)
            raw_response = None
            if attachment_delivered:
                error_message += " (attachment was already delivered; partial delivery)"
                raw_response = {
                    "partial_delivery": True,
                    "attachment_delivered": True,
                    "category": "attachment_delivery",
                }
            return SendResult(
                success=False,
                error=error_message,
                retryable=not attachment_delivered,
                raw_response=raw_response,
            )

    async def send_image(self, chat_id: str, image_url: str, caption: Optional[str] = None,
                         reply_to: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None) -> SendResult:
        image_ref = str(image_url or "")
        if image_ref.startswith(("http://", "https://")):
            text = f"{caption}\n{image_ref}" if caption else image_ref
            try:
                result = await self.send(
                    chat_id, text, reply_to=reply_to, metadata=metadata
                )
            except BaseException:
                self._record_media_delivery_result(None)
                raise
            self._record_media_delivery_result(result)
            return result
        if image_ref.startswith("file://"):
            image_ref = parse.unquote(parse.urlparse(image_ref).path)
        return await self._send_tracked_media_attachment(
            chat_id, image_ref, caption=caption, reply_to=reply_to, metadata=metadata
        )

    async def send_image_file(self, chat_id: str, image_path: str, caption: Optional[str] = None,
                              reply_to: Optional[str] = None, **kwargs) -> SendResult:
        return await self._send_tracked_media_attachment(
            chat_id, image_path, caption=caption, reply_to=reply_to,
            metadata=kwargs.get("metadata"),
        )

    async def send_document(self, chat_id: str, document_path: Optional[str] = None, caption: Optional[str] = None,
                            reply_to: Optional[str] = None, **kwargs) -> SendResult:
        file_path = str(document_path or kwargs.get("file_path") or kwargs.get("path") or "")
        if file_path.startswith(("http://", "https://")):
            text = f"{caption or 'Document'}: {file_path}"
            return await self.send(
                chat_id, text, reply_to=reply_to, metadata=kwargs.get("metadata")
            )
        if file_path.startswith("file://"):
            file_path = parse.unquote(parse.urlparse(file_path).path)
        return await self._send_tracked_media_attachment(
            chat_id, file_path, caption=caption, reply_to=reply_to,
            metadata=kwargs.get("metadata"),
        )

    async def send_voice(self, chat_id: str, audio_path: str, caption: Optional[str] = None,
                         reply_to: Optional[str] = None, **kwargs) -> SendResult:
        # A voice reply (TTS, ``[[audio_as_voice]]``) becomes a Talk voice
        # message with an inline player; Talk accepts that only for MP3/WAV.
        voice = (
            kwargs.get("is_voice") is True
            and Path(str(audio_path or "")).suffix.lower() in _VOICE_MESSAGE_SUFFIXES
        )
        return await self._send_tracked_media_attachment(
            chat_id, audio_path, caption=caption, reply_to=reply_to,
            metadata=kwargs.get("metadata"),
            **({"message_type": "voice-message"} if voice else {}),
        )

    async def play_tts(self, chat_id: str, audio_path: str, **kwargs) -> SendResult:
        """Auto-TTS replies are voice replies: share MP3/WAV as Talk voice messages."""
        kwargs.setdefault("is_voice", True)
        return await self.send_voice(chat_id=chat_id, audio_path=audio_path, **kwargs)

    async def send_video(self, chat_id: str, video_path: str, caption: Optional[str] = None,
                         reply_to: Optional[str] = None, **kwargs) -> SendResult:
        return await self._send_tracked_media_attachment(
            chat_id, video_path, caption=caption, reply_to=reply_to,
            metadata=kwargs.get("metadata"),
        )

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        room_type = "dm" if self._room_types.get(chat_id) == 1 else "group"
        return {"name": f"Nextcloud Talk {self._safe_room_token(chat_id)}", "type": room_type, "chat_id": chat_id}


def _configured_values(config: Optional[PlatformConfig] = None) -> tuple[str, str, str, str]:
    extra = getattr(config, "extra", {}) or {}
    return (
        _cfg(extra, "NEXTCLOUD_TALK_URL", "url", ""),
        _cfg(extra, "NEXTCLOUD_TALK_USERNAME", "username", ""),
        _cfg(extra, "NEXTCLOUD_TALK_PASSWORD", "password", ""),
        _cfg(extra, "NEXTCLOUD_TALK_ROOM_TOKENS", "room_tokens", "")
        or _cfg(extra, "NEXTCLOUD_TALK_ROOM_TOKEN", "room_token", ""),
    )


def check_requirements() -> bool:
    return True


def validate_config(config) -> bool:
    url, username, password, tokens = _configured_values(config)
    extra = getattr(config, "extra", {}) or {}
    allow_insecure = _truthy(
        _cfg(extra, "NEXTCLOUD_TALK_ALLOW_INSECURE_HTTP", "allow_insecure_http", None),
        False,
    )
    auto_discover = _truthy(
        _cfg(extra, "NEXTCLOUD_TALK_AUTO_DISCOVER_ROOMS", "auto_discover_rooms", None),
        True,
    )
    return bool(
        url and _secure_base_url(url, allow_insecure)
        and username and password and (tokens or auto_discover)
    )


def is_connected(config) -> bool:
    return validate_config(config)


def interactive_setup() -> None:
    try:
        from hermes_cli.config import get_env_value, save_env_value
        from hermes_cli.cli_output import prompt, prompt_yes_no, print_info, print_success, print_warning
    except Exception:
        from hermes_cli.setup import get_env_value, save_env_value, prompt, prompt_yes_no, print_info, print_success, print_warning

    existing_url = get_env_value("NEXTCLOUD_TALK_URL")
    if existing_url:
        print_info(f"Nextcloud Talk: already configured ({existing_url})")
        if not prompt_yes_no("Reconfigure Nextcloud Talk?", False):
            return

    print_info("Create a dedicated Nextcloud user for Hermes if possible.")
    print_info("Use a Nextcloud app password, not your main account password.")
    print_info("Room tokens are the last path segment in a Talk room URL, e.g. /call/abc123def.")
    print_info("For multiple rooms, separate tokens with commas.")
    print()

    url = prompt("Nextcloud base URL", default=existing_url or "https://")
    if not url or url == "https://":
        print_warning("Base URL is required — skipping Nextcloud Talk setup")
        return
    save_env_value("NEXTCLOUD_TALK_URL", url.rstrip("/"))

    username = prompt("Nextcloud username", default=get_env_value("NEXTCLOUD_TALK_USERNAME") or "")
    if not username:
        print_warning("Username is required — skipping Nextcloud Talk setup")
        return
    save_env_value("NEXTCLOUD_TALK_USERNAME", username.strip())

    password = prompt("Nextcloud app password", default=get_env_value("NEXTCLOUD_TALK_PASSWORD") or "", password=True)
    if not password:
        print_warning("Password is required — skipping Nextcloud Talk setup")
        return
    save_env_value("NEXTCLOUD_TALK_PASSWORD", password.strip())

    room_tokens = prompt(
        "Talk group/public room token(s) (optional; comma-separated)",
        default=get_env_value("NEXTCLOUD_TALK_ROOM_TOKENS") or get_env_value("NEXTCLOUD_TALK_ROOM_TOKEN") or "",
    )
    save_env_value("NEXTCLOUD_TALK_ROOM_TOKENS", room_tokens.replace(" ", "") if room_tokens else "")
    # Clear legacy single-room token to avoid confusion
    save_env_value("NEXTCLOUD_TALK_ROOM_TOKEN", "")

    bot_name = prompt("Bot name/mention to recognize", default=get_env_value("NEXTCLOUD_TALK_BOT_NAME") or "Hermes")
    save_env_value("NEXTCLOUD_TALK_BOT_NAME", bot_name.strip() or "Hermes")

    require_mention = prompt_yes_no("Only respond when mentioned?", False)
    save_env_value("NEXTCLOUD_TALK_REQUIRE_MENTION", "true" if require_mention else "false")

    if prompt_yes_no("Allow all room users to talk to Hermes?", False):
        save_env_value("NEXTCLOUD_TALK_ALLOW_ALL_USERS", "true")
        save_env_value("NEXTCLOUD_TALK_ALLOWED_USERS", "")
        print_warning("Open access — anyone in any Talk room can command Hermes.")
    else:
        save_env_value("NEXTCLOUD_TALK_ALLOW_ALL_USERS", "false")
        allowed = prompt(
            "Allowed stable Nextcloud actor IDs (comma-separated)",
            default=get_env_value("NEXTCLOUD_TALK_ALLOWED_USERS") or username,
        )
        save_env_value("NEXTCLOUD_TALK_ALLOWED_USERS", allowed.replace(" ", "") if allowed else "")

    print_success("Nextcloud Talk configuration saved to ~/.hermes/.env")
    print_info("Restart the gateway: hermes gateway restart")


def register(ctx) -> None:
    ctx.register_platform(
        name="nextcloud_talk",
        label="Nextcloud Talk",
        adapter_factory=lambda cfg: NextcloudTalkAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=is_connected,
        required_env=[
            "NEXTCLOUD_TALK_URL",
            "NEXTCLOUD_TALK_USERNAME",
            "NEXTCLOUD_TALK_PASSWORD",
        ],
        install_hint="No extra packages needed. Use a Nextcloud app password for NEXTCLOUD_TALK_PASSWORD.",
        setup_fn=interactive_setup,
        allowed_users_env="NEXTCLOUD_TALK_ALLOWED_USERS",
        allow_all_env="NEXTCLOUD_TALK_ALLOW_ALL_USERS",
        max_message_length=_DEFAULT_MAX_MESSAGE_LENGTH,
        emoji="☁️",
        allow_update_command=True,
        platform_hint=(
            "You are chatting via Nextcloud Talk (multi-room). Talk renders Markdown: bold, lists, "
            "code blocks, links and simple tables work; images are shown as link previews. "
            "Keep responses concise in busy group rooms."
        ),
    )
