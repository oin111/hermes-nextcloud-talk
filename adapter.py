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
    NEXTCLOUD_TALK_ACK_OVERLAP_IDS=4096
    NEXTCLOUD_TALK_PROCESSING_TIMEOUT=300
    NEXTCLOUD_TALK_ALLOW_INSECURE_HTTP=false
    NEXTCLOUD_TALK_ALLOW_PUBLIC_SHARE_FALLBACK=false
    NEXTCLOUD_TALK_AUTO_DISCOVER_ROOMS=true
    NEXTCLOUD_TALK_DISCOVERY_INTERVAL=30
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import ipaddress
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

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    ProcessingOutcome,
    SendResult,
)

logger = logging.getLogger(__name__)

_DEFAULT_POLL_TIMEOUT = 30
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
_DEFAULT_MAX_POLL_BATCH = 200
_DEFAULT_MAX_ROOMS = 200
_ACK_ROOM_LIMIT_MULTIPLIER = 4
_DEFAULT_MAX_BACKLOG_MESSAGES = 10000
_DEFAULT_ACK_RETENTION_COUNT = 4096
_DEFAULT_ACK_OVERLAP_IDS = 4096
_DEFAULT_PROCESSING_TIMEOUT = 300.0
_ACK_STATE_VERSION = 2
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
_MIN_CONVERSATION_TYPE = 1
_MAX_CONVERSATION_TYPE = 6
_ATTACHMENT_CACHE_MANIFEST_VERSION = 1
_MAX_ATTACHMENT_CACHE_MANIFEST_BYTES = 1024
_PERMANENT_ATTACHMENT_HTTP_STATUSES = {400, 404, 410, 413, 414, 415, 416, 422}


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
    tokens = _parse_room_tokens(os.getenv("NEXTCLOUD_TALK_ROOM_TOKENS", ""))
    legacy = _parse_room_tokens(os.getenv("NEXTCLOUD_TALK_ROOM_TOKEN", ""))
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
            base_url, _truthy(os.getenv("NEXTCLOUD_TALK_ALLOW_INSECURE_HTTP"), False)
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
        self.max_poll_batch = max(1, int(max_poll_batch))
        self.max_rooms = max(1, int(max_rooms))
        self.max_cache_bytes = max(0, int(max_cache_bytes))
        self.max_cache_files = max(0, int(max_cache_files))
        self._download_locks_guard = threading.Lock()
        self._download_locks: Dict[str, List[Any]] = {}
        self._cache_guard = threading.RLock()
        self._cache_inflight: Dict[str, int] = {}
        self._cache_leases: Dict[str, int] = {}
        self._base_origin = self._origin(self.base_url)
        token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
        self._headers = {
            "Authorization": f"Basic {token}",
            "OCS-APIRequest": "true",
            "Accept": "application/json",
            "User-Agent": "Hermes-Agent-Nextcloud-Talk/0.1.0",
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

    def _open_authenticated(self, req: request.Request):
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
        return opener.open(req, timeout=self.timeout)

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

    def _url(self, room_token: str, query: Optional[Dict[str, Any]] = None) -> str:
        encoded_token = parse.quote(str(room_token).strip("/"), safe="")
        url = f"{self.base_url}/ocs/v2.php/apps/spreed/api/v1/chat/{encoded_token}"
        if query:
            cleaned = {k: v for k, v in query.items() if v is not None}
            url += "?" + parse.urlencode(cleaned)
        return url

    def _request(self, method: str, room_token: str, *, query: Optional[Dict[str, Any]] = None,
                 data: Optional[Dict[str, Any]] = None,
                 expected_types: Optional[tuple[type, ...]] = None) -> Any:
        body = None
        headers = dict(self._headers)
        if data is not None:
            body = parse.urlencode(data).encode("utf-8")
            headers["Content-Type"] = "application/x-www-form-urlencoded"

        req = request.Request(self._url(room_token, query), data=body, headers=headers, method=method)
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
                           reference_id: Optional[str] = None) -> Any:
        data = {"message": message}
        if reference_id:
            data["referenceId"] = reference_id
        if reply_to:
            data["replyTo"] = reply_to
        return await asyncio.to_thread(
            self._request, "POST", room_token, data=data, expected_types=(dict,)
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
                                 reply_to: Optional[str] = None) -> Any:
        form: Dict[str, Any] = {
            "path": remote_path, "shareType": 10, "shareWith": room_token
        }
        talk_metadata: Dict[str, str] = {}
        if caption:
            talk_metadata["caption"] = _sanitize_reply_field(caption, 2000)
        if reply_to:
            talk_metadata["replyTo"] = _sanitize_reply_field(reply_to, 64)
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
                                    reply_to: Optional[str] = None) -> tuple[str, Any, bool, bool]:
        remote_path = await asyncio.to_thread(self._upload_file_sync, local_path, upload_folder)
        try:
            if caption or reply_to:
                share = await asyncio.to_thread(
                    self._share_file_to_talk_sync, remote_path, room_token,
                    caption=caption, reply_to=reply_to,
                )
            else:
                share = await asyncio.to_thread(
                    self._share_file_to_talk_sync, remote_path, room_token
                )
            return remote_path, share, True, True
        except NextcloudTalkAPIError as exc:
            if (caption or reply_to) and exc.category == "capability":
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

    _DETERMINISTIC_ATTACHMENT_CATEGORIES = {
        "security", "path", "overflow", "aggregate_overflow", "content", "shape", "protocol"
    }

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform("nextcloud_talk"))
        extra = getattr(config, "extra", {}) or {}

        self.base_url = os.getenv("NEXTCLOUD_TALK_URL") or extra.get("url", "")
        self.username = os.getenv("NEXTCLOUD_TALK_USERNAME") or extra.get("username", "")
        self.password = os.getenv("NEXTCLOUD_TALK_PASSWORD") or extra.get("password", "")
        self.bot_name = os.getenv("NEXTCLOUD_TALK_BOT_NAME") or extra.get("bot_name", "Hermes")
        self.poll_timeout = int(os.getenv("NEXTCLOUD_TALK_POLL_TIMEOUT") or extra.get("poll_timeout", _DEFAULT_POLL_TIMEOUT))
        self.require_mention = _truthy(os.getenv("NEXTCLOUD_TALK_REQUIRE_MENTION"), bool(extra.get("require_mention", False)))
        # Legacy PROCESS_HISTORY=true means unlimited first-run backfill.  The
        # safe default is a documented bounded backlog, never "skip to latest".
        self.process_history = _truthy(os.getenv("NEXTCLOUD_TALK_PROCESS_HISTORY"), bool(extra.get("process_history", False)))
        backlog_value = os.getenv("NEXTCLOUD_TALK_INITIAL_BACKLOG_LIMIT") or extra.get(
            "initial_backlog_limit", _DEFAULT_INITIAL_BACKLOG
        )
        self.initial_backlog_limit: Optional[int] = None if self.process_history else max(0, int(backlog_value))
        self.upload_folder = os.getenv("NEXTCLOUD_TALK_UPLOAD_FOLDER") or extra.get("upload_folder", "/Hermes Uploads")
        self.max_download_bytes = int(os.getenv("NEXTCLOUD_TALK_MAX_INBOUND_FILE_BYTES") or extra.get("max_inbound_file_bytes", _DEFAULT_MAX_DOWNLOAD_BYTES))
        self.max_upload_bytes = int(os.getenv("NEXTCLOUD_TALK_MAX_OUTBOUND_FILE_BYTES") or extra.get("max_outbound_file_bytes", _DEFAULT_MAX_UPLOAD_BYTES))
        self.max_attachments_per_message = max(0, int(
            os.getenv("NEXTCLOUD_TALK_MAX_ATTACHMENTS_PER_MESSAGE")
            or extra.get("max_attachments_per_message", _DEFAULT_MAX_ATTACHMENTS_PER_MESSAGE)
        ))
        self.max_attachment_total_bytes = max(0, int(
            os.getenv("NEXTCLOUD_TALK_MAX_ATTACHMENT_TOTAL_BYTES")
            or extra.get("max_attachment_total_bytes", _DEFAULT_MAX_ATTACHMENT_TOTAL_BYTES)
        ))
        self.max_cache_bytes = max(0, int(
            os.getenv("NEXTCLOUD_TALK_MAX_CACHE_BYTES")
            or extra.get("max_cache_bytes", _DEFAULT_MAX_CACHE_BYTES)
        ))
        self.max_cache_files = max(0, int(
            os.getenv("NEXTCLOUD_TALK_MAX_CACHE_FILES")
            or extra.get("max_cache_files", _DEFAULT_MAX_CACHE_FILES)
        ))
        self.max_json_bytes = max(1, int(os.getenv("NEXTCLOUD_TALK_MAX_JSON_BYTES") or extra.get("max_json_bytes", _DEFAULT_MAX_JSON_BYTES)))
        self.max_body_bytes = max(1, int(os.getenv("NEXTCLOUD_TALK_MAX_BODY_BYTES") or extra.get("max_body_bytes", _DEFAULT_MAX_BODY_BYTES)))
        self.max_poll_batch = max(1, int(os.getenv("NEXTCLOUD_TALK_MAX_POLL_BATCH") or extra.get("max_poll_batch", _DEFAULT_MAX_POLL_BATCH)))
        self.max_rooms = max(1, int(os.getenv("NEXTCLOUD_TALK_MAX_ROOMS") or extra.get("max_rooms", _DEFAULT_MAX_ROOMS)))
        configured_ack_rooms = os.getenv("NEXTCLOUD_TALK_MAX_ACK_ROOMS") or extra.get("max_ack_rooms")
        self.max_ack_rooms = max(
            self.max_rooms,
            int(configured_ack_rooms) if configured_ack_rooms is not None
            else self.max_rooms * _ACK_ROOM_LIMIT_MULTIPLIER,
        )
        self.max_backlog_messages = max(1, int(os.getenv("NEXTCLOUD_TALK_MAX_BACKLOG_MESSAGES") or extra.get("max_backlog_messages", _DEFAULT_MAX_BACKLOG_MESSAGES)))
        self.allow_public_share_fallback = _truthy(
            os.getenv("NEXTCLOUD_TALK_ALLOW_PUBLIC_SHARE_FALLBACK"),
            bool(extra.get("allow_public_share_fallback", False)),
        )
        self.auto_discover_rooms = _truthy(
            os.getenv("NEXTCLOUD_TALK_AUTO_DISCOVER_ROOMS"),
            bool(extra.get("auto_discover_rooms", True)),
        )
        self.discovery_interval = max(
            5,
            int(os.getenv("NEXTCLOUD_TALK_DISCOVERY_INTERVAL") or extra.get("discovery_interval", 30)),
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

        allowed_env = os.getenv("NEXTCLOUD_TALK_ALLOWED_USERS", "")
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
        self.allow_all = _truthy(os.getenv("NEXTCLOUD_TALK_ALLOW_ALL_USERS"), False)

        max_len = os.getenv("NEXTCLOUD_TALK_MAX_MESSAGE_LENGTH") or extra.get("max_message_length")
        self.max_message_length = int(max_len or _DEFAULT_MAX_MESSAGE_LENGTH)

        self._client: Optional[NextcloudTalkClient] = None
        self._poll_task: Optional[asyncio.Task] = None
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
            os.getenv("NEXTCLOUD_TALK_PROCESSING_TIMEOUT")
            or extra.get("processing_timeout", _DEFAULT_PROCESSING_TIMEOUT)
        ))
        self.ack_retention_count = max(32, int(
            os.getenv("NEXTCLOUD_TALK_ACK_RETENTION_COUNT")
            or extra.get("ack_retention_count", _DEFAULT_ACK_RETENTION_COUNT)
        ))
        self.ack_overlap_ids = max(32, int(
            os.getenv("NEXTCLOUD_TALK_ACK_OVERLAP_IDS")
            or extra.get("ack_overlap_ids", _DEFAULT_ACK_OVERLAP_IDS)
        ))
        if self.ack_retention_count < self.ack_overlap_ids:
            logger.warning(
                "[nextcloud_talk] ACK retention %d is below overlap %d; clamping retention to %d",
                self.ack_retention_count, self.ack_overlap_ids, self.ack_overlap_ids,
            )
            self.ack_retention_count = self.ack_overlap_ids
        self._cursor_path = get_hermes_home() / "cache" / "nextcloud_talk" / "cursors.json"
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
            return result

        base_setter = getattr(super(), "set_message_handler", None)
        if callable(base_setter):
            base_setter(tracked)
        else:
            self._message_handler = tracked

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

    async def connect(self, **kwargs) -> bool:
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
            self._load_cursors()
            # Room metadata is useful even when DM auto-discovery is disabled:
            # explicit type-1 rooms must still be classified as DMs.
            await self._refresh_discovered_rooms(force=True)
            for token in self.room_tokens:
                if not self._is_room_initialized(token):
                    await self._initialize_room(token)
            self._running = True
            self._poll_task = asyncio.create_task(self._poll_loop())
            rooms_safe = ", ".join(self._safe_room_token(t) for t in self.room_tokens)
            logger.info("[nextcloud_talk] Connected to rooms: %s", rooms_safe)
            self._mark_connected()
            return True
        except Exception as exc:
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
        self._ensure_ack_runtime()
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
        self._mark_disconnected()
        logger.info("[nextcloud_talk] Disconnected")

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
                    if self._normalize_ack_state(self._ack_rooms[token]):
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
        self.ack_overlap_ids = max(32, int(self.ack_overlap_ids))
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

    def _normalize_ack_state(self, state: Dict[str, Any]) -> bool:
        """Compact one room while retaining exact ACKs in the overlap window."""
        floor = int(state.get("floor", 0))
        successful = set(state.get("successful", set()))
        original_floor = floor
        original_successful = set(successful)
        if successful:
            floor = max(floor, max(successful) - self.ack_overlap_ids)
        successful = {value for value in successful if value > floor}
        if len(successful) > self.ack_retention_count:
            kept = sorted(successful)[-self.ack_retention_count:]
            floor = max(floor, kept[0] - 1)
            successful = set(kept)
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
            snapshot = {
                "floor": floor,
                "successful": set(old_successful),
                "initialized": state.get("initialized") is True,
                "last_seen": int(state.get("last_seen", 0)),
                "active": state.get("active") is True,
            }
            successful = set(old_successful)
            successful.add(numeric_id)
            highest = max(successful)
            new_floor = max(floor, highest - self.ack_overlap_ids)
            successful = {value for value in successful if value > new_floor}
            if len(successful) > self.ack_retention_count:
                kept = sorted(successful)[-self.ack_retention_count:]
                new_floor = max(new_floor, kept[0] - 1)
                successful = set(kept)
            state["floor"] = new_floor
            state["successful"] = successful
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
        successful = {
            message_id for value in state.get("successful", set())
            if (message_id := _strict_talk_message_id(value)) is not None
        }
        if not successful:
            return floor
        return max(floor, max(successful) - self.ack_overlap_ids)

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

    async def _initialize_room(self, room_token: str) -> None:
        self._ensure_ack_runtime()
        self._initializing_rooms.add(room_token)
        try:
            messages = await self._fetch_initial_backlog(room_token, self.initial_backlog_limit)
            if self.initial_backlog_limit == 0:
                # Explicit opt-out: establish a cursor at latest, with a visible warning.
                latest = await self._fetch_initial_backlog(room_token, 1)
                if latest:
                    self._commit_cursor(room_token, _strict_talk_message_id(latest[-1].get("id")))
                logger.warning("[nextcloud_talk] Initial backlog disabled for room %s", self._safe_room_token(room_token))
                self._mark_room_initialized(room_token)
                return
            for message in messages:
                await self._handle_talk_message(message, room_token, await_completion=True)
            self._mark_room_initialized(room_token)
        finally:
            self._initializing_rooms.discard(room_token)

    async def _poll_loop(self) -> None:
        assert self._client is not None
        backoff = 1.0
        while self._running:
            try:
                if self.auto_discover_rooms:
                    await self._refresh_discovered_rooms()
                # Poll all rooms concurrently
                tasks = []
                for token in self.room_tokens:
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
            state = self._ack_rooms.get(token)
            if state is not None and state.get("active") is not False:
                self._touch_ack_room(token, active=False)
                ack_activity_changed = True
        for token in new_tokens:
            if not self._is_room_initialized(token):
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
            if self._inflight_message_ids.get(room_token):
                return
            messages = await self._client.get_messages(
                room_token,
                last_known_id=self._poll_anchor(room_token),
                look_into_future=True,
                timeout=self.poll_timeout,
            )
            max_batch = getattr(self, "max_poll_batch", _DEFAULT_MAX_POLL_BATCH)
            if not isinstance(messages, list):
                raise NextcloudTalkAPIError("chat response data is not a list", category="protocol")
            if len(messages) > max_batch:
                raise NextcloudTalkAPIError("chat response exceeds poll batch limit", category="overflow")
            normalized: Dict[int, dict] = {}
            malformed = 0
            for msg in messages:
                if not isinstance(msg, dict):
                    malformed += 1
                    continue
                message_id = _strict_talk_message_id(msg.get("id"))
                if message_id is None:
                    malformed += 1
                    continue
                if not self._is_acknowledged(room_token, message_id):
                    normalized[message_id] = msg
            if malformed:
                logger.warning(
                    "[nextcloud_talk] Ignored %d poll message(s) with malformed/missing IDs in room %s",
                    malformed, self._safe_room_token(room_token),
                )
            for message_id in sorted(normalized):
                msg = normalized[message_id]
                await self._handle_talk_message(msg, room_token)
            # Rate-limit: if we got a full batch (say >5), delay slightly so we don't
            # DOS the server on backlog catch-up
            if len(messages) > 5:
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
        metadata["nextcloud_talk_processing_outcome"] = getattr(
            ProcessingOutcome.FAILURE, "value", str(ProcessingOutcome.FAILURE)
        )
        completion_event = metadata.get("nextcloud_talk_completion_event")
        if isinstance(completion_event, asyncio.Event):
            completion_event.set()
        return True

    async def _completion_watchdog(
        self, event: MessageEvent, room_token: str, numeric_id: int, generation: int
    ) -> None:
        key = (room_token, numeric_id, generation)
        try:
            await asyncio.sleep(self.processing_timeout)
            if self._generation_is_current(key):
                logger.warning(
                    "[nextcloud_talk] Processing completion timed out for room %s; "
                    "leaving message retryable",
                    self._safe_room_token(room_token),
                )
                self._finish_without_ack(event, room_token, numeric_id, generation)
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
            reply_required = metadata.get("nextcloud_talk_reply_required") is True
            delivery_succeeded = self._generation_outcomes.get(key, {}).get(
                "delivery_succeeded"
            ) is True
            if not reply_required or delivery_succeeded:
                await self.on_processing_complete(event, ProcessingOutcome.SUCCESS)
            else:
                self._finish_without_ack(event, room_token, numeric_id, generation)
            return
        self._finish_without_ack(event, room_token, numeric_id, generation)

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
        inflight = self._inflight_message_ids.setdefault(room_token, set())
        # One dispatched turn per room. This avoids Hermes's lossy busy
        # coalescing for ACK-bearing events; later IDs remain on Talk and
        # are picked up after this completion releases the room.
        if numeric_id in inflight or inflight:
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

        source = self.build_source(
            chat_id=room_token,
            chat_name=f"Nextcloud Talk {self._safe_room_token(room_token)}",
            chat_type=chat_type,
            user_id=actor_id or actor_name,
            user_name=actor_name,
        )
        parent = msg.get("parent")
        reply_fields: Dict[str, Any] = {}
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
            },
            **reply_fields,
        )
        if generation_key is not None:
            self._inflight_message_ids.setdefault(room_token, set()).add(numeric_id)
            self._inflight_generations[generation_key] = event
            self._generation_outcomes[generation_key] = {}
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
                raise RuntimeError(
                    "Nextcloud Talk startup message processing timed out"
                ) from exc
            expected_success = getattr(
                ProcessingOutcome.SUCCESS, "value", str(ProcessingOutcome.SUCCESS)
            )
            if event.metadata.get("nextcloud_talk_processing_outcome") != expected_success:
                raise RuntimeError("Nextcloud Talk startup message processing failed")

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
        metadata["nextcloud_talk_processing_outcome"] = getattr(
            effective_outcome, "value", str(effective_outcome)
        )
        completion_event = metadata.get("nextcloud_talk_completion_event")
        if isinstance(completion_event, asyncio.Event):
            completion_event.set()
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
        chunks = self._split_message(_strip_rich_text(content))
        message_ids: List[str] = []
        try:
            for chunk in chunks:
                reference_id = str(uuid.uuid4())
                result = await self._client.send_message(
                    target_room, chunk, reply_to=reply_to, reference_id=reference_id
                )
                talk_id = _strict_talk_message_id(
                    result.get("id") if isinstance(result, dict) else None
                )
                if talk_id is None:
                    raise NextcloudTalkAPIError(
                        "Talk send returned an invalid message ID", category="protocol"
                    )
                message_ids.append(str(talk_id))
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

    def _split_message(self, content: str) -> List[str]:
        content = content or ""
        if len(content) <= self.max_message_length:
            return [content]
        chunks: List[str] = []
        remaining = content
        while remaining:
            chunk = remaining[: self.max_message_length]
            split_at = max(chunk.rfind("\n"), chunk.rfind(" "))
            if split_at > self.max_message_length // 3:
                chunk = remaining[:split_at]
            chunks.append(chunk.strip())
            remaining = remaining[len(chunk):].lstrip()
        return chunks or [""]

    async def send_typing(self, chat_id: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        return None

    async def _send_file_attachment(self, chat_id: str, local_path: str, caption: Optional[str] = None,
                                    reply_to: Optional[str] = None) -> SendResult:
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
            upload_result = await self._client.upload_and_share_file(
                str(path), target_room, self.upload_folder,
                caption=caption, reply_to=reply_to,
            )
            if len(upload_result) == 4:
                remote_path, share, native_talk_share, atomic_metadata = upload_result
            else:  # compatibility with older test/provider wrappers
                remote_path, share, native_talk_share = upload_result
                atomic_metadata = False
            attachment_delivered = True
            link = share.get("url") if isinstance(share, dict) else None
            delivery_result: Optional[SendResult] = None
            if native_talk_share and (caption or reply_to) and not atomic_metadata:
                delivery_result = await self.send(target_room, caption, reply_to=reply_to)
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
                    target_room, f"{caption or path.name}\n{link}", reply_to=reply_to
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
            return await self.send(chat_id, text, reply_to=reply_to, metadata=metadata)
        if image_ref.startswith("file://"):
            image_ref = parse.unquote(parse.urlparse(image_ref).path)
        return await self._send_file_attachment(chat_id, image_ref, caption=caption, reply_to=reply_to)

    async def send_image_file(self, chat_id: str, image_path: str, caption: Optional[str] = None,
                              reply_to: Optional[str] = None, **kwargs) -> SendResult:
        return await self._send_file_attachment(chat_id, image_path, caption=caption, reply_to=reply_to)

    async def send_document(self, chat_id: str, document_path: Optional[str] = None, caption: Optional[str] = None,
                            reply_to: Optional[str] = None, **kwargs) -> SendResult:
        file_path = str(document_path or kwargs.get("file_path") or kwargs.get("path") or "")
        if file_path.startswith(("http://", "https://")):
            text = f"{caption or 'Document'}: {file_path}"
            return await self.send(chat_id, text, reply_to=reply_to)
        if file_path.startswith("file://"):
            file_path = parse.unquote(parse.urlparse(file_path).path)
        return await self._send_file_attachment(chat_id, file_path, caption=caption, reply_to=reply_to)

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        room_type = "dm" if self._room_types.get(chat_id) == 1 else "group"
        return {"name": f"Nextcloud Talk {self._safe_room_token(chat_id)}", "type": room_type, "chat_id": chat_id}


def _configured_values(config: Optional[PlatformConfig] = None) -> tuple[str, str, str, str]:
    extra = getattr(config, "extra", {}) or {}
    return (
        os.getenv("NEXTCLOUD_TALK_URL") or extra.get("url", ""),
        os.getenv("NEXTCLOUD_TALK_USERNAME") or extra.get("username", ""),
        os.getenv("NEXTCLOUD_TALK_PASSWORD") or extra.get("password", ""),
        os.getenv("NEXTCLOUD_TALK_ROOM_TOKENS") or extra.get("room_tokens", "")
        or os.getenv("NEXTCLOUD_TALK_ROOM_TOKEN") or extra.get("room_token", ""),
    )


def check_requirements() -> bool:
    return True


def validate_config(config) -> bool:
    url, username, password, tokens = _configured_values(config)
    extra = getattr(config, "extra", {}) or {}
    allow_insecure = _truthy(
        os.getenv("NEXTCLOUD_TALK_ALLOW_INSECURE_HTTP"),
        bool(extra.get("allow_insecure_http", False)),
    )
    auto_discover = _truthy(
        os.getenv("NEXTCLOUD_TALK_AUTO_DISCOVER_ROOMS"),
        bool(extra.get("auto_discover_rooms", True)),
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
            "You are chatting via Nextcloud Talk (multi-room). Plain text and simple links work best. "
            "Avoid complex markdown tables. Keep responses concise in busy group rooms."
        ),
    )
