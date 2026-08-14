"""
Nextcloud Talk platform adapter for Hermes Agent.

Multi-room adapter that polls and responds in multiple Talk rooms simultaneously.

Configuration via ~/.hermes/.env:
    NEXTCLOUD_TALK_URL=https://cloud.example.com
    NEXTCLOUD_TALK_USERNAME=hermes-bot
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
    NEXTCLOUD_TALK_MAX_OUTBOUND_FILE_BYTES=26214400
    NEXTCLOUD_TALK_ALLOW_PUBLIC_SHARE_FALLBACK=false
    NEXTCLOUD_TALK_AUTO_DISCOVER_ROOMS=true
    NEXTCLOUD_TALK_DISCOVERY_INTERVAL=30
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import mimetypes
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
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
    SendResult,
)

logger = logging.getLogger(__name__)

_DEFAULT_POLL_TIMEOUT = 30
_DEFAULT_MAX_MESSAGE_LENGTH = 32000
_DEFAULT_MAX_DOWNLOAD_BYTES = 25 * 1024 * 1024
_DEFAULT_MAX_UPLOAD_BYTES = 25 * 1024 * 1024
_DEFAULT_INITIAL_BACKLOG = 50
_STREAM_CHUNK_SIZE = 64 * 1024


def _truthy(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _csv_set(value: str) -> set[str]:
    return {part.strip().lower() for part in (value or "").split(",") if part.strip()}


def _strip_rich_text(text: str) -> str:
    """Convert the small markdown-ish subset Hermes often emits to Talk-friendly text."""
    text = re.sub(r"```\w*\n?", "", text or "")
    text = text.replace("```", "")
    text = re.sub(r"!\[([^\]]*)\]\(([^)]+)\)", r"\2", text)
    text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"\1 (\2)", text)
    return text.strip()


class NextcloudTalkAPIError(RuntimeError):
    def __init__(self, message: str, *, category: str = "generic", status_code: Optional[int] = None):
        super().__init__(message)
        self.category = category
        self.status_code = status_code


class AttachmentDownloadError(NextcloudTalkAPIError):
    """Inbound attachment failure that must block cursor commit."""


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


class _SameOriginRedirectHandler(request.HTTPRedirectHandler):
    """Refuse redirects that could leak the authenticated request."""

    def __init__(self, allowed_origin: tuple[str, str, int]):
        self.allowed_origin = allowed_origin

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        resolved = parse.urljoin(req.full_url, newurl)
        if NextcloudTalkClient._origin(resolved) != self.allowed_origin:
            raise error.HTTPError(newurl, code, "cross-origin redirect refused", headers, fp)
        return super().redirect_request(req, fp, code, msg, headers, resolved)


class NextcloudTalkClient:
    """Tiny stdlib OCS client for the Nextcloud Talk chat endpoint."""

    def __init__(self, base_url: str, username: str, password: str, *, timeout: int = 35,
                 max_download_bytes: int = _DEFAULT_MAX_DOWNLOAD_BYTES,
                 max_upload_bytes: int = _DEFAULT_MAX_UPLOAD_BYTES,
                 allow_public_share_fallback: bool = False):
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.timeout = timeout
        self.max_download_bytes = max_download_bytes
        self.max_upload_bytes = max_upload_bytes
        self.allow_public_share_fallback = allow_public_share_fallback
        self._base_origin = self._origin(self.base_url)
        token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
        self._headers = {
            "Authorization": f"Basic {token}",
            "OCS-APIRequest": "true",
            "Accept": "application/json",
            "User-Agent": "Hermes-Agent-Nextcloud-Talk/0.2",
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
        return scheme, hostname.lower(), effective_port

    def _authenticated_url(self, untrusted_url: str) -> Optional[str]:
        try:
            resolved = parse.urljoin(self.base_url + "/", str(untrusted_url or ""))
            return resolved if self._origin(resolved) == self._base_origin else None
        except (TypeError, ValueError):
            return None

    def _open_authenticated(self, req: request.Request):
        opener = request.build_opener(_SameOriginRedirectHandler(self._base_origin))
        return opener.open(req, timeout=self.timeout)

    def _url(self, room_token: str, query: Optional[Dict[str, Any]] = None) -> str:
        encoded_token = parse.quote(str(room_token).strip("/"), safe="")
        url = f"{self.base_url}/ocs/v2.php/apps/spreed/api/v1/chat/{encoded_token}"
        if query:
            cleaned = {k: v for k, v in query.items() if v is not None}
            url += "?" + parse.urlencode(cleaned)
        return url

    def _request(self, method: str, room_token: str, *, query: Optional[Dict[str, Any]] = None,
                 data: Optional[Dict[str, Any]] = None) -> Any:
        body = None
        headers = dict(self._headers)
        if data is not None:
            body = parse.urlencode(data).encode("utf-8")
            headers["Content-Type"] = "application/x-www-form-urlencoded"

        req = request.Request(self._url(room_token, query), data=body, headers=headers, method=method)
        try:
            with self._open_authenticated(req) as resp:
                payload = resp.read().decode("utf-8", errors="replace")
        except error.HTTPError as exc:
            if exc.code == 304:
                return []
            detail = exc.read().decode("utf-8", errors="replace")[:500]
            raise NextcloudTalkAPIError(f"HTTP {exc.code}: {detail}") from exc
        except error.URLError as exc:
            raise NextcloudTalkAPIError(str(exc)) from exc

        try:
            parsed_payload = json.loads(payload) if payload else {}
        except json.JSONDecodeError as exc:
            raise NextcloudTalkAPIError(f"Invalid JSON response: {payload[:200]!r}") from exc

        if isinstance(parsed_payload, dict) and "ocs" in parsed_payload:
            meta = parsed_payload.get("ocs", {}).get("meta", {}) or {}
            statuscode = int(meta.get("statuscode") or 200)
            if statuscode >= 400:
                message = meta.get("message") or "OCS error"
                raise NextcloudTalkAPIError(f"OCS {statuscode}: {message}")
            return parsed_payload.get("ocs", {}).get("data")
        return parsed_payload

    async def get_messages(self, room_token: str, *, last_known_id: Optional[int],
                           look_into_future: bool, timeout: int, limit: Optional[int] = None) -> List[dict]:
        query = {
            "lookIntoFuture": 1 if look_into_future else 0,
            "timeout": timeout,
            "lastKnownMessageId": last_known_id,
            "limit": limit,
        }
        result = await asyncio.to_thread(self._request, "GET", room_token, query=query)
        return result if isinstance(result, list) else []

    def _ocs_get(self, endpoint: str, query: Optional[Dict[str, Any]] = None) -> Any:
        url = f"{self.base_url}{endpoint}"
        if query:
            cleaned = {key: value for key, value in query.items() if value is not None}
            if cleaned:
                url += "?" + parse.urlencode(cleaned)
        req = request.Request(url, headers=self._headers, method="GET")
        try:
            with self._open_authenticated(req) as resp:
                payload = resp.read().decode("utf-8", errors="replace")
        except error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")[:500]
            raise NextcloudTalkAPIError(f"HTTP {exc.code}: {detail}") from exc
        except error.URLError as exc:
            raise NextcloudTalkAPIError(str(exc)) from exc

        try:
            parsed = json.loads(payload) if payload else {}
        except json.JSONDecodeError as exc:
            raise NextcloudTalkAPIError(f"Invalid JSON response: {payload[:200]!r}") from exc
        if isinstance(parsed, dict) and "ocs" in parsed:
            meta = parsed.get("ocs", {}).get("meta", {}) or {}
            statuscode = int(meta.get("statuscode") or 200)
            if statuscode >= 400:
                raise NextcloudTalkAPIError(
                    f"OCS {statuscode}: {meta.get('message') or 'OCS error'}"
                )
            return parsed.get("ocs", {}).get("data")
        return parsed

    async def list_conversations(self) -> List[dict]:
        conversations = await asyncio.to_thread(
            self._ocs_get, "/ocs/v2.php/apps/spreed/api/v4/room"
        )
        seen: Set[str] = set()
        result: List[dict] = []
        for conversation in conversations if isinstance(conversations, list) else []:
            if not isinstance(conversation, dict):
                continue
            token = str(conversation.get("token") or "").strip()
            try:
                conversation_type = int(conversation.get("type"))
            except (TypeError, ValueError):
                continue
            if token and token not in seen:
                seen.add(token)
                result.append({"token": token, "type": conversation_type})
        return result

    async def list_conversation_tokens(self) -> List[str]:
        return [room["token"] for room in await self.list_conversations() if room["type"] == 1]

    async def send_message(self, room_token: str, message: str, *, reply_to: Optional[str] = None) -> Any:
        data = {"message": message}
        if reply_to:
            data["replyTo"] = reply_to
        return await asyncio.to_thread(self._request, "POST", room_token, data=data)

    def _dav_url(self, remote_path: str) -> str:
        parts = [parse.quote(part, safe="") for part in remote_path.strip("/").split("/") if part]
        user = parse.quote(self.username, safe="")
        return f"{self.base_url}/remote.php/dav/files/{user}/" + "/".join(parts)

    def _raw_request(self, method: str, url: str, *, data: Any = None,
                     headers: Optional[Dict[str, str]] = None) -> tuple[int, bytes, Dict[str, str]]:
        req_headers = dict(self._headers)
        req_headers.update(headers or {})
        req = request.Request(url, data=data, headers=req_headers, method=method)
        try:
            with self._open_authenticated(req) as resp:
                return resp.status, resp.read(), dict(resp.headers)
        except error.HTTPError as exc:
            detail = exc.read()[:500]
            raise NextcloudTalkAPIError(
                f"HTTP {exc.code}: {detail.decode('utf-8', errors='replace')}"
            ) from exc
        except error.URLError as exc:
            raise NextcloudTalkAPIError(str(exc)) from exc

    def _ensure_folder_sync(self, folder: str) -> None:
        current = ""
        for part in [p for p in folder.strip("/").split("/") if p]:
            current = f"{current}/{part}"
            req = request.Request(self._dav_url(current), headers=self._headers, method="MKCOL")
            try:
                with self._open_authenticated(req) as resp:
                    if resp.status not in (201, 405):
                        raise NextcloudTalkAPIError(f"MKCOL {current}: HTTP {resp.status}")
            except error.HTTPError as exc:
                if exc.code != 405:
                    detail = exc.read().decode("utf-8", errors="replace")[:500]
                    raise NextcloudTalkAPIError(f"MKCOL {current}: HTTP {exc.code}: {detail}") from exc

    @staticmethod
    def _safe_filename(name: str) -> str:
        name = Path(name or "file").name
        name = re.sub(r"[^A-Za-z0-9._ -]+", "_", name).strip(" .")
        return name or "file"

    def _download_file(self, url: str, download_dir: str) -> str:
        resolved = self._authenticated_url(url)
        if not resolved:
            raise AttachmentDownloadError(
                "refused non-same-origin attachment URL", category="security"
            )
        target_dir = Path(download_dir).expanduser()
        target_dir.mkdir(parents=True, exist_ok=True)
        headers = dict(self._headers)
        headers["Accept"] = "*/*"
        temp_path: Optional[Path] = None
        try:
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
                if declared_size is not None and declared_size > self.max_download_bytes:
                    raise AttachmentDownloadError("inbound file exceeds size limit", category="overflow")
                fname = self._safe_filename(self._extract_filename(resolved, resp.headers))
                local = target_dir / fname
                counter = 1
                while local.exists():
                    local = target_dir / f"{Path(fname).stem}_{counter}{Path(fname).suffix}"
                    counter += 1
                temp_path = local.with_name(local.name + ".part")
                total = 0
                with temp_path.open("wb") as output:
                    while True:
                        chunk = resp.read(_STREAM_CHUNK_SIZE)
                        if not chunk:
                            break
                        total += len(chunk)
                        if total > self.max_download_bytes:
                            raise AttachmentDownloadError(
                                "inbound file exceeds size limit", category="overflow"
                            )
                        output.write(chunk)
                temp_path.replace(local)
                logger.info("[nextcloud_talk] Downloaded attachment -> %s (%d bytes)", local, total)
                return str(local)
        except (error.HTTPError, error.URLError, TimeoutError, OSError,
                NextcloudTalkAPIError, ValueError) as exc:
            if temp_path:
                try:
                    temp_path.unlink(missing_ok=True)
                except OSError:
                    logger.warning("[nextcloud_talk] Could not remove partial attachment %s", temp_path)
            if isinstance(exc, AttachmentDownloadError):
                raise
            category = "network" if isinstance(
                exc, (error.HTTPError, error.URLError, TimeoutError)
            ) else "io"
            raise AttachmentDownloadError(
                f"attachment download failed: {exc}", category=category
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

    @staticmethod
    def _save_to_cache(content: bytes, fname: str, download_dir: str) -> str:
        Path(download_dir).expanduser().mkdir(parents=True, exist_ok=True)
        fname = NextcloudTalkClient._safe_filename(fname)
        local = Path(download_dir).expanduser() / fname
        counter = 1
        stem = local.stem
        ext = local.suffix
        while local.exists():
            local = Path(local.parent, f"{stem}_{counter}{ext}")
            counter += 1
        local.write_bytes(content)
        logger.info("[nextcloud_talk] Downloaded %s -> %s (%d bytes)", local, local, len(content))
        return str(local)

    def _upload_file_sync(self, local_path: str, upload_folder: str) -> str:
        path = Path(local_path).expanduser()
        if not path.is_file():
            raise NextcloudTalkAPIError(f"File not found: {local_path}")
        size = path.stat().st_size
        if size > self.max_upload_bytes:
            raise NextcloudTalkAPIError(
                f"outbound file exceeds size limit ({size} > {self.max_upload_bytes} bytes)"
            )
        folder = "/" + upload_folder.strip("/") if upload_folder else "/Hermes Uploads"
        self._ensure_folder_sync(folder)
        filename = self._safe_filename(path.name)
        stamp = time.strftime("%Y%m%d-%H%M%S")
        remote_path = f"{folder.rstrip('/')}/{stamp}-{filename}"
        content_type = mimetypes.guess_type(str(path))[0] or "application/octet-stream"
        headers = {"Content-Type": content_type, "Content-Length": str(size)}
        # urllib streams file-like request bodies when Content-Length is supplied.
        with path.open("rb") as stream:
            status, _body, _headers = self._raw_request(
                "PUT", self._dav_url(remote_path), data=stream, headers=headers
            )
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
                payload = resp.read().decode("utf-8", errors="replace")
        except error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")[:500]
            category = (
                "permission" if exc.code in {401, 403}
                else "capability" if exc.code in {404, 405, 501}
                else "generic"
            )
            raise NextcloudTalkAPIError(
                f"HTTP {exc.code}: {detail}", category=category, status_code=exc.code
            ) from exc
        except error.URLError as exc:
            raise NextcloudTalkAPIError(str(exc), category="network") from exc
        try:
            parsed = json.loads(payload) if payload else {}
        except json.JSONDecodeError as exc:
            raise NextcloudTalkAPIError(f"Invalid JSON response: {payload[:200]!r}") from exc
        meta = parsed.get("ocs", {}).get("meta", {}) if isinstance(parsed, dict) else {}
        statuscode = int(meta.get("statuscode") or 200)
        if statuscode >= 400:
            message = str(meta.get("message") or "OCS error")
            lowered = message.lower()
            category = (
                "permission" if statuscode in {401, 403}
                else "capability" if statuscode in {404, 405, 501} or (
                    statuscode == 400 and any(
                        word in lowered for word in ("unsupported", "not supported", "capability")
                    )
                )
                else "generic"
            )
            raise NextcloudTalkAPIError(
                f"OCS {statuscode}: {message}", category=category, status_code=statuscode
            )
        return parsed.get("ocs", {}).get("data") if isinstance(parsed, dict) else parsed

    def _share_file_to_talk_sync(self, remote_path: str, room_token: str) -> Any:
        return self._ocs_post(
            "/ocs/v2.php/apps/files_sharing/api/v1/shares",
            {"path": remote_path, "shareType": 10, "shareWith": room_token},
        )

    def _create_public_share_sync(self, remote_path: str) -> Any:
        return self._ocs_post(
            "/ocs/v2.php/apps/files_sharing/api/v1/shares",
            {"path": remote_path, "shareType": 3},
        )

    async def upload_and_share_file(self, local_path: str, room_token: str, upload_folder: str) -> tuple[str, Any, bool]:
        remote_path = await asyncio.to_thread(self._upload_file_sync, local_path, upload_folder)
        try:
            share = await asyncio.to_thread(self._share_file_to_talk_sync, remote_path, room_token)
            return remote_path, share, True
        except NextcloudTalkAPIError as exc:
            if not self.allow_public_share_fallback:
                raise
            if exc.category not in {"capability", "permission"}:
                raise
            logger.warning(
                "[nextcloud_talk] Native Talk share failed; explicit public-share fallback is enabled"
            )
            share = await asyncio.to_thread(self._create_public_share_sync, remote_path)
            return remote_path, share, False


class NextcloudTalkAdapter(BasePlatformAdapter):
    MAX_MESSAGE_LENGTH = _DEFAULT_MAX_MESSAGE_LENGTH
    splits_long_messages = True

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
        allowed_cfg = extra.get("allowed_users", [])
        if allowed_env:
            self.allowed_users = _csv_set(allowed_env)
        elif isinstance(allowed_cfg, list):
            self.allowed_users = {str(u).strip().lower() for u in allowed_cfg if str(u).strip()}
        else:
            self.allowed_users = _csv_set(str(allowed_cfg or ""))
        self.allow_all = _truthy(os.getenv("NEXTCLOUD_TALK_ALLOW_ALL_USERS"), bool(extra.get("allow_all_users", False)))

        max_len = os.getenv("NEXTCLOUD_TALK_MAX_MESSAGE_LENGTH") or extra.get("max_message_length")
        self.max_message_length = int(max_len or _DEFAULT_MAX_MESSAGE_LENGTH)

        self._client: Optional[NextcloudTalkClient] = None
        self._poll_task: Optional[asyncio.Task] = None
        # Per-room last message IDs
        self._last_message_ids: Dict[str, int] = {}
        self._cursor_path = get_hermes_home() / "cache" / "nextcloud_talk" / "cursors.json"
        self._last_discovery_at = 0.0
        self._running = False

    @property
    def name(self) -> str:
        return "Nextcloud Talk"

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
        )

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
                if token not in self._last_message_ids:
                    await self._initialize_room(token)
            self._running = True
            self._poll_task = asyncio.create_task(self._poll_loop())
            rooms_safe = ", ".join(self._safe_room_token(t) for t in self.room_tokens)
            logger.info("[nextcloud_talk] Connected to rooms: %s", rooms_safe)
            self._mark_connected()
            return True
        except Exception as exc:
            logger.error("[nextcloud_talk] Failed to connect: %s", exc)
            self._set_fatal_error("connect_failed", str(exc), retryable=True)
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
        self._mark_disconnected()
        logger.info("[nextcloud_talk] Disconnected")

    def _load_cursors(self) -> None:
        try:
            payload = json.loads(self._cursor_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                for token, value in payload.items():
                    try:
                        numeric_value = int(value)
                    except (TypeError, ValueError):
                        logger.warning(
                            "[nextcloud_talk] Ignoring invalid cursor entry for room %s",
                            self._safe_room_token(str(token)),
                        )
                        continue
                    if numeric_value >= 0:
                        self._last_message_ids[str(token)] = numeric_value
        except FileNotFoundError:
            return
        except Exception as exc:
            logger.warning("[nextcloud_talk] Ignoring invalid cursor cache: %s", exc)

    def _persist_cursors(self) -> None:
        path = self._cursor_path
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.with_suffix(".tmp")
        temp.write_text(json.dumps(self._last_message_ids, sort_keys=True), encoding="utf-8")
        temp.replace(path)

    def _commit_cursor(self, room_token: str, numeric_id: Optional[int]) -> None:
        if numeric_id is None:
            return
        current = self._last_message_ids.get(room_token)
        if current is None or numeric_id > current:
            self._last_message_ids[room_token] = numeric_id
            self._persist_cursors()

    async def _fetch_initial_backlog(self, room_token: str, limit: Optional[int]) -> List[dict]:
        """Fetch newest bounded history (or all history) in backward pages."""
        assert self._client is not None
        remaining = limit
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
                try:
                    message_id = int(message.get("id"))
                except (TypeError, ValueError):
                    continue
                if message_id not in collected:
                    added += 1
                collected[message_id] = message
                valid.append(message_id)
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
        messages = await self._fetch_initial_backlog(room_token, self.initial_backlog_limit)
        if self.initial_backlog_limit == 0:
            # Explicit opt-out: establish a cursor at latest, with a visible warning.
            latest = await self._fetch_initial_backlog(room_token, 1)
            if latest:
                self._commit_cursor(room_token, int(latest[-1]["id"]))
            else:
                self._commit_cursor(room_token, 0)
            logger.warning("[nextcloud_talk] Initial backlog disabled for room %s", self._safe_room_token(room_token))
            return
        for message in messages:
            await self._handle_talk_message(message, room_token)
        if not messages and room_token not in self._last_message_ids:
            self._commit_cursor(room_token, 0)

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
                await asyncio.sleep(0.2)  # tiny breather between poll cycles
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("[nextcloud_talk] Poll loop error: %s", exc)
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
        discovered_set = set(discovered) - configured_set
        previous_discovered = set(self._discovered_room_tokens)
        new_tokens = [token for token in discovered if token in discovered_set and token not in previous_discovered]
        removed_tokens = previous_discovered - discovered_set

        self._discovered_room_tokens = discovered_set
        self.room_tokens = configured + [token for token in discovered if token not in configured_set]
        for token in removed_tokens:
            # Keep persisted cursors so rejoining a room does not replay ancient history.
            self._room_types.pop(token, None)
        for token in new_tokens:
            if token not in self._last_message_ids:
                await self._initialize_room(token)
            logger.info("[nextcloud_talk] Auto-discovered DM %s", self._safe_room_token(token))

    async def _poll_room(self, room_token: str) -> None:
        assert self._client is not None
        try:
            messages = await self._client.get_messages(
                room_token,
                last_known_id=self._last_message_ids.get(room_token),
                look_into_future=True,
                timeout=self.poll_timeout,
            )
            committed = self._last_message_ids.get(room_token, 0)
            normalized: Dict[int, dict] = {}
            malformed = 0
            for msg in messages:
                if not isinstance(msg, dict):
                    malformed += 1
                    continue
                try:
                    message_id = int(msg.get("id"))
                except (TypeError, ValueError):
                    malformed += 1
                    continue
                if message_id > committed:
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
            if "304" in str(exc) or "HTTP 304" in str(exc):
                pass  # No new messages — normal
            else:
                logger.warning("[nextcloud_talk] Poll room %s failed: %s",
                               self._safe_room_token(room_token), exc)
        except Exception as exc:
            logger.warning("[nextcloud_talk] Poll room %s failed: %s",
                           self._safe_room_token(room_token), exc)

    async def _handle_talk_message(self, msg: Dict[str, Any], room_token: str) -> None:
        msg_id = msg.get("id")
        try:
            numeric_id = int(msg_id)
        except (TypeError, ValueError):
            numeric_id = None

        actor_id = str(msg.get("actorId") or "")
        actor_name = str(msg.get("actorDisplayName") or actor_id or "")
        actor_type = str(msg.get("actorType") or "")
        system_message = str(msg.get("systemMessage") or "")
        text = str(msg.get("message") or "").strip()

        file_refs = msg.get("messageParameters", {}) or {}
        if (not text and not file_refs) or system_message:
            self._commit_cursor(room_token, numeric_id)
            return
        if actor_id and actor_id.lower() == self.username.lower():
            self._commit_cursor(room_token, numeric_id)
            return
        if actor_name and actor_name.lower() == self.username.lower():
            self._commit_cursor(room_token, numeric_id)
            return
        if actor_type and actor_type.lower() not in {"users", "guests"}:
            self._commit_cursor(room_token, numeric_id)
            return
        if not self._is_allowed(actor_id, actor_name):
            logger.debug("[nextcloud_talk] Ignoring unauthorized user %s", actor_id or actor_name)
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
        for key, param in file_refs.items():
            if isinstance(param, dict) and param.get("type") == "file":
                fname = param.get("name", "file")
                mimetype = str(param.get("mimetype") or mimetypes.guess_type(fname)[0] or "application/octet-stream")
                file_link = param.get("link", "")
                file_path = param.get("path", "")
                if file_path and self._client:
                    dav_url = self._client._dav_url(file_path)
                    local_path = await asyncio.to_thread(
                        self._client._download_file, dav_url, download_dir
                    )
                    if local_path:
                        media_urls.append(local_path)
                        media_types.append(mimetype)
                        text = text.replace(
                            "{" + key + "}",
                            f"[{fname}](file://{local_path})",
                        )
                        continue
                if file_link:
                    local_path = await asyncio.to_thread(
                        self._client._download_file, file_link, download_dir
                    ) if self._client else None
                    if local_path:
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
            chat_type="dm" if self._room_types.get(room_token) == 1 else "group",
            user_id=actor_id or actor_name,
            user_name=actor_name,
        )
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
        )
        await self.handle_message(event)
        self._commit_cursor(room_token, numeric_id)

    def _is_allowed(self, actor_id: str, actor_name: str) -> bool:
        if self.allow_all:
            return True
        stable_id = str(actor_id or "").strip().lower()
        return bool(stable_id and stable_id in self.allowed_users)

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
        t = str(token or "") or str(self.room_tokens[0] if self.room_tokens else "")
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
                result = await self._client.send_message(target_room, chunk, reply_to=reply_to)
                if isinstance(result, dict):
                    if result.get("id") is not None:
                        message_ids.append(str(result["id"]))
                await asyncio.sleep(0.15)
            return SendResult(
                success=True,
                message_id=message_ids[-1] if message_ids else None,
                continuation_message_ids=tuple(message_ids[:-1]),
            )
        except Exception as exc:
            return SendResult(
                success=False,
                error=str(exc),
                retryable=not bool(message_ids),
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
            return SendResult(success=False, error=f"File not found: {local_path}")
        size = path.stat().st_size
        if size > self.max_upload_bytes:
            return SendResult(
                success=False,
                error=f"Outbound file exceeds size limit ({size} > {self.max_upload_bytes} bytes)",
            )
        target_room = chat_id or (self.room_tokens[0] if self.room_tokens else "")
        if not target_room:
            return SendResult(success=False, error="No room token configured")
        attachment_delivered = False
        remote_path: Optional[str] = None
        share: Any = None
        try:
            remote_path, share, native_talk_share = await self._client.upload_and_share_file(
                str(path), target_room, self.upload_folder
            )
            attachment_delivered = True
            link = share.get("url") if isinstance(share, dict) else None
            delivery_result: Optional[SendResult] = None
            if native_talk_share and caption:
                delivery_result = await self.send(target_room, caption, reply_to=reply_to)
            elif not native_talk_share:
                if not link:
                    return SendResult(
                        success=False,
                        error="Public-share fallback returned no URL (share already created; partial delivery)",
                        retryable=False,
                        raw_response={
                            "remote_path": remote_path,
                            "share": share,
                            "partial_delivery": True,
                            "attachment_delivered": True,
                        },
                    )
                delivery_result = await self.send(
                    target_room, f"{caption or path.name}\n{link}", reply_to=reply_to
                )
            if delivery_result is not None and not delivery_result.success:
                return SendResult(
                    success=False,
                    error=(delivery_result.error or "Attachment follow-up delivery failed")
                    + " (attachment was already delivered; partial delivery)",
                    retryable=False,
                    message_id=getattr(delivery_result, "message_id", None),
                    continuation_message_ids=getattr(
                        delivery_result, "continuation_message_ids", ()
                    ),
                    raw_response={
                        "remote_path": remote_path,
                        "share": share,
                        "partial_delivery": True,
                        "attachment_delivered": True,
                    },
                )
            raw_response = {
                "remote_path": remote_path,
                "share": share,
                "native_talk_share": native_talk_share,
            }
            if link and not native_talk_share:
                raw_response["fallback_link"] = link
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
            error_message = str(exc)
            raw_response = None
            if attachment_delivered:
                error_message += " (attachment was already delivered; partial delivery)"
                raw_response = {
                    "remote_path": remote_path,
                    "share": share,
                    "partial_delivery": True,
                    "attachment_delivered": True,
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
    auto_discover = _truthy(
        os.getenv("NEXTCLOUD_TALK_AUTO_DISCOVER_ROOMS"),
        bool(extra.get("auto_discover_rooms", True)),
    )
    return bool(url and username and password and (tokens or auto_discover))


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
