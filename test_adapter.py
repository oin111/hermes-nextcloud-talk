import asyncio
import concurrent.futures
import contextlib
import hashlib
import importlib.util
import io
import json
import os
import stat
import sys
import tempfile
import threading
import types
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

# Minimal Hermes stubs for standalone stdlib-only tests.
gateway_config = types.ModuleType("gateway.config")
class Platform:
    def __init__(self, value): self.value = value
class PlatformConfig:
    def __init__(self, extra=None): self.extra = extra or {}
gateway_config.Platform = Platform
gateway_config.PlatformConfig = PlatformConfig

gateway_base = types.ModuleType("gateway.platforms.base")
class BasePlatformAdapter:
    def __init__(self, config, platform):
        self.config, self.platform = config, platform
    def build_source(self, **kwargs): return types.SimpleNamespace(**kwargs)
    async def on_processing_complete(self, event, outcome): return None
class MessageEvent:
    def __init__(self, **kwargs): self.__dict__.update(kwargs)
class MessageType: TEXT = "text"
class ProcessingOutcome:
    SUCCESS = "success"
    FAILURE = "failure"
    CANCELLED = "cancelled"
class SendResult:
    def __init__(self, **kwargs): self.__dict__.update(kwargs)
gateway_base.BasePlatformAdapter = BasePlatformAdapter
gateway_base.MessageEvent = MessageEvent
gateway_base.MessageType = MessageType
gateway_base.ProcessingOutcome = ProcessingOutcome
gateway_base.SendResult = SendResult
# Load the plugin against standalone stubs without leaking them into the process.
# The combined suite imports real Hermes after this module, so both ``gateway``
# and the ordinary ``adapter`` module name must remain untouched.
_stub_names = ("gateway", "gateway.config", "gateway.platforms", "gateway.platforms.base")
_saved_modules = {name: sys.modules.get(name) for name in _stub_names}
try:
    sys.modules["gateway"] = types.ModuleType("gateway")
    sys.modules["gateway.config"] = gateway_config
    sys.modules["gateway.platforms"] = types.ModuleType("gateway.platforms")
    sys.modules["gateway.platforms.base"] = gateway_base
    _adapter_spec = importlib.util.spec_from_file_location(
        "adapter_standalone", Path(__file__).with_name("adapter.py")
    )
    assert _adapter_spec is not None and _adapter_spec.loader is not None
    adapter = importlib.util.module_from_spec(_adapter_spec)
    sys.modules["adapter_standalone"] = adapter
    _adapter_spec.loader.exec_module(adapter)
finally:
    for _name, _module in _saved_modules.items():
        if _module is None:
            sys.modules.pop(_name, None)
        else:
            sys.modules[_name] = _module


class FakeResponse:
    def __init__(self, body=b"data", headers=None, status=200):
        self._body = io.BytesIO(body)
        self.headers = headers or {"Content-Type": "application/octet-stream"}
        self.status = status
    def read(self, size=-1): return self._body.read(size)
    def __enter__(self): return self
    def __exit__(self, *args): return False


class TrackingStream:
    def __init__(self, total, byte=b"x"):
        self.remaining = total
        self.byte = byte
        self.requests = []
    def read(self, size=-1):
        self.requests.append(size)
        if self.remaining <= 0:
            return b""
        amount = self.remaining if size < 0 else min(size, self.remaining)
        self.remaining -= amount
        return self.byte * amount
    def __enter__(self): return self
    def __exit__(self, *_args): return False
    def close(self): pass


class SecurityDownloadTests(unittest.TestCase):
    def setUp(self):
        self.client = adapter.NextcloudTalkClient("https://cloud.example:8443/nc", "bot", "secret")

    def test_cross_origin_link_is_rejected_before_network_and_gets_no_credentials(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(self.client, "_open_authenticated") as opened:
            with self.assertRaises(adapter.AttachmentDownloadError) as raised:
                self.client._download_file("https://evil.example/file", tmp)
            self.assertEqual(raised.exception.category, "security")
            opened.assert_not_called()

    def test_userinfo_scheme_host_and_port_mismatches_are_rejected(self):
        bad = [
            "https://bot@cloud.example:8443/file", "http://cloud.example:8443/file",
            "https://other.example:8443/file", "https://cloud.example/file",
            "file:///etc/passwd",
        ]
        with tempfile.TemporaryDirectory() as tmp, patch.object(self.client, "_open_authenticated") as opened:
            for url in bad:
                with self.assertRaises(adapter.AttachmentDownloadError):
                    self.client._download_file(url, tmp)
            opened.assert_not_called()

    def test_origin_validation_rejects_malformed_empty_and_zero_ports(self):
        for url in (
            "https://cloud.example:", "https://cloud.example:0", "https://[::1]:",
            "https://[::1", "https://cloud example/path", "https://cloud.example:bad",
        ):
            with self.subTest(url=url), self.assertRaises(ValueError):
                adapter.NextcloudTalkClient._origin(url)
        self.assertEqual(
            adapter.NextcloudTalkClient._origin("https://[2001:db8::1]/nc"),
            ("https", "2001:db8::1", 443),
        )
        self.assertEqual(
            adapter.NextcloudTalkClient._origin("http://[::1]:8080/nc"),
            ("http", "::1", 8080),
        )

    def test_same_origin_relative_link_is_authenticated_and_streamed(self):
        response = FakeResponse(b"abcdef", {"Content-Type": "image/png", "Content-Length": "6"})
        captured = {}
        def fake_open(req, **kwargs):
            captured["req"] = req
            return response
        with tempfile.TemporaryDirectory() as tmp, patch.object(self.client, "_open_authenticated", fake_open):
            path = self.client._download_file("/nc/index.php/f/1", tmp)
            self.assertEqual(Path(path).read_bytes(), b"abcdef")
        self.assertTrue(captured["req"].get_header("Authorization").startswith("Basic "))
        self.assertEqual(captured["req"].full_url, "https://cloud.example:8443/nc/index.php/f/1")

    def test_download_size_limit_rejects_header_and_stream_overflow(self):
        self.client.max_download_bytes = 4
        for response in (
            FakeResponse(b"abcde", {"Content-Type": "x", "Content-Length": "5"}),
            FakeResponse(b"abcde", {"Content-Type": "x"}),
        ):
            with tempfile.TemporaryDirectory() as tmp, patch.object(self.client, "_open_authenticated", return_value=response):
                with self.assertRaises(adapter.AttachmentDownloadError) as raised:
                    self.client._download_file("https://cloud.example:8443/nc/f", tmp)
                self.assertEqual(raised.exception.category, "overflow")
                self.assertEqual(list(Path(tmp).iterdir()), [])

    def test_short_declared_body_is_removed_and_retried_before_cache_reuse(self):
        responses = [
            FakeResponse(b"abc", {"Content-Type": "x", "Content-Length": "6"}),
            FakeResponse(b"abcdef", {"Content-Type": "x", "Content-Length": "6"}),
        ]
        with tempfile.TemporaryDirectory() as tmp, patch.object(
            self.client, "_open_authenticated", side_effect=responses
        ) as opened:
            with self.assertRaises(adapter.AttachmentDownloadError) as raised:
                self.client._download_file("/nc/f", tmp, cache_identity="room:short:file")
            self.assertEqual(raised.exception.category, "network")
            self.assertEqual(list(Path(tmp).iterdir()), [])
            completed = self.client._download_file(
                "/nc/f", tmp, cache_identity="room:short:file"
            )
            self.assertEqual(Path(completed).read_bytes(), b"abcdef")
            self.assertEqual(opened.call_count, 2)
            self.assertEqual(
                [path.name for path in Path(tmp).iterdir() if not path.name.endswith(".complete.json")],
                [Path(completed).name],
            )

    def test_body_longer_than_declared_is_rejected_as_content(self):
        response = FakeResponse(b"abcdef", {"Content-Type": "x", "Content-Length": "3"})
        with tempfile.TemporaryDirectory() as tmp, patch.object(
            self.client, "_open_authenticated", return_value=response
        ):
            with self.assertRaises(adapter.AttachmentDownloadError) as raised:
                self.client._download_file("/nc/f", tmp, cache_identity="room:long:file")
            self.assertEqual(raised.exception.category, "content")
            self.assertEqual(list(Path(tmp).iterdir()), [])

    def test_partial_file_is_cleaned_when_stream_read_raises_oserror(self):
        class BrokenResponse(FakeResponse):
            def __init__(self):
                super().__init__(b"first", {"Content-Type": "application/octet-stream"})
                self.calls = 0
            def read(self, size=-1):
                self.calls += 1
                if self.calls == 2:
                    raise OSError("socket read failed")
                return b"first"
        with tempfile.TemporaryDirectory() as tmp, patch.object(
            self.client, "_open_authenticated", return_value=BrokenResponse()
        ):
            with self.assertRaises(adapter.AttachmentDownloadError) as raised:
                self.client._download_file("https://cloud.example:8443/nc/file", tmp)
            self.assertEqual(raised.exception.category, "io")
            self.assertEqual(list(Path(tmp).iterdir()), [])

    def test_direct_raw_request_rejects_cross_origin_before_sending_auth(self):
        received = []

        class Sink(BaseHTTPRequestHandler):
            def do_GET(self):
                received.append(self.headers.get("Authorization"))
                self.send_response(200); self.end_headers(); self.wfile.write(b"ok")
            def log_message(self, *_args): pass

        sink = ThreadingHTTPServer(("127.0.0.1", 0), Sink)
        thread = threading.Thread(target=sink.serve_forever)
        thread.start()
        try:
            client = adapter.NextcloudTalkClient(
                "http://127.0.0.1:9", "bot", "secret", timeout=1
            )
            with self.assertRaises(adapter.NextcloudTalkAPIError) as raised:
                client._raw_request("GET", f"http://127.0.0.1:{sink.server_port}/direct")
            self.assertEqual(raised.exception.category, "security")
            self.assertEqual(received, [])
        finally:
            sink.shutdown(); sink.server_close(); thread.join()

    def test_cross_origin_redirect_never_reaches_sink_with_basic_auth(self):
        observed = {"source": None, "sink": []}

        class Sink(BaseHTTPRequestHandler):
            def do_GET(self):
                observed["sink"].append(self.headers.get("Authorization"))
                self.send_response(200); self.end_headers(); self.wfile.write(b"{}")
            def log_message(self, *_args): pass

        sink = ThreadingHTTPServer(("127.0.0.1", 0), Sink)

        class Source(BaseHTTPRequestHandler):
            def do_GET(self):
                observed["source"] = self.headers.get("Authorization")
                self.send_response(302)
                self.send_header("Location", f"http://127.0.0.1:{sink.server_port}/stolen")
                self.end_headers()
            def log_message(self, *_args): pass

        source = ThreadingHTTPServer(("127.0.0.1", 0), Source)
        threads = [threading.Thread(target=server.serve_forever) for server in (sink, source)]
        for thread in threads:
            thread.start()
        try:
            client = adapter.NextcloudTalkClient(
                f"http://127.0.0.1:{source.server_port}", "bot", "secret", timeout=2
            )
            with self.assertRaises(adapter.NextcloudTalkAPIError):
                client._ocs_get("/redirect")
            self.assertTrue(observed["source"].startswith("Basic "))
            self.assertEqual(observed["sink"], [])
        finally:
            source.shutdown(); sink.shutdown()
            source.server_close(); sink.server_close()
            for thread in threads: thread.join()

    def test_cross_origin_attachment_redirect_remains_deterministic_security_rejection(self):
        class Sink(BaseHTTPRequestHandler):
            received_authorization = []
            def do_GET(self):
                self.received_authorization.append(self.headers.get("Authorization"))
                self.send_response(200); self.end_headers(); self.wfile.write(b"stolen")
            def log_message(self, *_args): pass

        sink = ThreadingHTTPServer(("127.0.0.1", 0), Sink)
        class Source(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(302)
                self.send_header("Location", f"http://127.0.0.1:{sink.server_port}/stolen")
                self.end_headers()
            def log_message(self, *_args): pass

        source = ThreadingHTTPServer(("127.0.0.1", 0), Source)
        threads = [threading.Thread(target=server.serve_forever) for server in (sink, source)]
        for thread in threads:
            thread.start()
        try:
            client = adapter.NextcloudTalkClient(
                f"http://127.0.0.1:{source.server_port}", "bot", "secret", timeout=2
            )
            with tempfile.TemporaryDirectory() as tmp:
                with self.assertRaises(adapter.AttachmentDownloadError) as raised:
                    client._download_file("/attachment", tmp, cache_identity="redirect-test")
                self.assertEqual(raised.exception.category, "security")
                self.assertEqual(list(Path(tmp).iterdir()), [])
            self.assertEqual(Sink.received_authorization, [])
        finally:
            source.shutdown(); sink.shutdown()
            source.server_close(); sink.server_close()
            for thread in threads: thread.join()


class SharePolicyTests(unittest.IsolatedAsyncioTestCase):
    async def test_ambiguous_put_returns_safe_nonretryable_attachment_result(self):
        instance = self._attachment_adapter()
        client = adapter.NextcloudTalkClient("https://cloud.example", "bot", "secret")
        client._ensure_folder_sync = lambda *_args: None
        put_calls = []

        def ambiguous_put(method, url, *, data=None, headers=None):
            put_calls.append((method, url))
            while data.read(64):
                pass
            raise adapter.NextcloudTalkAPIError("network request failed", category="network")

        client._raw_request = ambiguous_put
        instance._client = client
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "private-name.txt"
            source.write_bytes(b"payload")
            result = await instance._send_file_attachment("room", str(source))
        self.assertFalse(result.success)
        self.assertFalse(result.retryable)
        self.assertEqual(result.error, "attachment upload outcome unknown (write timed out)")
        self.assertEqual(result.raw_response, {"category": "ambiguous_upload"})
        self.assertEqual(len(put_calls), 1)
        self.assertNotIn("private-name", repr(result.__dict__))

    async def test_public_share_fallback_is_off_by_default(self):
        client = adapter.NextcloudTalkClient("https://cloud.example", "bot", "secret")
        client._upload_file_sync = lambda *a: "/Hermes/file"
        client._share_file_to_talk_sync = lambda *a: (_ for _ in ()).throw(RuntimeError("native failed"))
        public_calls = []
        client._create_public_share_sync = lambda *a: public_calls.append(a)
        with self.assertRaises(RuntimeError):
            await client.upload_and_share_file("x", "room", "/Hermes")
        self.assertEqual(public_calls, [])

    async def test_public_fallback_only_for_capability_or_permission_errors(self):
        for failure in (
            adapter.NextcloudTalkAPIError("timeout", category="network"),
            adapter.NextcloudTalkAPIError("unexpected", category="generic"),
            RuntimeError("raw transient failure"),
        ):
            client = adapter.NextcloudTalkClient(
                "https://cloud.example", "bot", "secret", allow_public_share_fallback=True
            )
            client._upload_file_sync = lambda *a: "/Hermes/file"
            client._share_file_to_talk_sync = lambda *a, failure=failure: (_ for _ in ()).throw(failure)
            public_calls = []
            client._create_public_share_sync = lambda *a: public_calls.append(a)
            with self.subTest(failure=failure), self.assertRaises(type(failure)):
                await client.upload_and_share_file("x", "room", "/Hermes")
            self.assertEqual(public_calls, [])

        client = adapter.NextcloudTalkClient(
            "https://cloud.example", "bot", "secret", allow_public_share_fallback=True
        )
        client._upload_file_sync = lambda *a: "/Hermes/file"
        client._share_file_to_talk_sync = lambda *a: (_ for _ in ()).throw(
            adapter.NextcloudTalkAPIError("forbidden", category="permission", status_code=403)
        )
        client._create_public_share_sync = lambda *a: {"url": "https://cloud.example/s/ok"}
        _path, share, native, atomic = await client.upload_and_share_file("x", "room", "/Hermes")
        self.assertFalse(native)
        self.assertFalse(atomic)
        self.assertEqual(share["url"], "https://cloud.example/s/ok")

    async def test_failed_public_fallback_is_returned_as_delivery_failure(self):
        instance = self._attachment_adapter()
        instance._client.upload_and_share_file = lambda *a, **k: asyncio.sleep(0, result=("/f", {"url": "https://c/s"}, False))
        instance.send = lambda *a, **k: asyncio.sleep(0, result=SendResult(success=False, error="send failed"))
        result = await instance._send_file_attachment("room", __file__, caption="caption")
        self.assertFalse(result.success)
        self.assertIn("follow-up delivery failed", result.error)
        self.assertFalse(result.retryable)
        self.assertTrue(result.raw_response["partial_delivery"])

    async def test_native_share_carries_caption_and_reply_atomically_without_followup(self):
        instance = self._attachment_adapter()
        calls = []
        async def share(*args, **kwargs):
            calls.append((args, kwargs)); return ("/PRIVATE/REMOTE", {"id": 999}, True, True)
        instance._client.upload_and_share_file = share
        sends = []
        instance.send = lambda *a, **k: sends.append((a, k))
        result = await instance._send_file_attachment(
            "room", __file__, caption="caption", reply_to="44"
        )
        self.assertTrue(result.success)
        self.assertEqual(calls[0][1], {"caption": "caption", "reply_to": "44"})
        self.assertEqual(sends, [])
        self.assertNotIn("PRIVATE", repr(result.raw_response))

    async def test_talk_share_form_encodes_bounded_atomic_metadata(self):
        client = adapter.NextcloudTalkClient("https://cloud.example", "bot", "secret")
        captured = []
        client._ocs_post = lambda endpoint, data: captured.append((endpoint, data)) or {"id": 7}
        client._share_file_to_talk_sync(
            "/PRIVATE/REMOTE", "room", caption="caption", reply_to="44"
        )
        form = captured[0][1]
        self.assertEqual(
            json.loads(form["talkMetaData"]), {"caption": "caption", "replyTo": "44"}
        )

    async def test_sendresult_never_leaks_private_paths_urls_or_provider_objects(self):
        instance = self._attachment_adapter()
        private = "PRIVATE-MARKER-TOKEN"
        instance._client.upload_and_share_file = lambda *a, **k: asyncio.sleep(
            0, result=(f"/remote/{private}", {"url": f"https://x/{private}", private: private}, False, False)
        )
        instance.send = lambda *a, **k: asyncio.sleep(
            0, result=SendResult(success=False, error=f"failed {private}")
        )
        result = await instance._send_file_attachment("room", __file__, caption="caption")
        self.assertNotIn(private, repr(result.__dict__))

    async def test_followup_exception_after_share_is_partial_and_nonretryable(self):
        instance = self._attachment_adapter()
        instance._client.upload_and_share_file = lambda *a, **k: asyncio.sleep(
            0, result=("/f", {"id": 999}, True)
        )
        async def fail_send(*_args, **_kwargs): raise OSError("connection reset")
        instance.send = fail_send
        result = await instance._send_file_attachment("room", __file__, caption="caption")
        self.assertFalse(result.success)
        self.assertFalse(result.retryable)
        self.assertIn("partial delivery", result.error)
        self.assertTrue(result.raw_response["attachment_delivered"])

    @staticmethod
    def _attachment_adapter():
        instance = adapter.NextcloudTalkAdapter.__new__(adapter.NextcloudTalkAdapter)
        instance._client = types.SimpleNamespace()
        instance.room_tokens = ["room"]
        instance.upload_folder = "/Hermes"
        instance.max_upload_bytes = 1024 * 1024
        return instance


class CursorAndDeliveryTests(unittest.IsolatedAsyncioTestCase):
    def make_adapter(self, handler):
        instance = adapter.NextcloudTalkAdapter.__new__(adapter.NextcloudTalkAdapter)
        instance.username = "bot"
        instance.require_mention = False
        instance.allowed_users = set()
        instance.group_allowed_users = set()
        instance.allow_all = True
        instance.bot_name = "Hermes"
        instance._last_message_ids = {}
        instance._room_types = {"dm-room": 1}
        instance._client = None
        instance._persist_cursors = lambda: None
        async def handle_and_complete(event):
            result = await handler(event)
            await instance.on_processing_complete(event, ProcessingOutcome.SUCCESS)
            return result
        instance.handle_message = handle_and_complete
        return instance

    async def test_cursor_remains_retryable_when_handler_raises(self):
        async def fail(_event): raise RuntimeError("boom")
        instance = self.make_adapter(fail)
        msg = {"id": 42, "actorType": "users", "actorId": "alice", "actorDisplayName": "Alice", "message": "hello"}
        with self.assertRaises(RuntimeError): await instance._handle_talk_message(msg, "dm-room")
        self.assertNotIn("dm-room", instance._last_message_ids)

    async def test_dm_type_and_event_actor_fields_are_populated(self):
        handled = []
        instance = self.make_adapter(lambda event: asyncio.sleep(0, result=handled.append(event)))
        msg = {"id": 42, "actorType": "users", "actorId": "alice", "actorDisplayName": "Alice", "message": "hello"}
        await instance._handle_talk_message(msg, "dm-room")
        event = handled[0]
        self.assertEqual(event.source.chat_type, "dm")
        self.assertEqual((event.user_id, event.user_name), ("alice", "Alice"))
        self.assertEqual(instance._last_message_ids["dm-room"], 42)

    async def test_authorization_uses_stable_actor_id_not_display_name(self):
        instance = self.make_adapter(lambda event: asyncio.sleep(0))
        instance.allow_all = False
        instance.allowed_users = {"alice"}
        self.assertTrue(instance._is_allowed("alice", "Changed Name"))
        self.assertFalse(instance._is_allowed("mallory", "alice"))

    async def test_authorization_fails_closed_and_denies_guests_without_stable_id(self):
        instance = self.make_adapter(lambda event: asyncio.sleep(0))
        instance.allow_all = False
        instance.allowed_users = set()
        self.assertFalse(instance._is_allowed("", "Guest"))
        self.assertFalse(instance._is_allowed("alice", "Alice"))
        instance.allowed_users = {"alice"}
        self.assertFalse(instance._is_allowed("", "alice"))
        instance.allow_all = True
        self.assertFalse(instance._is_allowed("", "Guest"))

    async def test_allow_all_requires_supported_actor_type_and_stable_actor_id(self):
        handled = []
        instance = self.make_adapter(lambda event: asyncio.sleep(0, result=handled.append(event.user_id)))
        messages = [
            {"id": 1, "actorType": "users", "actorId": "", "actorDisplayName": "Alice", "message": "missing"},
            {"id": 2, "actorType": "bots", "actorId": "service", "message": "unsupported"},
            {"id": 3, "actorType": "guests", "actorId": "guest/42", "message": "valid"},
        ]
        for message in messages:
            await instance._handle_talk_message(message, "dm-room")
        self.assertEqual(handled, ["guest/42"])
        self.assertEqual(instance._last_message_ids["dm-room"], 3)

    async def test_allow_all_denies_non_string_and_blank_actor_ids_before_normalization(self):
        handled = []
        instance = self.make_adapter(
            lambda event: asyncio.sleep(0, result=handled.append(event.user_id))
        )
        invalid_ids = [123, True, ["alice"], {"id": "alice"}, b"alice", " \t\n"]
        for message_id, actor_id in enumerate(invalid_ids, 1):
            await instance._handle_talk_message(
                {"id": message_id, "actorType": "users", "actorId": actor_id,
                 "actorDisplayName": "Alice", "message": "denied"},
                "dm-room",
            )
        await instance._handle_talk_message(
            {"id": 100, "actorType": "users", "actorId": "alice/stable-42",
             "actorDisplayName": "Alice", "message": "allowed"},
            "dm-room",
        )
        self.assertEqual(handled, ["alice/stable-42"])
        self.assertEqual(instance._last_message_ids["dm-room"], 100)

    async def test_captioned_image_media_reaches_handler(self):
        handled = []
        instance = self.make_adapter(lambda event: asyncio.sleep(0, result=handled.append(event)))
        instance._client = types.SimpleNamespace(
            _dav_url=lambda path: f"https://cloud.example/dav/{path}",
            _download_file=lambda url, download_dir, **kwargs: __file__,
        )
        msg = {"id": 7, "actorType": "users", "actorId": "alice", "actorDisplayName": "Alice",
               "message": "caption", "messageParameters": {"file": {"type": "file", "name": "image.png", "path": "Talk/image.png", "mimetype": "image/png"}}}
        await instance._handle_talk_message(msg, "dm-room")
        self.assertEqual(handled[0].media_urls, [__file__])
        self.assertEqual(handled[0].media_types, ["image/png"])

    async def test_oversized_attachment_is_quarantined_and_cursor_commits(self):
        handled = []
        instance = self.make_adapter(lambda event: asyncio.sleep(0, result=handled.append(event)))
        instance._client = types.SimpleNamespace(
            _dav_url=lambda path: f"https://cloud.example/dav/{path}",
            _download_file=lambda *a, **k: (_ for _ in ()).throw(
                adapter.AttachmentDownloadError("too large", category="overflow")
            ),
        )
        msg = {"id": 8, "actorType": "users", "actorId": "alice", "message": "caption",
               "messageParameters": {"file": {"type": "file", "path": "Talk/a", "link": "/f/a"}}}
        with self.assertLogs(adapter.logger, level="WARNING"):
            await instance._handle_talk_message(msg, "dm-room")
        self.assertEqual(handled, [])
        self.assertEqual(instance._last_message_ids["dm-room"], 8)

    async def test_poll_sorts_deduplicates_skips_cursor_and_ignores_malformed_ids(self):
        seen = []
        instance = self.make_adapter(lambda event: asyncio.sleep(0))
        instance.poll_timeout = 1
        instance._last_message_ids = {"dm-room": 5}
        batch = [
            {"id": 7}, {"id": 5}, {"id": "6"}, {"id": 6}, {"id": 7},
            {"id": True}, {"id": 6.0}, {"id": 0}, {"id": -1},
            {"id": 2**63}, {"id": "bad"}, {}, None,
        ]
        async def get_messages(*_args, **_kwargs): return batch
        instance._client = types.SimpleNamespace(get_messages=get_messages)
        async def record(msg, room): seen.append((msg["id"], room))
        instance._handle_talk_message = record
        with self.assertLogs(adapter.logger, level="WARNING") as logs:
            await instance._poll_room("dm-room")
        self.assertEqual(seen, [(6, "dm-room"), (7, "dm-room")])
        self.assertTrue(any("malformed/missing IDs" in line for line in logs.output))

    async def test_strict_inbound_ids_never_dispatch_or_advance_before_later_valid_message(self):
        seen = []
        instance = self.make_adapter(
            lambda event: asyncio.sleep(0, result=seen.append(event.message_id))
        )
        invalid_ids = [True, False, "12", 12.0, 0, -1, 2**63]
        for invalid_id in invalid_ids:
            await instance._handle_talk_message(
                {"id": invalid_id, "actorType": "users", "actorId": "alice", "message": "bad"},
                "dm-room",
            )
        self.assertEqual(seen, [])
        self.assertNotIn("dm-room", instance._last_message_ids)
        await instance._handle_talk_message(
            {"id": 12, "actorType": "users", "actorId": "alice", "message": "good"},
            "dm-room",
        )
        self.assertEqual(seen, ["12"])
        self.assertEqual(instance._last_message_ids["dm-room"], 12)

    def test_cursor_and_ack_helpers_reject_non_strict_talk_ids(self):
        instance = self.make_adapter(lambda event: asyncio.sleep(0))
        for invalid_id in (True, False, "12", 12.0, 0, -1, 2**63):
            with self.subTest(message_id=invalid_id):
                instance._commit_cursor("dm-room", invalid_id)
                self.assertFalse(instance._is_acknowledged("dm-room", invalid_id))
        self.assertNotIn("dm-room", instance._last_message_ids)

    async def test_poll_equal_cursor_is_not_redispatched(self):
        instance = self.make_adapter(lambda event: asyncio.sleep(0))
        instance.poll_timeout = 1
        instance._last_message_ids = {"dm-room": 9}
        async def get_messages(*_args, **_kwargs): return [{"id": 9}]
        instance._client = types.SimpleNamespace(get_messages=get_messages)
        instance._handle_talk_message = lambda *_args: (_ for _ in ()).throw(
            AssertionError("equal cursor was redispatched")
        )
        await instance._poll_room("dm-room")


class DiscoveryAndBacklogTests(unittest.IsolatedAsyncioTestCase):
    async def test_discovery_retains_metadata_and_marks_type_one_as_dm(self):
        client = adapter.NextcloudTalkClient("https://cloud.example", "bot", "secret")
        client._ocs_get = lambda *a, **k: [{"token": "dm", "type": 1}, {"token": "group", "type": 2}]
        rooms = await client.list_conversations()
        self.assertEqual(rooms, [{"token": "dm", "type": 1}, {"token": "group", "type": 2}])

    async def test_conversation_token_requires_bounded_safe_nonempty_string(self):
        client = adapter.NextcloudTalkClient("https://cloud.example", "bot", "secret")
        invalid_tokens = [
            123, "", "   ", "unsafe/token", "line\nbreak",
            "a" * (adapter._MAX_ROOM_TOKEN_LENGTH + 1), None,
        ]
        for token in invalid_tokens:
            with self.subTest(token=token):
                entry = {"type": 1}
                if token is not None:
                    entry["token"] = token
                client._ocs_get = lambda *a, entry=entry, **k: [entry]
                with self.assertRaises(adapter.NextcloudTalkAPIError) as raised:
                    await client.list_conversations()
                self.assertEqual(raised.exception.category, "protocol")

    async def test_conversation_type_requires_actual_integer_not_string_or_bool(self):
        client = adapter.NextcloudTalkClient("https://cloud.example", "bot", "secret")
        for conversation_type in ("1", True, False, 1.0, None):
            with self.subTest(conversation_type=conversation_type):
                entry = {"token": "SafeToken123"}
                if conversation_type is not None:
                    entry["type"] = conversation_type
                client._ocs_get = lambda *a, entry=entry, **k: [entry]
                with self.assertRaises(adapter.NextcloudTalkAPIError) as raised:
                    await client.list_conversations()
                self.assertEqual(raised.exception.category, "protocol")

    async def test_conversation_type_rejects_values_outside_supported_range(self):
        client = adapter.NextcloudTalkClient("https://cloud.example", "bot", "secret")
        for conversation_type in (0, -1, 7, 999):
            with self.subTest(conversation_type=conversation_type):
                client._ocs_get = lambda *a, conversation_type=conversation_type, **k: [
                    {"token": "SafeToken123", "type": conversation_type}
                ]
                with self.assertRaises(adapter.NextcloudTalkAPIError) as raised:
                    await client.list_conversations()
                self.assertEqual(raised.exception.category, "protocol")

        valid = [
            {"token": "DmToken1", "type": 1},
            {"token": "NoteToSelf6", "type": 6},
        ]
        client._ocs_get = lambda *a, **k: valid
        self.assertEqual(await client.list_conversations(), valid)

    async def test_malformed_ocs_status_uses_fixed_protocol_message_only(self):
        hostile = "PRIVATE-MALFORMED-STATUS-BODY\ncredential=STOLEN"
        payload = {
            "ocs": {
                "meta": {"status": "maybe", "statuscode": 200, "message": hostile},
                "data": [],
            }
        }
        with self.assertRaises(adapter.NextcloudTalkAPIError) as raised:
            adapter.NextcloudTalkClient._ocs_data(payload, expected_types=(list,))
        self.assertEqual(raised.exception.category, "protocol")
        self.assertEqual(raised.exception.status_code, 200)
        self.assertEqual(str(raised.exception), "malformed OCS status (status 200)")
        self.assertNotIn("PRIVATE-MALFORMED-STATUS-BODY", str(raised.exception))
        self.assertFalse(hasattr(raised.exception, "raw_response"))

    async def test_unsuccessful_ocs_body_message_never_reaches_exception_or_poll_log(self):
        client = adapter.NextcloudTalkClient(
            "https://cloud.example", "bot", "PASSWORD-CREDENTIAL-MARKER"
        )
        hostile = "PRIVATE-RESPONSE-BODY\nLOG-INJECTION\u0085credential=STOLEN"
        payload = {
            "ocs": {
                "meta": {"status": "failure", "statuscode": "500", "message": hostile},
                "data": [],
            }
        }
        with self.assertRaises(adapter.NextcloudTalkAPIError) as raised:
            client._ocs_data(payload, expected_types=(list,))
        self.assertEqual(raised.exception.category, "generic")
        self.assertEqual(raised.exception.status_code, 500)
        self.assertEqual(str(raised.exception), "OCS request failed (status 500)")
        self.assertFalse(hasattr(raised.exception, "raw_response"))

        async def fail_poll(*_args, **_kwargs):
            return client._ocs_data(payload, expected_types=(list,))

        instance = adapter.NextcloudTalkAdapter.__new__(adapter.NextcloudTalkAdapter)
        instance._client = types.SimpleNamespace(get_messages=fail_poll)
        instance._last_message_ids = {}
        instance.room_tokens = ["safe-room"]
        instance.poll_timeout = 1
        instance.max_poll_batch = 100
        with self.assertLogs(adapter.logger, level="WARNING") as captured:
            await instance._poll_room("safe-room")
        rendered = "\n".join(captured.output)
        for marker in (
            "PRIVATE-RESPONSE-BODY", "LOG-INJECTION", "STOLEN",
            "PASSWORD-CREDENTIAL-MARKER", "\nLOG-INJECTION", "\u0085",
        ):
            self.assertNotIn(marker, rendered)
        self.assertIn("status 500", rendered)

    async def test_backfill_pages_without_fixed_100_message_truncation(self):
        instance = adapter.NextcloudTalkAdapter.__new__(adapter.NextcloudTalkAdapter)
        calls = []
        pages = {None: [{"id": i} for i in range(205, 105, -1)], 106: [{"id": i} for i in range(105, 5, -1)], 6: [{"id": i} for i in range(5, 0, -1)]}
        async def get_messages(token, **kwargs):
            calls.append(kwargs.get("last_known_id")); return pages.get(kwargs.get("last_known_id"), [])
        instance._client = types.SimpleNamespace(get_messages=get_messages)
        result = await instance._fetch_initial_backlog("room", None)
        self.assertEqual(len(result), 205)
        self.assertEqual([m["id"] for m in result[:2]], [1, 2])
        self.assertEqual(calls, [None, 106, 6])

    async def test_backlog_ignores_non_strict_ids_and_keeps_later_valid_progress(self):
        instance = adapter.NextcloudTalkAdapter.__new__(adapter.NextcloudTalkAdapter)
        instance.room_tokens = ["room"]
        page = [
            {"id": True}, {"id": "12"}, {"id": 12.0}, {"id": 0},
            {"id": -1}, {"id": 2**63}, {"id": 12},
        ]
        instance._client = types.SimpleNamespace(
            get_messages=lambda *_args, **_kwargs: asyncio.sleep(0, result=page)
        )
        result = await instance._fetch_initial_backlog("room", 20)
        self.assertEqual(result, [{"id": 12}])

    async def test_unlimited_backlog_stops_on_repeated_page_with_warning(self):
        instance = adapter.NextcloudTalkAdapter.__new__(adapter.NextcloudTalkAdapter)
        instance.room_tokens = ["room-token"]
        calls = []
        page = [{"id": i} for i in range(100, 0, -1)]
        async def get_messages(*_args, **kwargs):
            calls.append(kwargs.get("last_known_id")); return list(page)
        instance._client = types.SimpleNamespace(get_messages=get_messages)
        with self.assertLogs(adapter.logger, level="WARNING") as logs:
            result = await instance._fetch_initial_backlog("room-token", None)
        self.assertEqual(len(result), 100)
        self.assertEqual(calls, [None, 1])
        self.assertTrue(any("repeated/no-progress" in line for line in logs.output))


class SendResultTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def make_adapter(ids):
        instance = adapter.NextcloudTalkAdapter.__new__(adapter.NextcloudTalkAdapter)
        pending = iter(ids)
        async def send_message(*_args, **_kwargs): return {"id": next(pending)}
        instance._client = types.SimpleNamespace(send_message=send_message)
        instance.room_tokens = ["room"]
        instance.max_message_length = 4
        return instance

    async def test_single_chunk_has_no_continuation_id(self):
        instance = self.make_adapter([11])
        result = await instance.send("room", "one")
        self.assertEqual(result.message_id, "11")
        self.assertEqual(result.continuation_message_ids, ())

    async def test_send_uses_stable_reference_id_and_ambiguous_failure_is_not_retryable(self):
        instance = self.make_adapter([])
        calls = []
        async def ambiguous(*_args, **kwargs):
            calls.append(kwargs)
            raise adapter.NextcloudTalkAPIError("network request failed", category="network")
        instance._client.send_message = ambiguous
        result = await instance.send("room", "one")
        self.assertFalse(result.success)
        self.assertFalse(result.retryable)
        self.assertEqual(result.error, "delivery outcome unknown (write timed out)")
        self.assertEqual(result.raw_response, {"category": "ambiguous_delivery"})
        self.assertEqual(len(calls), 1)
        self.assertRegex(calls[0]["reference_id"], r"^[0-9a-f-]{36}$")

    async def test_send_rejects_malformed_or_nonpositive_talk_message_ids(self):
        for bad_id in (None, True, False, 0, -1, "12", {}, []):
            with self.subTest(message_id=bad_id):
                instance = self.make_adapter([])
                async def malformed(*_args, **_kwargs): return {"id": bad_id}
                instance._client.send_message = malformed
                result = await instance.send("room", "one")
                self.assertFalse(result.success)
                self.assertFalse(result.retryable)
                self.assertIsNone(result.message_id)

    async def test_only_additional_chunks_are_continuation_ids(self):
        instance = self.make_adapter([11, 12, 13])
        result = await instance.send("room", "abcdefghijkl")
        self.assertEqual(result.message_id, "13")
        self.assertEqual(result.continuation_message_ids, ("11", "12"))

    async def test_attachment_success_preserves_followup_continuation_ids(self):
        instance = SharePolicyTests._attachment_adapter()
        instance._client.upload_and_share_file = lambda *a, **k: asyncio.sleep(
            0, result=("/f", {"id": 999}, True)
        )
        instance.send = lambda *a, **k: asyncio.sleep(
            0,
            result=SendResult(
                success=True,
                message_id="13",
                continuation_message_ids=("11", "12"),
            ),
        )
        result = await instance._send_file_attachment("room", __file__, caption="caption")
        self.assertTrue(result.success)
        self.assertEqual(result.message_id, "13")
        self.assertEqual(result.continuation_message_ids, ("11", "12"))

    async def test_partial_chunk_failure_uses_last_delivered_id(self):
        instance = self.make_adapter([11, 12])
        result = await instance.send("room", "abcdefghijkl")
        self.assertFalse(result.success)
        self.assertFalse(result.retryable)
        self.assertEqual(result.message_id, "12")
        self.assertEqual(result.continuation_message_ids, ("11",))

    async def test_public_link_followup_preserves_continuation_ids(self):
        instance = SharePolicyTests._attachment_adapter()
        instance._client.upload_and_share_file = lambda *a, **k: asyncio.sleep(
            0, result=("/f", {"url": "https://cloud.example/s/link"}, False)
        )
        instance.send = lambda *a, **k: asyncio.sleep(
            0,
            result=SendResult(
                success=True,
                message_id="23",
                continuation_message_ids=("21", "22"),
            ),
        )
        result = await instance._send_file_attachment("room", __file__, caption="caption")
        self.assertTrue(result.success)
        self.assertEqual(result.message_id, "23")
        self.assertEqual(result.continuation_message_ids, ("21", "22"))


class CursorPersistenceTests(unittest.TestCase):
    def test_cursor_cache_oserror_warnings_hide_paths_and_exception_text(self):
        private_path = "/PRIVATE/CACHE/PATH/cursors.json"
        detail = f"permission denied: {private_path}\nINJECTED-CREDENTIAL"

        load_instance = adapter.NextcloudTalkAdapter.__new__(adapter.NextcloudTalkAdapter)
        load_instance._last_message_ids = {}
        load_instance._cursor_path = Path(private_path)
        with patch.object(Path, "read_text", side_effect=OSError(detail)), self.assertLogs(
            adapter.logger, level="WARNING"
        ) as load_logs:
            load_instance._load_cursors()

        persist_instance = adapter.NextcloudTalkAdapter.__new__(adapter.NextcloudTalkAdapter)
        persist_instance._last_message_ids = {"room": 1}
        persist_instance._cursor_path = Path(private_path)
        persist_instance._cursor_lock = threading.RLock()
        with patch.object(Path, "mkdir"), patch.object(
            adapter.tempfile, "mkstemp", side_effect=OSError(detail)
        ), self.assertLogs(adapter.logger, level="WARNING") as persist_logs:
            with self.assertRaises(adapter.CursorCacheError) as raised:
                persist_instance._persist_cursors()

        for operation, output in (
            ("load", load_logs.output), ("persist", persist_logs.output),
        ):
            rendered = "\n".join(output)
            self.assertIn(f"Cursor cache {operation} failed", rendered)
            self.assertIn("OSError", rendered)
            self.assertNotIn(private_path, rendered)
            self.assertNotIn("permission denied", rendered)
            self.assertNotIn("INJECTED-CREDENTIAL", rendered)
            self.assertFalse(any(adapter.unicodedata.category(char) == "Cc" for char in rendered))
        self.assertNotIn(private_path, str(raised.exception))
        self.assertNotIn("INJECTED-CREDENTIAL", str(raised.exception))

    def test_invalid_cursor_entry_does_not_discard_valid_entries(self):
        instance = adapter.NextcloudTalkAdapter.__new__(adapter.NextcloudTalkAdapter)
        instance._last_message_ids = {}
        instance.room_tokens = []
        with tempfile.TemporaryDirectory() as tmp:
            instance._cursor_path = Path(tmp) / "cursors.json"
            instance._cursor_path.write_text(
                json.dumps({"good": 42, "bad": "oops", "also_good": "7"}),
                encoding="utf-8",
            )
            with self.assertLogs(adapter.logger, level="WARNING"):
                instance._load_cursors()
        self.assertEqual(instance._last_message_ids, {"good": 42})


class AckVersionAndHistoryBoundTests(unittest.TestCase):
    @staticmethod
    def make_adapter(path):
        instance = adapter.NextcloudTalkAdapter.__new__(adapter.NextcloudTalkAdapter)
        instance._last_message_ids = {}
        instance._ack_rooms = {}
        instance._inflight_message_ids = {}
        instance._initializing_rooms = set()
        instance._configured_room_tokens = []
        instance._discovered_room_tokens = set()
        instance.room_tokens = []
        instance.max_rooms = 2
        instance.max_ack_rooms = 3
        instance.ack_retention_count = 4096
        instance.ack_overlap_ids = 4096
        instance._cursor_lock = threading.RLock()
        instance._cursor_path = Path(path)
        return instance

    def test_unknown_envelope_is_preserved_and_blocks_downgrade_persistence(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "cursors.json"
            original = b'{"version":3,"rooms":{"real":{"floor":91}},"future":"keep"}'
            path.write_bytes(original)
            instance = self.make_adapter(path)
            with self.assertLogs(adapter.logger, level="WARNING") as logs:
                instance._load_cursors()
            self.assertEqual(instance._ack_rooms, {})
            self.assertTrue(instance._ack_persistence_blocked)
            self.assertEqual(path.read_bytes(), original)
            with self.assertRaises(adapter.CursorCacheError):
                instance._commit_cursor("room", 92)
            self.assertEqual(instance._ack_rooms, {})
            self.assertEqual(path.read_bytes(), original)
            rendered = "\n".join(logs.output)
            self.assertIn("unsupported ACK state version", rendered)
            self.assertNotIn("real", rendered)
            self.assertNotIn("future", rendered)

    def test_unversioned_v1_entries_migrate_independently(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "cursors.json"
            for payload, expected in (
                ({"a": 1, "b": 0}, {"a": 1}),
                ({"good": 1, "bad": "oops", "numeric": "2"},
                 {"good": 1}),
                ({"bool": True, "negative": -1, "object": {}}, {}),
            ):
                with self.subTest(payload=payload):
                    path.write_text(json.dumps(payload), encoding="utf-8")
                    instance = self.make_adapter(path)
                    if expected != payload:
                        with self.assertLogs(adapter.logger, level="WARNING"):
                            instance._load_cursors()
                    else:
                        instance._load_cursors()
                    self.assertEqual(instance._last_message_ids, expected)
                    self.assertFalse(instance._ack_persistence_blocked)
                    self.assertEqual(json.loads(path.read_text()), payload)

    def test_envelope_fields_without_supported_version_fail_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "cursors.json"
            for payload in (
                {"rooms": {"real": 1}},
                {"version": 3, "rooms": {}},
                {"version": "2", "rooms": {}},
            ):
                with self.subTest(payload=payload):
                    original = json.dumps(payload).encode()
                    path.write_bytes(original)
                    instance = self.make_adapter(path)
                    with self.assertLogs(adapter.logger, level="WARNING"):
                        instance._load_cursors()
                    self.assertEqual(instance._ack_rooms, {})
                    self.assertTrue(instance._ack_persistence_blocked)
                    with self.assertRaises(adapter.CursorCacheError):
                        instance._commit_cursor("room", 1)
                    self.assertEqual(path.read_bytes(), original)

    def test_v2_loads_and_persists_with_lru_metadata(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "cursors.json"
            path.write_text(json.dumps({"version": 2, "rooms": {
                "a": {"floor": 4, "successful": [5], "initialized": True,
                      "last_seen": 7, "active": False},
            }}), encoding="utf-8")
            instance = self.make_adapter(path)
            instance._load_cursors()
            self.assertTrue(instance._is_acknowledged("a", 5))
            instance._persist_cursors()
            state = json.loads(path.read_text())["rooms"]["a"]
            self.assertEqual((state["last_seen"], state["active"]), (7, False))

    def test_inactive_history_is_deterministically_lru_bounded(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(Path(tmp) / "cursors.json")
            for index, token in enumerate(("old", "middle", "new", "newest"), 1):
                instance._ack_rooms[token] = {
                    "floor": index, "successful": set(), "initialized": True,
                    "last_seen": index, "active": False,
                }
                instance._sync_legacy_cursor(token)
            instance._prune_ack_rooms()
            self.assertEqual(set(instance._ack_rooms), {"middle", "new", "newest"})
            instance._persist_cursors()
            restarted = self.make_adapter(instance._cursor_path)
            restarted._load_cursors()
            self.assertEqual(set(restarted._ack_rooms), {"middle", "new", "newest"})

    def test_active_configured_initializing_and_inflight_rooms_are_never_evicted(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(Path(tmp) / "cursors.json")
            instance.max_ack_rooms = 2
            instance._configured_room_tokens = ["configured"]
            instance._discovered_room_tokens = {"discovered"}
            instance._initializing_rooms = {"initializing"}
            instance._inflight_message_ids = {"inflight": {1}}
            for token in ("configured", "discovered", "initializing", "inflight", "stale"):
                instance._ack_rooms[token] = {
                    "floor": 1, "successful": set(),
                    "initialized": token != "initializing", "last_seen": 0,
                    "active": token in {"configured", "discovered"},
                }
                instance._sync_legacy_cursor(token)
            instance._prune_ack_rooms()
            self.assertEqual(
                set(instance._ack_rooms),
                {"configured", "discovered", "initializing", "inflight"},
            )


class ConfigurationTests(unittest.TestCase):
    def test_ack_overlap_is_clamped_to_one_poll_page(self):
        with patch.dict(os.environ, {}, clear=True), self.assertLogs(adapter.logger, level="WARNING"):
            instance = adapter.NextcloudTalkAdapter(PlatformConfig(extra={
                "max_poll_batch": 200,
                "ack_overlap_ids": 4096,
            }))
        self.assertEqual(instance.ack_overlap_ids, 199)

        with patch.dict(os.environ, {}, clear=True), self.assertLogs(adapter.logger, level="WARNING"):
            single_item_page = adapter.NextcloudTalkAdapter(PlatformConfig(extra={
                "max_poll_batch": 1,
                "ack_overlap_ids": 32,
            }))
        self.assertEqual(single_item_page.ack_overlap_ids, 0)
        single_item_page._ensure_ack_runtime()
        self.assertEqual(single_item_page.ack_overlap_ids, 0)

        with patch.dict(os.environ, {}, clear=True), self.assertLogs(adapter.logger, level="WARNING"):
            ten_item_page = adapter.NextcloudTalkAdapter(PlatformConfig(extra={
                "max_poll_batch": 10,
                "ack_overlap_ids": 4096,
            }))
        ten_item_page._ensure_ack_runtime()
        self.assertEqual(ten_item_page.ack_overlap_ids, 9)

        with patch.dict(os.environ, {}, clear=True), self.assertLogs(adapter.logger, level="WARNING"):
            oversized_page = adapter.NextcloudTalkAdapter(PlatformConfig(extra={
                "max_poll_batch": 4097,
                "ack_overlap_ids": 4096,
            }))
        oversized_page._ensure_ack_runtime()
        self.assertEqual(oversized_page.max_poll_batch, 200)
        self.assertEqual(oversized_page.ack_overlap_ids, 199)

    def test_ack_overlap_honors_configured_values_below_legacy_floor(self):
        for configured_overlap in (0, 1, 31):
            with self.subTest(configured_overlap=configured_overlap), patch.dict(
                os.environ, {}, clear=True
            ):
                instance = adapter.NextcloudTalkAdapter(PlatformConfig(extra={
                    "max_poll_batch": 200,
                    "ack_overlap_ids": configured_overlap,
                }))
            self.assertEqual(instance.ack_overlap_ids, configured_overlap)
            instance._ensure_ack_runtime()
            self.assertEqual(instance.ack_overlap_ids, configured_overlap)

    def test_tokenless_auto_discovery_configuration_and_registration(self):
        env = {"NEXTCLOUD_TALK_URL": "https://cloud.example", "NEXTCLOUD_TALK_USERNAME": "bot",
               "NEXTCLOUD_TALK_PASSWORD": "secret", "NEXTCLOUD_TALK_AUTO_DISCOVER_ROOMS": "true"}
        with patch.dict(os.environ, env, clear=True): self.assertTrue(adapter.validate_config(PlatformConfig()))
        captured = {}
        adapter.register(types.SimpleNamespace(register_platform=lambda **kwargs: captured.update(kwargs)))
        self.assertEqual(captured["required_env"], ["NEXTCLOUD_TALK_URL", "NEXTCLOUD_TALK_USERNAME", "NEXTCLOUD_TALK_PASSWORD"])

    def test_validation_uses_strict_client_origin_parser(self):
        base = {
            "NEXTCLOUD_TALK_USERNAME": "bot",
            "NEXTCLOUD_TALK_PASSWORD": "secret",
            "NEXTCLOUD_TALK_AUTO_DISCOVER_ROOMS": "true",
        }
        invalid = (
            "https://user@cloud.example", "https://cloud.example:",
            "https://cloud.example:bad", "https://cloud.example:0",
            "https://cloud example", "https://[::1", "https://.cloud.example",
            "https://cloud..example", "https://-cloud.example", "https://cloud_.example",
        )
        for url in invalid:
            with self.subTest(url=url), patch.dict(
                os.environ, {**base, "NEXTCLOUD_TALK_URL": url}, clear=True
            ):
                self.assertFalse(adapter.validate_config(PlatformConfig()))
        for url in ("https://cloud.example", "https://[2001:db8::1]:8443/nc"):
            with self.subTest(url=url), patch.dict(
                os.environ, {**base, "NEXTCLOUD_TALK_URL": url}, clear=True
            ):
                self.assertTrue(adapter.validate_config(PlatformConfig()))
        with patch.dict(
            os.environ, {**base, "NEXTCLOUD_TALK_URL": "http://127.0.0.1:8080"}, clear=True
        ):
            self.assertTrue(adapter.validate_config(PlatformConfig()))
        with patch.dict(os.environ, {
            **base, "NEXTCLOUD_TALK_URL": "http://cloud.example:8080",
            "NEXTCLOUD_TALK_ALLOW_INSECURE_HTTP": "true",
        }, clear=True):
            self.assertTrue(adapter.validate_config(PlatformConfig()))

    def test_outbound_size_limit_fails_before_upload(self):
        instance = adapter.NextcloudTalkAdapter.__new__(adapter.NextcloudTalkAdapter)
        instance._client = types.SimpleNamespace(upload_and_share_file=lambda *a: None)
        instance.room_tokens = ["room"]
        instance.upload_folder = "/Hermes"
        instance.max_upload_bytes = 1
        result = asyncio.run(instance._send_file_attachment("room", __file__))
        self.assertFalse(result.success)
        self.assertIn("size limit", result.error.lower())

    def test_upload_mutation_during_folder_creation_never_sends_oversized_body(self):
        client = adapter.NextcloudTalkClient(
            "https://cloud.example", "bot", "secret", max_upload_bytes=8
        )
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "upload.bin"
            source.write_bytes(b"safe")
            captured = {}

            def mutate(_folder):
                replacement = Path(tmp) / "replacement.bin"
                replacement.write_bytes(b"X" * 100)
                os.replace(replacement, source)

            def raw_request(_method, _url, *, data, headers):
                body = bytearray()
                while True:
                    chunk = data.read(64)
                    if not chunk:
                        break
                    body.extend(chunk)
                captured["declared"] = int(headers["Content-Length"])
                captured["body"] = bytes(body)
                return 201, b"", {}

            client._ensure_folder_sync = mutate
            client._raw_request = raw_request
            try:
                client._upload_file_sync(str(source), "/Hermes")
            except adapter.NextcloudTalkAPIError:
                pass
            if captured:
                self.assertLessEqual(len(captured["body"]), captured["declared"])
                self.assertEqual(captured["body"], b"safe")


class SecurityHardeningRegressionTests(unittest.IsolatedAsyncioTestCase):
    def make_adapter(self, handler=None):
        instance = CursorAndDeliveryTests.make_adapter(
            self, handler or (lambda event: asyncio.sleep(0))
        )
        instance.max_attachments_per_message = 4
        instance.max_attachment_total_bytes = 1024
        instance.max_poll_batch = 100
        return instance

    async def test_retry_disk_amplification_reuses_stable_attachment_cache(self):
        client = adapter.NextcloudTalkClient("https://cloud.example", "bot", "secret")
        with tempfile.TemporaryDirectory() as tmp, patch.object(
            client, "_open_authenticated",
            side_effect=[FakeResponse(b"same", {"Content-Type": "x", "Content-Length": "4"}),
                         FakeResponse(b"same", {"Content-Type": "x", "Content-Length": "4"})],
        ):
            first = client._download_file("/f/a", tmp, cache_identity="room:7:file")
            second = client._download_file("/f/a", tmp, cache_identity="room:7:file")
            self.assertEqual(first, second)
            self.assertEqual(
                [p.name for p in Path(tmp).iterdir() if not p.name.endswith(".complete.json")],
                [Path(first).name],
            )

    async def test_retry_cache_reuses_completed_chunked_download_without_network(self):
        client = adapter.NextcloudTalkClient("https://cloud.example", "bot", "secret")
        response = FakeResponse(
            b"same", {"Content-Type": "x", "Content-Disposition": 'attachment; filename="same.bin"'}
        )
        with tempfile.TemporaryDirectory() as tmp, patch.object(
            client, "_open_authenticated", return_value=response
        ) as opened:
            first = client._download_file("/f/a", tmp, cache_identity="room:chunked:file")
            second = client._download_file("/f/a", tmp, cache_identity="room:chunked:file")
            self.assertEqual(first, second)
            self.assertEqual(opened.call_count, 1)
            manifests = list(Path(tmp).glob("*.complete.json"))
            self.assertEqual(len(manifests), 1)
            manifest = json.loads(manifests[0].read_text(encoding="utf-8"))
            self.assertEqual(manifest["version"], 1)
            self.assertEqual(manifest["filename"], Path(first).name)
            self.assertEqual(manifest["size"], 4)
            self.assertEqual(manifest["sha256"], hashlib.sha256(b"same").hexdigest())
            self.assertEqual(
                manifest["identity_sha256"],
                hashlib.sha256(b"room:chunked:file").hexdigest(),
            )
            rendered = manifests[0].read_text(encoding="utf-8")
            self.assertNotIn("room:chunked:file", rendered)
            self.assertNotIn("https://", rendered)

    async def test_legacy_truncated_cache_without_manifest_is_invalidated_and_redownloaded(self):
        client = adapter.NextcloudTalkClient("https://cloud.example", "bot", "secret")
        identity = "room:legacy:file"
        prefix = hashlib.sha256(identity.encode()).hexdigest()[:24]
        response = FakeResponse(
            b"complete", {"Content-Type": "x", "Content-Length": "8",
                          "Content-Disposition": 'attachment; filename="same.bin"'}
        )
        with tempfile.TemporaryDirectory() as tmp:
            legacy = Path(tmp) / f"{prefix}-same.bin"
            legacy.write_bytes(b"abc")
            with patch.object(client, "_open_authenticated", return_value=response) as opened:
                result = client._download_file("/f/a", tmp, cache_identity=identity)
            self.assertEqual(opened.call_count, 1)
            self.assertEqual(Path(result).read_bytes(), b"complete")
            self.assertFalse(legacy.exists() and legacy.read_bytes() == b"abc")

    async def test_same_size_cache_corruption_is_detected_and_redownloaded(self):
        client = adapter.NextcloudTalkClient("https://cloud.example", "bot", "secret")
        responses = [
            FakeResponse(b"good", {"Content-Type": "x", "Content-Length": "4"}),
            FakeResponse(b"fresh", {"Content-Type": "x", "Content-Length": "5"}),
        ]
        with tempfile.TemporaryDirectory() as tmp, patch.object(
            client, "_open_authenticated", side_effect=responses
        ) as opened:
            first = client._download_file("/f/a", tmp, cache_identity="room:corrupt:file")
            Path(first).write_bytes(b"evil")
            second = client._download_file("/f/a", tmp, cache_identity="room:corrupt:file")
            self.assertEqual(opened.call_count, 2)
            self.assertEqual(Path(second).read_bytes(), b"fresh")

    async def test_tampered_manifest_identity_is_detected_and_redownloaded(self):
        client = adapter.NextcloudTalkClient("https://cloud.example", "bot", "secret")
        responses = [
            FakeResponse(b"first", {"Content-Type": "x", "Content-Length": "5"}),
            FakeResponse(b"fresh", {"Content-Type": "x", "Content-Length": "5"}),
        ]
        with tempfile.TemporaryDirectory() as tmp, patch.object(
            client, "_open_authenticated", side_effect=responses
        ) as opened:
            client._download_file("/f/a", tmp, cache_identity="room:tamper:file")
            manifest_path = next(Path(tmp).glob("*.complete.json"))
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["identity_sha256"] = "0" * 64
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            result = client._download_file("/f/a", tmp, cache_identity="room:tamper:file")
            self.assertEqual(opened.call_count, 2)
            self.assertEqual(Path(result).read_bytes(), b"fresh")

    async def test_malformed_manifest_and_orphan_manifest_are_invalidated_and_redownloaded(self):
        client = adapter.NextcloudTalkClient("https://cloud.example", "bot", "secret")
        identity = "room:malformed:file"
        prefix = hashlib.sha256(identity.encode()).hexdigest()[:24]
        response = FakeResponse(b"fresh", {"Content-Type": "x", "Content-Length": "5"})
        with tempfile.TemporaryDirectory() as tmp:
            cached = Path(tmp) / f"{prefix}-old.bin"
            cached.write_bytes(b"old!!")
            (Path(tmp) / f"{cached.name}.complete.json").write_text(
                '{"filename":"../../escape","size":5}', encoding="utf-8"
            )
            orphan = Path(tmp) / f"{prefix}-orphan.bin.complete.json"
            orphan.write_text("{}", encoding="utf-8")
            with patch.object(client, "_open_authenticated", return_value=response) as opened:
                result = client._download_file("/f/a", tmp, cache_identity=identity)
            self.assertEqual(opened.call_count, 1)
            self.assertEqual(Path(result).read_bytes(), b"fresh")
            self.assertFalse(orphan.exists())
            self.assertFalse(cached.exists())

    async def test_same_identity_concurrent_downloads_are_single_flight_and_integrity_safe(self):
        # The test deliberately blocks two worker threads for 250 ms; keep
        # asyncio debug output focused on leaks rather than this known stress wait.
        asyncio.get_running_loop().slow_callback_duration = 1.0
        client = adapter.NextcloudTalkClient("https://cloud.example", "bot", "secret")
        second_opened = threading.Event()
        open_count = 0
        count_lock = threading.Lock()

        def opened(_req):
            nonlocal open_count
            with count_lock:
                open_count += 1
                call_number = open_count
            if call_number == 1:
                second_opened.wait(0.25)
                payload = b"first-complete"
            else:
                second_opened.set()
                payload = b"hostile-racing-body"
            return FakeResponse(
                payload,
                {"Content-Type": "x", "Content-Length": str(len(payload)),
                 "Content-Disposition": 'attachment; filename="same.bin"'},
            )

        with tempfile.TemporaryDirectory() as tmp, patch.object(
            client, "_open_authenticated", opened
        ):
            start = threading.Barrier(2)
            def download(_):
                start.wait()
                return client._download_file("/f/a", tmp, cache_identity="room:8:file")
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                paths = list(pool.map(download, range(2)))
            self.assertEqual(open_count, 1)
            self.assertEqual(paths[0], paths[1])
            self.assertEqual(Path(paths[0]).read_bytes(), b"first-complete")
            self.assertEqual(
                [path.name for path in Path(tmp).iterdir() if not path.name.endswith(".complete.json")],
                [Path(paths[0]).name],
            )
            self.assertFalse(any(".part" in path.name for path in Path(tmp).iterdir()))
            self.assertEqual(client._download_locks, {})

    async def test_concurrent_same_name_different_payloads_remain_distinct_and_clean(self):
        client = adapter.NextcloudTalkClient("https://cloud.example", "bot", "secret")
        barrier = threading.Barrier(2)
        def opened(req):
            barrier.wait()
            payload = b"alpha" if req.full_url.endswith("/a") else b"bravo"
            return FakeResponse(payload, {"Content-Type": "x", "Content-Length": "5",
                                          "Content-Disposition": 'attachment; filename="same.bin"'})
        with tempfile.TemporaryDirectory() as tmp, patch.object(client, "_open_authenticated", opened):
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                first = pool.submit(client._download_file, "/a", tmp, cache_identity="message:a")
                second = pool.submit(client._download_file, "/b", tmp, cache_identity="message:b")
                paths = [first.result(), second.result()]
            self.assertNotEqual(paths[0], paths[1])
            self.assertEqual({Path(path).read_bytes() for path in paths}, {b"alpha", b"bravo"})
            self.assertEqual(
                len([path for path in Path(tmp).iterdir() if not path.name.endswith(".complete.json")]),
                2,
            )
            self.assertFalse(any(".part" in path.name for path in Path(tmp).iterdir()))

    async def test_attachment_larger_than_whole_cache_quota_is_rejected_before_publish(self):
        client = adapter.NextcloudTalkClient(
            "https://cloud.example", "bot", "secret",
            max_download_bytes=100, max_cache_files=2, max_cache_bytes=4,
        )
        for response in (
            FakeResponse(b"12345", {"Content-Type": "x", "Content-Length": "5"}),
            FakeResponse(b"12345", {"Content-Type": "x"}),
        ):
            with self.subTest(headers=dict(response.headers)), tempfile.TemporaryDirectory() as tmp, patch.object(
                client, "_open_authenticated", return_value=response
            ):
                with self.assertRaises(adapter.AttachmentDownloadError) as raised:
                    client._download_file("/oversized", tmp, cache_identity="room:70:file")
                self.assertEqual(raised.exception.category, "overflow")
                self.assertEqual(list(Path(tmp).iterdir()), [])

    async def test_zero_file_cache_quota_rejects_instead_of_returning_evicted_path(self):
        client = adapter.NextcloudTalkClient(
            "https://cloud.example", "bot", "secret",
            max_download_bytes=100, max_cache_files=0, max_cache_bytes=100,
        )
        with tempfile.TemporaryDirectory() as tmp, patch.object(
            client, "_open_authenticated",
            return_value=FakeResponse(b"x", {"Content-Type": "x", "Content-Length": "1"}),
        ) as opened:
            with self.assertRaises(adapter.AttachmentDownloadError) as raised:
                client._download_file("/no-file-capacity", tmp, cache_identity="room:71:file")
            self.assertEqual(raised.exception.category, "overflow")
            self.assertEqual(list(Path(tmp).iterdir()), [])
            opened.assert_not_called()

    async def test_cache_wide_file_and_byte_quota_evicts_deterministically(self):
        client = adapter.NextcloudTalkClient(
            "https://cloud.example", "bot", "secret",
            max_cache_files=2, max_cache_bytes=7,
        )
        responses = [
            FakeResponse(b"aaa", {"Content-Type": "x", "Content-Length": "3"}),
            FakeResponse(b"bbbb", {"Content-Type": "x", "Content-Length": "4"}),
            FakeResponse(b"ccccc", {"Content-Type": "x", "Content-Length": "5"}),
        ]
        with tempfile.TemporaryDirectory() as tmp, patch.object(
            client, "_open_authenticated", side_effect=responses
        ):
            paths = [client._download_file(
                f"/f/{index}", tmp, cache_identity=f"identity-{index}"
            ) for index in range(3)]
            data_files = [p for p in Path(tmp).iterdir() if not p.name.endswith(".complete.json")]
            self.assertLessEqual(len(data_files), 2)
            self.assertLessEqual(sum(p.stat().st_size for p in data_files), 7)
            self.assertFalse(Path(paths[0]).exists())
            self.assertTrue(Path(paths[-1]).exists())

    async def test_distinct_concurrent_download_paths_survive_consumer_boundary(self):
        client = adapter.NextcloudTalkClient(
            "https://cloud.example", "bot", "secret",
            max_cache_files=1, max_cache_bytes=8,
        )
        download_barrier = threading.Barrier(2)
        consume_barrier = threading.Barrier(2)
        bodies = iter((b"first", b"second"))
        lock = threading.Lock()

        def opened(_req):
            with lock:
                body = next(bodies)
            download_barrier.wait(timeout=2)
            return FakeResponse(body, {"Content-Type": "x", "Content-Length": str(len(body))})

        def download_and_consume(index):
            cache_lease = client._download_file(
                f"/f/{index}", tmp, cache_identity=f"id-{index}", lease=True
            )
            try:
                consume_barrier.wait(timeout=2)
                return cache_lease.path, Path(cache_lease.path).read_bytes()
            finally:
                cache_lease.release()

        with tempfile.TemporaryDirectory() as tmp, patch.object(
            client, "_open_authenticated", opened
        ):
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                results = list(pool.map(download_and_consume, range(2)))
            self.assertEqual({payload for _path, payload in results}, {b"first", b"second"})
            self.assertEqual(sum(Path(path).exists() for path, _payload in results), 1)
            self.assertEqual(client._cache_leases, {})
            self.assertEqual(client._cache_inflight, {})
            self.assertEqual(client._download_locks, {})
            self.assertEqual(len(list(Path(tmp).glob("*.complete.json"))), 1)
            self.assertFalse(any(p.name.endswith(".part") for p in Path(tmp).iterdir()))

    async def test_multi_attachment_dispatch_holds_every_lease_until_handler_consumes(self):
        consumed = []

        async def consume(event):
            self.assertTrue(all(Path(path).is_file() for path in event.media_urls))
            consumed.extend(Path(path).read_bytes() for path in event.media_urls)

        instance = self.make_adapter(consume)
        client = adapter.NextcloudTalkClient(
            "https://cloud.example", "bot", "secret",
            max_cache_files=1, max_cache_bytes=10,
        )
        instance._client = client
        message = {
            "id": 72,
            "actorType": "users",
            "actorId": "alice",
            "message": "two files",
            "messageParameters": {
                str(index): {
                    "type": "file", "name": f"{index}.bin", "path": f"Talk/{index}.bin"
                }
                for index in range(2)
            },
        }
        responses = [
            FakeResponse(b"one", {"Content-Type": "x", "Content-Length": "3"}),
            FakeResponse(b"two", {"Content-Type": "x", "Content-Length": "3"}),
        ]
        with tempfile.TemporaryDirectory() as tmp, patch.object(
            adapter, "get_hermes_home", return_value=Path(tmp)
        ), patch.object(client, "_open_authenticated", side_effect=responses):
            await instance._handle_talk_message(message, "dm-room")
            cache_dir = Path(tmp) / "cache" / "documents"
            self.assertEqual(consumed, [b"one", b"two"])
            self.assertEqual(client._cache_leases, {})
            self.assertEqual(client._cache_inflight, {})
            self.assertEqual(len(list(cache_dir.glob("*.complete.json"))), 1)
            self.assertEqual(
                len([path for path in cache_dir.iterdir() if not path.name.endswith(".complete.json")]),
                1,
            )
            self.assertFalse(any(path.name.endswith(".part") for path in cache_dir.iterdir()))

    async def test_handler_exception_releases_attachment_lease_without_orphans(self):
        async def fail(event):
            self.assertTrue(Path(event.media_urls[0]).is_file())
            raise RuntimeError("handler failed")

        instance = self.make_adapter(fail)
        client = adapter.NextcloudTalkClient(
            "https://cloud.example", "bot", "secret", max_cache_files=1
        )
        instance._client = client
        message = {
            "id": 73, "actorType": "users", "actorId": "alice", "message": "file",
            "messageParameters": {
                "file": {"type": "file", "name": "a.bin", "path": "Talk/a.bin"}
            },
        }
        response = FakeResponse(b"data", {"Content-Type": "x", "Content-Length": "4"})
        with tempfile.TemporaryDirectory() as tmp, patch.object(
            adapter, "get_hermes_home", return_value=Path(tmp)
        ), patch.object(client, "_open_authenticated", return_value=response):
            with self.assertRaisesRegex(RuntimeError, "handler failed"):
                await instance._handle_talk_message(message, "dm-room")
            cache_dir = Path(tmp) / "cache" / "documents"
            manifests = list(cache_dir.glob("*.complete.json"))
            self.assertEqual(client._cache_leases, {})
            self.assertEqual(client._cache_inflight, {})
            self.assertEqual(client._download_locks, {})
            self.assertEqual(len(manifests), 1)
            manifest = json.loads(manifests[0].read_text(encoding="utf-8"))
            self.assertTrue((cache_dir / manifest["filename"]).is_file())
            self.assertFalse(any(path.name.endswith(".part") for path in cache_dir.iterdir()))

    async def test_handler_cancellation_releases_attachment_lease(self):
        started = asyncio.Event()
        block = asyncio.Event()

        async def wait_forever(event):
            self.assertEqual(Path(event.media_urls[0]).read_bytes(), b"data")
            started.set()
            await block.wait()

        instance = self.make_adapter(wait_forever)
        client = adapter.NextcloudTalkClient(
            "https://cloud.example", "bot", "secret", max_cache_files=1
        )
        instance._client = client
        message = {
            "id": 74, "actorType": "users", "actorId": "alice", "message": "file",
            "messageParameters": {
                "file": {"type": "file", "name": "a.bin", "path": "Talk/a.bin"}
            },
        }
        response = FakeResponse(b"data", {"Content-Type": "x", "Content-Length": "4"})
        with tempfile.TemporaryDirectory() as tmp, patch.object(
            adapter, "get_hermes_home", return_value=Path(tmp)
        ), patch.object(client, "_open_authenticated", return_value=response):
            task = asyncio.create_task(instance._handle_talk_message(message, "dm-room"))
            await asyncio.wait_for(started.wait(), timeout=1)
            self.assertEqual(sum(client._cache_leases.values()), 1)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            cache_dir = Path(tmp) / "cache" / "documents"
            self.assertEqual(client._cache_leases, {})
            self.assertEqual(client._cache_inflight, {})
            self.assertEqual(client._download_locks, {})
            self.assertFalse(any(path.name.endswith(".part") for path in cache_dir.iterdir()))

    async def test_cancellation_before_download_lease_handoff_releases_every_lease(self):
        iteration_count = 50
        worker_state = {}

        client = adapter.NextcloudTalkClient(
            "https://cloud.example", "bot", "secret",
            max_cache_files=1, max_cache_bytes=4,
        )
        original_download = client._download_file

        def opened(_req):
            state = worker_state
            state["entered"].set()
            self.assertTrue(state["release"].wait(timeout=5))
            return FakeResponse(
                b"data", {"Content-Type": "x", "Content-Length": "4"}
            )

        def tracked_download(*args, **kwargs):
            state = worker_state
            try:
                return original_download(*args, **kwargs)
            finally:
                state["finished"].set()

        client._download_file = tracked_download
        instance = self.make_adapter()
        instance._client = client

        def message(index):
            return {
                "id": 1000 + index,
                "actorType": "users",
                "actorId": "alice",
                "message": f"file {index}",
                "messageParameters": {
                    "file": {
                        "type": "file",
                        "name": f"{index}.bin",
                        "path": f"Talk/{index}.bin",
                    }
                },
            }

        with tempfile.TemporaryDirectory() as tmp, patch.object(
            adapter, "get_hermes_home", return_value=Path(tmp)
        ), patch.object(client, "_open_authenticated", side_effect=opened):
            results = []
            for index in range(iteration_count):
                worker_state.clear()
                worker_state.update(
                    entered=threading.Event(),
                    release=threading.Event(),
                    finished=threading.Event(),
                )
                task = asyncio.create_task(
                    instance._handle_talk_message(message(index), "dm-room")
                )
                self.assertTrue(
                    await asyncio.wait_for(
                        asyncio.to_thread(worker_state["entered"].wait, 5), timeout=6
                    )
                )
                task.cancel()
                result = await asyncio.gather(task, return_exceptions=True)
                results.extend(result)
                worker_state["release"].set()
                self.assertTrue(
                    await asyncio.wait_for(
                        asyncio.to_thread(worker_state["finished"].wait, 5), timeout=6
                    )
                )
            self.assertTrue(all(isinstance(result, asyncio.CancelledError) for result in results))
            for _ in range(100):
                if not client._cache_leases and not client._cache_inflight and not client._download_locks:
                    break
                await asyncio.sleep(0)

            cache_dir = Path(tmp) / "cache" / "documents"
            manifests = list(cache_dir.glob("*.complete.json"))
            data_files = [
                path for path in cache_dir.iterdir()
                if not path.name.endswith(".complete.json")
            ]
            self.assertEqual(client._cache_leases, {})
            self.assertEqual(client._cache_inflight, {})
            self.assertEqual(client._download_locks, {})
            self.assertLessEqual(len(manifests), client.max_cache_files)
            self.assertLessEqual(len(data_files), client.max_cache_files)
            self.assertLessEqual(sum(path.stat().st_size for path in data_files), client.max_cache_bytes)
            self.assertFalse(any(".part" in path.name for path in cache_dir.iterdir()))
            self.assertEqual(
                {json.loads(path.read_text(encoding="utf-8"))["filename"] for path in manifests},
                {path.name for path in data_files},
            )

    async def test_download_completion_cancellation_race_has_exactly_one_lease_owner(self):
        iteration_count = 50
        worker_state = {}
        loop = asyncio.get_running_loop()
        client = adapter.NextcloudTalkClient(
            "https://cloud.example", "bot", "secret",
            max_cache_files=1, max_cache_bytes=4,
        )
        original_download = client._download_file

        def opened(_req):
            state = worker_state

            class RacingResponse(FakeResponse):
                def __init__(self):
                    super().__init__(
                        b"data", {"Content-Type": "x", "Content-Length": "4"}
                    )
                    self._raced = False

                def read(self, size=-1):
                    if not self._raced:
                        self._raced = True
                        state["entered"].set()
                        state["race"].wait(timeout=5)
                    return super().read(size)

            return RacingResponse()

        def tracked_download(*args, **kwargs):
            state = worker_state
            try:
                return original_download(*args, **kwargs)
            finally:
                state["finished"].set()

        client._download_file = tracked_download
        instance = self.make_adapter()
        instance._client = client

        with tempfile.TemporaryDirectory() as tmp, patch.object(
            adapter, "get_hermes_home", return_value=Path(tmp)
        ), patch.object(client, "_open_authenticated", side_effect=opened):
            for index in range(iteration_count):
                worker_state.clear()
                worker_state.update(
                    entered=threading.Event(),
                    race=threading.Barrier(2),
                    cancel_sent=threading.Event(),
                    finished=threading.Event(),
                )
                message = {
                    "id": 2000 + index,
                    "actorType": "users",
                    "actorId": "alice",
                    "message": f"race {index}",
                    "messageParameters": {
                        "file": {
                            "type": "file",
                            "name": f"{index}.bin",
                            "path": f"Talk/race-{index}.bin",
                        }
                    },
                }
                task = asyncio.create_task(instance._handle_talk_message(message, "dm-room"))
                self.assertTrue(
                    await asyncio.wait_for(
                        asyncio.to_thread(worker_state["entered"].wait, 5), timeout=6
                    )
                )

                state = worker_state

                def cancel_at_completion_boundary():
                    state["race"].wait(timeout=5)
                    loop.call_soon_threadsafe(task.cancel)
                    state["cancel_sent"].set()

                canceller = threading.Thread(target=cancel_at_completion_boundary)
                canceller.start()
                await asyncio.gather(task, return_exceptions=True)
                self.assertTrue(
                    await asyncio.wait_for(
                        asyncio.to_thread(state["cancel_sent"].wait, 5), timeout=6
                    )
                )
                self.assertTrue(
                    await asyncio.wait_for(
                        asyncio.to_thread(state["finished"].wait, 5), timeout=6
                    )
                )
                canceller.join(timeout=1)
                self.assertFalse(canceller.is_alive())
                for _ in range(100):
                    if not client._cache_leases and not client._cache_inflight and not client._download_locks:
                        break
                    await asyncio.sleep(0)
                self.assertEqual(client._cache_leases, {})
                self.assertEqual(client._cache_inflight, {})
                self.assertEqual(client._download_locks, {})

            cache_dir = Path(tmp) / "cache" / "documents"
            manifests = list(cache_dir.glob("*.complete.json"))
            data_files = [
                path for path in cache_dir.iterdir()
                if not path.name.endswith(".complete.json")
            ]
            self.assertLessEqual(len(manifests), client.max_cache_files)
            self.assertLessEqual(len(data_files), client.max_cache_files)
            self.assertLessEqual(sum(path.stat().st_size for path in data_files), client.max_cache_bytes)
            self.assertFalse(any(".part" in path.name for path in cache_dir.iterdir()))
            self.assertEqual(
                {json.loads(path.read_text(encoding="utf-8"))["filename"] for path in manifests},
                {path.name for path in data_files},
            )

    async def test_authenticated_dav_traversal_is_rejected(self):
        client = adapter.NextcloudTalkClient("https://cloud.example/nc", "bot", "secret")
        for path in (
            "../secret", "Talk/../secret", "/Talk/a", "https://evil/x",
            "Talk/%2e%2e/x", "Talk/%252e%252e/x",
            "Talk/%252525252e%252525252e/x", "Talk/\x00x",
        ):
            with self.subTest(path=path):
                with self.assertRaises(adapter.NextcloudTalkAPIError) as raised:
                    client._dav_url(path)
                self.assertEqual(raised.exception.category, "security")

    async def test_authenticated_dav_preserves_unicode_and_percent_filenames(self):
        client = adapter.NextcloudTalkClient("https://cloud.example/nc", "bot", "secret")
        self.assertEqual(
            client._dav_url("Talk/caf%C3%A9%20100%25.txt"),
            "https://cloud.example/nc/remote.php/dav/files/bot/Talk/caf%C3%A9%20100%25.txt",
        )
        self.assertEqual(
            client._dav_url("Talk/literal%2520name.txt"),
            "https://cloud.example/nc/remote.php/dav/files/bot/Talk/literal%2520name.txt",
        )
        self.assertEqual(
            client._dav_url("Talk/資料.txt"),
            "https://cloud.example/nc/remote.php/dav/files/bot/Talk/%E8%B3%87%E6%96%99.txt",
        )

    async def test_authenticated_dav_rejects_raw_and_repeatedly_encoded_c1_controls(self):
        client = adapter.NextcloudTalkClient("https://cloud.example/nc", "bot", "secret")
        for path in (
            "Talk/a\u0085b.txt", "Talk/a%C2%85b.txt", "Talk/a%25C2%2585b.txt",
            "Talk/a%252525C2%25252585b.txt",
        ):
            with self.subTest(path=path):
                with self.assertRaises(adapter.NextcloudTalkAPIError) as raised:
                    client._dav_url(path)
                self.assertEqual(raised.exception.category, "security")

    async def test_empty_list_message_parameters_is_treated_as_no_metadata(self):
        seen = []
        instance = self.make_adapter(
            lambda event: asyncio.sleep(0, result=seen.append((event.message_id, event.text)))
        )
        msg = {
            "id": 9,
            "actorType": "users",
            "actorId": "alice",
            "message": "plain Talk text",
            "messageParameters": [],
        }
        await instance._handle_talk_message(msg, "dm-room", await_completion=True)
        self.assertEqual(seen, [("9", "plain Talk text")])
        self.assertEqual(instance._last_message_ids["dm-room"], 9)

    async def test_poll_overlap_fits_page_and_reaches_newest_message(self):
        seen = []
        calls = []
        instance = self.make_adapter(lambda event: asyncio.sleep(0))
        instance.max_poll_batch = 10
        instance.poll_timeout = 1
        instance.ack_overlap_ids = 9
        instance.ack_retention_count = 32
        instance._persist_cursors = lambda: None
        for message_id in range(1, 101):
            instance._commit_cursor("dm-room", message_id)

        async def get_messages(*_args, **kwargs):
            anchor = kwargs["last_known_id"]
            limit = kwargs["limit"]
            calls.append((anchor, limit))
            return [{"id": value} for value in range(anchor + 1, min(anchor + limit, 101) + 1)]

        instance._client = types.SimpleNamespace(get_messages=get_messages)

        async def record(msg, room):
            seen.append((msg["id"], room))

        instance._handle_talk_message = record
        await instance._poll_room("dm-room")
        self.assertEqual(calls, [(91, 10)])
        self.assertEqual(seen, [(101, "dm-room")])

    async def test_malformed_message_parameters_does_not_block_newer_valid_message(self):
        seen = []
        instance = self.make_adapter(lambda event: asyncio.sleep(0, result=seen.append(event.message_id)))
        instance.poll_timeout = 1
        async def get_messages(*_args, **_kwargs):
            return [
                {"id": 10, "actorType": "users", "actorId": "alice", "message": "bad", "messageParameters": ["not-metadata"]},
                {"id": 11, "actorType": "users", "actorId": "alice", "message": "good"},
            ]
        instance._client = types.SimpleNamespace(get_messages=get_messages)
        with self.assertLogs(adapter.logger, level="WARNING") as logs:
            await instance._poll_room("dm-room")
        self.assertEqual(seen, ["11"])
        self.assertEqual(instance._last_message_ids["dm-room"], 11)
        self.assertTrue(any("messageParameters" in line for line in logs.output))

    async def test_cache_quota_oversize_is_quarantined_and_later_message_progresses(self):
        seen = []
        instance = self.make_adapter(
            lambda event: asyncio.sleep(0, result=seen.append(event.message_id))
        )
        instance.poll_timeout = 1
        client = adapter.NextcloudTalkClient(
            "https://cloud.example", "bot", "secret",
            max_download_bytes=100, max_cache_bytes=4, max_cache_files=2,
        )

        async def get_messages(*_args, **_kwargs):
            return [
                {"id": 70, "actorType": "users", "actorId": "alice", "message": "file",
                 "messageParameters": {"file": {"type": "file", "name": "large.bin",
                                                   "path": "Talk/large.bin"}}},
                {"id": 71, "actorType": "users", "actorId": "alice", "message": "valid"},
            ]

        client.get_messages = get_messages
        instance._client = client
        with tempfile.TemporaryDirectory() as tmp, patch.object(
            adapter, "get_hermes_home", return_value=Path(tmp)
        ), patch.object(
            client, "_open_authenticated",
            return_value=FakeResponse(b"12345", {"Content-Type": "x", "Content-Length": "5"}),
        ) as opened, self.assertLogs(adapter.logger, level="WARNING"):
            await instance._poll_room("dm-room")
            documents = Path(tmp) / "cache" / "documents"
            self.assertFalse(documents.exists() and any(documents.iterdir()))
        self.assertEqual(opened.call_count, 1)
        self.assertEqual(seen, ["71"])
        self.assertEqual(instance._last_message_ids["dm-room"], 71)

    async def test_traversal_message_10_is_quarantined_and_valid_message_11_dispatches(self):
        seen = []
        instance = self.make_adapter(lambda event: asyncio.sleep(0, result=seen.append(event.message_id)))
        instance.poll_timeout = 1
        client = adapter.NextcloudTalkClient("https://cloud.example", "bot", "secret")
        async def get_messages(*_args, **_kwargs):
            return [
                {"id": 10, "actorType": "users", "actorId": "alice", "message": "bad",
                 "messageParameters": {"file": {"type": "file", "path": "Talk/../secret"}}},
                {"id": 11, "actorType": "users", "actorId": "alice", "message": "good"},
            ]
        client.get_messages = get_messages
        instance._client = client
        with self.assertLogs(adapter.logger, level="WARNING") as logs:
            await instance._poll_room("dm-room")
        self.assertEqual(seen, ["11"])
        self.assertEqual(instance._last_message_ids["dm-room"], 11)
        self.assertTrue(any("quarantin" in line.lower() for line in logs.output))

    async def test_nested_non_file_metadata_is_iteratively_bounded_and_does_not_block_newer(self):
        deep = "leaf"
        for _ in range(2000):
            deep = {"child": deep}
        invalid_parameters = [
            {"mention": {"type": "user", "id": "x" * (adapter._MAX_METADATA_VALUE + 1)}},
            {"mention": {"type": "user", "profile": deep}},
            {"mention": {"type": "user", "profile": {1: "non-string key"}}},
            {"mention": {"type": "user", "profile": object()}},
        ]
        for offset, parameters in enumerate(invalid_parameters):
            with self.subTest(offset=offset):
                seen = []
                instance = self.make_adapter(
                    lambda event: asyncio.sleep(0, result=seen.append(event.message_id))
                )
                instance.poll_timeout = 1
                async def get_messages(*_args, **_kwargs):
                    return [
                        {"id": 20, "actorType": "users", "actorId": "alice", "message": "bad",
                         "messageParameters": parameters},
                        {"id": 21, "actorType": "users", "actorId": "alice", "message": "good"},
                    ]
                instance._client = types.SimpleNamespace(get_messages=get_messages)
                with self.assertLogs(adapter.logger, level="WARNING"):
                    await instance._poll_room("dm-room")
                self.assertEqual(seen, ["21"])
                self.assertEqual(instance._last_message_ids["dm-room"], 21)

    async def test_deterministic_download_rejection_advances_but_network_error_retries(self):
        for category, should_advance in (("overflow", True), ("content", True), ("network", False)):
            with self.subTest(category=category):
                instance = self.make_adapter()
                instance._client = types.SimpleNamespace(
                    _dav_url=lambda path: f"https://cloud.example/dav/{path}",
                    _download_file=lambda *a, **k: (_ for _ in ()).throw(
                        adapter.AttachmentDownloadError("rejected", category=category)
                    ),
                )
                msg = {"id": 15, "actorType": "users", "actorId": "alice", "message": "file",
                       "messageParameters": {"file": {"type": "file", "path": "Talk/a"}}}
                if should_advance:
                    with self.assertLogs(adapter.logger, level="WARNING"):
                        await instance._handle_talk_message(msg, "dm-room")
                    self.assertEqual(instance._last_message_ids["dm-room"], 15)
                else:
                    with self.assertRaises(adapter.AttachmentDownloadError):
                        await instance._handle_talk_message(msg, "dm-room")
                    self.assertNotIn("dm-room", instance._last_message_ids)

    async def test_attachment_http_status_taxonomy_controls_batch_progress(self):
        hostile = b"PRIVATE-BODY\nPRIVATE-URL"
        for status in (400, 404, 410, 413, 414, 415, 416, 422):
            with self.subTest(permanent=status):
                seen = []
                instance = self.make_adapter(
                    lambda event: asyncio.sleep(0, result=seen.append(event.message_id))
                )
                instance.poll_timeout = 1
                client = adapter.NextcloudTalkClient("https://cloud.example", "bot", "secret")
                client.get_messages = lambda *_a, **_k: asyncio.sleep(0, result=[
                    {"id": 30, "actorType": "users", "actorId": "alice", "message": "bad",
                     "messageParameters": {"file": {"type": "file", "path": "Talk/missing"}}},
                    {"id": 31, "actorType": "users", "actorId": "alice", "message": "good"},
                ])
                failure = adapter.error.HTTPError(
                    "https://cloud.example/PRIVATE-URL", status, "failure", {}, io.BytesIO(hostile)
                )
                client._open_authenticated = lambda *_a, failure=failure, **_k: (_ for _ in ()).throw(failure)
                instance._client = client
                with self.assertLogs(adapter.logger, level="WARNING") as logs:
                    await instance._poll_room("dm-room")
                rendered = "\n".join(logs.output)
                self.assertEqual(seen, ["31"])
                self.assertEqual(instance._last_message_ids["dm-room"], 31)
                self.assertNotIn("PRIVATE-BODY", rendered)
                self.assertNotIn("PRIVATE-URL", rendered)

        for status in (301, 401, 403, 408, 409, 418, 423, 425, 429, 500, 503, 599, 700):
            with self.subTest(transient=status):
                instance = self.make_adapter()
                instance.poll_timeout = 1
                client = adapter.NextcloudTalkClient("https://cloud.example", "bot", "secret")
                client.get_messages = lambda *_a, **_k: asyncio.sleep(0, result=[
                    {"id": 40, "actorType": "users", "actorId": "alice", "message": "retry",
                     "messageParameters": {"file": {"type": "file", "path": "Talk/later"}}},
                    {"id": 41, "actorType": "users", "actorId": "alice", "message": "blocked"},
                ])
                failure = adapter.error.HTTPError(
                    "https://cloud.example/PRIVATE-URL", status, "failure", {}, io.BytesIO(hostile)
                )
                client._open_authenticated = lambda *_a, failure=failure, **_k: (_ for _ in ()).throw(failure)
                instance._client = client
                with self.assertLogs(adapter.logger, level="WARNING") as logs:
                    await instance._poll_room("dm-room")
                rendered = "\n".join(logs.output)
                self.assertNotIn("dm-room", instance._last_message_ids)
                self.assertNotIn("PRIVATE-BODY", rendered)
                self.assertNotIn("PRIVATE-URL", rendered)

    async def test_attachment_mkdir_failure_is_sanitized_retryable_and_uncommitted(self):
        private_path = "/PRIVATE/CACHE/PATH/documents"
        detail = f"permission denied: {private_path}\nINJECTED-CREDENTIAL"
        handled = []
        failures = []
        instance = self.make_adapter(
            lambda event: asyncio.sleep(0, result=handled.append(event))
        )
        instance.poll_timeout = 1
        client = adapter.NextcloudTalkClient("https://cloud.example", "bot", "secret")
        original_download = client._download_file

        def capture_failure(*args, **kwargs):
            try:
                return original_download(*args, **kwargs)
            except adapter.AttachmentDownloadError as exc:
                failures.append(exc)
                raise

        async def get_messages(*_args, **_kwargs):
            return [{
                "id": 16, "actorType": "users", "actorId": "alice", "message": "file",
                "messageParameters": {"file": {"type": "file", "path": "Talk/a"}},
            }]

        client._download_file = capture_failure
        client.get_messages = get_messages
        instance._client = client
        with patch.object(adapter.Path, "mkdir", side_effect=OSError(detail)), self.assertLogs(
            adapter.logger, level="WARNING"
        ) as logs:
            await instance._poll_room("dm-room")

        rendered = "\n".join(logs.output)
        self.assertNotIn(private_path, rendered)
        self.assertNotIn("permission denied", rendered)
        self.assertNotIn("INJECTED-CREDENTIAL", rendered)
        self.assertEqual(handled, [])
        self.assertNotIn("dm-room", instance._last_message_ids)
        self.assertEqual(len(failures), 1)
        self.assertEqual(failures[0].category, "io")
        self.assertNotIn(
            failures[0].category, instance._DETERMINISTIC_ATTACHMENT_CATEGORIES
        )
        self.assertNotIn(private_path, str(failures[0]))
        self.assertNotIn("INJECTED-CREDENTIAL", str(failures[0]))

    async def test_attachment_count_and_aggregate_limits_quarantine_message(self):
        instance = self.make_adapter()
        instance.max_attachments_per_message = 1
        msg = {"id": 12, "actorType": "users", "actorId": "alice", "message": "files",
               "messageParameters": {str(i): {"type": "file", "name": "a", "path": "Talk/a"} for i in range(2)}}
        with self.assertLogs(adapter.logger, level="WARNING"):
            await instance._handle_talk_message(msg, "dm-room")
        self.assertEqual(instance._last_message_ids["dm-room"], 12)

    async def test_attachment_aggregate_overflow_is_quarantined(self):
        seen = []
        instance = self.make_adapter(lambda event: asyncio.sleep(0, result=seen.append(event)))
        instance.max_attachment_total_bytes = 1000
        client = adapter.NextcloudTalkClient(
            "https://cloud.example", "bot", "secret", max_download_bytes=1000
        )
        instance._client = client
        msg = {
            "id": 14, "actorType": "users", "actorId": "alice", "message": "files",
            "messageParameters": {
                str(i): {"type": "file", "name": f"{i}.bin", "path": f"Talk/{i}.bin"}
                for i in range(2)
            },
        }
        responses = [
            FakeResponse(b"a" * 700, {"Content-Type": "x", "Content-Length": "700"}),
            FakeResponse(b"b" * 700, {"Content-Type": "x", "Content-Length": "700"}),
        ]
        with tempfile.TemporaryDirectory() as tmp, patch.object(
            adapter, "get_hermes_home", return_value=Path(tmp)
        ), patch.object(client, "_open_authenticated", side_effect=responses), self.assertLogs(
            adapter.logger, level="WARNING"
        ):
            await instance._handle_talk_message(msg, "dm-room")
            cache_dir = Path(tmp) / "cache" / "documents"
            manifests = list(cache_dir.glob("*.complete.json"))
            self.assertEqual(client._cache_leases, {})
            self.assertEqual(client._cache_inflight, {})
            self.assertEqual(client._download_locks, {})
            self.assertEqual(len(manifests), 1)
            manifest = json.loads(manifests[0].read_text(encoding="utf-8"))
            self.assertTrue((cache_dir / manifest["filename"]).is_file())
            self.assertFalse(any(path.name.endswith(".part") for path in cache_dir.iterdir()))
        self.assertEqual(seen, [])
        self.assertEqual(instance._last_message_ids["dm-room"], 14)

    async def test_oversized_json_api_response_is_bounded(self):
        client = adapter.NextcloudTalkClient(
            "https://cloud.example", "bot", "secret", max_json_bytes=32
        )
        response = FakeResponse(b"{" + b" " * 100 + b"}", {"Content-Type": "application/json"})
        with patch.object(client, "_open_authenticated", return_value=response):
            with self.assertRaises(adapter.NextcloudTalkAPIError) as raised:
                client._ocs_get("/ocs")
        self.assertEqual(raised.exception.category, "overflow")

    async def test_chat_raw_and_httperror_reads_are_strictly_bounded(self):
        client = adapter.NextcloudTalkClient(
            "https://cloud.example", "bot", "secret", max_json_bytes=16, max_body_bytes=12
        )
        chat_stream = TrackingStream(100)
        with patch.object(client, "_open_authenticated", return_value=chat_stream):
            with self.assertRaises(adapter.NextcloudTalkAPIError) as raised:
                await client.get_messages("room", last_known_id=0, look_into_future=True, timeout=1)
        self.assertEqual(raised.exception.category, "overflow")
        self.assertLessEqual(sum(chat_stream.requests), 17)

        raw_stream = FakeResponse(b"x" * 100, status=200)
        original_read = raw_stream.read
        raw_stream.requests = []
        def tracked_read(size=-1):
            raw_stream.requests.append(size)
            return original_read(size)
        raw_stream.read = tracked_read
        with patch.object(client, "_open_authenticated", return_value=raw_stream):
            with self.assertRaises(adapter.NextcloudTalkAPIError) as raised:
                client._raw_request("GET", "https://cloud.example/raw")
        self.assertEqual(raised.exception.category, "overflow")
        self.assertLessEqual(sum(raw_stream.requests), 13)

        http_body = TrackingStream(adapter._MAX_ERROR_BYTES + 100)
        http_error = adapter.error.HTTPError(
            "https://cloud.example/ocs", 500, "failure", {}, http_body
        )
        with patch.object(client, "_open_authenticated", side_effect=http_error):
            with self.assertRaises(adapter.NextcloudTalkAPIError) as raised:
                client._ocs_get("/ocs")
        self.assertEqual(raised.exception.category, "overflow")
        self.assertLessEqual(sum(http_body.requests), adapter._MAX_ERROR_BYTES + 1)
        self.assertNotIn("PRIVATE", str(raised.exception))

    async def test_room_poll_and_backlog_caps_fail_closed(self):
        def ocs(data):
            return {"ocs": {"meta": {"status": "ok", "statuscode": 200}, "data": data}}
        room_client = adapter.NextcloudTalkClient("https://cloud.example", "bot", "secret", max_rooms=1)
        with patch.object(
            room_client, "_open_authenticated",
            return_value=FakeResponse(json.dumps(ocs([{"token": "a", "type": 1}, {"token": "b", "type": 1}])).encode()),
        ):
            with self.assertRaises(adapter.NextcloudTalkAPIError) as raised:
                await room_client.list_conversations()
        self.assertEqual(raised.exception.category, "overflow")

        poll_client = adapter.NextcloudTalkClient("https://cloud.example", "bot", "secret", max_poll_batch=1)
        with patch.object(
            poll_client, "_open_authenticated",
            return_value=FakeResponse(json.dumps(ocs([{"id": 1}, {"id": 2}])).encode()),
        ):
            with self.assertRaises(adapter.NextcloudTalkAPIError) as raised:
                await poll_client.get_messages("room", last_known_id=0, look_into_future=True, timeout=1)
        self.assertEqual(raised.exception.category, "overflow")

        instance = self.make_adapter()
        instance.max_backlog_messages = 3
        async def get_messages(*_args, **_kwargs):
            return [{"id": value} for value in range(10, 0, -1)]
        instance._client = types.SimpleNamespace(get_messages=get_messages)
        with self.assertLogs(adapter.logger, level="WARNING"):
            backlog = await instance._fetch_initial_backlog("dm-room", None)
        self.assertEqual([message["id"] for message in backlog], [8, 9, 10])

    async def test_malformed_ocs_list_and_nonnumeric_status_are_categorized(self):
        client = adapter.NextcloudTalkClient("https://cloud.example", "bot", "secret")
        bad = [
            ["not-an-envelope"],
            {"ocs": {"meta": {"statuscode": "nope"}, "data": []}},
        ]
        for payload in bad:
            with self.subTest(payload=payload), patch.object(
                client, "_open_authenticated",
                return_value=FakeResponse(json.dumps(payload).encode(), {"Content-Type": "application/json"}),
            ):
                with self.assertRaises(adapter.NextcloudTalkAPIError) as raised:
                    client._ocs_get("/ocs")
                self.assertEqual(raised.exception.category, "protocol")

    async def test_ocs_envelope_requires_success_meta_and_expected_data_shape(self):
        bad = [
            {},
            {"ocs": []},
            {"ocs": {"meta": [], "data": {}}},
            {"ocs": {"meta": {"statuscode": 200}, "data": {}}},
            {"ocs": {"meta": {"status": "maybe", "statuscode": 200}, "data": {}}},
            {"ocs": {"meta": {"status": "ok", "statuscode": True}, "data": {}}},
            {"ocs": {"meta": {"status": "ok", "statuscode": 199}, "data": {}}},
            {"ocs": {"meta": {"status": "ok", "statuscode": 300}, "data": {}}},
            {"ocs": {"meta": {"status": "failure", "statuscode": 200}, "data": {}}},
            {"ocs": {"meta": {"status": "ok", "statuscode": 200}}},
            {"ocs": {"meta": {"status": "ok", "statuscode": 200}, "data": []}},
        ]
        for payload in bad:
            with self.subTest(payload=payload):
                with self.assertRaises(adapter.NextcloudTalkAPIError) as raised:
                    adapter.NextcloudTalkClient._ocs_data(payload, expected_types=(dict,))
                self.assertIn(raised.exception.category, {"protocol", "generic"})
        good = {"ocs": {"meta": {"status": "ok", "statuscode": "200"}, "data": {"id": 1}}}
        self.assertEqual(
            adapter.NextcloudTalkClient._ocs_data(good, expected_types=(dict,)), {"id": 1}
        )

    async def test_ocs_endpoints_reject_raw_or_wrong_data_types(self):
        client = adapter.NextcloudTalkClient("https://cloud.example", "bot", "secret")
        payloads = [
            [{"id": 1}],
            {"ocs": {"meta": {"status": "ok", "statuscode": 200}, "data": "not-list"}},
        ]
        for payload in payloads:
            with self.subTest(payload=payload), patch.object(
                client, "_open_authenticated",
                return_value=FakeResponse(json.dumps(payload).encode(), {"Content-Type": "application/json"}),
            ):
                with self.assertRaises(adapter.NextcloudTalkAPIError) as raised:
                    await client.get_messages("room", last_known_id=0, look_into_future=True, timeout=1)
                self.assertEqual(raised.exception.category, "protocol")
        wrong_object = {"ocs": {"meta": {"status": "ok", "statuscode": 200}, "data": []}}
        with patch.object(
            client, "_open_authenticated",
            return_value=FakeResponse(json.dumps(wrong_object).encode(), {"Content-Type": "application/json"}),
        ):
            with self.assertRaises(adapter.NextcloudTalkAPIError) as raised:
                await client.send_message("room", "hello")
            self.assertEqual(raised.exception.category, "protocol")

        malformed_rooms = {
            "ocs": {"meta": {"status": "ok", "statuscode": 200}, "data": ["not-an-object"]}
        }
        with patch.object(
            client, "_open_authenticated",
            return_value=FakeResponse(json.dumps(malformed_rooms).encode(), {"Content-Type": "application/json"}),
        ):
            with self.assertRaises(adapter.NextcloudTalkAPIError) as raised:
                await client.list_conversations()
            self.assertEqual(raised.exception.category, "protocol")

    async def test_missing_actor_type_is_ignored_and_cursor_advances(self):
        seen = []
        instance = self.make_adapter(lambda event: asyncio.sleep(0, result=seen.append(event)))
        await instance._handle_talk_message(
            {"id": 13, "actorId": "alice", "message": "no type"}, "dm-room"
        )
        self.assertEqual(seen, [])
        self.assertEqual(instance._last_message_ids["dm-room"], 13)

    async def test_cursor_file_is_atomic_mode_0600_and_temps_cleaned(self):
        instance = self.make_adapter()
        instance._last_message_ids = {"good": 42}
        instance._persist_cursors = types.MethodType(adapter.NextcloudTalkAdapter._persist_cursors, instance)
        with tempfile.TemporaryDirectory() as tmp:
            instance._cursor_path = Path(tmp) / "cursors.json"
            instance._persist_cursors()
            self.assertEqual(stat.S_IMODE(instance._cursor_path.stat().st_mode), 0o600)
            self.assertEqual(
                json.loads(instance._cursor_path.read_text()),
                {"version": 2, "rooms": {"good": {
                    "floor": 42, "successful": [], "initialized": False,
                    "last_seen": 0, "active": False,
                }}},
            )
            self.assertEqual([p.name for p in Path(tmp).iterdir()], ["cursors.json"])

    async def test_cursor_persistence_fsyncs_file_and_directory_and_replace_failure_is_atomic(self):
        instance = self.make_adapter()
        instance._persist_cursors = types.MethodType(adapter.NextcloudTalkAdapter._persist_cursors, instance)
        with tempfile.TemporaryDirectory() as tmp:
            instance._cursor_path = Path(tmp) / "cursors.json"
            instance._last_message_ids = {"room": 1}
            with patch.object(adapter.os, "fsync", wraps=os.fsync) as synced:
                instance._persist_cursors()
            self.assertGreaterEqual(synced.call_count, 2)
            original = instance._cursor_path.read_bytes()
            instance._last_message_ids = {"room": 2}
            with patch.object(adapter.os, "replace", side_effect=OSError("replace failed")):
                with self.assertRaises(OSError):
                    instance._persist_cursors()
            self.assertEqual(instance._cursor_path.read_bytes(), original)
            self.assertEqual([path.name for path in Path(tmp).iterdir()], ["cursors.json"])

    async def test_cursor_commit_rolls_back_memory_on_file_dir_and_replace_failures(self):
        for failure_stage in ("file_fsync", "dir_fsync", "dir_fsync_after_replace", "replace"):
            with self.subTest(failure_stage=failure_stage), tempfile.TemporaryDirectory() as tmp:
                instance = self.make_adapter()
                instance._persist_cursors = types.MethodType(
                    adapter.NextcloudTalkAdapter._persist_cursors, instance
                )
                instance._cursor_path = Path(tmp) / "cursors.json"
                instance._last_message_ids = {"room": 1}
                instance._persist_cursors()
                original = instance._cursor_path.read_bytes()

                real_fsync = os.fsync
                real_replace = os.replace
                replaced_cursor = False
                def injected_fsync(fd):
                    is_directory = stat.S_ISDIR(os.fstat(fd).st_mode)
                    if failure_stage == "dir_fsync" and is_directory:
                        raise OSError("injected directory fsync failure")
                    if failure_stage == "dir_fsync_after_replace" and is_directory and replaced_cursor:
                        raise OSError("injected post-replace directory fsync failure")
                    if failure_stage == "file_fsync" and not is_directory:
                        raise OSError("injected file fsync failure")
                    return real_fsync(fd)

                fsync_patch = patch.object(adapter.os, "fsync", side_effect=injected_fsync)
                if failure_stage == "replace":
                    replace_patch = patch.object(
                        adapter.os, "replace", side_effect=OSError("injected replace failure")
                    )
                elif failure_stage == "dir_fsync_after_replace":
                    def track_replace(source, destination):
                        nonlocal replaced_cursor
                        result = real_replace(source, destination)
                        if Path(destination) == instance._cursor_path:
                            replaced_cursor = True
                        return result
                    replace_patch = patch.object(adapter.os, "replace", side_effect=track_replace)
                else:
                    replace_patch = contextlib.nullcontext()
                with fsync_patch, replace_patch, self.assertRaises(OSError):
                    instance._commit_cursor("room", 2)
                self.assertEqual(instance._last_message_ids, {"room": 1})
                self.assertEqual(instance._cursor_path.read_bytes(), original)
                self.assertEqual([path.name for path in Path(tmp).iterdir()], ["cursors.json"])

    async def test_cursor_double_failure_reconciles_disk_and_preserves_recovery_backup(self):
        instance = self.make_adapter()
        instance._persist_cursors = types.MethodType(
            adapter.NextcloudTalkAdapter._persist_cursors, instance
        )
        with tempfile.TemporaryDirectory(prefix="PRIVATE-CURSOR-PATH-") as tmp:
            instance._cursor_path = Path(tmp) / "cursors.json"
            instance._last_message_ids = {"room": 1}
            instance._persist_cursors()

            real_fsync = os.fsync
            real_replace = os.replace
            replaced = False
            replace_calls = 0

            def injected_fsync(fd):
                if replaced and stat.S_ISDIR(os.fstat(fd).st_mode):
                    raise OSError("PRIVATE post-replace fsync detail")
                return real_fsync(fd)

            def injected_replace(source, destination):
                nonlocal replaced, replace_calls
                replace_calls += 1
                if replace_calls == 2:
                    raise OSError("PRIVATE rollback replace detail")
                result = real_replace(source, destination)
                if Path(destination) == instance._cursor_path:
                    replaced = True
                return result

            with patch.object(adapter.os, "fsync", side_effect=injected_fsync), patch.object(
                adapter.os, "replace", side_effect=injected_replace
            ), self.assertLogs(adapter.logger, level="WARNING") as logs:
                with self.assertRaises(adapter.CursorCacheError) as raised:
                    instance._commit_cursor("room", 2)

            self.assertEqual(instance._last_message_ids, {"room": 2})
            self.assertEqual(
                json.loads(instance._cursor_path.read_text()),
                {"version": 2, "rooms": {"room": {
                    "floor": 1, "successful": [2], "initialized": False,
                    "last_seen": 1, "active": False,
                }}},
            )
            artifacts = [path for path in Path(tmp).iterdir() if path != instance._cursor_path]
            self.assertEqual(len(artifacts), 1)
            self.assertEqual(
                json.loads(artifacts[0].read_text()),
                {"version": 2, "rooms": {"room": {
                    "floor": 1, "successful": [], "initialized": False,
                    "last_seen": 0, "active": False,
                }}},
            )
            restarted = self.make_adapter()
            restarted._last_message_ids = {}
            restarted._cursor_path = instance._cursor_path
            restarted._load_cursors()
            self.assertEqual(restarted._last_message_ids, instance._last_message_ids)
            rendered = "\n".join(logs.output) + str(raised.exception)
            self.assertNotIn(tmp, rendered)
            self.assertNotIn("PRIVATE", rendered)

    async def test_cursor_commit_rolls_back_memory_on_persist_failure_and_threads_serialize(self):
        instance = self.make_adapter()
        instance._last_message_ids = {"room": 4}
        instance._persist_cursors = lambda: (_ for _ in ()).throw(OSError("disk full"))
        with self.assertRaises(OSError):
            instance._commit_cursor("room", 5)
        self.assertEqual(instance._last_message_ids, {"room": 4})

        instance._persist_cursors = types.MethodType(adapter.NextcloudTalkAdapter._persist_cursors, instance)
        with tempfile.TemporaryDirectory() as tmp:
            instance._cursor_path = Path(tmp) / "cursors.json"
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
                list(pool.map(lambda value: instance._commit_cursor("room", value), range(5, 105)))
            self.assertEqual(instance._last_message_ids["room"], 104)
            persisted = json.loads(instance._cursor_path.read_text())
            self.assertEqual(persisted["version"], 2)
            self.assertEqual(max(persisted["rooms"]["room"]["successful"]), 104)

    async def test_attachment_success_log_contains_only_safe_basename_hash_and_byte_count(self):
        client = adapter.NextcloudTalkClient(
            "https://cloud.example", "bot-user", "PASSWORD-CREDENTIAL"
        )
        response_body = b"PRIVATE-RESPONSE-BODY"
        response = FakeResponse(
            response_body,
            {"Content-Type": "application/octet-stream",
             "Content-Length": str(len(response_body)),
             "Content-Disposition": 'attachment; filename="unsafe\\nname.bin"'},
        )
        room_token = "PRIVATE-ROOM-TOKEN"
        signed_query = "PRIVATE-SIGNED-QUERY"
        with tempfile.TemporaryDirectory(
            prefix="PRIVATE-CACHE-PATH-PASSWORD-CREDENTIAL-"
        ) as tmp, patch.object(client, "_open_authenticated", return_value=response), self.assertLogs(
            adapter.logger, level="INFO"
        ) as captured:
            local_path = client._download_file(
                f"/download/file?signature={signed_query}", tmp,
                cache_identity=f"{room_token}:message:attachment",
            )
        rendered = "\n".join(captured.output)
        for secret in (
            tmp, str(Path(local_path).parent), client.base_url, signed_query,
            "PASSWORD-CREDENTIAL", room_token, response_body.decode(), "unsafe\\nname.bin",
        ):
            self.assertNotIn(secret, rendered)
        self.assertIn(Path(local_path).name, rendered)
        self.assertIn(f"{len(response_body)} bytes", rendered)
        self.assertFalse(any(adapter.unicodedata.category(char) == "Cc" for char in rendered))

    async def test_log_sanitization_removes_controls_credentials_and_response_bodies(self):
        dirty = "line\nnext\u0085last\x00"
        clean = adapter._safe_log_text(dirty)
        self.assertFalse(any(adapter.unicodedata.category(char) == "Cc" for char in clean))
        client = adapter.NextcloudTalkClient("https://cloud.example", "bot", "TOP-SECRET")
        http_error = adapter.error.HTTPError(
            "https://cloud.example/ocs", 500, "failure", {}, io.BytesIO(b"PRIVATE-BODY\nInjected")
        )
        with patch.object(client, "_open_authenticated", side_effect=http_error):
            with self.assertRaises(adapter.NextcloudTalkAPIError) as raised:
                client._ocs_get("/ocs")
        rendered = str(raised.exception)
        self.assertNotIn("TOP-SECRET", rendered)
        self.assertNotIn("PRIVATE-BODY", rendered)
        self.assertFalse(any(adapter.unicodedata.category(char) == "Cc" for char in rendered))

    async def test_non_loopback_http_requires_explicit_opt_in(self):
        env = {"NEXTCLOUD_TALK_URL": "http://cloud.example", "NEXTCLOUD_TALK_USERNAME": "bot",
               "NEXTCLOUD_TALK_PASSWORD": "secret", "NEXTCLOUD_TALK_AUTO_DISCOVER_ROOMS": "true"}
        with patch.dict(os.environ, env, clear=True):
            self.assertFalse(adapter.validate_config(PlatformConfig()))
        env["NEXTCLOUD_TALK_ALLOW_INSECURE_HTTP"] = "true"
        with patch.dict(os.environ, env, clear=True):
            self.assertTrue(adapter.validate_config(PlatformConfig()))
        env.pop("NEXTCLOUD_TALK_ALLOW_INSECURE_HTTP")
        env["NEXTCLOUD_TALK_URL"] = "http://127.0.0.1:8080"
        with patch.dict(os.environ, env, clear=True):
            self.assertTrue(adapter.validate_config(PlatformConfig()))

    async def test_authenticated_network_failures_are_fixed_safe_across_all_paths(self):
        markers = (
            "/PRIVATE/network/path", "CREDENTIAL", "RESPONSE-BODY",
            "secret=PRIVATE-QUERY", "cloud.example",
        )
        operations = {
            "chat": lambda client, tmp: client._request(
                "GET", "PRIVATE-ROOM", query={"secret": "PRIVATE-QUERY"},
                expected_types=(list,),
            ),
            "ocs_get": lambda client, tmp: client._ocs_get(
                "/ocs/private", {"secret": "PRIVATE-QUERY"}
            ),
            "ocs_post": lambda client, tmp: client._ocs_post(
                "/ocs/private", {"secret": "PRIVATE-QUERY"}
            ),
            "raw": lambda client, tmp: client._raw_request(
                "GET", "https://cloud.example/private?secret=PRIVATE-QUERY"
            ),
            "webdav": lambda client, tmp: client._ensure_folder_sync(
                "/PRIVATE/network/path"
            ),
            "download": lambda client, tmp: client._download_file(
                "/private/download?secret=PRIVATE-QUERY", tmp,
                cache_identity="PRIVATE-ROOM:CREDENTIAL",
            ),
        }
        failures = {
            "nested_urlerror": lambda: adapter.error.URLError(
                OSError("/PRIVATE/network/path CREDENTIAL RESPONSE-BODY")
            ),
            "timeout": lambda: TimeoutError(
                "/PRIVATE/network/path CREDENTIAL RESPONSE-BODY"
            ),
        }

        for operation_name, operation in operations.items():
            for failure_name, make_failure in failures.items():
                with self.subTest(operation=operation_name, failure=failure_name), tempfile.TemporaryDirectory() as tmp:
                    client = adapter.NextcloudTalkClient(
                        "https://cloud.example", "PRIVATE-USER", "CREDENTIAL"
                    )

                    def invoke():
                        failure = make_failure()
                        with patch.object(
                            client, "_open_authenticated", side_effect=failure
                        ):
                            try:
                                operation(client, tmp)
                            except adapter.NextcloudTalkAPIError as exc:
                                self.assertIs(exc.__cause__, failure)
                                raise

                    with self.assertRaises(adapter.NextcloudTalkAPIError) as raised:
                        invoke()
                    self.assertEqual(raised.exception.category, "network")
                    self.assertEqual(str(raised.exception), "network request failed")

                    send_adapter = self.make_adapter(lambda _event: asyncio.sleep(0))
                    send_adapter.room_tokens = ["PRIVATE-ROOM"]
                    send_adapter.max_message_length = 32000

                    async def send_message(*_args, **_kwargs):
                        return invoke()

                    send_adapter._client = types.SimpleNamespace(send_message=send_message)
                    result = await send_adapter.send("PRIVATE-ROOM", "hello")
                    self.assertFalse(result.success)
                    # A Talk chat POST may already have been accepted before a
                    # network/read failure, so blindly retrying can duplicate
                    # a visible message. The transport error remains safely
                    # categorized, but outbound delivery is conservative.
                    self.assertFalse(result.retryable)
                    self.assertEqual(result.error, "delivery outcome unknown (write timed out)")
                    self.assertEqual(
                        result.raw_response, {"category": "ambiguous_delivery"}
                    )

                    poll_adapter = self.make_adapter(lambda _event: asyncio.sleep(0))
                    poll_adapter.poll_timeout = 1
                    poll_adapter.max_poll_batch = 10

                    async def get_messages(*_args, **_kwargs):
                        return invoke()

                    poll_adapter._client = types.SimpleNamespace(get_messages=get_messages)
                    with self.assertLogs(adapter.logger, level="WARNING") as captured:
                        await poll_adapter._poll_room("PRIVATE-ROOM")
                    rendered = "\n".join(captured.output)
                    self.assertIn("network request failed", rendered)
                    self.assertNotIn("PRIVATE-ROOM", rendered)
                    self.assertNotIn("PRIVATE-ROOM", poll_adapter._last_message_ids)

                    outward = "\n".join((
                        str(raised.exception), result.error, rendered,
                    ))
                    for marker in markers:
                        self.assertNotIn(marker, outward)


if __name__ == "__main__": unittest.main()
