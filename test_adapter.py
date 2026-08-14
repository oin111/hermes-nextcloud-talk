import asyncio
import io
import json
import os
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
class MessageEvent:
    def __init__(self, **kwargs): self.__dict__.update(kwargs)
class MessageType: TEXT = "text"
class SendResult:
    def __init__(self, **kwargs): self.__dict__.update(kwargs)
gateway_base.BasePlatformAdapter = BasePlatformAdapter
gateway_base.MessageEvent = MessageEvent
gateway_base.MessageType = MessageType
gateway_base.SendResult = SendResult
sys.modules.setdefault("gateway", types.ModuleType("gateway"))
sys.modules["gateway.config"] = gateway_config
sys.modules.setdefault("gateway.platforms", types.ModuleType("gateway.platforms"))
sys.modules["gateway.platforms.base"] = gateway_base

import adapter


class FakeResponse:
    def __init__(self, body=b"data", headers=None, status=200):
        self._body = io.BytesIO(body)
        self.headers = headers or {"Content-Type": "application/octet-stream"}
        self.status = status
    def read(self, size=-1): return self._body.read(size)
    def __enter__(self): return self
    def __exit__(self, *args): return False


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


class SharePolicyTests(unittest.IsolatedAsyncioTestCase):
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
        _path, share, native = await client.upload_and_share_file("x", "room", "/Hermes")
        self.assertFalse(native)
        self.assertEqual(share["url"], "https://cloud.example/s/ok")

    async def test_failed_public_fallback_is_returned_as_delivery_failure(self):
        instance = self._attachment_adapter()
        instance._client.upload_and_share_file = lambda *a: asyncio.sleep(0, result=("/f", {"url": "https://c/s"}, False))
        instance.send = lambda *a, **k: asyncio.sleep(0, result=SendResult(success=False, error="send failed"))
        result = await instance._send_file_attachment("room", __file__, caption="caption")
        self.assertFalse(result.success)
        self.assertIn("send failed", result.error)
        self.assertFalse(result.retryable)
        self.assertTrue(result.raw_response["partial_delivery"])

    async def test_captioned_native_image_propagates_caption_failure_and_no_share_message_id(self):
        instance = self._attachment_adapter()
        instance._client.upload_and_share_file = lambda *a: asyncio.sleep(0, result=("/f", {"id": 999}, True))
        instance.send = lambda *a, **k: asyncio.sleep(0, result=SendResult(success=False, error="caption failed"))
        result = await instance._send_file_attachment("room", __file__, caption="caption")
        self.assertFalse(result.success)
        self.assertIn("caption failed", result.error)
        self.assertFalse(result.retryable)
        self.assertTrue(result.raw_response["attachment_delivered"])

    async def test_followup_exception_after_share_is_partial_and_nonretryable(self):
        instance = self._attachment_adapter()
        instance._client.upload_and_share_file = lambda *a: asyncio.sleep(
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
        instance.allow_all = True
        instance.bot_name = "Hermes"
        instance._last_message_ids = {}
        instance._room_types = {"dm-room": 1}
        instance._client = None
        instance._persist_cursors = lambda: None
        instance.handle_message = handler
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
        self.assertTrue(instance._is_allowed("", "Guest"))

    async def test_captioned_image_media_reaches_handler(self):
        handled = []
        instance = self.make_adapter(lambda event: asyncio.sleep(0, result=handled.append(event)))
        instance._client = types.SimpleNamespace(
            _dav_url=lambda path: f"https://cloud.example/dav/{path}",
            _download_file=lambda url, download_dir: "/tmp/image.png",
        )
        msg = {"id": 7, "actorType": "users", "actorId": "alice", "actorDisplayName": "Alice",
               "message": "caption", "messageParameters": {"file": {"type": "file", "name": "image.png", "path": "Talk/image.png", "mimetype": "image/png"}}}
        await instance._handle_talk_message(msg, "dm-room")
        self.assertEqual(handled[0].media_urls, ["/tmp/image.png"])
        self.assertEqual(handled[0].media_types, ["image/png"])

    async def test_attachment_failure_blocks_dispatch_and_cursor_commit(self):
        handled = []
        instance = self.make_adapter(lambda event: asyncio.sleep(0, result=handled.append(event)))
        instance._client = types.SimpleNamespace(
            _dav_url=lambda path: f"https://cloud.example/dav/{path}",
            _download_file=lambda *a: (_ for _ in ()).throw(
                adapter.AttachmentDownloadError("too large", category="overflow")
            ),
        )
        msg = {"id": 8, "actorType": "users", "actorId": "alice", "message": "caption",
               "messageParameters": {"file": {"type": "file", "path": "Talk/a", "link": "/f/a"}}}
        with self.assertRaises(adapter.AttachmentDownloadError):
            await instance._handle_talk_message(msg, "dm-room")
        self.assertEqual(handled, [])
        self.assertNotIn("dm-room", instance._last_message_ids)

    async def test_poll_sorts_deduplicates_skips_cursor_and_ignores_malformed_ids(self):
        seen = []
        instance = self.make_adapter(lambda event: asyncio.sleep(0))
        instance.poll_timeout = 1
        instance._last_message_ids = {"dm-room": 5}
        batch = [{"id": 7}, {"id": 5}, {"id": "6"}, {"id": 7}, {"id": "bad"}, {}, None]
        async def get_messages(*_args, **_kwargs): return batch
        instance._client = types.SimpleNamespace(get_messages=get_messages)
        async def record(msg, room): seen.append((int(msg["id"]), room))
        instance._handle_talk_message = record
        with self.assertLogs(adapter.logger, level="WARNING") as logs:
            await instance._poll_room("dm-room")
        self.assertEqual(seen, [(6, "dm-room"), (7, "dm-room")])
        self.assertTrue(any("malformed/missing IDs" in line for line in logs.output))

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
        client._ocs_get = lambda *a: [{"token": "dm", "type": 1}, {"token": "group", "type": 2}]
        rooms = await client.list_conversations()
        self.assertEqual(rooms, [{"token": "dm", "type": 1}, {"token": "group", "type": 2}])

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

    async def test_only_additional_chunks_are_continuation_ids(self):
        instance = self.make_adapter([11, 12, 13])
        result = await instance.send("room", "abcdefghijkl")
        self.assertEqual(result.message_id, "13")
        self.assertEqual(result.continuation_message_ids, ("11", "12"))

    async def test_attachment_success_preserves_followup_continuation_ids(self):
        instance = SharePolicyTests._attachment_adapter()
        instance._client.upload_and_share_file = lambda *a: asyncio.sleep(
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
        instance._client.upload_and_share_file = lambda *a: asyncio.sleep(
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
        self.assertEqual(instance._last_message_ids, {"good": 42, "also_good": 7})


class ConfigurationTests(unittest.TestCase):
    def test_tokenless_auto_discovery_configuration_and_registration(self):
        env = {"NEXTCLOUD_TALK_URL": "https://cloud.example", "NEXTCLOUD_TALK_USERNAME": "bot",
               "NEXTCLOUD_TALK_PASSWORD": "secret", "NEXTCLOUD_TALK_AUTO_DISCOVER_ROOMS": "true"}
        with patch.dict(os.environ, env, clear=True): self.assertTrue(adapter.validate_config(PlatformConfig()))
        captured = {}
        adapter.register(types.SimpleNamespace(register_platform=lambda **kwargs: captured.update(kwargs)))
        self.assertEqual(captured["required_env"], ["NEXTCLOUD_TALK_URL", "NEXTCLOUD_TALK_USERNAME", "NEXTCLOUD_TALK_PASSWORD"])

    def test_outbound_size_limit_fails_before_upload(self):
        instance = adapter.NextcloudTalkAdapter.__new__(adapter.NextcloudTalkAdapter)
        instance._client = types.SimpleNamespace(upload_and_share_file=lambda *a: None)
        instance.room_tokens = ["room"]
        instance.upload_folder = "/Hermes"
        instance.max_upload_bytes = 1
        result = asyncio.run(instance._send_file_attachment("room", __file__))
        self.assertFalse(result.success)
        self.assertIn("size limit", result.error.lower())


if __name__ == "__main__": unittest.main()
