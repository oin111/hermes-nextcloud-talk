"""Integration probes against the installed Hermes 0.20.1 gateway lifecycle."""

import asyncio
import json
import os
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

from gateway.config import PlatformConfig
from gateway.platform_registry import PlatformEntry, platform_registry
from gateway.platforms.base import BasePlatformAdapter, ProcessingOutcome, SendResult
from gateway.run import GatewayRunner
from gateway.session import build_session_key

import adapter


if not platform_registry.is_registered("nextcloud_talk"):
    platform_registry.register(
        PlatformEntry(
            name="nextcloud_talk",
            label="Nextcloud Talk",
            adapter_factory=lambda cfg: adapter.NextcloudTalkAdapter(cfg),
            check_fn=lambda: True,
            source="builtin",  # global test registration, matching pre-construction load order
            allowed_users_env="NEXTCLOUD_TALK_ALLOWED_USERS",
            allow_all_env="NEXTCLOUD_TALK_ALLOW_ALL_USERS",
        )
    )


_MESSAGE = {
    "actorType": "users",
    "actorId": "alice",
    "actorDisplayName": "Alice",
    "message": "hello",
}


class RealHermesLifecycleTests(unittest.IsolatedAsyncioTestCase):
    def test_combined_suite_uses_installed_real_hermes_classes(self):
        self.assertEqual(adapter.BasePlatformAdapter.__module__, "gateway.platforms.base")
        self.assertEqual(PlatformConfig.__module__, "gateway.config")
        self.assertEqual(GatewayRunner.__module__, "gateway.run")
        self.assertNotIn("adapter_standalone", adapter.NextcloudTalkAdapter.__mro__[1].__module__)

    def make_adapter(self, cache_dir: str) -> adapter.NextcloudTalkAdapter:
        config = PlatformConfig(
            enabled=True,
            typing_indicator=False,
            extra={
                "url": "https://cloud.example",
                "username": "bot",
                "password": "secret",
                "room_tokens": "room",
                "auto_discover_rooms": False,
                "allow_from": ["alice"],
                "group_allow_from": ["alice"],
            },
        )
        instance = adapter.NextcloudTalkAdapter(config)
        instance._cursor_path = Path(cache_dir) / "cursors.json"
        instance._room_types = {"room": 1}
        # Keep this probe focused on lifecycle; authorization has separate real-core tests.
        instance.allow_all = True
        return instance

    async def wait_for_background(self, instance, timeout: float = 2.0):
        deadline = asyncio.get_running_loop().time() + timeout
        while instance._background_tasks and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.01)
        self.assertFalse(instance._background_tasks, "Hermes background handler did not finish")

    async def test_ambiguous_talk_post_through_real_base_is_not_retried_or_acked(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            post_calls = []

            async def ambiguous_post(*_args, **kwargs):
                post_calls.append(kwargs["reference_id"])
                raise adapter.NextcloudTalkAPIError(
                    "network request failed", category="network"
                )

            instance._client = types.SimpleNamespace(send_message=ambiguous_post)
            instance.set_message_handler(lambda _event: asyncio.sleep(0, result="reply"))
            await instance._handle_talk_message({**_MESSAGE, "id": 904}, "room")
            await self.wait_for_background(instance)
            self.assertEqual(len(post_calls), 1)
            self.assertFalse(instance._is_acknowledged("room", 904))

    async def test_ambiguous_put_through_real_base_performs_one_upload(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            source = Path(tmp) / "private-upload-name.txt"
            source.write_bytes(b"payload")
            client = adapter.NextcloudTalkClient("https://cloud.example", "bot", "secret")
            client._ensure_folder_sync = lambda *_args: None
            put_calls = []

            def ambiguous_put(method, url, *, data=None, headers=None):
                put_calls.append(method)
                while data.read(64):
                    pass
                raise adapter.NextcloudTalkAPIError(
                    "network request failed", category="network"
                )

            client._raw_request = ambiguous_put
            instance._client = client
            instance.send = lambda chat_id, content, **_kwargs: instance._send_file_attachment(
                chat_id, content
            )
            result = await BasePlatformAdapter._send_with_retry(
                instance, chat_id="room", content=str(source), base_delay=0
            )
            self.assertFalse(result.success)
            self.assertFalse(result.retryable)
            self.assertEqual(result.error, "attachment upload outcome unknown (write timed out)")
            self.assertEqual(result.raw_response, {"category": "ambiguous_upload"})
            self.assertEqual(put_calls, ["PUT"])
            self.assertNotIn("private-upload-name", repr(result.__dict__))

    async def test_media_failure_overrides_core_success_and_is_generation_scoped(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            image = Path(tmp) / "probe.png"
            image.write_bytes(b"png")
            instance.set_message_handler(
                lambda _event: asyncio.sleep(
                    0, result="text succeeded\n![probe](https://example.invalid/probe.png)"
                )
            )
            instance.send = lambda **_kwargs: asyncio.sleep(
                0, result=SendResult(success=True, message_id="text")
            )
            media_ok = False
            async def send_image(**_kwargs):
                return SendResult(success=media_ok, error=None if media_ok else "image failed")
            instance.send_image = send_image

            await instance._handle_talk_message({**_MESSAGE, "id": 901}, "room")
            await self.wait_for_background(instance)
            self.assertFalse(instance._is_acknowledged("room", 901))

            media_ok = True
            await instance._handle_talk_message({**_MESSAGE, "id": 902}, "room")
            await self.wait_for_background(instance)
            self.assertTrue(instance._is_acknowledged("room", 902))
            self.assertFalse(instance._generation_outcomes)

    async def test_cursor_stays_uncommitted_until_real_processing_success(self):
        with tempfile.TemporaryDirectory() as tmp, patch.dict(os.environ, {}, clear=False):
            instance = self.make_adapter(tmp)
            started = asyncio.Event()
            release = asyncio.Event()

            async def handler(_event):
                started.set()
                await release.wait()
                return None

            instance.set_message_handler(handler)
            await instance._handle_talk_message({**_MESSAGE, "id": 42}, "room")
            await asyncio.wait_for(started.wait(), timeout=1)

            self.assertNotIn("room", instance._last_message_ids)
            self.assertFalse(instance._cursor_path.exists())

            release.set()
            await self.wait_for_background(instance)
            self.assertEqual(instance._last_message_ids["room"], 42)
            persisted = json.loads(instance._cursor_path.read_text(encoding="utf-8"))
            self.assertEqual(persisted["rooms"]["room"]["successful"], [42])

    async def test_background_failure_is_retryable_after_restart(self):
        with tempfile.TemporaryDirectory() as tmp:
            failed = self.make_adapter(tmp)

            async def fail(_event):
                raise RuntimeError("agent failed")

            failed.set_message_handler(fail)
            await failed._handle_talk_message({**_MESSAGE, "id": 51}, "room")
            await self.wait_for_background(failed)
            self.assertNotIn("room", failed._last_message_ids)

            retried = self.make_adapter(tmp)
            retried._load_cursors()
            seen = []

            async def succeed(event):
                seen.append(event.message_id)
                return None

            retried.set_message_handler(succeed)
            await retried._handle_talk_message({**_MESSAGE, "id": 51}, "room")
            await self.wait_for_background(retried)
            self.assertEqual(seen, ["51"])
            self.assertEqual(retried._last_message_ids["room"], 51)

    async def test_delayed_lower_id_and_duplicate_are_handled_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            seen = []

            async def handler(event):
                seen.append(int(event.message_id))
                return None

            instance.set_message_handler(handler)
            for message_id in (8, 7, 8, 7):
                await instance._handle_talk_message({**_MESSAGE, "id": message_id}, "room")
                await self.wait_for_background(instance)
            self.assertEqual(seen, [8, 7])

    def test_effective_overlap_retains_exact_ack_without_flooring_recent_delayed_id(self):
        delayed_id = 4900
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            instance.ack_overlap_ids = 199
            instance.ack_retention_count = 2048
            instance.max_poll_batch = 200
            instance._ensure_ack_runtime()
            instance._persist_cursors = lambda: None

            for message_id in range(1, 5001):
                if message_id != delayed_id:
                    instance._commit_cursor("room", message_id)

            state = instance._ack_rooms["room"]
            self.assertEqual(instance.ack_retention_count, 2048)
            self.assertEqual(state["floor"], 4801)
            self.assertFalse(instance._is_acknowledged("room", delayed_id))
            self.assertEqual(instance._poll_anchor("room"), 4801)
            self.assertEqual(len(state["successful"]), 198)

            instance._commit_cursor("room", delayed_id)
            self.assertTrue(instance._is_acknowledged("room", delayed_id))
            before = set(state["successful"])
            instance._commit_cursor("room", delayed_id)
            self.assertEqual(state["successful"], before)

    def test_legacy_oversized_overlap_normalizes_and_persists_on_restart(self):
        delayed_id = 4900
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            instance._cursor_path.parent.mkdir(parents=True, exist_ok=True)
            legacy_successful = [
                message_id for message_id in range(1, 5003)
                if message_id != delayed_id
            ]
            instance._cursor_path.write_text(json.dumps({
                "version": 2,
                "rooms": {
                    "room": {
                        "floor": 0,
                        "successful": legacy_successful,
                        "initialized": True,
                        "last_seen": 1,
                        "active": True,
                    }
                },
            }), encoding="utf-8")

            reduced = self.make_adapter(tmp)
            reduced.max_poll_batch = 200
            reduced.ack_overlap_ids = 199
            reduced.ack_retention_count = 4096
            reduced._load_cursors()

            state = reduced._ack_rooms["room"]
            self.assertTrue(state["initialized"])
            self.assertEqual(state["floor"], 4803)
            self.assertEqual(reduced._poll_anchor("room"), 4803)
            self.assertEqual(len(state["successful"]), 198)
            self.assertFalse(reduced._is_acknowledged("room", delayed_id))
            self.assertTrue(reduced._is_acknowledged("room", 5002))

            before_duplicate = set(state["successful"])
            reduced._commit_cursor("room", 5002)
            self.assertEqual(state["successful"], before_duplicate)
            self.assertEqual(
                len(json.loads(reduced._cursor_path.read_text(encoding="utf-8"))
                    ["rooms"]["room"]["successful"]),
                198,
            )

            reduced._commit_cursor("room", delayed_id)
            self.assertTrue(reduced._is_acknowledged("room", delayed_id))
            persisted_reduced = json.loads(
                reduced._cursor_path.read_text(encoding="utf-8")
            )["rooms"]["room"]
            self.assertTrue(persisted_reduced["initialized"])
            self.assertEqual(persisted_reduced["floor"], 4803)
            self.assertEqual(len(persisted_reduced["successful"]), 199)

            restarted = self.make_adapter(tmp)
            restarted.max_poll_batch = 200
            restarted.ack_overlap_ids = 199
            restarted.ack_retention_count = 4096
            restarted._load_cursors()
            self.assertTrue(restarted._is_room_initialized("room"))
            self.assertEqual(restarted._ack_rooms["room"], reduced._ack_rooms["room"])
            self.assertEqual(restarted._poll_anchor("room"), 4803)

    async def test_busy_room_leaves_followup_retryable_and_failure_uncommitted(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            release = asyncio.Event()
            calls = []

            async def handler(event):
                calls.append(int(event.message_id))
                if event.message_id == "1":
                    await release.wait()
                    return None
                if calls.count(2) == 1:
                    raise RuntimeError("queued turn failed")
                return None

            instance.set_message_handler(handler)
            await instance._handle_talk_message({**_MESSAGE, "id": 1}, "room")
            await instance._handle_talk_message({**_MESSAGE, "id": 2}, "room")
            self.assertEqual(calls, [])
            release.set()
            await self.wait_for_background(instance)
            self.assertEqual(calls, [1])

            await instance._handle_talk_message({**_MESSAGE, "id": 2}, "room")
            await self.wait_for_background(instance)
            self.assertNotIn(2, instance._ack_rooms["room"]["successful"])
            await instance._handle_talk_message({**_MESSAGE, "id": 2}, "room")
            await self.wait_for_background(instance)
            self.assertEqual(calls, [1, 2, 2])
            self.assertIn(2, instance._ack_rooms["room"]["successful"])

    async def test_deterministic_ignore_commits_immediately(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            await instance._handle_talk_message(
                {"id": 60, "actorType": "bots", "actorId": "service", "message": "ignore"},
                "room",
            )
            self.assertTrue(instance._is_acknowledged("room", 60))

    async def test_discovery_initialization_failure_retries(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            instance.auto_discover_rooms = True
            instance._configured_room_tokens = []
            instance.room_tokens = []
            instance.discovery_interval = 0
            instance._client = types.SimpleNamespace(
                list_conversations=lambda: asyncio.sleep(0, result=[{"token": "dm", "type": 1}])
            )
            attempts = []

            async def initialize(token):
                attempts.append(token)
                if len(attempts) == 1:
                    raise RuntimeError("transient init failure")

            instance._initialize_room = initialize
            with self.assertRaises(RuntimeError):
                await instance._refresh_discovered_rooms(force=True)
            await instance._refresh_discovered_rooms(force=True)
            self.assertEqual(attempts, ["dm", "dm"])
            self.assertEqual(instance._discovered_room_tokens, {"dm"})

    async def test_partial_initialization_retries_only_failed_id_and_survives_restart(self):
        with tempfile.TemporaryDirectory() as tmp:
            first = self.make_adapter(tmp)
            first.auto_discover_rooms = True
            first._configured_room_tokens = []
            first.room_tokens = []
            first.discovery_interval = 0
            first._room_types = {"dm": 1}
            backlog = [{**_MESSAGE, "id": 1}, {**_MESSAGE, "id": 2}]
            first._client = types.SimpleNamespace(
                list_conversations=lambda: asyncio.sleep(0, result=[{"token": "dm", "type": 1}])
            )
            first._fetch_initial_backlog = lambda *_args: asyncio.sleep(0, result=backlog)
            first_calls = []

            async def fail_second(event):
                first_calls.append(int(event.message_id))
                if event.message_id == "2":
                    raise RuntimeError("id 2 failed")

            first.set_message_handler(fail_second)
            with self.assertRaises(RuntimeError):
                await first._refresh_discovered_rooms(force=True)
            self.assertEqual(first_calls, [1, 2])
            self.assertFalse(first._is_room_initialized("dm"))
            self.assertNotIn("dm", first._discovered_room_tokens)

            restarted = self.make_adapter(tmp)
            restarted.auto_discover_rooms = True
            restarted._configured_room_tokens = []
            restarted.room_tokens = []
            restarted.discovery_interval = 0
            restarted._room_types = {"dm": 1}
            restarted._load_cursors()
            restarted._client = types.SimpleNamespace(
                list_conversations=lambda: asyncio.sleep(0, result=[{"token": "dm", "type": 1}])
            )
            restarted._fetch_initial_backlog = lambda *_args: asyncio.sleep(0, result=backlog)
            retry_calls = []

            async def succeed(event):
                retry_calls.append(int(event.message_id))

            restarted.set_message_handler(succeed)
            await restarted._refresh_discovered_rooms(force=True)
            self.assertEqual(retry_calls, [2])
            self.assertTrue(restarted._is_room_initialized("dm"))
            self.assertEqual(restarted._discovered_room_tokens, {"dm"})

    async def test_preexisting_busy_consumed_control_paths_ack_once_and_restart_cleanly(self):
        for path_name in ("steer", "redirect", "approval", "draining-reject"):
            with self.subTest(path=path_name), tempfile.TemporaryDirectory() as tmp:
                instance = self.make_adapter(tmp)
                instance.set_message_handler(lambda _event: asyncio.sleep(0))
                source = instance.build_source(
                    chat_id="room", chat_type="dm", user_id="alice", user_name="Alice"
                )
                key = build_session_key(
                    source,
                    group_sessions_per_user=instance.config.extra.get("group_sessions_per_user", True),
                    thread_sessions_per_user=instance.config.extra.get("thread_sessions_per_user", False),
                )
                instance._active_sessions[key] = asyncio.Event()
                calls = []

                async def consume(event, session_key):
                    calls.append((event.message_id, session_key))
                    return True

                instance.set_busy_session_handler(consume)
                await asyncio.wait_for(
                    instance._handle_talk_message({**_MESSAGE, "id": 80}, "room"),
                    timeout=1,
                )
                self.assertEqual(calls, [("80", key)])
                self.assertTrue(instance._is_acknowledged("room", 80))
                self.assertFalse(instance._inflight_message_ids.get("room"))
                restarted = self.make_adapter(tmp)
                restarted._load_cursors()
                self.assertTrue(restarted._is_acknowledged("room", 80))

    async def test_preexisting_busy_queued_input_waits_for_real_outcome(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            instance.set_message_handler(lambda _event: asyncio.sleep(0))
            source = instance.build_source(chat_id="room", chat_type="dm", user_id="alice")
            key = build_session_key(
                source,
                group_sessions_per_user=instance.config.extra.get("group_sessions_per_user", True),
                thread_sessions_per_user=instance.config.extra.get("thread_sessions_per_user", False),
            )
            instance._active_sessions[key] = asyncio.Event()
            captured = []

            async def queue(event, session_key):
                captured.append(event)
                instance._pending_messages[session_key] = event
                return True

            instance.set_busy_session_handler(queue)
            await instance._handle_talk_message({**_MESSAGE, "id": 81}, "room")
            self.assertFalse(instance._is_acknowledged("room", 81))
            self.assertEqual(instance._inflight_message_ids["room"], {81})
            await instance.on_processing_complete(captured[0], ProcessingOutcome.FAILURE)
            self.assertFalse(instance._inflight_message_ids.get("room"))
            self.assertFalse(instance._is_acknowledged("room", 81))
            await instance._handle_talk_message({**_MESSAGE, "id": 81}, "room")
            await instance.on_processing_complete(captured[-1], ProcessingOutcome.SUCCESS)
            self.assertTrue(instance._is_acknowledged("room", 81))

    async def test_busy_commands_success_ack_but_failure_and_unknown_remain_retryable(self):
        for message_id, text, should_ack, fail in (
            (82, "/status", True, False),
            (83, "/status", False, True),
        ):
            with self.subTest(message_id=message_id), tempfile.TemporaryDirectory() as tmp:
                instance = self.make_adapter(tmp)
                source = instance.build_source(chat_id="room", chat_type="dm", user_id="alice")
                key = build_session_key(
                    source,
                    group_sessions_per_user=instance.config.extra.get("group_sessions_per_user", True),
                    thread_sessions_per_user=instance.config.extra.get("thread_sessions_per_user", False),
                )
                instance._active_sessions[key] = asyncio.Event()

                async def command(_event):
                    if fail:
                        raise RuntimeError("control failed")
                    return None

                instance.set_message_handler(command)
                await instance._handle_talk_message({**_MESSAGE, "id": message_id, "message": text}, "room")
                self.assertEqual(instance._is_acknowledged("room", message_id), should_ack)
                self.assertFalse(instance._inflight_message_ids.get("room"))

        with tempfile.TemporaryDirectory() as tmp:
            unknown = self.make_adapter(tmp)
            unknown.set_message_handler(lambda _event: asyncio.sleep(0))
            original_build = unknown.build_source

            def mismatched_source(**kwargs):
                source = original_build(**kwargs)
                return source

            unknown.build_source = mismatched_source
            # Base rejects a strict internal route mismatch without a hook.
            message = {**_MESSAGE, "id": 84}
            await unknown._handle_talk_message(message, "room")
            await self.wait_for_background(unknown)
            # Ordinary fresh dispatch is still a real outcome, covered above;
            # explicitly exercise the no-handler unknown path instead.
            unknown._message_handler = None
            await unknown._handle_talk_message({**_MESSAGE, "id": 85}, "room")
            self.assertFalse(unknown._is_acknowledged("room", 85))
            self.assertFalse(unknown._inflight_message_ids.get("room"))

    async def test_busy_status_ack_requires_actual_inline_reply_delivery(self):
        cases = (
            ("success", SendResult(success=True, message_id="reply"), True),
            ("failure", SendResult(success=False, error="timed out"), False),
            ("ambiguous", SendResult(success=None, error="timed out"), False),
            ("exception", RuntimeError("send exploded"), False),
        )
        for name, send_outcome, should_ack in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmp:
                instance = self.make_adapter(tmp)
                source = instance.build_source(chat_id="room", chat_type="dm", user_id="alice")
                key = build_session_key(
                    source,
                    group_sessions_per_user=instance.config.extra.get("group_sessions_per_user", True),
                    thread_sessions_per_user=instance.config.extra.get("thread_sessions_per_user", False),
                )
                instance._active_sessions[key] = asyncio.Event()
                instance.set_message_handler(
                    lambda _event: asyncio.sleep(0, result="status reply")
                )
                send_attempts = []

                async def send(**kwargs):
                    send_attempts.append(kwargs)
                    if isinstance(send_outcome, BaseException):
                        raise send_outcome
                    return send_outcome

                instance.send = send
                await instance._handle_talk_message(
                    {**_MESSAGE, "id": 880, "message": "/status"}, "room"
                )
                self.assertEqual(len(send_attempts), 1)
                self.assertEqual(instance._is_acknowledged("room", 880), should_ack)
                self.assertFalse(instance._inflight_message_ids.get("room"))

    async def test_busy_status_without_response_needs_no_delivery_and_acks(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            source = instance.build_source(chat_id="room", chat_type="dm", user_id="alice")
            key = build_session_key(source)
            instance._active_sessions[key] = asyncio.Event()
            instance.set_message_handler(lambda _event: asyncio.sleep(0, result=None))
            sends = []

            async def send(**kwargs):
                sends.append(kwargs)
                return SendResult(success=True)

            instance.send = send
            await instance._handle_talk_message(
                {**_MESSAGE, "id": 881, "message": "/status"}, "room"
            )
            self.assertEqual(sends, [])
            self.assertTrue(instance._is_acknowledged("room", 881))

    async def test_late_generation_completion_cannot_touch_retry_generation(self):
        for late_outcome in (
            ProcessingOutcome.SUCCESS,
            ProcessingOutcome.FAILURE,
            ProcessingOutcome.CANCELLED,
        ):
            with self.subTest(late_outcome=late_outcome), tempfile.TemporaryDirectory() as tmp:
                instance = self.make_adapter(tmp)
                instance.processing_timeout = 0.02
                instance.set_message_handler(lambda _event: asyncio.sleep(0))
                source = instance.build_source(chat_id="room", chat_type="dm", user_id="alice")
                key = build_session_key(source)
                instance._active_sessions[key] = asyncio.Event()
                captured = []

                async def defer(event, session_key):
                    captured.append(event)
                    instance._pending_messages[session_key] = event
                    return True

                instance.set_busy_session_handler(defer)
                message = {**_MESSAGE, "id": 882}
                await instance._handle_talk_message(message, "room")
                generation_a = captured[-1].metadata["nextcloud_talk_generation"]
                await asyncio.sleep(0.05)
                self.assertFalse(instance._inflight_message_ids.get("room"))

                await instance._handle_talk_message(message, "room")
                event_b = captured[-1]
                generation_b = event_b.metadata["nextcloud_talk_generation"]
                self.assertNotEqual(generation_a, generation_b)
                watchdog_b = instance._completion_watchdogs[("room", 882, generation_b)]

                await instance.on_processing_complete(captured[0], late_outcome)
                self.assertEqual(instance._inflight_message_ids["room"], {882})
                self.assertIs(
                    instance._completion_watchdogs[("room", 882, generation_b)],
                    watchdog_b,
                )
                self.assertFalse(watchdog_b.done())
                self.assertFalse(instance._is_acknowledged("room", 882))

                await instance.on_processing_complete(event_b, ProcessingOutcome.SUCCESS)
                await asyncio.sleep(0)
                self.assertTrue(instance._is_acknowledged("room", 882))
                self.assertFalse(instance._inflight_message_ids.get("room"))
                self.assertTrue(watchdog_b.done())
                self.assertEqual(instance._completion_watchdogs, {})
                self.assertEqual(instance._current_generations, {})
                self.assertEqual(instance._inflight_generations, {})
                self.assertEqual(instance._generation_outcomes, {})

    async def test_disconnect_cancels_watchdogs_clears_transient_state_and_allows_retry(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            instance.processing_timeout = 60
            instance.set_message_handler(lambda _event: asyncio.sleep(0))
            source = instance.build_source(chat_id="room", chat_type="dm", user_id="alice")
            key = build_session_key(source)
            instance._active_sessions[key] = asyncio.Event()
            captured = []

            async def defer(event, session_key):
                captured.append(event)
                instance._pending_messages[session_key] = event
                return True

            instance.set_busy_session_handler(defer)
            message = {**_MESSAGE, "id": 883}
            instance._running = True
            dispatch = asyncio.create_task(
                instance._handle_talk_message(message, "room", await_completion=True)
            )
            while not captured:
                await asyncio.sleep(0)
            watchdog = next(iter(instance._completion_watchdogs.values()))
            completion_event = captured[0].metadata["nextcloud_talk_completion_event"]

            await instance.disconnect()
            with self.assertRaisesRegex(RuntimeError, "processing failed"):
                await dispatch
            self.assertTrue(watchdog.done())
            self.assertEqual(instance._completion_watchdogs, {})
            self.assertEqual(instance._current_generations, {})
            self.assertEqual(instance._generation_outcomes, {})
            self.assertFalse(instance._inflight_message_ids.get("room"))
            self.assertFalse(instance._is_acknowledged("room", 883))
            self.assertTrue(completion_event.is_set())

            class FakeClient(adapter.NextcloudTalkClient):
                def __init__(self, *_args, **_kwargs):
                    pass

                async def list_conversations(self):
                    return [{"token": "room", "type": 1}]

                async def get_messages(self, *_args, **_kwargs):
                    return []

            instance._active_sessions.pop(key, None)
            instance._pending_messages.pop(key, None)
            instance.set_busy_session_handler(None)
            instance.set_message_handler(lambda _event: asyncio.sleep(0, result=None))
            with patch.object(adapter, "NextcloudTalkClient", FakeClient):
                self.assertTrue(await instance.connect())
            await instance._handle_talk_message(message, "room")
            await self.wait_for_background(instance)
            self.assertTrue(instance._is_acknowledged("room", 883))
            await instance.disconnect()

    async def test_initialization_busy_control_wait_is_bounded_and_completes(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            instance.set_message_handler(lambda _event: asyncio.sleep(0))
            source = instance.build_source(chat_id="room", chat_type="dm", user_id="alice")
            key = build_session_key(
                source,
                group_sessions_per_user=instance.config.extra.get("group_sessions_per_user", True),
                thread_sessions_per_user=instance.config.extra.get("thread_sessions_per_user", False),
            )
            instance._active_sessions[key] = asyncio.Event()
            instance.set_busy_session_handler(lambda _event, _key: asyncio.sleep(0, result=True))
            await asyncio.wait_for(
                instance._handle_talk_message(
                    {**_MESSAGE, "id": 86}, "room", await_completion=True
                ),
                timeout=1,
            )
            self.assertTrue(instance._is_acknowledged("room", 86))

    async def test_deferred_watchdog_never_acks_unknown_work_and_unblocks_retry(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            instance.processing_timeout = 0.02
            instance.set_message_handler(lambda _event: asyncio.sleep(0))
            source = instance.build_source(chat_id="room", chat_type="dm", user_id="alice")
            key = build_session_key(
                source,
                group_sessions_per_user=instance.config.extra.get("group_sessions_per_user", True),
                thread_sessions_per_user=instance.config.extra.get("thread_sessions_per_user", False),
            )
            instance._active_sessions[key] = asyncio.Event()

            async def queue(event, session_key):
                instance._pending_messages[session_key] = event
                return True

            instance.set_busy_session_handler(queue)
            await instance._handle_talk_message({**_MESSAGE, "id": 87}, "room")
            self.assertEqual(instance._inflight_message_ids["room"], {87})
            await asyncio.sleep(0.05)
            self.assertFalse(instance._inflight_message_ids.get("room"))
            self.assertFalse(instance._is_acknowledged("room", 87))

    async def test_room_churn_is_bounded_persisted_and_evicted_room_reinitializes(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            instance.max_rooms = 1
            instance.max_ack_rooms = 2
            instance.auto_discover_rooms = True
            instance._configured_room_tokens = []
            instance.room_tokens = []
            instance.discovery_interval = 0
            instance.initial_backlog_limit = 2
            current = ["r0"]
            fetched = []

            async def conversations():
                return [{"token": current[0], "type": 1}]

            async def backlog(token, limit):
                fetched.append((token, limit))
                return []

            instance._client = types.SimpleNamespace(list_conversations=conversations)
            instance._fetch_initial_backlog = backlog
            for index in range(6):
                current[0] = f"r{index}"
                await instance._refresh_discovered_rooms(force=True)
                self.assertLessEqual(len(instance._ack_rooms), 2)
                self.assertIn(current[0], instance._ack_rooms)
            self.assertNotIn("r0", instance._ack_rooms)

            restarted = self.make_adapter(tmp)
            restarted.max_rooms = 1
            restarted.max_ack_rooms = 2
            restarted.auto_discover_rooms = True
            restarted._configured_room_tokens = []
            restarted.room_tokens = []
            restarted.discovery_interval = 0
            restarted.initial_backlog_limit = 2
            restarted._load_cursors()
            self.assertLessEqual(len(restarted._ack_rooms), 2)
            current[0] = "r0"
            restarted._client = types.SimpleNamespace(list_conversations=conversations)
            restarted._fetch_initial_backlog = backlog
            await restarted._refresh_discovered_rooms(force=True)
            self.assertIn(("r0", 2), fetched)
            self.assertTrue(restarted._is_room_initialized("r0"))
            self.assertLessEqual(len(restarted._ack_rooms), 2)

    async def test_reply_parent_populates_bounded_sanitized_context(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            events = []

            async def capture(event):
                events.append(event)
                return None

            instance.set_message_handler(capture)
            parent_text = "quoted\ntext\x00\u0085Привет" + ("x" * 1000)
            message = {
                **_MESSAGE,
                "id": 70,
                "parent": {
                    "id": "69\nINJECT\x00\u0085",
                    "actorId": "bot\x00id\n\u0085",
                    "actorDisplayName": "Гермес\nINJECT\x00\u0085",
                    "message": parent_text,
                },
            }
            await instance._handle_talk_message(message, "room")
            await self.wait_for_background(instance)
            event = events[0]
            self.assertEqual(event.reply_to_message_id, "69?INJECT??")
            self.assertEqual(event.reply_to_author_id, "bot?id??")
            self.assertEqual(event.reply_to_author_name, "Гермес?INJECT??")
            self.assertFalse(event.reply_to_is_own_message)
            self.assertLessEqual(len(event.reply_to_text), 500)
            self.assertNotIn("\x00", event.reply_to_text)
            for value in (
                event.reply_to_message_id,
                event.reply_to_author_id,
                event.reply_to_author_name,
                event.reply_to_text,
            ):
                self.assertNotRegex(value, r"[\x00-\x1f\x7f-\x9f]")


class RealHermesAuthorizationTests(unittest.TestCase):
    def test_core_and_adapter_agree_on_canonical_dm_and_group_scopes(self):
        config = PlatformConfig(
            enabled=True,
            extra={
                "url": "https://cloud.example",
                "username": "bot",
                "password": "secret",
                "room_tokens": "dm,group",
                "auto_discover_rooms": False,
                "allow_from": ["alice"],
                "group_allow_from": ["bob"],
            },
        )
        with patch.dict(
            os.environ,
            {"NEXTCLOUD_TALK_ALLOWED_USERS": "", "NEXTCLOUD_TALK_ALLOW_ALL_USERS": ""},
            clear=False,
        ):
            instance = adapter.NextcloudTalkAdapter(config)
        runner = object.__new__(GatewayRunner)
        runner.adapters = {instance.platform: instance}
        runner._profile_adapters = {}
        runner.config = types.SimpleNamespace(platforms={instance.platform: config})
        runner.pairing_store = types.SimpleNamespace(is_approved=lambda *_args: False)
        runner.pairing_stores = {}

        dm_alice = instance.build_source(chat_id="dm", chat_type="dm", user_id="alice")
        dm_bob = instance.build_source(chat_id="dm", chat_type="dm", user_id="bob")
        group_alice = instance.build_source(chat_id="group", chat_type="group", user_id="alice")
        group_bob = instance.build_source(chat_id="group", chat_type="group", user_id="bob")
        auth_env = {
            "NEXTCLOUD_TALK_ALLOWED_USERS": "",
            "NEXTCLOUD_TALK_ALLOW_ALL_USERS": "",
            "GATEWAY_ALLOWED_USERS": "",
            "GATEWAY_ALLOW_ALL_USERS": "",
        }
        with patch.dict(os.environ, auth_env, clear=False):
            self.assertTrue(instance._is_allowed("alice", "Alice", chat_type="dm"))
            self.assertFalse(instance._is_allowed("bob", "Bob", chat_type="dm"))
            self.assertFalse(instance._is_allowed("alice", "Alice", chat_type="group"))
            self.assertTrue(instance._is_allowed("bob", "Bob", chat_type="group"))
            self.assertEqual(
                [runner._is_user_authorized(source) for source in (dm_alice, dm_bob, group_alice, group_bob)],
                [True, False, False, True],
            )

    def test_core_and_adapter_exact_wildcard_env_and_missing_actor_matrix(self):
        runner = object.__new__(GatewayRunner)
        runner._profile_adapters = {}
        runner.pairing_store = types.SimpleNamespace(is_approved=lambda *_args: False)
        runner.pairing_stores = {}

        cases = [
            ({"allow_from": ["Alice"], "group_allow_from": ["Alice"]}, {}, "Alice", "dm", True),
            ({"allow_from": ["Alice"], "group_allow_from": ["Alice"]}, {}, "alice", "dm", False),
            ({"allow_from": ["Alice"], "group_allow_from": ["Alice"]}, {}, " Alice ", "dm", False),
            ({"allow_from": ["*"], "group_allow_from": []}, {}, "anyone", "dm", True),
            ({"allow_from": [], "group_allow_from": ["*"]}, {}, "anyone", "group", True),
            ({"allow_from": [], "group_allow_from": []}, {"NEXTCLOUD_TALK_ALLOWED_USERS": "Alice"}, "Alice", "dm", True),
            ({"allow_from": [], "group_allow_from": []}, {"NEXTCLOUD_TALK_ALLOWED_USERS": "Alice"}, "Alice", "group", True),
            ({"allow_from": [], "group_allow_from": []}, {"NEXTCLOUD_TALK_ALLOWED_USERS": "Alice"}, "alice", "group", False),
        ]
        base_env = {
            "NEXTCLOUD_TALK_ALLOWED_USERS": "",
            "NEXTCLOUD_TALK_ALLOW_ALL_USERS": "",
            "GATEWAY_ALLOWED_USERS": "",
            "GATEWAY_ALLOW_ALL_USERS": "",
        }
        for extra_auth, env, actor_id, chat_type, expected in cases:
            config = PlatformConfig(enabled=True, extra={
                "url": "https://cloud.example", "username": "bot", "password": "secret",
                "room_tokens": "room", "auto_discover_rooms": False, **extra_auth,
            })
            with self.subTest(extra=extra_auth, env=env, actor=actor_id, chat_type=chat_type):
                with patch.dict(os.environ, {**base_env, **env}, clear=False):
                    instance = adapter.NextcloudTalkAdapter(config)
                    runner.adapters = {instance.platform: instance}
                    runner.config = types.SimpleNamespace(platforms={instance.platform: config})
                    source = instance.build_source(chat_id="room", chat_type=chat_type, user_id=actor_id)
                    self.assertEqual(instance._is_allowed(actor_id, actor_id, chat_type=chat_type), expected)
                    self.assertEqual(runner._is_user_authorized(source), expected)

        config = PlatformConfig(enabled=True, extra={
            "url": "https://cloud.example", "username": "bot", "password": "secret",
            "room_tokens": "room", "auto_discover_rooms": False,
            "allow_from": ["*"], "group_allow_from": ["*"],
        })
        with patch.dict(os.environ, base_env, clear=False):
            instance = adapter.NextcloudTalkAdapter(config)
            runner.adapters = {instance.platform: instance}
            runner.config = types.SimpleNamespace(platforms={instance.platform: config})
            source = instance.build_source(chat_id="room", chat_type="dm", user_id=None)
            self.assertFalse(instance._is_allowed("", "Alice", chat_type="dm"))
            self.assertFalse(runner._is_user_authorized(source))


if __name__ == "__main__":
    unittest.main(verbosity=2)
