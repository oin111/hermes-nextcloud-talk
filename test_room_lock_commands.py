"""Slash commands for a busy session must pass the Talk room lock.

While a turn runs, the adapter keeps later room messages on Talk (they are
dispatched after the turn).  Hermes core, however, expects recognized slash
commands such as ``/approve``, ``/deny`` and ``/stop`` to reach a busy session
immediately: the agent thread may be blocked waiting for exactly that command.
These probes drive the real Hermes ``BasePlatformAdapter`` lifecycle.
"""

import asyncio
import tempfile
import unittest
from pathlib import Path

from gateway.config import PlatformConfig
from gateway.platform_registry import PlatformEntry, platform_registry
from gateway.platforms.base import SendResult
from gateway.session import build_session_key

import adapter


if not platform_registry.is_registered("nextcloud_talk"):
    platform_registry.register(
        PlatformEntry(
            name="nextcloud_talk",
            label="Nextcloud Talk",
            adapter_factory=lambda cfg: adapter.NextcloudTalkAdapter(cfg),
            check_fn=lambda: True,
            source="builtin",
            allowed_users_env="NEXTCLOUD_TALK_ALLOWED_USERS",
            allow_all_env="NEXTCLOUD_TALK_ALLOW_ALL_USERS",
        )
    )


def _msg(message_id, text, actor="alice", **extra):
    return {
        "id": message_id,
        "actorType": "users",
        "actorId": actor,
        "actorDisplayName": actor.title(),
        "message": text,
        **extra,
    }


class _Client:
    """Fake Talk client: serves a fixed room history and records requests."""

    def __init__(self, messages):
        self.messages = list(messages)
        self.timeouts = []
        self.downloads = []

    async def get_messages(self, _room, *, last_known_id=None, timeout=0, limit=200, **_kw):
        self.timeouts.append(timeout)
        floor = last_known_id or 0
        return [m for m in self.messages if m["id"] > floor][:limit]

    @staticmethod
    def _dav_url(path):
        return f"https://nextcloud.invalid{path}"

    def _download_file(self, *_args, **_kwargs):
        self.downloads.append(True)
        raise AssertionError("held message performed attachment I/O")


class RoomLockCommandTests(unittest.IsolatedAsyncioTestCase):
    def make_adapter(self, cache_dir, *, room_type=1, per_user=True):
        config = PlatformConfig(
            enabled=True,
            typing_indicator=False,
            extra={
                "url": "https://cloud.example",
                "username": "bot",
                "password": "secret",
                "room_tokens": "room",
                "auto_discover_rooms": False,
                "group_sessions_per_user": per_user,
                "reactions": False,
                "read_marker": False,
            },
        )
        instance = adapter.NextcloudTalkAdapter(config)
        instance._cursor_path = Path(cache_dir) / "cursors.json"
        instance._room_types = {"room": room_type}
        instance.allow_all = True
        self.sent = []

        async def send(chat_id=None, content=None, **_kwargs):
            self.sent.append(content)
            return SendResult(success=True, message_id=str(len(self.sent)))

        instance.send = send
        return instance

    def session_key(self, instance, actor="alice"):
        chat_type = "dm" if instance._room_types.get("room") == 1 else "group"
        source = instance.build_source(
            chat_id="room", chat_type=chat_type, user_id=actor, user_name=actor.title()
        )
        return build_session_key(
            source,
            group_sessions_per_user=instance.config.extra.get("group_sessions_per_user", True),
            thread_sessions_per_user=False,
        )

    async def start_turn(self, instance, client, release):
        """Dispatch message 100 and leave its turn running."""
        seen = []

        async def handler(event):
            seen.append(event.text)
            if event.text.startswith("/"):
                return f"handled {event.text}"
            await release.wait()
            return "turn finished"

        instance._client = client
        self.raw_handler = handler
        instance.set_message_handler(handler)
        await instance._handle_talk_message(_msg(100, "long task"), "room")
        for _ in range(100):
            if seen:
                break
            await asyncio.sleep(0.01)
        self.assertEqual(seen, ["long task"])
        self.assertEqual(instance._inflight_message_ids.get("room"), {100})
        return seen

    async def finish(self, instance, release):
        release.set()
        for _ in range(200):
            if not instance._background_tasks and not instance._inflight_message_ids.get("room"):
                break
            await asyncio.sleep(0.01)

    async def test_approve_reaches_busy_session_during_turn(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            client = _Client([_msg(100, "long task"), _msg(101, "/approve")])
            release = asyncio.Event()
            seen = await self.start_turn(instance, client, release)
            try:
                await asyncio.wait_for(instance._poll_room("room"), timeout=5)
                self.assertEqual(seen, ["long task", "/approve"])
                self.assertIn("handled /approve", self.sent)
                self.assertTrue(instance._is_acknowledged("room", 101))
                # The running turn itself is untouched.
                self.assertEqual(instance._inflight_message_ids.get("room"), {100})
                self.assertFalse(instance._is_acknowledged("room", 100))
            finally:
                await self.finish(instance, release)
            self.assertTrue(instance._is_acknowledged("room", 100))

    async def test_deny_reaches_busy_session_during_turn(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            client = _Client([_msg(100, "long task"), _msg(101, "/deny")])
            release = asyncio.Event()
            seen = await self.start_turn(instance, client, release)
            try:
                await asyncio.wait_for(instance._poll_room("room"), timeout=5)
                self.assertEqual(seen, ["long task", "/deny"])
                self.assertTrue(instance._is_acknowledged("room", 101))
            finally:
                await self.finish(instance, release)

    async def test_plain_text_stays_held_during_turn(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            client = _Client([_msg(100, "long task"), _msg(101, "follow-up")])
            release = asyncio.Event()
            seen = await self.start_turn(instance, client, release)
            try:
                await asyncio.wait_for(instance._poll_room("room"), timeout=5)
                self.assertEqual(seen, ["long task"])
                self.assertFalse(instance._is_acknowledged("room", 101))
            finally:
                await self.finish(instance, release)
            # After the turn the held message is dispatched normally.
            await asyncio.wait_for(instance._poll_room("room"), timeout=5)
            for _ in range(200):
                if instance._is_acknowledged("room", 101):
                    break
                await asyncio.sleep(0.01)
            self.assertEqual(seen, ["long task", "follow-up"])

    async def test_unknown_slash_text_stays_held(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            client = _Client([_msg(100, "long task"), _msg(101, "/usr/bin/env is a path")])
            release = asyncio.Event()
            seen = await self.start_turn(instance, client, release)
            try:
                await asyncio.wait_for(instance._poll_room("room"), timeout=5)
                self.assertEqual(seen, ["long task"])
                self.assertFalse(instance._is_acknowledged("room", 101))
            finally:
                await self.finish(instance, release)

    async def test_command_with_attachment_stays_held_without_io(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            attachment = {
                "file": {
                    "type": "file",
                    "name": "payload.txt",
                    "path": "/payload.txt",
                    "link": "",
                    "mimetype": "text/plain",
                }
            }
            client = _Client([
                _msg(100, "long task"),
                _msg(101, "/approve", messageParameters=attachment),
            ])
            release = asyncio.Event()
            seen = await self.start_turn(instance, client, release)
            try:
                await asyncio.wait_for(instance._poll_room("room"), timeout=5)
                self.assertEqual(seen, ["long task"])
                self.assertEqual(client.downloads, [])
                self.assertFalse(instance._is_acknowledged("room", 101))
            finally:
                await self.finish(instance, release)

    async def test_other_participant_command_stays_held_in_shared_group_session(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp, room_type=2, per_user=False)
            client = _Client([_msg(100, "long task"), _msg(101, "/stop", actor="bob")])
            release = asyncio.Event()
            seen = await self.start_turn(instance, client, release)
            try:
                await asyncio.wait_for(instance._poll_room("room"), timeout=5)
                self.assertEqual(seen, ["long task"])
                self.assertFalse(instance._is_acknowledged("room", 101))
                self.assertIn(self.session_key(instance), instance._active_sessions)
            finally:
                await self.finish(instance, release)

    async def test_command_for_idle_session_in_group_stays_held(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp, room_type=2, per_user=True)
            client = _Client([_msg(100, "long task"), _msg(101, "/approve", actor="bob")])
            release = asyncio.Event()
            seen = await self.start_turn(instance, client, release)
            try:
                await asyncio.wait_for(instance._poll_room("room"), timeout=5)
                self.assertEqual(seen, ["long task"])
                self.assertFalse(instance._is_acknowledged("room", 101))
            finally:
                await self.finish(instance, release)

    async def test_owner_command_passes_in_group_room(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp, room_type=2, per_user=False)
            client = _Client([_msg(100, "long task"), _msg(101, "/approve")])
            release = asyncio.Event()
            seen = await self.start_turn(instance, client, release)
            try:
                await asyncio.wait_for(instance._poll_room("room"), timeout=5)
                self.assertEqual(seen, ["long task", "/approve"])
                self.assertTrue(instance._is_acknowledged("room", 101))
            finally:
                await self.finish(instance, release)

    async def test_stop_cancels_turn_and_acknowledges_it(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            client = _Client([_msg(100, "long task"), _msg(101, "/stop")])
            release = asyncio.Event()
            seen = await self.start_turn(instance, client, release)
            try:
                await asyncio.wait_for(instance._poll_room("room"), timeout=10)
                for _ in range(300):
                    if not instance._inflight_message_ids.get("room"):
                        break
                    await asyncio.sleep(0.01)
                self.assertEqual(seen, ["long task", "/stop"])
                self.assertTrue(instance._is_acknowledged("room", 101))
                # The stopped turn must not be replayed on the next poll.
                self.assertTrue(instance._is_acknowledged("room", 100))
                await asyncio.wait_for(instance._poll_room("room"), timeout=5)
                self.assertEqual(seen, ["long task", "/stop"])
            finally:
                await self.finish(instance, release)

    async def test_poll_during_turn_uses_short_long_poll(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            instance.poll_timeout = 30
            client = _Client([_msg(100, "long task")])
            release = asyncio.Event()
            await self.start_turn(instance, client, release)
            try:
                await asyncio.wait_for(instance._poll_room("room"), timeout=5)
                long_polls = [t for t in client.timeouts if t]
                self.assertTrue(long_polls, "no long poll during the turn")
                self.assertTrue(all(t <= adapter._INFLIGHT_POLL_TIMEOUT for t in long_polls))
                self.assertLessEqual(adapter._INFLIGHT_POLL_TIMEOUT, 5)
            finally:
                await self.finish(instance, release)

    async def test_failed_command_reply_is_not_replayed(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            client = _Client([_msg(100, "long task"), _msg(101, "/approve")])
            release = asyncio.Event()
            seen = await self.start_turn(instance, client, release)

            async def failing_send(chat_id=None, content=None, **_kwargs):
                return SendResult(success=False, error="refused", retryable=False)

            instance.send = failing_send
            try:
                for _ in range(3):
                    await asyncio.wait_for(instance._poll_room("room"), timeout=5)
                self.assertEqual(seen, ["long task", "/approve"])
                self.assertTrue(instance._is_acknowledged("room", 101))
            finally:
                await self.finish(instance, release)

    async def test_unrelated_background_task_does_not_stall_inline_command(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            instance.processing_timeout = 30
            client = _Client([_msg(100, "long task"), _msg(101, "/approve")])
            release = asyncio.Event()
            seen = await self.start_turn(instance, client, release)
            other_release = asyncio.Event()
            original = self.raw_handler

            async def handler(event):
                if event.text == "/approve":
                    # Another room's turn starts while core handles the command.
                    other = asyncio.create_task(other_release.wait())
                    instance._background_tasks.add(other)
                    other.add_done_callback(instance._background_tasks.discard)
                return await original(event)

            instance.set_message_handler(handler)
            try:
                loop = asyncio.get_running_loop()
                started = loop.time()
                await asyncio.wait_for(instance._poll_room("room"), timeout=10)
                self.assertLess(loop.time() - started, 6.5)
                self.assertTrue(instance._is_acknowledged("room", 101))
                await asyncio.wait_for(instance._poll_room("room"), timeout=10)
                self.assertEqual(seen.count("/approve"), 1)
            finally:
                other_release.set()
                await self.finish(instance, release)

    async def test_flood_from_others_does_not_block_owner_stop(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp, room_type=2, per_user=True)
            instance.allow_all = False
            instance.allowed_users = {"alice"}
            instance.group_allowed_users = {"alice"}
            instance.ack_retention_count = 20
            flood = [_msg(200 + i, f"spam {i}", actor="eve") for i in range(60)]
            client = _Client([_msg(100, "long task"), *flood, _msg(400, "/stop")])
            release = asyncio.Event()
            seen = await self.start_turn(instance, client, release)
            try:
                for _ in range(3):
                    await asyncio.wait_for(instance._poll_room("room"), timeout=10)
                    if "/stop" in seen:
                        break
                self.assertIn("/stop", seen)
                self.assertTrue(instance._is_acknowledged("room", 400))
                self.assertFalse(any(instance._is_acknowledged("room", 200 + i) for i in range(60)))
            finally:
                await self.finish(instance, release)

    async def test_turn_poll_reads_only_after_live_anchor(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            client = _Client([_msg(100, "long task"), _msg(101, "held one")])
            release = asyncio.Event()
            await self.start_turn(instance, client, release)
            try:
                client.anchors = []
                original = client.get_messages

                async def recording(room, *, last_known_id=None, **kw):
                    client.anchors.append(last_known_id)
                    return await original(room, last_known_id=last_known_id, **kw)

                client.get_messages = recording
                for _ in range(3):
                    await asyncio.wait_for(instance._poll_room("room"), timeout=5)
                # First cycle starts after the running message; later cycles
                # after the newest observed ID, never from the floor again.
                self.assertEqual(client.anchors, [100, 101, 101])
                self.assertFalse(instance._is_acknowledged("room", 101))
            finally:
                await self.finish(instance, release)

    async def test_failed_stop_does_not_mark_turn_for_ack(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            client = _Client([_msg(100, "long task"), _msg(101, "/stop")])
            release = asyncio.Event()
            seen = await self.start_turn(instance, client, release)
            original = self.raw_handler

            async def handler(event):
                if event.text == "/stop":
                    seen.append("/stop")
                    raise RuntimeError("stop handler failed")
                return await original(event)

            instance.set_message_handler(handler)
            try:
                await asyncio.wait_for(instance._poll_room("room"), timeout=10)
                self.assertIn("/stop", seen)
                self.assertEqual(instance._inflight_message_ids.get("room"), {100})
                # A later shutdown-style cancellation must leave the turn retryable.
                await instance.cancel_session_processing(self.session_key(instance))
                for _ in range(100):
                    if not instance._inflight_message_ids.get("room"):
                        break
                    await asyncio.sleep(0.01)
                self.assertFalse(instance._is_acknowledged("room", 100))
            finally:
                await self.finish(instance, release)

    async def test_oversized_inflight_page_is_retried_smaller(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            client = _Client([_msg(100, "long task"), _msg(101, "/stop")])
            original = client.get_messages

            async def capped(room, *, limit=200, **kw):
                if limit > 1:
                    raise adapter.NextcloudTalkAPIError("too large", category="overflow")
                return await original(room, limit=limit, **kw)

            release = asyncio.Event()
            seen = await self.start_turn(instance, client, release)
            client.get_messages = capped
            try:
                await asyncio.wait_for(instance._poll_room("room"), timeout=10)
                self.assertIn("/stop", seen)
                self.assertTrue(instance._is_acknowledged("room", 101))
            finally:
                await self.finish(instance, release)

    async def test_inflight_page_size_recovers_after_large_posts(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            client = _Client([_msg(100, "long task"), _msg(101, "/approve")])
            original = client.get_messages
            state = {"overflow": True}

            async def once_too_large(room, *, limit=200, **kw):
                if state["overflow"] and limit > 50:
                    raise adapter.NextcloudTalkAPIError("too large", category="overflow")
                return await original(room, limit=limit, **kw)

            release = asyncio.Event()
            seen = await self.start_turn(instance, client, release)
            client.get_messages = once_too_large
            try:
                await asyncio.wait_for(instance._poll_room("room"), timeout=10)
                self.assertIn("/approve", seen)
                # The short page proves the large messages are behind us.
                self.assertNotIn("room", instance._poll_page_limits)
            finally:
                await self.finish(instance, release)

    async def test_burst_before_command_is_read_in_one_cycle(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp, room_type=2)
            instance.allow_all = False
            instance.allowed_users = {"alice"}
            instance.group_allowed_users = {"alice"}
            burst = [_msg(200 + i, f"spam {i}", actor="eve") for i in range(450)]
            client = _Client([_msg(100, "long task"), *burst, _msg(900, "/stop")])
            release = asyncio.Event()
            seen = await self.start_turn(instance, client, release)
            try:
                await asyncio.wait_for(instance._poll_room("room"), timeout=10)
                self.assertIn("/stop", seen)
            finally:
                await self.finish(instance, release)

    def register_clarify(self, instance):
        from tools import clarify_gateway

        key = self.session_key(instance)
        entry = clarify_gateway.register("clarify-x", key, "Pick one", ["A", "B"])
        self.addCleanup(clarify_gateway.clear_session, key)
        return entry

    async def test_stop_during_pending_clarify_is_not_replayed(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            client = _Client([_msg(100, "long task"), _msg(101, "/stop")])
            release = asyncio.Event()
            seen = await self.start_turn(instance, client, release)
            self.register_clarify(instance)
            try:
                await asyncio.wait_for(instance._poll_room("room"), timeout=10)
                for _ in range(300):
                    if not instance._inflight_message_ids.get("room"):
                        break
                    await asyncio.sleep(0.01)
                self.assertIn("/stop", seen)
                self.assertTrue(instance._is_acknowledged("room", 100))
                self.assertTrue(instance._is_acknowledged("room", 101))
                await asyncio.wait_for(instance._poll_room("room"), timeout=5)
                self.assertEqual(seen.count("long task"), 1)
            finally:
                await self.finish(instance, release)

    async def test_failed_command_during_clarify_is_not_replayed(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            client = _Client([_msg(100, "long task"), _msg(101, "/status")])
            release = asyncio.Event()
            seen = await self.start_turn(instance, client, release)
            self.register_clarify(instance)

            async def failing_send(chat_id=None, content=None, **_kwargs):
                return SendResult(success=False, error="refused", retryable=False)

            instance.send = failing_send
            try:
                for _ in range(3):
                    await asyncio.wait_for(instance._poll_room("room"), timeout=5)
                self.assertEqual(seen.count("/status"), 1)
                self.assertTrue(instance._is_acknowledged("room", 101))
            finally:
                await self.finish(instance, release)

    async def test_command_whose_ack_fails_is_not_run_again(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            client = _Client([_msg(100, "long task"), _msg(101, "/approve")])
            release = asyncio.Event()
            seen = await self.start_turn(instance, client, release)
            real_commit = instance._commit_cursor
            failures = {"left": 1}

            def flaky_commit(room, message_id, *args, **kwargs):
                if message_id == 101 and failures["left"]:
                    failures["left"] -= 1
                    raise OSError("disk full")
                return real_commit(room, message_id, *args, **kwargs)

            instance._commit_cursor = flaky_commit
            try:
                await asyncio.wait_for(instance._poll_room("room"), timeout=5)
                self.assertFalse(instance._is_acknowledged("room", 101))
            finally:
                await self.finish(instance, release)
            # After the turn the floor sweep sees 101 again: only the ACK is retried.
            await asyncio.wait_for(instance._poll_room("room"), timeout=5)
            self.assertEqual(seen.count("/approve"), 1)
            self.assertTrue(instance._is_acknowledged("room", 101))


if __name__ == "__main__":
    unittest.main()
