"""Reaction approvals through real Base and the prefix-floor/busy-room pollers."""
import asyncio
import tempfile
import time
import unittest
from unittest.mock import patch

import adapter
import test_room_lock_commands as room_tests
from test_room_lock_commands import _Client, _msg


class ApprovalPollIntegrationTests(unittest.IsolatedAsyncioTestCase):
    make_adapter = room_tests.RoomLockCommandTests.make_adapter
    session_key = room_tests.RoomLockCommandTests.session_key
    start_turn = room_tests.RoomLockCommandTests.start_turn
    finish = room_tests.RoomLockCommandTests.finish

    def register(self, instance, session_key="session"):
        return instance._register_approval_prompt(
            session_key=session_key, chat_id="room", message_id="500",
            requester_user_id=None, choices=("once", "deny"), seeded_emojis=(),
        )

    def row(self, numeric_id=101, actor_id="alice", **extra):
        return _msg(numeric_id, "✅", messageType="system", systemMessage="reaction",
                    parent=500, actorId=actor_id, **extra)

    async def test_reaction_is_consumed_while_turn_inflight_then_floor_advances(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            client = _Client([_msg(100, "long task"), self.row()])
            release = asyncio.Event()
            seen = await self.start_turn(instance, client, release)
            self.register(instance, self.session_key(instance))
            try:
                with patch("tools.approval.resolve_gateway_approval", return_value=1) as resolve:
                    await instance._poll_room("room")
                    self.assertEqual(resolve.call_count, 1)
                    self.assertTrue(instance._is_acknowledged("room", 101))
                    await instance._poll_room("room")
                    self.assertEqual(resolve.call_count, 1)
                self.assertEqual(seen, ["long task"])
                self.assertFalse(instance._is_acknowledged("room", 100))
            finally:
                await self.finish(instance, release)
            await instance._poll_room("room")
            with patch.object(adapter, "_ledger_clock", return_value=10**12):
                await instance._poll_room("room")
            self.assertGreaterEqual(instance._ack_rooms["room"]["floor"], 101)
            self.assertEqual(seen, ["long task"])
            # Persistence/restart retains the consumed reaction.
            reloaded = self.make_adapter(tmp)
            reloaded._load_cursors()
            self.assertTrue(reloaded._is_acknowledged("room", 101))

    async def test_idle_reaction_is_consumed_once_and_settles(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            instance._client = _Client([self.row()])
            self.register(instance)
            seen = []
            async def handler(event):
                seen.append(event.text)
            instance.set_message_handler(handler)
            with patch("tools.approval.resolve_gateway_approval", return_value=1) as resolve:
                await instance._poll_room("room")
                await instance._poll_room("room")
                self.assertEqual(resolve.call_count, 1)
            with patch.object(adapter, "_ledger_clock", return_value=10**12):
                await instance._poll_room("room")
            self.assertEqual(seen, [])
            self.assertGreaterEqual(instance._ack_rooms["room"]["floor"], 101)

    async def test_prune_runs_even_when_room_is_inflight(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            client = _Client([_msg(100, "long task")])
            release = asyncio.Event()
            await self.start_turn(instance, client, release)
            prompt = self.register(instance, self.session_key(instance))
            prompt.expires_at = time.monotonic() - 1
            try:
                await instance._poll_room("room")
                self.assertNotIn("500", instance._approval_prompts_by_event)
            finally:
                await self.finish(instance, release)

    async def test_reaction_removal_never_approves(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            instance._client = _Client([])
            self.register(instance)
            row = self.row()
            row["systemMessage"] = "reaction_revoked"
            with patch("tools.approval.resolve_gateway_approval", return_value=1) as resolve:
                await instance._handle_talk_message(row, "room")
                resolve.assert_not_called()
            self.assertTrue(instance._is_acknowledged("room", 101))

    async def test_non_user_actor_never_approves(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            instance._client = _Client([])
            self.register(instance)
            with patch("tools.approval.resolve_gateway_approval", return_value=1) as resolve:
                await instance._handle_talk_message(self.row(actorType="bots"), "room")
                resolve.assert_not_called()

    async def test_malformed_actor_on_polled_reaction_never_approves(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            instance._client = _Client([self.row(actor_id=123)])
            self.register(instance)
            with patch("tools.approval.resolve_gateway_approval", return_value=1) as resolve:
                await instance._poll_room("room")
                resolve.assert_not_called()

    async def test_foreign_reaction_stays_unacked_behind_busy_turn(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            instance.allow_all = False
            instance.allowed_users = {"alice"}
            client = _Client([_msg(100, "long task"), self.row(actor_id="mallory")])
            release = asyncio.Event()
            seen = await self.start_turn(instance, client, release)
            self.register(instance, self.session_key(instance))
            try:
                with patch("tools.approval.resolve_gateway_approval", return_value=1) as resolve:
                    await instance._poll_room("room")
                    resolve.assert_not_called()
                self.assertFalse(instance._is_acknowledged("room", 101))
                self.assertEqual(seen, ["long task"])
            finally:
                await self.finish(instance, release)

    async def test_cross_room_reaction_cannot_retire_expired_card(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            instance._client = _Client([])
            prompt = self.register(instance)
            prompt.expires_at = time.monotonic() - 1
            with patch("tools.approval.resolve_gateway_approval") as resolve:
                await instance._handle_approval_system_message(self.row(), "other-room")
                resolve.assert_not_called()
            self.assertIs(instance._approval_prompts_by_event.get("500"), prompt)

    async def test_ack_failure_does_not_reapply_zero_resolution_tap(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            instance._client = _Client([])
            self.register(instance)
            with patch("tools.approval.resolve_gateway_approval", return_value=0) as resolve:
                with patch.object(instance, "_commit_cursor", side_effect=OSError("disk")):
                    with self.assertRaises(OSError):
                        await instance._handle_talk_message(self.row(), "room")
                await instance._handle_talk_message(self.row(), "room")
                self.assertEqual(resolve.call_count, 1)
            self.assertTrue(instance._is_acknowledged("room", 101))
