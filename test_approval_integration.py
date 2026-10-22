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

    async def send_race_card(self, instance):
        # Older CI pins have no shared builder; still exercise the adapter hook
        # and real send transport there, rather than skipping lifecycle coverage.
        if hasattr(adapter.BasePlatformAdapter, "send_exec_approval"):
            return await instance.send_exec_approval(
                "room", "probe", "session", allow_permanent=False, allow_session=False)
        from types import SimpleNamespace
        prompt = SimpleNamespace(
            chat_id="room", session_key="session", text="Approval probe", metadata={},
            actions=[("Allow Once", "once", "primary"), ("Deny", "deny", "danger")])
        return await instance._send_exec_approval_prompt(prompt)

    async def approval_disconnect_race(self, *, stage, reconnect=False, block_teardown=False):
        import types

        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            instance.send = types.MethodType(type(instance).send, instance)
            entered, release = asyncio.Event(), asyncio.Event()
            seeds, removed = [], []

            async def post(*args, **kwargs):
                if stage == "post":
                    entered.set()
                    await release.wait()
                return {"id": 500}

            async def seed(room, mid, emoji):
                seeds.append((mid, emoji))
                if stage == "seed":
                    entered.set()
                    await release.wait()

            async def remove(room, mid, emoji):
                removed.append((mid, emoji))

            old_client = types.SimpleNamespace(send_message=post, add_reaction=seed, remove_reaction=remove)
            instance._client = old_client
            pending = asyncio.create_task(self.send_race_card(instance))
            await asyncio.wait_for(entered.wait(), 2)
            teardown_entered, teardown_release = asyncio.Event(), asyncio.Event()

            async def poller():
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    teardown_entered.set()
                    await teardown_release.wait()

            disconnect = None
            if block_teardown:
                instance._poll_task = asyncio.create_task(poller())
                await asyncio.sleep(0)
                disconnect = asyncio.create_task(instance.disconnect())
                await asyncio.wait_for(teardown_entered.wait(), 2)
            else:
                await instance.disconnect()
            new_seeds = []
            new_prompt = None
            try:
                self.assertNotIn("500", instance._approval_prompts_by_event)
                if reconnect:
                    async def new_seed(room, mid, emoji):
                        new_seeds.append((mid, emoji))
                    new_client = types.SimpleNamespace(
                        send_message=lambda *a, **k: asyncio.sleep(0, result={"id": 600}),
                        add_reaction=new_seed, remove_reaction=remove)
                    async def refresh(**kwargs):
                        pass
                    async def idle_poll():
                        await asyncio.Event().wait()
                    with patch.object(adapter, "NextcloudTalkClient", return_value=new_client) as factory, patch.object(
                        instance, "_refresh_discovered_rooms", side_effect=refresh
                    ), patch.object(instance, "_poll_loop", side_effect=idle_poll):
                        factory._origin = lambda url: ("https", "cloud.example", 443)
                        self.assertTrue(await instance.connect())
                    result = await self.send_race_card(instance)
                    self.assertTrue(result.success)
                    new_prompt = instance._approval_prompts_by_event["600"]
                release.set()
                result = await asyncio.wait_for(pending, 3)
                self.assertFalse(result.success, "a stale card send must fail closed")
                self.assertNotIn("500", instance._approval_prompts_by_event)
                self.assertEqual(len(seeds), int(stage == "seed"), "stale send continued seeding")
                if stage == "seed":
                    self.assertEqual(removed, seeds, "the possibly landed seed needs conservative cleanup")
                if reconnect:
                    self.assertIs(instance._approval_prompts_by_event.get("600"), new_prompt)
                    self.assertEqual(instance._approval_prompt_by_session, {"session": "600"})
                    self.assertEqual(len(new_seeds), 2, "old send borrowed the new client")
                with patch("tools.approval.resolve_gateway_approval", return_value=1) as resolve:
                    await instance._handle_talk_message(self.row(), "room")
                    resolve.assert_not_called()
                    if reconnect:
                        fresh_row = self.row(102)
                        fresh_row["parent"] = 600
                        await instance._handle_talk_message(fresh_row, "room")
                        self.assertEqual(resolve.call_count, 1)
            finally:
                release.set()
                teardown_release.set()
                if disconnect is not None:
                    await asyncio.wait_for(disconnect, 2)
                await asyncio.gather(pending, return_exceptions=True)
                await instance.disconnect()

    async def test_disconnect_during_card_post(self):
        await self.approval_disconnect_race(stage="post")

    async def test_disconnect_fences_card_post_before_first_teardown_await(self):
        await self.approval_disconnect_race(stage="post", block_teardown=True)

    async def test_disconnect_during_seed_cleans_attempted_reaction(self):
        await self.approval_disconnect_race(stage="seed")

    async def test_reconnect_does_not_revive_old_post_or_seed(self):
        for stage in ("post", "seed"):
            with self.subTest(stage=stage):
                await self.approval_disconnect_race(stage=stage, reconnect=True)

    async def test_exact_reaction_actor_matches_real_core_idle_and_busy(self):
        import os
        import types
        from gateway.run import GatewayRunner

        for busy in (False, True):
            for actor in (" alice ", "Alice", "alice", "", "   ", 123):
                with self.subTest(busy=busy, actor=actor), tempfile.TemporaryDirectory() as tmp, patch.dict(
                    os.environ, {name: "" for name in (
                        "NEXTCLOUD_TALK_ALLOWED_USERS", "NEXTCLOUD_TALK_ALLOW_ALL_USERS",
                        "GATEWAY_ALLOWED_USERS", "GATEWAY_ALLOW_ALL_USERS")}
                ):
                    instance = self.make_adapter(tmp)
                    instance.allow_all = False
                    instance.allowed_users = {"alice"}
                    instance.group_allowed_users = {"alice"}
                    instance.config.extra["allow_from"] = ["alice"]
                    runner = object.__new__(GatewayRunner)
                    runner._profile_adapters = {}
                    runner.pairing_store = types.SimpleNamespace(is_approved=lambda *args: False)
                    runner.pairing_stores = {}
                    runner.adapters = {instance.platform: instance}
                    runner.config = types.SimpleNamespace(platforms={instance.platform: instance.config})
                    expected = actor == "alice"
                    source = instance.build_source(chat_id="room", chat_type="dm", user_id=actor)
                    self.assertEqual(runner._is_user_authorized(source), expected)
                    self.assertEqual(instance._is_allowed(actor, "", chat_type="dm"), expected)
                    client = _Client([self.row(actor_id=actor)])
                    release = asyncio.Event()
                    if busy:
                        await self.start_turn(instance, client, release)
                    else:
                        instance._client = client
                    self.register(instance, self.session_key(instance))
                    try:
                        with patch("tools.approval.resolve_gateway_approval", return_value=1) as resolve:
                            await instance._poll_room("room")
                            self.assertEqual(resolve.call_count, int(expected))
                    finally:
                        if busy:
                            await self.finish(instance, release)
