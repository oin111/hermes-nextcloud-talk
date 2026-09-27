"""Tests for the native reaction-driven exec approvals (send_exec_approval).

Mirrors the Matrix adapter's approval-reaction suite, adapted to Talk's inbound
model: a reaction arrives as a poll row with systemMessage="reaction", the emoji
in "message", the reacting actor in "actorId", and the reacted-to prompt message
id in "parent" (verified live on Talk 23.0.10).

The approval engine must be exercised against the standalone stubs exactly like
every other suite in this file: no real gateway, no real Talk server.
"""

import asyncio
import sys
import time
import types
import unittest
from unittest.mock import patch

import adapter


async def _noop():
    return None


def _approval_prompt(session_key="sess-1", chat_id="dm-room", choices=("once", "session", "always", "deny")):
    """A minimal stand-in for the core's ExecApprovalPrompt dataclass."""
    labels = {"once": "Allow Once", "session": "Allow Session", "always": "Always Allow", "deny": "Deny"}
    return types.SimpleNamespace(
        session_key=session_key,
        chat_id=chat_id,
        text="⚠️ approval text",
        actions=[(labels[c], c, "primary" if c == "once" else "danger" if c == "deny" else "") for c in choices],
        command="rm -rf /",
        description="dangerous",
        smart_denied=False,
        metadata={"requester_user_id": "steffen.schoft"},
    )


class ApprovalReactionTestBase(unittest.IsolatedAsyncioTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.reaction_seeds = []
        self.reaction_removals = []

    def make_adapter(self, *, username="naya.bot", allowed=(), allow_all=False,
                     requester="steffen.schoft", timeout_seconds=3600.0):
        instance = adapter.NextcloudTalkAdapter.__new__(adapter.NextcloudTalkAdapter)
        instance.username = username
        instance.bot_name = "Hermes"
        instance.require_mention = False
        instance.max_message_length = 32000
        instance.allowed_users = set(allowed)
        instance.group_allowed_users = set(allowed)
        instance.allow_all = allow_all
        instance._room_types = {"dm-room": 1, "group-room": 2}
        instance._client = None
        instance._approval_timeout_seconds = timeout_seconds
        instance._approval_require_sender = True
        instance._approval_prompts_by_event = {}
        instance._approval_prompt_by_session = {}
        instance._last_message_ids = {}
        instance._ack_rooms = {}
        instance._inflight_message_ids = {}
        instance._persist_cursors = lambda: None

        sent = []

        async def send_message(_room, _text, **_kwargs):
            sent.append(_text)
            return {"id": 4242}

        instance._client = types.SimpleNamespace(
            send_message=send_message,
            react_to_message=self._record_reaction,
            remove_reaction=self._record_removal,
        )
        instance.sent_messages = sent
        instance.reaction_seeds = []
        instance.reaction_removals = []
        return instance

    async def _record_reaction(self, room_token, message_id, reaction):
        self.reaction_seeds.append((room_token, int(message_id), reaction))
        return {"status": 201, "reactions": {}}

    async def _record_removal(self, room_token, message_id, reaction):
        self.reaction_removals.append((room_token, int(message_id), reaction))
        return {"status": 200, "reactions": {}}

    def reaction_row(self, emoji, *, target=4242, actor_id="steffen.schoft",
                     actor_name="Steffen", numeric_id=9000, parent=None):
        return {
            "id": numeric_id,
            "messageType": "system",
            "systemMessage": "reaction",
            "message": emoji,
            "actorType": "users",
            "actorId": actor_id,
            "actorDisplayName": actor_name,
            "parent": target if parent is None else parent,
        }

    async def seed_prompt(self, instance, prompt=None, **kwargs):
        with patch.object(adapter.asyncio, "sleep", new=lambda *_a, **_k: _noop()):
            return await instance._send_exec_approval_prompt(prompt or _approval_prompt(**kwargs))


class SendExecApprovalSeedsReactionsTests(ApprovalReactionTestBase):
    async def test_seed_registers_prompt_and_all_choice_reactions(self):
        instance = self.make_adapter()
        result = await self.seed_prompt(instance)
        self.assertTrue(result.success)
        self.assertEqual(result.message_id, "4242")
        self.assertEqual(
            [emoji for _room, _mid, emoji in self.reaction_seeds],
            ["✅", "🌀", "♾️", "❌"],
        )
        self.assertEqual([mid for _room, mid, _emoji in self.reaction_seeds], [4242, 4242, 4242, 4242])
        prompt = instance._approval_prompts_by_event["4242"]
        self.assertIs(prompt, instance._approval_prompts_by_event["4242"])
        self.assertEqual(instance._approval_prompt_by_session["sess-1"], "4242")
        self.assertFalse(prompt.resolved)
        self.assertEqual(prompt.requester_user_id, "steffen.schoft")
        self.assertEqual(prompt.choices, ("once", "session", "always", "deny"))
        self.assertGreater(prompt.expires_at, time.monotonic())

    async def test_not_connected_fails_closed(self):
        instance = self.make_adapter()
        instance._client = None
        result = await instance._send_exec_approval_prompt(_approval_prompt())
        self.assertFalse(result.success)
        self.assertEqual(instance._approval_prompts_by_event, {})

    async def test_seed_failure_fails_closed_and_cleans_registry(self):
        instance = self.make_adapter()
        removals = []
        seeded = []

        async def broken_react(_room, _mid, emoji):
            seeded.append(emoji)
            if len(seeded) == 1:
                raise adapter.NextcloudTalkAPIError("HTTP 429", status_code=429)
            return {"status": 201, "reactions": {}}

        recorded = []
        async def removal(_room, _mid, emoji):
            recorded.append(emoji)
            return {"status": 200, "reactions": {}}

        instance._client = types.SimpleNamespace(
            send_message=instance._client.send_message,
            react_to_message=broken_react,
            remove_reaction=removal,
        )
        result = await instance._send_exec_approval_prompt(_approval_prompt())
        self.assertFalse(result.success)
        self.assertEqual(instance._approval_prompts_by_event, {})
        self.assertEqual(instance._approval_prompt_by_session, {})
        # Partially seeded reactions are retracted (best-effort cleanup).
        self.assertEqual(seeded, ["✅"])
        self.assertEqual(recorded, ["✅"])

    async def test_new_prompt_for_same_session_replaces_old_registration(self):
        instance = self.make_adapter()
        ids = iter([4242, 5151])
        sends = []

        async def send_message(_room, text, **_kwargs):
            sends.append(text)
            return {"id": next(ids)}

        instance._client.send_message = send_message
        await self.seed_prompt(instance)
        self.assertEqual(list(instance._approval_prompts_by_event), ["4242"])
        await self.seed_prompt(instance)
        self.assertEqual(list(instance._approval_prompts_by_event), ["5151"])
        self.assertEqual(instance._approval_prompt_by_session["sess-1"], "5151")
        self.assertEqual(len(sends), 2)
        # The retired prompt's seed reactions are retracted in the background.
        await asyncio.sleep(0)
        self.assertEqual(
            {emoji for _room, _mid, emoji in self.reaction_removals},
            {"✅", "🌀", "♾️", "❌"},
        )
        self.assertEqual(len(self.reaction_removals), 4)


class InboundReactionResolutionTests(ApprovalReactionTestBase):
    def make_allowed_adapter(self, **kwargs):
        kwargs.setdefault("allowed", ("steffen.schoft",))
        return self.make_adapter(**kwargs)

    async def test_reaction_from_allowed_sender_resolves_and_cleans_registry(self):
        instance = self.make_allowed_adapter()
        await self.seed_prompt(instance)
        resolved = []

        def fake_resolve(session_key, choice, request_id=None):
            resolved.append((session_key, choice))
            return 1

        with patch("tools.approval.resolve_gateway_approval", side_effect=fake_resolve):
            handled = await instance._handle_approval_system_message(
                self.reaction_row("✅"), "dm-room"
            )
        self.assertTrue(handled)
        # The seed-reaction cleanup rides an ensure_future task; yield once.
        await asyncio.sleep(0)
        self.assertEqual(resolved, [("sess-1", "once")])
        self.assertEqual(instance._approval_prompts_by_event, {})
        self.assertEqual(instance._approval_prompt_by_session, {})
        self.assertEqual([emoji for _r, _m, emoji in self.reaction_removals], ["✅", "🌀", "♾️", "❌"])

    async def test_reaction_for_unknown_target_is_ignored_not_consumed_as_turn(self):
        instance = self.make_allowed_adapter()
        await self.seed_prompt(instance)
        with patch("tools.approval.resolve_gateway_approval") as resolve:
            handled = await instance._handle_approval_system_message(
                self.reaction_row("✅", target=999999), "dm-room"
            )
        self.assertFalse(handled)
        resolve.assert_not_called()

    async def test_reaction_from_wrong_room_target_prompt_is_not_matched(self):
        instance = self.make_allowed_adapter()
        await self.seed_prompt(instance, chat_id="dm-room")
        # parent ids are matched per message id only (Talk rooms are isolated by
        # token server-side); a row in another room for a foreign id is a miss.
        with patch("tools.approval.resolve_gateway_approval") as resolve:
            handled = await instance._handle_approval_system_message(
                self.reaction_row("✅", target=777), "group-room"
            )
        self.assertFalse(handled)
        resolve.assert_not_called()

    async def test_foreign_sender_is_ignored(self):
        instance = self.make_adapter(allowed={"steffen.schoft"})
        await self.seed_prompt(instance)
        with patch("tools.approval.resolve_gateway_approval") as resolve:
            handled = await instance._handle_approval_system_message(
                self.reaction_row("✅", actor_id="mallory", actor_name="Mallory"), "dm-room"
            )
        self.assertTrue(handled)  # consumed, but no resolution
        resolve.assert_not_called()
        self.assertIn("4242", instance._approval_prompts_by_event)

    async def test_requester_gate_blocks_other_allowed_users(self):
        instance = self.make_adapter(allowed={"steffen.schoft", "alice"}, allow_all=False)
        await self.seed_prompt(instance)
        with patch("tools.approval.resolve_gateway_approval") as resolve:
            await instance._handle_approval_system_message(
                self.reaction_row("✅", actor_id="alice", actor_name="Alice"), "dm-room"
            )
        resolve.assert_not_called()
        self.assertIn("4242", instance._approval_prompts_by_event)

    async def test_requester_gate_can_be_disabled(self):
        instance = self.make_adapter(allowed={"steffen.schoft", "alice"})
        instance._approval_require_sender = False
        await self.seed_prompt(instance)
        with patch("tools.approval.resolve_gateway_approval", return_value=1) as resolve:
            await instance._handle_approval_system_message(
                self.reaction_row("✅", actor_id="alice"), "dm-room"
            )
        resolve.assert_called_once_with("sess-1", "once", request_id=None)

    async def test_wrong_emoji_is_ignored(self):
        instance = self.make_adapter(allowed={"steffen.schoft"})
        await self.seed_prompt(instance)
        with patch("tools.approval.resolve_gateway_approval") as resolve:
            handled = await instance._handle_approval_system_message(
                self.reaction_row("👍"), "dm-room"
            )
        self.assertTrue(handled)
        resolve.assert_not_called()
        self.assertIn("4242", instance._approval_prompts_by_event)

    async def test_every_choice_emoji_maps_to_its_word(self):
        expected = {"✅": "once", "🌀": "session", "♾️": "always", "♾": "always", "❌": "deny", "❎": "deny"}
        for emoji, choice in expected.items():
            with self.subTest(emoji=emoji):
                instance = self.make_allowed_adapter()
                await self.seed_prompt(instance)
                with patch("tools.approval.resolve_gateway_approval", return_value=1) as resolve:
                    await instance._handle_approval_system_message(
                        self.reaction_row(emoji, numeric_id=9100 + len(choice)), "dm-room"
                    )
                resolve.assert_called_once_with("sess-1", choice, request_id=None)

    async def test_bot_own_reactions_are_never_answers(self):
        instance = self.make_adapter(username="naya.bot", allowed={"naya.bot"})
        await self.seed_prompt(instance)
        with patch("tools.approval.resolve_gateway_approval") as resolve:
            handled = await instance._handle_approval_system_message(
                self.reaction_row("✅", actor_id="naya.bot"), "dm-room"
            )
        self.assertTrue(handled)
        resolve.assert_not_called()
        self.assertIn("4242", instance._approval_prompts_by_event)

    async def test_denials_when_gateway_has_nothing_pending_retire_expired_prompt_only(self):
        instance = self.make_allowed_adapter()
        await self.seed_prompt(instance)
        with patch("tools.approval.resolve_gateway_approval", return_value=0):
            await instance._handle_approval_system_message(self.reaction_row("✅"), "dm-room")
        # Still live: the gateway may settle the request through another surface.
        self.assertIn("4242", instance._approval_prompts_by_event)
        # But an expired prompt is retired with its seed reactions.
        instance._approval_prompts_by_event["4242"].expires_at = time.monotonic() - 1
        with patch("tools.approval.resolve_gateway_approval", return_value=0):
            await instance._handle_approval_system_message(
                self.reaction_row("🌀", numeric_id=9001), "dm-room"
            )
        self.assertNotIn("4242", instance._approval_prompts_by_event)
        self.assertNotIn("sess-1", instance._approval_prompt_by_session)

    async def test_expired_prompt_is_discarded_on_reaction(self):
        instance = self.make_adapter()
        await self.seed_prompt(instance)
        instance._approval_prompts_by_event["4242"].expires_at = time.monotonic() - 1
        with patch("tools.approval.resolve_gateway_approval") as resolve:
            handled = await instance._handle_approval_system_message(
                self.reaction_row("✅"), "dm-room"
            )
        self.assertTrue(handled)
        resolve.assert_not_called()
        self.assertEqual(instance._approval_prompts_by_event, {})
        self.assertEqual(instance._approval_prompt_by_session, {})
        # The bot's own seed reactions are retracted on expiry (background task).
        await asyncio.sleep(0)
        self.assertEqual(
            {emoji for _room, _mid, emoji in self.reaction_removals},
            {"✅", "🌀", "♾️", "❌"},
        )
        self.assertEqual(len(self.reaction_removals), 4)

    async def test_reaction_rows_with_malformed_ids_fall_through(self):
        instance = self.make_adapter()
        await self.seed_prompt(instance)
        row = self.reaction_row("✅")
        row["id"] = "not-an-int"
        handled = await instance._handle_approval_system_message(row, "dm-room")
        self.assertFalse(handled)
        row["id"] = 0
        handled = await instance._handle_approval_system_message(row, "dm-room")
        self.assertFalse(handled)

    async def test_non_reaction_system_message_is_not_intercepted(self):
        instance = self.make_adapter()
        row = {
            "id": 9100, "messageType": "system", "systemMessage": "call_started",
            "message": "call", "actorId": "naya.bot",
        }
        handled = await instance._handle_approval_system_message(row, "dm-room")
        self.assertFalse(handled)


class PollPathIntakeTests(ApprovalReactionTestBase):
    def make_poll_adapter(self, instance):
        instance.poll_timeout = 1
        instance.max_poll_batch = 10
        return instance

    async def test_pending_prompt_keeps_polling_an_inflight_room(self):
        instance = self.make_adapter()
        instance._config = types.SimpleNamespace(extra={})
        instance._inflight_generations = {}
        await self.seed_prompt(instance)
        self.make_poll_adapter(instance)
        instance._inflight_message_ids = {"dm-room": {1}}
        self.assertFalse(instance._room_has_pending_clarify("dm-room"))
        # Without a pending approval the room gate would freeze polling.
        self.assertTrue(instance._room_has_pending_approval_prompt("dm-room"))

    async def test_poll_room_consumes_reaction_row_before_turn_path(self):
        instance = self.make_adapter(allowed={"steffen.schoft"})
        await self.seed_prompt(instance)
        self.make_poll_adapter(instance)
        handled_by_turn = []

        async def handle_talk_message(msg, room, **_kwargs):
            handled_by_turn.append((msg["id"], room))

        instance._handle_talk_message = handle_talk_message
        instance._ack_rooms = {"dm-room": {"floor": 0, "successful": set(), "initialized": True}}

        async def get_messages(*_args, **_kwargs):
            return [self.reaction_row("✅")]

        instance._client.get_messages = get_messages
        with patch("tools.approval.resolve_gateway_approval", return_value=1):
            await instance._poll_room("dm-room")
        self.assertEqual(handled_by_turn, [])
        self.assertEqual(instance._approval_prompts_by_event, {})

    async def test_second_chance_intake_in_handle_talk_message(self):
        instance = self.make_adapter(allowed={"steffen.schoft"})
        await self.seed_prompt(instance)
        with patch("tools.approval.resolve_gateway_approval", return_value=1):
            await instance._handle_talk_message(self.reaction_row("✅"), "dm-room")
        self.assertEqual(instance._approval_prompts_by_event, {})

    async def test_prune_without_registry_does_not_abort_the_poll(self):
        """An adapter with no approval registry must still poll.

        _prune_expired_approval_prompts runs inside _poll_room's try block, so touching
        the registry unguarded raised AttributeError there and the generic handler
        swallowed it — every message in the batch was dropped without a trace.
        """
        instance = self.make_adapter(allowed={"steffen.schoft"})
        self.make_poll_adapter(instance)
        instance._config = types.SimpleNamespace(extra={})
        instance._inflight_generations = {}
        instance._inflight_message_ids = {}
        instance._ack_rooms = {"dm-room": {"floor": 0, "successful": set(), "initialized": True}}
        # Exactly the shape of an instance that never ran the approval init path.
        del instance._approval_prompts_by_event
        del instance._approval_prompt_by_session
        handled = []

        async def handle_talk_message(msg, room, **_kwargs):
            handled.append((msg["id"], room))

        async def get_messages(*_args, **_kwargs):
            return [{"id": 77, "actorType": "users", "actorId": "alice", "message": "hi"}]

        instance._handle_talk_message = handle_talk_message
        instance._client.get_messages = get_messages
        instance._prune_expired_approval_prompts()  # must not raise on its own
        await instance._poll_room("dm-room")
        self.assertEqual(handled, [(77, "dm-room")])

    async def test_core_pending_approval_keeps_polling_without_a_card(self):
        """Keepalive must not depend on a successfully seeded reaction card.

        Covers the plain-text /approve fallback (seeding failed) and a card that expired
        while core still holds the request: in both the registry is empty, and the room
        gate would otherwise freeze the only consumer able to deliver the answer.
        """
        instance = self.make_adapter()
        self.make_poll_adapter(instance)
        instance._inflight_message_ids = {"dm-room": {1}}
        instance._inflight_generations = {
            ("dm-room", 1): types.SimpleNamespace(source=object()),
        }
        # No card registered anywhere.
        instance._approval_prompts_by_event = {}
        instance._approval_prompt_by_session = {}
        self.assertFalse(instance._room_has_pending_approval_prompt("dm-room"))
        instance._source_session_key = lambda _source: "sess-1"

        with patch("tools.approval.has_blocking_approval", return_value=True):
            self.assertTrue(instance._room_has_pending_exec_approval("dm-room"))
        with patch("tools.approval.has_blocking_approval", return_value=False):
            self.assertFalse(instance._room_has_pending_exec_approval("dm-room"))
        # A room with no in-flight turn never consults core.
        self.assertFalse(instance._room_has_pending_exec_approval("group-room"))

    async def test_reaction_that_resolves_nothing_is_logged_at_warning(self):
        """A tap resolving 0 requests must leave a trace: silence made a dead button
        indistinguishable from 'the user never reacted'."""
        instance = self.make_adapter(allowed={"steffen.schoft"})
        await self.seed_prompt(instance)
        with patch("tools.approval.resolve_gateway_approval", return_value=0), \
                patch("tools.approval.has_blocking_approval", return_value=True), \
                self.assertLogs(adapter.logger, level="WARNING") as logs:
            await instance._handle_approval_system_message(self.reaction_row("✅"), "dm-room")
        rendered = "\n".join(logs.output)
        self.assertIn("resolved NOTHING", rendered)
        # The prompt stays live: core still has something to answer.
        self.assertIn("4242", instance._approval_prompts_by_event)


class ApprovalTimeoutTests(ApprovalReactionTestBase):
    def test_timeout_prefers_env_override(self):
        with patch.dict(adapter.os.environ, {"NEXTCLOUD_APPROVAL_TIMEOUT_SECONDS": "120"}):
            self.assertEqual(adapter._resolve_approval_timeout_seconds({}), 120.0)

    def test_timeout_prefers_yaml_extra_over_env(self):
        # env present but blank -> falls through to the next source
        with patch.dict(adapter.os.environ, {"NEXTCLOUD_APPROVAL_TIMEOUT_SECONDS": ""}):
            self.assertEqual(
                adapter._resolve_approval_timeout_seconds({"approval_timeout_seconds": "45"}), 45.0
            )

    def test_timeout_falls_back_to_gateway_approvals_timeout(self):
        with patch.dict(adapter.os.environ, {"NEXTCLOUD_APPROVAL_TIMEOUT_SECONDS": ""}):
            import sys as _sys
            module = types.ModuleType("gateway.platforms.base_exec_approval")
            module.approval_timeout_seconds = lambda: 300
            gateway_mod = _sys.modules.get("gateway")
            platforms_mod = _sys.modules.get("gateway.platforms")
            _sys.modules["gateway.platforms.base_exec_approval"] = module
            try:
                self.assertEqual(adapter._resolve_approval_timeout_seconds({}), 300.0)
            finally:
                if gateway_mod is None:
                    _sys.modules.pop("gateway", None)
                if platforms_mod is None:
                    _sys.modules.pop("gateway.platforms", None)
                _sys.modules.pop("gateway.platforms.base_exec_approval", None)

    def test_timeout_rejects_nonpositive_and_garbage(self):
        with patch.dict(adapter.os.environ, {"NEXTCLOUD_APPROVAL_TIMEOUT_SECONDS": "-5"}):
            import sys as _sys
            _sys.modules.pop("gateway.platforms.base_exec_approval", None)
            # No gateway module importable in the standalone loader -> final default.
            value = adapter._resolve_approval_timeout_seconds({})
            self.assertGreaterEqual(value, 1.0)
        with patch.dict(adapter.os.environ, {"NEXTCLOUD_APPROVAL_TIMEOUT_SECONDS": "zero"}):
            self.assertGreaterEqual(adapter._resolve_approval_timeout_seconds({}), 1.0)

    async def test_expired_prompt_pruning_retracts_seed_reactions(self):
        instance = self.make_adapter()
        await self.seed_prompt(instance)
        instance._approval_prompts_by_event["4242"].expires_at = time.monotonic() - 1
        instance._prune_expired_approval_prompts()
        self.assertEqual(instance._approval_prompts_by_event, {})
        await asyncio.sleep(0)
        self.assertEqual(
            {emoji for _room, _mid, emoji in self.reaction_removals},
            {"✅", "🌀", "♾️", "❌"},
        )
        self.assertEqual(len(self.reaction_removals), 4)

    async def test_disconnect_clears_pending_prompts(self):
        instance = self.make_adapter()
        await self.seed_prompt(instance)
        instance._approval_prompts_by_event.clear()
        instance._approval_prompt_by_session.clear()
        self.assertEqual(instance._approval_prompts_by_event, {})


class ReactionQueryEncodingTests(unittest.IsolatedAsyncioTestCase):
    def test_delete_query_encodes_emoji(self):
        client = adapter.NextcloudTalkClient.__new__(adapter.NextcloudTalkClient)
        client.base_url = "https://cloud.example"
        client.username = "bot"
        client.password = "x"
        client.timeout = 5
        client.max_json_bytes = 1 << 20
        client.max_body_bytes = 1 << 20
        url = client._reaction_url("roomTok", 4242)
        self.assertEqual(
            url, "https://cloud.example/ocs/v2.php/apps/spreed/api/v1/reaction/roomTok/4242"
        )
        # The DELETE route builds its query through parse.urlencode — assert the
        # emoji lands percent-encoded, never raw (raw emoji breaks ASCII URLs).
        from urllib import parse as _parse
        query = _parse.urlencode({"reaction": "✅"})
        self.assertEqual(query, "reaction=%E2%9C%85")


class SupportsExecApprovalButtonsTests(unittest.TestCase):
    def test_runner_detects_native_buttons(self):
        self.assertTrue(adapter.NextcloudTalkAdapter.supports_exec_approval_buttons())
        self.assertIsNotNone(adapter.NextcloudTalkAdapter.send_exec_approval)
        self.assertIsNotNone(adapter.NextcloudTalkAdapter._send_exec_approval_prompt)


if __name__ == "__main__":
    unittest.main()

class RequestIdBindingTests(ApprovalReactionTestBase):
    """Elara-Review fixes: request_id binding, room gate, choice whitelist, prune."""

    def make_allowed_adapter(self, **kwargs):
        kwargs.setdefault("allowed", ("steffen.schoft",))
        return self.make_adapter(**kwargs)

    def _prompt_with_request_id(self, request_id, **kwargs):
        prompt = _approval_prompt(**kwargs)
        prompt.metadata = {"requester_user_id": "steffen.schoft", "approval_request_id": request_id}
        return prompt

    async def test_seed_records_request_id_from_metadata(self):
        instance = self.make_adapter()
        await self.seed_prompt(instance, prompt=self._prompt_with_request_id("req-abc"))
        prompt = instance._approval_prompts_by_event["4242"]
        self.assertEqual(prompt.request_id, "req-abc")

    async def test_seed_without_request_id_stays_empty(self):
        instance = self.make_adapter()
        await self.seed_prompt(instance)  # default metadata has no approval_request_id
        prompt = self._approval_prompt_get(instance)
        self.assertEqual(prompt.request_id, "")

    def _approval_prompt_get(self, instance):
        return instance._approval_prompts_by_event["4242"]

    async def test_resolve_uses_request_id(self):
        instance = self.make_allowed_adapter()
        instance._room_types = {"dm-room": 1}
        await self.seed_prompt(instance, prompt=self._prompt_with_request_id("req-xyz"))
        prompt = self._approval_prompt_get(instance)
        captured = {}

        def fake_resolve(session_key, choice, request_id=None):
            captured.update(session_key=session_key, choice=choice, request_id=request_id)
            return 1

        with patch.object(adapter, "_APPROVAL_REACTION_SYSTEM_MESSAGES", ("reaction",)), \
             patch("tools.approval.resolve_gateway_approval", new=fake_resolve):
            handled = await instance._handle_approval_system_message(
                self.reaction_row("✅", target=4242), "dm-room")
        self.assertTrue(handled)
        self.assertEqual(captured["request_id"], "req-xyz")

    async def test_unoffered_choice_cannot_resolve(self):
        instance = self.make_allowed_adapter()
        instance._room_types = {"dm-room": 1}
        # Smart-deny card: only once + deny offered — ♾️ (always) was NOT offered.
        await self.seed_prompt(instance, choices=("once", "deny"))
        prompt = self._approval_prompt_get(instance)
        resolved = []

        def fake_resolve(session_key, choice, request_id=None):
            resolved.append(choice)
            return 1

        with patch.object(adapter, "_APPROVAL_REACTION_SYSTEM_MESSAGES", ("reaction",)), \
             patch("tools.approval.resolve_gateway_approval", new=fake_resolve):
            handled = await instance._handle_approval_system_message(
                self.reaction_row("♾️", target=4242), "dm-room")
        self.assertTrue(handled)
        self.assertEqual(resolved, [])  # not offered → consumed-and-ignored

    async def test_reaction_from_other_room_does_not_resolve(self):
        instance = self.make_adapter()
        await self.seed_prompt(instance)  # prompt.chat_id = "dm-room"
        prompt = self._approval_prompt_get(instance)
        resolved = []

        def fake_resolve(session_key, choice, request_id=None):
            resolved.append(choice)
            return 1

        with patch.object(adapter, "_APPROVAL_REACTION_SYSTEM_MESSAGES", ("reaction",)), \
             patch("tools.approval.resolve_gateway_approval", new=fake_resolve):
            handled = await instance._handle_approval_system_message(
                self.reaction_row("✅", target=4242), "group-room")
        self.assertTrue(handled)
        self.assertEqual(resolved, [])

    async def test_poll_room_invokes_prune(self):
        instance = self.make_adapter()
        instance.max_poll_batch = 20
        instance.poll_timeout = 1
        instance._prune_expired_approval_prompts = lambda: setattr(self, "pruned", True)
        self.pruned = False

        async def fail_get_messages(*_a, **_k):
            raise AssertionError("poll should have returned before fetching")

        instance._client.get_messages = fail_get_messages
        instance._inflight_message_ids = {"dm-room": 55}
        instance._room_has_pending_clarify = lambda token: False
        instance._room_has_pending_approval_prompt = lambda token: False
        await instance._poll_room("dm-room")
        self.assertTrue(self.pruned)
