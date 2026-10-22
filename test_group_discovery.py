"""Safe group discovery, using real Hermes intake and isolated cursor state."""
import asyncio
import os
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

import adapter
from gateway.config import PlatformConfig
import test_lifecycle_real as lifecycle


class GroupDiscoveryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.env_patch = patch.dict(os.environ, {
            k: v for k, v in os.environ.items() if not k.startswith('NEXTCLOUD_TALK_')
        }, clear=True)
        self.env_patch.start()
        self.addCleanup(self.env_patch.stop)
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def make_adapter(self, **extra):
        settings = dict(url='https://cloud.example', username='bot', password='secret',
                        group_allow_from=['alice'], allow_from=['alice'])
        settings.update(extra)
        instance = adapter.NextcloudTalkAdapter(PlatformConfig(
            enabled=True, typing_indicator=False, extra=settings))
        instance._cursor_path = Path(self.tmp.name) / 'cursors.json'
        return instance

    async def discover(self, instance, rooms):
        instance._client = types.SimpleNamespace(
            list_conversations=lambda: asyncio.sleep(0, result=rooms))
        await instance._refresh_discovered_rooms(force=True, process_new_messages=False)

    async def test_default_discovers_private_groups_with_restrictive_sender_allowlist(self):
        instance = self.make_adapter()
        await self.discover(instance, [dict(token='group', type=2), dict(token='dm', type=1)])
        self.assertEqual(instance.room_tokens, ['group', 'dm'])
        self.assertEqual(instance._room_types['group'], 2)


    async def test_group_opt_out_keeps_dm_discovery(self):
        instance = self.make_adapter(auto_discover_group_rooms=False)
        await self.discover(instance, [dict(token='group', type=2), dict(token='dm', type=1)])
        self.assertEqual(instance.room_tokens, ['dm'])

    async def test_group_opt_out_env_overrides_yaml(self):
        with patch.dict(os.environ, {'NEXTCLOUD_TALK_AUTO_DISCOVER_GROUP_ROOMS': 'false'}):
            instance = self.make_adapter(auto_discover_group_rooms=True)
        await self.discover(instance, [dict(token='group', type=2)])
        self.assertEqual(instance.room_tokens, [])

    async def test_group_opt_in_env_overrides_yaml(self):
        with patch.dict(os.environ, {'NEXTCLOUD_TALK_AUTO_DISCOVER_GROUP_ROOMS': 'true'}):
            instance = self.make_adapter(auto_discover_group_rooms=False)
        await self.discover(instance, [dict(token='group', type=2)])
        self.assertEqual(instance.room_tokens, ['group'])


    async def test_discovery_disabled_preserves_explicit_groups(self):
        instance = self.make_adapter(auto_discover_rooms=False, room_tokens='manual')
        await self.discover(instance, [dict(token='manual', type=2),
                                      dict(token='group', type=2), dict(token='dm', type=1)])
        self.assertEqual(instance.room_tokens, ['manual'])

    async def test_group_discovery_rejects_unrestricted_or_missing_group_allowlist(self):
        cases = [dict(group_allow_from=[]), dict(group_allow_from=''),
                 dict(group_allow_from=None), dict(group_allow_from=['*']),
                 dict(group_allow_from=['alice', '*'])]
        for settings in cases:
            with self.subTest(settings=settings):
                instance = self.make_adapter(auto_discover_group_rooms=True, **settings)
                await self.discover(instance, [dict(token='group', type=2), dict(token='dm', type=1)])
                self.assertEqual(instance.room_tokens, ['dm'])

    async def test_env_allowlist_is_authoritative_for_discovery(self):
        for allowed, expected in [('alice', ['group']), ('*', []), ('alice,*', [])]:
            with self.subTest(allowed=allowed), patch.dict(
                    os.environ, {'NEXTCLOUD_TALK_ALLOWED_USERS': allowed}):
                instance = self.make_adapter(group_allow_from=['alice'])
                await self.discover(instance, [dict(token='group', type=2)])
                self.assertEqual(instance.room_tokens, expected)

    async def test_allow_all_disables_automatic_groups_not_manual_rooms(self):
        with patch.dict(os.environ, {'NEXTCLOUD_TALK_ALLOW_ALL_USERS': 'true'}):
            instance = self.make_adapter(room_tokens='manual', auto_discover_group_rooms=True)
        await self.discover(instance, [dict(token='manual', type=2),
                                      dict(token='group', type=2), dict(token='dm', type=1)])
        self.assertEqual(instance.room_tokens, ['manual', 'dm'])
        self.assertTrue(instance._is_allowed('outsider', '', chat_type='group'))

    async def test_public_system_note_unknown_types_are_not_discovered(self):
        instance = self.make_adapter()
        await self.discover(instance, [dict(token=f'room{t}', type=t)
                                      for t in [3, 4, 5, 6, 0, 99]])
        self.assertEqual(instance.room_tokens, [])

    async def test_explicit_rooms_unchanged_under_group_optout(self):
        instance = self.make_adapter(room_tokens='group,public', auto_discover_group_rooms=False,
                                     group_allow_from=[])
        await self.discover(instance, [dict(token='group', type=2), dict(token='public', type=3)])
        self.assertEqual(instance.room_tokens, ['group', 'public'])

    async def test_combined_discovery_bound_does_not_partially_admit_groups(self):
        instance = self.make_adapter(max_rooms=2, room_tokens='manual')
        with self.assertRaises(adapter.NextcloudTalkAPIError):
            await self.discover(instance, [dict(token='group', type=2), dict(token='dm', type=1)])
        self.assertEqual(instance.room_tokens, ['manual'])
        self.assertEqual(instance._discovered_room_tokens, set())

    async def test_overflow_revokes_public_group_before_next_poll_cycle(self):
        instance = self.make_adapter(max_rooms=3, room_tokens='manual,manualtwo')
        await self.discover(instance, [dict(token='oldgroup', type=2)])
        for token in instance.room_tokens:
            instance._mark_room_initialized(token)
        instance._commit_cursor('oldgroup', 900)
        events, polled = [], []
        async def handle(event):
            events.append(event)
        instance.set_message_handler(handle)
        async def messages(token, **kwargs):
            polled.append(token)
            if token == 'manualtwo':
                instance._running = False
            return [{**lifecycle._MESSAGE, 'id': 901}] if token == 'oldgroup' else []
        instance._client = types.SimpleNamespace(
            list_conversations=lambda: asyncio.sleep(0, result=[
                dict(token='oldgroup', type=3), dict(token='newgroup1', type=2),
                dict(token='newgroup2', type=2)]), get_messages=messages)
        with self.assertRaises(adapter.NextcloudTalkAPIError):
            await instance._refresh_discovered_rooms(force=True, process_new_messages=False)
        # The real loop skips discovery during its interval after the failed refresh.
        instance._running = True
        try:
            await asyncio.wait_for(instance._poll_loop(), 3)
            await lifecycle.RealHermesLifecycleTests.wait_for_background(self, instance)
            self.assertEqual(events, [])
            self.assertNotIn('oldgroup', polled)
            self.assertEqual(set(polled), {'manual', 'manualtwo'})
            self.assertEqual(instance.room_tokens, ['manual', 'manualtwo'])
            self.assertEqual(instance._discovered_room_tokens, set())
            self.assertFalse(instance._ack_rooms['oldgroup']['active'])
            self.assertTrue(instance._is_acknowledged('oldgroup', 900))
        finally:
            await instance.disconnect()

    async def test_overflow_cancels_revoked_group_initialization(self):
        instance = self.make_adapter(max_rooms=3, room_tokens='manual,manualtwo')
        await self.discover(instance, [dict(token='oldgroup', type=2)])
        instance._commit_cursor('oldgroup', 900)
        instance._inflight_message_ids['oldgroup'] = {901}
        instance._running = True
        entered = asyncio.Event()
        async def history(*args):
            entered.set()
            await asyncio.Event().wait()
        instance._fetch_initial_backlog = history
        instance._schedule_room_initialization('oldgroup')
        await asyncio.wait_for(entered.wait(), 2)
        owner = instance._initialization_tasks['oldgroup']
        try:
            with self.assertRaises(adapter.NextcloudTalkAPIError):
                await self.discover(instance, [dict(token='oldgroup', type=3),
                    dict(token='newgroup1', type=2), dict(token='newgroup2', type=2)])
            self.assertTrue(owner.done())
            self.assertFalse(instance._initialization_tasks)
            self.assertNotIn('oldgroup', instance.room_tokens)
            self.assertEqual(instance._inflight_message_ids['oldgroup'], {901})
            self.assertFalse(instance._is_acknowledged('oldgroup', 901))
            self.assertTrue(instance._is_acknowledged('oldgroup', 900))
            reloaded = self.make_adapter()
            reloaded._load_cursors()
            self.assertTrue(reloaded._is_acknowledged('oldgroup', 900))
            self.assertFalse(reloaded._ack_rooms['oldgroup']['active'])
        finally:
            await instance.disconnect()

    async def test_explicit_group_is_not_counted_twice(self):
        instance = self.make_adapter(max_rooms=2, room_tokens='manual')
        await self.discover(instance, [dict(token='manual', type=2), dict(token='group', type=2)])
        self.assertEqual(instance.room_tokens, ['manual', 'group'])

    async def test_new_group_uses_existing_backlog_initialization(self):
        instance = self.make_adapter()
        calls = []
        async def initialize(token):
            calls.append(token)
        instance._initialize_room = initialize
        await self.discover(instance, [dict(token='group', type=2)])
        # Recreate a fresh admission with normal (not readiness-only) discovery.
        instance._discovered_room_tokens.clear()
        instance.room_tokens.clear()
        await instance._refresh_discovered_rooms(force=True)
        self.assertEqual(calls, ['group'])
        await instance._refresh_discovered_rooms(force=True)
        self.assertEqual(calls, ['group'])

    async def test_removed_group_stops_intake_without_losing_ack(self):
        instance = self.make_adapter()
        await self.discover(instance, [dict(token='group', type=2)])
        instance._commit_cursor('group', 900)
        await self.discover(instance, [])
        self.assertNotIn('group', instance.room_tokens)
        self.assertNotIn('group', instance._discovered_room_tokens)
        self.assertTrue(instance._is_acknowledged('group', 900))
        self.assertFalse(instance._ack_rooms['group']['active'])
        await self.discover(instance, [dict(token='group', type=2)])
        self.assertTrue(instance._is_acknowledged('group', 900))
        self.assertTrue(instance._ack_rooms['group']['active'])

    async def test_removed_group_cancels_pending_initialization(self):
        instance = self.make_adapter()
        await self.discover(instance, [dict(token='group', type=2)])
        self.assertIn('group', instance.room_tokens)
        instance._running = True
        entered = asyncio.Event()
        async def history(*args):
            entered.set()
            await asyncio.Event().wait()
        instance._fetch_initial_backlog = history
        instance._schedule_room_initialization('group')
        await asyncio.wait_for(entered.wait(), 2)
        owner = instance._initialization_tasks['group']
        try:
            await self.discover(instance, [])
            self.assertTrue(owner.done())
            self.assertFalse(instance._initialization_tasks)
            self.assertNotIn('group', instance.room_tokens)
        finally:
            await instance.disconnect()

    async def test_group_authorization_change_removes_automatic_group(self):
        instance = self.make_adapter()
        await self.discover(instance, [dict(token='group', type=2)])
        instance.allow_all = True
        await self.discover(instance, [dict(token='group', type=2)])
        self.assertEqual(instance.room_tokens, [])

    async def test_group_intake_uses_stable_id_before_dispatch(self):
        instance = self.make_adapter()
        await self.discover(instance, [dict(token='group', type=2)])
        self.assertIn('group', instance.room_tokens)
        events = []
        entered, release = asyncio.Event(), asyncio.Event()
        async def handle(event):
            events.append(event)
            entered.set()
            await release.wait()
        instance.set_message_handler(handle)
        # A display-name impersonator cannot dispatch; an allowed ID can.
        await instance._handle_talk_message({**lifecycle._MESSAGE, 'id': 900,
            'actorId': 'outsider', 'actorDisplayName': 'alice'}, 'group')
        self.assertEqual(events, [])
        self.assertTrue(instance._is_acknowledged('group', 900))
        try:
            await instance._handle_talk_message({**lifecycle._MESSAGE, 'id': 901}, 'group')
            await asyncio.wait_for(entered.wait(), 2)
            self.assertEqual(len(events), 1)
            self.assertEqual(events[0].source.chat_type, 'group')
            self.assertEqual(events[0].source.user_id, 'alice')
            self.assertFalse(instance._is_acknowledged('group', 901))
        finally:
            release.set()
            await lifecycle.RealHermesLifecycleTests.wait_for_background(self, instance)
        self.assertTrue(instance._is_acknowledged('group', 901))


if __name__ == '__main__':
    unittest.main()
