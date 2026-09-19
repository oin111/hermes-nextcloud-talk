"""Connection readiness must not depend on completion of an inbound turn."""
import asyncio
import tempfile
import types
import unittest
from unittest.mock import patch
import test_lifecycle_real as lifecycle
_MESSAGE = lifecycle._MESSAGE
import adapter


class StartupTests(unittest.IsolatedAsyncioTestCase):
    make_adapter = lifecycle.RealHermesLifecycleTests.make_adapter
    wait_for_background = lifecycle.RealHermesLifecycleTests.wait_for_background

    def setUp(self):
        import os
        self.env_patch = patch.dict(os.environ, {k: v for k, v in os.environ.items() if not k.startswith('NEXTCLOUD_TALK_')}, clear=True)
        self.env_patch.start()
        self.addCleanup(self.env_patch.stop)
    async def test_connect_returns_before_backlog_handler_completes(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            entered, release = asyncio.Event(), asyncio.Event()
            calls = []
            async def handler(event):
                calls.append(event.message_id)
                entered.set()
                await release.wait()
                return 'done'
            async def messages(room, **kwargs):
                if not kwargs.get('look_into_future', False):
                    return [{**_MESSAGE, 'id': 900}]
                await asyncio.sleep(0.01)
                return []
            client = types.SimpleNamespace(
                list_conversations=lambda: asyncio.sleep(0, result=[{'token': 'room', 'type': 1}]),
                get_messages=messages,
                send_message=lambda *a, **k: asyncio.sleep(0, result={'id': 901}),
            )
            instance.set_message_handler(handler)
            with patch.object(adapter, 'NextcloudTalkClient', return_value=client) as factory:
                factory._origin = lambda url: ('https', 'cloud.example', 443)
                try:
                    self.assertTrue(await asyncio.wait_for(instance.connect(), 2))
                    await asyncio.wait_for(entered.wait(), 2)
                    self.assertFalse(instance._is_room_initialized('room'))
                    self.assertFalse(instance._is_acknowledged('room', 900))
                    await asyncio.sleep(0.05)
                    self.assertEqual(len(calls), 1)
                    release.set()
                    for _ in range(200):
                        if instance._is_room_initialized('room'):
                            break
                        await asyncio.sleep(0.01)
                    self.assertTrue(instance._is_room_initialized('room'))
                    self.assertTrue(instance._is_acknowledged('room', 900))
                    self.assertEqual(len(calls), 1)
                finally:
                    release.set()
                    await instance.disconnect()
                    await self.wait_for_background(instance)

    async def test_partial_backlog_retries_without_blocking_other_rooms(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            instance.room_tokens = ['room', 'other']
            instance._mark_room_initialized('other')
            instance._running = True
            entered, release, other_polled = asyncio.Event(), asyncio.Event(), asyncio.Event()
            attempts = []
            async def handler(event):
                attempts.append(int(event.message_id))
                if event.message_id == '902':
                    entered.set()
                    await release.wait()
                    if attempts.count(902) == 1:
                        raise RuntimeError('intentional transient failure')
                return 'done'
            async def history(room, limit):
                return [{**_MESSAGE, 'id': i} for i in (900, 902)]
            async def poll(room):
                self.assertEqual(room, 'other')
                other_polled.set()
            instance._client = types.SimpleNamespace(send_message=lambda *a, **k: asyncio.sleep(0, result={'id': 999}))
            instance._fetch_initial_backlog = history
            instance._poll_room = poll
            instance.set_message_handler(handler)
            instance._poll_task = asyncio.create_task(instance._poll_loop())
            try:
                await asyncio.wait_for(entered.wait(), 2)
                await asyncio.wait_for(other_polled.wait(), 2)
                self.assertTrue(instance._is_acknowledged('room', 900))
                self.assertFalse(instance._is_room_initialized('room'))
                owner = instance._initialization_tasks['room']
                instance._schedule_room_initialization('room')
                self.assertIs(owner, instance._initialization_tasks['room'])
                release.set()
                for _ in range(400):
                    if instance._is_room_initialized('room'):
                        break
                    await asyncio.sleep(0.01)
                self.assertTrue(instance._is_room_initialized('room'))
                self.assertEqual(attempts, [900, 902, 902])
            finally:
                release.set()
                await instance.disconnect()
                await self.wait_for_background(instance)
            self.assertTrue(owner.done())
            self.assertFalse(instance._initialization_tasks)

    async def test_discovery_removal_cancels_pending_initialization(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            instance._running = True
            instance.auto_discover_rooms = True
            instance._configured_room_tokens = []
            instance._discovered_room_tokens = {'room'}
            entered = asyncio.Event()
            async def history(room, limit):
                entered.set()
                await asyncio.Event().wait()
            instance._fetch_initial_backlog = history
            instance._client = types.SimpleNamespace(list_conversations=lambda: asyncio.sleep(0, result=[]))
            instance._schedule_room_initialization('room')
            await asyncio.wait_for(entered.wait(), 2)
            owner = instance._initialization_tasks['room']
            try:
                await instance._refresh_discovered_rooms(force=True, process_new_messages=False)
                self.assertTrue(owner.done(), 'removed room still owns pending initialization')
                self.assertFalse(instance._initialization_tasks)
            finally:
                await instance.disconnect()

    async def test_backlog_does_not_pass_existing_inflight_message(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            instance._fetch_initial_backlog = lambda *a: asyncio.sleep(0, result=[{**_MESSAGE, 'id': 900}])
            instance._inflight_message_ids['room'] = {900}
            with self.assertRaises(RuntimeError):
                await instance._initialize_room('room')
            self.assertFalse(instance._is_room_initialized('room'))

    async def test_completed_initialization_releases_task_registry(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            instance._running = True
            instance._fetch_initial_backlog = lambda *a: asyncio.sleep(0, result=[])
            instance._schedule_room_initialization('room')
            task = instance._initialization_tasks['room']
            await task
            await asyncio.sleep(0)
            try:
                self.assertFalse(instance._initialization_tasks)
            finally:
                await instance.disconnect()

    async def test_disconnect_cancels_blocked_backlog_without_marking_initialized(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            instance._running = True
            entered = asyncio.Event()
            async def history(room, limit):
                entered.set()
                await asyncio.Event().wait()
            instance._fetch_initial_backlog = history
            instance._schedule_room_initialization('room')
            await asyncio.wait_for(entered.wait(), 2)
            owner = instance._initialization_tasks['room']
            await asyncio.wait_for(instance.disconnect(), 2)
            self.assertTrue(owner.cancelled())
            self.assertFalse(instance._is_room_initialized('room'))
            self.assertFalse(instance._initializing_rooms)
