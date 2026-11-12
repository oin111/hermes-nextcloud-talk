"""Talk restart crash window and mention gate, using installed core lifecycle."""
import asyncio
import tempfile
import types
import unittest
from unittest.mock import patch

import adapter
from gateway.config import GatewayConfig
from gateway.hooks import HookRegistry
from gateway.run import GatewayRunner
import test_lifecycle_real as lifecycle
_MESSAGE = lifecycle._MESSAGE


@unittest.skipUnless(hasattr(GatewayRunner, '_is_user_authorized_for_source') and hasattr(GatewayRunner, '_hm_resolve_command'), 'requires installed core authorization and command resolution APIs')
class RestartCommandTests(unittest.IsolatedAsyncioTestCase):
    make_adapter = lifecycle.RealHermesLifecycleTests.make_adapter
    wait_for_background = lifecycle.RealHermesLifecycleTests.wait_for_background

    async def test_restart_consumed_before_real_core_restart_and_reload(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            runner = GatewayRunner.__new__(GatewayRunner)
            runner.config = GatewayConfig()
            runner.hooks = HookRegistry()
            runner._restart_requested = False
            runner._draining = False
            runner._running_agent_count = lambda: 0
            restarts = []
            acks = []
            runner.request_restart = lambda **kw: restarts.append(kw)
            instance.gateway_runner = runner

            async def handler(event):
                # Use real core resolution/access and restart implementation; only
                # process restart and marker destination are isolated from the host.
                handled, result, command, canonical = await runner._hm_resolve_command(
                    event, event.source, "test-session")
                if handled:
                    return result
                self.assertEqual(canonical, "restart")
                return await runner._handle_restart_command(event)

            async def post(*args, **kwargs):
                acks.append(kwargs)
                # Simulate gateway disconnect cancelling delivery before completion.
                raise asyncio.CancelledError()

            instance._client = types.SimpleNamespace(send_message=post)
            instance.set_message_handler(handler)
            with patch.dict('os.environ', {'NEXTCLOUD_TALK_ALLOWED_USERS': 'alice'}), patch(
                'gateway.run._hermes_home', __import__('pathlib').Path(tmp)
            ):
                await instance._handle_talk_message({**_MESSAGE, 'id': 33619, 'message': 'Hermes, /restart'}, 'room')
                await self.wait_for_background(instance)
                await instance.disconnect()
                restored = self.make_adapter(tmp)
                restored._load_cursors()
                restored.gateway_runner = runner
                restored._client = types.SimpleNamespace(send_message=post)
                restored.set_message_handler(handler)
                await restored._handle_talk_message({**_MESSAGE, 'id': 33619, 'message': 'Hermes, /restart'}, 'room')
                await self.wait_for_background(restored)
            self.assertEqual(len(acks), 1)
            self.assertEqual(len(restarts), 1)
            self.assertTrue(restored._is_acknowledged('room', 33619))

    async def test_persistence_failure_prevents_restart_handler(self):
        for primitive in ('json.dump', 'os.replace', 'os.fsync'):
            with self.subTest(primitive=primitive), tempfile.TemporaryDirectory() as tmp:
                instance = self.make_adapter(tmp)
                runner = GatewayRunner.__new__(GatewayRunner)
                runner.config = GatewayConfig()
                instance.gateway_runner = runner
                called = []
                async def handler(event):
                    called.append(event)
                    return None
                instance.set_message_handler(handler)
                with patch.dict('os.environ', {'NEXTCLOUD_TALK_ALLOWED_USERS': 'alice'}), patch(
                    'adapter.' + primitive, side_effect=OSError('synthetic storage fault')
                ):
                    await instance._handle_talk_message({**_MESSAGE, 'id': 204, 'message': '/restart'}, 'room')
                    await self.wait_for_background(instance)
                self.assertEqual(called, [])
                restored = self.make_adapter(tmp)
                restored._load_cursors()
                self.assertFalse(restored._is_acknowledged('room', 204))

    async def test_directory_fsync_failure_blocks_restart_with_existing_cursor(self):
        import os
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            instance._commit_cursor('room', 202)
            runner = GatewayRunner.__new__(GatewayRunner)
            runner.config = GatewayConfig()
            instance.gateway_runner = runner
            called = []
            async def handler(event):
                called.append(event)
                return None
            instance.set_message_handler(handler)
            real_fsync = os.fsync
            fsyncs = []
            def fsync(fd):
                fsyncs.append(fd)
                if len(fsyncs) == 2:
                    raise OSError('synthetic directory fsync failure')
                return real_fsync(fd)
            with patch.dict('os.environ', {'NEXTCLOUD_TALK_ALLOWED_USERS': 'alice'}), patch('adapter.os.fsync', side_effect=fsync):
                await instance._handle_talk_message({**_MESSAGE, 'id': 203, 'message': '/restart'}, 'room')
                await self.wait_for_background(instance)
            self.assertEqual(called, [])
            restored = self.make_adapter(tmp)
            restored._load_cursors()
            self.assertTrue(restored._is_acknowledged('room', 202))
            self.assertFalse(restored._is_acknowledged('room', 203))

    async def test_core_slash_denial_does_not_preconsume_restart(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            runner = GatewayRunner.__new__(GatewayRunner)
            runner.config = GatewayConfig(platforms={instance.platform: instance.config})
            instance.config.extra['allow_admin_from'] = ['admin']
            instance.gateway_runner = runner
            observed = []
            async def handler(event):
                observed.append(instance._is_acknowledged('room', 205))
                self.assertIsNotNone(runner._check_slash_access(event.source, 'restart'))
                return None
            instance.set_message_handler(handler)
            with patch.dict('os.environ', {'NEXTCLOUD_TALK_ALLOWED_USERS': 'alice'}):
                await instance._handle_talk_message({**_MESSAGE, 'id': 205, 'message': '/restart'}, 'room')
                await self.wait_for_background(instance)
            self.assertEqual(observed, [False])

    async def test_core_sender_denial_does_not_preconsume_restart(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            runner = GatewayRunner.__new__(GatewayRunner)
            runner.config = GatewayConfig()
            instance.gateway_runner = runner
            observed = []
            async def handler(event):
                observed.append(instance._is_acknowledged('room', 206))
                self.assertFalse(runner._is_user_authorized_for_source(event.source))
                return None
            instance.set_message_handler(handler)
            with patch.dict('os.environ', {'NEXTCLOUD_TALK_ALLOWED_USERS': 'different', 'NEXTCLOUD_TALK_ALLOW_ALL_USERS': 'false'}):
                await instance._handle_talk_message({**_MESSAGE, 'id': 206, 'message': '/restart'}, 'room')
                await self.wait_for_background(instance)
            self.assertEqual(observed, [False])

    async def test_restart_crash_after_durable_consumption_is_not_retried(self):
        with tempfile.TemporaryDirectory() as tmp:
            calls = []
            runner = GatewayRunner.__new__(GatewayRunner)
            runner.config = GatewayConfig()
            async def handler(event):
                calls.append(event.message_id)
                raise RuntimeError('crash after restart side effect')
            with patch.dict('os.environ', {'NEXTCLOUD_TALK_ALLOWED_USERS': 'alice'}):
                for _ in range(2):
                    instance = self.make_adapter(tmp)
                    instance._load_cursors()
                    instance.gateway_runner = runner
                    instance.set_message_handler(handler)
                    await instance._handle_talk_message({**_MESSAGE, 'id': 208, 'message': '/restart'}, 'room')
                    await self.wait_for_background(instance)
            self.assertEqual(calls, ['208'])

    async def test_ordinary_command_and_dm_prose_keep_completion_ack(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            instance.require_mention = True
            seen = []
            async def handler(event):
                seen.append((event.text, instance._is_acknowledged('room', int(event.message_id))))
                return None
            instance.set_message_handler(handler)
            for i, text in enumerate(['/help', 'dm prose'], 210):
                await instance._handle_talk_message({**_MESSAGE, 'id': i, 'message': text}, 'room')
                await self.wait_for_background(instance)
                self.assertTrue(instance._is_acknowledged('room', i))
            self.assertEqual(seen, [('/help', False), ('dm prose', False)])

    async def test_plain_slash_passes_mention_gate_without_history_enrichment(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            instance.require_mention = True
            instance._room_types['room'] = 2
            seen = []
            async def handler(event):
                seen.append(event.text)
                return None
            instance.set_message_handler(handler)
            for i, text in enumerate(['/help', '/status', 'unaddressed prose'], 101):
                await instance._handle_talk_message({**_MESSAGE, 'id': i, 'message': text}, 'room')
                await self.wait_for_background(instance)
            self.assertEqual(seen, ['/help', '/status'])
