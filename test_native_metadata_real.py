"""Talk process-local lifecycle state must not cross native admission."""
import asyncio
import json
import tempfile
import types
import unittest
from dataclasses import replace

import adapter
from gateway.run import GatewayRunner
from gateway.session import build_session_key
try:
    from gateway.session_envelope import prepare_native, restore_native
    from hermes_state_runtime import RuntimeStoreError
except ImportError:  # older supported cores have no native-admission codec
    prepare_native = restore_native = None
    RuntimeStoreError = RuntimeError
import test_lifecycle_real as lifecycle


@unittest.skipUnless(prepare_native is not None, 'core has no native-admission codec')
class NativeMetadataTests(unittest.IsolatedAsyncioTestCase):
    make_adapter = lifecycle.RealHermesLifecycleTests.make_adapter
    wait_for_background = lifecycle.RealHermesLifecycleTests.wait_for_background

    def runner_for(self, instance):
        runner = object.__new__(GatewayRunner)
        runner.adapters = {instance.platform: instance}
        runner._profile_adapters = {}
        runner.config = types.SimpleNamespace(platforms={instance.platform: instance.config})
        runner.pairing_store = types.SimpleNamespace(is_approved=lambda *_args: False)
        runner.pairing_stores = {}
        runner.session_store = types.SimpleNamespace(_generate_session_key=build_session_key)
        return runner

    async def test_native_admission_keeps_exact_local_lifecycle_and_replay_ack(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            runner = self.runner_for(instance)
            captured, errors, payloads = [], [], []
            async def handler(event):
                captured.append(event)
                try:
                    payloads.append(await prepare_native(runner, event))
                except Exception as exc:
                    errors.append(exc)
                return None
            instance.set_message_handler(handler)
            await instance._handle_talk_message({**lifecycle._MESSAGE, 'id': 28482}, 'room', await_completion=True)
            await self.wait_for_background(instance)
            self.assertEqual(errors, [], 'installed native admission refused Talk input')
            self.assertEqual(len(payloads), 1)
            self.assertEqual(captured[0].metadata, {})
            json.dumps(payloads[0], allow_nan=False)
            replay = restore_native(payloads[0], runner)
            self.assertEqual(replay.metadata, {})
            self.assertIsNone(instance._event_generation_key(replay))
            self.assertTrue(instance._is_acknowledged('room', 28482))
            loaded = self.make_adapter(tmp)
            loaded._load_cursors()
            self.assertTrue(loaded._is_acknowledged('room', 28482))
            await instance._handle_talk_message({**lifecycle._MESSAGE, 'id': 28482}, 'room')
            self.assertEqual(len(captured), 1)

    async def test_projection_does_not_weaken_metadata_or_authorization(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            runner = self.runner_for(instance)
            captured = []
            async def handler(event):
                captured.append(event)
                return None
            instance.set_message_handler(handler)
            await instance._handle_talk_message({**lifecycle._MESSAGE, 'id': 28486}, 'room')
            await self.wait_for_background(instance)
            event = captured[0]
            await prepare_native(runner, event)
            with self.assertRaises(RuntimeStoreError) as bad_metadata:
                await prepare_native(runner, replace(event, metadata={'untrusted': True}))
            self.assertEqual(bad_metadata.exception.field, 'metadata')
            denied = replace(event, source=replace(event.source, user_id='mallory'))
            with self.assertRaises(RuntimeStoreError) as bad_actor:
                await prepare_native(runner, denied)
            self.assertEqual(bad_actor.exception.field, 'user_not_authorized')

    async def test_busy_queue_keeps_exact_waiter_while_native_projection_drops_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            runner = self.runner_for(instance)
            started, release = asyncio.Event(), asyncio.Event()
            captured = []
            async def handler(event):
                captured.append(event)
                await prepare_native(runner, event)
                if event.message_id == '28482':
                    started.set()
                    await release.wait()
                return None
            instance.processing_timeout = .02
            async def queue(event, session_key):
                instance._pending_messages[session_key] = event
                return True
            instance.set_busy_session_handler(queue)
            instance.set_message_handler(handler)
            # Model an already-owned core session without bypassing Talk's
            # room lock (ordinary second Talk inputs remain upstream).
            active = adapter.MessageEvent(
                text='active', message_id='28482',
                source=instance.build_source(chat_id='room', chat_type='dm', user_id='alice'))
            instance._start_session_processing(active, build_session_key(active.source))
            await asyncio.wait_for(started.wait(), 1)
            queued = asyncio.create_task(instance._handle_talk_message(
                {**lifecycle._MESSAGE, 'id': 28486}, 'room', await_completion=True))
            try:
                await asyncio.sleep(.06)
                event = next(e for k, e in instance._inflight_generations.items() if k[1] == 28486)
                state = instance._event_local_state(event)
                waiter = state['nextcloud_talk_completion_event']
                self.assertIsInstance(waiter, asyncio.Event)
                self.assertFalse(waiter.is_set())
                self.assertFalse(queued.done())
                self.assertFalse(instance._is_acknowledged('room', 28486))
                self.assertIsNone(instance._event_generation_key(replace(event)))
                payload = await prepare_native(runner, event)
                restored = restore_native(payload, runner)
                self.assertEqual(instance._event_local_state(restored), {})
                self.assertIs(instance._event_local_state(event), state)
                self.assertIs(instance._event_local_state(event)['nextcloud_talk_completion_event'], waiter)
            finally:
                release.set()
                await asyncio.wait_for(queued, 2)
                await self.wait_for_background(instance)
            self.assertTrue(waiter.is_set())
            self.assertTrue(instance._is_acknowledged('room', 28486))
            self.assertEqual([e.message_id for e in captured], ['28482', '28486'])


if __name__ == '__main__':
    unittest.main()
