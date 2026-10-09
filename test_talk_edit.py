"""Talk message editing and deletion (0.1.16)."""

import asyncio
import tempfile
import unittest
from unittest.mock import patch, AsyncMock

import adapter
from gateway.platforms.base import BasePlatformAdapter
from test_threads import _Base, _msg


class _EditClient:
    """Fake Talk client recording sends, edits and deletions."""

    def __init__(self, *, edit_error=None, delete_error=None):
        self.sent = []
        self.edits = []
        self.deletes = []
        self.edit_error = edit_error
        self.delete_error = delete_error
        self._ids = iter(range(9000, 10000))

    async def send_message(self, room, message, **kwargs):
        self.sent.append((room, message, kwargs))
        return {"id": next(self._ids)}

    async def edit_message(self, room, message_id, message):
        self.edits.append((room, message_id, message))
        if self.edit_error is not None:
            raise self.edit_error
        return {"id": message_id}

    async def delete_message(self, room, message_id):
        self.deletes.append((room, message_id))
        if self.delete_error is not None:
            raise self.delete_error
        return {}


class EditMessageTests(_Base):
    async def test_adapter_overrides_edit_and_delete(self):
        # Hermes enables progress bubbles, streaming edits and cleanup only for
        # adapters that override these two methods.
        self.assertIsNot(adapter.NextcloudTalkAdapter.edit_message, BasePlatformAdapter.edit_message)
        self.assertIsNot(adapter.NextcloudTalkAdapter.delete_message, BasePlatformAdapter.delete_message)

    async def test_edit_own_message(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            client = instance._client = _EditClient()
            sent = await instance.send("room", "working…")
            result = await instance.edit_message("room", sent.message_id, "**done** ![x](https://e.org/a.png)")
            self.assertTrue(result.success)
            self.assertEqual(result.message_id, sent.message_id)
            self.assertEqual(client.edits, [("room", int(sent.message_id), "**done** https://e.org/a.png")])

    async def test_edit_accepts_finalize_and_metadata(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            instance._client = _EditClient()
            sent = await instance.send("room", "a")
            result = await instance.edit_message(
                chat_id="room", message_id=sent.message_id, content="b",
                finalize=True, metadata={"thread_id": "5"},
            )
            self.assertTrue(result.success)

    async def test_foreign_or_unknown_message_is_never_touched(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            client = instance._client = _EditClient()
            sent = await instance.send("room", "mine")
            cases = [("room", "123"), ("room", "abc"), ("room", None), ("other", sent.message_id),
                     ("", sent.message_id), ("room", "-1"), ("room", "9" * 30)]
            for chat_id, message_id in cases:
                with self.subTest(chat_id=chat_id, message_id=message_id):
                    edit = await instance.edit_message(chat_id, message_id, "x")
                    self.assertFalse(edit.success)
                    self.assertFalse(edit.retryable)
                    self.assertFalse(await instance.delete_message(chat_id, message_id))
            self.assertEqual(client.edits, [])
            self.assertEqual(client.deletes, [])

    async def test_continuation_chunks_are_editable(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            instance.max_message_length = 128
            instance._client = _EditClient()
            sent = await instance.send("room", " ".join("word%03d" % i for i in range(60)))
            self.assertTrue(sent.continuation_message_ids)
            for message_id in (*sent.continuation_message_ids, sent.message_id):
                self.assertTrue((await instance.edit_message("room", message_id, "x")).success)

    async def test_refusals_are_final_and_unknown_outcome_uses_timeout_wording(self):
        cases = (
            (adapter.NextcloudTalkAPIError("HTTP 400", status_code=400), "status 400"),
            (adapter.NextcloudTalkAPIError("HTTP 403", status_code=403), "status 403"),
            (adapter.NextcloudTalkAPIError("net", category="network"), "timed out"),
            (RuntimeError("boom"), "operation failed"),
        )
        for error, wording in cases:
            with self.subTest(error=error), tempfile.TemporaryDirectory() as tmp:
                instance = self.make_adapter(tmp)
                instance._client = _EditClient(edit_error=error)
                sent = await instance.send("room", "a")
                result = await instance.edit_message("room", sent.message_id, "b")
                self.assertFalse(result.success)
                self.assertIn(wording, result.error)
                self.assertEqual(result.retryable, error.category == "network"
                                 if isinstance(error, adapter.NextcloudTalkAPIError) else False)

    async def test_configured_length_is_visible_to_core(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            instance.max_message_length = 1000
            self.assertEqual(instance.max_message_length_for_chat("room"), 1000)

    async def test_empty_or_oversized_edit_is_refused_locally(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            client = instance._client = _EditClient()
            instance.max_message_length = 128
            sent = await instance.send("room", "a")
            self.assertFalse((await instance.edit_message("room", sent.message_id, "  ")).success)
            self.assertFalse((await instance.edit_message("room", sent.message_id, "x" * 129)).success)
            self.assertEqual(client.edits, [])

    async def test_not_connected(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            instance._client = None
            self.assertFalse((await instance.edit_message("room", "1", "x")).success)
            self.assertFalse(await instance.delete_message("room", "1"))

    async def test_sent_id_registry_is_bounded(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(adapter, "_SENT_MESSAGE_RETENTION", 3):
            instance = self.make_adapter(tmp)
            instance._client = _EditClient()
            ids = [(await instance.send("room", str(i))).message_id for i in range(5)]
            self.assertEqual(len(instance._sent_message_ids), 3)
            self.assertFalse((await instance.edit_message("room", ids[0], "x")).success)
            self.assertTrue((await instance.edit_message("room", ids[-1], "x")).success)


class DeleteMessageTests(_Base):
    async def test_delete_own_message_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            client = instance._client = _EditClient()
            sent = await instance.send("room", "temporary")
            self.assertTrue(await instance.delete_message("room", sent.message_id))
            self.assertEqual(client.deletes, [("room", int(sent.message_id))])
            # Forgotten after deletion: a second delete or a late edit is refused locally.
            self.assertFalse(await instance.delete_message("room", sent.message_id))
            self.assertFalse((await instance.edit_message("room", sent.message_id, "x")).success)
            self.assertEqual(len(client.deletes), 1)

    async def test_already_gone_counts_as_deleted_other_errors_do_not(self):
        cases = (
            (adapter.NextcloudTalkAPIError("HTTP 404", status_code=404), True),
            (adapter.NextcloudTalkAPIError("HTTP 400", status_code=400), False),
            (adapter.NextcloudTalkAPIError("net", category="network"), False),
            (RuntimeError("boom"), False),
        )
        for error, expected in cases:
            with self.subTest(error=error), tempfile.TemporaryDirectory() as tmp:
                instance = self.make_adapter(tmp)
                instance._client = _EditClient(delete_error=error)
                sent = await instance.send("room", "a")
                self.assertIs(await instance.delete_message("room", sent.message_id), expected)


class ClientEndpointTests(unittest.TestCase):
    def test_edit_and_delete_endpoints(self):
        client = adapter.NextcloudTalkClient("https://cloud.example", "bot", "secret")
        calls = []

        def fake_request(method, room, **kwargs):
            calls.append((method, client._url(room, kwargs.get("query"), kwargs.get("suffix", "")),
                          kwargs.get("data")))
            return {}

        client._request = fake_request
        asyncio.run(client.edit_message("ro/om", 42, "new text"))
        asyncio.run(client.delete_message("room", 43))
        self.assertEqual(calls, [
            ("PUT", "https://cloud.example/ocs/v2.php/apps/spreed/api/v1/chat/ro%2Fom/42",
             {"message": "new text"}),
            ("DELETE", "https://cloud.example/ocs/v2.php/apps/spreed/api/v1/chat/room/43", None),
        ])

    def test_message_id_must_be_an_integer(self):
        client = adapter.NextcloudTalkClient("https://cloud.example", "bot", "secret")
        client._request = lambda *a, **k: {}
        with self.assertRaises((TypeError, ValueError)):
            asyncio.run(client.edit_message("room", "1/../2", "x"))


class InboundEditEventsTests(_Base):
    async def test_profile_cursor_allows_only_one_writer(self):
        with tempfile.TemporaryDirectory() as tmp:
            first = self.make_adapter(tmp)
            second = self.make_adapter(tmp)
            first._claim_cursor_writer()
            try:
                with self.assertRaises(adapter.CursorCacheError):
                    second._claim_cursor_writer()
                self.assertIsNone(getattr(second, "_cursor_writer_fd", None))
            finally:
                first._release_cursor_writer()
            second._claim_cursor_writer()
            second._release_cursor_writer()

    async def test_second_live_gateway_fails_connection_and_release_on_disconnect(self):
        with tempfile.TemporaryDirectory() as tmp:
            first = self.make_adapter(tmp)
            second = self.make_adapter(tmp)
            third = self.make_adapter(tmp)
            async def idle_poll(_instance):
                await asyncio.Event().wait()
            with patch.object(adapter.NextcloudTalkAdapter, "_refresh_discovered_rooms", new_callable=AsyncMock), \
                 patch.object(adapter.NextcloudTalkAdapter, "_poll_loop", idle_poll):
                self.assertTrue(await first.connect())
                try:
                    self.assertFalse(await second.connect())
                finally:
                    await first.disconnect()
                self.assertTrue(await third.connect())
                await third.disconnect()

    async def test_repeat_connect_keeps_one_poller_until_disconnect(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            successor = self.make_adapter(tmp)
            started = []
            async def idle_poll(_instance):
                started.append(asyncio.current_task())
                await asyncio.Event().wait()
            with patch.object(adapter.NextcloudTalkAdapter, "_refresh_discovered_rooms", new_callable=AsyncMock), \
                 patch.object(adapter.NextcloudTalkAdapter, "_poll_loop", idle_poll):
                self.assertTrue(await instance.connect())
                await asyncio.sleep(0)
                self.assertTrue(await instance.connect())
                await asyncio.sleep(0)
                self.assertEqual(len(started), 1)
                await instance.disconnect()
                self.assertTrue(started[0].done())
                self.assertTrue(await successor.connect())
                await successor.disconnect()

    async def test_cancelled_connect_releases_writer_lease(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            successor = self.make_adapter(tmp)
            entered = asyncio.Event()
            async def stalled_discovery(*_args, **_kwargs):
                entered.set()
                await asyncio.Event().wait()
            with patch.object(adapter.NextcloudTalkAdapter, "_refresh_discovered_rooms", stalled_discovery):
                task = asyncio.create_task(instance.connect())
                await entered.wait()
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
            successor._claim_cursor_writer()
            successor._release_cursor_writer()

    async def test_connect_failure_after_topic_start_cleans_all_tasks(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp, thread_topics=[{"room": "room", "title": "Research"}])
            successor = self.make_adapter(tmp)
            tasks = []
            async def idle(_instance):
                tasks.append(asyncio.current_task())
                await asyncio.Event().wait()
            with patch.object(adapter.NextcloudTalkAdapter, "_refresh_discovered_rooms", new_callable=AsyncMock), \
                 patch.object(adapter.NextcloudTalkAdapter, "_poll_loop", idle), \
                 patch.object(adapter.NextcloudTalkAdapter, "_ensure_thread_topics_bounded", idle), \
                 patch.object(instance, "_mark_connected", side_effect=RuntimeError("probe")):
                self.assertFalse(await instance.connect())
            self.assertFalse(instance._running)
            self.assertIsNone(instance._poll_task)
            self.assertIsNone(instance._topics_task)
            self.assertTrue(all(task.done() for task in tasks))
            successor._claim_cursor_writer()
            successor._release_cursor_writer()

    async def test_own_edit_and_delete_system_messages_are_skipped_without_exact_acks(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            instance._client = _EditClient()
            seen = []

            async def handler(event):
                seen.append(event)

            instance.set_message_handler(handler)
            instance._ensure_ack_runtime()
            for message_id, kind in ((801, "message_edited"), (802, "message_deleted")):
                msg = _msg(message_id, text="You edited a message", actorId="bot",
                           actorDisplayName="bot", systemMessage=kind, messageType="system")
                await instance._handle_talk_message(msg, "room")
                self.assertNotIn(message_id, instance._ack_rooms.get("room", {}).get("successful", set()))
            await self.wait_for_background(instance)
            self.assertEqual(seen, [])

    async def test_unresolved_observation_capacity_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            instance._ensure_ack_runtime()
            instance.ack_retention_count = 32
            instance._ack_rooms["room"] = {
                "floor": 1000, "successful": set(), "initialized": True,
                "last_seen": 1000, "active": True,
            }
            with self.assertRaises(adapter.NextcloudTalkAPIError):
                instance._observe_room_ids("room", range(1001, 2026))
            self.assertIn(1001, instance._room_seen_ids["room"])
            self.assertEqual(len(instance._room_seen_ids["room"]), 1024)
            instance._settle_room_floor("room")
            self.assertEqual(instance._ack_rooms["room"]["floor"], 1000)

    async def test_malformed_system_message_does_not_stall_poll_page(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            instance._ensure_ack_runtime()
            instance._ack_rooms["room"] = {
                "floor": 1000, "successful": set(), "initialized": True,
                "last_seen": 1000, "active": True,
            }
            history = [
                _msg(1001, text="bad system flag", actorId="bot", systemMessage=[]),
                _msg(1002, text="valid user", actorId="alice"),
            ]
            class PollClient(_EditClient):
                async def get_messages(self, room, *, last_known_id, look_into_future,
                                       timeout, limit=None):
                    return [m for m in history if m["id"] > (last_known_id or 0)][:limit or 200]
            instance._client = PollClient()
            seen = []
            async def handler(event):
                seen.append(event.message_id)
            instance.set_message_handler(handler)
            await instance._poll_room("room")
            await self.wait_for_background(instance)
            self.assertEqual(seen, ["1002"])

    async def test_exact_ack_retention_never_crosses_unresolved_user(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            instance._ensure_ack_runtime()
            instance.ack_retention_count = 32
            instance.ack_overlap_ids = 0
            instance.floor_settle_seconds = 60
            instance._ack_rooms["room"] = {
                "floor": 1000, "successful": set(), "initialized": True,
                "last_seen": 1000, "active": True,
            }
            instance._observe_room_ids("room", range(1001, 1100))
            for mid in range(1052, 1084):
                instance._commit_cursor("room", mid)
            with self.assertRaises(adapter.CursorCacheError):
                instance._commit_cursor("room", 1084)
            self.assertFalse(instance._is_acknowledged("room", 1051))
            loaded = self.make_adapter(tmp)
            loaded._load_cursors()
            self.assertFalse(loaded._is_acknowledged("room", 1051))
            self.assertEqual(len(loaded._ack_rooms["room"]["successful"]), 32)

    async def test_full_ledger_allows_only_earliest_barrier_to_recover(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            instance._ensure_ack_runtime()
            instance.ack_retention_count = 32
            instance.ack_overlap_ids = 0
            instance.floor_settle_seconds = 0
            instance._ack_rooms["room"] = {
                "floor": 1000, "successful": set(), "initialized": True,
                "last_seen": 1000, "active": True,
            }
            instance._observe_room_ids("room", range(1001, 1035))
            for mid in range(1002, 1034):
                instance._commit_cursor("room", mid)
            self.assertFalse(instance._is_recovery_barrier("room", 1034, instance._ack_rooms["room"]))
            with self.assertRaises(adapter.CursorCacheError):
                instance._commit_cursor("room", 1034)
            self.assertTrue(instance._is_recovery_barrier("room", 1001, instance._ack_rooms["room"]))
            instance._commit_cursor("room", 1001)
            self.assertEqual(instance._ack_rooms["room"]["floor"], 1033)
            self.assertEqual(len(instance._ack_rooms["room"]["successful"]), 0)
            instance._commit_cursor("room", 1034)
            self.assertTrue(instance._is_acknowledged("room", 1034))


class RealStreamConsumerTests(_Base):
    async def test_streamed_reply_is_one_message_edited_in_place(self):
        try:
            from gateway.stream_consumer import GatewayStreamConsumer, StreamConsumerConfig
            consumer_cls, config = GatewayStreamConsumer, StreamConsumerConfig(
                edit_interval=0.01, buffer_threshold=1, cursor=""
            )
        except Exception as exc:  # older runtimes: different consumer API
            self.skipTest(f"stream consumer unavailable: {type(exc).__name__}")
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            client = instance._client = _EditClient()
            try:
                consumer = consumer_cls(adapter=instance, chat_id="room", config=config)
            except TypeError as exc:
                self.skipTest(f"stream consumer signature differs: {exc}")
            task = asyncio.create_task(consumer.run())
            consumer.on_delta("Hello")
            await asyncio.sleep(0.1)
            consumer.on_delta(" world, this is a streamed answer")
            await asyncio.sleep(0.1)
            consumer.finish()
            await asyncio.wait_for(task, 5)
            self.assertEqual(len(client.sent), 1)
            self.assertTrue(client.edits)
            self.assertEqual(client.edits[-1][2], "Hello world, this is a streamed answer")
            self.assertTrue(all(edit[1] == client.edits[0][1] for edit in client.edits))
            # Talk edits do not notify: the initial streamed POST must notify.
            self.assertEqual(len([s for s in client.sent if not s[2].get("silent")]), 1)

    async def test_interim_preview_remains_silent(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            client = instance._client = _EditClient()
            await instance.send("room", "draft", metadata={"expect_edits": True, "_interim_send": True})
            self.assertTrue(client.sent[0][2].get("silent"))

    async def test_bot_edit_events_do_not_exhaust_exact_ack_retention(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            instance.ack_retention_count = 32
            instance.floor_settle_seconds = 0
            instance._ensure_ack_runtime()
            instance._ack_rooms["room"] = {
                "floor": 1000, "successful": set(), "initialized": True,
                "last_seen": 1000, "active": True,
            }
            history = [
                _msg(mid, text="You edited a message", actorId="bot",
                     systemMessage="message_edited", messageType="system")
                for mid in range(1001, 2302)
            ]
            history[50] = _msg(1051, text="retryable user request", actorId="alice")
            class PollClient(_EditClient):
                async def get_messages(self, room, *, last_known_id, look_into_future,
                                       timeout, limit=None):
                    return [m for m in history if m["id"] > (last_known_id or 0)][:limit or 200]
            instance._client = PollClient()
            release = asyncio.Event()
            async def handler(_event):
                await release.wait()
                raise RuntimeError("retryable failure")
            instance.set_message_handler(handler)
            writes = []
            original = instance._persist_cursors
            def counted():
                writes.append(1)
                original()
            instance._persist_cursors = counted
            await instance._poll_room("room")
            release.set()
            await self.wait_for_background(instance)
            self.assertFalse(instance._is_acknowledged("room", 1051))
            self.assertIn(1051, instance._room_seen_ids["room"])
            self.assertLess(len(instance._room_seen_ids["room"]), 100)
            self.assertLess(len(writes), 4)
            self.assertEqual(len(instance._ack_rooms["room"]["successful"]), 0)
            instance._settle_room_floor("room")
            self.assertFalse(instance._is_acknowledged("room", 1051))
            self.assertLessEqual(instance._ack_rooms["room"]["floor"], 1050)
            self.assertLess(len(writes), 4)
            # No durable IDs were needed; a new instance will re-ignore them.
            newer = self.make_adapter(tmp)
            newer._load_cursors()
            self.assertFalse(newer._is_acknowledged("room", 1051))


if __name__ == "__main__":
    unittest.main()
