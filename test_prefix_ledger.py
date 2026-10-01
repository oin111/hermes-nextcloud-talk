"""Bounded prefix-ledger regressions over the real Hermes poll/dispatch path.

Each test pins a failure mode the unreleased exact-ACK build had on released
Hermes: unbounded ACK state muting a room, a first-seen room replaying its
whole history, and legacy cursor files. Hermes core is used unmodified.
"""

import asyncio
import json
import tempfile
import types
import unittest
from pathlib import Path

import adapter
from gateway.platforms.base import SendResult
import test_lifecycle_real as _real

_MESSAGE = _real._MESSAGE


class BoundedPrefixLedgerTests(unittest.IsolatedAsyncioTestCase):
    make_adapter = _real.RealHermesLifecycleTests.make_adapter
    wait_for_background = _real.RealHermesLifecycleTests.wait_for_background

    def _room(self, tmp, history, *, backlog=5):
        instance = self.make_adapter(tmp)
        instance.floor_settle_seconds = 0.0
        instance.initial_backlog_limit = backlog
        instance.max_poll_batch = 50
        instance.ack_overlap_ids = 49  # keep the legacy overlap inside one page
        instance.poll_timeout = 0
        runs = []

        async def handler(event):
            runs.append(int(event.message_id))
            return "ok"

        instance.set_message_handler(handler)
        instance.send = lambda *_a, **_k: asyncio.sleep(0, result=SendResult(success=True, message_id="r"))

        async def get_messages(_room, *, last_known_id=None, look_into_future=False, limit=None, **_kw):
            ids = sorted(m["id"] for m in history)
            if look_into_future:
                chosen = [i for i in ids if i > (last_known_id or 0)][: limit or 200]
            else:
                older = [i for i in ids if last_known_id is None or i < last_known_id]
                chosen = older[-(limit or 200):]
            by_id = {m["id"]: m for m in history}
            return [by_id[i] for i in chosen]

        instance._client = types.SimpleNamespace(get_messages=get_messages)
        return instance, runs

    async def _drain(self, instance, cycles):
        for _ in range(cycles):
            await instance._poll_room("room")
            await self.wait_for_background(instance)

    async def test_first_seen_room_runs_only_the_backlog_window(self):
        with tempfile.TemporaryDirectory() as tmp:
            history = [{**_MESSAGE, "id": 100 + i} for i in range(20)]
            instance, runs = self._room(tmp, history, backlog=5)
            await instance._initialize_room("room")
            await self.wait_for_background(instance)
            await self._drain(instance, 5)
            self.assertEqual(runs, list(range(115, 120)))
            self.assertEqual(instance._ack_rooms["room"]["floor"], 119)

    async def test_backlog_zero_starts_at_latest(self):
        with tempfile.TemporaryDirectory() as tmp:
            history = [{**_MESSAGE, "id": 100 + i} for i in range(20)]
            instance, runs = self._room(tmp, history, backlog=0)
            await instance._initialize_room("room")
            await self._drain(instance, 3)
            self.assertEqual(runs, [])
            history.append({**_MESSAGE, "id": 500})
            await self._drain(instance, 2)
            self.assertEqual(runs, [500])

    async def test_room_never_stalls_and_state_stays_bounded(self):
        # 6000 messages from a non-allowlisted participant followed by one
        # allowed message. The old build refused every inbound after 4096.
        with tempfile.TemporaryDirectory() as tmp:
            history = [{**_MESSAGE, "id": 1}]
            instance, runs = self._room(tmp, history, backlog=1)
            await instance._initialize_room("room")
            await self.wait_for_background(instance)
            instance.allow_all = False
            instance.allowed_users = {"alice"}
            instance.group_allowed_users = {"alice"}
            history.extend({**_MESSAGE, "id": 10 + i, "actorId": "mallory",
                            "actorDisplayName": "Mallory"} for i in range(6000))
            history.append({**_MESSAGE, "id": 7000})
            await self._drain(instance, 200)
            self.assertIn(7000, runs)
            state = instance._ack_rooms["room"]
            self.assertEqual(state["floor"], 7000)
            self.assertEqual(state["successful"], set())
            saved = json.loads(instance._cursor_path.read_text())["rooms"]["room"]
            self.assertLess(len(saved["successful"]), 10)

    async def test_failing_turn_is_bounded_and_does_not_pin_the_floor(self):
        with tempfile.TemporaryDirectory() as tmp:
            history = [{**_MESSAGE, "id": 1}]
            instance, runs = self._room(tmp, history, backlog=1)
            await instance._initialize_room("room")
            await self.wait_for_background(instance)
            attempts = []

            async def failing(event):
                attempts.append(int(event.message_id))
                if event.message_id == "5":
                    raise RuntimeError("tool exploded")
                return "ok"

            instance.set_message_handler(failing)
            instance.max_dispatch_attempts = 3
            instance.retry_backoff_seconds = (0.0,)
            history.extend([{**_MESSAGE, "id": 5}, {**_MESSAGE, "id": 6}])
            await self._drain(instance, 12)
            self.assertEqual(attempts.count(5), 3)
            self.assertEqual(attempts.count(6), 1)
            self.assertEqual(instance._ack_rooms["room"]["floor"], 6)

    async def test_backed_off_id_does_not_starve_ids_beyond_one_cycle_of_pages(self):
        # One message keeps failing (floor pinned below it) while more history
        # than one poll cycle can scan arrives above it. The newest message
        # must still be reached, and the failing one is retried later.
        with tempfile.TemporaryDirectory() as tmp:
            history = [{**_MESSAGE, "id": 1}]
            instance, runs = self._room(tmp, history, backlog=1)
            instance.max_poll_batch = 10
            instance.ack_overlap_ids = 9
            await instance._initialize_room("room")
            await self.wait_for_background(instance)
            calls = []

            async def handler(event):
                calls.append(int(event.message_id))
                if event.message_id == "2":
                    raise RuntimeError("tool exploded")
                return "ok"

            instance.set_message_handler(handler)
            instance.retry_backoff_seconds = (3600.0,)
            instance.max_dispatch_attempts = 10
            instance.allow_all = False
            instance.allowed_users = {"alice"}
            instance.group_allowed_users = {"alice"}
            history.append({**_MESSAGE, "id": 2})
            history.extend({**_MESSAGE, "id": 10 + i, "actorId": "mallory",
                            "actorDisplayName": "Mallory"} for i in range(500))
            history.append({**_MESSAGE, "id": 9000})
            await self._drain(instance, 80)
            self.assertIn(9000, calls)
            self.assertEqual(calls.count(2), 1)
            self.assertFalse(instance._is_acknowledged("room", 2))
            self.assertEqual(instance._ack_rooms["room"]["floor"], 1)

    async def test_failed_reply_delivery_survives_handler_budget_then_is_bounded(self):
        # The agent answered but Talk refused the reply. A short outage must
        # not drop the message (it outlives the handler-failure budget), and
        # a reply Talk never accepts must not re-run the agent forever.
        with tempfile.TemporaryDirectory() as tmp:
            history = [{**_MESSAGE, "id": 1}]
            instance, runs = self._room(tmp, history, backlog=1)
            await instance._initialize_room("room")
            await self.wait_for_background(instance)
            instance.retry_backoff_seconds = (0.0,)
            instance.max_dispatch_attempts = 3
            instance.max_delivery_attempts = 6
            instance.send = lambda *_a, **_k: asyncio.sleep(
                0, result=SendResult(success=False, error="HTTP 503"))
            history.append({**_MESSAGE, "id": 5})
            await self._drain(instance, 4)
            self.assertEqual(runs.count(5), 4)
            self.assertFalse(instance._is_acknowledged("room", 5))
            instance.send = lambda *_a, **_k: asyncio.sleep(
                0, result=SendResult(success=True, message_id="r"))
            await self._drain(instance, 2)
            self.assertEqual(runs.count(5), 5)
            self.assertTrue(instance._is_acknowledged("room", 5))
            # Permanently refused reply: bounded by max_delivery_attempts.
            instance.send = lambda *_a, **_k: asyncio.sleep(
                0, result=SendResult(success=False, error="HTTP 403"))
            history.append({**_MESSAGE, "id": 7})
            await self._drain(instance, 12)
            self.assertEqual(runs.count(7), 6)
            self.assertTrue(instance._is_acknowledged("room", 7))
            self.assertEqual(instance._dispatch_attempts.get("room", {}), {})

    async def test_settle_window_is_measured_against_a_later_sweep(self):
        # A lower ID that appears while the adapter long-polls after a higher
        # one must be dispatched: the floor may not pass the higher ID until a
        # sweep from the floor started at least the settle window later.
        with tempfile.TemporaryDirectory() as tmp:
            history = [{**_MESSAGE, "id": 1}]
            instance, runs = self._room(tmp, history, backlog=1)
            clock = [1000.0]
            instance.floor_settle_seconds = 30.0
            real_clock = adapter._ledger_clock
            adapter._ledger_clock = lambda: clock[0]
            try:
                await instance._initialize_room("room")
                await self.wait_for_background(instance)
                history.append({**_MESSAGE, "id": 10})
                await self._drain(instance, 1)
                self.assertIn(10, runs)
                clock[0] += 5.0
                history.append({**_MESSAGE, "id": 9})  # late commit, lower ID
                clock[0] += 40.0  # a long poll elapses before the next sweep
                await self._drain(instance, 3)
                clock[0] += 40.0
                await self._drain(instance, 3)
            finally:
                adapter._ledger_clock = real_clock
            self.assertIn(9, runs)
            self.assertEqual(instance._ack_rooms["room"]["floor"], 10)

    async def test_floor_settles_under_steady_traffic(self):
        # One new message every cycle: sweeps must still complete, so the
        # floor settles by time and the exact set stays small (no reliance on
        # the retention bound).
        with tempfile.TemporaryDirectory() as tmp:
            history = [{**_MESSAGE, "id": 1}]
            instance, runs = self._room(tmp, history, backlog=1)
            instance.max_poll_batch = 20
            instance.ack_retention_count = 64
            clock = [1000.0]
            real_clock = adapter._ledger_clock
            adapter._ledger_clock = lambda: clock[0]
            instance.floor_settle_seconds = 30.0
            try:
                await instance._initialize_room("room")
                await self.wait_for_background(instance)
                instance.allow_all = False
                instance.allowed_users = {"alice"}
                instance.group_allowed_users = {"alice"}
                next_id = 10
                for cycle in range(100):
                    history.append({**_MESSAGE, "id": next_id, "actorId": "mallory",
                                    "actorDisplayName": "Mallory"})
                    if cycle == 5:
                        history.append({**_MESSAGE, "id": next_id + 1})
                        next_id += 1
                    next_id += 1
                    await self._drain(instance, 1)
                    clock[0] += 1.0
            finally:
                adapter._ledger_clock = real_clock
            state = instance._ack_rooms["room"]
            self.assertIn(16, runs)
            self.assertGreater(state["floor"], 60)
            self.assertLess(len(state["successful"]), 40)

    async def test_awaited_turn_timeout_counts_toward_give_up(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance, _runs = self._room(tmp, [{**_MESSAGE, "id": 1}], backlog=1)
            instance._ensure_ack_runtime()
            instance.processing_timeout = 0.05

            async def slow(event):
                await asyncio.sleep(5)
                return "late"

            instance.set_message_handler(slow)
            with self.assertRaises(RuntimeError) as caught:
                await instance._handle_talk_message(
                    {**_MESSAGE, "id": 1}, "room", await_completion=True)
            self.assertNotIsInstance(caught.exception, adapter._OutcomeAlreadyRecorded)
            await instance.disconnect()

    async def test_startup_backlog_timeout_is_bounded(self):
        # A backlog turn that never finishes within processing_timeout must be
        # counted and given up so the room initializes, not re-run forever.
        with tempfile.TemporaryDirectory() as tmp:
            history = [{**_MESSAGE, "id": 1}]
            instance, _runs = self._room(tmp, history, backlog=1)
            instance.processing_timeout = 0.05
            instance.max_dispatch_attempts = 3
            instance.retry_backoff_seconds = (0.0,)
            calls = []

            async def slow(event):
                calls.append(int(event.message_id))
                await asyncio.sleep(1.5)  # beyond processing_timeout + 1 s
                return "late"

            instance.set_message_handler(slow)
            for _ in range(6):
                if instance._is_room_initialized("room"):
                    break
                try:
                    await instance._initialize_room("room")
                except RuntimeError:
                    pass
                await asyncio.sleep(0.6)  # let the late turn finish (stale generation)
            self.assertEqual(calls, [1, 1, 1])
            self.assertTrue(instance._is_room_initialized("room"))
            self.assertTrue(instance._is_acknowledged("room", 1))
            await self.wait_for_background(instance)

    async def test_waited_failure_is_counted_once(self):
        # A turn awaited inline (busy room/clarify, startup backlog) reports
        # its failure through on_processing_complete; dispatch must not count
        # it again.
        with tempfile.TemporaryDirectory() as tmp:
            instance, _runs = self._room(tmp, [{**_MESSAGE, "id": 1}], backlog=1)
            instance._ensure_ack_runtime()
            instance.max_dispatch_attempts = 5
            instance.retry_backoff_seconds = (0.0,)

            async def handle(msg, room):
                instance._record_dispatch_failure(room, msg["id"])
                raise adapter._OutcomeAlreadyRecorded("failed")

            instance._handle_talk_message = handle
            await instance._poll_room("room")
            self.assertEqual(instance._dispatch_attempts["room"], {1: 1})

    async def test_pre_dispatch_error_is_bounded_and_does_not_block_newer(self):
        # E.g. an attachment whose download keeps failing with a retryable
        # error: it raises before Hermes dispatch. Newer messages still run
        # and the failing one is given up after bounded attempts.
        with tempfile.TemporaryDirectory() as tmp:
            history = [{**_MESSAGE, "id": 1}]
            instance, runs = self._room(tmp, history, backlog=1)
            await instance._initialize_room("room")
            await self.wait_for_background(instance)
            instance.retry_backoff_seconds = (0.0,)
            instance.max_dispatch_attempts = 3
            real_handle = instance._handle_talk_message
            downloads = []

            async def handle(msg, room):
                if msg["id"] == 5:
                    downloads.append(5)
                    raise adapter.AttachmentDownloadError("HTTP 403", category="network")
                return await real_handle(msg, room)

            instance._handle_talk_message = handle
            history.extend([{**_MESSAGE, "id": 5}, {**_MESSAGE, "id": 6}])
            await self._drain(instance, 8)
            self.assertIn(6, runs)
            self.assertEqual(len(downloads), 3)
            self.assertEqual(instance._ack_rooms["room"]["floor"], 6)

    async def test_oversized_poll_page_shrinks_instead_of_muting(self):
        # A few huge messages make a full page exceed max_json_bytes; the
        # adapter must page through them with smaller pages.
        with tempfile.TemporaryDirectory() as tmp:
            history = [{**_MESSAGE, "id": 1}]
            instance, runs = self._room(tmp, history, backlog=1)
            await instance._initialize_room("room")
            await self.wait_for_background(instance)
            inner = instance._client.get_messages
            huge = set(range(10, 22))

            async def get_messages(room, **kwargs):
                page = await inner(room, **kwargs)
                if sum(1 for m in page if m["id"] in huge) > 2:
                    raise adapter.NextcloudTalkAPIError(
                        "upstream response exceeds size limit", category="overflow")
                return page

            instance._client = types.SimpleNamespace(get_messages=get_messages)
            instance.allow_all = False
            instance.allowed_users = {"alice"}
            instance.group_allowed_users = {"alice"}
            history.extend({**_MESSAGE, "id": i, "actorId": "mallory",
                            "actorDisplayName": "Mallory"} for i in sorted(huge))
            history.append({**_MESSAGE, "id": 50})
            await self._drain(instance, 20)
            self.assertIn(50, runs)
            self.assertEqual(instance._ack_rooms["room"]["floor"], 50)

    async def test_process_history_backfill_cap_does_not_replay_older_history(self):
        with tempfile.TemporaryDirectory() as tmp:
            history = [{**_MESSAGE, "id": i} for i in range(1, 351)]
            instance, runs = self._room(tmp, history, backlog=None)
            instance.max_backlog_messages = 100
            # Realistic settle window: the floor cannot race ahead of the scan.
            instance.floor_settle_seconds = 3600.0
            await instance._initialize_room("room")
            await self.wait_for_background(instance)
            await self._drain(instance, 10)
            self.assertEqual(sorted(runs), list(range(251, 351)))
            self.assertEqual(instance._ack_rooms["room"]["floor"], 250)

    async def test_short_page_with_invisible_messages_does_not_end_sweep(self):
        # Talk applies the page limit before dropping invisible messages, so a
        # short page is not proof there is nothing newer.
        with tempfile.TemporaryDirectory() as tmp:
            history = [{**_MESSAGE, "id": 1}]
            instance, runs = self._room(tmp, history, backlog=1)
            instance.max_poll_batch = 10
            await instance._initialize_room("room")
            await self.wait_for_background(instance)
            inner = instance._client.get_messages
            invisible = set(range(10, 30, 2))

            async def get_messages(room, **kwargs):
                page = await inner(room, **kwargs)
                return [m for m in page if m["id"] not in invisible]

            instance._client = types.SimpleNamespace(get_messages=get_messages)
            instance.allow_all = False
            instance.allowed_users = {"alice"}
            instance.group_allowed_users = {"alice"}
            history.extend({**_MESSAGE, "id": i, "actorId": "mallory",
                            "actorDisplayName": "Mallory"} for i in range(10, 60))
            await self._drain(instance, 10)
            # Pin the floor below the invisible range with a backed-off ID so
            # every later sweep crosses the short pages again.
            instance._ack_rooms["room"]["floor"] = 1
            instance._ack_rooms["room"]["successful"] = {
                i for i in range(10, 60) if i not in invisible}
            instance._room_seen_ids["room"][5] = 0.0
            instance._retry_not_before["room"] = {5: float("inf")}
            history.append({**_MESSAGE, "id": 90})
            await self._drain(instance, 1)
            self.assertIn(90, runs)

    async def test_released_cursor_file_keeps_its_contract(self):
        # A 0.1.11 file (no floor_mode marker) is normalized with that
        # release's overlap rule once, then rewritten in prefix mode.
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            instance.ack_overlap_ids = 199
            instance._cursor_path.write_text(json.dumps({"version": 2, "rooms": {"room": {
                "floor": 23242, "successful": list(range(23400, 23442)) + [23441],
                "initialized": True, "last_seen": 9, "active": True,
            }}}))
            instance._load_cursors()
            state = instance._ack_rooms["room"]
            self.assertEqual(state["floor"], 23441 - 199)
            self.assertTrue(instance._is_room_initialized("room"))
            instance._persist_cursors()
            saved = json.loads(instance._cursor_path.read_text())
            self.assertEqual(saved["floor_mode"], "prefix")
            again = self.make_adapter(tmp)
            again.ack_overlap_ids = 199
            again._load_cursors()
            # Prefix-mode files are not pushed forward by the overlap rule.
            self.assertEqual(again._ack_rooms["room"]["floor"], 23441 - 199)

    async def test_restart_resumes_from_persisted_floor_without_replay(self):
        with tempfile.TemporaryDirectory() as tmp:
            history = [{**_MESSAGE, "id": 100 + i} for i in range(10)]
            instance, runs = self._room(tmp, history, backlog=3)
            await instance._initialize_room("room")
            await self.wait_for_background(instance)
            await self._drain(instance, 3)
            self.assertEqual(runs, [107, 108, 109])
            restarted, runs2 = self._room(tmp, history, backlog=3)
            restarted._load_cursors()
            history.append({**_MESSAGE, "id": 200})
            await self._drain(restarted, 3)
            self.assertEqual(runs2, [200])


if __name__ == "__main__":
    unittest.main()
