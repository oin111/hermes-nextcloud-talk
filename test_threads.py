"""Talk threads as Hermes topics, exercised through the real Hermes core."""

import asyncio
import json
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

from gateway.config import PlatformConfig
from gateway.platform_registry import PlatformEntry, platform_registry
from gateway.platforms.base import SendResult

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


def _msg(message_id, text="hello", **extra):
    return {
        "id": message_id,
        "actorType": "users",
        "actorId": "alice",
        "actorDisplayName": "Alice",
        "message": text,
        "threadId": message_id,
        **extra,
    }


def _thread_msg(message_id, thread_id, title="Research", text="hello", **extra):
    return _msg(
        message_id, text, threadId=thread_id, isThread=True, threadTitle=title,
        parent={"id": thread_id, "message": "root", "actorId": "alice"}, **extra,
    )


class _Base(unittest.IsolatedAsyncioTestCase):
    def make_adapter(self, tmp, **extra):
        config = PlatformConfig(
            enabled=True,
            typing_indicator=False,
            extra={
                "url": "https://cloud.example",
                "username": "bot",
                "password": "secret",
                "room_tokens": "room",
                "auto_discover_rooms": False,
                **extra,
            },
        )
        instance = adapter.NextcloudTalkAdapter(config)
        instance._cursor_path = Path(tmp) / "cursors.json"
        instance._topics_path = Path(tmp) / "thread_topics.json"
        instance._room_types = {"room": 2}
        instance.allow_all = True
        return instance

    async def capture_event(self, instance, msg, room="room"):
        seen = []

        async def handler(event):
            seen.append(event)
            return None

        instance.set_message_handler(handler)
        async def capture(event):
            seen.append(event)
        with patch.object(instance, "handle_message", side_effect=capture):
            await instance._handle_talk_message(msg, room)
        self.assertEqual(len(seen), 1)
        return seen[0]

    async def wait_for_background(self, instance, timeout=2.0):
        deadline = asyncio.get_running_loop().time() + timeout
        while instance._background_tasks and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.01)
        self.assertFalse(instance._background_tasks)


class InboundThreadTests(_Base):
    async def test_real_thread_message_sets_thread_id_and_topic(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            event = await self.capture_event(instance, _thread_msg(105, 100, title="Research"))
            self.assertEqual(event.source.thread_id, "100")
            self.assertEqual(event.source.chat_topic, "Research")
            self.assertEqual(instance._event_local_state(event).get("nextcloud_talk_thread_id"), 100)
            # The implicit parent (thread root) is not a user-chosen quote.
            self.assertIsNone(getattr(event, "reply_to_message_id", None))

    async def test_explicit_reply_inside_thread_keeps_quote(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            msg = _thread_msg(106, 100)
            msg["parent"] = {"id": 103, "message": "earlier", "actorId": "alice"}
            event = await self.capture_event(instance, msg)
            self.assertEqual(event.source.thread_id, "100")
            self.assertEqual(event.reply_to_message_id, "103")

    async def test_plain_message_is_not_a_topic(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            event = await self.capture_event(instance, _msg(107))
            self.assertIsNone(event.source.thread_id)
            self.assertNotIn("nextcloud_talk_thread_id", event.metadata)

    async def test_malformed_thread_markers_fail_closed(self):
        cases = [
            {"isThread": "true", "threadId": 100},
            {"isThread": 1, "threadId": 100},
            {"isThread": True, "threadId": "100"},
            {"isThread": True, "threadId": True},
            {"isThread": True, "threadId": -5},
            {"isThread": True, "threadId": 0},
            {"isThread": True, "threadId": 999},  # newer than the message itself
            {"isThread": True},
        ]
        for index, fields in enumerate(cases):
            with self.subTest(fields=fields), tempfile.TemporaryDirectory() as tmp:
                instance = self.make_adapter(tmp)
                msg = _msg(200 + index)
                msg.pop("threadId")
                msg.update(fields)
                event = await self.capture_event(instance, msg)
                self.assertIsNone(event.source.thread_id)

    async def test_thread_title_is_sanitized_and_bounded(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            title = "Evil\u202e\ntitle\x00" + "x" * 500
            event = await self.capture_event(instance, _thread_msg(305, 300, title=title))
            topic = event.source.chat_topic
            self.assertLessEqual(len(topic), 200)
            self.assertNotIn("\u202e", topic)
            self.assertNotIn("\n", topic)
            self.assertNotIn("\x00", topic)

    async def test_threads_get_separate_sessions(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            main = await self.capture_event(instance, _msg(401))
            first = await self.capture_event(instance, _thread_msg(402, 400))
            second = await self.capture_event(instance, _thread_msg(411, 410, title="Other"))
            again = await self.capture_event(instance, _thread_msg(412, 400))
            keys = [instance._source_session_key(e.source) for e in (main, first, second, again)]
            self.assertEqual(len(set(keys[:3])), 3)
            self.assertEqual(keys[1], keys[3])


class OutboundThreadTests(_Base):
    def recording_client(self, calls, *, fail_thread_with=None):
        counter = iter(range(5000, 6000))

        async def send_message(room, message, **kwargs):
            calls.append((room, message, kwargs))
            if fail_thread_with is not None and kwargs.get("thread_id") is not None:
                raise fail_thread_with
            return {"id": next(counter)}

        return types.SimpleNamespace(send_message=send_message)

    async def test_send_maps_metadata_thread_id_to_talk(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            calls = []
            instance._client = self.recording_client(calls)
            result = await instance.send("room", "hi", metadata={"thread_id": "100"})
            self.assertTrue(result.success)
            self.assertEqual(calls[0][2].get("thread_id"), 100)

    async def test_send_without_thread_sends_no_thread(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            calls = []
            instance._client = self.recording_client(calls)
            for metadata in (None, {}, {"thread_id": None}, {"thread_id": "abc"},
                             {"thread_id": "0"}, {"thread_id": "-1"}, {"thread_id": True},
                             {"thread_id": "1" * 30}, {"thread_id": "１２"}):
                with self.subTest(metadata=metadata):
                    calls.clear()
                    await instance.send("room", "hi", metadata=metadata)
                    self.assertIsNone(calls[0][2].get("thread_id"))

    async def test_rejected_thread_falls_back_to_main_chat_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            calls = []
            instance._client = self.recording_client(
                calls, fail_thread_with=adapter.NextcloudTalkAPIError("HTTP 400", status_code=400)
            )
            result = await instance.send("room", "hi", metadata={"thread_id": "100"})
            self.assertTrue(result.success)
            self.assertEqual(len(calls), 2)
            self.assertIsNone(calls[1][2].get("thread_id"))

    async def test_ambiguous_thread_send_is_not_resent(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            calls = []
            instance._client = self.recording_client(
                calls,
                fail_thread_with=adapter.NextcloudTalkAPIError("network", category="network"),
            )
            result = await instance.send("room", "hi", metadata={"thread_id": "100"})
            self.assertFalse(result.success)
            self.assertEqual(len(calls), 1)

    async def test_client_posts_thread_id_and_title(self):
        client = adapter.NextcloudTalkClient("https://cloud.example", "bot", "secret")
        posted = []
        client._request = lambda method, room, **kw: posted.append(kw["data"]) or {"id": 1}
        await client.send_message("room", "hi", thread_id=100)
        await client.send_message("room", "root", thread_title="Topic")
        await client.send_message("room", "plain")
        self.assertEqual(posted[0].get("threadId"), "100")
        self.assertNotIn("threadTitle", posted[0])
        self.assertEqual(posted[1].get("threadTitle"), "Topic")
        self.assertNotIn("threadId", posted[1])
        self.assertNotIn("threadId", posted[2])
        self.assertNotIn("threadTitle", posted[2])

    async def test_file_share_carries_thread_in_talk_metadata(self):
        client = adapter.NextcloudTalkClient("https://cloud.example", "bot", "secret")
        forms = []
        client._ocs_post = lambda endpoint, form: forms.append(form) or {"id": "1"}
        client._share_file_to_talk_sync("Talk/a.txt", "room", caption="cap", thread_id=100)
        meta = json.loads(forms[0]["talkMetaData"])
        self.assertEqual(meta["threadId"], 100)
        self.assertEqual(meta["caption"], "cap")

    async def test_media_send_passes_thread_to_upload_and_followup(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            image = Path(tmp) / "a.png"
            image.write_bytes(b"png")
            uploads, calls = [], []
            client = self.recording_client(calls)

            async def upload(path, room, folder, **kwargs):
                uploads.append(kwargs)
                return "/f", {"id": "1"}, True, False  # native share, metadata not atomic

            client.upload_and_share_file = upload
            instance._client = client
            result = await instance.send_image_file(
                "room", str(image), caption="look", metadata={"thread_id": "100"}
            )
            self.assertTrue(result.success)
            self.assertEqual(uploads[0].get("thread_id"), 100)
            # The non-atomic caption follow-up lands in the same thread.
            self.assertEqual(calls[0][2].get("thread_id"), 100)

    async def test_document_url_send_stays_in_thread(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            calls = []
            instance._client = self.recording_client(calls)
            await instance.send_document(
                "room", "https://example.org/a.pdf", metadata={"thread_id": "100"}
            )
            self.assertEqual(calls[0][2].get("thread_id"), 100)

    async def test_real_base_reply_to_thread_message_lands_in_thread(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            calls = []
            instance._client = self.recording_client(calls)
            instance.set_message_handler(lambda _event: asyncio.sleep(0, result="answer"))
            await instance._handle_talk_message(_thread_msg(505, 500), "room")
            await self.wait_for_background(instance)
            sends = [c for c in calls if c[1] == "answer"]
            self.assertEqual(len(sends), 1)
            self.assertEqual(sends[0][2].get("thread_id"), 500)

    async def test_real_base_reply_to_main_message_has_no_thread(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            calls = []
            instance._client = self.recording_client(calls)
            instance.set_message_handler(lambda _event: asyncio.sleep(0, result="answer"))
            await instance._handle_talk_message(_msg(506), "room")
            await self.wait_for_background(instance)
            sends = [c for c in calls if c[1] == "answer"]
            self.assertEqual(len(sends), 1)
            self.assertIsNone(sends[0][2].get("thread_id"))


class _FakeTopicClient:
    def __init__(self, *, threads=None, recent=None, capability=True, get_error=None):
        self.threads = dict(threads or {})
        self.recent = list(recent or [])
        self.capability = capability
        self.get_error = get_error
        self.created = []
        self.next_id = 9000

    async def supports_threads(self):
        return self.capability

    async def get_thread(self, room, thread_id):
        if self.get_error is not None:
            raise self.get_error
        return self.threads.get((room, thread_id))

    async def list_recent_threads(self, room, limit):
        if not 1 <= limit <= 50:  # Talk 24 answers HTTP 400 above 50
            raise adapter.NextcloudTalkAPIError("HTTP 400", status_code=400)
        return [item for item in self.recent if item["thread"]["roomToken"] == room]

    async def send_message(self, room, message, **kwargs):
        self.next_id += 1
        self.created.append((room, message, kwargs))
        self.threads[(room, self.next_id)] = {"thread": {"id": self.next_id}}
        return {"id": self.next_id}


def _recent(room, thread_id, title, actor="bot"):
    return {
        "thread": {"id": thread_id, "roomToken": room, "title": title},
        "first": {"id": thread_id, "actorType": "users", "actorId": actor},
    }


class ThreadTopicTests(_Base):
    TOPICS = [
        {"room": "room", "title": "Research", "skill": "arxiv", "prompt": "Be brief."},
        {"room": "room", "title": "Ops", "skills": ["ssh-connection", "mikrotik-firewall"]},
    ]

    def test_topic_config_is_validated(self):
        topics = adapter._parse_thread_topics([
            {"room": "room", "title": "Ok", "skill": "arxiv"},
            {"room": "room", "title": "ok"},  # duplicate title (case-insensitive)
            {"room": "../x", "title": "Bad room"},
            {"room": "room"},  # no title
            "not-a-mapping",
            {"room": "room", "title": "Skills", "skills": ["good", "bad skill!", 5]},
            {"room": "room", "title": "Ctl\u202e\nx", "prompt": "a\x00b\nc"},
        ])
        self.assertEqual([t["title"] for t in topics], ["Ok", "Skills", "Ctl? x"])
        self.assertEqual(topics[0]["skills"], ["arxiv"])
        self.assertEqual(topics[1]["skills"], ["good"])
        self.assertEqual(topics[2]["prompt"], "ab\nc")
        self.assertEqual(adapter._parse_thread_topics("nope"), [])
        self.assertEqual(adapter._parse_thread_topics(None), [])

    async def test_topics_are_created_persisted_and_reused(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp, thread_topics=self.TOPICS)
            fake = _FakeTopicClient()
            instance._client = fake
            instance._load_topic_threads()
            await instance._ensure_thread_topics()
            self.assertEqual([c[2].get("thread_title") for c in fake.created], ["Research", "Ops"])
            self.assertTrue(all(c[2].get("thread_id") is None for c in fake.created))
            saved = json.loads((Path(tmp) / "thread_topics.json").read_text())
            self.assertEqual(saved["rooms"]["room"], {"research": 9001, "ops": 9002})

            again = self.make_adapter(tmp, thread_topics=self.TOPICS)
            again._client = fake
            again._load_topic_threads()
            await again._ensure_thread_topics()
            self.assertEqual(len(fake.created), 2)

    async def test_deleted_topic_thread_is_recreated(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp, thread_topics=self.TOPICS[:1])
            fake = _FakeTopicClient()
            instance._client = fake
            await instance._ensure_thread_topics()
            fake.threads.clear()  # thread deleted in Talk
            await instance._ensure_thread_topics()
            self.assertEqual(len(fake.created), 2)
            self.assertEqual(instance._topic_threads[("room", "research")], 9002)

    async def test_unknown_lookup_error_never_duplicates(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp, thread_topics=self.TOPICS[:1])
            fake = _FakeTopicClient()
            instance._client = fake
            await instance._ensure_thread_topics()
            fake.get_error = adapter.NextcloudTalkAPIError("network", category="network")
            await instance._ensure_thread_topics()
            self.assertEqual(len(fake.created), 1)
            self.assertEqual(instance._topic_threads[("room", "research")], 9001)

    async def test_existing_thread_adopted_only_when_bot_created_it(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp, thread_topics=self.TOPICS)
            fake = _FakeTopicClient(recent=[
                _recent("room", 700, "research", actor="BOT"),
                _recent("room", 701, "Ops", actor="mallory"),
            ])
            instance._client = fake
            await instance._ensure_thread_topics()
            self.assertEqual(instance._topic_threads[("room", "research")], 700)
            self.assertEqual([c[2].get("thread_title") for c in fake.created], ["Ops"])
            self.assertNotEqual(instance._topic_threads[("room", "ops")], 701)

    async def test_no_threads_capability_creates_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp, thread_topics=self.TOPICS)
            fake = _FakeTopicClient(capability=False)
            instance._client = fake
            await instance._ensure_thread_topics()
            self.assertEqual(fake.created, [])

    async def test_topic_in_unpolled_room_is_skipped(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(
                tmp, thread_topics=[{"room": "elsewhere", "title": "X"}]
            )
            fake = _FakeTopicClient()
            instance._client = fake
            await instance._ensure_thread_topics()
            self.assertEqual(fake.created, [])

    async def test_persisted_topics_ignored_for_other_identity(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "thread_topics.json"
            path.write_text(json.dumps({
                "version": 1, "identity": "https://other.example|bot",
                "rooms": {"room": {"research": 123}},
            }))
            instance = self.make_adapter(tmp, thread_topics=self.TOPICS[:1])
            instance._load_topic_threads()
            self.assertEqual(instance._topic_threads, {})

    async def test_topic_thread_binds_skill_and_prompt(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp, thread_topics=self.TOPICS)
            instance._topic_threads = {("room", "research"): 100, ("room", "ops"): 110}
            research = await self.capture_event(instance, _thread_msg(105, 100))
            self.assertEqual(research.auto_skill, ["arxiv"])
            self.assertEqual(research.channel_prompt, "Be brief.")
            ops = await self.capture_event(instance, _thread_msg(115, 110, title="Ops"))
            self.assertEqual(ops.auto_skill, ["ssh-connection", "mikrotik-firewall"])
            self.assertIsNone(ops.channel_prompt)
            other = await self.capture_event(instance, _thread_msg(125, 120, title="Research"))
            self.assertIsNone(other.auto_skill)  # same title, not the bot's topic thread
            main = await self.capture_event(instance, _msg(130))
            self.assertIsNone(main.auto_skill)

    async def test_connect_starts_and_disconnect_cancels_topic_task(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp, thread_topics=self.TOPICS[:1])
            started = asyncio.Event()

            async def slow_topics():
                started.set()
                await asyncio.sleep(3600)

            async def no_discovery(**_kwargs):
                return None

            async def idle_poll():
                await asyncio.sleep(3600)

            with patch.object(instance, "_ensure_thread_topics", slow_topics, create=True), \
                    patch.object(instance, "_refresh_discovered_rooms", no_discovery), \
                    patch.object(instance, "_poll_loop", idle_poll):
                self.assertTrue(await instance.connect())
                await asyncio.wait_for(started.wait(), 2)
                task = instance._topics_task
                await instance.disconnect()
                self.assertTrue(task.cancelled() or task.done())

    async def test_repeated_connect_reuses_topic_and_poll_tasks(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp, thread_topics=self.TOPICS[:1])

            async def slow():
                await asyncio.sleep(3600)

            async def no_discovery(**_kwargs):
                return None

            with patch.object(instance, "_ensure_thread_topics", slow), \
                    patch.object(instance, "_refresh_discovered_rooms", no_discovery), \
                    patch.object(instance, "_poll_loop", slow):
                self.assertTrue(await instance.connect())
                first = instance._topics_task
                first_poll = instance._poll_task
                await asyncio.sleep(0)
                self.assertTrue(await instance.connect())
                second = instance._topics_task
                try:
                    self.assertIs(first, second)
                    self.assertIs(instance._poll_task, first_poll)
                    self.assertFalse(first.done())
                finally:
                    await instance.disconnect()
                    self.assertTrue(first.done())
                    self.assertTrue(first_poll.done())

    async def test_each_topic_is_persisted_before_a_later_timeout(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp, thread_topics=self.TOPICS)
            fake = _FakeTopicClient()
            original = fake.send_message

            async def hang_on_second(room, message, **kwargs):
                if fake.created:
                    await asyncio.sleep(3600)
                return await original(room, message, **kwargs)

            fake.send_message = hang_on_second
            instance._client = fake
            with patch.object(adapter, "_THREAD_TOPICS_TIMEOUT", 0.2):
                await instance._ensure_thread_topics_bounded()
            saved = json.loads((Path(tmp) / "thread_topics.json").read_text())
            self.assertEqual(saved["rooms"]["room"], {"research": 9001})

    async def test_lookup_404_keeps_binding_when_room_unreachable(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "thread_topics.json"
            instance = self.make_adapter(tmp, thread_topics=self.TOPICS[:1])
            instance._topic_threads = {("room", "research"): 9001}
            instance._persist_topic_threads()
            fake = _FakeTopicClient()  # get_thread -> None (404)

            async def room_gone(room, limit):
                raise adapter.NextcloudTalkAPIError("HTTP 404", status_code=404)

            fake.list_recent_threads = room_gone
            instance._client = fake
            await instance._ensure_thread_topics()
            self.assertEqual(fake.created, [])
            self.assertEqual(instance._topic_threads, {("room", "research"): 9001})
            saved = json.loads(path.read_text())
            self.assertEqual(saved["rooms"]["room"], {"research": 9001})

    def test_topic_room_accepts_full_length_token(self):
        token = "a" * adapter._MAX_ROOM_TOKEN_LENGTH
        topics = adapter._parse_thread_topics([
            {"room": token, "title": "Long"},
            {"room": token + "a", "title": "Too long"},
        ])
        self.assertEqual([t["title"] for t in topics], ["Long"])


class OutboundThreadCoverageTests(_Base):
    """Every outbound media path keeps the thread; only HTTP 400 falls back."""

    def client(self, calls, *, fail=None, fail_on_call=None):
        counter = iter(range(5000, 6000))
        count = {"n": 0}

        async def send_message(room, message, **kwargs):
            count["n"] += 1
            calls.append((room, message, kwargs))
            if fail is not None and kwargs.get("thread_id") is not None and (
                fail_on_call is None or count["n"] == fail_on_call
            ):
                raise fail
            return {"id": next(counter)}

        return types.SimpleNamespace(send_message=send_message)

    def with_upload(self, instance, client, uploads, result=("/f", {"id": "1"}, True, True)):
        async def upload(path, room, folder, **kwargs):
            uploads.append(kwargs)
            return result

        client.upload_and_share_file = upload
        instance._client = client

    async def test_image_url_and_inherited_animation_stay_in_thread(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            calls = []
            instance._client = self.client(calls)
            md = {"thread_id": "100"}
            self.assertTrue((await instance.send_image(
                "room", "https://x.example/a.png", caption="c", metadata=md)).success)
            self.assertTrue((await instance.send_animation(
                "room", "https://x.example/a.gif", metadata=md)).success)
            self.assertEqual([c[2].get("thread_id") for c in calls], [100, 100])

    async def test_multiple_images_stay_in_thread(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            image = Path(tmp) / "a.png"
            image.write_bytes(b"x")
            calls, uploads = [], []
            self.with_upload(instance, self.client(calls), uploads)
            await instance.send_multiple_images(
                "room",
                [("https://x.example/a.png", "alt"), ("https://x.example/b.gif", ""),
                 (f"file://{image}", "")],
                metadata={"thread_id": "100"},
            )
            self.assertEqual([c[2].get("thread_id") for c in calls], [100, 100])
            self.assertEqual([u.get("thread_id") for u in uploads], [100])

    async def test_voice_video_and_local_documents_stay_in_thread(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            media = Path(tmp) / "a.ogg"
            media.write_bytes(b"x")
            calls, uploads = [], []
            self.with_upload(instance, self.client(calls), uploads)
            md = {"thread_id": "100"}
            await instance.send_voice("room", str(media), metadata=md, is_voice=True)
            await instance.send_video("room", str(media), metadata=md)
            await instance.send_document("room", file_path=str(media), metadata=md)
            await instance.send_document("room", f"file://{media}", metadata=md)
            self.assertEqual([u.get("thread_id") for u in uploads], [100] * 4)

    async def test_public_link_fallback_stays_in_thread(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            doc = Path(tmp) / "a.txt"
            doc.write_bytes(b"x")
            calls, uploads = [], []
            self.with_upload(
                instance, self.client(calls), uploads,
                result=("/f", {"url": "https://cloud.example/s/abc"}, False, False),
            )
            result = await instance.send_document("room", str(doc), metadata={"thread_id": "100"})
            self.assertTrue(result.success)
            self.assertEqual(calls[0][2].get("thread_id"), 100)

    async def test_thread_only_file_without_atomic_metadata_is_logged(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            doc = Path(tmp) / "a.txt"
            doc.write_bytes(b"x")
            calls, uploads = [], []
            self.with_upload(
                instance, self.client(calls), uploads, result=("/f", {"id": "1"}, True, False),
            )
            with self.assertLogs(adapter.logger, level="WARNING") as logs:
                result = await instance.send_document(
                    "room", str(doc), metadata={"thread_id": "100"}
                )
            self.assertTrue(result.success)
            self.assertEqual(calls, [])  # no caption: nothing to follow up with
            self.assertTrue(any("thread metadata" in line for line in logs.output))

    async def test_400_fallback_drops_reply_anchor(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            calls = []
            instance._client = self.client(
                calls, fail=adapter.NextcloudTalkAPIError("HTTP 400", status_code=400)
            )
            result = await instance.send(
                "room", "hi", reply_to="105", metadata={"thread_id": "100"}
            )
            self.assertTrue(result.success)
            self.assertEqual(calls[0][2].get("reply_to"), "105")
            self.assertEqual(calls[0][2].get("thread_id"), 100)
            self.assertIsNone(calls[1][2].get("reply_to"))
            self.assertIsNone(calls[1][2].get("thread_id"))

    async def test_400_in_the_middle_of_a_split_reply(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            instance.max_message_length = 10
            calls = []
            instance._client = self.client(
                calls, fail=adapter.NextcloudTalkAPIError("HTTP 400", status_code=400),
                fail_on_call=2,
            )
            result = await instance.send(
                "room", "aaaaaaaa bbbbbbbb cccccccc", metadata={"thread_id": "100"}
            )
            self.assertTrue(result.success)
            self.assertEqual(
                [(m, k.get("thread_id")) for _, m, k in calls],
                [("aaaaaaaa", 100), ("bbbbbbbb", 100), ("bbbbbbbb", None), ("cccccccc", None)],
            )

    async def test_other_errors_never_fall_back(self):
        for code in (403, 404, 413, 500):
            with self.subTest(code=code), tempfile.TemporaryDirectory() as tmp:
                instance = self.make_adapter(tmp)
                calls = []
                instance._client = self.client(
                    calls, fail=adapter.NextcloudTalkAPIError(f"HTTP {code}", status_code=code)
                )
                result = await instance.send("room", "hi", metadata={"thread_id": "100"})
                self.assertFalse(result.success)
                self.assertEqual(len(calls), 1)


if __name__ == "__main__":
    unittest.main()
