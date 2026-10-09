"""Talk UX features (0.1.15): Markdown, silent sends, status reactions, read marker, voice."""

import asyncio
import json
import tempfile
import threading
import time
import types
import unittest
from pathlib import Path
from unittest.mock import patch

import adapter
from test_threads import _Base, _msg, _thread_msg


class _UXClient:
    """Fake Talk client recording every call the UX features make."""

    def __init__(self, *, reaction_error=None, reaction_delay=None, read_error=None):
        self.sent = []
        self.reactions = []
        self.reads = []
        self.uploads = []
        self.reaction_error = reaction_error
        self.reaction_delay = reaction_delay
        self.read_error = read_error
        self._ids = iter(range(7000, 8000))

    async def send_message(self, room, message, **kwargs):
        self.sent.append((room, message, kwargs))
        return {"id": next(self._ids)}

    async def _react(self, kind, room, message_id, emoji):
        self.reactions.append((kind, room, message_id, emoji))
        if self.reaction_delay is not None:
            await asyncio.sleep(self.reaction_delay)
        if self.reaction_error is not None:
            raise self.reaction_error

    async def add_reaction(self, room, message_id, emoji):
        await self._react("add", room, message_id, emoji)

    async def remove_reaction(self, room, message_id, emoji):
        await self._react("remove", room, message_id, emoji)

    async def mark_read(self, room, message_id):
        self.reads.append((room, message_id))
        if self.read_error is not None:
            raise self.read_error

    async def upload_and_share_file(self, path, room, folder, **kwargs):
        self.uploads.append(kwargs)
        return "/f", {"id": "1"}, True, True


async def _drain_presentation(instance, timeout=2.0):
    """Wait for the detached reaction/read-marker tasks of the adapter."""
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        pending = [t for t in getattr(instance, "_presentation_tasks", set()) if not t.done()]
        pending += [t for t in getattr(instance, "_read_marker_tasks", {}).values() if not t.done()]
        if not pending:
            return
        await asyncio.wait(pending, timeout=0.05)


class MarkdownTests(_Base):
    def test_markdown_is_kept_and_images_become_urls(self):
        text = "**bold**\n```python\nprint(1)\n```\n[docs](https://example.org/d) ![pic](https://example.org/p.png)"
        out = adapter._talk_text(text)
        self.assertIn("```python\nprint(1)\n```", out)
        self.assertIn("[docs](https://example.org/d)", out)
        self.assertIn("https://example.org/p.png", out)
        self.assertNotIn("![pic]", out)
        plain = adapter._talk_text(text, markdown=False)
        self.assertNotIn("```", plain)
        self.assertIn("docs (https://example.org/d)", plain)

    async def test_send_keeps_code_blocks_by_default_and_strips_when_disabled(self):
        for extra, expect_fence in (({}, True), ({"markdown": False}, False)):
            with self.subTest(extra=extra), tempfile.TemporaryDirectory() as tmp:
                instance = self.make_adapter(tmp, **extra)
                instance._client = _UXClient()
                await instance.send("room", "```\ncode\n```", metadata={"notify": True})
                self.assertEqual("```" in instance._client.sent[0][1], expect_fence)


class SilentSendTests(_Base):
    async def test_only_final_replies_and_prompts_notify(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            self.assertEqual(instance.notifications, "important")
            client = instance._client = _UXClient()
            cases = [
                (None, True),
                ({}, True),
                ({"thread_id": "100"}, True),
                ({"notify": True}, False),
                ({"is_approval_prompt": True}, False),
            ]
            for metadata, silent in cases:
                with self.subTest(metadata=metadata):
                    client.sent.clear()
                    await instance.send("room", "hi", metadata=metadata)
                    self.assertEqual(client.sent[0][2].get("silent", False), silent)

    async def test_all_mode_never_silences(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp, notifications="all")
            client = instance._client = _UXClient()
            await instance.send("room", "progress")
            self.assertNotIn("silent", client.sent[0][2])

    def test_unknown_notifications_mode_falls_back_to_important(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertLogs(adapter.logger, level="WARNING"):
                instance = self.make_adapter(tmp, notifications="loud")
            self.assertEqual(instance.notifications, "important")

    async def test_thread_fallback_stays_silent(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            sent = []

            async def send_message(room, message, **kwargs):
                sent.append(kwargs)
                if kwargs.get("thread_id") is not None:
                    raise adapter.NextcloudTalkAPIError("HTTP 400", status_code=400)
                return {"id": 1}

            instance._client = types.SimpleNamespace(send_message=send_message)
            await instance.send("room", "status", metadata={"thread_id": "100"})
            self.assertEqual([k.get("silent") for k in sent], [True, True])

    async def test_real_base_final_reply_notifies(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            client = instance._client = _UXClient()
            instance.set_message_handler(lambda _event: asyncio.sleep(0, result="answer"))
            await instance._handle_talk_message(_msg(601), "room")
            await self.wait_for_background(instance)
            finals = [c for c in client.sent if c[1] == "answer"]
            self.assertEqual(len(finals), 1)
            self.assertNotIn("silent", finals[0][2])

    def test_client_posts_silent_flag(self):
        client = adapter.NextcloudTalkClient("https://cloud.example", "bot", "secret")
        posted = []
        client._request = lambda method, room, **kw: posted.append(kw["data"]) or {"id": 1}
        asyncio.run(client.send_message("room", "a", silent=True))
        asyncio.run(client.send_message("room", "b"))
        self.assertEqual(posted[0].get("silent"), "true")
        self.assertNotIn("silent", posted[1])


class StatusReactionTests(_Base):
    async def run_turn(self, instance, msg, room="room", result="answer"):
        async def handler(_event):
            if isinstance(result, BaseException):
                raise result
            return result

        instance.set_message_handler(handler)
        await instance._handle_talk_message(msg, room)
        await self.wait_for_background(instance)
        await _drain_presentation(instance)

    async def test_successful_turn_sends_no_reaction(self):
        # Talk notifies the author about every reaction and has no silent
        # mode, so a normal turn must not react at all.
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            client = instance._client = _UXClient()
            await self.run_turn(instance, _msg(611))
            self.assertEqual(client.reactions, [])
            self.assertEqual(len([c for c in client.sent if c[1] == "answer"]), 1)

    async def test_one_to_one_room_has_no_reactions_by_default(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            instance._room_types = {"room": 1}
            client = instance._client = _UXClient()
            await self.run_turn(instance, _msg(612))
            self.assertEqual(client.reactions, [])
            self.assertEqual(len([c for c in client.sent if c[1] == "answer"]), 1)

    async def test_reactions_all_marks_one_to_one_failures(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp, reactions=True)
            instance._room_types = {"room": 1}
            client = instance._client = _UXClient()
            await self.run_turn(instance, _msg(613))
            self.assertEqual(client.reactions, [])
            await self.run_turn(instance, _msg(618), result=RuntimeError("boom"))
            self.assertEqual(client.reactions, [("add", "room", 618, "❌")])

    async def test_reactions_off(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp, reactions="off")
            client = instance._client = _UXClient()
            await self.run_turn(instance, _msg(614), result=RuntimeError("boom"))
            self.assertEqual(client.reactions, [])

    async def test_unknown_room_type_gets_no_reaction(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            instance._room_types = {}
            client = instance._client = _UXClient()
            await self.run_turn(instance, _msg(615), result=RuntimeError("boom"))
            self.assertEqual(client.reactions, [])

    async def test_failed_turn_gets_only_cross(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            client = instance._client = _UXClient()
            await self.run_turn(instance, _msg(616), result=RuntimeError("boom"))
            self.assertEqual(client.reactions, [("add", "room", 616, "❌")])

    async def test_reaction_errors_never_block_reply_or_ack(self):
        for error, delay in ((adapter.NextcloudTalkAPIError("HTTP 500", status_code=500), None),
                             (None, 3600)):
            with self.subTest(error=error, delay=delay), tempfile.TemporaryDirectory() as tmp, \
                    patch.object(adapter, "_STATUS_REACTION_TIMEOUT", 0.05):
                instance = self.make_adapter(tmp)
                client = instance._client = _UXClient(reaction_error=error, reaction_delay=delay)
                committed = []
                real_commit = instance._commit_cursor

                def spy(room, numeric_id, *args, **kwargs):
                    committed.append(numeric_id)
                    return real_commit(room, numeric_id, *args, **kwargs)

                instance._commit_cursor = spy
                await self.run_turn(instance, _msg(617))
                await self.run_turn(instance, _msg(619), result=RuntimeError("boom"))
                self.assertEqual(len([c for c in client.sent if c[1] == "answer"]), 1)
                self.assertIn(617, committed)

    async def test_thread_message_reaction_targets_the_message(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            client = instance._client = _UXClient()
            await self.run_turn(instance, _thread_msg(625, 620), result=RuntimeError("boom"))
            self.assertEqual(client.reactions, [("add", "room", 625, "❌")])

    def test_reaction_mode_parsing(self):
        for raw, mode in ((None, "groups"), ("", "groups"), ("groups", "groups"), (True, "all"),
                          ("true", "all"), ("all", "all"), (False, "off"), ("off", "off"),
                          ("0", "off")):
            with self.subTest(raw=raw):
                self.assertEqual(adapter._reaction_mode(raw), mode)
        with self.assertLogs(adapter.logger, level="WARNING"):
            self.assertEqual(adapter._reaction_mode("sometimes"), "groups")

    def test_client_reaction_endpoints(self):
        client = adapter.NextcloudTalkClient("https://cloud.example", "bot", "secret")
        calls = []
        client._ocs_form_sync = lambda method, endpoint, data: calls.append((method, endpoint, data)) or {}
        asyncio.run(client.add_reaction("ro/om", 5, "👀"))
        asyncio.run(client.remove_reaction("room", 5, "👀"))
        asyncio.run(client.mark_read("room", 9))
        self.assertEqual(calls, [
            ("POST", "/ocs/v2.php/apps/spreed/api/v1/reaction/ro%2Fom/5", {"reaction": "👀"}),
            ("DELETE", "/ocs/v2.php/apps/spreed/api/v1/reaction/room/5", {"reaction": "👀"}),
            ("POST", "/ocs/v2.php/apps/spreed/api/v1/chat/room/read", {"lastReadMessage": 9}),
        ])

    def test_removing_a_missing_reaction_is_not_an_error(self):
        client = adapter.NextcloudTalkClient("https://cloud.example", "bot", "secret")

        def gone(*_args):
            raise adapter.NextcloudTalkAPIError("HTTP 404", status_code=404)

        client._ocs_form_sync = gone
        asyncio.run(client.remove_reaction("room", 5, "👀"))
        with self.assertRaises(adapter.NextcloudTalkAPIError):
            asyncio.run(client.add_reaction("room", 5, "👀"))


class ReadMarkerTests(_Base):
    async def test_dispatched_message_is_marked_read_monotonically(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            client = instance._client = _UXClient()
            instance.set_message_handler(lambda _event: asyncio.sleep(0, result=None))
            await instance._handle_talk_message(_msg(631), "room")
            await self.wait_for_background(instance)
            await asyncio.sleep(0.05)
            instance._queue_read_marker("room", 630)  # older: ignored
            await asyncio.sleep(0.05)
            self.assertEqual(client.reads, [("room", 631)])

    async def test_unauthorized_message_is_not_marked(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            instance.allow_all = False
            instance.group_allowed_users = {"someone-else"}
            client = instance._client = _UXClient()
            await instance._handle_talk_message(_msg(632), "room")
            await asyncio.sleep(0.05)
            self.assertEqual(client.reads, [])

    async def test_read_marker_failure_is_harmless_and_disabled_mode(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            client = instance._client = _UXClient(
                read_error=adapter.NextcloudTalkAPIError("HTTP 500", status_code=500)
            )
            instance._queue_read_marker("room", 640)
            await asyncio.sleep(0.05)
            self.assertEqual(client.reads, [("room", 640)])
            self.assertNotIn("room", instance._read_marker_done)
            off = self.make_adapter(tmp, read_marker=False)
            off_client = off._client = _UXClient()
            off._queue_read_marker("room", 641)
            await asyncio.sleep(0.05)
            self.assertEqual(off_client.reads, [])

    async def test_disconnect_cancels_pending_read_marker(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            gate = asyncio.Event()

            class Slow(_UXClient):
                async def mark_read(self, room, message_id):
                    self.reads.append((room, message_id))
                    await gate.wait()

            instance._client = Slow()
            instance._queue_read_marker("room", 650)
            await asyncio.sleep(0.01)
            task = instance._read_marker_tasks["room"]
            await instance.disconnect()
            self.assertTrue(task.done())


class VoiceMessageTests(_Base):
    async def test_voice_mp3_and_wav_become_voice_messages(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            client = instance._client = _UXClient()
            for name, is_voice, expected in (("a.mp3", True, "voice-message"),
                                             ("a.wav", True, "voice-message"),
                                             ("a.ogg", True, None),
                                             ("a.mp3", False, None)):
                with self.subTest(name=name, is_voice=is_voice):
                    path = Path(tmp) / name
                    path.write_bytes(b"x")
                    client.uploads.clear()
                    result = await instance.send_voice("room", str(path), is_voice=is_voice)
                    self.assertTrue(result.success)
                    self.assertEqual(client.uploads[0].get("message_type"), expected)

    def test_share_carries_voice_message_type(self):
        client = adapter.NextcloudTalkClient("https://cloud.example", "bot", "secret")
        forms = []
        client._ocs_post = lambda endpoint, form: forms.append(form) or {"id": "1"}
        client._share_file_to_talk_sync("Talk/a.mp3", "room", message_type="voice-message")
        client._share_file_to_talk_sync("Talk/a.mp3", "room", message_type="record-video")
        self.assertEqual(json.loads(forms[0]["talkMetaData"]), {"messageType": "voice-message"})
        self.assertNotIn("talkMetaData", forms[1])  # only the voice type is ever forwarded


class PresentationLatencyTests(_Base):
    """Reactions and read markers must not delay replies or starve shared threads."""

    async def test_hung_reaction_does_not_delay_reply_or_followup(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            client = instance._client = _UXClient(reaction_delay=3600)
            loop = asyncio.get_running_loop()
            start = loop.time()
            sent_at = []
            orig = client.send_message

            async def send_message(room, message, **kwargs):
                sent_at.append((message, loop.time() - start))
                return await orig(room, message, **kwargs)

            client.send_message = send_message

            async def handler(event):
                if event.message_id == "701":
                    raise RuntimeError("boom")  # its ❌ reaction hangs
                return "answer-" + event.message_id

            instance.set_message_handler(handler)
            await instance._handle_talk_message(_msg(701), "room")
            await self.wait_for_background(instance)
            # A second turn in the same session starts right after the first.
            await instance._handle_talk_message(_msg(702), "room")
            await self.wait_for_background(instance)
            self.assertIn("answer-702", [m for m, _ in sent_at])
            self.assertLess(max(t for _, t in sent_at), 1.0)
            self.assertTrue(instance._presentation_tasks)  # still hanging, detached
            await instance.disconnect()
            self.assertFalse(instance._presentation_tasks)

    async def test_stuck_presentation_requests_leave_default_executor_free(self):
        client = adapter.NextcloudTalkClient("https://cloud.example", "bot", "secret")
        release = threading.Event()
        started = []

        def stuck(method, endpoint, data):
            started.append(endpoint)
            release.wait(5)

        client._ocs_form_sync = stuck
        loop = asyncio.get_running_loop()
        import concurrent.futures
        loop.set_default_executor(concurrent.futures.ThreadPoolExecutor(max_workers=2))
        try:
            calls = [asyncio.ensure_future(client.add_reaction("room", i, "👀")) for i in range(10)]
            await asyncio.sleep(0.05)
            start = time.monotonic()
            await asyncio.to_thread(lambda: None)
            self.assertLess(time.monotonic() - start, 0.5)
            # The queue is bounded: the surplus is refused at once, not queued.
            refused = [c for c in calls if c.done() and isinstance(c.exception(), adapter.NextcloudTalkAPIError)]
            self.assertEqual(len(refused), 10 - adapter._PRESENTATION_MAX_PENDING)
            self.assertLessEqual(len(started), adapter._PRESENTATION_WORKERS)
        finally:
            release.set()
            await asyncio.gather(*calls, return_exceptions=True)
            client.close_presentation()

    def test_presentation_requests_use_short_http_timeout(self):
        client = adapter.NextcloudTalkClient("https://cloud.example", "bot", "secret", timeout=40)
        seen = []

        class _Resp:
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

        def opener(req, *, timeout=None):
            seen.append(timeout)
            return _Resp()

        client._open_authenticated = opener
        client._read_json = lambda resp: {"ocs": {"data": {}}}
        client._ocs_data = lambda parsed: {}
        client._ocs_form_sync("POST", "/x", {})
        self.assertEqual(seen, [adapter._PRESENTATION_HTTP_TIMEOUT])

    async def test_poll_disables_server_read_marker_only_when_plugin_marks(self):
        for flag, expected in ((True, "setReadMarker=0"), (False, None)):
            with self.subTest(flag=flag):
                client = adapter.NextcloudTalkClient("https://cloud.example", "bot", "secret")
                client.explicit_read_marker = flag
                urls = []
                client._request = lambda method, room, **kw: urls.append(client._url(room, kw["query"])) or []
                await client.get_messages("room", last_known_id=5, look_into_future=True, timeout=30)
                if expected:
                    self.assertIn(expected, urls[0])
                else:
                    self.assertNotIn("setReadMarker", urls[0])


class MediaNotifyTests(_Base):
    async def _send_doc(self, upload_result, caption=None):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            client = instance._client = _UXClient()

            async def upload(path, room, folder, **kwargs):
                return upload_result

            client.upload_and_share_file = upload
            path = Path(tmp) / "report.pdf"
            path.write_bytes(b"x")
            result = await instance.send_document(
                "room", str(path), caption=caption, metadata={"notify": True}
            )
            self.assertTrue(result.success)
            return client.sent

    async def test_caption_followup_of_final_attachment_notifies(self):
        sent = await self._send_doc(("/f", {"id": "1"}, True, False), caption="your report")
        self.assertEqual(sent[-1][1], "your report")
        self.assertNotIn("silent", sent[-1][2])

    async def test_public_link_fallback_of_final_attachment_notifies(self):
        sent = await self._send_doc(("/f", {"url": "https://cloud.example/s/x"}, False, False))
        self.assertIn("https://cloud.example/s/x", sent[-1][1])
        self.assertNotIn("silent", sent[-1][2])

    async def test_progress_attachment_followup_stays_silent(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            client = instance._client = _UXClient()

            async def upload(path, room, folder, **kwargs):
                return "/f", {"id": "1"}, True, False

            client.upload_and_share_file = upload
            path = Path(tmp) / "a.pdf"
            path.write_bytes(b"x")
            await instance.send_document("room", str(path), caption="progress")
            self.assertTrue(client.sent[-1][2].get("silent"))


class SilentFileShareTests(_Base):
    async def _share(self, metadata):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            client = instance._client = _UXClient()
            path = Path(tmp) / "a.pdf"
            path.write_bytes(b"x")
            result = await instance.send_document("room", str(path), metadata=metadata)
            self.assertTrue(result.success)
            return client.uploads[-1]

    async def test_progress_file_share_is_silent_final_one_notifies(self):
        self.assertIs((await self._share(None)).get("silent"), True)
        self.assertNotIn("silent", await self._share({"notify": True}))

    async def test_all_mode_never_silences_file_shares(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp, notifications="all")
            client = instance._client = _UXClient()
            path = Path(tmp) / "a.pdf"
            path.write_bytes(b"x")
            await instance.send_document("room", str(path))
            self.assertNotIn("silent", client.uploads[-1])

    def test_share_metadata_carries_silent(self):
        client = adapter.NextcloudTalkClient("https://cloud.example", "bot", "secret")
        forms = []
        client._ocs_post = lambda endpoint, form: forms.append(form) or {"id": "1"}
        client._share_file_to_talk_sync("Talk/a.pdf", "room", silent=True)
        client._share_file_to_talk_sync("Talk/a.pdf", "room")
        self.assertEqual(json.loads(forms[0]["talkMetaData"]), {"silent": True})
        self.assertNotIn("talkMetaData", forms[1])

    async def test_silent_share_falls_back_on_capability_refusal(self):
        client = adapter.NextcloudTalkClient("https://cloud.example", "bot", "secret")
        calls = []
        client._upload_file_sync = lambda path, folder: "Talk/a.pdf"

        def share(remote_path, room, **kwargs):
            calls.append(kwargs)
            if kwargs:
                raise adapter.NextcloudTalkAPIError("no metadata", category="capability")
            return {"id": "1"}

        client._share_file_to_talk_sync = share
        _path, _share, native, atomic = await client.upload_and_share_file(
            "a.pdf", "room", "/Talk", silent=True
        )
        self.assertIs(calls[0].get("silent"), True)
        self.assertEqual(calls[1], {})  # plain share after the refusal
        self.assertTrue(native)
        self.assertFalse(atomic)


class AutoTtsVoiceTests(_Base):
    async def test_auto_tts_mp3_is_a_voice_message(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            client = instance._client = _UXClient()
            path = Path(tmp) / "tts.mp3"
            path.write_bytes(b"x")
            result = await instance.play_tts(chat_id="room", audio_path=str(path),
                                             caption=None, metadata={"notify": True})
            self.assertTrue(result.success)
            self.assertEqual(client.uploads[-1].get("message_type"), "voice-message")


class FenceSplitTests(_Base):
    def test_split_keeps_every_chunk_fenced_and_indented(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            instance.max_message_length = 200
            code = "\n".join("    value_%02d = compute(%d)" % (i, i) for i in range(30))
            text = "Intro line\n```python\n" + code + "\n```\nOutro"
            chunks = instance._split_message(adapter._talk_text(text))
            self.assertGreater(len(chunks), 2)
            for chunk in chunks:
                self.assertLessEqual(len(chunk), 200)
                self.assertEqual(chunk.count("```") % 2, 0, chunk)
            joined = "\n".join(chunks)
            for i in range(30):
                self.assertIn("    value_%02d = compute(%d)" % (i, i), joined)
            self.assertTrue(chunks[1].startswith("```python\n    value_"))

    def test_plain_text_split_is_unchanged_in_content(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            instance.max_message_length = 128
            words = " ".join("word%03d" % i for i in range(100))
            chunks = instance._split_message(words)
            self.assertEqual(" ".join(chunks), words)
            self.assertTrue(all(len(c) <= 128 for c in chunks))

    def test_leading_indented_code_survives_talk_text(self):
        self.assertEqual(adapter._talk_text("\n    x = 1\n"), "    x = 1")


if __name__ == "__main__":
    unittest.main()
