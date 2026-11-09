"""Mention gating must apply to groups, never to private Talk rooms."""
import tempfile
from unittest.mock import patch
from test_threads import _Base, _msg


class GroupMentionTests(_Base):
    async def test_private_plain_message_is_delivered_with_gate_enabled(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            instance.require_mention = True
            instance._room_types['room'] = 1
            event = await self.capture_event(instance, _msg(101, 'привет'))
            self.assertEqual(event.text, 'привет')
            self.assertEqual(event.source.chat_type, 'dm')

    async def test_group_plain_message_is_ignored(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            instance.require_mention = True
            with patch.object(instance, 'handle_message') as handler:
                await instance._handle_talk_message(_msg(102, 'привет'), 'room')
                handler.assert_not_called()

    async def test_group_addressed_message_is_delivered(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            instance.require_mention = True
            instance.bot_name = 'Hermes'
            event = await self.capture_event(instance, _msg(103, 'Hermes, привет'))
            self.assertEqual(event.text, 'привет')

    def test_direct_helper_preserves_private_commands(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = self.make_adapter(tmp)
            instance.require_mention = True
            self.assertEqual(instance._strip_bot_mention('/status', chat_type='dm'), '/status')
            self.assertEqual(instance._strip_bot_mention('/status', chat_type='group'), '')
