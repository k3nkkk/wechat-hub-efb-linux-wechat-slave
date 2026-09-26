"""delivery_mode: "fast" delivers as soon as ready; "ordered" keeps chat order."""
from __future__ import annotations

import shutil
import sys
import unittest
import uuid
from pathlib import Path

TESTS = Path(__file__).resolve().parent
if str(TESTS) not in sys.path:
    sys.path.insert(0, str(TESTS))

from stub_ehforwarderbot import install_stubs

install_stubs()

from efb_wechat_comwechat_slave.ComWechat import LinuxWeChatChannel
from test_rc14_f2_pending_media import DelayedMediaCore

AUTHOR = {"member_id": "peer-1", "display_name": "Peer", "is_self": False}


def image(chat_id: str = "chat-1", created_at: str = "2026-09-27T10:00:00Z"):
    return {
        "account_id": "account-1",
        "chat_id": chat_id,
        "message_id": f"img-{chat_id}",
        "type": "sticker",
        "direction": "incoming",
        "author": AUTHOR,
        "created_at": created_at,
        "media_id": f"media-{chat_id}",
        "media_role": "original",
        "media_status": "original_pending",
        "filename": "sticker.webp",
        "mime_type": "image/webp",
    }


def text(message_id: str, chat_id: str = "chat-1", created_at: str = "2026-09-27T10:00:00Z"):
    return {
        "account_id": "account-1",
        "chat_id": chat_id,
        "message_id": message_id,
        "type": "text",
        "direction": "incoming",
        "author": AUTHOR,
        "created_at": created_at,
        "text": message_id,
    }


class DeliveryModeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.data_path = Path(__file__).resolve().parents[1] / ".tmp" / f"order-{uuid.uuid4().hex}"
        self.data_path.mkdir(parents=True, exist_ok=True)
        self.core = DelayedMediaCore()
        self.deliveries = []
        self.cursor = 0
        self.channel = None

    def tearDown(self) -> None:
        if self.channel is not None:
            self.channel.stop_polling()
        shutil.rmtree(self.data_path, ignore_errors=True)

    def _channel(self, **extra) -> LinuxWeChatChannel:
        config = {
            "startup_healthcheck": False,
            "shutdown_install_deferred": False,
            "consumer_id": "order-test",
            "account_ids": ["account-1"],
            "media_retry_max_attempts": 20,
            "media_retry_deadline_sec": 300,
            "media_retry_base_sec": 0,
            "media_retry_max_sec": 0,
            "core": {"poll_timeout": 0},
        }
        config.update(extra)
        channel = LinuxWeChatChannel(core_client=self.core, config=config, data_path=self.data_path)
        for chat_id in ("chat-1", "chat-2"):
            channel.chat_mgr.build_core_chat(
                {"account_id": "account-1", "chat_id": chat_id, "type": "private", "display_name": chat_id},
                "Self",
            )

        def capture(message):
            if message.file is not None:
                message.file.close()
            self.deliveries.append(str(message.uid))

        channel._deliver_message = capture
        self.channel = channel
        return channel

    def _send(self, message, event_type: str = "message.created") -> None:
        self.cursor += 1
        self.channel._handle_event(
            {
                "event_type": event_type,
                "account_id": "account-1",
                "cursor": str(self.cursor),
                "payload": {"message": message},
            }
        )

    def _media_ready(self, chat_id: str = "chat-1") -> None:
        self.core.ready = True
        self.cursor += 1
        self.channel._handle_event(
            {
                "event_type": "media.ready",
                "account_id": "account-1",
                "cursor": str(self.cursor),
                "payload": {
                    "media": {
                        "media_id": f"media-{chat_id}",
                        "role": "original",
                        "status": "ready",
                        "filename": "sticker.webp",
                        "mime_type": "image/webp",
                    }
                },
            }
        )

    def test_fast_mode_is_default_and_text_overtakes_media(self) -> None:
        channel = self._channel()
        self.assertEqual("fast", channel.delivery_mode)
        self._send(image())
        self._send(text("t1"))
        self.assertEqual(["t1"], self.deliveries)
        self._media_ready()
        self.assertEqual(["t1", "img-chat-1"], self.deliveries)

    def test_ordered_mode_holds_text_behind_pending_media(self) -> None:
        self._channel(delivery_mode="ordered")
        self._send(image())
        self._send(text("t1"))
        self._send(text("t2"))
        self.assertEqual([], self.deliveries)
        self._media_ready()
        self.assertEqual(["img-chat-1", "t1", "t2"], self.deliveries)
        # Nothing is delivered twice by later retries.
        self.channel._retry_pending_media()
        self.assertEqual(["img-chat-1", "t1", "t2"], self.deliveries)

    def test_ordered_mode_scheduled_retry_releases_in_order(self) -> None:
        self._channel(delivery_mode="ordered")
        self._send(image())
        self._send(text("t1"))
        self.core.ready = True
        self.channel._retry_pending_media()
        self.assertEqual(["img-chat-1", "t1"], self.deliveries)

    def test_ordered_mode_media_update_with_newer_cursor_is_not_blocked(self) -> None:
        self._channel(delivery_mode="ordered")
        self._send(image())
        self._send(text("t1"))
        self.core.ready = True
        ready = dict(image(), media_status="ready")
        self._send(ready, event_type="message.updated")
        self.channel._retry_pending_media()
        self.assertEqual(["img-chat-1", "t1"], self.deliveries)

    def test_ordered_mode_other_chats_are_not_blocked(self) -> None:
        self._channel(delivery_mode="ordered")
        self._send(image("chat-1"))
        self._send(text("other", chat_id="chat-2"))
        self.assertEqual(["other"], self.deliveries)

    def test_ordered_mode_earlier_text_is_not_blocked(self) -> None:
        self._channel(delivery_mode="ordered")
        self._send(image(created_at="2026-09-27T10:00:05Z"))
        # Arrives later in the stream but was sent earlier in the chat.
        self._send(text("early", created_at="2026-09-27T10:00:01Z"))
        self.assertEqual(["early"], self.deliveries)

    def test_ordered_mode_max_wait_releases_text(self) -> None:
        self._channel(delivery_mode="ordered", ordered_max_wait_sec=0)
        self._send(image())
        self._send(text("t1"))
        self.assertEqual(["t1"], self.deliveries)

    def test_unknown_mode_falls_back_to_fast(self) -> None:
        self.assertEqual("fast", self._channel(delivery_mode="bogus").delivery_mode)


if __name__ == "__main__":
    unittest.main()
