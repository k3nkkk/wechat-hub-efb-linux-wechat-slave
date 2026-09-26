"""Outgoing echoes that arrive before Core links them to our send are not delivered twice."""
from __future__ import annotations

import shutil
import sys
import time
import unittest
import uuid
from pathlib import Path

TESTS = Path(__file__).resolve().parent
if str(TESTS) not in sys.path:
    sys.path.insert(0, str(TESTS))

from stub_ehforwarderbot import install_stubs

install_stubs()

from efb_wechat_comwechat_slave.ComWechat import LinuxWeChatChannel
from efb_wechat_comwechat_slave.EffectLedger import STATE_DELIVERED
from test_rc14_f2_pending_media import DelayedMediaCore

SELF = {"member_id": "self", "display_name": "Self", "is_self": True}


class SendEchoHoldTest(unittest.TestCase):
    def setUp(self) -> None:
        self.data_path = Path(__file__).resolve().parents[1] / ".tmp" / f"echo-{uuid.uuid4().hex}"
        self.data_path.mkdir(parents=True, exist_ok=True)
        self.deliveries = []
        self.channel = LinuxWeChatChannel(
            core_client=DelayedMediaCore(),
            config={
                "startup_healthcheck": False,
                "shutdown_install_deferred": False,
                "consumer_id": "echo-test",
                "account_ids": ["account-1"],
                "core": {"poll_timeout": 0},
            },
            data_path=self.data_path,
        )
        for chat_id in ("chat-1", "chat-2"):
            self.channel.chat_mgr.build_core_chat(
                {"account_id": "account-1", "chat_id": chat_id, "type": "private", "display_name": chat_id}, "Self"
            )
        self.channel._deliver_message = lambda m: self.deliveries.append(str(m.uid))

    def tearDown(self) -> None:
        self.channel.stop_polling()
        shutil.rmtree(self.data_path, ignore_errors=True)

    def _sent_from_telegram(self, send_id: str, chat_id: str = "chat-1") -> None:
        # What send_message records when Core answers with a send_id only.
        self.channel.echo_store.mark_pending(send_id, send_id)
        self.channel._inflight_sends[send_id] = ("account-1", chat_id, time.time())

    def _echo(self, message_id: str, chat_id: str = "chat-1") -> None:
        self.channel._handle_event(
            {
                "event_type": "message.created",
                "account_id": "account-1",
                "payload": {
                    "message": {
                        "account_id": "account-1",
                        "chat_id": chat_id,
                        "message_id": message_id,
                        "type": "text",
                        "direction": "outgoing",
                        "author": SELF,
                        "text": "hi",
                    }
                },
            }
        )

    def _linked(self, send_id: str, echo_id: str) -> None:
        self.channel._handle_send_update(
            {"send": {"send_id": send_id, "echo_message_id": echo_id, "status": "sent", "kind": "text"}}
        )

    def test_echo_before_link_is_held_then_suppressed(self) -> None:
        self._sent_from_telegram("send-1")
        self._echo("echo-1")
        self.assertEqual([], self.deliveries)
        self._linked("send-1", "echo-1")
        self.channel._retry_pending_media()
        self.assertEqual([], self.deliveries)
        effect_id = self.channel.effect_ledger.compute_effect_id("account-1", "echo-1")
        self.assertEqual(STATE_DELIVERED, self.channel.effect_ledger.get_effect_status(self.channel.consumer_id, effect_id))
        self.channel._retry_pending_media()
        self.assertEqual([], self.deliveries)

    def test_unlinked_echo_is_delivered_after_the_wait(self) -> None:
        self.channel.send_echo_wait_sec = 0.0
        self._sent_from_telegram("send-2")
        self._echo("native-2")
        self.assertEqual(["native-2"], self.deliveries)

    def test_uncertain_send_releases_the_held_message(self) -> None:
        self._sent_from_telegram("send-3")
        self._echo("phone-3")
        self.assertEqual([], self.deliveries)
        self.channel._handle_send_update({"send": {"send_id": "send-3", "status": "uncertain", "kind": "image"}})
        self.channel._retry_pending_media()
        self.assertEqual(["phone-3"], self.deliveries)

    def test_other_chats_are_not_held(self) -> None:
        self._sent_from_telegram("send-4", chat_id="chat-1")
        self._echo("other-4", chat_id="chat-2")
        self.assertEqual(["other-4"], self.deliveries)


if __name__ == "__main__":
    unittest.main()
