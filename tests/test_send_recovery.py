"""send_recovery: rejected sends are resent after WeChat is back; uncertain ones get a button."""
from __future__ import annotations

import shutil
import sys
import threading
import time
import unittest
import uuid
from pathlib import Path

TESTS = Path(__file__).resolve().parent
if str(TESTS) not in sys.path:
    sys.path.insert(0, str(TESTS))

from stub_ehforwarderbot import install_stubs

install_stubs()

from ehforwarderbot import Message, MsgType
from ehforwarderbot.exceptions import EFBMessageError

from efb_wechat_comwechat_slave.ComWechat import LinuxWeChatChannel
from efb_wechat_comwechat_slave.Core import CoreAPIError
from test_rc14_f2_pending_media import DelayedMediaCore


class SendCore(DelayedMediaCore):
    def __init__(self) -> None:
        super().__init__()
        self.ready = True
        self.sends = []
        self.fail_code = ""
        self.lock = threading.Lock()

    def _send(self, kind, payload, key):
        with self.lock:
            if not self.ready:
                raise CoreAPIError(409, "wechat_not_ready", "微信正在完成登录，请稍候。")
            if self.fail_code:
                raise CoreAPIError(400, self.fail_code, "bad request")
            self.sends.append((kind, dict(payload), key))
            return {"send_id": f"send-{len(self.sends)}", "status": "accepted", "kind": kind}

    def send_text(self, payload, key):
        return self._send("text", payload, key)

    def send_image(self, payload, key):
        return self._send("image", payload, key)

    def send_file(self, payload, key):
        return self._send("file", payload, key)


def wait_for(predicate, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return predicate()


class SendRecoveryTest(unittest.TestCase):
    def setUp(self) -> None:
        self.data_path = Path(__file__).resolve().parents[1] / ".tmp" / f"recovery-{uuid.uuid4().hex}"
        self.data_path.mkdir(parents=True, exist_ok=True)
        self.core = SendCore()
        config = {
            "startup_healthcheck": False,
            "shutdown_install_deferred": False,
            "consumer_id": "recovery-test",
            "account_ids": ["acc"],
            "core": {"poll_timeout": 0},
            "send_recovery": {"uncertain_grace_sec": 0.05, "retry_interval_sec": 1.0},
        }
        self.channel = LinuxWeChatChannel(core_client=self.core, config=config, data_path=self.data_path)
        self.channel._sender_capabilities_for = lambda account_id: {}
        self.posted = []
        self.channel._deliver_message = lambda msg: self.posted.append(msg)
        self.chat = self.channel.chat_mgr.build_core_chat(
            {"account_id": "acc", "chat_id": "g1@chatroom", "type": "group", "display_name": "调货群"}
        )

    def tearDown(self) -> None:
        self.channel._stop_event.set()
        self.channel.stop_polling()
        shutil.rmtree(self.data_path, ignore_errors=True)

    def _status(self, state: str) -> None:
        self.channel._handle_account_status("acc", {"account": {"state": state, "display_name": "1130"}})

    def _send(self, text: str) -> Message:
        return self.channel.send_message(Message(chat=self.chat, type=MsgType.Text, text=text))

    def _recovery_notes(self):
        return [m for m in self.posted if str(m.uid).startswith("recovery-")]

    # -- rejected ------------------------------------------------------------------

    def test_rejected_send_is_queued_and_resent_in_order_when_back_online(self) -> None:
        self._status("online")
        self._status("login_required")
        self.core.ready = False
        with self.assertRaises(EFBMessageError) as ctx:
            self._send("报价 A 100")
        self.assertIn("已排队", str(ctx.exception))
        with self.assertRaises(EFBMessageError):
            self._send("报价 B 200")
        self.assertEqual(2, self.channel.send_recovery.pending("acc"))
        time.sleep(0.2)
        self.assertEqual([], self.core.sends)  # nothing is sent while WeChat is down

        self.core.ready = True
        self._status("online")
        self.assertTrue(wait_for(lambda: len(self.core.sends) == 2))
        self.assertEqual(["报价 A 100", "报价 B 200"], [p["text"] for _, p, _ in self.core.sends])
        # The original idempotency key is reused: Core refused before storing anything.
        self.assertTrue(all(key for _, _, key in self.core.sends))
        self.assertTrue(wait_for(lambda: self._recovery_notes()))
        note = self._recovery_notes()[-1].text
        self.assertIn("自动补发 2 条", note)
        self.assertIn("调货群", note)

    def test_other_rejections_are_not_queued(self) -> None:
        self.core.fail_code = "invalid_request"
        with self.assertRaises(EFBMessageError) as ctx:
            self._send("hello")
        self.assertNotIn("已排队", str(ctx.exception))
        self.assertEqual(0, self.channel.send_recovery.pending("acc"))

    def test_rejected_resend_can_be_disabled(self) -> None:
        self.channel.send_recovery.auto_resend_rejected = False
        self.core.ready = False
        with self.assertRaises(EFBMessageError) as ctx:
            self._send("hello")
        self.assertNotIn("已排队", str(ctx.exception))

    def test_expired_rejected_sends_are_dropped_and_reported(self) -> None:
        self.core.ready = False
        with self.assertRaises(EFBMessageError):
            self._send("old quote")
        recovery = self.channel.send_recovery
        with recovery._lock:
            recovery._queues["acc"][0].queued_at -= recovery.max_age_sec + 1
        recovery._wake["acc"].set()
        self.assertTrue(wait_for(lambda: self._recovery_notes()))
        self.assertIn("已放弃 1 条", self._recovery_notes()[-1].text)
        self.assertEqual([], self.core.sends)

    # -- uncertain -----------------------------------------------------------------

    def _uncertain(self, send_id: str) -> None:
        self.channel._handle_send_update(
            {"send": {"send_id": send_id, "kind": "text", "status": "uncertain", "account_id": "acc", "chat_id": "g1@chatroom"}}
        )

    def test_uncertain_send_gets_a_resend_button_after_recovery(self) -> None:
        self._status("online")
        sent = self._send("报价 C 300")
        send_id = str(sent.uid)
        self._status("login_required")
        self._uncertain(send_id)
        time.sleep(0.3)
        self.assertEqual([], self._recovery_notes())  # never while WeChat is down
        self._status("online")
        self.assertTrue(wait_for(lambda: self._recovery_notes(), timeout=4))
        note = self._recovery_notes()[-1]
        self.assertIn("报价 C 300", note.text)
        self.assertIn("调货群", note.text)
        self.assertEqual("resend_uncertain_send", note.commands[0].callable_name)
        self.assertEqual((send_id,), tuple(note.commands[0].args))
        self.assertEqual(1, len(self.core.sends))  # never resent automatically

        self.assertEqual("已重发。", self.channel.resend_uncertain_send(send_id))
        self.assertEqual(2, len(self.core.sends))
        kind, payload, key = self.core.sends[-1]
        self.assertEqual("报价 C 300", payload["text"])
        self.assertNotEqual(self.core.sends[0][2], key)  # a new send, not a replay of the old key
        self.assertIn("不再重复发送", self.channel.resend_uncertain_send(send_id))
        self.assertEqual(2, len(self.core.sends))

    def test_uncertain_send_confirmed_later_is_not_prompted(self) -> None:
        self._status("online")
        send_id = str(self._send("hello").uid)
        self.channel.send_recovery.uncertain_grace_sec = 0.3
        self.channel.send_recovery._uncertain.clear()
        self._uncertain(send_id)
        self.channel._handle_send_update(
            {"send": {"send_id": send_id, "kind": "text", "status": "sent", "echo_message_id": "e1", "account_id": "acc", "chat_id": "g1@chatroom"}}
        )
        time.sleep(0.6)
        self.assertEqual([], self._recovery_notes())

    def test_button_after_late_confirmation_does_not_resend(self) -> None:
        self._status("online")
        send_id = str(self._send("hello").uid)
        self._uncertain(send_id)
        self.assertTrue(wait_for(lambda: self._recovery_notes(), timeout=4))
        self.channel._handle_send_update(
            {"send": {"send_id": send_id, "kind": "text", "status": "sent", "echo_message_id": "e1", "account_id": "acc", "chat_id": "g1@chatroom"}}
        )
        self.assertIn("不再重复发送", self.channel.resend_uncertain_send(send_id))
        self.assertEqual(1, len(self.core.sends))


if __name__ == "__main__":
    unittest.main()
