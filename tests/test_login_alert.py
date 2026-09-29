"""login_alert: a button in Telegram starts the WeChat login when an account drops out."""
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

from ehforwarderbot import MsgType

from efb_wechat_comwechat_slave.ComWechat import LinuxWeChatChannel
from efb_wechat_comwechat_slave.Core import CoreAPIError
from test_rc14_f2_pending_media import DelayedMediaCore

PNG = b"\x89PNG\r\n\x1a\n" + b"0" * 16


class LoginCore(DelayedMediaCore):
    def __init__(self) -> None:
        super().__init__()
        self.login_calls = []
        self.statuses = []
        self.start_result = {"login_flow_state": "starting"}

    def start_login(self, account_id):
        self.login_calls.append(account_id)
        return dict(self.start_result)

    def login_status(self, account_id):
        return self.statuses.pop(0) if len(self.statuses) > 1 else dict(self.statuses[0])

    def login_snapshot(self, account_id):
        return PNG


class LoginAlertTest(unittest.TestCase):
    def setUp(self) -> None:
        self.data_path = Path(__file__).resolve().parents[1] / ".tmp" / f"login-{uuid.uuid4().hex}"
        self.data_path.mkdir(parents=True, exist_ok=True)
        self.core = LoginCore()
        config = {
            "startup_healthcheck": False,
            "shutdown_install_deferred": False,
            "consumer_id": "login-test",
            "account_ids": ["acc"],
            "core": {"poll_timeout": 0},
            "login_watch_sec": 10,
        }
        self.channel = LinuxWeChatChannel(core_client=self.core, config=config, data_path=self.data_path)
        self.sent = []
        self.channel._deliver_message = lambda msg: self.sent.append(msg)

    def tearDown(self) -> None:
        self.channel._stop_event.set()
        self.channel.stop_polling()
        shutil.rmtree(self.data_path, ignore_errors=True)

    def _status(self, state: str) -> None:
        self.channel._handle_account_status("acc", {"account": {"state": state, "display_name": "1130"}})

    def test_logout_posts_a_login_button_once(self) -> None:
        self._status("online")
        self._status("login_required")
        self._status("login_required")
        self.assertEqual(1, len(self.sent))
        msg = self.sent[0]
        self.assertIn("1130", msg.text)
        self.assertEqual("start_wechat_login", msg.commands[0].callable_name)
        self.assertEqual(("acc",), tuple(msg.commands[0].args))
        self.assertEqual("system", type(msg.chat).__name__.replace("Chat", "").lower())

    def test_back_online_posts_a_note(self) -> None:
        self._status("login_required")
        self._status("online")
        self._status("online")
        self.assertEqual(2, len(self.sent))
        self.assertIn("已重新登录", self.sent[1].text)
        self.assertFalse(getattr(self.sent[1], "commands", None))

    def test_disabled(self) -> None:
        self.channel.login_alert = False
        self._status("login_required")
        self.assertEqual([], self.sent)

    def test_button_starts_login_and_sends_qr_when_needed(self) -> None:
        self._status("login_required")
        self.core.statuses = [
            {"login_flow_state": "starting"},
            {"login_flow_state": "waiting_for_scan", "snapshot_available": True},
            {"auth_status": "logged_in", "state": "online"},
        ]
        self.channel._stop_event.wait = lambda timeout=None: False
        reply = self.channel.start_wechat_login("acc")
        self.assertIn("已发起登录", reply)
        self.assertEqual(["acc"], self.core.login_calls)
        self.channel._login_watchers["acc"].join(5)
        images = [m for m in self.sent if m.type == MsgType.Image]
        self.assertEqual(1, len(images))
        self.assertEqual("image/png", images[0].mime)

    def test_button_reports_core_errors(self) -> None:
        def fail(account_id):
            raise CoreAPIError(503, "runtime_unavailable", "Runtime is down")

        self.core.start_login = fail
        self.assertIn("发起登录失败", self.channel.start_wechat_login("acc"))

    def test_already_logged_in(self) -> None:
        self.core.start_result = {"login_flow_state": "logged_in"}
        self.assertIn("已经是登录状态", self.channel.start_wechat_login("acc"))


if __name__ == "__main__":
    unittest.main()
