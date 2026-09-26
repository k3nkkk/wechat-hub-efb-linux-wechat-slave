"""send_reactions: react on the Telegram message once Core reports a send outcome."""
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


class SendReactionsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.data_path = Path(__file__).resolve().parents[1] / ".tmp" / f"react-{uuid.uuid4().hex}"
        self.data_path.mkdir(parents=True, exist_ok=True)
        self.channel = None
        self.reactions = []
        self.targets = {}

    def tearDown(self) -> None:
        if self.channel is not None:
            self.channel.stop_polling()
        shutil.rmtree(self.data_path, ignore_errors=True)

    def _channel(self, **reactions) -> LinuxWeChatChannel:
        config = {
            "startup_healthcheck": False,
            "shutdown_install_deferred": False,
            "consumer_id": "react-test",
            "account_ids": ["account-1"],
            "core": {"poll_timeout": 0},
        }
        if reactions:
            config["send_reactions"] = reactions
        channel = LinuxWeChatChannel(core_client=DelayedMediaCore(), config=config, data_path=self.data_path)
        channel._telegram_target = lambda send_id: self.targets.get(send_id)
        channel._set_telegram_reaction = lambda chat, msg, emoji: self.reactions.append((chat, msg, emoji))
        self.channel = channel
        return channel

    def _update(self, send_id: str, kind: str, status: str) -> None:
        receipt = {"send_id": send_id, "kind": kind, "status": status, "account_id": "account-1", "chat_id": "c"}
        if status == "sent":
            receipt["echo_message_id"] = f"echo-{send_id}"
        self.channel._handle_send_update({"send": receipt})

    def test_disabled_by_default(self) -> None:
        channel = self._channel()
        self.assertEqual(set(), channel.send_reaction_kinds)
        self.targets["s1"] = ("-100", 5)
        self._update("s1", "image", "sent")
        self.assertEqual([], self.reactions)

    def test_reacts_per_kind_and_status(self) -> None:
        self._channel(image=True, file=True)
        self.targets.update({"img": ("-100", 1), "file": ("-100", 2), "txt": ("-100", 3), "bad": ("-100", 4)})
        self._update("img", "image", "sent")
        self._update("file", "file", "uncertain")
        self._update("txt", "text", "sent")  # text switch is off
        self._update("bad", "image", "failed")
        self._update("img", "image", "submitted")  # no reaction for intermediate states
        self.assertEqual(
            [("-100", 1, "👌"), ("-100", 2, "🤔"), ("-100", 4, "👎")],
            self.reactions,
        )

    def test_custom_and_empty_emoji(self) -> None:
        self._channel(text=True, sent="👍", uncertain="")
        self.targets.update({"a": ("-1", 1), "b": ("-1", 2)})
        self._update("a", "text", "sent")
        self._update("b", "text", "uncertain")
        self.assertEqual([("-1", 1, "👍")], self.reactions)

    def test_waits_for_telegram_message_log(self) -> None:
        channel = self._channel(image=True)
        self._update("late", "image", "sent")
        self.assertEqual([], self.reactions)
        self.targets["late"] = ("-100", 9)
        channel._pending_reactions["late"]["due"] = 0.0
        channel._flush_send_reactions()
        self.assertEqual([("-100", 9, "👌")], self.reactions)
        channel._flush_send_reactions()
        self.assertEqual(1, len(self.reactions))

    def test_gives_up_when_no_telegram_message(self) -> None:
        channel = self._channel(image=True)
        self._update("ghost", "image", "sent")
        for _ in range(channel._REACTION_MAX_ATTEMPTS + 1):
            if "ghost" in channel._pending_reactions:
                channel._pending_reactions["ghost"]["due"] = 0.0
            channel._flush_send_reactions()
        self.assertNotIn("ghost", channel._pending_reactions)
        self.assertEqual([], self.reactions)


if __name__ == "__main__":
    unittest.main()
