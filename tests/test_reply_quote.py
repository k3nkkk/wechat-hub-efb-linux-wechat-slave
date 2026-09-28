"""WeChat quote replies ("reply") are forwarded as text with the quoted context."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

TESTS = Path(__file__).resolve().parent
if str(TESTS) not in sys.path:
    sys.path.insert(0, str(TESTS))

from stub_ehforwarderbot import install_stubs

install_stubs()

from ehforwarderbot import MsgType

from efb_wechat_comwechat_slave.ChatMgr import ChatMgr
from efb_wechat_comwechat_slave.CoreMessage import CoreMessageBuilder
from test_rc14_f1_media_selection import DummySlave

SEP = CoreMessageBuilder._QUOTE_SEPARATOR


class ReplyQuoteTest(unittest.TestCase):
    def setUp(self) -> None:
        chats = ChatMgr(DummySlave())
        self.chat = chats.build_core_chat(
            {"account_id": "a", "chat_id": "g@chatroom", "type": "group", "display_name": "G"}, "Self"
        )
        self.builder = CoreMessageBuilder(object(), chats)

    def _reply(self, text: str, content: str, sender: str = "小王", **extra):
        message = {
            "account_id": "a",
            "chat_id": "g@chatroom",
            "message_id": "m1",
            "type": "reply",
            "direction": "incoming",
            "author": {"member_id": "wxid_b", "display_name": "B", "is_self": False},
            "text": text,
            "attributes": {"reply": {"content": content, "sender": sender}},
        }
        message.update(extra)
        return self.builder.build(message, self.chat)

    def test_text_quote(self) -> None:
        msg = self._reply("1", "960到付求喷火龙礼盒")
        self.assertEqual(MsgType.Text, msg.type)
        self.assertEqual(f"「小王: 960到付求喷火龙礼盒」\n{SEP}\n1", msg.text)

    def test_image_quote_and_link_prefix(self) -> None:
        msg = self._reply("[Link] 谁有这款的", '<msg><img md5="x" cdnbigimgurl="y"/></msg>')
        self.assertEqual(f"「小王: [图片]」\n{SEP}\n谁有这款的", msg.text)

    def test_appmsg_quote_uses_title_and_is_shortened(self) -> None:
        msg = self._reply("ok", "<msg><appmsg><title>" + "长" * 200 + "</title></appmsg></msg>", sender="")
        quoted = msg.text.split("\n", 1)[0]
        self.assertTrue(quoted.startswith("「长"))
        self.assertLessEqual(len(quoted), CoreMessageBuilder._QUOTE_MAX_LEN + 2)

    def test_resolved_target_skips_inline_quote(self) -> None:
        msg = self._reply("1", "原文", target_message_id="m0")
        self.assertEqual("1", msg.text)
        self.assertEqual("m0", str(msg.target.uid))

    def test_undelivered_target_keeps_inline_quote(self) -> None:
        self.builder.target_delivered = lambda message_id: message_id == "m_seen"
        msg = self._reply("1", "原文", target_message_id="m_missing")
        self.assertEqual(f"「小王: 原文」\n{SEP}\n1", msg.text)
        msg = self._reply("1", "原文", target_message_id="m_seen")
        self.assertEqual("1", msg.text)
        self.assertEqual("m_seen", str(msg.target.uid))

    def test_failing_delivery_check_keeps_inline_quote(self) -> None:
        def boom(_message_id: str) -> bool:
            raise RuntimeError("db closed")

        self.builder.target_delivered = boom
        msg = self._reply("1", "原文", target_message_id="m0")
        self.assertTrue(msg.text.startswith("「小王: 原文」"))

    def test_mentions_are_shifted_past_the_quote(self) -> None:
        msg = self._reply(
            "@B 好",
            "原文",
            substitutions=[{"start": 0, "end": 2, "member_id": "wxid_b", "display_name": "B"}],
        )
        prefix = f"「小王: 原文」\n{SEP}\n"
        (start, end), = msg.substitutions.keys()
        self.assertEqual("@B", msg.text[start:end])
        self.assertEqual(len(prefix), start)


if __name__ == "__main__":
    unittest.main()
