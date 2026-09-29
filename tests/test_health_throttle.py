"""The poll loop must not hit Core /health (a live Runtime probe of every account) on every round."""
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

from efb_wechat_comwechat_slave import ComWechat as cw
from efb_wechat_comwechat_slave.ComWechat import LinuxWeChatChannel
from test_rc14_f2_pending_media import DelayedMediaCore


class CountingCore(DelayedMediaCore):
    def __init__(self) -> None:
        super().__init__()
        self.health_calls = 0

    def health(self):
        self.health_calls += 1
        return {"ok": True, "sender_capabilities": {"file": True}}


class HealthThrottleTest(unittest.TestCase):
    def setUp(self) -> None:
        self.data_path = Path(__file__).resolve().parents[1] / ".tmp" / f"health-{uuid.uuid4().hex}"
        self.data_path.mkdir(parents=True, exist_ok=True)
        self.channel = None

    def tearDown(self) -> None:
        if self.channel is not None:
            self.channel.stop_polling()
        shutil.rmtree(self.data_path, ignore_errors=True)

    def _channel(self, **extra) -> LinuxWeChatChannel:
        config = {
            "startup_healthcheck": False,
            "shutdown_install_deferred": False,
            "consumer_id": "health-test",
            "account_ids": ["acc"],
            "core": {"poll_timeout": 0},
            **extra,
        }
        self.channel = LinuxWeChatChannel(core_client=CountingCore(), config=config, data_path=self.data_path)
        return self.channel

    def test_health_is_refreshed_at_most_once_per_interval(self) -> None:
        channel = self._channel()
        clock = [1000.0]
        original = cw.time.time
        cw.time.time = lambda: clock[0]
        try:
            for _ in range(5):
                channel._health_throttled()
                clock[0] += 1.0
            self.assertEqual(1, channel.core.health_calls)
            clock[0] += 60.0
            channel._health_throttled()
            self.assertEqual(2, channel.core.health_calls)
        finally:
            cw.time.time = original
        self.assertEqual({"file": True}, channel.sender_capabilities)

    def test_zero_interval_keeps_old_behaviour(self) -> None:
        channel = self._channel(health_interval_sec=0)
        for _ in range(3):
            channel._health_throttled()
        self.assertEqual(3, channel.core.health_calls)

    def test_failed_health_is_retried_next_round(self) -> None:
        channel = self._channel()
        calls = []

        def flaky():
            calls.append(1)
            if len(calls) == 1:
                raise RuntimeError("core down")
            return {"ok": True}

        channel.core.health = flaky
        with self.assertRaises(RuntimeError):
            channel._health_throttled()
        channel._health_throttled()
        self.assertEqual(2, len(calls))


if __name__ == "__main__":
    unittest.main()
