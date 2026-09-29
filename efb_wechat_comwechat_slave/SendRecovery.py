"""Recover Telegram -> WeChat sends that were hit by a WeChat outage.

Two cases, handled differently on purpose:

* **Rejected** (Core answered ``409 wechat_not_ready`` before accepting the
  send): nothing reached WeChat, so the send is queued and submitted again,
  in order and with the original idempotency key, once WeChat is usable.
* **Uncertain** (Core accepted the send but could not confirm it, e.g. the
  WeChat client crashed while typing): it may or may not have been delivered,
  so it is never resent automatically. After WeChat is back and a grace period
  for a late echo has passed, a notice with a "重发" button is posted; the
  user decides.
"""
from __future__ import annotations

import logging
import threading
import time
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple

from .Core import CoreAPIError, CoreError

logger = logging.getLogger("efb_wechat_linux_slave.send_recovery")

DEFAULTS: Dict[str, Any] = {
    "auto_resend_rejected": True,
    "prompt_uncertain": True,
    "max_age_sec": 1800.0,
    "uncertain_grace_sec": 60.0,
    "retry_interval_sec": 15.0,
}

_REMEMBER_TEXT = 500
_REMEMBER_MEDIA = 10
_PREVIEW = 40


def kind_for_type_name(type_name: str) -> str:
    name = type_name.rsplit(".", 1)[-1].lower()
    if name in {"text", "link"}:
        return "text"
    if name in {"image", "sticker", "animation"}:
        return "image"
    return "file"


def preview_of(kind: str, payload: Mapping[str, Any]) -> str:
    if kind == "text":
        text = " ".join(str(payload.get("text") or "").split())
        return text if len(text) <= _PREVIEW else text[:_PREVIEW] + "…"
    label = "图片" if kind == "image" else "文件"
    name = str(payload.get("filename") or "")
    return f"[{label}] {name}".strip()


@dataclass
class _Queued:
    kind: str
    payload: Dict[str, Any]
    request_id: str
    chat_label: str
    queued_at: float = field(default_factory=time.time)


@dataclass
class _Uncertain:
    account_id: str
    chat_label: str
    kind: str
    payload: Optional[Dict[str, Any]]
    since: float = field(default_factory=time.time)
    prompted: bool = False
    resent: bool = False


class SendRecovery:
    def __init__(
        self,
        *,
        send: Callable[[str, Mapping[str, Any], str], Dict[str, Any]],
        post_notice: Callable[..., None],
        chat_label: Callable[[str, str], str],
        stop_event: threading.Event,
        config: Optional[Mapping[str, Any]] = None,
    ) -> None:
        cfg = dict(DEFAULTS)
        if isinstance(config, Mapping):
            cfg.update({k: v for k, v in config.items() if k in DEFAULTS})
        self.auto_resend_rejected = bool(cfg["auto_resend_rejected"])
        self.prompt_uncertain = bool(cfg["prompt_uncertain"])
        self.max_age_sec = max(60.0, float(cfg["max_age_sec"]))
        self.uncertain_grace_sec = max(0.0, float(cfg["uncertain_grace_sec"]))
        self.retry_interval_sec = max(1.0, float(cfg["retry_interval_sec"]))
        self._send = send
        self._post_notice = post_notice
        self._chat_label = chat_label
        self._stop = stop_event
        self._lock = threading.Lock()
        self._queues: Dict[str, List[_Queued]] = {}
        self._flushers: Dict[str, threading.Thread] = {}
        self._wake: Dict[str, threading.Event] = {}
        self._state: Dict[str, str] = {}
        self._sent: "OrderedDict[str, Tuple[str, str, str, Dict[str, Any]]]" = OrderedDict()
        self._uncertain: Dict[str, _Uncertain] = {}

    # -- bookkeeping -----------------------------------------------------------

    def label(self, account_id: str, chat_id: str) -> str:
        try:
            return self._chat_label(account_id, chat_id) or chat_id
        except Exception:
            return chat_id

    def remember_sent(self, send_id: str, kind: str, payload: Mapping[str, Any], account_id: str, chat_id: str) -> None:
        """Keep what a send contained, so an uncertain one can be offered for resend."""
        if not send_id or not self.prompt_uncertain:
            return
        with self._lock:
            self._sent[send_id] = (kind, account_id, chat_id, dict(payload))
            self._sent.move_to_end(send_id)
            media = [k for k, v in self._sent.items() if v[0] != "text"]
            for old in media[:-_REMEMBER_MEDIA]:
                self._sent.pop(old, None)
            texts = [k for k, v in self._sent.items() if v[0] == "text"]
            for old in texts[:-_REMEMBER_TEXT]:
                self._sent.pop(old, None)

    def on_account_status(self, account_id: str, payload: Mapping[str, Any]) -> None:
        account = payload.get("account") if isinstance(payload.get("account"), Mapping) else payload
        state = str(account.get("state") or "")
        if not account_id or not state:
            return
        with self._lock:
            self._state[account_id] = state
            wake = self._wake.get(account_id)
        if state == "online" and wake is not None:
            wake.set()

    def is_down(self, account_id: str) -> bool:
        with self._lock:
            return self._state.get(account_id) in {"login_required", "starting", "degraded", "stopped"}

    # -- rejected sends: queue and resend -----------------------------------------

    def queue_rejected(
        self,
        exc: BaseException,
        kind: str,
        payload: Mapping[str, Any],
        request_id: str,
        account_id: str,
        chat_id: str,
    ) -> bool:
        """Queue a send Core refused because WeChat was not ready. True if queued."""
        if not self.auto_resend_rejected or not isinstance(exc, CoreAPIError):
            return False
        if exc.code != "wechat_not_ready" or exc.status_code != 409:
            return False
        item = _Queued(kind=kind, payload=dict(payload), request_id=request_id, chat_label=self.label(account_id, chat_id))
        with self._lock:
            self._queues.setdefault(account_id, []).append(item)
            wake = self._wake.setdefault(account_id, threading.Event())
            running = self._flushers.get(account_id)
            if running is None or not running.is_alive():
                thread = threading.Thread(
                    target=self._flush_loop,
                    args=(account_id, wake),
                    name=f"send-recovery-{account_id}",
                    daemon=True,
                )
                self._flushers[account_id] = thread
                thread.start()
        logger.info("Queued rejected %s send for %s (%s)", kind, account_id, exc.code)
        return True

    def pending(self, account_id: str) -> int:
        with self._lock:
            return len(self._queues.get(account_id, []))

    def _flush_loop(self, account_id: str, wake: threading.Event) -> None:
        resent: List[str] = []
        failed: List[str] = []
        expired: List[str] = []
        while not self._stop.is_set():
            with self._lock:
                queue = self._queues.get(account_id, [])
                head = queue[0] if queue else None
            if head is None:
                break
            if time.time() - head.queued_at > self.max_age_sec:
                with self._lock:
                    self._queues[account_id].pop(0)
                expired.append(f"{head.chat_label}：{preview_of(head.kind, head.payload)}")
                continue
            if self.is_down(account_id):
                wake.clear()
                wake.wait(self.retry_interval_sec)
                continue
            try:
                self._send(head.kind, head.payload, head.request_id)
            except CoreAPIError as exc:
                if exc.code == "wechat_not_ready":
                    wake.clear()
                    wake.wait(self.retry_interval_sec)
                    continue
                with self._lock:
                    self._queues[account_id].pop(0)
                failed.append(f"{head.chat_label}：{preview_of(head.kind, head.payload)}（{exc.message}）")
                continue
            except CoreError as exc:
                # Core itself unreachable: keep the item and try again later.
                logger.info("Resend for %s deferred: %s", account_id, exc)
                wake.clear()
                wake.wait(self.retry_interval_sec)
                continue
            with self._lock:
                self._queues[account_id].pop(0)
            resent.append(f"{head.chat_label}：{preview_of(head.kind, head.payload)}")
        with self._lock:
            self._flushers.pop(account_id, None)
        self._report(account_id, resent, failed, expired)

    def _report(self, account_id: str, resent: List[str], failed: List[str], expired: List[str]) -> None:
        parts = []
        if resent:
            parts.append(f"✅ 微信恢复后已自动补发 {len(resent)} 条：\n" + "\n".join(f"· {x}" for x in resent))
        if failed:
            parts.append(f"❌ 补发失败 {len(failed)} 条：\n" + "\n".join(f"· {x}" for x in failed))
        if expired:
            parts.append(
                f"⌛ 超过 {int(self.max_age_sec // 60)} 分钟未能补发、已放弃 {len(expired)} 条：\n"
                + "\n".join(f"· {x}" for x in expired)
            )
        if not parts:
            return
        try:
            self._post_notice(account_id, "\n\n".join(parts))
        except Exception as exc:
            logger.warning("Resend report for %s failed: %s", account_id, exc)

    # -- uncertain sends: ask before resending ------------------------------------

    def on_send_update(self, send_id: str, status: str, echo_message_id: str, account_id: str) -> None:
        if not send_id:
            return
        if status == "uncertain" and not echo_message_id:
            if not self.prompt_uncertain:
                return
            with self._lock:
                if send_id in self._uncertain:
                    return
                kind, acc, chat_id, payload = self._sent.get(send_id, ("", account_id, "", None))
                self._uncertain[send_id] = _Uncertain(
                    account_id=acc or account_id,
                    chat_label=self.label(acc or account_id, chat_id) if chat_id else "",
                    kind=kind,
                    payload=payload,
                )
            self._schedule_check(send_id, self.uncertain_grace_sec)
        elif status == "sent" or echo_message_id:
            with self._lock:
                entry = self._uncertain.get(send_id)
                if entry is not None and not entry.prompted:
                    self._uncertain.pop(send_id, None)
                elif entry is not None:
                    entry.payload = None  # delivered after all: the button must not resend
                    entry.resent = True

    def _schedule_check(self, send_id: str, delay: float) -> None:
        timer = threading.Timer(delay, self._check_uncertain, args=(send_id,))
        timer.daemon = True
        timer.start()

    def _check_uncertain(self, send_id: str) -> None:
        if self._stop.is_set():
            return
        with self._lock:
            entry = self._uncertain.get(send_id)
        if entry is None or entry.prompted:
            return
        if time.time() - entry.since > self.max_age_sec:
            with self._lock:
                self._uncertain.pop(send_id, None)
            return
        if self.is_down(entry.account_id):
            self._schedule_check(send_id, self.retry_interval_sec)
            return
        with self._lock:
            entry.prompted = True
        what = preview_of(entry.kind, entry.payload or {}) if entry.kind else "（内容未知）"
        where = f"「{entry.chat_label}」" if entry.chat_label else ""
        text = f"🤔 这条消息没能确认是否已发到微信{where}：\n{what}\n微信里如果没有这条，点下面的按钮重发。"
        try:
            self._post_notice(entry.account_id, text, resend_send_id=send_id if entry.payload else None)
        except Exception as exc:
            logger.warning("Uncertain-send notice for %s failed: %s", send_id, exc)

    def resend_uncertain(self, send_id: str) -> str:
        """Button callback: resend an uncertain send once, as a new send."""
        with self._lock:
            entry = self._uncertain.get(send_id)
            if entry is None:
                return "这条消息的记录已过期，请手动重发。"
            if entry.resent:
                return "这条已经处理过了（已重发或已确认送达），不再重复发送。"
            if not entry.payload or not entry.kind:
                return "没有保存这条消息的内容，请手动重发。"
            entry.resent = True
            kind, payload = entry.kind, dict(entry.payload)
        payload["client_request_id"] = f"resend-{uuid.uuid4().hex}"
        try:
            self._send(kind, payload, payload["client_request_id"])
        except CoreError as exc:
            with self._lock:
                entry.resent = False
            return f"重发失败：{exc}"
        return "已重发。"
