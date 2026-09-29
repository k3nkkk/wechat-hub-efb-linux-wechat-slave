"""EFB Linux WeChat Slave backed by WeChat Core HTTP API V1.

The class keeps the lifecycle/chat/message-dispatch architecture of the
ComWechat upstream channel while replacing its Windows Hook backend with the
frozen account-aware Core contract.
"""

from __future__ import annotations

import base64
import json
import logging
import mimetypes
import tempfile
import threading
import time
import uuid
from collections import OrderedDict
from pathlib import Path
from datetime import datetime
from typing import Any, BinaryIO, Collection, Dict, Mapping, Optional, Set, Tuple

import yaml

from ehforwarderbot import Message, MsgType, Status, coordinator
from ehforwarderbot import utils as efb_utils
from ehforwarderbot.channel import SlaveChannel
from ehforwarderbot.chat import Chat, ChatMember, SystemChatMember
from ehforwarderbot.exceptions import (
    EFBChatNotFound,
    EFBException,
    EFBMessageError,
    EFBMessageTypeNotSupported,
    EFBOperationNotSupported,
)
from ehforwarderbot.message import MessageCommand, MessageCommands
from ehforwarderbot.status import ChatUpdates, MessageRemoval
from ehforwarderbot.types import ChatID, InstanceID, MessageID

from . import __version__ as version
from .ChatMgr import ChatMgr
from .Core import CoreAPIError, CoreClient, CoreContractError, CoreError, CoreUnavailableError, CursorStore, EchoStore
from .SendRecovery import SendRecovery, kind_for_type_name
from .CoreMessage import CoreMessageBuilder, MediaPendingError, MediaPermanentError
from .EffectLedger import (
    BLOCKED_UNCERTAIN_EFFECT,
    DETAIL_RETRY_PARKED,
    DELIVERY_SEMANTICS,
    STATE_DELIVERED,
    STATE_MEDIA_FAILED,
    STATE_PENDING_MEDIA,
    STATE_RESERVED,
    STATE_UNCERTAIN,
    EffectLedger,
)
from .ShutdownCoordinator import (
    SHUTDOWN_EVIDENCE_MARKER,
    ShutdownCoordinator,
    install_shutdown_coordinator,
)
from .MessageMapping import MessageMappingStore
from .Provenance import (
    CLASSIFICATION_FIRST_BUSINESS_EFFECT,
    CLASSIFICATION_INDETERMINATE,
    CLASSIFICATION_KNOWN_EFFECT,
    CLASSIFICATION_REPROJECTION_OF_PREEXISTING_OBJECT,
    PROVENANCE_INDETERMINATE_CORE_READ_ABSENT,
    PROVENANCE_INDETERMINATE_CORE_READ_FAILED,
    PROVENANCE_INDETERMINATE_FLOOR_UNAVAILABLE,
    PROVENANCE_INDETERMINATE_MISSING_CREATED_AT,
    PROVENANCE_INDETERMINATE_NO_EFFECT_IDENTITY,
    ProvenanceDeferralStore,
    SubscriptionFloorStore,
    extract_business_origin,
)
from .UID import InvalidUID, decode_chat_uid


DEFAULT_CONFIG: Dict[str, Any] = {
    "core": {
        "base_url": "http://127.0.0.1:8080",
        "timeout": 10,
        "poll_timeout": 15,
        "verify_tls": True,
    },
    "consumer_id": "efb-linux-wechat",
    "account_ids": [],
    "poll_interval": 1.0,
    # Core's /health refreshes every account's live Runtime status, which makes each
    # agent-wechat walk WeChat's whole accessibility tree and take a screenshot. The
    # poll loop only needs it to notice capability changes, so it is refreshed at most
    # once per this many seconds (0 = every poll, the old behaviour).
    "health_interval_sec": 60.0,
    "event_limit": 50,
    "startup_healthcheck": True,
    "bootstrap_mode": "at_head",
    "startup_history_projection": False,
    "media_retry_max_attempts": 20,
    "media_retry_deadline_sec": 300.0,
    "media_retry_base_sec": 1.0,
    "media_retry_max_sec": 30.0,
    # Delivery ordering within a chat. "fast" delivers every message as soon as
    # it is ready (a text can overtake a still-downloading image). "ordered"
    # holds later messages of the same chat behind pending media, for at most
    # ``ordered_max_wait_sec`` seconds, so the chat keeps its original order.
    # ``ordered_scope`` picks what is held: "all" (text too) or "media" (only
    # images/videos/files keep their order; text is forwarded immediately).
    "delivery_mode": "fast",
    "ordered_scope": "all",
    "ordered_max_wait_sec": 20.0,
    # A send from Telegram can reach WeChat (and come back from Core as an
    # outgoing message) before Core has linked it to the send. Outgoing
    # messages in a chat with an unconfirmed send are held for up to this
    # many seconds so the echo is recognised instead of delivered twice.
    "send_echo_wait_sec": 20.0,
    # React on the Telegram message once Core reports the outcome of a send
    # made from Telegram. Per kind: true reacts to every outcome, "problems"
    # only to uncertain/failed sends (a later success clears the mark), false
    # disables it. An empty emoji disables that outcome. Only Telegram's
    # standard reaction emoji are accepted.
    "send_reactions": {
        "text": False,
        "image": False,
        "file": False,
        "sent": "👌",
        "uncertain": "🤔",
        "failed": "👎",
    },
    # When an account needs to log in again, post a message with a button
    # that starts the login flow (phone confirmation, or a QR code to scan).
    "login_alert": True,
    "login_watch_sec": 120.0,
    # Retry3 graceful-shutdown coordination (defect R14-EFB-D3 / Exit 137).
    "shutdown_master_budget_sec": 1.0,
    "shutdown_slave_drain_budget_sec": 0.75,
    "shutdown_hard_exit": True,
    "shutdown_install_deferred": True,
    # Unknown-identity hardening: durable subscription provenance and the
    # fail-closed deferral retry budget for an unresolvable business origin.
    "provenance_core_read_enabled": True,
    "provenance_core_read_max_pages": 2,
    "provenance_retry_max_attempts": 5,
    "provenance_deferral_max_entries": 512,
}


class LinuxWeChatChannel(SlaveChannel):
    channel_name = "Linux WeChat"
    channel_emoji = "🐧"
    channel_id = "wechat.linux"
    __version__ = version.__version__

    supported_message_types = {
        MsgType.Text,
        MsgType.Link,
        MsgType.Image,
        MsgType.Sticker,
        MsgType.Animation,
        MsgType.File,
        MsgType.Video,
        MsgType.Voice,
    }

    logger = logging.getLogger("efb_wechat_linux_slave")

    def __init__(
        self,
        instance_id: InstanceID = None,
        *,
        core_client: Optional[CoreClient] = None,
        config: Optional[Mapping[str, Any]] = None,
        data_path: Optional[Path] = None,
    ) -> None:
        super().__init__(instance_id=instance_id)
        self.config = self._merge_config(config if config is not None else self._load_config())
        core_cfg = self.config["core"]
        self.core = core_client or CoreClient(
            str(core_cfg["base_url"]),
            timeout=float(core_cfg["timeout"]),
            verify_tls=bool(core_cfg["verify_tls"]),
        )
        self.poll_timeout = max(0, min(int(core_cfg.get("poll_timeout", 15)), 30))
        self.event_limit = max(1, min(int(self.config.get("event_limit", 50)), 200))
        self.poll_interval = max(0.05, float(self.config.get("poll_interval", 1.0)))
        self.health_interval_sec = max(0.0, float(self.config.get("health_interval_sec", 60.0)))
        self._last_health_ok = 0.0
        self.account_filter: Set[str] = {
            str(item) for item in self.config.get("account_ids", []) if str(item)
        }
        self.sender_capabilities: Dict[str, Any] = {}
        self.account_sender_capabilities: Dict[str, Dict[str, Any]] = {}
        consumer_base = str(self.config.get("consumer_id") or "efb-linux-wechat")
        self.consumer_id = f"{consumer_base}:{self.channel_id}"

        self.chat_mgr = ChatMgr(self)
        self.message_builder = CoreMessageBuilder(
            self.core, self.chat_mgr, target_delivered=self._delivered_to_master
        )
        self._stop_event = threading.Event()
        self.login_alert = bool(self.config.get("login_alert", True))
        self.login_watch_sec = max(10.0, float(self.config.get("login_watch_sec", 120.0)))
        self._login_lock = threading.Lock()
        self._account_login_state: Dict[str, str] = {}
        self._login_watchers: Dict[str, threading.Thread] = {}
        self.send_recovery = SendRecovery(
            send=self._core_send,
            post_notice=self._post_recovery_notice,
            chat_label=self._chat_label,
            stop_event=self._stop_event,
            config=self.config.get("send_recovery"),
        )
        # Retry3 shutdown coordination state.
        self._shutdown_in_progress = threading.Event()
        self._checkpoint_flush_lock = threading.Lock()
        self._checkpoint_flush_completed = False
        self._checkpoint_flush_cursor: Optional[int] = None
        self._stop_polling_lock = threading.Lock()
        self._shutdown_coordinator: Optional[ShutdownCoordinator] = None
        self._shutdown_evidence: Dict[str, Any] = {}
        self._message_cache: "OrderedDict[Tuple[str, str], Message]" = OrderedDict()
        self._message_cache_size = 2048
        self.media_retry_max_attempts = max(1, int(self.config.get("media_retry_max_attempts", 20)))
        self.media_retry_deadline_sec = max(0.0, float(self.config.get("media_retry_deadline_sec", 300.0)))
        self.media_retry_base_sec = max(0.0, float(self.config.get("media_retry_base_sec", 1.0)))
        self.media_retry_max_sec = max(
            self.media_retry_base_sec,
            float(self.config.get("media_retry_max_sec", 30.0)),
        )
        delivery_mode = str(self.config.get("delivery_mode") or "fast").strip().lower()
        if delivery_mode not in {"fast", "ordered"}:
            self.logger.warning("Unknown delivery_mode %r; using 'fast'", delivery_mode)
            delivery_mode = "fast"
        self.delivery_mode = delivery_mode
        ordered_scope = str(self.config.get("ordered_scope") or "all").strip().lower()
        if ordered_scope not in {"all", "media"}:
            self.logger.warning("Unknown ordered_scope %r; using 'all'", ordered_scope)
            ordered_scope = "all"
        self.ordered_scope = ordered_scope
        self.ordered_max_wait_sec = max(0.0, float(self.config.get("ordered_max_wait_sec", 20.0)))
        reactions = dict(DEFAULT_CONFIG["send_reactions"])
        if isinstance(self.config.get("send_reactions"), Mapping):
            reactions.update(self.config["send_reactions"])
        self.send_reaction_modes: Dict[str, str] = {}
        for kind in ("text", "image", "file"):
            mode = reactions.get(kind)
            if isinstance(mode, str) and mode.strip().lower() in {"problems", "failures", "problem", "failure"}:
                self.send_reaction_modes[kind] = "problems"
            elif isinstance(mode, str) and mode.strip().lower() in {"", "false", "off", "no", "none"}:
                continue
            elif mode:
                self.send_reaction_modes[kind] = "all"
        self.send_reaction_kinds = set(self.send_reaction_modes)
        self._problem_marked: Dict[str, float] = {}
        self.send_reaction_emoji = {
            status: str(reactions.get(status) or "").strip()
            for status in ("sent", "uncertain", "failed")
        }
        self._pending_reactions: Dict[str, Dict[str, Any]] = {}
        self.send_echo_wait_sec = max(0.0, float(self.config.get("send_echo_wait_sec", 20.0)))
        self._inflight_sends: Dict[str, Tuple[str, str, float]] = {}
        self._reaction_lock = threading.Lock()

        resolved_data_path = Path(data_path) if data_path is not None else efb_utils.get_data_path(self.channel_id)
        self.cursor_store = CursorStore(resolved_data_path / "core-event-cursor.json")
        self.echo_store = EchoStore(resolved_data_path / "core-send-echo.json")
        self.effect_ledger = EffectLedger(resolved_data_path / "core-effect-ledger.sqlite3")
        self.message_mapping = MessageMappingStore(resolved_data_path / "core-message-mapping.sqlite3")
        # Unknown-identity hardening (RC.14 EFB reprojection idempotency). Both stores
        # are additive files of their own: the durable 417-row EffectLedger, the
        # message mapping DB and the Core checkpoint are never touched by them.
        self.subscription_floor = SubscriptionFloorStore(
            resolved_data_path / "core-subscription-floor.json",
            self.consumer_id,
            logger=self.logger,
        )
        self.provenance_deferrals = ProvenanceDeferralStore(
            resolved_data_path / "core-provenance-deferrals.json",
            self.consumer_id,
            max_attempts=max(1, int(self.config.get("provenance_retry_max_attempts", 5))),
            max_entries=max(1, int(self.config.get("provenance_deferral_max_entries", 512))),
            logger=self.logger,
        )
        self.provenance_core_read_enabled = bool(
            self.config.get("provenance_core_read_enabled", True)
        )
        self.provenance_core_read_max_pages = max(
            1, int(self.config.get("provenance_core_read_max_pages", 2))
        )
        self._provenance_counts: Dict[str, int] = {
            CLASSIFICATION_FIRST_BUSINESS_EFFECT: 0,
            CLASSIFICATION_REPROJECTION_OF_PREEXISTING_OBJECT: 0,
            CLASSIFICATION_INDETERMINATE: 0,
        }
        reconciled = self.effect_ledger.reconcile_on_startup(self.consumer_id)
        if reconciled:
            self.logger.warning(
                "Startup reconciliation marked %d lingering RESERVED effects as UNCERTAIN (%s): %s",
                len(reconciled),
                BLOCKED_UNCERTAIN_EFFECT,
                reconciled,
            )
        self.startup_history_projection = bool(self.config.get("startup_history_projection", False))

        # Register graceful shutdown handlers
        try:
            import signal
            if threading.current_thread() is threading.main_thread():
                signal.signal(signal.SIGINT, self._sig_handler)
                if hasattr(signal, "SIGTERM"):
                    signal.signal(signal.SIGTERM, self._sig_handler)
        except (ValueError, AttributeError):
            pass

        if bool(self.config.get("startup_healthcheck", True)):
            try:
                self._health()
            except CoreContractError:
                raise
            except CoreUnavailableError as exc:
                # Core may start after EFB. Polling retries without losing cursor.
                self.logger.warning("Core is not reachable during channel startup: %s", exc)

        try:
            aligned_cursor = self._ensure_bootstrap_aligned()
            self.logger.info("Cursor aligned with Core initial position: %s", aligned_cursor)
        except Exception as exc:
            self.logger.debug("Could not align cursor during init (will align before first poll): %s", exc)

        try:
            floor = self._ensure_subscription_floor()
            self.logger.info(
                "Subscription floor established: %s",
                json.dumps(floor, ensure_ascii=False, sort_keys=True),
            )
        except Exception as exc:
            self.logger.warning(
                "Could not establish the subscription floor during init "
                "(unknown identities will fail closed until it is available): %s",
                exc,
            )

        self.logger.info(
            "Linux WeChat Slave initialized: version=%s core=%s accounts=%s",
            self.__version__,
            getattr(self.core, "base_url", "injected"),
            sorted(self.account_filter) or "all",
        )

        self._install_shutdown_coordinator()

    # ------------------------------------------------------------------ #
    # Retry3 graceful shutdown coordination (defect R14-EFB-D3)
    # ------------------------------------------------------------------ #

    def _install_shutdown_coordinator(self) -> Optional[ShutdownCoordinator]:
        """Install the ordered/bounded shutdown coordinator (idempotent)."""
        if self._shutdown_coordinator is not None:
            return self._shutdown_coordinator
        try:
            coordinator_obj = install_shutdown_coordinator(
                self,
                logger=self.logger,
                master_budget_sec=float(self.config.get("shutdown_master_budget_sec", 1.0)),
                slave_drain_budget_sec=float(
                    self.config.get("shutdown_slave_drain_budget_sec", 0.75)
                ),
                hard_exit=bool(self.config.get("shutdown_hard_exit", True)),
                install_deferred=bool(self.config.get("shutdown_install_deferred", True)),
            )
        except Exception:
            self.logger.exception("Failed to install shutdown coordinator")
            return None
        self._shutdown_coordinator = coordinator_obj
        return coordinator_obj

    def suppress_external_dispatch(self) -> None:
        """Refuse any further outbound external delivery (idempotent)."""
        self._shutdown_in_progress.set()

    def drain_for_shutdown(self, *, budget_sec: float = 0.75) -> Dict[str, Any]:
        """Quiesce polling and durably flush local state before the master stops.

        Order (Retry3 corrective engineering, defect R14-EFB-D3):
          1. stop polling Core for new work;
          2. flush the durable cursor / checkpoint to Core;
          3. checkpoint the effect-ledger WAL so DELIVERED/RESERVED rows are
             durable on disk (the ledger is never deleted or truncated);
          4. forbid any further external dispatch.
        """
        started = time.monotonic()
        detail: Dict[str, Any] = {
            "poll_stopped": False,
            "core_session_closed": False,
            "checkpoint_flushed": False,
            "checkpoint_cursor": None,
            "ledger_wal_checkpointed": False,
            "poll_thread_joined": False,
            "delivery_suppressed": False,
        }

        # 4 first: no new outbound delivery may be attempted from here on.
        self.suppress_external_dispatch()
        detail["delivery_suppressed"] = True

        # 1: stop the Core poll loop.
        self.stop_polling()
        detail["poll_stopped"] = self._stop_event.is_set()

        # 1b: break any in-flight Core poll request so the poll thread can exit.
        session = getattr(self.core, "session", None)
        if session is not None:
            try:
                session.close()
                detail["core_session_closed"] = True
            except Exception:
                self.logger.debug("Core session close during shutdown failed", exc_info=True)

        # 1c: bounded join of the slave poll thread when it is reachable.
        deadline = started + max(0.0, float(budget_sec))
        for thread in self._slave_poll_threads():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            if thread.is_alive():
                thread.join(timeout=remaining)
            detail["poll_thread_joined"] = not thread.is_alive()

        # 2: flush the final checkpoint synchronously (idempotent).
        cursor = self._flush_final_checkpoint()
        detail["checkpoint_flushed"] = self._checkpoint_flush_completed
        detail["checkpoint_cursor"] = cursor

        # 3: make the effect-ledger WAL durable without mutating any row.
        try:
            self.effect_ledger.checkpoint_wal()
            detail["ledger_wal_checkpointed"] = True
        except Exception:
            self.logger.warning("Effect-ledger WAL checkpoint on shutdown failed", exc_info=True)

        detail["elapsed_sec"] = round(time.monotonic() - started, 6)
        self._shutdown_evidence = detail
        self.logger.info(
            "%s drain complete: %s",
            SHUTDOWN_EVIDENCE_MARKER,
            json.dumps(detail, ensure_ascii=False, sort_keys=True),
        )
        return detail

    def _slave_poll_threads(self) -> Tuple[threading.Thread, ...]:
        """Best-effort lookup of this channel's EFB poll thread."""
        try:
            from ehforwarderbot import coordinator as efb_coordinator

            threads = getattr(efb_coordinator, "slave_threads", None) or {}
            found = [t for key, t in threads.items() if str(key).startswith(self.channel_id)]
            return tuple(found)
        except Exception:
            return ()

    def _sig_handler(self, signum: int, frame: Any) -> None:
        self.logger.info("Received termination signal %s, initiating graceful shutdown < 2s...", signum)
        coordinator_obj = self._shutdown_coordinator
        if coordinator_obj is not None and not coordinator_obj.ran:
            # Run the full ordered/bounded path even if EFB's own SIGTERM handler
            # is not the one that fired (e.g. the slave registered last).
            coordinator_obj.run(trigger=f"signal:{signum}")
            return
        self.stop_polling()

    def _ensure_bootstrap_aligned(self) -> str:
        """Align local cursor with Core initial_cursor before polling."""
        return self.cursor_store.align_with_core(
            self.core,
            self.consumer_id,
            default_mode=str(self.config.get("bootstrap_mode") or "at_head"),
        )

    # ------------------------------------------------------------------ #
    # Unknown-identity hardening (RC.14 EFB reprojection idempotency)
    # ------------------------------------------------------------------ #

    def _floor_account_scope(self) -> Tuple[str, ...]:
        """Accounts the durable subscription floor must cover.

        A configured account filter wins. Otherwise the floor is recorded under the
        wildcard scope so it still applies when Core reports the account list only
        later, or when Core is unreachable at startup.
        """
        if self.account_filter:
            return tuple(sorted(self.account_filter))
        try:
            accounts = {
                str(account.get("account_id") or "")
                for account in self.core.list_accounts()
                if str(account.get("account_id") or "")
            }
        except Exception:
            accounts = set()
        return tuple(sorted(accounts)) or ("*",)

    def _ensure_subscription_floor(self) -> Dict[str, Any]:
        """Establish/verify the durable subscription provenance before polling."""
        return self.subscription_floor.ensure_from_core(
            self.core,
            self._floor_account_scope(),
            allow_core_read=self.provenance_core_read_enabled,
        )

    def _authoritative_business_origin(
        self,
        account_id: str,
        message: Mapping[str, Any],
    ) -> Tuple[Any, str]:
        """Resolve a message's stable business origin from Core's authoritative data.

        The event payload already *is* Core's projection of the object, so it is the
        first source. When it does not carry a usable origin the authoritative
        single-message projection is read from Core; a failure there is reported as an
        indeterminate reason rather than being guessed around.
        """
        origin, reason = extract_business_origin(message)
        if origin is not None:
            return origin, ""
        if reason != PROVENANCE_INDETERMINATE_MISSING_CREATED_AT:
            return None, reason
        if not self.provenance_core_read_enabled:
            return None, reason
        chat_id = str(message.get("chat_id") or "")
        message_id = str(message.get("message_id") or "")
        if not chat_id or not message_id:
            return None, reason
        try:
            projection = self.core.get_message_projection(
                account_id,
                chat_id,
                message_id,
                max_pages=self.provenance_core_read_max_pages,
            )
        except CoreError as exc:
            self.logger.warning(
                "Core provenance read for %s/%s failed: %s", account_id, message_id, exc
            )
            return None, PROVENANCE_INDETERMINATE_CORE_READ_FAILED
        except Exception as exc:  # noqa: BLE001 - a read failure must never be fatal
            self.logger.warning(
                "Core provenance read for %s/%s raised: %s", account_id, message_id, exc
            )
            return None, PROVENANCE_INDETERMINATE_CORE_READ_FAILED
        if not isinstance(projection, Mapping):
            return None, PROVENANCE_INDETERMINATE_CORE_READ_ABSENT
        origin, reason = extract_business_origin(projection)
        if origin is None:
            return None, reason or PROVENANCE_INDETERMINATE_CORE_READ_ABSENT
        return origin, ""

    def _classify_unknown_effect(
        self,
        account_id: str,
        message: Mapping[str, Any],
        *,
        has_effect_identity: bool,
    ) -> Tuple[str, str]:
        """Classify an event whose effect identity is not yet in the ledger.

        Returns ``(classification, reason)``. The decision is a function of the
        durable subscription floor and Core's authoritative business provenance only:
        ``event_type`` is not an input, because it describes Core's internal storage
        transition rather than whether this consumer owes an external effect.
        """
        if not has_effect_identity:
            return CLASSIFICATION_INDETERMINATE, PROVENANCE_INDETERMINATE_NO_EFFECT_IDENTITY
        origin, reason = self._authoritative_business_origin(account_id, message)
        floor = self.subscription_floor.get(account_id)
        if floor is None or not floor.complete:
            return CLASSIFICATION_INDETERMINATE, PROVENANCE_INDETERMINATE_FLOOR_UNAVAILABLE
        classification = floor.classify(origin, reason)
        if classification == CLASSIFICATION_INDETERMINATE and not reason:
            reason = PROVENANCE_INDETERMINATE_FLOOR_UNAVAILABLE
        return classification, reason

    def _record_provenance_classification(self, classification: str) -> None:
        if classification in self._provenance_counts:
            self._provenance_counts[classification] += 1

    def _defer_provenance(
        self,
        account_id: str,
        message: Mapping[str, Any],
        event_type: str,
        *,
        reason: str,
    ) -> None:
        """Park an indeterminate event without any external effect or terminal write."""
        attempts = self.provenance_deferrals.defer(
            account_id,
            message,
            event_type,
            reason=reason,
            cursor=str(message.get("_core_cursor") or ""),
        )
        self.logger.warning(
            "Indeterminate business provenance for %s/%s (%s); failing closed "
            "(attempt=%d, no external effect, no terminal ledger write)",
            account_id,
            str(message.get("message_id") or "<no-identity>"),
            reason,
            attempts,
        )

    def _retry_deferred_provenance(self) -> int:
        """Re-offer parked indeterminate events to the classifier.

        Called from every poll cycle and on ``media.ready``. An entry that now
        classifies is dispatched normally and removed from the parking lot; an entry
        that stays indeterminate keeps its place until its attempt budget is spent.
        Nothing here produces an external effect on its own.
        """
        if self._shutdown_in_progress.is_set():
            return 0
        entries = self.provenance_deferrals.due()
        attempted = 0
        for entry in entries:
            raw_message = entry.get("message")
            if not isinstance(raw_message, Mapping):
                self.provenance_deferrals.resolve(
                    str(entry.get("account_id") or ""), str(entry.get("message_id") or "")
                )
                continue
            message = dict(raw_message)
            message["_core_cursor"] = str(entry.get("cursor") or "")
            self._handle_message_event(
                str(entry.get("event_type") or "message.updated"),
                str(entry.get("account_id") or ""),
                message,
            )
            attempted += 1
        return attempted

    def _health_throttled(self) -> None:
        """Refresh Core health at most once per ``health_interval_sec``."""
        now = time.time()
        if self.health_interval_sec > 0 and now - self._last_health_ok < self.health_interval_sec:
            return
        self._health()
        self._last_health_ok = now

    def _health(self) -> Dict[str, Any]:
        payload = self.core.health()
        capabilities = payload.get("sender_capabilities")
        if isinstance(capabilities, Mapping):
            self.sender_capabilities = dict(capabilities)
        return payload

    @staticmethod
    def _merge_config(raw: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
        merged = {
            "core": dict(DEFAULT_CONFIG["core"]),
            **{key: value for key, value in DEFAULT_CONFIG.items() if key != "core"},
        }
        if raw:
            for key, value in raw.items():
                if key == "core" and isinstance(value, Mapping):
                    merged["core"].update(dict(value))
                else:
                    merged[key] = value
        account_ids = merged.get("account_ids")
        if account_ids in (None, ""):
            merged["account_ids"] = []
        elif isinstance(account_ids, str):
            merged["account_ids"] = [account_ids]
        elif not isinstance(account_ids, list):
            raise ValueError("account_ids must be a list of Core account IDs")
        return merged

    def _load_config(self) -> Dict[str, Any]:
        path = efb_utils.get_config_path(self.channel_id)
        if not path.exists():
            self.logger.warning("No config file at %s; using defaults", path)
            return {}
        try:
            with path.open("r", encoding="utf-8") as handle:
                loaded = yaml.safe_load(handle) or {}
        except (OSError, yaml.YAMLError) as exc:
            raise EFBException(f"Unable to read Linux WeChat Slave config: {exc}") from exc
        if not isinstance(loaded, dict):
            raise EFBException("Linux WeChat Slave config must contain a YAML mapping")
        return loaded

    def _selected_accounts(self) -> Collection[Dict[str, Any]]:
        accounts = self.core.list_accounts()
        selected = []
        for account in accounts:
            account_id = str(account.get("account_id") or "")
            if not account_id:
                continue
            if self.account_filter and account_id not in self.account_filter:
                continue
            display_name = str(account.get("display_name") or account_id)
            self.chat_mgr.set_account_name(account_id, display_name)
            runtime = account.get("runtime") if isinstance(account.get("runtime"), Mapping) else {}
            capabilities = runtime.get("sender_capabilities") if isinstance(runtime, Mapping) else None
            if isinstance(capabilities, Mapping):
                self.account_sender_capabilities[account_id] = dict(capabilities)
            selected.append(account)
        return selected

    def _sender_capabilities_for(self, account_id: str) -> Dict[str, Any]:
        capabilities = self.account_sender_capabilities.get(account_id)
        if capabilities is not None:
            return dict(capabilities)
        try:
            self._selected_accounts()
        except CoreError:
            pass
        capabilities = self.account_sender_capabilities.get(account_id)
        return dict(capabilities if capabilities is not None else self.sender_capabilities)

    def _resolve_core_chat(self, account_id: str, chat_id: str) -> Chat:
        cached = self.chat_mgr.get_by_core(account_id, chat_id)
        if cached is not None:
            return cached
        account_name = self.chat_mgr.account_name(account_id)
        if account_name == account_id:
            for account in self._selected_accounts():
                if str(account.get("account_id")) == account_id:
                    account_name = str(account.get("display_name") or account_id)
                    break
        for record in self.core.list_chats(account_id):
            chat = self.chat_mgr.build_core_chat(record, account_name)
            if str(record.get("chat_id")) == chat_id:
                return chat
        raise EFBChatNotFound(f"Core chat not found: account={account_id!r} chat={chat_id!r}")

    def get_chats(self) -> Collection[Chat]:
        try:
            self._health()
            result = []
            for account in self._selected_accounts():
                account_id = str(account["account_id"])
                account_name = str(account.get("display_name") or account_id)
                for record in self.core.list_chats(account_id):
                    result.append(self.chat_mgr.build_core_chat(record, account_name))
            return result
        except CoreError as exc:
            raise EFBException(str(exc)) from exc

    def get_chat(self, chat_uid: ChatID) -> Chat:
        uid = str(chat_uid)
        cached = self.chat_mgr.get_by_uid(uid)
        if cached is not None:
            return cached
        try:
            account_id, chat_id = decode_chat_uid(uid)
        except InvalidUID as exc:
            raise EFBChatNotFound(
                "Linux WeChat chat IDs are account-scoped encoded IDs; refresh ETM /link instead of using a raw wxid"
            ) from exc
        if self.account_filter and account_id not in self.account_filter:
            raise EFBChatNotFound(f"Account is not enabled for this slave: {account_id}")
        try:
            return self._resolve_core_chat(account_id, chat_id)
        except CoreError as exc:
            raise EFBChatNotFound(str(exc)) from exc

    def get_chat_picture(self, chat: Chat) -> BinaryIO:
        raise EFBOperationNotSupported("Core Interface Contract V1 does not expose chat avatars")

    def get_chat_member_picture(self, chat_member: ChatMember) -> BinaryIO:
        raise EFBOperationNotSupported("Core Interface Contract V1 does not expose member avatars")

    @staticmethod
    def _read_message_file(msg: Message) -> bytes:
        if msg.file is not None:
            handle = msg.file
            try:
                current = handle.tell()
            except (OSError, AttributeError):
                current = None
            try:
                handle.seek(0)
                content = handle.read()
            finally:
                if current is not None:
                    try:
                        handle.seek(current)
                    except OSError:
                        pass
            if isinstance(content, str):
                return content.encode("utf-8")
            return bytes(content)
        if msg.path:
            return Path(msg.path).read_bytes()
        raise EFBMessageError("Media message has neither file nor path")

    @staticmethod
    def _mention_member_ids(msg: Message) -> Collection[str]:
        if not msg.substitutions:
            return []
        result = []
        for target in msg.substitutions.values():
            vendor = getattr(target, "vendor_specific", {}) or {}
            core = vendor.get("core") if isinstance(vendor, dict) else None
            member_id = core.get("member_id") if isinstance(core, dict) else None
            if member_id and member_id not in result:
                result.append(str(member_id))
        return result

    @staticmethod
    def _link_text(msg: Message) -> str:
        text = msg.text or ""
        attrs = getattr(msg, "attributes", None)
        url = str(getattr(attrs, "url", "") or "")
        if url and url not in text:
            text = f"{text}\n{url}".strip()
        return text or url or "[Link]"

    @staticmethod
    def _quote_fallback(msg: Message, text: str) -> str:
        target = msg.target if isinstance(msg.target, Message) else None
        if target is None:
            return text
        target_text = target.text or target.type.name
        author = getattr(target, "author", None)
        author_name = getattr(author, "display_name", None) or getattr(author, "name", None) or ""
        prefix = f"@{author_name}：" if author_name else ""
        return f"「{prefix}{target_text}」\n---\n{text}"

    def _send_target_id(
        self,
        msg: Message,
        account_id: str,
        chat_id: str,
        capabilities: Mapping[str, Any],
    ) -> Tuple[Optional[str], bool]:
        if not isinstance(msg.target, Message):
            return None, False
        if not msg.target.chat:
            raise EFBMessageError("Reply target has no chat identity; refusing an unscoped reply")
        try:
            target_account, target_chat = decode_chat_uid(str(msg.target.chat.uid))
        except InvalidUID as exc:
            raise EFBMessageError("Reply target has an invalid chat identity") from exc
        if target_account != account_id or target_chat != chat_id:
            raise EFBMessageError(
                "Reply target belongs to a different account/chat; refusing cross-conversation delivery"
            )
        if not msg.target.uid:
            return None, True
        target_uid = str(msg.target.uid)
        mapping = self.message_mapping.resolve_target(
            self.consumer_id,
            account_id,
            chat_id,
            target_uid,
        )
        translated = str(mapping.get("core_message_id") or "") if mapping else ""
        if not translated:
            # Outgoing Telegram-originated messages retain their send_id in
            # Kettly until Core reports a final echo message identity.
            translated = str(self.echo_store.linked_echo(target_uid) or "")
        if not translated:
            return None, True
        if capabilities.get("native_reply") is False:
            return None, True
        return translated, False

    def _base_send_payload(
        self,
        msg: Message,
        account_id: str,
        chat_id: str,
        capabilities: Mapping[str, Any],
    ) -> Tuple[Dict[str, Any], str, bool]:
        target_message_id, quote_fallback = self._send_target_id(
            msg, account_id, chat_id, capabilities
        )
        core_vendor = msg.vendor_specific.get("core", {}) if isinstance(msg.vendor_specific, dict) else {}
        request_id = str(core_vendor.get("client_request_id") or msg.uid or uuid.uuid4().hex)
        payload: Dict[str, Any] = {
            "account_id": account_id,
            "chat_id": chat_id,
            "client_request_id": request_id,
        }
        if target_message_id:
            payload["target_message_id"] = target_message_id
        return payload, request_id, quote_fallback

    def _delivered_to_master(self, core_message_id: str) -> bool:
        """Whether the Telegram master logged a message for ``core_message_id``.

        Messages sent from Telegram are logged under their send UID, so the
        Core echo id is translated first. Without a logged message the master
        cannot render a native reply to it.
        """
        try:
            from efb_telegram_master.db import MsgLog  # type: ignore
        except Exception:
            return True  # unknown master: keep the native reply
        uid = self.echo_store.efb_message_id(str(core_message_id))
        try:
            return MsgLog.select().where(MsgLog.slave_message_id == uid).exists()
        except Exception:
            return True

    def send_message(self, msg: Message) -> Message:
        if msg.edit:
            raise EFBOperationNotSupported("Core Interface Contract V1 has no message-edit operation")
        try:
            account_id, chat_id = decode_chat_uid(str(msg.chat.uid))
        except (AttributeError, InvalidUID) as exc:
            raise EFBChatNotFound("Destination is not a Linux WeChat account-scoped chat") from exc
        if self.account_filter and account_id not in self.account_filter:
            raise EFBChatNotFound(f"Account is not enabled for this slave: {account_id}")

        capabilities = self._sender_capabilities_for(account_id)
        payload, request_id, quote_fallback = self._base_send_payload(
            msg, account_id, chat_id, capabilities
        )
        if quote_fallback and msg.type not in {MsgType.Text, MsgType.Link}:
            raise EFBOperationNotSupported(
                "The connected Core sender cannot safely preserve reply semantics for this non-text message"
            )
        try:
            if msg.type in {MsgType.Text, MsgType.Link}:
                text = msg.text if msg.type == MsgType.Text else self._link_text(msg)
                if quote_fallback:
                    text = self._quote_fallback(msg, text)
                if not text:
                    raise EFBMessageError("Cannot send an empty text message")
                payload["text"] = text
                mentions = list(self._mention_member_ids(msg))
                if mentions:
                    raw_limit = capabilities.get("max_mentions")
                    try:
                        mention_limit = int(raw_limit)
                    except (TypeError, ValueError):
                        mention_limit = 0
                    if mention_limit > 0 and len(mentions) > mention_limit:
                        raise EFBOperationNotSupported(
                            f"The connected Core sender supports at most {mention_limit} verified mention(s) per send"
                        )
                    payload["mention_member_ids"] = mentions
                receipt = self.core.send_text(payload, request_id)
            elif msg.type in {MsgType.Image, MsgType.Sticker, MsgType.Animation}:
                if msg.text and capabilities.get("media_caption") is False:
                    raise EFBOperationNotSupported(
                        "The connected Core sender cannot currently preserve image captions"
                    )
                content = self._read_message_file(msg)
                payload["content_base64"] = base64.b64encode(content).decode("ascii")
                filename = msg.filename or (Path(msg.path).name if msg.path else "image")
                payload["filename"] = filename
                payload["mime_type"] = msg.mime or mimetypes.guess_type(filename)[0] or "application/octet-stream"
                if msg.text:
                    payload["caption"] = msg.text
                receipt = self.core.send_image(payload, request_id)
            elif msg.type in {MsgType.File, MsgType.Video, MsgType.Voice}:
                if capabilities.get("file") is False:
                    raise EFBOperationNotSupported(
                        "The connected Core sender does not currently provide a verified arbitrary-file send primitive"
                    )
                content = self._read_message_file(msg)
                payload["content_base64"] = base64.b64encode(content).decode("ascii")
                filename = msg.filename or (Path(msg.path).name if msg.path else "attachment")
                payload["filename"] = filename
                payload["mime_type"] = msg.mime or mimetypes.guess_type(filename)[0] or "application/octet-stream"
                if msg.text:
                    payload["caption"] = msg.text
                receipt = self.core.send_file(payload, request_id)
            else:
                raise EFBMessageTypeNotSupported(f"Unsupported outgoing EFB message type: {msg.type}")
        except CoreAPIError as exc:
            if self.send_recovery.queue_rejected(
                exc, kind_for_type_name(str(msg.type)), payload, request_id, account_id, chat_id
            ):
                raise EFBMessageError("微信正在重新登录，这条消息已排队，恢复后会自动补发。") from exc
            raise EFBMessageError(f"Core rejected message [{exc.code}]: {exc.message}") from exc
        except CoreError as exc:
            raise EFBMessageError(str(exc)) from exc

        send_id = str(receipt.get("send_id") or "")
        echo_message_id = str(receipt.get("echo_message_id") or "")
        self.send_recovery.remember_sent(send_id, kind_for_type_name(str(msg.type)), payload, account_id, chat_id)
        msg.uid = MessageID(echo_message_id or send_id or request_id)
        vendor = dict(msg.vendor_specific or {})
        vendor["core"] = {
            **dict(vendor.get("core") or {}),
            "client_request_id": request_id,
            "send_receipt": dict(receipt),
            "uid_is_send_id": bool(send_id and not echo_message_id),
        }
        msg.vendor_specific = vendor
        if send_id:
            if echo_message_id:
                self.echo_store.link(send_id, echo_message_id)
            else:
                self.echo_store.mark_pending(send_id, request_id)
                with self._reaction_lock:
                    self._inflight_sends[send_id] = (account_id, chat_id, time.time())
        telegram_chat_id, telegram_message_id = self._telegram_message_ids(msg)
        self.message_mapping.record(
            self.consumer_id,
            account_id,
            chat_id,
            str(msg.uid),
            core_message_id=echo_message_id,
            telegram_chat_id=telegram_chat_id,
            telegram_message_id=telegram_message_id,
            direction="outgoing",
            sender_identity=account_id,
        )
        self._remember_message(msg)
        return msg

    def send_status(self, status: Status) -> None:
        if isinstance(status, MessageRemoval):
            raise EFBOperationNotSupported("Core Interface Contract V1 does not expose outgoing recall")
        raise EFBOperationNotSupported(f"Unsupported outgoing EFB status: {type(status).__name__}")

    def _remember_message(self, msg: Message) -> None:
        if not msg.chat or not msg.uid:
            return
        key = (str(msg.chat.uid), str(msg.uid))
        self._message_cache[key] = msg
        self._message_cache.move_to_end(key)
        while len(self._message_cache) > self._message_cache_size:
            self._message_cache.popitem(last=False)

    def get_message_by_id(self, chat: Chat, msg_id: MessageID) -> Optional[Message]:
        return self._message_cache.get((str(chat.uid), str(msg_id)))

    # -- login alerts ------------------------------------------------------------

    _STATUS_CHAT_ID = "__wechat_hub_login__"

    def _handle_account_status(self, account_id: str, payload: Mapping[str, Any]) -> None:
        """Post a login button when an account drops out, and a note when it is back."""
        self.send_recovery.on_account_status(account_id, payload)
        if not self.login_alert or not account_id:
            return
        account = payload.get("account") if isinstance(payload.get("account"), dict) else payload
        state = str(account.get("state") or "")
        if not state:
            return
        with self._login_lock:
            previous = self._account_login_state.get(account_id)
            self._account_login_state[account_id] = state
        name = str(account.get("display_name") or self.chat_mgr.account_name(account_id))
        try:
            if state == "login_required" and previous != "login_required":
                self._post_login_notice(
                    account_id,
                    f"⚠️ 微信「{name}」已退出登录，转发和发送都已暂停。\n点下面的按钮发起登录，然后在手机上确认（或扫描发来的二维码）。",
                    with_button=True,
                )
            elif state == "online" and previous == "login_required":
                self._post_login_notice(account_id, f"✅ 微信「{name}」已重新登录。")
        except Exception as exc:  # an alert must never block the event stream
            self.logger.warning("Login notice for %s failed: %s", account_id, exc)

    def _login_status_chat(self, account_id: str) -> Chat:
        name = self.chat_mgr.account_name(account_id)
        return self.chat_mgr.build_core_chat(
            {
                "account_id": account_id,
                "chat_id": self._STATUS_CHAT_ID,
                "type": "system",
                "display_name": f"{name} · 登录状态",
            }
        )

    def _post_login_notice(
        self,
        account_id: str,
        text: str,
        *,
        with_button: bool = False,
        image: Optional[bytes] = None,
    ) -> None:
        chat = self._login_status_chat(account_id)
        try:
            author = chat.get_member(SystemChatMember.SYSTEM_ID)
        except KeyError:
            author = chat.add_system_member()
        msg = Message(
            uid=MessageID(f"login-{uuid.uuid4().hex}"),
            type=MsgType.Text,
            text=text,
            chat=chat,
            author=author,
        )
        if with_button:
            msg.commands = MessageCommands(
                [MessageCommand(name="发起登录", callable_name="start_wechat_login", args=(account_id,))]
            )
        if image is not None:
            handle = tempfile.NamedTemporaryFile(prefix="wechat-login-", suffix=".png")
            handle.write(image)
            handle.flush()
            handle.seek(0)
            msg.type = MsgType.Image
            msg.file = handle
            msg.path = Path(handle.name)
            msg.filename = "login-qr.png"
            msg.mime = "image/png"
        self._deliver_message(msg)

    def start_wechat_login(self, account_id: str) -> str:
        """Button callback (ETM message command): start the login flow."""
        try:
            result = self.core.start_login(account_id)
        except CoreError as exc:
            self.logger.warning("Starting login for %s failed: %s", account_id, exc)
            return f"发起登录失败：{exc}"
        state = str(result.get("login_flow_state") or "")
        if state == "logged_in":
            return "这个号已经是登录状态了。"
        with self._login_lock:
            running = self._login_watchers.get(account_id)
            if running is None or not running.is_alive():
                watcher = threading.Thread(
                    target=self._watch_login,
                    args=(account_id,),
                    name=f"login-watch-{account_id}",
                    daemon=True,
                )
                self._login_watchers[account_id] = watcher
                watcher.start()
        return "已发起登录，请在手机微信上确认；需要扫码的话会发来二维码。"

    def _watch_login(self, account_id: str) -> None:
        """Follow the login flow: send the QR code if one is needed, report a timeout."""
        deadline = time.time() + self.login_watch_sec
        qr_sent = False
        while time.time() < deadline and not self._stop_event.is_set():
            try:
                status = self.core.login_status(account_id)
            except CoreError as exc:
                self.logger.info("Login status for %s unavailable: %s", account_id, exc)
                status = {}
            if str(status.get("auth_status") or "") == "logged_in" or status.get("state") == "online":
                return  # the account.status event posts the success note
            flow = str(status.get("login_flow_state") or "")
            if flow in {"error", "timeout"}:
                break
            if not qr_sent and status.get("snapshot_available") and flow == "waiting_for_scan":
                try:
                    png = self.core.login_snapshot(account_id)
                except CoreError as exc:
                    self.logger.info("Login QR for %s not ready: %s", account_id, exc)
                else:
                    self._post_login_notice(account_id, "请用手机微信扫描这个二维码登录。", image=png)
                    qr_sent = True
            self._stop_event.wait(3.0)
        if self._stop_event.is_set():
            return
        with self._login_lock:
            still_out = self._account_login_state.get(account_id) == "login_required"
        if still_out:
            try:
                self._post_login_notice(
                    account_id, "登录还没有完成，可以再点一次按钮重试。", with_button=True
                )
            except Exception as exc:
                self.logger.warning("Login retry notice for %s failed: %s", account_id, exc)

    # -- send recovery ---------------------------------------------------------------

    def _core_send(self, kind: str, payload: Mapping[str, Any], idempotency_key: str) -> Dict[str, Any]:
        return getattr(self.core, f"send_{kind}")(payload, idempotency_key)

    def _chat_label(self, account_id: str, chat_id: str) -> str:
        chat = self.chat_mgr.get_by_core(account_id, chat_id)
        return str(getattr(chat, "name", "") or chat_id)

    def _post_recovery_notice(self, account_id: str, text: str, *, resend_send_id: Optional[str] = None) -> None:
        chat = self._login_status_chat(account_id)
        try:
            author = chat.get_member(SystemChatMember.SYSTEM_ID)
        except KeyError:
            author = chat.add_system_member()
        msg = Message(
            uid=MessageID(f"recovery-{uuid.uuid4().hex}"),
            type=MsgType.Text,
            text=text,
            chat=chat,
            author=author,
        )
        if resend_send_id:
            msg.commands = MessageCommands(
                [MessageCommand(name="重发", callable_name="resend_uncertain_send", args=(resend_send_id,))]
            )
        self._deliver_message(msg)

    def resend_uncertain_send(self, send_id: str) -> str:
        """Button callback (ETM message command): resend an uncertain send."""
        return self.send_recovery.resend_uncertain(send_id)

    def _deliver_message(self, msg: Message) -> None:
        if getattr(coordinator, "master", None) is None:
            raise CoreUnavailableError("EFB master is not ready; event cursor will not advance")
        msg.deliver_to = coordinator.master
        self._remember_message(msg)
        try:
            coordinator.send_message(msg)
        finally:
            if msg.file is not None and not getattr(msg.file, "closed", False):
                msg.file.close()

    def _emit_removal(self, account_id: str, payload: Mapping[str, Any]) -> None:
        nested = payload.get("message") if isinstance(payload.get("message"), dict) else {}
        chat_id = str(nested.get("chat_id") or payload.get("chat_id") or "")
        message_id = str(nested.get("message_id") or payload.get("message_id") or "")
        if not chat_id or not message_id:
            self.logger.warning("Ignoring message.removed without chat_id/message_id: %r", payload)
            return
        chat = self._resolve_core_chat(account_id, chat_id)
        efb_message_id = self.echo_store.efb_message_id(message_id)
        efb_msg = Message(chat=chat, uid=MessageID(efb_message_id), type=MsgType.Text)
        if getattr(coordinator, "master", None) is None:
            raise CoreUnavailableError("EFB master is not ready; recall cursor will not advance")
        coordinator.send_status(
            MessageRemoval(source_channel=self, destination_channel=coordinator.master, message=efb_msg)
        )

    def _emit_chat_update(self, account_id: str, payload: Mapping[str, Any]) -> None:
        chat_id = str(payload.get("chat_id") or "")
        if not chat_id:
            return
        old = self.chat_mgr.get_by_core(account_id, chat_id)
        try:
            records = self.core.list_chats(account_id)
        except CoreError:
            raise
        record = next((item for item in records if str(item.get("chat_id")) == chat_id), None)
        if record is None:
            removed_uid = self.chat_mgr.remove(account_id, chat_id)
            if removed_uid and getattr(coordinator, "master", None) is not None:
                coordinator.send_status(ChatUpdates(channel=self, removed_chats=(ChatID(removed_uid),)))
            return
        chat = self.chat_mgr.build_core_chat(record, self.chat_mgr.account_name(account_id))
        if getattr(coordinator, "master", None) is not None:
            if old is None:
                coordinator.send_status(ChatUpdates(channel=self, new_chats=(chat.uid,)))
            else:
                coordinator.send_status(ChatUpdates(channel=self, modified_chats=(chat.uid,)))

    @staticmethod
    def _telegram_message_ids(msg: Message) -> Tuple[str, str]:
        vendor = msg.vendor_specific if isinstance(msg.vendor_specific, dict) else {}
        telegram = vendor.get("telegram") if isinstance(vendor.get("telegram"), Mapping) else {}
        return (
            str(telegram.get("chat_id") or vendor.get("telegram_chat_id") or ""),
            str(telegram.get("message_id") or vendor.get("telegram_message_id") or ""),
        )

    def _handle_send_update(self, payload: Mapping[str, Any], event_account_id: str = "") -> None:
        receipt = payload.get("send") if isinstance(payload.get("send"), dict) else payload
        send_id = str(receipt.get("send_id") or "")
        echo_message_id = str(receipt.get("echo_message_id") or "")
        if send_id and echo_message_id:
            self.echo_store.link(send_id, echo_message_id)
            account_id = str(receipt.get("account_id") or event_account_id or "")
            chat_id = str(receipt.get("chat_id") or "")
            if account_id and chat_id:
                self.message_mapping.link_core_message(
                    self.consumer_id,
                    account_id,
                    chat_id,
                    send_id,
                    echo_message_id,
                )
        status = str(receipt.get("status") or "")
        if status == "submitted":
            # Do not present upstream FSM success as confirmed delivery.  The
            # final WeChat/Core identity is only known after the DB echo is
            # uniquely reconciled.
            self.logger.info(
                "Core send %s 已提交、等待微信确认 (delivery_certainty=%s)",
                send_id or "<unknown>",
                str(receipt.get("delivery_certainty") or "pending_confirmation"),
            )
        elif status == "sent":
            self.logger.info(
                "Core send %s 已由微信确认 (echo_message_id=%s)",
                send_id or "<unknown>",
                echo_message_id or "<unknown>",
            )
        elif status == "uncertain":
            error = payload.get("error") if isinstance(payload.get("error"), Mapping) else {}
            details = payload.get("details") if isinstance(payload.get("details"), Mapping) else {}
            self.logger.warning(
                "Core delivery certainty is unknown for send %s [%s]: %s details=%r",
                send_id or "<unknown>",
                str(error.get("code") or "agent_wechat_delivery_unknown"),
                str(error.get("message") or "upstream response was not received"),
                dict(details),
            )
        if send_id and (echo_message_id or status in {"sent", "failed", "uncertain"}):
            with self._reaction_lock:
                self._inflight_sends.pop(send_id, None)
        self.send_recovery.on_send_update(
            send_id, status, echo_message_id, str(receipt.get("account_id") or event_account_id or "")
        )
        self._queue_send_reaction(receipt)

    # -- send echo hold ---------------------------------------------------------

    def _awaiting_send_echo(self, account_id: str, chat_id: str) -> bool:
        """Is a send from Telegram to this chat still waiting for its echo link?"""
        if not self._inflight_sends:
            return False
        now = time.time()
        with self._reaction_lock:
            for send_id, (acc, chat, started) in list(self._inflight_sends.items()):
                if now - started > self.send_echo_wait_sec:
                    self._inflight_sends.pop(send_id, None)
                elif acc == account_id and chat == chat_id:
                    return True
        return False

    def _hold_pending(
        self,
        account_id: str,
        message: Mapping[str, Any],
        event_type: str,
        effect_id: str,
        reason: str,
    ) -> None:
        """Park a message durably; the pending-media retry path re-evaluates it."""
        now = time.time()
        current = self.effect_ledger.get_effect(self.consumer_id, effect_id)
        details = current.get("details", {}) if current else {}
        try:
            attempt_count = int(details.get("attempt_count") or 1)
        except (TypeError, ValueError):
            attempt_count = 1
        try:
            deadline_at = float(details.get("deadline_at"))
        except (TypeError, ValueError):
            deadline_at = now + self.media_retry_deadline_sec
        self.effect_ledger.mark_media_pending(
            self.consumer_id,
            effect_id,
            account_id=account_id,
            message_id=str(message.get("message_id") or ""),
            event_type=event_type,
            message=message,
            media_id=str(message.get("media_id") or ""),
            attempt_count=attempt_count,
            next_retry_at=now,
            deadline_at=deadline_at,
            last_error=reason,
        )
        self.logger.info("Holding effect %s: %s", effect_id, reason)

    def _close_held_echo(
        self,
        account_id: str,
        core_message_id: str,
        efb_message_id: str,
        event_type: str,
    ) -> None:
        """A held message turned out to be the echo of our own send: close it."""
        effect_id = self.effect_ledger.compute_effect_id(account_id, core_message_id)
        if self.effect_ledger.get_effect_status(self.consumer_id, effect_id) != STATE_PENDING_MEDIA:
            return
        self.effect_ledger.mark_delivered(
            self.consumer_id,
            effect_id,
            efb_uid=efb_message_id,
            event_type=event_type,
            details={"suppressed_send_echo": True},
        )

    # -- send outcome reactions ------------------------------------------------

    _REACTION_RETRY_SEC = 3.0
    _REACTION_MAX_ATTEMPTS = 10

    def _queue_send_reaction(self, receipt: Mapping[str, Any]) -> None:
        """Remember a reaction for the Telegram message behind a Core send."""
        kind = str(receipt.get("kind") or "")
        status = str(receipt.get("status") or "")
        send_id = str(receipt.get("send_id") or "")
        mode = self.send_reaction_modes.get(kind)
        if not send_id or mode is None:
            return
        emoji = self.send_reaction_emoji.get(status, "")
        if mode == "problems":
            if status == "sent":
                # Only clear a problem mark shown earlier; success stays quiet.
                with self._reaction_lock:
                    if self._problem_marked.pop(send_id, None) is None:
                        return
                emoji = ""
            elif status in {"uncertain", "failed"} and emoji:
                with self._reaction_lock:
                    self._problem_marked[send_id] = time.time()
                    cutoff = time.time() - 86400
                    for old in [k for k, t in self._problem_marked.items() if t < cutoff]:
                        self._problem_marked.pop(old, None)
            else:
                return
        elif not emoji:
            return
        with self._reaction_lock:
            self._pending_reactions[send_id] = {"emoji": emoji, "attempts": 0, "due": 0.0}
        self._flush_send_reactions()

    def _flush_send_reactions(self) -> None:
        if not self._pending_reactions:
            return
        now = time.time()
        with self._reaction_lock:
            due = [(sid, dict(item)) for sid, item in self._pending_reactions.items() if item["due"] <= now]
        for send_id, item in due:
            target = self._telegram_target(send_id)
            if target is None:
                attempts = item["attempts"] + 1
                with self._reaction_lock:
                    if attempts >= self._REACTION_MAX_ATTEMPTS:
                        self._pending_reactions.pop(send_id, None)
                        self.logger.info("No Telegram message found for send %s; reaction dropped", send_id)
                    elif send_id in self._pending_reactions:
                        self._pending_reactions[send_id].update(
                            attempts=attempts, due=now + self._REACTION_RETRY_SEC
                        )
                continue
            with self._reaction_lock:
                if self._pending_reactions.get(send_id, {}).get("emoji") == item["emoji"]:
                    self._pending_reactions.pop(send_id, None)
            self._set_telegram_reaction(target[0], target[1], item["emoji"])

    @staticmethod
    def _telegram_target(send_id: str) -> Optional[Tuple[str, int]]:
        """Telegram chat/message of the message that produced ``send_id``.

        The Telegram master logs the slave UID it got back from
        ``send_message`` (the Core send_id) next to the Telegram message ID.
        """
        try:
            from efb_telegram_master.db import MsgLog  # type: ignore
        except Exception:
            return None
        try:
            row = (
                MsgLog.select()
                .where(MsgLog.slave_message_id == send_id)
                .order_by(MsgLog.time.desc())
                .first()
            )
        except Exception:
            return None
        if row is None:
            return None
        chat_id, _, message_id = str(row.master_msg_id).rpartition(".")
        try:
            return chat_id, int(message_id)
        except ValueError:
            return None

    def _set_telegram_reaction(self, chat_id: str, message_id: int, emoji: str) -> None:
        master = getattr(coordinator, "master", None)
        bot = getattr(getattr(master, "bot_manager", None), "_bot", None)
        if bot is None:
            return
        try:
            bot._post(
                "setMessageReaction",
                {
                    "chat_id": chat_id,
                    "message_id": message_id,
                    "reaction": [{"type": "emoji", "emoji": emoji}] if emoji else [],
                },
            )
        except Exception as exc:  # reactions are cosmetic; never break polling
            self.logger.warning("Telegram reaction %s on %s/%s failed: %s", emoji, chat_id, message_id, exc)

    @staticmethod
    def _is_media_message(message: Mapping[str, Any]) -> bool:
        return str(message.get("type") or "") in {"image", "sticker", "voice", "video", "file"}

    def _defer_media(
        self,
        account_id: str,
        message: Mapping[str, Any],
        event_type: str,
        effect_id: str,
        error: Exception,
    ) -> str:
        now = time.time()
        current = self.effect_ledger.get_effect(self.consumer_id, effect_id)
        details = current.get("details", {}) if current else {}
        try:
            previous_attempts = int(details.get("attempt_count") or 0)
        except (TypeError, ValueError):
            previous_attempts = 0
        attempt_count = previous_attempts + 1
        try:
            deadline_at = float(details.get("deadline_at"))
        except (TypeError, ValueError):
            deadline_at = now + self.media_retry_deadline_sec
        delay = min(
            self.media_retry_base_sec * (2 ** max(0, attempt_count - 1)),
            self.media_retry_max_sec,
        )
        next_retry_at = now + delay
        message_id = str(message.get("message_id") or "")
        media_id = str(message.get("media_id") or "")
        self.effect_ledger.mark_media_pending(
            self.consumer_id,
            effect_id,
            account_id=account_id,
            message_id=message_id,
            event_type=event_type,
            message=message,
            media_id=media_id,
            attempt_count=attempt_count,
            next_retry_at=next_retry_at,
            deadline_at=deadline_at,
            last_error=str(error),
        )
        if attempt_count >= self.media_retry_max_attempts or now >= deadline_at:
            reason = (
                f"media retry exhausted after {attempt_count} attempt(s)"
                if attempt_count >= self.media_retry_max_attempts
                else "media retry deadline exceeded"
            )
            # Defect R14-EFB-MISSING-MEDIA-TERMINAL. Running out of *active* retry
            # budget is not evidence that the media will never exist. Core publishes
            # media asynchronously, so a ``missing_file`` / ``original_pending``
            # observation means "not materialised yet", not "gone". Terminalising here
            # permanently destroyed the recovery path for every object still in flight
            # when the budget ran out (12 post-floor objects during the Retry1
            # preflight), and the resulting terminal row then suppressed every later
            # authoritative update as SUPPRESSED_TERMINAL.
            #
            # The effect is therefore *parked*: active polling stops, but the row stays
            # PENDING_MEDIA -- non-terminal -- so a later ``media.ready`` or
            # ``message.updated`` re-activates it and delivers exactly once. Genuinely
            # unrecoverable media never reaches this branch: it raises
            # MediaPermanentError and fails closed in ``_fail_media``.
            self.effect_ledger.mark_media_parked(
                self.consumer_id,
                effect_id,
                account_id=account_id,
                message_id=message_id,
                event_type=event_type,
                reason=reason,
                attempt_count=attempt_count,
                media_id=media_id,
                message=dict(message),
                last_error=str(error),
            )
            self.logger.warning(
                "Parked media effect %s as recoverable after %s; it remains "
                "PENDING_MEDIA and is retried when Core reports the media available",
                effect_id,
                reason,
            )
            return STATE_PENDING_MEDIA
        self.logger.info(
            "Deferred media effect %s in %s (attempt=%d next_retry_at=%.3f): %s",
            effect_id,
            STATE_PENDING_MEDIA,
            attempt_count,
            next_retry_at,
            error,
        )
        return STATE_PENDING_MEDIA

    def _fail_media(
        self,
        account_id: str,
        message: Mapping[str, Any],
        event_type: str,
        effect_id: str,
        error: Exception,
    ) -> None:
        message_id = str(message.get("message_id") or "")
        self.effect_ledger.mark_media_failed(
            self.consumer_id,
            effect_id,
            account_id=account_id,
            message_id=message_id,
            event_type=event_type,
            reason=str(error),
            details={
                "message": dict(message),
                "media_id": str(message.get("media_id") or ""),
                "last_error": str(error),
                "permanent": True,
            },
        )
        self.logger.error("Media effect %s failed closed: %s", effect_id, error)

    def _handle_message_event(
        self,
        event_type: str,
        account_id: str,
        message: Mapping[str, Any],
    ) -> None:
        chat_id = str(message.get("chat_id") or "")
        if not chat_id:
            self.logger.warning("Ignoring %s without chat_id: %r", event_type, message)
            return
        core_message_id = str(message.get("message_id") or "")
        efb_message_id = self.echo_store.efb_message_id(core_message_id) if core_message_id else ""
        if (
            core_message_id
            and efb_message_id != core_message_id
            and str(message.get("direction") or "") == "outgoing"
        ):
            self.logger.debug(
                "Suppressing reconciled outgoing Core echo %s (EFB uid %s)",
                core_message_id,
                efb_message_id,
            )
            self._close_held_echo(account_id, core_message_id, efb_message_id, event_type)
            return

        is_media = self._is_media_message(message)
        if is_media and not core_message_id:
            self.logger.error("Ignoring media message without durable message_id: %r", message)
            return
        effect_id = (
            self.effect_ledger.compute_effect_id(account_id, core_message_id)
            if core_message_id
            else ""
        )
        current_status = (
            self.effect_ledger.get_effect_status(self.consumer_id, effect_id)
            if effect_id
            else None
        )
        # Terminal effects are closed for every event type.
        #
        # Defect R14-EFB-R1: this branch used to be gated on
        # ``(event_type == "message.created" or is_media)``. A Core re-projection is
        # delivered as ``message.updated``, so that gate let a non-media replay fall
        # through to the delivery path and re-open an effect that was already
        # DELIVERED (a second external delivery) or already MEDIA_FAILED (a second
        # terminal row). Terminal state is a property of the durable effect identity,
        # not of the event that happens to carry it, so it must be evaluated before
        # any projection-dependent branching.
        if current_status in {STATE_DELIVERED, STATE_MEDIA_FAILED}:
            self.logger.info(
                "Suppressing replay of terminal effect %s (state=%s event_type=%s)",
                effect_id,
                current_status,
                event_type,
            )
            if core_message_id and self.provenance_deferrals.count():
                self.provenance_deferrals.resolve(account_id, core_message_id)
            return
        if current_status in {STATE_RESERVED, STATE_UNCERTAIN}:
            self.logger.warning(
                "Suppressing delivery for effect %s: state is %s; fail-closed (%s)",
                effect_id,
                current_status,
                BLOCKED_UNCERTAIN_EFFECT,
            )
            return
        # Defect R14-EFB-R2, hardened. An event whose effect identity is not yet in the
        # ledger must be decided from *business provenance*, never from the event type.
        # ``message.updated`` only says Core already held the row and its digest
        # changed; it says nothing about whether this consumer owed a delivery. Two
        # opposite cases arrive under that one event type:
        #
        #   * the object predates this consumer's subscription floor — Core is
        #     re-projecting an object that was never ours to deliver (suppress);
        #   * the object was created at or after the floor and this consumer has never
        #     seen it — it is a genuine first business effect (deliver).
        #
        # When Core cannot supply the origin, nothing is guessed: no external effect,
        # no terminal ledger write, and the event is parked for a bounded retry.
        classification = CLASSIFICATION_KNOWN_EFFECT
        classification_reason = ""
        if current_status is None:
            classification, classification_reason = self._classify_unknown_effect(
                account_id,
                message,
                has_effect_identity=bool(effect_id),
            )
            self._record_provenance_classification(classification)
        if classification == CLASSIFICATION_REPROJECTION_OF_PREEXISTING_OBJECT:
            self.logger.info(
                "Suppressing reprojection of preexisting object %s (event_type=%s, floor=%s)",
                effect_id or "<no-identity>",
                event_type,
                self.subscription_floor.get(account_id),
            )
            return
        if classification == CLASSIFICATION_INDETERMINATE:
            self._defer_provenance(
                account_id,
                message,
                event_type,
                reason=classification_reason,
            )
            return

        if (
            effect_id
            and str(message.get("direction") or "") == "outgoing"
            and self._awaiting_send_echo(account_id, chat_id)
        ):
            self._hold_pending(account_id, message, event_type, effect_id, "waiting for send echo")
            return
        if effect_id and self.delivery_mode == "ordered" and current_status == STATE_PENDING_MEDIA:
            message = self._with_first_cursor(effect_id, message)
        if effect_id and self._ordered_blocked(account_id, chat_id, message, effect_id):
            self._hold_for_order(account_id, message, event_type, effect_id)
            return

        chat = self._resolve_core_chat(account_id, chat_id)
        try:
            efb_msg = self.message_builder.build(message, chat)
        except MediaPendingError as exc:
            if not is_media or not effect_id:
                raise
            self._defer_media(account_id, message, event_type, effect_id, exc)
            return
        except MediaPermanentError as exc:
            if not is_media or not effect_id:
                raise
            self._fail_media(account_id, message, event_type, effect_id, exc)
            return

        reserved = False
        if effect_id and current_status == STATE_PENDING_MEDIA:
            reserved = self.effect_ledger.reserve_pending_effect(
                self.consumer_id,
                effect_id,
                details={"chat_id": chat_id, "media_status": "ready"},
            )
        elif effect_id:
            # Reaching this branch means the effect identity is unestablished and the
            # object's business provenance places it at or after the subscription
            # floor: this is the consumer's first business effect for the object, so it
            # is reserved and delivered like any other first delivery. Every terminal,
            # fail-closed, pending, re-projection and indeterminate combination
            # returned above.
            reserved, reserve_state = self.effect_ledger.reserve_effect(
                self.consumer_id,
                effect_id,
                account_id=account_id,
                message_id=core_message_id,
                event_type=event_type,
                details={"chat_id": chat_id},
            )
            if not reserved:
                self.logger.warning(
                    "Failed to reserve effect %s: state is %s; fail-closed (%s)",
                    effect_id,
                    reserve_state,
                    BLOCKED_UNCERTAIN_EFFECT,
                )
                if efb_msg.file is not None and not getattr(efb_msg.file, "closed", False):
                    efb_msg.file.close()
                return
        if current_status == STATE_PENDING_MEDIA and not reserved:
            if efb_msg.file is not None and not getattr(efb_msg.file, "closed", False):
                efb_msg.file.close()
            self.logger.warning("Pending media effect %s could not transition to RESERVED", effect_id)
            return

        if core_message_id:
            efb_msg.uid = MessageID(efb_message_id or core_message_id)
        if isinstance(efb_msg.target, Message) and efb_msg.target.uid:
            efb_msg.target.uid = MessageID(self.echo_store.efb_message_id(str(efb_msg.target.uid)))
        # The edit flag is derived from the effect state machine, not from the event
        # type: a terminal effect is closed above and an effect that reaches this point
        # is either a first business effect or a pending-media progression, so it is
        # never an edit of an already-delivered master message.
        efb_msg.edit = bool(
            current_status is not None
            and current_status != STATE_PENDING_MEDIA
            and classification == CLASSIFICATION_KNOWN_EFFECT
        )

        try:
            self._deliver_message(efb_msg)
        except Exception as exc:
            if reserved and effect_id:
                self.effect_ledger.mark_uncertain(
                    self.consumer_id,
                    effect_id,
                    reason=f"Exception during external delivery: {exc}",
                )
            raise

        telegram_chat_id, telegram_message_id = self._telegram_message_ids(efb_msg)
        author = message.get("author") if isinstance(message.get("author"), Mapping) else {}
        sender_identity = str(
            message.get("sender_id")
            or author.get("member_id")
            or author.get("sender_id")
            or ""
        )
        try:
            self.message_mapping.record(
                self.consumer_id,
                account_id,
                chat_id,
                str(efb_msg.uid),
                core_message_id=core_message_id,
                telegram_chat_id=telegram_chat_id,
                telegram_message_id=telegram_message_id,
                direction=str(message.get("direction") or ""),
                sender_identity=sender_identity,
                core_cursor=str(message.get("_core_cursor") or ""),
            )
        except Exception as exc:
            if reserved and effect_id:
                self.effect_ledger.mark_uncertain(
                    self.consumer_id,
                    effect_id,
                    reason=f"Mapping persistence failed after external delivery: {exc}",
                )
            raise

        if reserved and effect_id:
            self.effect_ledger.mark_delivered(
                self.consumer_id,
                effect_id,
                efb_uid=str(efb_msg.uid),
                event_type=event_type,
                details={"chat_id": chat_id, "type": str(efb_msg.type)},
            )
        if core_message_id and self.provenance_deferrals.count():
            # The event resolved, so it must stop being retried.
            self.provenance_deferrals.resolve(account_id, core_message_id)

    @staticmethod
    def _order_key(message: Mapping[str, Any]) -> Tuple[float, int]:
        raw = message.get("created_at")
        try:
            created_at = float(raw or 0.0)
        except (TypeError, ValueError):
            try:
                created_at = datetime.fromisoformat(
                    str(raw).replace("Z", "+00:00")
                ).timestamp()
            except ValueError:
                created_at = 0.0
        try:
            cursor = int(message.get("_core_cursor") or 0)
        except (TypeError, ValueError):
            cursor = 0
        return created_at, cursor

    def _with_first_cursor(self, effect_id: str, message: Mapping[str, Any]) -> Mapping[str, Any]:
        """Keep the cursor of the first event seen for an effect.

        A later ``message.updated`` (e.g. media became ready) carries a newer
        cursor; ordering must still use the message's original position.
        """
        current = self.effect_ledger.get_effect(self.consumer_id, effect_id)
        stored = ((current or {}).get("details") or {}).get("message") or {}
        try:
            first = int(stored.get("_core_cursor") or 0)
            now = int(message.get("_core_cursor") or 0)
        except (TypeError, ValueError):
            return message
        if first and (not now or first < now):
            return dict(message, _core_cursor=str(first))
        return message

    def _ordered_blocked(
        self,
        account_id: str,
        chat_id: str,
        message: Mapping[str, Any],
        effect_id: str,
    ) -> bool:
        """In ordered mode, is an earlier message of this chat still pending?

        A pending row only blocks for ``ordered_max_wait_sec`` after it first
        became pending, so one slow or lost media never stalls a chat for long.
        """
        if self.delivery_mode != "ordered":
            return False
        if self.ordered_scope == "media" and not self._is_media_message(message):
            return False
        key = self._order_key(message)
        now = time.time()
        for row in self.effect_ledger.pending_media(self.consumer_id):
            if str(row.get("effect_id") or "") == effect_id:
                continue
            if str(row.get("account_id") or "") != account_id:
                continue
            details = row.get("details") or {}
            if details.get(DETAIL_RETRY_PARKED):
                continue
            other = details.get("message")
            if not isinstance(other, dict) or str(other.get("chat_id") or "") != chat_id:
                continue
            if self._order_key(other) >= key:
                continue
            try:
                pending_since = float(details.get("deadline_at")) - self.media_retry_deadline_sec
            except (TypeError, ValueError):
                pending_since = now
            if now - pending_since < self.ordered_max_wait_sec:
                return True
        return False

    def _hold_for_order(
        self,
        account_id: str,
        message: Mapping[str, Any],
        event_type: str,
        effect_id: str,
    ) -> None:
        """Park a ready message durably until the earlier ones are delivered."""
        now = time.time()
        current = self.effect_ledger.get_effect(self.consumer_id, effect_id)
        details = current.get("details", {}) if current else {}
        try:
            attempt_count = int(details.get("attempt_count") or 1)
        except (TypeError, ValueError):
            attempt_count = 1
        try:
            deadline_at = float(details.get("deadline_at"))
        except (TypeError, ValueError):
            deadline_at = now + self.media_retry_deadline_sec
        self.effect_ledger.mark_media_pending(
            self.consumer_id,
            effect_id,
            account_id=account_id,
            message_id=str(message.get("message_id") or ""),
            event_type=event_type,
            message=message,
            media_id=str(message.get("media_id") or ""),
            attempt_count=attempt_count,
            # Due on the next poll tick; it is also re-checked right after the
            # blocking message within the same retry pass.
            next_retry_at=now,
            deadline_at=deadline_at,
            last_error="held for ordered delivery",
        )
        self.logger.info("Holding effect %s for ordered delivery", effect_id)

    def _retry_pending_media(
        self,
        *,
        account_id: str = "",
        ready_media: Optional[Mapping[str, Any]] = None,
    ) -> int:
        if self._shutdown_in_progress.is_set():
            return 0
        media_id = str(ready_media.get("media_id") or "") if ready_media else ""
        pending = self.effect_ledger.pending_media(
            self.consumer_id,
            due_before=None if ready_media else time.time(),
            media_id=media_id,
        )
        if self.delivery_mode == "ordered":
            pending = sorted(
                pending,
                key=lambda r: self._order_key(
                    (r.get("details") or {}).get("message") or {}
                ),
            )
        attempted = 0
        for row in pending:
            if account_id and str(row.get("account_id") or "") != account_id:
                continue
            raw_message = row.get("details", {}).get("message")
            if not isinstance(raw_message, dict):
                self.effect_ledger.mark_media_failed(
                    self.consumer_id,
                    str(row["effect_id"]),
                    account_id=str(row["account_id"]),
                    message_id=str(row["message_id"]),
                    event_type=str(row["event_type"]),
                    reason="pending media record has no recoverable message payload",
                )
                continue
            message = dict(raw_message)
            # A scheduled retry probes the authoritative media endpoint. A
            # media.ready event additionally supplies the latest role/status.
            message["media_status"] = str(
                ready_media.get("status") if ready_media else "ready"
            )
            if ready_media and ready_media.get("role"):
                message["media_role"] = str(ready_media["role"])
            if ready_media and ready_media.get("filename"):
                message["filename"] = str(ready_media["filename"])
            if ready_media and ready_media.get("mime_type"):
                message["mime_type"] = str(ready_media["mime_type"])
            self._handle_message_event(
                "message.updated",
                str(row["account_id"]),
                message,
            )
            attempted += 1
        if ready_media and attempted and self.delivery_mode == "ordered":
            # Media became ready: release the messages held behind it now
            # instead of waiting for the next poll tick.
            attempted += self._retry_pending_media(account_id=account_id)
        return attempted

    def _handle_event(self, event: Mapping[str, Any]) -> None:
        if self._shutdown_in_progress.is_set():
            # Shutdown has begun: the durable state has already been flushed and
            # no further external side effect may be produced. Events are left
            # unprocessed on purpose so that the preserved cursor replays them
            # after the next start, where the effect ledger decides delivery.
            self.logger.info(
                "Suppressing event processing during shutdown (event_type=%s)",
                str(event.get("event_type") or ""),
            )
            return
        event_type = str(event.get("event_type") or "")
        account_id = str(event.get("account_id") or "")
        if self.account_filter and account_id not in self.account_filter:
            return
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}

        if event_type in {"message.created", "message.updated"}:
            raw_message = payload.get("message") if isinstance(payload.get("message"), dict) else payload
            message = dict(raw_message)
            message["_core_cursor"] = str(event.get("cursor") or "")
            self._handle_message_event(event_type, account_id, message)
        elif event_type == "message.removed":
            self._emit_removal(account_id, payload)
        elif event_type == "chat.updated":
            self._emit_chat_update(account_id, payload)
        elif event_type == "send.updated":
            self._handle_send_update(payload, account_id)
        elif event_type == "media.ready":
            media = payload.get("media") if isinstance(payload.get("media"), dict) else payload
            self._retry_pending_media(account_id=account_id, ready_media=media)
            self._retry_deferred_provenance()
        elif event_type == "account.status":
            self.logger.info("Core event %s for %s: %r", event_type, account_id, payload)
            self._handle_account_status(account_id, payload)
        else:
            # Contract explicitly requires future unknown event types to be tolerated.
            self.logger.warning("Ignoring unknown Core event type %r", event_type)

    def _flush_final_checkpoint(self) -> Optional[int]:
        """Flush the final checkpoint to Core before exiting (Defect D3).

        Idempotent and thread-safe: repeated shutdown signals, or a concurrent
        call from ``poll()``'s ``finally`` block and the shutdown coordinator,
        must not double-flush. Returns the flushed cursor (or ``None``).
        """
        with self._checkpoint_flush_lock:
            if self._checkpoint_flush_completed:
                return self._checkpoint_flush_cursor
            cursor: Optional[int] = None
            try:
                current_cursor = self.cursor_store.load(default=None)
                if current_cursor is not None:
                    cur_int = int(current_cursor)
                    self.core.checkpoint_events(
                        self.consumer_id,
                        cur_int,
                        subscription_account_id=next(iter(self.account_filter), "") if len(self.account_filter) == 1 else "",
                    )
                    cursor = cur_int
                    self.logger.info("Flushed final checkpoint %s on shutdown", cur_int)
                else:
                    self.logger.debug("No local cursor present; nothing to flush on shutdown")
            except Exception as exc:
                self.logger.debug("Final checkpoint flush on shutdown skipped/failed: %s", exc)
                return None
            self._checkpoint_flush_completed = True
            self._checkpoint_flush_cursor = cursor
            return cursor

    def poll_once(self, poll_timeout: Optional[int] = None) -> int:
        """Process one Core event page. Exposed for deterministic integration tests."""
        self._health_throttled()
        if not self.subscription_floor.scoped_accounts():
            # Core may have been unreachable at construction time; without a durable
            # floor every unknown identity fails closed, so keep trying to establish it.
            self._ensure_subscription_floor()
        self._retry_pending_media()
        self._retry_deferred_provenance()
        self._flush_send_reactions()
        cursor = self.cursor_store.load(default=None)
        if cursor is None:
            cursor = self._ensure_bootstrap_aligned()
        single_account = next(iter(self.account_filter)) if len(self.account_filter) == 1 else None
        effective_timeout = self.poll_timeout if poll_timeout is None else poll_timeout
        page = self.core.poll_events(
            after=cursor,
            consumer_id=self.consumer_id,
            timeout=effective_timeout,
            limit=self.event_limit,
            account_id=single_account,
        )
        events = page.get("events")
        if not isinstance(events, list):
            raise CoreUnavailableError("Core poll response has no events list")
        processed = 0
        for event in events:
            if not isinstance(event, dict):
                continue
            self._handle_event(event)
            event_cursor = str(event.get("cursor") or "")
            if event_cursor:
                # Cursor is persisted only after local processing succeeds.
                self.cursor_store.save(event_cursor)
            event_id = str(event.get("event_id") or "")
            if event_id:
                try:
                    self.core.ack_events(self.consumer_id, [event_id])
                except CoreError as exc:
                    # Contract states ack does not replace cursor persistence.
                    self.logger.warning("Core event ack failed after cursor persistence: %s", exc)
            processed += 1
        has_more = bool(page.get("has_more"))
        stream_head = page.get("stream_head_cursor")
        current_cursor = self.cursor_store.load(default="0")
        try:
            cur_int = int(current_cursor or "0")
        except ValueError:
            cur_int = 0
        checkpoint_cursor = cur_int if has_more else (int(stream_head) if stream_head is not None else cur_int)
        try:
            self.core.checkpoint_events(
                self.consumer_id,
                checkpoint_cursor,
                subscription_account_id=single_account or "",
            )
        except Exception as exc:
            self.logger.warning("Core event checkpoint failed after local processing: %s", exc)
        return processed

    def poll(self) -> None:
        # The master channel only exists after all slaves are constructed, so the
        # shutdown coordinator is installed here as well (idempotent) to close the
        # slave-before-master initialisation race in ehforwarderbot.__main__.init.
        self._install_shutdown_coordinator()
        backoff = self.poll_interval
        try:
            while not self._stop_event.is_set():
                try:
                    # Bound long-poll duration during loop to <= 1s so stop_event is checked
                    # promptly, guaranteeing shutdown drain completes in < 2s (Defect D3).
                    loop_timeout = min(self.poll_timeout, 1)
                    processed = self.poll_once(poll_timeout=loop_timeout)
                    backoff = self.poll_interval
                    if processed == 0:
                        self._stop_event.wait(self.poll_interval)
                except CoreContractError:
                    self.logger.exception("Core contract is incompatible; polling stopped")
                    raise
                except Exception as exc:
                    if self._stop_event.is_set():
                        self.logger.info("Polling loop cleanly terminated on stop event")
                        break
                    self.logger.exception("Core polling iteration failed; cursor retained for retry: %s", exc)
                    self._stop_event.wait(backoff)
                    backoff = min(max(backoff * 2, self.poll_interval), 30.0)
        finally:
            self._flush_final_checkpoint()

    def stop_polling(self) -> None:
        """Idempotent, re-entrant stop of the Core poll loop (Retry3)."""
        with self._stop_polling_lock:
            already_stopped = self._stop_event.is_set()
            self._stop_event.set()
        if already_stopped:
            self.logger.debug("stop_polling() called again; stop event already set")
        try:
            if hasattr(self.core, "session") and self.core.session:
                self.core.session.close()
        except Exception:
            pass

    @efb_utils.extra(name="Core status", desc="Show WeChat Core V1 health and configured account states.")
    def core_status(self, _: str = "") -> str:
        try:
            payload = {
                "health": self.core.health(),
                "accounts": [
                    account
                    for account in self.core.list_accounts()
                    if not self.account_filter or str(account.get("account_id")) in self.account_filter
                ],
                "event_cursor": self.cursor_store.load(),
            }
            return json.dumps(payload, ensure_ascii=False, indent=2)
        except CoreError as exc:
            return f"Core unavailable: {exc}"


# Compatibility alias for code importing the upstream class name.  The class
# implementation itself is Linux/Core-backed; no ComWechatRobot backend remains.
ComWeChatChannel = LinuxWeChatChannel
