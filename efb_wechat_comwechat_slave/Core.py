"""HTTP adapter for WeChat Core Interface Contract V1.

This module intentionally has no EFB dependency. It is the only backend-facing
layer used by the Linux slave; Windows Hook/ComWechatRobot APIs must not leak
past this boundary.
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional
from urllib.parse import quote

import requests


CONTRACT_VERSION = 1


class CoreError(RuntimeError):
    """Base class for Core adapter failures."""


class CoreUnavailableError(CoreError):
    """Core could not be reached or returned a non-protocol response."""


class CoreContractError(CoreError):
    """Core advertises an unsupported interface contract."""


class CoreAPIError(CoreError):
    """Structured non-2xx Core response."""

    def __init__(self, status_code: int, code: str, message: str, details: Optional[Mapping[str, Any]] = None):
        super().__init__(f"Core API {status_code} {code}: {message}")
        self.status_code = status_code
        self.code = code
        self.message = message
        self.details = dict(details or {})


@dataclass(frozen=True)
class CoreMedia:
    content: bytes
    mime_type: str
    filename: Optional[str]
    media_id: str
    role: str
    status: str


class CoreClient:
    """Small synchronous client for the frozen Core V1 contract."""

    def __init__(
        self,
        base_url: str,
        *,
        timeout: float = 10.0,
        verify_tls: bool = True,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = float(timeout)
        self.verify_tls = bool(verify_tls)
        self.session = session or requests.Session()

    def _url(self, path: str) -> str:
        return f"{self.base_url}{path}"

    def _request(self, method: str, path: str, **kwargs: Any) -> requests.Response:
        kwargs.setdefault("timeout", self.timeout)
        kwargs.setdefault("verify", self.verify_tls)
        try:
            response = self.session.request(method, self._url(path), **kwargs)
        except requests.RequestException as exc:
            raise CoreUnavailableError(f"Core request failed: {method} {path}: {exc}") from exc

        if response.ok:
            return response

        try:
            payload = response.json()
        except ValueError as exc:
            raise CoreUnavailableError(
                f"Core returned non-JSON error response: {method} {path}: HTTP {response.status_code}"
            ) from exc
        error = payload.get("error") if isinstance(payload, dict) else None
        if not isinstance(error, dict):
            raise CoreUnavailableError(
                f"Core returned malformed error response: {method} {path}: HTTP {response.status_code}"
            )
        raise CoreAPIError(
            response.status_code,
            str(error.get("code") or "unknown_error"),
            str(error.get("message") or "Core request failed"),
            error.get("details") if isinstance(error.get("details"), dict) else {},
        )

    @staticmethod
    def _json(response: requests.Response) -> Dict[str, Any]:
        try:
            payload = response.json()
        except ValueError as exc:
            raise CoreUnavailableError("Core returned a non-JSON success response") from exc
        if not isinstance(payload, dict):
            raise CoreUnavailableError("Core returned a non-object JSON response")
        return payload

    def health(self) -> Dict[str, Any]:
        payload = self._json(self._request("GET", "/health"))
        version = payload.get("contract_version")
        if version != CONTRACT_VERSION:
            raise CoreContractError(
                f"Unsupported Core contract_version={version!r}; this slave requires {CONTRACT_VERSION}"
            )
        return payload

    def list_accounts(self) -> List[Dict[str, Any]]:
        payload = self._json(self._request("GET", "/v1/accounts"))
        accounts = payload.get("accounts")
        if not isinstance(accounts, list):
            raise CoreUnavailableError("Core /v1/accounts response has no accounts list")
        return [item for item in accounts if isinstance(item, dict)]

    def start_login(self, account_id: str) -> Dict[str, Any]:
        """Ask Core/Runtime to start the WeChat login flow for an account."""
        path = f"/v1/runtime/accounts/{quote(str(account_id), safe='')}/login"
        return self._json(self._request("POST", path, json={}))

    def login_status(self, account_id: str) -> Dict[str, Any]:
        path = f"/v1/runtime/accounts/{quote(str(account_id), safe='')}/login"
        return self._json(self._request("GET", path))

    def login_snapshot(self, account_id: str) -> bytes:
        """PNG of the login QR code; raises CoreAPIError(409) while not ready."""
        path = f"/v1/runtime/accounts/{quote(str(account_id), safe='')}/login/snapshot"
        response = self._request("GET", path)
        content = response.content
        if not content.startswith(b"\x89PNG\r\n\x1a\n"):
            raise CoreUnavailableError("Core login snapshot is not a PNG")
        return content

    def list_chats(self, account_id: str, *, query: str = "", limit: int = 200) -> List[Dict[str, Any]]:
        """Return all pages for one account without assuming cursor syntax."""
        rows: List[Dict[str, Any]] = []
        cursor = ""
        seen_cursors = set()
        while True:
            params: Dict[str, Any] = {"limit": max(1, min(int(limit), 200))}
            if cursor:
                params["cursor"] = cursor
            if query:
                params["query"] = query
            path = f"/v1/accounts/{quote(account_id, safe='')}/chats"
            payload = self._json(self._request("GET", path, params=params))
            chats = payload.get("chats")
            if not isinstance(chats, list):
                raise CoreUnavailableError("Core chat list response has no chats list")
            rows.extend(item for item in chats if isinstance(item, dict))
            next_cursor = payload.get("next_cursor")
            if not isinstance(next_cursor, str) or not next_cursor or next_cursor == cursor:
                break
            if next_cursor in seen_cursors:
                raise CoreUnavailableError("Core chat pagination returned a cursor loop")
            seen_cursors.add(next_cursor)
            cursor = next_cursor
        return rows

    def poll_events(
        self,
        *,
        after: str,
        consumer_id: str,
        timeout: int = 15,
        limit: int = 50,
        account_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "after": after or "0",
            "consumer_id": consumer_id,
            "timeout": max(0, min(int(timeout), 30)),
            "limit": max(1, min(int(limit), 200)),
        }
        if account_id:
            params["account_id"] = account_id
        return self._json(self._request("GET", "/v1/events/poll", params=params))

    def ack_events(self, consumer_id: str, event_ids: Iterable[str]) -> Dict[str, Any]:
        event_ids = [str(item) for item in event_ids if str(item)]
        if not event_ids:
            return {"consumer_id": consumer_id, "acked_event_ids": [], "acked_count": 0}
        return self._json(
            self._request(
                "POST",
                "/v1/events/ack",
                json={"consumer_id": consumer_id, "event_ids": event_ids},
            )
        )

    def checkpoint_events(
        self,
        consumer_id: str,
        processed_through_cursor: int,
        *,
        last_event_id: str = "",
        subscription_account_id: str = "",
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "consumer_id": consumer_id,
            "processed_through_cursor": int(processed_through_cursor),
        }
        if last_event_id:
            payload["last_event_id"] = last_event_id
        if subscription_account_id:
            payload["subscription_account_id"] = subscription_account_id
        return self._json(
            self._request(
                "POST",
                "/v1/events/checkpoint",
                json=payload,
            )
        )

    def get_bootstrap_provenance(self, consumer_id: str) -> Optional[Dict[str, Any]]:
        path = f"/v1/consumers/{quote(consumer_id, safe='')}/bootstrap"
        try:
            return self._json(self._request("GET", path))
        except CoreAPIError as exc:
            if exc.status_code == 404:
                return None
            raise

    def get_message_projection(
        self,
        account_id: str,
        chat_id: str,
        message_id: str,
        *,
        max_pages: int = 2,
        page_limit: int = 200,
    ) -> Optional[Dict[str, Any]]:
        """Read one message's authoritative Core projection by identity.

        Used as the fallback business-provenance source when an event payload does not
        carry the projection fields. The V1 contract exposes no single-message
        endpoint, so this is a bounded forward scan of the chat's message index:

        * the scan is capped at ``max_pages`` pages of ``page_limit`` rows;
        * a message that is not found within the bound returns ``None`` — the caller
          must fail closed rather than treat "not found" as "pre-existing";
        * ``CoreAPIError`` / ``CoreUnavailableError`` propagate so the caller can
          classify the failure as indeterminate and retryable.

        Returns ``None`` when the chat itself is unknown to Core.
        """
        pages = max(1, int(max_pages))
        limit = max(1, min(int(page_limit), 200))
        cursor = ""
        for _ in range(pages):
            params: Dict[str, Any] = {"limit": limit}
            if cursor:
                params["cursor"] = cursor
            path = (
                f"/v1/accounts/{quote(account_id, safe='')}"
                f"/chats/{quote(chat_id, safe='')}/messages"
            )
            try:
                payload = self._json(self._request("GET", path, params=params))
            except CoreAPIError as exc:
                if exc.status_code == 404:
                    return None
                raise
            messages = payload.get("messages")
            if not isinstance(messages, list):
                raise CoreUnavailableError("Core message listing response has no messages list")
            for item in messages:
                if isinstance(item, dict) and str(item.get("message_id") or "") == str(message_id):
                    return item
            next_cursor = payload.get("next_cursor")
            if not isinstance(next_cursor, str) or not next_cursor or next_cursor == cursor:
                return None
            cursor = next_cursor
        return None

    def bootstrap_consumer(
        self,
        consumer_id: str,
        *,
        mode: str = "at_head",
        window: Optional[Mapping[str, Any]] = None,
        operator_token: str = "",
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "consumer_id": consumer_id,
            "mode": mode,
        }
        if window:
            payload["window"] = dict(window)
        if operator_token:
            payload["operator_token"] = operator_token
        return self._json(self._request("POST", "/v1/consumers/bootstrap", json=payload))

    def rebootstrap_consumer(
        self,
        consumer_id: str,
        *,
        mode: str = "bounded_window",
        window: Optional[Mapping[str, Any]] = None,
        operator_token: str = "",
        quiescence_evidence: str = "",
        reason: str = "",
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "consumer_id": consumer_id,
            "mode": mode,
            "operator_token": operator_token,
            "quiescence_evidence": quiescence_evidence,
        }
        if window:
            payload["window"] = dict(window)
        if reason:
            payload["reason"] = reason
        return self._json(self._request("POST", "/v1/consumers/rebootstrap", json=payload))

    def get_media(self, account_id: str, media_id: str) -> CoreMedia:
        path = f"/v1/media/{quote(media_id, safe='')}"
        response = self._request("GET", path, params={"account_id": account_id})
        if response.status_code == 202:
            # Defect R14-EFB-MEDIA-PENDING-AS-TERMINAL. Core answers
            # ``202 media_pending`` while the WeChat client is still downloading the
            # object. ``202`` is a 2xx status, so ``_request`` returns it as a
            # *success* and the JSON error body was handed back as if it were media
            # bytes. With no ``X-Media-Role`` header the role check in
            # ``CoreMessage._media_file`` then raised ``MediaPermanentError`` and
            # ``ComWechat._fail_media`` wrote a terminal ``MEDIA_FAILED`` row for
            # media that Core explicitly says is still coming -- the same
            # "not materialised yet recorded as never" class as
            # R14-EFB-MISSING-MEDIA-TERMINAL.
            #
            # Surfacing it as the structured ``202 media_pending`` Core error is
            # what makes the documented retryable classification in
            # ``CoreMessage._is_transient_media_error`` reachable, so the object is
            # deferred with backoff and parks recoverably instead of failing closed.
            raise CoreAPIError(
                202,
                "media_pending",
                "Media content is still downloading in the WeChat client",
            )
        content_type = response.headers.get("Content-Type", "application/octet-stream").split(";", 1)[0].strip()
        disposition = response.headers.get("Content-Disposition", "")
        filename: Optional[str] = None
        if "filename=" in disposition:
            filename = disposition.split("filename=", 1)[1].strip().strip('"') or None
        return CoreMedia(
            content=response.content,
            mime_type=content_type or "application/octet-stream",
            filename=filename,
            media_id=response.headers.get("X-Media-Id", media_id),
            role=str(response.headers.get("X-Media-Role") or ""),
            status=str(response.headers.get("X-Media-Status") or ""),
        )

    def _send(self, kind: str, payload: Mapping[str, Any], idempotency_key: str) -> Dict[str, Any]:
        return self._json(
            self._request(
                "POST",
                f"/v1/send/{kind}",
                json=dict(payload),
                headers={"Idempotency-Key": idempotency_key} if idempotency_key else {},
            )
        )

    def send_text(self, payload: Mapping[str, Any], idempotency_key: str) -> Dict[str, Any]:
        return self._send("text", payload, idempotency_key)

    def send_image(self, payload: Mapping[str, Any], idempotency_key: str) -> Dict[str, Any]:
        return self._send("image", payload, idempotency_key)

    def send_file(self, payload: Mapping[str, Any], idempotency_key: str) -> Dict[str, Any]:
        return self._send("file", payload, idempotency_key)


class CursorStore:
    """Tiny durable cursor store using atomic replace, not a TTL delivery cache."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    def load(self, default: Optional[str] = None) -> Optional[str]:
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
            cursor = payload.get("cursor") if isinstance(payload, dict) else None
            return str(cursor) if cursor not in (None, "") else default
        except FileNotFoundError:
            return default
        except (OSError, ValueError, TypeError):
            return default

    def has_cursor(self) -> bool:
        return self.load(default=None) is not None

    def save(self, cursor: str) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, temp_name = tempfile.mkstemp(prefix=self.path.name + ".", suffix=".tmp", dir=str(self.path.parent))
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump({"cursor": str(cursor)}, handle, ensure_ascii=False, separators=(",", ":"))
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_name, self.path)
        finally:
            if os.path.exists(temp_name):
                os.unlink(temp_name)

    def align_with_core(
        self,
        core: CoreClient,
        consumer_id: str,
        *,
        default_mode: str = "at_head",
        window: Optional[Mapping[str, Any]] = None,
        operator_token: str = "",
    ) -> str:
        """Align local cursor with Core bootstrap state.

        If the consumer is not bootstrapped in Core, executes governed bootstrap
        using default_mode.
        If local cursor is missing or regressed behind Core initial_cursor, advances
        it to Core initial_cursor so full historical catchup from 0 is blocked.
        """
        local_cur_str = self.load(default=None)
        try:
            prov = core.get_bootstrap_provenance(consumer_id)
        except (CoreAPIError, CoreUnavailableError):
            prov = None

        if prov is None:
            try:
                prov = core.bootstrap_consumer(
                    consumer_id,
                    mode=default_mode,
                    window=window,
                    operator_token=operator_token,
                )
            except (CoreAPIError, CoreUnavailableError):
                prov = None

        core_init = int(prov.get("initial_cursor") or 0) if prov is not None else 0
        local_cur = int(local_cur_str) if local_cur_str is not None else None

        if local_cur is None or local_cur < core_init:
            effective = core_init
            self.save(str(effective))
            return str(effective)
        return str(local_cur)


class EchoStore:
    """Persist Core send-id to eventual message-id aliases across restarts.

    Kettly ETM stores the UID returned by ``send_message`` immediately.  Core
    may only learn the final WeChat echo message ID later via ``send.updated``.
    Persisting the alias lets the slave keep ETM's already-stored UID stable
    while translating reply targets and recalls back to the real Core ID.
    """

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self._loaded = False
        self._send_to_echo: Dict[str, str] = {}
        self._pending: Dict[str, str] = {}

    def _load(self) -> None:
        if self._loaded:
            return
        self._loaded = True
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, ValueError, TypeError):
            return
        if not isinstance(payload, dict):
            return
        aliases = payload.get("send_to_echo")
        pending = payload.get("pending")
        if isinstance(aliases, dict):
            self._send_to_echo = {
                str(send_id): str(echo_id)
                for send_id, echo_id in aliases.items()
                if send_id and echo_id
            }
        if isinstance(pending, dict):
            self._pending = {
                str(send_id): str(request_id)
                for send_id, request_id in pending.items()
                if send_id and request_id
            }

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, temp_name = tempfile.mkstemp(prefix=self.path.name + ".", suffix=".tmp", dir=str(self.path.parent))
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(
                    {"send_to_echo": self._send_to_echo, "pending": self._pending},
                    handle,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                )
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_name, self.path)
        finally:
            if os.path.exists(temp_name):
                os.unlink(temp_name)

    def mark_pending(self, send_id: str, request_id: str) -> None:
        if not send_id:
            return
        self._load()
        self._pending[str(send_id)] = str(request_id or send_id)
        self._save()

    def link(self, send_id: str, echo_message_id: str) -> None:
        if not send_id or not echo_message_id:
            return
        self._load()
        self._send_to_echo[str(send_id)] = str(echo_message_id)
        self._pending.pop(str(send_id), None)
        self._save()

    def core_message_id(self, efb_message_id: str) -> Optional[str]:
        """Translate an ETM-stored send UID into Core's final message ID.

        ``None`` means the UID is a known outgoing send whose echo is not yet
        known, so it must not be passed to Core as ``target_message_id``.
        Unknown IDs are already Core message IDs and are returned unchanged.
        """
        value = str(efb_message_id)
        self._load()
        if value in self._send_to_echo:
            return self._send_to_echo[value]
        if value in self._pending:
            return None
        return value

    def efb_message_id(self, core_message_id: str) -> str:
        """Translate final Core echo/recall/target ID to ETM's stable send UID."""
        value = str(core_message_id)
        self._load()
        for send_id, echo_id in self._send_to_echo.items():
            if echo_id == value:
                return send_id
        return value

    def linked_echo(self, send_id: str) -> Optional[str]:
        self._load()
        return self._send_to_echo.get(str(send_id))
