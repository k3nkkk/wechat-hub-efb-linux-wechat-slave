"""Normalized Core message -> EFB message conversion.

The shape follows ComWechat's MsgProcess/MsgWrapper split, but consumes the
frozen normalized Core schema instead of Hook callback dictionaries.
"""

from __future__ import annotations

import mimetypes
import tempfile
from pathlib import Path
import re
from typing import Any, Dict, Mapping, Optional, Tuple

from ehforwarderbot import Message, MsgType
from ehforwarderbot.chat import Chat, GroupChat
from ehforwarderbot.message import LinkAttribute, LocationAttribute, Substitutions
from ehforwarderbot.types import MessageID

from .ChatMgr import ChatMgr
from .Core import CoreAPIError, CoreClient, CoreUnavailableError


class MediaSelectionError(RuntimeError):
    """Core media cannot be used as the requested final artifact."""


class MediaPendingError(MediaSelectionError):
    """The original media artifact is not ready yet and may be retried."""


class MediaPermanentError(MediaSelectionError):
    """The original media artifact failed permanently and must not fall back."""


#: HTTP statuses that mean "the upstream could not serve this right now" rather
#: than "this object will never be servable": the retry budget must own them.
#: 408/425/429 are the IANA transient client-facing statuses and every 5xx is a
#: server-side failure, including Core's ``504 agent_wechat_error``.
_TRANSIENT_MEDIA_STATUSES = frozenset({408, 425, 429})


def _is_transient_media_error(exc: CoreAPIError) -> bool:
    """Whether a structured Core media error is recoverable on a later attempt.

    Defect R14-EFB-MEDIA-RETRY-HEAD-OF-LINE. ``_media_file`` used to convert only
    ``404 media_not_found`` into :class:`MediaPendingError` and re-raise everything
    else verbatim. ``ComWechat._handle_message_event`` catches only
    ``MediaPendingError`` / ``MediaPermanentError``, so a transient upstream
    failure such as ``504 agent_wechat_error: CDN download failed`` escaped
    ``_retry_pending_media`` -> ``poll_once`` and aborted the whole polling
    iteration *before* any new event was fetched. One unservable sticker therefore
    blocked every later text and media message, the cursor stopped advancing, and
    the attempt counter was never incremented, so the existing backoff and
    recoverable-park machinery never engaged.

    Classification is deliberately explicit and fail-closed:

    * transient (``202 media_pending``, 408/425/429, every 5xx) -> retryable;
    * ``404 media_unsupported`` -> the upstream already gave a final verdict;
    * every other status (401/403, unknown 4xx, ``404 account_not_found``, ...) is
      *not* a media-readiness statement and keeps propagating unchanged, so it
      still fails closed instead of being disguised as a retryable media error.
    """
    status = int(exc.status_code)
    if status == 202 and exc.code == "media_pending":
        return True
    if status in _TRANSIENT_MEDIA_STATUSES:
        return True
    return 500 <= status < 600


class CoreMessageBuilder:
    def __init__(self, core: CoreClient, chats: ChatMgr) -> None:
        self.core = core
        self.chats = chats

    def _media_file(
        self,
        account_id: str,
        media_id: str,
        filename: Optional[str],
        mime_type: Optional[str],
    ) -> Tuple[Any, str, str, Path]:
        try:
            media = self.core.get_media(account_id, media_id)
        except CoreUnavailableError as exc:
            # Core unreachable / non-protocol answer: the media may well be
            # servable later, so this is a retryable media observation, never a
            # reason to abandon the polling iteration.
            raise MediaPendingError(
                f"Core media endpoint is unavailable for {media_id}: {exc}"
            ) from exc
        except CoreAPIError as exc:
            if exc.status_code == 404 and exc.code == "media_not_found":
                raise MediaPendingError(f"Core media bytes are not ready for {media_id}") from exc
            if exc.status_code == 404 and exc.code == "media_unsupported":
                # The upstream stated a final verdict for this object. Terminal,
                # so it leaves the retry set instead of blocking it forever.
                raise MediaPermanentError(
                    f"Core reports media {media_id} as unsupported by the upstream"
                ) from exc
            if _is_transient_media_error(exc):
                raise MediaPendingError(
                    f"Core media fetch for {media_id} failed transiently: {exc}"
                ) from exc
            raise
        if media.role != "original":
            raise MediaPermanentError(
                f"Core returned media role {media.role!r} for {media_id}; expected 'original'"
            )
        if media.status != "ready":
            raise MediaPendingError(
                f"Core returned original media {media_id} with status {media.status!r}; expected 'ready'"
            )
        if not media.content:
            raise MediaPermanentError(f"Core returned an empty original media artifact for {media_id}")
        final_name = filename or media.filename or media_id
        final_mime = mime_type or media.mime_type or mimetypes.guess_type(final_name)[0] or "application/octet-stream"
        suffix = Path(final_name).suffix
        handle = tempfile.NamedTemporaryFile(prefix="efb-core-", suffix=suffix)
        handle.write(media.content)
        handle.flush()
        handle.seek(0)
        return handle, final_name, final_mime, Path(handle.name)

    _QUOTE_SEPARATOR = "- - - - - - - - - - - - - - -"
    _QUOTE_MAX_LEN = 80
    _QUOTED_XML_KINDS = (
        ("<img", "[图片]"),
        ("<videomsg", "[视频]"),
        ("<voicemsg", "[语音]"),
        ("<emoji", "[表情]"),
        ("<location", "[位置]"),
    )

    @classmethod
    def _quoted_summary(cls, content: str) -> str:
        """One-line summary of the quoted message (its content may be XML)."""
        text = str(content or "").strip()
        if text.startswith("<"):
            lowered = text.lower()
            for marker, label in cls._QUOTED_XML_KINDS:
                if marker in lowered:
                    return label
            title = re.search(r"<title>(.*?)</title>", text, re.S)
            if title and title.group(1).strip():
                text = title.group(1)
            else:
                text = re.sub(r"<[^>]+>", " ", text)
        text = re.sub(r"\s+", " ", text).strip()
        if len(text) > cls._QUOTE_MAX_LEN:
            text = text[: cls._QUOTE_MAX_LEN - 1] + "…"
        return text or "[消息]"

    @classmethod
    def _quote_prefix(cls, message: Mapping[str, Any]) -> str:
        attrs = message.get("attributes") if isinstance(message.get("attributes"), Mapping) else {}
        reply = attrs.get("reply") if isinstance(attrs.get("reply"), Mapping) else {}
        if not reply:
            return ""
        sender = str(reply.get("sender") or "").strip()
        summary = cls._quoted_summary(str(reply.get("content") or ""))
        quoted = f"{sender}: {summary}" if sender else summary
        return f"「{quoted}」\n{cls._QUOTE_SEPARATOR}\n"

    def _substitutions(self, message: Mapping[str, Any], chat: Chat, account_id: str) -> Optional[Substitutions]:
        raw = message.get("substitutions")
        if not isinstance(raw, list):
            return None
        result: Dict[Tuple[int, int], Any] = {}
        for item in raw:
            if not isinstance(item, dict):
                continue
            try:
                start = int(item.get("start"))
                end = int(item.get("end"))
            except (TypeError, ValueError):
                continue
            member_id = str(item.get("member_id") or "")
            if not member_id or end <= start:
                continue
            if isinstance(chat, GroupChat):
                member = self.chats.upsert_author(
                    chat,
                    account_id,
                    {
                        "member_id": member_id,
                        "display_name": item.get("display_name") or member_id,
                        "alias": item.get("alias"),
                        "is_self": bool(item.get("is_self")),
                    },
                )
                result[(start, end)] = member
            elif item.get("is_self") and chat.self:
                result[(start, end)] = chat.self
        return Substitutions(result) if result else None

    @staticmethod
    def _bool_value(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return value != 0
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "self", "outgoing"}
        return bool(value)

    def _author_data(self, message: Mapping[str, Any], account_id: str) -> Dict[str, Any]:
        nested = message.get("author") if isinstance(message.get("author"), Mapping) else {}
        sender_id = str(
            message.get("sender_id")
            or nested.get("sender_id")
            or nested.get("member_id")
            or ""
        )
        sender_name = str(
            message.get("sender_name")
            or nested.get("sender_name")
            or nested.get("display_name")
            or sender_id
            or "Unknown"
        )
        if "is_self" in message:
            is_self = self._bool_value(message.get("is_self"))
        elif "is_self" in nested:
            is_self = self._bool_value(nested.get("is_self"))
        else:
            direction = str(message.get("direction") or "").strip().lower()
            if direction not in {"incoming", "outgoing"}:
                raise ValueError("Core message has no reliable is_self or direction identity")
            is_self = direction == "outgoing"
        if not sender_id:
            if is_self:
                sender_id = account_id
            else:
                raise ValueError("Non-self Core message has no stable sender_id")
        return {
            "member_id": sender_id,
            "display_name": sender_name,
            "alias": nested.get("alias"),
            "is_self": is_self,
        }

    def build(self, message: Mapping[str, Any], chat: Chat) -> Message:
        account_id = str(message.get("account_id") or "")
        message_id = str(message.get("message_id") or "")
        author_data = self._author_data(message, account_id)
        author = self.chats.upsert_author(chat, account_id, author_data)
        msg_type = str(message.get("type") or "unsupported")
        text = str(message.get("text") or "")
        vendor_specific = {
            "core": {
                "account_id": account_id,
                "chat_id": str(message.get("chat_id") or ""),
                "message_id": message_id,
                "direction": str(message.get("direction") or ""),
                "sender_id": author_data["member_id"],
                "sender_name": author_data["display_name"],
                "is_self": author_data["is_self"],
                "created_at": message.get("created_at"),
                "attributes": dict(message.get("attributes") or {}),
                "message": dict(message.get("vendor_specific") or {}),
            }
        }
        reply_prefix = ""
        efb_msg = Message(
            chat=chat,
            author=author,
            uid=MessageID(message_id),
            text=text,
            vendor_specific=vendor_specific,
        )

        if msg_type == "text":
            efb_msg.type = MsgType.Text
        elif msg_type == "image":
            efb_msg.type = MsgType.Image
        elif msg_type == "sticker":
            efb_msg.type = MsgType.Sticker
        elif msg_type == "voice":
            efb_msg.type = MsgType.Voice
        elif msg_type == "video":
            efb_msg.type = MsgType.Video
        elif msg_type == "file":
            efb_msg.type = MsgType.File
        elif msg_type == "link":
            attrs = message.get("attributes") if isinstance(message.get("attributes"), dict) else {}
            url = str(attrs.get("url") or "")
            if url:
                efb_msg.type = MsgType.Link
                efb_msg.attributes = LinkAttribute(
                    title=str(attrs.get("title") or text or url),
                    description=str(attrs.get("description") or "") or None,
                    image=str(attrs.get("image") or "") or None,
                    url=url,
                )
            else:
                efb_msg.type = MsgType.Text
                if not efb_msg.text:
                    efb_msg.text = "[Link]"
        elif msg_type == "location":
            attrs = message.get("attributes") if isinstance(message.get("attributes"), dict) else {}
            try:
                latitude = float(attrs["latitude"])
                longitude = float(attrs["longitude"])
            except (KeyError, TypeError, ValueError):
                efb_msg.type = MsgType.Unsupported
                efb_msg.text = efb_msg.text or "[Location message without coordinates]"
            else:
                efb_msg.type = MsgType.Location
                efb_msg.attributes = LocationAttribute(latitude=latitude, longitude=longitude)
        elif msg_type == "reply":
            # A WeChat quote reply. Without a resolvable target message the
            # quoted content is shown inline so the context is not lost.
            efb_msg.type = MsgType.Text
            if efb_msg.text.startswith("[Link] "):
                efb_msg.text = efb_msg.text[len("[Link] "):]
            if not str(message.get("target_message_id") or ""):
                reply_prefix = self._quote_prefix(message)
            efb_msg.text = efb_msg.text or "[引用]"
        elif msg_type in {"contact_card", "system"}:
            efb_msg.type = MsgType.Text
            efb_msg.text = efb_msg.text or f"[{msg_type.replace('_', ' ')}]"
        else:
            efb_msg.type = MsgType.Unsupported
            efb_msg.text = efb_msg.text or f"[Unsupported WeChat message: {msg_type}]"

        if efb_msg.type in {MsgType.Image, MsgType.Sticker, MsgType.Voice, MsgType.Video, MsgType.File}:
            media_id = str(message.get("media_id") or "")
            media_role = str(message.get("media_role") or "")
            media_status = str(message.get("media_status") or "")
            if not media_id:
                raise MediaPendingError(f"{msg_type} original media reference is not available")
            if media_role != "original":
                raise MediaPermanentError(
                    f"{msg_type} media {media_id} has role {media_role!r}; thumbnail cannot be final"
                )
            if media_status != "ready":
                if media_status in {"decode_failed", "encrypted_or_unknown", "unsupported_hevc"}:
                    raise MediaPermanentError(
                        f"{msg_type} original media {media_id} failed with status {media_status!r}"
                    )
                raise MediaPendingError(
                    f"{msg_type} original media {media_id} is not ready (status={media_status!r})"
                )
            file_obj, filename, mime_type, path = self._media_file(
                account_id,
                media_id,
                str(message.get("filename") or "") or None,
                str(message.get("mime_type") or "") or None,
            )
            efb_msg.file = file_obj
            efb_msg.filename = filename
            efb_msg.mime = mime_type
            efb_msg.path = path

        substitutions = self._substitutions(message, chat, account_id)
        if reply_prefix:
            efb_msg.text = reply_prefix + efb_msg.text
            if substitutions:
                shift = len(reply_prefix)
                substitutions = Substitutions(
                    {(start + shift, end + shift): value for (start, end), value in substitutions.items()}
                )
        if substitutions:
            efb_msg.substitutions = substitutions

        target_message_id = str(message.get("target_message_id") or "")
        if target_message_id:
            # Kettly ETM resolves replies by target.uid + target.chat.  Full target
            # content/author is not required for its DB lookup.
            efb_msg.target = Message(chat=chat, uid=MessageID(target_message_id), type=MsgType.Text)

        return efb_msg
