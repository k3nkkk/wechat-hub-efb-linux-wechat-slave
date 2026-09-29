"""Minimal standalone stubs for ehforwarderbot framework types.
Used for offline unit testing when ehforwarderbot is not installed.
"""

from __future__ import annotations

import enum
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional


class MsgType(enum.Enum):
    Text = "Text"
    Image = "Image"
    Audio = "Audio"
    Video = "Video"
    Voice = "Voice"
    Animation = "Animation"
    File = "File"
    Link = "Link"
    Location = "Location"
    Sticker = "Sticker"
    Status = "Status"
    Unsupported = "Unsupported"


class ChatID(str):
    pass


class InstanceID(str):
    pass


class MessageID(str):
    pass


class Chat:
    def __init__(self, uid: str = "", name: str = "", alias: str = "", channel: Any = None, vendor_specific: Optional[Dict[str, Any]] = None, **kwargs: Any):
        self.uid = ChatID(uid)
        self.name = name
        self.alias = alias
        self.channel = channel
        self.vendor_specific = vendor_specific or {}
        self.self = ChatMember(uid="__self__", name="Self", alias="Self", channel=channel)


class GroupChat(Chat):
    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.members: List[ChatMember] = []

    def get_member(self, uid: str) -> ChatMember:
        for member in self.members:
            if str(member.uid) == str(uid):
                return member
        raise KeyError(uid)

    def add_member(self, uid: str, name: str, alias: Optional[str] = None, vendor_specific: Optional[Dict[str, Any]] = None) -> ChatMember:
        member = ChatMember(
            uid=uid,
            name=name,
            alias=alias,
            channel=self.channel,
            vendor_specific=vendor_specific,
        )
        self.members.append(member)
        return member


class PrivateChat(Chat):
    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.other = ChatMember(uid=self.uid, name=self.name, alias=self.alias, channel=self.channel)


class SystemChat(Chat):
    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.members: List[ChatMember] = []

    def get_member(self, uid: str) -> ChatMember:
        for member in self.members:
            if str(member.uid) == str(uid):
                return member
        raise KeyError(uid)

    def add_system_member(self) -> ChatMember:
        member = SystemChatMember(
            uid=SystemChatMember.SYSTEM_ID,
            name="System",
            channel=self.channel,
        )
        self.members.append(member)
        return member


class ChatMember:
    def __init__(self, uid: str = "", name: str = "", alias: str = "", channel: Any = None, vendor_specific: Optional[Dict[str, Any]] = None, **kwargs: Any):
        self.uid = uid
        self.name = name
        self.alias = alias
        self.channel = channel
        self.vendor_specific = vendor_specific or {}


class SystemChatMember(ChatMember):
    SYSTEM_ID = "__system__"


class Message:
    def __init__(
        self,
        uid: Optional[str] = None,
        type: MsgType = MsgType.Text,
        text: str = "",
        chat: Optional[Chat] = None,
        author: Optional[ChatMember] = None,
        target: Optional[Message] = None,
        edit: bool = False,
        file: Any = None,
        filename: Optional[str] = None,
        mime: Optional[str] = None,
        vendor_specific: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ):
        self.uid = MessageID(uid) if uid else None
        self.type = type
        self.text = text
        self.chat = chat
        self.author = author
        self.target = target
        self.edit = edit
        self.file = file
        self.filename = filename
        self.mime = mime
        self.path = kwargs.get("path")
        self.attributes = None
        self.substitutions = kwargs.get("substitutions")
        self.vendor_specific: Dict[str, Any] = vendor_specific or {}


class MessageCommand:
    def __init__(self, name: str, callable_name: str, args: Any = None, kwargs: Any = None):
        self.name = name
        self.callable_name = callable_name
        self.args = tuple(args or ())
        self.kwargs = dict(kwargs or {})


class MessageCommands(list):
    pass


class Status:
    pass


class ChatUpdates(Status):
    def __init__(self, chat: Chat):
        self.chat = chat


class MessageRemoval(Status):
    def __init__(self, source_channel: Any, destination_channel: Any, message: Message):
        self.source_channel = source_channel
        self.destination_channel = destination_channel
        self.message = message


class EFBException(Exception):
    pass


class EFBMessageError(EFBException):
    pass


class EFBMessageTypeNotSupported(EFBException):
    pass


class EFBChatNotFound(EFBException):
    pass


class EFBOperationNotSupported(EFBException):
    pass


class SlaveChannel:
    channel_id: str = "slave"
    channel_name: str = "Slave Channel"
    channel_emoji: str = "🤖"

    def __init__(self, instance_id: Optional[InstanceID] = None):
        self.instance_id = instance_id or InstanceID("default")

    def send_message(self, message: Message) -> Optional[Message]:
        return None

    def send_status(self, status: Status) -> None:
        pass

    def get_chat(self, chat_uid: ChatID) -> Optional[Chat]:
        return None

    def get_chats(self) -> List[Chat]:
        return []


class _Coordinator:
    def __init__(self):
        self.master = None
        self.slaves: Dict[str, SlaveChannel] = {}

    def send_message(self, message: Message) -> Optional[Message]:
        if self.master:
            return self.master.send_message(message)
        return None

    def send_status(self, status: Status) -> None:
        if self.master:
            self.master.send_status(status)


coordinator = _Coordinator()


class LinkAttribute:
    def __init__(self, title: str = "", description: str = "", url: str = "", image: str = ""):
        self.title = title
        self.description = description
        self.url = url
        self.image = image


class LocationAttribute:
    def __init__(self, latitude: float = 0.0, longitude: float = 0.0, title: str = ""):
        self.latitude = latitude
        self.longitude = longitude
        self.title = title


class Substitutions(dict):
    """Like EFB's Substitutions: a mapping of (start, end) -> chat/member."""


def extra(name: str = "", desc: str = ""):
    def decorator(fn):
        return fn
    return decorator


def get_data_path(channel_id: str) -> Path:
    base = Path(tempfile.gettempdir()) / "efb_test_data" / channel_id
    base.mkdir(parents=True, exist_ok=True)
    return base


def install_stubs():
    """Install stub modules into sys.modules if ehforwarderbot is missing."""
    import types

    mod_efb = types.ModuleType("ehforwarderbot")
    mod_efb.Message = Message
    mod_efb.MsgType = MsgType
    mod_efb.Status = Status
    mod_efb.coordinator = coordinator

    mod_types = types.ModuleType("ehforwarderbot.types")
    mod_types.ChatID = ChatID
    mod_types.InstanceID = InstanceID
    mod_types.MessageID = MessageID
    mod_efb.types = mod_types

    mod_chat = types.ModuleType("ehforwarderbot.chat")
    mod_chat.Chat = Chat
    mod_chat.GroupChat = GroupChat
    mod_chat.PrivateChat = PrivateChat
    mod_chat.SystemChat = SystemChat
    mod_chat.ChatMember = ChatMember
    mod_chat.SystemChatMember = SystemChatMember
    mod_efb.chat = mod_chat

    mod_channel = types.ModuleType("ehforwarderbot.channel")
    mod_channel.SlaveChannel = SlaveChannel
    mod_efb.channel = mod_channel

    mod_status = types.ModuleType("ehforwarderbot.status")
    mod_status.ChatUpdates = ChatUpdates
    mod_status.MessageRemoval = MessageRemoval
    mod_efb.status = mod_status

    mod_exc = types.ModuleType("ehforwarderbot.exceptions")
    mod_exc.EFBException = EFBException
    mod_exc.EFBMessageError = EFBMessageError
    mod_exc.EFBMessageTypeNotSupported = EFBMessageTypeNotSupported
    mod_exc.EFBChatNotFound = EFBChatNotFound
    mod_exc.EFBOperationNotSupported = EFBOperationNotSupported
    mod_efb.exceptions = mod_exc

    mod_msg = types.ModuleType("ehforwarderbot.message")
    mod_msg.LinkAttribute = LinkAttribute
    mod_msg.LocationAttribute = LocationAttribute
    mod_msg.Substitutions = Substitutions
    mod_msg.MessageCommand = MessageCommand
    mod_msg.MessageCommands = MessageCommands
    mod_efb.message = mod_msg

    mod_utils = types.ModuleType("ehforwarderbot.utils")
    mod_utils.extra = extra
    mod_utils.get_data_path = get_data_path
    mod_efb.utils = mod_utils

    sys.modules.setdefault("ehforwarderbot", mod_efb)
    sys.modules.setdefault("ehforwarderbot.types", mod_types)
    sys.modules.setdefault("ehforwarderbot.chat", mod_chat)
    sys.modules.setdefault("ehforwarderbot.channel", mod_channel)
    sys.modules.setdefault("ehforwarderbot.status", mod_status)
    sys.modules.setdefault("ehforwarderbot.exceptions", mod_exc)
    sys.modules.setdefault("ehforwarderbot.message", mod_msg)
    sys.modules.setdefault("ehforwarderbot.utils", mod_utils)
