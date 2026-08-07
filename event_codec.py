"""Convert an AstrBot QQ event into the search index's stable record shape."""

from __future__ import annotations

from typing import Any


def _field(value: Any, name: str, default=None):
    try:
        if isinstance(value, dict) or hasattr(value, "get"):
            return value.get(name, default)
    except Exception:
        pass
    return getattr(value, name, default)


def _text_of(segment: Any) -> str:
    if isinstance(segment, str):
        return segment
    if not isinstance(segment, dict):
        return ""
    kind = str(segment.get("type") or "")
    data = segment.get("data") if isinstance(segment.get("data"), dict) else {}
    if kind == "text":
        return str(data.get("text") or "")
    if kind == "at":
        return "@" + str(data.get("qq") or "")
    if kind == "file":
        return f"[文件:{data.get('name') or data.get('file') or ''}]"
    if kind == "markdown":
        return str(data.get("content") or "[Markdown]")
    return {
        "image": "[图片]",
        "record": "[语音]",
        "video": "[视频]",
        "face": "[表情]",
        "mface": "[表情]",
        "reply": "[回复]",
        "forward": "[合并转发]",
        "json": "[卡片]",
        "xml": "[卡片]",
    }.get(kind, f"[{kind or '未知消息'}]")


def _stable_segment(segment: Any) -> dict | None:
    if isinstance(segment, str):
        return {"type": "text", "data": {"text": segment}}
    if not isinstance(segment, dict):
        return None
    kind = str(segment.get("type") or "unknown")
    data = segment.get("data") if isinstance(segment.get("data"), dict) else {}
    keys = {
        "text": ("text",),
        "at": ("qq", "name"),
        "reply": ("id",),
        "image": ("file", "file_id", "summary", "sub_type"),
        "record": ("file", "file_id", "name"),
        "video": ("file", "file_id", "name"),
        "file": ("file", "file_id", "name", "size"),
        "face": ("id",),
        "mface": ("emoji_id", "emoji_package_id", "summary"),
        "forward": ("id", "resid"),
        "markdown": ("content",),
    }.get(kind, ())
    stable = {}
    for key in keys:
        value = data.get(key)
        if value is not None and key != "url":
            stable[key] = value if isinstance(value, (str, int, float, bool)) else str(value)
    return {"type": kind, "data": stable}


def event_to_record(event) -> dict | None:
    """Return None for non-group events or events without a stable message id."""
    group_id = str(event.get_group_id() or "")
    if not group_id:
        return None
    message_obj = getattr(event, "message_obj", None)
    raw = getattr(message_obj, "raw_message", None)
    message_id = str(
        getattr(message_obj, "message_id", None)
        or _field(raw, "message_id", "")
        or ""
    )
    if not message_id:
        return None
    raw_segments = _field(raw, "message", [])
    raw_segments = raw_segments if isinstance(raw_segments, list) else []
    segments = [item for item in (_stable_segment(seg) for seg in raw_segments) if item]
    if raw_segments:
        body = "".join(_text_of(seg) for seg in raw_segments)
    else:
        body = str(getattr(event, "message_str", "") or "")
        if not body:
            try:
                body = str(event.get_message_outline() or "")
            except Exception:
                body = ""
    types = sorted({str(item.get("type") or "unknown") for item in segments})
    if not types and body:
        types = ["text"]
    reply = next((item for item in segments if item.get("type") == "reply"), None)
    forward = next((item for item in segments if item.get("type") == "forward"), None)
    sender_id = str(event.get_sender_id() or _field(raw, "user_id", "") or "")
    sender = _field(raw, "sender", {})
    sender_name = str(
        event.get_sender_name()
        or _field(sender, "card", "")
        or _field(sender, "nickname", "")
        or ""
    )
    timestamp = _field(raw, "time", getattr(message_obj, "timestamp", 0))
    try:
        timestamp = int(timestamp or 0)
    except (TypeError, ValueError):
        timestamp = 0
    real_sequence = str(_field(raw, "real_seq", "") or "")
    return {
        "message_id": message_id,
        "message_seq": real_sequence
        or str(_field(raw, "message_seq", message_id) or message_id),
        "time": timestamp,
        "group_id": group_id,
        "user_id": sender_id,
        "sender": {
            "id": sender_id,
            "name": sender_name,
            "role": str(_field(sender, "role", "") or ""),
        },
        "text": body,
        "segment_types": types,
        "segments": segments,
        "reply_to": str(_field(_field(reply, "data", {}), "id", "") or ""),
        "forward_id": str(
            _field(_field(forward, "data", {}), "id", "")
            or _field(_field(forward, "data", {}), "resid", "")
            or ""
        ),
        "source": "live",
    }


def onebot_message_to_record(message: Any) -> dict | None:
    """Normalize a ``get_group_msg_history`` row into the live-event schema."""
    if not isinstance(message, dict):
        return None
    group_id = str(message.get("group_id") or "")
    message_id = str(message.get("message_id") or "")
    if not group_id or not message_id:
        return None
    raw_segments = message.get("message")
    raw_segments = raw_segments if isinstance(raw_segments, list) else []
    segments = [item for item in (_stable_segment(seg) for seg in raw_segments) if item]
    body = "".join(_text_of(seg) for seg in raw_segments)
    if not body:
        body = str(message.get("raw_message") or "")
    types = sorted({str(item.get("type") or "unknown") for item in segments})
    if not types and body:
        types = ["text"]
    reply = next((item for item in segments if item.get("type") == "reply"), None)
    forward = next((item for item in segments if item.get("type") == "forward"), None)
    sender = message.get("sender") if isinstance(message.get("sender"), dict) else {}
    sender_id = str(message.get("user_id") or sender.get("user_id") or "")
    try:
        timestamp = int(message.get("time") or 0)
    except (TypeError, ValueError):
        timestamp = 0
    return {
        "message_id": message_id,
        "message_seq": str(
            message.get("real_seq") or message.get("message_seq") or message_id
        ),
        "time": timestamp,
        "group_id": group_id,
        "user_id": sender_id,
        "sender": {
            "id": sender_id,
            "name": str(sender.get("card") or sender.get("nickname") or ""),
            "role": str(sender.get("role") or ""),
        },
        "text": body,
        "segment_types": types,
        "segments": segments,
        "reply_to": str(_field(_field(reply, "data", {}), "id", "") or ""),
        "forward_id": str(
            _field(_field(forward, "data", {}), "id", "")
            or _field(_field(forward, "data", {}), "resid", "")
            or ""
        ),
        "source": "history",
    }
