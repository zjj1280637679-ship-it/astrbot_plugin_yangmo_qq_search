"""OneBot-first QQ history source with optional NapCat-native enhancements."""

from __future__ import annotations

import asyncio
import json
import uuid

from .event_codec import onebot_message_to_record


class QQSourceError(RuntimeError):
    """The local QQ fact source is unavailable or returned an invalid response."""


class UnixJsonRpcClient:
    def __init__(
        self,
        socket_path: str,
        *,
        connect_timeout: float = 2.0,
        request_timeout: float = 30.0,
        max_response_bytes: int = 8 * 1024 * 1024,
    ) -> None:
        self.socket_path = str(socket_path)
        self.connect_timeout = max(float(connect_timeout), 0.1)
        self.request_timeout = max(float(request_timeout), 0.1)
        self.max_response_bytes = max(int(max_response_bytes), 4096)

    async def call(self, method: str, params: dict | None = None) -> dict:
        request_id = uuid.uuid4().hex
        payload = json.dumps(
            {"id": request_id, "method": str(method), "params": params or {}},
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8") + b"\n"
        writer = None
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_unix_connection(
                    self.socket_path,
                    limit=self.max_response_bytes + 1,
                ),
                timeout=self.connect_timeout,
            )
            writer.write(payload)
            await writer.drain()
            line = await asyncio.wait_for(
                reader.readline(),
                timeout=self.request_timeout,
            )
        except (OSError, asyncio.TimeoutError) as exc:
            raise QQSourceError(
                "QQ 信息源不可用；请检查 NapCat 信息源插件和本机 Socket。"
            ) from exc
        finally:
            if writer is not None:
                writer.close()
                try:
                    await writer.wait_closed()
                except (OSError, RuntimeError):
                    pass

        if not line:
            raise QQSourceError("QQ 信息源返回空响应。")
        if len(line) > self.max_response_bytes:
            raise QQSourceError("QQ 信息源响应超过本地安全上限。")
        try:
            response = json.loads(line.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise QQSourceError("QQ 信息源返回了无效 JSON。") from exc
        if not isinstance(response, dict) or str(response.get("id", "")) != request_id:
            raise QQSourceError("QQ 信息源响应与请求不匹配。")
        if response.get("ok") is not True:
            reason = str(response.get("error") or "未知错误")[:300]
            raise QQSourceError(f"QQ 信息源拒绝本次读取：{reason}")
        result = response.get("result")
        if not isinstance(result, dict):
            raise QQSourceError("QQ 信息源返回形状不符合契约。")
        return result


def _unwrap_action(value):
    if isinstance(value, dict) and isinstance(value.get("data"), dict):
        return value["data"]
    return value if isinstance(value, dict) else {}


class EventOneBotSource:
    """Own standard history paging; delegate only nonstandard features."""

    _BRIDGE_ONLY = frozenset({"history.member_page", "events.peek", "events.ack"})

    def __init__(self, bridge: UnixJsonRpcClient | None = None) -> None:
        self.bridge = bridge
        self._bot = None
        self._account_id = ""

    def bind_event(self, event) -> None:
        bot = getattr(event, "bot", None)
        if callable(getattr(bot, "call_action", None)):
            self._bot = bot
        try:
            account_id = str(event.get_self_id() or "")
        except Exception:
            account_id = ""
        if account_id:
            self._account_id = account_id

    async def _bridge_call(self, method: str, params: dict) -> dict:
        if self.bridge is None:
            raise QQSourceError("NapCat 原生增强未配置。")
        return await self.bridge.call(method, params)

    def _require_bot(self):
        if self._bot is None:
            raise QQSourceError("当前还没有可用的 QQ OneBot 会话；请先让机器人收到一条 QQ 消息。")
        return self._bot

    async def _history_page(self, params: dict) -> dict:
        bot = self._require_bot()
        group_id = str(params.get("group_id") or "").strip()
        cursor = str(params.get("cursor") or "").strip()
        if not group_id.isdigit():
            raise QQSourceError("group_id 必须是纯数字群号。")
        try:
            count = min(max(int(params.get("count") or 50), 1), 100)
        except (TypeError, ValueError):
            count = 50
        payload = {
            "group_id": int(group_id),
            "count": count,
            "reverse_order": bool(cursor),
            "disable_get_url": True,
        }
        if cursor:
            payload["message_seq"] = int(cursor) if cursor.lstrip("-").isdigit() else cursor
        try:
            raw = _unwrap_action(await bot.call_action("get_group_msg_history", **payload))
        except Exception as exc:
            raise QQSourceError(f"QQ OneBot 历史读取失败：{str(exc)[:240]}") from exc
        rows = raw.get("messages") if isinstance(raw.get("messages"), list) else []
        messages = [
            record
            for record in (onebot_message_to_record(row) for row in rows)
            if record is not None and record["group_id"] == group_id
        ]
        messages.sort(key=lambda item: (int(item.get("time") or 0), item["message_id"]))
        # NapCat's action parameter is named ``message_seq``, but its current
        # OneBot implementation resolves that value through the short
        # ``message_id`` mapping. Keep the backend cursor opaque; ``real_seq``
        # remains a separate identity alias for deduplication and recalls.
        next_cursor = str(messages[0]["message_id"]) if messages else ""
        return {
            "source": "astrbot.onebot.get_group_msg_history",
            "direction": "older" if cursor else "latest",
            "group_id": group_id,
            "cursor": cursor,
            "next_cursor": next_cursor,
            "has_more": bool(next_cursor and next_cursor != cursor and messages),
            "messages": messages,
        }

    async def _health(self) -> dict:
        capabilities = ["history.page"] if self._bot is not None else []
        bridge_health = None
        if self.bridge is not None:
            try:
                bridge_health = await self.bridge.call("health", {})
                for capability in bridge_health.get("capabilities") or []:
                    if capability not in capabilities:
                        capabilities.append(capability)
            except QQSourceError as exc:
                bridge_health = {"ready": False, "error": str(exc)}
        return {
            "ready": bool(self._bot is not None or (bridge_health or {}).get("ready")),
            "version": "astrbot-onebot-v1",
            "transport": "event.bot.call_action",
            "capabilities": capabilities,
            "backend_history_action_available": self._bot is not None,
            "native_enhancement": bridge_health,
        }

    async def call(self, method: str, params: dict | None = None) -> dict:
        params = params or {}
        if method == "health":
            return await self._health()
        if method == "history.page":
            try:
                return await self._history_page(params)
            except QQSourceError:
                if self.bridge is None:
                    raise
                return await self._bridge_call(method, params)
        if method == "account.info":
            if self._bot is None:
                return await self._bridge_call(method, params)
            raw = _unwrap_action(await self._bot.call_action("get_login_info"))
            return {
                "account_id": str(raw.get("user_id") or raw.get("self_id") or self._account_id),
                "nickname": str(raw.get("nickname") or ""),
            }
        if method == "groups.list":
            if self._bot is None:
                return await self._bridge_call(method, params)
            raw_value = await self._bot.call_action("get_group_list", no_cache=True)
            raw = _unwrap_action(raw_value)
            groups = raw.get("groups") if isinstance(raw.get("groups"), list) else raw_value
            return {"groups": groups if isinstance(groups, list) else []}
        if method in self._BRIDGE_ONLY:
            if self.bridge is None:
                if method == "events.peek":
                    return {"events": [], "remaining": 0, "source": "standalone_no_recall_bridge"}
                if method == "events.ack":
                    return {"acknowledged": 0, "remaining": 0}
            return await self._bridge_call(method, params)
        return await self._bridge_call(method, params)
