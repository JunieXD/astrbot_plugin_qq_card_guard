"""Account-bound OneBot calls, durable read budgets and connection generations."""

from __future__ import annotations

import asyncio
import random
import secrets
import time
from contextlib import asynccontextmanager

from .config import GuardError, Later
from .rules import Member, message_fingerprint, message_id, number

WRITES = {"send_group_msg", "set_group_ban", "delete_msg"}


class Adapter:
    def __init__(self, pid, bot, store, pace, journal, *, clock=time.time, sleep=asyncio.sleep):
        self.pid, self.bot, self.store, self.pace, self.journal = pid, bot, store, pace, journal
        self.clock, self.sleep = clock, sleep
        self.account = ""
        self.lock = asyncio.Lock()
        self.lock_owner = None
        self.session = secrets.token_hex(6)
        self.generation = 0
        self.objects = self.clients()
        self.token = self.client_token()
        self.recovery_until = 0
        self.next_read = 0
        self.identity_at = 0

    def clients(self):
        raw = getattr(self.bot, "_wsr_api_clients", None)
        return tuple(raw.values()) if isinstance(raw, dict) else ()

    def client_token(self):
        raw = getattr(self.bot, "_wsr_api_clients", None)
        return tuple(sorted((str(a), id(ws)) for a, ws in raw.items())) if isinstance(raw, dict) else None

    def stamp(self):
        token = self.client_token()
        if token != self.token:
            self.token, self.objects = token, self.clients()
            self.generation += 1
            self.identity_at = 0
            self.recovery_until = max(
                self.recovery_until, self.clock() + random.uniform(*self.pace().interval("recovery"))
            )
        if token is not None and len(token) > 1:
            raise GuardError("同一接入连接多个QQ，无法安全定位账号，请拆分接入。")
        if token == () and getattr(getattr(self.bot, "_api", None), "_http_api", None) is None:
            raise Later("NapCat未连接，稍后再核对。", self.clock() + 30)
        if token and self.account and token[0][0] != self.account:
            raise GuardError("机器人登录账号改变，请重载插件重新绑定。")
        return f"{self.session}:{self.generation}"

    @asynccontextmanager
    async def preparation(self):
        """Keep a write's fresh reads together, with cancellation-safe reentrancy."""
        task = asyncio.current_task()
        if self.lock_owner is task:
            yield
            return
        async with self.lock:
            self.lock_owner = task
            try:
                yield
            finally:
                self.lock_owner = None

    async def call(self, action, *, priority=0, before_send=None, **params):
        async with self.preparation():
            try:
                self.stamp()
                quota_key = self.account or (self.token[0][0] if self.token else self.pid)
                if action not in WRITES:
                    await self.store.call(
                        "reserve_read", quota_key, self.clock(), self.pace().reads_per_hour, priority
                    )
                    await self.sleep(max(0, self.next_read - self.clock()))
                    self.next_read = self.clock() + random.uniform(1.5, 3)

                async def invoke():
                    self.stamp()
                    if self.token:
                        params["self_id"] = self.token[0][0]
                    if before_send:
                        before_send()
                    return await self.bot.call_action(action=action, **params)

                with self.journal.span("平台接口", api=action, account=self.account, platform=self.pid):
                    try:
                        result = await asyncio.wait_for(asyncio.create_task(invoke()), timeout=25)
                    except GuardError:
                        raise
                    except Exception as exc:
                        self.generation += 1
                        self.identity_at = 0
                        self.recovery_until = max(
                            self.recovery_until,
                            self.clock() + random.uniform(*self.pace().interval("recovery")),
                        )
                        raise GuardError(
                            f"{action}未正常完成，请检查NapCat连接和权限；不会盲目重发。"
                        ) from exc
                    if isinstance(result, dict) and ("retcode" in result or "status" in result):
                        if result.get("status") != "ok" or result.get("retcode", 0) != 0:
                            raise GuardError(f"{action}返回失败，请检查权限和账号状态。")
                        result = result.get("data")
                    return result
            finally:
                if self.recovery_until:
                    self.recovery_until = await self.store.call(
                        "extend", "connection:" + self.pid, self.recovery_until
                    )

    async def identity(self, force=False, priority=0):
        self.stamp()
        if self.account and not force and self.clock() - self.identity_at < 300:
            return self.account
        data = await self.call("get_login_info", priority=priority)
        if not isinstance(data, dict) or not number(data.get("user_id")):
            raise GuardError("无法确认机器人身份。")
        account = str(data["user_id"])
        if self.account and self.account != account:
            raise GuardError("机器人账号改变，请重载插件。")
        self.account, self.identity_at = account, self.clock()
        self.recovery_until = max(
            self.recovery_until, await self.store.call("get", "connection:" + self.pid, 0)
        )
        return account

    async def online(self, priority=2):
        data = await self.call("get_status", priority=priority)
        connected = isinstance(data, dict) and data.get("online") is True and data.get("good", True) is True
        if not connected:
            self.generation += 1
            self.recovery_until = await self.store.call(
                "extend",
                "connection:" + self.pid,
                self.clock() + random.uniform(*self.pace().interval("recovery")),
            )
        return connected

    async def member(self, gid, uid, priority=0):
        data = await self.call(
            "get_group_member_info", group_id=gid, user_id=uid, no_cache=True, priority=priority
        )
        if (
            not isinstance(data, dict)
            or str(data.get("user_id")) != uid
            or str(data.get("group_id", gid)) != gid
        ):
            raise GuardError("群成员资料身份不一致，暂缓处理。")
        return Member.parse(data)

    async def all_muted(self, gid):
        data = await self.call("get_group_detail_info", group_id=gid, priority=0)
        if not isinstance(data, dict) or str(data.get("group_id", gid)) != gid:
            raise GuardError("群详情无法确认，暂缓新增处理。")
        value = str(data.get("group_all_shut"))
        if value not in ("-1", "0", "1"):
            raise GuardError("全员禁言状态未知，需要支持群详情接口的NapCat。")
        return value != "0"

    async def notify(self, gid, message, before_send):
        data = await self.call("send_group_msg", group_id=gid, message=message, before_send=before_send)
        if not isinstance(data, dict) or not message_id(data.get("message_id")):
            raise GuardError("提醒发送结果没有有效消息ID，停止后续禁言并等待核对。")
        return message_id(data["message_id"])

    async def ban(self, gid, uid, seconds, before_send):
        return await self.call(
            "set_group_ban", group_id=gid, user_id=uid, duration=seconds, before_send=before_send
        )

    async def locate_message(self, case):
        data = await self.call("get_msg", message_id=case["message_id"], priority=2)
        if not isinstance(data, dict):
            raise GuardError("提醒消息无法定位，保留原消息。")
        sender = data.get("sender", {})
        if not isinstance(sender, dict):
            raise GuardError("提醒发送者资料缺失，无法确认归属。")
        valid = (
            str(data.get("group_id")) == case["gid"]
            and str(sender.get("user_id")) == self.account
            and str(data.get("message_id")) == case["message_id"]
            and number(data.get("time"))
            and abs(int(data["time"]) - case["sent_at"]) <= 60
            and message_fingerprint(data.get("message")) == case["message_hash"]
        )
        if not valid:
            raise GuardError("提醒消息身份或内容无法确认，不撤回其他消息。")

    async def recall(self, mid, before_send):
        return await self.call("delete_msg", message_id=mid, before_send=before_send)


class Router:
    def __init__(self, context, store, pace, journal):
        self.context, self.store, self.pace, self.journal = context, store, pace, journal
        self.adapters = {}
        self.lock = asyncio.Lock()

    def known_stamp(self, pid, account):
        """A cache lookup must not query QQ, nor trust a replaced/disconnected adapter."""
        adapter = self.adapters.get(pid)
        if adapter is None or adapter.account != account:
            return None
        platforms = [p for p in self.context.platform_manager.get_insts() if p.meta().name == "aiocqhttp"]
        if not any(str(p.meta().id) == pid and getattr(p, "bot", None) is adapter.bot for p in platforms):
            return None
        identities = []
        for platform in platforms:
            clients = getattr(getattr(platform, "bot", None), "_wsr_api_clients", None)
            if isinstance(clients, dict):
                identities.extend(str(a) for a in clients)
        if len(identities) != len(set(identities)):
            return None
        return adapter.stamp()

    async def resolve(self, policy, *, expected=None, priority=0):
        async with self.lock:
            found = []
            available = []
            for platform in self.context.platform_manager.get_insts():
                meta = platform.meta()
                if meta.name != "aiocqhttp" or not hasattr(getattr(platform, "bot", None), "call_action"):
                    continue
                pid = str(meta.id)
                adapter = self.adapters.get(pid)
                if adapter is None or adapter.bot is not platform.bot:
                    adapter = self.adapters[pid] = Adapter(
                        pid, platform.bot, self.store, self.pace, self.journal
                    )
                available.append(adapter)
            accounts = [account for adapter in available for account, _ in (adapter.client_token() or ())]
            if len(accounts) != len(set(accounts)):
                raise GuardError("同一QQ接入多个平台，请只保留一个接入。")
            if len(available) > 1 and not policy.bot_qq:
                raise GuardError("多QQ接入时请填写这个群负责的机器人QQ，防止重复管理。")
            target_account = expected[1] if expected else policy.bot_qq
            for adapter in available:
                token = adapter.client_token()
                advertised = {entry[0] for entry in token} if token else set()
                if target_account and advertised and target_account not in advertised:
                    continue
                try:
                    await adapter.identity(priority=priority)
                except GuardError:
                    if expected and adapter.pid != expected[0]:
                        continue
                    if policy.bot_qq and adapter.account and adapter.account != policy.bot_qq:
                        continue
                    raise
                found.append(adapter)
            if len({a.account for a in found}) != len(found):
                raise GuardError("同一QQ接入多个平台，请只保留一个接入。")
            if len(found) > 1 and not policy.bot_qq:
                raise GuardError("多QQ接入时请填写这个群负责的机器人QQ，防止重复管理。")
            if expected:
                found = [a for a in found if (a.pid, a.account) == expected]
            elif policy.bot_qq:
                found = [a for a in found if a.account == policy.bot_qq]
            if len(found) != 1:
                raise GuardError("无法唯一确定机器人；多QQ接入时请填写这个群负责的机器人QQ。")
            if policy.bot_qq and found[0].account != policy.bot_qq:
                raise GuardError("群配置已改由另一个QQ负责，旧事项请人工核对。")
            return found[0]

    def shared_guard(self):
        guard = getattr(self.context.platform_manager, "_qq_automation_guard_v1", None)
        if guard is not None and (
            not callable(getattr(guard, "run", None)) or not hasattr(guard, "deferred_error")
        ):
            raise GuardError("现有共享操作队列版本不兼容，停止自动操作。")
        return guard
