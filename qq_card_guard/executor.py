"""Durable, one-attempt actions and conservative ownership of moderation effects."""

from __future__ import annotations

import asyncio
import random
from contextlib import nullcontext

from .config import GuardError, Later
from .rules import message_fingerprint
from .scheduling import followup_window
from .store import key


class Executor:
    def __init__(self, service):
        self.s = service
        self.db = service.store
        self.now = service.clock

    def delay(self, action):
        return random.uniform(*self.s.settings().pace.interval(action))

    async def patch(self, case, **changes):
        return await self.db.call("patch_case", case["id"], changes)

    async def poll(self, case, **changes):
        count = case["polls"] + 1
        planned = {**case, **changes}
        stage, low, high = followup_window(planned, self.s.settings().pace, self.now())
        due = self.now() + random.uniform(low, high)
        result = await self.patch(case, due=due, polls=count, **changes)
        self.s.journal.record(
            "安排补查",
            account=case["account"],
            group=case["gid"],
            user=case["uid"],
            case=case["id"],
            stage=stage,
            due=due,
            minimum=low,
            maximum=high,
            reason=changes.get("reason", "等待名片修改"),
        )
        return result

    def versions(self, case):
        a, g, u = case["account"], case["gid"], case["uid"]
        return tuple(self.s.version(a, g, x) for x in (u, a, "0"))

    async def gate(self, case, adapter, new):
        self.s.healthy()
        pace = self.s.settings().pace
        adapter.stamp()
        until = max(
            adapter.recovery_until,
            await self.db.call("get", "recovery:" + adapter.account, 0),
            await self.db.call("get", "gap:" + adapter.account, 0),
        )
        if new:
            until = max(until, await self.db.call("get", "startup", 0))
            if await self.db.call("get", "block:" + adapter.account, {}):
                raise Later("账号有结果不明的操作，新增处理已暂停。", self.now() + 300)
            if await self.db.call("get", "pause:" + key(adapter.account, case["gid"]), ""):
                raise Later("本群已暂停新增处理。", self.now() + 300)
        if until > self.now():
            raise Later("等待操作间隔或连接恢复冷却。", until)
        return pace

    async def process(self, case, adapter):
        self.s.healthy()
        phase = case["phase"]
        if phase in ("notify", "ban"):
            try:
                policy = self.s.settings().group(case["gid"])
            except GuardError:
                policy = None
            if (
                not self.s.settings().enabled
                or not policy
                or not policy.enabled
                or policy.mode == "仅观察"
                or policy.revision != case["revision"]
                or adapter.stamp() != case["connection"]
            ):
                await self.patch(
                    case,
                    phase="watch" if case["message_id"] else "closed",
                    reason="配置或连接已变化，取消尚未提交的处罚",
                    due=self.now(),
                )
                return
        if phase in ("notify", "ban", "settle"):
            if phase == "settle" and self.s.policy(case).unmute_on_compliance:
                for other in await self.db.call("member_cases", case["account"], case["gid"], case["uid"]):
                    if (
                        other["id"] != case["id"]
                        and other["phase"] == "settle"
                        and other["mute_state"] in ("owned", "unverified")
                        and other["mute_until"] > self.now()
                        and case["mute_state"] not in ("owned", "unverified")
                    ):
                        raise Later("先完成此成员当前禁言的解禁核验。", max(self.now() + 1, other["due"]))
            await self.write(case, adapter, phase)
        elif phase in ("watch", "review"):
            await self.watch(case, adapter)

    async def inspect_case(self, case, adapter, stable=False, priority=1):
        subject = await self.db.call("subject", case["account"], case["gid"], case["uid"])
        if not subject["present"] or subject["epoch"] != case["epoch"]:
            return None, subject, None
        member, subject, verdict = await self.s.inspect(
            self.s.policy(case), adapter, case["uid"], stable=stable, priority=priority
        )
        if subject["epoch"] != case["epoch"] or subject["joined"] != case["joined"]:
            return None, subject, None
        return member, subject, verdict

    async def ownership(self, case, member, subject):
        state = case["mute_state"]
        if state not in ("owned", "unverified"):
            return state
        if member is None:
            return "external"
        if member.muted_until is None:
            return "unverified"
        if member.muted_until <= self.now():
            return "expired"
        operation = await self.db.call("has_operation", case["id"], "ban")
        notice = subject["ban"]
        if not operation or operation["status"] != "confirmed":
            return "manual"
        baseline = case.get("ban_baseline")
        if baseline is None:
            return "manual"
        if subject.get("ban_serial", 0) > baseline + 1:
            return "external"
        if not notice or notice["received"] < case["ban_at"] - 1:
            return "unverified"
        if (
            notice["operator"] == case["account"]
            and notice["duration"] == case["minutes"] * 60
            and abs(notice["at"] - case["ban_at"]) <= 15
            and abs(member.muted_until - case["mute_until"]) <= 15
            and abs(member.muted_until - notice["until"]) <= 15
        ):
            return "owned"
        return "external"

    async def watch(self, case, adapter):
        if case["phase"] == "review":
            # Unknown writes are never inferred from an unrelated later membership/card change.
            return
        urgent = case["mute_state"] in ("owned", "unverified") and case["mute_until"] > self.now()
        member, subject, verdict = await self.inspect_case(case, adapter, priority=2 if urgent else 1)
        state = await self.ownership(case, member, subject)
        if member is None:
            await self.patch(
                case,
                phase="settle",
                mute_state=state,
                reason="成员已离群或重新入群，只核对旧提醒",
                due=self.now() + self.delay("recall"),
            )
            return
        if verdict.state in ("compliant", "exempt"):
            await self.patch(case, mute_state=state)
            await self.s.settle_member(case["account"], case["gid"], case["uid"], verdict.reason)
        else:
            await self.poll(case, mute_state=state, reason=verdict.reason)

    async def bot_permission(self, case, adapter, priority=2):
        if not await self.s.manager(adapter, case["gid"], adapter.account, priority):
            raise Later("机器人当前没有可确认的群管理权限。", self.now() + 300)

    async def write(self, case, adapter, phase):
        lock = self.s.account_locks.setdefault(adapter.account, asyncio.Lock())
        new = phase in ("notify", "ban")
        await self.gate(case, adapter, new)
        guard = self.s.router.shared_guard()

        async def attempt():
            async with lock:
                try:
                    # Shared queue waits may be arbitrarily long. All facts are read afterwards.
                    await self.gate(case, adapter, new)
                    # Acquire the read lock before measuring freshness. Other members' reads
                    # cannot repeatedly age this preparation out while it is in progress.
                    async with adapter.preparation() if hasattr(adapter, "preparation") else nullcontext():
                        return await self.attempt(case, adapter, phase)
                except GuardError as exc:
                    if guard:
                        raise guard.deferred_error(str(exc)) from exc
                    raise

        async def online():
            return await adapter.online(priority=0 if new else 2)

        if guard:
            pace = self.s.settings().pace
            options = {}
            if getattr(guard, "scheduling_version", 1) >= 3:
                options = dict(priority=2 if new else 1, group=case["gid"], label="名片规范")
            try:
                await guard.run(
                    account=adapter.pid,
                    online=online,
                    action=attempt,
                    config={
                        "recovery_min_seconds": pace.recovery_min,
                        "recovery_max_seconds": pace.recovery_max,
                        "failure_threshold": 1,
                        "failure_cooldown_seconds": 900,
                    },
                    delay=(0, 0),
                    gap=pace.interval("gap"),
                    key=None,
                    **options,
                )
            except guard.deferred_error as exc:
                cause = exc.__cause__
                raise Later(
                    str(exc),
                    getattr(cause, "until", self.now() + 60),
                    code=getattr(cause, "code", "deferred"),
                    **getattr(cause, "details", {}),
                ) from exc
        else:
            if not await online():
                raise Later("QQ当前离线，等待连接恢复。", self.now() + 300)
            await attempt()

    async def attempt(self, case, adapter, phase):
        new = phase in ("notify", "ban")
        try:
            policy = self.s.settings().group(case["gid"]) if new else self.s.policy(case)
        except GuardError:
            await self.patch(
                case,
                phase="watch" if case["message_id"] else "closed",
                reason="群配置已移除或无效，取消未提交的处罚",
                due=self.now(),
            )
            return
        if new:
            if (
                not self.s.settings().enabled
                or not policy.enabled
                or policy.mode == "仅观察"
                or policy.revision != case["revision"]
                or adapter.stamp() != case["connection"]
            ):
                await self.patch(
                    case,
                    phase="watch" if case["message_id"] else "closed",
                    reason="等待期间规则或连接变化，取消处罚",
                    due=self.now(),
                )
                return
            deadline = case.get(
                "execute_before", case["created"] + 300 if phase == "notify" else case["sent_at"] + 120
            )
            if self.now() > deadline:
                await self.patch(
                    case,
                    phase="watch" if case["message_id"] else "closed",
                    reason="处理已过期，等待新的发言",
                    due=self.now(),
                )
                return
        versions = self.versions(case)
        stamp = adapter.stamp()
        prepared_at = self.now()
        if await adapter.identity(force=True, priority=0 if new else 2) != case["account"]:
            raise GuardError("提交前机器人身份改变，请重新核对账号绑定。")
        await self.bot_permission(case, adapter, 0 if new else 2)
        if new and await adapter.all_muted(case["gid"]):
            raise Later("全员禁言期间暂停新增处理。", self.now() + 300)
        member, subject, verdict = await self.inspect_case(
            case, adapter, stable=True, priority=0 if new else 2
        )
        if new and (member is None or verdict.state in ("compliant", "exempt")):
            await self.patch(
                case,
                phase="settle",
                reason="等待期间成员已合规、豁免或离群",
                due=self.now() + self.delay("recall"),
            )
            return
        if verdict and verdict.state == "unknown":
            raise Later(verdict.reason, self.now() + 300)
        if new and member.muted_until is None:
            raise Later("当前禁言状态未知，暂缓。", self.now() + 300)
        if new and (member.muted_until > self.now() or subject["ban"].get("until", 0) > self.now()):
            await self.patch(
                case,
                phase="watch" if case["message_id"] else "closed",
                reason="成员已被禁言，不覆盖管理员操作",
                due=self.now() + self.delay("poll"),
            )
            return
        if phase == "ban" and subject.get("ban_serial", 0) != case.get("notice_serial", 0):
            await self.poll(case, phase="watch", reason="提醒后已有人操作禁言，本轮不再覆盖")
            return
        if phase == "settle":
            if member and verdict.state not in ("compliant", "exempt"):
                await self.poll(case, phase="watch", reason="名片再次不合规，保留现有处理")
                return
            mute_state = await self.ownership(case, member, subject)
            case = await self.patch(case, mute_state=mute_state)
            if mute_state in ("owned", "unverified") and policy.unmute_on_compliance:
                if mute_state != "owned":
                    # Allow late notices briefly; never infer ownership from the expiry alone.
                    if self.now() < case["ban_at"] + 120:
                        await self.poll(case, reason="等待禁言归属通知")
                        return
                    case = await self.patch(case, mute_state="manual", reason="缺少禁言归属证据，请人工核对")
                    kind = "recall"
                else:
                    kind = "unmute"
            else:
                kind = "recall"
            if kind == "recall":
                if (
                    not policy.recall_on_compliance
                    or not case["message_id"]
                    or case["recall_state"] != "pending"
                ):
                    await self.patch(case, phase="closed", reason=case["reason"] or "已完成核验")
                    return
                try:
                    await adapter.locate_message(case)
                except Later:
                    raise
                except GuardError as exc:
                    await self.patch(case, phase="closed", recall_state="manual", reason=str(exc))
                    self.s.journal.record("旧提醒需人工撤回", case=case["id"], exception=exc)
                    return
        else:
            kind = phase
        if await self.db.call("has_operation", case["id"], kind):
            # Normally only reachable after interrupted shutdown or a stale scheduler snapshot.
            await self.uncertain(case, kind, "已存在提交记录，不能重复执行")
            return
        if kind == "ban" and (policy.mode != "提醒并禁言" or case["minutes"] <= 0):
            await self.poll(case, phase="watch", reason="本轮只提醒")
            return

        message = [
            {"type": "at", "data": {"qq": case["uid"]}},
            {
                "type": "text",
                "data": {"text": " " + policy.render(case["uid"], case["round"], case["minutes"])},
            },
        ]
        payload = {"minutes": case["minutes"]} if kind == "ban" else {}
        sent = False

        def fence():
            nonlocal sent
            self.s.healthy()
            if adapter.stamp() != stamp or self.versions(case) != versions or self.now() - prepared_at > 20:
                raise Later(
                    "提交前资料或连接变化，已取消本次发送。",
                    self.now() + 5,
                    code="stale_preparation",
                    age_seconds=round(self.now() - prepared_at, 3),
                )
            try:
                current = self.s.settings().group(case["gid"]) if new else self.s.policy(case)
            except GuardError as exc:
                raise Later("提交前群配置已移除或无效，取消发送。", self.now() + 1) from exc
            if current.revision != policy.revision:
                raise Later("提交前配置变化，已取消本次发送。", self.now() + 5)
            if new and self.now() > deadline:
                raise Later("提交前处理已过期，取消发送。", self.now() + 1)
            if new and (
                not self.s.settings().enabled
                or not current.enabled
                or current.mode == "仅观察"
                or key(case["account"], case["gid"]) in self.s.paused
            ):
                raise Later("新增处理已暂停。", self.now() + 300)
            sent = True

        pace = self.s.settings().pace
        try:
            oid = await self.db.call("reserve", case, kind, self.now(), payload, policy, pace)
        except asyncio.CancelledError:
            reserved = await self.db.call("has_operation", case["id"], kind)
            if reserved and reserved["status"] == "submitted":
                await self.db.call("cancel_unsent", reserved["id"], self.now())
            raise
        except Later as exc:
            if kind == "ban":
                await self.poll(case, phase="watch", reason=str(exc))
                return
            raise
        gap = self.delay("gap")
        try:
            # Persist the gap before the write, including uncertain/cancelled writes.
            await self.db.call("extend", "gap:" + adapter.account, self.now() + gap)
            at = self.now()
            if kind == "notify":
                result = await adapter.notify(case["gid"], message, fence)
                due = self.now() + self.delay("ban" if case["minutes"] else "poll")
                next_round = (
                    at
                    + max(
                        policy.repeat_minutes,
                        policy.first_grace_minutes if case["round"] == 1 else 0,
                        case["minutes"],
                    )
                    * 60
                )
                changes = dict(
                    message_id=result,
                    message_hash=message_fingerprint(message),
                    sent_at=at,
                    recall_state="pending",
                    next_round=next_round,
                    phase="ban" if case["minutes"] else "watch",
                    reason="已提醒，等待名片修改",
                    due=due,
                    execute_before=due + 120,
                    notice_serial=subject.get("ban_serial", 0),
                )
            elif kind in ("ban", "unmute"):
                seconds = case["minutes"] * 60 if kind == "ban" else 0
                result = await adapter.ban(case["gid"], case["uid"], seconds, fence)
                if kind == "ban":
                    changes = dict(
                        phase="watch",
                        ban_at=at,
                        ban_baseline=subject.get("ban_serial", 0),
                        mute_until=at + seconds,
                        next_round=max(case["next_round"], at + seconds),
                        mute_state="unverified",
                        due=self.now() + self.delay("muted_poll"),
                        reason="禁言接口成功，等待归属核验",
                    )
                else:
                    changes = dict(
                        phase="settle",
                        mute_state="released",
                        due=self.now() + self.delay("recall"),
                        reason="已核验解除本插件禁言",
                    )
            else:
                result = await adapter.recall(case["message_id"], fence)
                changes = dict(phase="closed", recall_state="recalled", reason="已撤回提醒")
            await self.db.call("finish", oid, "confirmed", {"accepted": True}, changes, self.now())
            self.s.journal.record(
                "操作完成",
                case=case["id"],
                action=kind,
                account=adapter.account,
                group=case["gid"],
                user=case["uid"],
            )
        except BaseException as exc:
            if not sent:
                await self.db.call("cancel_unsent", oid, self.now())
                raise
            recorded = await self.db.call("has_operation", case["id"], kind)
            if recorded and recorded["status"] == "confirmed":
                # Database cancellation drains the transaction before raising; keep its known result.
                raise
            # The request may have reached QQ. A timeout or cancellation is not a failed effect.
            changes = self.uncertain_changes(kind, "操作返回异常或中断，结果需要核对")
            await self.db.call("finish", oid, "unknown", {"needs_review": True}, changes, self.now())
            if new:
                await self.block(case)
            self.s.journal.record("操作结果不明", case=case["id"], action=kind, exception=exc)
            if isinstance(exc, asyncio.CancelledError):
                raise
        finally:
            if sent:
                await self.db.call("extend", "gap:" + adapter.account, self.now() + gap)

    def uncertain_changes(self, kind, reason):
        if kind in ("notify", "ban"):
            return dict(
                phase="review",
                mute_state="manual" if kind == "ban" else "none",
                reason=reason,
                due=self.now() + self.delay("poll"),
            )
        if kind == "unmute":
            return dict(
                phase="settle", mute_state="manual", reason=reason, due=self.now() + self.delay("recall")
            )
        return dict(phase="closed", recall_state="manual", reason=reason)

    async def block(self, case):
        await self.db.call("set", "block:" + case["account"], {"gid": case["gid"], "case": case["id"]})

    async def uncertain(self, case, kind, reason):
        await self.patch(case, **self.uncertain_changes(kind, reason))
        if kind in ("notify", "ban"):
            await self.block(case)
