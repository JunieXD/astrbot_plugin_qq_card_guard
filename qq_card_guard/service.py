"""Event ingestion and due-work scheduling; messages never directly trigger a write."""

from __future__ import annotations

import asyncio
import random
import time
from contextvars import ContextVar
from dataclasses import replace

from .config import GuardError, Later, Policy, Stage, fingerprint
from .rules import judge, number
from .scheduling import cache_check
from .store import key


def saved_policy(case):
    data = dict(case["policy"])
    data["stages"] = tuple(Stage(**x) for x in data["stages"])
    data["exempt_users"] = tuple(data["exempt_users"])
    return Policy(**data)


class Service:
    def __init__(
        self,
        settings,
        store,
        router,
        journal,
        *,
        clock=time.time,
        sleep=asyncio.sleep,
        monotonic=time.monotonic,
    ):
        self.settings, self.store, self.router, self.journal = settings, store, router, journal
        self.clock, self.sleep, self.monotonic = clock, sleep, monotonic
        self.stopped = False
        self.failure = ""
        self.versions = {}
        self.card_hints = {}
        self.member_locks = {}
        self.account_locks = {}
        self.paused = set()
        self.group_errors = {}
        self.jobs = set()
        self.running = set()
        self.task = None
        self.last_clock = (clock(), monotonic())
        self.command_until = {}
        self.wake = asyncio.Event()
        self.inspections = ContextVar(f"card-guard-inspections-{id(self)}", default=None)

    def member_lock(self, a, g, u):
        return self.member_locks.setdefault(key(a, g, u), asyncio.Lock())

    def version(self, a, g, u):
        return self.versions.get(key(a, g, u), 0)

    def invalidate(self, a, g, u):
        k = key(a, g, u)
        self.versions[k] = self.versions.get(k, 0) + 1
        if len(self.versions) > 100000:
            self.failure = "活动状态超过容量上限，已停止自动操作，请检查配置后重载。"

    def healthy(self):
        if self.stopped or self.failure or not self.store.healthy:
            raise GuardError(self.failure or "插件已停止或存储不可用。")
        now, mono = self.clock(), self.monotonic()
        if abs((now - self.last_clock[0]) - (mono - self.last_clock[1])) > 120:
            self.failure = "系统时间异常跳变，已停止操作，请校时后重载。"
            raise GuardError(self.failure)
        self.last_clock = (now, mono)
        self.journal.check()

    def policy(self, case):
        try:
            return self.settings().group(case["gid"])
        except GuardError:
            return saved_policy(case)

    async def start(self):
        self.healthy()
        now = self.clock()
        if now < await self.store.call("get", "last-wall", now) - 120:
            raise GuardError("系统时间早于上次运行，请校时后重载。")
        await self.store.call(
            "extend", "startup", now + random.uniform(*self.settings().pace.interval("startup"))
        )
        self.task = asyncio.create_task(self.loop(), name="qq-card-guard-scheduler")

    async def stop(self):
        self.stopped = True
        self.wake.set()
        tasks = {t for t in self.jobs if t is not asyncio.current_task()}
        if self.task and self.task is not asyncio.current_task():
            tasks.add(self.task)
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def observe(self, raw, pid):
        if self.stopped or self.failure:
            return
        get = raw.get if isinstance(raw, dict) else lambda k, d=None: getattr(raw, k, d)
        kind = (
            "message"
            if get("post_type") == "message" and get("message_type") == "group"
            else get("notice_type")
        )
        if kind not in (
            "message",
            "group_card",
            "group_admin",
            "group_ban",
            "group_increase",
            "group_decrease",
        ):
            return
        a, g, u = (str(get(n, "")) for n in ("self_id", "group_id", "user_id"))
        if not all(number(v) for v in (a, g)) or not (number(u) or (kind == "group_ban" and u == "0")):
            return
        now, at = self.clock(), number(get("time"))
        if not at or at > now + 300:
            self.journal.record("忽略异常事件时间", account=a, group=g, user=u)
            return
        try:
            policy = self.settings().group(g)
        except GuardError:
            # Existing duties still need notices after configuration is disabled/removed.
            cases = await self.store.call("member_cases", a, g, u)
            if not cases:
                return
            policy = saved_policy(cases[0])
        if policy.bot_qq and policy.bot_qq != a:
            return
        if kind == "message" and a == u:
            return
        sender = get("sender", {})
        card = (
            get("card_new")
            if kind == "group_card"
            else (sender.get("card") if isinstance(sender, dict) else None)
        )
        if isinstance(card, str) and len(card) > 512:
            return
        if kind != "message" or (isinstance(card, str) and self.card_hints.get(key(a, g, u)) != card):
            self.invalidate(a, g, u)
        if isinstance(card, str):
            self.card_hints[key(a, g, u)] = card
        duration = 0
        if kind == "group_admin" and get("sub_type") not in ("set", "unset"):
            return
        if kind == "group_ban":
            duration = number(get("duration"), zero=True)
            if u == "0" and str(get("duration")) == "-1":
                duration = -1
            if (
                get("sub_type") not in ("ban", "lift_ban")
                or duration is None
                or duration > 2592000
                or not number(get("operator_id"))
            ):
                return
        event = {
            "account": a,
            "gid": g,
            "uid": u,
            "platform": pid,
            "kind": kind,
            "at": at,
            "card": card,
            "subtype": str(get("sub_type", "")),
            "operator": str(get("operator_id", "")),
            "duration": int(duration or 0),
        }
        event["id"] = fingerprint(
            [
                a,
                g,
                u,
                kind,
                at,
                get("message_id"),
                card,
                event["operator"],
                event["subtype"],
                event["duration"],
            ]
        )
        enabled = self.settings().enabled and policy.enabled and now - at <= 120
        result = await self.store.call("observe", event, now, enabled)
        if result and result.get("cache_invalidated"):
            self.journal.record(
                "免查缓存失效", account=a, group=g, user=u, reason=result["cache_invalidated"], source=kind
            )
        self.journal.record("成员事件", account=a, group=g, user=u, event_type=kind, occurred=at)
        self.wake.set()

    async def inspect(self, policy, adapter, uid, *, stable=False, priority=0):
        a, g = adapter.account, policy.group_id
        version = self.version(a, g, uid)
        connection = adapter.stamp()
        memo = self.inspections.get()
        memo_key = (a, g, uid, policy.revision, connection, version)
        cached = memo.get(memo_key) if memo is not None and not stable else None
        if cached and 0 <= self.clock() - cached[0] < 10:
            subject = await self.store.call("subject", a, g, uid)
            if subject["epoch"] == cached[2]["epoch"] and subject["version"] == cached[2]["version"]:
                self.journal.record("合并资料核验", account=a, group=g, user=uid)
                await self.store.call("metric", a, g, "merged", self.clock())
                return cached[1:]
        first = await adapter.member(g, uid, priority)
        hint = await self.store.call("subject", a, g, uid)
        reconcile_role = (
            hint["role_hint"] == "admin" and hint["role_at"] < self.clock() - 300 and first.role == "member"
        )
        second = await adapter.member(g, uid, priority) if stable or reconcile_role else first
        if version != self.version(a, g, uid) or adapter.stamp() != connection:
            raise Later("核验期间资料变化，稍后重新核验。", self.clock() + 5)
        if first.card != second.card or first.joined != second.joined:
            raise Later("成员资料缓存尚未一致，稍后核验。", self.clock() + 15)
        if stable and first.muted_until != second.muted_until:
            raise Later("禁言资料正在变化，稍后核验。", self.clock() + 15)

        # Protective facts from both responses win over a later stale cache.
        def high(left, right):
            return max(left, right) if left is not None and right is not None else None

        member = replace(
            second,
            role=(first.role if first.role in ("admin", "owner") else second.role)
            if first.role in ("admin", "owner", "member") and second.role in ("admin", "owner", "member")
            else "",
            title=(first.title or second.title)
            if first.title is not None and second.title is not None
            else None,
            level=high(first.level, second.level),
            qq_level=high(first.qq_level, second.qq_level),
            muted_until=high(first.muted_until, second.muted_until),
            robot=first.robot or second.robot,
        )
        subject = await self.store.call("verified", a, g, member, self.clock())
        if subject["role_hint"] == "admin" and not (
            reconcile_role and subject["role_at"] == hint["role_at"] and first.role == second.role
        ):
            member = replace(member, role="admin")
        manual = await self.store.call("exempt", a, g, uid, self.clock())
        verdict = judge(policy, member, a, manual)
        if (
            verdict.state != "exempt"
            and subject.get("card_unconfirmed", False)
            and subject["card_at"] > self.clock() - 300
            and subject["card_hint"] is not None
            and subject["card_hint"] != member.card
        ):
            raise Later("新名片通知与接口缓存不一致，等待资料更新。", self.clock() + 15)
        self.journal.record(
            "名片判断",
            screening=True,
            account=a,
            group=g,
            user=uid,
            card=member.card,
            member_level=member.level,
            title=member.title,
            decision=verdict.state,
            reason=verdict.reason,
            revision=policy.revision,
            epoch=subject["epoch"],
        )
        await self.store.call("metric", a, g, "inspected", self.clock())
        record = None
        if verdict.state in ("compliant", "exempt"):
            record = {
                "at": self.clock(),
                "decision": verdict.state,
                "reason": verdict.reason,
                "revision": policy.revision,
                "connection": connection,
                "epoch": subject["epoch"],
                "exemption_until": await self.store.call("exemption_until", a, g, uid) if manual else 0,
            }
        await self.store.call("cache_screening", a, g, uid, subject["version"], record)
        if memo is not None:
            memo[memo_key] = (self.clock(), member, subject, verdict)
        return member, subject, verdict

    async def skip_evaluation(self, subject, reason, metric, **details):
        a, g, u = subject["account"], subject["gid"], subject["uid"]
        await self.store.call("evaluated", a, g, u, subject["speech_at"], subject["version"])
        await self.store.call("metric", a, g, metric, self.clock(), {"reason": reason})
        self.journal.record(
            "发言检查跳过", account=a, group=g, user=u, reason=reason, category=metric, **details
        )

    async def preflight(self, subject, policy, connection):
        """Skip work before even resolving identity; never skip unfinished relief."""
        a, g, u = subject["account"], subject["gid"], subject["uid"]
        if not self.settings().enabled or not policy.enabled or not subject["eval_at"]:
            await self.store.call("evaluated", a, g, u, subject["speech_at"], subject["version"])
            return True
        if self.clock() - subject["speech_at"] > 300:
            await self.skip_evaluation(
                subject,
                "发言超过5分钟，等待新的发言",
                "expired",
                speech_at=subject["speech_at"],
                deferred=subject.get("deferred"),
            )
            return True
        cases = await self.store.call("member_cases", a, g, u)
        active = [c for c in cases if c["phase"] in ("notify", "ban", "watch", "review", "settle")]
        if active:
            if not subject.get("card_unconfirmed") and any(
                c["phase"] != "watch"
                or self.clock() < c["next_round"]
                or subject["speech_at"] < c["next_round"]
                for c in active
            ):
                await self.skip_evaluation(subject, "由已有事项继续跟进，合并重复发言检查", "merged")
                return True
            return False
        cached, reason = cache_check(subject, policy, connection, self.clock())
        if cached:
            await self.skip_evaluation(
                subject,
                cached["reason"],
                "cache_hit",
                decision=cached["decision"],
                verified_at=cached["at"],
                valid_until=cached["until"],
            )
            return True
        # A router that has not resolved yet is not evidence that the connection changed.
        if subject.get("screening") and not (connection is None and reason == "连接已变化或未确认"):
            cleared = await self.store.call(
                "invalidate_screening", a, g, u, subject["screening"], reason, self.clock()
            )
            if cleared:
                self.journal.record("免查缓存失效", account=a, group=g, user=u, reason=reason)
        return False

    async def authorize(self, policy, actor, pid, account):
        adapter = await self.router.resolve(policy, expected=(pid, account), priority=2)
        if not await self.manager(adapter, policy.group_id, actor):
            raise GuardError("只有该群当前的群主或管理员可以使用此命令。")
        return adapter

    async def manager(self, adapter, gid, uid, priority=2):
        version = self.version(adapter.account, gid, uid)
        first = await adapter.member(gid, uid, priority)
        hint = await self.store.call("subject", adapter.account, gid, uid)
        negative = hint["role_hint"] == "member"
        if negative and hint["role_at"] >= self.clock() - 300:
            return False
        second = await adapter.member(gid, uid, priority) if negative or not hint["present"] else first
        if (
            first.role not in ("admin", "owner")
            or second.role not in ("admin", "owner")
            or first.joined != second.joined
        ):
            return False
        await self.store.call("verified", adapter.account, gid, second, self.clock())
        return version == self.version(adapter.account, gid, uid)

    async def evaluate(self, subject, adapter):
        self.healthy()
        a, g, u = subject["account"], subject["gid"], subject["uid"]
        settings = self.settings()
        policy = settings.group(g)
        if await self.preflight(subject, policy, adapter.stamp()):
            return
        if key(a, g) in self.paused or await self.store.call("get", "pause:" + key(a, g), ""):
            raise Later("本群已暂停新增提醒。", self.clock() + 300)
        error = self.group_errors.get(g)
        if error and error[0] == policy.revision:
            raise Later(error[1], self.clock() + 300)
        member, current, verdict = await self.inspect(policy, adapter, u)
        if verdict.state in ("compliant", "exempt"):
            await self.settle_member(a, g, u, verdict.reason)
        elif verdict.state == "invalid" and policy.mode != "仅观察":
            if await self.store.call("get", "block:" + a, {}):
                raise Later("账号新操作已暂停，请查看状态和待核对记录。", self.clock() + 300)
            if (
                member.muted_until is None
                or member.muted_until > self.clock()
                or current["ban"].get("until", 0) > self.clock()
            ):
                await self.store.call("evaluated", a, g, u, subject["speech_at"])
                return
            cases = await self.store.call("member_cases", a, g, u)
            cooldown = max((c["next_round"] for c in cases if c["sent_at"]), default=0)
            if self.clock() < cooldown or subject["speech_at"] < cooldown:
                await self.store.call("evaluated", a, g, u, subject["speech_at"])
                return
            active = [c for c in cases if c["phase"] in ("notify", "ban", "watch", "review", "settle")]
            if any(
                c["phase"] != "watch"
                or self.clock() < c["next_round"]
                or subject["speech_at"] < c["next_round"]
                for c in active
            ):
                await self.store.call("evaluated", a, g, u, subject["speech_at"])
                return
            if await self.store.call("pending_count", a) >= settings.pace.max_pending and not active:
                raise Later("待处理成员已达上限，优先完成现有核验。", self.clock() + 300)
            round_no = (
                1 if self.clock() - current["round_at"] >= policy.reset_days * 86400 else current["round"] + 1
            )
            minutes = policy.stage(round_no).minutes if policy.mode == "提醒并禁言" else 0
            if self.clock() - member.joined < policy.newcomer_minutes * 60:
                minutes = 0
            if self.settings().group(g).revision != policy.revision:
                raise Later("配置在核验期间改变，重新检查。", self.clock() + 1)
            case = await self.store.call(
                "new_case",
                current,
                policy,
                adapter.pid,
                adapter.stamp(),
                round_no,
                minutes,
                self.clock(),
                self.clock() + random.uniform(*settings.pace.interval("notify")),
                settings.pace.max_pending,
            )
            self.journal.record(
                "安排提醒", case=case["id"], account=a, group=g, user=u, round=round_no, due=case["due"]
            )
        await self.store.call("evaluated", a, g, u, subject["speech_at"], subject["version"])

    async def settle_member(self, a, g, u, reason):
        for case in await self.store.call("member_cases", a, g, u):
            if case["phase"] in ("closed", "settle", "review"):
                continue
            action = "unmute" if case["mute_state"] in ("owned", "unverified") else "recall"
            await self.store.call(
                "patch_case",
                case["id"],
                {
                    "phase": "settle",
                    "reason": reason,
                    "due": self.clock() + random.uniform(*self.settings().pace.interval(action)),
                },
            )

    async def pause(self, a, g, actor):
        self.paused.add(key(a, g))
        self.invalidate(a, g, "0")
        await self.store.call("set", "pause:" + key(a, g), "管理员暂停新增处理")
        await self.store.call("audit", self.clock(), "暂停", {"account": a, "group": g, "actor": actor})

    async def resume(self, a, g, actor):
        block = await self.store.call("get", "block:" + a, {})
        if block and block.get("gid") != g:
            raise GuardError("账号暂停来自另一个群，需由该群管理员处理。")
        if block:
            if await self.store.call("review_cases", a):
                raise GuardError("仍有结果不明的事项，请先核对或交由人工处理。")
            await self.store.call("set", "block:" + a, {})
        await self.store.call("set", "pause:" + key(a, g), "")
        self.paused.discard(key(a, g))
        until = await self.store.call(
            "extend",
            "recovery:" + a,
            self.clock() + random.uniform(*self.settings().pace.interval("recovery")),
        )
        await self.store.call(
            "audit", self.clock(), "恢复", {"account": a, "group": g, "actor": actor, "until": until}
        )
        return until

    async def _work(self, kind, item):
        from .executor import Executor

        a, g, u = item["account"], item["gid"], item["uid"]
        policy = None
        token = self.inspections.set({})
        try:
            async with self.member_lock(a, g, u):
                policy = self.policy(item) if kind == "case" else self.settings().group(g)
                if kind == "subject":
                    subject = await self.store.call("subject", a, g, u)
                    known = getattr(self.router, "known_stamp", lambda *_: None)(item["platform"], a)
                    if await self.preflight(subject, policy, known):
                        return
                urgent = (
                    kind == "case"
                    and item["mute_state"] in ("owned", "unverified")
                    and item["mute_until"] > self.clock()
                )
                adapter = await self.router.resolve(
                    policy,
                    expected=(item["platform"], a),
                    priority=(
                        2 if item["phase"] == "settle" or urgent else 1 if item["phase"] == "watch" else 0
                    )
                    if kind == "case"
                    else 0,
                )
                if kind == "case":
                    await Executor(self).process(await self.store.call("case", item["id"]), adapter)
                    if item["phase"] == "watch":
                        subject = await self.store.call("subject", a, g, u)
                        if subject["eval_at"] and subject["eval_at"] <= self.clock():
                            try:
                                await self.evaluate(subject, adapter)
                            except Later as exc:
                                await self.store.call(
                                    "defer_evaluation", a, g, u, exc.until, str(exc), exc.code
                                )
                                await self.store.call(
                                    "metric",
                                    a,
                                    g,
                                    "deferred",
                                    self.clock(),
                                    {"reason": str(exc), "until": exc.until, "code": exc.code},
                                )
                                self.journal.record(
                                    "发言检查暂缓",
                                    account=a,
                                    group=g,
                                    user=u,
                                    exception=exc,
                                    retry_at=exc.until,
                                    code=exc.code,
                                    limits=exc.details,
                                )
                else:
                    await self.evaluate(await self.store.call("subject", a, g, u), adapter)
        except GuardError as exc:
            until = getattr(exc, "until", self.clock() + 300)
            self.journal.record(
                "处理暂缓",
                account=a,
                group=g,
                user=u,
                exception=exc,
                retry_at=until,
                code=getattr(exc, "code", "deferred"),
                limits=getattr(exc, "details", {}),
            )
            if "正则" in str(exc) and policy:
                self.group_errors[g] = (policy.revision, str(exc))
            try:
                await self.store.call(
                    "metric",
                    a,
                    g,
                    "deferred",
                    self.clock(),
                    {"reason": str(exc), "until": until, "code": getattr(exc, "code", "deferred")},
                )
                if kind == "case":
                    changes = {"due": until, "reason": str(exc)}
                    if getattr(exc, "code", "") == "read_budget":
                        changes["read_defer_until"] = until
                    await self.store.call("patch_case", item["id"], changes)
                elif policy is None:
                    await self.store.call("evaluated", a, g, u, item["speech_at"])
                else:
                    await self.store.call(
                        "defer_evaluation", a, g, u, until, str(exc), getattr(exc, "code", "deferred")
                    )
            except GuardError:
                self.failure = "无法保存待处理状态，已停止操作，请检查日志后重载。"
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.failure = "发生内部异常，已停止自动操作，请查看日志后重载。"
            self.journal.record("任务异常", exception=exc, account=a, group=g, user=u)
        finally:
            self.inspections.reset(token)
            self.running.discard(key(a, g, u))

    async def tick(self):
        self.healthy()
        adjusted = await self.store.call(
            "refresh_read_deferrals", self.clock(), self.settings().pace.reads_per_hour
        )
        if adjusted:
            self.journal.record(
                "读取额度调整后重新调度", resumed=adjusted, limit=self.settings().pace.reads_per_hour
            )
        cases, subjects = await self.store.call("due", self.clock())
        for kind, item in [("case", c) for c in cases] + [("subject", s) for s in subjects]:
            k = key(item["account"], item["gid"], item["uid"])
            if k in self.running:
                continue
            if len(self.running) >= 4:
                break
            if len(self.running) >= 2 and (kind == "subject" or item["phase"] in ("notify", "ban")):
                continue
            self.running.add(k)
            task = asyncio.create_task(self._work(kind, item))
            self.jobs.add(task)
            task.add_done_callback(self.jobs.discard)

    async def loop(self):
        checkpoint = 0
        last_error = ""
        while not self.stopped:
            try:
                await self.tick()
                if self.clock() >= checkpoint:
                    await self.store.call("set", "last-wall", self.clock())
                    await self.store.call("maintain", self.clock())
                    self.journal.maintain()
                    checkpoint = self.clock() + 60
            except GuardError as exc:
                if str(exc) != last_error:
                    self.journal.record("调度暂停", exception=exc)
                    last_error = str(exc)
            except Exception as exc:
                self.failure = "调度异常，已停止自动操作，请查看日志后重载。"
                self.journal.record("调度异常", exception=exc)
            self.wake.clear()
            try:
                await asyncio.wait_for(self.wake.wait(), timeout=1)
            except asyncio.TimeoutError:
                pass
