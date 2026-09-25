import asyncio
from dataclasses import replace
from types import SimpleNamespace

import pytest

from qq_card_guard.config import GuardError, Pace, Policy, Settings
from qq_card_guard.executor import Executor
from qq_card_guard.platform import Adapter
from qq_card_guard.resources import Journal
from qq_card_guard.rules import Member
from qq_card_guard.service import Service
from qq_card_guard.store import Store

A, G, U, ADMIN = "100001", "100002", "200001", "300001"


class Clock:
    def __init__(self):
        self.now = 1800000000.0

    def __call__(self):
        return self.now

    async def sleep(self, seconds):
        self.now += seconds
        await asyncio.sleep(0)


class FakeJournal(Journal):
    def __init__(self):
        self.records = []
        self.failure = None

    def record(self, kind, detail="", **fields):
        self.records.append({"kind": kind, "detail": detail, **fields})

    def check(self):
        if self.failure:
            raise self.failure

    def maintain(self):
        self.check()


class FakeAdapter:
    account, pid = A, "platform"
    recovery_until = 0

    def __init__(self, clock):
        self.clock = clock
        self.people = {
            uid: Member(uid, "member", "未改名", int(clock() - 100000), 0, 1, "", 0)
            for uid in (A, U, ADMIN, "200002")
        }
        self.people[A] = replace(self.people[A], role="admin")
        self.people[ADMIN] = replace(self.people[ADMIN], role="owner")
        self.writes, self.reads, self.messages = [], [], {}
        self.binding = "session:0"
        self.connected = True
        self.all_ban = False
        self.before_hook = None
        self.after_hook = None
        self.read_hook = None
        self.fail = ""
        self.notices = True
        self.service = None

    def stamp(self):
        return self.binding

    async def identity(self, force=False, priority=0):
        return self.account

    async def online(self, priority=2):
        return self.connected

    async def all_muted(self, gid):
        return self.all_ban

    async def member(self, gid, uid, priority=0):
        self.reads.append((gid, uid, priority))
        if self.read_hook:
            await self.read_hook(uid)
        if uid not in self.people:
            raise GuardError("成员不存在")
        return self.people[uid]

    async def call(self, action, **kwargs):
        assert action == "get_msg"
        if self.fail == "locate":
            raise GuardError("旧消息映射丢失")
        return self.messages.get(kwargs["message_id"])

    locate_message = Adapter.locate_message

    async def write(self, kind, before_send, data):
        if self.before_hook:
            await self.before_hook(kind)
        before_send()
        self.writes.append((kind, data, self.clock()))
        if self.after_hook:
            await self.after_hook(kind)
        if self.fail == kind:
            raise GuardError("模拟请求超时")

    async def notify(self, gid, message, before_send):
        await self.write("notify", before_send, message)
        mid = str(1000 + len(self.messages))
        self.messages[mid] = dict(
            group_id=gid, message_id=mid, sender={"user_id": A}, time=int(self.clock()), message=message
        )
        return mid

    async def ban(self, gid, uid, seconds, before_send):
        await self.write("ban" if seconds else "unmute", before_send, seconds)
        self.people[uid] = replace(
            self.people[uid], muted_until=int(self.clock()) + seconds if seconds else 0
        )
        if self.notices:
            await self.service.observe(
                dict(
                    post_type="notice",
                    notice_type="group_ban",
                    group_id=gid,
                    user_id=uid,
                    self_id=A,
                    operator_id=A,
                    time=int(self.clock()),
                    duration=seconds,
                    sub_type="ban" if seconds else "lift_ban",
                ),
                self.pid,
            )

    async def recall(self, mid, before_send):
        await self.write("recall", before_send, mid)


class FakeRouter:
    def __init__(self, adapter):
        self.adapter = adapter
        self.guard = None

    async def resolve(self, policy, expected=None, priority=0):
        return self.adapter

    def shared_guard(self):
        return self.guard


@pytest.fixture
async def env(tmp_path):
    clock = Clock()
    policy = Policy(G, mode="提醒并禁言")
    box = SimpleNamespace(settings=Settings(True, (policy,), Pace()))
    store = Store(tmp_path / "state.sqlite3")
    await store.call("open_db")
    adapter, journal = FakeAdapter(clock), FakeJournal()
    router = FakeRouter(adapter)
    service = Service(
        lambda: box.settings, store, router, journal, clock=clock, sleep=clock.sleep, monotonic=clock
    )
    adapter.service = service
    event_id = 0

    async def speak(uid=U, age=0):
        nonlocal event_id
        event_id += 1
        await service.observe(
            dict(
                post_type="message",
                message_type="group",
                group_id=G,
                user_id=uid,
                self_id=A,
                time=int(clock()) - age,
                message_id=event_id,
                sender={"card": adapter.people[uid].card},
                message="SHOULD_NOT_PERSIST",
            ),
            adapter.pid,
        )
        subject = await store.call("subject", A, G, uid)
        await service.evaluate(subject, adapter)
        cases = await store.call("member_cases", A, G, uid)
        return cases[0] if cases else None

    async def step(case):
        case = await store.call("case", case["id"])
        clock.now = max(clock(), case["due"], await store.call("get", "gap:" + A, 0)) + 1
        await Executor(service).process(case, adapter)
        return await store.call("case", case["id"])

    async def notice(kind, uid=U, **fields):
        await service.observe(
            dict(
                post_type="notice",
                notice_type=kind,
                group_id=G,
                user_id=uid,
                self_id=A,
                time=int(clock()),
                **fields,
            ),
            adapter.pid,
        )

    yield SimpleNamespace(
        clock=clock,
        policy=policy,
        box=box,
        store=store,
        adapter=adapter,
        journal=journal,
        router=router,
        service=service,
        speak=speak,
        step=step,
        notice=notice,
        path=tmp_path,
    )
    await service.stop()
    await store.close()
