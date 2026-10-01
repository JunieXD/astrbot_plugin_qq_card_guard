"""Real adapter latency and lifecycle regressions, with no live QQ operations."""

import asyncio
import json
from dataclasses import replace

import pytest
from conftest import ADMIN, A, G, U
from test_early_preparation import pending_ban
from test_execution_timing import PipelineBot
from test_platform import Bot, adapter

from qq_card_guard.config import Later, Stage
from qq_card_guard.executor import Executor
from qq_card_guard.resources import Journal
from qq_card_guard.scheduling import action_schedule
from qq_card_guard.timing import flow_timing


@pytest.fixture(autouse=True)
def deterministic_delays(monkeypatch):
    monkeypatch.setattr("qq_card_guard.platform.random.uniform", lambda low, high: (low + high) / 2)


async def test_local_status_is_budgeted_but_does_not_wait_or_advance_qq_read_deadline(env):
    bot, start = Bot(), env.clock()
    api = adapter(env, bot)
    api.journal = Journal(env.path)
    try:
        await api.identity()
        assert api.next_read == 0 and env.clock() == start
        await api.call("get_group_member_info")
        deadline = api.next_read
        await api.call("get_status")
        await api.identity(force=True)
        assert env.clock() == start and api.next_read == deadline
        await api.call("get_group_detail_info")
    finally:
        api.journal.close()
    assert env.clock() == start + 1.5
    status = await env.store.call("read_status", A, env.clock(), 600)
    assert status["used"] == 5
    spans = [
        json.loads(line)
        for line in (env.path / "logs/guard.log").read_text(encoding="utf-8").splitlines()
        if json.loads(line)["event"] == "平台接口完成"
    ]
    assert [r["read_scope"] for r in spans] == ["local", "qq", "local", "local", "qq"]


async def test_local_query_cannot_bypass_budget(env):
    env.box.settings = replace(env.box.settings, pace=replace(env.box.settings.pace, reads_per_hour=60))
    bot, api = Bot(), None
    api = adapter(env, bot)
    for _ in range(39):
        await api.call("get_status")
    with pytest.raises(Later) as caught:
        await api.call("get_login_info")
    assert caught.value.code == "read_budget" and len(bot.calls) == 39


async def test_local_status_waits_for_read_lock_and_cancellation_releases_it(env):
    bot = Bot()
    api = adapter(env, bot)
    entered, release = asyncio.Event(), asyncio.Event()

    async def call(action, **params):
        bot.calls.append(action)
        if action == "get_group_member_info":
            entered.set()
            await release.wait()
        return {"online": True}

    bot.call_action = call
    first = asyncio.create_task(api.call("get_group_member_info"))
    await entered.wait()
    second = asyncio.create_task(api.call("get_status"))
    await asyncio.sleep(0)
    assert bot.calls == ["get_group_member_info"]
    second.cancel()
    with pytest.raises(asyncio.CancelledError):
        await second
    release.set()
    await first
    await api.call("get_status")
    assert not api.lock.locked() and bot.calls == ["get_group_member_info", "get_status"]


async def ready_case(env, phase):
    if phase == "ban":
        return await pending_ban(env)
    env.box.settings = replace(
        env.box.settings,
        groups=(replace(env.policy, stages=(Stage(1),)),),
        pace=replace(env.box.settings.pace, notify_min=1, notify_max=2, recall_min=1, recall_max=2),
    )
    case = await env.speak()
    if phase == "settle":
        case = await env.step(case)
        await env.store.call("set", "gap:" + A, 0)
        env.adapter.people[U] = replace(env.adapter.people[U], card="大三-某某大学")
        await env.service.settle_member(A, G, U, "名片符合规则")
        case = await env.store.call("case", case["id"])
    env.clock.now = case["due"]
    return case


@pytest.mark.parametrize("phase", ["notify", "ban", "settle"])
async def test_early_timer_wakeup_corrects_once_without_repeating_status_or_sending_early(env, phase):
    case = await ready_case(env, phase)
    env.clock.now = case["due"]
    boundary = max(case[phase + "_not_before"], await env.store.call("get", "gap:" + A, 0))
    sleeps, statuses = [], []

    async def sleep(seconds):
        assert not env.service.account_locks[A].locked()
        sleeps.append(seconds)
        await env.clock.sleep(seconds - 0.005 if len(sleeps) == 1 else seconds)

    async def online(priority=2):
        statuses.append(env.clock())
        return True

    env.service.sleep = sleep
    env.adapter.online = online
    await Executor(env.service).process(case, env.adapter)
    assert len(sleeps) == 2 and len(statuses) == 1
    assert 0 < sleeps[1] <= 0.06
    assert env.adapter.writes[-1][2] >= boundary
    trace = [r for r in env.journal.records if r["kind"] == "操作耗时"][-1]
    assert trace["outcome"] == "confirmed" and trace["stages_ms"]["timer_correction"] > 0
    assert trace["scheduled_for"] == case["due"]
    assert trace["write_not_before"] == case[phase + "_not_before"]


@pytest.mark.parametrize("change", ["tiny_extension", "pause", "recovery", "rollback", "cancel"])
async def test_timer_correction_never_masks_new_wait_or_clock_rollback(env, change):
    case = await pending_ban(env)
    env.clock.now = case["due"]
    sleeps = []

    async def sleep(seconds):
        sleeps.append(seconds)
        await env.clock.sleep(seconds - 0.005)
        if change == "tiny_extension":
            await env.store.call("extend", "gap:" + A, case["ban_not_before"] + 0.003)
        elif change == "pause":
            await env.service.pause(A, G, ADMIN)
        elif change == "recovery":
            env.adapter.recovery_until = env.clock() + 0.01
        elif change == "rollback":
            env.clock.now -= 1
        else:
            raise asyncio.CancelledError()

    env.service.sleep = sleep
    with pytest.raises(asyncio.CancelledError if change == "cancel" else Later):
        await Executor(env.service).process(case, env.adapter)
    assert len(sleeps) == 1
    assert [w[0] for w in env.adapter.writes] == ["notify"]
    assert not await env.store.call("has_operation", case["id"], "ban")


@pytest.mark.parametrize("phase", ["notify", "settle"])
@pytest.mark.parametrize("change", ["card", "permission", "connection"])
async def test_notification_and_settlement_preparation_revalidates_member_and_permission(env, phase, change):
    case = await ready_case(env, phase)

    async def sleep(seconds):
        await env.clock.sleep(seconds)
        if change == "card":
            env.adapter.people[U] = replace(
                env.adapter.people[U], card="大三-某某大学" if phase == "notify" else "又改坏了"
            )
        elif change == "permission":
            env.adapter.people[A] = replace(env.adapter.people[A], role="member")
        else:
            env.adapter.binding = "new-connection"

    env.service.sleep = sleep
    before = len(env.adapter.writes)
    if change == "permission":
        with pytest.raises(Later):
            await Executor(env.service).process(case, env.adapter)
    else:
        await Executor(env.service).process(case, env.adapter)
    # Cleanup can rebind the same account after reconnect, but it still reads the
    # target and locates its own reminder before recalling. New punishment cancels.
    expected = 1 if phase == "settle" and change == "connection" else 0
    assert len(env.adapter.writes) == before + expected


@pytest.mark.parametrize("phase", ["notify", "settle"])
async def test_final_fence_checks_all_action_deadlines_after_clock_rollback(env, phase):
    case = await ready_case(env, phase)

    async def rollback(kind):
        env.clock.now = case[phase + "_not_before"] - 0.1

    env.adapter.before_hook = rollback
    with pytest.raises(Later) as caught:
        await Executor(env.service).process(case, env.adapter)
    assert caught.value.code == "action_delay"
    assert not await env.store.call("has_operation", case["id"], "notify" if phase == "notify" else "recall")


class LifecycleBot(PipelineBot):
    async def call_action(self, action, **params):
        if action in ("get_msg", "delete_msg"):
            self.calls.append({"action": action, "at": self.env.clock(), **params})
            await self.env.clock.sleep(0.2)
            return self.reminder if action == "get_msg" else {}
        started = self.env.clock()
        result = await super().call_action(action, **params)
        if action == "get_group_member_info":
            member = self.env.adapter.people[params["user_id"]]
            result.update(card=member.card, role=member.role, shut_up_timestamp=member.muted_until)
        elif action == "send_group_msg":
            self.reminder = dict(
                group_id=G,
                sender={"user_id": A},
                message_id=1234,
                time=int(started),
                message=params["message"],
            )
        elif action == "set_group_ban":
            duration = params["duration"]
            self.env.adapter.people[U] = replace(
                self.env.adapter.people[U], muted_until=int(started + duration) if duration else 0
            )
            await self.env.notice(
                "group_ban", sub_type="ban" if duration else "lift_ban", duration=duration, operator_id=A
            )
        return result


@pytest.mark.parametrize("scheduler_delay", [0, 1])
async def test_complete_real_adapter_lifecycle_is_responsive_and_logs_confirmed_milestones(
    env, scheduler_delay
):
    env.box.settings = replace(
        env.box.settings,
        groups=(replace(env.policy, stages=(Stage(1),)),),
        pace=replace(
            env.box.settings.pace,
            notify_min=0,
            notify_max=1,
            ban_min=1,
            ban_max=2,
            unmute_min=0,
            unmute_max=1,
            recall_min=1,
            recall_max=2,
        ),
    )
    received, speech = env.clock(), env.clock() - 2
    await env.service.observe(
        dict(
            post_type="message",
            message_type="group",
            group_id=G,
            user_id=U,
            self_id=A,
            time=int(speech),
            message_id=1,
            sender={"card": "未改名"},
        ),
        "platform",
    )
    await env.clock.sleep(scheduler_delay)
    bot = LifecycleBot(env)
    api = adapter(env, bot)
    await api.identity()
    subject = await env.store.call("subject", A, G, U)
    await env.service.evaluate(subject, api)
    case = (await env.store.call("member_cases", A, G, U))[0]
    assert case["trigger_received_at"] == received and case["trigger_speech_at"] == speech
    await Executor(env.service).process(case, api)
    case = await env.store.call("case", case["id"])
    notify_flow = flow_timing(case)["flow_ms"]
    assert notify_flow["received_to_notify"] == pytest.approx(6600 + scheduler_delay * 1000, abs=1)
    assert notify_flow["speech_to_notify"] < 10000
    await Executor(env.service).process(case, api)
    case = await env.store.call("case", case["id"])
    assert case["phase"] == "watch"
    env.adapter.people[U] = replace(env.adapter.people[U], card="大三-某某大学")
    await env.notice("group_card", card_new="大三-某某大学")
    await Executor(env.service).process(case, api)
    case = await env.store.call("case", case["id"])
    assert case["mute_state"] == "owned" and case["phase"] == "settle"
    detected = case["settlement_detected_at"]
    for _ in range(2):
        await Executor(env.service).process(case, api)
        case = await env.store.call("case", case["id"])
    assert case["phase"] == "closed" and case["mute_state"] == "released"
    assert case["settlement_detected_at"] == detected
    final = [r for r in env.journal.records if r["kind"] == "操作完成"][-1]
    assert final["flow_ms"] == flow_timing(case)["flow_ms"]
    assert final["flow_ms"]["detection_to_unmute"] < 6000
    assert final["flow_ms"]["unmute_to_recall"] < 9000
    assert [c["duration"] for c in bot.calls if c["action"] == "set_group_ban"] == [60, 0]
    assert len([c for c in bot.calls if c["action"] == "delete_msg"]) == 1
    assert not any(r["kind"] == "跳过解禁" for r in env.journal.records)


async def test_expired_ban_logs_skipped_unmute_once_and_does_not_lift_other_bans(env):
    case = await pending_ban(env)
    case = await env.step(case)
    env.clock.now = case["mute_until"] + 1
    env.adapter.people[U] = replace(env.adapter.people[U], card="大三-某某大学", muted_until=0)
    case = await env.step(case)
    case = await env.step(case)
    assert case["phase"] == "closed"
    assert case["unmute_skip_reason"] == "expired"
    assert [w[0] for w in env.adapter.writes] == ["notify", "ban", "recall"]
    assert [r["reason"] for r in env.journal.records if r["kind"] == "跳过解禁"] == ["expired"]
    assert "detection_to_unmute" not in flow_timing(case)["flow_ms"]


def test_legacy_cases_and_rollback_do_not_fabricate_flow_milestones():
    assert flow_timing({}) == {"flow_at": {}, "flow_ms": {}}
    case = dict(trigger_speech_at=100, trigger_received_at=99, notify_completed_at=98)
    assert flow_timing(case)["flow_ms"] == {}
    assert action_schedule("notify", 100, 600)["due"] == 698


@pytest.mark.parametrize("change", ["admin_ban", "expired", "invalid", "departed"])
async def test_settlement_delay_cannot_release_changed_or_expired_mute(env, change):
    env.box.settings = replace(
        env.box.settings, pace=replace(env.box.settings.pace, unmute_min=1, unmute_max=2)
    )
    case = await pending_ban(env)
    case = await env.step(case)
    env.adapter.people[U] = replace(env.adapter.people[U], card="大三-某某大学")
    await Executor(env.service).process(case, env.adapter)
    case = await env.store.call("case", case["id"])
    assert case["phase"] == "settle" and case["mute_state"] == "owned"

    async def sleep(seconds):
        assert not env.service.account_locks[A].locked()
        await env.clock.sleep(seconds)
        if change == "admin_ban":
            env.adapter.people[U] = replace(env.adapter.people[U], muted_until=int(env.clock()) + 600)
            await env.notice("group_ban", sub_type="ban", duration=600, operator_id=ADMIN)
        elif change == "expired":
            env.adapter.people[U] = replace(env.adapter.people[U], muted_until=0)
        elif change == "invalid":
            env.adapter.people[U] = replace(env.adapter.people[U], card="又改坏了")
        else:
            await env.notice("group_decrease", sub_type="leave", operator_id=U)
            env.adapter.people.pop(U)

    env.service.sleep = sleep
    await Executor(env.service).process(case, env.adapter)
    saved = await env.store.call("case", case["id"])
    assert "unmute" not in [w[0] for w in env.adapter.writes]
    if change == "invalid":
        assert saved["phase"] == "watch" and saved["settlement_detected_at"] == 0
    else:
        assert saved["phase"] == "closed" and saved["recall_state"] == "recalled"


async def test_repeated_early_wakeup_has_bounded_correction_and_still_defers(env):
    case = await pending_ban(env)
    env.clock.now = case["due"]
    sleeps = []

    async def sleep(seconds):
        sleeps.append(seconds)
        await env.clock.sleep(max(0, seconds - 0.02))

    env.service.sleep = sleep
    with pytest.raises(Later):
        await Executor(env.service).process(case, env.adapter)
    assert len(sleeps) == 2 and not env.service.account_locks[A].locked()
    assert [w[0] for w in env.adapter.writes] == ["notify"]


async def test_late_wakeup_does_not_execute_expired_ban(env):
    case = await pending_ban(env)
    env.clock.now = case["due"]

    async def sleep(seconds):
        await env.clock.sleep(seconds + 121)

    env.service.sleep = sleep
    await Executor(env.service).process(case, env.adapter)
    saved = await env.store.call("case", case["id"])
    assert saved["phase"] == "watch" and "过期" in saved["reason"]
    assert [w[0] for w in env.adapter.writes] == ["notify"]


async def test_message_received_during_verification_does_not_replace_trigger_time(env):
    first = env.clock()
    triggered = False

    async def read(uid):
        nonlocal triggered
        if uid == U and not triggered:
            triggered = True
            await env.clock.sleep(1)
            await env.service.observe(
                dict(
                    post_type="message",
                    message_type="group",
                    group_id=G,
                    user_id=U,
                    self_id=A,
                    time=int(env.clock()),
                    message_id=99,
                    sender={"card": "未改名"},
                ),
                "platform",
            )

    env.adapter.read_hook = read
    case = await env.speak()
    assert case["trigger_speech_at"] == case["trigger_received_at"] == first
    assert case["created"] > case["trigger_received_at"]
