"""Delay overlap must never turn into an early or stale moderation action."""

import asyncio
from dataclasses import replace

import pytest
from conftest import ADMIN, A, G, U
from test_execution_timing import PipelineBot
from test_platform import adapter

from qq_card_guard.config import Later, Stage
from qq_card_guard.executor import Executor
from qq_card_guard.store import key


@pytest.fixture(autouse=True)
def deterministic_delays(monkeypatch):
    monkeypatch.setattr("qq_card_guard.executor.random.uniform", lambda low, high: (low + high) / 2)


async def pending_ban(env, delay=1.5):
    env.box.settings = replace(
        env.box.settings,
        groups=(replace(env.policy, stages=(Stage(1),)),),
        pace=replace(env.box.settings.pace, ban_min=delay, ban_max=delay),
    )
    case = await env.step(await env.speak())
    assert case["phase"] == "ban"
    return case


@pytest.mark.parametrize("delay", [0, 1, 2, 600])
async def test_preview_is_bounded_and_writes_respect_both_deadlines(env, delay):
    case = await pending_ban(env, delay)
    start = case["notify_completed_at"]
    assert case["due"] == start + max(0, delay - 2)
    assert case["execute_before"] == start + delay + 120
    boundary = max(case["ban_not_before"], await env.store.call("get", "gap:" + A, 0))
    env.clock.now = case["due"]
    reads = []

    async def online(priority=2):
        assert not env.service.account_locks[A].locked()
        reads.append(env.clock())
        return True

    async def member_read(uid):
        assert env.clock() >= boundary

    env.adapter.online = online
    env.adapter.read_hook = member_read
    await Executor(env.service).process(case, env.adapter)
    assert reads[0] < boundary
    assert env.adapter.writes[-1][0] == "ban"
    assert env.adapter.writes[-1][2] >= boundary


@pytest.mark.parametrize("blocked", ["startup", "recovery", "connection", "pause", "unknown", "long_gap"])
async def test_early_preparation_does_not_bypass_cooldowns_or_pause(env, blocked):
    case = await pending_ban(env)
    if blocked == "connection":
        env.adapter.recovery_until = env.clock() + 0.5
    else:
        k, value = {
            "startup": ("startup", env.clock() + 0.5),
            "recovery": ("recovery:" + A, env.clock() + 0.5),
            "pause": ("pause:" + key(A, G), "暂停"),
            "unknown": ("block:" + A, {"gid": G}),
            "long_gap": ("gap:" + A, env.clock() + 5),
        }[blocked]
        await env.store.call("set", k, value)

    async def forbidden(*args, **kwargs):
        pytest.fail("A paused/cooled account must not start speculative queries")

    env.adapter.online = forbidden
    env.adapter.identity = forbidden
    with pytest.raises(Later):
        await Executor(env.service).process(case, env.adapter)
    assert [w[0] for w in env.adapter.writes] == ["notify"]


@pytest.mark.parametrize(
    "change", ["card", "exempt", "admin_lift", "config", "connection", "pause", "permission", "all_muted"]
)
async def test_changes_during_remaining_delay_cannot_use_stale_member_facts(env, change):
    case = await pending_ban(env)
    sleeps = []

    async def sleep(seconds):
        assert not env.service.account_locks[A].locked()
        sleeps.append(seconds)
        await env.clock.sleep(seconds)
        if change == "card":
            # No event: fresh reads alone must detect compliance.
            env.adapter.people[U] = replace(env.adapter.people[U], card="大三-某某大学")
        elif change == "exempt":
            env.adapter.people[U] = replace(env.adapter.people[U], role="admin")
        elif change == "admin_lift":
            await env.notice("group_ban", sub_type="lift_ban", duration=0, operator_id=ADMIN)
        elif change == "config":
            env.box.settings = replace(env.box.settings, groups=())
        elif change == "connection":
            env.adapter.binding = "new-session"
        elif change == "pause":
            await env.service.pause(A, G, ADMIN)
        elif change == "permission":
            env.adapter.people[A] = replace(env.adapter.people[A], role="member")
        elif change == "all_muted":
            env.adapter.all_ban = True

    env.service.sleep = sleep
    if change in ("pause", "permission", "all_muted"):
        with pytest.raises(Later):
            await Executor(env.service).process(case, env.adapter)
    else:
        await Executor(env.service).process(case, env.adapter)
    assert len(sleeps) == 1
    assert [w[0] for w in env.adapter.writes] == ["notify"]
    assert not await env.store.call("has_operation", case["id"], "ban")


async def test_gap_extended_during_delay_defers_without_busy_waiting_or_queue_lock(env):
    case = await pending_ban(env)
    sleeps = []

    async def sleep(seconds):
        assert not env.service.account_locks[A].locked()
        sleeps.append(seconds)
        await env.clock.sleep(seconds)
        await env.store.call("extend", "gap:" + A, env.clock() + 1)

    env.service.sleep = sleep
    with pytest.raises(Later) as caught:
        await Executor(env.service).process(case, env.adapter)
    assert caught.value.code == "operation_gap" and len(sleeps) == 1
    assert not await env.store.call("has_operation", case["id"], "ban")


@pytest.mark.parametrize("failure", ["cancel", "budget"])
async def test_interrupted_preview_releases_work_and_preserves_safe_retry(env, failure):
    case = await pending_ban(env)
    entered = asyncio.Event()

    async def online(priority=2):
        entered.set()
        if failure == "budget":
            raise Later("读取预算不足", env.clock() + 60, code="read_budget")
        await asyncio.Event().wait()

    env.adapter.online = online
    env.service.running.add(key(A, G, U))
    task = asyncio.create_task(env.service._work("case", case))
    await entered.wait()
    if failure == "cancel":
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    else:
        await task
        saved = await env.store.call("case", case["id"])
        assert saved["due"] == saved["read_defer_until"] == env.clock() + 60
    assert not env.service.running and not env.service.account_locks[A].locked()
    assert not await env.store.call("has_operation", case["id"], "ban")
    trace = [r for r in env.journal.records if r["kind"] == "操作耗时"][-1]
    assert trace["outcome"] == ("cancelled" if failure == "cancel" else "deferred")


async def test_old_case_has_no_preview_and_keeps_original_due(env):
    case = await pending_ban(env)
    case.pop("ban_not_before")
    case["due"] = env.clock() + 1.5
    with pytest.raises(Later, match="操作间隔"):
        await Executor(env.service).process(case, env.adapter)
    env.clock.now = case["due"]
    await Executor(env.service).process(case, env.adapter)
    assert env.adapter.writes[-1][0] == "ban"


@pytest.mark.parametrize("queue_delay", [0.5, 15])
async def test_preview_is_revalidated_after_long_queue_and_queue_timing_is_separate(env, queue_delay):
    case = await pending_ban(env)
    bot = PipelineBot(env)
    api = adapter(env, bot)
    # Model a bound identity last confirmed more than ten seconds ago.
    api.account = A
    api.identity_at = env.clock() - 11
    case = await env.store.call("patch_case", case["id"], {"connection": api.stamp()})

    class Guard:
        class deferred_error(Exception):
            pass

        async def run(self, **kwargs):
            assert not env.service.account_locks[A].locked()
            assert await kwargs["online"]()
            await env.clock.sleep(queue_delay)
            assert await kwargs["online"]()
            await kwargs["action"]()

    env.router.guard = Guard()
    await Executor(env.service).process(case, api)
    actions = [c["action"] for c in bot.calls]
    assert actions.count("get_status") == (2 if queue_delay == 15 else 1)
    assert actions.count("get_login_info") == (2 if queue_delay == 15 else 1)
    assert actions[-1] == "set_group_ban"
    trace = [r for r in env.journal.records if r["kind"] == "操作耗时"][-1]
    assert trace["stages_ms"]["queue_wait"] == pytest.approx(queue_delay * 1000, abs=1)
    assert trace["stages_ms"]["identity_preview"] > 0
    assert abs(sum(trace["stages_ms"].values()) - trace["duration_ms"]) < 1


async def test_reminder_completion_wakes_scheduler_after_releasing_member_slot(env):
    env.box.settings = replace(env.box.settings, groups=(replace(env.policy, stages=(Stage(1),)),))
    case = await env.speak()
    env.clock.now = case["due"]
    env.service.wake.clear()
    env.service.running.add(key(A, G, U))
    await env.service._work("case", case)
    assert env.service.wake.is_set() and not env.service.running
    case = await env.store.call("case", case["id"])
    assert case["phase"] == "ban"


async def test_final_fence_cannot_send_before_persisted_ban_deadline(env):
    case = await pending_ban(env)

    async def clock_rollback(kind):
        env.clock.now = case["ban_not_before"] - 0.5

    env.adapter.before_hook = clock_rollback
    with pytest.raises(Later) as caught:
        await Executor(env.service).process(case, env.adapter)
    assert caught.value.code == "action_delay"
    assert not await env.store.call("has_operation", case["id"], "ban")
    assert [w[0] for w in env.adapter.writes] == ["notify"]


async def test_reload_cancels_pending_ban_even_with_early_preparation_schedule(env):
    case = await pending_ban(env)
    await env.store.call("close_db")
    await env.store.call("open_db")
    saved = await env.store.call("case", case["id"])
    assert saved["phase"] == "watch"
    assert saved["ban_not_before"] == case["ban_not_before"]
    await Executor(env.service).process(saved, env.adapter)
    assert [w[0] for w in env.adapter.writes] == ["notify"]
