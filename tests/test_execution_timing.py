import asyncio
import json
from dataclasses import replace

import pytest
from conftest import A, G, U
from test_platform import Bot, adapter

from qq_card_guard.config import GuardError, Later, Stage, parse_settings
from qq_card_guard.executor import Executor
from qq_card_guard.resources import Journal
from qq_card_guard.timing import ActionTiming, api_timing


@pytest.mark.parametrize("wait,change,expected", [(0, False, 1), (4, False, 2), (0, True, 2)])
async def test_online_reuse_is_scoped_to_one_attempt_and_unchanged_connection(env, wait, change, expected):
    case = await env.speak()
    calls = []

    async def online(priority=2):
        calls.append(env.clock())
        return True

    class Guard:
        class deferred_error(Exception):
            pass

        async def run(self, **kwargs):
            assert await kwargs["online"]()
            env.clock.now += wait
            if change:
                env.adapter.binding = "new-connection"
            assert await kwargs["online"]()
            await kwargs["action"]()

    env.adapter.online = online
    env.router.guard = Guard()
    await env.step(case)
    assert len(calls) == expected
    if change:
        assert not env.adapter.writes
    else:
        assert [w[0] for w in env.adapter.writes] == ["notify"]
    trace = [r for r in env.journal.records if r["kind"] == "操作耗时"][-1]
    assert trace["stages_ms"]["queue_wait"] == wait * 1000


async def test_false_online_result_is_never_cached(env):
    case = await env.speak()
    env.clock.now = case["notify_not_before"]
    calls = []

    async def offline(priority=2):
        calls.append(1)
        return False

    class Guard:
        class deferred_error(Exception):
            pass

        async def run(self, **kwargs):
            assert not await kwargs["online"]()
            assert not await kwargs["online"]()
            raise self.deferred_error("离线")

    env.adapter.online = offline
    env.router.guard = Guard()
    with pytest.raises(Later):
        await env.step(case)
    assert len(calls) == 2 and not env.adapter.writes


async def test_short_identity_reuse_expires_and_cannot_survive_reconnect(env):
    bot = Bot()
    api = adapter(env, bot)
    await api.identity()
    start = api.identity_at
    env.clock.now += 9
    assert await api.identity(max_age=10) == A
    assert api.identity_at == start and len(bot.calls) == 1
    env.clock.now += 1
    await api.identity(max_age=10)
    assert len(bot.calls) == 2
    bot._wsr_api_clients[A] = object()
    await api.identity(max_age=10)
    assert len(bot.calls) == 3
    await api.identity(force=True, max_age=10)
    assert len(bot.calls) == 4
    env.clock.now -= 10
    await api.identity(max_age=10)
    assert len(bot.calls) == 5


async def test_identity_response_from_replaced_connection_is_not_cached(env):
    bot = Bot()
    api = adapter(env, bot)

    async def replaced(**kwargs):
        bot._wsr_api_clients[A] = object()
        return {"user_id": A}

    bot.call_action = replaced
    with pytest.raises(Later, match="连接变化"):
        await api.identity(max_age=10)
    assert not api.identity_at and not api.account


async def test_write_identity_without_bound_websocket_is_always_rechecked(env):
    bot = Bot()
    del bot._wsr_api_clients
    api = adapter(env, bot)
    await api.identity()
    bot.result = {"user_id": "999999"}
    with pytest.raises(GuardError, match="账号改变"):
        await api.identity(max_age=10)
    assert len(bot.calls) == 2


async def test_slow_identity_response_does_not_extend_short_reuse_window(env):
    bot = Bot()
    api = adapter(env, bot)
    start = env.clock()

    async def slow(**kwargs):
        bot.calls.append(kwargs)
        await env.clock.sleep(11)
        return {"user_id": A}

    bot.call_action = slow
    await api.identity()
    assert api.identity_at == start
    await api.identity(max_age=10)
    assert len(bot.calls) == 2


async def test_read_pacing_follows_config_and_remains_serial(env, monkeypatch):
    monkeypatch.setattr("qq_card_guard.platform.random.uniform", lambda low, high: high)
    env.box.settings = replace(env.box.settings, pace=replace(env.box.settings.pace, read_min=1, read_max=2))
    bot = Bot()
    api = adapter(env, bot)
    at = env.clock()
    await api.call("first")
    await api.call("second")
    assert env.clock() - at == 2
    env.box.settings = replace(env.box.settings, pace=replace(env.box.settings.pace, read_min=3, read_max=4))
    await api.call("third")  # Preserve the interval already reserved by the previous call.
    await api.call("fourth")
    assert env.clock() - at == 8


@pytest.mark.parametrize("pace", [{"read_min": 0}, {"read_min": 3, "read_max": 2}, {"gap_min": 0}])
def test_invalid_intervals_fail_closed(pace):
    with pytest.raises(GuardError):
        parse_settings({"pace": pace})


async def test_cancelled_queue_records_elapsed_and_cleans_timing_context(env):
    case = await env.speak()
    entered = asyncio.Event()

    class Guard:
        class deferred_error(Exception):
            pass

        async def run(self, **kwargs):
            entered.set()
            await asyncio.Event().wait()

    env.router.guard = Guard()
    task = asyncio.create_task(env.step(case))
    await entered.wait()
    env.clock.now += 7
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    trace = [r for r in env.journal.records if r["kind"] == "操作耗时"][-1]
    assert trace["outcome"] == "cancelled"
    assert trace["stages_ms"]["queue_wait"] == 7000
    assert not env.service.account_locks[A].locked() and not env.adapter.writes
    assert not trace["details_ms"]
    with api_timing("outside"):
        env.clock.now += 5
    assert not trace["details_ms"]


async def test_timing_correlates_api_logs_and_preserves_uncertain_outcome(env):
    case = await env.speak()
    journal = env.service.journal = Journal(env.path)
    try:
        with pytest.raises(asyncio.CancelledError):
            with ActionTiming(env.service, case, "notify").trace() as timing:
                with timing.stage("submit"), api_timing("api:send_group_msg"):
                    env.clock.now += 2
                    journal.record("平台接口开始", api="send_group_msg")
                    timing.outcome = "uncertain"
                    raise asyncio.CancelledError()
        journal.record("outside")
    finally:
        journal.close()
        env.service.journal = env.journal
    call, trace, outside = [
        json.loads(s) for s in (env.path / "logs/guard.log").read_text(encoding="utf-8").splitlines()
    ]
    assert call["case"] == case["id"] and call["attempt"] == trace["attempt"]
    assert trace["outcome"] == "uncertain" and trace["duration_ms"] == 2000
    assert trace["stages_ms"]["submit"] == trace["details_ms"]["api:send_group_msg"] == 2000
    assert "attempt" not in outside


class PipelineBot(Bot):
    """Real adapter and pacing with deterministic OneBot responses, no QQ traffic."""

    def __init__(self, env):
        super().__init__()
        self.env = env

    async def call_action(self, action, **params):
        self.calls.append({"action": action, "at": self.env.clock(), **params})
        await self.env.clock.sleep(0.2)
        if action == "get_login_info":
            return {"user_id": A}
        if action == "get_status":
            return {"online": True, "good": True}
        if action == "get_group_detail_info":
            return {"group_id": G, "group_all_shut": 0}
        if action == "get_group_member_info":
            return dict(
                group_id=G,
                user_id=params["user_id"],
                card="未改名",
                role="admin" if params["user_id"] == A else "member",
                join_time=self.env.adapter.people[U].joined,
                level=1,
                title="",
                shut_up_timestamp=0,
            )
        if action == "send_group_msg":
            return {"message_id": 1234}
        if action == "set_group_ban":
            return {}
        raise AssertionError(action)


@pytest.mark.parametrize("early", [False, True])
async def test_reminder_to_ban_pipeline_is_faster_and_keeps_fresh_member_checks(env, monkeypatch, early):
    monkeypatch.setattr("qq_card_guard.platform.random.uniform", lambda low, high: (low + high) / 2)
    env.box.settings = replace(
        env.box.settings,
        groups=(replace(env.policy, stages=(Stage(1),)),),
        pace=replace(env.box.settings.pace, notify_min=1, notify_max=2, ban_min=1, ban_max=2),
    )
    case = await env.speak()
    bot = PipelineBot(env)
    api = adapter(env, bot)
    await api.identity()
    case = await env.store.call("patch_case", case["id"], {"connection": api.stamp()})

    class Guard:
        class deferred_error(Exception):
            pass

        async def run(self, **kwargs):
            assert await kwargs["online"]()
            assert await kwargs["online"]()
            await kwargs["action"]()

    env.router.guard = Guard()
    for stage in range(2):
        due = case["due"]
        if stage == 1 and not early:
            due = max(case["ban_not_before"], await env.store.call("get", "gap:" + A, 0))
        env.clock.now = max(due, env.clock()) + 0.01
        await Executor(env.service).process(case, api)
        case = await env.store.call("case", case["id"])
    assert case["phase"] == "watch" and case["mute_state"] == "unverified"
    actions = [c["action"] for c in bot.calls]
    assert actions.count("get_status") == 2  # One per write, rather than two per write.
    assert actions[-1] == "set_group_ban"
    after_notify = bot.calls[actions.index("send_group_msg") + 1 :]
    assert len([c for c in after_notify if c["action"] == "get_group_member_info" and c["user_id"] == U]) == 2
    trace = [r for r in env.journal.records if r["kind"] == "操作耗时" and r["phase"] == "ban"][-1]
    assert trace["outcome"] == "confirmed" and trace["reminder_reference"] == "completed"
    assert 1500 <= trace["since_reminder_ms"] < 11000
    assert trace["configured_delay_ms"] == 1500
    assert trace["since_reminder_ms"] == pytest.approx(6400 if early else 6610, abs=1)
    if early:
        assert trace["preparation_overlap_ms"] == pytest.approx(200, abs=1)
    assert (after_notify[0]["at"] < trace["write_not_before"]) == early
    assert after_notify[-1]["at"] >= trace["write_not_before"]
    assert trace["details_ms"]["read_wait"] > trace["details_ms"]["api:set_group_ban"]
    assert abs(sum(trace["stages_ms"].values()) - trace["duration_ms"]) < 1
