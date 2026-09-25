import asyncio
from dataclasses import replace

import pytest
from conftest import A, G, U

from qq_card_guard.config import GuardError, Later, Stage
from qq_card_guard.executor import Executor


def policy(env, **changes):
    env.box.settings = replace(env.box.settings, groups=(replace(env.policy, **changes),))


async def muted_case(env):
    policy(env, stages=(Stage(10),))
    case = await env.step(await env.speak())
    assert case["phase"] == "ban"
    case = await env.step(case)
    assert case["phase"] == "watch"
    return case


async def comply(env):
    env.adapter.people[U] = replace(env.adapter.people[U], card="大三-某某大学")
    await env.notice("group_card", card_new="大三-某某大学", card_old="bad")


async def test_burst_is_one_round_and_grace_requires_new_speech(env):
    case = await env.speak()
    for _ in range(5):
        assert (await env.speak())["id"] == case["id"]
    case = await env.step(case)
    assert [w[0] for w in env.adapter.writes] == ["notify"]
    assert case["round"] == 1 and case["minutes"] == 0
    await env.speak()
    assert len(await env.store.call("member_cases", A, G, U)) == 1
    env.clock.now = case["next_round"] + 1
    await env.step(case)  # A timer only polls; no round escalation.
    assert len(env.adapter.writes) == 1
    second = await env.speak()
    assert second["round"] == 2 and second["minutes"] == 10
    assert second["id"] != case["id"]


async def test_unmute_before_recall_and_closed_incident_keeps_round(env):
    case = await muted_case(env)
    await comply(env)
    case = await env.step(case)
    assert case["phase"] == "settle" and case["mute_state"] == "owned"
    case = await env.step(case)
    assert case["mute_state"] == "released"
    case = await env.step(case)
    assert case["phase"] == "closed"
    assert [w[0] for w in env.adapter.writes] == ["notify", "ban", "unmute", "recall"]
    env.clock.now += 1000
    env.adapter.people[U] = replace(env.adapter.people[U], card="bad")
    second = await env.speak()
    assert second["round"] == 2


async def test_failed_recall_never_prevents_unmute(env):
    case = await muted_case(env)
    await comply(env)
    case = await env.step(await env.step(case))
    assert env.adapter.people[U].muted_until == 0
    env.adapter.fail = "recall"
    case = await env.step(case)
    assert case["recall_state"] == "manual"
    await env.step(case)
    assert [w[0] for w in env.adapter.writes].count("recall") == 1


@pytest.mark.parametrize("kind", ["notify", "ban"])
async def test_unknown_write_is_never_retried_and_blocks_account(env, kind):
    policy(env, stages=(Stage(10),))
    case = await env.speak()
    if kind == "ban":
        case = await env.step(case)
    env.adapter.fail = kind
    case = await env.step(case)
    assert case["phase"] == "review"
    assert await env.store.call("get", "block:" + A, {})
    await env.step(case)
    assert [w[0] for w in env.adapter.writes].count(kind) == 1
    if kind == "notify":
        assert "ban" not in [w[0] for w in env.adapter.writes]


@pytest.mark.parametrize("change", ["card", "admin", "title", "leave", "config", "connection", "disable"])
async def test_waiting_changes_cancel_punishment(env, change):
    case = await env.speak()
    if change == "card":
        await comply(env)
    elif change == "admin":
        await env.notice("group_admin", sub_type="set")
    elif change == "title":
        env.adapter.people[U] = replace(env.adapter.people[U], title="头衔")
    elif change == "leave":
        await env.notice("group_decrease", sub_type="leave")
    elif change == "config":
        policy(env, pattern=".*")
    elif change == "connection":
        env.adapter.binding = "another-session"
    else:
        env.box.settings = replace(env.box.settings, enabled=False)
    await env.step(case)
    assert not env.adapter.writes


async def test_notice_inside_transport_fence_stops_write_and_frees_intent(env):
    case = await env.speak()

    async def intervene(kind):
        await env.notice("group_admin", sub_type="set")

    env.adapter.before_hook = intervene
    with pytest.raises(Later, match="提交前"):
        await env.step(case)
    assert not env.adapter.writes
    assert not await env.store.call("has_operation", case["id"], "notify")


async def test_failed_or_cancelled_reminder_never_mutes(env):
    policy(env, stages=(Stage(10),))
    case = await env.speak()

    async def cancel(kind):
        raise asyncio.CancelledError

    env.adapter.after_hook = cancel
    with pytest.raises(asyncio.CancelledError):
        await env.step(case)
    stored = await env.store.call("case", case["id"])
    assert stored["phase"] == "review" and not stored["message_id"]
    op = await env.store.call("has_operation", case["id"], "notify")
    assert op["status"] == "unknown"


async def test_cancel_before_send_releases_reservation(env):
    case = await env.speak()

    async def cancel(kind):
        raise asyncio.CancelledError

    env.adapter.before_hook = cancel
    with pytest.raises(asyncio.CancelledError):
        await env.step(case)
    assert not await env.store.call("has_operation", case["id"], "notify")
    assert not env.adapter.writes


@pytest.mark.parametrize(
    "intervention", ["other_admin", "same_bot", "manual_unmute", "no_notice", "new_membership"]
)
async def test_ambiguous_or_external_mute_is_never_unmuted(env, intervention):
    if intervention == "no_notice":
        env.adapter.notices = False
    case = await muted_case(env)
    env.clock.now += 30
    if intervention in ("other_admin", "same_bot"):
        await env.notice(
            "group_ban",
            sub_type="ban",
            duration=1800,
            operator_id=A if intervention == "same_bot" else "300001",
        )
        env.adapter.people[U] = replace(env.adapter.people[U], muted_until=int(env.clock()) + 1800)
    elif intervention == "manual_unmute":
        await env.notice("group_ban", sub_type="lift_ban", duration=0, operator_id="300001")
        env.adapter.people[U] = replace(env.adapter.people[U], muted_until=0)
    elif intervention == "new_membership":
        await env.notice("group_decrease", sub_type="leave")
        env.clock.now += 2
        await env.notice("group_increase", sub_type="approve")
        env.adapter.people[U] = replace(env.adapter.people[U], joined=int(env.clock()))
    await comply(env)
    env.clock.now = max(env.clock(), case["ban_at"] + 130)
    for _ in range(4):
        case = await env.step(case)
    assert "unmute" not in [w[0] for w in env.adapter.writes]


async def test_manual_unmute_does_not_trigger_same_round_remute(env):
    case = await muted_case(env)
    env.adapter.people[U] = replace(env.adapter.people[U], muted_until=0)
    await env.notice("group_ban", sub_type="lift_ban", duration=0, operator_id="300001")
    await env.speak()
    await env.step(case)
    assert [w[0] for w in env.adapter.writes].count("ban") == 1


async def test_poll_finds_card_without_notice_while_muted(env):
    case = await muted_case(env)
    env.adapter.people[U] = replace(env.adapter.people[U], card="大三-某某大学")
    case = await env.step(case)
    assert case["phase"] == "settle"
    case = await env.step(case)
    assert case["mute_state"] == "released"


async def test_disable_new_work_keeps_existing_relief(env):
    case = await muted_case(env)
    env.box.settings = replace(env.box.settings, enabled=False)
    await comply(env)
    for _ in range(3):
        case = await env.step(case)
    assert [w[0] for w in env.adapter.writes][-2:] == ["unmute", "recall"]


async def test_recall_checks_original_message_identity(env):
    case = await env.step(await env.speak())
    env.adapter.messages[case["message_id"]]["sender"]["user_id"] = "999999"
    await comply(env)
    case = await env.step(await env.step(case))
    assert case["recall_state"] == "manual"
    assert [w[0] for w in env.adapter.writes] == ["notify"]


async def test_ban_quota_skips_current_round_instead_of_delayed_ban(env):
    policy(env, stages=(Stage(1),), daily_mutes=1, first_grace_minutes=1, repeat_minutes=1)
    case = await env.step(await env.step(await env.speak()))
    env.clock.now = case["next_round"] + 2
    case = await env.step(await env.speak())
    case = await env.step(case)
    assert case["phase"] == "watch" and "上限" in case["reason"]
    assert [w[0] for w in env.adapter.writes].count("ban") == 1


async def test_readonly_observation_and_newcomer_policy(env):
    policy(env, mode="仅观察")
    assert await env.speak() is None
    assert not env.adapter.writes
    policy(env, stages=(Stage(20),))
    env.adapter.people[U] = replace(env.adapter.people[U], joined=int(env.clock()) - 5)
    case = await env.speak()
    assert case["minutes"] == 0


async def test_counters_reset_after_seven_days_without_new_round(env):
    case = await env.step(await env.speak())
    await comply(env)
    case = await env.step(await env.step(case))
    assert case["phase"] == "closed"
    env.clock.now += 8 * 86400
    env.adapter.people[U] = replace(env.adapter.people[U], card="bad")
    assert (await env.speak())["round"] == 1


async def test_stale_messages_are_not_punished_and_body_is_not_stored(env):
    assert await env.speak(age=301) is None
    await env.store.call("maintain")
    for path in env.path.glob("state.sqlite3*"):
        assert b"SHOULD_NOT_PERSIST" not in path.read_bytes()


async def test_paused_group_and_all_mute_cannot_send(env):
    case = await env.speak()
    await env.service.pause(A, G, "300001")
    with pytest.raises(Later):
        await env.step(case)
    env.service.paused.clear()
    await env.store.call("set", f"pause:{A}:{G}:", "")
    env.adapter.all_ban = True
    with pytest.raises(Later, match="全员禁言"):
        await env.step(case)
    assert not env.adapter.writes


async def test_shared_queue_checks_again_after_wait(env):
    case = await env.speak()

    class Guard:
        class deferred_error(Exception):
            pass

        async def run(self, **kwargs):
            assert kwargs["account"] == env.adapter.pid
            assert kwargs["gap"] == env.box.settings.pace.interval("gap")
            await comply(env)
            return await kwargs["action"]()

    env.router.guard = Guard()
    await env.step(case)
    assert not env.adapter.writes


async def test_health_failure_fails_closed(env):
    case = await env.speak()
    env.journal.failure = GuardError("日志不可写")
    with pytest.raises(GuardError, match="日志"):
        await Executor(env.service).process(case, env.adapter)
    assert not env.adapter.writes
