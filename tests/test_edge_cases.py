import asyncio
from dataclasses import replace

import pytest
from conftest import ADMIN, A, G, U

from qq_card_guard.commands import Commands
from qq_card_guard.config import GuardError, Later, Stage, parse_settings
from qq_card_guard.executor import Executor


async def prepare_mute(env):
    env.box.settings = replace(env.box.settings, groups=(replace(env.policy, stages=(Stage(10),)),))
    return await env.step(await env.step(await env.speak()))


async def compliant(env):
    env.adapter.people[U] = replace(env.adapter.people[U], card="大三-某某大学")
    await env.notice("group_card", card_new="大三-某某大学")


async def test_new_invalid_card_notice_overrules_stale_compliant_cache(env):
    case = await prepare_mute(env)
    await compliant(env)
    case = await env.step(case)
    assert case["phase"] == "settle"
    await env.notice("group_card", card_new="又改回不合规了")
    with pytest.raises(Later, match="名片.*缓存|缓存.*名片"):
        await env.step(case)
    assert "unmute" not in [w[0] for w in env.adapter.writes]


async def test_two_different_mute_snapshots_cannot_prove_ownership(env):
    case = await prepare_mute(env)
    await compliant(env)
    case = await env.step(case)
    count = 0

    async def change_deadline(uid):
        nonlocal count
        if uid == U:
            count += 1
            if count == 2:
                env.adapter.people[U] = replace(env.adapter.people[U], muted_until=int(env.clock()) + 30)

    env.adapter.read_hook = change_deadline
    with pytest.raises(Later, match="资料|禁言"):
        await env.step(case)
    assert "unmute" not in [w[0] for w in env.adapter.writes]


@pytest.mark.parametrize("duration", [0, 1800])
async def test_admin_intervention_between_reminder_and_ban_cancels_ban(env, duration):
    env.box.settings = replace(env.box.settings, groups=(replace(env.policy, stages=(Stage(10),)),))
    case = await env.step(await env.speak())
    await env.notice(
        "group_ban", sub_type="ban" if duration else "lift_ban", duration=duration, operator_id=ADMIN
    )
    # Keep member-info at its old cached unmuted value.
    case = await env.step(case)
    assert case["phase"] == "watch"
    assert [w[0] for w in env.adapter.writes] == ["notify"]


async def test_second_same_account_ban_inside_time_tolerance_is_external(env):
    case = await prepare_mute(env)
    env.clock.now += 5
    await env.notice("group_ban", sub_type="ban", duration=600, operator_id=A)
    env.adapter.people[U] = replace(env.adapter.people[U], muted_until=int(env.clock()) + 600)
    await compliant(env)
    for _ in range(3):
        case = await env.step(case)
    assert "unmute" not in [w[0] for w in env.adapter.writes]


async def test_configured_long_delays_are_not_mistaken_for_stale_tasks(env):
    env.box.settings = replace(
        env.box.settings,
        groups=(replace(env.policy, stages=(Stage(10),)),),
        pace=replace(env.box.settings.pace, notify_min=600, notify_max=600, ban_min=600, ban_max=600),
    )
    case = await env.step(await env.speak())
    assert case["phase"] == "ban"
    case = await env.step(case)
    assert [w[0] for w in env.adapter.writes] == ["notify", "ban"]


async def test_missed_whole_group_unmute_notice_does_not_pause_forever(env):
    await env.notice("group_ban", uid="0", sub_type="ban", duration=-1, operator_id=ADMIN)
    env.adapter.all_ban = False
    case = await env.step(await env.speak())
    assert case["message_id"]


async def test_missed_rejoin_notice_can_be_reconciled_by_new_join_time(env):
    await env.notice("group_decrease", sub_type="leave")
    env.clock.now += 10
    env.adapter.people[U] = replace(env.adapter.people[U], joined=int(env.clock()))
    case = await env.speak()
    assert case and case["joined"] == int(env.clock())


async def test_pending_limit_is_atomic_for_concurrent_members(env):
    env.box.settings = replace(env.box.settings, pace=replace(env.box.settings.pace, max_pending=1))
    await asyncio.gather(env.speak(U), env.speak("200002"), return_exceptions=True)
    assert await env.store.call("pending_count", A) == 1


async def test_old_reminder_cleanup_waits_for_current_mute_relief(env):
    first = await env.step(await env.speak())
    env.clock.now = first["next_round"] + 1
    current = await env.step(await env.step(await env.speak()))
    await compliant(env)
    await env.step(current)
    # Put an earlier reminder first in the due queue on purpose.
    first = await env.store.call("case", first["id"])
    await env.store.call("patch_case", current["id"], {"due": env.clock() + 20})
    with pytest.raises(Later, match="解禁"):
        await Executor(env.service).process(first, env.adapter)
    assert "recall" not in [w[0] for w in env.adapter.writes]


async def test_chat_burst_cannot_shorten_saved_backoff(env):
    await env.speak()
    until = env.clock() + 3600
    await env.store.call("defer_evaluation", A, G, U, until)
    await env.service.observe(
        dict(
            post_type="message",
            message_type="group",
            self_id=A,
            group_id=G,
            user_id=U,
            time=int(env.clock()),
            message_id=99999,
            sender={"card": "未改名"},
        ),
        "platform",
    )
    assert (await env.store.call("subject", A, G, U))["eval_at"] == until


async def test_cancel_during_intent_commit_does_not_leave_unknown_effect(env):
    case = await env.speak()
    call = env.store.call

    async def cancel_after_reserve(name, *args):
        result = await call(name, *args)
        if name == "reserve":
            raise asyncio.CancelledError
        return result

    env.store.call = cancel_after_reserve
    with pytest.raises(asyncio.CancelledError):
        await env.step(case)
    assert not await call("has_operation", case["id"], "notify")
    assert not env.adapter.writes


async def test_cancel_after_result_commit_keeps_successful_result(env):
    case = await env.speak()
    call = env.store.call

    async def cancel_after_finish(name, *args):
        result = await call(name, *args)
        if name == "finish":
            raise asyncio.CancelledError
        return result

    env.store.call = cancel_after_finish
    with pytest.raises(asyncio.CancelledError):
        await env.step(case)
    assert (await call("has_operation", case["id"], "notify"))["status"] == "confirmed"
    assert (await call("case", case["id"]))["message_id"]
    assert not await call("get", "block:" + A)


async def test_shared_queue_wait_does_not_hold_local_account_lock(env):
    case = await env.speak()

    class Guard:
        scheduling_version = 3

        class deferred_error(Exception):
            pass

        async def run(self, **kwargs):
            assert kwargs["priority"] == 2
            assert kwargs["group"] == G
            assert not env.service.account_locks[A].locked()
            return await kwargs["action"]()

    env.router.guard = Guard()
    await env.step(case)
    assert [w[0] for w in env.adapter.writes] == ["notify"]


async def test_command_rechecks_permission_after_waiting_for_member_lock(env):
    lock = env.service.member_lock(A, G, U)
    await lock.acquire()
    checked = asyncio.Event()
    authorize = env.service.authorize

    async def observe_authorization(*args):
        result = await authorize(*args)
        checked.set()
        return result

    env.service.authorize = observe_authorization
    task = asyncio.create_task(Commands(env.service).run(f"/名片规范 豁免 {G} {U}", ADMIN, "platform", A))
    await asyncio.wait_for(checked.wait(), 2)
    env.adapter.people[ADMIN] = replace(env.adapter.people[ADMIN], role="member")
    await env.notice("group_admin", uid=ADMIN, sub_type="unset")
    lock.release()
    with pytest.raises(GuardError, match="只有"):
        await task
    assert not await env.store.call("exempt", A, G, U, env.clock())


@pytest.mark.parametrize(
    "raw",
    [
        {"reminder": " "},
        {"format_help": " "},
        {"reminder": "{格式说明}" * 100, "format_help": "很长" * 100},
        {"pattern": "(" * 500 + "a" + ")" * 500},
        {"protect_level": "1" * 5000},
    ],
)
def test_bad_group_config_is_isolated_instead_of_crashing_every_group(raw):
    settings = parse_settings({"groups": [{"group_id": G, **raw}, {"group_id": "300002"}]})
    assert settings.errors and settings.group("300002")


async def test_malformed_notice_cannot_stop_event_ingestion(env):
    await env.service.observe(
        dict(
            post_type="message",
            message_type="group",
            self_id=A,
            group_id=G,
            user_id=U,
            time=int(env.clock()),
            duration="invalid",
            sender={"card": "未改名"},
        ),
        "platform",
    )
    await env.service.observe(
        dict(
            post_type="notice",
            notice_type="group_ban",
            self_id=A,
            group_id=G,
            user_id=U,
            time=int(env.clock()),
            duration="9" * 5000,
            sub_type="ban",
            operator_id=ADMIN,
        ),
        "platform",
    )
    assert not env.service.failure
    assert not (await env.store.call("subject", A, G, U))["ban"]


async def test_old_case_update_does_not_reorder_history(env):
    first = await env.step(await env.speak())
    env.clock.now = first["next_round"] + 1
    second = await env.speak()
    await env.store.call("patch_case", first["id"], {"reason": "an old case changed"})
    assert (await env.store.call("list_cases", A, G))[0]["id"] == second["id"]


async def test_account_is_reconfirmed_before_each_effect(env):
    case = await env.speak()

    async def switched_identity(force=False, priority=0):
        return "900001" if force else A

    env.adapter.identity = switched_identity
    with pytest.raises(GuardError, match="身份改变"):
        await env.step(case)
    assert not env.adapter.writes


async def test_missing_promotion_notice_does_not_deny_manager_forever(env):
    await env.notice("group_admin", uid=A, sub_type="unset")
    assert not await env.service.manager(env.adapter, G, A)
    env.clock.now += 301
    assert await env.service.manager(env.adapter, G, A)
    assert len([read for read in env.adapter.reads if read[1] == A]) == 3


async def test_stale_admin_hint_is_reconciled_with_two_member_responses(env):
    await env.notice("group_admin", sub_type="set")
    _, _, verdict = await env.service.inspect(env.policy, env.adapter, U)
    assert verdict.state == "exempt"
    env.clock.now += 301
    _, _, verdict = await env.service.inspect(env.policy, env.adapter, U)
    assert verdict.state == "invalid"


@pytest.mark.parametrize("moment", ["queue", "transport"])
async def test_removing_group_config_during_wait_cancels_pending_write(env, moment):
    case = await env.speak()

    async def remove_config(*_args):
        env.box.settings = replace(env.box.settings, groups=())

    if moment == "transport":
        env.adapter.before_hook = remove_config
        with pytest.raises(Later, match="群配置"):
            await env.step(case)
        assert not await env.store.call("has_operation", case["id"], "notify")
    else:

        class Guard:
            class deferred_error(Exception):
                pass

            async def run(self, **kwargs):
                await remove_config()
                return await kwargs["action"]()

        env.router.guard = Guard()
        case = await env.step(case)
        assert case["phase"] == "closed"
    assert not env.adapter.writes


async def test_slow_revalidation_cannot_use_old_permission_snapshot(env):
    case = await env.speak()

    async def slow(uid):
        env.clock.now += 12

    env.adapter.read_hook = slow
    with pytest.raises(Later, match="提交前"):
        await env.step(case)
    assert not env.adapter.writes


async def test_delayed_ban_from_previous_membership_cannot_affect_rejoined_member(env):
    await env.speak()
    old_time = int(env.clock())
    await env.notice("group_decrease", sub_type="leave")
    env.clock.now += 10
    await env.notice("group_increase", sub_type="approve")
    env.adapter.people[U] = replace(env.adapter.people[U], joined=int(env.clock()))
    await env.service.inspect(env.policy, env.adapter, U)
    await env.service.observe(
        dict(
            post_type="notice",
            notice_type="group_ban",
            self_id=A,
            group_id=G,
            user_id=U,
            time=old_time,
            duration=86400,
            sub_type="ban",
            operator_id=ADMIN,
        ),
        "platform",
    )
    assert not (await env.store.call("subject", A, G, U))["ban"]
