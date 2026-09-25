import asyncio
import json
from dataclasses import replace

import pytest
from conftest import A, G, U
from test_platform import Bot, adapter

from qq_card_guard.config import GuardError, Later, Pace, Stage, parse_settings
from qq_card_guard.resources import Journal
from qq_card_guard.scheduling import followup_window
from qq_card_guard.store import Store, key


def compliant(env):
    env.adapter.people[U] = replace(env.adapter.people[U], card="大三-某某大学")


async def queue(env, message_id=999):
    await env.service.observe(
        dict(
            post_type="message",
            message_type="group",
            self_id=A,
            group_id=G,
            user_id=U,
            time=int(env.clock()),
            message_id=message_id,
            sender={"card": env.adapter.people[U].card},
        ),
        "platform",
    )
    return await env.store.call("subject", A, G, U)


async def test_cache_is_fixed_from_real_verification_not_extended_by_speech(env):
    compliant(env)
    await env.speak()
    start = env.clock()
    for elapsed in (10, 1000, 3599):
        env.clock.now = start + elapsed
        await env.speak()
    assert len(env.adapter.reads) == 1
    assert (await env.store.call("subject", A, G, U))["screening"]["at"] == start
    env.clock.now = start + 3600
    await env.speak()
    assert len(env.adapter.reads) == 2
    assert not env.adapter.writes


async def test_disabled_cache_queries_each_new_speech(env):
    compliant(env)
    env.box.settings = replace(env.box.settings, groups=(replace(env.policy, compliant_cache_minutes=0),))
    await env.speak()
    await env.speak()
    assert len(env.adapter.reads) == 2


async def test_changed_message_card_invalidates_cache_and_creates_case(env):
    compliant(env)
    await env.speak()
    env.adapter.people[U] = replace(env.adapter.people[U], card="改回不合格")
    assert (await env.speak())["phase"] == "notify"
    assert len(env.adapter.reads) == 2


async def test_changed_notice_does_not_trust_stale_api_or_old_cache(env):
    compliant(env)
    await env.speak()
    await env.notice("group_card", card_new="改回不合格")
    with pytest.raises(Later, match="缓存不一致"):
        await env.service.inspect(env.policy, env.adapter, U)
    assert not (await env.store.call("subject", A, G, U)).get("screening")


@pytest.mark.parametrize("change", ["policy", "connection", "membership", "role"])
async def test_cache_invalidates_on_identity_rule_or_membership_change(env, change):
    compliant(env)
    await env.speak()
    if change == "policy":
        env.box.settings = replace(env.box.settings, groups=(replace(env.policy, format_help="新说明"),))
    elif change == "connection":
        env.adapter.binding = "new-session:1"
    elif change == "membership":
        env.clock.now += 10
        await env.notice("group_increase")
        env.adapter.people[U] = replace(env.adapter.people[U], joined=int(env.clock()))
    else:
        await env.notice("group_admin", sub_type="set")
    await env.speak()
    assert len(env.adapter.reads) == 2


async def test_temporary_manual_exemption_expiry_limits_cache(env):
    await env.store.call("set_exempt", A, G, U, env.clock() + 30, A, env.clock())
    await env.speak()
    env.clock.now += 29
    await env.speak()
    assert len(env.adapter.reads) == 1
    env.clock.now += 2
    assert (await env.speak())["phase"] == "notify"
    assert len(env.adapter.reads) == 2


async def test_manual_exemption_removal_invalidates_cached_exempt(env):
    await env.store.call("set_exempt", A, G, U, 0, A, env.clock())
    await env.speak()
    await env.store.call("remove_exempt", A, G, U, A, env.clock())
    assert (await env.speak())["phase"] == "notify"


async def test_cached_skip_runs_before_identity_lookup_even_when_budget_full(env):
    compliant(env)
    await env.speak()
    subject = await queue(env)
    env.router.known_stamp = lambda *_: env.adapter.stamp()

    async def unavailable(*args, **kwargs):
        raise AssertionError("Cache hit must not query identity or consume budget")

    env.router.resolve = unavailable
    await env.service._work("subject", subject)
    assert len(env.adapter.reads) == 1
    assert not (await env.store.call("subject", A, G, U))["eval_at"]


async def test_expired_cache_logs_once_across_preflight_retry_and_reverification(env):
    compliant(env)
    await env.speak()
    env.clock.now += 3601
    subject = await queue(env)
    env.router.known_stamp = lambda *_: env.adapter.stamp()
    original = env.router.resolve

    async def unavailable(*args, **kwargs):
        raise Later("读取额度耗尽", env.clock() + 60, code="read_budget")

    env.router.resolve = unavailable
    await env.service._work("subject", subject)
    await env.service._work("subject", subject)
    assert not (await env.store.call("subject", A, G, U)).get("screening")
    env.router.resolve = original
    env.clock.now += 61
    await env.service._work("subject", subject)

    def invalidations():
        return [r for r in env.journal.records if r["kind"] == "免查缓存失效"]

    assert len(invalidations()) == 1
    assert len(env.adapter.reads) == 2
    # The next cache lifetime must still produce its own invalidation record.
    env.clock.now += 3601
    await env.service._work("subject", await queue(env, 1000))
    assert len(invalidations()) == 2
    assert len(env.adapter.reads) == 3


async def test_unresolved_connection_does_not_retire_cache_before_identity_resolution(env):
    compliant(env)
    await env.speak()
    subject = await queue(env)
    assert not await env.service.preflight(subject, env.policy, None)
    assert (await env.store.call("subject", A, G, U)).get("screening")
    assert await env.service.preflight(subject, env.policy, env.adapter.stamp())
    assert not [r for r in env.journal.records if r["kind"] == "免查缓存失效"]


async def test_stale_preflight_cannot_remove_newer_cache(env):
    compliant(env)
    await env.speak()
    env.clock.now += 3601
    stale = await queue(env)
    await env.service.inspect(env.policy, env.adapter, U)
    fresh = (await env.store.call("subject", A, G, U))["screening"]
    assert not await env.service.preflight(stale, env.policy, env.adapter.stamp())
    assert (await env.store.call("subject", A, G, U))["screening"] == fresh
    assert not [r for r in env.journal.records if r["kind"] == "免查缓存失效"]


async def test_card_notice_invalidation_not_logged_again_by_preflight(env):
    compliant(env)
    await env.speak()
    env.adapter.people[U] = replace(env.adapter.people[U], card="bad")
    await env.notice("group_card", card_new="bad")
    await env.service._work("subject", await queue(env))
    invalidations = [r for r in env.journal.records if r["kind"] == "免查缓存失效"]
    assert len(invalidations) == 1 and invalidations[0]["reason"] == "名片变化"
    assert (await env.store.call("member_cases", A, G, U))[0]["phase"] == "notify"


async def test_same_second_new_event_not_cleared_by_old_completion(env):
    compliant(env)
    old = await queue(env, 991)
    env.adapter.people[U] = replace(env.adapter.people[U], card="不符合")
    new = await queue(env, 992)
    assert old["speech_at"] == new["speech_at"]
    await env.store.call("evaluated", A, G, U, old["speech_at"], old["version"])
    assert (await env.store.call("subject", A, G, U))["eval_at"]


async def test_repeated_speech_in_grace_uses_followup_without_queries(env):
    case = await env.step(await env.speak())
    count = len(env.adapter.reads)
    for _ in range(5):
        env.clock.now += 2
        await env.speak()
    assert len(env.adapter.reads) == count
    assert len(await env.store.call("member_cases", A, G, U)) == 1
    assert (await env.store.call("case", case["id"]))["phase"] == "watch"


async def test_cache_never_hides_pending_recall(env):
    case = await env.step(await env.speak())
    compliant(env)
    await env.speak()
    assert (await env.store.call("case", case["id"]))["phase"] == "settle"
    await env.speak()
    case = await env.step(case)
    assert case["recall_state"] == "recalled"
    assert [w[0] for w in env.adapter.writes] == ["notify", "recall"]


async def test_due_followup_and_new_speech_share_one_inspection(env):
    case = await env.step(await env.speak())
    env.clock.now = case["next_round"] + 1
    await queue(env)
    env.clock.now += 2
    before = len(env.adapter.reads)
    await env.service._work("case", case)
    assert len(env.adapter.reads) == before + 1
    assert (await env.store.call("member_cases", A, G, U))[0]["round"] == 2


async def test_stable_inspection_never_reuses_coalesced_read(env):
    token = env.service.inspections.set({})
    try:
        await env.service.inspect(env.policy, env.adapter, U)
        await env.service.inspect(env.policy, env.adapter, U)
        assert len(env.adapter.reads) == 1
        await env.service.inspect(env.policy, env.adapter, U, stable=True)
        assert len(env.adapter.reads) == 3
    finally:
        env.service.inspections.reset(token)


@pytest.mark.parametrize(
    "age,bounds",
    [
        (0, (30, 60)),
        (179, (30, 60)),
        (180, (60, 120)),
        (600, (180, 300)),
        (1800, (600, 1200)),
        (7200, (1800, 3600)),
    ],
)
def test_followup_tiers_from_sent_time(age, bounds):
    case = dict(created=1, sent_at=1000, mute_state="none", mute_until=0)
    assert followup_window(case, Pace(), 1000 + age)[1:] == bounds


async def test_active_mute_uses_fast_tier_and_reserved_budget_priority(env):
    env.box.settings = replace(env.box.settings, groups=(replace(env.policy, stages=(Stage(10),)),))
    case = await env.step(await env.step(await env.speak()))
    assert case["mute_state"] == "unverified"
    assert 20 <= case["due"] - env.clock() <= 40
    before = len(env.adapter.reads)
    case = await env.step(case)
    assert env.adapter.reads[before][2] == 2
    assert 20 <= case["due"] - env.clock() <= 40


async def test_budget_resume_waits_for_enough_slots_and_exposes_reason(env):
    start = env.clock()
    for i in range(50):
        await env.store.call("reserve_read", A, start + i, 60, 2)
    with pytest.raises(Later) as caught:
        await env.store.call("reserve_read", A, start + 100, 60, 0)
    assert caught.value.code == "read_budget"
    assert caught.value.details["threshold"] == 39
    assert caught.value.until == start + 11 + 3601
    await env.store.call("reserve_read", A, caught.value.until, 60, 0)


async def test_changed_limit_reschedules_budget_waits(env):
    case = await env.step(await env.speak())
    await env.store.call("set", "read-limit", 60)
    await env.store.call("defer_evaluation", A, G, U, env.clock() + 3000, "额度", "read_budget")
    await env.store.call(
        "patch_case", case["id"], {"due": env.clock() + 3000, "read_defer_until": env.clock() + 3000}
    )
    assert await env.store.call("refresh_read_deferrals", env.clock(), 600) == 2
    assert (await env.store.call("subject", A, G, U))["eval_at"] == env.clock()
    assert (await env.store.call("case", case["id"]))["due"] == env.clock()


async def test_notices_do_not_defeat_budget_backoff(env):
    case = await env.step(await env.speak())
    later = env.clock() + 3000
    await env.store.call("patch_case", case["id"], {"due": later, "read_defer_until": later})
    await env.notice("group_card", card_new="大三-某某大学")
    assert (await env.store.call("case", case["id"]))["due"] == later


async def test_expired_deferred_speech_is_counted_without_api_queries(env):
    subject = await queue(env)
    env.clock.now += 301
    await env.service._work("subject", subject)
    assert not env.adapter.reads
    stats = await env.store.call("get", "stats:" + key(A, G))
    assert stats["counts"]["expired"] == 1


async def test_pending_limit_counts_distinct_members(env):
    case = await env.speak()

    def seed(row):
        with env.store.db:
            env.store._save_case(row)

    env.store.seed = seed
    await env.store.call("seed", {**case, "id": "another-case", "phase": "settle"})
    assert await env.store.call("pending_count", A) == 1
    await env.store.call("seed", {**case, "id": "another-member", "uid": "200002"})
    assert await env.store.call("pending_count", A) == 2


async def test_cache_survives_storage_but_not_new_connection_session(env):
    compliant(env)
    await env.speak()
    await env.store.close()
    store = Store(env.path / "state.sqlite3")
    try:
        await store.call("open_db")
        subject = await store.call("subject", A, G, U)
        from qq_card_guard.scheduling import cache_check

        assert cache_check(subject, env.policy, "different-process:0", env.clock())[0] is None
    finally:
        await store.close()


async def test_read_batch_prevents_interleaving_and_releases_on_cancel(env):
    bot = Bot()
    api = adapter(env, bot)
    entered, hold = asyncio.Event(), asyncio.Event()

    async def batch():
        async with api.preparation():
            await api.call("first")
            entered.set()
            await hold.wait()
            await api.call("second")

    task = asyncio.create_task(batch())
    await entered.wait()
    other = asyncio.create_task(api.call("other"))
    await asyncio.sleep(0)
    assert [c["action"] for c in bot.calls] == ["first"]
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.wait_for(other, 2)
    assert [c["action"] for c in bot.calls] == ["first", "other"]
    assert api.lock_owner is None


def test_invalid_followup_steps_rejected():
    for rows in (
        [],
        [{"after_minutes": 3, "minimum": 90, "maximum": 30}],
        [
            {"after_minutes": 3, "minimum": 60, "maximum": 90},
            {"after_minutes": 2, "minimum": 60, "maximum": 90},
        ],
    ):
        with pytest.raises(GuardError):
            parse_settings({"pace": {"followup_steps": rows}})


def test_member_level_cannot_overwrite_log_severity(tmp_path):
    log = Journal(tmp_path)
    try:
        log.record("名片判断", screening=True, level=70)
    finally:
        log.close()
    record = json.loads((tmp_path / "logs/decisions.log").read_text(encoding="utf-8"))
    assert record["level"] == "INFO" and record["member_level"] == 70
