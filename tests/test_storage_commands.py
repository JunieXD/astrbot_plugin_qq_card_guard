import asyncio
import json
import threading
from dataclasses import replace

import pytest
from conftest import ADMIN, A, G, U

from qq_card_guard.commands import Commands
from qq_card_guard.config import GuardError, Later
from qq_card_guard.resources import InstanceLock, Journal
from qq_card_guard.store import Store


async def test_submitted_intent_survives_restart_as_unknown(env):
    case = await env.speak()
    await env.store.call("reserve", case, "notify", env.clock(), {}, env.policy, env.box.settings.pace)
    await env.store.close()
    store = Store(env.path / "state.sqlite3")
    try:
        await store.call("open_db")
        assert (await store.call("case", case["id"]))["phase"] == "review"
        assert (await store.call("has_operation", case["id"], "notify"))["status"] == "unknown"
        assert await store.call("get", "block:" + A)
        assert not (await store.call("due", env.clock() + 1000))[1]
        assert (await store.call("subject", A, G, U))["eval_at"] == 0
    finally:
        await store.close()


async def test_restarting_unsent_case_does_not_replay_punishment(env):
    case = await env.speak()
    await env.store.close()
    store = Store(env.path / "state.sqlite3")
    try:
        await store.call("open_db")
        assert (await store.call("case", case["id"]))["phase"] == "closed"
    finally:
        await store.close()


async def test_read_budget_reserves_relief_capacity(env):
    for _ in range(39):
        await env.store.call("reserve_read", A, env.clock(), 60, 0)
    with pytest.raises(Later):
        await env.store.call("reserve_read", A, env.clock(), 60, 0)
    for _ in range(12):
        await env.store.call("reserve_read", A, env.clock(), 60, 1)
    with pytest.raises(Later):
        await env.store.call("reserve_read", A, env.clock(), 60, 1)
    for _ in range(9):
        await env.store.call("reserve_read", A, env.clock(), 60, 2)
    with pytest.raises(Later):
        await env.store.call("reserve_read", A, env.clock(), 60, 2)
    await env.store.call("reserve_read", "900001", env.clock(), 60, 0)


async def test_quota_survives_store_reopen(env):
    case = await env.speak()
    pace = replace(env.box.settings.pace, account_hourly_reminders=1)
    await env.store.call("reserve", case, "notify", env.clock(), {}, env.policy, pace)
    await env.store.close()
    store = Store(env.path / "state.sqlite3")
    try:
        await store.call("open_db")
        different = {**case, "id": "new-case"}
        with pytest.raises(Later, match="额度"):
            await store.call("reserve", different, "notify", env.clock(), {}, env.policy, pace)
    finally:
        await store.close()


async def test_cancel_drains_database_worker_before_close(env):
    entered, release = threading.Event(), threading.Event()

    def slow():
        entered.set()
        release.wait(5)
        env.store.set("completed", True)

    env.store.slow = slow
    task = asyncio.create_task(env.store.call("slow"))
    await asyncio.to_thread(entered.wait, 5)
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert await env.store.call("get", "completed") is True


async def test_commands_require_current_group_role(env):
    command = Commands(env.service)
    with pytest.raises(GuardError, match="只有"):
        await command.run(f"/名片规范 暂停 {G}", U, "platform", A)
    assert not env.service.paused
    result = await command.run(f"/名片规范 暂停 {G}", ADMIN, "platform", A)
    assert "已暂停" in result
    await env.notice("group_admin", uid=ADMIN, sub_type="unset")
    env.clock.now += 11
    with pytest.raises(GuardError, match="只有"):
        await command.run(f"/名片规范 恢复 {G}", ADMIN, "platform", A)


async def test_test_command_preserves_spaces(env):
    result = await Commands(env.service).run(f"/名片规范 测试 {G} 大三-某某大学", ADMIN, "platform", A)
    assert result.startswith("符合")
    env.clock.now += 11
    result = await Commands(env.service).run(f"/名片规范 测试 {G} 大三-某某大学 额外", ADMIN, "platform", A)
    assert result.startswith("不符合")
    assert not env.adapter.writes


async def test_exemption_closes_existing_without_new_penalty(env):
    case = await env.step(await env.speak())
    result = await Commands(env.service).run(f"/名片规范 豁免 {G} {U} 2", ADMIN, "platform", A)
    assert "2天" in result
    assert await env.store.call("exempt", A, G, U, env.clock())
    case = await env.step(case)
    assert case["phase"] == "closed" and case["recall_state"] == "recalled"
    assert [w[0] for w in env.adapter.writes] == ["notify", "recall"]


async def test_handoff_then_resume_never_claims_effect_was_reversed(env):
    env.adapter.fail = "notify"
    case = await env.step(await env.speak())
    command = Commands(env.service)
    with pytest.raises(GuardError, match="仍有"):
        await command.run(f"/名片规范 恢复 {G}", ADMIN, "platform", A)
    env.clock.now += 11
    result = await command.run(f"/名片规范 接手 {G} {case['id']}", ADMIN, "platform", A)
    assert "人工" in result
    env.clock.now += 11
    assert "冷却" in await command.run(f"/名片规范 恢复 {G}", ADMIN, "platform", A)
    assert not await env.store.call("get", "block:" + A)
    assert [w[0] for w in env.adapter.writes] == ["notify"]


def test_instance_lock_can_reopen_after_release(tmp_path):
    lock = InstanceLock(tmp_path / "instance.lock")
    with pytest.raises(GuardError, match="实例"):
        InstanceLock(tmp_path / "instance.lock")
    lock.close()
    InstanceLock(tmp_path / "instance.lock").close()


def test_json_logging_redaction_full_exception_and_rotation(tmp_path):
    journal = Journal(tmp_path)
    journal.handler.maxBytes = 1500
    try:
        try:
            raise ValueError("token=SHOULD_BE_HIDDEN\nnot-a-forged-record")
        except ValueError as exc:
            journal.record("异常", exception=exc, card="line1\nline2", auth="Bearer ALSO_HIDDEN")
        for i in range(35):
            journal.record("轮转验证", index=i, detail="x" * 300)
        paths = list(journal.directory.glob("guard.log*"))
        assert 1 < len(paths) <= 8
        for path in paths:
            for line in path.read_text(encoding="utf-8").splitlines():
                assert json.loads(line)["event"]
                assert "SHOULD_BE_HIDDEN" not in line and "ALSO_HIDDEN" not in line
        journal.record("带堆栈异常", exception=RuntimeError("https://host/secret?token=abc"))
        last = json.loads((journal.directory / "guard.log").read_text(encoding="utf-8").splitlines()[-1])
        assert last["exception"][0]["type"] == "RuntimeError"
        assert "host/secret" not in str(last)
    finally:
        journal.close()


async def test_clock_jump_stops_writes(env):
    case = await env.speak()
    env.service.monotonic = lambda: env.clock() - 500
    with pytest.raises(GuardError, match="跳变"):
        await env.step(case)
    assert not env.adapter.writes
