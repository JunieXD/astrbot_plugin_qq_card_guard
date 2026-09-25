import asyncio
import json

import pytest
from aiocqhttp.exceptions import ActionFailed
from conftest import A
from test_platform import Bot, adapter
from test_workflow import comply, muted_case

from qq_card_guard.config import ApiFailure, GuardError, Later
from qq_card_guard.executor import Executor
from qq_card_guard.resources import Journal


@pytest.mark.parametrize("wrapped", [True, False])
@pytest.mark.parametrize("message", ["消息不存在", "权限不足", "Timeout: QQ request timed out"])
async def test_business_response_does_not_invalidate_connection_or_extend_cooldown(env, wrapped, message):
    bot = Bot()
    api = adapter(env, bot)
    await api.identity()
    stamp, identity_at = api.stamp(), api.identity_at
    # A business failure must neither extend nor clear an existing recovery period.
    until = api.recovery_until = env.clock() + 15
    result = dict(status="failed", retcode=1200, message=message)
    if wrapped:
        bot.failure = ActionFailed(result)
    else:
        bot.result = result
    with pytest.raises(ApiFailure) as caught:
        await api.call("get_msg", message_id="123")
    assert caught.value.code == ("message_not_found" if message == "消息不存在" else "api_business_failure")
    assert caught.value.retcode == 1200
    assert api.stamp() == stamp and api.identity_at == identity_at
    assert api.recovery_until == until
    assert await env.store.call("get", "connection:platform") == until
    assert len(bot.calls) == 2


@pytest.mark.parametrize(
    "error", [TimeoutError(), ConnectionResetError(10054, "reset"), RuntimeError("unknown")]
)
async def test_transport_or_unclassified_failure_still_invalidates_connection(env, error):
    bot = Bot()
    api = adapter(env, bot)
    await api.identity()
    stamp = api.stamp()
    bot.failure = error
    with pytest.raises(GuardError) as caught:
        await api.call("get_msg", message_id="123")
    assert not isinstance(caught.value, ApiFailure)
    assert caught.value.__cause__ is error
    assert api.stamp() != stamp and api.identity_at == 0
    assert await env.store.call("get", "connection:platform") > env.clock()


async def test_cancellation_does_not_claim_business_failure_or_cool_connection(env):
    bot = Bot()
    api = adapter(env, bot)
    stamp = api.stamp()
    bot.failure = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await api.call("get_msg", message_id="123")
    assert api.stamp() == stamp and not api.recovery_until
    assert api.lock_owner is None


async def test_missing_old_message_allows_unmute_and_other_member_reminder(env):
    case = await muted_case(env)
    await comply(env)
    case = await env.step(await env.step(case))
    assert case["mute_state"] == "released"
    bot = Bot()
    api = adapter(env, bot)
    await api.identity()
    stamp = api.stamp()
    bot.failure = ActionFailed(dict(status="failed", retcode=1200, wording="消息不存在"))
    env.adapter.call = api.call
    case = await env.step(case)
    assert case["phase"] == "closed" and case["recall_state"] == "manual"
    assert "查询不到" in case["reason"]
    await env.step(case)
    assert [c["action"] for c in bot.calls].count("get_msg") == 1
    assert api.stamp() == stamp and not api.recovery_until
    assert not await env.store.call("get", "block:" + A, {})
    other = await env.step(await env.speak("200002"))
    assert other["phase"] == "ban" and other["message_id"]
    assert [w[0] for w in env.adapter.writes] == ["notify", "ban", "unmute", "notify"]


async def test_failed_write_response_is_not_retried_or_assumed_unsent(env):
    case = await env.speak()

    async def fail(kind):
        raise ApiFailure("send_group_msg", dict(retcode=1200, message="Timeout: QQ internal timeout"))

    env.adapter.after_hook = fail
    case = await env.step(case)
    assert case["phase"] == "review"
    assert await env.store.call("get", "block:" + A, {})
    await env.step(case)
    assert [w[0] for w in env.adapter.writes] == ["notify"]


@pytest.mark.parametrize(
    "code,text,level",
    [
        ("operation_gap", "等待操作间隔", "INFO"),
        ("startup_wait", "等待插件启动缓冲", "INFO"),
        ("manual_recovery", "等待管理员恢复后的冷却", "INFO"),
        ("connection_recovery", "等待连接恢复冷却", "WARNING"),
    ],
)
async def test_gate_preserves_deadline_and_logs_specific_wait(env, code, text, level):
    case = await env.speak()
    until = env.clock() + 30
    if code == "connection_recovery":
        env.adapter.recovery_until = until
    else:
        key = {"operation_gap": "gap:" + A, "startup_wait": "startup", "manual_recovery": "recovery:" + A}[
            code
        ]
        await env.store.call("set", key, until)
    with pytest.raises(Later) as caught:
        await Executor(env.service).gate(case, env.adapter, True)
    exc = caught.value
    assert exc.code == code and text in str(exc) and exc.until == until
    assert exc.details["waits"] == {code: until}
    journal = Journal(env.path)
    try:
        journal.record("处理暂缓", exception=exc)
    finally:
        journal.close()
    record = json.loads((env.path / "logs/guard.log").read_text(encoding="utf-8"))
    assert record["level"] == level
    assert record["code"] == code and record["retry_at"] == until
    assert ("exception" in record) == (level == "WARNING")
    env.clock.now = until
    await Executor(env.service).gate(case, env.adapter, True)


async def test_overlapping_waits_keep_all_causes_and_do_not_hide_connection_warning(env):
    case = await env.speak()
    env.adapter.recovery_until = env.clock() + 10
    await env.store.call("set", "gap:" + A, env.clock() + 20)
    with pytest.raises(Later) as caught:
        await Executor(env.service).gate(case, env.adapter, True)
    assert caught.value.code == "operation_gap"
    assert not caught.value.routine
    assert caught.value.until == env.clock() + 20
    assert set(caught.value.details["waits"]) == {"connection_recovery", "operation_gap"}


def test_business_error_log_retains_classification_and_redacts_cause(tmp_path):
    journal = Journal(tmp_path)
    try:
        try:
            raise ActionFailed(dict(retcode=1200, message="消息不存在", token="private-value"))
        except ActionFailed as cause:
            try:
                raise ApiFailure("get_msg", cause.result) from cause
            except ApiFailure as exc:
                journal.record("平台接口失败", exception=exc)
        journal.record("处理暂缓", exception=Later("读取额度耗尽", 10, code="read_budget"))
    finally:
        journal.close()
    text = (tmp_path / "logs/guard.log").read_text(encoding="utf-8")
    record, budget = map(json.loads, text.splitlines())
    assert record["level"] == "WARNING" and record["failure_kind"] == "business"
    assert record["code"] == "message_not_found" and record["retcode"] == 1200
    assert record["exception"][1]["type"] == "ActionFailed"
    assert "private-value" not in text
    assert budget["level"] == "WARNING" and budget["exception"]
