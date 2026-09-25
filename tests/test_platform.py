import asyncio
from types import SimpleNamespace

import pytest
from conftest import A, G, U

from qq_card_guard.config import GuardError, Later
from qq_card_guard.platform import Adapter, Router
from qq_card_guard.rules import message_fingerprint


class Bot:
    def __init__(self):
        self._wsr_api_clients = {A: object()}
        self.calls = []
        self.result = {"user_id": A}
        self.failure = None

    async def call_action(self, **kwargs):
        self.calls.append(kwargs)
        if self.failure:
            raise self.failure
        return self.result


def adapter(env, bot):
    return Adapter(
        "platform",
        bot,
        env.store,
        lambda: env.box.settings.pace,
        env.journal,
        clock=env.clock,
        sleep=env.clock.sleep,
    )


async def test_explicit_self_id_and_segment_messages(env):
    bot = Bot()
    api = adapter(env, bot)
    assert await api.identity() == A
    bot.result = {"message_id": 1234}
    message = [{"type": "text", "data": {"text": "[CQ:at,qq=all] is literal text"}}]
    assert await api.notify(G, message, lambda: None) == "1234"
    assert bot.calls[-1]["self_id"] == A
    assert bot.calls[-1]["message"] == message


async def test_transport_task_calls_final_fence_after_queued_notice(env):
    bot = Bot()
    api = adapter(env, bot)
    await api.identity()
    changed = False

    def notice():
        nonlocal changed
        changed = True

    def fence():
        if changed:
            raise Later("资料变更", env.clock() + 5)

    asyncio.get_running_loop().call_soon(notice)
    with pytest.raises(Later):
        await api.ban(G, U, 60, fence)
    assert len(bot.calls) == 1


async def test_connection_generation_changes_and_blocks_wrong_account(env):
    bot = Bot()
    api = adapter(env, bot)
    await api.identity()
    original = api.stamp()
    bot._wsr_api_clients[A] = object()
    assert api.stamp() != original
    assert api.recovery_until > env.clock()
    bot._wsr_api_clients = {"999999": object()}
    with pytest.raises(GuardError, match="账号改变"):
        api.stamp()


async def test_missing_or_multiple_connections_fail_closed(env):
    bot = Bot()
    api = adapter(env, bot)
    bot._wsr_api_clients = {}
    with pytest.raises(Later, match="未连接"):
        await api.identity()
    bot._wsr_api_clients = {A: object(), "999999": object()}
    with pytest.raises(GuardError, match="多个"):
        await api.identity()


async def test_api_error_cools_connection_and_preserves_redacted_cause(env):
    bot = Bot()
    api = adapter(env, bot)
    bot.failure = RuntimeError("10054 token=private-token")
    with pytest.raises(GuardError) as error:
        await api.identity()
    assert error.value.__cause__ is bot.failure
    assert "private-token" not in str(error.value)
    assert await env.store.call("get", "connection:platform", 0) > env.clock()


@pytest.mark.parametrize("wrong", ["group", "sender", "time", "contents", "id"])
async def test_recall_rejects_reused_id_or_wrong_message(env, wrong):
    bot = Bot()
    api = adapter(env, bot)
    await api.identity()
    message = [{"type": "at", "data": {"qq": U}}, {"type": "text", "data": {"text": "改名"}}]
    case = dict(gid=G, message_id="123", sent_at=env.clock(), message_hash=message_fingerprint(message))
    bot.result = dict(
        group_id=G, sender={"user_id": A}, message_id=123, time=int(env.clock()), message=message
    )
    field = {
        "group": "group_id",
        "sender": "sender",
        "time": "time",
        "contents": "message",
        "id": "message_id",
    }[wrong]
    bot.result[field] = {"sender": {"user_id": U}, "contents": [], "time": 1}.get(wrong, "999999")
    with pytest.raises(GuardError, match="身份或内容"):
        await api.locate_message(case)
    assert all(c["action"] != "delete_msg" for c in bot.calls)


async def test_router_refuses_duplicate_qq_connections(env):
    bots = [Bot(), Bot()]
    platforms = [
        SimpleNamespace(bot=b, meta=lambda i=i: SimpleNamespace(id=str(i), name="aiocqhttp"))
        for i, b in enumerate(bots)
    ]
    context = SimpleNamespace(platform_manager=SimpleNamespace(get_insts=lambda: platforms))
    router = Router(context, env.store, lambda: env.box.settings.pace, env.journal)
    with pytest.raises(GuardError, match="同一QQ"):
        await router.resolve(env.policy)


async def test_group_all_mute_unknown_is_not_assumed_false(env):
    bot = Bot()
    api = adapter(env, bot)
    await api.identity()
    bot.result = {"group_id": G}
    with pytest.raises(GuardError, match="全员禁言状态未知"):
        await api.all_muted(G)
    bot.result["group_all_shut"] = 0
    assert await api.all_muted(G) is False


async def test_member_identity_is_verified(env):
    bot = Bot()
    api = adapter(env, bot)
    await api.identity()
    bot.result = {"user_id": U, "group_id": "999999"}
    with pytest.raises(GuardError, match="身份不一致"):
        await api.member(G, U)
