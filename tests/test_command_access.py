from types import SimpleNamespace

import pytest

from qq_card_guard.command_access import CommandAccess
from qq_card_guard.config import CommandPermissionError


@pytest.mark.asyncio
async def test_access_checks_are_bounded_and_denials_do_not_grant_cached_permission():
    now, calls = [100], []
    permitted = [False]

    async def authorize(*args):
        calls.append(args)
        if not permitted[0]:
            raise CommandPermissionError("denied")

    service = SimpleNamespace(
        clock=lambda: now[0],
        settings=lambda: SimpleNamespace(group=lambda gid: SimpleNamespace(group_id=gid)),
        authorize=authorize,
    )
    access = CommandAccess()
    arguments = dict(
        text="名片规范 状态 100002", actor="100003", platform="onebot", account="100001", bot_admin=False
    )
    assert not await access.allowed(service, **arguments)
    assert not await access.allowed(service, **{**arguments, "text": "名片规范 状态 100004"})
    assert len(calls) == 1
    now[0] += 31
    permitted[0] = True
    assert await access.allowed(service, **arguments)
    now[0] += 6
    permitted[0] = False
    assert not await access.allowed(service, **arguments)
    assert len(calls) == 3


@pytest.mark.asyncio
async def test_unscoped_help_never_enumerates_groups():
    async def forbidden(*args):
        pytest.fail("unscoped help must not call QQ")

    service = SimpleNamespace(authorize=forbidden)
    access = CommandAccess()
    arguments = dict(text="名片规范", actor="100003", platform="onebot", account="100001")
    assert not await access.allowed(service, **arguments, bot_admin=False)
    assert await access.allowed(service, **arguments, bot_admin=True)


@pytest.mark.asyncio
async def test_transport_failure_does_not_reply_to_unknown_callers():
    async def failed(*args):
        raise RuntimeError("offline")

    service = SimpleNamespace(
        clock=lambda: 100,
        settings=lambda: SimpleNamespace(group=lambda gid: gid),
        authorize=failed,
    )
    arguments = dict(text="名片规范 状态 100002", actor="100003", platform="onebot", account="100001")
    assert not await CommandAccess().allowed(service, **arguments, bot_admin=False)
    assert await CommandAccess().allowed(service, **arguments, bot_admin=True)
