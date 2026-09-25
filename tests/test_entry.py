"""Exercise the plugin boundary without importing a running AstrBot singleton."""

import asyncio
import importlib.util
import logging
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest


@pytest.fixture
def entry(monkeypatch, tmp_path):
    handlers = {}

    def decorator(kind):
        def factory(*args, **kwargs):
            def apply(fn):
                handlers.setdefault(fn.__name__, []).append((kind, args, kwargs))
                return fn

            return apply

        return factory

    filters = SimpleNamespace(
        event_message_type=decorator("event"),
        platform_adapter_type=decorator("platform"),
        command=decorator("command"),
        EventMessageType=SimpleNamespace(ALL="all", PRIVATE_MESSAGE="private"),
        PlatformAdapterType=SimpleNamespace(AIOCQHTTP="aiocqhttp"),
    )

    class Star:
        def __init__(self, context, config):
            self.context = context

    def data_dir(name):
        path = tmp_path / name
        path.mkdir(exist_ok=True)
        return path

    for name in ("astrbot", "astrbot.api", "astrbot.api.event", "astrbot.api.star", "card_fixture"):
        monkeypatch.setitem(sys.modules, name, ModuleType(name))
    sys.modules["astrbot.api"].logger = logging.getLogger("entry-test")
    sys.modules["astrbot.api.event"].filter = filters
    star_module = sys.modules["astrbot.api.star"]
    star_module.Context = object
    star_module.Star = Star
    star_module.StarTools = SimpleNamespace(get_data_dir=data_dir)
    star_module.register = decorator("register")
    sys.modules["card_fixture"].__path__ = [str(Path.cwd())]
    spec = importlib.util.spec_from_file_location("card_fixture.main", Path("main.py"))
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)
    yield module.QQCardGuard, handlers
    for name in list(sys.modules):
        if name.startswith("card_fixture.") and name != spec.name:
            sys.modules.pop(name, None)


async def test_real_entry_initialization_termination_and_decorator_order(entry):
    plugin_cls, handlers = entry
    # AstrBot takes priority from the first (innermost) registration decorator.
    assert handlers["observe_group"][0] == ("event", ("all",), {"priority": 100})
    assert any(kind == "event" and args == ("private",) for kind, args, _ in handlers["card_command"])
    plugin = plugin_cls(SimpleNamespace(platform_manager=SimpleNamespace(get_insts=lambda: [])), {})
    await plugin.initialize()
    assert plugin.service is not None and plugin.start_error == ""
    task = plugin.service.task
    await plugin.terminate()
    assert task.done() and plugin.service is None
    await plugin.terminate()


async def test_invalid_configuration_has_useful_error_and_no_background_tasks(entry):
    plugin_cls, _ = entry
    plugin = plugin_cls(SimpleNamespace(), {"pace": {"gap_min": 0}})
    await plugin.initialize()
    assert plugin.service is None
    assert "操作间隔" in plugin.start_error


async def test_private_help_responds_without_network_or_llm(entry):
    plugin_cls, _ = entry
    plugin = plugin_cls(SimpleNamespace(platform_manager=SimpleNamespace(get_insts=lambda: [])), {})
    await plugin.initialize()
    stopped = []
    event = SimpleNamespace(
        stop_event=lambda: stopped.append(True),
        plain_result=lambda text: text,
        message_obj=SimpleNamespace(raw_message={"self_id": "100001"}),
        get_message_str=lambda: "名片规范",
        get_sender_id=lambda: "100003",
        platform_meta=SimpleNamespace(id="test-platform"),
    )
    try:
        results = [result async for result in plugin.card_command(event)]
        assert len(results) == 1 and "测试" in results[0]
        assert stopped == [True]
    finally:
        await plugin.terminate()


async def test_cancelling_initialization_releases_database_and_instance_lock(entry, monkeypatch):
    plugin_cls, _ = entry
    module = sys.modules[plugin_cls.__module__]
    entered = asyncio.Event()

    async def start(service):
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(module.Service, "start", start)
    plugin = plugin_cls(SimpleNamespace(platform_manager=SimpleNamespace(get_insts=lambda: [])), {})
    task = asyncio.create_task(plugin.initialize())
    await entered.wait()
    store = plugin.store
    lock_path = store.path.parent / "instance.lock"
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert store.closed and plugin.service is None
    module.InstanceLock(lock_path).close()


async def test_log_close_failure_does_not_leak_instance_lock(entry):
    plugin_cls, _ = entry
    plugin = plugin_cls(SimpleNamespace(), {})
    released = []

    def fail():
        raise OSError("disk unavailable")

    plugin.journal = SimpleNamespace(close=fail)
    plugin.lock = SimpleNamespace(close=lambda: released.append(True))
    with pytest.raises(OSError):
        await plugin.terminate()
    assert released == [True] and plugin.lock is None
