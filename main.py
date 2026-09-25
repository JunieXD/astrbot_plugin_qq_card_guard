"""AstrBot entry point; no business rules or platform writes in event handlers."""

from __future__ import annotations

import asyncio

from astrbot.api import logger
from astrbot.api.event import filter
from astrbot.api.star import Context, Star, StarTools, register

from .qq_card_guard.commands import Commands
from .qq_card_guard.config import GuardError, parse_settings
from .qq_card_guard.platform import Router
from .qq_card_guard.resources import InstanceLock, Journal, exception_detail
from .qq_card_guard.service import Service
from .qq_card_guard.store import Store


@register("astrbot_plugin_qq_card_guard", "JunieXD", "按群名片规则提醒，支持阶梯禁言和改名后恢复", "0.1.0")
class QQCardGuard(Star):
    def __init__(self, context: Context, config=None):
        super().__init__(context=context, config=config)
        self.raw_config = config if config is not None else {}
        self.service = self.store = self.journal = self.lock = None
        self.start_error = "插件尚未初始化。"

    def settings(self):
        return parse_settings(dict(self.raw_config))

    async def initialize(self):
        try:
            settings = self.settings()
            root = StarTools.get_data_dir("astrbot_plugin_qq_card_guard")
            root.mkdir(parents=True, exist_ok=True)
            self.lock = InstanceLock(root / "instance.lock")
            self.journal = Journal(root)
            self.store = Store(root / "state.sqlite3")
            await self.store.call("open_db")
            self.service = Service(
                self.settings,
                self.store,
                Router(self.context, self.store, lambda: self.settings().pace, self.journal),
                self.journal,
            )
            await self.service.start()
            self.start_error = ""
            for gid, error in settings.errors:
                logger.warning("QQ 群名片规范：群 %s 配置无效：%s", gid, error)
            logger.info("QQ 群名片规范 v0.1.0 已加载；默认仅观察，请先配置群并测试正则。")
        except BaseException as exc:
            if self.journal:
                self.journal.record("初始化失败", exception=exc)
            self.start_error = (
                str(exc) if isinstance(exc, GuardError) else "初始化失败，请检查插件日志和数据目录。"
            )
            logger.error("QQ 群名片规范：%s；%s", self.start_error, exception_detail(exc))
            await self.terminate()
            if not isinstance(exc, Exception):
                raise

    async def terminate(self):
        try:
            if self.service:
                await self.service.stop()
        finally:
            try:
                if self.store:
                    await self.store.close()
            finally:
                try:
                    if self.journal:
                        self.journal.close()
                finally:
                    if self.lock:
                        self.lock.close()
                    self.service = self.store = self.journal = self.lock = None

    @filter.platform_adapter_type(filter.PlatformAdapterType.AIOCQHTTP)
    @filter.event_message_type(filter.EventMessageType.ALL, priority=100)
    async def observe_group(self, event):
        service = self.service
        if service:
            raw = getattr(getattr(event, "message_obj", None), "raw_message", None)
            if raw is not None:
                task = asyncio.current_task()
                service.jobs.add(task)
                try:
                    await service.observe(raw, str(event.platform_meta.id))
                except Exception as exc:
                    service.failure = "事件保存失败，已停止操作，请检查日志后重载。"
                    service.journal.record("事件接收异常", exception=exc)
                finally:
                    service.jobs.discard(task)

    @filter.command("名片规范", alias={"qcard"})
    @filter.event_message_type(filter.EventMessageType.PRIVATE_MESSAGE)
    @filter.platform_adapter_type(filter.PlatformAdapterType.AIOCQHTTP)
    async def card_command(self, event):
        event.stop_event()
        service = self.service
        if not service:
            yield event.plain_result(self.start_error or "插件正在停止，请稍后重试。")
            return
        task = asyncio.current_task()
        service.jobs.add(task)
        try:
            raw = getattr(event.message_obj, "raw_message", None)
            account = (
                str(raw.get("self_id", "")) if isinstance(raw, dict) else str(getattr(raw, "self_id", ""))
            )
            result = await Commands(service).run(
                event.get_message_str(), str(event.get_sender_id()), str(event.platform_meta.id), account
            )
        except GuardError as exc:
            result = str(exc)
        except Exception as exc:
            service.journal.record("命令异常", exception=exc)
            result = "命令未完成，请检查插件状态和日志。"
        finally:
            service.jobs.discard(task)
        yield event.plain_result(result)
