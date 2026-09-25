"""Instance ownership and bounded, privacy-conscious operational logging."""

from __future__ import annotations

import asyncio
import json
import logging
import logging.handlers
import os
import re
import secrets
import shutil
import sys
import time
import traceback
from contextlib import ExitStack, contextmanager
from contextvars import ContextVar
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .config import GuardError

_LOG_CONTEXT = ContextVar("qq_card_guard_log_context", default={})


def redact(text):
    """Remove transport credentials without discarding useful exception messages."""
    text = re.sub(r"(?i)\b(?:https?|wss?)://[^\s\"'<>]+", "[URL已隐藏]", str(text))
    text = re.sub(r"(?i)\babk_[A-Za-z0-9_-]+", "[密钥已隐藏]", text)
    text = re.sub(r"(?i)\b(Bearer|ApiKey)\s+[^\s,;\"']+", r"\1 [已隐藏]", text)
    text = re.sub(r"(?im)\b(cookie|set-cookie)\s*:\s*[^\r\n]+", r"\1: [已隐藏]", text)
    return re.sub(
        r"(?i)((?:authorization|cookie|access_token|api_key|password|secret|token|p_skey|skey)"
        r"[\"']?\s*[:=]\s*)(?:\"[^\"]*\"|'[^']*'|[^\s,;}]+)",
        r"\1[已隐藏]",
        text,
    )


def exception_detail(exc):
    """Keep exception chains and frame locations; never include locals or source lines."""
    chain, seen = [], set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        chain.append(
            {
                "type": type(exc).__name__,
                "message": redact(str(exc)),
                "frames": [
                    {"file": frame.filename, "line": frame.lineno, "function": frame.name}
                    for frame in traceback.extract_tb(exc.__traceback__)
                ],
            }
        )
        exc = exc.__cause__ or (None if exc.__suppress_context__ else exc.__context__)
    return chain


class InstanceLock:
    def __init__(self, path: Path):
        self.file = path.open("a+b")
        try:
            # Windows mandatory byte locks also prohibit reading an already locked byte.
            # Inspect metadata instead; all owners lock offset zero even on first-create races.
            if os.fstat(self.file.fileno()).st_size == 0:
                self.file.write(b"0")
                self.file.flush()
            self.file.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(self.file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(self.file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            self.file.close()
            raise GuardError("已有一个名片规范插件实例在运行，请等待旧实例退出。") from exc

    def close(self):
        self.file.close()


class CheckedHandler(logging.handlers.RotatingFileHandler):
    healthy = True

    def handleError(self, record):
        was_healthy = self.healthy
        self.healthy = False
        if was_healthy:
            exc = sys.exc_info()[1]
            try:
                logging.getLogger(__name__).error(
                    "QQ 名片规范日志写入或轮转失败：%s",
                    json.dumps(
                        {"file": self.baseFilename, "exception": exception_detail(exc)}, ensure_ascii=False
                    ),
                )
            except Exception:
                # A broken host logger must not prevent marking the plugin unhealthy.
                pass


class Journal:
    def __init__(self, root: Path):
        self.root = root
        self.directory = root / "logs"
        self.directory.mkdir(parents=True, exist_ok=True)
        self.handler = CheckedHandler(
            self.directory / "guard.log",
            maxBytes=20 * 1024 * 1024,
            backupCount=7,
            encoding="utf-8",
            delay=False,
        )
        self.logger = logging.Logger(f"qq-card-guard-{id(self)}", level=logging.INFO)
        self.logger.addHandler(self.handler)
        try:
            self.screening_handler = CheckedHandler(
                self.directory / "decisions.log",
                maxBytes=20 * 1024 * 1024,
                backupCount=7,
                encoding="utf-8",
                delay=False,
            )
        except BaseException:
            self.handler.close()
            self.logger.removeHandler(self.handler)
            raise
        # Each line is independently parseable JSON, including multiline exception text.
        self.handler.setFormatter(logging.Formatter("%(message)s"))
        self.screening_handler.setFormatter(logging.Formatter("%(message)s"))
        self.screening_logger = logging.Logger(f"qq-card-decisions-{id(self)}", level=logging.INFO)
        self.screening_logger.addHandler(self.screening_handler)
        self.session = secrets.token_hex(6)

    @contextmanager
    def context(self, **fields):
        token = _LOG_CONTEXT.set({**_LOG_CONTEXT.get(), **fields})
        try:
            yield
        finally:
            _LOG_CONTEXT.reset(token)

    @contextmanager
    def span(self, kind, **fields):
        with self.context(**fields):
            started = time.monotonic()
            self.record(kind + "开始")
            try:
                yield
            except BaseException as exc:
                self.record(
                    kind + ("取消" if isinstance(exc, asyncio.CancelledError) else "失败"),
                    exception=exc,
                    duration_ms=round((time.monotonic() - started) * 1000, 2),
                    retry_at=getattr(exc, "until", None),
                )
                raise
            else:
                self.record(kind + "完成", duration_ms=round((time.monotonic() - started) * 1000, 2))

    def record(self, kind: str, detail: str = "", *, exception=None, screening=False, **fields):
        level = "INFO"
        if exception is not None:
            level = "WARNING" if isinstance(exception, (GuardError, asyncio.CancelledError)) else "ERROR"
            fields["exception"] = exception_detail(exception)
        # Business fields must never overwrite severity or the event identity.
        context = {**_LOG_CONTEXT.get(), **fields}
        if "level" in context:
            context["member_level"] = context.pop("level")
        payload = {
            **context,
            "at": datetime.now(timezone(timedelta(hours=8))).isoformat(timespec="milliseconds"),
            "level": level,
            "session": self.session,
            "event": kind,
            "detail": detail,
        }

        # Redact string values before encoding so untrusted newlines cannot forge log records.
        def clean(value):
            if isinstance(value, str):
                return redact(value)
            if isinstance(value, dict):
                return {key: clean(item) for key, item in value.items()}
            if isinstance(value, (list, tuple)):
                return [clean(item) for item in value]
            return value

        target = self.screening_logger if screening else self.logger
        target.log(getattr(logging, level), json.dumps(clean(payload), ensure_ascii=False))

    def check(self):
        if not self.handler.healthy or not self.screening_handler.healthy:
            raise GuardError("插件日志无法写入或轮转，已停止新处理，请检查磁盘和文件占用。")
        if shutil.disk_usage(self.root).free < 256 * 1024 * 1024:
            raise GuardError("插件数据盘剩余空间不足 256 MiB，已停止新处理。")
        total = sum(p.stat().st_size for p in self.root.glob("state.sqlite3*"))
        if total > 512 * 1024 * 1024:
            raise GuardError("插件数据库超过容量上限，已停止新处理，请检查并归档历史记录。")

    def maintain(self):
        self.check()

    def close(self):
        with ExitStack() as cleanup:
            for logger, handler in (
                (self.logger, self.handler),
                (self.screening_logger, self.screening_handler),
            ):
                cleanup.callback(logger.removeHandler, handler)
                cleanup.callback(handler.close)
                cleanup.callback(handler.flush)
