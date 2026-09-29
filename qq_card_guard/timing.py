"""Per-attempt timing; API details are subdivisions, not extra elapsed time."""

from __future__ import annotations

import asyncio
import secrets
from contextlib import AsyncExitStack, asynccontextmanager, contextmanager
from contextvars import ContextVar

from .config import Later

_CURRENT = ContextVar("card_guard_action_timing", default=None)


class ActionTiming:
    def __init__(self, service, case, phase):
        self.service, self.case, self.phase = service, case, phase
        self.clock = service.monotonic
        self.stages, self.details = {}, {}
        self.outcome = "no_write"
        self.fields = dict(
            account=case["account"],
            group=case["gid"],
            user=case["uid"],
            case=case["id"],
            phase=phase,
            attempt=secrets.token_hex(6),
        )

    def add(self, name, seconds, *, detail=False):
        target = self.details if detail else self.stages
        target[name] = target.get(name, 0) + max(0, seconds)

    @contextmanager
    def stage(self, name, *, detail=False):
        start = self.clock()
        try:
            yield
        finally:
            self.add(name, self.clock() - start, detail=detail)

    @asynccontextmanager
    async def waiting(self, context, name):
        async with AsyncExitStack() as stack:
            with self.stage(name):
                await stack.enter_async_context(context)
            yield

    @contextmanager
    def trace(self):
        token = _CURRENT.set(self)
        started = self.clock()
        dispatched = self.service.clock()
        exception = None
        try:
            with self.service.journal.context(**self.fields):
                yield self
        except BaseException as exc:
            exception = exc
            if self.outcome not in ("confirmed", "uncertain"):
                self.outcome = (
                    "cancelled"
                    if isinstance(exc, asyncio.CancelledError)
                    else ("deferred" if isinstance(exc, Later) else "failed")
                )
            raise
        finally:
            _CURRENT.reset(token)
            elapsed = max(0, self.clock() - started)
            self.stages["other"] = max(0, elapsed - sum(self.stages.values()))
            fields = dict(
                **self.fields,
                outcome=self.outcome,
                duration_ms=round(elapsed * 1000, 2),
                stages_ms={k: round(v * 1000, 2) for k, v in self.stages.items()},
                details_ms={k: round(v * 1000, 2) for k, v in self.details.items()},
                scheduled_for=self.case["due"],
                scheduler_lag_ms=round(max(0, dispatched - self.case["due"]) * 1000, 2),
            )
            if self.phase == "ban" and self.case["sent_at"]:
                at = self.case.get("notify_completed_at", self.case["sent_at"])
                fields.update(
                    write_not_before=self.case.get("ban_not_before"),
                    since_reminder_ms=round(max(0, self.service.clock() - at) * 1000, 2),
                    reminder_reference="completed" if "notify_completed_at" in self.case else "submitted",
                    configured_delay_ms=self.case["ban_delay_seconds"] * 1000
                    if "ban_delay_seconds" in self.case
                    else None,
                )
            self.service.journal.record("操作耗时", exception=exception, **fields)


@contextmanager
def api_timing(name):
    timing = _CURRENT.get()
    if timing is None:
        yield
    else:
        with timing.stage(name, detail=True):
            yield
