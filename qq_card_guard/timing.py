"""Per-attempt timing; API details are subdivisions, not extra elapsed time."""

from __future__ import annotations

import asyncio
import secrets
from contextlib import AsyncExitStack, asynccontextmanager, contextmanager
from contextvars import ContextVar

from .config import Later

_CURRENT = ContextVar("card_guard_action_timing", default=None)


def flow_timing(case):
    """Use confirmed milestones only; a failed attempt cannot look like a write."""
    names = (
        "trigger_speech_at",
        "trigger_received_at",
        "notify_completed_at",
        "ban_completed_at",
        "settlement_detected_at",
        "unmute_completed_at",
        "recall_completed_at",
    )
    milestones = {name: case[name] for name in names if case.get(name)}
    spans = {
        "delivery": ("trigger_speech_at", "trigger_received_at"),
        "speech_to_notify": ("trigger_speech_at", "notify_completed_at"),
        "received_to_notify": ("trigger_received_at", "notify_completed_at"),
        "speech_to_ban": ("trigger_speech_at", "ban_completed_at"),
        "notify_to_ban": ("notify_completed_at", "ban_completed_at"),
        "detection_to_unmute": ("settlement_detected_at", "unmute_completed_at"),
        "detection_to_recall": ("settlement_detected_at", "recall_completed_at"),
        "unmute_to_recall": ("unmute_completed_at", "recall_completed_at"),
    }
    return {
        "flow_at": milestones,
        "flow_ms": {
            name: round((milestones[end] - milestones[start]) * 1000, 2)
            for name, (start, end) in spans.items()
            if start in milestones and end in milestones and milestones[end] >= milestones[start]
        },
    }


class ActionTiming:
    def __init__(self, service, case, phase):
        self.service, self.case, self.phase = service, case, phase
        self.clock = service.monotonic
        self.stages, self.details = {}, {}
        self.outcome = "no_write"
        self.scheduled_for = case["due"]
        self.fields = dict(
            account=case["account"],
            group=case["gid"],
            user=case["uid"],
            case=case["id"],
            phase=phase,
            attempt=secrets.token_hex(6),
            write_not_before=case.get(phase + "_not_before"),
            configured_delay_ms=case[phase + "_delay_seconds"] * 1000
            if phase + "_delay_seconds" in case
            else None,
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
                scheduled_for=self.scheduled_for,
                scheduler_lag_ms=round(max(0, dispatched - self.scheduled_for) * 1000, 2),
                **flow_timing(self.case),
            )
            if self.phase == "ban" and self.case["sent_at"]:
                at = self.case.get("notify_completed_at", self.case["sent_at"])
                fields.update(
                    since_reminder_ms=round(max(0, self.service.clock() - at) * 1000, 2),
                    reminder_reference="completed" if "notify_completed_at" in self.case else "submitted",
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
