from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache

import regex

from .config import GuardError, Policy


def number(value, *, zero=False):
    text = str(value)
    if len(text) <= 20 and text.isascii() and text.isdecimal() and int(text) >= (0 if zero else 1):
        return int(text)
    return None


def message_id(value):
    text = str(value)
    digits = text[1:] if text.startswith("-") else text
    if len(text) <= 20 and digits.isascii() and digits.isdecimal() and int(text) != 0:
        return str(int(text))
    return None


@lru_cache(maxsize=64)
def compiled(pattern, ignore_case):
    return regex.compile(pattern, regex.IGNORECASE if ignore_case else 0)


def matches(policy: Policy, card: str | None):
    if card is None:
        raise GuardError("群名片资料缺失，暂缓处理。")
    if len(card) > 512:
        raise GuardError("名片长度异常，暂缓处理。")
    text = card.strip() if policy.strip_spaces else card
    if not text:
        return False
    try:
        return compiled(policy.pattern, policy.ignore_case).fullmatch(text, timeout=0.05) is not None
    except TimeoutError as exc:
        raise GuardError("名片正则匹配超时，已暂停本群，请简化正则。") from exc


@dataclass(frozen=True)
class Member:
    uid: str
    role: str
    card: str | None
    joined: int | None
    level: int | None
    qq_level: int | None
    title: str | None
    muted_until: int | None
    robot: bool = False

    @classmethod
    def parse(cls, raw):
        return cls(
            str(raw.get("user_id", "")),
            str(raw.get("role", "")),
            raw.get("card") if isinstance(raw.get("card"), str) else None,
            number(raw.get("join_time")),
            number(raw.get("level"), zero=True),
            number(raw.get("qq_level")),
            raw.get("title") if isinstance(raw.get("title"), str) else None,
            number(raw.get("shut_up_timestamp"), zero=True),
            raw.get("is_robot") is True,
        )


@dataclass(frozen=True)
class Verdict:
    state: str
    reason: str


def judge(policy, member, account, manual=False):
    if member.uid == account or member.role in ("admin", "owner"):
        return Verdict("exempt", "群主、管理员或机器人自身")
    if member.role != "member":
        return Verdict("unknown", "群身份未知，暂缓")
    if manual or member.uid in policy.exempt_users:
        return Verdict("exempt", "手动豁免名单")
    if member.robot:
        return Verdict("exempt", "平台标记的机器人")
    if policy.protect_title:
        if member.title is None:
            return Verdict("unknown", "头衔资料缺失，暂缓")
        if member.title:
            return Verdict("exempt", "拥有群专属头衔")
    for threshold, value, label in (
        (policy.protect_level, member.level, "群等级"),
        (policy.protect_qq_level, member.qq_level, "QQ等级"),
    ):
        if threshold and value is None:
            return Verdict("unknown", label + "资料缺失，暂缓")
        if threshold and value >= threshold:
            return Verdict("exempt", "达到" + label + "保护线")
    if not member.joined:
        return Verdict("unknown", "入群身份不明，暂缓")
    return (
        Verdict("compliant", "名片符合规则")
        if matches(policy, member.card)
        else Verdict("invalid", "名片不符合规则")
    )


def message_fingerprint(message):
    """Only hashes the bot's own reminder, never ordinary group conversation."""
    from .config import fingerprint

    if not isinstance(message, list):
        return ""
    parts = []
    for segment in message:
        if not isinstance(segment, dict):
            return ""
        kind, data = segment.get("type"), segment.get("data", {})
        if not isinstance(data, dict):
            return ""
        if kind == "text":
            parts.append((kind, str(data.get("text", ""))))
        elif kind == "at":
            parts.append((kind, str(data.get("qq", ""))))
        else:
            return ""
    return fingerprint(parts)
