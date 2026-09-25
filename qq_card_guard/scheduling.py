"""Pure scheduling decisions; cached screening never authorizes a QQ write."""

from __future__ import annotations


def followup_window(case, pace, now):
    if case["mute_state"] in ("owned", "unverified") and case["mute_until"] > now:
        return "禁言中", pace.muted_poll_min, pace.muted_poll_max
    age = max(0, now - (case["sent_at"] or case["created"])) / 60
    label, low, high = "提醒初期", pace.poll_min, pace.poll_max
    for step in pace.followup_steps:
        if age < step.after_minutes:
            break
        label, low, high = f"提醒后{step.after_minutes}分钟", step.minimum, step.maximum
    return label, low, high


def cache_check(subject, policy, connection, now):
    cached = subject.get("screening")
    if not cached:
        return None, subject.get("cache_invalidated", "尚无缓存")
    minutes = (
        policy.compliant_cache_minutes if cached["decision"] == "compliant" else policy.exempt_cache_minutes
    )
    until = min(cached["at"] + minutes * 60, cached.get("exemption_until") or float("inf"))
    if not minutes or not cached["at"] <= now < until:
        return None, "缓存已到期或免查已关闭"
    if cached["revision"] != policy.revision:
        return None, "群规则已变化"
    if not connection or cached["connection"] != connection:
        return None, "连接已变化或未确认"
    if cached["epoch"] != subject["epoch"] or not subject["present"] or subject["waiting_join"]:
        return None, "成员入群身份已变化"
    if subject.get("card_unconfirmed"):
        return None, "有未核实的名片变化"
    return {**cached, "until": until}, "缓存有效"
