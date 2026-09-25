from __future__ import annotations

import hashlib
import json
import string
from dataclasses import asdict, dataclass, field

import regex


class GuardError(Exception):
    """Administrator-safe error text, without transport credentials."""


class ApiFailure(GuardError):
    """OneBot returned a failure response; this alone does not imply disconnection."""

    def __init__(self, action, result):
        result = result if isinstance(result, dict) else {}
        self.action = action
        self.retcode = result.get("retcode") if type(result.get("retcode")) is int else None
        self.response = {
            field: result[field]
            for field in ("status", "message", "wording")
            if isinstance(result.get(field), str)
        }
        self.code = "api_business_failure"
        if action == "get_msg" and any(
            isinstance(result.get(field), str) and result[field].strip() == "消息不存在"
            for field in ("message", "wording")
        ):
            self.code = "message_not_found"
            message = "NapCat查询不到这条提醒，无法确认撤回结果，请人工核对。"
        else:
            code = self.retcode if self.retcode is not None else "未知"
            message = f"{action}返回业务失败（返回码：{code}），请检查NapCat日志和权限。"
        super().__init__(message)


class Later(GuardError):
    def __init__(self, message, until, *, code="deferred", **details):
        super().__init__(message)
        self.until = until
        self.code, self.details = code, details

    @property
    def routine(self):
        return self.code in {
            "operation_gap",
            "startup_wait",
            "manual_recovery",
        } and "connection_recovery" not in self.details.get("waits", {})


MODES = ("仅观察", "只提醒", "提醒并禁言")
DEFAULT_TEXT = "请将群名片改为：{格式说明}。例如：{正确示例}。改好后会自动核验并撤回提醒；如有本插件设置的禁言，会一并核验解除。"
FIELDS = {"群号", "QQ号", "格式说明", "正确示例", "违规轮次", "禁言分钟"}


@dataclass(frozen=True)
class Stage:
    minutes: int = 0
    text: str = ""


@dataclass(frozen=True)
class Policy:
    group_id: str
    enabled: bool = True
    mode: str = "仅观察"
    pattern: str = r"(?:大[一二三四五]|研[一二三])-\S{2,40}"
    format_help: str = "年级-学校"
    example: str = "大三-某某大学"
    strip_spaces: bool = True
    ignore_case: bool = False
    protect_level: int = 0
    protect_qq_level: int = 0
    protect_title: bool = True
    exempt_users: tuple[str, ...] = ()
    newcomer_minutes: int = 10
    first_grace_minutes: int = 10
    repeat_minutes: int = 5
    reset_days: int = 7
    daily_mutes: int = 3
    recall_on_compliance: bool = True
    unmute_on_compliance: bool = True
    reminder: str = DEFAULT_TEXT
    stages: tuple[Stage, ...] = (Stage(), Stage(10), Stage(60), Stage(360))
    bot_qq: str = ""
    compliant_cache_minutes: int = 60
    exempt_cache_minutes: int = 10

    @property
    def revision(self):
        return fingerprint(asdict(self))

    def stage(self, round_no):
        return self.stages[min(max(1, round_no), len(self.stages)) - 1]

    def render(self, uid, round_no, minutes):
        text = self.stage(round_no).text or self.reminder
        return text.format_map(
            {
                "群号": self.group_id,
                "QQ号": uid,
                "格式说明": self.format_help,
                "正确示例": self.example,
                "违规轮次": round_no,
                "禁言分钟": minutes,
            }
        )


@dataclass(frozen=True)
class FollowupStep:
    after_minutes: int
    minimum: int
    maximum: int


@dataclass(frozen=True)
class Pace:
    notify_min: int = 8
    notify_max: int = 20
    ban_min: int = 3
    ban_max: int = 8
    unmute_min: int = 1
    unmute_max: int = 3
    recall_min: int = 2
    recall_max: int = 6
    gap_min: int = 3
    gap_max: int = 8
    poll_min: int = 30
    poll_max: int = 60
    muted_poll_min: int = 20
    muted_poll_max: int = 40
    followup_steps: tuple[FollowupStep, ...] = (
        FollowupStep(3, 60, 120),
        FollowupStep(10, 180, 300),
        FollowupStep(30, 600, 1200),
        FollowupStep(120, 1800, 3600),
    )
    startup_min: int = 30
    startup_max: int = 90
    recovery_min: int = 300
    recovery_max: int = 900
    group_hourly_reminders: int = 10
    account_hourly_reminders: int = 30
    account_daily_mutes: int = 30
    reads_per_hour: int = 600
    max_pending: int = 30

    def interval(self, action):
        return getattr(self, action + "_min"), getattr(self, action + "_max")


@dataclass(frozen=True)
class Settings:
    enabled: bool = False
    groups: tuple[Policy, ...] = ()
    pace: Pace = field(default_factory=Pace)
    errors: tuple[tuple[str, str], ...] = ()

    def group(self, gid):
        for policy in self.groups:
            if policy.group_id == gid:
                return policy
        for key, error in self.errors:
            if key == gid:
                raise GuardError(error)
        raise GuardError("此群尚未配置，请在插件配置中添加。")


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:24]


def integer(value, label, low, high):
    text = str(value)
    if len(text) > 20 or not text.isascii() or not text.isdecimal() or not low <= int(text) <= high:
        raise GuardError(f"{label}应为 {low}～{high} 的整数。")
    return int(text)


def qq(value, label="QQ号", optional=False):
    text = str(value).strip()
    if optional and not text:
        return ""
    return str(integer(text, label, 10000, 9999999999999))


def switch(raw, key, default):
    value = raw.get(key, default)
    if type(value) is not bool:
        raise GuardError(f"{key}请使用开关设置。")
    return value


def template(value):
    if not isinstance(value, str) or len(value) > 1500:
        raise GuardError("提醒模板应为不超过1500字的文本。")
    try:
        for _, name, spec, conversion in string.Formatter().parse(value):
            if name is not None and (name not in FIELDS or spec or conversion):
                raise ValueError()
    except ValueError as exc:
        raise GuardError("提醒模板占位符仅支持：" + "、".join(sorted(FIELDS))) from exc
    return value


def parse_policy(raw):
    gid = qq(raw.get("group_id", ""), "群号")
    more = raw.get("advanced", {})
    if not isinstance(more, dict):
        raise GuardError("更多设置格式不正确。")
    mode = raw.get("mode", "仅观察")
    pattern = raw.get("pattern", Policy.pattern)
    if mode not in MODES:
        raise GuardError("请选择有效运行方式。")
    if not isinstance(pattern, str) or not 1 <= len(pattern) <= 2000:
        raise GuardError("请填写1～2000字的名片正则。")
    try:
        regex.compile(pattern)
    except (regex.error, RecursionError, OverflowError) as exc:
        raise GuardError("名片正则语法错误，请使用测试命令或检查配置。") from exc
    whitelist = raw.get("exempt_users", [])
    if not isinstance(whitelist, list) or len(whitelist) > 2000:
        raise GuardError("豁免名单应为QQ号列表，最多2000项。")
    stages = raw.get("stage_minutes", [0, 10, 60, 360])
    if not isinstance(stages, list) or not 1 <= len(stages) <= 8:
        raise GuardError("处理阶梯应为1～8档。")
    texts = raw.get("stage_reminders", [])
    if not isinstance(texts, list) or len(texts) > len(stages):
        raise GuardError("分轮提醒应为列表，项数不能超过禁言阶梯。")
    parsed_stages = [
        Stage(integer(value, "禁言分钟", 0, 1440), template(texts[i] if i < len(texts) else ""))
        for i, value in enumerate(stages)
    ]
    for key in ("format_help", "example"):
        if (
            not isinstance(raw.get(key, getattr(Policy, key)), str)
            or not 1 <= len(raw.get(key, getattr(Policy, key)).strip()) <= 300
        ):
            raise GuardError("格式说明和正确示例应填写1～300字。")
    policy = Policy(
        group_id=gid,
        enabled=switch(raw, "enabled", True),
        mode=mode,
        pattern=pattern,
        format_help=raw.get("format_help", Policy.format_help),
        example=raw.get("example", Policy.example),
        strip_spaces=switch(more, "strip_spaces", True),
        ignore_case=switch(more, "ignore_case", False),
        protect_level=integer(raw.get("protect_level", 0), "群等级保护线", 0, 999),
        protect_qq_level=integer(more.get("protect_qq_level", 0), "QQ等级保护线", 0, 999),
        protect_title=switch(raw, "protect_title", True),
        exempt_users=tuple(sorted({qq(x) for x in whitelist})),
        newcomer_minutes=integer(more.get("newcomer_minutes", 10), "新成员只提醒分钟", 0, 10080),
        first_grace_minutes=integer(raw.get("first_grace_minutes", 10), "首次宽限分钟", 1, 1440),
        repeat_minutes=integer(more.get("repeat_minutes", 5), "同成员提醒间隔分钟", 1, 1440),
        reset_days=integer(raw.get("reset_days", 7), "无新违规重置天数", 1, 365),
        daily_mutes=integer(more.get("daily_mutes", 3), "每人每日禁言上限", 1, 10),
        recall_on_compliance=switch(more, "recall_on_compliance", True),
        unmute_on_compliance=switch(more, "unmute_on_compliance", True),
        reminder=template(raw.get("reminder", DEFAULT_TEXT)),
        stages=tuple(parsed_stages),
        bot_qq=qq(more.get("bot_qq", ""), optional=True),
        compliant_cache_minutes=integer(raw.get("compliant_cache_minutes", 60), "合规免查分钟", 0, 1440),
        exempt_cache_minutes=integer(raw.get("exempt_cache_minutes", 10), "豁免免查分钟", 0, 1440),
    )
    if not policy.reminder.strip():
        raise GuardError("提醒内容不能是空白。")
    for round_no in range(1, len(policy.stages) + 1):
        if len(policy.render("9999999999999", round_no, 1440)) > 2000:
            raise GuardError("占位符展开后的提醒超过2000字，请缩短模板或格式说明。")
    return policy


def parse_settings(raw):
    rows = raw.get("groups", [])
    if not isinstance(rows, list) or len(rows) > 30:
        raise GuardError("群配置应为列表，最多30个群。")
    groups, errors, seen = [], [], set()
    for index, row in enumerate(rows, 1):
        gid = str(row.get("group_id", "")) if isinstance(row, dict) else ""
        try:
            if not isinstance(row, dict):
                raise GuardError("群配置格式不正确。")
            policy = parse_policy(row)
            gid = policy.group_id
            if gid in seen:
                groups = [g for g in groups if g.group_id != gid]
                raise GuardError("同一个群不能重复配置。")
            seen.add(gid)
            groups.append(policy)
        except GuardError as exc:
            errors.append((gid, f"第{index}个群：{exc}"))
    groups = [g for g in groups if g.group_id not in {k for k, _ in errors}]
    raw_pace = raw.get("pace", {})
    if not isinstance(raw_pace, dict):
        raise GuardError("执行节奏格式不正确。")
    limits = {
        "group_hourly_reminders": (1, 100),
        "account_hourly_reminders": (1, 300),
        "account_daily_mutes": (1, 100),
        "reads_per_hour": (60, 1200),
        "max_pending": (1, 100),
    }
    values = {}
    for key, default in asdict(Pace()).items():
        if key == "followup_steps":
            rows = raw.get(key, raw_pace.get(key, default))
            if not isinstance(rows, list) and not isinstance(rows, tuple):
                raise GuardError("后续补查阶梯应为列表。")
            if not 1 <= len(rows) <= 8:
                raise GuardError("后续补查阶梯应有1～8档。")
            steps = []
            for row in rows:
                if not isinstance(row, dict):
                    raise GuardError("后续补查阶梯格式不正确。")
                step = FollowupStep(
                    integer(row.get("after_minutes"), "补查起始分钟", 1, 43200),
                    integer(row.get("minimum"), "补查最短秒数", 30, 86400),
                    integer(row.get("maximum"), "补查最长秒数", 30, 86400),
                )
                if step.minimum > step.maximum or (
                    steps
                    and (
                        step.after_minutes <= steps[-1].after_minutes
                        or step.minimum < steps[-1].minimum
                        or step.maximum < steps[-1].maximum
                    )
                ):
                    raise GuardError("补查阶梯的起始时间须递增，间隔须逐档不缩短，最短不能超过最长。")
                steps.append(step)
            values[key] = tuple(steps)
            continue
        bounds = limits.get(key, (1, 3600) if key.startswith(("poll", "recovery")) else (0, 600))
        values[key] = integer(raw_pace.get(key, default), f"执行节奏 {key}", *bounds)
    pace = Pace(**values)
    for kind in ("notify", "ban", "unmute", "recall", "gap", "poll", "muted_poll", "startup", "recovery"):
        low, high = pace.interval(kind)
        if (
            low > high
            or (kind == "gap" and low < 1)
            or (kind == "poll" and low < 30)
            or (kind == "muted_poll" and low < 15)
        ):
            raise GuardError("等待最小值不能大于最大值；操作间隔至少1秒，补查至少30秒。")
    return Settings(switch(raw, "enabled", False), tuple(groups), pace, tuple(errors))
