"""Small private-message control surface; every group command checks live group roles."""

from __future__ import annotations

import datetime as dt

from .config import GuardError, integer, qq
from .rules import matches
from .store import key

HELP = """QQ 群名片规范（请私聊使用）
/名片规范 状态 群号
/名片规范 测试 群号 要测试的名片
/名片规范 检查 群号 QQ号
/名片规范 待处理 群号 [页码]
/名片规范 历史 群号 [页码]
/名片规范 豁免 群号 QQ号 [天数，0为永久]
/名片规范 取消豁免 群号 QQ号
/名片规范 暂停 群号
/名片规范 恢复 群号
/名片规范 核对 群号 记录号
/名片规范 接手 群号 记录号
只有目标群当前的群主/管理员可操作。暂停只停止新增处理；改好后的核验仍继续。
“接手”表示你将人工处理该事项，停止插件继续操作；不会替你发送、禁言、解禁或撤回。"""

PHASES = {
    "notify": "待提醒",
    "ban": "待禁言",
    "watch": "等待修改",
    "review": "需人工核对",
    "settle": "处理解禁/撤回",
    "closed": "已结束",
    "archived": "较早一轮",
}
MUTES = {
    "none": "无插件禁言",
    "unverified": "待确认归属",
    "owned": "已确认归属",
    "external": "其他操作介入",
    "manual": "需人工核对",
    "expired": "已到期/被解除",
    "released": "插件已解禁",
}
RECALLS = {"none": "无已知提醒", "pending": "待撤回", "manual": "需人工撤回", "recalled": "已撤回"}


def when(value):
    if not value:
        return "无"
    return dt.datetime.fromtimestamp(value, dt.timezone(dt.timedelta(hours=8))).strftime("%m-%d %H:%M:%S")


def describe(case):
    return (
        f"{case['id']} · QQ {case['uid']} · 第{case['round']}轮 · {PHASES[case['phase']]}\n"
        f"禁言：{MUTES[case['mute_state']]}；提醒：{RECALLS[case['recall_state']]}\n"
        f"{case['reason']}"
    )


class Commands:
    def __init__(self, service):
        self.s = service
        self.db = service.store

    async def run(self, text, actor, pid, account):
        parts = text.strip().split(maxsplit=3)
        if parts and parts[0].lstrip("/") in ("名片规范", "qcard"):
            parts.pop(0)
        else:
            parts = text.strip().split(maxsplit=2)
        if not parts or parts[0] in ("帮助", "help"):
            return HELP
        if len(parts) < 2:
            raise GuardError("请填写群号。例如：/名片规范 状态 123456789")
        action, gid = parts[:2]
        tail = parts[2] if len(parts) > 2 else ""
        policy = self.s.settings().group(qq(gid, "群号"))
        a = qq(account, "机器人QQ")
        actor = qq(actor)
        now = self.s.clock()
        throttle = key(a, gid, actor)
        if self.s.command_until.get(throttle, 0) > now:
            raise GuardError("命令过于频繁，请10秒后再试。")
        self.s.command_until[throttle] = now + 10
        adapter = await self.s.authorize(policy, actor, pid, a)
        g = policy.group_id
        if action == "状态":
            block = await self.db.call("get", "block:" + a, {})
            pause = await self.db.call("get", "pause:" + key(a, g), "")
            pending = await self.db.call("pending_cases", a, g, 101)
            errors = self.s.group_errors.get(g)
            return (
                f"群 {g} · {policy.mode}\n"
                f"新增处理：{'已开启' if self.s.settings().enabled and policy.enabled else '已关闭'}"
                f"；暂停：{'是' if pause or block else '否'}\n"
                f"格式：{policy.format_help}；示例：{policy.example}\n"
                f"待处理：{len(pending)}；阶梯禁言分钟：{' → '.join(str(x.minutes) for x in policy.stages)}\n"
                f"运行异常：{self.s.failure or (errors[1] if errors and errors[0] == policy.revision else '无')}\n"
                f"账号待核对：{'有（请查看待处理，接手后恢复）' if block else '无'}"
            )
        if action == "测试":
            return ("符合" if matches(policy, tail) else "不符合") + f"群 {g} 的名片规则（完整匹配）。"
        if action in ("检查", "豁免", "取消豁免"):
            values = tail.split()
            if not values:
                raise GuardError("请填写目标QQ号。")
            uid = qq(values[0])
            async with self.s.member_lock(a, g, uid):
                if action == "检查":
                    member, _, verdict = await self.s.inspect(policy, adapter, uid, priority=2)
                    return (
                        f"QQ {uid}：{verdict.reason}\n当前群名片：{member.card or '(空)'}\n"
                        f"当前禁言截止：{when(member.muted_until)}\n此命令仅核对资料，不新增处罚。"
                    )
                self.s.invalidate(a, g, uid)
                if action == "豁免":
                    days = integer(values[1] if len(values) > 1 else 0, "豁免天数", 0, 3650)
                    await self.db.call("set_exempt", a, g, uid, now + days * 86400 if days else 0, actor, now)
                    await self.s.settle_member(a, g, uid, "管理员手动豁免")
                    return f"已豁免QQ {uid}（{str(days) + '天' if days else '永久'}），已有提醒和禁言将按归属核验处理。"
                await self.db.call("remove_exempt", a, g, uid, actor, now)
                return "已移除命令设置的豁免；配置里的名单和其他保护条件仍然有效。"
        if action in ("待处理", "历史"):
            page = integer(tail.strip() or 1, "页码", 1, 100000)
            cases = await self.db.call(
                "pending_cases" if action == "待处理" else "list_cases", a, g, 10, (page - 1) * 10
            )
            return f"第{page}页（每页最多10条）\n" + ("\n\n".join(describe(c) for c in cases) or "暂无记录。")
        if action == "暂停":
            await self.s.pause(a, g, actor)
            return "已暂停本群新增提醒/禁言；已有事项的合规核验、解禁和撤回继续。"
        if action == "恢复":
            until = await self.s.resume(a, g, actor)
            return f"已解除人工暂停，恢复冷却至 {when(until)}。全局开关和群运行方式仍按配置执行。"
        if action in ("核对", "接手"):
            case = await self.db.call("case", tail.strip())
            if case["account"] != a or case["gid"] != g:
                raise GuardError("该记录不属于当前群和机器人。")
            async with self.s.member_lock(a, g, case["uid"]):
                case = await self.db.call("case", case["id"])
                if action == "核对":
                    member = await adapter.member(g, case["uid"], 2)
                    return (
                        describe(case)
                        + f"\n当前名片：{member.card or '(空)'}\n当前禁言截止：{when(member.muted_until)}\n"
                        "请结合群内实际提醒和禁言记录检查；结果不明时插件不会自动重发。确认由你处理后使用“接手”。"
                    )
                self.s.invalidate(a, g, case["uid"])
                await self.db.call(
                    "patch_case",
                    case["id"],
                    {
                        "phase": "closed",
                        "mute_state": "manual",
                        "recall_state": "manual",
                        "reason": "管理员已接手；插件不再处理此事项",
                    },
                )
                await self.db.call("audit", now, "人工接手", {"case": case["id"], "actor": actor, "group": g})
                # Stop immediately recurring enforcement until the administrator explicitly resumes.
                await self.s.pause(a, g, actor)
                return "已交由你人工处理，并暂停本群新增处理。请自行核对提醒和禁言，处理完后使用“恢复”。"
        raise GuardError("不认识这个命令。私聊 /名片规范 查看帮助。")
