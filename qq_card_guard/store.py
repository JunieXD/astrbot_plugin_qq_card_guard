"""Single-threaded SQLite transactions; no network awaits inside a transaction."""

from __future__ import annotations

import asyncio
import json
import secrets
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict

from .config import GuardError, Later


def key(account, gid, uid=""):
    return f"{account}:{gid}:{uid}"


class Store:
    def __init__(self, path):
        self.path = path
        self.worker = ThreadPoolExecutor(max_workers=1, thread_name_prefix="card-guard-db")
        self.healthy = True
        self.closed = False
        self.pending = 0

    async def call(self, name, *args):
        if self.closed or (not self.healthy and name != "close_db"):
            raise GuardError("审计存储不可用，已停止自动操作，请检查磁盘和日志。")
        self.pending += 1
        if self.pending > 512:
            self.pending -= 1
            self.healthy = False
            raise GuardError("事件积压，已停止自动操作，请检查日志后重载。")
        future = asyncio.get_running_loop().run_in_executor(self.worker, getattr(self, name), *args)
        cancelled = False
        try:
            while True:
                try:
                    result = await asyncio.shield(future)
                    break
                except asyncio.CancelledError:
                    cancelled = True
                    if future.done():
                        result = future.result()
                        break
            if cancelled:
                raise asyncio.CancelledError
            return result
        except (sqlite3.Error, OSError) as exc:
            self.healthy = False
            if cancelled:
                raise asyncio.CancelledError from exc
            raise GuardError("审计存储写入失败，已停止自动操作，请检查磁盘和日志。") from exc
        finally:
            self.pending -= 1

    async def close(self):
        if self.closed:
            return
        try:
            await self.call("close_db")
        finally:
            self.closed = True
            self.worker.shutdown(wait=True)

    def open_db(self):
        self.db = sqlite3.connect(self.path, timeout=5)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA busy_timeout=5000")
        if self.db.execute("PRAGMA user_version").fetchone()[0] not in (0, 1):
            raise GuardError("数据库来自更新版本，请升级插件，不要删除数据库。")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, data TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS subjects (key TEXT PRIMARY KEY, account TEXT, gid TEXT, uid TEXT,
                eval_at REAL NOT NULL, data TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS subjects_work ON subjects(eval_at);
            CREATE TABLE IF NOT EXISTS cases (id TEXT PRIMARY KEY, account TEXT, gid TEXT, uid TEXT,
                phase TEXT, due REAL, data TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS cases_work ON cases(phase,due);
            CREATE INDEX IF NOT EXISTS cases_member ON cases(account,gid,uid);
            CREATE TABLE IF NOT EXISTS operations (id TEXT PRIMARY KEY, case_id TEXT, account TEXT, gid TEXT,
                uid TEXT, kind TEXT, at REAL, status TEXT, data TEXT, result TEXT,
                UNIQUE(case_id,kind));
            CREATE INDEX IF NOT EXISTS ops_quota ON operations(account,kind,at);
            CREATE TABLE IF NOT EXISTS reads (account TEXT, at REAL);
            CREATE INDEX IF NOT EXISTS reads_budget ON reads(account,at);
            CREATE TABLE IF NOT EXISTS exemptions (key TEXT PRIMARY KEY, until REAL, actor TEXT);
            CREATE TABLE IF NOT EXISTS events (id TEXT PRIMARY KEY, at REAL);
            CREATE INDEX IF NOT EXISTS events_time ON events(at);
            CREATE TABLE IF NOT EXISTS audit (id INTEGER PRIMARY KEY, at REAL, event TEXT, data TEXT);
            PRAGMA user_version=1;
        """)
        with self.db:
            self.db.execute("UPDATE operations SET status='unknown' WHERE status='submitted'")
            for row in self.db.execute(
                "SELECT data FROM cases WHERE phase NOT IN ('closed','archived')"
            ).fetchall():
                case = json.loads(row[0])
                uncertain = [self.has_operation(case["id"], kind) for kind in ("notify", "ban")]
                if any(op and op["status"] == "unknown" for op in uncertain):
                    case["phase"] = "review"
                    case["mute_state"] = "manual"
                    case["reason"] = "重载发现结果不明的提交，请管理员核对后接手"
                    self._set("block:" + case["account"], {"gid": case["gid"], "case": case["id"]})
                elif case["phase"] == "notify":
                    case["phase"] = "closed"
                elif case["phase"] == "ban":
                    case["phase"] = "watch"
                self._save_case(case)
            for row in self.db.execute("SELECT data FROM subjects WHERE eval_at>0").fetchall():
                subject = json.loads(row[0])
                subject["eval_at"] = 0
                self._save_subject(subject)

    def close_db(self):
        if hasattr(self, "db"):
            try:
                self.db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            finally:
                self.db.close()

    def get(self, name, default=None):
        row = self.db.execute("SELECT data FROM state WHERE key=?", (name,)).fetchone()
        return json.loads(row[0]) if row else default

    def _set(self, name, value):
        self.db.execute(
            "INSERT OR REPLACE INTO state VALUES(?,?)", (name, json.dumps(value, ensure_ascii=False))
        )

    def set(self, name, value):
        with self.db:
            self._set(name, value)

    def extend(self, name, until):
        with self.db:
            value = max(self.get(name, 0), until)
            self._set(name, value)
            return value

    def _audit(self, now, event, data):
        self.db.execute(
            "INSERT INTO audit(at,event,data) VALUES(?,?,?)",
            (now, event, json.dumps(data, ensure_ascii=False)),
        )

    def audit(self, now, event, data):
        with self.db:
            self._audit(now, event, data)

    def subject(self, account, gid, uid):
        row = self.db.execute("SELECT data FROM subjects WHERE key=?", (key(account, gid, uid),)).fetchone()
        return (
            json.loads(row[0])
            if row
            else {
                "account": account,
                "gid": gid,
                "uid": uid,
                "epoch": 1,
                "joined": 0,
                "present": True,
                "membership_at": 0,
                "waiting_join": False,
                "version": 0,
                "speech_at": 0,
                "received_at": 0,
                "eval_at": 0,
                "round": 0,
                "round_at": 0,
                "verified_card": None,
                "verified_at": 0,
                "card_hint": None,
                "card_at": 0,
                "card_unconfirmed": False,
                "role_hint": "",
                "role_at": 0,
                "ban": {},
                "ban_serial": 0,
                "platform": "",
            }
        )

    def _save_subject(self, subject):
        self.db.execute(
            "INSERT OR REPLACE INTO subjects VALUES(?,?,?,?,?,?)",
            (
                key(subject["account"], subject["gid"], subject["uid"]),
                subject["account"],
                subject["gid"],
                subject["uid"],
                subject["eval_at"],
                json.dumps(subject, ensure_ascii=False),
            ),
        )

    def observe(self, event, now, evaluate_enabled):
        with self.db:
            if not self.db.execute("INSERT OR IGNORE INTO events VALUES(?,?)", (event["id"], now)).rowcount:
                return False
            a, g, u = event["account"], event["gid"], event["uid"]
            subject = self.subject(a, g, u)
            subject["platform"] = event["platform"]
            subject["version"] += 1
            kind, occurred = event["kind"], event["at"]
            if kind == "message" and occurred >= subject["speech_at"]:
                subject["speech_at"] = occurred
                subject["received_at"] = now
                if evaluate_enabled:
                    subject["eval_at"] = subject["eval_at"] or now + 1
            if (
                kind in ("message", "group_card")
                and occurred >= subject["card_at"]
                and isinstance(event.get("card"), str)
            ):
                subject["card_hint"], subject["card_at"] = event["card"], occurred
                subject["card_unconfirmed"] = event["card"] != subject["verified_card"]
            if kind in ("group_increase", "group_decrease") and occurred >= subject["membership_at"]:
                subject["membership_at"] = occurred
                subject["epoch"] += 1
                subject["present"] = kind == "group_increase"
                subject["waiting_join"] = kind == "group_increase"
                subject["ban"] = {}
                subject["role_hint"] = ""
            if kind == "group_admin" and occurred >= max(subject["role_at"], subject["membership_at"]):
                subject["role_at"] = occurred
                subject["role_hint"] = "admin" if event["subtype"] == "set" else "member"
            if kind == "group_ban" and occurred >= max(subject["ban"].get("at", 0), subject["membership_at"]):
                subject["ban_serial"] = subject.get("ban_serial", 0) + 1
                subject["ban"] = {
                    "at": occurred,
                    "received": now,
                    "operator": event["operator"],
                    "duration": event["duration"],
                    "until": occurred + event["duration"],
                }
            self._save_subject(subject)
            # A card/admin/ban notice expedites existing follow-up, never creates punishment.
            if kind != "message":
                for row in self.db.execute(
                    "SELECT data FROM cases WHERE account=? AND gid=? AND uid=? AND phase IN ('watch','review')",
                    (a, g, u),
                ).fetchall():
                    case = json.loads(row[0])
                    case["due"] = min(case["due"], now + 1)
                    self._save_case(case)
            return True

    def verified(self, account, gid, member, now):
        with self.db:
            subject = self.subject(account, gid, member.uid)
            if not member.joined or member.joined > now + 300:
                raise GuardError("入群身份不明或成员已离群，暂缓处理。")
            if not subject["present"]:
                if member.joined <= subject["membership_at"]:
                    raise GuardError("成员已离群，等待可确认的新入群身份。")
                subject["present"] = True
                subject["waiting_join"] = False
                subject["epoch"] += 1
                subject["ban"] = {}
                subject["role_hint"] = ""
            if subject["waiting_join"]:
                if member.joined < subject["membership_at"] - 5:
                    raise GuardError("入群通知与成员缓存冲突，暂缓处理。")
                subject["waiting_join"] = False
            if subject["joined"] and member.joined < subject["joined"]:
                raise GuardError("入群时间缓存回退，暂缓处理。")
            if subject["joined"] and member.joined > subject["joined"]:
                subject["epoch"] += 1
                subject["membership_at"] = now
                subject["ban"] = {}
                subject["role_hint"] = ""
            subject["joined"] = member.joined
            subject["verified_card"] = member.card
            subject["verified_at"] = now
            if member.card == subject["card_hint"]:
                subject["card_unconfirmed"] = False
            self._save_subject(subject)
            return subject

    def evaluated(self, account, gid, uid, speech_at):
        with self.db:
            subject = self.subject(account, gid, uid)
            if subject["speech_at"] <= speech_at:
                subject["eval_at"] = 0
            self._save_subject(subject)

    def defer_evaluation(self, account, gid, uid, until):
        with self.db:
            subject = self.subject(account, gid, uid)
            subject["eval_at"] = until
            self._save_subject(subject)

    def due(self, now, limit=40):
        cases = [
            json.loads(r[0])
            for r in self.db.execute(
                "SELECT data FROM cases WHERE due<=? AND phase IN ('notify','ban','watch','settle') ORDER BY CASE WHEN phase='settle' THEN 0 ELSE 1 END,due LIMIT ?",
                (now, limit),
            )
        ]
        subjects = [
            json.loads(r[0])
            for r in self.db.execute(
                "SELECT data FROM subjects WHERE eval_at>0 AND eval_at<=? ORDER BY eval_at LIMIT ?",
                (now, limit),
            )
        ]
        return cases, subjects

    def case(self, cid):
        row = self.db.execute("SELECT data FROM cases WHERE id=?", (cid,)).fetchone()
        if not row:
            raise GuardError("处理记录不存在。")
        return json.loads(row[0])

    def _save_case(self, case):
        self.db.execute(
            "INSERT INTO cases VALUES(?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET phase=excluded.phase,due=excluded.due,data=excluded.data",
            (
                case["id"],
                case["account"],
                case["gid"],
                case["uid"],
                case["phase"],
                case["due"],
                json.dumps(case, ensure_ascii=False),
            ),
        )

    def patch_case(self, cid, changes):
        with self.db:
            case = self.case(cid)
            case.update(changes)
            self._save_case(case)
            return case

    def member_cases(self, account, gid, uid):
        return [
            json.loads(r[0])
            for r in self.db.execute(
                "SELECT data FROM cases WHERE account=? AND gid=? AND uid=? ORDER BY rowid DESC",
                (account, gid, uid),
            )
        ]

    def list_cases(self, account, gid, limit=20, offset=0):
        return [
            json.loads(r[0])
            for r in self.db.execute(
                "SELECT data FROM cases WHERE account=? AND gid=? ORDER BY rowid DESC LIMIT ? OFFSET ?",
                (account, gid, limit, offset),
            )
        ]

    def pending_cases(self, account, gid, limit=20, offset=0):
        return [
            json.loads(r[0])
            for r in self.db.execute(
                "SELECT data FROM cases WHERE account=? AND gid=? AND phase NOT IN ('closed','archived') ORDER BY CASE WHEN phase='review' THEN 0 ELSE 1 END,due LIMIT ? OFFSET ?",
                (account, gid, limit, offset),
            )
        ]

    def review_cases(self, account):
        return [
            json.loads(r[0])
            for r in self.db.execute("SELECT data FROM cases WHERE account=? AND phase='review'", (account,))
        ]

    def remove_exempt(self, account, gid, uid, actor, now):
        with self.db:
            self.db.execute("DELETE FROM exemptions WHERE key=?", (key(account, gid, uid),))
            self._audit(now, "取消豁免", {"account": account, "group": gid, "user": uid, "actor": actor})

    def new_case(self, subject, policy, platform, connection, round_no, minutes, now, due, max_pending=100):
        with self.db:
            existing_cases = self.member_cases(subject["account"], subject["gid"], subject["uid"])
            if (
                not any(c["phase"] == "watch" for c in existing_cases)
                and self.pending_count(subject["account"]) >= max_pending
            ):
                raise Later("待处理成员已达上限，优先完成现有核验。", now + 300)
            for existing in existing_cases:
                if existing["phase"] in ("notify", "ban", "review", "settle"):
                    raise GuardError("已有待处理事项，先完成当前核验。")
                if existing["phase"] == "watch":
                    existing["phase"] = "archived"
                    self._save_case(existing)
            case = {
                "id": secrets.token_hex(8),
                "account": subject["account"],
                "gid": policy.group_id,
                "uid": subject["uid"],
                "epoch": subject["epoch"],
                "joined": subject["joined"],
                "platform": platform,
                "connection": connection,
                "policy": asdict(policy),
                "revision": policy.revision,
                "round": round_no,
                "minutes": minutes,
                "created": now,
                "due": due,
                "execute_before": due + 300,
                "phase": "notify",
                "next_round": 0,
                "message_id": None,
                "message_hash": "",
                "sent_at": 0,
                "recall_state": "none",
                "mute_state": "none",
                "mute_until": 0,
                "ban_at": 0,
                "reason": "等待随机提醒",
                "polls": 0,
            }
            self._save_case(case)
            self._audit(
                now,
                "创建事项",
                {"case": case["id"], "group": policy.group_id, "user": subject["uid"], "round": round_no},
            )
            return case

    def exempt(self, account, gid, uid, now):
        row = self.db.execute(
            "SELECT until FROM exemptions WHERE key=?", (key(account, gid, uid),)
        ).fetchone()
        return bool(row and (row[0] == 0 or row[0] > now))

    def set_exempt(self, account, gid, uid, until, actor, now):
        with self.db:
            self.db.execute(
                "INSERT OR REPLACE INTO exemptions VALUES(?,?,?)", (key(account, gid, uid), until, actor)
            )
            self._audit(
                now,
                "设置豁免",
                {"account": account, "group": gid, "user": uid, "until": until, "actor": actor},
            )

    def has_operation(self, cid, kind):
        row = self.db.execute("SELECT * FROM operations WHERE case_id=? AND kind=?", (cid, kind)).fetchone()
        if not row:
            return None
        return {
            **dict(row),
            "data": json.loads(row["data"]),
            "result": json.loads(row["result"]) if row["result"] else None,
        }

    def reserve(self, case, kind, now, payload, policy, pace):
        with self.db:
            a, g, u = case["account"], case["gid"], case["uid"]
            if self.has_operation(case["id"], kind):
                raise GuardError("该步骤已有提交记录，不重复发送，请查看历史。")
            if kind == "notify":
                rows = self.db.execute(
                    "SELECT gid,at FROM operations WHERE account=? AND kind='notify' AND at>?",
                    (a, now - 3600),
                ).fetchall()
                if (
                    len(rows) >= pace.account_hourly_reminders
                    or sum(r[0] == g for r in rows) >= pace.group_hourly_reminders
                ):
                    raise Later("提醒额度已用完，稍后再检查。", min(r[1] for r in rows) + 3601)
            if kind == "ban":
                rows = self.db.execute(
                    "SELECT gid,uid,at FROM operations WHERE account=? AND kind='ban' AND at>?",
                    (a, now - 86400),
                ).fetchall()
                if (
                    len(rows) >= pace.account_daily_mutes
                    or sum(r[0] == g and r[1] == u for r in rows) >= policy.daily_mutes
                ):
                    raise Later("最近24小时禁言次数已达上限，本轮不再禁言。", min(r[2] for r in rows) + 86401)
            oid = secrets.token_hex(8)
            self.db.execute(
                "INSERT INTO operations VALUES(?,?,?,?,?,?,?,'submitted',?,NULL)",
                (oid, case["id"], a, g, u, kind, now, json.dumps(payload, ensure_ascii=False)),
            )
            self._audit(now, "提交意图", {"operation": oid, "case": case["id"], "kind": kind})
            return oid

    def finish(self, oid, status, result, changes, now):
        with self.db:
            op = self.db.execute("SELECT * FROM operations WHERE id=?", (oid,)).fetchone()
            self.db.execute(
                "UPDATE operations SET status=?,result=? WHERE id=?",
                (status, json.dumps(result, ensure_ascii=False), oid),
            )
            case = self.case(op["case_id"])
            case.update(changes)
            self._save_case(case)
            if op["kind"] == "notify" and status == "confirmed":
                subject = self.subject(op["account"], op["gid"], op["uid"])
                subject["round"], subject["round_at"] = case["round"], now
                self._save_subject(subject)
            self._audit(
                now,
                "操作结果",
                {
                    "operation": oid,
                    "case": case["id"],
                    "kind": op["kind"],
                    "status": status,
                    "result": result,
                },
            )
            return case

    def cancel_unsent(self, oid, now):
        with self.db:
            self.db.execute("DELETE FROM operations WHERE id=? AND status='submitted'", (oid,))
            self._audit(now, "发送前取消", {"operation": oid})

    def reserve_read(self, account, now, limit, priority):
        with self.db:
            self.db.execute("DELETE FROM reads WHERE at<?", (now - 3600,))
            rows = self.db.execute("SELECT at FROM reads WHERE account=? ORDER BY at", (account,)).fetchall()
            available = int(limit * (0.65 if priority == 0 else 0.85 if priority == 1 else 1))
            if len(rows) >= available:
                raise Later("资料读取已达预算，已安排稍后核对。", rows[0][0] + 3601)
            self.db.execute("INSERT INTO reads VALUES(?,?)", (account, now))

    def pending_count(self, account):
        return self.db.execute(
            "SELECT count(*) FROM cases WHERE account=? AND phase IN ('notify','ban','watch','review','settle')",
            (account,),
        ).fetchone()[0]

    def maintain(self, now=None):
        with self.db:
            self.db.execute(
                "DELETE FROM events WHERE at<?", ((time.time() if now is None else now) - 172800,)
            )
        self.db.execute("PRAGMA wal_checkpoint(PASSIVE)")
