"""Sealed public-procurement tendering and evaluation service."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import uuid
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parent
DEFAULT_DB = ROOT / "public_procurement.db"


class DomainError(Exception):
    def __init__(self, message: str, status: int = 400):
        super().__init__(message)
        self.status = status


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def parse_time(value: str) -> datetime:
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise DomainError("时间格式无效") from exc
    if result.tzinfo is None:
        result = result.replace(tzinfo=timezone.utc)
    return result.astimezone(timezone.utc)


def clean_actor(actor: str) -> str:
    actor = (actor or "").strip()
    if not actor:
        raise DomainError("缺少操作人")
    return actor


def require_role(role: str, allowed: set[str], action: str) -> None:
    if role not in allowed:
        raise DomainError("角色无权执行：%s" % action, 403)


def canonical_hash(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


GENESIS_HASH = "0" * 64


def chain_node_hash(seq: int, tender_id: int | None, bid_id: int | None, evaluation_id: int | None,
                    evaluation_round: int, batch_id: str, event_type: str, payload: str, prev_hash: str) -> str:
    """摘要链节点哈希：覆盖序号、采购项目、投标、评分、批次、事件类型、报文和前序哈希。"""
    return canonical_hash({
        "seq": seq,
        "tender_id": tender_id,
        "bid_id": bid_id,
        "evaluation_id": evaluation_id,
        "evaluation_round": evaluation_round,
        "batch_id": batch_id,
        "event_type": event_type,
        "payload": payload,
        "prev_hash": prev_hash,
    })


class ProcurementService:
    def __init__(self, db_path: str | os.PathLike[str] = DEFAULT_DB):
        self.db_path = str(db_path)
        self._init_schema()
        self._migrate_chain()
        # 服务重启时自检摘要链：可修复的半写节点先修复，无法修复的置为阻断授标
        self.startup_chain_status = self.verify_chain(repair=True)

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=10000")
        return conn

    def _init_schema(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS tenders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tender_no TEXT NOT NULL UNIQUE,
                    title TEXT NOT NULL,
                    description TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'draft',
                    deadline TEXT NOT NULL,
                    criteria TEXT NOT NULL DEFAULT '[]',
                    evaluation_round INTEGER NOT NULL DEFAULT 1,
                    evaluations_locked INTEGER NOT NULL DEFAULT 0,
                    awarded_bid_id INTEGER,
                    award_snapshot TEXT,
                    version INTEGER NOT NULL DEFAULT 1,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS vendors (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    vendor_no TEXT NOT NULL UNIQUE,
                    name TEXT NOT NULL,
                    representative TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS bids (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tender_id INTEGER NOT NULL REFERENCES tenders(id),
                    vendor_id INTEGER NOT NULL REFERENCES vendors(id),
                    payload TEXT NOT NULL,
                    payload_hash TEXT NOT NULL,
                    price REAL NOT NULL,
                    status TEXT NOT NULL DEFAULT 'sealed',
                    version INTEGER NOT NULL DEFAULT 1,
                    submitted_by TEXT NOT NULL,
                    submitted_at TEXT NOT NULL,
                    opened_at TEXT,
                    UNIQUE(tender_id,vendor_id)
                );
                CREATE TABLE IF NOT EXISTS evaluations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bid_id INTEGER NOT NULL REFERENCES bids(id),
                    evaluation_round INTEGER NOT NULL,
                    evaluator TEXT NOT NULL,
                    criterion TEXT NOT NULL,
                    raw_value REAL NOT NULL,
                    score REAL NOT NULL,
                    comment TEXT NOT NULL DEFAULT '',
                    version INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(bid_id,evaluation_round,evaluator,criterion)
                );
                CREATE TABLE IF NOT EXISTS conflicts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tender_id INTEGER NOT NULL REFERENCES tenders(id),
                    evaluator TEXT NOT NULL,
                    vendor_id INTEGER REFERENCES vendors(id),
                    reason TEXT NOT NULL,
                    declared_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(tender_id,evaluator,vendor_id)
                );
                CREATE TABLE IF NOT EXISTS clarifications (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tender_id INTEGER NOT NULL REFERENCES tenders(id),
                    vendor_id INTEGER REFERENCES vendors(id),
                    question TEXT NOT NULL,
                    answer TEXT,
                    status TEXT NOT NULL DEFAULT 'pending',
                    answered_by TEXT,
                    created_at TEXT NOT NULL,
                    answered_at TEXT
                );
                CREATE TABLE IF NOT EXISTS complaints (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tender_id INTEGER NOT NULL REFERENCES tenders(id),
                    complainant TEXT NOT NULL,
                    body TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'open',
                    resolution TEXT,
                    reviewed_by TEXT,
                    created_at TEXT NOT NULL,
                    resolved_at TEXT
                );
                CREATE TABLE IF NOT EXISTS timeline (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tender_id INTEGER REFERENCES tenders(id),
                    actor TEXT NOT NULL,
                    action TEXT NOT NULL,
                    details TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS score_chain (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    tender_id INTEGER,
                    bid_id INTEGER,
                    evaluation_id INTEGER,
                    evaluation_round INTEGER NOT NULL,
                    batch_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    payload TEXT NOT NULL DEFAULT '',
                    prev_hash TEXT NOT NULL DEFAULT '',
                    hash TEXT,
                    status TEXT NOT NULL DEFAULT 'valid',
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS chain_state (
                    id INTEGER PRIMARY KEY CHECK(id=1),
                    tip_seq INTEGER NOT NULL DEFAULT 0,
                    tip_hash TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'ok',
                    break_seq INTEGER,
                    break_reason TEXT,
                    updated_at TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_bids_tender ON bids(tender_id,status);
                CREATE INDEX IF NOT EXISTS idx_eval_bid_round ON evaluations(bid_id,evaluation_round);
                CREATE INDEX IF NOT EXISTS idx_chain_eval ON score_chain(evaluation_id);
                CREATE INDEX IF NOT EXISTS idx_chain_tender_round ON score_chain(tender_id,evaluation_round);
                """
            )

    def _audit(self, conn: sqlite3.Connection, tender_id: int | None, actor: str,
               action: str, details: dict[str, Any]) -> None:
        conn.execute(
            "INSERT INTO timeline(tender_id,actor,action,details,created_at) VALUES(?,?,?,?,?)",
            (tender_id, actor, action, json.dumps(details, ensure_ascii=False, sort_keys=True), utcnow()),
        )

    def _append_chain(self, conn: sqlite3.Connection, *, tender_id: int | None, bid_id: int | None,
                      evaluation_id: int | None, evaluation_round: int, batch_id: str, event_type: str,
                      payload_obj: dict[str, Any], status: str = "valid") -> dict[str, Any]:
        """向连续摘要链追加一个节点，并推进链末端状态。必须与业务写入处于同一事务。"""
        tip = conn.execute("SELECT seq,hash FROM score_chain ORDER BY seq DESC LIMIT 1").fetchone()
        prev_hash = tip["hash"] if tip else GENESIS_HASH
        seq = (tip["seq"] if tip else 0) + 1
        payload_text = json.dumps(payload_obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        digest = chain_node_hash(seq, tender_id, bid_id, evaluation_id, evaluation_round,
                                 batch_id, event_type, payload_text, prev_hash)
        conn.execute(
            """INSERT INTO score_chain(seq,tender_id,bid_id,evaluation_id,evaluation_round,batch_id,
                                       event_type,payload,prev_hash,hash,status,created_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
            (seq, tender_id, bid_id, evaluation_id, evaluation_round, batch_id,
             event_type, payload_text, prev_hash, digest, status, utcnow()),
        )
        conn.execute("UPDATE chain_state SET tip_seq=?,tip_hash=?,updated_at=? WHERE id=1",
                     (seq, digest, utcnow()))
        return {"seq": seq, "hash": digest}

    def _migrate_chain(self) -> None:
        """升级补链：为没有摘要节点的历史评分按主键顺序补链。

        只插入链节点，不覆盖 evaluations 原值；投标或采购项目无法确认的记录标记 pending（待核）。
        """
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            columns = {row["name"] for row in conn.execute("PRAGMA table_info(evaluations)").fetchall()}
            if "status" not in columns:
                conn.execute("ALTER TABLE evaluations ADD COLUMN status TEXT NOT NULL DEFAULT 'valid'")
            conn.execute(
                "INSERT OR IGNORE INTO chain_state(id,tip_seq,tip_hash,status,updated_at) VALUES(1,0,?,'ok',?)",
                (GENESIS_HASH, utcnow()),
            )
            rows = conn.execute("SELECT * FROM evaluations ORDER BY id").fetchall()
            for row in rows:
                chained = conn.execute(
                    "SELECT 1 FROM score_chain WHERE evaluation_id=? AND event_type='score'", (row["id"],)
                ).fetchone()
                if chained:
                    continue
                bid = conn.execute("SELECT * FROM bids WHERE id=?", (row["bid_id"],)).fetchone()
                tender = conn.execute("SELECT * FROM tenders WHERE id=?", (bid["tender_id"],)).fetchone() if bid else None
                if not bid or not tender:
                    status, tender_id = "pending", bid["tender_id"] if bid else None
                elif row["evaluation_round"] < tender["evaluation_round"]:
                    status, tender_id = "voided", bid["tender_id"]
                else:
                    status, tender_id = "valid", bid["tender_id"]
                self._append_chain(
                    conn,
                    tender_id=tender_id,
                    bid_id=row["bid_id"],
                    evaluation_id=row["id"],
                    evaluation_round=row["evaluation_round"],
                    batch_id="legacy-%d" % row["id"],
                    event_type="score",
                    payload_obj={"evaluator": row["evaluator"], "criterion": row["criterion"],
                                 "raw_value": row["raw_value"], "score": row["score"],
                                 "comment": row["comment"], "legacy": True},
                    status=status,
                )

    def verify_chain(self, repair: bool = False, actor: str = "system") -> dict[str, Any]:
        """校验摘要链完整性；repair=True 时修复可修复节点，无法修复的置为阻断授标。"""
        with self.connect() as conn:
            if repair:
                conn.execute("BEGIN IMMEDIATE")
            nodes = [dict(r) for r in conn.execute("SELECT * FROM score_chain ORDER BY seq").fetchall()]
            prev = GENESIS_HASH
            repairs: list[tuple[int, str, str]] = []
            break_info: dict[str, Any] | None = None
            rounds: dict[int, dict[str, Any]] = {}
            seq_round: dict[int, int] = {}
            for node in nodes:
                seq_round[node["seq"]] = node["evaluation_round"]
                agg = rounds.setdefault(node["evaluation_round"],
                                        {"round": node["evaluation_round"], "nodes": 0,
                                         "valid": 0, "voided": 0, "pending": 0, "events": 0})
                agg["nodes"] += 1
                if node["event_type"] == "void":
                    agg["events"] += 1
                elif node["status"] in {"valid", "voided", "pending"}:
                    agg[node["status"]] += 1
                try:
                    payload_ok = bool(node["payload"]) and json.loads(node["payload"]) is not None
                except (TypeError, ValueError):
                    payload_ok = False
                if not payload_ok:
                    break_info = {"seq": node["seq"], "reason": "链节点数据残缺，无法确认原始内容"}
                    break
                fixed_prev = node["prev_hash"] or ""
                need_fix = fixed_prev != prev
                if need_fix:
                    fixed_prev = prev
                expected = chain_node_hash(node["seq"], node["tender_id"], node["bid_id"], node["evaluation_id"],
                                           node["evaluation_round"], node["batch_id"], node["event_type"],
                                           node["payload"], fixed_prev)
                fixed_hash = node["hash"] or ""
                if fixed_hash != expected:
                    fixed_hash = expected
                    need_fix = True
                if need_fix:
                    repairs.append((node["seq"], fixed_prev, fixed_hash))
                prev = fixed_hash
            if break_info is None and repairs:
                break_info = {"seq": repairs[0][0], "reason": "链节点哈希缺失或与内容不匹配"}
            tip_seq = nodes[-1]["seq"] if nodes else 0
            tip_hash = prev if nodes else GENESIS_HASH
            state = conn.execute("SELECT * FROM chain_state WHERE id=1").fetchone()
            if break_info is None and (state["tip_seq"] != tip_seq or state["tip_hash"] != tip_hash):
                break_info = {"seq": tip_seq, "reason": "链末端状态与节点不一致"}
            unrepairable = break_info is not None and "无法确认" in break_info["reason"]
            repaired = 0
            if repair:
                if unrepairable:
                    conn.execute(
                        "UPDATE chain_state SET status='blocked',break_seq=?,break_reason=?,updated_at=? WHERE id=1",
                        (break_info["seq"], break_info["reason"], utcnow()),
                    )
                    self._audit(conn, None, actor, "chain.blocked", dict(break_info))
                    status = "blocked"
                else:
                    if repairs:
                        for seq, fixed_prev, fixed_hash in repairs:
                            conn.execute("UPDATE score_chain SET prev_hash=?,hash=? WHERE seq=?",
                                         (fixed_prev, fixed_hash, seq))
                        repaired = len(repairs)
                        self._audit(conn, None, actor, "chain.repaired",
                                    {"from_seq": repairs[0][0], "repaired_nodes": repaired})
                    conn.execute(
                        "UPDATE chain_state SET tip_seq=?,tip_hash=?,status='ok',break_seq=NULL,break_reason=NULL,updated_at=? WHERE id=1",
                        (tip_seq, tip_hash, utcnow()),
                    )
                    break_info = None
                    status = "ok"
            else:
                persisted = state["status"]
                if unrepairable or persisted == "blocked":
                    status = "blocked"
                elif break_info is not None:
                    status = "broken"
                else:
                    status = "ok"
            break_round = seq_round.get(break_info["seq"]) if break_info else None
            round_list = []
            for rnd in sorted(rounds):
                agg = rounds[rnd]
                round_status = "ok"
                if break_round == rnd:
                    round_status = "broken"
                elif agg["pending"]:
                    round_status = "pending"
                round_list.append({**agg, "status": round_status})
            return {
                "status": status,
                "total_nodes": len(nodes),
                "tip": {"seq": tip_seq, "hash": tip_hash},
                "break_at": break_info,
                "rounds": round_list,
                "repaired": repaired,
                "checked_at": utcnow(),
            }

    def repair_chain(self, actor: str, role: str) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"supervisor", "auditor"}, "修复评分摘要链")
        return self.verify_chain(repair=True, actor=actor)

    def _tender(self, conn: sqlite3.Connection, tender_id: int) -> sqlite3.Row:
        row = conn.execute("SELECT * FROM tenders WHERE id=?", (tender_id,)).fetchone()
        if not row:
            raise DomainError("采购项目不存在", 404)
        return row

    def create_vendor(self, actor: str, role: str, vendor_no: str, name: str,
                      representative: str) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"procurement", "supervisor"}, "创建供应商")
        if not vendor_no.strip() or not name.strip() or not representative.strip():
            raise DomainError("供应商编号、名称和代表不能为空")
        with self.connect() as conn:
            try:
                cur = conn.execute(
                    "INSERT INTO vendors(vendor_no,name,representative,created_at) VALUES(?,?,?,?)",
                    (vendor_no.strip(), name.strip(), representative.strip(), utcnow()),
                )
            except sqlite3.IntegrityError as exc:
                raise DomainError("供应商编号已存在", 409) from exc
            self._audit(conn, None, actor, "vendor.created", {"vendor_no": vendor_no.strip()})
            return dict(conn.execute("SELECT * FROM vendors WHERE id=?", (cur.lastrowid,)).fetchone())

    def create_tender(self, actor: str, role: str, tender_no: str, title: str,
                      deadline: str, criteria: list[dict[str, Any]], description: str = "") -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"procurement"}, "创建采购项目")
        parse_time(deadline)
        if not tender_no.strip() or not title.strip():
            raise DomainError("项目编号和标题不能为空")
        normalized_criteria = []
        total_weight = Decimal("0")
        for item in criteria:
            if not isinstance(item, dict) or not str(item.get("name", "")).strip():
                raise DomainError("评分项格式无效")
            kind = item.get("kind", "direct")
            if kind not in {"direct", "cost"}:
                raise DomainError("评分项类型只支持 direct 或 cost")
            try:
                weight = Decimal(str(item["weight"]))
                max_value = Decimal(str(item.get("max_value", 100)))
            except (KeyError, InvalidOperation) as exc:
                raise DomainError("评分权重或上限无效") from exc
            if weight <= 0 or max_value <= 0:
                raise DomainError("评分权重和上限必须大于0")
            total_weight += weight
            normalized_criteria.append({"name": str(item["name"]).strip(), "kind": kind,
                                        "weight": float(weight), "max_value": float(max_value)})
        if not normalized_criteria or total_weight != 100:
            raise DomainError("评分项权重合计必须等于100")
        with self.connect() as conn:
            try:
                cur = conn.execute(
                    """INSERT INTO tenders(tender_no,title,description,deadline,criteria,created_by,created_at,updated_at)
                       VALUES(?,?,?,?,?,?,?,?)""",
                    (tender_no.strip(), title.strip(), description.strip(), parse_time(deadline).isoformat(timespec="seconds"),
                     json.dumps(normalized_criteria, ensure_ascii=False), actor, utcnow(), utcnow()),
                )
            except sqlite3.IntegrityError as exc:
                raise DomainError("项目编号已存在", 409) from exc
            self._audit(conn, cur.lastrowid, actor, "tender.created", {"tender_no": tender_no.strip()})
            return dict(self._tender(conn, cur.lastrowid))

    def publish_tender(self, actor: str, role: str, tender_id: int, expected_version: int) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"procurement"}, "发布采购项目")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            tender = self._tender(conn, tender_id)
            if tender["status"] != "draft":
                raise DomainError("只有草稿项目可以发布", 409)
            if tender["version"] != int(expected_version):
                raise DomainError("项目已变化，请刷新后重试", 409)
            conn.execute("UPDATE tenders SET status='published',version=version+1,updated_at=? WHERE id=?", (utcnow(), tender_id))
            self._audit(conn, tender_id, actor, "tender.published", {"deadline": tender["deadline"]})
            return dict(self._tender(conn, tender_id))

    def submit_bid(self, actor: str, role: str, tender_id: int, vendor_id: int,
                   payload: dict[str, Any], price: float, expected_version: int | None = None) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"vendor"}, "提交投标")
        if not isinstance(payload, dict):
            raise DomainError("投标内容必须是对象")
        try:
            price = float(price)
        except (TypeError, ValueError) as exc:
            raise DomainError("报价必须是数值") from exc
        if price <= 0:
            raise DomainError("报价必须大于0")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            tender = self._tender(conn, tender_id)
            if tender["status"] != "published":
                raise DomainError("当前项目不接受投标", 409)
            if datetime.now(timezone.utc) >= parse_time(tender["deadline"]):
                raise DomainError("投标截止时间已过", 409)
            vendor = conn.execute("SELECT * FROM vendors WHERE id=?", (vendor_id,)).fetchone()
            if not vendor:
                raise DomainError("供应商不存在", 404)
            if not conn.execute("SELECT 1 FROM conflicts WHERE tender_id=? AND vendor_id=? AND evaluator=?", (tender_id, vendor_id, actor)).fetchone():
                pass
            existing = conn.execute("SELECT * FROM bids WHERE tender_id=? AND vendor_id=?", (tender_id, vendor_id)).fetchone()
            payload_text = json.dumps(payload, ensure_ascii=False, sort_keys=True)
            digest = canonical_hash(payload)
            if existing:
                if existing["status"] != "sealed":
                    raise DomainError("投标已撤回或已开标，不能修改", 409)
                if expected_version is None or existing["version"] != int(expected_version):
                    raise DomainError("投标已变化，请刷新后重试", 409)
                conn.execute(
                    "UPDATE bids SET payload=?,payload_hash=?,price=?,version=version+1,submitted_at=? WHERE id=? AND version=?",
                    (payload_text, digest, price, utcnow(), existing["id"], expected_version),
                )
                bid_id = existing["id"]
                action = "bid.updated"
            else:
                cur = conn.execute(
                    """INSERT INTO bids(tender_id,vendor_id,payload,payload_hash,price,submitted_by,submitted_at)
                       VALUES(?,?,?,?,?,?,?)""",
                    (tender_id, vendor_id, payload_text, digest, price, actor, utcnow()),
                )
                bid_id = cur.lastrowid
                action = "bid.submitted"
            self._audit(conn, tender_id, actor, action, {"bid_id": bid_id, "vendor_id": vendor_id, "hash": digest})
            bid = dict(conn.execute("SELECT * FROM bids WHERE id=?", (bid_id,)).fetchone())
            bid["payload_hash"] = digest
            return bid

    def withdraw_bid(self, actor: str, role: str, bid_id: int, expected_version: int) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"vendor"}, "撤回投标")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            bid = conn.execute("SELECT * FROM bids WHERE id=?", (bid_id,)).fetchone()
            if not bid:
                raise DomainError("投标不存在", 404)
            tender = self._tender(conn, bid["tender_id"])
            if bid["submitted_by"] != actor:
                raise DomainError("只能撤回自己的投标", 403)
            if bid["version"] != int(expected_version):
                raise DomainError("投标已变化，请刷新后重试", 409)
            if datetime.now(timezone.utc) >= parse_time(tender["deadline"]) or bid["status"] != "sealed":
                raise DomainError("截止后不能撤回投标", 409)
            conn.execute("UPDATE bids SET status='withdrawn',version=version+1 WHERE id=?", (bid_id,))
            self._audit(conn, bid["tender_id"], actor, "bid.withdrawn", {"bid_id": bid_id})
            return dict(conn.execute("SELECT * FROM bids WHERE id=?", (bid_id,)).fetchone())

    def open_bids(self, actor: str, role: str, tender_id: int, expected_version: int) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"procurement", "supervisor"}, "开标")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            tender = self._tender(conn, tender_id)
            if tender["status"] != "published":
                raise DomainError("项目当前不能开标", 409)
            if tender["version"] != int(expected_version):
                raise DomainError("项目已变化，请刷新后重试", 409)
            if datetime.now(timezone.utc) < parse_time(tender["deadline"]):
                raise DomainError("尚未到开标时间", 409)
            rows = conn.execute("SELECT * FROM bids WHERE tender_id=? AND status='sealed' ORDER BY id", (tender_id,)).fetchall()
            opened = []
            now = utcnow()
            for row in rows:
                digest = canonical_hash(json.loads(row["payload"]))
                if digest != row["payload_hash"]:
                    raise DomainError("投标完整性校验失败: %s" % row["id"], 409)
                conn.execute("UPDATE bids SET status='opened',opened_at=?,version=version+1 WHERE id=?", (now, row["id"]))
                opened.append(dict(conn.execute("SELECT * FROM bids WHERE id=?", (row["id"],)).fetchone()))
            conn.execute("UPDATE tenders SET status='opened',version=version+1,updated_at=? WHERE id=?", (now, tender_id))
            self._audit(conn, tender_id, actor, "tender.opened", {"bid_count": len(opened)})
            return {"tender": dict(self._tender(conn, tender_id)), "bids": opened}

    def declare_conflict(self, actor: str, role: str, tender_id: int, evaluator: str,
                         vendor_id: int | None, reason: str) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"evaluator", "procurement", "supervisor"}, "申报利益冲突")
        if not evaluator.strip() or not reason.strip():
            raise DomainError("评审人和冲突原因不能为空")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            tender = self._tender(conn, tender_id)
            try:
                cur = conn.execute(
                    "INSERT INTO conflicts(tender_id,evaluator,vendor_id,reason,declared_by,created_at) VALUES(?,?,?,?,?,?)",
                    (tender_id, evaluator.strip(), vendor_id, reason.strip(), actor, utcnow()),
                )
            except sqlite3.IntegrityError as exc:
                raise DomainError("利益冲突已申报", 409) from exc
            conflict_id = cur.lastrowid
            # 已授标项目的快照保持冻结；未授标的，该评审人当前轮评分失效作废、排名重算
            frozen = tender["status"] in {"awarded", "cancelled"}
            voided: list[int] = []
            if not frozen:
                if vendor_id is not None:
                    bid_ids = [r["id"] for r in conn.execute(
                        "SELECT id FROM bids WHERE tender_id=? AND vendor_id=?", (tender_id, vendor_id)).fetchall()]
                else:
                    bid_ids = [r["id"] for r in conn.execute(
                        "SELECT id FROM bids WHERE tender_id=?", (tender_id,)).fetchall()]
                if bid_ids:
                    marks = ",".join("?" for _ in bid_ids)
                    rows = conn.execute(
                        "SELECT id FROM evaluations WHERE evaluator=? AND evaluation_round=? AND status='valid' AND bid_id IN (%s)" % marks,
                        (evaluator.strip(), tender["evaluation_round"], *bid_ids),
                    ).fetchall()
                    now = utcnow()
                    for row in rows:
                        conn.execute("UPDATE evaluations SET status='voided',updated_at=? WHERE id=?", (now, row["id"]))
                        conn.execute("UPDATE score_chain SET status='voided' WHERE evaluation_id=? AND event_type='score'", (row["id"],))
                    voided = [row["id"] for row in rows]
                    if voided:
                        self._append_chain(
                            conn,
                            tender_id=tender_id,
                            bid_id=None,
                            evaluation_id=None,
                            evaluation_round=tender["evaluation_round"],
                            batch_id="VOID-CF-%d" % conflict_id,
                            event_type="void",
                            payload_obj={"source": "conflict", "source_id": conflict_id,
                                         "evaluator": evaluator.strip(), "vendor_id": vendor_id,
                                         "round": tender["evaluation_round"],
                                         "voided_evaluation_ids": voided, "reason": reason.strip()},
                        )
            self._audit(conn, tender_id, actor, "conflict.declared", {
                "evaluator": evaluator.strip(), "vendor_id": vendor_id, "reason": reason.strip(),
                "voided_evaluation_ids": voided, "snapshot_frozen": frozen,
            })
            result = dict(conn.execute("SELECT * FROM conflicts WHERE id=?", (conflict_id,)).fetchone())
            result["voided_evaluation_ids"] = voided
            result["snapshot_frozen"] = frozen
            return result

    def evaluate_bid(self, actor: str, role: str, bid_id: int, values: dict[str, float],
                     comment: str = "", expected_round: int | None = None) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"evaluator"}, "评分")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            bid = conn.execute("SELECT * FROM bids WHERE id=?", (bid_id,)).fetchone()
            if not bid:
                raise DomainError("投标不存在", 404)
            tender = self._tender(conn, bid["tender_id"])
            if tender["status"] not in {"opened", "reevaluation"} or tender["evaluations_locked"]:
                raise DomainError("当前项目不能评分", 409)
            if expected_round is not None and int(expected_round) != tender["evaluation_round"]:
                raise DomainError("评分轮次已变化：投诉处理已生效，请按最新轮次重新评分", 409)
            if bid["status"] not in {"opened", "qualified"}:
                raise DomainError("该投标不能评分", 409)
            conflict = conn.execute(
                "SELECT 1 FROM conflicts WHERE tender_id=? AND evaluator=? AND (vendor_id=? OR vendor_id IS NULL)",
                (tender["id"], actor, bid["vendor_id"]),
            ).fetchone()
            if conflict:
                raise DomainError("评审人与该供应商存在利益冲突", 403)
            criteria = json.loads(tender["criteria"])
            missing = [c["name"] for c in criteria if c["name"] not in values]
            if missing:
                raise DomainError("缺少评分项: " + ",".join(missing))
            created = []
            chain_nodes = []
            now = utcnow()
            batch_id = "R%d-%s" % (tender["evaluation_round"], uuid.uuid4().hex[:12])
            for criterion in criteria:
                try:
                    raw = float(values[criterion["name"]])
                except (TypeError, ValueError) as exc:
                    raise DomainError("评分值必须是数值") from exc
                if raw < 0 or raw > criterion["max_value"]:
                    raise DomainError("评分值超出范围: " + criterion["name"])
                if criterion["kind"] == "direct":
                    score = raw / criterion["max_value"] * 100
                else:
                    benchmark = criterion["max_value"]
                    score = min(100.0, benchmark / raw * 100) if raw > 0 else 0.0
                existing = conn.execute(
                    """SELECT * FROM evaluations WHERE bid_id=? AND evaluation_round=? AND evaluator=? AND criterion=?""",
                    (bid_id, tender["evaluation_round"], actor, criterion["name"]),
                ).fetchone()
                if existing:
                    raise DomainError("该评分项已提交，不能覆盖", 409)
                cur = conn.execute(
                    """INSERT INTO evaluations(bid_id,evaluation_round,evaluator,criterion,raw_value,score,comment,created_at,updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?)""",
                    (bid_id, tender["evaluation_round"], actor, criterion["name"], raw, score, comment.strip(), now, now),
                )
                evaluation_id = cur.lastrowid
                chain_nodes.append(self._append_chain(
                    conn,
                    tender_id=tender["id"],
                    bid_id=bid_id,
                    evaluation_id=evaluation_id,
                    evaluation_round=tender["evaluation_round"],
                    batch_id=batch_id,
                    event_type="score",
                    payload_obj={"evaluator": actor, "criterion": criterion["name"], "raw_value": raw,
                                 "score": score, "comment": comment.strip()},
                ))
                created.append(dict(conn.execute("SELECT * FROM evaluations WHERE id=?", (evaluation_id,)).fetchone()))
            # 评分也是项目变更：推进版本，使并发的投诉处理能按先提交事务为准、另一方 409
            conn.execute("UPDATE tenders SET version=version+1,updated_at=? WHERE id=?", (now, tender["id"]))
            self._audit(conn, tender["id"], actor, "bid.evaluated", {
                "bid_id": bid_id, "batch_id": batch_id,
                "criteria": [item["criterion"] for item in created],
                "chain_seqs": [node["seq"] for node in chain_nodes],
            })
            return {"bid_id": bid_id, "evaluator": actor, "round": tender["evaluation_round"],
                    "batch_id": batch_id, "evaluations": created, "chain": chain_nodes}

    def disqualify_bid(self, actor: str, role: str, bid_id: int, reason: str,
                       expected_version: int) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"procurement", "supervisor"}, "废标")
        if not reason.strip():
            raise DomainError("废标理由不能为空")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            bid = conn.execute("SELECT * FROM bids WHERE id=?", (bid_id,)).fetchone()
            if not bid:
                raise DomainError("投标不存在", 404)
            if bid["version"] != int(expected_version):
                raise DomainError("投标已变化，请刷新后重试", 409)
            if bid["status"] not in {"opened", "qualified"}:
                raise DomainError("当前投标不能废标", 409)
            conn.execute("UPDATE bids SET status='disqualified',version=version+1 WHERE id=?", (bid_id,))
            self._audit(conn, bid["tender_id"], actor, "bid.disqualified", {"bid_id": bid_id, "reason": reason.strip()})
            return dict(conn.execute("SELECT * FROM bids WHERE id=?", (bid_id,)).fetchone())

    def ask_clarification(self, actor: str, role: str, tender_id: int, vendor_id: int, question: str) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"vendor", "procurement", "supervisor"}, "提交澄清")
        if not question.strip():
            raise DomainError("澄清问题不能为空")
        with self.connect() as conn:
            self._tender(conn, tender_id)
            cur = conn.execute(
                "INSERT INTO clarifications(tender_id,vendor_id,question,created_at) VALUES(?,?,?,?)",
                (tender_id, vendor_id, question.strip(), utcnow()),
            )
            self._audit(conn, tender_id, actor, "clarification.asked", {"clarification_id": cur.lastrowid})
            return dict(conn.execute("SELECT * FROM clarifications WHERE id=?", (cur.lastrowid,)).fetchone())

    def answer_clarification(self, actor: str, role: str, clarification_id: int,
                             answer: str, publish: bool = True) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"procurement", "supervisor"}, "答复澄清")
        if not answer.strip():
            raise DomainError("澄清答复不能为空")
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM clarifications WHERE id=?", (clarification_id,)).fetchone()
            if not row:
                raise DomainError("澄清不存在", 404)
            if row["status"] != "pending":
                raise DomainError("澄清已经处理", 409)
            status = "published" if publish else "answered"
            conn.execute(
                "UPDATE clarifications SET answer=?,status=?,answered_by=?,answered_at=? WHERE id=?",
                (answer.strip(), status, actor, utcnow(), clarification_id),
            )
            self._audit(conn, row["tender_id"], actor, "clarification.answered", {"clarification_id": clarification_id, "published": publish})
            return dict(conn.execute("SELECT * FROM clarifications WHERE id=?", (clarification_id,)).fetchone())

    def submit_complaint(self, actor: str, role: str, tender_id: int, body: str) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"vendor", "evaluator", "procurement", "supervisor"}, "提交投诉")
        if not body.strip():
            raise DomainError("投诉内容不能为空")
        with self.connect() as conn:
            tender = self._tender(conn, tender_id)
            if tender["status"] in {"awarded", "cancelled"}:
                raise DomainError("项目已经结束，不能提交投诉", 409)
            cur = conn.execute(
                "INSERT INTO complaints(tender_id,complainant,body,created_at) VALUES(?,?,?,?)",
                (tender_id, actor, body.strip(), utcnow()),
            )
            self._audit(conn, tender_id, actor, "complaint.submitted", {"complaint_id": cur.lastrowid})
            return dict(conn.execute("SELECT * FROM complaints WHERE id=?", (cur.lastrowid,)).fetchone())

    def resolve_complaint(self, actor: str, role: str, complaint_id: int, decision: str,
                          resolution: str, expected_version: int | None = None) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"supervisor"}, "处理投诉")
        if decision not in {"accepted", "rejected"} or not resolution.strip():
            raise DomainError("投诉决定或处理说明无效")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            complaint = conn.execute("SELECT * FROM complaints WHERE id=?", (complaint_id,)).fetchone()
            if not complaint:
                raise DomainError("投诉不存在", 404)
            if complaint["status"] != "open":
                raise DomainError("投诉已经处理", 409)
            conn.execute(
                "UPDATE complaints SET status=?,resolution=?,reviewed_by=?,resolved_at=? WHERE id=?",
                (decision, resolution.strip(), actor, utcnow(), complaint_id),
            )
            voided: list[int] = []
            if decision == "accepted":
                tender = self._tender(conn, complaint["tender_id"])
                if tender["status"] in {"awarded", "cancelled"}:
                    raise DomainError("已结束项目不能重新评审", 409)
                if expected_version is not None and tender["version"] != int(expected_version):
                    raise DomainError("项目已变化，请刷新后重试", 409)
                # 尚未授标：当前轮评分和排名失效作废，进入新一轮重评
                rows = conn.execute(
                    """SELECT e.id FROM evaluations e JOIN bids b ON b.id=e.bid_id
                       WHERE b.tender_id=? AND e.evaluation_round=? AND e.status='valid'""",
                    (tender["id"], tender["evaluation_round"]),
                ).fetchall()
                now = utcnow()
                for row in rows:
                    conn.execute("UPDATE evaluations SET status='voided',updated_at=? WHERE id=?", (now, row["id"]))
                    conn.execute("UPDATE score_chain SET status='voided' WHERE evaluation_id=? AND event_type='score'", (row["id"],))
                voided = [row["id"] for row in rows]
                conn.execute(
                    "UPDATE tenders SET status='reevaluation',evaluation_round=evaluation_round+1,evaluations_locked=0,version=version+1,updated_at=? WHERE id=?",
                    (now, tender["id"]),
                )
                self._append_chain(
                    conn,
                    tender_id=tender["id"],
                    bid_id=None,
                    evaluation_id=None,
                    evaluation_round=tender["evaluation_round"],
                    batch_id="VOID-CMP-%d" % complaint_id,
                    event_type="void",
                    payload_obj={"source": "complaint", "source_id": complaint_id,
                                 "round": tender["evaluation_round"],
                                 "voided_evaluation_ids": voided, "resolution": resolution.strip()},
                )
            self._audit(conn, complaint["tender_id"], actor, "complaint.resolved", {
                "complaint_id": complaint_id, "decision": decision, "voided_evaluation_ids": voided,
            })
            result = dict(conn.execute("SELECT * FROM complaints WHERE id=?", (complaint_id,)).fetchone())
            result["voided_evaluation_ids"] = voided
            return result

    def award_tender(self, actor: str, role: str, tender_id: int, expected_version: int) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"supervisor"}, "授标")
        chain = self.verify_chain()
        if chain["status"] != "ok":
            seq = (chain["break_at"] or {}).get("seq", "?")
            raise DomainError("评分摘要链存在断点(seq=%s)，授标已阻断，请先修复链" % seq, 409)
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            tender = self._tender(conn, tender_id)
            if tender["status"] not in {"opened", "reevaluation"}:
                raise DomainError("当前项目不能授标", 409)
            if tender["version"] != int(expected_version):
                raise DomainError("项目已变化，请刷新后重试", 409)
            open_complaint = conn.execute("SELECT COUNT(*) AS c FROM complaints WHERE tender_id=? AND status='open'", (tender_id,)).fetchone()["c"]
            if open_complaint:
                raise DomainError("存在未处理投诉，不能授标", 409)
            bids = conn.execute("SELECT * FROM bids WHERE tender_id=? AND status IN ('opened','qualified')", (tender_id,)).fetchall()
            criteria = json.loads(tender["criteria"])
            expected_criteria = {c["name"] for c in criteria}
            ranking = []
            for bid in bids:
                rows = conn.execute(
                    "SELECT criterion,AVG(score) AS score FROM evaluations WHERE bid_id=? AND evaluation_round=? AND status='valid' GROUP BY criterion",
                    (bid["id"], tender["evaluation_round"]),
                ).fetchall()
                scores = {row["criterion"]: row["score"] for row in rows}
                if set(scores) != expected_criteria:
                    raise DomainError("投标尚未完成全部评分: %s" % bid["id"], 409)
                weighted = 0.0
                for criterion in criteria:
                    weighted += scores[criterion["name"]] * criterion["weight"] / 100
                ranking.append({"bid_id": bid["id"], "vendor_id": bid["vendor_id"], "price": bid["price"], "score": round(weighted, 2)})
            if not ranking:
                raise DomainError("没有可授标的有效投标", 409)
            ranking.sort(key=lambda item: (-item["score"], item["price"], item["bid_id"]))
            winner = ranking[0]
            chain_tip = conn.execute("SELECT tip_hash FROM chain_state WHERE id=1").fetchone()["tip_hash"]
            snapshot = {"tender_id": tender_id, "round": tender["evaluation_round"], "ranking": ranking, "winner": winner,
                        "awarded_by": actor, "awarded_at": utcnow(), "chain_tip": chain_tip}
            conn.execute(
                "UPDATE tenders SET status='awarded',awarded_bid_id=?,award_snapshot=?,evaluations_locked=1,version=version+1,updated_at=? WHERE id=? AND version=?",
                (winner["bid_id"], json.dumps(snapshot, ensure_ascii=False), utcnow(), tender_id, expected_version),
            )
            conn.execute("UPDATE bids SET status='awarded',version=version+1 WHERE id=?", (winner["bid_id"],))
            self._audit(conn, tender_id, actor, "tender.awarded", {"winner": winner, "ranking": ranking})
            return {"tender": dict(self._tender(conn, tender_id)), "award": snapshot}

    def get_tender(self, actor: str, role: str, tender_id: int) -> dict[str, Any]:
        with self.connect() as conn:
            tender = dict(self._tender(conn, tender_id))
            bids = []
            if role in {"procurement", "supervisor", "auditor"} and tender["status"] in {"opened", "reevaluation", "awarded"}:
                bids = [dict(r) for r in conn.execute("SELECT * FROM bids WHERE tender_id=? ORDER BY id", (tender_id,)).fetchall()]
            elif role == "vendor":
                bids = []
                for row in conn.execute(
                    "SELECT b.*,t.status AS tender_status FROM bids b JOIN tenders t ON t.id=b.tender_id WHERE b.tender_id=? AND b.submitted_by=?",
                    (tender_id, actor),
                ).fetchall():
                    item = dict(row)
                    item.pop("tender_status", None)
                    if tender["status"] not in {"opened", "reevaluation", "awarded"}:
                        item.pop("payload", None)
                    bids.append(item)
            else:
                bids = [dict(r) for r in conn.execute(
                    "SELECT id,tender_id,vendor_id,price,status,payload_hash,submitted_at,opened_at FROM bids WHERE tender_id=? ORDER BY id",
                    (tender_id,),
                ).fetchall()]
            clarifications = [dict(r) for r in conn.execute(
                "SELECT id,tender_id,vendor_id,question,answer,status,answered_at FROM clarifications WHERE tender_id=? AND status='published' ORDER BY id",
                (tender_id,),
            ).fetchall()]
            return {"tender": tender, "bids": bids, "clarifications": clarifications}

    def state(self, actor: str = "", role: str = "public") -> dict[str, Any]:
        with self.connect() as conn:
            tenders = [dict(r) for r in conn.execute(
                "SELECT id,tender_no,title,description,status,deadline,evaluation_round,version,awarded_bid_id,created_at,updated_at FROM tenders ORDER BY id DESC"
            ).fetchall()]
            timeline = [dict(r) for r in conn.execute("SELECT * FROM timeline ORDER BY id DESC LIMIT 200").fetchall()]
            if role in {"procurement", "supervisor", "auditor"}:
                bids = [dict(r) for r in conn.execute(
                    """SELECT b.id,b.tender_id,b.vendor_id,b.price,b.status,b.payload_hash,b.submitted_at,b.opened_at,
                              CASE WHEN t.status IN ('opened','reevaluation','awarded') THEN b.payload ELSE NULL END AS payload
                       FROM bids b JOIN tenders t ON t.id=b.tender_id ORDER BY b.id DESC LIMIT 200"""
                ).fetchall()]
                complaints = [dict(r) for r in conn.execute("SELECT * FROM complaints ORDER BY id DESC LIMIT 100").fetchall()]
            elif role == "vendor":
                bids = []
                for row in conn.execute(
                    """SELECT b.*,t.status AS tender_status FROM bids b JOIN tenders t ON t.id=b.tender_id
                       WHERE b.submitted_by=? ORDER BY b.id DESC LIMIT 100""",
                    (actor,),
                ).fetchall():
                    item = dict(row)
                    status = item.pop("tender_status")
                    if status not in {"opened", "reevaluation", "awarded"}:
                        item.pop("payload", None)
                    bids.append(item)
                complaints = [dict(r) for r in conn.execute(
                    "SELECT * FROM complaints WHERE complainant=? ORDER BY id DESC LIMIT 100", (actor,)
                ).fetchall()]
            else:
                bids, complaints = [], []
        return {"tenders": tenders, "bids": bids, "complaints": complaints, "timeline": timeline, "role": role}

    def seed_demo(self) -> dict[str, Any]:
        with self.connect() as conn:
            if conn.execute("SELECT COUNT(*) AS c FROM tenders").fetchone()["c"]:
                return {"seeded": False, "reason": "已有数据"}
        vendor = self.create_vendor("proc-demo", "procurement", "V-001", "启明科技", "vendor-demo")
        deadline = (datetime.now(timezone.utc) + __import__("datetime").timedelta(hours=1)).isoformat(timespec="seconds")
        tender = self.create_tender(
            "proc-demo", "procurement", "TENDER-DEMO", "服务器采购", deadline,
            [{"name": "价格", "weight": 60, "kind": "cost", "max_value": 1000000},
             {"name": "质量", "weight": 40, "kind": "direct", "max_value": 100}],
        )
        published = self.publish_tender("proc-demo", "procurement", tender["id"], tender["version"])
        self.submit_bid("vendor-demo", "vendor", tender["id"], vendor["id"], {"价格": 900000, "质量": 90}, 900000)
        return {"seeded": True, "tender_id": tender["id"], "vendor_id": vendor["id"], "published_version": published["version"]}


class ApiHandler(BaseHTTPRequestHandler):
    service: ProcurementService

    def _send(self, status: int, payload: Any) -> None:
        body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _headers(self) -> tuple[str, str]:
        return self.headers.get("X-User", ""), self.headers.get("X-Role", "public")

    def _json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        if length > 2_000_000:
            raise DomainError("请求体过大", 413)
        if not length:
            return {}
        try:
            value = json.loads(self.rfile.read(length).decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise DomainError("请求体不是有效 JSON") from exc
        if not isinstance(value, dict):
            raise DomainError("JSON 请求体必须是对象")
        return value

    def do_GET(self) -> None:
        try:
            path = urlparse(self.path).path
            if path in {"/", "/index.html"}:
                body = (ROOT / "static" / "index.html").read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            actor, role = self._headers()
            if path == "/health":
                self._send(200, {"status": "ok", "service": "public-procurement"})
            elif path == "/api/state":
                self._send(200, self.service.state(actor, role))
            elif path == "/api/chain/status":
                self._send(200, self.service.verify_chain())
            elif path.startswith("/api/tenders/"):
                self._send(200, self.service.get_tender(actor, role, int(path.split("/")[3])))
            else:
                self._send(404, {"error": "接口不存在"})
        except DomainError as exc:
            self._send(exc.status, {"error": str(exc)})
        except (ValueError, IndexError) as exc:
            self._send(400, {"error": str(exc)})

    def do_POST(self) -> None:
        try:
            path, data, (actor, role) = urlparse(self.path).path, self._json(), self._headers()
            if path == "/api/vendors":
                result = self.service.create_vendor(actor, role, **data)
            elif path == "/api/tenders":
                result = self.service.create_tender(actor, role, **data)
            elif path == "/api/tenders/publish":
                result = self.service.publish_tender(actor, role, **data)
            elif path == "/api/bids":
                result = self.service.submit_bid(actor, role, **data)
            elif path == "/api/bids/withdraw":
                result = self.service.withdraw_bid(actor, role, **data)
            elif path == "/api/tenders/open":
                result = self.service.open_bids(actor, role, **data)
            elif path == "/api/conflicts":
                result = self.service.declare_conflict(actor, role, **data)
            elif path == "/api/evaluations":
                result = self.service.evaluate_bid(actor, role, **data)
            elif path == "/api/bids/disqualify":
                result = self.service.disqualify_bid(actor, role, **data)
            elif path == "/api/clarifications":
                result = self.service.ask_clarification(actor, role, **data)
            elif path == "/api/clarifications/answer":
                result = self.service.answer_clarification(actor, role, **data)
            elif path == "/api/complaints":
                result = self.service.submit_complaint(actor, role, **data)
            elif path == "/api/complaints/resolve":
                result = self.service.resolve_complaint(actor, role, **data)
            elif path == "/api/tenders/award":
                result = self.service.award_tender(actor, role, **data)
            elif path == "/api/chain/repair":
                result = self.service.repair_chain(actor, role)
            else:
                raise DomainError("接口不存在", 404)
            self._send(201, result)
        except DomainError as exc:
            self._send(exc.status, {"error": str(exc)})
        except (KeyError, TypeError, ValueError) as exc:
            self._send(400, {"error": "请求参数错误: %s" % exc})
        except Exception as exc:
            self._send(500, {"error": "服务器内部错误", "detail": str(exc)})

    def log_message(self, fmt: str, *args: Any) -> None:
        return


def serve(service: ProcurementService, host: str, port: int) -> None:
    ApiHandler.service = service
    server = ThreadingHTTPServer((host, port), ApiHandler)
    print("Public procurement service listening on http://%s:%s" % (host, port))
    server.serve_forever()


def main() -> None:
    parser = argparse.ArgumentParser(description="公共采购密封投标服务")
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8209)
    parser.add_argument("--init", action="store_true")
    parser.add_argument("--seed", action="store_true")
    args = parser.parse_args()
    service = ProcurementService(args.db)
    if args.init:
        print(json.dumps(service.seed_demo() if args.seed else {"initialized": True, "db": args.db}, ensure_ascii=False))
        return
    serve(service, args.host, args.port)


if __name__ == "__main__":
    main()
