"""Sealed public-procurement tendering and evaluation service.

评标留痕：每次评分连同采购项目、投标与评分批次写入连续摘要链（score_chain），
作废、投诉重评、授标同样入链，审计人员可逐节点重算哈希验证完整性。
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from urllib.parse import parse_qs, urlparse

ROOT = Path(__file__).resolve().parent
DEFAULT_DB = ROOT / "public_procurement.db"

GENESIS_HASH = "0" * 64
# 待核旧记录（例如评标行找不到所属投标/项目）挂在 0 号占位链上
PENDING_CHAIN_TENDER_ID = 0


class DomainError(Exception):
    def __init__(self, message: str, status: int = 400):
        super().__init__(message)
        self.status = status


class _RetryChainSeal(Exception):
    """内部信号：前序链节点尚未封口，让出写锁后重试。"""


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


def link_hash(prev_hash: str, seq: int, payload_hash: str) -> str:
    raw = ("%s|%d|%s" % (prev_hash, seq, payload_hash)).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


class ProcurementService:
    def __init__(self, db_path: str | os.PathLike[str] = DEFAULT_DB):
        self.db_path = str(db_path)
        # 测试钩子：评分 / 投诉处理在事务提交前触发，用于制造确定性并发交错
        self.before_evaluate_commit = None
        self.before_resolve_commit = None
        self._init_schema()
        self._migrate_legacy()
        self.recovery_report = self._recover_unsealed_nodes()

    # ------------------------------------------------------------------ schema

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
                    invalidation_revision INTEGER NOT NULL DEFAULT 0,
                    score_commit_counter INTEGER NOT NULL DEFAULT 0,
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
                    status TEXT NOT NULL DEFAULT 'active',
                    void_reason TEXT NOT NULL DEFAULT '',
                    version INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT,
                    voided_at TEXT,
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
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tender_id INTEGER NOT NULL,
                    seq INTEGER NOT NULL,
                    kind TEXT NOT NULL,
                    bid_id INTEGER,
                    evaluation_round INTEGER,
                    evaluator TEXT,
                    eval_ids TEXT NOT NULL DEFAULT '[]',
                    payload TEXT NOT NULL,
                    payload_hash TEXT NOT NULL DEFAULT '',
                    prev_hash TEXT NOT NULL,
                    node_hash TEXT,
                    node_status TEXT NOT NULL DEFAULT 'writing',
                    source TEXT NOT NULL DEFAULT 'live',
                    verified TEXT NOT NULL DEFAULT 'ok',
                    note TEXT NOT NULL DEFAULT '',
                    created_by TEXT,
                    created_at TEXT NOT NULL,
                    sealed_at TEXT,
                    UNIQUE(tender_id, seq)
                );
                CREATE TABLE IF NOT EXISTS schema_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_bids_tender ON bids(tender_id,status);
                CREATE INDEX IF NOT EXISTS idx_eval_bid_round ON evaluations(bid_id,evaluation_round);
                CREATE INDEX IF NOT EXISTS idx_chain_tender ON score_chain(tender_id,seq);
                """
            )

    def _columns(self, conn: sqlite3.Connection, table: str) -> set[str]:
        return {row["name"] for row in conn.execute("PRAGMA table_info(%s)" % table).fetchall()}

    def _ensure_column(self, conn: sqlite3.Connection, table: str, column: str, decl: str) -> None:
        if column not in self._columns(conn, table):
            conn.execute("ALTER TABLE %s ADD COLUMN %s %s" % (table, column, decl))

    def _migrate_legacy(self) -> None:
        """旧库升级：补列 + 按现有主键给历史评分补链（只追加，不改原值）。"""
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._ensure_column(conn, "tenders", "invalidation_revision", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(conn, "tenders", "score_commit_counter", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(conn, "evaluations", "status", "TEXT NOT NULL DEFAULT 'active'")
            self._ensure_column(conn, "evaluations", "void_reason", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(conn, "evaluations", "voided_at", "TEXT")
            flag = conn.execute("SELECT value FROM schema_meta WHERE key='chain_backfill_v1'").fetchone()
            if flag is None:
                self._backfill_chain(conn)
                conn.execute(
                    "INSERT OR REPLACE INTO schema_meta(key,value) VALUES('chain_backfill_v1',?)",
                    (utcnow(),),
                )

    # ------------------------------------------------------------------ chain

    def _backfill_chain(self, conn: sqlite3.Connection) -> None:
        """按 evaluations 现有主键顺序重建历史摘要链；找不到归属的记录标待核。"""
        rows = conn.execute(
            """SELECT e.*, b.tender_id AS owner_tender
                 FROM evaluations e
                 LEFT JOIN bids b ON b.id = e.bid_id
             ORDER BY e.id"""
        ).fetchall()
        # tender_id -> 已存在节点数（升级中断后重跑：已有链的项目不动）
        existing = {
            row["tender_id"]: row["c"]
            for row in conn.execute("SELECT tender_id, COUNT(*) AS c FROM score_chain GROUP BY tender_id")
        }
        groups: dict[int, list[sqlite3.Row]] = {}
        order: list[int] = []
        for row in rows:
            owner = row["owner_tender"]
            if owner is None:
                # 找不到投标归属：整条待核，不覆盖 evaluation 原值
                owner = PENDING_CHAIN_TENDER_ID
            if existing.get(owner, 0):
                continue
            groups.setdefault(owner, []).append(row)
            if owner not in order:
                order.append(owner)
        for owner in order:
            batch: dict[tuple, list[sqlite3.Row]] = {}
            batch_order: list[tuple] = []
            for row in groups[owner]:
                key = (row["bid_id"], row["evaluation_round"], row["evaluator"])
                if key not in batch:
                    batch[key] = []
                    batch_order.append(key)
                batch[key].append(row)
            batch_order.sort(key=lambda k: (k[1], k[0] if k[0] is not None else -1, k[2]))
            for bid_id, round_no, evaluator in batch_order:
                items = [
                    {
                        "evaluation_id": row["id"],
                        "criterion": row["criterion"],
                        "raw_value": row["raw_value"],
                        "score": row["score"],
                    }
                    for row in sorted(batch[(bid_id, round_no, evaluator)], key=lambda r: r["id"])
                ]
                orphan = owner == PENDING_CHAIN_TENDER_ID
                payload = {
                    "kind": "score",
                    "tender_id": owner if not orphan else None,
                    "bid_id": bid_id,
                    "evaluation_round": round_no,
                    "evaluator": evaluator,
                    "items": items,
                }
                node = self._append_node(
                    conn, owner, "score", payload,
                    bid_id=bid_id, round_no=round_no, evaluator=evaluator,
                    eval_ids=[item["evaluation_id"] for item in items],
                    source="backfill",
                    verified="pending" if orphan else "ok",
                    note="legacy-orphan-pending" if orphan else "legacy-backfill",
                    actor="migration", created_at=batch[(bid_id, round_no, evaluator)][0]["created_at"],
                )
                conn.execute(
                    "UPDATE score_chain SET node_status='sealed', node_hash=?, sealed_at=? WHERE id=?",
                    (self._seal_hash(node), node["created_at"], node["id"]),
                )

    def _last_node(self, conn: sqlite3.Connection, tender_id: int) -> sqlite3.Row | None:
        return conn.execute(
            "SELECT * FROM score_chain WHERE tender_id=? ORDER BY seq DESC LIMIT 1", (tender_id,)
        ).fetchone()

    def _append_node(self, conn: sqlite3.Connection, tender_id: int, kind: str, payload: dict[str, Any],
                     *, bid_id: int | None = None, round_no: int | None = None, evaluator: str | None = None,
                     eval_ids: list[int] | None = None, source: str = "live", verified: str = "ok",
                     note: str = "", actor: str | None = None, created_at: str | None = None,
                     seal: bool = False) -> sqlite3.Row:
        last = self._last_node(conn, tender_id)
        seq = (last["seq"] + 1) if last else 1
        prev_hash = last["node_hash"] if last and last["node_hash"] else GENESIS_HASH
        payload_text = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(payload_text.encode("utf-8")).hexdigest()
        now = created_at or utcnow()
        node_hash = link_hash(prev_hash, seq, digest) if seal else None
        cur = conn.execute(
            """INSERT INTO score_chain(tender_id,seq,kind,bid_id,evaluation_round,evaluator,eval_ids,
                   payload,payload_hash,prev_hash,node_hash,node_status,source,verified,note,created_by,created_at,sealed_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (tender_id, seq, kind, bid_id, round_no, evaluator, json.dumps(eval_ids or []),
             payload_text, digest, prev_hash, node_hash, "sealed" if seal else "writing",
             source, verified, note, actor, now, now if seal else None),
        )
        node = conn.execute("SELECT * FROM score_chain WHERE id=?", (cur.lastrowid,)).fetchone()
        return dict(node)

    @staticmethod
    def _seal_hash(node: sqlite3.Row | SimpleNamespace) -> str:
        return link_hash(node["prev_hash"], node["seq"], node["payload_hash"])

    def _seal_node(self, conn: sqlite3.Connection, node_id: int, *, status: str,
                   eval_ids: list[int] | None = None, verified: str = "ok", note: str = "",
                   wait_predecessor: bool = False) -> None:
        node = conn.execute("SELECT * FROM score_chain WHERE id=?", (node_id,)).fetchone()
        # 并发时前序可能出现更晚封口的相邻节点：封口前重新锚定 prev_hash，保证链不断
        writing = conn.execute(
            "SELECT COUNT(*) AS c FROM score_chain WHERE tender_id=? AND seq<? AND node_status='writing'",
            (node["tender_id"], node["seq"]),
        ).fetchone()["c"]
        if writing:
            if wait_predecessor:
                raise _RetryChainSeal()
            # 恢复路径：前序仍有半截节点，本节点也保持断点
            conn.execute(
                "UPDATE score_chain SET node_status='broken',verified='pending',note=? WHERE id=?",
                ("recovery-predecessor-unsealed", node_id),
            )
            return
        if eval_ids is not None:
            conn.execute("UPDATE score_chain SET eval_ids=? WHERE id=?", (json.dumps(eval_ids), node_id))
        predecessor = conn.execute(
            "SELECT node_hash,prev_hash FROM score_chain WHERE tender_id=? AND seq<? AND node_hash IS NOT NULL ORDER BY seq DESC LIMIT 1",
            (node["tender_id"], node["seq"]),
        ).fetchone()
        prev_hash = predecessor["node_hash"] if predecessor else GENESIS_HASH
        node_hash = link_hash(prev_hash, node["seq"], node["payload_hash"])
        conn.execute(
            """UPDATE score_chain SET prev_hash=?,node_status=?,node_hash=?,verified=?,note=?,sealed_at=? WHERE id=?""",
            (prev_hash, status, node_hash, verified, note or node["note"], utcnow(), node_id),
        )

    def _abort_writing_node(self, node_id: int, note: str) -> None:
        """把一个 writing 占位节点封成 aborted 墓碑；前序未封口时带界等待。"""
        for _ in range(50):
            try:
                with self.connect() as conn:
                    conn.execute("BEGIN IMMEDIATE")
                    pending = conn.execute(
                        "SELECT * FROM score_chain WHERE id=? AND node_status='writing'", (node_id,)
                    ).fetchone()
                    if not pending:
                        return
                    self._seal_node(conn, node_id, status="aborted", note=note, wait_predecessor=True)
                return
            except _RetryChainSeal:
                time.sleep(0.02)
        # 等待超时：保持 writing 断点，由重启恢复或人工修复处理

    def _recover_unsealed_nodes(self) -> dict[str, Any]:
        """重启恢复：找出上次只写了一半的链节点，能判定的自动修复，不能判定的留断阻断授标。"""
        report: dict[str, Any] = {"repaired": [], "aborted": [], "broken": []}
        with self.connect() as conn:
            tender_ids = [
                row["tender_id"]
                for row in conn.execute(
                    "SELECT DISTINCT tender_id FROM score_chain WHERE node_status!='sealed' ORDER BY tender_id"
                )
            ]
        for tender_id in tender_ids:
            with self.connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                result = self._recover_tender(conn, tender_id)
                for key in report:
                    report[key].extend(result[key])
        return report

    def _recover_tender(self, conn: sqlite3.Connection, tender_id: int) -> dict[str, list]:
        result: dict[str, list] = {"repaired": [], "aborted": [], "broken": []}
        nodes = conn.execute(
            "SELECT * FROM score_chain WHERE tender_id=? AND node_status!='sealed' ORDER BY seq",
            (tender_id,),
        ).fetchall()
        for node in nodes:
            if node["kind"] != "score":
                # 事件节点理论上与业务事务同生共死；无法判定，保持断点
                result["broken"].append({"tender_id": tender_id, "seq": node["seq"], "reason": "事件节点未完成"})
                conn.execute("UPDATE score_chain SET node_status='broken', verified='pending' WHERE id=?", (node["id"],))
                continue
            payload = json.loads(node["payload"])
            expected = {(item["criterion"]): item for item in payload.get("items", [])}
            rows = conn.execute(
                """SELECT e.* FROM evaluations e JOIN bids b ON b.id=e.bid_id
                    WHERE b.tender_id=? AND e.bid_id=? AND e.evaluation_round=? AND e.evaluator=?
                    ORDER BY e.id""",
                (tender_id, node["bid_id"], node["evaluation_round"], node["evaluator"]),
            ).fetchall()
            matched = [r for r in rows if r["criterion"] in expected
                       and abs(r["raw_value"] - expected[r["criterion"]]["raw_value"]) < 1e-9
                       and abs(r["score"] - expected[r["criterion"]]["score"]) < 1e-9]
            if not rows:
                # 评分事务未提交：节点封成 aborted 墓碑，链保持连续，评分需重走
                self._seal_node(conn, node["id"], status="aborted", note=(node["note"] or "recovery-aborted"))
                result["aborted"].append({"tender_id": tender_id, "seq": node["seq"]})
            elif len(matched) == len(expected) and len(rows) == len(expected):
                self._seal_node(conn, node["id"], status="repaired", eval_ids=[r["id"] for r in matched],
                                note=(node["note"] or "recovery-repaired"))
                result["repaired"].append({"tender_id": tender_id, "seq": node["seq"],
                                           "eval_ids": [r["id"] for r in matched]})
            else:
                # 只写进去一部分：无法确认意图，标记待核并留断，阻断授标
                conn.execute(
                    "UPDATE score_chain SET node_status='broken', verified='pending', note=? WHERE id=?",
                    ("recovery-partial-write", node["id"]),
                )
                result["broken"].append({"tender_id": tender_id, "seq": node["seq"],
                                         "reason": "评分行只写入一部分，需人工核验"})
        return result

    def verify_chain(self, tender_id: int) -> dict[str, Any]:
        """逐节点重算哈希，返回校验状态与第一个断点位置。"""
        with self.connect() as conn:
            return self._verify_tender(conn, tender_id)

    def _verify_tender(self, conn: sqlite3.Connection, tender_id: int) -> dict[str, Any]:
        nodes = conn.execute(
            "SELECT * FROM score_chain WHERE tender_id=? ORDER BY seq", (tender_id,)
        ).fetchall()
        prev_hash = GENESIS_HASH
        expect_seq = 1
        breakpoint: dict[str, Any] | None = None
        for node in nodes:
            if breakpoint is None:
                payload = json.loads(node["payload"])
                payload_hash = hashlib.sha256(
                    json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
                ).hexdigest()
                reasons = []
                if node["seq"] != expect_seq:
                    reasons.append("节点序号不连续")
                if node["prev_hash"] != prev_hash:
                    reasons.append("前序摘要不匹配")
                if node["payload_hash"] != payload_hash:
                    reasons.append("节点内容与摘要不符")
                if node["node_hash"] is None:
                    reasons.append("节点未封口（疑似写入中断）")
                elif link_hash(node["prev_hash"], node["seq"], node["payload_hash"]) != node["node_hash"]:
                    reasons.append("链路摘要校验失败")
                if node["node_status"] == "broken":
                    reasons.append("节点已标记损坏")
                if node["verified"] == "pending" and not reasons:
                    reasons.append("记录待核")
                if reasons:
                    breakpoint = {"seq": node["seq"], "kind": node["kind"],
                                  "node_status": node["node_status"], "reasons": reasons}
            expect_seq = node["seq"] + 1
            if node["node_hash"]:
                prev_hash = node["node_hash"]
        return self._summarize(tender_id, nodes, breakpoint)

    def _summarize(self, tender_id: int, nodes: list[sqlite3.Row],
                   breakpoint: dict[str, Any] | None) -> dict[str, Any]:
        rounds_map: dict[int, dict[str, Any]] = {}
        counts = {"score": 0, "void": 0, "round_reset": 0, "award": 0,
                  "repaired": 0, "aborted": 0, "pending": 0}
        for node in nodes:
            counts[node["kind"]] = counts.get(node["kind"], 0) + 1
            if node["node_status"] == "repaired":
                counts["repaired"] += 1
            if node["node_status"] == "aborted":
                counts["aborted"] += 1
            if node["verified"] == "pending":
                counts["pending"] += 1
            round_no = node["evaluation_round"]
            if round_no is None:
                continue
            item = rounds_map.setdefault(round_no, {"round": round_no, "nodes": 0, "score_nodes": 0,
                                                    "events": {}, "status": "完整"})
            item["nodes"] += 1
            if node["kind"] == "score" and node["node_status"] in {"sealed", "repaired"}:
                item["score_nodes"] += 1
            if node["kind"] != "score":
                item["events"][node["kind"]] = item["events"].get(node["kind"], 0) + 1
        for round_no, item in rounds_map.items():
            if breakpoint and any(n["evaluation_round"] == round_no and n["seq"] == breakpoint["seq"]
                                  for n in nodes):
                item["status"] = "断裂"
            elif any(n["evaluation_round"] == round_no and n["verified"] == "pending" for n in nodes):
                item["status"] = "待核"
        last = nodes[-1] if nodes else None
        return {
            "tender_id": tender_id,
            "verified": breakpoint is None,
            "nodes": len(nodes),
            "counts": counts,
            "rounds": [rounds_map[k] for k in sorted(rounds_map)],
            "breakpoint": breakpoint,
            "last_seq": last["seq"] if last else 0,
            "last_hash": (last["node_hash"][:16] + "…") if last and last["node_hash"] else None,
        }

    def _chain_overview(self, conn: sqlite3.Connection) -> list[dict[str, Any]]:
        grouped: dict[int, list[sqlite3.Row]] = {}
        for row in conn.execute("SELECT * FROM score_chain ORDER BY tender_id,seq").fetchall():
            grouped.setdefault(row["tender_id"], []).append(row)
        result = []
        for tender_id, nodes in grouped.items():
            prev_hash = GENESIS_HASH
            expect_seq = 1
            breakpoint = None
            for node in nodes:
                if breakpoint is None:
                    reasons = []
                    if node["seq"] != expect_seq:
                        reasons.append("节点序号不连续")
                    if node["prev_hash"] != prev_hash:
                        reasons.append("前序摘要不匹配")
                    if node["node_hash"] is None or link_hash(
                        node["prev_hash"], node["seq"], node["payload_hash"]
                    ) != node["node_hash"]:
                        reasons.append("链路摘要校验失败")
                    if node["node_status"] == "broken":
                        reasons.append("节点已标记损坏")
                    if node["verified"] == "pending" and not reasons:
                        reasons.append("记录待核")
                    if reasons:
                        breakpoint = {"seq": node["seq"], "kind": node["kind"],
                                      "node_status": node["node_status"], "reasons": reasons}
                expect_seq = node["seq"] + 1
                if node["node_hash"]:
                    prev_hash = node["node_hash"]
            summary = self._summarize(tender_id, nodes, breakpoint)
            if tender_id == PENDING_CHAIN_TENDER_ID:
                summary["tender_no"] = "（待核记录）"
                summary["title"] = "无法确认归属的历史评分"
            else:
                tender = conn.execute("SELECT tender_no,title FROM tenders WHERE id=?", (tender_id,)).fetchone()
                if tender:
                    summary["tender_no"] = tender["tender_no"]
                    summary["title"] = tender["title"]
            result.append(summary)
        return result

    def repair_chain(self, actor: str, role: str, tender_id: int) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"supervisor"}, "修复摘要链")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._tender(conn, tender_id)
            recovered = self._recover_tender(conn, tender_id)
            report = self._verify_tender(conn, tender_id)
            self._audit(conn, tender_id, actor, "chain.repaired",
                        {"recovered": recovered, "verified": report["verified"]})
        return {"recovered": recovered, "chain": report}

    # ------------------------------------------------------------ concurrency

    def _read_basis(self, tender_id: int, expected_round: int | None = None,
                    expected_invalidation_revision: int | None = None,
                    expected_score_commit_counter: int | None = None) -> SimpleNamespace:
        """读取调用方基线：传了的字段以调用方快照为准（用于乐观并发），未传则取当前值。"""
        with self.connect() as conn:
            row = conn.execute(
                "SELECT evaluation_round,invalidation_revision,score_commit_counter FROM tenders WHERE id=?",
                (tender_id,),
            ).fetchone()
        if not row:
            raise DomainError("采购项目不存在", 404)
        return SimpleNamespace(
            round=(row["evaluation_round"] if expected_round is None else int(expected_round)),
            invalidation_revision=(row["invalidation_revision"]
                                   if expected_invalidation_revision is None
                                   else int(expected_invalidation_revision)),
            score_commit_counter=(row["score_commit_counter"]
                                  if expected_score_commit_counter is None
                                  else int(expected_score_commit_counter)),
        )

    def _check_round(self, tender: sqlite3.Row, basis: SimpleNamespace) -> None:
        if tender["evaluation_round"] != basis.round:
            raise DomainError("评分批次已因投诉重评作废，请按新一轮重新提交", 409)

    def _check_basis(self, tender: sqlite3.Row, basis: SimpleNamespace, check_counter: bool = False) -> None:
        self._check_round(tender, basis)
        if tender["invalidation_revision"] != basis.invalidation_revision:
            raise DomainError("评分批次已作废，请刷新批次基线后重新评分", 409)
        if check_counter and tender["score_commit_counter"] != basis.score_commit_counter:
            raise DomainError("评分先于投诉处理提交，投诉处理须以最新批次重试（409）", 409)

    # ----------------------------------------------------------------- audit

    def _audit(self, conn: sqlite3.Connection, tender_id: int | None, actor: str,
               action: str, details: dict[str, Any]) -> None:
        conn.execute(
            "INSERT INTO timeline(tender_id,actor,action,details,created_at) VALUES(?,?,?,?,?)",
            (tender_id, actor, action, json.dumps(details, ensure_ascii=False, sort_keys=True), utcnow()),
        )

    def _tender(self, conn: sqlite3.Connection, tender_id: int) -> sqlite3.Row:
        row = conn.execute("SELECT * FROM tenders WHERE id=?", (tender_id,)).fetchone()
        if not row:
            raise DomainError("采购项目不存在", 404)
        return row

    # ------------------------------------------------------------- tenders etc

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
            conflict = dict(conn.execute("SELECT * FROM conflicts WHERE id=?", (cur.lastrowid,)).fetchone())
            frozen = tender["status"] in {"awarded", "cancelled"}
            voided_ids: list[int] = []
            if not frozen:
                # 受理利益冲突：该评审人尚未授标的评分立即作废（可按供应商缩小范围）
                query = [
                    "SELECT e.id FROM evaluations e JOIN bids b ON b.id=e.bid_id",
                    "WHERE b.tender_id=? AND e.evaluation_round=? AND e.evaluator=? AND e.status='active'",
                ]
                params: list[Any] = [tender_id, tender["evaluation_round"], evaluator.strip()]
                if vendor_id is not None:
                    query.append("AND b.vendor_id=?")
                    params.append(vendor_id)
                voided_ids = [r["id"] for r in conn.execute(" ".join(query), params).fetchall()]
                now = utcnow()
                if voided_ids:
                    conn.execute(
                        """UPDATE evaluations SET status='voided',void_reason=?,voided_at=? WHERE id IN (%s)"""
                        % ",".join("?" * len(voided_ids)),
                        ["conflict:%s" % reason.strip(), now, *voided_ids],
                    )
                    self._append_node(
                        conn, tender_id, "void",
                        {"kind": "void", "tender_id": tender_id, "evaluation_round": tender["evaluation_round"],
                         "evaluator": evaluator.strip(), "vendor_id": vendor_id, "reason": reason.strip(),
                         "trigger": "conflict", "voided_eval_ids": voided_ids},
                        round_no=tender["evaluation_round"], evaluator=evaluator.strip(),
                        eval_ids=voided_ids, actor=actor, seal=True,
                    )
                conn.execute("UPDATE tenders SET updated_at=? WHERE id=?", (now, tender_id))
            self._audit(conn, tender_id, actor, "conflict.declared",
                        {"evaluator": evaluator.strip(), "vendor_id": vendor_id,
                         "reason": reason.strip(), "voided": voided_ids, "frozen": frozen})
            return {"conflict": conflict, "frozen": frozen, "voided_evaluation_ids": voided_ids}

    def evaluate_bid(self, actor: str, role: str, bid_id: int, values: dict[str, float],
                     comment: str = "", expected_round: int | None = None,
                     expected_invalidation_revision: int | None = None) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"evaluator"}, "评分")
        if not isinstance(values, dict) or not values:
            raise DomainError("评分内容必须是对象")
        # 加锁前先冻结调用方基线（评分批次号等）：并发的投诉受理若先提交，旧基线在第二阶段被判 409
        with self.connect() as pre:
            ref = pre.execute(
                """SELECT b.id AS bid_id,b.tender_id AS tender_id,b.vendor_id AS vendor_id
                     FROM bids b JOIN tenders t ON t.id=b.tender_id WHERE b.id=?""",
                (bid_id,),
            ).fetchone()
        if ref is None:
            # 留给事务内返回 404
            basis = SimpleNamespace(round=expected_round or -1,
                                    invalidation_revision=expected_invalidation_revision or 0,
                                    score_commit_counter=0)
            tender_id = None
            vendor_id = None
        else:
            tender_id = ref["tender_id"]
            vendor_id = ref["vendor_id"]
            basis = self._read_basis(tender_id, expected_round, expected_invalidation_revision)

        node_id: int | None = None
        # 第一阶段：校验并占位一个 writing 链节点（崩溃后可被重启恢复发现）
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            bid = conn.execute("SELECT * FROM bids WHERE id=?", (bid_id,)).fetchone()
            if not bid:
                raise DomainError("投标不存在", 404)
            tender = self._tender(conn, bid["tender_id"])
            tender_id = tender["id"]
            if tender["status"] not in {"opened", "reevaluation"} or tender["evaluations_locked"]:
                raise DomainError("当前项目不能评分", 409)
            if bid["status"] not in {"opened", "qualified"}:
                raise DomainError("该投标不能评分", 409)
            self._check_round(tender, basis)
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
            items = []
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
                    """SELECT id FROM evaluations WHERE bid_id=? AND evaluation_round=? AND evaluator=? AND criterion=?
                       AND status='active'""",
                    (bid_id, tender["evaluation_round"], actor, criterion["name"]),
                ).fetchone()
                if existing:
                    raise DomainError("该评分项已提交，不能覆盖", 409)
                items.append({"criterion": criterion["name"], "raw_value": raw,
                              "score": round(score, 6), "max_value": criterion["max_value"],
                              "kind": criterion["kind"]})
            now = utcnow()
            payload = {
                "kind": "score", "tender_id": tender["id"], "bid_id": bid_id,
                "evaluation_round": tender["evaluation_round"], "evaluator": actor,
                "comment": comment.strip(), "items": items, "submitted_at": now,
            }
            node = self._append_node(
                conn, tender["id"], "score", payload, bid_id=bid_id,
                round_no=tender["evaluation_round"], evaluator=actor, actor=actor, created_at=now,
            )
            node_id = node["id"]

        # 第二阶段：写评分行并封口；任一步失败都留下 writing 节点，由恢复流程处理。
        # 前序相邻节点可能还在封口途中（并发评分同一项目），让出写锁后带界重试。
        created: list[dict[str, Any]] = []
        failure: DomainError | None = None
        for attempt in range(50):
            try:
                with self.connect() as conn:
                    conn.execute("BEGIN IMMEDIATE")
                    tender = self._tender(conn, tender_id)
                    self._check_round(tender, basis)
                    # 两阶段之间可能有利益冲突受理：再查一次，冲突评审人不能落分
                    conflict = conn.execute(
                        "SELECT 1 FROM conflicts WHERE tender_id=? AND evaluator=? AND (vendor_id=? OR vendor_id IS NULL)",
                        (tender_id, actor, vendor_id),
                    ).fetchone()
                    if conflict:
                        raise DomainError("评审人与该供应商存在利益冲突", 403)
                    if self.before_evaluate_commit:
                        self.before_evaluate_commit()
                    created = []
                    now = utcnow()
                    for item in items:
                        try:
                            cur = conn.execute(
                                """INSERT INTO evaluations(bid_id,evaluation_round,evaluator,criterion,raw_value,score,comment,created_at,updated_at)
                                   VALUES(?,?,?,?,?,?,?,?,?)""",
                                (bid_id, tender["evaluation_round"], actor, item["criterion"],
                                 item["raw_value"], item["score"], comment.strip(), now, now),
                            )
                        except sqlite3.IntegrityError as exc:
                            raise DomainError("该评分项已提交，不能覆盖", 409) from exc
                        created.append(dict(conn.execute("SELECT * FROM evaluations WHERE id=?", (cur.lastrowid,)).fetchone()))
                    self._seal_node(conn, node_id, status="sealed",
                                    eval_ids=[row["id"] for row in created], wait_predecessor=True)
                    conn.execute(
                        "UPDATE tenders SET score_commit_counter=score_commit_counter+1 WHERE id=?", (tender_id,)
                    )
                    self._audit(conn, tender_id, actor, "bid.evaluated",
                                {"bid_id": bid_id, "round": tender["evaluation_round"],
                                 "chain_seq": node["seq"], "criteria": [row["criterion"] for row in created]})
                failure = None
                break
            except _RetryChainSeal:
                time.sleep(0.02)
                continue
            except DomainError as exc:
                failure = exc
                break
        if failure is not None:
            # 并发败诉：立刻把占位节点收尸成 aborted 墓碑，避免留断（语义等同事务回滚后的修复）
            self._abort_writing_node(node_id, "conflict-409-aborted")
            raise failure
        return {"bid_id": bid_id, "evaluator": actor, "round": tender["evaluation_round"],
                "chain_seq": node["seq"], "evaluations": created}

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
                          resolution: str, expected_round: int | None = None,
                          expected_invalidation_revision: int | None = None,
                          expected_score_commit_counter: int | None = None) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"supervisor"}, "处理投诉")
        if decision not in {"accepted", "rejected"} or not resolution.strip():
            raise DomainError("投诉决定或处理说明无效")
        # 加锁前基线：与评分并发时，谁先提交谁为准，后者 409
        with self.connect() as pre:
            ref = pre.execute("SELECT tender_id FROM complaints WHERE id=?", (complaint_id,)).fetchone()
        basis = (self._read_basis(ref["tender_id"], expected_round,
                                  expected_invalidation_revision,
                                  expected_score_commit_counter)
                 if ref else None)
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
            if decision == "accepted":
                tender = self._tender(conn, complaint["tender_id"])
                if tender["status"] in {"awarded", "cancelled"}:
                    raise DomainError("已结束项目不能重新评审", 409)
                if basis is not None:
                    self._check_basis(tender, basis, check_counter=True)
                if self.before_resolve_commit:
                    self.before_resolve_commit()
                now = utcnow()
                # 受理投诉：当前批次尚未授标的全部 active 评分作废，排名随后按新批次重算
                voided_ids = [
                    row["id"]
                    for row in conn.execute(
                        """SELECT e.id FROM evaluations e JOIN bids b ON b.id=e.bid_id
                            WHERE b.tender_id=? AND e.evaluation_round=? AND e.status='active'""",
                        (tender["id"], tender["evaluation_round"]),
                    ).fetchall()
                ]
                if voided_ids:
                    conn.execute(
                        """UPDATE evaluations SET status='voided',void_reason=?,voided_at=?
                           WHERE id IN (%s)""" % ",".join("?" * len(voided_ids)),
                        ["complaint:%s" % complaint_id, now, *voided_ids],
                    )
                self._append_node(
                    conn, tender["id"], "void",
                    {"kind": "void", "tender_id": tender["id"], "evaluation_round": tender["evaluation_round"],
                     "trigger": "complaint", "complaint_id": complaint_id, "voided_eval_ids": voided_ids},
                    round_no=tender["evaluation_round"], actor=actor, eval_ids=voided_ids, seal=True,
                )
                old_round = tender["evaluation_round"]
                self._append_node(
                    conn, tender["id"], "round_reset",
                    {"kind": "round_reset", "tender_id": tender["id"], "old_round": old_round,
                     "new_round": old_round + 1, "complaint_id": complaint_id, "resolution": resolution.strip()},
                    round_no=old_round + 1, actor=actor, seal=True,
                )
                conn.execute(
                    """UPDATE tenders SET status='reevaluation',evaluation_round=evaluation_round+1,
                           evaluations_locked=0,invalidation_revision=invalidation_revision+1,
                           version=version+1,updated_at=? WHERE id=?""",
                    (now, tender["id"]),
                )
            self._audit(conn, complaint["tender_id"], actor, "complaint.resolved",
                        {"complaint_id": complaint_id, "decision": decision})
            return dict(conn.execute("SELECT * FROM complaints WHERE id=?", (complaint_id,)).fetchone())

    def _current_ranking(self, conn: sqlite3.Connection, tender: sqlite3.Row) -> dict[str, Any]:
        bids = conn.execute(
            "SELECT * FROM bids WHERE tender_id=? AND status IN ('opened','qualified','awarded') ORDER BY id",
            (tender["id"],),
        ).fetchall()
        criteria = json.loads(tender["criteria"])
        expected_criteria = {c["name"] for c in criteria}
        ranking = []
        complete = True
        for bid in bids:
            rows = conn.execute(
                """SELECT criterion,AVG(score) AS score FROM evaluations
                    WHERE bid_id=? AND evaluation_round=? AND status='active' GROUP BY criterion""",
                (bid["id"], tender["evaluation_round"]),
            ).fetchall()
            scores = {row["criterion"]: row["score"] for row in rows}
            if set(scores) != expected_criteria:
                complete = False
                continue
            weighted = 0.0
            for criterion in criteria:
                weighted += scores[criterion["name"]] * criterion["weight"] / 100
            ranking.append({"bid_id": bid["id"], "vendor_id": bid["vendor_id"],
                            "price": bid["price"], "score": round(weighted, 2)})
        ranking.sort(key=lambda item: (-item["score"], item["price"], item["bid_id"]))
        return {"round": tender["evaluation_round"], "complete": complete,
                "voided_excluded": True, "ranking": ranking}

    def award_tender(self, actor: str, role: str, tender_id: int, expected_version: int) -> dict[str, Any]:
        actor = clean_actor(actor)
        require_role(role, {"supervisor"}, "授标")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            tender = self._tender(conn, tender_id)
            if tender["status"] not in {"opened", "reevaluation"}:
                raise DomainError("当前项目不能授标", 409)
            if tender["version"] != int(expected_version):
                raise DomainError("项目已变化，请刷新后重试", 409)
            chain = self._verify_tender(conn, tender_id)
            if not chain["verified"]:
                raise DomainError(
                    "评标摘要链存在断点（节点 %s：%s），须先修复才能授标"
                    % (chain["breakpoint"]["seq"], "、".join(chain["breakpoint"]["reasons"])), 409)
            open_complaint = conn.execute(
                "SELECT COUNT(*) AS c FROM complaints WHERE tender_id=? AND status='open'", (tender_id,)
            ).fetchone()["c"]
            if open_complaint:
                raise DomainError("存在未处理投诉，不能授标", 409)
            ranking_info = self._current_ranking(conn, tender)
            ranking = ranking_info["ranking"]
            if not ranking or not ranking_info["complete"]:
                raise DomainError("评分已作废或尚未完成全部评分，不能授标，请重算后再试", 409)
            winner = ranking[0]
            snapshot = {"tender_id": tender_id, "round": tender["evaluation_round"], "ranking": ranking,
                        "winner": winner, "awarded_by": actor, "awarded_at": utcnow()}
            snapshot_hash = canonical_hash(snapshot)
            self._append_node(
                conn, tender_id, "award",
                {"kind": "award", "tender_id": tender_id, "evaluation_round": tender["evaluation_round"],
                 "snapshot": snapshot, "snapshot_hash": snapshot_hash},
                round_no=tender["evaluation_round"], actor=actor, seal=True,
            )
            conn.execute(
                "UPDATE tenders SET status='awarded',awarded_bid_id=?,award_snapshot=?,evaluations_locked=1,version=version+1,updated_at=? WHERE id=? AND version=?",
                (winner["bid_id"], json.dumps(snapshot, ensure_ascii=False), utcnow(), tender_id, expected_version),
            )
            conn.execute("UPDATE bids SET status='awarded',version=version+1 WHERE id=?", (winner["bid_id"],))
            self._audit(conn, tender_id, actor, "tender.awarded",
                        {"winner": winner, "ranking": ranking, "snapshot_hash": snapshot_hash})
            return {"tender": dict(self._tender(conn, tender_id)), "award": snapshot,
                    "snapshot_hash": snapshot_hash}

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
            result = {"tender": tender, "bids": bids, "clarifications": clarifications,
                      "chain": self._verify_tender(conn, tender_id)}
            if role in {"procurement", "supervisor", "auditor", "evaluator"}:
                row = self._tender(conn, tender_id)
                result["ranking"] = self._current_ranking(conn, row)
            return result

    def state(self, actor: str = "", role: str = "public") -> dict[str, Any]:
        with self.connect() as conn:
            tenders = [dict(r) for r in conn.execute(
                "SELECT id,tender_no,title,description,status,deadline,evaluation_round,version,awarded_bid_id,created_at,updated_at FROM tenders ORDER BY id DESC"
            ).fetchall()]
            timeline = [dict(r) for r in conn.execute("SELECT * FROM timeline ORDER BY id DESC LIMIT 200").fetchall()]
            chain_overview = self._chain_overview(conn)
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
        return {"tenders": tenders, "bids": bids, "complaints": complaints, "timeline": timeline,
                "chain_status": chain_overview, "role": role}

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
            parsed = urlparse(self.path)
            path = parsed.path
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
            elif path == "/api/chain":
                query = parse_qs(parsed.query)
                if "tender_id" in query:
                    self._send(200, self.service.verify_chain(int(query["tender_id"][0])))
                else:
                    with self.service.connect() as conn:
                        self._send(200, {"chains": self.service._chain_overview(conn)})
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
            elif path == "/api/chain/verify":
                result = self.service.verify_chain(int(data["tender_id"]))
            elif path == "/api/chain/repair":
                result = self.service.repair_chain(actor, role, int(data["tender_id"]))
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
