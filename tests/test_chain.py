"""评标留痕：摘要链、作废重算、授标冻结、并发 409、旧数据补链与重启恢复。"""
import hashlib
import json
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import GENESIS_HASH, DomainError, ProcurementService, link_hash  # noqa: E402

CRITERIA = [
    {"name": "报价", "weight": 60, "kind": "cost", "max_value": 1000000},
    {"name": "质量", "weight": 40, "kind": "direct", "max_value": 100},
]


class ChainTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Path(self.tmp.name) / "chain.db"
        self.svc = ProcurementService(self.db)
        self.v1 = self.svc.create_vendor("p", "procurement", "V-1", "甲公司", "r1")
        self.v2 = self.svc.create_vendor("p", "procurement", "V-2", "乙公司", "r2")
        deadline = (datetime.now(timezone.utc) + timedelta(seconds=2)).isoformat()
        tender = self.svc.create_tender("p", "procurement", "T-1", "设备采购", deadline, CRITERIA)
        self.tender = self.svc.publish_tender("p", "procurement", tender["id"], tender["version"])
        self.bid1 = self.svc.submit_bid("vendor1", "vendor", self.tender["id"], self.v1["id"],
                                        {"报价": 800000, "质量": 90}, 800000)
        self.bid2 = self.svc.submit_bid("vendor2", "vendor", self.tender["id"], self.v2["id"],
                                        {"报价": 700000, "质量": 80}, 700000)
        time.sleep(2.1)
        self.opened = self.svc.open_bids("p", "procurement", self.tender["id"], self.tender["version"])
        self.tid = self.tender["id"]

    def tearDown(self):
        self.tmp.cleanup()

    def eval(self, actor, bid, price, quality):
        return self.svc.evaluate_bid(actor, "evaluator", bid["id"], {"报价": price, "质量": quality})

    def tender_row(self):
        with self.svc.connect() as conn:
            return conn.execute("SELECT * FROM tenders WHERE id=?", (self.tid,)).fetchone()

    # ---------------------------------------------------------------- 链完整

    def test_every_evaluation_appends_verifiable_chain_node(self):
        r1 = self.eval("eval1", self.bid1, 800000, 90)
        r2 = self.eval("eval1", self.bid2, 700000, 80)
        self.assertEqual(1, r1["chain_seq"])
        self.assertEqual(2, r2["chain_seq"])
        chain = self.svc.verify_chain(self.tid)
        self.assertTrue(chain["verified"])
        self.assertEqual(2, chain["nodes"])
        self.assertIsNone(chain["breakpoint"])
        rounds = {r["round"]: r for r in chain["rounds"]}
        self.assertEqual("完整", rounds[1]["status"])
        self.assertEqual(2, rounds[1]["score_nodes"])
        # 节点必须携带项目、投标与评分批次
        with self.svc.connect() as conn:
            node = conn.execute("SELECT * FROM score_chain WHERE tender_id=? AND seq=1", (self.tid,)).fetchone()
        payload = json.loads(node["payload"])
        self.assertEqual(self.tid, payload["tender_id"])
        self.assertEqual(self.bid1["id"], payload["bid_id"])
        self.assertEqual(1, payload["evaluation_round"])
        self.assertEqual("eval1", payload["evaluator"])
        self.assertEqual({"报价", "质量"}, {i["criterion"] for i in payload["items"]})
        # 独立重算哈希也应通过
        self.assertEqual(link_hash(GENESIS_HASH, 1, node["payload_hash"]), node["node_hash"])

    def test_auditor_can_verify_and_unprivileged_cannot_repair(self):
        self.eval("eval1", self.bid1, 800000, 90)
        chain = self.svc.verify_chain(self.tid)
        self.assertTrue(chain["verified"])
        with self.assertRaises(DomainError) as ctx:
            self.svc.repair_chain("aud1", "auditor", self.tid)
        # auditor 只读；supervisor 可触发修复（无断点时原样通过）
        self.assertEqual(403, ctx.exception.status)
        report = self.svc.repair_chain("sup1", "supervisor", self.tid)
        self.assertTrue(report["chain"]["verified"])

    # ------------------------------------------------------------ 作废与重算

    def test_conflict_voids_unawarded_scores_and_ranking_excludes_them(self):
        self.eval("eval1", self.bid1, 800000, 90)
        self.eval("eval1", self.bid2, 700000, 80)
        result = self.svc.declare_conflict("eval1", "evaluator", self.tid, "eval1",
                                           None, "评审人独立性受质疑，全部评分回避")
        # 两次评分各含两个评分项，共 4 行评分全部作废
        self.assertEqual([1, 2, 3, 4], sorted(result["voided_evaluation_ids"]))
        self.assertFalse(result["frozen"])
        # 评分原值保留但失效，链上多了 void 事件
        with self.svc.connect() as conn:
            statuses = [r["status"] for r in conn.execute(
                "SELECT status FROM evaluations ORDER BY id").fetchall()]
            kinds = [r["kind"] for r in conn.execute(
                "SELECT kind FROM score_chain WHERE tender_id=?", (self.tid,)).fetchall()]
        self.assertEqual(["voided"] * 4, statuses)
        self.assertIn("void", kinds)
        chain = self.svc.verify_chain(self.tid)
        self.assertTrue(chain["verified"])
        # 排名按有效评分重算：缺分的投标不进入排名，授标被阻断
        ranking = self.svc.get_tender("sup1", "supervisor", self.tid)["ranking"]
        self.assertFalse(ranking["complete"])
        with self.assertRaises(DomainError) as ctx:
            self.svc.award_tender("sup1", "supervisor", self.tid, self.tender_row()["version"])
        self.assertEqual(409, ctx.exception.status)

    def test_conflict_only_blocks_the_conflicted_evaluator_and_vendor(self):
        self.eval("eval1", self.bid1, 800000, 90)
        # 针对 v1 的定向冲突：eval1 对 v1 的投标不能评分，对 v2 可以
        self.svc.declare_conflict("eval1", "evaluator", self.tid, "eval1", self.v1["id"], "亲属关系")
        with self.assertRaises(DomainError) as ctx:
            self.eval("eval1", self.bid1, 800000, 90)
        self.assertEqual(403, ctx.exception.status)
        self.eval("eval1", self.bid2, 700000, 80)
        # 无冲突的其他评审人对任何投标都可继续评分
        self.eval("eval2", self.bid1, 800000, 90)
        ranking = self.svc.get_tender("sup1", "supervisor", self.tid)["ranking"]
        self.assertTrue(ranking["complete"])

    def test_accepted_complaint_resets_round_and_award_freezes_snapshot(self):
        self.eval("eval1", self.bid1, 800000, 90)
        self.eval("eval1", self.bid2, 700000, 80)
        first_round = self.svc.get_tender("sup1", "supervisor", self.tid)["ranking"]
        self.assertEqual(self.bid1["id"], first_round["ranking"][0]["bid_id"])
        complaint = self.svc.submit_complaint("vendor2", "vendor", self.tid, "评分规则理解错误")
        resolved = self.svc.resolve_complaint("sup1", "supervisor", complaint["id"], "accepted", "按新口径重评")
        self.assertEqual("accepted", resolved["status"])
        row = self.tender_row()
        self.assertEqual(2, row["evaluation_round"])
        self.assertEqual("reevaluation", row["status"])
        with self.svc.connect() as conn:
            self.assertEqual(0, conn.execute(
                "SELECT COUNT(*) c FROM evaluations WHERE status='active'").fetchone()["c"])
            kinds = [r["kind"] for r in conn.execute(
                "SELECT kind FROM score_chain WHERE tender_id=? ORDER BY seq", (self.tid,)).fetchall()]
        self.assertEqual(["score", "score", "void", "round_reset"], kinds)
        # 新一轮重新评分（结果与第一轮不同），再授标
        self.eval("eval2", self.bid1, 800000, 70)
        self.eval("eval2", self.bid2, 700000, 95)
        version = self.tender_row()["version"]
        award = self.svc.award_tender("sup1", "supervisor", self.tid, version)
        self.assertEqual(self.bid2["id"], award["award"]["winner"]["bid_id"])
        self.assertEqual(2, award["award"]["round"])
        self.assertIn("snapshot_hash", award)
        # 授标后再申报冲突/受理投诉都不能改变冻结快照
        frozen = self.svc.declare_conflict("eval2", "evaluator", self.tid, "eval2", self.v2["id"], "新发现")
        self.assertTrue(frozen["frozen"])
        self.assertEqual([], frozen["voided_evaluation_ids"])
        chain = self.svc.verify_chain(self.tid)
        self.assertTrue(chain["verified"])
        with self.svc.connect() as conn:
            snap = json.loads(conn.execute(
                "SELECT award_snapshot FROM tenders WHERE id=?", (self.tid,)).fetchone()["award_snapshot"])
        self.assertEqual(self.bid2["id"], snap["winner"]["bid_id"])

    # --------------------------------------------------------------- 并发 409

    def _race(self, eval_first: int):
        """确定性并发：先拿写锁的一方在提交点挂起，等另一方阻塞在写锁上后再放行。

        两边基于同一个 GET 快照基线（第 1 轮）；先提交者生效，后者必须 409。
        """
        results = {}
        complaint = self.svc.submit_complaint("vendor2", "vendor", self.tid, "异议")
        winner_in = threading.Event()     # 先到方已进入提交点
        loser_started = threading.Event()  # 后到方已经开始（阻塞在写锁）
        release_winner = threading.Event()  # 放行先到方提交

        def winner_hook():
            winner_in.set()
            loser_started.wait(5)
            release_winner.wait(5)

        def loser_delay():
            # 确保先到方已经拿到写锁并挂起，再去抢锁
            winner_in.wait(5)
            loser_started.set()

        if eval_first:
            self.svc.before_evaluate_commit = winner_hook
            self.svc.before_resolve_commit = None
        else:
            self.svc.before_resolve_commit = winner_hook
            self.svc.before_evaluate_commit = None

        def do_eval():
            if not eval_first:
                loser_delay()
            try:
                self.svc.evaluate_bid("eval3", "evaluator", self.bid1["id"],
                                      {"报价": 800000, "质量": 90},
                                      expected_round=1, expected_invalidation_revision=0)
                results["eval"] = "ok"
            except DomainError as exc:
                results["eval"] = exc.status
            finally:
                release_winner.set()

        def do_resolve():
            if eval_first:
                loser_delay()
            try:
                self.svc.resolve_complaint(
                    "sup1", "supervisor", complaint["id"], "accepted", "重评",
                    expected_round=1, expected_invalidation_revision=0,
                    expected_score_commit_counter=0)
                results["resolve"] = "ok"
            except DomainError as exc:
                results["resolve"] = exc.status
            finally:
                release_winner.set()

        threads = [threading.Thread(target=do_eval), threading.Thread(target=do_resolve)]
        for th in threads:
            th.start()
        for th in threads:
            th.join()
        self.svc.before_evaluate_commit = None
        self.svc.before_resolve_commit = None
        return results

    def test_evaluate_first_evaluate_wins_resolver_gets_409(self):
        # 让评分先于投诉处理拿到同一快照基线
        results = self._race(eval_first=1)
        self.assertEqual({"eval": "ok", "resolve": 409}, results)
        self.assertTrue(self.svc.verify_chain(self.tid)["verified"])
        # 投诉仍 open；处理方可读取新基线后重新决策
        with self.svc.connect() as conn:
            self.assertEqual("open", conn.execute(
                "SELECT status FROM complaints WHERE id=1").fetchone()["status"])

    def test_resolve_first_resolve_wins_evaluator_gets_409(self):
        results = self._race(eval_first=0)
        self.assertEqual({"eval": 409, "resolve": "ok"}, results)
        chain = self.svc.verify_chain(self.tid)
        self.assertTrue(chain["verified"])
        # 败诉的评分没有写入任何 active 评分行
        with self.svc.connect() as conn:
            self.assertEqual(0, conn.execute(
                "SELECT COUNT(*) c FROM evaluations WHERE evaluator='eval3' AND status='active'"
            ).fetchone()["c"])

    # --------------------------------------------------------------- 升级补链

    def test_legacy_scores_are_backfilled_without_overwriting(self):
        db = Path(self.tmp.name) / "legacy.db"
        conn = sqlite3.connect(db)
        conn.executescript(
            """
            CREATE TABLE tenders(id INTEGER PRIMARY KEY AUTOINCREMENT, tender_no TEXT UNIQUE, title TEXT,
              description TEXT DEFAULT '', status TEXT, deadline TEXT, criteria TEXT,
              evaluation_round INTEGER DEFAULT 1, evaluations_locked INTEGER DEFAULT 0,
              awarded_bid_id INTEGER, award_snapshot TEXT, version INTEGER DEFAULT 1,
              created_by TEXT, created_at TEXT, updated_at TEXT);
            CREATE TABLE vendors(id INTEGER PRIMARY KEY AUTOINCREMENT, vendor_no TEXT UNIQUE, name TEXT,
              representative TEXT, created_at TEXT);
            CREATE TABLE bids(id INTEGER PRIMARY KEY AUTOINCREMENT, tender_id INTEGER, vendor_id INTEGER,
              payload TEXT, payload_hash TEXT, price REAL, status TEXT, version INTEGER DEFAULT 1,
              submitted_by TEXT, submitted_at TEXT, opened_at TEXT);
            CREATE TABLE evaluations(id INTEGER PRIMARY KEY AUTOINCREMENT, bid_id INTEGER,
              evaluation_round INTEGER, evaluator TEXT, criterion TEXT, raw_value REAL, score REAL,
              comment TEXT DEFAULT '', version INTEGER DEFAULT 1, created_at TEXT, updated_at TEXT);
            CREATE TABLE conflicts(id INTEGER PRIMARY KEY AUTOINCREMENT, tender_id INTEGER, evaluator TEXT,
              vendor_id INTEGER, reason TEXT, declared_by TEXT, created_at TEXT);
            CREATE TABLE clarifications(id INTEGER PRIMARY KEY AUTOINCREMENT);
            CREATE TABLE complaints(id INTEGER PRIMARY KEY AUTOINCREMENT, tender_id INTEGER, complainant TEXT,
              body TEXT, status TEXT DEFAULT 'open', resolution TEXT, reviewed_by TEXT, created_at TEXT,
              resolved_at TEXT);
            CREATE TABLE timeline(id INTEGER PRIMARY KEY AUTOINCREMENT, tender_id INTEGER, actor TEXT,
              action TEXT, details TEXT, created_at TEXT);
            INSERT INTO tenders VALUES(1,'L-1','旧项目','','opened','2020-01-01T00:00:00+00:00','[]',1,0,
              NULL,NULL,1,'p','2020-01-01','2020-01-01');
            INSERT INTO vendors VALUES(1,'LV','旧供应商','r','2020-01-01');
            INSERT INTO bids VALUES(1,1,1,'{}','deadbeef',500000,'opened',1,'v','2020-01-01','2020-01-01');
            INSERT INTO evaluations(id,bid_id,evaluation_round,evaluator,criterion,raw_value,score,created_at,updated_at)
              VALUES(1,1,1,'evalA','价格',500000,100.0,'2020-01-02','2020-01-02'),
                    (2,1,1,'evalA','质量',88,88.0,'2020-01-02','2020-01-02'),
                    (3,999,1,'ghost','价格',1,1.0,'2020-01-03','2020-01-03');
            """
        )
        conn.commit()
        conn.close()

        svc = ProcurementService(db)
        # 原值不动
        conn = sqlite3.connect(db)
        conn.row_factory = sqlite3.Row
        self.assertEqual(100.0, conn.execute("SELECT score FROM evaluations WHERE id=1").fetchone()["score"])
        # 正常项目链完整
        chain = svc.verify_chain(1)
        self.assertTrue(chain["verified"])
        self.assertEqual("legacy-backfill",
                         conn.execute("SELECT note FROM score_chain WHERE tender_id=1").fetchone()["note"])
        # 归属不明记录进入待核链，不覆盖原值
        orphan = svc.verify_chain(0)
        self.assertFalse(orphan["verified"])
        self.assertEqual(1, orphan["counts"]["pending"])
        self.assertTrue(any("待核" in r for r in orphan["breakpoint"]["reasons"]))
        self.assertEqual(1.0, conn.execute("SELECT score FROM evaluations WHERE id=3").fetchone()["score"])
        # 升级幂等
        ProcurementService(db)
        self.assertEqual(2, conn.execute("SELECT COUNT(*) c FROM score_chain").fetchone()["c"])
        conn.close()

    # ------------------------------------------------------------ 重启恢复

    def _insert_writing_node(self, with_eval_rows: str, payload_score=800000):
        """直接在库里制造一个只写了一半的评分链节点。with_eval_rows: all/none/partial"""
        payload = {
            "kind": "score", "tender_id": self.tid, "bid_id": self.bid1["id"], "evaluation_round": 1,
            "evaluator": "eval9", "comment": "",
            "items": [
                {"criterion": "报价", "raw_value": float(payload_score), "score": 80.0,
                 "max_value": 1000000.0, "kind": "cost"},
                {"criterion": "质量", "raw_value": 90.0, "score": 90.0, "max_value": 100.0, "kind": "direct"},
            ],
            "submitted_at": "2026-01-01T00:00:00+00:00",
        }
        text = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
        with self.svc.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """INSERT INTO score_chain(tender_id,seq,kind,bid_id,evaluation_round,evaluator,eval_ids,
                   payload,payload_hash,prev_hash,node_hash,node_status,source,verified,note,created_by,created_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (self.tid, 1, "score", self.bid1["id"], 1, "eval9", "[]", text, digest,
                 GENESIS_HASH, None, "writing", "live", "ok", "", "eval9", "2026-01-01T00:00:00+00:00"),
            )
            if with_eval_rows in {"all", "partial"}:
                conn.execute(
                    """INSERT INTO evaluations(bid_id,evaluation_round,evaluator,criterion,raw_value,score,
                       created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)""",
                    (self.bid1["id"], 1, "eval9", "报价", payload_score, 80.0, "2026-01-01", "2026-01-01"),
                )
            if with_eval_rows == "all":
                conn.execute(
                    """INSERT INTO evaluations(bid_id,evaluation_round,evaluator,criterion,raw_value,score,
                       created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)""",
                    (self.bid1["id"], 1, "eval9", "质量", 90, 90.0, "2026-01-01", "2026-01-01"),
                )

    def test_restart_repairs_completed_but_unsealed_node(self):
        self._insert_writing_node("all")
        svc = ProcurementService(self.db)
        self.assertEqual([{"tender_id": self.tid, "seq": 1, "eval_ids": [1, 2]}],
                         svc.recovery_report["repaired"])
        chain = svc.verify_chain(self.tid)
        self.assertTrue(chain["verified"])
        self.assertEqual(1, chain["counts"]["repaired"])

    def test_restart_aborts_node_without_eval_rows_and_chain_stays_continuous(self):
        self._insert_writing_node("none")
        svc = ProcurementService(self.db)
        self.assertEqual(1, len(svc.recovery_report["aborted"]))
        self.assertEqual(0, len(svc.recovery_report["broken"]))
        chain = svc.verify_chain(self.tid)
        self.assertTrue(chain["verified"])
        self.assertEqual(1, chain["counts"]["aborted"])
        # 墓碑之后仍可继续写新节点，链不断
        self.eval("eval1", self.bid1, 800000, 90)
        self.assertTrue(self.svc.verify_chain(self.tid)["verified"])

    def test_restart_keeps_partial_write_broken_and_blocks_award(self):
        self._insert_writing_node("partial")
        svc = ProcurementService(self.db)
        self.assertEqual(1, len(svc.recovery_report["broken"]))
        chain = svc.verify_chain(self.tid)
        self.assertFalse(chain["verified"])
        self.assertEqual(1, chain["breakpoint"]["seq"])
        # 修复接口对部分写入同样不能凭空恢复
        report = svc.repair_chain("sup1", "supervisor", self.tid)
        self.assertFalse(report["chain"]["verified"])
        with self.assertRaises(DomainError) as ctx:
            svc.award_tender("sup1", "supervisor", self.tid, self.tender_row()["version"])
        self.assertEqual(409, ctx.exception.status)

    def test_tampered_sealed_node_blocks_award_with_exact_breakpoint(self):
        self.eval("eval1", self.bid1, 800000, 90)
        self.eval("eval1", self.bid2, 700000, 80)
        with self.svc.connect() as conn:
            conn.execute("UPDATE score_chain SET node_hash='f'||substr(node_hash,2) WHERE tender_id=? AND seq=1",
                         (self.tid,))
        chain = self.svc.verify_chain(self.tid)
        self.assertFalse(chain["verified"])
        self.assertEqual(1, chain["breakpoint"]["seq"])
        self.assertIn("链路摘要校验失败", chain["breakpoint"]["reasons"])
        with self.assertRaises(DomainError) as ctx:
            self.svc.award_tender("sup1", "supervisor", self.tid, self.tender_row()["version"])
        self.assertEqual(409, ctx.exception.status)

    # --------------------------------------------------------- 公开页面数据

    def test_state_exposes_per_round_chain_status_and_breakpoint(self):
        self.eval("eval1", self.bid1, 800000, 90)
        state = self.svc.state("anon", "public")
        chain = next(c for c in state["chain_status"] if c["tender_id"] == self.tid)
        self.assertTrue(chain["verified"])
        self.assertEqual("完整", chain["rounds"][0]["status"])
        self.assertIsNone(chain["breakpoint"])
        # 公开状态不泄露评分明细
        self.assertEqual([], state["bids"])
        # 出现断点后公开状态直接暴露位置
        with self.svc.connect() as conn:
            conn.execute("UPDATE score_chain SET payload_hash=? WHERE tender_id=? AND seq=1",
                         ("0" * 64, self.tid))
        state = self.svc.state("anon", "public")
        chain = next(c for c in state["chain_status"] if c["tender_id"] == self.tid)
        self.assertFalse(chain["verified"])
        self.assertEqual(1, chain["breakpoint"]["seq"])


if __name__ == "__main__":
    unittest.main()
