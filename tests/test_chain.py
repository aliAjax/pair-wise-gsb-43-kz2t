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
from app import GENESIS_HASH, DomainError, ProcurementService, canonical_hash  # noqa: E402

LEGACY_SCHEMA = """
CREATE TABLE tenders (
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
CREATE TABLE vendors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    vendor_no TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    representative TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE bids (
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
CREATE TABLE evaluations (
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
"""


class ChainTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Path(self.tmp.name) / "test.db"
        self.service = ProcurementService(self.db)
        self.vendor1 = self.service.create_vendor("proc1", "procurement", "V-001", "启明科技", "vendor1")
        self.vendor2 = self.service.create_vendor("proc1", "procurement", "V-002", "远山系统", "vendor2")
        criteria = [
            {"name": "报价", "weight": 60, "kind": "cost", "max_value": 1000000},
            {"name": "质量", "weight": 40, "kind": "direct", "max_value": 100},
        ]
        self.tender = self.service.create_tender(
            "proc1", "procurement", "T-001", "数据中心设备",
            (datetime.now(timezone.utc) + timedelta(seconds=2)).isoformat(), criteria,
        )
        self.tender = self.service.publish_tender("proc1", "procurement", self.tender["id"], self.tender["version"])

    def tearDown(self):
        self.tmp.cleanup()

    def open_with_bids(self):
        self.service.submit_bid("vendor1", "vendor", self.tender["id"], self.vendor1["id"], {"报价": 800000, "质量": 90}, 800000)
        self.service.submit_bid("vendor2", "vendor", self.tender["id"], self.vendor2["id"], {"报价": 700000, "质量": 80}, 700000)
        time.sleep(2.1)
        opened = self.service.open_bids("proc1", "procurement", self.tender["id"], self.tender["version"])
        return opened["bids"]

    def chain_nodes(self, where="1=1"):
        with sqlite3.connect(self.db) as conn:
            conn.row_factory = sqlite3.Row
            return [dict(r) for r in conn.execute("SELECT * FROM score_chain WHERE %s ORDER BY seq" % where).fetchall()]

    def award_now(self):
        current = self.service.get_tender("sup1", "supervisor", self.tender["id"])
        return self.service.award_tender("sup1", "supervisor", self.tender["id"], current["tender"]["version"])


class ScoreChainTest(ChainTestBase):
    def test_scores_written_to_continuous_chain_and_verifiable(self):
        bids = self.open_with_bids()
        result = self.service.evaluate_bid("eval1", "evaluator", bids[0]["id"], {"报价": 800000, "质量": 90})
        self.assertIn("batch_id", result)
        self.assertEqual(2, len(result["chain"]))
        status = self.service.verify_chain()
        self.assertEqual("ok", status["status"])
        self.assertEqual(2, status["total_nodes"])
        self.assertEqual([1], [r["round"] for r in status["rounds"]])
        nodes = self.chain_nodes()
        # 节点携带采购项目、投标和评分批次，且前序哈希连续
        self.assertEqual(self.tender["id"], nodes[0]["tender_id"])
        self.assertEqual(bids[0]["id"], nodes[0]["bid_id"])
        self.assertEqual(result["batch_id"], nodes[0]["batch_id"])
        self.assertEqual(GENESIS_HASH, nodes[0]["prev_hash"])
        self.assertEqual(nodes[0]["hash"], nodes[1]["prev_hash"])
        # 审计人员可独立重算节点哈希
        body = {k: nodes[0][k] for k in ("seq", "tender_id", "bid_id", "evaluation_id", "evaluation_round",
                                         "batch_id", "event_type", "payload", "prev_hash")}
        self.assertEqual(nodes[0]["hash"], canonical_hash(body))
        payload = json.loads(nodes[0]["payload"])
        self.assertEqual({"evaluator": "eval1", "criterion": "报价", "raw_value": 800000.0},
                         {k: payload[k] for k in ("evaluator", "criterion", "raw_value")})
        # 篡改链节点内容后校验能定位断点
        with sqlite3.connect(self.db) as conn:
            conn.execute("UPDATE score_chain SET payload=? WHERE seq=?",
                         ('{"criterion":"报价","score":1}', nodes[0]["seq"]))
        broken = self.service.verify_chain()
        self.assertEqual("broken", broken["status"])
        self.assertEqual(nodes[0]["seq"], broken["break_at"]["seq"])

    def test_conflict_voids_unawarded_scores_and_recomputes_ranking(self):
        bids = self.open_with_bids()
        self.service.evaluate_bid("eval1", "evaluator", bids[0]["id"], {"报价": 800000, "质量": 90})
        self.service.evaluate_bid("eval1", "evaluator", bids[1]["id"], {"报价": 700000, "质量": 80})
        conflict = self.service.declare_conflict("sup1", "supervisor", self.tender["id"], "eval1",
                                                 self.vendor1["id"], "曾受雇于供应商")
        self.assertFalse(conflict["snapshot_frozen"])
        self.assertEqual(2, len(conflict["voided_evaluation_ids"]))
        # 排名失效：被作废评分的投标不再完整，授标被拒绝
        current = self.service.get_tender("sup1", "supervisor", self.tender["id"])
        with self.assertRaises(DomainError) as ctx:
            self.service.award_tender("sup1", "supervisor", self.tender["id"], current["tender"]["version"])
        self.assertEqual(409, ctx.exception.status)
        # 作废事件与评分作废均已上链
        void_events = self.chain_nodes("event_type='void'")
        self.assertEqual(1, len(void_events))
        self.assertEqual("conflict", json.loads(void_events[0]["payload"])["source"])
        self.assertEqual(2, len(self.chain_nodes("event_type='score' AND status='voided'")))
        # 其他评审人重算评分后可以授标
        self.service.evaluate_bid("eval2", "evaluator", bids[0]["id"], {"报价": 800000, "质量": 85})
        award = self.award_now()
        self.assertEqual("awarded", award["tender"]["status"])
        self.assertEqual("ok", self.service.verify_chain()["status"])

    def test_awarded_snapshot_frozen_when_conflict_declared(self):
        bids = self.open_with_bids()
        self.service.evaluate_bid("eval1", "evaluator", bids[0]["id"], {"报价": 800000, "质量": 90})
        self.service.evaluate_bid("eval1", "evaluator", bids[1]["id"], {"报价": 700000, "质量": 80})
        award = self.award_now()
        snapshot_before = award["tender"]["award_snapshot"]
        conflict = self.service.declare_conflict("sup1", "supervisor", self.tender["id"], "eval1",
                                                 self.vendor1["id"], "事后发现利益冲突")
        self.assertTrue(conflict["snapshot_frozen"])
        self.assertEqual([], conflict["voided_evaluation_ids"])
        after = self.service.get_tender("sup1", "supervisor", self.tender["id"])
        self.assertEqual(snapshot_before, after["tender"]["award_snapshot"])
        self.assertEqual([], self.chain_nodes("status='voided'"))

    def test_complaint_acceptance_voids_round_and_reevaluates(self):
        bids = self.open_with_bids()
        self.service.evaluate_bid("eval1", "evaluator", bids[0]["id"], {"报价": 800000, "质量": 90})
        self.service.evaluate_bid("eval1", "evaluator", bids[1]["id"], {"报价": 700000, "质量": 80})
        complaint = self.service.submit_complaint("vendor1", "vendor", self.tender["id"], "评分标准适用错误")
        resolved = self.service.resolve_complaint("sup1", "supervisor", complaint["id"], "accepted", "重新评审")
        self.assertEqual(4, len(resolved["voided_evaluation_ids"]))
        tender = self.service.get_tender("sup1", "supervisor", self.tender["id"])["tender"]
        self.assertEqual(2, tender["evaluation_round"])
        self.assertEqual(4, len(self.chain_nodes("event_type='score' AND status='voided'")))
        void_events = self.chain_nodes("event_type='void'")
        self.assertEqual("complaint", json.loads(void_events[0]["payload"])["source"])
        # 新一轮重评后授标，链仍然完整
        self.service.evaluate_bid("eval1", "evaluator", bids[0]["id"], {"报价": 800000, "质量": 95})
        self.service.evaluate_bid("eval1", "evaluator", bids[1]["id"], {"报价": 700000, "质量": 85})
        award = self.award_now()
        self.assertEqual(2, award["award"]["round"])
        self.assertEqual("ok", self.service.verify_chain()["status"])

    def test_score_and_complaint_resolution_race_first_commit_wins(self):
        bids = self.open_with_bids()
        self.service.evaluate_bid("eval1", "evaluator", bids[0]["id"], {"报价": 800000, "质量": 90})
        complaint = self.service.submit_complaint("vendor1", "vendor", self.tender["id"], "程序异议")
        tender = self.service.get_tender("sup1", "supervisor", self.tender["id"])["tender"]
        barrier = threading.Barrier(2)
        results = {}

        def score():
            barrier.wait()
            try:
                self.service.evaluate_bid("eval2", "evaluator", bids[1]["id"], {"报价": 700000, "质量": 80},
                                          expected_round=tender["evaluation_round"])
                results["score"] = "ok"
            except DomainError as exc:
                results["score"] = exc.status

        def resolve():
            barrier.wait()
            try:
                self.service.resolve_complaint("sup1", "supervisor", complaint["id"], "accepted", "重评",
                                               expected_version=tender["version"])
                results["resolve"] = "ok"
            except DomainError as exc:
                results["resolve"] = exc.status

        threads = [threading.Thread(target=score), threading.Thread(target=resolve)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        # 先提交事务的一方生效，另一方 409
        self.assertEqual(["409", "ok"], sorted(str(v) for v in results.values()))
        self.assertEqual("ok", self.service.verify_chain()["status"])


class ChainMigrationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Path(self.tmp.name) / "legacy.db"

    def tearDown(self):
        self.tmp.cleanup()

    def build_legacy_db(self):
        conn = sqlite3.connect(self.db)  # 旧库：无摘要链、evaluations 无 status 列
        conn.executescript(LEGACY_SCHEMA)
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        conn.execute("INSERT INTO vendors(id,vendor_no,name,representative,created_at) VALUES(1,'V-1','启明','vp',?)", (now,))
        conn.execute(
            """INSERT INTO tenders(id,tender_no,title,description,status,deadline,criteria,evaluation_round,
                                  evaluations_locked,created_by,created_at,updated_at)
               VALUES(1,'T-OLD','旧项目','','opened','2030-01-01T00:00:00+00:00','[]',1,0,'proc',?,?)""",
            (now, now),
        )
        conn.execute(
            "INSERT INTO bids(id,tender_id,vendor_id,payload,payload_hash,price,status,submitted_by,submitted_at) VALUES(1,1,1,'{}','h',100,'opened','vp',?)",
            (now,),
        )
        conn.execute(
            "INSERT INTO evaluations(id,bid_id,evaluation_round,evaluator,criterion,raw_value,score,comment,created_at,updated_at) VALUES(1,1,1,'eval1','质量',90,90,'原值',?,?)",
            (now, now),
        )
        # 无法确认的记录：投标不存在
        conn.execute(
            "INSERT INTO evaluations(id,bid_id,evaluation_round,evaluator,criterion,raw_value,score,comment,created_at,updated_at) VALUES(2,999,1,'eval2','质量',80,80,'孤儿',?,?)",
            (now, now),
        )
        conn.commit()
        conn.close()

    def test_legacy_scores_backfilled_and_unconfirmable_marked_pending(self):
        self.build_legacy_db()
        service = ProcurementService(self.db)  # 升级：按现有主键补链
        status = service.verify_chain()
        self.assertEqual("ok", status["status"])
        self.assertEqual(2, status["total_nodes"])
        self.assertEqual("pending", status["rounds"][0]["status"])
        with sqlite3.connect(self.db) as conn:
            conn.row_factory = sqlite3.Row
            nodes = [dict(r) for r in conn.execute("SELECT * FROM score_chain ORDER BY seq").fetchall()]
            ev1 = dict(conn.execute("SELECT * FROM evaluations WHERE id=1").fetchone())
            ev2 = dict(conn.execute("SELECT * FROM evaluations WHERE id=2").fetchone())
        self.assertEqual("legacy-1", nodes[0]["batch_id"])
        self.assertEqual("valid", nodes[0]["status"])
        self.assertEqual("legacy-2", nodes[1]["batch_id"])
        self.assertEqual("pending", nodes[1]["status"])  # 待核
        # 原值不被覆盖
        self.assertEqual(90, ev1["raw_value"])
        self.assertEqual("原值", ev1["comment"])
        self.assertEqual(80, ev2["raw_value"])
        self.assertEqual("孤儿", ev2["comment"])
        # 重复启动不会重复补链
        again = ProcurementService(self.db)
        self.assertEqual(2, again.verify_chain()["total_nodes"])


class ChainRecoveryTest(ChainTestBase):
    def test_half_written_node_repaired_on_restart(self):
        bids = self.open_with_bids()
        self.service.evaluate_bid("eval1", "evaluator", bids[0]["id"], {"报价": 800000, "质量": 90})
        self.service.evaluate_bid("eval1", "evaluator", bids[1]["id"], {"报价": 700000, "质量": 80})
        # 模拟上次只写了一半的链节点：哈希未落盘
        with sqlite3.connect(self.db) as conn:
            conn.execute("UPDATE score_chain SET hash='' WHERE seq=(SELECT MAX(seq) FROM score_chain)")
        restarted = ProcurementService(self.db)
        self.assertEqual("ok", restarted.startup_chain_status["status"])
        self.assertGreaterEqual(restarted.startup_chain_status["repaired"], 1)
        award = self.award_now()
        self.assertEqual("awarded", award["tender"]["status"])

    def test_unrepairable_break_blocks_award_until_repaired(self):
        bids = self.open_with_bids()
        self.service.evaluate_bid("eval1", "evaluator", bids[0]["id"], {"报价": 800000, "质量": 90})
        self.service.evaluate_bid("eval1", "evaluator", bids[1]["id"], {"报价": 700000, "质量": 80})
        with sqlite3.connect(self.db) as conn:
            conn.row_factory = sqlite3.Row
            first = dict(conn.execute("SELECT * FROM score_chain ORDER BY seq LIMIT 1").fetchone())
            saved_payload = first["payload"]
            conn.execute("UPDATE score_chain SET payload='{' WHERE seq=?", (first["seq"],))
        restarted = ProcurementService(self.db)
        self.assertEqual("blocked", restarted.startup_chain_status["status"])
        self.assertEqual(first["seq"], restarted.startup_chain_status["break_at"]["seq"])
        # 链阻断期间授标被拒绝
        current = restarted.get_tender("sup1", "supervisor", self.tender["id"])
        with self.assertRaises(DomainError) as ctx:
            restarted.award_tender("sup1", "supervisor", self.tender["id"], current["tender"]["version"])
        self.assertEqual(409, ctx.exception.status)
        self.assertIn("断点", str(ctx.exception))
        # 公开状态展示断点位置
        public_status = restarted.verify_chain()
        self.assertEqual("blocked", public_status["status"])
        self.assertEqual(first["seq"], public_status["break_at"]["seq"])
        broken_rounds = [r for r in public_status["rounds"] if r["status"] == "broken"]
        self.assertEqual(1, len(broken_rounds))
        # 人工修复残缺数据后执行修复，解除阻断
        with sqlite3.connect(self.db) as conn:
            conn.execute("UPDATE score_chain SET payload=? WHERE seq=?", (saved_payload, first["seq"]))
        repaired = restarted.repair_chain("aud1", "auditor")
        self.assertEqual("ok", repaired["status"])
        award = self.award_now()
        self.assertEqual("awarded", award["tender"]["status"])


if __name__ == "__main__":
    unittest.main()
