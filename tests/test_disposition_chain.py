import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from src.domain import ConflictError
from src.repository import Repository
from src.rules import classify_readings
from src.service import Service

OP = "operator"
CO = "compliance_officer"
DIR = "director"


class DispositionChainTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tmp.name) / "test.db")
        self.repo = Repository(self.db_path)
        self.service = Service(self.repo)
        self.outlet = self.service.register_outlet({
            "code": "DW001", "name": "一号废水排放口", "pollutant": "COD",
            "permit_limit": 10, "unit": "mg/L",
        }, "monitor", OP)

    def tearDown(self):
        self.repo.close()
        self.tmp.cleanup()

    def _online(self, batch_ref, values, actor="monitor"):
        return self.service.submit_batch({
            "outlet_code": "DW001", "batch_ref": batch_ref, "kind": "online",
            "data": {"readings": [{"ts": f"2026-09-29T0{i}:00:00Z", "value": v}
                                   for i, v in enumerate(values)]},
        }, actor, OP)

    def _condition(self, batch_ref, status, actor="ops"):
        return self.service.submit_batch({
            "outlet_code": "DW001", "batch_ref": batch_ref, "kind": "condition",
            "data": {"status": status, "note": "脱硫塔跳闸" if status == "abnormal" else "恢复"},
        }, actor, OP)

    def _retest(self, batch_ref, value, actor="lab"):
        return self.service.submit_batch({
            "outlet_code": "DW001", "batch_ref": batch_ref, "kind": "retest",
            "data": {"value": value},
        }, actor, OP)

    def test_classify_continuous_merged_single_fluctuation_kept(self):
        findings = classify_readings(
            [{"ts": "t1", "value": 11}, {"ts": "t2", "value": 12},
             {"ts": "t3", "value": 5}, {"ts": "t4", "value": 20}], 10)
        kinds = sorted(f["kind"] for f in findings)
        self.assertEqual(kinds, ["continuous", "fluctuation"])
        merged = next(f for f in findings if f["kind"] == "continuous")
        self.assertTrue(merged["merged"])
        self.assertEqual(merged["count"], 2)
        self.assertEqual(merged["peak"], 12)
        # 单次波动留在批次发现中，但不升级为超标
        self.assertTrue(any(f["kind"] == "fluctuation" for f in findings))

    def test_fluctuation_only_stays_watch(self):
        result = self._online("B-FLU", [4, 11, 5])
        j = result["current_judgment"]
        self.assertEqual(j["severity"], "watch")
        self.assertEqual(j["enforcement"], "notice")
        self.assertEqual(j["n_fluctuation"], 1)
        self.assertEqual(j["n_continuous"], 0)
        self.assertFalse(j["unresolved"])

    def test_continuous_exceedance_recalculates_severity_deadline_enforcement(self):
        first = self._online("B-1", [4, 11, 5])   # 仅单次波动 -> watch
        self.assertEqual(first["current_judgment"]["severity"], "watch")
        second = self._online("B-2", [13, 14])    # 连续异常 -> 超标
        j = second["current_judgment"]
        self.assertEqual(j["severity"], "exceedance")
        self.assertEqual(j["enforcement"], "rectification_order")
        self.assertTrue(j["escalation_required"])
        self.assertTrue(j["unresolved"])
        # 整改期限随严重度收紧
        self.assertLess(j["deadline_hours"], first["current_judgment"]["deadline_hours"])

    def test_major_on_peak_or_condition_escalation(self):
        self._online("B-PEAK", [25, 26])  # 峰值>=2倍限值
        self.assertEqual(self.repo.current_judgment(self._event_id())["severity"], "major")
        # 新排放口：连续异常叠加工况异常也判重大
        self.service.register_outlet({
            "code": "DW002", "name": "二号排放口", "pollutant": "氨氮",
            "permit_limit": 10,
        }, "monitor", OP)
        self.service.submit_batch({
            "outlet_code": "DW002", "batch_ref": "C-1", "kind": "online",
            "data": {"readings": [{"value": 11}, {"value": 12}]},
        }, "monitor", OP)
        cond = self.service.submit_batch({
            "outlet_code": "DW002", "batch_ref": "C-2", "kind": "condition",
            "data": {"status": "abnormal"},
        }, "ops", OP)
        self.assertEqual(cond["current_judgment"]["severity"], "major")
        self.assertEqual(cond["current_judgment"]["enforcement"], "penalty")

    def test_new_batch_joins_current_event_by_outlet(self):
        r1 = self._online("E-1", [11, 12])
        r2 = self._online("E-2", [5, 6])
        self.assertEqual(r1["event"]["id"], r2["event"]["id"])
        # 另一个排放口各自成事件
        self.service.register_outlet({
            "code": "DW999", "name": "其他排放口", "pollutant": "pH", "permit_limit": 7,
        }, "monitor", OP)
        r3 = self.service.submit_batch({
            "outlet_code": "DW999", "batch_ref": "X-1", "kind": "online",
            "data": {"readings": [{"value": 8}, {"value": 9}]},
        }, "monitor", OP)
        self.assertNotEqual(r3["event"]["id"], r1["event"]["id"])

    def test_closed_outlet_event_starts_new_event(self):
        first = self._online("Z-1", [11, 12])
        event_id = first["event"]["id"]
        # 复测合格后关闭旧事件
        self._retest("Z-R1", 6)
        current = self.repo.get_item(event_id)
        self._advance_to_close(event_id)
        again = self._online("Z-2", [11, 12])
        self.assertNotEqual(again["event"]["id"], event_id)

    def _advance_to_close(self, event_id, open_record_id=None):
        # reported -> assessing (合规复核) -> remediation (运行人员整改)
        for target in ("assessing", "remediation"):
            item = self.repo.get_item(event_id)
            role = {"assessing": CO, "remediation": OP}[target]
            self.service.transition(event_id, target, item["version"], "u", role)
        # 整改项关闭后：remediation -> inspection -> closed
        recs = self.repo.list_records(event_id)
        for rec in recs:
            if rec["status"] == "open":
                self.repo.conn.execute("UPDATE records SET status='closed' WHERE id=?", (rec["id"],))
        item = self.repo.get_item(event_id)
        self.service.transition(event_id, "inspection", item["version"], "u", CO)
        item = self.repo.get_item(event_id)
        closed = self.service.transition(event_id, "closed", item["version"], "boss", DIR)
        return closed

    def test_duplicate_batch_recorded_once(self):
        payload = {
            "outlet_code": "DW001", "batch_ref": "DUP", "kind": "online",
            "data": {"readings": [{"value": 11}, {"value": 12}]},
        }
        first = self.service.submit_batch(payload, "monitor", OP)
        second = self.service.submit_batch(payload, "monitor", OP)
        self.assertFalse(first["duplicated"])
        self.assertTrue(second["duplicated"])
        self.assertEqual(second["batch"]["id"], first["batch"]["id"])
        batches = self.repo.list_batches(first["event"]["id"])
        self.assertEqual(len(batches), 1)
        findings = self.service.list_findings(first["event"]["id"], "viewer")
        self.assertEqual(len(findings), 1)
        judgments = self.service.list_judgments(first["event"]["id"], "viewer")
        # 重复提交不产生新的判定版本
        self.assertEqual(judgments[-1]["judgment_version"], first["current_judgment"]["judgment_version"])

    def test_original_judgments_retained_detail_shows_current_only(self):
        self._online("H-1", [4, 11, 5])          # watch
        self._online("H-2", [13, 14])            # exceedance
        self._retest("H-R", 6)                   # 复测合格 -> watch
        event_id = self._event_id()
        history = self.service.list_judgments(event_id, "viewer")
        severities = [j["severity"] for j in history]
        self.assertEqual(severities[0], "normal")  # 事件开立
        self.assertIn("watch", severities)
        self.assertIn("exceedance", severities)
        self.assertEqual(severities[-1], "watch")
        self.assertTrue(history[-1]["resolved"])
        # 每次原始判定都保留
        self.assertEqual(len(history), len({j["id"] for j in history}))
        # 详情只看当前结论
        detail = self.service.get_item(event_id, "viewer")
        self.assertEqual(detail["current_judgment"]["id"], history[-1]["id"])
        self.assertNotIn("judgments", detail)

    def test_retest_fail_bumps_enforcement(self):
        self._online("R-1", [11, 12])
        bumped = self._retest("R-2", 13)
        j = bumped["current_judgment"]
        self.assertTrue(j["retest_exceed"])
        self.assertEqual(j["enforcement"], "penalty")  # order升一级
        self.assertTrue(j["unresolved"])
        # 未解除超标时主管不能关闭
        self.service.transition(self._event_id(), "assessing",
                                bumped["event"]["version"], "u", CO)
        item = self.repo.get_item(self._event_id())
        self.service.transition(item["id"], "remediation", item["version"], "u", OP)
        item = self.repo.get_item(item["id"])
        self.service.transition(item["id"], "inspection", item["version"], "u", CO)
        item = self.repo.get_item(item["id"])
        with self.assertRaises(ConflictError):
            self.service.transition(item["id"], "closed", item["version"], "boss", DIR)

    def test_concurrent_handlers_later_one_must_reassess_latest_version(self):
        result = self._online("Q-1", [11, 12])
        event_id = result["event"]["id"]
        stale_version = result["event"]["version"]
        # 第二个人（处理另一批次）先到，事件版本被推进
        newer = self._condition("Q-2", "abnormal")
        self.assertGreater(newer["event"]["version"], stale_version)
        # 后到的人仍拿旧expected_version处理 -> 冲突，要求按最新版本重评
        with self.assertRaises(ConflictError):
            self.service.transition(event_id, "assessing", stale_version, "late", CO)
        item = self.repo.get_item(event_id)
        done = self.service.transition(event_id, "assessing", item["version"], "late", CO)
        self.assertEqual(done["status"], "assessing")

    def test_workflow_roles_operator_compliance_director(self):
        result = self._online("W-1", [11, 12])
        event_id = result["event"]["id"]
        self._retest("W-R", 6)
        self.repo.add_record(event_id, "remediation", "更换滤芯", "open", "RM-1", "op")
        closed = self._advance_to_close(event_id)
        self.assertEqual(closed["status"], "closed")
        self.assertTrue(self.repo.verify_audit_chain())

    def _event_id(self):
        return self.repo.find_open_item_for_outlet(self.outlet["id"])["id"]

    def test_old_data_upgraded_still_queryable(self):
        self.repo.close()
        # 删除v2库文件（含WAL/SHM），从零构造v1旧库
        for suffix in ("", "-wal", "-shm"):
            p = Path(self.db_path + suffix)
            if p.exists():
                p.unlink()
        legacy = sqlite3.connect(self.db_path)
        legacy.execute(
            """CREATE TABLE items(id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT NOT NULL,
               description TEXT NOT NULL, severity TEXT NOT NULL, quantity REAL NOT NULL DEFAULT 0,
               threshold REAL NOT NULL DEFAULT 1, status TEXT NOT NULL, version INTEGER NOT NULL DEFAULT 1,
               external_ref TEXT, created_by TEXT, created_at TEXT, updated_at TEXT)""")
        legacy.execute(
            """INSERT INTO items(title,description,severity,quantity,threshold,status,version,external_ref,created_by,created_at,updated_at)
               VALUES('旧事件','升级前开立','exceedance',12,6,'remediation',3,'OLD-1','legacy',
               '2026-09-01T00:00:00Z','2026-09-02T00:00:00Z')""")
        legacy.execute(
            """CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT, item_id INTEGER, kind TEXT,
               detail TEXT, status TEXT DEFAULT 'open', external_ref TEXT, created_by TEXT, created_at TEXT)""")
        legacy.execute(
            """INSERT INTO records(item_id,kind,detail,status,created_by,created_at)
               VALUES(1,'evidence','旧处置记录','open','legacy','2026-09-01T01:00:00Z')""")
        legacy.execute("PRAGMA user_version = 1")
        legacy.commit()
        legacy.close()

        upgraded = Repository(self.db_path)
        svc = Service(upgraded)
        # 旧事件可查
        item = svc.get_item(1, "viewer")
        self.assertEqual(item["title"], "旧事件")
        self.assertEqual(item["status"], "remediation")
        # 旧处置历史可查
        records = svc.list_records(1, "viewer")
        self.assertEqual(len(records), 1)
        # 旧事件补有初始判定，且当前结论可读
        history = svc.list_judgments(1, "viewer")
        self.assertEqual(history[0]["severity"], "exceedance")
        self.assertEqual(item["current_judgment"]["severity"], "exceedance")
        # 升级后旧排放口仍可接收新批次，批次历史可查
        result = svc.submit_batch({
            "outlet_code": "LEGACY", "batch_ref": "POST-UPGRADE-1", "kind": "online",
            "data": {"readings": [{"value": 5}, {"value": 6}]},
        }, "monitor", OP)
        # 旧事件在remediation（未关闭），新批次应归入同一当前事件
        self.assertEqual(result["event"]["id"], 1)
        batches = svc.list_batches(1, "viewer")
        self.assertTrue(any(b["batch_ref"] == "POST-UPGRADE-1" for b in batches))
        events = svc.audit("director")
        self.assertTrue(upgraded.verify_audit_chain())
        upgraded.close()


if __name__ == "__main__":
    unittest.main()
