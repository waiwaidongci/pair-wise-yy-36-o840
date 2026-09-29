from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

from .audit import make_entry, utc_now
from .domain import ConflictError, NotFoundError
from .rules import (ENFORCEMENT_BY_SEVERITY, ID_PREFIX, SEVERITIES, STATES,
                    evaluate, response_deadline_hours)

SCHEMA_VERSION = 2
FINDING_STATUSES = ("open", "resolved")


class Repository:
    def __init__(self, db_path: str):
        self.db_path = str(db_path)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.execute("PRAGMA journal_mode = WAL")
        self._create_schema()
        self._migrate()

    # ---------- 建表与升级 ----------
    def _create_schema(self) -> None:
        statuses = ",".join("'" + s.replace("'", "''") + "'" for s in STATES)
        with self.conn:
            self.conn.executescript(f"""
                CREATE TABLE IF NOT EXISTS items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    description TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    quantity REAL NOT NULL DEFAULT 0,
                    threshold REAL NOT NULL DEFAULT 1,
                    status TEXT NOT NULL CHECK(status IN ({statuses})),
                    version INTEGER NOT NULL DEFAULT 1,
                    external_ref TEXT,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS ux_items_external_ref
                    ON items(external_ref) WHERE external_ref IS NOT NULL;
                CREATE TABLE IF NOT EXISTS records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
                    kind TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'open'
                        CHECK(status IN ('open','closed')),
                    external_ref TEXT,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(item_id, external_ref)
                );
                CREATE TABLE IF NOT EXISTS audit_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    action TEXT NOT NULL,
                    entity_type TEXT NOT NULL,
                    entity_id INTEGER NOT NULL,
                    actor TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    previous_hash TEXT NOT NULL,
                    entry_hash TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS outlets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL UNIQUE,
                    name TEXT NOT NULL,
                    pollutant TEXT NOT NULL,
                    permit_limit REAL NOT NULL,
                    unit TEXT,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS batches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    outlet_id INTEGER NOT NULL REFERENCES outlets(id) ON DELETE CASCADE,
                    item_id INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
                    batch_ref TEXT NOT NULL UNIQUE,
                    kind TEXT NOT NULL CHECK(kind IN ('online','condition','retest','legacy')),
                    payload TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS findings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id INTEGER NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
                    item_id INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
                    kind TEXT NOT NULL CHECK(kind IN ('continuous','fluctuation')),
                    start_ts TEXT, end_ts TEXT,
                    peak REAL NOT NULL, count INTEGER NOT NULL, merged INTEGER NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS judgments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
                    judgment_version INTEGER NOT NULL,
                    trigger_kind TEXT NOT NULL,
                    trigger_batch_id INTEGER,
                    severity TEXT NOT NULL,
                    peak_value REAL NOT NULL, permit_limit REAL NOT NULL, ratio REAL NOT NULL,
                    n_continuous INTEGER NOT NULL DEFAULT 0,
                    n_fluctuation INTEGER NOT NULL DEFAULT 0,
                    condition_status TEXT,
                    retest_value REAL, retest_exceed INTEGER NOT NULL DEFAULT 0,
                    resolved INTEGER NOT NULL DEFAULT 0, unresolved INTEGER NOT NULL DEFAULT 0,
                    enforcement TEXT NOT NULL,
                    escalation_required INTEGER NOT NULL DEFAULT 0,
                    deadline_hours INTEGER NOT NULL,
                    rationale TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(item_id, judgment_version)
                );
            """)

    def _columns(self, table: str) -> set:
        return {row["name"] for row in self.conn.execute(f"PRAGMA table_info({table})").fetchall()}

    def _migrate(self) -> None:
        """旧数据升级：加列、回填排放口与初始判定，旧的批次/事件/处置历史仍可查。"""
        version = int(self.conn.execute("PRAGMA user_version").fetchone()[0] or 0)
        with self._lock, self.conn:
            if version >= SCHEMA_VERSION:
                return
            cols = self._columns("items")
            if "outlet_id" not in cols:
                self.conn.execute("ALTER TABLE items ADD COLUMN outlet_id INTEGER REFERENCES outlets(id)")
                now = utc_now()
                # 旧事件归入一个回填排放口，许可限值沿用事件上记录的threshold
                self.conn.execute(
                    """INSERT OR IGNORE INTO outlets(code, name, pollutant, permit_limit, unit, created_by, created_at)
                       VALUES('LEGACY','旧数据迁移排放口','历史污染物', 1, NULL, 'system', ?)""", (now,))
                legacy_id = self.conn.execute(
                    "SELECT id FROM outlets WHERE code='LEGACY'").fetchone()["id"]
                self.conn.execute("UPDATE items SET outlet_id=? WHERE outlet_id IS NULL", (legacy_id,))
                # 旧事件补记v1判定快照（取事件自带severity/quantity/threshold）
                rows = self.conn.execute("SELECT * FROM items").fetchall()
                for row in rows:
                    exists = self.conn.execute(
                        "SELECT 1 FROM judgments WHERE item_id=? AND judgment_version=1",
                        (row["id"],)).fetchone()
                    if exists:
                        continue
                    limit = float(row["threshold"]) or 1.0
                    peak = float(row["quantity"])
                    ratio = peak / limit if limit > 0 else 0.0
                    sev = row["severity"] if row["severity"] in SEVERITIES else "normal"
                    self.conn.execute(
                        """INSERT INTO judgments(item_id, judgment_version, trigger_kind, trigger_batch_id,
                           severity, peak_value, permit_limit, ratio, n_continuous, n_fluctuation,
                           condition_status, retest_value, retest_exceed, resolved, unresolved,
                           enforcement, escalation_required, deadline_hours, rationale, created_by, created_at)
                           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (row["id"], 1, "legacy", None, sev, peak, limit, round(ratio, 4), 0, 0,
                         None, None, 0, 1 if sev in ("normal", "watch") else 0,
                         0 if sev in ("normal", "watch") else 1,
                         ENFORCEMENT_BY_SEVERITY[sev],
                         1 if ENFORCEMENT_BY_SEVERITY[sev] in ("rectification_order", "penalty") else 0,
                         response_deadline_hours(sev, peak, limit), "旧数据升级回填的初始判定",
                         "system", now))
            self.conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")

    # ---------- 排放口 ----------
    @staticmethod
    def _outlet(row: sqlite3.Row) -> Dict[str, Any]:
        return dict(row)

    def create_outlet(self, code: str, name: str, pollutant: str, permit_limit: float,
                      unit: Optional[str], actor: str) -> Dict[str, Any]:
        now = utc_now()
        try:
            with self._lock, self.conn:
                cur = self.conn.execute(
                    """INSERT INTO outlets(code, name, pollutant, permit_limit, unit, created_by, created_at)
                       VALUES(?,?,?,?,?,?,?)""",
                    (code, name, pollutant, permit_limit, unit, actor, now))
                outlet_id = int(cur.lastrowid)
        except sqlite3.IntegrityError as exc:
            raise ConflictError("排放口编号已存在") from exc
        return self.get_outlet(outlet_id)

    def get_outlet(self, outlet_id: int) -> Dict[str, Any]:
        with self._lock:
            row = self.conn.execute("SELECT * FROM outlets WHERE id=?", (outlet_id,)).fetchone()
        if row is None:
            raise NotFoundError("排放口不存在")
        return self._outlet(row)

    def get_outlet_by_code(self, code: str) -> Dict[str, Any]:
        with self._lock:
            row = self.conn.execute("SELECT * FROM outlets WHERE code=?", (code,)).fetchone()
        if row is None:
            raise NotFoundError("排放口不存在")
        return self._outlet(row)

    def list_outlets(self) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute("SELECT * FROM outlets ORDER BY id").fetchall()
        return [dict(row) for row in rows]

    # ---------- 事件（items） ----------
    @staticmethod
    def _item(row: sqlite3.Row) -> Dict[str, Any]:
        return dict(row)

    def create_item(self, title: str, description: str, severity: str,
                    quantity: float, threshold: float, external_ref: Optional[str],
                    actor: str, outlet_id: Optional[int] = None,
                    permit_limit: Optional[float] = None) -> Dict[str, Any]:
        now = utc_now()
        try:
            with self._lock, self.conn:
                cur = self.conn.execute(
                    """INSERT INTO items(title, description, severity, quantity, threshold,
                       status, version, external_ref, outlet_id, created_by, created_at, updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (title, description, severity, quantity, threshold, STATES[0], 1,
                     external_ref, outlet_id, actor, now, now),
                )
                item_id = int(cur.lastrowid)
                limit = float(permit_limit if permit_limit is not None else threshold) or 1.0
                ratio = quantity / limit if limit > 0 else 0.0
                # 事件开立即有v1判定：每次原始判定都保留
                self.conn.execute(
                    """INSERT INTO judgments(item_id, judgment_version, trigger_kind, trigger_batch_id,
                       severity, peak_value, permit_limit, ratio, n_continuous, n_fluctuation,
                       condition_status, retest_value, retest_exceed, resolved, unresolved,
                       enforcement, escalation_required, deadline_hours, rationale, created_by, created_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (item_id, 1, "manual", None, severity, quantity, limit, round(ratio, 4), 0, 0,
                     None, None, 0, 0, 0,
                     ENFORCEMENT_BY_SEVERITY[severity],
                     1 if ENFORCEMENT_BY_SEVERITY[severity] in ("rectification_order", "penalty") else 0,
                     response_deadline_hours(severity, quantity, limit), "事件开立时的初始判定",
                     actor, now))
        except sqlite3.IntegrityError as exc:
            raise ConflictError("external_ref已存在") from exc
        return self.get_item(item_id)

    def get_item(self, item_id: int) -> Dict[str, Any]:
        with self._lock:
            row = self.conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
        if row is None:
            raise NotFoundError("项目不存在")
        return self._item(row)

    def list_items(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM items"
        params: tuple = ()
        if status:
            sql += " WHERE status=?"
            params = (status,)
        sql += " ORDER BY id DESC"
        with self._lock:
            rows = self.conn.execute(sql, params).fetchall()
        return [self._item(row) for row in rows]

    def find_open_item_for_outlet(self, outlet_id: int) -> Optional[Dict[str, Any]]:
        """新批次按排放口归到当前事件：找该排放口最近一个未关闭事件。"""
        with self._lock:
            row = self.conn.execute(
                """SELECT * FROM items WHERE outlet_id=? AND status!='closed'
                   ORDER BY id DESC LIMIT 1""", (outlet_id,)).fetchone()
        return dict(row) if row else None

    def create_event_for_outlet(self, outlet: Dict[str, Any], actor: str) -> Dict[str, Any]:
        now = utc_now()
        title = f"{outlet['code']}-{outlet['pollutant']}排污事件"
        description = f"排放口{outlet['name']}({outlet['code']})监测异常自动开立"
        limit = float(outlet["permit_limit"])
        with self._lock, self.conn:
            cur = self.conn.execute(
                """INSERT INTO items(title, description, severity, quantity, threshold,
                   status, version, external_ref, outlet_id, created_by, created_at, updated_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                (title, description, "normal", 0.0, limit, STATES[0], 1,
                 None, outlet["id"], actor, now, now))
            item_id = int(cur.lastrowid)
            # 自动开立同样写入v1基线判定，后续每次重算追加新版本
            self.conn.execute(
                """INSERT INTO judgments(item_id, judgment_version, trigger_kind, trigger_batch_id,
                   severity, peak_value, permit_limit, ratio, n_continuous, n_fluctuation,
                   condition_status, retest_value, retest_exceed, resolved, unresolved,
                   enforcement, escalation_required, deadline_hours, rationale, created_by, created_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (item_id, 1, "auto_open", None, "normal", 0.0, limit, 0.0, 0, 0,
                 None, None, 0, 0, 0, ENFORCEMENT_BY_SEVERITY["normal"], 0,
                 response_deadline_hours("normal", 0.0, limit),
                 "批次到达自动开立事件时的基线判定", actor, now))
        return self.get_item(item_id)

    def transition_item(self, item_id: int, target: str, expected_version: int,
                        actor: str) -> Dict[str, Any]:
        now = utc_now()
        with self._lock, self.conn:
            cur = self.conn.execute(
                """UPDATE items SET status=?, version=version+1, updated_at=?
                   WHERE id=? AND version=?""",
                (target, now, item_id, expected_version),
            )
            if cur.rowcount == 0:
                exists = self.conn.execute("SELECT 1 FROM items WHERE id=?", (item_id,)).fetchone()
                if exists is None:
                    raise NotFoundError("项目不存在")
                raise ConflictError("版本冲突，请按最新版本重评后重试")
        return self.get_item(item_id)

    # ---------- 监测批次 ----------
    def get_batch_by_ref(self, batch_ref: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self.conn.execute("SELECT * FROM batches WHERE batch_ref=?", (batch_ref,)).fetchone()
        if row is None:
            return None
        batch = dict(row)
        batch["payload"] = json.loads(batch["payload"])
        return batch

    def insert_batch(self, outlet_id: int, item_id: int, batch_ref: str, kind: str,
                     payload: Any, actor: str) -> Dict[str, Any]:
        now = utc_now()
        with self._lock, self.conn:
            cur = self.conn.execute(
                """INSERT INTO batches(outlet_id, item_id, batch_ref, kind, payload, created_by, created_at)
                   VALUES(?,?,?,?,?,?,?)""",
                (outlet_id, item_id, batch_ref, kind,
                 json.dumps(payload, ensure_ascii=False, sort_keys=True), actor, now))
            batch_id = int(cur.lastrowid)
        return self.get_batch(batch_id)

    def get_batch(self, batch_id: int) -> Dict[str, Any]:
        with self._lock:
            row = self.conn.execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
        if row is None:
            raise NotFoundError("批次不存在")
        batch = dict(row)
        batch["payload"] = json.loads(batch["payload"])
        return batch

    def list_batches(self, item_id: Optional[int] = None) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM batches"
        params: tuple = ()
        if item_id is not None:
            sql += " WHERE item_id=?"
            params = (item_id,)
        sql += " ORDER BY id"
        with self._lock:
            rows = self.conn.execute(sql, params).fetchall()
        result = []
        for row in rows:
            batch = dict(row)
            batch["payload"] = json.loads(batch["payload"])
            result.append(batch)
        return result

    def insert_findings(self, batch_id: int, item_id: int, findings: List[Dict[str, Any]]) -> None:
        now = utc_now()
        with self._lock, self.conn:
            for f in findings:
                self.conn.execute(
                    """INSERT INTO findings(batch_id, item_id, kind, start_ts, end_ts, peak, count, merged, created_at)
                       VALUES(?,?,?,?,?,?,?,?,?)""",
                    (batch_id, item_id, f["kind"], f.get("start_ts"), f.get("end_ts"),
                     float(f["peak"]), int(f["count"]), 1 if f["merged"] else 0, now))

    def list_findings(self, item_id: int) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM findings WHERE item_id=? ORDER BY id", (item_id,)).fetchall()
        result = []
        for row in rows:
            f = dict(row)
            f["merged"] = bool(f["merged"])
            result.append(f)
        return result

    def latest_signal(self, item_id: int, kind: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM batches WHERE item_id=? AND kind=? ORDER BY id DESC LIMIT 1",
                (item_id, kind)).fetchone()
        if row is None:
            return None
        batch = dict(row)
        batch["payload"] = json.loads(batch["payload"])
        return batch

    # ---------- 判定版本 ----------
    def append_judgment(self, item_id: int, conclusion: Dict[str, Any], trigger_kind: str,
                        trigger_batch_id: Optional[int], actor: str) -> Dict[str, Any]:
        now = utc_now()
        with self._lock, self.conn:
            row = self.conn.execute(
                "SELECT COALESCE(MAX(judgment_version),0) AS v FROM judgments WHERE item_id=?",
                (item_id,)).fetchone()
            version = int(row["v"]) + 1
            cur = self.conn.execute(
                """INSERT INTO judgments(item_id, judgment_version, trigger_kind, trigger_batch_id,
                   severity, peak_value, permit_limit, ratio, n_continuous, n_fluctuation,
                   condition_status, retest_value, retest_exceed, resolved, unresolved,
                   enforcement, escalation_required, deadline_hours, rationale, created_by, created_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (item_id, version, trigger_kind, trigger_batch_id,
                 conclusion["severity"], float(conclusion["peak_value"]),
                 float(conclusion["permit_limit"]), float(conclusion["ratio"]),
                 int(conclusion["n_continuous"]), int(conclusion["n_fluctuation"]),
                 conclusion.get("condition_status"),
                 conclusion.get("retest_value"),
                 1 if conclusion["retest_exceed"] else 0,
                 1 if conclusion["resolved"] else 0,
                 1 if conclusion["unresolved"] else 0,
                 conclusion["enforcement"],
                 1 if conclusion["escalation_required"] else 0,
                 int(conclusion["deadline_hours"]),
                 conclusion["rationale"], actor, now))
            judgment_id = int(cur.lastrowid)
            # 事件汇总列同步到当前结论；version在批次/状态动作里递增，这里不重复递增
            self.conn.execute(
                """UPDATE items SET severity=?, quantity=?, threshold=?, updated_at=? WHERE id=?""",
                (conclusion["severity"], float(conclusion["peak_value"]),
                 float(conclusion["permit_limit"]), now, item_id))
        return self.get_judgment(judgment_id)

    @staticmethod
    def _judgment(row: sqlite3.Row) -> Dict[str, Any]:
        j = dict(row)
        for flag in ("retest_exceed", "resolved", "unresolved", "escalation_required"):
            j[flag] = bool(j[flag])
        return j

    def get_judgment(self, judgment_id: int) -> Dict[str, Any]:
        with self._lock:
            row = self.conn.execute("SELECT * FROM judgments WHERE id=?", (judgment_id,)).fetchone()
        if row is None:
            raise NotFoundError("判定不存在")
        return self._judgment(row)

    def current_judgment(self, item_id: int) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM judgments WHERE item_id=? ORDER BY judgment_version DESC LIMIT 1",
                (item_id,)).fetchone()
        return self._judgment(row) if row else None

    def list_judgments(self, item_id: int) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM judgments WHERE item_id=? ORDER BY judgment_version",
                (item_id,)).fetchall()
        return [self._judgment(row) for row in rows]

    def bump_item_version(self, item_id: int) -> None:
        now = utc_now()
        with self._lock, self.conn:
            self.conn.execute(
                "UPDATE items SET version=version+1, updated_at=? WHERE id=?", (now, item_id))

    # ---------- 整改记录 ----------
    def add_record(self, item_id: int, kind: str, detail: str, status: str,
                   external_ref: Optional[str], actor: str) -> Dict[str, Any]:
        now = utc_now()
        self.get_item(item_id)
        try:
            with self._lock, self.conn:
                cur = self.conn.execute(
                    """INSERT INTO records(item_id, kind, detail, status, external_ref,
                       created_by, created_at) VALUES(?,?,?,?,?,?,?)""",
                    (item_id, kind, detail, status, external_ref, actor, now),
                )
                record_id = int(cur.lastrowid)
        except sqlite3.IntegrityError as exc:
            raise ConflictError("记录唯一标识已存在") from exc
        with self._lock:
            row = self.conn.execute("SELECT * FROM records WHERE id=?", (record_id,)).fetchone()
        return dict(row)

    def list_records(self, item_id: int) -> List[Dict[str, Any]]:
        self.get_item(item_id)
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM records WHERE item_id=? ORDER BY id", (item_id,)
            ).fetchall()
        return [dict(row) for row in rows]

    def open_record_count(self, item_id: int) -> int:
        with self._lock:
            row = self.conn.execute(
                "SELECT COUNT(*) AS n FROM records WHERE item_id=? AND status='open'",
                (item_id,),
            ).fetchone()
        return int(row["n"])

    # ---------- 审计链 ----------
    def append_audit(self, action: str, entity_type: str, entity_id: int,
                     actor: str, detail: dict) -> Dict[str, Any]:
        with self._lock, self.conn:
            row = self.conn.execute(
                "SELECT entry_hash FROM audit_events ORDER BY id DESC LIMIT 1"
            ).fetchone()
            previous = row["entry_hash"] if row else "GENESIS"
            event = make_entry(action, entity_type, entity_id, actor, detail, previous)
            cur = self.conn.execute(
                """INSERT INTO audit_events(action, entity_type, entity_id, actor, detail,
                   previous_hash, entry_hash, created_at) VALUES(?,?,?,?,?,?,?,?)""",
                (event["action"], event["entity_type"], event["entity_id"], event["actor"],
                 json.dumps(event["detail"], ensure_ascii=False, sort_keys=True),
                 event["previous_hash"], event["entry_hash"], event["created_at"]),
            )
            event_id = int(cur.lastrowid)
        event["id"] = event_id
        return event

    def list_audit(self, entity_id: Optional[int] = None) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM audit_events"
        params: tuple = ()
        if entity_id is not None:
            sql += " WHERE entity_id=?"
            params = (entity_id,)
        sql += " ORDER BY id"
        with self._lock:
            rows = self.conn.execute(sql, params).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["detail"] = json.loads(item["detail"])
            result.append(item)
        return result

    def verify_audit_chain(self) -> bool:
        from .audit import calculate_hash
        with self._lock:
            rows = self.conn.execute("SELECT * FROM audit_events ORDER BY id").fetchall()
        previous = "GENESIS"
        for row in rows:
            if row["previous_hash"] != previous:
                return False
            payload = {
                "action": row["action"], "entity_type": row["entity_type"],
                "entity_id": row["entity_id"], "actor": row["actor"],
                "detail": json.loads(row["detail"]), "created_at": row["created_at"],
            }
            if calculate_hash(previous, payload) != row["entry_hash"]:
                return False
            previous = row["entry_hash"]
        return True

    def close(self) -> None:
        with self._lock:
            self.conn.close()
