from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

from .audit import make_entry, utc_now
from .domain import ConflictError, NotFoundError
from .rules import STATES


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
                    outlet TEXT NOT NULL DEFAULT '',
                    conclusion TEXT,
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
                    batch_id INTEGER REFERENCES batches(id) ON DELETE SET NULL,
                    episode_key TEXT,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(item_id, external_ref)
                );
                CREATE TABLE IF NOT EXISTS batches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
                    batch_ref TEXT NOT NULL UNIQUE,
                    batch_type TEXT NOT NULL CHECK(batch_type IN
                        ('permit','online','retest','condition')),
                    measured_at TEXT NOT NULL,
                    value REAL,
                    limit_value REAL,
                    condition_status TEXT CHECK(condition_status IS NULL
                        OR condition_status IN ('normal','abnormal')),
                    raw_judgment TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    duplicate_of INTEGER REFERENCES batches(id),
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS ix_batches_item ON batches(item_id, id);
                CREATE TABLE IF NOT EXISTS batch_readings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id INTEGER NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
                    item_id INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
                    seq INTEGER NOT NULL,
                    measured_at TEXT NOT NULL,
                    value REAL NOT NULL,
                    limit_value REAL NOT NULL,
                    raw_judgment TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    UNIQUE(batch_id, seq)
                );
                CREATE INDEX IF NOT EXISTS ix_readings_item_time
                    ON batch_readings(item_id, measured_at, id);
                CREATE TABLE IF NOT EXISTS item_versions (
                    item_id INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
                    version INTEGER NOT NULL,
                    title TEXT NOT NULL,
                    description TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    quantity REAL NOT NULL,
                    threshold REAL NOT NULL,
                    status TEXT NOT NULL,
                    outlet TEXT NOT NULL,
                    conclusion TEXT,
                    changed_by TEXT NOT NULL,
                    changed_at TEXT NOT NULL,
                    PRIMARY KEY(item_id, version)
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
            """)
            self._migrate_schema()

    def _migrate_schema(self) -> None:
        def columns(table: str) -> set[str]:
            return {row["name"] for row in self.conn.execute(f"PRAGMA table_info({table})")}

        item_columns = columns("items")
        if "outlet" not in item_columns:
            self.conn.execute("ALTER TABLE items ADD COLUMN outlet TEXT NOT NULL DEFAULT ''")
        if "conclusion" not in item_columns:
            self.conn.execute("ALTER TABLE items ADD COLUMN conclusion TEXT")
        self.conn.execute(
            "UPDATE items SET outlet='OUTLET-' || id WHERE outlet IS NULL OR TRIM(outlet)=''"
        )

        record_columns = columns("records")
        if "batch_id" not in record_columns:
            self.conn.execute("ALTER TABLE records ADD COLUMN batch_id INTEGER")
        if "episode_key" not in record_columns:
            self.conn.execute("ALTER TABLE records ADD COLUMN episode_key TEXT")

        self.conn.executescript("""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_items_active_outlet
                ON items(outlet) WHERE status <> 'closed';
            CREATE UNIQUE INDEX IF NOT EXISTS ux_records_episode
                ON records(item_id, episode_key) WHERE episode_key IS NOT NULL;
            CREATE INDEX IF NOT EXISTS ix_batches_item ON batches(item_id, id);
            CREATE INDEX IF NOT EXISTS ix_readings_item_time
                ON batch_readings(item_id, measured_at, id);
        """)
        self.conn.execute("""
            INSERT OR IGNORE INTO item_versions(item_id, version, title, description,
                severity, quantity, threshold, status, outlet, conclusion, changed_by, changed_at)
            SELECT id, version, title, description, severity, quantity, threshold, status,
                   outlet, conclusion, created_by, updated_at
            FROM items
        """)

    @staticmethod
    def _decode(value: Optional[str]) -> Optional[Dict[str, Any]]:
        return json.loads(value) if value is not None else None

    def _item(self, row: sqlite3.Row) -> Dict[str, Any]:
        item = dict(row)
        item["conclusion"] = self._decode(item.get("conclusion"))
        return item

    def _batch(self, row: sqlite3.Row, readings: bool = False) -> Dict[str, Any]:
        batch = dict(row)
        batch["payload"] = self._decode(batch["payload"])
        if readings:
            batch["readings"] = self.list_readings_for_batch(batch["id"])
        return batch

    def create_item(self, title: str, description: str, severity: str,
                    quantity: float, threshold: float, actor: str,
                    outlet: Optional[str] = None,
                    external_ref: Optional[str] = None) -> Dict[str, Any]:
        now = utc_now()
        try:
            with self._lock, self.conn:
                cur = self.conn.execute(
                    """INSERT INTO items(title, description, severity, quantity, threshold,
                       status, version, outlet, conclusion, external_ref,
                       created_by, created_at, updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (title, description, severity, quantity, threshold, STATES[0], 1,
                     outlet or f"__pending_{now.replace('-', '').replace(':', '')}", None,
                     external_ref, actor, now, now),
                )
                item_id = int(cur.lastrowid)
                final_outlet = outlet or f"OUTLET-{item_id}"
                self.conn.execute("UPDATE items SET outlet=? WHERE id=?", (final_outlet, item_id))
                current = self.get_item(item_id)
                self._insert_version(current, actor, now)
        except sqlite3.IntegrityError as exc:
            raise ConflictError("事件唯一标识或排放口已存在") from exc
        return self.get_item(item_id)

    def find_active_item_by_outlet(self, outlet: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM items WHERE outlet=? AND status <> 'closed' ORDER BY id DESC",
                (outlet,),
            ).fetchone()
        return self._item(row) if row else None

    def get_item(self, item_id: int) -> Dict[str, Any]:
        with self._lock:
            row = self.conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
        if row is None:
            raise NotFoundError("事件不存在")
        return self._item(row)

    def list_items(self, status: Optional[str] = None,
                   outlet: Optional[str] = None) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM items WHERE 1=1"
        params: List[Any] = []
        if status:
            sql += " AND status=?"
            params.append(status)
        if outlet:
            sql += " AND outlet=?"
            params.append(outlet)
        sql += " ORDER BY id DESC"
        with self._lock:
            rows = self.conn.execute(sql, tuple(params)).fetchall()
        return [self._item(row) for row in rows]

    def _insert_version(self, item: Dict[str, Any], actor: str,
                        changed_at: Optional[str] = None) -> None:
        changed_at = changed_at or utc_now()
        self.conn.execute(
            """INSERT INTO item_versions(item_id, version, title, description, severity,
               quantity, threshold, status, outlet, conclusion, changed_by, changed_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
            (item["id"], item["version"], item["title"], item["description"],
             item["severity"], item["quantity"], item["threshold"], item["status"],
             item["outlet"], json.dumps(item.get("conclusion"), ensure_ascii=False, sort_keys=True),
             actor, changed_at),
        )

    def apply_conclusion(self, item_id: int, severity: str, quantity: float,
                         threshold: float, conclusion: Dict[str, Any],
                         actor: str) -> Dict[str, Any]:
        now = utc_now()
        with self._lock, self.conn:
            current = self.get_item(item_id)
            self.conn.execute(
                """UPDATE items SET severity=?, quantity=?, threshold=?, conclusion=?,
                   version=version+1, updated_at=? WHERE id=?""",
                (severity, quantity, threshold,
                 json.dumps(conclusion, ensure_ascii=False, sort_keys=True), now, item_id),
            )
            updated = self.get_item(item_id)
            self._insert_version(updated, actor, now)
        return updated

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
                    raise NotFoundError("事件不存在")
                raise ConflictError("版本冲突，请刷新后重试")
            updated = self.get_item(item_id)
            self._insert_version(updated, actor, now)
        return self.get_item(item_id)

    def add_record(self, item_id: int, kind: str, detail: str, status: str,
                   external_ref: Optional[str], actor: str,
                   batch_id: Optional[int] = None,
                   episode_key: Optional[str] = None) -> Dict[str, Any]:
        now = utc_now()
        self.get_item(item_id)
        try:
            with self._lock, self.conn:
                cur = self.conn.execute(
                    """INSERT INTO records(item_id, kind, detail, status, external_ref,
                       batch_id, episode_key, created_by, created_at)
                       VALUES(?,?,?,?,?,?,?,?,?)""",
                    (item_id, kind, detail, status, external_ref, batch_id, episode_key,
                     actor, now),
                )
                record_id = int(cur.lastrowid)
        except sqlite3.IntegrityError as exc:
            raise ConflictError("记录唯一标识已存在") from exc
        with self._lock:
            row = self.conn.execute("SELECT * FROM records WHERE id=?", (record_id,)).fetchone()
        return dict(row)

    def upsert_episode_record(self, item_id: int, episode_key: str, kind: str,
                              detail: Dict[str, Any], status: str, actor: str,
                              batch_id: int) -> Dict[str, Any]:
        now = utc_now()
        payload = json.dumps(detail, ensure_ascii=False, sort_keys=True)
        with self._lock, self.conn:
            row = self.conn.execute(
                "SELECT * FROM records WHERE item_id=? AND episode_key=?",
                (item_id, episode_key),
            ).fetchone()
            if row is None:
                cur = self.conn.execute(
                    """INSERT INTO records(item_id, kind, detail, status, external_ref,
                       batch_id, episode_key, created_by, created_at)
                       VALUES(?,?,?,?,?,?,?,?,?)""",
                    (item_id, kind, payload, status, None, batch_id, episode_key, actor, now),
                )
                record_id = int(cur.lastrowid)
            else:
                record_id = int(row["id"])
                self.conn.execute(
                    """UPDATE records SET kind=?, detail=?, status=?, batch_id=?,
                       created_by=? WHERE id=?""",
                    (kind, payload, status, batch_id, actor, record_id),
                )
            row = self.conn.execute("SELECT * FROM records WHERE id=?", (record_id,)).fetchone()
        return dict(row)

    def set_episode_status(self, item_id: int, episode_key: str, status: str,
                           actor: str, batch_id: Optional[int] = None) -> None:
        with self._lock, self.conn:
            self.conn.execute(
                """UPDATE records SET status=?, created_by=?, batch_id=COALESCE(?, batch_id)
                   WHERE item_id=? AND episode_key=?""",
                (status, actor, batch_id, item_id, episode_key),
            )

    def list_episode_keys(self, item_id: int) -> List[str]:
        with self._lock:
            rows = self.conn.execute(
                """SELECT episode_key FROM records
                   WHERE item_id=? AND episode_key IS NOT NULL""",
                (item_id,),
            ).fetchall()
        return [row["episode_key"] for row in rows]

    def close_episode_records(self, item_id: int, actor: str,
                              batch_id: Optional[int] = None) -> None:
        with self._lock, self.conn:
            self.conn.execute(
                """UPDATE records SET status='closed'
                   WHERE item_id=? AND episode_key IS NOT NULL AND status='open'""",
                (item_id,),
            )

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

    def create_batch(self, item_id: int, batch_ref: str, batch_type: str,
                     measured_at: str, raw_judgment: str, payload: Dict[str, Any],
                     actor: str, value: Optional[float] = None,
                     limit_value: Optional[float] = None,
                     condition_status: Optional[str] = None,
                     duplicate_of: Optional[int] = None) -> Dict[str, Any]:
        now = utc_now()
        try:
            with self._lock, self.conn:
                cur = self.conn.execute(
                    """INSERT INTO batches(item_id, batch_ref, batch_type, measured_at, value,
                       limit_value, condition_status, raw_judgment, payload, duplicate_of,
                       created_by, created_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (item_id, batch_ref, batch_type, measured_at, value, limit_value,
                     condition_status, raw_judgment,
                     json.dumps(payload, ensure_ascii=False, sort_keys=True), duplicate_of,
                     actor, now),
                )
                batch_id = int(cur.lastrowid)
        except sqlite3.IntegrityError as exc:
            raise ConflictError("批次唯一标识已存在") from exc
        return self.get_batch(batch_id, with_readings=True)

    def add_reading(self, batch_id: int, item_id: int, seq: int, measured_at: str,
                    value: float, limit_value: float, raw_judgment: str,
                    payload: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock, self.conn:
            cur = self.conn.execute(
                """INSERT INTO batch_readings(batch_id, item_id, seq, measured_at, value,
                   limit_value, raw_judgment, payload) VALUES(?,?,?,?,?,?,?,?)""",
                (batch_id, item_id, seq, measured_at, value, limit_value, raw_judgment,
                 json.dumps(payload, ensure_ascii=False, sort_keys=True)),
            )
            reading_id = int(cur.lastrowid)
            row = self.conn.execute(
                "SELECT * FROM batch_readings WHERE id=?", (reading_id,)
            ).fetchone()
        result = dict(row)
        result["payload"] = json.loads(result["payload"])
        return result

    def get_batch_by_ref(self, batch_ref: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM batches WHERE batch_ref=?", (batch_ref,)
            ).fetchone()
        return self.get_batch(int(row["id"]), with_readings=True) if row else None

    def get_batch(self, batch_id: int, with_readings: bool = True) -> Dict[str, Any]:
        with self._lock:
            row = self.conn.execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
        if row is None:
            raise NotFoundError("批次不存在")
        return self._batch(row, with_readings)

    def list_batches(self, item_id: int) -> List[Dict[str, Any]]:
        self.get_item(item_id)
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM batches WHERE item_id=? ORDER BY id", (item_id,)
            ).fetchall()
        return [self._batch(row, readings=True) for row in rows]

    def list_readings_for_batch(self, batch_id: int) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM batch_readings WHERE batch_id=? ORDER BY seq,id", (batch_id,)
            ).fetchall()
        result = []
        for row in rows:
            reading = dict(row)
            reading["payload"] = json.loads(reading["payload"])
            result.append(reading)
        return result

    def list_online_readings(self, item_id: int) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute(
                """SELECT r.* FROM batch_readings r
                   JOIN batches b ON b.id=r.batch_id
                   WHERE r.item_id=? AND b.batch_type='online'
                   ORDER BY r.measured_at, r.id""",
                (item_id,),
            ).fetchall()
        result = []
        for row in rows:
            reading = dict(row)
            reading["payload"] = json.loads(reading["payload"])
            result.append(reading)
        return result

    def list_versions(self, item_id: int) -> List[Dict[str, Any]]:
        self.get_item(item_id)
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM item_versions WHERE item_id=? ORDER BY version", (item_id,)
            ).fetchall()
        result = []
        for row in rows:
            version = dict(row)
            version["conclusion"] = self._decode(version["conclusion"])
            result.append(version)
        return result

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
