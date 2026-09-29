from __future__ import annotations

import threading
from typing import Any, Dict, Optional

from .domain import (ConflictError, ensure_role, normalize_severity,
                     require_number, require_text)
from .repository import Repository
from .rules import (AUDIT_ROLES, BATCH_KINDS, CONDITION_STATUSES, CREATE_ROLES,
                    ENTITY, FINDING_CONTINUOUS, FINDING_FLUCTUATION, RECORD_ROLES,
                    VIEW_ROLES, classify_readings, close_blockers, evaluate,
                    escalation_required, priority_score,
                    response_deadline_hours, role_for_transition,
                    validate_transition)


class Service:
    def __init__(self, repository: Repository):
        self.repository = repository
        # 串行化"提交批次→重算→写快照"，配合版本号保证两人同时处理时按最新版本重评
        self._submit_lock = threading.RLock()

    def _view(self, role: str) -> None:
        ensure_role(role, VIEW_ROLES)

    # ---------- 排放口与许可限值 ----------
    def register_outlet(self, payload: Dict[str, Any], actor: str, role: str) -> Dict[str, Any]:
        ensure_role(role, CREATE_ROLES)
        actor = require_text(actor, "actor", 100)
        code = require_text(payload.get("code"), "code", 64)
        name = require_text(payload.get("name"), "name", 200)
        pollutant = require_text(payload.get("pollutant"), "pollutant", 100)
        permit_limit = require_number(payload.get("permit_limit"), "permit_limit", 0.000001)
        unit = payload.get("unit")
        if unit is not None:
            unit = require_text(unit, "unit", 32)
        outlet = self.repository.create_outlet(code, name, pollutant, permit_limit, unit, actor)
        self.repository.append_audit("outlet_register", "排放口", outlet["id"], actor, {
            "code": code, "pollutant": pollutant, "permit_limit": permit_limit,
        })
        return outlet

    def list_outlets(self, role: str) -> list:
        self._view(role)
        return self.repository.list_outlets()

    # ---------- 事件（兼容原开立接口） ----------
    def create_item(self, payload: Dict[str, Any], actor: str, role: str) -> Dict[str, Any]:
        ensure_role(role, CREATE_ROLES)
        actor = require_text(actor, "actor", 100)
        title = require_text(payload.get("title"), "title", 200)
        description = require_text(payload.get("description"), "description")
        severity = normalize_severity(payload.get("severity"))
        quantity = require_number(payload.get("quantity", 0), "quantity")
        threshold = require_number(payload.get("threshold", 1), "threshold", 0.000001)
        external_ref = payload.get("external_ref")
        if external_ref is not None:
            external_ref = require_text(external_ref, "external_ref", 100)
        outlet_id = None
        permit_limit = None
        outlet_code = payload.get("outlet_code")
        if outlet_code is not None:
            outlet = self.repository.get_outlet_by_code(require_text(outlet_code, "outlet_code", 64))
            outlet_id = outlet["id"]
            permit_limit = outlet["permit_limit"]
        item = self.repository.create_item(title, description, severity, quantity,
                                           threshold, external_ref, actor,
                                           outlet_id=outlet_id, permit_limit=permit_limit)
        self.repository.append_audit("create", ENTITY, item["id"], actor, {
            "title": title, "severity": severity, "quantity": quantity,
            "priority": priority_score(severity, quantity, threshold),
        })
        return self.enrich(item)

    # ---------- 监测批次 ----------
    def submit_batch(self, payload: Dict[str, Any], actor: str, role: str) -> Dict[str, Any]:
        ensure_role(role, CREATE_ROLES)
        actor = require_text(actor, "actor", 100)
        outlet_code = require_text(payload.get("outlet_code"), "outlet_code", 64)
        batch_ref = require_text(payload.get("batch_ref"), "batch_ref", 100)
        kind = require_text(payload.get("kind"), "kind", 32)
        if kind not in BATCH_KINDS:
            from .domain import ValidationError
            raise ValidationError(f"kind必须是{','.join(BATCH_KINDS)}之一")
        outlet = self.repository.get_outlet_by_code(outlet_code)
        limit = float(outlet["permit_limit"])

        data = payload.get("data")
        if data is None or not isinstance(data, dict):
            from .domain import ValidationError
            raise ValidationError("data必须为对象")

        signal_payload: Dict[str, Any]
        if kind == "online":
            readings = data.get("readings")
            if not isinstance(readings, list) or not readings:
                from .domain import ValidationError
                raise ValidationError("online批次的data.readings必须是非空数组")
            clean = []
            for i, reading in enumerate(readings):
                if not isinstance(reading, dict):
                    from .domain import ValidationError
                    raise ValidationError(f"readings[{i}]必须为对象")
                value = require_number(reading.get("value"), f"readings[{i}].value", 0.0)
                point: Dict[str, Any] = {"value": value}
                ts = reading.get("ts")
                if ts is not None:
                    point["ts"] = require_text(str(ts), f"readings[{i}].ts", 40)
                clean.append(point)
            signal_payload = {"readings": clean}
        elif kind == "condition":
            status = require_text(data.get("status"), "data.status", 32)
            if status not in CONDITION_STATUSES:
                from .domain import ValidationError
                raise ValidationError(f"工况status必须是{','.join(CONDITION_STATUSES)}之一")
            note = data.get("note")
            signal_payload = {"status": status}
            if note is not None:
                signal_payload["note"] = require_text(note, "data.note", 2000)
        else:  # retest
            value = require_number(data.get("value"), "data.value", 0.0)
            note = data.get("note")
            signal_payload = {"value": value}
            if note is not None:
                signal_payload["note"] = require_text(note, "data.note", 2000)

        with self._submit_lock:
            # 重复批次只记一次
            existing = self.repository.get_batch_by_ref(batch_ref)
            if existing is not None:
                event = self.repository.get_item(existing["item_id"])
                self.repository.append_audit("batch_deduplicated", "批次", existing["id"], actor, {
                    "batch_ref": batch_ref, "item_id": event["id"],
                })
                return {"duplicated": True, "batch": existing,
                        "event": self.enrich(event),
                        "current_judgment": self.repository.current_judgment(event["id"])}

            # 新批次按排放口归到当前事件；没有未关闭事件则自动开立
            event = self.repository.find_open_item_for_outlet(outlet["id"])
            if event is None:
                event = self.repository.create_event_for_outlet(outlet, actor)
                self.repository.append_audit("event_auto_open", ENTITY, event["id"], actor, {
                    "outlet_code": outlet_code, "pollutant": outlet["pollutant"],
                })

            batch = self.repository.insert_batch(
                outlet["id"], event["id"], batch_ref, kind, signal_payload, actor)
            new_findings = []
            if kind == "online":
                # 连续异常合并，单次波动留下
                new_findings = classify_readings(signal_payload["readings"], limit)
                self.repository.insert_findings(batch["id"], event["id"], new_findings)

            # 工况或复测到达后重算严重度、整改期限和执法升级
            conclusion = self._recompute(event["id"], limit)
            judgment = self.repository.append_judgment(
                event["id"], conclusion, kind, batch["id"], actor)
            # 批次改变了当前结论，事件版本递增（并发处理的乐观锁）
            self.repository.bump_item_version(event["id"])
            self.repository.append_audit("batch_submit", "批次", batch["id"], actor, {
                "batch_ref": batch_ref, "kind": kind, "item_id": event["id"],
                "new_findings": new_findings,
                "judgment_version": judgment["judgment_version"],
                "severity": judgment["severity"], "enforcement": judgment["enforcement"],
                "deadline_hours": judgment["deadline_hours"],
            })
            refreshed = self.repository.get_item(event["id"])
            return {"duplicated": False, "batch": batch, "new_findings": new_findings,
                    "event": self.enrich(refreshed), "current_judgment": judgment}

    def _recompute(self, item_id: int, limit: Optional[float] = None) -> Dict[str, Any]:
        event = self.repository.get_item(item_id)
        if limit is None:
            limit = float(event["threshold"]) or 1.0
        findings = [
            {"kind": FINDING_CONTINUOUS if f["kind"] == "continuous" else FINDING_FLUCTUATION,
             "peak": f["peak"]}
            for f in self.repository.list_findings(item_id)
        ]
        condition_batch = self.repository.latest_signal(item_id, "condition")
        retest_batch = self.repository.latest_signal(item_id, "retest")
        prior = self.repository.current_judgment(item_id)
        return evaluate({
            "limit": limit,
            "findings": findings,
            "condition": condition_batch["payload"] if condition_batch else None,
            "retest": retest_batch["payload"] if retest_batch else None,
            "prior_severity": prior["severity"] if prior else event["severity"],
        })

    def recompute(self, item_id: int, actor: str, role: str) -> Dict[str, Any]:
        self._view(role)
        actor = require_text(actor, "actor", 100)
        event = self.repository.get_item(item_id)
        with self._submit_lock:
            conclusion = self._recompute(item_id)
            judgment = self.repository.append_judgment(item_id, conclusion, "manual", None, actor)
            self.repository.append_audit("judgment_recompute", ENTITY, item_id, actor, {
                "judgment_version": judgment["judgment_version"],
                "severity": judgment["severity"], "enforcement": judgment["enforcement"],
            })
        return self.enrich(self.repository.get_item(item_id))

    # ---------- 整改记录 ----------
    def add_record(self, item_id: int, payload: Dict[str, Any], actor: str,
                   role: str) -> Dict[str, Any]:
        ensure_role(role, RECORD_ROLES)
        actor = require_text(actor, "actor", 100)
        kind = require_text(payload.get("kind"), "kind", 100)
        detail = require_text(payload.get("detail"), "detail")
        status = payload.get("status", "open")
        if status not in ("open", "closed"):
            from .domain import ValidationError
            raise ValidationError("status必须是open或closed")
        external_ref = payload.get("external_ref")
        if external_ref is not None:
            external_ref = require_text(external_ref, "external_ref", 100)
        record = self.repository.add_record(item_id, kind, detail, status,
                                            external_ref, actor)
        self.repository.append_audit("record", ENTITY, item_id, actor, {
            "record_id": record["id"], "kind": kind, "status": status,
        })
        return record

    # ---------- 处置链流转 ----------
    def transition(self, item_id: int, target: str, expected_version: int,
                   actor: str, role: str) -> Dict[str, Any]:
        actor = require_text(actor, "actor", 100)
        item = self.repository.get_item(item_id)
        validate_transition(item["status"], target)
        ensure_role(role, role_for_transition(target))
        if not isinstance(expected_version, int) or expected_version < 1:
            from .domain import ValidationError
            raise ValidationError("expected_version必须是正整数")
        judgment = self.repository.current_judgment(item_id)
        blockers = close_blockers(judgment, self.repository.open_record_count(item_id)) \
            if target == "closed" else []
        if blockers:
            raise ConflictError("；".join(blockers))
        # expected_version是处理人评估时依据的版本；后到的人若拿到旧版本，须按最新版本重评
        updated = self.repository.transition_item(item_id, target, expected_version, actor)
        self.repository.append_audit("transition", ENTITY, item_id, actor, {
            "from": item["status"], "to": target,
            "expected_version": expected_version,
            "severity": judgment["severity"] if judgment else item["severity"],
            "enforcement": judgment["enforcement"] if judgment else None,
            "escalation_required": judgment["escalation_required"] if judgment else
                escalation_required(item["severity"], item["quantity"], item["threshold"]),
        })
        return self.enrich(updated)

    # ---------- 查询：详情只看当前结论，历史另查 ----------
    def get_item(self, item_id: int, role: str) -> Dict[str, Any]:
        self._view(role)
        item = self.enrich(self.repository.get_item(item_id))
        item["current_judgment"] = self.repository.current_judgment(item_id)
        item["open_records"] = self.repository.open_record_count(item_id)
        return item

    def list_items(self, role: str, status: Optional[str] = None) -> list:
        self._view(role)
        result = []
        for item in self.repository.list_items(status):
            enriched = self.enrich(item)
            judgment = self.repository.current_judgment(item["id"])
            enriched["current_judgment"] = judgment
            result.append(enriched)
        return result

    def list_records(self, item_id: int, role: str) -> list:
        self._view(role)
        return self.repository.list_records(item_id)

    def list_batches(self, item_id: Optional[int], role: str) -> list:
        self._view(role)
        return self.repository.list_batches(item_id)

    def list_findings(self, item_id: int, role: str) -> list:
        self._view(role)
        return self.repository.list_findings(item_id)

    def list_judgments(self, item_id: int, role: str) -> list:
        # 每次原始判定都保留：判定版本历史供审计角色回看
        self._view(role)
        return self.repository.list_judgments(item_id)

    def audit(self, role: str, item_id: Optional[int] = None) -> list:
        ensure_role(role, AUDIT_ROLES)
        return self.repository.list_audit(item_id)

    @staticmethod
    def enrich(item: Dict[str, Any]) -> Dict[str, Any]:
        result = dict(item)
        result["priority"] = priority_score(
            item["severity"], item["quantity"], item["threshold"])
        result["deadline_hours"] = response_deadline_hours(
            item["severity"], item["quantity"], item["threshold"])
        result["escalation_required"] = escalation_required(
            item["severity"], item["quantity"], item["threshold"])
        return result
