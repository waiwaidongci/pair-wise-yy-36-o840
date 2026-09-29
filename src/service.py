from __future__ import annotations

import json
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from .domain import (ConflictError, ensure_role, normalize_severity, optional_text,
                     require_choice, require_dict, require_list,
                     require_number, require_text, ValidationError)
from .repository import Repository
from .rules import (AUDIT_ROLES, BATCH_ROLES, BATCH_TYPES, CONDITION_STATUSES,
                    CONTINUOUS_COUNT, CREATE_ROLES, ENTITY, RECORD_ROLES,
                    VIEW_ROLES, apply_condition, classify_online_group,
                    classify_reading, classify_retest, completion_blockers, escalation_level,
                    escalation_required, max_severity, priority_score,
                    response_deadline_hours, role_for_transition,
                    severity_rank, validate_transition)


class Service:
    def __init__(self, repository: Repository):
        self.repository = repository
        self._ingest_lock = threading.RLock()

    def _view(self, role: str) -> None:
        ensure_role(role, VIEW_ROLES)

    def _timestamp(self, value: Any, field: str) -> str:
        value = require_text(value, field, 100)
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).isoformat()
        except ValueError as exc:
            raise ValidationError(f"{field}必须是ISO-8601时间") from exc

    def _readings(self, payload: Dict[str, Any], fallback_at: str) -> List[Dict[str, Any]]:
        values = require_list(payload.get("readings"), "readings")
        if not values:
            raise ValidationError("readings不能为空")
        readings: List[Dict[str, Any]] = []
        for index, raw in enumerate(values, start=1):
            require_dict(raw, f"readings[{index}]")
            measured_at = raw.get("measured_at") or fallback_at
            readings.append({
                "seq": index,
                "measured_at": self._timestamp(measured_at, f"readings[{index}].measured_at"),
                "value": require_number(raw.get("value"), f"readings[{index}].value"),
            })
        return readings

    def create_item(self, payload: Dict[str, Any], actor: str, role: str) -> Dict[str, Any]:
        ensure_role(role, CREATE_ROLES)
        actor = require_text(actor, "actor", 100)
        outlet = optional_text(payload.get("outlet"), "outlet", 100)
        title = optional_text(payload.get("title"), "title", 200) or f"排放口 {outlet or '自动'} 事件"
        description = optional_text(payload.get("description"), "description") or \
            "监测、复测、工况与处置链条"
        severity = normalize_severity(payload.get("severity", "normal"))
        quantity = require_number(payload.get("quantity", 0), "quantity")
        threshold = require_number(payload.get("threshold", 1), "threshold", 0.000001)
        external_ref = payload.get("external_ref")
        if external_ref is not None:
            external_ref = require_text(external_ref, "external_ref", 100)
        with self._ingest_lock:
            if outlet and self.repository.find_active_item_by_outlet(outlet):
                raise ConflictError("该排放口已有未关闭事件")
            try:
                item = self.repository.create_item(
                    title, description, severity, quantity, threshold, actor,
                    outlet=outlet, external_ref=external_ref,
                )
            except ConflictError as exc:
                raise ConflictError("事件唯一标识或排放口已存在") from exc
        self.repository.append_audit("create", ENTITY, item["id"], actor, {
            "title": title, "outlet": outlet, "severity": severity,
            "quantity": quantity, "threshold": threshold,
        })
        return self.enrich(item)

    def ingest_batch(self, payload: Dict[str, Any], actor: str, role: str) -> Dict[str, Any]:
        ensure_role(role, BATCH_ROLES)
        actor = require_text(actor, "actor", 100)
        batch_ref = require_text(payload.get("batch_ref"), "batch_ref", 100)
        outlet = require_text(payload.get("outlet"), "outlet", 100)
        batch_type = require_choice(payload.get("batch_type"), "batch_type", BATCH_TYPES)
        measured_at = self._timestamp(
            payload.get("measured_at"), "measured_at",
        )

        with self._ingest_lock:
            duplicate = self.repository.get_batch_by_ref(batch_ref)
            if duplicate is not None:
                duplicate_outlet = (duplicate.get("payload") or {}).get("outlet")
                if duplicate_outlet != outlet:
                    raise ValidationError("batch_ref已属于其他排放口")
                return {
                    "duplicate": True,
                    "batch": duplicate,
                    "item": self.enrich(self.repository.get_item(duplicate["item_id"])),
                }

            limit_value = payload.get("permit_limit")
            if limit_value is not None:
                limit_value = require_number(limit_value, "permit_limit", 0.000001)
            active_item = self.repository.find_active_item_by_outlet(outlet)

            value: Optional[float] = None
            condition_status: Optional[str] = None
            readings: List[Dict[str, Any]] = []
            raw_judgment: str

            if batch_type == "permit":
                if limit_value is None:
                    raise ValidationError("permit批次必须提交permit_limit")
                raw_judgment = "permit_updated"
            elif batch_type in ("online",):
                readings = self._readings(payload, measured_at)
                values = [reading["value"] for reading in readings]
                value = max(values)
                if limit_value is None and active_item is None:
                    raise ValidationError("首批在线数据必须提交permit_limit")
                event_limit = limit_value or float(active_item["threshold"])
                limit_value = limit_value if limit_value is not None else event_limit
                for reading in readings:
                    reading["limit_value"] = limit_value
                    reading["raw_judgment"] = classify_reading(reading["value"], limit_value)
                raw_judgment = classify_online_group(value, limit_value, len(readings))
            elif batch_type == "retest":
                value = require_number(payload.get("value"), "value")
                if limit_value is None and active_item is None:
                    raise ValidationError("首批复测数据必须提交permit_limit")
                event_limit = limit_value or float(active_item["threshold"])
                limit_value = limit_value if limit_value is not None else event_limit
                raw_judgment = classify_retest(value, limit_value)
            else:
                condition_status = require_choice(
                    payload.get("condition_status"), "condition_status", CONDITION_STATUSES
                )
                if payload.get("value") is not None:
                    value = require_number(payload.get("value"), "value")
                event_limit = limit_value or (
                    float(active_item["threshold"]) if active_item else 1.0
                )
                limit_value = limit_value if limit_value is not None else event_limit
                raw_judgment = f"condition_{condition_status}"

            item = active_item
            created_event = False
            if item is None:
                now_title = f"排放口 {outlet} 事件"
                item = self.repository.create_item(
                    now_title, "由监测批次自动归并到当前排放口事件", "normal",
                    value or 0.0, limit_value or 1.0, actor, outlet=outlet,
                )
                created_event = True
            item_id = item["id"]

            stored_batch = self.repository.create_batch(
                item_id, batch_ref, batch_type, measured_at, raw_judgment, payload,
                actor, value=value, limit_value=limit_value,
                condition_status=condition_status,
            )
            for reading in readings:
                self.repository.add_reading(
                    stored_batch["id"], item_id, reading["seq"], reading["measured_at"],
                    reading["value"], reading["limit_value"], reading["raw_judgment"],
                    {"batch_ref": batch_ref, "value": reading["value"],
                     "limit_value": reading["limit_value"]},
                )

            if batch_type == "online":
                self._reconcile_online_episodes(item_id, stored_batch, actor, limit_value)
            elif batch_type == "retest":
                severity = classify_retest(value, limit_value)
                self.repository.add_record(
                    item_id, "retest",
                    f"{measured_at} 复测：{value} / {limit_value}，原始判定 {severity}",
                    "closed", f"RETEST-{batch_ref}", actor, batch_id=stored_batch["id"],
                )
                if severity == "normal":
                    self.repository.close_episode_records(item_id, actor, stored_batch["id"])
            elif batch_type == "condition":
                self.repository.add_record(
                    item_id, "condition",
                    f"{measured_at} 工况：{condition_status}",
                    "closed", f"CONDITION-{batch_ref}", actor, batch_id=stored_batch["id"],
                )
            elif batch_type == "permit":
                self.repository.add_record(
                    item_id, "permit", f"{measured_at} 许可限值更新为 {limit_value}",
                    "closed", f"PERMIT-{batch_ref}", actor, batch_id=stored_batch["id"],
                )
                self._reconcile_online_episodes(item_id, stored_batch, actor, limit_value)

            before = self.repository.get_item(item_id)
            conclusion = self._build_conclusion(item_id)
            severity = conclusion["severity"]
            quantity = conclusion["quantity"]
            threshold = conclusion["threshold"]
            open_records = self.repository.open_record_count(item_id)
            deadline_hours = response_deadline_hours(severity, quantity, threshold)
            conclusion.update({
                "priority": priority_score(severity, quantity, threshold, open_records),
                "deadline_hours": deadline_hours,
                "rectification_deadline": self._deadline_at(
                    conclusion["recalculated_at"], deadline_hours, severity),
                "escalation_level": escalation_level(severity),
                "escalation_required": escalation_required(severity, quantity, threshold),
                "open_records": open_records,
            })
            updated = self.repository.apply_conclusion(
                item_id, severity, quantity, threshold, conclusion, actor
            )
            self.repository.append_audit("batch_ingested", ENTITY, item_id, actor, {
                "batch_id": stored_batch["id"], "batch_ref": batch_ref,
                "batch_type": batch_type, "raw_judgment": raw_judgment,
                "severity_from": before["severity"], "severity_to": severity,
                "created_event": created_event,
            })
            return {
                "duplicate": False,
                "batch": self.repository.get_batch(stored_batch["id"], with_readings=True),
                "item": self.enrich(updated),
            }

    def _reconcile_online_episodes(self, item_id: int, current_batch: Dict[str, Any],
                                   actor: str, current_limit: float) -> None:
        """Re-derive runs over all online data; a normal reading closes the run before it."""
        all_readings = self.repository.list_online_readings(item_id)
        known = set(self.repository.list_episode_keys(item_id))
        run: List[Dict[str, Any]] = []

        def flush(closed_by_normal: bool = False) -> None:
            if not run:
                return
            peak = max(run, key=lambda r: r["value"] / current_limit)
            count = len(run)
            severity = classify_online_group(peak["value"], current_limit, count)
            kind = "continuous_anomaly" if count >= CONTINUOUS_COUNT else "single_fluctuation"
            # Single波动保留为历史证据；连续异常未复测前作为待处置事项。
            status = "closed" if closed_by_normal or count < CONTINUOUS_COUNT else "open"
            episode_key = f"ONLINE-{run[0]['id']}"
            self.repository.upsert_episode_record(
                item_id, episode_key, kind,
                {
                    "batch_ref": run[0]["payload"].get("batch_ref"),
                    "from": run[0]["measured_at"],
                    "to": run[-1]["measured_at"],
                    "peak_value": peak["value"],
                    "limit_value": current_limit,
                    "ratio": peak["value"] / current_limit,
                    "reading_count": count,
                    "judgment": severity,
                },
                status, actor, current_batch["id"],
            )
            known.discard(episode_key)

        for reading in all_readings:
            abnormal = reading["value"] > current_limit
            if abnormal:
                run.append(reading)
                continue
            if run:
                flush(closed_by_normal=True)
                run = []
        flush()

        # A permit batch re-derives every online episode under the new limit. Online
        # normal readings already close only the run immediately before them above.
        if current_batch["batch_type"] == "permit":
            for episode_key in known:
                self.repository.set_episode_status(item_id, episode_key, "closed", actor,
                                                   current_batch["id"])

    def _build_conclusion(self, item_id: int) -> Dict[str, Any]:
        item = self.repository.get_item(item_id)
        batches = self.repository.list_batches(item_id)
        originals = [batch for batch in batches if not batch.get("duplicate_of")]

        permit = [b for b in originals if b["batch_type"] == "permit" and b["limit_value"] is not None]
        threshold = permit[-1]["limit_value"] if permit else item["threshold"]

        latest_online = self._latest_batch(originals, "online")
        latest_retest = self._latest_batch(originals, "retest")
        latest_condition = self._latest_batch(originals, "condition")
        evidence = latest_retest or latest_online

        quantity = item["quantity"]
        base_severity = item["severity"]
        basis = "manual"
        evidence_batch_ref: Optional[str] = None
        episodes = self._episodes(item_id)

        if latest_online is not None:
            readings = latest_online.get("readings", [])
            if readings:
                peak = max(readings, key=lambda r: r["value"] / threshold)
                quantity = peak["value"]
                base_severity = classify_online_group(quantity, threshold, len(readings))
                basis = "online"
                evidence_batch_ref = latest_online["batch_ref"]

        if latest_retest is not None:
            quantity = latest_retest["value"] if latest_retest["value"] is not None else quantity
            base_severity = classify_retest(quantity, threshold)
            basis = "retest"
            evidence_batch_ref = latest_retest["batch_ref"]

        severity = base_severity
        if evidence is None and not permit:
            severity = "normal" if latest_condition is not None else item["severity"]
            if latest_condition is not None:
                basis = "condition"
                evidence_batch_ref = latest_condition["batch_ref"]
        elif evidence is None and permit:
            basis = "permit"
            evidence_batch_ref = permit[-1]["batch_ref"]
        condition_abnormal = bool(
            latest_condition and latest_condition.get("condition_status") == "abnormal"
        )
        if condition_abnormal:
            severity = apply_condition(severity, True)

        open_episodes = [e for e in episodes if e["status"] == "open"]
        if open_episodes and latest_retest is None:
            episode_severity = max_severity(*[e["judgment"] for e in open_episodes])
            severity = max_severity(severity, episode_severity)

        if severity == "normal" and open_episodes and latest_retest is None:
            # A still-open continuous anomaly remains visible even if the latest batch is near-limit.
            severity = "watch"
        if latest_retest is not None and classify_retest(quantity, threshold) == "normal":
            severity = "normal" if not condition_abnormal else "watch"

        return {
            "severity": severity,
            "quantity": float(quantity),
            "threshold": float(threshold),
            "basis": basis,
            "evidence_batch_ref": evidence_batch_ref,
            "condition_status": latest_condition.get("condition_status") if latest_condition else None,
            "condition_abnormal": condition_abnormal,
            "episodes": [episode for episode in episodes if episode["status"] == "open"],
            "recalculated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        }

    def _latest_batch(self, batches: List[Dict[str, Any]], batch_type: str):
        matching = [b for b in batches if b["batch_type"] == batch_type]
        return matching[-1] if matching else None

    def _episodes(self, item_id: int) -> List[Dict[str, Any]]:
        result = []
        for record in self.repository.list_records(item_id):
            if record.get("episode_key") is None:
                continue
            try:
                detail = json.loads(record["detail"])
            except (TypeError, json.JSONDecodeError):
                continue
            result.append({
                "episode_key": record["episode_key"],
                "kind": record["kind"],
                "status": record["status"],
                **detail,
            })
        return result

    def add_record(self, item_id: int, payload: Dict[str, Any], actor: str,
                   role: str) -> Dict[str, Any]:
        ensure_role(role, RECORD_ROLES)
        actor = require_text(actor, "actor", 100)
        kind = require_text(payload.get("kind"), "kind", 100)
        detail = require_text(payload.get("detail"), "detail")
        status = payload.get("status", "open")
        if status not in ("open", "closed"):
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

    def transition(self, item_id: int, target: str, expected_version: int,
                   actor: str, role: str) -> Dict[str, Any]:
        actor = require_text(actor, "actor", 100)
        item = self.repository.get_item(item_id)
        validate_transition(item["status"], target)
        ensure_role(role, role_for_transition(target))
        if isinstance(expected_version, bool) or not isinstance(expected_version, int) \
                or expected_version < 1:
            raise ValidationError("expected_version必须是正整数")
        blockers = completion_blockers(target, self.repository.open_record_count(item_id))
        if blockers:
            raise ConflictError("；".join(blockers))
        updated = self.repository.transition_item(item_id, target, expected_version, actor)
        self.repository.append_audit("transition", ENTITY, item_id, actor, {
            "from": item["status"], "to": target,
            "escalation_required": escalation_required(
                updated["severity"], updated["quantity"], updated["threshold"]),
        })
        return self.enrich(updated)

    def get_item(self, item_id: int, role: str) -> Dict[str, Any]:
        self._view(role)
        return self.enrich(self.repository.get_item(item_id))

    def list_items(self, role: str, status: Optional[str] = None,
                   outlet: Optional[str] = None) -> list:
        self._view(role)
        return [self.enrich(item) for item in self.repository.list_items(status, outlet)]

    def list_records(self, item_id: int, role: str) -> list:
        self._view(role)
        return self.repository.list_records(item_id)

    def list_batches(self, item_id: int, role: str) -> list:
        self._view(role)
        return self.repository.list_batches(item_id)

    def list_versions(self, item_id: int, role: str) -> list:
        self._view(role)
        return self.repository.list_versions(item_id)

    def audit(self, role: str, item_id: Optional[int] = None) -> list:
        ensure_role(role, AUDIT_ROLES)
        return self.repository.list_audit(item_id)

    @staticmethod
    def enrich(item: Dict[str, Any]) -> Dict[str, Any]:
        result = dict(item)
        threshold = float(item["threshold"])
        quantity = float(item["quantity"])
        result["priority"] = priority_score(
            item["severity"], quantity, threshold)
        result["deadline_hours"] = response_deadline_hours(
            item["severity"], quantity, threshold)
        result["escalation_required"] = escalation_required(
            item["severity"], quantity, threshold)
        result["escalation_level"] = escalation_level(item["severity"])
        conclusion = item.get("conclusion") or {}
        for key in ("priority", "deadline_hours", "escalation_level", "escalation_required"):
            if key in conclusion:
                result[key] = conclusion[key]
        result["current"] = {
            "severity": item["severity"],
            "quantity": quantity,
            "threshold": threshold,
            "priority": conclusion.get("priority", result["priority"]),
            "deadline_hours": conclusion.get("deadline_hours", result["deadline_hours"]),
            "rectification_deadline": conclusion.get("rectification_deadline"),
            "escalation_level": conclusion.get(
                "escalation_level", result["escalation_level"]),
            "escalation_required": conclusion.get(
                "escalation_required", result["escalation_required"]),
            "basis": conclusion.get("basis", "manual"),
            "evidence_batch_ref": conclusion.get("evidence_batch_ref"),
            "condition_status": conclusion.get("condition_status"),
            "condition_abnormal": bool(conclusion.get("condition_abnormal")),
            "episodes": conclusion.get("episodes", []),
        }
        deadline_at = conclusion.get("rectification_deadline") or Service._deadline_at(
            item["updated_at"], result["deadline_hours"], item["severity"])
        if deadline_at:
            result["rectification_deadline"] = deadline_at
        return result

    @staticmethod
    def _deadline_at(base_at: str, hours: int, severity: str) -> Optional[str]:
        if severity == "normal":
            return None
        try:
            base = datetime.fromisoformat(base_at.replace("Z", "+00:00"))
        except ValueError:
            return None
        if base.tzinfo is None:
            base = base.replace(tzinfo=timezone.utc)
        return (base + timedelta(hours=hours)).isoformat()
