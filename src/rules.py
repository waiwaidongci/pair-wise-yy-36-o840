from __future__ import annotations

from .domain import ConflictError, ValidationError

TITLE = '企业排污许可与超标处置'
ENTITY = '排污事件'
ID_PREFIX = 'ED'

SEVERITIES = ['normal', 'watch', 'exceedance', 'major']
STATES = ['reported', 'assessing', 'remediation', 'inspection', 'closed']
TRANSITIONS = {
    'reported': ['assessing'],
    'assessing': ['remediation'],
    'remediation': ['inspection'],
    'inspection': ['closed'],
    'closed': [],
}
TRANSITION_ROLES = {
    'assessing': ['compliance_officer'],
    'remediation': ['operator'],
    'inspection': ['compliance_officer'],
    'closed': ['director'],
}
CREATE_ROLES = set(['operator', 'compliance_officer'])
RECORD_ROLES = set(['operator', 'compliance_officer'])
BATCH_ROLES = set(['operator', 'compliance_officer'])
AUDIT_ROLES = set(['director', 'viewer'])
VIEW_ROLES = set(['operator', 'compliance_officer', 'director', 'viewer'])

SEVERITY_WEIGHT = {'normal': 1.0, 'watch': 3.0, 'exceedance': 6.0, 'major': 9.0}
DEADLINE_HOURS = {'normal': 72, 'watch': 24, 'exceedance': 8, 'major': 4}
TERMINAL_STATES = set(['closed'])

BATCH_TYPES = ('permit', 'online', 'retest', 'condition')
CONDITION_STATUSES = ('normal', 'abnormal')
WATCH_RATIO = 0.8
MAJOR_RATIO = 2.0
CONTINUOUS_COUNT = 2


def severity_rank(severity: str) -> int:
    if severity not in SEVERITIES:
        raise ValidationError("unknown severity")
    return SEVERITIES.index(severity)


def max_severity(*values: str) -> str:
    return max(values, key=severity_rank) if values else SEVERITIES[0]


def escalate_severity(severity: str, steps: int = 1) -> str:
    return SEVERITIES[min(len(SEVERITIES) - 1, severity_rank(severity) + steps)]


def ratio_for(quantity: float, threshold: float) -> float:
    if threshold <= 0:
        return 1.0
    return float(quantity) / float(threshold)


def classify_ratio(ratio: float) -> str:
    """Original judgment for one measurement; grouping can soften a single spike."""
    if ratio <= WATCH_RATIO:
        return 'normal'
    if ratio <= 1.0:
        return 'watch'
    if ratio < MAJOR_RATIO:
        return 'exceedance'
    return 'major'


def classify_reading(quantity: float, threshold: float) -> str:
    return classify_ratio(ratio_for(quantity, threshold))


def classify_retest(quantity: float, threshold: float) -> str:
    """A retest at or under the permit limit is compliant; no online warning band applies."""
    ratio = ratio_for(quantity, threshold)
    if ratio <= 1.0:
        return 'normal'
    if ratio < MAJOR_RATIO:
        return 'exceedance'
    return 'major'


def classify_online_group(peak_quantity: float, threshold: float, reading_count: int) -> str:
    """Merge a continuous abnormal run while retaining one isolated fluctuation."""
    ratio = ratio_for(peak_quantity, threshold)
    if ratio <= 1.0:
        return 'normal'
    if reading_count < CONTINUOUS_COUNT:
        return 'watch'
    if ratio >= MAJOR_RATIO:
        return 'major'
    return 'exceedance'


def apply_condition(severity: str, abnormal: bool) -> str:
    if not abnormal:
        return severity
    if severity_rank(severity) < severity_rank('watch'):
        return 'watch'
    return escalate_severity(severity)


def priority_score(severity, quantity=0.0, threshold=1.0, open_records=0):
    if severity not in SEVERITY_WEIGHT:
        raise ValidationError("unknown severity")
    ratio = ratio_for(quantity, threshold)
    return max(0, min(10, int(round(
        SEVERITY_WEIGHT[severity] + min(4.0, ratio * 4.0) + min(3.0, float(open_records))
    ))))


def response_deadline_hours(severity, quantity=0.0, threshold=1.0):
    if severity not in DEADLINE_HOURS:
        raise ValidationError("unknown severity")
    ratio = ratio_for(quantity, threshold)
    return max(1, int(DEADLINE_HOURS[severity] / max(1.0, ratio)))


def escalation_level(severity: str) -> int:
    rank = severity_rank(severity)
    return max(0, rank - 1)


def escalation_required(severity, quantity=0.0, threshold=1.0):
    # Backward-compatible predicate; current conclusions primarily drive this by severity.
    return severity_rank(severity) >= severity_rank('exceedance') or (
        threshold > 0 and quantity >= threshold)


def can_transition(current, target):
    return target in TRANSITIONS.get(current, [])


def validate_transition(current, target):
    if current not in STATES or target not in STATES:
        raise ValidationError("未知状态")
    if not can_transition(current, target):
        raise ConflictError(f"不能从{current}转换到{target}")


def completion_blockers(target, open_records):
    return ["仍有未关闭事项"] if target in TERMINAL_STATES and open_records > 0 else []


def role_for_transition(target):
    return set(TRANSITION_ROLES.get(target, []))
