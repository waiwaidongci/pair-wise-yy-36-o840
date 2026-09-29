from __future__ import annotations
from .domain import ConflictError, ValidationError
TITLE='企业排污许可与超标处置'; ENTITY='排污事件'; ID_PREFIX='ED'
SEVERITIES=['normal', 'watch', 'exceedance', 'major']; STATES=['reported', 'assessing', 'remediation', 'inspection', 'closed']; TRANSITIONS={'reported': ['assessing'], 'assessing': ['remediation'], 'remediation': ['inspection'], 'inspection': ['closed'], 'closed': []}; TRANSITION_ROLES={'assessing': ['compliance_officer'], 'remediation': ['operator'], 'inspection': ['compliance_officer'], 'closed': ['director']}
CREATE_ROLES=set(['operator', 'compliance_officer']); RECORD_ROLES=set(['operator', 'compliance_officer']); AUDIT_ROLES=set(['director', 'viewer']); VIEW_ROLES=set(['operator', 'compliance_officer', 'director', 'viewer'])
SEVERITY_WEIGHT={'normal': 1.0, 'watch': 3.0, 'exceedance': 6.0, 'major': 9.0}; DEADLINE_HOURS={'normal': 72, 'watch': 24, 'exceedance': 8, 'major': 4}; TERMINAL_STATES=set(['closed'])

# 监测批次类型：连续在线数据 / 工况变化 / 复测；legacy仅用于旧数据升级回填
BATCH_KINDS=['online', 'condition', 'retest', 'legacy']
CONDITION_STATUSES=['normal', 'abnormal']
# 执法升级阶梯：无 → 告诫 → 责令整改 → 立案处罚
ENFORCEMENT_LADDER=['none', 'notice', 'rectification_order', 'penalty']
ENFORCEMENT_BY_SEVERITY={'normal': 'none', 'watch': 'notice', 'exceedance': 'rectification_order', 'major': 'penalty'}
ESCALATION_ENFORCEMENT=set(['rectification_order', 'penalty'])
FINDING_CONTINUOUS='continuous'; FINDING_FLUCTUATION='fluctuation'

def priority_score(severity,quantity=0.0,threshold=1.0,open_records=0):
    if severity not in SEVERITY_WEIGHT: raise ValidationError("unknown severity")
    ratio=quantity/threshold if threshold>0 else 1.0
    return max(0,min(10,int(round(SEVERITY_WEIGHT[severity]+min(4.0,ratio*4.0)+min(3.0,float(open_records))))))
def response_deadline_hours(severity,quantity=0.0,threshold=1.0):
    if severity not in DEADLINE_HOURS: raise ValidationError("unknown severity")
    ratio=quantity/threshold if threshold>0 else 1.0
    return max(1,int(DEADLINE_HOURS[severity]/max(1.0,ratio)))
def escalation_required(severity,quantity=0.0,threshold=1.0):
    return severity==SEVERITIES[-1] or (threshold>0 and quantity>=threshold)
def can_transition(current,target): return target in TRANSITIONS.get(current,[])
def validate_transition(current,target):
    if current not in STATES or target not in STATES: raise ValidationError("未知状态")
    if not can_transition(current,target): raise ConflictError(f"不能从{current}转换到{target}")

def classify_readings(readings, limit):
    """连续在线数据按许可限值判异：连续异常合并为一个发现，孤立单点记为单次波动。

    readings: [{"ts": 可选, "value": 数值}]，异常口径为 value>limit（等于限值不算超标）。
    连续指按时间排序后相邻异常读数；异常段长度>=2合并，长度==1保留为波动。
    """
    ordered=sorted(enumerate(readings), key=lambda pair: (pair[1].get('ts') is None, str(pair[1].get('ts')), pair[0]))
    findings=[]; run=[]
    def flush(segment):
        if not segment: return
        peak=max(float(point['value']) for point in segment)
        if len(segment)>=2:
            findings.append({'kind': FINDING_CONTINUOUS, 'start_ts': segment[0].get('ts'),
                             'end_ts': segment[-1].get('ts'), 'peak': peak,
                             'count': len(segment), 'merged': True})
        else:
            only=segment[0]
            findings.append({'kind': FINDING_FLUCTUATION, 'start_ts': only.get('ts'),
                             'end_ts': only.get('ts'), 'peak': peak,
                             'count': 1, 'merged': False})
    for _, point in ordered:
        if float(point['value'])>float(limit): run.append(point)
        else: flush(run); run=[]
    flush(run)
    return findings

def evaluate(state):
    """依据事件当前全部证据重算严重度、整改期限与执法升级，返回可落库的判定结论。"""
    limit=float(state['limit'])
    findings=state.get('findings') or []
    condition=state.get('condition'); retest=state.get('retest')
    prior=state.get('prior_severity', 'normal')
    if prior not in SEVERITY_WEIGHT: prior='normal'

    retest_exceed=bool(retest) and float(retest.get('value', 0.0))>limit
    # 复测合格则此前的在线异常视为已解除；复测数据本身仍留痕在快照里
    resolved=bool(retest) and not retest_exceed
    active=[] if resolved else findings
    condition_abnormal=bool(condition) and condition.get('status')=='abnormal'

    n_continuous=sum(1 for f in active if f['kind']==FINDING_CONTINUOUS)
    n_fluctuation=sum(1 for f in active if f['kind']==FINDING_FLUCTUATION)
    peaks=[float(f['peak']) for f in active]
    if retest_exceed: peaks.append(float(retest['value']))
    peak=max(peaks, default=0.0)
    ratio=peak/limit if limit>0 else 0.0

    if retest_exceed or n_continuous>0:
        severity='exceedance'
    elif n_fluctuation>0:
        severity='watch'  # 单次波动留下，只升级到关注，不按连续超标处理
    else:
        severity='normal'
    # 连续异常峰值超2倍、两段以上连续异常、复测仍超标且此前已超标、超标叠加工况异常 → 重大
    is_major=(n_continuous>0 and ratio>=2.0) or n_continuous>=2 \
        or (retest_exceed and prior in ('exceedance', 'major')) \
        or (severity=='exceedance' and condition_abnormal)
    if is_major: severity='major'
    # 复测合格但此前曾超标：保留观察，待主管关闭
    if resolved and prior in ('exceedance', 'major') and severity=='normal':
        severity='watch'

    enforcement=ENFORCEMENT_BY_SEVERITY[severity]
    if retest_exceed:  # 复测仍超标，执法再升一级（到顶为立案处罚）
        idx=min(len(ENFORCEMENT_LADDER)-1, ENFORCEMENT_LADDER.index(enforcement)+1)
        enforcement=ENFORCEMENT_LADDER[idx]

    unresolved=n_continuous>0 or retest_exceed or (condition_abnormal and ratio>=1.0) or (severity=='major' and (findings or retest_exceed))
    reasons=[]
    if n_continuous: reasons.append(f"连续异常{n_continuous}段")
    if n_fluctuation: reasons.append(f"单次波动{n_fluctuation}次")
    if condition_abnormal: reasons.append("工况异常")
    if retest_exceed: reasons.append("复测仍超标")
    if resolved: reasons.append("复测合格，原异常解除")
    if not reasons: reasons.append("无有效异常证据")
    return {
        'severity': severity, 'peak_value': peak, 'permit_limit': limit, 'ratio': round(ratio, 4),
        'n_continuous': n_continuous, 'n_fluctuation': n_fluctuation,
        'condition_status': condition.get('status') if condition else None,
        'retest_value': float(retest['value']) if retest else None,
        'retest_exceed': retest_exceed, 'resolved': resolved,
        'unresolved': unresolved, 'enforcement': enforcement,
        'escalation_required': enforcement in ESCALATION_ENFORCEMENT,
        'deadline_hours': response_deadline_hours(severity, peak if peak>0 else 0.0, limit),
        'rationale': '；'.join(reasons),
    }

def close_blockers(judgment, open_records):
    """关闭不变量：不得有未完成整改项，且当前结论不得仍有未解除的超标。"""
    blockers=[]
    if open_records and int(open_records)>0: blockers.append("仍有未关闭整改事项")
    if judgment and judgment.get('unresolved'): blockers.append("当前结论仍有未解除超标，须复测合格")
    return blockers

def completion_blockers(target,open_records): return close_blockers(None, open_records) if target in TERMINAL_STATES else []
def role_for_transition(target): return set(TRANSITION_ROLES.get(target,[]))
