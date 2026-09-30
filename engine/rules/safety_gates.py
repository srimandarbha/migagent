from ..contracts import EvidenceStatus, Readiness
from .evidence_policy import required_for


def verified_capabilities(state):
    return {e.source for e in state.evidence if e.status == EvidenceStatus.SUCCESS}


def required_complete(state):
    required = required_for(state.classification)
    have = {(e.domain, e.signal) for e in state.evidence if e.status == EvidenceStatus.SUCCESS}
    missing = []
    for item in required:
        p = item.get("parameters", {})
        key = (p.get("domain"), p.get("signal"))
        if key not in have:
            missing.append(f'{item["capability"]}:{p.get("domain")}/{p.get("signal")}')
    return not missing, missing


def gate_recovery(state, action):
    ok, missing = required_complete(state)
    if not ok:
        return Readiness.UNKNOWN, [f'Required evidence not verified: {x}' for x in missing]

    facts = {e.fact for e in state.evidence if e.status == EvidenceStatus.SUCCESS}
    if action == 'RETRY':
        if state.evidence_evaluation.get('diagnosis_status') != 'SUFFICIENT':
            return Readiness.NOT_READY, ['Diagnosis is not sufficiently evidenced for a safe retry decision.']
        needed = {'PVC_BOUND', 'VOLUME_AVAILABLE', 'BACKEND_HEALTHY'}
        missing = sorted(needed - facts)
        return (Readiness.READY, []) if not missing else (Readiness.NOT_READY, [f'Retry precondition not verified: {x}' for x in missing])
    if action == 'FIX_FORWARD':
        return Readiness.CANDIDATE, ['Approved deterministic remediation procedure has not been evaluated.']
    if action == 'ROLLBACK':
        return Readiness.UNKNOWN, ['Rollback readiness capability/procedure is not evaluated.']
    if action == 'CONTINUE_MONITOR':
        if 'MIGRATION_FAILED' in facts or 'FAILED' in facts:
            return Readiness.NOT_READY, ['Migration is already in a failed state; current evidence does not support continue-monitor.']
        if 'MIGRATION_RUNNING' in facts or 'RUNNING' in facts:
            return Readiness.READY, []
        return Readiness.UNKNOWN, ['Current migration progress evidence is not sufficient to establish a running state.']
    if action == 'ESCALATE':
        return Readiness.CANDIDATE, []
    return Readiness.UNKNOWN, ['No safety policy for action.']
