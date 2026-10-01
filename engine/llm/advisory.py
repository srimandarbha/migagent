import json
import re

SYSTEM_PROMPT = """You are an advisory assistant for an SRE diagnosing VMware-to-OpenShift Virtualization migration failures.
You are NOT the source of truth. Current infrastructure evidence and deterministic policy outrank you.
Never invent evidence, Red Hat KB/CVE IDs, commands, metrics, or infrastructure state.
Do not propose remediation or execution. Return only investigation suggestions.
If evidence is insufficient, identify the smallest useful next read-only investigations.
Respond as JSON with keys: summary, suggested_investigations, uncertainty.
Each suggested_investigation must contain action, purpose, capability, parameters.
"""


def build_messages(state):
    evidence = []
    for e in state.evidence:
        evidence.append({
            'id': e.id, 'fact': e.fact, 'status': e.status.value,
            'source': e.source, 'domain': e.domain, 'signal': e.signal,
            'observed_at': e.observed_at,
        })
    concise_knowledge = [
        {'id': k.get('id'), 'title': k.get('title'), 'score': k.get('score')}
        for k in (state.knowledge_context or [])[:3]
    ]
    concise_history = [
        {'id': h.get('failure_case_id') or h.get('event_id'), 'resolution_code': h.get('resolution_code')}
        for h in (state.historical_context or [])[:3]
    ]
    event = state.event or {}
    payload = {
        'classification': state.classification,
        'classification_confidence': state.classification_confidence,
        'event': {
            'event_id': event.get('event_id'),
            'message': event.get('message'),
            'failure_code': event.get('failure_code'),
            'phase': event.get('phase'),
        },
        'evidence': evidence,
        'evidence_evaluation': state.evidence_evaluation,
        'historical_context': concise_history,
        'knowledge_context': concise_knowledge,
        'constraints': state.constraints,
    }
    return [
        {'role': 'system', 'content': SYSTEM_PROMPT},
        {'role': 'user', 'content': json.dumps(payload, default=str)},
    ]


def parse_advisory(text):
    raw = str(text or '').strip()
    raw = re.sub(r'<think>.*?</think>', '', raw, flags=re.DOTALL).strip()
    raw = re.sub(r'^```(?:json)?\s*', '', raw)
    raw = re.sub(r'\s*```$', '', raw)
    try:
        data = json.loads(raw)
    except Exception:
        start, end = raw.find('{'), raw.rfind('}')
        if start < 0 or end <= start:
            return {'status': 'INVALID', 'raw': raw}
        try:
            data = json.loads(raw[start:end + 1])
        except Exception:
            return {'status': 'INVALID', 'raw': raw}
    if not isinstance(data, dict):
        return {'status': 'INVALID', 'raw': raw}
    suggestions = data.get('suggested_investigations', [])
    if not isinstance(suggestions, list):
        suggestions = []
    safe = []
    for item in suggestions[:5]:
        if not isinstance(item, dict):
            continue
        safe.append({
            'action': str(item.get('action', 'COLLECT_TARGETED_EVIDENCE')),
            'purpose': str(item.get('purpose', 'Obtain additional read-only evidence.')),
            'capability': str(item.get('capability', 'observability.search')),
            'parameters': item.get('parameters') if isinstance(item.get('parameters'), dict) else {},
        })
    return {
        'status': 'ADVISORY',
        'summary': str(data.get('summary', '')),
        'suggested_investigations': safe,
        'uncertainty': str(data.get('uncertainty', '')),
    }
