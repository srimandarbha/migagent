import json

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
    payload = {
        'classification': state.classification,
        'classification_confidence': state.classification_confidence,
        'event': state.context,
        'evidence': evidence,
        'evidence_evaluation': state.evidence_evaluation,
        'historical_context': state.historical_context,
        'knowledge_context': state.knowledge_context,
        'constraints': state.constraints,
    }
    return [
        {'role': 'system', 'content': SYSTEM_PROMPT},
        {'role': 'user', 'content': json.dumps(payload, default=str)},
    ]


def parse_advisory(text):
    raw = str(text or '').strip()
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
