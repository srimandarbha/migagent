import json
import re

SYSTEM_PROMPT = """You are an advisory assistant for an SRE diagnosing VMware-to-OpenShift Virtualization migration failures.
You are NOT the source of truth. Current infrastructure evidence and deterministic policy outrank you.
Never invent evidence, Red Hat KB/CVE IDs, commands, metrics, or infrastructure state.
Do not propose direct mutations or execution. Return only investigation suggestions.
If evidence is insufficient or the failure is uncataloged/unknown:
1. Synthesize a parametric technical hypothesis explaining the likely systems-level root mechanism (Linux kernel, SCSI/storage fabrics, virt-v2v, QEMU, KubeVirt, networking) based on the error message and systems engineering knowledge.
2. Identify the smallest useful next read-only investigations across approved capabilities (e.g. 'observability.search' with domain 'conversion', 'storage', 'ocv', 'vmware', 'mtv', 'guest_os' and a known signal).
3. Provide concrete read-only SRE diagnostic CLI commands (e.g. oc logs, dmesg, multipath) to inspect the cluster.

Respond strictly as JSON with keys:
- summary: Brief summary of the diagnostic state.
- parametric_hypothesis: Technical root-cause hypothesis from systems engineering knowledge when documentation is missing.
- suggested_investigations: Array of objects with keys: action, purpose, capability, parameters (must specify domain and signal).
- suggested_sre_diagnostics: Array of strings containing read-only diagnostic commands for the SRE.
- uncertainty: What remains unverified or ambiguous.
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


def parse_advisory(text, registry=None):
    from ..integrations.contracts import GLOBAL_CONTRACT_REGISTRY

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

    registered_caps = set()
    if registry is not None:
        for m in ('list_capabilities', 'available_capabilities'):
            fn = getattr(registry, m, None)
            if callable(fn):
                try:
                    registered_caps = {str(x) for x in fn()}
                    break
                except Exception:
                    pass
        if not registered_caps and hasattr(registry, 'capabilities'):
            registered_caps = set(registry.capabilities.keys())

    safe = []
    for item in suggestions[:5]:
        if not isinstance(item, dict):
            continue
        cap = str(item.get('capability', 'observability.search')).strip()
        params = item.get('parameters') if isinstance(item.get('parameters'), dict) else {}

        # Validate against capability contract schema
        is_schema_valid, schema_err = GLOBAL_CONTRACT_REGISTRY.validate(cap, params)
        is_registered = (cap in registered_caps) if registered_caps else True

        is_safe = is_schema_valid and is_registered
        validation_error = None
        if not is_schema_valid:
            validation_error = f"Schema violation: {schema_err}"
        elif not is_registered:
            validation_error = f"Capability '{cap}' is not registered in target environment"

        safe.append({
            'action': str(item.get('action', 'COLLECT_TARGETED_EVIDENCE')),
            'purpose': str(item.get('purpose', 'Obtain additional read-only evidence.')),
            'capability': cap,
            'parameters': params,
            'validated': is_safe,
            'validation_error': validation_error,
        })

    parametric_hypothesis = str(data.get('parametric_hypothesis', '')).strip()
    sre_diagnostics = data.get('suggested_sre_diagnostics', [])
    if not isinstance(sre_diagnostics, list):
        sre_diagnostics = [str(sre_diagnostics)] if sre_diagnostics else []
    sre_diagnostics = [str(cmd).strip() for cmd in sre_diagnostics if str(cmd).strip()]

    return {
        'status': 'ADVISORY',
        'summary': str(data.get('summary', '')),
        'parametric_hypothesis': parametric_hypothesis,
        'suggested_investigations': safe,
        'suggested_sre_diagnostics': sre_diagnostics,
        'uncertainty': str(data.get('uncertainty', '')),
    }

