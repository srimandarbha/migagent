import json
import re

SYSTEM_PROMPT = """You are an advisory assistant for an SRE diagnosing VMware-to-OpenShift Virtualization migration failures.
You are NOT the source of truth. Current infrastructure evidence and deterministic policy outrank you.
Never invent evidence, Red Hat KB/CVE IDs, commands, metrics, or infrastructure state.
Do not propose direct mutations or execution. Return only investigation suggestions.
If evidence is insufficient or the failure is uncataloged/unknown:
1. Synthesize a parametric technical hypothesis explaining the likely systems-level root mechanism (Linux kernel, SCSI/storage fabrics, virt-v2v, QEMU, KubeVirt, networking) based on the error message and systems engineering knowledge.
2. Identify the smallest useful next read-only investigations across approved capabilities (e.g. 'observability.search' with domain 'conversion', 'storage', 'ocv', 'vmware', 'mtv', 'guest_os' and a known signal).
3. Provide concrete read-only SRE diagnostic inspection targets or queries (e.g. oc logs, dmesg, multipath) to inspect the cluster without mutating state.

Respond strictly as JSON with keys:
- summary: Brief summary of the diagnostic state.
- parametric_hypothesis: Technical root-cause hypothesis from systems engineering knowledge when documentation is missing.
- suggested_investigations: Array of objects with keys: action, purpose, capability, parameters (must specify domain and signal).
- suggested_sre_diagnostics: Array of strings containing read-only diagnostic inspection queries or log targets for the SRE.
- uncertainty: What remains unverified or ambiguous.
"""

SRE_COMMAND_ALLOWLIST = (
    "oc get ",
    "oc describe ",
    "oc logs ",
    "dmesg ",
    "multipath -ll",
    "oc adm top ",
    "oc get events ",
)



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
    from ..security.sanitizer import sanitize_object
    sanitized_payload = sanitize_object(payload)
    return [
        {'role': 'system', 'content': SYSTEM_PROMPT},
        {'role': 'user', 'content': json.dumps(sanitized_payload, default=str)},
    ]


def parse_advisory(text, registry=None, policy_allowlist=None):
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

    registered_caps = None
    introspection_failed = False
    if registry is not None:
        introspected = False
        for m in ('list_capabilities', 'available_capabilities'):
            fn = getattr(registry, m, None)
            if callable(fn):
                introspected = True
                try:
                    registered_caps = {str(x) for x in fn()}
                    break
                except Exception:
                    introspection_failed = True
                    break
        if not introspected and not introspection_failed and hasattr(registry, 'capabilities'):
            try:
                registered_caps = set(registry.capabilities.keys())
                introspected = True
            except Exception:
                introspection_failed = True
        if not introspected and not introspection_failed:
            introspection_failed = True

    safe = []
    for item in suggestions[:5]:
        if not isinstance(item, dict):
            continue
        cap = str(item.get('capability', 'observability.search')).strip()
        params = item.get('parameters') if isinstance(item.get('parameters'), dict) else {}

        if introspection_failed:
            is_safe = False
            validation_error = "registry introspection failed"
        else:
            is_schema_valid, schema_err = GLOBAL_CONTRACT_REGISTRY.validate(cap, params)
            if registry is not None and (registered_caps is None or cap not in registered_caps):
                is_safe = False
                validation_error = f"Capability '{cap}' is not registered in target environment"
            elif not is_schema_valid:
                is_safe = False
                validation_error = f"Schema violation: {schema_err}"
            elif policy_allowlist is not None:
                from ..rules.evidence_policy import is_probe_allowed
                if not is_probe_allowed(cap, params, policy_allowlist):
                    is_safe = False
                    d = params.get("domain", "*")
                    s = params.get("signal", "*")
                    validation_error = f"Disallowed by incident policy allowlist for {cap}[domain={d},signal={s}]"
                else:
                    is_safe = True
                    validation_error = None
            else:
                is_safe = True
                validation_error = None

        safe.append({
            'action': str(item.get('action', 'COLLECT_TARGETED_EVIDENCE')),
            'purpose': str(item.get('purpose', 'Obtain additional read-only evidence.')),
            'capability': cap,
            'parameters': params,
            'validated': is_safe,
            'validation_error': validation_error,
        })

    parametric_hypothesis = str(data.get('parametric_hypothesis', '')).strip()
    sre_diagnostics_raw = data.get('suggested_sre_diagnostics', [])
    if not isinstance(sre_diagnostics_raw, list):
        sre_diagnostics_raw = [str(sre_diagnostics_raw)] if sre_diagnostics_raw else []

    filtered_count = 0
    sre_diagnostics = []
    for cmd in sre_diagnostics_raw:
        cmd_str = str(cmd).strip()
        if not cmd_str:
            continue
        cmd_lower = cmd_str.lower()
        if any(cmd_lower == p.strip() or cmd_lower.startswith(p.lower()) for p in SRE_COMMAND_ALLOWLIST):
            sre_diagnostics.append(f"# verify before running\n{cmd_str}")
        else:
            filtered_count += 1

    return {
        'status': 'ADVISORY',
        'summary': str(data.get('summary', '')),
        'parametric_hypothesis': parametric_hypothesis,
        'suggested_investigations': safe,
        'suggested_sre_diagnostics': sre_diagnostics,
        'sre_diagnostics_filtered': filtered_count,
        'uncertainty': str(data.get('uncertainty', '')),
    }

