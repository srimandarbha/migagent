from datetime import datetime, timezone
from ..contracts import Evidence, EvidenceStatus
from ..integrations.registry import RegistryError, CapabilityNotFound


def _now():
    return datetime.now(timezone.utc).isoformat()


class InvestigationTool:
    def __init__(self, registry, tracker=None, knowledge=None):
        self.registry = registry
        self.tracker = tracker
        self.knowledge = knowledge

    def collect(self, state, requirement):
        capability_id = requirement.get('capability', requirement) if isinstance(requirement, dict) else requirement
        params = dict(requirement.get('parameters', {})) if isinstance(requirement, dict) else {}
        params.update({'failure_case_id': state.failure_case_id, **state.event})
        state.attempted_capabilities.append(capability_id)
        retrieved_at = _now()
        try:
            result = self.registry.invoke(capability_id, params, 'read', 'migration-failure')
        except CapabilityNotFound as exc:
            state.capability_errors.append(f'{capability_id}: {exc}')
            state.capability_results.append({
                'capability_id': capability_id, 'status': 'NOT_REGISTERED', 'capability_status': 'NOT_REGISTERED', 'result_status': 'NOT_REGISTERED',
                'domain': params.get('domain'), 'signal': params.get('signal'),
                'retrieved_at': retrieved_at, 'error': str(exc)})
            state.trace.append(f'capability result {capability_id}:NOT_REGISTERED')
            return
        except RegistryError as exc:
            state.capability_errors.append(f'{capability_id}: {exc}')
            state.capability_results.append({
                'capability_id': capability_id, 'status': EvidenceStatus.UNAVAILABLE.value, 'capability_status': EvidenceStatus.UNAVAILABLE.value, 'result_status': 'UNAVAILABLE',
                'domain': params.get('domain'), 'signal': params.get('signal'),
                'retrieved_at': retrieved_at, 'error': str(exc)})
            state.trace.append(f'capability result {capability_id}:UNAVAILABLE')
            return
        except Exception as exc:
            state.capability_errors.append(f'{capability_id}: {exc}')
            state.capability_results.append({
                'capability_id': capability_id, 'status': EvidenceStatus.ERROR.value, 'capability_status': EvidenceStatus.ERROR.value, 'result_status': 'ERROR',
                'domain': params.get('domain'), 'signal': params.get('signal'),
                'retrieved_at': retrieved_at, 'error': str(exc)})
            state.trace.append(f'capability result {capability_id}:ERROR')
            return

        result_status = str(result.get('status', '')).upper()
        if not result_status:
            result_status = EvidenceStatus.SUCCESS.value if result.get('evidence') else EvidenceStatus.NO_DATA.value
        if result_status == EvidenceStatus.VERIFIED.value:
            result_status = EvidenceStatus.SUCCESS.value
        items = result.get('evidence', []) or []
        result_semantics = 'FOUND' if items else 'NO_DATA'
        state.capability_results.append({
            'capability_id': capability_id, 'status': result_status, 'capability_status': result_status, 'result_status': result_semantics,
            'domain': params.get('domain'), 'signal': params.get('signal'),
            'retrieved_at': retrieved_at, 'error': result.get('error')})

        items = result.get('evidence', []) or []
        for item in items:
            status = str(item.get('status', result_status or EvidenceStatus.SUCCESS.value)).upper()
            if status == EvidenceStatus.VERIFIED.value:
                status = EvidenceStatus.SUCCESS.value
            try:
                evidence_status = EvidenceStatus(status)
            except ValueError:
                evidence_status = EvidenceStatus.UNKNOWN
            ev = Evidence(
                id=item.get('id', f'E-{len(state.evidence)+1}'),
                source=item.get('source', 'splunk' if capability_id == 'observability.search' else capability_id),
                fact=item.get('fact', item.get('fact_code', 'UNKNOWN')),
                status=evidence_status,
                observed_at=item.get('observed_at') or state.event.get('event_time'),
                retrieved_at=item.get('retrieved_at') or retrieved_at,
                confidence=float(item.get('confidence', 1.0)),
                metadata=item.get('metadata', {}),
                capability_id=capability_id,
                domain=item.get('domain', params.get('domain')),
                signal=item.get('signal', params.get('signal')),
                provenance={**item.get('provenance', {}), 'retrieved_at': item.get('retrieved_at') or retrieved_at},
            )
            state.evidence.append(ev)
            if self.tracker:
                try:
                    self.tracker.save_evidence(state.failure_case_id, ev.__dict__)
                except Exception as exc:
                    state.errors.append(f'tracker evidence persistence: {exc}')
        state.trace.append(f'investigate {capability_id}:{params.get("domain")}/{params.get("signal")}:{result_status}')
