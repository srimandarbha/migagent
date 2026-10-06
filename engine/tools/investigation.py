import os
import time
import concurrent.futures
from datetime import datetime, timezone
from ..contracts import Evidence, EvidenceStatus
from ..integrations.registry import RegistryError, CapabilityNotFound


def _now():
    return datetime.now(timezone.utc).isoformat()


class InvestigationTool:
    def __init__(self, registry, tracker=None, knowledge=None, timeout_seconds=None, failure_threshold=3, recovery_time_seconds=30.0):
        self.registry = registry
        self.tracker = tracker
        self.knowledge = knowledge
        env_timeout = os.environ.get('MFA_CAPABILITY_TIMEOUT_SECONDS')
        self.timeout_seconds = float(timeout_seconds if timeout_seconds is not None else (env_timeout or 5.0))
        self.failure_threshold = int(failure_threshold)
        self.recovery_time_seconds = float(recovery_time_seconds)
        self._consecutive_failures = {}
        self._circuit_opened_at = {}
        self.max_workers = int(os.environ.get('MFA_MAX_INVESTIGATION_WORKERS', 16))
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="mfa-tool-worker")

    def _is_circuit_open(self, capability_id: str) -> bool:
        opened_at = self._circuit_opened_at.get(capability_id)
        if opened_at is None:
            return False
        if time.time() - opened_at > self.recovery_time_seconds:
            self._circuit_opened_at.pop(capability_id, None)
            self._consecutive_failures[capability_id] = 0
            return False
        return True

    def _record_failure(self, capability_id: str):
        count = self._consecutive_failures.get(capability_id, 0) + 1
        self._consecutive_failures[capability_id] = count
        if count >= self.failure_threshold:
            self._circuit_opened_at[capability_id] = time.time()

    def _record_success(self, capability_id: str):
        self._consecutive_failures[capability_id] = 0
        self._circuit_opened_at.pop(capability_id, None)

    @staticmethod
    def _categorize_error(exc: Exception) -> str:
        msg = str(exc).lower()
        if any(w in msg for w in ('401', '403', 'unauthorized', 'forbidden', 'auth')):
            return 'AUTH_FAILURE'
        if any(w in msg for w in ('429', 'rate limit', 'throttl')):
            return 'RATE_LIMITED'
        if any(w in msg for w in ('timeout', 'timed out', 'deadline')):
            return 'TIMEOUT'
        if any(w in msg for w in ('connection refused', 'connect error', 'unreachable', 'econnrefused')):
            return 'CONNECTION_FAILURE'
        return 'UNKNOWN_ERROR'

    def collect(self, state, requirement):
        capability_id = requirement.get('capability', requirement) if isinstance(requirement, dict) else requirement
        allowed_event_fields = ('event_id', 'migration_id', 'vm_id', 'cluster_id', 'failure_code', 'severity', 'phase')
        params = {'failure_case_id': state.failure_case_id}
        if state.event:
            for k in allowed_event_fields:
                if k in state.event:
                    params[k] = state.event[k]
        if isinstance(requirement, dict) and requirement.get('parameters'):
            params.update(requirement['parameters'])
        state.attempted_capabilities.append(capability_id)
        retrieved_at = _now()

        if self._is_circuit_open(capability_id):
            state.capability_errors.append(f'{capability_id}: circuit breaker is OPEN (tripped after {self.failure_threshold} consecutive failures)')
            state.capability_results.append({
                'capability_id': capability_id,
                'status': EvidenceStatus.UNAVAILABLE.value,
                'capability_status': EvidenceStatus.UNAVAILABLE.value,
                'result_status': 'CIRCUIT_OPEN',
                'error_category': 'CIRCUIT_OPEN',
                'domain': params.get('domain'),
                'signal': params.get('signal'),
                'retrieved_at': retrieved_at,
                'error': f'Circuit breaker OPEN for {capability_id}'
            })
            state.trace.append(f'capability result {capability_id}:CIRCUIT_OPEN')
            return

        from ..integrations.contracts import GLOBAL_CONTRACT_REGISTRY
        if isinstance(requirement, dict) and 'parameters' in requirement:
            is_valid, err_msg = GLOBAL_CONTRACT_REGISTRY.validate(capability_id, params)
            if not is_valid:
                state.capability_errors.append(f'{capability_id}: {err_msg}')
                state.capability_results.append({
                    'capability_id': capability_id,
                    'status': 'ERROR',
                    'capability_status': 'ERROR',
                    'result_status': 'CONTRACT_INVALID',
                    'error_category': 'CONTRACT_INVALID',
                    'domain': params.get('domain'),
                    'signal': params.get('signal'),
                    'retrieved_at': retrieved_at,
                    'error': str(err_msg),
                })
                state.trace.append(f'capability result {capability_id}:CONTRACT_INVALID')
                return

        try:
            future = self._executor.submit(self.registry.invoke, capability_id, params, 'read', 'migration-failure')
            result = future.result(timeout=self.timeout_seconds)
            self._record_success(capability_id)
        except concurrent.futures.TimeoutError:
            self._record_failure(capability_id)
            try:
                future.cancel()
            except Exception:
                pass
            if getattr(self._executor, "_work_queue", None) and self._executor._work_queue.qsize() > 0:
                old_executor = self._executor
                self._executor = concurrent.futures.ThreadPoolExecutor(
                    max_workers=self.max_workers, thread_name_prefix="mfa-tool-worker"
                )
                try:
                    old_executor.shutdown(wait=False, cancel_futures=True)
                except Exception:
                    pass
            state.capability_errors.append(f'{capability_id}: invocation timed out after {self.timeout_seconds}s')
            state.capability_results.append({
                'capability_id': capability_id,
                'status': EvidenceStatus.UNAVAILABLE.value,
                'capability_status': EvidenceStatus.UNAVAILABLE.value,
                'result_status': 'TIMEOUT',
                'error_category': 'TIMEOUT',
                'domain': params.get('domain'),
                'signal': params.get('signal'),
                'retrieved_at': retrieved_at,
                'error': f"Capability execution timed out after {self.timeout_seconds}s"
            })
            state.trace.append(f'capability result {capability_id}:TIMEOUT')
            return
        except CapabilityNotFound as exc:
            state.capability_errors.append(f'{capability_id}: {exc}')
            state.capability_results.append({
                'capability_id': capability_id,
                'status': 'NOT_REGISTERED',
                'capability_status': 'NOT_REGISTERED',
                'result_status': 'NOT_REGISTERED',
                'error_category': 'NOT_FOUND',
                'domain': params.get('domain'),
                'signal': params.get('signal'),
                'retrieved_at': retrieved_at,
                'error': str(exc)
            })
            state.trace.append(f'capability result {capability_id}:NOT_REGISTERED')
            return
        except RegistryError as exc:
            self._record_failure(capability_id)
            category = self._categorize_error(exc)
            state.capability_errors.append(f'{capability_id}: {exc}')
            state.capability_results.append({
                'capability_id': capability_id,
                'status': EvidenceStatus.UNAVAILABLE.value,
                'capability_status': EvidenceStatus.UNAVAILABLE.value,
                'result_status': 'UNAVAILABLE',
                'error_category': category,
                'domain': params.get('domain'),
                'signal': params.get('signal'),
                'retrieved_at': retrieved_at,
                'error': str(exc)
            })
            state.trace.append(f'capability result {capability_id}:UNAVAILABLE')
            return
        except Exception as exc:
            self._record_failure(capability_id)
            category = self._categorize_error(exc)
            state.capability_errors.append(f'{capability_id}: {exc}')
            state.capability_results.append({
                'capability_id': capability_id,
                'status': EvidenceStatus.ERROR.value,
                'capability_status': EvidenceStatus.ERROR.value,
                'result_status': 'ERROR',
                'error_category': category,
                'domain': params.get('domain'),
                'signal': params.get('signal'),
                'retrieved_at': retrieved_at,
                'error': str(exc)
            })
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
            obs_time = item.get('observed_at') or state.event.get('event_time')
            freshness = None
            if obs_time and retrieved_at:
                try:
                    from datetime import datetime
                    dt_obs = datetime.fromisoformat(str(obs_time).replace('Z', '+00:00'))
                    dt_ret = datetime.fromisoformat(str(retrieved_at).replace('Z', '+00:00'))
                    freshness = max(0.0, (dt_ret - dt_obs).total_seconds())
                except Exception:
                    freshness = None

            ev = Evidence(
                id=item.get('id', f'E-{len(state.evidence)+1}'),
                source=item.get('source', 'splunk' if capability_id == 'observability.search' else capability_id),
                fact=item.get('fact', item.get('fact_code', 'UNKNOWN')),
                status=evidence_status,
                observed_at=obs_time,
                retrieved_at=item.get('retrieved_at') or retrieved_at,
                confidence=float(item.get('confidence', 1.0)),
                metadata=item.get('metadata', {}),
                capability_id=capability_id,
                domain=item.get('domain', params.get('domain')),
                signal=item.get('signal', params.get('signal')),
                provenance={**item.get('provenance', {}), 'retrieved_at': item.get('retrieved_at') or retrieved_at},
                resource=item.get('resource') or item.get('vm_id') or item.get('pvc') or state.event.get('vm_id'),
                value=item.get('value'),
                reliability=float(item.get('reliability', 1.0)),
                freshness_seconds=freshness if freshness is not None else item.get('freshness_seconds'),
                correlation_id=item.get('correlation_id') or state.event.get('migration_id'),
            )
            state.evidence.append(ev)
            if hasattr(state, 'observations'):
                state.observations.append(ev.to_observation())
            if self.tracker:
                try:
                    self.tracker.save_evidence(state.failure_case_id, ev.__dict__)
                except Exception as exc:
                    state.errors.append(f'tracker evidence persistence: {exc}')
        state.trace.append(f'investigate {capability_id}:{params.get("domain")}/{params.get("signal")}:{result_status}')

    def shutdown(self, wait: bool = False) -> None:
        if self._executor:
            try:
                self._executor.shutdown(wait=wait, cancel_futures=True)
            except Exception:
                pass

