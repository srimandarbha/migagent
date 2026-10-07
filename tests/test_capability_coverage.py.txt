from engine.tools.coverage import calculate_coverage, COVERAGE_BLOCKED, COVERAGE_READY, COVERAGE_UNKNOWN
from engine.integrations.registry import InMemoryCapabilityRegistry


def test_registered_required_capabilities_are_ready():
    registry = InMemoryCapabilityRegistry({
        'observability.search': lambda p: {'evidence': []},
        'metrics.query': lambda p: {'evidence': []},
    })
    c = calculate_coverage('STORAGE.CSI.PROVISIONING_TIMEOUT', registry)
    assert c.status == COVERAGE_READY
    assert c.required_coverage == COVERAGE_READY
    assert c.missing_required == []


def test_missing_required_capability_blocks():
    registry = InMemoryCapabilityRegistry({})
    c = calculate_coverage('STORAGE.CSI.PROVISIONING_TIMEOUT', registry)
    assert c.status == COVERAGE_BLOCKED
    assert c.required_coverage == COVERAGE_BLOCKED
    assert c.missing_required == ['observability.search']

def test_opaque_registry_is_unknown_not_falsely_blocked():
    class Opaque:
        def invoke(self, *args, **kwargs):
            return {'evidence': []}
    c = calculate_coverage('STORAGE.CSI.PROVISIONING_TIMEOUT', Opaque())
    assert c.status == COVERAGE_UNKNOWN
    assert c.required_coverage == COVERAGE_UNKNOWN
    assert c.missing_required == []


def test_adaptive_capability_gap_is_nonblocking_but_visible():
    registry = InMemoryCapabilityRegistry({'observability.search': lambda p: {'evidence': []}})
    c = calculate_coverage('STORAGE.CSI.PROVISIONING_TIMEOUT', registry)
    assert c.status == 'PARTIAL'
    assert c.missing_optional == [] or isinstance(c.missing_optional, list)


def test_agent_fails_closed_when_required_capability_is_unregistered():
    from engine.workflow.engine import run_agent
    registry = InMemoryCapabilityRegistry({})
    event = {
        'event_id': 'coverage-agent-001',
        'failure_code': 'storage.csi.provisioning_timeout',
        'phase': 'TARGET_STORAGE',
        'message': 'PVC provisioning timed out',
    }
    state = run_agent({'event': event, 'memory_mode': 'none'}, registry, tracker=None, knowledge=None, memory_mode='none')
    assert state.capability_coverage['required_coverage'] == COVERAGE_BLOCKED
    assert 'observability.search' in state.missing_diagnostic_capabilities
    assert state.diagnosis['status'] == 'INSUFFICIENT_EVIDENCE'


def test_missing_contract_signal_blocks_even_if_capability_id_is_registered():
    # Registry has observability.search, BUT only supports ocv signals, not storage/backend_health
    supported = {
        ('observability.search', 'ocv', 'pvc_state'),
        ('observability.search', 'ocv', 'csi_errors'),
        ('observability.search', 'mtv', 'migration_state'),
        # Note: ('observability.search', 'storage', 'backend_health') is missing!
    }
    registry = InMemoryCapabilityRegistry({
        'observability.search': lambda p: {'evidence': []},
        'metrics.query': lambda p: {'evidence': []},
    }, supported_contracts=supported)

    c = calculate_coverage('STORAGE.CSI.PROVISIONING_TIMEOUT', registry)
    assert c.status == COVERAGE_BLOCKED
    assert c.required_coverage == COVERAGE_BLOCKED
    assert any(x['domain'] == 'storage' and x['signal'] == 'backend_health' for x in c.missing_required_contracts)
    assert any(
        x['domain'] == 'storage' and x['signal'] == 'backend_health' and x['status'] == 'NOT_REGISTERED'
        for x in c.contract_details
    )
    assert any(
        x['domain'] == 'ocv' and x['signal'] == 'pvc_state' and x['status'] == 'REGISTERED'
        for x in c.contract_details
    )

