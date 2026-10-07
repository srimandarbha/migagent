from engine.integrations.local.registry import build_local_registry
from engine.workflow.engine import MigrationFailureEngine


def _event():
    return {
        "event_id": "evt-v284-001",
        "event_type": "MigrationFailed",
        "event_time": "2026-09-30T18:00:00Z",
        "migration_id": "mig-v284-001",
        "vm_id": "vm-456",
        "cluster_id": "ocv-prod-a",
        "failure_code": "storage.csi.provisioning_timeout",
        "severity": "critical",
        "phase": "PVC provisioning",
        "message": "DataVolume provisioning timed out",
    }


def test_required_evidence_block_exposes_concrete_investigations():
    registry, tracker = build_local_registry()
    # Force a no-data registry so this specifically tests the blocked path.
    class EmptyRegistry:
        def invoke(self, capability_id, params, access='read', agent='migration-failure'):
            return {'evidence': []}

    state = MigrationFailureEngine(EmptyRegistry(), tracker=tracker, memory_mode='both').run({'event': _event()})
    assert state.status == 'INSUFFICIENT_EVIDENCE'
    assert state.next_step == 'COLLECT_MISSING_EVIDENCE'
    investigations = state.investigation_package['next_investigations']
    signals = {x['parameters']['signal'] for x in investigations}
    assert {'pvc_state', 'csi_errors', 'backend_health', 'migration_state'} <= signals


def test_local_kafka_fixture_path_can_produce_successful_current_evidence():
    registry, tracker = build_local_registry()
    state = MigrationFailureEngine(registry, tracker=tracker, memory_mode='both').run({'event': _event()})
    facts = {e.fact for e in state.evidence if e.status.value == 'SUCCESS'}
    assert {'PVC_PENDING', 'CSI_PROVISIONING_TIMEOUT', 'BACKEND_HEALTHY', 'MIGRATION_FAILED'} <= facts
    assert 'CSI_CONTROLLER_PROVISIONING_ERROR' in facts
    assert state.diagnosis['status'] == 'LIKELY'
    assert state.diagnosis['mechanism'] == 'STORAGE.CSI_CONTROLLER_OR_PROVISIONING_PATH'
    assert state.next_step == 'REVIEW_CSI_CONTROLLER_FAILURE'
