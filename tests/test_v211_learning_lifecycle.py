from engine.workflow.engine import MigrationFailureEngine, run_agent
from engine.learning import LearningLifecycle
from engine.integrations.local.registry import build_local_registry
from engine.integrations.local.tracker import FixtureSRETrackerAdapter
from simulator.world import make_world


def _unknown_request(event_id):
    return {
        'failure_case_id': event_id,
        'memory_mode': 'sre',
        'event': {
            'event_id': event_id,
            'event_type': 'MigrationFailed',
            'failure_code': 'totally.new.migration.failure',
            'phase': 'transfer',
            'message': 'A previously unseen migration failure occurred',
            'cluster_id': 'ocv-learning-01',
            'migration_id': f'mig-{event_id}',
            'environment': {
                'target': {'cluster_id': 'ocv-learning-01', 'mtv_version': '2.11.0', 'ocp_version': '4.19.23'},
                'source': {'provider': 'vmware', 'vcenter_version': '8.0'},
                'migration': {'type': 'warm'},
            },
        },
    }


def test_brand_new_unknown_is_not_an_error_and_is_marked_knowledge_gap():
    _, registry, _, _ = make_world('unknown', memory_mode='none')
    state = run_agent(_unknown_request('unknown-001'), registry, tracker=None, knowledge=None, memory_mode='none')
    assert state.classification == 'UNKNOWN'
    assert state.recurrence['recurrence_status'] == 'FIRST_SEEN'
    assert state.learning['status'] == 'MEMORY_DISABLED'
    assert state.learning['knowledge_gap'] is True
    assert state.diagnosis['status'] == 'INSUFFICIENT_EVIDENCE'


def test_brand_new_unknown_with_tracker_no_data_is_new_failure():
    tracker = FixtureSRETrackerAdapter()
    _, registry, _, _ = make_world('unknown', memory_mode='none')
    state = run_agent(_unknown_request('unknown-002'), registry, tracker=tracker, knowledge=None, memory_mode='sre')
    assert state.recurrence['status'] == 'NO_DATA'
    assert state.recurrence['recurrence_status'] == 'FIRST_SEEN'
    assert state.learning['status'] == 'NEW_FAILURE'


def test_same_unknown_without_recorded_resolution_becomes_recurring_unresolved():
    tracker = FixtureSRETrackerAdapter()
    _, registry, _, _ = make_world('unknown', memory_mode='none')
    first = run_agent(_unknown_request('unknown-101'), registry, tracker=tracker, knowledge=None, memory_mode='sre')
    second = run_agent(_unknown_request('unknown-102'), registry, tracker=tracker, knowledge=None, memory_mode='sre')
    assert first.recurrence['recurrence_status'] == 'FIRST_SEEN'
    assert second.recurrence['recurrence_status'] == 'RECURRING_UNRESOLVED'
    assert second.recurrence['occurrence_count'] == 2
    assert first.failure_signature == second.failure_signature
    assert second.learning['status'] == 'KNOWLEDGE_GAP'
    assert second.learning['validated_solution_exists'] is False


def test_resolved_but_undocumented_fix_does_not_become_knowledge():
    tracker = FixtureSRETrackerAdapter()
    _, registry, _, _ = make_world('unknown', memory_mode='none')
    first = run_agent(_unknown_request('unknown-201'), registry, tracker=tracker, knowledge=None, memory_mode='sre')

    result = LearningLifecycle(tracker).record_resolution(
        failure_case_id=first.failure_case_id,
        resolution_code=None,
        description='SRE changed something outside the agent workflow; exact fix was not recorded.',
        outcome_status='RESOLVED',
        verification_status='PASSED',
        recorded_by='sre-user',
        failure_signature=first.failure_signature,
        failure_class=first.classification,
        failure_code='totally.new.migration.failure',
    )
    assert result['learning_status'] == 'NOT_LEARNABLE'
    assert tracker.learning_candidates == []

    second = run_agent(_unknown_request('unknown-202'), registry, tracker=tracker, knowledge=None, memory_mode='sre')
    assert second.recurrence['recurrence_status'] == 'RECURRING_RESOLVED'
    assert second.learning['status'] == 'RESOLVED_HISTORY_UNVALIDATED'


def test_verified_resolution_becomes_candidate_but_requires_human_validation():
    tracker = FixtureSRETrackerAdapter()
    _, registry, _, _ = make_world('unknown', memory_mode='none')
    first = run_agent(_unknown_request('unknown-301'), registry, tracker=tracker, knowledge=None, memory_mode='sre')
    result = LearningLifecycle(tracker).record_resolution(
        failure_case_id=first.failure_case_id,
        resolution_code='VMWARE.UNKNOWN.RECOVERY_001',
        description='Verified recovery action recorded by SRE.',
        outcome_status='RESOLVED',
        verification_status='PASSED',
        recorded_by='sre-user',
        failure_signature=first.failure_signature,
        failure_class=first.classification,
        failure_code='totally.new.migration.failure',
        diagnosis_code='UNKNOWN',
    )
    assert result['learning_status'] == 'CANDIDATE'
    assert result['candidate_id']

    second = run_agent(_unknown_request('unknown-302'), registry, tracker=tracker, knowledge=None, memory_mode='sre')
    assert second.recurrence['recurrence_status'] == 'RECURRING_RESOLVED'
    assert second.learning['validated_solution_exists'] is False


def test_human_validation_promotes_candidate_to_known_issue():
    tracker = FixtureSRETrackerAdapter()
    _, registry, _, _ = make_world('unknown', memory_mode='none')
    first = run_agent(_unknown_request('unknown-401'), registry, tracker=tracker, knowledge=None, memory_mode='sre')
    result = LearningLifecycle(tracker).record_resolution(
        failure_case_id=first.failure_case_id,
        resolution_code='VMWARE.UNKNOWN.RECOVERY_002',
        description='Verified and reviewed recovery action.',
        outcome_status='RESOLVED',
        verification_status='PASSED',
        recorded_by='sre-user',
        validated_by='sre-lead',
        validation_reason='Reviewed the evidence and confirmed the recorded resolution.',
        failure_signature=first.failure_signature,
        failure_class=first.classification,
        failure_code='totally.new.migration.failure',
        diagnosis_code='UNKNOWN',
    )
    assert result['learning_status'] == 'VALIDATED'
    second = run_agent(_unknown_request('unknown-402'), registry, tracker=tracker, knowledge=None, memory_mode='sre')
    assert second.recurrence['recurrence_status'] == 'KNOWN_ISSUE'
    assert second.learning['status'] == 'VALIDATED_KNOWLEDGE_AVAILABLE'
    assert second.recurrence['validated_solution_refs']


def test_current_cbt_golden_event_still_classifies_and_records_recurrence_metadata():
    scenario, registry, tracker, knowledge = make_world('vmware-cbt-retry', memory_mode='both')
    event = {
        'event_id': 'evt-v211-cbt-001',
        'event_type': 'MigrationFailed',
        'migration_id': 'mig-payroll-001',
        'vm_id': 'vm-payroll-01',
        'cluster_id': 'ocv-prod-17',
        'change_id': 'CHG0012345',
        'failure_code': 'VMWARE.CBT.RETRY_LIMIT',
        'failure_class': 'VMWARE_CBT',
        'severity': 'HIGH',
        'event_time': '2026-10-01T10:30:00Z',
        'environment': {
            'target': {'cluster_id': 'ocv-prod-17', 'ocp_version': '4.19.23', 'ocv_version': '4.19', 'mtv_version': '2.11.0'},
            'source': {'provider': 'vmware', 'vcenter_version': '8.0', 'esxi_versions': ['8.0']},
            'migration': {'type': 'warm', 'plan': 'payroll-wave-07'},
        },
        'message': 'VMware CBT retry limit exceeded during warm migration',
    }
    state = run_agent({'failure_case_id': 'evt-v211-cbt-001', 'memory_mode': 'both', 'event': event}, registry,
                      tracker=tracker, knowledge=knowledge, memory_mode='both')
    assert state.classification == 'VMWARE.CBT'
    assert state.skill_id == 'vmware/cbt'
    assert state.environment.migration_type == 'warm'
    assert state.failure_signature
    assert state.recurrence['recurrence_status'] == 'FIRST_SEEN'
    assert state.diagnosis['mechanism'] == 'VMWARE.CBT_STATE'
    assert state.evidence_evaluation['collection_status'] == 'COMPLETE'
    assert {e.fact for e in state.evidence} >= {'CBT_FAILED', 'TRANSFER_FAILED', 'CBT_QUERY_FAILED'}
