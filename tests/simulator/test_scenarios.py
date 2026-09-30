import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parents[2]))
from simulator.world import make_world
from engine.workflow.engine import run_agent

def run(name):
    s,r,t,k=make_world(name); return s,run_agent({'failure_case_id':'T','event':{'scenario':name,'phase':s['phase'],'message':s['message'],'cluster_id':'ocv-prod-a'}},r,tracker=t,knowledge=k)

def test_storage_timeout():
    s,x=run('storage-csi-timeout'); assert x.classification==s['hidden_truth']; assert x.diagnosis['status']=='LIKELY'

def test_backend_healthy_not_backend_failure():
    s,x=run('storage-csi-backend-healthy'); assert 'backend outage' not in x.diagnosis['statement'].lower(); assert any(e.fact=='BACKEND_HEALTHY' for e in x.evidence)

def test_insufficient_evidence_fails_closed():
    s,x=run('insufficient-evidence'); assert x.diagnosis['status']=='INSUFFICIENT_EVIDENCE'; assert x.next_step=='COLLECT_MISSING_EVIDENCE'

def test_network():
    s,x=run('network-nad-missing'); assert x.classification==s['hidden_truth']

def test_vmware():
    s,x=run('vmware-cbt-retry'); assert x.classification==s['hidden_truth']


def test_explicit_failure_code_classifies_and_collects_evidence():
    from engine.integrations.local.registry import build_local_registry
    event={
        "event_id":"evt-local-001", "event_type":"MigrationFailed",
        "migration_id":"mig-123", "vm_id":"vm-456", "cluster_id":"ocv-prod-01",
        "failure_code":"storage.csi.provisioning_timeout",
        "severity":"critical", "phase":"PVC provisioning",
        "message":"DataVolume provisioning timed out",
    }
    registry, tracker=build_local_registry()
    # The fixture should contain a generic matching record for this documented local E2E event.
    state=run_agent({"failure_case_id":"evt-local-001","event":event}, registry=registry, tracker=tracker)
    assert state.classification == "STORAGE.CSI.PROVISIONING_TIMEOUT"
    assert state.skill_id == "storage/csi-provisioning-timeout"
    assert len(state.attempted_capabilities) >= 4
    assert len(state.evidence) >= 4
    assert state.diagnosis["status"] == "LIKELY"
    assert not any("create_or_get_failure_case" in e for e in state.errors)


def test_storage_analysis_has_hypothesis_and_specific_next_step():
    from engine.integrations.local.registry import build_local_registry
    event={"event_id":"evt-local-002","event_type":"MigrationFailed","migration_id":"mig-123","vm_id":"vm-456","cluster_id":"ocv-prod-01","failure_code":"storage.csi.provisioning_timeout","phase":"PVC provisioning","message":"DataVolume provisioning timed out"}
    registry, tracker=build_local_registry()
    state=run_agent({"failure_case_id":"evt-local-002","event":event}, registry=registry, tracker=tracker)
    assert any(h.code == "STORAGE.CSI_CONTROLLER_OR_PROVISIONING_PATH" for h in state.hypotheses)
    assert state.next_step == "REVIEW_CSI_CONTROLLER_FAILURE"


def test_evidence_contract_has_timestamps_and_explicit_capability_status():
    from engine.integrations.local.registry import build_local_registry
    event={"event_id":"evt-v262-contract","event_type":"MigrationFailed","event_time":"2026-09-30T18:00:00Z","migration_id":"mig-123","vm_id":"vm-456","cluster_id":"ocv-prod-01","failure_code":"storage.csi.provisioning_timeout","phase":"PVC provisioning","message":"DataVolume provisioning timed out"}
    registry, tracker=build_local_registry()
    state=run_agent({"failure_case_id":"evt-v262-contract","event":event}, registry=registry, tracker=tracker)
    assert all(e.observed_at for e in state.evidence)
    assert all(e.retrieved_at for e in state.evidence)
    assert all(r["status"] in {"SUCCESS","NO_DATA","UNKNOWN","UNAVAILABLE","ERROR"} for r in state.capability_results)
    assert any(r["capability_id"]=="metrics.query" and r["status"]=="NO_DATA" for r in state.capability_results)
    assert state.evidence_evaluation["status"] == "SUFFICIENT"


def test_diagnosis_confidence_is_derived_from_supported_hypothesis():
    from engine.integrations.local.registry import build_local_registry
    event={"event_id":"evt-v262-confidence","event_type":"MigrationFailed","event_time":"2026-09-30T18:00:00Z","migration_id":"mig-123","vm_id":"vm-456","cluster_id":"ocv-prod-01","failure_code":"storage.csi.provisioning_timeout","phase":"PVC provisioning","message":"DataVolume provisioning timed out"}
    registry, tracker=build_local_registry()
    state=run_agent({"failure_case_id":"evt-v262-confidence","event":event}, registry=registry, tracker=tracker)
    best=max(h.score for h in state.hypotheses if h.status=='SUPPORTED')
    assert state.diagnosis["confidence"] == best


def test_recovery_uses_migration_failure_evidence():
    from engine.integrations.local.registry import build_local_registry
    event={"event_id":"evt-v262-recovery","event_type":"MigrationFailed","event_time":"2026-09-30T18:00:00Z","migration_id":"mig-123","vm_id":"vm-456","cluster_id":"ocv-prod-01","failure_code":"storage.csi.provisioning_timeout","phase":"PVC provisioning","message":"DataVolume provisioning timed out"}
    registry, tracker=build_local_registry()
    state=run_agent({"failure_case_id":"evt-v262-recovery","event":event}, registry=registry, tracker=tracker)
    monitor=next(r for r in state.recovery if r.action=='CONTINUE_MONITOR')
    assert monitor.readiness.value == 'NOT_READY'
    assert any('failed state' in b.lower() for b in monitor.blockers)


def test_adaptive_investigation_collects_targeted_evidence():
    from engine.integrations.local.registry import build_local_registry
    event={"event_id":"evt-v270-adaptive","event_type":"MigrationFailed","event_time":"2026-09-30T18:00:00Z","migration_id":"mig-123","vm_id":"vm-456","cluster_id":"ocv-prod-01","failure_code":"storage.csi.provisioning_timeout","phase":"PVC provisioning","message":"DataVolume provisioning timed out"}
    registry, tracker=build_local_registry()
    state=run_agent({"failure_case_id":"evt-v270-adaptive","event":event}, registry=registry, tracker=tracker)
    assert state.evidence_round == 2
    assert any(e.fact == 'CSI_CONTROLLER_PROVISIONING_ERROR' for e in state.evidence)
    assert any(e.fact == 'PVC_PROVISIONING_FAILED' for e in state.evidence)
    assert any(e.fact == 'CSI_PROVISIONING_LATENCY_HIGH' for e in state.evidence)
    assert len(state.investigation_history) == 2
    assert state.evidence_evaluation['diagnosis_status'] == 'SUFFICIENT'
    assert state.diagnosis['status'] == 'LIKELY'
    assert state.diagnosis['confidence'] == 0.92
    assert state.next_step == 'REVIEW_CSI_CONTROLLER_FAILURE'


def test_adaptive_loop_stops_at_round_limit_when_targeted_evidence_unavailable():
    from engine.integrations.local.registry import build_local_registry
    event={"event_id":"evt-v270-blocked","event_type":"MigrationFailed","event_time":"2026-09-30T18:00:00Z","migration_id":"mig-123","vm_id":"vm-456","cluster_id":"ocv-prod-01","failure_code":"storage.csi.provisioning_timeout","phase":"PVC provisioning","message":"DataVolume provisioning timed out"}
    registry, tracker=build_local_registry()
    original=registry.capabilities['observability.search']
    def filtered(params):
        if params.get('signal') in {'csi_controller_errors','pvc_events'}:
            return {'status':'NO_DATA','evidence':[]}
        return original(params)
    registry.capabilities['observability.search']=filtered
    state=run_agent({"failure_case_id":"evt-v270-blocked","event":event}, registry=registry, tracker=tracker)
    assert state.diagnosis['status'] == 'INSUFFICIENT_EVIDENCE'
    assert state.evidence_round == 1
    assert state.next_step == 'COLLECT_TARGETED_EVIDENCE'
    assert state.evidence_evaluation['status'] in {'INSUFFICIENT','BLOCKED'}


def test_memory_modes_are_real_and_do_not_leak_current_case():
    from simulator.world import make_world
    from engine.workflow.engine import run_agent
    results={}
    for mode in ('both','sre','rhokp','none'):
        scenario, registry, tracker, knowledge = make_world('storage-csi-timeout', memory_mode=mode)
        state=run_agent({'failure_case_id':f'mem-{mode}','memory_mode':mode,'event':{'scenario':'storage-csi-timeout','phase':scenario['phase'],'message':scenario['message'],'cluster_id':'ocv-prod-a'}}, registry, tracker=tracker, knowledge=knowledge)
        results[mode]=state
    assert results['both'].memory_context['sre_tracker_enabled'] is True
    assert results['both'].memory_context['rhokp_enabled'] is True
    assert results['sre'].memory_context['rhokp_enabled'] is False
    assert results['rhokp'].memory_context['sre_tracker_enabled'] is False
    assert results['none'].historical_context == []
    assert results['none'].knowledge_context == []
    assert all(s.diagnosis['code']==results['none'].diagnosis['code'] for s in results.values())
    assert all(s.diagnosis['status']==results['none'].diagnosis['status'] for s in results.values())
    assert results['sre'].historical_context and not results['sre'].knowledge_context
    assert results['rhokp'].knowledge_context and not results['rhokp'].historical_context
    assert results['both'].historical_context and results['both'].knowledge_context
    assert not set(results['none'].diagnosis_basis['historical'])
    assert not set(results['none'].diagnosis_basis['knowledge'])


def test_recommendation_is_investigation_only():
    scenario, registry, tracker, knowledge = make_world('storage-csi-backend-healthy', memory_mode='both')
    state=run_agent({'failure_case_id':'recommendation-test','memory_mode':'both','event':{'scenario':'storage-csi-backend-healthy','phase':scenario['phase'],'message':scenario['message'],'cluster_id':'ocv-prod-a'}}, registry, tracker=tracker, knowledge=knowledge)
    assert state.recommendation['type']=='INVESTIGATION'
    assert 'do not execute remediation or retry' in ' '.join(state.recommendation['do_not_do']).lower()
    assert state.recommendation['approval_required'] is False
