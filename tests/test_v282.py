from simulator.runner import simulate
from simulator.world import make_world
from engine.workflow.engine import run_agent


def test_memory_isolation_and_enrichment():
    outputs = {m: simulate('storage-csi-controller-error', memory_mode=m) for m in ('both','sre','rhokp','none')}
    assert len({(o['classification'], o['diagnosis']['status'], o['diagnosis'].get('mechanism')) for o in outputs.values()}) == 1
    assert outputs['both']['recommendation']['memory_enrichment']
    assert any(x['source'] == 'SRE_TRACKER' for x in outputs['sre']['recommendation']['memory_enrichment'])
    assert any(x['source'] == 'RHOKP' for x in outputs['rhokp']['recommendation']['memory_enrichment'])
    assert outputs['none']['recommendation']['memory_enrichment'] == []


def test_current_evidence_beats_conflicting_history():
    out = simulate('storage-csi-conflict', memory_mode='both')
    assert out['diagnosis']['mechanism'] == 'STORAGE.CSI_CONTROLLER_OR_PROVISIONING_PATH'
    conflicts = [x for x in out['recommendation']['memory_enrichment'] if x.get('source') == 'MEMORY_CONFLICT']
    assert conflicts
    assert out['diagnosis_basis']['authoritative']
    assert out['diagnosis_basis']['historical']


def test_controller_state_unavailable_fails_closed():
    out = simulate('storage-csi-controller-healthy', memory_mode='none')
    assert out['diagnosis']['status'] == 'INSUFFICIENT_EVIDENCE'
    assert out['status'] == 'INSUFFICIENT_EVIDENCE'
    assert out['next_step'] == 'COLLECT_TARGETED_EVIDENCE'


def test_recommendation_is_diagnostic_only():
    out = simulate('network-nad-missing', memory_mode='both')
    rec = out['recommendation']
    assert rec['type'] == 'INVESTIGATION'
    assert rec['approval_required'] is False
    assert any('Do not' in x and 'remediat' in x and 'retry' in x for x in rec['do_not_do'])
    assert 'automation_reference' not in rec
    assert 'execution_status' not in rec


def test_redhat_memory_has_source_provenance():
    out = simulate('storage-csi-controller-error', memory_mode='rhokp')
    rh = [x for x in out['recommendation']['memory_enrichment'] if x.get('source') == 'RHOKP']
    assert rh
    assert all('id' in r and 'title' in r for r in rh[0]['records'])
