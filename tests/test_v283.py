from simulator.runner import simulate


def test_structured_diagnosis_basis_for_controller_path():
    out = simulate('storage-csi-controller-error', memory_mode='none')
    basis = out['diagnosis_basis']
    assert basis['supporting_evidence']
    assert any(x['mechanism'] == 'STORAGE.CSI_CONTROLLER_OR_PROVISIONING_PATH' for x in basis['supporting_evidence'])
    assert any(x['mechanism'] == 'STORAGE.BACKEND_DEGRADED' for x in basis['excluded_mechanisms'])
    assert basis['excluded_mechanisms'][0]['evidence_ids']


def test_root_cause_is_not_invented():
    out = simulate('storage-csi-controller-error', memory_mode='none')
    assert out['diagnosis']['mechanism'] == 'STORAGE.CSI_CONTROLLER_OR_PROVISIONING_PATH'
    assert out['diagnosis'].get('root_cause') == 'UNKNOWN'


def test_investigation_package_is_actionable_and_read_only():
    out = simulate('storage-csi-controller-error', memory_mode='none')
    package = out['investigation_package']
    assert package['priority'] == 'HIGH'
    actions = {x['action'] for x in package['next_investigations']}
    assert 'INVESTIGATE_CSI_CONTROLLER_ERRORS' in actions
    assert all(x['required_capability'] for x in package['next_investigations'])
    assert package['do_not_do']
    assert any('retry' in x.lower() for x in package['do_not_do'])
    assert package['success_conditions']


def test_decision_readiness_separates_remediation():
    out = simulate('storage-csi-controller-error', memory_mode='none')
    dr = out['decision_readiness']
    assert dr['RETRY']['status'] == 'NOT_READY'
    assert dr['ROLLBACK']['status'] == 'UNKNOWN'
    assert dr['REMEDIATION']['status'] == 'NOT_EVALUATED'
    assert dr['REMEDIATION']['execution'] == 'NOT_PERFORMED'


def test_unknown_and_insufficient_are_distinct():
    unknown = simulate('unknown', memory_mode='none')
    insufficient = simulate('storage-csi-controller-healthy', memory_mode='none')
    assert unknown['classification'] == 'UNKNOWN'
    assert unknown['next_step'] == 'COLLECT_MISSING_EVIDENCE'
    assert insufficient['classification'] == 'STORAGE.CSI.PROVISIONING_TIMEOUT'
    assert insufficient['next_step'] == 'COLLECT_TARGETED_EVIDENCE'
    assert unknown['diagnosis']['status'] == 'INSUFFICIENT_EVIDENCE'
    assert insufficient['diagnosis']['status'] == 'INSUFFICIENT_EVIDENCE'


def test_memory_enriches_without_changing_diagnosis():
    outputs = {m: simulate('storage-csi-controller-error', memory_mode=m) for m in ('both', 'sre', 'rhokp', 'none')}
    signatures = {(o['classification'], o['diagnosis']['mechanism'], o['diagnosis']['status']) for o in outputs.values()}
    assert len(signatures) == 1
    assert any(x['source'] == 'SRE_TRACKER' for x in outputs['sre']['recommendation']['memory_enrichment'])
    assert any(x['source'] == 'RHOKP' for x in outputs['rhokp']['recommendation']['memory_enrichment'])
    assert outputs['none']['recommendation']['memory_enrichment'] == []


def test_memory_conflict_is_explicit_and_current_evidence_wins():
    out = simulate('storage-csi-conflict', memory_mode='both')
    assert out['diagnosis']['mechanism'] == 'STORAGE.CSI_CONTROLLER_OR_PROVISIONING_PATH'
    conflicts = [x for x in out['recommendation']['memory_enrichment'] if x.get('source') == 'MEMORY_CONFLICT']
    assert conflicts
    assert any(x.get('role') == 'CONTEXT' for x in out['diagnosis_basis']['memory_context'])
