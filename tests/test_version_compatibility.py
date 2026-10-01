from engine.environment import from_event
from engine.knowledge_compatibility import evaluate, resolve_applicable


def env():
    return {
        'cluster_id': 'ocv-prod-01',
        'ocp_version': '4.19.23',
        'ocv_version': '4.19.23',
        'mtv_version': '2.10.4',
        'source_provider': 'vmware',
        'source_provider_version': '8.0',
    }


def doc(name, ocv=None, mtv=None, status=None):
    d={'id': name, 'title': name, 'product': 'migration-toolkit-for-virtualization'}
    app={}
    if ocv: app['ocv']={'versions': ocv}
    if mtv: app['mtv']={'versions': mtv}
    if status: app['status']=status
    if app: d['applicability']=app
    return d


def test_environment_fingerprint_reads_nested_event_contract():
    e=from_event({'cluster_id':'c1','environment':{'target':{'ocp_version':'4.19','ocv_version':'4.19','mtv_version':'2.10'},'source':{'provider':'vmware','vcenter_version':'8.0'}}})
    assert e.ocv_version == '4.19'
    assert e.mtv_version == '2.10'
    assert e.source_provider_version == '8.0'


def test_exact_supported_knowledge_is_selected():
    result=evaluate(env(), doc('mtv-210', ocv=['4.19'], mtv=['2.10']))
    assert result['status']=='SUPPORTED_EXACT'


def test_version_mismatch_is_excluded():
    result=evaluate(env(), doc('mtv-212', ocv=['4.20','4.21','4.22'], mtv=['2.12']))
    assert result['status']=='VERSION_MISMATCH'


def test_unknown_applicability_is_background_not_supported():
    result=evaluate(env(), doc('generic'))
    assert result['status']=='VERSION_UNKNOWN'


def test_resolver_never_promotes_mismatch_to_supported():
    result=resolve_applicable(env(), [
        doc('supported', ocv=['4.19'], mtv=['2.10']),
        doc('mismatch', ocv=['4.20'], mtv=['2.12']),
        doc('generic'),
    ])
    assert [x['id'] for x in result['supported']] == ['supported']
    assert [x['id'] for x in result['excluded']] == ['mismatch']
    assert [x['id'] for x in result['background']] == ['generic']


def doc_with_error(name, failure_code, mtv=None, mtv_min=None, mtv_max=None):
    d = doc(name, mtv=mtv)
    d['failure_code'] = failure_code
    if mtv_min is not None or mtv_max is not None:
        d['applicability'] = {'mtv': {'min': mtv_min, 'max': mtv_max}}
    return d


def test_older_solution_can_apply_to_higher_version_when_range_is_open_ended():
    from engine.knowledge_compatibility import evaluate_candidate
    current = dict(env(), mtv_version='2.11')
    result = evaluate_candidate(current, {'failure_code': 'vmware.cbt.retry_limit'}, doc_with_error('old-fix', 'vmware.cbt.retry_limit', mtv_min='2.8'))
    assert result['version_applicability']['status'] == 'SUPPORTED_RANGE'
    assert result['error_match']['status'] == 'MATCH'
    assert result['recommendation_status'] == 'ELIGIBLE'


def test_older_solution_is_not_assumed_valid_without_applicability_metadata():
    from engine.knowledge_compatibility import evaluate_candidate
    current = dict(env(), mtv_version='2.11')
    result = evaluate_candidate(current, {'failure_code': 'vmware.cbt.retry_limit'}, {
        'id': 'old-unknown', 'failure_code': 'vmware.cbt.retry_limit',
        'applicability': {'documented_version': '2.8'},
    })
    assert result['error_match']['status'] == 'MATCH'
    assert result['version_applicability']['status'] == 'VERSION_UNKNOWN'
    assert result['recommendation_status'] == 'NEEDS_VALIDATION'


def test_higher_version_solution_is_rejected_for_lower_current_version():
    from engine.knowledge_compatibility import evaluate_candidate
    current = dict(env(), mtv_version='2.10')
    result = evaluate_candidate(current, {'failure_code': 'vmware.cbt.retry_limit'}, doc_with_error('new-fix', 'vmware.cbt.retry_limit', mtv_min='2.12'))
    assert result['version_applicability']['status'] == 'VERSION_MISMATCH'
    assert result['recommendation_status'] == 'REJECT_VERSION'


def test_same_error_but_wrong_solution_error_signature_is_rejected():
    from engine.knowledge_compatibility import evaluate_candidate
    current = dict(env(), mtv_version='2.10')
    result = evaluate_candidate(current, {'failure_code': 'vmware.cbt.retry_limit'}, doc_with_error('storage-fix', 'storage.csi.provisioning_timeout', mtv_min='2.8'))
    assert result['error_match']['status'] == 'NO_MATCH'
    assert result['recommendation_status'] == 'REJECT_ERROR_MISMATCH'


def test_unknown_error_signature_never_becomes_eligible_solely_from_version_match():
    from engine.knowledge_compatibility import evaluate_candidate
    current = dict(env(), mtv_version='2.10')
    result = evaluate_candidate(current, {}, doc_with_error('old-fix', 'vmware.cbt.retry_limit', mtv_min='2.8'))
    assert result['error_match']['status'] == 'UNKNOWN'
    assert result['version_applicability']['status'] == 'SUPPORTED_RANGE'
    assert result['recommendation_status'] == 'NEEDS_VALIDATION'


def test_patch_versions_are_not_collapsed_to_major_minor():
    from engine.knowledge_compatibility import evaluate
    current = dict(env(), mtv_version='2.10.4')
    assert evaluate(current, doc('patch-a', mtv=['2.10.4']))['status'] == 'SUPPORTED_EXACT'
    assert evaluate(current, doc('patch-b', mtv=['2.10.5']))['status'] == 'VERSION_MISMATCH'


def test_multiple_environment_constraints_all_must_match():
    from engine.knowledge_compatibility import evaluate
    current = dict(env(), mtv_version='2.11', ocv_version='4.19.23')
    d = {
        'id': 'multi',
        'applicability': {
            'mtv': {'min': '2.8'},
            'ocv': {'min': '4.19', 'max': '4.20'},
            'vmware': {'min': '8.0'},
        },
    }
    assert evaluate(current, d)['status'] == 'SUPPORTED_RANGE'
    d['applicability']['ocv'] = {'min': '4.20'}
    assert evaluate(current, d)['status'] == 'VERSION_MISMATCH'
