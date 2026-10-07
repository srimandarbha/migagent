from engine.knowledge_compatibility import evaluate_candidate, judge_eligibility, apply_judge_verdict


def env():
    return {'mtv_version':'2.11','ocv_version':'4.19','ocp_version':'4.19','source_provider_version':'8.0'}


def candidate(mtv=None, error='VMWARE.CBT.RETRY_LIMIT'):
    d={'id':'kb-1','title':'old solution','failure_code':error}
    d['applicability']={'mtv': {'documented_version':'2.8'} if mtv is None else {'min':mtv}}
    return d


def test_unknown_candidate_is_eligible_for_judge():
    c=candidate()
    c['compatibility']=evaluate_candidate(env(), {'failure_code':'VMWARE.CBT.RETRY_LIMIT'}, c)
    assert c['compatibility']['recommendation_status']=='NEEDS_VALIDATION'
    assert judge_eligibility(env(), {'failure_code':'VMWARE.CBT.RETRY_LIMIT'}, [c]) == [c]


def test_explicit_version_mismatch_is_never_sent_to_judge():
    c=candidate('2.12')
    c['compatibility']=evaluate_candidate({'mtv_version':'2.10','ocv_version':'4.19','ocp_version':'4.19','source_provider_version':'8.0'}, {'failure_code':'VMWARE.CBT.RETRY_LIMIT'}, c)
    assert c['compatibility']['recommendation_status']=='REJECT_VERSION'
    assert judge_eligibility({'mtv_version':'2.10'}, {'failure_code':'VMWARE.CBT.RETRY_LIMIT'}, [c]) == []


def test_judge_can_promote_unknown_to_candidate_but_not_hard_reject():
    c=candidate()
    c['compatibility']=evaluate_candidate(env(), {'failure_code':'VMWARE.CBT.RETRY_LIMIT'}, c)
    result=apply_judge_verdict([c], {'status':'JUDGED','verdict':'LIKELY_APPLICABLE','confidence':0.82})
    assert result[0]['compatibility']['recommendation_status']=='CANDIDATE_APPLICABLE'


def test_judge_cannot_override_error_mismatch():
    c=candidate(error='STORAGE.CSI.PROVISIONING_TIMEOUT')
    c['compatibility']=evaluate_candidate(env(), {'failure_code':'VMWARE.CBT.RETRY_LIMIT'}, c)
    assert c['compatibility']['recommendation_status']=='REJECT_ERROR_MISMATCH'
    result=apply_judge_verdict([c], {'status':'JUDGED','verdict':'LIKELY_APPLICABLE','confidence':0.99})
    assert result[0]['compatibility']['recommendation_status']=='REJECT_ERROR_MISMATCH'


def test_judge_cannot_override_version_mismatch():
    c=candidate('2.12')
    c['compatibility']=evaluate_candidate({'mtv_version':'2.10'}, {'failure_code':'VMWARE.CBT.RETRY_LIMIT'}, c)
    result=apply_judge_verdict([c], {'status':'JUDGED','verdict':'LIKELY_APPLICABLE','confidence':0.99})
    assert result[0]['compatibility']['recommendation_status']=='REJECT_VERSION'


class FakeJudgeProvider:
    model = 'fake-judge-v1'
    def generate(self, messages, **kwargs):
        return '{"verdict":"LIKELY_APPLICABLE","confidence":0.83,"basis":["same structured failure code","SRE history shows successful use on the current version"],"validation_needed":["verify current component behavior"]}'


def test_engine_judge_node_records_advisory_and_candidate_state():
    from engine.workflow.engine import MigrationFailureEngine
    from engine.workflow.nodes.implementation import MigrationFailureNodes
    from simulator.runner import make_world
    scenario, registry, tracker, knowledge = make_world('vmware-cbt-retry')
    engine = MigrationFailureEngine(registry, tracker=tracker, knowledge=knowledge, memory_mode='both', llm_provider=FakeJudgeProvider())
    nodes = MigrationFailureNodes(engine)
    state = engine._create_state({'failure_case_id':'judge-node-1','event':{
        'event_id':'judge-node-1','scenario':'vmware-cbt-retry','phase':scenario['phase'],
        'message':scenario['message'],'cluster_id':'ocv-prod-a','failure_code':'VMWARE.CBT.RETRY_LIMIT'
    }})
    state.classification='VMWARE.CBT'
    state.environment = __import__('engine.environment', fromlist=['EnvironmentFingerprint']).EnvironmentFingerprint(mtv_version='2.11', ocv_version='4.19', ocp_version='4.19', source_provider='vmware', source_provider_version='8.0')
    state.knowledge_candidates=[{'id':'old-kb','title':'MTV 2.8 CBT workaround','failure_code':'VMWARE.CBT.RETRY_LIMIT','applicability':{'documented_version':'2.8'}}]
    state.knowledge_candidates[0]['compatibility']={'error_match':{'status':'MATCH'},'version_applicability':{'status':'VERSION_UNKNOWN'},'recommendation_status':'NEEDS_VALIDATION'}
    out=nodes.judge_knowledge_applicability({'agent_state':state})['agent_state']
    assert out['knowledge_judge']['status']=='COMPLETED'
    assert out['knowledge_judge']['results'][0]['verdict']=='LIKELY_APPLICABLE'
    assert out['knowledge_candidates'][0]['compatibility']['recommendation_status']=='CANDIDATE_APPLICABLE'


def test_engine_judge_never_receives_explicit_version_mismatch():
    from engine.workflow.engine import MigrationFailureEngine
    from engine.workflow.nodes.implementation import MigrationFailureNodes
    from simulator.runner import make_world
    scenario, registry, tracker, knowledge = make_world('vmware-cbt-retry')
    class ExplodingProvider(FakeJudgeProvider):
        def generate(self, messages, **kwargs):
            raise AssertionError('hard reject must not reach LLM judge')
    engine = MigrationFailureEngine(registry, tracker=tracker, knowledge=knowledge, memory_mode='both', llm_provider=ExplodingProvider())
    nodes=MigrationFailureNodes(engine)
    state=engine._create_state({'failure_case_id':'judge-node-2','event':{'event_id':'judge-node-2','scenario':'vmware-cbt-retry','message':scenario['message']}})
    state.classification='VMWARE.CBT'
    state.environment=__import__('engine.environment', fromlist=['EnvironmentFingerprint']).EnvironmentFingerprint(mtv_version='2.10')
    c={'id':'future-kb','failure_code':'VMWARE.CBT.RETRY_LIMIT','applicability':{'mtv':{'min':'2.12'}}}
    from engine.knowledge_compatibility import evaluate_candidate
    c['compatibility']=evaluate_candidate(state.environment.to_dict(), {'failure_code':'VMWARE.CBT.RETRY_LIMIT'}, c)
    state.knowledge_candidates=[c]
    out=nodes.judge_knowledge_applicability({'agent_state':state})['agent_state']
    assert out['knowledge_judge']['status']=='NOT_REQUESTED'
