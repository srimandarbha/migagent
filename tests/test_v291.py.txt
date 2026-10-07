import os
from simulator.world import make_world
from engine.workflow.engine import run_agent
from engine.llm.advisory import parse_advisory

class FakeLLM:
    model='fake-model'
    def generate(self,messages,**kwargs):
        assert messages[0]['role']=='system'
        return '{"summary":"Need more read-only evidence.","suggested_investigations":[{"action":"INSPECT_ESXI_CONNECTIVITY","purpose":"Confirm endpoint reachability.","capability":"observability.search","parameters":{"domain":"vmware","signal":"esxi_connectivity"}}],"uncertainty":"Current evidence is insufficient."}'

def test_mtv009_both_executes():
    scenario, registry, tracker, knowledge = make_world('mtv-009-esxi-port443', memory_mode='both')
    event={'event_id':'test-mtv009','scenario':'mtv-009-esxi-port443','failure_code':'vmware.esxi.port443_unreachable','phase':scenario['phase'],'message':scenario['message'],'cluster_id':'ocv-prod-a'}
    state=run_agent({'event':event,'memory_mode':'both'},registry,tracker=tracker,knowledge=knowledge,memory_mode='both')
    assert state.classification=='VMWARE.ESXI.CONNECTIVITY'
    assert state.diagnosis['mechanism']=='VMWARE.ESXI_CONNECTIVITY'
    assert state.llm_advisory['status']=='NOT_REQUESTED'
    assert state.next_step=='INVESTIGATE_ESXI_CONNECTIVITY'

def test_llm_advisory_only_after_insufficient_evidence():
    scenario, registry, tracker, knowledge = make_world('mtv-009-esxi-port443', memory_mode='none')
    class NoData:
        def invoke(self,*args,**kwargs): return {'status':'NO_DATA','evidence':[]}
    event={'event_id':'test-mtv009-llm','scenario':'mtv-009-esxi-port443','failure_code':'vmware.esxi.port443_unreachable','phase':scenario['phase'],'message':scenario['message'],'cluster_id':'ocv-prod-a'}
    state=run_agent({'event':event,'memory_mode':'none'},NoData(),tracker=None,knowledge=None,memory_mode='none',llm_provider=FakeLLM())
    assert state.diagnosis['status']=='INSUFFICIENT_EVIDENCE'
    assert state.llm_advisory['status']=='ADVISORY'
    assert state.investigation_package['llm_advisory']['suggested_investigations'][0]['action']=='INSPECT_ESXI_CONNECTIVITY'

def test_llm_output_invalid_is_not_used_as_evidence():
    assert parse_advisory('not json')['status']=='INVALID'
