import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parents[2]))
from simulator.world import make_world
from engine.workflow.engine import run_agent

def test_retry_not_ready_without_preconditions():
    s,r,t,k=make_world('storage-csi-timeout'); x=run_agent({'incident_id':'T','event':{'scenario':'storage-csi-timeout','phase':s['phase'],'message':s['message']}},r,tracker=t,knowledge=k)
    retry=next(a for a in x.recovery if a.action=='RETRY'); assert retry.readiness.value=='NOT_READY'; assert retry.blockers


def run_agent_from_scenario(name):
    scenario, registry, tracker, knowledge = make_world(name, memory_mode='none')
    req={'failure_case_id':f'gate-{name}', 'event':{'scenario':name,'phase':scenario['phase'],'message':scenario['message']}}
    return run_agent(req, registry, tracker=tracker, knowledge=knowledge, memory_mode='none')


def test_non_storage_retry_does_not_use_storage_preconditions():
    for scenario, forbidden in [
        ('vmware-cbt-retry', {'BACKEND_HEALTHY','PVC_BOUND','VOLUME_AVAILABLE'}),
        ('network-nad-missing', {'BACKEND_HEALTHY','PVC_BOUND','VOLUME_AVAILABLE'}),
        ('mtv-009-esxi-port443', {'BACKEND_HEALTHY','PVC_BOUND','VOLUME_AVAILABLE'}),
    ]:
        state = run_agent_from_scenario(scenario)
        retry = next(a for a in state.recovery if a.action == 'RETRY')
        assert not any(any(f in blocker for f in forbidden) for blocker in retry.blockers), retry.blockers
