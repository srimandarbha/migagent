import json
from .world import make_world
from engine.workflow.engine import run_agent

def simulate(name, pretty=False, mode='agent', provider=None, memory_mode='both'):
    scenario,registry,tracker,knowledge=make_world(name, memory_mode=memory_mode)
    cluster = 'ocv-prod-b' if name == 'vmware-cbt-retry' else 'ocv-prod-a'
    req={'failure_case_id':f'sim-{name}-001','memory_mode':memory_mode,'event':{'scenario':name,'phase':scenario['phase'],'message':scenario['message'],'cluster_id':cluster}}
    if name=='capability-error':
        # Deliberately omit diagnostic capability registration to test fail-closed behavior.
        pass
    state=run_agent(req,registry,tracker=tracker,knowledge=knowledge, memory_mode=memory_mode)
    out=state.to_dict()
    print('=== MIGRATION FAILURE AGENT SIMULATOR ===')
    print(f'Scenario       : {name}')
    print(f'Failure Case   : {req["failure_case_id"]}')
    print(f'Hidden truth   : {scenario["hidden_truth"]} (simulator only, not given to agent)')
    print(f'Classification : {state.classification} confidence={state.classification_confidence}')
    print(f'Diagnosis      : {state.diagnosis}')
    print('Decision readiness:')
    for action, item in state.decision_readiness.items(): print(f'  {action}: {item["status"]} blockers={item["blockers"]}')
    print(f'Next step      : {state.next_step}')
    print(f'Status         : {state.status}')
    print(f'Mode/Provider  : {mode}/{provider or "deterministic"}')
    print(f'Memory         : {memory_mode} history={len(state.historical_context)} knowledge={len(state.knowledge_context)}')
    if pretty: print(json.dumps(out,indent=2,default=str))
    return out
