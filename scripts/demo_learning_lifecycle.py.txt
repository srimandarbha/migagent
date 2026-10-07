"""Demonstrate first-seen -> recurring -> verified candidate -> validated knowledge."""
from simulator.world import make_world
from engine.workflow.engine import run_agent
from engine.learning import LearningLifecycle
from engine.integrations.local.tracker import FixtureSRETrackerAdapter

tracker=FixtureSRETrackerAdapter()
_, registry, _, _=make_world('unknown', memory_mode='none')

def request(event_id):
    return {'failure_case_id':event_id,'memory_mode':'sre','event':{
        'event_id':event_id,'event_type':'MigrationFailed','failure_code':'demo.unknown.failure',
        'phase':'transfer','message':'new migration failure','cluster_id':'ocv-learning-demo',
        'environment':{'target':{'mtv_version':'2.11.0'},'source':{'provider':'vmware'},'migration':{'type':'warm'}}}}

first=run_agent(request('demo-001'), registry, tracker=tracker, knowledge=None, memory_mode='sre')
print('FIRST:', first.recurrence['recurrence_status'], first.learning['status'])
second=run_agent(request('demo-002'), registry, tracker=tracker, knowledge=None, memory_mode='sre')
print('SECOND:', second.recurrence['recurrence_status'], second.learning['status'])

result=LearningLifecycle(tracker).record_resolution(
    failure_case_id=first.failure_case_id,
    resolution_code='DEMO.RECOVERY',
    description='Verified recovery performed by SRE.',
    outcome_status='RESOLVED', verification_status='PASSED', recorded_by='demo-sre',
    failure_signature=first.failure_signature, failure_class=first.classification,
    failure_code='demo.unknown.failure', diagnosis_code='UNKNOWN')
print('CANDIDATE:', result['learning_status'])

result=LearningLifecycle(tracker).record_resolution(
    failure_case_id=first.failure_case_id,
    resolution_code='DEMO.RECOVERY',
    description='Reviewed verified recovery.',
    outcome_status='RESOLVED', verification_status='PASSED', recorded_by='demo-sre',
    validated_by='demo-lead', validation_reason='Human review completed.',
    failure_signature=first.failure_signature, failure_class=first.classification,
    failure_code='demo.unknown.failure', diagnosis_code='UNKNOWN')
print('VALIDATED:', result['learning_status'])
third=run_agent(request('demo-003'), registry, tracker=tracker, knowledge=None, memory_mode='sre')
print('THIRD:', third.recurrence['recurrence_status'], third.learning['status'])
