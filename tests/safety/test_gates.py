import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parents[2]))
from simulator.world import make_world
from engine.workflow.engine import run_agent

def test_retry_not_ready_without_preconditions():
    s,r,t,k=make_world('storage-csi-timeout'); x=run_agent({'incident_id':'T','event':{'scenario':'storage-csi-timeout','phase':s['phase'],'message':s['message']}},r,tracker=t,knowledge=k)
    retry=next(a for a in x.recovery if a.action=='RETRY'); assert retry.readiness.value=='NOT_READY'; assert retry.blockers
