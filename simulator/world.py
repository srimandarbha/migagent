from .scenarios import SCENARIOS
from engine.integrations.registry import InMemoryCapabilityRegistry, CapabilityNotFound
from engine.integrations.local.dataset import load_common_dataset
from engine.integrations.local.tracker import FixtureSRETrackerAdapter
from engine.integrations.local.rhokp import FixtureRHOKPRAGAdapter

# Local adapter: Splunk is the logical operational evidence source for OCV/MTV/VMware/Storage/Migration.
def make_world(name, memory_mode='both'):
    s=SCENARIOS.get(name,SCENARIOS['unknown'])
    records=[]
    for old_cap,facts in s.get('capabilities',{}).items():
        domain_signal={
            'storage.pvc_state':('ocv','pvc_state'),
            'storage.volume_state':('ocv','csi_errors'),
            'storage.backend_state':('storage','backend_health'),
            'network.nad_state':('ocv','nad_state'),
            'network.network_attachment_state':('ocv','network_attachment_state'),
            'vmware.cbt_state':('vmware','cbt_state'),
            'vmware.task_state':('vmware','task_state'),
            'vmware.transfer_errors':('mtv','transfer_errors'),
            'network.network_events':('ocv','network_events'),
            'vmware.esxi_connectivity':('vmware','esxi_connectivity'),
        }.get(old_cap)
        if domain_signal:
            domain,signal=domain_signal
            for fact in facts:
                records.append({'domain':domain,'signal':signal,'fact':fact,'source':'splunk','provenance':{'fixture':name}})
    # Migration state is a cross-domain operational record searchable in Splunk.
    records.append({'domain':'mtv','signal':'migration_state','fact':'FAILED','source':'splunk','provenance':{'fixture':name}})
    # Adaptive investigation fixtures. They are deliberately only available for the
    # backend-healthy path, because that scenario is intended to prove the agent can
    # select and execute targeted follow-up evidence rather than stop at the checklist.
    if name in {'storage-csi-backend-healthy','storage-csi-controller-error','storage-csi-conflict'}:
        records.extend([
            {'domain':'ocv','signal':'csi_controller_errors','fact':'CSI_CONTROLLER_PROVISIONING_ERROR','source':'splunk','provenance':{'fixture':name}},
            {'domain':'ocv','signal':'pvc_events','fact':'PVC_PROVISIONING_FAILED','source':'splunk','provenance':{'fixture':name}},
        ])

    def splunk(params):
        matches=[r for r in records if r['domain']==params.get('domain') and r['signal']==params.get('signal')]
        return {'evidence':matches}
    def prometheus(params):
        if name in {'storage-csi-backend-healthy','storage-csi-controller-error','storage-csi-conflict'} and params.get('domain') == 'ocv' and params.get('signal') == 'csi_provisioning_latency':
            return {'evidence':[{'id':'prom-adaptive-001','domain':'ocv','signal':'csi_provisioning_latency','fact':'CSI_PROVISIONING_LATENCY_HIGH','value':187.4,'unit':'seconds','source':'prometheus'}]}
        return {'evidence':[]}
    funcs={'observability.search':splunk,'metrics.query':prometheus}
    if name=='capability-error':
        funcs.pop('observability.search',None)
    data=load_common_dataset()
    cases=list(data.get('sre_tracker',{}).get('failure_cases',[]))
    patterns=list(data.get('sre_tracker',{}).get('periodic_patterns',[]))
    if name == 'storage-csi-conflict':
        cases.append({'failure_case_id':'conflict-history-001','event_id':'evt-conflict-001','migration_id':'mig-conflict-001','vm_id':'vm-conflict-001','cluster_id':'ocv-prod-a','failure_class':'STORAGE.CSI','failure_code':'PROVISIONING_TIMEOUT','status':'VERIFIED','resolution_code':'STORAGE_BACKEND_RECOVERY'})
    tracker=FixtureSRETrackerAdapter(cases, patterns)
    knowledge=FixtureRHOKPRAGAdapter(data.get('redhat_knowledge',[]))
    if memory_mode not in {'both','sre','rhokp','none'}: raise ValueError(f'unsupported memory mode: {memory_mode}')
    return s, InMemoryCapabilityRegistry(funcs), (tracker if memory_mode in {'both','sre'} else None), (knowledge if memory_mode in {'both','rhokp'} else None)
