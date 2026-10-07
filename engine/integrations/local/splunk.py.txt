class FixtureSplunkAdapter:
    """Fixture-backed Splunk contract. Records may optionally be keyed by failure_code.

    A record without cluster_id/vm_id is a wildcard fixture for local integration tests.
    """
    def __init__(self, records): self.records=list(records)
    def search(self, params):
        matches=[]
        for r in self.records:
            if params.get('failure_code') and r.get('event_failure_code') and r.get('event_failure_code') != params['failure_code']:
                continue
            if params.get('domain') and r.get('domain') != params['domain']:
                continue
            if params.get('signal') and r.get('signal') != params['signal']:
                continue
            if r.get('cluster_id') and params.get('cluster_id') and r.get('cluster_id') != params['cluster_id']:
                continue
            if r.get('vm_id') and params.get('vm_id') and r.get('vm_id') != params['vm_id']:
                continue
            matches.append({**r, 'fact': r.get('fact', r.get('fact_code', 'UNKNOWN'))})
        return {'evidence':matches}
