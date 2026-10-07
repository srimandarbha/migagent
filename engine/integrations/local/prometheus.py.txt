class FixturePrometheusAdapter:
    def __init__(self, records): self.records=list(records)
    def query(self, params):
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
            matches.append(r)
        return {'evidence':matches}
