from datetime import datetime, timezone
from uuid import uuid4

class FixtureSRETrackerAdapter:
    """Local SRE Tracker adapter implementing the same operational contract used by the engine."""
    def __init__(self, cases=None, patterns=None):
        self.cases=list(cases or [])
        self.patterns=list(patterns or [])
        self.events={}
        self.evidence=[]
        self.diagnoses=[]
        self._case_by_event={str(r.get("event_id")): str(r.get("failure_case_id")) for r in self.cases if r.get("event_id")}

    def event_exists(self, event_id):
        return str(event_id) in self.events

    def create_or_get_failure_case(self, *, event_id, migration_id, vm_id=None, cluster_id=None, change_id=None, failure_class=None, failure_code=None, agent_version="", policy_version=None, severity=None, failure_case_id=None):
        event_id=str(event_id)
        if event_id in self._case_by_event:
            return self._case_by_event[event_id]
        case_id=str(failure_case_id or uuid4())
        self._case_by_event[event_id]=case_id
        self.cases.insert(0, {
            "failure_case_id": case_id, "event_id": event_id, "migration_id": migration_id,
            "vm_id": vm_id, "cluster_id": cluster_id, "change_id": change_id,
            "failure_class": failure_class, "failure_code": failure_code, "status": "NEW",
            "severity": severity, "agent_version": agent_version, "policy_version": policy_version,
        })
        return case_id

    def save_event(self, *, event_id, failure_case_id, event_type, event_time, payload, kafka_topic=None, kafka_partition=None, kafka_offset=None):
        self.events[str(event_id)]={
            "event_id":str(event_id), "failure_case_id":str(failure_case_id), "event_type":event_type,
            "event_time":event_time, "payload":payload, "kafka_topic":kafka_topic,
            "kafka_partition":kafka_partition, "kafka_offset":kafka_offset,
        }

    def save_evidence(self, case_id, evidence):
        evidence_id=str(evidence.get("id") or f"fixture-evidence:{case_id}:{len(self.evidence)+1}")
        self.evidence.append({"evidence_id":evidence_id,"failure_case_id":str(case_id),**evidence})
        return evidence_id

    def save_diagnosis(self, case_id, diagnosis, evidence_ids=None):
        diagnosis_id=str(uuid4())
        self.diagnoses.append({"diagnosis_id":diagnosis_id,"failure_case_id":str(case_id),"diagnosis":diagnosis,"evidence_ids":list(evidence_ids or [])})
        return diagnosis_id

    def search_failure_history(self, *, failure_class=None, cluster_id=None, limit=10):
        rows=[r for r in self.cases if (not failure_class or r.get('failure_class')==failure_class) and (not cluster_id or r.get('cluster_id')==cluster_id)]
        return rows[:limit]

    def search_periodic_patterns(self, *, failure_class=None, cluster_id=None, limit=20):
        rows=[r for r in self.patterns if (not failure_class or r.get('failure_class')==failure_class) and (not cluster_id or r.get('cluster_id')==cluster_id)]
        return rows[:limit]
