from datetime import datetime, timezone
from uuid import uuid4


class FixtureSRETrackerAdapter:
    """Local SRE Tracker adapter implementing the operational contract."""
    def __init__(self, cases=None, patterns=None):
        self.cases=list(cases or [])
        self.patterns=list(patterns or [])
        self.events={}
        self.evidence=[]
        self.diagnoses=[]
        self.learning_candidates=[]
        self.dynamic_learning_candidates=[]
        self._case_by_event={str(r.get("event_id")): str(r.get("failure_case_id")) for r in self.cases if r.get("event_id")}

    def event_exists(self, event_id):
        event = self.events.get(str(event_id))
        return bool(event and event.get("status") == "COMPLETED")

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
            "recurrence_status": "FIRST_SEEN", "occurrence_number": 1,
        })
        return case_id

    def save_event(self, *, event_id, failure_case_id, event_type, event_time, payload, kafka_topic=None, kafka_partition=None, kafka_offset=None, status="RECEIVED"):
        self.events[str(event_id)]={
            "event_id":str(event_id), "failure_case_id":str(failure_case_id), "event_type":event_type,
            "event_time":event_time, "payload":payload, "kafka_topic":kafka_topic,
            "kafka_partition":kafka_partition, "kafka_offset":kafka_offset,
            "status": status,
        }

    def mark_event_completed(self, event_id):
        event_id = str(event_id)
        if event_id in self.events:
            self.events[event_id]["status"] = "COMPLETED"
            self.events[event_id]["completed_at"] = datetime.now(timezone.utc).isoformat()

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

    def get_failure_case(self, case_id):
        for row in self.cases:
            if str(row.get('failure_case_id')) == str(case_id):
                return row
        return None

    def update_case_metadata(self, case_id, *, failure_signature=None, evidence_signature=None,
                             recurrence_status=None, occurrence_number=None):
        for row in self.cases:
            if str(row.get('failure_case_id')) == str(case_id):
                if failure_signature is not None: row['failure_signature']=failure_signature
                if evidence_signature is not None: row['evidence_signature']=evidence_signature
                if recurrence_status is not None: row['recurrence_status']=recurrence_status
                if occurrence_number is not None: row['occurrence_number']=occurrence_number
                return

    def correlate_failure_signature(self, *, signature, failure_case_id=None, classification=None, cluster_id=None):
        rows=[r for r in self.cases
              if r.get('failure_signature') == signature
              and str(r.get('failure_case_id')) != str(failure_case_id)
              and (not classification or r.get('failure_class') == classification)
              and (not cluster_id or r.get('cluster_id') == cluster_id)]
        previous_ids=[str(r.get('failure_case_id')) for r in rows]
        outcomes=[]
        for r in rows:
            outcomes.extend(r.get('resolutions', []))
        validated=[r for r in self.learning_candidates if r.get('failure_signature')==signature and r.get('status')=='VALIDATED']
        resolved_case_ids = {
            str(r.get('failure_case_id')) for r in outcomes
            if str(r.get('outcome_status', '')).upper() == 'RESOLVED' and str(r.get('verification_status', '')).upper() == 'PASSED'
        }
        resolved = len(resolved_case_ids)
        unresolved = max(0, len(rows) - resolved)
        if validated: status='KNOWN_ISSUE'
        elif not rows: status='FIRST_SEEN'
        elif resolved and unresolved==0: status='RECURRING_RESOLVED'
        elif unresolved: status='RECURRING_UNRESOLVED'
        else: status='RECURRING_UNKNOWN'
        return {
            'status':'SUCCESS' if rows else 'NO_DATA', 'recurrence_status':status,
            'occurrence_count':len(rows)+1, 'previous_case_ids':previous_ids,
            'validated_solution_refs':[r.get('resolution_code') for r in validated],
            'historical_actions':[], 'previous_outcomes':outcomes,
            'validated_candidates':validated,
        }

    def record_resolution(self, *, failure_case_id, resolution_code=None, description=None,
                          action_id=None, outcome_status='RESOLVED', verification_status=None,
                          recorded_by=None, evidence_ids=None, validation_status='UNVALIDATED',
                          validated_by=None, validation_reason=None, action=None,
                          expected_state=None, observed_state=None, environment_context=None):
        if verification_status is None:
            verification_status = 'UNVERIFIED'
        resolution={
            'resolution_id':str(uuid4()), 'failure_case_id':str(failure_case_id), 'action_id':action_id,
            'resolution_code':resolution_code, 'description':description,
            'outcome_status':outcome_status, 'verification_status':verification_status,
            'recorded_by':recorded_by, 'recorded_at':datetime.now(timezone.utc).isoformat(),
            'validation_status':validation_status, 'validated_by':validated_by,
            'validation_reason':validation_reason,
        }
        for row in self.cases:
            if str(row.get('failure_case_id')) == str(failure_case_id):
                row.setdefault('resolutions', []).append(resolution)
                row['status']='VERIFIED' if outcome_status=='RESOLVED' and verification_status=='PASSED' else 'RETAINED'
                return resolution['resolution_id']
        raise KeyError(f'failure case not found: {failure_case_id}')

    def upsert_learning_candidate(self, *, failure_signature, failure_class=None, failure_code=None,
                                  diagnosis_code=None, resolution_code=None, description=None,
                                  evidence_case_ids=None, verified_success_count=1, verified_failure_count=0, applicability=None):
        for c in self.learning_candidates:
            if c.get('failure_signature')==failure_signature and c.get('resolution_code')==resolution_code:
                c.update({'verified_success_count':verified_success_count,'verified_failure_count':verified_failure_count,
                          'evidence_case_ids':list(evidence_case_ids or []),'description':description or c.get('description')})
                return c['candidate_id']
        cid=str(uuid4())
        self.learning_candidates.append({
            'candidate_id':cid,'failure_signature':failure_signature,'failure_class':failure_class,
            'failure_code':failure_code,'diagnosis_code':diagnosis_code,'resolution_code':resolution_code,
            'description':description,'evidence_case_ids':list(evidence_case_ids or []),
            'verified_success_count':verified_success_count,'verified_failure_count':verified_failure_count,
            'status':'CANDIDATE','applicability':dict(applicability or {})
        })
        return cid

    def validate_learning_candidate(self, *, candidate_id, validated_by, validation_reason=None):
        cid_str = str(candidate_id)
        for c in list(self.dynamic_learning_candidates) + list(self.learning_candidates):
            if str(c.get('candidate_id')) == cid_str or str(c.get('failure_signature')) == cid_str:
                c.update({'status': 'VALIDATED', 'validated_by': validated_by, 'validation_reason': validation_reason})
                return c
        raise KeyError(f'learning candidate not found: {candidate_id}')

    def save_learning_candidate(self, candidate_data):
        cid = candidate_data.get("candidate_id")
        for i, c in enumerate(self.dynamic_learning_candidates):
            if c.get("candidate_id") == cid or c.get("signature_hash") == candidate_data.get("signature_hash"):
                self.dynamic_learning_candidates[i] = dict(candidate_data)
                return cid
        self.dynamic_learning_candidates.append(dict(candidate_data))
        return cid

    def get_learning_candidates(self, status=None):
        if status:
            return [c for c in self.dynamic_learning_candidates if c.get('status') == status]
        return list(self.dynamic_learning_candidates)

    def get_known_issues(self):
        return [c for c in self.learning_candidates if c.get('status') in ('VALIDATED', 'ACTIVE', 'DEPRECATED', 'INACTIVE')]

    def save_known_issue(self, sig_data):
        sig_id = sig_data.get('signature_id')
        sig_norm = str(sig_id).replace("CAND-", "").lower()
        for c in self.learning_candidates:
            fsig = str(c.get('failure_signature') or '').lower()
            if (
                c.get('signature_id') == sig_id
                or str(c.get('candidate_id')) == str(sig_id)
                or (fsig and (fsig.startswith(sig_norm) or sig_norm.startswith(fsig)))
            ):
                c.update({
                    'signature_id': sig_id,
                    'pattern': sig_data.get('canonical_pattern'),
                    'failure_class': sig_data.get('domain'),
                    'mechanism': sig_data.get('mechanism'),
                    'description': sig_data.get('description'),
                    'status': sig_data.get('status', 'VALIDATED'),
                    'occurrence_count': sig_data.get('frequency', c.get('occurrence_count', 1)),
                    'applicability_rules': sig_data.get('applicability_rules', {}),
                    'required_evidence': sig_data.get('required_evidence', []),
                    'contraindicated_evidence': sig_data.get('contraindicated_evidence', []),
                    'solutions': sig_data.get('solutions', []),
                })
                return
        self.learning_candidates.append({
            'signature_id': sig_id,
            'pattern': sig_data.get('canonical_pattern'),
            'failure_class': sig_data.get('domain'),
            'mechanism': sig_data.get('mechanism'),
            'description': sig_data.get('description'),
            'status': sig_data.get('status', 'VALIDATED'),
            'occurrence_count': sig_data.get('frequency', 1),
            'applicability_rules': sig_data.get('applicability_rules', {}),
            'required_evidence': sig_data.get('required_evidence', []),
            'contraindicated_evidence': sig_data.get('contraindicated_evidence', []),
            'solutions': sig_data.get('solutions', []),
        })
