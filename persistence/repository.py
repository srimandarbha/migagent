from __future__ import annotations
import json
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator
from uuid import UUID, uuid4

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:  # pragma: no cover
    psycopg = None
    dict_row = None

try:
    from psycopg_pool import ConnectionPool
except ImportError:  # pragma: no cover
    ConnectionPool = None

SCHEMA_FILE = Path(__file__).with_name("schema.sql")

class PostgresUnavailable(RuntimeError):
    pass

class SRETrackerRepository:
    """Durable operational source of truth with connection pooling. LangGraph state is not persisted here."""
    def __init__(self, dsn: str, min_size: int = 1, max_size: int = 10, timeout: float = 30.0):
        if psycopg is None:
            raise PostgresUnavailable("psycopg is required for PostgreSQL SRE Tracker")
        self.dsn = dsn
        self.min_size = min_size
        self.max_size = max_size
        self.timeout = timeout
        self._pool = None
        if ConnectionPool is not None:
            try:
                self._pool = ConnectionPool(
                    conninfo=dsn,
                    min_size=min_size,
                    max_size=max_size,
                    timeout=timeout,
                    kwargs={"row_factory": dict_row},
                    open=False,
                )
            except Exception:
                self._pool = None

    @contextmanager
    def connection(self) -> Iterator[Any]:
        if self._pool is not None:
            if self._pool.closed:
                self._pool.open()
            with self._pool.connection() as conn:
                yield conn
        else:
            with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
                yield conn

    def close(self) -> None:
        if self._pool is not None and not self._pool.closed:
            self._pool.close()

    def migrate(self) -> None:
        with self.connection() as conn:
            conn.execute(SCHEMA_FILE.read_text())
            conn.commit()

    def create_or_get_failure_case(self, *, event_id: str, migration_id: str,
                                   vm_id: str | None, cluster_id: str | None,
                                   change_id: str | None, failure_class: str | None,
                                   failure_code: str | None, agent_version: str,
                                   policy_version: str | None, severity: str | None = None,
                                   failure_case_id: UUID | None = None) -> UUID:
        if failure_case_id:
            try:
                case_id = UUID(str(failure_case_id))
            except (ValueError, TypeError):
                case_id = uuid4()
        else:
            case_id = uuid4()
        now = datetime.now(timezone.utc)
        with self.connection() as conn:
            row = conn.execute("SELECT failure_case_id FROM sre.failure_cases WHERE event_id=%s", (event_id,)).fetchone()
            if row:
                conn.commit(); return row["failure_case_id"]
            conn.execute("""INSERT INTO sre.failure_cases
                (failure_case_id,event_id,migration_id,vm_id,cluster_id,change_id,failure_class,failure_code,status,severity,first_seen_at,last_updated_at,agent_version,policy_version)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'NEW',%s,%s,%s,%s,%s)
                ON CONFLICT (event_id) DO UPDATE SET updated_at=now()""",
                (case_id,event_id,migration_id,vm_id,cluster_id,change_id,failure_class,failure_code,severity,now,now,agent_version,policy_version))
            row = conn.execute("SELECT failure_case_id FROM sre.failure_cases WHERE event_id=%s", (event_id,)).fetchone()
            conn.commit()
            return row["failure_case_id"]

    def event_exists(self, event_id: str) -> bool:
        with self.connection() as conn:
            row = conn.execute("SELECT 1 FROM sre.failure_events WHERE event_id=%s AND status='COMPLETED'", (event_id,)).fetchone()
            conn.commit()
            return row is not None

    def mark_event_completed(self, event_id: str) -> None:
        with self.connection() as conn:
            conn.execute("UPDATE sre.failure_events SET status='COMPLETED', completed_at=now() WHERE event_id=%s", (event_id,))
            conn.commit()

    def save_event(self, *, event_id: str, failure_case_id: UUID, event_type: str,
                   event_time: datetime, payload: dict, kafka_topic: str | None = None,
                   kafka_partition: int | None = None, kafka_offset: int | None = None,
                   status: str = 'RECEIVED') -> None:
        with self.connection() as conn:
            conn.execute("""INSERT INTO sre.failure_events
                (event_id,failure_case_id,event_type,event_time,kafka_topic,kafka_partition,kafka_offset,payload,status)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (event_id) DO UPDATE SET
                    kafka_topic=COALESCE(EXCLUDED.kafka_topic, sre.failure_events.kafka_topic),
                    kafka_partition=COALESCE(EXCLUDED.kafka_partition, sre.failure_events.kafka_partition),
                    kafka_offset=COALESCE(EXCLUDED.kafka_offset, sre.failure_events.kafka_offset)""",
                (event_id,failure_case_id,event_type,event_time,kafka_topic,kafka_partition,kafka_offset,json.dumps(payload),status))
            conn.commit()

    def save_evidence(self, case_id: UUID, evidence: dict) -> UUID:
        raw_id = str(evidence.get("id", "")).strip()
        import uuid as _uuid
        try:
            case_uuid = UUID(str(case_id))
        except (ValueError, TypeError):
            case_uuid = _uuid.uuid5(_uuid.NAMESPACE_URL, f"case:{case_id}")

        if raw_id:
            evidence_id = _uuid.uuid5(case_uuid, raw_id)
        else:
            evidence_id = uuid4()
        with self.connection() as conn:
            cur = conn.execute("""INSERT INTO sre.evidence
              (evidence_id,failure_case_id,capability_id,source,domain,signal,fact_code,claim,observed_at,retrieved_at,reliability,provenance,data)
              VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
              ON CONFLICT (evidence_id) DO NOTHING RETURNING evidence_id""",
              (evidence_id,case_id,evidence.get("capability_id",evidence.get("source","unknown")),
               evidence.get("source","unknown"),evidence.get("domain"),evidence.get("signal"),evidence.get("fact_code",evidence.get("fact")),
               evidence.get("claim",evidence.get("fact","")),evidence.get("observed_at"),evidence.get("retrieved_at",datetime.now(timezone.utc)),
               evidence.get("reliability",evidence.get("confidence",1.0)),json.dumps(evidence.get("provenance",{})),json.dumps(evidence.get("data",evidence.get("metadata",{})))))
            row = cur.fetchone() if hasattr(cur, "fetchone") else None
            if not row:
                import logging
                logging.getLogger(__name__).warning("evidence dropped as duplicate: id=%s, case=%s", evidence_id, case_id)
            conn.commit()
        return evidence_id


    def save_diagnosis(self, case_id: UUID, diagnosis: dict, evidence_ids: list[UUID] | None = None) -> UUID:
        did = uuid4()
        with self.connection() as conn:
            conn.execute("""INSERT INTO sre.diagnoses
              (diagnosis_id,failure_case_id,diagnosis_code,diagnosis,confidence,status,reasoning_summary)
              VALUES (%s,%s,%s,%s,%s,%s,%s)""",
              (did,case_id,diagnosis.get("code","UNKNOWN"),diagnosis.get("statement",diagnosis.get("diagnosis","")),
               diagnosis.get("confidence",0.0),diagnosis.get("status","UNKNOWN"),diagnosis.get("reasoning_summary")))
            for eid in evidence_ids or []:
                conn.execute("INSERT INTO sre.diagnosis_evidence(diagnosis_id,evidence_id,relationship) VALUES (%s,%s,'SUPPORTS') ON CONFLICT DO NOTHING", (did,eid))
            conn.execute("UPDATE sre.failure_cases SET status=%s,last_updated_at=now(),updated_at=now() WHERE failure_case_id=%s", ("DIAGNOSED" if diagnosis.get("status") not in ("INSUFFICIENT_EVIDENCE","UNKNOWN") else "INVESTIGATING", case_id))
            conn.commit()
        return did

    def search_failure_history(self, *, failure_class: str | None = None, cluster_id: str | None = None, limit: int = 20) -> list[dict]:
        clauses=[]; params=[]
        if failure_class: clauses.append("failure_class=%s"); params.append(failure_class)
        if cluster_id: clauses.append("cluster_id=%s"); params.append(cluster_id)
        where=("WHERE " + " AND ".join(clauses)) if clauses else ""
        with self.connection() as conn:
            return conn.execute(f"SELECT * FROM sre.failure_cases {where} ORDER BY last_updated_at DESC LIMIT %s", (*params,limit)).fetchall()

    def search_known_issues(self, failure_class: str | None = None, limit: int = 10) -> list[dict]:
        with self.connection() as conn:
            if failure_class:
                return conn.execute("SELECT * FROM sre.known_issues WHERE failure_class=%s AND status='VALIDATED' ORDER BY updated_at DESC LIMIT %s", (failure_class,limit)).fetchall()
            return conn.execute("SELECT * FROM sre.known_issues WHERE status='VALIDATED' ORDER BY updated_at DESC LIMIT %s", (limit,)).fetchall()

    def search_periodic_patterns(self, *, cluster_id: str | None = None, failure_class: str | None = None, limit: int = 20) -> list[dict]:
        clauses=[]; params=[]
        if cluster_id: clauses.append("cluster_id=%s"); params.append(cluster_id)
        if failure_class: clauses.append("failure_class=%s"); params.append(failure_class)
        where=("WHERE " + " AND ".join(clauses)) if clauses else ""
        with self.connection() as conn:
            return conn.execute(f"SELECT * FROM memory.periodic_failure_patterns {where} ORDER BY period_end DESC LIMIT %s", (*params,limit)).fetchall()

    def refresh_periodic_patterns(self, period_start: datetime, period_end: datetime) -> int:
        """Deterministic periodic memory derived from SRE Tracker; no LLM involvement."""
        with self.connection() as conn:
            rows = conn.execute("""SELECT cluster_id,failure_class,failure_code,
                COUNT(*) occurrence_count,COUNT(DISTINCT migration_id) affected_migrations,
                COUNT(DISTINCT vm_id) affected_vms,
                COUNT(*) FILTER (WHERE status='VERIFIED') resolved_count,
                COUNT(*) FILTER (WHERE status NOT IN ('VERIFIED','RETAINED')) unresolved_count,
                MIN(first_seen_at) first_seen_at,MAX(last_updated_at) last_seen_at
                FROM sre.failure_cases
                WHERE first_seen_at >= %s AND first_seen_at < %s
                GROUP BY cluster_id,failure_class,failure_code""", (period_start,period_end)).fetchall()
            for r in rows:
                conn.execute("""INSERT INTO memory.periodic_failure_patterns
                (pattern_id,period_start,period_end,cluster_id,failure_class,failure_code,occurrence_count,affected_migrations,affected_vms,resolved_count,unresolved_count,first_seen_at,last_seen_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (uuid4(),period_start,period_end,r["cluster_id"],r["failure_class"],r["failure_code"],r["occurrence_count"],r["affected_migrations"],r["affected_vms"],r["resolved_count"],r["unresolved_count"],r["first_seen_at"],r["last_seen_at"]))
            conn.commit()
            return len(rows)

class PostgresKnowledgeRepository:
    """Compatibility wrapper for the real PostgreSQL/pgvector knowledge store."""
    def __init__(self, tracker: SRETrackerRepository, embed=None):
        from .knowledge import PostgresVectorKnowledgeRepository
        self._impl = PostgresVectorKnowledgeRepository(tracker, embed=embed)
    def search(self, query: str, top_k: int = 5, **kwargs):
        return self._impl.search(query, top_k=top_k, **kwargs)

# v2.11 learning/recurrence repository operations.  These methods are additive and
# intentionally separate from LangGraph checkpoint state.
def _repo_get_failure_case(self, case_id):
    with self.connection() as conn:
        row=conn.execute("SELECT * FROM sre.failure_cases WHERE failure_case_id=%s", (case_id,)).fetchone()
        conn.commit()
    return dict(row) if row else None

def _repo_update_case_metadata(self, case_id, *, failure_signature=None, evidence_signature=None,
                               recurrence_status=None, occurrence_number=None):
    sets=[]; params=[]
    for column, value in (("failure_signature", failure_signature), ("evidence_signature", evidence_signature),
                          ("recurrence_status", recurrence_status), ("occurrence_number", occurrence_number)):
        if value is not None:
            sets.append(f"{column}=%s"); params.append(value)
    if not sets:
        return
    params.append(case_id)
    with self.connection() as conn:
        conn.execute(f"UPDATE sre.failure_cases SET {', '.join(sets)}, last_updated_at=now(), updated_at=now() WHERE failure_case_id=%s", params)
        conn.commit()


def _repo_correlate_failure_signature(self, *, signature, failure_case_id=None, classification=None, cluster_id=None):
    clauses=["failure_signature=%s"]; params=[signature]
    if classification:
        clauses.append("failure_class=%s"); params.append(classification)
    if cluster_id:
        clauses.append("cluster_id=%s"); params.append(cluster_id)
    if failure_case_id:
        try:
            valid_uuid = str(UUID(str(failure_case_id)))
            clauses.append("failure_case_id<>%s::uuid")
            params.append(valid_uuid)
        except (ValueError, TypeError):
            pass
    where=" AND ".join(clauses)
    with self.connection() as conn:
        rows=conn.execute(f"SELECT * FROM sre.failure_cases WHERE {where} ORDER BY last_updated_at DESC LIMIT 100", params).fetchall()
        case_ids=[str(r['failure_case_id']) for r in rows]
        resolutions=[]
        if case_ids:
            resolutions=conn.execute("""SELECT r.*, f.failure_signature, f.failure_class, f.failure_code
                FROM sre.resolutions r JOIN sre.failure_cases f ON f.failure_case_id=r.failure_case_id
                WHERE r.failure_case_id = ANY(%s::uuid[]) ORDER BY r.recorded_at DESC""", (case_ids,)).fetchall()
        validated=conn.execute("SELECT * FROM sre.learning_candidates WHERE failure_signature=%s AND status='VALIDATED' LIMIT 20", (signature,)).fetchall()
        conn.commit()
    resolved_case_ids = {
        str(r['failure_case_id']) for r in resolutions
        if str(r.get('outcome_status','')).upper()=='RESOLVED' and str(r.get('verification_status','')).upper()=='PASSED'
    }
    resolved = len(resolved_case_ids)
    unresolved = max(0, len(rows) - resolved)
    if validated:
        status='KNOWN_ISSUE'
    elif not rows:
        status='FIRST_SEEN'
    elif resolved and unresolved==0:
        status='RECURRING_RESOLVED'
    elif unresolved:
        status='RECURRING_UNRESOLVED'
    else:
        status='RECURRING_UNKNOWN'
    return {
        'status':'SUCCESS' if rows else 'NO_DATA',
        'recurrence_status':status,
        'occurrence_count':len(rows)+1,
        'previous_case_ids':case_ids,
        'validated_solution_refs':[str(r.get('solution_code') or r.get('known_solution_id')) for r in validated],
        'historical_actions':[],
        'previous_outcomes':[dict(r) for r in resolutions],
        'validated_candidates':[dict(r) for r in validated],
    }


def _repo_record_resolution(self, *, failure_case_id, resolution_code=None, description=None,
                            action_id=None, outcome_status='RESOLVED', verification_status=None,
                            recorded_by=None, evidence_ids=None, validation_status='UNVALIDATED',
                            validated_by=None, validation_reason=None, environment_context=None,
                            action=None, expected_state=None, observed_state=None):
    if verification_status is None:
        verification_status = 'UNVERIFIED'
    if verification_status == 'PASSED' and expected_state is None and observed_state is None:
        raise ValueError("PASSED verification requires expected_state and observed_state")
    rid=uuid4()
    with self.connection() as conn:
        actual_action_id=action_id
        if action and not actual_action_id:
            actual_action_id=uuid4()
            conn.execute("""INSERT INTO sre.actions
                (action_id,failure_case_id,action_code,description,action_type,risk_level,approval_required,approval_status,execution_status)
                VALUES (%s,%s,%s,%s,'REMEDIATE','UNKNOWN',true,'RECORDED','SUCCEEDED')""",
                (actual_action_id,failure_case_id,action.get('code') or resolution_code or 'RECORDED_ACTION',action.get('description') or description))
        conn.execute("""INSERT INTO sre.resolutions
            (resolution_id,failure_case_id,action_id,resolution_code,description,outcome_status,verification_status,recorded_by,validation_status,validated_by,validated_at,validation_reason,environment_context)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (rid,failure_case_id,actual_action_id,resolution_code,description,outcome_status,verification_status,recorded_by,
             validation_status,validated_by,datetime.now(timezone.utc) if validated_by else None,validation_reason,
             json.dumps(environment_context or {})))
        if actual_action_id and (expected_state is not None or observed_state is not None):
            conn.execute("""INSERT INTO sre.verifications
                (verification_id,failure_case_id,action_id,verification_type,expected_state,observed_state,status,verified_at)
                VALUES (%s,%s,%s,'RESOLUTION',%s,%s,%s,%s)""",
                (uuid4(),failure_case_id,actual_action_id,json.dumps(expected_state or {}),json.dumps(observed_state or {}),verification_status,
                 datetime.now(timezone.utc) if verification_status=='PASSED' else None))
        for eid in evidence_ids or []:
            conn.execute("INSERT INTO sre.resolution_evidence(resolution_id,evidence_id,relationship) VALUES (%s,%s,'VERIFIES') ON CONFLICT DO NOTHING", (rid,eid))
        conn.execute("""INSERT INTO sre.outcomes
            (outcome_id,failure_case_id,final_status,resolution_code,resolution_summary,resolved_at,validated_by,validation_type)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
            (uuid4(),failure_case_id,outcome_status,resolution_code,description,
             datetime.now(timezone.utc) if outcome_status=='RESOLVED' else None,validated_by,
             'HUMAN' if validated_by else None))
        conn.execute("UPDATE sre.failure_cases SET status=%s,last_updated_at=now(),updated_at=now() WHERE failure_case_id=%s",
                     ('VERIFIED' if outcome_status=='RESOLVED' and verification_status=='PASSED' else 'RETAINED', failure_case_id))
        conn.commit()
    return rid


def _repo_upsert_learning_candidate(self, *, failure_signature, failure_class=None, failure_code=None,
                                    diagnosis_code=None, resolution_code=None, description=None,
                                    evidence_case_ids=None, verified_success_count=1, verified_failure_count=0,
                                    applicability=None):
    with self.connection() as conn:
        row=conn.execute("SELECT * FROM sre.learning_candidates WHERE failure_signature=%s AND COALESCE(resolution_code,'')=COALESCE(%s,'') LIMIT 1", (failure_signature,resolution_code)).fetchone()
        if row:
            cid=row['candidate_id']
            existing_cases = json.loads(row['evidence_case_ids']) if isinstance(row['evidence_case_ids'], str) else (row['evidence_case_ids'] or [])
            merged_cases = list(dict.fromkeys(existing_cases + (evidence_case_ids or [])))
            success_count = (row.get('verified_success_count') or 0) + (verified_success_count or 1)
            conn.execute("""UPDATE sre.learning_candidates SET
                evidence_case_ids=%s, verified_success_count=%s, verified_failure_count=%s,
                updated_at=now(), description=COALESCE(%s,description), applicability=%s
                WHERE candidate_id=%s""", (json.dumps(merged_cases),success_count,verified_failure_count,description,json.dumps(applicability or {}),cid))
        else:
            cid=uuid4()
            conn.execute("""INSERT INTO sre.learning_candidates
                (candidate_id,failure_signature,failure_class,failure_code,diagnosis_code,resolution_code,description,evidence_case_ids,verified_success_count,verified_failure_count,status,applicability)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'CANDIDATE',%s)""",
                (cid,failure_signature,failure_class,failure_code,diagnosis_code,resolution_code,description,json.dumps(evidence_case_ids or []),verified_success_count,verified_failure_count,json.dumps(applicability or {})))
        conn.commit()
    return cid



def _repo_validate_learning_candidate(self, *, candidate_id, validated_by, validation_reason=None):
    with self.connection() as conn:
        row=conn.execute("SELECT * FROM sre.learning_candidates WHERE candidate_id=%s", (candidate_id,)).fetchone()
        if not row:
            raise KeyError(f'learning candidate not found: {candidate_id}')
        conn.execute("UPDATE sre.learning_candidates SET status='VALIDATED',validated_by=%s,validated_at=now(),validation_reason=%s,updated_at=now() WHERE candidate_id=%s", (validated_by,validation_reason,candidate_id))
        issue_code='ISSUE.' + str(row['failure_signature'])[:16].upper()
        solution_code=str(row['resolution_code'] or ('SOLUTION.' + str(row['failure_signature'])[:16])).upper()
        issue_id=uuid4(); solution_id=uuid4()
        conn.execute("""INSERT INTO sre.known_issues
            (known_issue_id,issue_code,title,description,failure_class,status,validated_by,validated_at)
            VALUES (%s,%s,%s,%s,%s,'VALIDATED',%s,now())
            ON CONFLICT (issue_code) DO UPDATE SET updated_at=now(),validated_by=EXCLUDED.validated_by,validated_at=EXCLUDED.validated_at""",
            (issue_id,issue_code,'Validated migration failure signature',row['description'],row['failure_class'],validated_by))
        issue=conn.execute("SELECT known_issue_id FROM sre.known_issues WHERE issue_code=%s", (issue_code,)).fetchone()
        issue_id=issue['known_issue_id']
        conn.execute("""INSERT INTO sre.known_solutions
            (known_solution_id,solution_code,title,description,automation_system,automation_reference,risk_level,approval_required,status,success_count,failure_count,validated_by,validated_at)
            VALUES (%s,%s,%s,%s,NULL,NULL,'UNKNOWN',true,'VALIDATED',%s,%s,%s,now())
            ON CONFLICT (solution_code) DO UPDATE SET success_count=sre.known_solutions.success_count+EXCLUDED.success_count,validated_by=EXCLUDED.validated_by,validated_at=EXCLUDED.validated_at,status='VALIDATED',updated_at=now()""",
            (solution_id,solution_code,'Validated resolution for migration failure',row['description'],row['verified_success_count'],row['verified_failure_count'],validated_by))
        solution=conn.execute("SELECT known_solution_id FROM sre.known_solutions WHERE solution_code=%s", (solution_code,)).fetchone()
        conn.execute("""INSERT INTO sre.known_issue_solutions(known_issue_id,known_solution_id,relationship)
            VALUES (%s,%s,'VALIDATED') ON CONFLICT DO NOTHING""", (issue_id,solution['known_solution_id']))
        conn.commit()
        row=dict(row); row.update({'status':'VALIDATED','known_issue_id':str(issue_id),'known_solution_id':str(solution['known_solution_id']), 'issue_code':issue_code,'solution_code':solution_code})
        return row


def _repo_get_known_issues(self):
    with self.connection() as conn:
        rows = conn.execute("""
            SELECT ki.known_issue_id, ki.issue_code, ki.title, ki.description, ki.failure_class,
                   ki.pattern, ki.issue_summary, ki.status,
                   ks.known_solution_id, ks.solution_code, ks.title as solution_title,
                   ks.description as solution_description, ks.automation_system,
                   ks.risk_level, ks.approval_required, ks.success_count, ks.failure_count,
                   ks.recommended_action, ks.action_type, ks.action_summary
            FROM sre.known_issues ki
            LEFT JOIN sre.known_issue_solutions kis ON kis.known_issue_id = ki.known_issue_id
            WHERE ki.status IN ('VALIDATED', 'ACTIVE', 'DEPRECATED', 'INACTIVE')
        """).fetchall()
        conn.commit()

    issues_by_code = {}
    for r in rows:
        code = r['issue_code']
        if code not in issues_by_code:
            issues_by_code[code] = {
                'signature_id': code,
                'canonical_signature': r.get('pattern') or r.get('description') or '',
                'domain': r.get('failure_class') or 'general',
                'mechanism': r.get('failure_class') or 'UNKNOWN',
                'description': r.get('issue_summary') or r.get('title') or '',
                'status': r.get('status') or 'ACTIVE',
                'solutions': [],
            }
        if r.get('solution_code'):
            issues_by_code[code]['solutions'].append({
                'solution_id': r['solution_code'],
                'title': r.get('solution_title') or '',
                'action_summary': r.get('action_summary') or r.get('solution_description') or '',
                'recommended_action': r.get('recommended_action') or r.get('action_summary') or '',
                'automation_system': r.get('automation_system') or 'MANUAL',
                'risk_level': r.get('risk_level') or 'LOW',
                'requires_approval': r.get('approval_required', True),
                'success_count': r.get('success_count', 0),
                'failure_count': r.get('failure_count', 0),
            })
    return list(issues_by_code.values())


def _repo_save_known_issue(self, sig_data):
    issue_code = str(sig_data.get('signature_id', uuid4()))
    with self.connection() as conn:
        issue_id = uuid4()
        conn.execute("""INSERT INTO sre.known_issues
            (known_issue_id, issue_code, title, description, failure_class, pattern, issue_summary, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, 'ACTIVE')
            ON CONFLICT (issue_code) DO UPDATE SET
                pattern=EXCLUDED.pattern, description=EXCLUDED.description, updated_at=now()""",
            (issue_id, issue_code, sig_data.get('description', issue_code), sig_data.get('description', ''),
             sig_data.get('domain', 'general'), sig_data.get('canonical_pattern', ''), sig_data.get('description', '')))
        issue = conn.execute("SELECT known_issue_id FROM sre.known_issues WHERE issue_code=%s", (issue_code,)).fetchone()
        issue_id = issue['known_issue_id']

        for sol in sig_data.get('solutions', []):
            sol_code = str(sol.get('solution_id', uuid4()))
            sol_id = uuid4()
            conn.execute("""INSERT INTO sre.known_solutions
                (known_solution_id, solution_code, title, description, automation_system, risk_level, approval_required, status, success_count, failure_count, recommended_action, action_summary)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'ACTIVE', %s, %s, %s, %s)
                ON CONFLICT (solution_code) DO UPDATE SET
                    success_count=EXCLUDED.success_count, failure_count=EXCLUDED.failure_count, updated_at=now()""",
                (sol_id, sol_code, sol.get('title', sol_code), sol.get('action_summary', ''),
                 sol.get('automation_system', 'MANUAL'), str(sol.get('risk_level', 'LOW')),
                 sol.get('requires_approval', True), sol.get('success_count', 0), sol.get('failure_count', 0),
                 sol.get('recommended_action', ''), sol.get('action_summary', '')))
            sol_row = conn.execute("SELECT known_solution_id FROM sre.known_solutions WHERE solution_code=%s", (sol_code,)).fetchone()
            conn.execute("""INSERT INTO sre.known_issue_solutions(known_issue_id, known_solution_id, relationship)
                VALUES (%s, %s, 'RECOMMENDED') ON CONFLICT DO NOTHING""", (issue_id, sol_row['known_solution_id']))
        conn.commit()


def _repo_get_learning_candidates(self, status=None):
    with self.connection() as conn:
        if status:
            rows = conn.execute("SELECT * FROM sre.learning_candidates WHERE status=%s ORDER BY updated_at DESC LIMIT 100", (status,)).fetchall()
        else:
            rows = conn.execute("SELECT * FROM sre.learning_candidates ORDER BY updated_at DESC LIMIT 100").fetchall()
        conn.commit()
    return [dict(r) for r in rows]


def _repo_save_learning_candidate(self, candidate_data):
    shash = candidate_data.get("signature_hash")
    meta = candidate_data.get("metadata", {})
    return self.upsert_learning_candidate(
        failure_signature=shash,
        failure_class=candidate_data.get("suggested_domain"),
        diagnosis_code=candidate_data.get("suggested_mechanism"),
        resolution_code=candidate_data.get("suggested_action"),
        description=candidate_data.get("raw_log_sample", "")[:200],
        evidence_case_ids=[meta.get('failure_case_id')] if meta.get('failure_case_id') else [],
        verified_success_count=candidate_data.get("occurrence_count", 1),
        applicability=meta,
    )


SRETrackerRepository.get_failure_case = _repo_get_failure_case
SRETrackerRepository.update_case_metadata = _repo_update_case_metadata
SRETrackerRepository.correlate_failure_signature = _repo_correlate_failure_signature
SRETrackerRepository.record_resolution = _repo_record_resolution
SRETrackerRepository.upsert_learning_candidate = _repo_upsert_learning_candidate
SRETrackerRepository.save_learning_candidate = _repo_save_learning_candidate
SRETrackerRepository.validate_learning_candidate = _repo_validate_learning_candidate
SRETrackerRepository.get_learning_candidates = _repo_get_learning_candidates
SRETrackerRepository.get_known_issues = _repo_get_known_issues
SRETrackerRepository.save_known_issue = _repo_save_known_issue
