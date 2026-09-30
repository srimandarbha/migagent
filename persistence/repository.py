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

SCHEMA_FILE = Path(__file__).with_name("schema.sql")

class PostgresUnavailable(RuntimeError):
    pass

class SRETrackerRepository:
    """Durable operational source of truth. LangGraph state is not persisted here."""
    def __init__(self, dsn: str):
        if psycopg is None:
            raise PostgresUnavailable("psycopg is required for PostgreSQL SRE Tracker")
        self.dsn = dsn

    @contextmanager
    def connection(self) -> Iterator[Any]:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            yield conn

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
        case_id = failure_case_id or uuid4()
        now = datetime.now(timezone.utc)
        with self.connection() as conn:
            row = conn.execute("SELECT failure_case_id FROM sre.failure_cases WHERE event_id=%s", (event_id,)).fetchone()
            if row:
                conn.commit(); return row["failure_case_id"]
            conn.execute("""INSERT INTO sre.failure_cases
                (failure_case_id,event_id,migration_id,vm_id,cluster_id,change_id,failure_class,failure_code,status,severity,first_seen_at,last_updated_at,agent_version,policy_version)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'NEW',%s,%s,%s,%s,%s)""",
                (case_id,event_id,migration_id,vm_id,cluster_id,change_id,failure_class,failure_code,severity,now,now,agent_version,policy_version))
            conn.commit()
        return case_id

    def event_exists(self, event_id: str) -> bool:
        with self.connection() as conn:
            row = conn.execute("SELECT 1 FROM sre.failure_events WHERE event_id=%s", (event_id,)).fetchone()
            conn.commit()
            return row is not None

    def save_event(self, *, event_id: str, failure_case_id: UUID, event_type: str,
                   event_time: datetime, payload: dict, kafka_topic: str | None = None,
                   kafka_partition: int | None = None, kafka_offset: int | None = None) -> None:
        with self.connection() as conn:
            conn.execute("""INSERT INTO sre.failure_events
                (event_id,failure_case_id,event_type,event_time,kafka_topic,kafka_partition,kafka_offset,payload)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (event_id) DO NOTHING""",
                (event_id,failure_case_id,event_type,event_time,kafka_topic,kafka_partition,kafka_offset,json.dumps(payload)))
            conn.commit()

    def save_evidence(self, case_id: UUID, evidence: dict) -> UUID:
        try:
            evidence_id = UUID(str(evidence.get("id"))) if evidence.get("id") else uuid4()
        except ValueError:
            evidence_id = uuid4()
        with self.connection() as conn:
            conn.execute("""INSERT INTO sre.evidence
              (evidence_id,failure_case_id,capability_id,source,domain,signal,fact_code,claim,observed_at,retrieved_at,reliability,provenance,data)
              VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
              ON CONFLICT (evidence_id) DO NOTHING""",
              (evidence_id,case_id,evidence.get("capability_id",evidence.get("source","unknown")),
               evidence.get("source","unknown"),evidence.get("domain"),evidence.get("signal"),evidence.get("fact_code",evidence.get("fact")),
               evidence.get("claim",evidence.get("fact","")),evidence.get("observed_at"),evidence.get("retrieved_at",datetime.now(timezone.utc)),
               evidence.get("reliability",evidence.get("confidence",1.0)),json.dumps(evidence.get("provenance",{})),json.dumps(evidence.get("data",evidence.get("metadata",{})))))
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
