-- Migration Failure Agent local PostgreSQL setup.
-- Run against an existing database, e.g. migration_agent.
-- Requires the pgvector extension to be installed on the PostgreSQL host.

CREATE EXTENSION IF NOT EXISTS vector;

CREATE SCHEMA IF NOT EXISTS sre;
CREATE SCHEMA IF NOT EXISTS memory;
CREATE SCHEMA IF NOT EXISTS knowledge;
CREATE SCHEMA IF NOT EXISTS config;

CREATE TABLE IF NOT EXISTS sre.failure_cases (
    failure_case_id UUID PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    migration_id TEXT NOT NULL,
    vm_id TEXT,
    cluster_id TEXT,
    change_id TEXT,
    failure_class TEXT,
    failure_code TEXT,
    status TEXT NOT NULL,
    severity TEXT,
    first_seen_at TIMESTAMPTZ NOT NULL,
    last_updated_at TIMESTAMPTZ NOT NULL,
    agent_version TEXT NOT NULL,
    policy_version TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sre.failure_events (
    event_id TEXT PRIMARY KEY,
    failure_case_id UUID REFERENCES sre.failure_cases(failure_case_id),
    event_type TEXT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    kafka_topic TEXT,
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    payload JSONB NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sre.evidence (
    evidence_id UUID PRIMARY KEY,
    failure_case_id UUID NOT NULL REFERENCES sre.failure_cases(failure_case_id),
    capability_id TEXT NOT NULL,
    source TEXT NOT NULL,
    domain TEXT,
    signal TEXT,
    fact_code TEXT,
    claim TEXT NOT NULL,
    observed_at TIMESTAMPTZ,
    retrieved_at TIMESTAMPTZ NOT NULL,
    reliability NUMERIC(4,3),
    provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
    data JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sre.hypotheses (
    hypothesis_id UUID PRIMARY KEY,
    failure_case_id UUID NOT NULL REFERENCES sre.failure_cases(failure_case_id),
    hypothesis_code TEXT NOT NULL,
    description TEXT NOT NULL,
    status TEXT NOT NULL,
    confidence NUMERIC(4,3),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sre.hypothesis_evidence (
    hypothesis_id UUID REFERENCES sre.hypotheses(hypothesis_id),
    evidence_id UUID REFERENCES sre.evidence(evidence_id),
    relationship TEXT NOT NULL,
    weight NUMERIC(4,3),
    PRIMARY KEY (hypothesis_id, evidence_id)
);

CREATE TABLE IF NOT EXISTS sre.diagnoses (
    diagnosis_id UUID PRIMARY KEY,
    failure_case_id UUID NOT NULL REFERENCES sre.failure_cases(failure_case_id),
    diagnosis_code TEXT NOT NULL,
    diagnosis TEXT NOT NULL,
    confidence NUMERIC(4,3),
    status TEXT NOT NULL,
    reasoning_summary TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sre.diagnosis_evidence (
    diagnosis_id UUID REFERENCES sre.diagnoses(diagnosis_id),
    evidence_id UUID REFERENCES sre.evidence(evidence_id),
    relationship TEXT NOT NULL,
    PRIMARY KEY (diagnosis_id, evidence_id)
);

CREATE TABLE IF NOT EXISTS sre.actions (
    action_id UUID PRIMARY KEY,
    failure_case_id UUID NOT NULL REFERENCES sre.failure_cases(failure_case_id),
    diagnosis_id UUID REFERENCES sre.diagnoses(diagnosis_id),
    action_code TEXT NOT NULL,
    description TEXT,
    action_type TEXT,
    risk_level TEXT,
    approval_required BOOLEAN NOT NULL DEFAULT true,
    approval_status TEXT,
    execution_status TEXT NOT NULL,
    automation_system TEXT,
    automation_reference TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sre.verifications (
    verification_id UUID PRIMARY KEY,
    failure_case_id UUID NOT NULL REFERENCES sre.failure_cases(failure_case_id),
    action_id UUID REFERENCES sre.actions(action_id),
    verification_type TEXT NOT NULL,
    expected_state JSONB,
    observed_state JSONB,
    status TEXT NOT NULL,
    verified_at TIMESTAMPTZ,
    evidence_id UUID REFERENCES sre.evidence(evidence_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sre.outcomes (
    outcome_id UUID PRIMARY KEY,
    failure_case_id UUID NOT NULL REFERENCES sre.failure_cases(failure_case_id),
    final_status TEXT NOT NULL,
    resolution_code TEXT,
    resolution_summary TEXT,
    resolved_at TIMESTAMPTZ,
    validated_by TEXT,
    validation_type TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sre.known_issues (
    known_issue_id UUID PRIMARY KEY,
    issue_code TEXT UNIQUE NOT NULL,
    title TEXT NOT NULL,
    description TEXT,
    failure_class TEXT,
    status TEXT NOT NULL,
    validated_by TEXT,
    validated_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sre.known_solutions (
    known_solution_id UUID PRIMARY KEY,
    solution_code TEXT UNIQUE NOT NULL,
    title TEXT NOT NULL,
    description TEXT,
    automation_system TEXT,
    automation_reference TEXT,
    risk_level TEXT,
    approval_required BOOLEAN NOT NULL,
    status TEXT NOT NULL,
    success_count INTEGER NOT NULL DEFAULT 0,
    failure_count INTEGER NOT NULL DEFAULT 0,
    validated_by TEXT,
    validated_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sre.known_issue_solutions (
    known_issue_id UUID REFERENCES sre.known_issues(known_issue_id) ON DELETE CASCADE,
    known_solution_id UUID REFERENCES sre.known_solutions(known_solution_id) ON DELETE CASCADE,
    relationship TEXT NOT NULL DEFAULT 'RECOMMENDED',
    PRIMARY KEY (known_issue_id, known_solution_id)
);

CREATE TABLE IF NOT EXISTS memory.periodic_failure_patterns (
    pattern_id UUID PRIMARY KEY,
    period_start TIMESTAMPTZ NOT NULL,
    period_end TIMESTAMPTZ NOT NULL,
    cluster_id TEXT,
    failure_class TEXT,
    failure_code TEXT,
    occurrence_count INTEGER NOT NULL,
    affected_migrations INTEGER,
    affected_vms INTEGER,
    resolved_count INTEGER,
    unresolved_count INTEGER,
    avg_resolution_seconds NUMERIC,
    first_seen_at TIMESTAMPTZ,
    last_seen_at TIMESTAMPTZ,
    trend TEXT,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS config.policy_versions (
    policy_id TEXT NOT NULL,
    version TEXT NOT NULL,
    policy_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (policy_id, version)
);

CREATE TABLE IF NOT EXISTS knowledge.documents (
    document_id UUID PRIMARY KEY,
    source TEXT NOT NULL,
    source_url TEXT,
    product TEXT,
    product_version TEXT,
    content_type TEXT NOT NULL,
    title TEXT NOT NULL,
    summary TEXT,
    content TEXT NOT NULL DEFAULT '',
    published_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    retrieved_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    content_hash TEXT NOT NULL UNIQUE,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

-- Default local embedding contract: Nomic Embed Text v1.5, 768 dimensions.
-- The database dimension must match the selected embedding model exactly.
CREATE TABLE IF NOT EXISTS knowledge.chunks (
    chunk_id UUID PRIMARY KEY,
    document_id UUID NOT NULL REFERENCES knowledge.documents(document_id) ON DELETE CASCADE,
    chunk_index INTEGER NOT NULL,
    content TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    embedding vector(768),
    UNIQUE(document_id, chunk_index)
);

CREATE INDEX IF NOT EXISTS idx_failure_cases_migration ON sre.failure_cases(migration_id);
CREATE INDEX IF NOT EXISTS idx_failure_cases_cluster_class ON sre.failure_cases(cluster_id, failure_class);
CREATE INDEX IF NOT EXISTS idx_evidence_case ON sre.evidence(failure_case_id);
CREATE INDEX IF NOT EXISTS idx_hypotheses_case ON sre.hypotheses(failure_case_id);
CREATE INDEX IF NOT EXISTS idx_actions_case ON sre.actions(failure_case_id);
CREATE INDEX IF NOT EXISTS idx_periodic_pattern_lookup ON memory.periodic_failure_patterns(cluster_id, failure_class, period_start, period_end);
CREATE INDEX IF NOT EXISTS idx_knowledge_documents_product ON knowledge.documents(product, product_version);
CREATE INDEX IF NOT EXISTS idx_knowledge_chunks_document ON knowledge.chunks(document_id, chunk_index);
CREATE INDEX IF NOT EXISTS idx_knowledge_chunks_embedding ON knowledge.chunks USING hnsw (embedding vector_cosine_ops);
