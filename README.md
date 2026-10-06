# Migration Failure Agent (MFA) v2.12.3

Production-oriented VMware &rarr; OpenShift Virtualization (OCV / MTV / Forklift) Migration Failure Decision Engine.

The agent investigates `MigrationFailed` events, correlates historical recurrence, gathers evidence through approved capability adapters, produces an evidence-backed root cause diagnosis and safe next step, and remains strictly fail-closed.

---

## Current State

- **Version:** 2.12.3
- **Purpose:** Read-only MTV migration-failure diagnosis and SRE next-step guidance.
- **Execution:** Strictly read-only diagnostic agent. No remediation, retry, rollback, EDA, AAP, or ServiceNow platform mutation is executed autonomously.
- **LLM Boundary:** Advisory only. LLM output is never evidence, diagnosis authority, approval, or execution authority. It is activated only on unknown failures or insufficient evidence.
- **Test Suite Status:** 216 passed, 0 skipped, 0 failed (verified across all unit, integration, Kafka offset discipline, and safety suites).

---

## How to Run in Real

### 1. Prerequisites & Services

The real runtime stack requires:
- **Python 3.11+**
- **PostgreSQL 15+ with `pgvector`** (default: `127.0.0.1:5432`)
- **Apache Kafka Broker** (default: `127.0.0.1:9092`)
- **Local or remote LLM / Embedding provider** (OpenRouter, Ollama, or llama.cpp)

#### Start Local Kafka Broker
If using Podman/Docker, start the bundled Kafka container:
```bash
podman start mfa-kafka
```
Verify connectivity:
```bash
python -c "from confluent_kafka import Producer; p = Producer({'bootstrap.servers':'127.0.0.1:9092'}); print('Kafka OK. Topics:', list(p.list_topics(timeout=5).topics.keys()))"
```

If Kafka topics do not exist, initialize them:
```bash
python scripts/create_kafka_topics.py
```
*(Default topics: `mfa.migration.failed`, `mfa.agent.result`, `mfa.migration.failed.dlq`)*

#### Start & Seed PostgreSQL SRE Tracker
Verify the database connection:
```bash
PGPASSWORD=postgres psql -h 127.0.0.1 -U postgres -d migration_agent -c "SELECT count(*) FROM sre.failure_cases;"
```

To apply schemas and seed baseline knowledge records:
```bash
export DATABASE_URL='postgresql://postgres:postgres@127.0.0.1:5432/migration_agent'
python scripts/seed_postgres.py
```

---

### 2. Configure Environment (`.env`)

Configure your `.env` file in the project root:
```ini
POSTGRES_HOST=127.0.0.1
POSTGRES_PORT=5432
POSTGRES_DB=migration_agent
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:5432/migration_agent

SRE_TRACKER_PROVIDER=postgres
KNOWLEDGE_PROVIDER=postgres_pgvector

KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092
KAFKA_MIGRATION_FAILED_TOPIC=mfa.migration.failed
KAFKA_AGENT_RESULT_TOPIC=mfa.agent.result
KAFKA_CONSUMER_GROUP=migration-failure-agent

# Optional LLM Advisory (for unknown failures / novel errors)
LLM_PROVIDER=openrouter
OPENROUTER_API_KEY=your_key_here
OPENROUTER_MODEL=openrouter/free

# Local pgvector Embeddings (Ollama or llama.cpp)
EMBEDDING_BASE_URL=http://127.0.0.1:11434/v1
EMBEDDING_DIMENSION=768
```

---

### 3. Running the Event-Driven Kafka Pipeline

To run the agent in end-to-end event-driven production mode:

#### Terminal 1 — Start Agent Result Listener
Subscribes to `mfa.agent.result` and outputs completed diagnoses and decision readiness:
```bash
python scripts/read_agent_results.py
```

#### Terminal 2 — Start Migration Failure Agent Daemon
Starts `KafkaIngress`, which listens on `mfa.migration.failed`, invokes the LangGraph reasoning engine, updates the PostgreSQL SRE Tracker, and publishes results:
```bash
python scripts/run_kafka_agent.py
```

#### Terminal 3 — Publish a Migration Failure Event
Publish a real or simulated `MigrationFailed` payload into Kafka:
```bash
cat <<'JSON' | python scripts/publish_migration_failed.py
{
  "event_id": "evt-prod-001",
  "event_type": "MigrationFailed",
  "event_time": "2026-10-06T12:00:00Z",
  "migration_id": "mig-cluster-east-042",
  "vm_id": "vm-rhel8-oracle",
  "cluster_id": "ocv-prod-east",
  "failure_code": "storage.csi.provisioning_timeout",
  "severity": "critical",
  "phase": "PVC provisioning",
  "message": "DataVolume provisioning timed out"
}
JSON
```

---

### 4. Running Interactive CLI Diagnosis (Direct Mode)

Diagnose an incident immediately without Kafka:

#### Direct Error Message:
```bash
python scripts/diagnose_failure.py --message "DataVolume provisioning timed out" --cluster-id ocv-prod-01
```

#### From JSON File:
```bash
python scripts/diagnose_failure.py --file examples/sample_failure_event.json --output-json /tmp/diagnosis.json
```

Outputs a comprehensive terminal report detailing incident identity, episodic memory recurrence, collected telemetry facts, RAG knowledge matches, diagnosed mechanism, and fail-closed readiness.

---

### 5. Connecting Real Observability (DMZ Splunk & Prometheus MCP)

In restricted enterprise / DMZ environments without direct hypervisor access:
- Container logs, CDI importer logs, and forwarded VMware syslogs are queried via **Splunk MCP** (`DMZSplunkMCPAdapter`).
- CSI provisioning latency, MTV throughput, and storage health are evaluated via **Prometheus MCP** (`DMZPrometheusMCPAdapter`).

To build a clean production bundle excluding all simulators and test fixtures:
```bash
./scripts_build_dmz.sh ./dist/migration-failure-agent-dmz
```

---

### 6. SRE Operational Learning Feedback Loop

When an engineer completes a real-world resolution, record the verified outcome to train episodic memory:
```bash
python scripts/record_resolution.py \
  --case-id "<failure-case-id>" \
  --action "FIX_STORAGE_QUOTA" \
  --verification "PASSED" \
  --notes "Expanded storage backend volume pool; PVC bound immediately."
```

---

## Architectural Principles

### Two-Tier Diagnostic Protocol
1. **Tier 1 (Known Pattern)**: When classification or dynamic knowledge signatures match with high confidence, diagnosis is derived **100% deterministically** using policy rules and verified facts.
2. **Tier 2 (Novel / Unknown Failure)**: If classification is `UNKNOWN` or evidence is `INSUFFICIENT_EVIDENCE`, the bounded LLM Advisory Node activates to formulate a `parametric_hypothesis` and suggest allowlisted, read-only diagnostic checks.

### Explicit Workflow Graph (LangGraph)
```text
START -> create_case -> classify -> persist_case -> load_context -> correlate_recurrence
      -> build_evidence_plan -> collect_evidence -> evaluate_evidence
      -> [evaluate_hypotheses -> diagnose] -> [llm_advisory when insufficient]
      -> calculate_readiness -> recommend -> persist -> finalize -> END
```

### Memory Separation
| Layer | Implementation | Purpose |
|---|---|---|
| Procedural Memory | `skills/*/skill.md` | How to investigate (investigation workflows, runbooks) |
| Episodic Memory | PostgreSQL `sre.failure_cases` | What happened in previous incidents |
| Periodic Memory | `memory.periodic_failure_patterns` | Recurring operational failure signatures |
| Product Knowledge (RAG) | PostgreSQL + pgvector `knowledge.chunks` | Official vendor documentation (RHOKP) |
| Operational Ground Truth | Splunk / Prometheus MCP adapters | Current platform telemetry (always supersedes memory) |

---

## Release Notes & Features

### v2.12.3: Generic Declarative Hypotheses & Capability Contracts
- **Declarative Hypothesis Schema**: Evaluates `supporting.all`, `supporting.any`, `contradicting.all`, and `contradicting.any` against live facts from declarative YAML policies.
- **First-Class Contradiction Handling**: Contradictory facts immediately exclude mechanisms and populate `excluded_mechanisms` with evidence provenance.
- **Contract-Level Coverage Validation**: Hardened capability coverage to validate domain, signal, and parameters against `GLOBAL_CONTRACT_REGISTRY`. Unregistered contracts fail closed with `COVERAGE_BLOCKED`.
- **Fact Ontology & Compiler**: Canonical `FACT_REGISTRY` in `engine/rules/facts.py` and policy validator preventing vocabulary drift.
- **Guarded LLM Probing**: LLM-suggested investigations are validated against capability contracts and restricted to read-only diagnostics.

### v2.11.0: Operational Learning Lifecycle
- Recurrence correlation tracking: `FIRST_SEEN`, `RECURRING_UNKNOWN`, `RECURRING_UNRESOLVED`, `RECURRING_RESOLVED`, and `KNOWN_ISSUE`.
- Explicit learning states distinguishing knowledge gaps from agent failures (`NEW_FAILURE`, `KNOWLEDGE_GAP`, `VALIDATED_KNOWLEDGE_AVAILABLE`).
- `record_resolution.py` interface for recording externally verified SRE outcomes.

### v2.10.x: Golden CBT Slice & Version Awareness
- Preserved exact VMware CBT vertical slice (`VMWARE.CBT.RETRY_LIMIT` &rarr; `INVESTIGATE_VMWARE_CBT`).
- Version-aware RHOKP semantic retrieval with deterministic compatibility filtering.

---

## Verification & Testing Standards

Before deploying or submitting changes:
```bash
# 1. Bytecode compilation
python -m compileall engine persistence simulator scripts tests

# 2. Complete test suite (unit + contracts + safety)
pytest -q

# 3. Live integration test suite (with local Kafka & PostgreSQL pgvector)
KAFKA_BOOTSTRAP_SERVERS="127.0.0.1:9092" RUN_INTEGRATION=1 RUN_LIVE_BENCHMARK=1 pytest -q
```
