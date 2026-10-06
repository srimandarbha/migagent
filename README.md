# Migration Failure Agent (MFA) v2.12.3

Production-oriented VMware &rarr; OpenShift Virtualization (OCV / MTV / Forklift) Migration Failure Decision Engine.

The agent investigates `MigrationFailed` events, correlates historical recurrence, gathers evidence through approved capability adapters, produces an evidence-backed root cause diagnosis and safe next step, and remains strictly fail-closed.

---

## Current State

- **Version:** 2.12.3
- **Purpose:** Read-only MTV migration-failure diagnosis and SRE next-step guidance.
- **Execution:** Strictly read-only diagnostic agent. No remediation, retry, rollback, EDA, AAP, or ServiceNow platform mutation is executed autonomously.
- **LLM Boundary:** Advisory only. LLM output is never evidence, diagnosis authority, approval, or execution authority. It is activated only on unknown failures or insufficient evidence.
- **Decision Readiness:** Canonical five-action view: `CONTINUE_MONITOR`, `RETRY`, `FIX_FORWARD`, `ROLLBACK`, `ESCALATE`.
- **CI & Quality Gate:** [![CI Quality Gate](https://github.com/openshift-virtualization/migration-failure-agent/actions/workflows/ci.yaml/badge.svg)](.github/workflows/ci.yaml) Verified continuously across Python 3.11/3.12 matrices, policy contract compilers, source corpus integrity, and full test suites via [`scripts/ci_check.sh`](file:///home/sdarbha/Downloads/mfa/scripts/ci_check.sh).

---

## Architecture & Principles

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
| **Procedural Memory** | `skills/*/skill.md` | How to investigate (investigation workflows, runbooks) |
| **Episodic Memory** | PostgreSQL `sre.failure_cases` | What happened in previous incidents |
| **Periodic Memory** | `memory.periodic_failure_patterns` | Recurring operational failure signatures |
| **Product Knowledge (RAG)** | PostgreSQL + pgvector `knowledge.chunks` | Official vendor documentation (RHOKP) |
| **Operational Ground Truth** | Splunk / Prometheus MCP adapters | Current platform telemetry (always supersedes memory) |

---

## How to Run in Real

### 1. Python Environment Setup

Always use `python -m pip` to ensure python and pip point to the same virtual environment:
```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m pip install -e .
```

---

### 2. Infrastructure & Prerequisites

The real runtime stack requires:
- **Python 3.11+**
- **PostgreSQL 15+ with `pgvector`** (default: `127.0.0.1:5432`)
- **Apache Kafka Broker** (default: `127.0.0.1:9092`)
- **Local or remote LLM / Embedding endpoint** (default embedding port: `127.0.0.1:11434`)

#### Start Local Kafka Broker
If using Podman/Docker, start the bundled Kafka container:
```bash
podman start mfa-kafka
```
Verify broker connectivity:
```bash
python -c "from confluent_kafka import Producer; p = Producer({'bootstrap.servers':'127.0.0.1:9092'}); print('Kafka OK. Topics:', list(p.list_topics(timeout=5).topics.keys()))"
```
If Kafka topics do not exist, initialize them:
```bash
python scripts/create_kafka_topics.py
```
*(Default topics: `mfa.migration.failed`, `mfa.agent.result`, `mfa.migration.failed.dlq`)*

#### Start & Seed PostgreSQL SRE Tracker
Verify pgvector extension:
```bash
psql -h 127.0.0.1 -U postgres -d migration_agent \
  -c "SELECT name, default_version FROM pg_available_extensions WHERE name='vector';"
```
Apply schemas and seed baseline knowledge records:
```bash
export DATABASE_URL='postgresql://postgres:postgres@127.0.0.1:5432/migration_agent'
python scripts/seed_postgres.py
```
*(Optional: Ingest custom Red Hat knowledge documentation via `python scripts/ingest_redhat.py`)*

#### Start Local Vector Embedding Endpoint
Serve embeddings using llama.cpp or Ollama on the standard port `11434`:
```bash
llama serve -hf nomic-ai/nomic-embed-text-v1.5-GGUF:Q4_K_M --embedding --host 127.0.0.1 --port 11434
```
Verify endpoint:
```bash
curl -s http://127.0.0.1:11434/v1/models
```

---

### 3. Environment Configuration (`.env`)

Configure your `.env` file in the project root (see `.env.example` for reference):
```ini
# Persistence
POSTGRES_HOST=127.0.0.1
POSTGRES_PORT=5432
POSTGRES_DB=migration_agent
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:5432/migration_agent
SRE_TRACKER_PROVIDER=postgres
KNOWLEDGE_PROVIDER=postgres_pgvector

# Observability Adapters
SPLUNK_PROVIDER=fixture
PROMETHEUS_PROVIDER=fixture

# LLM Advisory (groq, openrouter, or none)
LLM_PROVIDER=groq
GROQ_API_KEY=your_groq_key_here
GROQ_MODEL=llama-3.3-70b-versatile
GROQ_TIMEOUT=8.0

# Vector Embeddings
EMBEDDING_BASE_URL=http://127.0.0.1:11434/v1
EMBEDDING_MODEL=nomic-ai/nomic-embed-text-v1.5-GGUF:Q4_K_M
EMBEDDING_DIMENSION=768
EMBEDDING_DOCUMENT_PREFIX=search_document: 
EMBEDDING_QUERY_PREFIX=search_query: 

# Kafka Ingress & Egress
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092
KAFKA_MIGRATION_FAILED_TOPIC=mfa.migration.failed
KAFKA_AGENT_RESULT_TOPIC=mfa.agent.result
KAFKA_CONSUMER_GROUP=migration-failure-agent
KAFKA_AUTO_OFFSET_RESET=earliest
KAFKA_POLL_TIMEOUT_SECONDS=1.0
```

---

### 4. Running the Event-Driven Kafka Pipeline

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
Publish a real or test `MigrationFailed` payload into Kafka:
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

### 5. Running Interactive CLI Diagnosis (Direct Mode)

Diagnose an incident immediately without Kafka:

#### Direct Error Message:
```bash
python scripts/diagnose_failure.py --message "DataVolume provisioning timed out" --cluster-id ocv-prod-01 --dry-run
```

#### From JSON File:
```bash
python scripts/diagnose_failure.py --file examples/sample_failure_event.json --output-json /tmp/diagnosis.json --dry-run
```

Use `--dry-run` during diagnostic troubleshooting to prevent test executions from polluting recurrence counts in the production SRE Tracker.

---

### 6. Observability Adapters & Prompt Boundaries

- **DMZ Observability Adapters**: Platform access flows strictly through approved capability adapters in `engine/integrations/dmz/` (Splunk MCP, Prometheus MCP). Production adapters must register with `GLOBAL_CONTRACT_REGISTRY` and remain strictly read-only.
- **Prompt Isolation**: Prompts in `engine/llm/` and `prompts/` are isolated from deterministic policy rules. The LLM is strictly advisory and cannot alter safety gates or flip decision readiness.

---

### 7. Simulation & Scenario Matrix Testing

Test specific scenarios locally with fixture data:
```bash
python run_simulator.py storage-csi-timeout
python run_simulator.py storage-csi-backend-healthy
python run_simulator.py network-nad-missing
python run_simulator.py vmware-cbt-retry
python run_simulator.py insufficient-evidence
```

Run the 80-scenario benchmark matrix plan:
```bash
python scripts/run_v29_80_matrix.py
```

---

### 8. SRE Operational Learning Feedback Loop

When an engineer completes a real-world resolution, record the verified outcome to train episodic memory:
```bash
python scripts/record_resolution.py \
  --case-id "<failure-case-id>" \
  --action "FIX_STORAGE_QUOTA" \
  --verification "PASSED" \
  --notes "Expanded storage backend volume pool; PVC bound immediately."
```

---

## Verification & Testing Standards

Before deploying or submitting changes:
```bash
# 1. Bytecode compilation
python -m compileall engine persistence simulator scripts tests

# 2. Complete unit & safety test suite
pytest -q

# 3. Live integration test suite (with local Kafka, PostgreSQL, & pgvector embedding server)
KAFKA_BOOTSTRAP_SERVERS="127.0.0.1:9092" RUN_INTEGRATION=1 RUN_LIVE_BENCHMARK=1 pytest -q
```

---

## Version Evolution & Changelog

- **v2.12.3**: Generic declarative hypothesis schema (`supporting.all`, `supporting.any`, `contradicting.all`, `contradicting.any`); first-class contradiction exclusions; contract coverage validation with `GLOBAL_CONTRACT_REGISTRY`; native Groq and OpenRouter provider governance with 8.0s timeout and 1024 token caps; DLQ write-failure resilience (N1); zero-loss Kafka offset discipline.
- **v2.11.0**: Operational learning lifecycle; recurrence tracking (`FIRST_SEEN`, `RECURRING_UNKNOWN`, `RECURRING_UNRESOLVED`, `RECURRING_RESOLVED`, `KNOWN_ISSUE`); learning candidate persistence (`save_learning_candidate`); SRE outcome recording (`record_resolution.py`).
- **v2.10.x**: Golden CBT vertical slice preservation; version-aware RHOKP semantic retrieval with deterministic cluster version compatibility filtering (`ELIGIBLE` vs `REJECT_ERROR_MISMATCH`).
- **v2.9.x**: 80-scenario MTV diagnostic corpus (`datasets/mtv_source_backed_corpus.yaml`); 480-case evaluation matrix; canonical five-action readiness contract (`CONTINUE_MONITOR`, `RETRY`, `FIX_FORWARD`, `ROLLBACK`, `ESCALATE`).
- **v2.8.x**: Memory matrix modes (`both`, `sre`, `rhokp`, `none`); memory isolation preventing current-case self-contamination; structured `diagnosis_basis`, `root_cause`, and `investigation_package`; fail-closed readiness.
- **v2.7.x**: Initial two-tier diagnostic protocol (deterministic Tier 1 vs probabilistic Tier 2); LangGraph orchestration; fail-closed safety boundaries.
