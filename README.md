# Migration Failure Agent v2.9.2

## Current state

- **Version:** 2.9.2
- **Purpose:** Read-only MTV migration-failure diagnosis and SRE next-step guidance.
- **Execution:** No remediation, retry, rollback, EDA, AAP, or ServiceNow action is executed by the diagnostic agent.
- **LLM:** Advisory only. LLM output is never evidence, diagnosis authority, approval, or execution authority.
- **Safety:** Retry readiness is classification-specific and policy-driven. Generic engine code does not contain storage-specific retry facts.
- **Decision readiness:** One canonical five-action view: `CONTINUE_MONITOR`, `RETRY`, `FIX_FORWARD`, `ROLLBACK`, `ESCALATE`.
- **Corpus:** 80 scenarios are defined; only scenarios with an implemented policy, skill, fixture, and expected-output test are executable.
- **Evaluation:** Planned matrix is 80 scenarios × 6 memory/evidence conditions = 480 evaluations. Planned rows are not counted as passing until executable.

## v2.9.2 changes

- Removed hard-coded storage PVC/backend retry prerequisites from the generic safety gate.
- Added per-classification `decision_readiness.retry.required_facts` policies.
- Added cross-classification retry-readiness regression tests for CSI, CBT, NAD, and ESXi connectivity.
- Made `decision_readiness` the single canonical five-action readiness view; legacy `recovery` is retained only as a compatibility projection.
- Updated repository version metadata and README current-state header.


Production-oriented VMware -> OpenShift Virtualization migration-failure decision engine.

## Included

- LangGraph-compatible workflow boundary with deterministic engine underneath.
- Explicit `AgentState`: objective, success conditions, constraints, context, evidence, hypotheses, diagnosis, recovery, verification-oriented status and trace.
- Procedural memory in `skills/*/skill.md`.
- PostgreSQL SRE Tracker in `sre` schema.
- Deterministic periodic memory in `memory.periodic_failure_patterns`.
- PostgreSQL + pgvector RHOKP/RAG model in `knowledge.documents` and `knowledge.chunks`, with vector search exposed by `persistence/knowledge.py`.
- Red Hat ingestion via `scripts/ingest_redhat.py` for public/authorized sources.
- Common local dataset consumed by fixture Splunk, Prometheus, RHOKP and SRE Tracker adapters.
- OpenRouter and local OpenAI-compatible LLM provider abstraction.
- Fail-closed recovery gates. Recommendations, approvals, execution and verification remain distinct states.
- Simulator adversarial scenarios including healthy-backend contradiction and insufficient evidence.
- DMZ adapter boundary remains separate from local fixtures.

## Memory separation

| Layer | Purpose |
|---|---|
| skill.md | How to investigate |
| SRE Tracker | What happened in previous cases |
| periodic memory | What repeats over time |
| RHOKP/RAG | Product/documentation knowledge |
| Splunk/Prometheus | Current operational evidence |

Current evidence has priority over historical memory and documentation when determining the current case.

Default local embeddings use Nomic Embed Text v1.5 at 768 dimensions through a llama.cpp OpenAI-compatible endpoint. Document/query retrieval prefixes are applied separately.

## Local PostgreSQL

See `LOCAL_RUN.md`.

Default local DSN:

`postgresql://postgres:postgres@127.0.0.1:5432/migration_agent`

The project does not create a second PostgreSQL container.

## Important RAG dimension note

The bundled SQL uses `vector(768)` because the local embedding contract defaults to a 768-dimensional model. If a different embedding model is selected, its actual dimension must match the database schema. Embedding dimension is not inferred by the agent.

## Kafka ingress

The agent accepts `MigrationFailed` events from Kafka through `engine.ingress.kafka.KafkaIngress`. Kafka is not part of the LangGraph/business engine.

Default topics:

- input: `mfa.migration.failed`
- output: `mfa.agent.result`

Start the consumer:

```bash
python scripts/run_kafka_agent.py
```

Create topics:

```bash
python scripts/create_kafka_topics.py
```

Publish a test event:

```bash
cat <<'JSON' | python scripts/publish_migration_failed.py
{"event_id":"evt-local-001","event_type":"MigrationFailed","event_time":"2026-09-30T18:00:00Z","migration_id":"mig-123","vm_id":"vm-456","cluster_id":"ocv-prod-01","failure_code":"storage.csi.provisioning_timeout","severity":"critical","phase":"PVC provisioning","message":"DataVolume provisioning timed out"}
JSON
```

Read results:

```bash
python scripts/read_agent_results.py
```

The consumer uses a manual Kafka commit. Processing errors are not committed, so the event can be redelivered. Duplicate `event_id` values are recognized through the SRE Tracker when PostgreSQL is configured.

## AI coding-agent contract

Read `AGENTS.md` before modifying this repository. It defines the repository-specific AI coding rules. `docs/FRAMEWORK_CONTRACT.md` explains how the original framework input contract maps Kafka messages into the existing `run_agent(request)` interface.

## v2.9.1 LLM and corpus runner

The agent now loads a local `.env` automatically. Existing shell environment variables take precedence.

Example `.env`:

```text
LLM_PROVIDER=openrouter
OPENROUTER_API_KEY=your_token_here
OPENROUTER_MODEL=openrouter/free
```

The LLM is advisory only. It is called when deterministic evidence is insufficient. Its output is never treated as infrastructure evidence, diagnosis authority, approval, or remediation execution.

Execute a real corpus evaluation for an implemented scenario:

```bash
python scripts/run_v29_corpus_matrix.py --scenario MTV-009 --condition BOTH
```

Use `--provider openrouter` only if you want to override `LLM_PROVIDER` from `.env`.

To exercise the LLM advisory path, use an insufficient/unknown case with OpenRouter configured. A sufficient case such as MTV-009 will normally report `llm_advisory.status=NOT_REQUESTED` because deterministic evidence already establishes the mechanism.
