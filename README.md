# Migration Failure Agent v2.8.3

See `docs/V2.8.3_TESTING.md` for the exact test procedure.

## v2.6.2 changes

- Adds an explicit capability result contract: `SUCCESS`, `NO_DATA`, `UNKNOWN`, `UNAVAILABLE`, `ERROR`.
- Evidence now records both `observed_at` and `retrieved_at`, with provenance retained.
- Adds deterministic evidence sufficiency evaluation to AgentState.
- Diagnosis confidence is derived from the strongest supported hypothesis rather than a disconnected constant.
- Recovery gates consume the same verified evidence graph, including migration failure state.
- Optional metrics `NO_DATA` is recorded rather than silently discarded.
- v2.7 is reserved for the adaptive evidence-selection loop.

# Migration Failure Agent v2.5

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
