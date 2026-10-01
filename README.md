# Migration Failure Agent v2.11.0


## v2.10.3 golden CBT vertical slice retained in v2.11

The current release includes the v2.10.3 fixes that established the exact Kafka CBT path:

```text
VMWARE.CBT.RETRY_LIMIT
        -> VMWARE.CBT
        -> vmware/cbt
        -> CBT evidence plan
        -> CBT_FAILED / TRANSFER_FAILED / CBT_QUERY_FAILED
        -> VMWARE.CBT_STATE
        -> LIKELY diagnosis
        -> INVESTIGATE_VMWARE_CBT
```

The v2.10.3 environment normalization also preserves nested `environment.migration.type`, and evidence evaluation exposes `collection_status` separately from diagnostic sufficiency.

## v2.11.0: Operational learning lifecycle

The agent now distinguishes **absence of knowledge** from **failure of the agent** and tracks recurrence without treating historical operator behavior as a trusted fix.

### New capabilities
- Deterministic failure signatures for recurrence correlation.
- `FIRST_SEEN`, `RECURRING_UNKNOWN`, `RECURRING_UNRESOLVED`, `RECURRING_RESOLVED`, and `KNOWN_ISSUE` states.
- Explicit learning states including `NEW_FAILURE`, `KNOWLEDGE_GAP`, `RESOLVED_HISTORY_UNVALIDATED`, `VALIDATED_KNOWLEDGE_AVAILABLE`, `MEMORY_DISABLED`, and `MEMORY_UNAVAILABLE`.
- Resolution records require an actual recorded resolution plus a passed verification before becoming a learning candidate.
- Human validation is the final promotion gate for reusable knowledge.
- A resolved case with an undocumented fix is retained as historical evidence but does not become a solution.
- Current evidence remains higher priority than historical actions or knowledge.
- `correlate_recurrence` is an explicit LangGraph node before evidence planning.
- `LearningLifecycle` provides the write-side contract for recording externally performed SRE outcomes without executing remediation.

### Important semantics
```text
NO_DATA from SRE Tracker + NO_DATA from RHOKP
    -> legitimate NEW_FAILURE / KNOWLEDGE_GAP state

SRE Tracker unavailable
    -> MEMORY_UNAVAILABLE; do not claim FIRST_SEEN

Same signature occurs again with no verified resolution
    -> RECURRING_UNRESOLVED

Previous occurrence recovered but exact fix was not recorded
    -> RECURRING_RESOLVED; resolution remains UNKNOWN

Verified resolution without human validation
    -> CANDIDATE

Verified + explicitly human validated
    -> VALIDATED_KNOWN_ISSUE / VALIDATED_KNOWLEDGE_AVAILABLE
```

The initial Migration Failure Agent remains read-only. `LearningLifecycle.record_resolution()` records what an SRE reports happened after the recommendation and does not execute the action.


## v2.10.2 state-propagation and adaptive-step fix

- Preserves LangGraph graph-level request/routing state across every workflow node.
- Keeps the original Kafka/request payload available to `persist_case` and `load_context`.
- Preserves adaptive collection requirements across node boundaries.
- Keeps `failure_case_id` semantics aligned with the persisted SRE Tracker identifier.
- Regression suite: 81 passed, 3 skipped (require a live PostgreSQL instance and a populated RAG benchmark index — not LangGraph, which is a hard dependency), 1 environment-dependent failure without a running database.



## Current state

- **Version:** 2.10.2
- **Purpose:** Read-only MTV migration-failure diagnosis and SRE next-step guidance.
- **Execution:** No remediation, retry, rollback, EDA, AAP, or ServiceNow action is executed by the diagnostic agent.
- **LLM:** Advisory only. LLM output is never evidence, diagnosis authority, approval, or execution authority.
- **Safety:** Retry readiness is classification-specific and policy-driven. Generic engine code does not contain storage-specific retry facts.
- **Decision readiness:** One canonical five-action view: `CONTINUE_MONITOR`, `RETRY`, `FIX_FORWARD`, `ROLLBACK`, `ESCALATE`.
- **Corpus:** 80 scenarios are defined; only scenarios with an implemented policy, skill, fixture, and expected-output test are executable.
- **Evaluation:** Planned matrix is 80 scenarios × 6 memory/evidence conditions = 480 evaluations. Planned rows are not counted as passing until executable.


## v2.10.2 explicit workflow graph

The production orchestration is now represented as explicit LangGraph nodes and conditional edges. The deterministic domain services remain the source of truth.

`START -> create_case -> classify -> persist_case -> load_context -> build_evidence_plan -> collect_evidence -> evaluate_evidence`

From evidence evaluation the graph either loops through `plan_next_evidence -> collect_evidence`, proceeds through `collect_optional -> evaluate_hypotheses -> diagnose`, or terminates investigation and diagnoses insufficient evidence. The final path is `diagnose -> [llm_advisory when insufficient] -> calculate_readiness -> recommend -> persist -> finalize -> END`.

The LLM remains advisory only. No remediation, EDA, AAP or ServiceNow execution was added. `run_agent(request)` remains unchanged.

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

## v2.10 version-aware investigation

The agent now records an environment fingerprint containing OCP/OCV/MTV and source VMware versions when available. RHOKP candidates are semantically retrieved first and then passed through deterministic version applicability checks. Version-mismatched knowledge is excluded from version-validated recommendations.

See `docs/TOOLS_AND_VERSION_AWARENESS.md` for the capability inventory and contracts.

## v2.11.0 Red Hat ingestion wiring

`scripts/ingest_redhat.py` is the database ingestion entrypoint. It can scrape authorized URLs or load an authorized normalized YAML export, then writes `knowledge.documents` and `knowledge.chunks` in PostgreSQL. `scripts/embed_knowledge.py` subsequently fills `knowledge.chunks.embedding` using the configured Nomic-compatible embedding endpoint. The old scrape-only behavior is no longer the default contract.

Useful commands:

```bash
python scripts/ingest_redhat.py --urls-file datasets/redhat_sources.yaml
python scripts/ingest_redhat.py --input-yaml datasets/redhat_knowledge_scraped.yaml
python scripts/embed_knowledge.py
python scripts/verify_knowledge_ingestion.py
```

Restricted Red Hat Knowledgebase solutions/articles must be supplied through an authorized export or authenticated retrieval mechanism. The ingestion code does not bypass Red Hat authentication.
