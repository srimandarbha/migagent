# Migration Failure Agent: AI Coding Instructions & Engineering Standards

## 1. Mission
Build and maintain a production-oriented VMware -> OpenShift Virtualization (OCV) Migration Failure Agent (MFA). The agent is a reasoning/orchestration layer. It investigates `MigrationFailed` events, gathers evidence through approved capabilities, produces an evidence-backed diagnosis and safe next step, and remains strictly fail-closed.

## 2. Repository Contract & Module Boundaries
- `engine/workflow/`: LangGraph orchestration, node implementations, and legacy parity runner.
- `engine/contracts/`: Authoritative domain schemas (`AgentState`, `Evidence`, `Readiness`, `DecisionReadiness`, capability contracts).
- `engine/rules/`: Deterministic rules (classification, facts, causal chains, hypothesis scoring, recovery feasibility, safety gates).
- `engine/memory/`: Dynamic procedural & episodic memory (`DynamicKnowledgeStore`, `ActionOntology`, `LearningPipeline`, recurrence correlation).
- `engine/llm/`: Bounded parametric advisory, version compatibility judge, and model provider integrations.
- `engine/tools/`: Investigation execution, capability coverage calculation, timeout and circuit-breaker resilience guards.
- `engine/integrations/`: Capability adapter contracts, DMZ MCP integrations (Splunk, Prometheus), and registries.
- `engine/ingress/`: Transport adapters (Kafka).
- `engine/observability/`: Diagnostic latency, error catalog, and telemetry meters.
- `persistence/`: PostgreSQL SRE Tracker, pgvector knowledge repository, and relational schemas.
- `policies/`: Declarative YAML evidence definitions and deterministic hypothesis formulas.
- `skills/`: Human-readable procedural investigation runbooks.
- `simulator/`: Test fixture infrastructure. NEVER leak simulator imports into production engine code.
- `tests/`: Contract, scenario, safety gate, and resilience unit suites.
- `config/manifest.yaml`: Declares the agent contract.

## 3. Framework Boundaries & Ingress Contract
HARNESS -> AGENT ADAPTER -> AGENT ENGINE -> TOOLS -> CAPABILITY REGISTRY -> AUTHORITATIVE SYSTEMS.

- The framework/execution layer and agent/domain reasoning layer must remain separate.
- The Kafka consumer is strictly an ingress adapter. LangGraph and `MigrationFailureEngine` must never import Kafka libraries or poll topics.
- Engine entry point: `engine.workflow.engine.run_agent(request)`.
- Supported request forms:
  - `{"event": <MigrationFailed event>, ...}`
  - Direct event dictionary for backward compatibility.
- Kafka converts a message into the first form and adds transport metadata under `request["kafka"]`.
- Required event fields:
  - `event_id`
  - `event_type == "MigrationFailed"`
- Preserve `migration_id`, `vm_id`, `cluster_id`, `change_id`, `severity`, `failure_code`, `phase`, `message`. Never invent missing values.

## 4. Two-Tier Diagnostic Protocol (Deterministic vs. Probabilistic)
- **Tier 1 (Known Pattern)**:
  - When classification or dynamic knowledge signatures match with high confidence, diagnosis is derived **100% deterministically** using policy rules and verified facts.
- **Tier 2 (Novel / Unknown Failure)**:
  - If classification is `UNKNOWN` or evidence is `INSUFFICIENT_EVIDENCE`, the bounded LLM Advisory Node is activated.
  - The LLM may formulate a `parametric_hypothesis`, suggest SRE diagnostic queries, and propose exploratory investigations.
  - Exploratory tool calls MUST be validated against `GLOBAL_CONTRACT_REGISTRY` and restricted strictly to **read-only diagnostic capabilities**.
  - LLM exploratory evidence may corroborate facts, but **CANNOT autonomously clear safety blockers**.

## 5. Safety, Remediation & Fail-Closed Guardrails
1. **Current evidence overrules all**: Fresh platform telemetry always supersedes historical memory, documentation, and LLM suggestions.
2. **LLM is never a safety boundary**: LLM output cannot flip a `NOT_READY` state into `READY`.
3. **Strictly Read-Only (Diagnostic V1)**: The diagnostic agent never mutates Kubernetes, VMware, storage, or network state.
4. **No Arbitrary Commands**: An LLM is strictly prohibited from generating arbitrary SPL, SQL, bash, K8s, or pyVmomi commands. Platform access must flow exclusively through approved capability adapters. LLM may propose allowlisted, display-only diagnostic commands; never executed, never arbitrary.
5. **Separation of Concerns**: Recommendation, approval, execution, and verification are independent states. Any future remediation must flow through approval -> EDA/AAP -> deterministic executor -> independent verification.
6. **Zero Hallucination Tolerance**: Never fabricate platform metrics, logs, root causes, commands, or remediation outcomes.

## 6. Resilience, Concurrency & Transport Operations
1. **Idempotency**: `event_id` is the Kafka delivery idempotency key. `failure_case_id` is the durable incident identity.
2. **Kafka Offset & Commit Discipline**:
   - *Successful Processing (Sufficient OR Insufficient Evidence)*: Publish result to result topic, then commit offset.
   - *Malformed / Poison-Pill Event*: Route immediately to Dead Letter Queue (DLQ), log error, and commit offset to prevent pipeline stall.
   - *Transient System Outage (Database / Kafka down)*: Do not commit offset; retry with exponential backoff; fail-fast if unrecoverable.
3. **Capability Timeouts & Circuit Breakers**:
   - Every capability invocation must adhere to a strict SLA deadline (default 5.0s, configurable via `MFA_CAPABILITY_TIMEOUT_SECONDS`).
   - Tools must implement a Circuit Breaker pattern (fail-fast with `CIRCUIT_OPEN` after 3 consecutive upstream timeouts or 5xx errors).
4. **Data Sanitization & Secret Scrubbing**:
   - All passwords, bearer tokens, authorization headers, and client secrets must be scrubbed before passing context to LLM providers or persisting trace logs.

## 7. LLM Operational Governance
- Max completion tokens capped at 1024 per call.
- Per-call timeout of 8.0s; on timeout or provider error, fail safe to `{}` and continue deterministic pipeline.
- Prompts must remain minimal and focused on error context, avoiding dumps of raw binaries or entire cluster configurations.

## 8. Memory Separation
- `skill.md`: Procedural memory (how to investigate, not an unconditional root cause or mutation command).
- `SRE Tracker`: Episodic memory (what happened in previous cases).
- `Periodic Memory`: Recurrence correlation (recurring operational patterns).
- `RHOKP RAG`: Documentation & knowledge base (product / vendor documentation).
- `Splunk / Prometheus`: Operational evidence (current live ground truth).

Historical memory never overrides current evidence.

## 9. AgentState Contract
Keep `AgentState` lightweight. It contains objectives, constraints, plan, evidence references, hypothesis references, diagnosis, action references, and verification references. Do not create permanent PostgreSQL tables for prompts, chain-of-thought, or transient agent state.

## 10. Required Engine API
```python
def run_agent(request: dict): ...
def run_to_dict(state): ...
```

## 11. Verification & Testing Standards
Before claiming completion of any task:
1. `python -m compileall engine persistence simulator scripts tests`
2. `pytest -q` (all unit, integration, and scenario tests must pass).
3. Verify backward compatibility against `decision_readiness` contracts.
4. Report exactly what was executed and what depended on external infrastructure.

## 12. Change Discipline
- Inspect existing framework contracts before creating new APIs.
- Reuse existing capabilities, skills, and tools.
- Do not silently change shared framework behavior.
- Do not invent capability IDs.
- Keep transport-specific code in `engine/ingress/`.
- Prefer deterministic rules over LLM decisions for safety, evidence sufficiency, and state transitions.
