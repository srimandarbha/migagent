# Migration Failure Agent: AI Coding Instructions

## Mission
Build and maintain a production-oriented VMware -> OpenShift Virtualization (OCV) Migration Failure Agent (MFA). The agent is a reasoning/orchestration layer. It investigates `MigrationFailed` events, gathers evidence through approved capabilities, produces an evidence-backed diagnosis and safe next step, and remains fail-closed.

## Repository contract
- `engine/` contains domain workflow, rules, skills, memory and tools.
- `persistence/` contains PostgreSQL SRE Tracker and pgvector knowledge persistence.
- `engine/integrations/` contains capability adapters and registries.
- `engine/ingress/` contains external transport adapters such as Kafka.
- `skills/` is procedural memory. A skill explains how to investigate, not an unconditional root cause or mutation command.
- `policies/` contains deterministic evidence requirements and safety policy.
- `simulator/` is test-only fixture infrastructure. Never leak simulator behavior into production paths.
- `tests/` contains contract and scenario tests.
- `config/manifest.yaml` declares the agent contract.

## Framework boundaries
HARNESS -> AGENT ADAPTER -> AGENT ENGINE -> TOOLS -> CAPABILITY REGISTRY -> AUTHORITATIVE SYSTEMS.

The framework/execution layer and agent/domain reasoning layer must remain separate.

The Kafka consumer is an ingress adapter. LangGraph and `MigrationFailureEngine` must not import Kafka clients or poll topics.

## Input contract
The framework passes a plain Python dictionary to:

`engine.workflow.engine.run_agent(request)`

Supported forms:
- `{"event": <MigrationFailed event>, ...}`
- direct event dictionary for backward compatibility.

Kafka converts a message into the first form and adds transport metadata under `request["kafka"]`.

Required event fields:
- `event_id`
- `event_type == "MigrationFailed"`

Preserve `migration_id`, `vm_id`, `cluster_id`, `change_id`, `severity`, `failure_code`, `phase`, `message` when present. Never invent missing values.

## Safety rules
1. Current evidence has priority over historical memory and documentation.
2. LLM output is not a safety boundary.
3. Never fabricate platform data, root causes, commands, successful remediation, or capability responses.
4. Never allow an LLM to issue arbitrary SPL, SQL, Kubernetes, VMware or storage commands when an approved capability exists.
5. Recommendations, approvals, execution and verification are distinct states.
6. Diagnostic V1 is read-only. No direct Kubernetes/VMware/storage mutation.
7. Any future remediation must flow through approval -> EDA/AAP -> deterministic executor -> independent verification.
8. `event_id` is the Kafka delivery idempotency key. `failure_case_id` is the durable investigation identity.
9. Do not commit a Kafka offset until successful processing and result publication. Do not commit processing failures.
10. Malformed/unsupported events are rejected and committed to prevent poison-message blocking; add a DLQ before production if operational requirements demand replay.

## Memory separation
- skill.md: how to investigate.
- SRE Tracker: what happened in previous cases.
- periodic memory: recurring operational patterns.
- RHOKP RAG: product/documentation knowledge.
- Splunk/Prometheus: current operational evidence.

Historical memory never overrides current evidence.

## AgentState
Keep AgentState lightweight. It may contain objective, constraints, plan, evidence references, hypothesis references, diagnosis, action references and verification references. Do not create permanent PostgreSQL tables for prompts, chain-of-thought, or transient agent state.

## Required engine API
```python
def run_agent(request: dict): ...
def run_to_dict(state): ...
```

## Capability rules
Use the Capability Registry for platform access. Do not bypass it with raw clients in agent nodes. Capability contracts must define inputs, outputs, authorization, reliability, provenance and error behavior.

## Testing
Before claiming completion:
1. `python -m compileall engine persistence simulator scripts`
2. `pytest -q`
3. For local E2E, verify Kafka -> agent -> PostgreSQL -> result topic.
4. Report exactly what was executed and what depended on external infrastructure.

## Change discipline
- Inspect existing framework contracts before creating new APIs.
- Reuse existing capabilities, skills and tools.
- Do not silently change shared framework behavior.
- Do not invent capability IDs.
- Keep transport-specific code in `engine/ingress/`.
- Prefer deterministic rules over LLM decisions for safety, evidence sufficiency and state transitions.
