# MFA Framework Contract

## 1. What is the framework?

The framework provides the execution boundary around agents. An agent provides domain intelligence.

```text
Harness
  -> registered adapter
  -> agent engine
  -> workflow/rules/skills/tools
  -> Capability Registry
  -> authoritative systems
```

For this project, Kafka is only an ingress transport:

```text
Kafka topic: mfa.migration.failed
  -> KafkaIngress
  -> normalized request dict
  -> MigrationFailureEngine.run(request)
  -> result topic: mfa.agent.result
```

## 2. Original input support

The original MFA framework already supports input as a plain dictionary. Its business entry point is:

```python
run_agent(request: Dict[str, Any])
```

The engine accepts either:

```python
{"event": event, "failure_case_id": "..."}
```

or a direct event dictionary for backward compatibility.

Therefore Kafka does not change the agent contract. It is an adapter that converts Kafka's `bytes` payload plus Kafka metadata into the existing request dictionary.

## 3. Why Kafka is outside the graph

The reasoning engine should be reusable from Kafka, CLI, tests, REST, or another event bus. The transport adapter owns polling, offsets, consumer groups, serialization and publication.

## 4. Idempotency

- Kafka `event_id` identifies the event delivery.
- PostgreSQL `sre.failure_events.event_id` is unique.
- `failure_case_id` identifies the durable investigation.
- Kafka offset is committed only after successful processing/result publication.

## 5. Another IDE

`AGENTS.md` is recommended. It is not the runtime contract and is not required by Python. It gives AI coding assistants repository-specific rules. Keep it concise and put detailed architecture in this file and `README.md`.

For an IDE/agent that does not automatically read `AGENTS.md`, explicitly instruct it to read:

1. `AGENTS.md`
2. `docs/FRAMEWORK_CONTRACT.md`
3. `config/manifest.yaml`
4. `engine/contracts/`
5. `engine/integrations/registry.py`
6. `engine/workflow/engine.py`
7. `policies/`
8. `skills/`
9. `tests/`

The AI must inspect these before changing code.

## Skill-to-tool wiring
A skill is procedural memory, not a tool implementation. The wiring path is:

SKILL.md -> deterministic evidence policy -> InvestigationTool -> Capability Registry -> adapter/backend.

SKILL.md documents the evidence needed, the intent of each capability, decision logic, safety and verification. The YAML policy is the executable evidence contract and supplies capability IDs plus parameters. InvestigationTool invokes those IDs only through the Capability Registry. Backend details such as SPL, SQL, Kubernetes APIs, VMware APIs and storage APIs belong in the capability adapter, never in the skill.

For a known failure, the engine must:
1. classify the event deterministically when a trusted failure_code or scenario is available;
2. load the matching skill;
3. load the matching evidence policy;
4. invoke each required capability through the registry;
5. record capability attempts, evidence, provenance and errors;
6. evaluate evidence sufficiency before diagnosis or recovery recommendations.

For an unknown failure, the generic skill must not invent a tool list. Classification or an evidence-discovery policy must first determine the relevant read-only capabilities.
