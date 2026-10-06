# Migration Failure Agent (MFA) — Operations & SRE Runbook

**Version:** 2.12.3  
**Classification:** Diagnostic V1 (Strictly Read-Only Orchestration Layer)  
**Target Platform:** Red Hat OpenShift Container Platform (OCP) 4.14+ / OpenShift Virtualization (MTV / Forklift)

---

## 1. System Overview & Trust Boundaries

The Migration Failure Agent (MFA) is a reasoning and diagnostic orchestration engine that consumes `MigrationFailed` events emitted by MTV/Forklift controllers, gathers authoritative telemetry via read-only capability adapters, correlates historical recurrence, and produces evidence-backed diagnoses with safe next-step recommendations.

### Architectural Ingress & Boundaries
```
Kafka (mfa.migration.failed)
     ↓
KafkaIngress Adapter (engine/ingress/kafka.py)
     ↓
MigrationFailureEngine (engine/workflow/engine.py)
     ↓
LangGraph Reasoning Graph (engine/workflow/graph.py)
     ├── Tier 1: Deterministic Policy & Knowledge Store (100% ground-truth)
     └── Tier 2: Bounded Advisory Node (Unknown/Insufficient only)
     ↓
KafkaResultPublisher (mfa.agent.result)
     ↓
Commit Kafka Offset (Strictly post-publish)
```

### Safety & Guardrails Guarantees
1. **Strictly Read-Only (Diagnostic V1)**: Never issues mutating requests (no pod deletions, CR patching, VM power operations, or pyVmomi calls).
2. **Current Evidence Overrules All**: Fresh platform telemetry supersedes historical knowledge, episodic memory, and LLM advice.
3. **LLM Is Never A Safety Boundary**: LLM output cannot flip `NOT_READY` into `READY`.
4. **Transport Fail-Closed**: No Kafka offset is committed before the result is acknowledged by Kafka or safely routed to the DLQ.
5. **Infrastructure vs. Poison-Pill Separation (N2 Fixed)**: Database or tracker outages cause partition pauses and 60s-capped backoffs without burning the poison-pill budget and without DLQ routing.

---

## 2. OpenShift Deployment & Configuration

### Prerequisites
- OpenShift namespace: `migration-system`
- AMQ Streams / Apache Kafka with topics:
  - `mfa.migration.failed` (partitioned input topic)
  - `mfa.agent.result` (diagnostic result topic)
  - `mfa.migration.failed.dlq` (dead letter queue)
- PostgreSQL 15+ with `pgvector` extension enabled (`mfa-postgresql`)

### Manifest Deployment
```bash
# 1. Apply Secret template (ensure passwords and tokens are replaced)
oc apply -f deploy/k8s/secret.example.yaml -n migration-system

# 2. Apply ConfigMap
oc apply -f deploy/k8s/configmap.yaml -n migration-system

# 3. Apply Service and ServiceMonitor for Prometheus scraping
oc apply -f deploy/k8s/service.yaml -n migration-system
oc apply -f deploy/k8s/servicemonitor.yaml -n migration-system

# 4. Deploy Agent
oc apply -f deploy/k8s/deployment.yaml -n migration-system

# 5. Verify deployment rollout
oc rollout status deployment/migration-failure-agent -n migration-system
```

---

## 3. Metrics, Health Probes & Monitoring

The agent includes an internal HTTP server running on port `8080`:
- `GET /livez`: Process liveness probe.
- `GET /readyz`: Readiness probe (validates Kafka consumer connectivity).
- `GET /metrics`: Standard Prometheus metrics export.

### Service Level Objectives (SLOs)
| Metric | SLO Target | Alert Threshold | Severity |
|---|---|---|---|
| **Diagnostic Latency** | p95 < 5.0 seconds | p95 > 8.0s over 5m | Warning |
| **Pipeline Availability** | 99.9% uptime | `mfa_up == 0` for 1m | Critical (P1) |
| **Kafka Consumer Lag** | < 10 messages | Lag > 50 messages for 10m | Warning (P2) |
| **DLQ Arrival Rate** | 0 events/hr | `increase(mfa_events_processed_total{status="DLQ"}[5m]) > 0` | Warning (P2) |
| **Transient Infra Outages** | 0 outages/hr | `increase(mfa_infrastructure_outages_total[5m]) > 3` | Critical (P1) |

### Key Prometheus Queries
```promql
# Overall processing throughput by outcome
sum by (status) (rate(mfa_events_processed_total[5m]))

# Rate of DLQ arrivals
rate(mfa_events_processed_total{status="DLQ"}[5m])

# Transient database / tracker outages
rate(mfa_infrastructure_outages_total[5m])

# Average event processing latency
rate(mfa_processing_duration_seconds_total[5m]) / rate(mfa_processing_events_measured_total[5m])

# Circuit breaker trips by capability
sum by (capability) (rate(mfa_circuit_breaker_trips_total[5m]))
```

---

## 4. Dead Letter Queue (DLQ) Operations

Events are routed to `mfa.migration.failed.dlq` under two conditions:
1. **Malformed Payload (`KafkaEventError`)**: Payload is not valid JSON, lacks `event_id`, or `event_type != "MigrationFailed"`.
2. **Deterministic Processing Failure**: 5 consecutive application retries failed on the exact same offset (poison-pill protection).

### Inspecting Parked DLQ Events
```bash
# Preview first 20 DLQ messages
python scripts/replay_dlq.py --mode list --limit 20

# Filter DLQ messages by specific error reason pattern
python scripts/replay_dlq.py --mode list --reason-filter "validation"

# Inspect specific event ID
python scripts/replay_dlq.py --mode list --event-filter "evt-cbt-failure-001"
```

### Replaying DLQ Events
Replaying republishes events back to `mfa.migration.failed` with tracing headers (`replayed_from_dlq: true`, `original_dlq_offset`) and safely commits the DLQ offset:

```bash
# Step 1: DRY RUN preview (default - does NOT modify state)
python scripts/replay_dlq.py --mode replay --reason-filter "CSI timeout"

# Step 2: EXECUTE replay and commit DLQ offset
python scripts/replay_dlq.py --mode replay --reason-filter "CSI timeout" --execute
```

---

## 5. Shadow Mode Deployment Protocol (2–4 Weeks)

Before promoting MFA to active SRE notification channels:
1. **Configure Shadow Target**:
   Update `deploy/k8s/configmap.yaml` to publish results to `mfa.agent.result.shadow`:
   ```yaml
   KAFKA_AGENT_RESULT_TOPIC: "mfa.agent.result.shadow"
   ```
2. **Dual-Triage Comparison**:
   - For every production migration failure, SREs log manual triage outcomes in Jira/ServiceNow.
   - Run daily audit comparing agent diagnoses against human SRE verdicts:
     ```bash
     python scripts/read_agent_results.py --topic mfa.agent.result.shadow --limit 50
     ```
3. **Acceptance Criteria for Production Cutover**:
   - Zero hallucinated root causes or actions (100% adherence to §5).
   - > 95% diagnostic concurrence on known failure patterns (CBT, VDDK, CSI, Firmware, NAD).
   - Zero unhandled crash loops or uncommitted offset stalls.
   - SRE sign-off across storage, virtualization, and networking domains.

---

## 6. Incident Response Playbooks

### Playbook 1: PostgreSQL Outage / Connection Pool Exhaustion
- **Symptom**: Logs show `Transient infrastructure outage (DurableStateError/OperationalError)`.
- **System Behavior**: KafkaIngress pauses partitions, backs off exponentially (capped at 60s), and does NOT commit offsets or send events to DLQ.
- **Recovery Action**:
  1. Check PostgreSQL health: `oc get pods -l app=mfa-postgresql -n migration-system`
  2. Inspect DB logs: `oc logs deployment/mfa-postgresql -n migration-system`
  3. Once DB recovers, KafkaIngress automatically resumes partitions and processes queued events without loss.

### Playbook 2: High Kafka Consumer Lag
- **Symptom**: Prometheus alert `MFAHighConsumerLag` (lag > 100).
- **Diagnosis**:
  1. Check capability latency: Inspect `mfa_processing_duration_seconds_total`.
  2. Check upstream Prometheus/Splunk response times.
  3. Verify whether circuit breakers are tripping (`mfa_circuit_breaker_trips_total`).
- **Mitigation**:
  1. Increase partition count on `mfa.migration.failed`.
  2. Scale deployment replicas to match partition count:
     ```bash
     oc scale deployment/migration-failure-agent --replicas=3 -n migration-system
     ```

### Playbook 3: Poison Pill Event
- **Symptom**: Log message `Partition exhausted 5 consecutive retries; routing to DLQ`.
- **System Behavior**: MFA routes the bad payload to `mfa.migration.failed.dlq` with diagnostic headers, commits the offset, and unblocks the partition.
- **Recovery Action**:
  1. Inspect the message: `python scripts/replay_dlq.py --mode list --limit 1`
  2. Fix underlying schema or application bug.
  3. Replay when resolved: `python scripts/replay_dlq.py --mode replay --event-filter <ID> --execute`

---

## 7. Rollback Plan

If an unexpected regression occurs in production:
1. Immediately scale down the MFA deployment to stop event processing:
   ```bash
   oc scale deployment/migration-failure-agent --replicas=0 -n migration-system
   ```
2. Revert image tag or configuration to prior release:
   ```bash
   oc rollout undo deployment/migration-failure-agent -n migration-system
   ```
3. Verify offset positions using Kafka consumer group tools:
   ```bash
   kafka-consumer-groups --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS --describe --group migration-failure-agent
   ```
4. Scale back up to 1 replica.
