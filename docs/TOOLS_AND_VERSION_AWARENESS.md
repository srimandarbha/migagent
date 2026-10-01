# Migration Failure Agent 2.10: Tool and Version-Awareness Contract

## 1. Tool families

| Capability | Purpose | Current local implementation | Production adapter |
|---|---|---|---|
| `observability.search` | Search current OCV/MTV/VMware/storage operational evidence | simulated Splunk fixture | Splunk/OTel-derived evidence service |
| `metrics.query` | Query current metrics | simulated Prometheus fixture | Prometheus |
| `knowledge.search` | Retrieve RHOKP/Red Hat knowledge candidates | fixture RHOKP adapter / PostgreSQL pgvector repository | RHOKP-backed knowledge adapter |
| `sre_tracker.search` | Historical failure correlation | fixture tracker / PostgreSQL tracker | PostgreSQL SRE Tracker |
| `sre_tracker.save` | Persist failure/evidence/diagnosis | tracker adapter | PostgreSQL |
| `environment.get_fingerprint` | Obtain OCP/OCV/MTV/source versions and migration context | event metadata fallback | ACM/OCV/MTV inventory adapter |
| `compatibility.check` | Determine whether knowledge applies to the observed environment | deterministic local resolver | same deterministic resolver over RHOKP metadata |
| `automation.execute` | Execute remediation | intentionally disabled | EDA/AAP, future scope |
| `approval.request` | HITL approval | intentionally disabled | ServiceNow/HITL, future scope |

The agent must not allow an LLM to substitute for a capability. The LLM can interpret evidence or provide advisory text, but current environment facts and compatibility decisions remain deterministic.

## 2. Environment fingerprint

The agent carries an `EnvironmentFingerprint` in `AgentState`:

```yaml
environment:
  cluster_id: ocv-prod-01
  ocp_version: 4.19.23
  ocv_version: 4.19.23
  mtv_version: 2.10.4
  source_provider: vmware
  source_provider_version: 8.0
  source_host_versions: [8.0]
  migration_type: warm
```

The production source for this data should be an injected capability, not direct API calls from graph nodes.

## 3. Knowledge applicability

Semantic retrieval and version applicability are separate operations:

```text
failure
  -> semantic RHOKP retrieval
  -> candidate knowledge
  -> deterministic applicability evaluation
  -> supported / background / excluded
```

Supported applicability states:

- `SUPPORTED_EXACT`
- `SUPPORTED_RANGE`
- `VERSION_UNKNOWN`
- `VERSION_MISMATCH`
- `HISTORICAL`
- `RETIRED`
- `CONFLICTING`
- `UNVERIFIED`

`VERSION_MISMATCH` must never be promoted to a supported remediation merely because its semantic similarity score is high.

## 4. Knowledge metadata

Structured applicability is stored with knowledge records when known:

```yaml
applicability:
  mtv:
    versions: ["2.10"]
  ocv:
    versions: ["4.18", "4.19", "4.20"]
```

Ranges can use `min` and `max`. Documents without structured version metadata remain usable as background context, not as version-validated fixes.

## 5. Graph placement

```text
load_context
    |
    v
load_environment
    |
    v
check_knowledge_compatibility
    |
    v
build_evidence_plan
```

This makes version applicability visible in the execution trace and checkpoint state.

## 6. Example

For an environment with OCV 4.19 and MTV 2.10:

```text
RH knowledge A
  OCV 4.19, MTV 2.10
  -> SUPPORTED_EXACT

RH knowledge B
  OCV 4.20+, MTV 2.12
  -> VERSION_MISMATCH

RH knowledge C
  no structured applicability
  -> VERSION_UNKNOWN
```

The recommendation may use A. B can be shown as a relevant but non-applicable reference. C can provide background context but must not be presented as a version-validated fix.

## 7. Version evidence provenance

Version facts should eventually be persisted with:

```text
field
value
source capability
observed_at
retrieved_at
reliability
```

This prevents an old inventory value or a guessed version from silently becoming the basis for a remediation recommendation.

## 8. Current Red Hat compatibility examples

Red Hat's MTV 2.10 documentation lists MTV 2.10 with OpenShift/OpenShift Virtualization 4.18, 4.19, and 4.20, and VMware vSphere 6.5 or later. MTV 2.12 lists OCP/OCV 4.20, 4.21, and 4.22, with VMware vSphere 6.5 or later.

Red Hat also documents live migration as requiring MTV 2.10 or later and OpenShift Virtualization 4.20 or later. These are examples of facts that belong in deterministic compatibility data rather than LLM inference.

## 9. Deterministic error -> solution -> version evaluation

The compatibility boundary is intentionally split into two deterministic checks after RAG retrieval:

```text
RAG / LLM candidate retrieval
        |
        v
structured error/failure match
        |
        v
version applicability
        |
        v
ELIGIBLE / NEEDS_VALIDATION / REJECT_*
```

The LLM must not override these results. A candidate with a matching failure code but a minimum MTV version above the installed version is rejected. A lower-version candidate is eligible on a higher version only when its applicability metadata explicitly permits that forward range.

Examples:

| Knowledge metadata | Current MTV | Deterministic result |
|---|---:|---|
| `min: 2.8`, error matches | 2.11 | `ELIGIBLE` |
| documented `2.8`, no applicability range | 2.11 | `NEEDS_VALIDATION` |
| `min: 2.12`, error matches | 2.10 | `REJECT_VERSION` |
| `min: 2.8`, different error code | 2.11 | `REJECT_ERROR_MISMATCH` |

A two-part exact constraint such as `4.19` represents the `4.19.x` product line. A three-part constraint such as `2.10.4` is an exact patch release.

SRE Tracker history can corroborate operational use on a version, but it does not rewrite RHOKP applicability metadata and does not override current evidence.
