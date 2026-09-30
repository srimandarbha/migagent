# CSI Provisioning Timeout Investigation

## Objective
Determine which part of the target storage provisioning path blocked the migration.

## Required evidence
- Target PVC state.
- CSI provisioning error/state.
- Storage backend health.
- Migration phase/state.

## Capability requirements
The skill requests capabilities through the Capability Registry. It does not call Splunk, Prometheus, Kubernetes, VMware, or storage APIs directly. The active policy maps these requirements to capability invocations.

| Evidence | Capability | Parameters | Access |
|---|---|---|---|
| Target PVC state | `observability.search` | `domain=ocv`, `signal=pvc_state` | read |
| CSI provisioning state | `observability.search` | `domain=ocv`, `signal=csi_errors` | read |
| Storage backend health | `observability.search` | `domain=storage`, `signal=backend_health` | read |
| Migration state | `observability.search` | `domain=mtv`, `signal=migration_state` | read |

The agent engine loads this skill, loads the deterministic evidence policy, and the investigation tool invokes each capability through the registry. A skill never contains raw SPL, SQL, `kubectl`, shell commands, or credentials.

## Investigation procedure
1. Confirm that the migration is actually blocked on target storage.
2. Confirm the PVC and volume provisioning state.
3. Correlate the CSI error with the migration timestamp.
4. Check current backend health.
5. If the backend is healthy, investigate CSI controller/provisioning path rather than attributing the failure to the backend.
6. Use historical cases only as context. Current infrastructure evidence takes precedence.
7. If required evidence is missing, stop at INSUFFICIENT_EVIDENCE.

## Safety
This skill never authorizes remediation. Any action must pass the active safety policy and approval gate.

## Verification
A recovery is successful only after current evidence verifies the expected PVC/volume state and migration progress.
