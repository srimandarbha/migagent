# Network NAD Missing Investigation

## Objective
Determine whether the migration failed because the required network attachment definition or network attachment could not be resolved.

## Required evidence
- NAD state.
- Network attachment state.
- Migration phase/state.


## Capability requirements
The skill requests read-only capabilities through the Capability Registry. It never calls infrastructure APIs directly.

| Evidence | Capability | Parameters | Access |
|---|---|---|---|
| NAD state | `observability.search` | `domain=ocv`, `signal=nad_state` | read |
| Network attachment state | `observability.search` | `domain=ocv`, `signal=network_attachment_state` | read |
| Migration state | `observability.search` | `domain=mtv`, `signal=migration_state` | read |

The active evidence policy is the executable source of truth for which capabilities are required. The skill documents their intent and decision procedure.

## Investigation procedure
1. Confirm the failed phase is network configuration.
2. Verify whether the required NAD exists and is usable.
3. Correlate the network attachment error with the migration timestamp.
4. Check for evidence that the requested network was renamed, removed, or unavailable.
5. Do not infer a missing NAD solely from a generic network failure.
6. If required evidence is missing, stop at INSUFFICIENT_EVIDENCE.

## Safety
This skill does not authorize creating or modifying NADs.

## Verification
After remediation, verify the network attachment resolves and the migration progresses.
