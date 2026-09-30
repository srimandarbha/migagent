# VMware CBT Failure Investigation

## Objective
Determine whether Changed Block Tracking state or the VMware transfer task caused the migration failure.

## Required evidence
- VMware CBT state.
- VMware transfer task state.
- Migration phase/state.


## Capability requirements
The skill requests read-only capabilities through the Capability Registry. It never calls VMware, MTV, or infrastructure APIs directly.

| Evidence | Capability | Parameters | Access |
|---|---|---|---|
| VMware CBT state | `observability.search` | `domain=vmware`, `signal=cbt_state` | read |
| VMware transfer task state | `observability.search` | `domain=vmware`, `signal=task_state` | read |
| Migration state | `observability.search` | `domain=mtv`, `signal=migration_state` | read |

The active evidence policy is the executable source of truth for which capabilities are required. The skill documents their intent and decision procedure.

## Investigation procedure
1. Confirm the failure occurred during source transfer.
2. Verify CBT state from current evidence.
3. Correlate CBT and task errors with the migration timestamp.
4. Distinguish CBT failure from a generic VMware transfer failure.
5. If evidence is incomplete, remain INSUFFICIENT_EVIDENCE.

## Safety
Do not reset CBT or alter VMware state automatically from this skill. Any remediation requires an approved deterministic procedure.

## Verification
Verify a subsequent transfer task reaches the expected state before considering recovery successful.
