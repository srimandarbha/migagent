# Windows VSS Snapshot Failure Investigation

## Objective
Determine whether Windows Volume Shadow Copy Service (VSS) or VMware Snapshot Provider failure prevented snapshot creation during warm migration.

## Required evidence
- Windows VSS service status (`domain=guest_os`, `signal=vss_state`).
- VMware snapshot error log (`domain=vmware`, `signal=snapshot_errors`).
- Migration state (`domain=mtv`, `signal=migration_state`).

## Capability requirements
| Evidence | Capability | Parameters | Access |
|---|---|---|---|
| VSS service status | `observability.search` | `domain=guest_os, signal=vss_state` | read |
| Snapshot errors | `observability.search` | `domain=vmware, signal=snapshot_errors` | read |
| Migration state | `observability.search` | `domain=mtv, signal=migration_state` | read |

## Investigation procedure
1. Confirm warm migration snapshot failure.
2. Verify VSS service state inside guest OS.
3. Check for VMware Snapshot Provider failure in VMware task logs.
4. If VSS service is stopped or in error, recommend starting VSS service.
5. If evidence is incomplete, remain INSUFFICIENT_EVIDENCE.
