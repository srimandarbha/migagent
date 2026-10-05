# VMware VDDK Permission Error 13 Investigation

## Objective
Determine whether VDDK failed to open disk snapshots due to insufficient access rights (Error 13).

## Required evidence
- VDDK transport log (`domain=vmware`, `signal=vddk_log`).
- VMware user permissions (`domain=vmware`, `signal=permissions`).
- Migration state (`domain=mtv`, `signal=migration_state`).

## Investigation procedure
1. Confirm `VixDiskLib: Error 13 (You do not have access rights to this file)` in transfer logs.
2. Verify vCenter user roles and privileges (`Cryptographic operations`, `Disk lease`, `Virtual machine snapshot`).
3. Recommend granting required VDDK privileges to the migration provider user account.
