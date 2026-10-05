# Windows Registry Hivex Error Investigation

## Objective
Determine whether virt-v2v failed during guest OS inspection due to Windows registry hive corruption or Hivex parsing exception.

## Required evidence
- virt-v2v inspection log (`domain=conversion`, `signal=virt_v2v_log`).
- Windows registry state (`domain=guest_os`, `signal=registry_state`).
- Migration state (`domain=mtv`, `signal=migration_state`).

## Investigation procedure
1. Confirm virt-v2v log contains `Hivex.Error` or unhandled exception in `inspect_os`.
2. Verify registry hive integrity and NTFS journal cleanliness.
3. Recommend guest filesystem check (`chkdsk`) or registry recovery before retry.
