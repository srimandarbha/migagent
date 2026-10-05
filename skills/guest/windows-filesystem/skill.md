# Windows Filesystem Read-Only / Dirty Investigation

## Objective
Determine whether guest conversion failed because the NTFS volume was left dirty or mounted read-only (e.g. Windows Fast Startup or unclean shutdown).

## Required evidence
- virt-v2v conversion log (`domain=conversion`, `signal=virt_v2v_log`).
- Guest OS filesystem state (`domain=guest_os`, `signal=filesystem_state`).
- Migration state (`domain=mtv`, `signal=migration_state`).

## Investigation procedure
1. Confirm error indicating `filesystem was mounted read-only` or `ntfs dirty`.
2. Verify source VM power state and Fast Startup configuration.
3. Recommend performing clean shutdown (`shutdown /s /t 0`) on source VM.
