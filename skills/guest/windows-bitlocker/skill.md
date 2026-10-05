# Windows BitLocker Encryption Investigation

## Objective
Determine whether Windows BitLocker volume encryption or unsupported Volume Master Key (VMK) metadata entry prevents virt-v2v disk inspection.

## Required evidence
- virt-v2v conversion log (`domain=conversion`, `signal=virt_v2v_log`).
- Guest BitLocker status (`domain=guest_os`, `signal=bitlocker_state`).
- Migration state (`domain=mtv`, `signal=migration_state`).

## Investigation procedure
1. Confirm error indicating `not a valid BITLK device` or `Unexpected metadata entry value '24'`.
2. Verify BitLocker encryption status on guest system volumes.
3. Recommend disabling or decrypting BitLocker on source VM prior to migration.
