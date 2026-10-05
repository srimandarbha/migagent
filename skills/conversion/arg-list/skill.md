# Argument List Too Long (E2BIG) Investigation

## Objective
Determine whether image conversion command execution failed due to system argument length limits (`E2BIG`).

## Required evidence
- virt-v2v / conversion log (`domain=conversion`, `signal=virt_v2v_log`).
- Migration state (`domain=mtv`, `signal=migration_state`).

## Investigation procedure
1. Confirm `exec: argument list too long` or `E2BIG` error.
2. Check disk and snapshot counts attached to the VM.
3. Recommend reducing snapshot count or migrating disks in smaller batches.
