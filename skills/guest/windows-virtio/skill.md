# Windows Missing VirtIO Drivers Investigation

## Objective
Determine whether migrated Windows VM fails to boot (`INACCESSIBLE_BOOT_DEVICE` / `0x0000007B`) due to missing VirtIO storage or block drivers.

## Required evidence
- Guest boot error log (`domain=guest_os`, `signal=boot_errors`).
- Guest driver status (`domain=guest_os`, `signal=driver_state`).
- Migration state (`domain=mtv`, `signal=migration_state`).

## Investigation procedure
1. Confirm boot crash with `INACCESSIBLE_BOOT_DEVICE` or bugcheck `0x7B`.
2. Verify if VirtIO drivers were injected during virt-v2v or pre-installed on the source.
3. Recommend installing Red Hat VirtIO drivers on source VM before migration or verifying virtio-win container image in MTV.
