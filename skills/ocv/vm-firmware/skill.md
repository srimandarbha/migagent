# OCV VM Firmware Conflict Investigation

## Objective
Determine whether KubeVirt admission webhook denied VM creation due to mutually exclusive bootloader firmware settings (both EFI and BIOS configured).

## Required evidence
- VM template specification (`domain=ocv`, `signal=vm_spec`).
- KubeVirt admission webhook events (`domain=ocv`, `signal=admission_events`).
- Migration state (`domain=mtv`, `signal=migration_state`).

## Investigation procedure
1. Confirm admission webhook rejection in KubeVirt validator logs.
2. Inspect target VM spec for conflicting `domain.firmware.bootloader` entries.
3. Recommend reconciling plan firmware mappings to select either EFI or BIOS exclusively.
