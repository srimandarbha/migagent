# virt-v2v CDROM VMX Entry Investigation

## Objective
Determine whether conversion failed due to invalid CD-ROM configuration or missing ISO file references in the source VMX.

## Required evidence
- VMware VMX configuration (`domain=vmware`, `signal=vmx_configuration`).
- virt-v2v error log (`domain=conversion`, `signal=virt_v2v_log`).
- Migration state (`domain=mtv`, `signal=migration_state`).

## Investigation procedure
1. Inspect source VMX file for disconnected or invalid `ideX:Y.fileName` cdrom entries.
2. Recommend detaching disconnected ISOs or removing stale CD-ROM drives before migration.
