# VMware VDDK Data Source NBD Export Investigation

## Objective
Determine whether VDDK NBD connection failed due to missing server export or data source configuration error.

## Required evidence
- VDDK connection log (`domain=vmware`, `signal=vddk_log`).
- MTV VDDK data source status (`domain=mtv`, `signal=vddk_data_source`).
- Migration state (`domain=mtv`, `signal=migration_state`).

## Investigation procedure
1. Confirm error `server has no export named ''` or `Unable to connect to vddk data source`.
2. Verify MTV VDDK init image configuration and NBD server endpoint.
3. Recommend validating the VDDK container image tag and provider settings.
