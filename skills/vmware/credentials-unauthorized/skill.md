# VMware Credentials Unauthorized Investigation

## Objective
Determine whether migration failed due to invalid credentials or expired sessions (HTTP 401 Unauthorized) when communicating with VMware vCenter or ESXi.

## Required evidence
- VMware authentication log (`domain=vmware`, `signal=auth_errors`).
- Migration state (`domain=mtv`, `signal=migration_state`).

## Investigation procedure
1. Confirm HTTP 401 Unauthorized or `vir_from_esx ... http response code 401`.
2. Verify vCenter provider Secret in OpenShift (`username`, `password`, `insecureSkipVerify`).
3. Recommend updating provider secret with active credentials.
