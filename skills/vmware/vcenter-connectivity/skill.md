# VMware vCenter Connectivity Failure Investigation

## Objective
Determine whether migration failed because the target OpenShift environment cannot reach the vCenter API endpoint on TCP 443 or TLS certificate thumbprint is mismatched.

## Required evidence
- vCenter connectivity / TLS log (`domain=vmware`, `signal=vcenter_connectivity`).
- Migration state (`domain=mtv`, `signal=migration_state`).

## Investigation procedure
1. Confirm TCP 443 connectivity between Forklift controller / MTV worker and vCenter host.
2. Verify TLS certificate thumbprint matches the secret configured in Provider CR.
3. Recommend updating certificate thumbprint or checking egress firewall rules.
