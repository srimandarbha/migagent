# VMware ESXi Connectivity Investigation

## Objective
Determine whether current evidence shows that the migration environment cannot reach the VMware ESXi endpoint on TCP 443.

## Required evidence
- Current ESXi connectivity evidence.
- Migration state.

## Procedure
1. Confirm the migration failed during a VMware interaction.
2. Verify current ESXi endpoint connectivity evidence.
3. Correlate the connectivity failure with the migration window.
4. Do not infer a firewall, DNS, or routing root cause unless current evidence establishes it.

## Safety
Read-only investigation only. Do not change firewall, routing, VMware, or cluster configuration.
