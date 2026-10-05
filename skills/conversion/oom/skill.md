# virt-v2v OOM Killed Investigation

## Objective
Determine whether virt-v2v conversion pod was terminated by the Linux OOM killer (exit code 137).

## Required evidence
- OCV conversion pod status (`domain=ocv`, `signal=pod_status`).
- virt-v2v memory/kernel log (`domain=conversion`, `signal=virt_v2v_log`).
- Migration state (`domain=mtv`, `signal=migration_state`).

## Investigation procedure
1. Confirm pod termination with `OOMKilled` or exit code 137.
2. Recommend increasing conversion pod memory limits in ForkliftController CR.
