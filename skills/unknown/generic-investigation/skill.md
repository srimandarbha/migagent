# Generic Migration Failure Investigation

## Objective
Collect enough current evidence to classify and diagnose an otherwise unknown migration failure.

## Procedure
1. Confirm migration identity, VM, cluster and failed phase.
2. Search current migration and infrastructure evidence.
3. Collect evidence from the most relevant domain before proposing a cause.
4. Correlate timestamps across sources.
5. Generate competing hypotheses only when evidence supports them.
6. Keep the diagnosis INSUFFICIENT_EVIDENCE when required evidence cannot be verified.

## Safety
Unknown failures must not trigger automated remediation solely from LLM reasoning.
