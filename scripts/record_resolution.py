"""Record an externally performed SRE resolution into PostgreSQL.

No remediation is executed. This command only records the SRE's reported action,
verification and optional human validation, then applies deterministic learning policy.
"""
from __future__ import annotations

import argparse
import json
import os

from engine.learning import LearningLifecycle
from persistence.repository import SRETrackerRepository


def main():
    p=argparse.ArgumentParser(description='Record and evaluate a migration failure resolution')
    p.add_argument('--dsn', default=os.getenv('DATABASE_URL'), help='PostgreSQL DSN or DATABASE_URL')
    p.add_argument('--case-id', required=True)
    p.add_argument('--resolution-code')
    p.add_argument('--description', required=True)
    p.add_argument('--outcome-status', default='RESOLVED', choices=['RESOLVED','UNRESOLVED','ESCALATED','ROLLED_BACK','ABANDONED'])
    p.add_argument('--verification-status', default='PASSED', choices=['PASSED','FAILED','INCONCLUSIVE'])
    p.add_argument('--recorded-by')
    p.add_argument('--validated-by')
    p.add_argument('--validation-reason')
    p.add_argument('--action-code')
    p.add_argument('--expected-state', default='{}')
    p.add_argument('--observed-state', default='{}')
    args=p.parse_args()
    if not args.dsn:
        p.error('--dsn or DATABASE_URL is required')

    tracker=SRETrackerRepository(args.dsn)
    tracker.migrate()
    case=tracker.get_failure_case(args.case_id)
    if not case:
        raise SystemExit(f'failure case not found: {args.case_id}')

    result=LearningLifecycle(tracker).record_resolution(
        failure_case_id=args.case_id,
        resolution_code=args.resolution_code,
        description=args.description,
        outcome_status=args.outcome_status,
        verification_status=args.verification_status,
        recorded_by=args.recorded_by,
        validated_by=args.validated_by,
        validation_reason=args.validation_reason,
        action={'code': args.action_code, 'description': args.description} if args.action_code else None,
        expected_state=json.loads(args.expected_state),
        observed_state=json.loads(args.observed_state),
        failure_signature=case.get('failure_signature'),
        failure_class=case.get('failure_class'),
        failure_code=case.get('failure_code'),
        diagnosis_code=case.get('diagnosis_code'),
        environment_context={},
    )
    print(json.dumps(result, indent=2, default=str))


if __name__ == '__main__':
    main()
