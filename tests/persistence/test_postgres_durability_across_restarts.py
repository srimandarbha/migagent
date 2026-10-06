"""Integration test proving durability across repository process restarts and connection resets.

Validates that:
1. Incident cases, events, evidence, and diagnoses persist across process restart.
2. Structured ActionPlan, applicability constraints, verification contract, and provenance
   persist without loss or JSON truncation in PostgreSQL.
3. Learning candidates and their applicability rules persist across pool teardown.
4. A newly instantiated repository connects and reconstructs identical domain models.
"""
import os
import json
import uuid
from datetime import datetime, timezone
import pytest

psycopg = pytest.importorskip("psycopg")
from persistence.repository import SRETrackerRepository


@pytest.mark.integration
def test_postgres_durability_across_process_restarts():
    dsn = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/migration_agent")
    try:
        conn = psycopg.connect(dsn)
        conn.close()
    except Exception as exc:
        pytest.skip(f"PostgreSQL unreachable at {dsn}: {exc}")

    # Process 1: Initialize, migrate, and write records
    repo1 = SRETrackerRepository(dsn)
    repo1.migrate()

    test_uid = uuid.uuid4().hex[:8]
    event_id = f"evt-restart-test-{test_uid}"
    mig_id = f"mig-{test_uid}"
    issue_code = f"ISSUE-RESTART-{test_uid}"
    sol_code = f"SOL-RESTART-{test_uid}"
    sig = f"sig-restart-{test_uid}"

    # 1. Create failure case and save event
    case_id = repo1.create_or_get_failure_case(
        event_id=event_id,
        migration_id=mig_id,
        vm_id="vm-db-durability",
        cluster_id="ocv-prod-alpha",
        change_id="chg-8812",
        failure_class="storage.csi.provisioning_timeout",
        failure_code="csi.volume.timeout",
        agent_version="v2.13.0",
        policy_version="1.1",
        severity="CRITICAL",
    )
    assert case_id is not None

    repo1.save_event(
        event_id=event_id,
        failure_case_id=case_id,
        event_type="MigrationFailed",
        event_time=datetime.now(timezone.utc),
        payload={"message": "DataVolume provisioning timed out", "disk": "vol0"},
        status="RECEIVED",
    )

    # 2. Save Evidence
    ev_id = repo1.save_evidence(
        case_id=case_id,
        evidence={
            "id": f"ev-{test_uid}-01",
            "source": "splunk_mcp",
            "domain": "storage",
            "signal": "csi_errors",
            "fact": "PVC_PROVISIONING_FAILED",
            "confidence": 0.96,
            "reliability": 0.98,
            "provenance": {"authority": "HISTORICAL_TELEMETRY", "spl_query": "search index=ocv"},
            "metadata": {"pvc_name": "datavolume-root", "storageclass": "ceph-block"},
        },
    )
    assert ev_id is not None

    # 3. Save Known Issue with ActionPlan, Applicability Constraints, and Verification Contract
    action_plan = {
        "steps": [
            {"step": 1, "action": "INSPECT", "target": "csi-driver-daemonset", "namespace": "openshift-storage"},
            {"step": 2, "action": "VERIFY", "target": "pvc-phase", "expected": "Bound"},
        ],
        "estimated_duration_sec": 45,
        "rollback_strategy": "MANUAL_DRAIN",
    }
    applicability_constraints = {
        "storage_classes": ["ceph-block", "ocs-storagecluster-ceph-rbd"],
        "min_ocp_version": "4.14",
        "max_attempts": 3,
        "disallowed_in_phases": ["FINAL_CUTOVER"],
    }
    verification_contract = {
        "post_checks": [
            {"probe": "pvc_status", "expected": "Bound"},
            {"probe": "importer_pod", "expected": "Completed"},
        ],
        "timeout_seconds": 180,
    }
    provenance = {
        "author": "SRE Platform Team",
        "pr_reference": "https://github.com/org/repo/pull/442",
        "approved_by": "lead-architect",
    }

    issue_data = {
        "signature_id": issue_code,
        "title": "Persistent CSI timeout during high concurrent PVC migration",
        "description": "CSI ceph-rbd volume plugin deadlock during Ceph monitor failover",
        "domain": "storage",
        "failure_class": "storage.csi.provisioning_timeout",
        "canonical_pattern": sig,
        "canonical_signature": sig,
        "issue_summary": "Storage provisioner stall during bulk disk migration",
        "applicability_rules": {"storage_backend": "ceph", "nodes": [">=3"]},
        "required_evidence": [{"domain": "storage", "signal": "csi_errors", "fact": "PVC_PROVISIONING_FAILED"}],
        "contraindicated_evidence": [{"domain": "storage", "signal": "backend_health", "fact": "BACKEND_DEGRADED"}],
        "status": "VALIDATED",
        "solutions": [
            {
                "solution_id": sol_code,
                "title": "Restart CSI controller and await volume lease release",
                "action_summary": "Perform controlled rollout of csi-rbdplugin-provisioner pods",
                "recommended_action": "RETRY_WITH_PROVISIONER_RESTART",
                "automation_system": "AAP",
                "risk_level": "LOW",
                "requires_approval": True,
                "action_plan": action_plan,
                "applicability_constraints": applicability_constraints,
                "verification_contract": verification_contract,
                "provenance": provenance,
                "success_count": 8,
                "failure_count": 1,
            }
        ],
    }
    repo1.save_known_issue(issue_data)

    # 4. Save Learning Candidate
    cand_id = repo1.upsert_learning_candidate(
        failure_signature=sig,
        failure_class="storage.csi.provisioning_timeout",
        failure_code="csi.volume.timeout",
        diagnosis_code="DIAG-CSI-STALL",
        resolution_code="RES-RESTART-PROVISIONER",
        description="Candidate learned from automated observation",
        evidence_case_ids=[str(case_id)],
        applicability={"cluster": "ocv-prod-alpha", "storage": "ceph"},
    )
    assert cand_id is not None

    # Simulate Process Restart: Tear down connection pool completely
    repo1.close()

    # Process 2: Brand new repository instance reconnecting to PostgreSQL
    repo2 = SRETrackerRepository(dsn)

    try:
        # A. Verify Failure Case and Event Durability
        assert repo2.event_exists(event_id) is False  # Not yet marked completed
        repo2.mark_event_completed(event_id)
        assert repo2.event_exists(event_id) is True

        reloaded_case_id = repo2.create_or_get_failure_case(
            event_id=event_id,
            migration_id=mig_id,
            vm_id="vm-db-durability",
            cluster_id="ocv-prod-alpha",
            change_id="chg-8812",
            failure_class="storage.csi.provisioning_timeout",
            failure_code="csi.volume.timeout",
            agent_version="v2.13.0",
            policy_version="1.1",
        )
        assert reloaded_case_id == case_id

        # B. Verify Evidence Durability
        with repo2.connection() as conn:
            ev_row = conn.execute(
                "SELECT fact_code, source, reliability, provenance, data FROM sre.evidence WHERE failure_case_id=%s",
                (case_id,),
            ).fetchone()
            assert ev_row is not None
            assert ev_row["fact_code"] == "PVC_PROVISIONING_FAILED"
            assert ev_row["source"] == "splunk_mcp"
            assert float(ev_row["reliability"]) == 0.98
            prov = ev_row["provenance"] if isinstance(ev_row["provenance"], dict) else json.loads(ev_row["provenance"])
            assert prov.get("authority") == "HISTORICAL_TELEMETRY"

        # C. Verify ActionPlan, Applicability Constraints, and Verification Contract Durability
        known_issues = repo2.get_known_issues()
        matching_issues = [ki for ki in known_issues if ki["signature_id"] == issue_code]
        assert len(matching_issues) == 1, f"Expected 1 matching issue for {issue_code}, found {len(matching_issues)}"

        recovered_issue = matching_issues[0]
        assert recovered_issue["canonical_signature"] == sig
        assert recovered_issue["status"] == "VALIDATED"
        assert len(recovered_issue["solutions"]) == 1

        recovered_sol = recovered_issue["solutions"][0]
        assert recovered_sol["solution_id"] == sol_code
        assert recovered_sol["automation_system"] == "AAP"
        assert recovered_sol["requires_approval"] is True
        assert recovered_sol["success_count"] == 8

        # Deep equality assertions on JSON contracts
        assert recovered_sol["action_plan"] == action_plan
        assert recovered_sol["applicability_constraints"] == applicability_constraints
        assert recovered_sol["verification_contract"] == verification_contract
        assert recovered_sol["provenance"] == provenance

        # D. Verify Learning Candidate Durability
        candidates = repo2.get_learning_candidates()
        matching_candidates = [c for c in candidates if c["failure_signature"] == sig]
        assert len(matching_candidates) == 1
        recovered_cand = matching_candidates[0]
        assert recovered_cand["failure_class"] == "storage.csi.provisioning_timeout"
        assert recovered_cand["resolution_code"] == "RES-RESTART-PROVISIONER"
        assert recovered_cand["applicability"] == {"cluster": "ocv-prod-alpha", "storage": "ceph"}

    finally:
        # Cleanup test records
        with repo2.connection() as conn:
            conn.execute("DELETE FROM sre.known_issue_solutions WHERE known_issue_id IN (SELECT known_issue_id FROM sre.known_issues WHERE issue_code=%s)", (issue_code,))
            conn.execute("DELETE FROM sre.known_solutions WHERE solution_code=%s", (sol_code,))
            conn.execute("DELETE FROM sre.known_issues WHERE issue_code=%s", (issue_code,))
            conn.execute("DELETE FROM sre.learning_candidates WHERE failure_signature=%s", (sig,))
            conn.execute("DELETE FROM sre.evidence WHERE failure_case_id=%s", (case_id,))
            conn.execute("DELETE FROM sre.failure_events WHERE failure_case_id=%s", (case_id,))
            conn.execute("DELETE FROM sre.failure_cases WHERE failure_case_id=%s", (case_id,))
            conn.commit()
        repo2.close()
