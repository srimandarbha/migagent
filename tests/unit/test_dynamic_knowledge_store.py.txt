"""Unit tests for Dynamic Knowledge Store, Action Ontology, and Learning Pipeline."""
import pytest
from engine.memory.action_ontology import (
    ActionCategory,
    ActionDefinition,
    ActionPlan,
    ActionType,
    ActionValidator,
    RiskLevel,
)
from engine.memory.dynamic_knowledge_store import (
    DynamicKnowledgeStore,
    EvidenceEvaluationStatus,
    FailureSignature,
    KnownSolution,
)
from engine.memory.learning_pipeline import LearningPipeline
from engine.memory.normalizer import FailureNormalizer
from engine.memory.seed_data import get_seed_signatures
from engine.workflow.engine import MigrationFailureEngine
from engine.integrations.registry import InMemoryCapabilityRegistry


class TestFailureNormalizer:
    def test_normalizer_replaces_tokens_and_produces_stable_hash(self):
        log1 = "2026-10-05T14:32:00Z error: storage core device list -d naa.600009700001200047295141 on pod virt-v2v-abc12-xyz89 failed"
        log2 = "2026-10-05T18:00:00Z error: storage core device list -d naa.500009800009900012345678 on pod virt-v2v-def34-uvw56 failed"

        norm1 = FailureNormalizer.normalize(log1)
        norm2 = FailureNormalizer.normalize(log2)

        # Both should normalize to identical canonical representation
        assert norm1.normalized_text == norm2.normalized_text
        assert norm1.signature_hash == norm2.signature_hash
        assert "<NAA_ID>" in norm1.normalized_text
        assert "<POD_NAME>" in norm1.normalized_text
        assert "<TIMESTAMP>" in norm1.normalized_text

    def test_normalizer_handles_uuids_and_ips(self):
        raw = "Connection to 192.168.1.100 failed for pvc-12345678-1234-1234-1234-123456789abc"
        norm = FailureNormalizer.normalize(raw)
        assert "<IP_ADDR>" in norm.normalized_text
        assert "<VOLUME_NAME>" in norm.normalized_text


class TestActionOntologyAndValidator:
    def test_action_definition_category_assignment(self):
        act_obs = ActionDefinition("A1", ActionType.COLLECT_LOGS, "Collect Logs", "Collects logs")
        assert act_obs.category == ActionCategory.OBSERVE
        assert not act_obs.requires_approval

        act_rep = ActionDefinition("A2", ActionType.SOURCE_VM_REPAIR, "Repair Symlinks", "Fixes broken links")
        assert act_rep.category == ActionCategory.REPAIR
        assert act_rep.requires_approval  # Auto-requires approval for REPAIR
        assert act_rep.approval_role == "SRE"

    def test_action_validator_rejects_missing_fields(self):
        act = ActionDefinition("", ActionType.COLLECT_LOGS, "", "")
        res = ActionValidator.validate_action(act)
        assert not res.valid
        assert any("action_id" in e for e in res.errors)
        assert any("title" in e for e in res.errors)

    def test_action_validator_requires_approval_for_repairs(self):
        act = ActionDefinition(
            action_id="A1",
            action_type=ActionType.SOURCE_VM_REPAIR,
            title="Repair Script",
            description="Executes in guest",
            verification_plan="Verify exit code 0",
        )
        res = ActionValidator.validate_action(act)
        assert res.valid
        assert act.requires_approval

    def test_plan_validator_enforces_safety_and_verification(self):
        step1 = ActionDefinition("S1", ActionType.SOURCE_VM_REPAIR, "Fix config", "Repair config", verification_plan="Check config")
        step2 = ActionDefinition("S2", ActionType.VALIDATE, "Verify status", "Validate healthy")
        plan = ActionPlan("P1", "SIG-1", steps=[step1, step2], rationale="Repair and verify")

        res = ActionValidator.validate_plan(plan)
        assert res.valid
        assert plan.requires_human_approval


class TestDynamicKnowledgeStoreAndSeedData:
    def test_seed_signatures_contain_all_14_fixes(self):
        seeds = get_seed_signatures()
        assert len(seeds) == 14
        sig_ids = {s.signature_id for s in seeds}
        assert "SIG-STORAGE-NAA-OFFLOAD" in sig_ids
        assert "SIG-GUEST-INSPECT-SYMLINKS" in sig_ids
        assert "SIG-VMWARE-CPUID-CORES" in sig_ids
        assert "SIG-MIG-PAYLOAD-OBJECT" in sig_ids
        assert "SIG-GUEST-AUGEAS-FSTAB" in sig_ids
        assert "SIG-GUEST-INODES-EXHAUSTED" in sig_ids
        assert "SIG-SEC-LUKS-KEY-MISSING" in sig_ids
        assert "SIG-SEC-DECRYPTION-IOCTL" in sig_ids
        assert "SIG-STORAGE-PARTITION-OUTSIDE-DISK" in sig_ids
        assert "SIG-GUEST-FS-ERRORS" in sig_ids
        assert "SIG-BOOT-GRUB-LEGACY" in sig_ids
        assert "SIG-CONV-OOM-KILLER" in sig_ids
        assert "SIG-STORAGE-DELL-HBA" in sig_ids
        assert "SIG-OCV-FAILED-TO-ENSURE" in sig_ids

    def test_store_matches_naa_serial_failure(self):
        store = DynamicKnowledgeStore()
        raw = "could not extract serial from NAA, trying to find by listing volumes on controller"
        matches = store.match_failure(raw)
        assert len(matches) > 0
        top_sig, conf = matches[0]
        assert top_sig.signature_id == "SIG-STORAGE-NAA-OFFLOAD"
        assert conf >= 0.9

        best_sol = store.get_best_solution(top_sig)
        assert best_sol is not None
        assert "anytoany" in best_sol.action_summary.lower()
        assert best_sol.action_plan.steps[0].action_type == ActionType.PLAN_SPEC_PATCH

    def test_store_matches_symlink_corruption(self):
        store = DynamicKnowledgeStore()
        raw = "virt-v2v: error: inspection could not detect the source guest (or physical machine) operating system. check filesystem: 15+ matched known OS partition"
        matches = store.match_failure(raw)
        assert len(matches) > 0
        top_sig, _ = matches[0]
        assert top_sig.signature_id == "SIG-GUEST-INSPECT-SYMLINKS"
        sol = store.get_best_solution(top_sig)
        assert sol is not None
        assert "readlink" in sol.recommended_action
        assert sol.action_plan.steps[1].action_type == ActionType.SOURCE_VM_REPAIR

    def test_store_matches_vmware_vcpu_cores(self):
        store = DynamicKnowledgeStore()
        raw = 'virt-v2v: error: exception: libvirt: VIR_ERR_INTERNAL_ERROR: VIR_FROM_NONE: internal error: vcpu entry "cpuid-coresPersocket" smaller than "numCpus"'
        matches = store.match_failure(raw)
        assert len(matches) > 0
        top_sig, _ = matches[0]
        assert top_sig.signature_id == "SIG-VMWARE-CPUID-CORES"
        sol = store.get_best_solution(top_sig)
        assert "Cores per Socket" in sol.recommended_action

    def test_evidence_evaluation_hierarchy(self):
        store = DynamicKnowledgeStore()
        sig = store.get_signature("SIG-STORAGE-NAA-OFFLOAD")
        assert sig is not None

        # When required evidence is missing
        status_no_ev = store.evaluate_signature_evidence(sig, set())
        assert status_no_ev == EvidenceEvaluationStatus.INSUFFICIENT_EVIDENCE

        # When required evidence is present
        status_confirmed = store.evaluate_signature_evidence(sig, {"TRANSFER_FAILED"})
        assert status_confirmed == EvidenceEvaluationStatus.CONFIRMED

        # When contradicted
        sig.contraindicated_evidence = ["STORAGE_OFFLOAD_SUCCESSFUL"]
        status_rejected = store.evaluate_signature_evidence(sig, {"TRANSFER_FAILED", "STORAGE_OFFLOAD_SUCCESSFUL"})
        assert status_rejected == EvidenceEvaluationStatus.REJECTED


class TestLearningPipeline:
    def test_ingest_unknown_failure_creates_pending_candidate(self):
        store = DynamicKnowledgeStore(auto_seed=False)
        pipeline = LearningPipeline(store)

        raw = "Failed to mount nfs share at 10.0.0.50:/exports/vm due to permission denied"
        cand = pipeline.ingest_unknown_failure(
            raw_log=raw,
            suggested_mechanism="STORAGE.NFS.PERMISSION_DENIED",
            suggested_action="Verify NFS export permissions and exportfs -ra",
            suggested_action_type="CHECK_STORAGE",
            suggested_domain="storage",
        )

        assert cand.status == "PENDING_VALIDATION"
        assert cand.occurrence_count == 1
        assert cand.suggested_mechanism == "STORAGE.NFS.PERMISSION_DENIED"

        # Ingesting same failure increments occurrence
        cand2 = pipeline.ingest_unknown_failure(raw_log=raw)
        assert cand2.candidate_id == cand.candidate_id
        assert cand2.occurrence_count == 2

    def test_promotion_requires_sre_validation_and_updates_store(self):
        store = DynamicKnowledgeStore(auto_seed=False)
        pipeline = LearningPipeline(store)

        cand = pipeline.ingest_unknown_failure(
            raw_log="Unknown SCSI controller abort timeout",
            suggested_mechanism="STORAGE.SCSI.TIMEOUT",
            suggested_action="Increase timeout to 60s and retry",
            suggested_action_type="RETRY",
        )

        # Promotion fails without valid SRE credentials
        with pytest.raises(ValueError):
            pipeline.promote_candidate(cand.candidate_id, validated_by="", validation_reason="")

        # Promote with SRE credentials
        sig = pipeline.promote_candidate(
            cand.candidate_id,
            validated_by="sre_alice",
            validation_reason="Verified on 5 migrations that 60s timeout resolves controller latency",
        )

        assert sig.signature_id == cand.candidate_id
        assert store.get_signature(cand.candidate_id) is not None
        assert cand.status == "VALIDATED"
        assert cand.validated_by == "sre_alice"

    def test_record_action_outcome_updates_success_rates(self):
        store = DynamicKnowledgeStore()
        pipeline = LearningPipeline(store)

        sig = store.get_signature("SIG-STORAGE-NAA-OFFLOAD")
        sol = sig.solutions[0]
        initial_success = sol.success_count

        pipeline.record_action_outcome(sig.signature_id, sol.solution_id, success=True)
        assert sol.success_count == initial_success + 1


class TestEngineDynamicKnowledgeIntegration:
    def test_engine_classifies_and_recommends_via_dynamic_store(self):
        from engine.contracts import Evidence, EvidenceStatus
        registry = InMemoryCapabilityRegistry({})
        engine = MigrationFailureEngine(registry=registry)

        event = {
            "event_id": "EVT-TEST-NAA-01",
            "event_type": "MigrationFailed",
            "migration_id": "mig-test-01",
            "message": "could not extract serial from NAA, trying to find by listing volumes on storage array",
        }

        # 1. Unconfirmed evidence case (P0-3): pattern matched, but evidence is insufficient
        state = engine.run({"event": event})

        assert state.failure_signature == "SIG-STORAGE-NAA-OFFLOAD"
        assert state.classification == "STORAGE.MTV.OFFLOAD_SERIAL_UNRESOLVED"
        rec = state.recommendation
        assert rec["signature_id"] == "SIG-STORAGE-NAA-OFFLOAD"
        assert rec["signature_evidence_status"] == "INSUFFICIENT_EVIDENCE"
        # Must NOT expose executable action_plan or recommended_action
        assert "recommended_action" not in rec
        assert "action_plan" not in rec
        # Must expose candidate_solution for advisory
        assert "candidate_solution" in rec
        assert rec["candidate_solution"]["status"] == "CANDIDATE"
        assert "anytoany" in rec["candidate_solution"]["potential_action"].lower()
        # Known signature must NOT pollute the learning pipeline (Finding 8)
        assert "learning_candidate_id" not in rec

        # 2. Confirmed evidence case: live telemetry corroborates the required evidence
        state.evidence = [
            Evidence(id="ev-1", source="splunk", fact="TRANSFER_FAILED", status=EvidenceStatus.SUCCESS)
        ]
        state.diagnosis = {"status": "SUFFICIENT", "mechanism": "STORAGE.MTV.OFFLOAD_SERIAL_UNRESOLVED"}
        rec_confirmed = engine._recommendation(state)
        assert rec_confirmed["signature_evidence_status"] == "CONFIRMED"
        assert "action_plan" in rec_confirmed
        assert "recommended_action" in rec_confirmed
        assert rec_confirmed["action_plan"]["steps"][0]["action_type"] == "PLAN_SPEC_PATCH"
        assert "anytoany" in rec_confirmed["recommended_action"].lower()

    def test_engine_ingests_learning_candidate_on_unclassified_failure(self):
        registry = InMemoryCapabilityRegistry({})
        engine = MigrationFailureEngine(registry=registry)

        event = {
            "event_id": "EVT-UNKNOWN-ERR-99",
            "event_type": "MigrationFailed",
            "migration_id": "mig-test-99",
            "message": "Strange unclassified quantum storage bus error: 0xDEADBEEF in sector 4096",
        }

        state = engine.run({"event": event})
        assert state.classification == "UNKNOWN"
        rec = state.recommendation
        assert "learning_candidate_id" in rec

        candidate_id = rec["learning_candidate_id"]
        cand = engine.learning_pipeline.get_candidate(candidate_id)
        assert cand is not None
        assert cand.status == "PENDING_VALIDATION"

        # SRE can promote candidate directly through engine API
        promoted = engine.promote_learning_candidate(
            candidate_id=candidate_id,
            validated_by="sre_bob",
            validation_reason="Quantum bus requires bus rescan parameter",
        )
        assert promoted.signature_id == candidate_id
        assert engine.knowledge_store.get_signature(candidate_id) is not None

    def test_tracker_roundtrip_persistence_reloads_dynamic_signatures(self):
        from engine.integrations.local.tracker import FixtureSRETrackerAdapter

        tracker = FixtureSRETrackerAdapter()
        store1 = DynamicKnowledgeStore(tracker=tracker, auto_seed=False)
        pipeline1 = LearningPipeline(store1, tracker=tracker)

        # Ingest novel failure
        cand = pipeline1.ingest_unknown_failure(
            raw_log="virt-v2v: fatal kernel panic during pivot_root: custom hardware device 0x1234 reset",
            suggested_mechanism="STORAGE.DEVICE.RESET_PANIC",
            suggested_action="Detach custom hardware device before migration retry",
        )
        assert cand.status == "PENDING_VALIDATION"

        # SRE promotes candidate
        pipeline1.promote_candidate(
            candidate_id=cand.candidate_id,
            validated_by="sre_alice",
            validation_reason="Confirmed hardware device detach resolves the panic.",
        )

        # Verify candidate is saved in tracker
        known_issues = tracker.get_known_issues()
        assert len(known_issues) == 1
        assert known_issues[0]["signature_id"] == cand.candidate_id

        # Now simulate a brand new engine / store startup with the same tracker
        store2 = DynamicKnowledgeStore(tracker=tracker, auto_seed=False)
        # Store2 should have loaded the signature dynamically from tracker!
        loaded_sig = store2.get_signature(cand.candidate_id)
        assert loaded_sig is not None
        assert loaded_sig.mechanism == "STORAGE.DEVICE.RESET_PANIC"

        # And store2 should match future occurrences of that error!
        matches = store2.match_failure("virt-v2v: fatal kernel panic during pivot_root: custom hardware device 0x5678 reset")
        assert len(matches) > 0
        assert matches[0][0].signature_id == cand.candidate_id

    def test_p0_1_database_record_overrides_seeded_python_signature(self):
        # P0-1 & P0-5: Live database record must override seeded Python signature definition
        from engine.integrations.local.tracker import FixtureSRETrackerAdapter

        tracker = FixtureSRETrackerAdapter()
        # Save an override in DB tracker for seeded signature SIG-STORAGE-NAA-OFFLOAD
        tracker.learning_candidates.append({
            "signature_id": "SIG-STORAGE-NAA-OFFLOAD",
            "canonical_signature": "could not extract serial from NAA, trying to find by listing volumes",
            "domain": "storage_v2",
            "mechanism": "STORAGE.OFFLOAD.DEPRECATED_MECHANISM",
            "issue_summary": "Storage array offload deprecated for NAA vVol devices.",
            "status": "DEPRECATED",
            "occurrence_count": 99,
            "solutions": [
                {
                    "solution_id": "SOL-DB-OVERRIDE",
                    "title": "DB Updated Solution",
                    "action_summary": "Use direct host copy instead of NAA array offload",
                    "recommended_action": "Set storage_offload=false permanently",
                    "risk_level": "LOW",
                    "requires_approval": True,
                    "action_plan": {
                        "plan_id": "PLAN-DB-OVERRIDE",
                        "failure_signature": "SIG-STORAGE-NAA-OFFLOAD",
                        "steps": [
                            {
                                "action_id": "STEP-1",
                                "action_type": "PLAN_SPEC_PATCH",
                                "title": "Patch Spec",
                                "description": "Set storage_offload=false",
                                "risk_level": "LOW",
                                "requires_approval": True,
                            }
                        ],
                    },
                }
            ],
        })

        # Load store with auto_seed=True (seeded data loaded first, then DB tracker overrides)
        store = DynamicKnowledgeStore(tracker=tracker, auto_seed=True)
        sig = store.get_signature("SIG-STORAGE-NAA-OFFLOAD")
        assert sig is not None
        assert sig.status == "DEPRECATED"
        assert sig.mechanism == "STORAGE.OFFLOAD.DEPRECATED_MECHANISM"
        assert sig.domain == "storage_v2"
        assert sig.frequency == 99
        assert len(sig.solutions) == 1
        assert sig.solutions[0].solution_id == "SOL-DB-OVERRIDE"
        assert sig.solutions[0].action_plan.plan_id == "PLAN-DB-OVERRIDE"
        assert sig.solutions[0].action_plan.steps[0].action_type == ActionType.PLAN_SPEC_PATCH

    def test_p0_4_unvalidated_learning_candidate_persists_across_restarts(self):
        # P0-4: Unvalidated learning candidates must be durably stored and rehydrated across restarts
        from engine.integrations.local.tracker import FixtureSRETrackerAdapter

        tracker = FixtureSRETrackerAdapter()
        store1 = DynamicKnowledgeStore(tracker=tracker, auto_seed=False)
        pipeline1 = LearningPipeline(store1, tracker=tracker)

        # Ingest novel failure without promoting it
        cand = pipeline1.ingest_unknown_failure(
            raw_log="virt-v2v: fatal unknown firmware crash: EFI_UNSUPPORTED in subcarrier 0x99",
            suggested_mechanism="FIRMWARE.CRASH.UNSUPPORTED",
            suggested_action="Disable secureboot EFI shim",
        )
        assert cand.status == "PENDING_VALIDATION"
        candidate_id = cand.candidate_id

        # Candidate must be in tracker storage
        assert len(tracker.get_learning_candidates(status="PENDING_VALIDATION")) == 1

        # Simulate agent pod restart with a brand new pipeline instance
        store2 = DynamicKnowledgeStore(tracker=tracker, auto_seed=False)
        pipeline2 = LearningPipeline(store2, tracker=tracker)

        # Rehydrated pipeline must have the candidate!
        rehydrated = pipeline2.get_candidate(candidate_id)
        assert rehydrated is not None
        assert rehydrated.candidate_id == candidate_id
        assert rehydrated.status == "PENDING_VALIDATION"
        assert rehydrated.suggested_mechanism == "FIRMWARE.CRASH.UNSUPPORTED"

    def test_three_state_applicability_fail_closed(self):
        from engine.memory.action_ontology import ApplicabilityStatus

        sig = FailureSignature(
            signature_id="SIG-TEST-APPLICABILITY",
            canonical_pattern="test error",
            domain="storage",
            mechanism="STORAGE.TEST",
            description="Test signature with applicability rules",
            applicability_rules={"storage_backend": "dell", "mtv_version": ["2.12.5", "2.12.6"]},
        )

        # 1. Missing context entirely -> UNKNOWN
        assert sig.evaluate_applicability(None) == ApplicabilityStatus.UNKNOWN
        assert sig.evaluate_applicability({}) == ApplicabilityStatus.UNKNOWN

        # 2. Context has partial key but missing another required key -> UNKNOWN
        assert sig.evaluate_applicability({"storage_backend": "dell"}) == ApplicabilityStatus.UNKNOWN

        # 3. Explicit mismatch -> INAPPLICABLE
        assert sig.evaluate_applicability({"storage_backend": "pure", "mtv_version": "2.12.5"}) == ApplicabilityStatus.INAPPLICABLE
        assert sig.evaluate_applicability({"storage_backend": "dell", "mtv_version": "2.10.0"}) == ApplicabilityStatus.INAPPLICABLE

        # 4. Exact match -> APPLICABLE
        assert sig.evaluate_applicability({"storage_backend": "dell", "mtv_version": "2.12.5"}) == ApplicabilityStatus.APPLICABLE
        assert sig.evaluate_applicability({"storage_backend": "dell", "mtv_version": "2.12.6"}) == ApplicabilityStatus.APPLICABLE

    def test_engine_blocks_action_recommendation_when_applicability_is_unknown(self):
        from engine.contracts import Evidence, EvidenceStatus
        from engine.memory.action_ontology import ApplicabilityStatus
        from engine.integrations.registry import InMemoryCapabilityRegistry

        registry = InMemoryCapabilityRegistry({})
        engine = MigrationFailureEngine(registry=registry)

        # Register a signature with strict applicability constraints
        sig = FailureSignature(
            signature_id="SIG-DELL-SPECIFIC",
            canonical_pattern="hba timeout on bus 4",
            domain="storage",
            mechanism="STORAGE.DELL.HBA_TIMEOUT",
            description="Dell HBA bus timeout",
            applicability_rules={"storage_backend": "dell"},
            required_evidence=["TRANSFER_FAILED"],
            solutions=[
                KnownSolution(
                    solution_id="SOL-DELL-RESCAN",
                    signature_id="SIG-DELL-SPECIFIC",
                    title="Rescan Dell HBA",
                    action_summary="Rescan SCSI bus",
                    recommended_action="Run rescan script",
                    action_plan=ActionPlan(
                        plan_id="PLAN-DELL-RESCAN",
                        failure_signature="SIG-DELL-SPECIFIC",
                        steps=[
                            ActionDefinition("S1", ActionType.SOURCE_VM_REPAIR, "Rescan", "Rescan HBA")
                        ],
                    ),
                    applicability_constraints={"storage_backend": "dell"},
                )
            ],
        )
        engine.knowledge_store.register_signature(sig)

        # Event occurs, but context does NOT specify storage_backend
        event = {
            "event_id": "EVT-DELL-01",
            "event_type": "MigrationFailed",
            "migration_id": "mig-dell-01",
            "message": "hba timeout on bus 4 while writing sector 100",
        }
        state = engine.run({"event": event})
        assert state.failure_signature == "SIG-DELL-SPECIFIC"

        # Telemetry provides CONFIRMED evidence
        state.evidence = [
            Evidence(id="ev-1", source="splunk", fact="TRANSFER_FAILED", status=EvidenceStatus.SUCCESS)
        ]
        state.diagnosis = {"status": "SUFFICIENT", "mechanism": "STORAGE.DELL.HBA_TIMEOUT"}

        # Context is missing storage_backend -> Applicability is UNKNOWN
        state.context = {}
        rec = engine._recommendation(state)

        # Trust Boundary Guard: Must NOT expose executable action_plan or recommended_action
        assert "action_plan" not in rec
        assert "recommended_action" not in rec
        assert rec["candidate_solution"]["status"] == "CANDIDATE"
        assert rec["signature_applicability"] == "UNKNOWN"
        assert "applicability is UNKNOWN" in rec["candidate_solution"]["reason"]

        # Now supply confirmed matching context -> Must promote to executable action_plan
        state.context = {"storage_backend": "dell"}
        rec_confirmed = engine._recommendation(state)
        assert rec_confirmed["signature_applicability"] == "APPLICABLE"
        assert rec_confirmed["solution_applicability"] == "APPLICABLE"
        assert "action_plan" in rec_confirmed
        assert "recommended_action" in rec_confirmed

    def test_full_action_plan_jsonb_roundtrip_persistence(self):
        from engine.integrations.local.tracker import FixtureSRETrackerAdapter

        tracker = FixtureSRETrackerAdapter()
        plan = ActionPlan(
            plan_id="PLAN-RICH-01",
            failure_signature="SIG-RICH-01",
            steps=[
                ActionDefinition(
                    action_id="STEP-1",
                    action_type=ActionType.PLAN_SPEC_PATCH,
                    title="Patch Spec",
                    description="Set storage offload to false",
                    parameters={"storage_offload": False, "timeout": 300},
                    preconditions=["STORAGE_HEALTHY"],
                    verification_plan="Verify MTV disk transfer progress",
                    rollback_plan="Re-enable offload",
                    risk_level=RiskLevel.LOW,
                    requires_approval=True,
                    approval_role="SRE",
                ),
                ActionDefinition(
                    action_id="STEP-2",
                    action_type=ActionType.RETRY,
                    title="Retry Migration",
                    description="Retry MTV migration plan",
                    verification_plan="Verify completion",
                    risk_level=RiskLevel.LOW,
                    requires_approval=True,
                    approval_role="SRE",
                ),
            ],
            rationale="Disabling array copy-offload forces host-based direct transfer",
            overall_risk=RiskLevel.LOW,
            requires_human_approval=True,
        )

        sol = KnownSolution(
            solution_id="SOL-RICH-01",
            signature_id="SIG-RICH-01",
            title="Disable Offload Workaround",
            action_summary="Patch spec to disable offload and retry",
            recommended_action="Set storage_offload=false in plan",
            action_plan=plan,
            applicability_constraints={"storage_backend": "dell", "mtv_version": "2.12.6"},
            verification_contract={"metric": "mtv_transfer_bytes", "min_rate_mb": 10},
            provenance={"author": "SRE SRE-Team", "ticket": "INC-9942"},
        )

        sig = FailureSignature(
            signature_id="SIG-RICH-01",
            canonical_pattern="rich test failure pattern",
            domain="storage",
            mechanism="STORAGE.RICH_TEST",
            description="Rich test signature",
            applicability_rules={"storage_backend": "dell"},
            required_evidence=["TRANSFER_FAILED"],
            contraindicated_evidence=["TRANSFER_COMPLETE"],
            solutions=[sol],
        )

        # Save to tracker
        tracker.save_known_issue(sig.to_dict())

        # Load into brand new DynamicKnowledgeStore with auto_seed=False
        store = DynamicKnowledgeStore(tracker=tracker, auto_seed=False)
        loaded_sig = store.get_signature("SIG-RICH-01")
        assert loaded_sig is not None
        assert loaded_sig.applicability_rules == {"storage_backend": "dell"}
        assert loaded_sig.required_evidence == ["TRANSFER_FAILED"]
        assert loaded_sig.contraindicated_evidence == ["TRANSFER_COMPLETE"]

        assert len(loaded_sig.solutions) == 1
        loaded_sol = loaded_sig.solutions[0]
        assert loaded_sol.solution_id == "SOL-RICH-01"
        assert loaded_sol.applicability_constraints == {"storage_backend": "dell", "mtv_version": "2.12.6"}
        assert loaded_sol.verification_contract == {"metric": "mtv_transfer_bytes", "min_rate_mb": 10}
        assert loaded_sol.provenance == {"author": "SRE SRE-Team", "ticket": "INC-9942"}

        # Verify full ActionPlan structure survived
        loaded_plan = loaded_sol.action_plan
        assert loaded_plan.plan_id == "PLAN-RICH-01"
        assert len(loaded_plan.steps) == 2
        assert loaded_plan.steps[0].action_id == "STEP-1"
        assert loaded_plan.steps[0].action_type == ActionType.PLAN_SPEC_PATCH
        assert loaded_plan.steps[0].parameters == {"storage_offload": False, "timeout": 300}
        assert loaded_plan.steps[0].preconditions == ["STORAGE_HEALTHY"]
        assert loaded_plan.steps[0].verification_plan == "Verify MTV disk transfer progress"
        assert loaded_plan.steps[0].rollback_plan == "Re-enable offload"
        assert loaded_plan.steps[1].action_id == "STEP-2"
        assert loaded_plan.steps[1].action_type == ActionType.RETRY

    def test_db_first_authority_discipline(self):
        from engine.integrations.local.tracker import FixtureSRETrackerAdapter

        tracker = FixtureSRETrackerAdapter()
        # Tracker has 1 custom issue
        tracker.save_known_issue({
            "signature_id": "SIG-CUSTOM-ONLY",
            "canonical_pattern": "custom isolated error",
            "domain": "custom",
            "mechanism": "CUSTOM.ERROR",
            "description": "Custom DB issue",
            "status": "VALIDATED",
        })

        # When tracker is provided, auto_seed defaults to False
        store = DynamicKnowledgeStore(tracker=tracker)
        assert store.get_signature("SIG-CUSTOM-ONLY") is not None
        # Seeded signatures must NOT be loaded when tracker is present (no competition)
        assert store.get_signature("SIG-STORAGE-NAA-OFFLOAD") is None
        assert len(store.all_signatures()) == 1


