from pathlib import Path
from engine.rules.policy_validator import validate_all_policies, validate_policy_dict


def test_all_repository_policies_pass_validation():
    policy_dir = Path(__file__).parents[2] / "policies"
    skill_root = Path(__file__).parents[2] / "skills"
    errors = validate_all_policies(policy_dir, skill_root=skill_root)
    assert not errors, f"Policy validation failed: {errors}"


def test_policy_validator_catches_invalid_fact():
    bad_policy = {
        "id": "test.policy",
        "version": "1.0",
        "skill": "storage/csi-provisioning-timeout",
        "required_evidence": [
            {"capability": "observability.search", "parameters": {"domain": "storage", "signal": "backend_health"}}
        ],
        "hypotheses": [
            {
                "id": "H-TEST",
                "mechanism": "TEST_MECH",
                "description": "Test",
                "score": 0.9,
                "supporting": {"any": ["NON_EXISTENT_FACT_TYPO"]},
            }
        ],
        "decision_readiness": {"retry": {"required_facts": []}},
    }
    errors = validate_policy_dict(bad_policy)
    assert any("NON_EXISTENT_FACT_TYPO" in e for e in errors)


def test_policy_validator_catches_invalid_capability_contract():
    bad_policy = {
        "id": "test.policy.contract",
        "version": "1.0",
        "skill": "storage/csi-provisioning-timeout",
        "required_evidence": [
            # Missing domain and signal
            {"capability": "observability.search", "parameters": {}}
        ],
        "decision_readiness": {},
    }
    errors = validate_policy_dict(bad_policy)
    assert any("Missing required parameter" in e for e in errors)
