import json
from engine.llm.advisory import parse_advisory
from engine.integrations.registry import InMemoryCapabilityRegistry


def test_advisory_validates_registered_conforming_suggestion():
    registry = InMemoryCapabilityRegistry({
        'observability.search': lambda p: {'evidence': []},
    })
    payload = {
        "summary": "Check storage controller",
        "suggested_investigations": [
            {
                "action": "COLLECT_TARGETED_EVIDENCE",
                "purpose": "Verify CSI controller errors",
                "capability": "observability.search",
                "parameters": {"domain": "ocv", "signal": "csi_controller_errors"},
            }
        ],
        "uncertainty": "Low",
    }
    result = parse_advisory(json.dumps(payload), registry=registry)
    assert result["status"] == "ADVISORY"
    assert len(result["suggested_investigations"]) == 1
    sug = result["suggested_investigations"][0]
    assert sug["validated"] is True
    assert sug["validation_error"] is None


def test_advisory_flags_unregistered_capability():
    registry = InMemoryCapabilityRegistry({
        'observability.search': lambda p: {'evidence': []},
    })
    payload = {
        "summary": "Attempt arbitrary bash execution",
        "suggested_investigations": [
            {
                "action": "EXECUTE_COMMAND",
                "purpose": "Run arbitrary tool",
                "capability": "unapproved.arbitrary_executor",
                "parameters": {"cmd": "kubectl delete pod"},
            }
        ],
        "uncertainty": "High",
    }
    result = parse_advisory(json.dumps(payload), registry=registry)
    assert result["status"] == "ADVISORY"
    assert len(result["suggested_investigations"]) == 1
    sug = result["suggested_investigations"][0]
    assert sug["validated"] is False
    assert "not registered" in sug["validation_error"].lower()


def test_advisory_flags_schema_violation():
    registry = InMemoryCapabilityRegistry({
        'observability.search': lambda p: {'evidence': []},
    })
    payload = {
        "summary": "Query invalid domain",
        "suggested_investigations": [
            {
                "action": "COLLECT_TARGETED_EVIDENCE",
                "purpose": "Query unsupported platform",
                "capability": "observability.search",
                "parameters": {"domain": "unsupported_cloud_provider", "signal": "random_signal"},
            }
        ],
        "uncertainty": "High",
    }
    result = parse_advisory(json.dumps(payload), registry=registry)
    assert result["status"] == "ADVISORY"
    sug = result["suggested_investigations"][0]
    assert sug["validated"] is False
    assert "schema violation" in sug["validation_error"].lower()


def test_advisory_parses_parametric_hypothesis_and_sre_diagnostics():
    registry = InMemoryCapabilityRegistry({
        'observability.search': lambda p: {'evidence': []},
    })
    payload = {
        "summary": "Target storage fabric dropped during disk stream",
        "parametric_hypothesis": "NVMe-oF controller reset due to SAN buffer pool timeout",
        "suggested_investigations": [
            {
                "action": "INSPECT_CONVERSION_LOGS",
                "purpose": "Check virt-v2v driver output for abort",
                "capability": "observability.search",
                "parameters": {"domain": "conversion", "signal": "virt_v2v_log"},
            }
        ],
        "suggested_sre_diagnostics": [
            "oc logs -n openshift-mtv -l app=forklift-controller",
            "dmesg | grep -i nvme",
        ],
        "uncertainty": "Awaiting storage switch error counter telemetry",
    }
    result = parse_advisory(json.dumps(payload), registry=registry)
    assert result["status"] == "ADVISORY"
    assert result["parametric_hypothesis"] == "NVMe-oF controller reset due to SAN buffer pool timeout"
    assert len(result["suggested_sre_diagnostics"]) == 2
    assert any("dmesg | grep -i nvme" in c for c in result["suggested_sre_diagnostics"])
    assert result["suggested_investigations"][0]["validated"] is True


def test_unknown_executes_exploratory_investigation_and_corroborates_evidence():
    from engine.workflow.engine import run_agent

    class ExploratoryLLM:
        model = "fake-exploratory-v1"

        def generate(self, messages, **kwargs):
            return json.dumps({
                "summary": "Novel NVMe fabric reset encountered",
                "parametric_hypothesis": "NVMe-oF target session reset during disk streaming",
                "suggested_investigations": [
                    {
                        "action": "INSPECT_VIRT_V2V_LOG",
                        "purpose": "Corroborate whether virt-v2v logged kernel abort",
                        "capability": "observability.search",
                        "parameters": {"domain": "conversion", "signal": "virt_v2v_log"},
                    }
                ],
                "suggested_sre_diagnostics": [
                    "oc logs -n openshift-mtv -l app=forklift-controller",
                    "multipath -ll",
                ],
                "uncertainty": "Needs conversion pod kernel log confirmation",
            })

    def mock_splunk(params):
        if params.get("domain") == "conversion" and params.get("signal") == "virt_v2v_log":
            return {
                "status": "SUCCESS",
                "evidence": [
                    {
                        "id": "ev-exploratory-001",
                        "domain": "conversion",
                        "signal": "virt_v2v_log",
                        "fact": "NVME_FABRIC_ABORT_OBSERVED",
                        "status": "SUCCESS",
                        "source": "splunk",
                    }
                ],
            }
        return {"evidence": []}

    registry = InMemoryCapabilityRegistry({
        "observability.search": mock_splunk,
    })

    req = {
        "event_id": "evt-unknown-exploratory-01",
        "event_type": "MigrationFailed",
        "failure_case_id": "case-unknown-exploratory-01",
        "migration_id": "mig-prd-nvme-7741",
        "vm_id": "vm-custom-edge-09",
        "cluster_id": "ocv-prod-c",
        "phase": "CONVERSION",
        "severity": "CRITICAL",
        "message": "virt-v2v-in-place: unexpected error: NVMe-oF multipath controller id 0x8a1f returned unrecognized fabric state ABORT_FABRIC_RESET",
        "memory_mode": "none",
    }

    state = run_agent(req, registry=registry, tracker=None, knowledge=None, memory_mode="none", llm_provider=ExploratoryLLM())
    out = state.to_dict()

    assert out["classification"] == "UNKNOWN"
    assert out["diagnosis"]["status"] == "INSUFFICIENT_EVIDENCE"
    adv = out["investigation_package"]["llm_advisory"]
    assert adv["corroboration_status"] == "CORROBORATED"
    assert len(adv["executed_investigations"]) == 1
    assert len(adv["evidence_gathered"]) == 1
    assert adv["parametric_hypothesis"] == "NVMe-oF target session reset during disk streaming"
    assert any("multipath -ll" in c for c in adv["suggested_sre_diagnostics"])
    # Verify mutation is safely blocked
    assert out["decision_readiness"]["RETRY"]["status"] in ("NOT_READY", "UNKNOWN")
    assert "Do not execute remediation or retry from this agent." in out["investigation_package"]["do_not_do"]

