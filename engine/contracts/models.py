from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional
from enum import Enum

class EvidenceStatus(str, Enum):
    # Capability result taxonomy. VERIFIED is retained as a compatibility alias for SUCCESS.
    SUCCESS='SUCCESS'; VERIFIED='SUCCESS'; NO_DATA='NO_DATA'; UNKNOWN='UNKNOWN'; UNAVAILABLE='UNAVAILABLE'; ERROR='ERROR'; STALE='STALE'; CONTRADICTED='CONTRADICTED'
class Readiness(str, Enum):
    UNKNOWN='UNKNOWN'; CANDIDATE='CANDIDATE'; READY='READY'; NOT_READY='NOT_READY'; EXECUTING='EXECUTING'; SUCCEEDED='SUCCEEDED'; FAILED='FAILED'

@dataclass
class Evidence:
    id: str
    source: str
    fact: str
    status: EvidenceStatus = EvidenceStatus.SUCCESS
    observed_at: Optional[str] = None
    retrieved_at: Optional[str] = None
    confidence: float = 1.0
    metadata: Dict[str, Any] = field(default_factory=dict)
    capability_id: Optional[str] = None
    domain: Optional[str] = None
    signal: Optional[str] = None
    provenance: Dict[str, Any] = field(default_factory=dict)

@dataclass
class Hypothesis:
    id: str
    code: str
    description: str
    score: float = 0.0
    status: str = 'UNTESTED'
    supporting: List[str] = field(default_factory=list)
    contradicting: List[str] = field(default_factory=list)

@dataclass
class RecoveryOption:
    action: str
    readiness: Readiness = Readiness.UNKNOWN
    blockers: List[str] = field(default_factory=list)
    rationale: str = ''
    risk: str = 'UNKNOWN'
    approval_required: bool = True

@dataclass
class AgentState:
    failure_case_id: str
    event: Dict[str, Any]
    objective: str = 'Diagnose migration failure and determine the safest next SRE step.'
    success_conditions: List[str] = field(default_factory=list)
    constraints: List[str] = field(default_factory=lambda: ['READ_ONLY', 'NO_DIRECT_CLUSTER_REMEDIATION', 'FAIL_CLOSED'])
    context: Dict[str, Any] = field(default_factory=dict)
    classification: Optional[str] = None
    classification_confidence: float = 0.0
    hypotheses: List[Hypothesis] = field(default_factory=list)
    evidence: List[Evidence] = field(default_factory=list)
    capability_results: List[Dict[str, Any]] = field(default_factory=list)
    evidence_evaluation: Dict[str, Any] = field(default_factory=dict)
    evidence_plan: List[Dict[str, Any]] = field(default_factory=list)
    next_evidence_requests: List[Dict[str, Any]] = field(default_factory=list)
    investigation_history: List[Dict[str, Any]] = field(default_factory=list)
    evidence_round: int = 0
    attempt_count: int = 0
    attempted_capabilities: List[str] = field(default_factory=list)
    capability_errors: List[str] = field(default_factory=list)
    diagnosis: Dict[str, Any] = field(default_factory=dict)
    recovery: List[RecoveryOption] = field(default_factory=list)
    # v2.8.3 structured operational decision contract. Legacy recovery remains for compatibility.
    decision_readiness: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    investigation_package: Dict[str, Any] = field(default_factory=dict)
    next_step: Optional[str] = None
    mechanism: Optional[str] = None
    status: str = 'STARTED'
    iteration: int = 0
    trace: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    skill_id: Optional[str] = None
    skill: Optional[str] = None
    historical_context: List[Dict[str, Any]] = field(default_factory=list)
    periodic_context: List[Dict[str, Any]] = field(default_factory=list)
    knowledge_context: List[Dict[str, Any]] = field(default_factory=list)
    diagnosis_basis: Dict[str, Any] = field(default_factory=lambda: {'supporting_evidence': [], 'contradicting_evidence': [], 'excluded_mechanisms': [], 'memory_context': [], 'priority_order': ['CURRENT_EVIDENCE', 'SRE_HISTORY', 'RHOKP_KNOWLEDGE', 'LLM_ADVISORY']})
    memory_context: Dict[str, Any] = field(default_factory=dict)
    recommendation: Dict[str, Any] = field(default_factory=dict)
    policy_version: Optional[str] = None
    agent_version: str = '2.8.3'

    def to_dict(self): return asdict(self)
