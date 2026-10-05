from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional
from engine.environment import EnvironmentFingerprint
from enum import Enum

class EvidenceStatus(str, Enum):
    # Capability result taxonomy. VERIFIED is retained as a compatibility alias for SUCCESS.
    SUCCESS='SUCCESS'; VERIFIED='SUCCESS'; NO_DATA='NO_DATA'; UNKNOWN='UNKNOWN'; UNAVAILABLE='UNAVAILABLE'; ERROR='ERROR'; STALE='STALE'; CONTRADICTED='CONTRADICTED'
class Readiness(str, Enum):
    UNKNOWN='UNKNOWN'; CANDIDATE='CANDIDATE'; READY='READY'; NOT_READY='NOT_READY'; EXECUTING='EXECUTING'; SUCCEEDED='SUCCEEDED'; FAILED='FAILED'

def _parse_evidence_status(val: Any) -> EvidenceStatus:
    if isinstance(val, EvidenceStatus):
        return val
    if not val:
        return EvidenceStatus.SUCCESS
    s = str(val).strip().upper()
    if s == 'VERIFIED':
        return EvidenceStatus.SUCCESS
    try:
        return EvidenceStatus(s)
    except ValueError:
        return EvidenceStatus.UNKNOWN

def _parse_readiness(val: Any) -> Readiness:
    if isinstance(val, Readiness):
        return val
    if not val:
        return Readiness.UNKNOWN
    s = str(val).strip().upper()
    try:
        return Readiness(s)
    except ValueError:
        return Readiness.UNKNOWN

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
    environment: Optional[EnvironmentFingerprint] = None
    compatibility_context: Dict[str, Any] = field(default_factory=dict)
    knowledge_candidates: List[Dict[str, Any]] = field(default_factory=list)
    applicable_knowledge: List[Dict[str, Any]] = field(default_factory=list)
    version_conflicts: List[Dict[str, Any]] = field(default_factory=list)
    llm_advisory: Dict[str, Any] = field(default_factory=lambda: {'status': 'NOT_REQUESTED'})
    knowledge_judge: Dict[str, Any] = field(default_factory=lambda: {'status': 'NOT_REQUESTED'})
    recommendation: Dict[str, Any] = field(default_factory=dict)
    # v2.11 operational learning / recurrence state. Historical observations are
    # context only; validated solutions require verified outcomes and human validation.
    failure_signature: Optional[str] = None
    evidence_signature: Optional[str] = None
    recurrence: Dict[str, Any] = field(default_factory=dict)
    learning: Dict[str, Any] = field(default_factory=lambda: {'status': 'NOT_EVALUATED'})
    capability_coverage: Dict[str, Any] = field(default_factory=dict)
    missing_diagnostic_capabilities: List[str] = field(default_factory=list)
    policy_version: Optional[str] = None
    agent_version: str = '2.12.3'

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, data):
        """Rehydrate graph/checkpoint state without retaining live domain objects."""
        values = dict(data or {})
        values['evidence'] = [
            e if isinstance(e, Evidence) else Evidence(
                id=e.get('id',''), source=e.get('source',''), fact=e.get('fact',''),
                status=_parse_evidence_status(e.get('status')),
                observed_at=e.get('observed_at'), retrieved_at=e.get('retrieved_at'),
                confidence=e.get('confidence',1.0), metadata=e.get('metadata',{}),
                capability_id=e.get('capability_id'), domain=e.get('domain'),
                signal=e.get('signal'), provenance=e.get('provenance',{}),
            ) for e in values.get('evidence', [])
        ]
        values['hypotheses'] = [
            h if isinstance(h, Hypothesis) else Hypothesis(
                id=h.get('id',''), code=h.get('code',''), description=h.get('description',''),
                score=h.get('score',0.0), status=h.get('status','UNTESTED'),
                supporting=h.get('supporting',[]), contradicting=h.get('contradicting',[]),
            ) for h in values.get('hypotheses', [])
        ]
        if isinstance(values.get('environment'), dict):
            values['environment'] = EnvironmentFingerprint(**{k: values['environment'].get(k) for k in EnvironmentFingerprint.__dataclass_fields__})
        values['recovery'] = [
            r if isinstance(r, RecoveryOption) else RecoveryOption(
                action=r.get('action',''), readiness=_parse_readiness(r.get('readiness')),
                blockers=r.get('blockers',[]), rationale=r.get('rationale',''),
                risk=r.get('risk','UNKNOWN'), approval_required=r.get('approval_required',True),
            ) for r in values.get('recovery', [])
        ]
        return cls(**values)
