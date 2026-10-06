"""Hierarchical Action Ontology and Deterministic Action Validator.

Establishes the canonical hierarchy of operational actions:
- OBSERVE: Read-only telemetry and state inspection (COLLECT_LOGS, COLLECT_STATE, VALIDATE).
- INVESTIGATE: Deep diagnostic probes across subsystems (CHECK_STORAGE, CHECK_NETWORK, CHECK_GUEST, CHECK_VCENTER).
- CONFIGURE: Specification patches and provider configurations (PLAN_SPEC_PATCH, VMWARE_CONFIG, PLATFORM_CONFIG).
- REPAIR: In-guest and infrastructure remediation routines (SOURCE_VM_REPAIR, SOURCE_VM_SCRIPT, STORAGE_REPAIR).
- CONTROL: Lifecycle orchestration (RETRY, ROLLBACK, CANCEL).
- ESCALATE: Organizational handoffs (STORAGE_TEAM, MIGRATION_TEAM, RED_HAT, COMMAND_CENTER).

Enforces fail-closed validation: no action may be presented or recommended without
explicit ontology registration, preconditions, approval requirements, and safety gating.
Diagnostic V1 remains strictly read-only: direct execution of mutations is forbidden.
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple


class ActionCategory(str, Enum):
    OBSERVE = "OBSERVE"
    INVESTIGATE = "INVESTIGATE"
    CONFIGURE = "CONFIGURE"
    REPAIR = "REPAIR"
    CONTROL = "CONTROL"
    ESCALATE = "ESCALATE"


class ActionType(str, Enum):
    # OBSERVE
    COLLECT_LOGS = "COLLECT_LOGS"
    COLLECT_STATE = "COLLECT_STATE"
    VALIDATE = "VALIDATE"

    # INVESTIGATE
    CHECK_STORAGE = "CHECK_STORAGE"
    CHECK_NETWORK = "CHECK_NETWORK"
    CHECK_GUEST = "CHECK_GUEST"
    CHECK_VCENTER = "CHECK_VCENTER"
    CHECK_PLATFORM = "CHECK_PLATFORM"

    # CONFIGURE
    PLAN_SPEC_PATCH = "PLAN_SPEC_PATCH"
    VMWARE_CONFIG = "VMWARE_CONFIG"
    PLATFORM_CONFIG = "PLATFORM_CONFIG"
    STORAGE_CLASS_CONFIG = "STORAGE_CLASS_CONFIG"
    MIGRATION_PARAMS = "MIGRATION_PARAMS"

    # REPAIR
    SOURCE_VM_REPAIR = "SOURCE_VM_REPAIR"
    SOURCE_VM_SCRIPT = "SOURCE_VM_SCRIPT"
    STORAGE_REPAIR = "STORAGE_REPAIR"
    NETWORK_REPAIR = "NETWORK_REPAIR"
    BOOTLOADER_REPAIR = "BOOTLOADER_REPAIR"
    FILESYSTEM_FSCK = "FILESYSTEM_FSCK"

    # CONTROL
    RETRY = "RETRY"
    ROLLBACK = "ROLLBACK"
    CANCEL = "CANCEL"

    # ESCALATE
    STORAGE_TEAM = "STORAGE_TEAM"
    MIGRATION_TEAM = "MIGRATION_TEAM"
    RED_HAT = "RED_HAT"
    COMMAND_CENTER = "COMMAND_CENTER"
    VM_OWNER = "VM_OWNER"


# Mapping from ActionType to its Category
ACTION_TO_CATEGORY: Dict[ActionType, ActionCategory] = {
    ActionType.COLLECT_LOGS: ActionCategory.OBSERVE,
    ActionType.COLLECT_STATE: ActionCategory.OBSERVE,
    ActionType.VALIDATE: ActionCategory.OBSERVE,
    ActionType.CHECK_STORAGE: ActionCategory.INVESTIGATE,
    ActionType.CHECK_NETWORK: ActionCategory.INVESTIGATE,
    ActionType.CHECK_GUEST: ActionCategory.INVESTIGATE,
    ActionType.CHECK_VCENTER: ActionCategory.INVESTIGATE,
    ActionType.CHECK_PLATFORM: ActionCategory.INVESTIGATE,
    ActionType.PLAN_SPEC_PATCH: ActionCategory.CONFIGURE,
    ActionType.VMWARE_CONFIG: ActionCategory.CONFIGURE,
    ActionType.PLATFORM_CONFIG: ActionCategory.CONFIGURE,
    ActionType.STORAGE_CLASS_CONFIG: ActionCategory.CONFIGURE,
    ActionType.MIGRATION_PARAMS: ActionCategory.CONFIGURE,
    ActionType.SOURCE_VM_REPAIR: ActionCategory.REPAIR,
    ActionType.SOURCE_VM_SCRIPT: ActionCategory.REPAIR,
    ActionType.STORAGE_REPAIR: ActionCategory.REPAIR,
    ActionType.NETWORK_REPAIR: ActionCategory.REPAIR,
    ActionType.BOOTLOADER_REPAIR: ActionCategory.REPAIR,
    ActionType.FILESYSTEM_FSCK: ActionCategory.REPAIR,
    ActionType.RETRY: ActionCategory.CONTROL,
    ActionType.ROLLBACK: ActionCategory.CONTROL,
    ActionType.CANCEL: ActionCategory.CONTROL,
    ActionType.STORAGE_TEAM: ActionCategory.ESCALATE,
    ActionType.MIGRATION_TEAM: ActionCategory.ESCALATE,
    ActionType.RED_HAT: ActionCategory.ESCALATE,
    ActionType.COMMAND_CENTER: ActionCategory.ESCALATE,
    ActionType.VM_OWNER: ActionCategory.ESCALATE,
}


class RiskLevel(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


@dataclass
class ActionDefinition:
    action_id: str
    action_type: ActionType
    title: str
    description: str
    category: ActionCategory = field(init=False)
    command_template: Optional[str] = None
    parameters: Dict[str, Any] = field(default_factory=dict)
    preconditions: List[str] = field(default_factory=list)
    postconditions: List[str] = field(default_factory=list)
    risk_level: RiskLevel = RiskLevel.LOW
    requires_approval: bool = False
    approval_role: Optional[str] = None
    verification_plan: Optional[str] = None
    rollback_plan: Optional[str] = None
    automation_target: str = "MANUAL"  # MANUAL, AAP, EDA, READ_ONLY

    def __post_init__(self):
        if isinstance(self.action_type, str):
            self.action_type = ActionType(self.action_type)
        if isinstance(self.risk_level, str):
            self.risk_level = RiskLevel(self.risk_level)
        self.category = ACTION_TO_CATEGORY.get(self.action_type, ActionCategory.OBSERVE)
        # Any REPAIR, CONFIGURE, or HIGH/CRITICAL action strictly requires human/SRE approval
        if self.category in {ActionCategory.REPAIR, ActionCategory.CONFIGURE} or self.risk_level in {RiskLevel.HIGH, RiskLevel.CRITICAL}:
            self.requires_approval = True
            if not self.approval_role:
                self.approval_role = "SRE"

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["action_type"] = self.action_type.value
        d["category"] = self.category.value
        d["risk_level"] = self.risk_level.value
        return d


@dataclass
class ActionPlan:
    plan_id: str
    failure_signature: str
    steps: List[ActionDefinition] = field(default_factory=list)
    rationale: str = ""
    confidence: float = 1.0
    overall_risk: RiskLevel = RiskLevel.LOW
    requires_human_approval: bool = False

    def __post_init__(self):
        if isinstance(self.overall_risk, str):
            self.overall_risk = RiskLevel(self.overall_risk)
        # Determine aggregate approval and risk
        for step in self.steps:
            if step.requires_approval:
                self.requires_human_approval = True
            if step.risk_level in {RiskLevel.HIGH, RiskLevel.CRITICAL}:
                self.overall_risk = step.risk_level

    def to_dict(self) -> Dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "failure_signature": self.failure_signature,
            "steps": [s.to_dict() for s in self.steps],
            "rationale": self.rationale,
            "confidence": self.confidence,
            "overall_risk": self.overall_risk.value,
            "requires_human_approval": self.requires_human_approval,
        }


@dataclass
class ActionValidationResult:
    valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    action_id: Optional[str] = None


class ActionValidator:
    """Deterministic validator for operational action definitions and plans."""

    @classmethod
    def validate_action(cls, action: ActionDefinition, facts: Optional[Set[str]] = None) -> ActionValidationResult:
        errors: List[str] = []
        warnings: List[str] = []

        if not action.action_id:
            errors.append("action_id cannot be empty")
        if not action.title:
            errors.append("title cannot be empty")
        if not action.description:
            errors.append("description cannot be empty")

        # Category and type consistency
        expected_cat = ACTION_TO_CATEGORY.get(action.action_type)
        if expected_cat is None:
            errors.append(f"Unregistered action_type: {action.action_type}")
        elif action.category != expected_cat:
            errors.append(f"Category mismatch: {action.category} vs expected {expected_cat}")

        # Safety rule: Repair and Configure must require approval
        if action.category in {ActionCategory.REPAIR, ActionCategory.CONFIGURE}:
            if not action.requires_approval:
                errors.append(f"Action in {action.category} must require approval")
            if not action.approval_role:
                errors.append(f"Action in {action.category} must specify approval_role")
            if not action.verification_plan:
                warnings.append(f"Action {action.action_id} lacks explicit verification_plan")

        # Command template safety: non-read-only commands must require approval
        if action.command_template and not action.requires_approval and action.category not in {ActionCategory.OBSERVE, ActionCategory.INVESTIGATE}:
            errors.append("Actions with executable command_template must require approval")

        # Precondition check if current facts are provided
        if facts is not None and action.preconditions:
            missing_preconditions = [p for p in action.preconditions if p not in facts]
            if missing_preconditions:
                warnings.append(f"Preconditions not yet satisfied in current evidence: {missing_preconditions}")

        return ActionValidationResult(
            valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            action_id=action.action_id,
        )

    @classmethod
    def validate_plan(cls, plan: ActionPlan, facts: Optional[Set[str]] = None) -> ActionValidationResult:
        errors: List[str] = []
        warnings: List[str] = []

        if not plan.plan_id:
            errors.append("plan_id cannot be empty")
        if not plan.steps:
            errors.append("ActionPlan must contain at least one step")

        has_repair_or_control = False
        has_verification = False

        for i, step in enumerate(plan.steps):
            res = cls.validate_action(step, facts)
            if not res.valid:
                for err in res.errors:
                    errors.append(f"Step {i+1} ({step.action_id}): {err}")
            warnings.extend([f"Step {i+1} ({step.action_id}): {w}" for w in res.warnings])

            if step.category in {ActionCategory.REPAIR, ActionCategory.CONTROL, ActionCategory.CONFIGURE}:
                has_repair_or_control = True
            if step.category == ActionCategory.OBSERVE and step.action_type == ActionType.VALIDATE:
                has_verification = True

        if has_repair_or_control and not plan.requires_human_approval:
            errors.append("Plan containing REPAIR, CONFIGURE, or CONTROL must require human approval")

        if has_repair_or_control and not has_verification:
            warnings.append("Plan contains mutation/control steps but no explicit VALIDATE step in steps")

        return ActionValidationResult(
            valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            action_id=plan.plan_id,
        )
