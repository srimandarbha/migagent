"""Pre-seeded Operational Failure Signatures and Multi-Solution Plans.

Represents proven SRE operational runbook fixes as structured data.
Zero static code edits or Git PRs needed for operational tuning: these records
are loaded into the DynamicKnowledgeStore and persisted in PostgreSQL.
"""
from __future__ import annotations

from typing import List

from .action_ontology import (
    ActionDefinition,
    ActionPlan,
    ActionType,
    RiskLevel,
)
from .dynamic_knowledge_store import FailureSignature, KnownSolution


def get_seed_signatures() -> List[FailureSignature]:
    signatures: List[FailureSignature] = []

    # 1. NAA Serial Offload Failure
    sig1 = FailureSignature(
        signature_id="SIG-STORAGE-NAA-OFFLOAD",
        canonical_pattern=r"could not extract serial from NAA, trying to find by listing volumes",
        domain="storage",
        mechanism="STORAGE.MTV.OFFLOAD_SERIAL_UNRESOLVED",
        description="Could not extract serial from NAA device during storage copy-offload.",
        frequency=15,
        required_evidence=["TRANSFER_FAILED"],
    )
    plan1 = ActionPlan(
        plan_id="PLAN-STORAGE-NAA-OFFLOAD",
        failure_signature=sig1.signature_id,
        steps=[
            ActionDefinition(
                action_id="STEP-NAA-PATCH-PARAMS",
                action_type=ActionType.PLAN_SPEC_PATCH,
                title="Disable Storage Offload & Enable AnyToAny",
                description="Set anytoany to true and storage offload to false in migration plan specification.",
                risk_level=RiskLevel.LOW,
                requires_approval=True,
                approval_role="SRE",
                parameters={"anytoany": True, "storage_offload": False},
                verification_plan="Verify migration plan spec reflects anytoany=true and storage_offload=false.",
            ),
            ActionDefinition(
                action_id="STEP-NAA-RETRY",
                action_type=ActionType.RETRY,
                title="Retry Migration",
                description="Retry the migration plan with offload disabled.",
                risk_level=RiskLevel.LOW,
                requires_approval=True,
                approval_role="SRE",
                verification_plan="Verify MTV disk transfer progresses past volume discovery.",
            ),
            ActionDefinition(
                action_id="STEP-NAA-VALIDATE",
                action_type=ActionType.VALIDATE,
                title="Verify Transfer Progress",
                description="Inspect DataVolume and MTV transfer metrics to confirm streaming copy is active.",
                risk_level=RiskLevel.LOW,
                requires_approval=False,
            ),
        ],
        rationale="Disabling hardware storage offload avoids NAA serial resolution failure by falling back to host streaming copy.",
    )
    sig1.solutions.append(
        KnownSolution(
            solution_id="SOL-STORAGE-NAA-OFFLOAD-1",
            signature_id=sig1.signature_id,
            title="Set AnyToAny and Disable Storage Offload",
            action_summary="Set anytoany to true and storage offload to false, then retry the migration.",
            recommended_action="Set anytoany to true and storage offload to false, then retry the migration.",
            action_plan=plan1,
            automation_system="AAP",
            risk_level=RiskLevel.LOW,
            requires_approval=True,
            approval_role="SRE",
            success_count=14,
            failure_count=1,
            source="PRE_SEEDED",
        )
    )
    signatures.append(sig1)

    # 2. Guest OS Inspection Symlink Corruption
    sig2 = FailureSignature(
        signature_id="SIG-GUEST-INSPECT-SYMLINKS",
        canonical_pattern=r"virt-v2v(?:-in-place)?: error: inspection could not detect the source guest|check filesystem: 15\+ matched known OS partition",
        domain="guest_os",
        mechanism="GUEST.INSPECTION.SYMLINK_CORRUPT",
        description="virt-v2v inspection failed to detect source guest OS due to corrupted or mismatched symlinks.",
        frequency=8,
        required_evidence=["INSPECTION_FAILED"],
    )
    plan2 = ActionPlan(
        plan_id="PLAN-GUEST-INSPECT-SYMLINKS",
        failure_signature=sig2.signature_id,
        steps=[
            ActionDefinition(
                action_id="STEP-SYMLINK-PROBE",
                action_type=ActionType.CHECK_GUEST,
                title="Inspect Standard System Symlinks",
                description="Run readlink /bin /sbin /lib /lib64 on the source guest to inspect symlink targets.",
                command_template="readlink /bin /sbin /lib /lib64",
                risk_level=RiskLevel.LOW,
                requires_approval=False,
            ),
            ActionDefinition(
                action_id="STEP-SYMLINK-REPAIR",
                action_type=ActionType.SOURCE_VM_REPAIR,
                title="Repair Broken Guest Symlinks",
                description="Update each affected symlink via ln -sfn <correct-target> <link-path> on the source guest.",
                command_template="ln -sfn <correct-target> <link-path>",
                risk_level=RiskLevel.HIGH,
                requires_approval=True,
                approval_role="VM_OWNER",
                verification_plan="Run readlink on each path and verify correct canonical target.",
                rollback_plan="Revert symlinks to prior targets if conversion fails.",
            ),
            ActionDefinition(
                action_id="STEP-SYMLINK-RETRY",
                action_type=ActionType.RETRY,
                title="Retry Migration",
                description="Retry migration after guest symlinks are restored.",
                risk_level=RiskLevel.LOW,
                requires_approval=True,
                approval_role="SRE",
            ),
            ActionDefinition(
                action_id="STEP-SYMLINK-VALIDATE",
                action_type=ActionType.VALIDATE,
                title="Verify OS Inspection",
                description="Confirm virt-v2v inspect_os successfully detects Linux OS distribution and kernel.",
                risk_level=RiskLevel.LOW,
                requires_approval=False,
            ),
        ],
        rationale="virt-v2v os inspection expects canonical symlinks (e.g. /bin -> usr/bin). Broken or circular symlinks cause 15+ false partition matches.",
    )
    sig2.solutions.append(
        KnownSolution(
            solution_id="SOL-GUEST-INSPECT-SYMLINKS-1",
            signature_id=sig2.signature_id,
            title="Inspect and Fix Guest OS Symlinks",
            action_summary="Run readlink /bin /sbin /lib /lib64 and ln -sfn to fix symlinks, then retry.",
            recommended_action="Possible soft link issue. Run readlink /bin /sbin /lib /lib64 on the source guest and ln -sfn <correct-target> <link-path> to update each affected symlink.",
            action_plan=plan2,
            automation_system="MANUAL",
            risk_level=RiskLevel.HIGH,
            requires_approval=True,
            approval_role="VM_OWNER",
            success_count=7,
            failure_count=1,
            source="PRE_SEEDED",
        )
    )
    signatures.append(sig2)

    # 3. VMware Cores Per Socket Decimal / Incompatible Ratio
    sig3 = FailureSignature(
        signature_id="SIG-VMWARE-CPUID-CORES",
        canonical_pattern=r"vcpu entry [\"']?cpuid-coresPersocket[\"']? smaller than [\"']?numCpus[\"']?|VIR_ERR_INTERNAL_ERROR.*cpuid-coresPersocket",
        domain="vmware",
        mechanism="VMWARE.VM_SPEC.CPUID_CORES_PER_SOCKET_INVALID",
        description="VMware VM configuration has invalid cpuid-coresPersocket ratio or non-integer value.",
        frequency=6,
    )
    plan3 = ActionPlan(
        plan_id="PLAN-VMWARE-CPUID-CORES",
        failure_signature=sig3.signature_id,
        steps=[
            ActionDefinition(
                action_id="STEP-VMWARE-VCPU-FIX",
                action_type=ActionType.VMWARE_CONFIG,
                title="Correct Cores Per Socket Setting",
                description="Update Cores per Socket in VMware to a positive whole number (integer), not a decimal. Ensure it does not exceed the total vCPU count and divides that count evenly.",
                risk_level=RiskLevel.MEDIUM,
                requires_approval=True,
                approval_role="SRE",
                verification_plan="Verify in vCenter that numCpus % cpuid-coresPersocket == 0.",
            ),
            ActionDefinition(
                action_id="STEP-VMWARE-VCPU-RETRY",
                action_type=ActionType.RETRY,
                title="Retry Migration",
                description="Retry migration with corrected CPU topology.",
                risk_level=RiskLevel.LOW,
                requires_approval=True,
                approval_role="SRE",
            ),
            ActionDefinition(
                action_id="STEP-VMWARE-VCPU-VALIDATE",
                action_type=ActionType.VALIDATE,
                title="Verify Libvirt Domain Creation",
                description="Confirm libvirt conversion pod boots without vCPU topology error.",
                risk_level=RiskLevel.LOW,
                requires_approval=False,
            ),
        ],
        rationale="Libvirt enforces that cpuid-coresPersocket is an integer divisor of numCpus.",
    )
    sig3.solutions.append(
        KnownSolution(
            solution_id="SOL-VMWARE-CPUID-CORES-1",
            signature_id=sig3.signature_id,
            title="Update VMware Cores Per Socket",
            action_summary="Update Cores per Socket in VMware to a positive integer dividing numCpus evenly.",
            recommended_action="Update Cores per Socket in VMware to a positive whole number (integer), not a decimal. Ensure it does not exceed the total vCPU count and divides that count evenly, then retry the migration.",
            action_plan=plan3,
            automation_system="MANUAL",
            risk_level=RiskLevel.MEDIUM,
            requires_approval=True,
            approval_role="SRE",
            success_count=6,
            failure_count=0,
            source="PRE_SEEDED",
        )
    )
    signatures.append(sig3)

    # 4. Clone Task Payload Object Missing / ESXCLI Fault
    sig4 = FailureSignature(
        signature_id="SIG-MIG-PAYLOAD-OBJECT",
        canonical_pattern=r"esxcli command failed.*ESXCLI-CLIFault|failed to start clone task: The object or item referred to could not be found|copy-offload\.xcopy\.rescan\.esxcli",
        domain="migration_plan",
        mechanism="MIGRATION.PLAN.PAYLOAD_OBJECT_INVALID",
        description="Clone task failed because referenced payload object or ESXCLI target was invalid.",
        frequency=12,
    )
    plan4 = ActionPlan(
        plan_id="PLAN-MIG-PAYLOAD-OBJECT",
        failure_signature=sig4.signature_id,
        steps=[
            ActionDefinition(
                action_id="STEP-PAYLOAD-ESCALATE",
                action_type=ActionType.COMMAND_CENTER,
                title="Notify Command Center / Migration Team",
                description="For UK, US, and HK, the Command Center will recreate the plan. For other regional countries, the respective Migration team will recreate the plan.",
                risk_level=RiskLevel.LOW,
                requires_approval=True,
                approval_role="COMMAND_CENTER",
            ),
            ActionDefinition(
                action_id="STEP-PAYLOAD-RECREATE",
                action_type=ActionType.PLAN_SPEC_PATCH,
                title="Recreate Migration Plan with Valid Payload",
                description="Recreate the migration plan with corrected target datastore and payload objects.",
                risk_level=RiskLevel.MEDIUM,
                requires_approval=True,
                approval_role="COMMAND_CENTER",
                verification_plan="Verify new plan references valid active ESXi datastores and LUNs.",
            ),
            ActionDefinition(
                action_id="STEP-PAYLOAD-VALIDATE",
                action_type=ActionType.VALIDATE,
                title="Verify Clone Task Launch",
                description="Confirm clone task starts successfully on the recreated plan.",
                risk_level=RiskLevel.LOW,
                requires_approval=False,
            ),
        ],
        rationale="Payload objects were invalidated by storage rescanning or migration pipeline mismatch.",
    )
    sig4.solutions.append(
        KnownSolution(
            solution_id="SOL-MIG-PAYLOAD-OBJECT-1",
            signature_id=sig4.signature_id,
            title="Recreate Migration Plan via Command Center / Migration Team",
            action_summary="Contact Migration team / Command Center to recreate migration plan with valid payload.",
            recommended_action="Contact the Migration team, as this is related to a payload issue. Payload objects wrong, migs is getting those corrected. Recreate the migration plan with a valid payload. For UK, US, and HK, the Command Center will recreate the plan. For other regional countries, the respective Migration team will recreate the plan.",
            action_plan=plan4,
            automation_system="COMMAND_CENTER",
            risk_level=RiskLevel.MEDIUM,
            requires_approval=True,
            approval_role="COMMAND_CENTER",
            success_count=11,
            failure_count=1,
            source="PRE_SEEDED",
        )
    )
    signatures.append(sig4)

    # 5. /etc/fstab Augeas Parse Failure
    sig5 = FailureSignature(
        signature_id="SIG-GUEST-AUGEAS-FSTAB",
        canonical_pattern=r"error: libguestfs error: inspect_os: /etc/fstab: augeas parse failure",
        domain="guest_os",
        mechanism="GUEST.CONFIG.FSTAB_PARSE_FAILURE",
        description="Augeas failed to parse /etc/fstab due to syntax errors or corrupt whitespace.",
        frequency=9,
    )
    plan5 = ActionPlan(
        plan_id="PLAN-GUEST-AUGEAS-FSTAB",
        failure_signature=sig5.signature_id,
        steps=[
            ActionDefinition(
                action_id="STEP-FSTAB-VALIDATE-SCRIPT",
                action_type=ActionType.SOURCE_VM_SCRIPT,
                title="Run fstab_validator.sh",
                description="Run the script fstab_validator.sh to identify syntax issues in /etc/fstab.",
                command_template="bash /opt/scripts/fstab_validator.sh",
                risk_level=RiskLevel.LOW,
                requires_approval=False,
                verification_plan="Check fstab_validator output for offending line numbers.",
            ),
            ActionDefinition(
                action_id="STEP-FSTAB-REPAIR",
                action_type=ActionType.SOURCE_VM_REPAIR,
                title="Correct /etc/fstab Syntax",
                description="Request OS support team to fix identified syntax issues and remove duplicate fields.",
                risk_level=RiskLevel.HIGH,
                requires_approval=True,
                approval_role="VM_OWNER",
                verification_plan="Re-run fstab_validator.sh until 0 errors reported.",
                rollback_plan="Restore from /etc/fstab.bak prior to edits.",
            ),
            ActionDefinition(
                action_id="STEP-FSTAB-RETRY",
                action_type=ActionType.RETRY,
                title="Retry Migration",
                description="Retry migration once /etc/fstab is clean.",
                risk_level=RiskLevel.LOW,
                requires_approval=True,
                approval_role="SRE",
            ),
            ActionDefinition(
                action_id="STEP-FSTAB-VALIDATE",
                action_type=ActionType.VALIDATE,
                title="Verify Libguestfs OS Inspection",
                description="Verify virt-v2v inspect_os completes without Augeas parse failure.",
                risk_level=RiskLevel.LOW,
                requires_approval=False,
            ),
        ],
        rationale="Libguestfs uses Augeas to parse /etc/fstab for mount points. Unescaped spaces or duplicate options break Augeas lenses.",
    )
    sig5.solutions.append(
        KnownSolution(
            solution_id="SOL-GUEST-AUGEAS-FSTAB-1",
            signature_id=sig5.signature_id,
            title="Validate and Fix /etc/fstab Syntax",
            action_summary="Run fstab_validator.sh to identify syntax issues and request support team fix.",
            recommended_action="Run the script fstab_validator.sh to identify the syntax issues and request the respective support team to fix them.",
            action_plan=plan5,
            automation_system="MANUAL",
            risk_level=RiskLevel.HIGH,
            requires_approval=True,
            approval_role="VM_OWNER",
            success_count=8,
            failure_count=1,
            source="PRE_SEEDED",
        )
    )
    signatures.append(sig5)

    # 6. Not Enough Inodes on Conversion Filesystem
    sig6 = FailureSignature(
        signature_id="SIG-GUEST-INODES-EXHAUSTED",
        canonical_pattern=r"error: not enough available inodes for conversion on filesystem",
        domain="guest_os",
        mechanism="GUEST.STORAGE.INODES_EXHAUSTED",
        description="Source guest filesystem lacks sufficient free inodes for conversion scaffolding.",
        frequency=5,
    )
    plan6 = ActionPlan(
        plan_id="PLAN-GUEST-INODES-EXHAUSTED",
        failure_signature=sig6.signature_id,
        steps=[
            ActionDefinition(
                action_id="STEP-INODES-CHECK",
                action_type=ActionType.CHECK_GUEST,
                title="Inspect Inode Usage",
                description="Run df -i on guest filesystems to identify exhausted mount points.",
                command_template="df -i",
                risk_level=RiskLevel.LOW,
                requires_approval=False,
            ),
            ActionDefinition(
                action_id="STEP-INODES-CLEANUP",
                action_type=ActionType.SOURCE_VM_REPAIR,
                title="Free Inodes or Expand Filesystem",
                description="Free inodes by deleting unnecessary files with owner approval, or extend the impacted filesystem if it supports adding inodes through expansion.",
                risk_level=RiskLevel.HIGH,
                requires_approval=True,
                approval_role="VM_OWNER",
                verification_plan="Run df -i to confirm at least 15% free inodes remain.",
            ),
            ActionDefinition(
                action_id="STEP-INODES-RETRY",
                action_type=ActionType.RETRY,
                title="Retry Migration",
                description="Retry migration once free inodes are available.",
                risk_level=RiskLevel.LOW,
                requires_approval=True,
                approval_role="SRE",
            ),
            ActionDefinition(
                action_id="STEP-INODES-VALIDATE",
                action_type=ActionType.VALIDATE,
                title="Verify Conversion Execution",
                description="Confirm virt-v2v conversion executes without inode exhaustion errors.",
                risk_level=RiskLevel.LOW,
                requires_approval=False,
            ),
        ],
        rationale="virt-v2v creates temporary helper files during conversion. 100% inode utilization aborts conversion.",
    )
    sig6.solutions.append(
        KnownSolution(
            solution_id="SOL-GUEST-INODES-EXHAUSTED-1",
            signature_id=sig6.signature_id,
            title="Free Inodes or Expand Filesystem",
            action_summary="Free inodes with owner approval or extend filesystem, then retry migration.",
            recommended_action="Free inodes by deleting unnecessary files with owner approval, or extend the impacted filesystem if it supports adding inodes through expansion, then retry the migration.",
            action_plan=plan6,
            automation_system="MANUAL",
            risk_level=RiskLevel.HIGH,
            requires_approval=True,
            approval_role="VM_OWNER",
            success_count=5,
            failure_count=0,
            source="PRE_SEEDED",
        )
    )
    signatures.append(sig6)

    # 7. LUKS Key Missing
    sig7 = FailureSignature(
        signature_id="SIG-SEC-LUKS-KEY-MISSING",
        canonical_pattern=r"virt-v2v(?:-in-place)?: could not read key from user",
        domain="security",
        mechanism="SECURITY.LUKS.KEY_MISSING",
        description="virt-v2v was unable to read user key required to decrypt LUKS-encrypted guest volume.",
        frequency=4,
    )
    plan7 = ActionPlan(
        plan_id="PLAN-SEC-LUKS-KEY-MISSING",
        failure_signature=sig7.signature_id,
        steps=[
            ActionDefinition(
                action_id="STEP-LUKS-PASS-KEY",
                action_type=ActionType.MIGRATION_PARAMS,
                title="Provide LUKS Encryption Key in Plan",
                description="Configure the migration secret/parameters with the LUKS encryption passphrase.",
                risk_level=RiskLevel.LOW,
                requires_approval=True,
                approval_role="SRE",
                verification_plan="Verify secret is created and referenced in MigrationPlan secretRef.",
            ),
            ActionDefinition(
                action_id="STEP-LUKS-RETRY",
                action_type=ActionType.RETRY,
                title="Retry Migration",
                description="Retry the migration with the encryption key passed.",
                risk_level=RiskLevel.LOW,
                requires_approval=True,
                approval_role="SRE",
            ),
            ActionDefinition(
                action_id="STEP-LUKS-VALIDATE",
                action_type=ActionType.VALIDATE,
                title="Verify LUKS Volume Unlock",
                description="Confirm virt-v2v opens the cryptsetup mapping successfully.",
                risk_level=RiskLevel.LOW,
                requires_approval=False,
            ),
        ],
        rationale="Encrypted partitions require non-interactive passphrase injection via migration plan secrets.",
    )
    sig7.solutions.append(
        KnownSolution(
            solution_id="SOL-SEC-LUKS-KEY-MISSING-1",
            signature_id=sig7.signature_id,
            title="Pass LUKS Encryption Key During Migration",
            action_summary="Retry the migration and ask the Migration team to pass the LUKS encryption key.",
            recommended_action="Retry the migration and ask the Migration team to pass the LUKS encryption key during migration.",
            action_plan=plan7,
            automation_system="AAP",
            risk_level=RiskLevel.LOW,
            requires_approval=True,
            approval_role="SRE",
            success_count=4,
            failure_count=0,
            source="PRE_SEEDED",
        )
    )
    signatures.append(sig7)

    # 8. Inappropriate ioctl for device (Decryption Prompt Blocked)
    sig8 = FailureSignature(
        signature_id="SIG-SEC-DECRYPTION-IOCTL",
        canonical_pattern=r"getline: Inappropriate ioctl for device",
        domain="security",
        mechanism="SECURITY.DECRYPTION.PROMPT_BLOCKED",
        description="Decryption prompt attempted interactive terminal read in non-interactive conversion container.",
        frequency=3,
    )
    plan8 = ActionPlan(
        plan_id="PLAN-SEC-DECRYPTION-IOCTL",
        failure_signature=sig8.signature_id,
        steps=[
            ActionDefinition(
                action_id="STEP-IOCTL-PARAM-PASS",
                action_type=ActionType.MIGRATION_PARAMS,
                title="Inject Decryption Key via Secret",
                description="Pass the decryption key via non-interactive secret to avoid tty ioctl prompt.",
                risk_level=RiskLevel.LOW,
                requires_approval=True,
                approval_role="SRE",
            ),
            ActionDefinition(
                action_id="STEP-IOCTL-RETRY",
                action_type=ActionType.RETRY,
                title="Retry Migration",
                description="Retry migration with non-interactive key configuration.",
                risk_level=RiskLevel.LOW,
                requires_approval=True,
                approval_role="SRE",
            ),
            ActionDefinition(
                action_id="STEP-IOCTL-VALIDATE",
                action_type=ActionType.VALIDATE,
                title="Verify Container Decryption",
                description="Confirm non-interactive volume unlock without tty prompts.",
                risk_level=RiskLevel.LOW,
                requires_approval=False,
            ),
        ],
        rationale="Containerized virt-v2v runs without a TTY; interactive passphrase prompts fail with ENOTTY / inappropriate ioctl.",
    )
    sig8.solutions.append(
        KnownSolution(
            solution_id="SOL-SEC-DECRYPTION-IOCTL-1",
            signature_id=sig8.signature_id,
            title="Inject Decryption Key Non-interactively",
            action_summary="Retry the migration and inform Migration team to pass decryption key.",
            recommended_action="Retry the migration and inform the Migration Team pass the decryption key during migration.",
            action_plan=plan8,
            automation_system="AAP",
            risk_level=RiskLevel.LOW,
            requires_approval=True,
            approval_role="SRE",
            success_count=3,
            failure_count=0,
            source="PRE_SEEDED",
        )
    )
    signatures.append(sig8)

    # 9. Partition Outside Disk
    sig9 = FailureSignature(
        signature_id="SIG-STORAGE-PARTITION-OUTSIDE-DISK",
        canonical_pattern=r"Error: Can't have a partition outside the disk",
        domain="storage",
        mechanism="STORAGE.PARTITION.OUTSIDE_DISK_BOUNDARY",
        description="Partition table sector offsets exceed target disk size or disk was truncated.",
        frequency=7,
    )
    plan9 = ActionPlan(
        plan_id="PLAN-STORAGE-PARTITION-OUTSIDE-DISK",
        failure_signature=sig9.signature_id,
        steps=[
            ActionDefinition(
                action_id="STEP-PARTITION-CHECK-SIZE",
                action_type=ActionType.CHECK_STORAGE,
                title="Check Disk Capacity and Partition Table",
                description="Check disk capacity and partition table: verify if partition extends beyond disk boundary or migrated disk is smaller than source.",
                risk_level=RiskLevel.LOW,
                requires_approval=False,
            ),
            ActionDefinition(
                action_id="STEP-PARTITION-REPAIR-LAYOUT",
                action_type=ActionType.STORAGE_REPAIR,
                title="Correct Disk Size and Partition Layout",
                description="Obtain owner approval and a backup before correcting the disk size or partition layout. Follow Red Hat solution 3236591.",
                risk_level=RiskLevel.CRITICAL,
                requires_approval=True,
                approval_role="VM_OWNER",
                verification_plan="Verify parted -l shows end sector within disk capacity.",
                rollback_plan="Restore source disk from verified snapshot/backup.",
            ),
            ActionDefinition(
                action_id="STEP-PARTITION-RETRY",
                action_type=ActionType.RETRY,
                title="Retry Migration",
                description="Retry migration with adjusted disk capacity and partition table.",
                risk_level=RiskLevel.MEDIUM,
                requires_approval=True,
                approval_role="SRE",
            ),
            ActionDefinition(
                action_id="STEP-PARTITION-VALIDATE",
                action_type=ActionType.VALIDATE,
                title="Verify Disk Partition Alignment",
                description="Confirm target DataVolume imports partitions without boundary errors.",
                risk_level=RiskLevel.LOW,
                requires_approval=False,
            ),
        ],
        rationale="Source VM disk may have been resized without expanding the partition table or target PVC is smaller than source VMDK.",
    )
    sig9.solutions.append(
        KnownSolution(
            solution_id="SOL-STORAGE-PARTITION-OUTSIDE-DISK-1",
            signature_id=sig9.signature_id,
            title="Correct Disk Capacity & Partition Alignment",
            action_summary="Verify disk capacity/partitions, obtain owner approval + backup, fix size, then retry.",
            recommended_action="Check the disk capacity and partition table: a partition may extend beyond the disk boundary. Confirm the migrated disk is not truncated or smaller than the source. Obtain owner approval and a backup before correcting the disk size or partition layout, then retry the migration. Red Hat reference: https://access.redhat.com/solutions/3236591",
            action_plan=plan9,
            automation_system="MANUAL",
            risk_level=RiskLevel.CRITICAL,
            requires_approval=True,
            approval_role="VM_OWNER",
            external_ref="https://access.redhat.com/solutions/3236591",
            success_count=6,
            failure_count=1,
            source="PRE_SEEDED",
        )
    )
    signatures.append(sig9)

    # 10. Filesystem Still Has Errors (fsck required)
    sig10 = FailureSignature(
        signature_id="SIG-GUEST-FS-ERRORS",
        canonical_pattern=r"WARNING: Filesystem still has errors",
        domain="guest_os",
        mechanism="GUEST.FILESYSTEM.CORRUPTION_DETECTED",
        description="Guest filesystem is marked dirty or contains unrecoverable journal errors.",
        frequency=10,
    )
    plan10 = ActionPlan(
        plan_id="PLAN-GUEST-FS-ERRORS",
        failure_signature=sig10.signature_id,
        steps=[
            ActionDefinition(
                action_id="STEP-FSCK-EXECUTE",
                action_type=ActionType.FILESYSTEM_FSCK,
                title="Run fsck on Reported Filesystem",
                description="Run fsck on reported filesystem. Get confirmation from Server/Service owner and ensure a backup is taken before running fsck.",
                command_template="fsck -y /dev/<impacted-partition>",
                risk_level=RiskLevel.HIGH,
                requires_approval=True,
                approval_role="VM_OWNER",
                verification_plan="Verify fsck exit code 0 or 1 (clean/corrected).",
                rollback_plan="Restore from snapshot taken prior to fsck execution.",
            ),
            ActionDefinition(
                action_id="STEP-FSCK-RETRY",
                action_type=ActionType.RETRY,
                title="Retry Migration",
                description="Retry migration once filesystem is verified clean.",
                risk_level=RiskLevel.LOW,
                requires_approval=True,
                approval_role="SRE",
            ),
            ActionDefinition(
                action_id="STEP-FSCK-VALIDATE",
                action_type=ActionType.VALIDATE,
                title="Verify Clean Filesystem Mount",
                description="Confirm virt-v2v mounts guest partitions cleanly without dirty journal flags.",
                risk_level=RiskLevel.LOW,
                requires_approval=False,
            ),
        ],
        rationale="Unclean shutdown or filesystem errors cause virt-v2v inspection to refuse mounting in read-write mode.",
    )
    sig10.solutions.append(
        KnownSolution(
            solution_id="SOL-GUEST-FS-ERRORS-1",
            signature_id=sig10.signature_id,
            title="Execute Backup and Run fsck",
            action_summary="Run fsck on reported filesystem after owner approval and backup, then retry.",
            recommended_action="Run fsck on the reported filesystem. Get confirmation from the Server/Service owner and ensure a backup is taken before running fsck.",
            action_plan=plan10,
            automation_system="MANUAL",
            risk_level=RiskLevel.HIGH,
            requires_approval=True,
            approval_role="VM_OWNER",
            success_count=9,
            failure_count=1,
            source="PRE_SEEDED",
        )
    )
    signatures.append(sig10)

    # 11. Stale GRUB Legacy Bootloader Configuration
    sig11 = FailureSignature(
        signature_id="SIG-BOOT-GRUB-LEGACY",
        canonical_pattern=r"no kernels were found in the bootloader configuration",
        domain="bootloader",
        mechanism="BOOTLOADER.GRUB.STALE_LEGACY_CONFIG",
        description="Conversion found empty or stale legacy GRUB configuration instead of active GRUB2 config.",
        frequency=5,
    )
    plan11 = ActionPlan(
        plan_id="PLAN-BOOT-GRUB-LEGACY",
        failure_signature=sig11.signature_id,
        steps=[
            ActionDefinition(
                action_id="STEP-GRUB-REMOVE-LEGACY",
                action_type=ActionType.BOOTLOADER_REPAIR,
                title="Remove Stale GRUB Legacy File",
                description="Remove stale GRUB Legacy bootloader file (/boot/grub/grub.conf) after taking a backup. Do NOT remove GRUB2 /boot/grub2/grub.cfg.",
                command_template="cp /boot/grub/grub.conf /boot/grub/grub.conf.bak && rm -f /boot/grub/grub.conf",
                risk_level=RiskLevel.HIGH,
                requires_approval=True,
                approval_role="SRE",
                verification_plan="Verify /boot/grub2/grub.cfg exists and contains kernel entries.",
                rollback_plan="Restore /boot/grub/grub.conf from backup.",
            ),
            ActionDefinition(
                action_id="STEP-GRUB-RETRY",
                action_type=ActionType.RETRY,
                title="Retry Migration",
                description="Retry migration after stale legacy grub.conf is removed.",
                risk_level=RiskLevel.LOW,
                requires_approval=True,
                approval_role="SRE",
            ),
            ActionDefinition(
                action_id="STEP-GRUB-VALIDATE",
                action_type=ActionType.VALIDATE,
                title="Verify Bootloader Kernel Detection",
                description="Verify virt-v2v inspects bootloader and discovers active kernel in grub2.",
                risk_level=RiskLevel.LOW,
                requires_approval=False,
            ),
        ],
        rationale="If /boot/grub/grub.conf exists from a legacy OS upgrade, virt-v2v prioritizes it over GRUB2 and fails if it has no kernels.",
    )
    sig11.solutions.append(
        KnownSolution(
            solution_id="SOL-BOOT-GRUB-LEGACY-1",
            signature_id=sig11.signature_id,
            title="Remove Stale GRUB Legacy Configuration",
            action_summary="Backup and remove /boot/grub/grub.conf, retain /boot/grub2/grub.cfg, retry.",
            recommended_action="Remove the stale GRUB Legacy bootloader file (/boot/grub/grub.conf) after taking a backup. Do NOT remove the GRUB2 /boot/grub2/grub.cfg.",
            action_plan=plan11,
            automation_system="MANUAL",
            risk_level=RiskLevel.HIGH,
            requires_approval=True,
            approval_role="SRE",
            success_count=5,
            failure_count=0,
            source="PRE_SEEDED",
        )
    )
    signatures.append(sig11)

    # 12. OOM Killer in Conversion Pod
    sig12 = FailureSignature(
        signature_id="SIG-CONV-OOM-KILLER",
        canonical_pattern=r"oom-killer out of memory:",
        domain="conversion",
        mechanism="CONVERSION.RESOURCE.OOM_KILLED",
        description="virt-v2v conversion pod exceeded memory limits and was killed by Linux OOM killer.",
        frequency=11,
    )
    plan12 = ActionPlan(
        plan_id="PLAN-CONV-OOM-KILLER",
        failure_signature=sig12.signature_id,
        steps=[
            ActionDefinition(
                action_id="STEP-OOM-INCREASE-MEM",
                action_type=ActionType.PLATFORM_CONFIG,
                title="Increase Conversion Pod Memory to 24G",
                description="Increase Forklift/virt-v2v conversion pod memory requests and limits to 24G in ForkliftController CR.",
                parameters={"memory_limit": "24Gi", "memory_request": "16Gi"},
                risk_level=RiskLevel.MEDIUM,
                requires_approval=True,
                approval_role="SRE",
                verification_plan="Verify forklift-controller CR reflects updated v2v_pod_resources.",
            ),
            ActionDefinition(
                action_id="STEP-OOM-RETRY",
                action_type=ActionType.RETRY,
                title="Retry Migration",
                description="Retry migration with 24G conversion pod memory limit.",
                risk_level=RiskLevel.LOW,
                requires_approval=True,
                approval_role="SRE",
            ),
            ActionDefinition(
                action_id="STEP-OOM-ESCALATE-RH",
                action_type=ActionType.RED_HAT,
                title="Escalate to Red Hat if Persists",
                description="If the issue persists with 24G memory, open a support case with Red Hat with must-gather logs.",
                risk_level=RiskLevel.LOW,
                requires_approval=True,
                approval_role="SRE",
            ),
            ActionDefinition(
                action_id="STEP-OOM-VALIDATE",
                action_type=ActionType.VALIDATE,
                title="Verify Conversion Memory Headroom",
                description="Monitor container_memory_working_set_bytes to ensure memory stays within 24G.",
                risk_level=RiskLevel.LOW,
                requires_approval=False,
            ),
        ],
        rationale="Large guest VMs or high filesystem fragmentation require extensive memory for guestfs in-memory structures.",
    )
    sig12.solutions.append(
        KnownSolution(
            solution_id="SOL-CONV-OOM-KILLER-1",
            signature_id=sig12.signature_id,
            title="Increase Conversion Memory to 24G",
            action_summary="Increase memory to 24G; if persists, raise a case with Red Hat.",
            recommended_action="Memory increased to 24G and no recent failures. If the issue persists, raise a case with Red Hat with all required logs.",
            action_plan=plan12,
            automation_system="AAP",
            risk_level=RiskLevel.MEDIUM,
            requires_approval=True,
            approval_role="SRE",
            success_count=10,
            failure_count=1,
            source="PRE_SEEDED",
        )
    )
    signatures.append(sig12)

    # 13. Dell ESX HBA UID Failure (Zoning / PTASK0176796)
    sig13 = FailureSignature(
        signature_id="SIG-STORAGE-DELL-HBA",
        canonical_pattern=r"failed to add the ESX HBA UID",
        domain="storage",
        mechanism="STORAGE.HBA.DELL_ZONING_FAILURE",
        description="Known Dell array issue where ESX HBA UID registration fails during LUN attachment.",
        frequency=8,
    )
    plan13 = ActionPlan(
        plan_id="PLAN-STORAGE-DELL-HBA",
        failure_signature=sig13.signature_id,
        steps=[
            ActionDefinition(
                action_id="STEP-DELL-RETRY",
                action_type=ActionType.RETRY,
                title="Retry Migration",
                description="Retry migration once to allow transient Dell HBA registration to succeed.",
                risk_level=RiskLevel.LOW,
                requires_approval=True,
                approval_role="SRE",
            ),
            ActionDefinition(
                action_id="STEP-DELL-ESCALATE-STORAGE",
                action_type=ActionType.STORAGE_TEAM,
                title="Validate SAN Zoning with Storage Team",
                description="If retry fails, validate zoning information with Migration/Storage Team. Correlate with known issue PTASK0176796.",
                risk_level=RiskLevel.MEDIUM,
                requires_approval=True,
                approval_role="SRE",
            ),
            ActionDefinition(
                action_id="STEP-DELL-VALIDATE",
                action_type=ActionType.VALIDATE,
                title="Verify HBA Discovery",
                description="Verify storage array registers ESXi host initiator WWNs cleanly.",
                risk_level=RiskLevel.LOW,
                requires_approval=False,
            ),
        ],
        rationale="Known Dell array race condition during initial HBA UID registration; documented under PTASK0176796.",
    )
    sig13.solutions.append(
        KnownSolution(
            solution_id="SOL-STORAGE-DELL-HBA-1",
            signature_id=sig13.signature_id,
            title="Retry Migration and Validate Dell SAN Zoning",
            action_summary="Known Dell issue. Retry migration; if it still fails, validate zoning with Storage Team (PTASK0176796).",
            recommended_action="Known Dell issue. Retry the migration; if it still fails, validate the zoning information with the help to Migration/Storage Team. For new arrays this issue has been seen repeatedly; raised PTASK0176796.",
            action_plan=plan13,
            automation_system="MANUAL",
            risk_level=RiskLevel.MEDIUM,
            requires_approval=True,
            approval_role="SRE",
            external_ref="PTASK0176796",
            success_count=7,
            failure_count=1,
            source="PRE_SEEDED",
        )
    )
    signatures.append(sig13)

    # 14. Failed to Ensure Prerequisite Resource
    sig14 = FailureSignature(
        signature_id="SIG-OCV-FAILED-TO-ENSURE",
        canonical_pattern=r"failed to ensure",
        domain="platform",
        mechanism="OCV.PREREQUISITE.RESOURCE_ENSURE_FAILED",
        description="Forklift controller failed to ensure a required resource (PVC, DataVolume, or Network).",
        frequency=14,
    )
    plan14 = ActionPlan(
        plan_id="PLAN-OCV-FAILED-TO-ENSURE",
        failure_signature=sig14.signature_id,
        steps=[
            ActionDefinition(
                action_id="STEP-ENSURE-INSPECT-POD",
                action_type=ActionType.CHECK_PLATFORM,
                title="Check Failed Resource & Review Pod Events",
                description="Check the resource/prerequisite that failed to be ensured (PVC, DataVolume, network). Review full pod events.",
                risk_level=RiskLevel.LOW,
                requires_approval=False,
                verification_plan="Identify whether PVC provisioning, NAD attachment, or permission error is recorded.",
            ),
            ActionDefinition(
                action_id="STEP-ENSURE-VALIDATE",
                action_type=ActionType.VALIDATE,
                title="Verify Prerequisite Status",
                description="Ensure the identified prerequisite resource reaches Ready/Bound state.",
                risk_level=RiskLevel.LOW,
                requires_approval=False,
            ),
            ActionDefinition(
                action_id="STEP-ENSURE-RETRY",
                action_type=ActionType.RETRY,
                title="Retry Migration",
                description="Retry migration once the target resource/prerequisite is healthy.",
                risk_level=RiskLevel.LOW,
                requires_approval=True,
                approval_role="SRE",
            ),
        ],
        rationale="Controller reconcile loop failed to create or bind an underlying Kubernetes dependency before starting the transfer.",
    )
    sig14.solutions.append(
        KnownSolution(
            solution_id="SOL-OCV-FAILED-TO-ENSURE-1",
            signature_id=sig14.signature_id,
            title="Inspect Prerequisite Resource & Review Events",
            action_summary="Check resource (PVC, DataVolume, network), review pod events, and retry once resolved.",
            recommended_action="Check the resource/prerequisite that failed to be ensured (PVC, DataVolume, network). Review the full pod events and retry once resolved.",
            action_plan=plan14,
            automation_system="MANUAL",
            risk_level=RiskLevel.LOW,
            requires_approval=True,
            approval_role="SRE",
            success_count=13,
            failure_count=1,
            source="PRE_SEEDED",
        )
    )
    signatures.append(sig14)

    return signatures
