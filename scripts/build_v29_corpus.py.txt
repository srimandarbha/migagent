from pathlib import Path
import yaml, json
ROOT=Path(__file__).resolve().parents[1]

sources={
'RH-MTV-2.12-PREREQ':'https://docs.redhat.com/en/documentation/migration_toolkit_for_virtualization/2.12/html/planning_your_migration_to_red_hat_openshift_virtualization/assembly_provider-specific-requirements-for-migration_mtv',
'RH-MTV-2.12-PLAN-VMWARE':'https://docs.redhat.com/en/documentation/migration_toolkit_for_virtualization/2.12/html/planning_your_migration_to_red_hat_openshift_virtualization/assembly_planning-migration-vmware_mtv',
'RH-MTV-2.12-ERRORS':'https://docs.redhat.com/en/documentation/migration_toolkit_for_virtualization/2.12/html/migrating_your_virtual_machines_to_red_hat_openshift_virtualization/assembly_troubleshooting-migration_mtv',
'RH-MTV-2.12-RELEASE':'https://docs.redhat.com/en/documentation/migration_toolkit_for_virtualization/2.12/html/release_notes/ref_rn-2-12_release-notes',
'RH-MTV-2.12-RELEASE-PDF':'https://docs.redhat.com/en/documentation/migration_toolkit_for_virtualization/2.12/pdf/release_notes/Migration_Toolkit_for_Virtualization-2.12-Release_notes-en-US.pdf',
'RH-MTV-2.12-PORTS':'https://docs.redhat.com/en/documentation/migration_toolkit_for_virtualization/2.12/pdf/planning_your_migration_to_red_hat_openshift_virtualization/Migration_Toolkit_for_Virtualization-2.12-Planning_your_migration_to_Red_Hat_OpenShift_Virtualization-en-US.pdf',
'RH-MTV-2.12-COLD-WARM':'https://docs.redhat.com/en/documentation/migration_toolkit_for_virtualization/2.12/html/planning_your_migration_to_red_hat_openshift_virtualization/assembly_cold-warm-migration_mtv',
'RH-MTV-2.12-SOFTWARE':'https://docs.redhat.com/en/documentation/migration_toolkit_for_virtualization/2.12/html/planning_your_migration_to_red_hat_openshift_virtualization/assembly_software-requirements-for-migration_mtv',
'RH-MTV-2.12-UNDERSTANDING':'https://docs.redhat.com/en/documentation/migration_toolkit_for_virtualization/2.12/html/migrating_your_virtual_machines_to_red_hat_openshift_virtualization/assembly_understanding-mtv-migration_mtv',
'RH-MTV-2.12-PLAN-PDF':'https://docs.redhat.com/en/documentation/migration_toolkit_for_virtualization/2.12/pdf/planning_your_migration_to_red_hat_openshift_virtualization/assembly_migrating-vms-cli_mtv',
}

raw=[
('MTV-001','Warm migration CBT snapshot retry limit','VMware/CBT','precopy','vmware.cbt.retry_limit','RH-MTV-2.12-ERRORS','Warm migration reaches the documented CBT snapshot retry limit.','VMWARE.CBT_SNAPSHOT_LIMIT'),
('MTV-002','CBT disabled on source VM','VMware/CBT','preflight','vmware.cbt.disabled','RH-MTV-2.12-PREREQ','Warm migration requires CBT on the source VM.','VMWARE.CBT_CONFIGURATION'),
('MTV-003','CBT disabled on source disk','VMware/CBT','preflight','vmware.cbt.disk_disabled','RH-MTV-2.12-PREREQ','Warm migration requires CBT on each source VM disk.','VMWARE.CBT_CONFIGURATION'),
('MTV-004','VMware Tools missing for warm migration','VMware/Guest prerequisites','preflight','vmware.tools.missing','RH-MTV-2.12-PREREQ','VMware Tools is required for warm migration.','VMWARE.TOOLS_CONFIGURATION'),
('MTV-005','Source VM hibernated','VMware/Guest prerequisites','preflight','vmware.vm.hibernated','RH-MTV-2.12-PREREQ','MTV does not support migrating hibernated VMware VMs.','VMWARE.GUEST_STATE_UNSUPPORTED'),
('MTV-006','Unsupported VMware vSphere version','VMware/Compatibility','preflight','vmware.vsphere.version_unsupported','RH-MTV-2.12-PREREQ','Source vSphere version is outside the documented compatibility range.','VMWARE.VERSION_COMPATIBILITY'),
('MTV-007','VMware source privilege missing','VMware/Credentials','provider_validation','vmware.vcenter.privilege_denied','RH-MTV-2.12-PREREQ','MTV source account lacks required VMware privileges.','VMWARE.AUTHORIZATION'),
('MTV-008','VMware vCenter endpoint unreachable on 443','VMware/Connectivity','provider_validation','vmware.vcenter.port443_unreachable','RH-MTV-2.12-PORTS','OpenShift nodes cannot reach the VMware vCenter endpoint on TCP 443.','VMWARE.VCENTER_CONNECTIVITY'),
('MTV-009','ESXi endpoint unreachable on 443','VMware/Connectivity','transfer','vmware.esxi.port443_unreachable','RH-MTV-2.12-PORTS','OpenShift nodes cannot reach ESXi on TCP 443.','VMWARE.ESXI_CONNECTIVITY'),
('MTV-010','ESXi disk transfer port 902 blocked','VMware/Connectivity','transfer','vmware.esxi.port902_unreachable','RH-MTV-2.12-PORTS','OpenShift nodes cannot reach ESXi on TCP 902 used for disk transfer.','VMWARE.ESXI_CONNECTIVITY'),
('MTV-011','Target namespace cannot reach VMware','VMware/Connectivity','transfer','network.vmware_egress_unreachable','RH-MTV-2.12-PREREQ','Target namespace lacks required network connectivity to the VMware source.','NETWORK.VMWARE_EGRESS'),
('MTV-012','NetworkPolicy blocks VMware egress','Network/Policy','transfer','networkpolicy.vmware_egress_blocked','RH-MTV-2.12-PREREQ','A NetworkPolicy blocks egress from the target namespace to VMware.','NETWORK.POLICY_EGRESS'),
('MTV-013','VDDK missing for VMware vSAN','VMware/VDDK','provider_validation','vmware.vddk.required_vsan','RH-MTV-2.12-ERRORS','Migration of a VM backed by vSAN lacks the required VDDK image.','MTV.VDDK_CONFIGURATION'),
('MTV-014','VDDK absent for warm migration','VMware/VDDK','provider_validation','vmware.vddk.required_warm','RH-MTV-2.12-PREREQ','Warm migration is attempted without the required VDDK configuration.','MTV.VDDK_CONFIGURATION'),
('MTV-015','VDDK image inaccessible to migration namespace','VMware/VDDK','provider_validation','vmware.vddk.image_unreachable','RH-MTV-2.12-ERRORS','Provider namespace cannot access the configured VDDK image.','MTV.VDDK_IMAGE_ACCESS'),
('MTV-016','VDDK image built from filesystem that loses symlinks','VMware/VDDK','provider_validation','vmware.vddk.symlink_corruption','RH-MTV-2.12-PREREQ','VDDK image build source does not preserve required symbolic links.','MTV.VDDK_IMAGE_INTEGRITY'),
('MTV-017','VMware provider certificate/FQDN mismatch','VMware/Certificates','provider_validation','vmware.provider.certificate_fqdn_mismatch','RH-MTV-2.12-PREREQ','Provider URL FQDN does not match the configured certificate identity.','VMWARE.TLS_CONFIGURATION'),
('MTV-018','VMware provider remains not Ready','VMware/Provider','provider_validation','vmware.provider.not_ready','RH-MTV-2.12-PREREQ','VMware provider validation does not reach Ready.','VMWARE.PROVIDER_VALIDATION'),
('MTV-019','NFC service memory limit exceeded by concurrent migrations','VMware/NFC','transfer','vmware.nfc.memory_limit','RH-MTV-2.12-PREREQ','More than 10 VMs are migrated from an ESXi host without sufficient NFC service memory.','VMWARE.NFC_CAPACITY'),
('MTV-020','Windows VSS unavailable during warm snapshot','VMware/Windows','precopy_snapshot','os.windows.vss_unavailable','RH-MTV-2.12-ERRORS','Windows warm-migration snapshot creation fails because VSS is unavailable.','OS.WINDOWS_VSS'),
('MTV-021','VMware Snapshot Provider unavailable','VMware/Windows','precopy_snapshot','os.windows.snapshot_provider_unavailable','RH-MTV-2.12-PREREQ','Required VMware Snapshot Provider service is unavailable for warm Windows migration.','OS.WINDOWS_SNAPSHOT_PROVIDER'),
('MTV-022','VM name violates Kubernetes DNS naming','Plan/Validation','plan_validation','plan.vm_name.invalid_dns','RH-MTV-2.12-ERRORS','Source VM name fails target Kubernetes DNS naming validation.','PLAN.VM_NAME'),
('MTV-023','Migration plan exceeds 500 VM limit','Plan/Scale','plan_validation','plan.vm_count.exceeded','RH-MTV-2.12-PLAN-PDF','Migration plan contains more than the documented maximum VM count.','PLAN.SCALE_LIMIT'),
('MTV-024','Migration plan exceeds 500 disk limit','Plan/Scale','plan_validation','plan.disk_count.exceeded','RH-MTV-2.12-PLAN-PDF','Migration plan contains more than the documented maximum disk count.','PLAN.SCALE_LIMIT'),
('MTV-025','Guest-initiated iSCSI connection present','Plan/Guest storage','preflight','guest.iscsi_connection_present','RH-MTV-2.12-PLAN-VMWARE','VM uses guest-initiated iSCSI storage requiring planning/reconfiguration.','GUEST.STORAGE_DEPENDENCY'),
('MTV-026','Guest-initiated NFS mount present','Plan/Guest storage','preflight','guest.nfs_mount_present','RH-MTV-2.12-PLAN-VMWARE','VM uses guest-initiated NFS storage requiring planning/reconfiguration.','GUEST.STORAGE_DEPENDENCY'),
('MTV-027','UDN overlaps VMware provider IP','Network/UDN','plan_validation','network.udn.provider_ip_overlap','RH-MTV-2.12-PLAN-VMWARE','Provider IP falls inside the UDN subnet and migration fails.','NETWORK.UDN_ADDRESSING'),
('MTV-028','Transfer network absent from target project','Network/Transfer','plan_validation','network.transfer_network_missing','RH-MTV-2.12-PLAN-VMWARE','Selected transfer network is not present in the target project.','NETWORK.TRANSFER_NETWORK'),
('MTV-029','Transfer network MTU mismatch','Network/MTU','transfer','network.transfer.mtu_mismatch','RH-MTV-2.12-PLAN-VMWARE','VMware migration network and OpenShift transfer network use inconsistent MTU.','NETWORK.MTU'),
('MTV-030','Migration network bandwidth insufficient','Network/Performance','transfer','network.transfer.bandwidth_insufficient','RH-MTV-2.12-PLAN-VMWARE','Migration network does not meet the documented throughput guidance.','NETWORK.TRANSFER_CAPACITY'),
('MTV-031','Transfer NAD gateway/route missing','Network/NAD','transfer','network.transfer_nad_route_missing','RH-MTV-2.12-PLAN-VMWARE','Transfer NAD lacks the documented gateway route configuration.','NETWORK.NAD_ROUTE'),
('MTV-032','WaitForFirstConsumer PVC stall','Storage/CSI','AllocateDisks','storage.storageclass.wait_for_first_consumer','RH-MTV-2.12-RELEASE','WaitForFirstConsumer behavior leaves migration PVCs pending in the documented scenario.','STORAGE.BINDING_MODE'),
('MTV-033','CSI import PVC remains Pending','Storage/CSI','CopyDisks','storage.csi_import_pvc.pending','RH-MTV-2.12-RELEASE','CSI import PVC remains pending during CopyDisks.','STORAGE.CSI_PROVISIONING'),
('MTV-034','PVC provisioning timeout','Storage/CSI','CopyDisks','storage.csi.provisioning_timeout','RH-MTV-2.12-ERRORS','Target PVC remains pending and CSI provisioning times out.','STORAGE.CSI_PROVISIONING'),
('MTV-035','EXT4 disk resize failure','Storage/Filesystem','conversion','disk.ext4.resize_failure','RH-MTV-2.12-ERRORS','Target block PVC cannot be resized to the required size because EXT4 overhead is underestimated.','DISK.EXT4_SIZE'),
('MTV-036','Storage mapping points to unavailable target storage','Storage/Mapping','plan_validation','storage.mapping.target_unavailable','RH-MTV-2.12-PLAN-VMWARE','Selected target storage mapping cannot be used by the target environment.','STORAGE.MAPPING'),
('MTV-037','Storage map missing for plan','Storage/Mapping','plan_validation','storage.mapping.missing','RH-MTV-2.12-PLAN-VMWARE','Plan references storage mapping information that is not available.','STORAGE.MAPPING'),
('MTV-038','Target StorageClass unavailable','Storage/CSI','AllocateDisks','storage.storageclass.unavailable','RH-MTV-2.12-UNDERSTANDING','Migration cannot create target PVCs using the selected storage class.','STORAGE.STORAGECLASS'),
('MTV-039','PVC cannot bind to target PV','Storage/CSI','AllocateDisks','storage.pvc.binding_failed','RH-MTV-2.12-UNDERSTANDING','PVC remains unbound during the DataVolume/PVC allocation workflow.','STORAGE.PVC_BINDING'),
('MTV-040','DataVolume allocation/import path stalls','Storage/CDI','AllocateDisks','storage.datavolume.pending','RH-MTV-2.12-UNDERSTANDING','DataVolume remains pending and blocks the migration workflow.','STORAGE.DATAVOLUME'),
('MTV-041','Importer cannot stream disk to PV','Storage/CDI','CopyDisks','storage.cdi.importer_failed','RH-MTV-2.12-UNDERSTANDING','CDI importer cannot complete the disk transfer to the target PV.','STORAGE.CDI_IMPORT'),
('MTV-042','Disk transfer stalls','Storage/Transfer','CopyDisks','disk.transfer.stalled','RH-MTV-2.12-RELEASE','Disk transfer makes no progress while migration remains active.','DISK.TRANSFER_PROGRESS'),
('MTV-043','Source disk larger than target capacity','Storage/Disk','AllocateDisks','disk.target_capacity_insufficient','RH-MTV-2.12-ERRORS','Target disk/PVC capacity cannot accommodate the source disk.','DISK.TARGET_CAPACITY'),
('MTV-044','virt-v2v conversion pod cannot start due to resources','Conversion/Resources','conversion','conversion.pod.resource_insufficient','RH-MTV-2.12-PREREQ','Conversion resources are insufficient for the migration workload.','RESOURCES.CONVERSION_POD'),
('MTV-045','virt-v2v appliance OOM','Conversion/Resources','conversion','conversion.virt_v2v.oom','RH-MTV-2.12-RELEASE-PDF','virt-v2v appliance runs out of memory during conversion.','RESOURCES.VIRT_V2V_MEMORY'),
('MTV-046','XFS v4 conversion failure with unsupported virt-v2v parameters','Conversion/XFS','conversion','conversion.xfs_v4.parameter_incompatibility','RH-MTV-2.12-RELEASE-PDF','XFS v4 conversion fails when unsupported virt-v2v memory/SMP parameters are used with xfsCompatibility.','CONVERSION.XFS_V4'),
('MTV-047','Unsupported guest OS for virt-v2v','Conversion/OS','conversion','conversion.guest.unsupported','RH-MTV-2.12-PREREQ','Guest OS is outside supported conversion coverage.','CONVERSION.GUEST_COMPATIBILITY'),
('MTV-048','Unsupported filesystem requires raw copy consideration','Conversion/Filesystem','preflight','conversion.filesystem.unsupported','RH-MTV-2.12-UNDERSTANDING','Guest filesystem is not compatible with normal virt-v2v conversion path.','CONVERSION.FILESYSTEM_COMPATIBILITY'),
('MTV-049','Unsupported macOS guest','Conversion/OS','preflight','conversion.guest.macos_unsupported','RH-MTV-2.12-UNDERSTANDING','virt-v2v does not support macOS guest conversion.','CONVERSION.GUEST_COMPATIBILITY'),
('MTV-050','Uncommon encryption without accessible keys','Conversion/Encryption','preflight','conversion.encryption.keys_unavailable','RH-MTV-2.12-UNDERSTANDING','Guest uses encryption for which conversion lacks required key access.','CONVERSION.ENCRYPTION'),
('MTV-051','Dual-boot guest root disk ambiguity','Conversion/Boot','inspection','conversion.dualboot.root_device_ambiguous','RH-MTV-2.12-SOFTWARE','Dual-boot VM requires explicit root-device handling.','CONVERSION.DUAL_BOOT'),
('MTV-052','RHEL GRUB/bootloader conversion issue','Conversion/Linux','conversion','conversion.rhel.grub_failure','RH-MTV-2.12-UNDERSTANDING','Guest bootloader configuration cannot be converted cleanly to the target.','CONVERSION.BOOTLOADER'),
('MTV-053','RHEL fstab conversion/inspection issue','Conversion/Linux','inspection','conversion.rhel.fstab_issue','RH-MTV-2.12-UNDERSTANDING','Guest filesystem mount configuration interferes with conversion/inspection.','CONVERSION.FSTAB'),
('MTV-054','SLES guest inspection failure','Conversion/Linux','inspection','conversion.sles.inspection_failed','RH-MTV-2.12-SOFTWARE','Guest inspection cannot establish a supported SLES conversion path.','CONVERSION.GUEST_INSPECTION'),
('MTV-055','Windows filesystem conversion issue','Conversion/Windows','inspection','conversion.windows.filesystem_issue','RH-MTV-2.12-SOFTWARE','Windows guest filesystem state prevents normal conversion.','CONVERSION.WINDOWS_FILESYSTEM'),
('MTV-056','Windows Secure Boot prevents destination boot','Guest/Windows','post_conversion','os.windows.secure_boot','RH-MTV-2.12-SOFTWARE','Secure Boot can prevent the migrated Windows VM from booting on the destination.','OS.WINDOWS_SECURE_BOOT'),
('MTV-057','Windows Measured Boot unsupported','Guest/Windows','preflight','os.windows.measured_boot','RH-MTV-2.12-SOFTWARE','Windows VM using Measured Boot cannot be migrated.','OS.WINDOWS_MEASURED_BOOT'),
('MTV-058','Package manager unavailable for qemu-guest-agent installation','Guest/Linux','post_migration','os.guest.qga_package_manager_unavailable','RH-MTV-2.12-COLD-WARM','MTV cannot automatically install qemu-guest-agent when the guest package manager is unavailable.','OS.GUEST_PACKAGE_MANAGER'),
('MTV-059','Source network settings changed after migration','Guest/Network','post_migration','os.network.interface_settings_changed','RH-MTV-2.12-RELEASE','Historical MTV issue where interface names/static IP settings changed after migration.','OS.NETWORK_CONFIGURATION'),
('MTV-060','Secondary static IP configuration not preserved','Guest/Windows','post_migration','os.windows.secondary_static_ip_lost','RH-MTV-2.12-RELEASE','Historical MTV issue involving validation of Windows IP configuration.','OS.WINDOWS_NETWORK_CONFIGURATION'),
('MTV-061','MTV infrastructure pod OOM','MTV/Platform','platform_health','mtv.infrastructure.forklift_cli_oom','RH-MTV-2.12-RELEASE-PDF','Historical forklift-cli-download OOM caused MTV infrastructure health degradation.','MTV.INFRASTRUCTURE_RESOURCE'),
('MTV-062','Migration queue/inflight limit causes queued work','MTV/Concurrency','scheduling','mtv.concurrency.inflight_limit','RH-MTV-2.12-RELEASE-PDF','Concurrency limits can queue migration plans/disks; the agent must distinguish queued from failed.','MTV.CONCURRENCY'),
('MTV-063','Large plan causes delayed migration start','MTV/Scale','scheduling','mtv.scale.large_plan_queue','RH-MTV-2.12-PLAN-VMWARE','Large plans can take time to start and should not be misclassified as failed.','MTV.SCALE_SCHEDULING'),
('MTV-064','More than 72 UDNs cause resource contention','Network/UDN','platform_health','network.udn.resource_contention','RH-MTV-2.12-RELEASE-PDF','Large simultaneous UDN creation can starve OVS/OVNK resources and cause node readiness problems.','NETWORK.UDN_RESOURCE_CONTENTION'),
('MTV-065','Migration network saturates source-side bandwidth','Network/Performance','transfer','network.source_bandwidth_saturation','RH-MTV-2.12-PLAN-VMWARE','Default/insufficient migration network bandwidth can negatively affect source performance.','NETWORK.BANDWIDTH_SATURATION'),
('MTV-066','Transfer network gateway unreachable','Network/Transfer','transfer','network.transfer_gateway_unreachable','RH-MTV-2.12-PLAN-VMWARE','Transfer network requires gateway configuration so the migration interface can reach the configured gateway.','NETWORK.TRANSFER_GATEWAY'),
('MTV-067','Network map source/target provider mismatch','Plan/Network mapping','plan_validation','plan.network_map_provider_mismatch','RH-MTV-2.12-PLAN-VMWARE','Existing network map belongs to different source/target providers than the migration plan.','PLAN.NETWORK_MAP'),
('MTV-068','Network mapping target unavailable','Plan/Network mapping','plan_validation','plan.network_mapping.target_unavailable','RH-MTV-2.12-PLAN-VMWARE','Selected target network mapping is not available in the destination environment.','PLAN.NETWORK_MAPPING'),
('MTV-069','Storage mapping source/target mismatch','Plan/Storage mapping','plan_validation','plan.storage_map_provider_mismatch','RH-MTV-2.12-PLAN-VMWARE','Storage mapping does not correspond to the selected provider pair.','PLAN.STORAGE_MAP'),
('MTV-070','Pre-migration hook failure','Plan/Hooks','pre_migration_hook','plan.pre_hook.failed','RH-MTV-2.12-PLAN-PDF','Configured pre-migration hook fails and blocks the migration workflow.','PLAN.HOOK'),
('MTV-071','Post-migration hook failure','Plan/Hooks','post_migration_hook','plan.post_hook.failed','RH-MTV-2.12-PLAN-PDF','Configured post-migration hook fails after migration work.','PLAN.HOOK'),
('MTV-072','VM provider IP inside UDN subnet','Network/UDN','plan_validation','network.udn.provider_ip_conflict','RH-MTV-2.12-PLAN-VMWARE','Provider IP overlaps UDN subnet, which causes migration failure.','NETWORK.UDN_ADDRESSING'),
('MTV-073','Default transfer network misconfigured','Network/Transfer','plan_validation','network.default_transfer_network.invalid','RH-MTV-2.12-PLAN-VMWARE','Provider default transfer network does not provide the required migration path.','NETWORK.TRANSFER_NETWORK'),
('MTV-074','Target NAD missing for additional network','Network/Multus','plan_validation','network.additional_nad_missing','RH-MTV-2.12-PLAN-VMWARE','Additional destination network requires a corresponding NAD.','NETWORK.MULTUS_NAD'),
('MTV-075','Migration provider CA validation failure','VMware/TLS','provider_validation','vmware.provider.ca_validation_failed','RH-MTV-2.12-PREREQ','Provider certificate validation fails because the configured trust model is invalid.','VMWARE.TLS_CONFIGURATION'),
('MTV-076','Migration provider credential validation failure','VMware/Credentials','provider_validation','vmware.provider.credentials_invalid','RH-MTV-2.12-PREREQ','Configured VMware credentials fail provider validation.','VMWARE.AUTHENTICATION'),
('MTV-077','Target project does not exist/incorrectly selected','Plan/Target','plan_validation','plan.target_project.invalid','RH-MTV-2.12-PLAN-VMWARE','Migration plan target project is invalid or not usable for the plan.','PLAN.TARGET_PROJECT'),
('MTV-078','Target VM name collision/renaming behavior','Plan/VM naming','plan_validation','plan.vm_name.collision_or_rename','RH-MTV-2.12-ERRORS','Noncompliant source names require target naming normalization and must be tracked.','PLAN.VM_NAME'),
('MTV-079','Source/target software compatibility mismatch','Compatibility','preflight','compatibility.software_matrix_mismatch','RH-MTV-2.12-PREREQ','Source/destination software versions are outside the supported MTV compatibility matrix.','COMPATIBILITY.SOFTWARE_MATRIX'),
('MTV-080','Migration failure with unknown mechanism','Cross-cutting/Unknown','diagnosis','unknown.migration_failure','RH-MTV-2.12-ERRORS','Migration failure is confirmed but available evidence does not identify a supported mechanism.','UNKNOWN')
]

cases=[]
for row in raw:
    i,title,category,phase,code,sid,sig,mech=row
    cases.append({
        'id':i,'title':title,'category':category,'phase':phase,'failure_code':code,
        'source_id':sid,'source_url':sources[sid],
        'source_backed_signature':sig,'expected_mechanism':mech,
        'authoritative_source_type':'documentation',
        'source_status':'VERIFIED',
        'synthetic_fixture_policy':'synthetic_test_record',
        'evaluation_contract':{
            'classification':'deterministic_or_rule_backed',
            'diagnosis':'LIKELY only when required authoritative evidence supports mechanism; otherwise INSUFFICIENT_EVIDENCE',
            'root_cause_default':'UNKNOWN',
            'must_not':['invent infrastructure evidence','treat memory as current evidence','execute remediation']
        }
    })

out={
'version':'2.9.0',
'status':'SOURCE_GROUNDED_80_SCENARIO_CORPUS',
'description':'Eighty MTV migration failure and preflight/post-migration diagnostic scenarios grounded in current Red Hat MTV 2.12 documentation/release notes. They are not claims of customer incidents. Synthetic fixtures derived from them must be labeled synthetic_test_record.',
'source_policy':{'preferred_domains':['docs.redhat.com'],'required_fields':['source_id','source_url','category','phase','failure_code','source_backed_signature','source_status'],'synthetic_record_label':'synthetic_test_record'},
'sources':[{ 'source_id':k,'source_url':v} for k,v in sources.items()],
'cases':cases,
'matrix':{'conditions':['NONE','SRE_ONLY','RHOKP_ONLY','BOTH','CONFLICT','INSUFFICIENT'],'planned_runs':480},
'notes':[
'Verified means the failure condition or migration constraint is explicitly grounded in the cited Red Hat documentation/release notes; it does not mean the exact synthetic log string is copied from Red Hat.',
'Customer Portal solution content must not be invented. Public documentation is used as the authoritative corpus anchor.',
'Historical/fixed issues are valid diagnostic scenarios because the agent must recognize known signatures and version applicability.',
'Each scenario needs deterministic evidence contracts, policy/skill, fixtures and expected-output assertions before its matrix rows can become executable PASS tests.'
]
}
(ROOT/'datasets/mtv_80_scenario_corpus.yaml').write_text(yaml.safe_dump(out,sort_keys=False),encoding='utf-8')

matrix={
'version':'2.9.0','corpus':'datasets/mtv_80_scenario_corpus.yaml',
'conditions':[
 {'id':'NONE','memory_mode':'none'}, {'id':'SRE_ONLY','memory_mode':'sre'}, {'id':'RHOKP_ONLY','memory_mode':'rhokp'},
 {'id':'BOTH','memory_mode':'both'}, {'id':'CONFLICT','memory_mode':'both','mutation':'conflicting_historical_resolution'},
 {'id':'INSUFFICIENT','memory_mode':'both','mutation':'remove_required_authoritative_evidence'}],
'minimum_invariants':['current_evidence_dominates_memory','no_root_cause_without_required_evidence','no_remediation_execution','recommendation_distinct_from_execution','source_backed_case_has_source_id','synthetic_fixture_marked','duplicate_event_idempotent','kafka_result_preserves_event_identity','unknown_capability_status_is_not_authoritative_evidence'],
'phases':{'source_validation':'80/80 have source provenance','contract_design':'80/80 have evidence/diagnosis/evaluation contract','implementation':'policy + skill + fixture + deterministic expected output','matrix_execution':'480/480 executable evaluations','capability_failure':'explicit ERROR/UNAVAILABLE/NO_DATA/UNKNOWN coverage','outcome_validation':'diagnosis versus later verified outcome'},
'execution_scale':{'corpus_cases':80,'conditions':6,'planned_case_condition_runs':480,'additional_kafka_idempotency_runs':80,'additional_capability_failure_runs':80,'additional_schema_validation_runs':80},
'status_policy':{'PASS':'all assertions pass','FAIL':'implemented scenario violates an invariant','NOT_IMPLEMENTED':'scenario contract exists but deterministic policy/skill/fixture is not yet implemented','BLOCKED':'required integration unavailable'},
'release_gates':['Do not report 480/480 PASS until all 80 scenarios have deterministic policies, skills, fixtures and expected outputs.','NOT_IMPLEMENTED is not PASS.','Source URLs provide provenance; runtime evidence must come from approved capabilities.']}
(ROOT/'datasets/v29_80_evaluation_matrix.yaml').write_text(yaml.safe_dump(matrix,sort_keys=False),encoding='utf-8')

# replace runner with honest 480 planner
script='''#!/usr/bin/env python3\nfrom pathlib import Path\nimport argparse,yaml\nROOT=Path(__file__).resolve().parents[1]\nCONDITIONS=["NONE","SRE_ONLY","RHOKP_ONLY","BOTH","CONFLICT","INSUFFICIENT"]\n# Existing executable mappings are intentionally small. Add only after policy+skill+fixture+expected-output tests exist.\nIMPLEMENTED={"storage.csi.provisioning_timeout":"storage-csi-controller-error","network.destination_nad_missing":"network-nad-missing","vmware.cbt.retry_limit":"vmware-cbt-retry"}\ndef main():\n    ap=argparse.ArgumentParser(); ap.add_argument("--status",choices=["all","implemented","not-implemented"],default="all"); args=ap.parse_args()\n    corpus=yaml.safe_load((ROOT/"datasets/mtv_80_scenario_corpus.yaml").read_text()); cases=corpus["cases"]\n    rows=[]\n    for c in cases:\n        impl=IMPLEMENTED.get(c["failure_code"])\n        for cond in CONDITIONS:\n            st="IMPLEMENTED" if impl else "NOT_IMPLEMENTED"\n            if args.status=="all" or (args.status=="implemented" and impl) or (args.status=="not-implemented" and not impl): rows.append((c,cond,st,impl))\n    print("=== V2.9.0 80-SCENARIO MTV MATRIX PLAN ===")\n    print(f"Corpus cases : {len(cases)}")\n    print(f"Conditions   : {len(CONDITIONS)}")\n    print(f"Planned runs : {len(cases)*len(CONDITIONS)}")\n    implemented=sum(1 for c in cases if c["failure_code"] in IMPLEMENTED)\n    print(f"Implemented  : {implemented*len(CONDITIONS)}")\n    print(f"Not ready    : {(len(cases)-implemented)*len(CONDITIONS)}")\n    print(f"Source status: {sum(1 for c in cases if c.get('source_status')=='VERIFIED')}/{len(cases)} VERIFIED")\n    print()\n    for c,cond,st,impl in rows: print(f"{c['id']} | {cond:<12} | {st:<15} | {c['failure_code']}")\nif __name__=="__main__": main()\n'''
(ROOT/'scripts/run_v29_80_matrix.py').write_text(script,encoding='utf-8')

release='''# v2.9.0 80-Scenario MTV Diagnostic Corpus\n\n## What changed\n- Expanded the MTV corpus from 20 to 80 source-grounded scenarios.\n- Added source provenance for every scenario using current Red Hat MTV 2.12 documentation/release notes.\n- Added deterministic evaluation-contract placeholders to every scenario.\n- Expanded the matrix from 120 planned case/condition runs to 480.\n- Preserved the six conditions: NONE, SRE_ONLY, RHOKP_ONLY, BOTH, CONFLICT, INSUFFICIENT.\n- Added explicit gates: NOT_IMPLEMENTED is not PASS.\n\n## Important\nThe corpus is source-grounded, not a claim that every listed condition has occurred in the user's organization. Synthetic fixtures must be marked `synthetic_test_record`.\n\n## Current implementation\nOnly the previously implemented deterministic scenarios are executable today. The 80-case manifest is the benchmark contract. Do not manufacture 480 green tests by treating planned rows as passing.\n\n## Target\n80 scenarios × 6 conditions = 480 semantic evaluations.\n\n## Categories\nVMware/CBT, VMware connectivity, VDDK, Windows warm migration, plan validation, network/UDN/NAD, storage/CSI/CDI, disk/EXT4, virt-v2v/conversion, Linux/Windows guest compatibility, MTV platform/concurrency, compatibility and unknown-failure handling.\n'''
(ROOT/'V2.9.0_RELEASE_NOTES.md').write_text(release,encoding='utf-8')

readme='''# v2.9.0 80-Scenario Corpus\n\nRun the matrix plan:\n\n```bash\npython scripts/run_v29_80_matrix.py\n```\n\nThis reports the full 480-row plan and clearly separates implemented from not implemented.\n\nDo not call the 480 planned rows tests until each scenario has a deterministic policy, skill, fixture, evidence contract, expected diagnosis and semantic assertions.\n'''
(ROOT/'V2.9.0_80_CORPUS_README.md').write_text(readme,encoding='utf-8')
print('created',len(cases),'cases')
