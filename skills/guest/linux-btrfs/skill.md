# Linux Btrfs Unsupported Filesystem Investigation

## Objective
Determine whether virt-v2v conversion failed because the source Linux VM contains an unsupported Btrfs filesystem.

## Required evidence
- virt-v2v log (`domain=conversion`, `signal=virt_v2v_log`).
- Guest OS filesystem type (`domain=guest_os`, `signal=filesystem_type`).
- Migration state (`domain=mtv`, `signal=migration_state`).

## Investigation procedure
1. Confirm error `unknown filesystem type btrfs` in virt-v2v output.
2. Verify partition and filesystem layout on source VM.
3. Recommend converting root/data partitions to supported filesystems (ext4/xfs) or using cold disk copy.
