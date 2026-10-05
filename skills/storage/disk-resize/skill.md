# Disk Image Resize Failure Investigation

## Objective
Determine whether target disk resizing failed due to filesystem overhead or insufficient storage provisioned on target block storage.

## Required evidence
- Storage disk resize error log (`domain=storage`, `signal=disk_resize_error`).
- Target volume capacity (`domain=storage`, `signal=volume_capacity`).
- Migration state (`domain=mtv`, `signal=migration_state`).

## Investigation procedure
1. Confirm error `unable to resize disk image` or `qemu-img resize failed`.
2. Compare source virtual disk size with provisioned target PVC capacity and filesystem overhead.
3. Recommend expanding target PVC allocation or adjusting storage overhead percentage.
