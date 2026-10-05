# VMware VMDK Not Found Investigation

## Objective
Determine whether migration transfer failed because the disk file or flat-vmdk URL could not be located on the source datastore.

## Required evidence
- VMware datastore VMDK query (`domain=vmware`, `signal=datastore_vmdk`).
- Migration state (`domain=mtv`, `signal=migration_state`).

## Investigation procedure
1. Confirm error `url not found: *-flat.vmdk` or datastore 404 in transfer logs.
2. Verify virtual disk inventory mapping and snapshot disk descriptor chains.
3. Recommend consolidating snapshots or refreshing vCenter inventory.
