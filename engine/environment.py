from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import Any, Optional

@dataclass
class EnvironmentFingerprint:
    cluster_id: str | None = None
    ocp_version: str | None = None
    ocv_version: str | None = None
    mtv_version: str | None = None
    source_provider: str | None = None
    source_provider_version: str | None = None
    source_host_versions: list[str] | None = None
    migration_type: str | None = None
    observed_at: str | None = None
    provenance: dict[str, Any] | None = None

    def to_dict(self):
        return asdict(self)

def _first(*values):
    for value in values:
        if value not in (None, ""):
            return value
    return None

def from_event(event: dict[str, Any]) -> EnvironmentFingerprint:
    target = event.get("target", {}) or {}
    source = event.get("source", {}) or {}
    env = event.get("environment", {}) or {}
    target = {**env.get("target", {}), **target}
    source = {**env.get("source", {}), **source}
    return EnvironmentFingerprint(
        cluster_id=_first(event.get("cluster_id"), target.get("cluster_id")),
        ocp_version=_first(event.get("ocp_version"), target.get("ocp_version"), target.get("openshift_version")),
        ocv_version=_first(event.get("ocv_version"), target.get("ocv_version"), target.get("openshift_virtualization_version")),
        mtv_version=_first(event.get("mtv_version"), target.get("mtv_version")),
        source_provider=_first(event.get("source_provider"), source.get("provider")),
        source_provider_version=_first(event.get("source_provider_version"), source.get("provider_version"), source.get("vcenter_version")),
        source_host_versions=event.get("source_host_versions", source.get("host_versions", source.get("esxi_versions", []))) or [],
        migration_type=_first(event.get("migration_type"), event.get("migration", {}).get("type"), env.get("migration", {}).get("type")),
        observed_at=_first(event.get("event_time"), event.get("observed_at")),
        provenance={"source": "event"},
    )

def resolve(registry, event: dict[str, Any]) -> EnvironmentFingerprint:
    """Resolve environment from a deterministic capability when available, else event metadata.

    The fallback is intentional for the local harness. Production should provide
    environment.get_fingerprint through the capability registry.
    """
    try:
        result = registry.invoke("environment.get_fingerprint", {"cluster_id": event.get("cluster_id"), "vm_id": event.get("vm_id"), "migration_id": event.get("migration_id")}, access="read", agent="migration-failure-agent")
        data = result.get("environment", result) if isinstance(result, dict) else {}
        base = from_event(event)
        merged = {**base.to_dict(), **{k: v for k, v in data.items() if v not in (None, "")}}
        merged["provenance"] = {"source": "environment.get_fingerprint"}
        return EnvironmentFingerprint(**{k: merged.get(k) for k in EnvironmentFingerprint.__dataclass_fields__})
    except Exception:
        return from_event(event)
