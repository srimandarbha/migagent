from pathlib import Path
from engine.integrations.local.dataset import load_common_dataset
from engine.integrations.local.registry import build_local_registry

def test_common_dataset_wires_all_local_adapters():
    data=load_common_dataset()
    assert data['redhat_knowledge'] and data['sre_tracker']['failure_cases']
    registry, tracker=build_local_registry()
    assert registry.invoke('observability.search', {'domain':'storage','signal':'backend_health'})['evidence']
    assert registry.invoke('metrics.query', {'domain':'ocv'})['evidence']
    assert registry.invoke('knowledge.search', {'query':'OpenShift Virtualization migration'})['documents']
    assert tracker.search_failure_history(failure_class='STORAGE.CSI')
