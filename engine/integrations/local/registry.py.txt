from ..registry import InMemoryCapabilityRegistry
from .splunk import FixtureSplunkAdapter
from .prometheus import FixturePrometheusAdapter
from .rhokp import FixtureRHOKPRAGAdapter
from .dataset import load_common_dataset
from .tracker import FixtureSRETrackerAdapter

def build_local_registry(*, dataset_path=None):
    data=load_common_dataset(dataset_path)
    splunk=FixtureSplunkAdapter(data.get('splunk', []))
    prom=FixturePrometheusAdapter(data.get('prometheus', []))
    rag=FixtureRHOKPRAGAdapter(data.get('redhat_knowledge', []))
    tracker=FixtureSRETrackerAdapter(data.get('sre_tracker',{}).get('failure_cases',[]), data.get('sre_tracker',{}).get('periodic_patterns',[]))
    registry=InMemoryCapabilityRegistry({
        'observability.search': splunk.search,
        'metrics.query': prom.query,
        'knowledge.search': rag.search,
        'sre_tracker.search_history': tracker.search_failure_history,
        'sre_tracker.search_periodic': tracker.search_periodic_patterns,
    })
    return registry, tracker
