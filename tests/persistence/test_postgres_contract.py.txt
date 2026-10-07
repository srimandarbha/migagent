import os
import pytest

psycopg = pytest.importorskip('psycopg')
from persistence.repository import SRETrackerRepository

@pytest.mark.integration
def test_schema_and_idempotent_failure_case():
    dsn=os.getenv('DATABASE_URL')
    if not dsn:
        pytest.skip('DATABASE_URL not configured')
    repo=SRETrackerRepository(dsn)
    repo.migrate()
    a=repo.create_or_get_failure_case(event_id='contract-event-1',migration_id='mig-1',vm_id='vm-1',cluster_id='ocv-1',change_id=None,failure_class='UNKNOWN',failure_code='UNKNOWN',agent_version='test',policy_version='1')
    b=repo.create_or_get_failure_case(event_id='contract-event-1',migration_id='mig-1',vm_id='vm-1',cluster_id='ocv-1',change_id=None,failure_class='UNKNOWN',failure_code='UNKNOWN',agent_version='test',policy_version='1')
    assert a == b
