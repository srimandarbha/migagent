"""Integration Chaos Suite: 5 Transport Certification Scenarios against live Kafka."""
import os
import pytest

from scripts.run_chaos_tests import (
    run_chaos_1_malformed_poison_pill,
    run_chaos_2_transient_infrastructure_outage,
    run_chaos_3_deterministic_poison_five_strikes,
    run_chaos_4_dlq_outage_resilience,
    run_chaos_5_rebalance_and_idempotent_replay,
)

pytestmark = pytest.mark.skipif(
    not os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    reason="Requires running Kafka broker (set KAFKA_BOOTSTRAP_SERVERS)",
)


def test_chaos_scenario_1_poison_pill():
    assert run_chaos_1_malformed_poison_pill() is True


def test_chaos_scenario_2_transient_db_outage_n2():
    assert run_chaos_2_transient_infrastructure_outage() is True


def test_chaos_scenario_3_poison_five_strikes():
    assert run_chaos_3_deterministic_poison_five_strikes() is True


def test_chaos_scenario_4_dlq_outage_n1():
    assert run_chaos_4_dlq_outage_resilience() is True


def test_chaos_scenario_5_idempotent_deduplication():
    assert run_chaos_5_rebalance_and_idempotent_replay() is True
