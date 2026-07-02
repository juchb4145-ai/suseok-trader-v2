from __future__ import annotations

from domain.oms.models import DryRunIntent
from domain.risk.models import RiskObservation
from domain.strategy.models import StrategyObservation
from services.risk_gate import get_latest_risk_observation, save_risk_observation
from services.strategy_engine import get_latest_strategy_observation, save_strategy_observation
from storage.sqlite import initialize_database


def test_strategy_observation_preserves_explicit_observe_only_value() -> None:
    observation = StrategyObservation(
        strategy_observation_id="strategy-observation-1",
        candidate_instance_id="candidate-1",
        trade_date="2026-06-27",
        code="005930",
        name="Samsung Electronics",
        evaluated_at="2026-06-27T00:00:00Z",
        overall_status="MATCHED_OBSERVATION",
        primary_setup_type=None,
        primary_setup_status=None,
        observe_only=False,
    )

    assert observation.observe_only is False
    assert observation.to_dict(include_setups=False)["observe_only"] is False


def test_strategy_observation_persistence_preserves_observe_only_value(tmp_path) -> None:
    connection = initialize_database(tmp_path / "strategy-observe-only.sqlite3")
    observation = StrategyObservation(
        strategy_observation_id="strategy-observation-1",
        candidate_instance_id="candidate-1",
        trade_date="2026-06-27",
        code="005930",
        name="Samsung Electronics",
        evaluated_at="2026-06-27T00:00:00Z",
        overall_status="MATCHED_OBSERVATION",
        primary_setup_type=None,
        primary_setup_status=None,
        observe_only=False,
    )

    save_strategy_observation(connection, observation)
    latest = get_latest_strategy_observation(connection, "candidate-1")
    stored = connection.execute(
        "SELECT observe_only FROM strategy_observations WHERE strategy_observation_id = ?",
        ("strategy-observation-1",),
    ).fetchone()
    connection.close()

    assert latest is not None
    assert latest["observe_only"] is False
    assert stored["observe_only"] == 0


def test_risk_observation_preserves_explicit_observe_only_value() -> None:
    observation = RiskObservation(
        risk_observation_id="risk-observation-1",
        candidate_instance_id="candidate-1",
        strategy_observation_id="strategy-observation-1",
        trade_date="2026-06-27",
        code="005930",
        name="Samsung Electronics",
        evaluated_at="2026-06-27T00:00:00Z",
        overall_status="OBSERVE_PASS",
        max_severity="INFO",
        observe_only=False,
    )

    assert observation.observe_only is False
    assert observation.to_dict(include_checks=False)["observe_only"] is False


def test_risk_observation_persistence_preserves_observe_only_value(tmp_path) -> None:
    connection = initialize_database(tmp_path / "risk-observe-only.sqlite3")
    observation = RiskObservation(
        risk_observation_id="risk-observation-1",
        candidate_instance_id="candidate-1",
        strategy_observation_id="strategy-observation-1",
        trade_date="2026-06-27",
        code="005930",
        name="Samsung Electronics",
        evaluated_at="2026-06-27T00:00:00Z",
        overall_status="OBSERVE_PASS",
        max_severity="INFO",
        observe_only=False,
    )

    save_risk_observation(connection, observation)
    latest = get_latest_risk_observation(connection, "candidate-1")
    stored = connection.execute(
        "SELECT observe_only FROM risk_observations WHERE risk_observation_id = ?",
        ("risk-observation-1",),
    ).fetchone()
    connection.close()

    assert latest is not None
    assert latest["observe_only"] is False
    assert stored["observe_only"] == 0


def test_dry_run_intent_preserves_observe_only_but_keeps_dry_run_safety_flags() -> None:
    intent = DryRunIntent(
        dry_run_intent_id="dry-run-intent-1",
        candidate_instance_id="candidate-1",
        strategy_observation_id="strategy-observation-1",
        risk_observation_id="risk-observation-1",
        trade_date="2026-06-27",
        code="005930",
        name="Samsung Electronics",
        observe_only=False,
    )
    restored = DryRunIntent.from_dict(intent.to_dict())

    assert intent.observe_only is False
    assert intent.to_dict()["observe_only"] is False
    assert restored.observe_only is False
    assert restored.dry_run_only is True
    assert restored.live_order_allowed is False
    assert restored.gateway_command_allowed is False
