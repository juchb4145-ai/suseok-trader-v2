from __future__ import annotations

from domain.strategy.models import StrategyCandidateContext
from domain.strategy.setup import StrategySetupType
from domain.strategy.status import StrategyObservationStatus
from services.config import Settings
from services.strategy_engine import (
    evaluate_breakout_retest,
    evaluate_theme_follower_expansion,
    evaluate_theme_leader_pullback,
    evaluate_vwap_reclaim,
)


def test_theme_leader_pullback_matches_valid_pullback_range() -> None:
    observation = evaluate_theme_leader_pullback(_context(), _settings())

    assert observation.setup_type is StrategySetupType.THEME_LEADER_PULLBACK
    assert observation.status is StrategyObservationStatus.MATCHED_OBSERVATION
    assert "PULLBACK_OBSERVED" in observation.reason_codes
    assert "SETUP_MATCHED" in observation.reason_codes


def test_theme_leader_pullback_handles_shallow_deep_and_missing_high() -> None:
    shallow = evaluate_theme_leader_pullback(
        _context(price=99.9, day_high=100.0),
        _settings(strategy_pullback_min_pct=0.3),
    )
    deep = evaluate_theme_leader_pullback(
        _context(price=90.0, day_high=100.0),
        _settings(strategy_pullback_max_pct=5.0),
    )
    missing = evaluate_theme_leader_pullback(_context(day_high=None), _settings())

    assert shallow.status is StrategyObservationStatus.WATCH
    assert "PULLBACK_TOO_SHALLOW" in shallow.reason_codes
    assert deep.status is StrategyObservationStatus.NO_SETUP
    assert "PULLBACK_TOO_DEEP" in deep.reason_codes
    assert missing.status is StrategyObservationStatus.DATA_WAIT


def test_vwap_reclaim_matches_near_above_vwap_with_flow() -> None:
    observation = evaluate_vwap_reclaim(_context(price=100.5, vwap=100.0), _settings())

    assert observation.status is StrategyObservationStatus.MATCHED_OBSERVATION
    assert "VWAP_RECLAIM_OBSERVED" in observation.reason_codes
    assert "PRICE_ABOVE_VWAP" in observation.reason_codes


def test_vwap_reclaim_handles_required_missing_vwap_and_below_vwap() -> None:
    missing = evaluate_vwap_reclaim(
        _context(vwap=None),
        _settings(strategy_engine_require_vwap=True),
    )
    below = evaluate_vwap_reclaim(_context(price=97.0, vwap=100.0), _settings())

    assert missing.status is StrategyObservationStatus.DATA_WAIT
    assert "VWAP_MISSING" in missing.reason_codes
    assert below.status is StrategyObservationStatus.NO_SETUP
    assert "PRICE_BELOW_VWAP" in below.reason_codes


def test_breakout_retest_matches_near_day_high_and_waits_for_missing_high() -> None:
    matched = evaluate_breakout_retest(_context(price=99.0, day_high=100.0), _settings())
    missing = evaluate_breakout_retest(_context(day_high=None), _settings())

    assert matched.status is StrategyObservationStatus.MATCHED_OBSERVATION
    assert "BREAKOUT_RETEST_OBSERVED" in matched.reason_codes
    assert missing.status is StrategyObservationStatus.DATA_WAIT


def test_theme_follower_expansion_requires_follower_role_and_theme_strength() -> None:
    matched = evaluate_theme_follower_expansion(
        _context(theme_role="FOLLOWER_CANDIDATE"),
        _settings(),
    )
    leader = evaluate_theme_follower_expansion(
        _context(theme_role="LEADER_CANDIDATE"),
        _settings(),
    )
    weak = evaluate_theme_follower_expansion(
        _context(theme_role="FOLLOWER_CANDIDATE", rising_ratio=0.1),
        _settings(),
    )

    assert matched.status is StrategyObservationStatus.MATCHED_OBSERVATION
    assert "FOLLOWER_EXPANSION_OBSERVED" in matched.reason_codes
    assert leader.status is StrategyObservationStatus.NO_SETUP
    assert "THEME_ROLE_NOT_ALLOWED" in leader.reason_codes
    assert weak.status is StrategyObservationStatus.NO_SETUP


def _settings(**overrides) -> Settings:
    values = {
        "market_data_tick_stale_sec": 999_999_999,
        "market_data_degraded_tick_stale_sec": 999_999_999,
        "candidate_source_stale_sec": 999_999_999,
        "candidate_tick_stale_sec": 999_999_999,
        "candidate_episode_ttl_sec": 999_999_999,
        "strategy_engine_stale_tick_sec": 999_999_999,
    }
    values.update(overrides)
    return Settings(**values)


def _context(**overrides) -> StrategyCandidateContext:
    values = {
        "candidate_instance_id": "CAND-2026-06-27-005930-1",
        "trade_date": "2026-06-27",
        "code": "005930",
        "name": "삼성전자",
        "candidate_state": "CONTEXT_READY",
        "theme_id": "semiconductor",
        "theme_name": "반도체",
        "theme_state": "LEADING",
        "theme_role": "LEADER_CANDIDATE",
        "market_readiness_status": "FRESH",
        "tick_age_sec": 1.0,
        "price": 97.0,
        "change_rate": 2.0,
        "cumulative_trade_value": 100_000_000.0,
        "trade_value_delta_1m": 10_000_000.0,
        "trade_value_delta_3m": 20_000_000.0,
        "trade_value_delta_5m": 30_000_000.0,
        "day_high": 100.0,
        "day_low": 90.0,
        "vwap": 97.0,
        "above_vwap": True,
        "bar_1m_ready": True,
        "bar_3m_ready": True,
        "bar_5m_ready": True,
        "source_count": 1,
        "active_source_count": 1,
        "reason_codes": [],
        "raw_context": {"theme_latest_snapshot": {"rising_ratio": 0.5}},
    }
    if "rising_ratio" in overrides:
        values["raw_context"] = {
            "theme_latest_snapshot": {"rising_ratio": overrides.pop("rising_ratio")}
        }
    values.update(overrides)
    return StrategyCandidateContext(**values)
