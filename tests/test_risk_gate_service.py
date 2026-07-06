from __future__ import annotations

import json

from domain.broker.utils import datetime_to_wire, utc_now
from domain.candidate.state import CandidateState
from domain.live_sim.status import LiveSimOrderStatus
from domain.risk.category import RiskCategory
from domain.risk.models import RiskInputContext
from domain.risk.reasons import RiskReasonCode
from domain.risk.status import RiskCheckStatus, RiskObservationStatus, RiskSeverity
from services.config import Settings, TradingMode
from services.risk_gate import (
    check_account_limits,
    check_chase_overheat,
    check_duplicate_cooldown,
    check_liquidity_spread,
    check_market_regime,
    check_strategy_context,
    check_theme_context,
    evaluate_risk_for_candidate,
    evaluate_risk_observations,
    get_latest_risk_observation,
    list_risk_observations_for_candidate,
    load_risk_input_context,
    save_risk_observation,
)
from services.strategy_engine import evaluate_candidate_strategy, save_strategy_observation
from storage.sqlite import initialize_database
from tests.test_strategy_service import _insert_strategy_fixture


def test_load_risk_input_context_reads_strategy_candidate_market_and_theme(tmp_path) -> None:
    connection = initialize_database(tmp_path / "risk_context.sqlite3")
    settings = _settings()
    candidate_id = _insert_strategy_fixture(connection)
    strategy = evaluate_candidate_strategy(connection, candidate_id, settings=settings)
    save_strategy_observation(connection, strategy)

    context = load_risk_input_context(connection, candidate_id, settings=settings)
    connection.close()

    assert context.candidate_instance_id == candidate_id
    assert context.strategy_observation_id == strategy.strategy_observation_id
    assert context.candidate_state == CandidateState.CONTEXT_READY.value
    assert context.strategy_status == "MATCHED_OBSERVATION"
    assert context.theme_state == "LEADING"
    assert context.theme_fresh_coverage_ratio == 1.0
    assert context.price == 97_000
    assert context.vwap == 96_500
    assert context.raw_context["context_hash"]


def test_theme_context_uses_observable_coverage_for_risk_gate(tmp_path) -> None:
    connection = initialize_database(tmp_path / "risk_observable_theme.sqlite3")
    settings = _settings()
    candidate_id = _insert_strategy_fixture(connection)
    connection.execute(
        """
        UPDATE theme_latest_snapshots
        SET fresh_coverage_ratio = 0.1,
            observable_member_count = 5,
            observable_fresh_member_count = 3,
            observable_fresh_coverage_ratio = 0.6
        WHERE theme_id = 'theme-005930'
        """
    )
    strategy = evaluate_candidate_strategy(connection, candidate_id, settings=settings)
    save_strategy_observation(connection, strategy)

    context = load_risk_input_context(connection, candidate_id, settings=settings)
    check = check_theme_context(context, settings)
    connection.close()

    assert context.theme_fresh_coverage_ratio == 0.6
    assert check.status is RiskCheckStatus.PASS_OBSERVED
    assert RiskReasonCode.THEME_FRESH_COVERAGE_LOW.value not in check.reason_codes
    assert check.evidence_json["coverage_basis"] == "OBSERVABLE"
    assert check.evidence_json["full_fresh_coverage_ratio"] == 0.1
    assert check.evidence_json["observable_member_count"] == 5


def test_risk_observation_persistence_latest_and_checks_stays_observe_only(tmp_path) -> None:
    connection = initialize_database(tmp_path / "risk_persistence.sqlite3")
    settings = _settings()
    candidate_id = _insert_strategy_fixture(connection)
    strategy = evaluate_candidate_strategy(connection, candidate_id, settings=settings)
    save_strategy_observation(connection, strategy)

    observation = evaluate_risk_for_candidate(connection, candidate_id, settings=settings)
    save_risk_observation(connection, observation)
    latest = get_latest_risk_observation(connection, candidate_id, include_checks=True)
    history = list_risk_observations_for_candidate(connection, candidate_id)
    command_count = connection.execute("SELECT COUNT(*) AS count FROM gateway_commands").fetchone()[
        "count"
    ]
    connection.close()

    assert observation.overall_status is RiskObservationStatus.OBSERVE_PASS
    assert observation.observe_only is True
    assert latest is not None
    assert latest["risk_observation_id"] == observation.risk_observation_id
    assert latest["observe_only"] is True
    assert latest["check_observations"]
    assert history[0]["risk_observation_id"] == observation.risk_observation_id
    assert command_count == 0


def test_recent_observation_cooldown_is_info_only_and_does_not_block_pass(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "risk_recent_cooldown.sqlite3")
    settings = _settings(risk_gate_observation_cooldown_sec=600)
    candidate_id = _insert_strategy_fixture(connection)
    strategy = evaluate_candidate_strategy(connection, candidate_id, settings=settings)
    save_strategy_observation(connection, strategy)
    first = evaluate_risk_for_candidate(connection, candidate_id, settings=settings)
    save_risk_observation(connection, first)

    second = evaluate_risk_for_candidate(connection, candidate_id, settings=settings)
    duplicate_check = _check_by_category(second, RiskCategory.DUPLICATE_COOLDOWN.value)
    connection.close()

    assert first.overall_status is RiskObservationStatus.OBSERVE_PASS
    assert second.overall_status is RiskObservationStatus.OBSERVE_PASS
    assert duplicate_check.status is RiskCheckStatus.PASS_OBSERVED
    assert duplicate_check.severity is RiskSeverity.INFO
    assert RiskReasonCode.RECENT_OBSERVATION_COOLDOWN.value in duplicate_check.reason_codes
    assert duplicate_check.evidence_json["recent_observation"]["risk_observation_id"] == (
        first.risk_observation_id
    )


def test_account_limits_ignore_order_expired_before_dispatch_daily_budget(tmp_path) -> None:
    connection = initialize_database(tmp_path / "risk-expired-live-sim-budget.sqlite3")
    _insert_live_sim_order(
        connection,
        status=LiveSimOrderStatus.ORDER_EXPIRED.value,
        notional=97_000,
    )
    settings = _settings(
        trading_mode=TradingMode.LIVE_SIM,
        trading_allow_live_sim=True,
        live_sim_enabled=True,
        live_sim_kill_switch=False,
        live_sim_max_order_notional=300_000,
        live_sim_max_daily_notional=300_000,
        live_sim_max_daily_order_count=1,
    )

    check = check_account_limits(connection, _context(price=97_000), settings)
    connection.close()

    live_sim = check.evidence_json["live_sim"]
    assert check.status is RiskCheckStatus.PASS_OBSERVED
    assert live_sim["daily_order_count"] == 0
    assert live_sim["daily_notional"] == 0.0
    assert live_sim["projected_daily_notional"] == 291_000.0
    assert RiskReasonCode.DAILY_ORDER_LIMIT_EXCEEDED.value not in check.reason_codes
    assert RiskReasonCode.DAILY_NOTIONAL_LIMIT_EXCEEDED.value not in check.reason_codes


def test_cross_exchange_divergence_is_opt_in_caution_only(tmp_path) -> None:
    connection = initialize_database(tmp_path / "risk_cross_exchange.sqlite3")
    candidate_id = _insert_strategy_fixture(connection)
    strategy = evaluate_candidate_strategy(connection, candidate_id, settings=_settings())
    save_strategy_observation(connection, strategy)
    _insert_cross_exchange_observation(connection, divergence_bp=120.0)

    disabled = evaluate_risk_for_candidate(connection, candidate_id, settings=_settings())
    enabled = evaluate_risk_for_candidate(
        connection,
        candidate_id,
        settings=_settings(risk_cross_exchange_divergence_bp=50.0),
    )
    command_count = connection.execute("SELECT COUNT(*) AS count FROM gateway_commands").fetchone()[
        "count"
    ]
    connection.close()

    assert disabled.overall_status is RiskObservationStatus.OBSERVE_PASS
    assert RiskReasonCode.CROSS_EXCHANGE_DIVERGENCE.value not in disabled.reason_codes
    assert enabled.overall_status is RiskObservationStatus.OBSERVE_CAUTION
    assert RiskReasonCode.CROSS_EXCHANGE_DIVERGENCE.value in enabled.reason_codes
    assert command_count == 0


def test_data_quality_missing_tick_is_data_wait_with_block_check(tmp_path) -> None:
    connection = initialize_database(tmp_path / "risk_missing_tick.sqlite3")
    settings = _settings()
    candidate_id = _insert_strategy_fixture(connection)
    strategy = evaluate_candidate_strategy(connection, candidate_id, settings=settings)
    save_strategy_observation(connection, strategy)
    connection.execute("DELETE FROM market_ticks_latest WHERE code = '005930'")
    connection.execute(
        """
        UPDATE candidate_context_latest
        SET market_context_json = ?
        WHERE candidate_instance_id = ?
        """,
        (json.dumps({}), candidate_id),
    )

    observation = evaluate_risk_for_candidate(connection, candidate_id, settings=settings)
    data_check = _check_by_category(observation, RiskCategory.DATA_QUALITY.value)
    connection.close()

    assert data_check.status is RiskCheckStatus.BLOCK_OBSERVED
    assert RiskReasonCode.LATEST_TICK_MISSING.value in data_check.reason_codes
    assert observation.overall_status is RiskObservationStatus.DATA_WAIT


def test_strategy_context_statuses_are_observation_only() -> None:
    matched = check_strategy_context(
        _context(strategy_status="MATCHED_OBSERVATION"),
        _settings(),
    )
    forming = check_strategy_context(_context(strategy_status="FORMING"), _settings())
    no_setup = check_strategy_context(_context(strategy_status="NO_SETUP"), _settings())
    missing = check_strategy_context(
        _context(strategy_observation_id=None, strategy_status=None),
        _settings(),
    )

    assert matched.status is RiskCheckStatus.PASS_OBSERVED
    assert RiskReasonCode.OBSERVE_PASS_NOT_ORDER_APPROVAL.value in matched.reason_codes
    assert forming.status is RiskCheckStatus.CAUTION_OBSERVED
    assert no_setup.status is RiskCheckStatus.BLOCK_OBSERVED
    assert missing.status is RiskCheckStatus.DATA_WAIT


def test_market_regime_risk_policy_is_observe_only_and_quality_aware() -> None:
    settings = _settings()
    weak = check_market_regime(
        _context(
            market_regime_status="WEAK",
            market_regime_quality_status="FRESH",
            primary_index_code="KOSPI",
            raw_context={"market_regime": {"regime_status": "WEAK", "reason_codes": []}},
        ),
        settings,
    )
    risk_off = check_market_regime(
        _context(
            market_regime_status="RISK_OFF",
            market_regime_quality_status="FRESH",
            primary_index_code="KOSPI",
            primary_index_return_5m=-0.5,
            raw_context={
                "market_regime": {
                    "regime_status": "RISK_OFF",
                    "quality_status": "FRESH",
                    "reason_codes": ["PRIMARY_INDEX_RISK_OFF"],
                }
            },
        ),
        settings,
    )
    stale = check_market_regime(
        _context(
            market_regime_status="DATA_WAIT",
            market_regime_quality_status="STALE",
            primary_index_code="KOSPI",
            raw_context={
                "market_regime": {
                    "regime_status": "DATA_WAIT",
                    "quality_status": "STALE",
                    "reason_codes": ["MARKET_INDEX_STALE"],
                }
            },
        ),
        settings,
    )

    assert weak.status is RiskCheckStatus.CAUTION_OBSERVED
    assert RiskReasonCode.MARKET_REGIME_WEAK.value in weak.reason_codes
    assert risk_off.status is RiskCheckStatus.BLOCK_OBSERVED
    assert RiskReasonCode.PRIMARY_INDEX_RISK_OFF.value in risk_off.reason_codes
    assert stale.status is RiskCheckStatus.CAUTION_OBSERVED
    assert RiskReasonCode.MARKET_INDEX_STALE.value in stale.reason_codes


def test_theme_candidate_chase_and_liquidity_checks() -> None:
    settings = _settings(
        risk_gate_max_change_rate=10.0,
        risk_gate_max_spread_ticks=2,
        risk_gate_min_trade_value_delta_1m=100.0,
        risk_gate_min_cumulative_trade_value=1_000.0,
        risk_gate_min_execution_strength=100.0,
    )
    weak_theme = check_theme_context(
        _context(theme_state="WATCH", theme_fresh_coverage_ratio=0.1, theme_rising_ratio=0.1),
        settings,
    )
    chase = check_chase_overheat(
        _context(change_rate=20.0, price=100.0, day_high=100.0, vwap=80.0),
        settings,
    )
    liquidity = check_liquidity_spread(
        _context(
            spread_ticks=3,
            trade_value_delta_1m=0.0,
            cumulative_trade_value=0.0,
            execution_strength=50.0,
        ),
        settings,
    )

    assert weak_theme.status is RiskCheckStatus.CAUTION_OBSERVED
    assert RiskReasonCode.THEME_FRESH_COVERAGE_LOW.value in weak_theme.reason_codes
    assert chase.status is RiskCheckStatus.BLOCK_OBSERVED
    assert RiskReasonCode.CHANGE_RATE_OVERHEAT.value in chase.reason_codes
    assert RiskReasonCode.VI_DATA_UNAVAILABLE.value in chase.reason_codes
    assert liquidity.status is RiskCheckStatus.BLOCK_OBSERVED
    assert RiskReasonCode.SPREAD_TOO_WIDE.value in liquidity.reason_codes


def test_duplicate_cooldown_observes_without_mutating_candidate_or_strategy(tmp_path) -> None:
    connection = initialize_database(tmp_path / "risk_duplicate.sqlite3")
    settings = _settings(risk_gate_duplicate_active_candidate_limit=1)
    first_id = _insert_strategy_fixture(connection, candidate_id="CAND-2026-06-27-005930-1")
    second_id = "CAND-2026-06-27-005930-2"
    now = datetime_to_wire(utc_now())
    connection.execute(
        """
        INSERT INTO candidates (
            candidate_instance_id,
            trade_date,
            code,
            name,
            generation,
            state,
            previous_state,
            detected_at,
            last_seen_at,
            state_updated_at,
            closed_at,
            primary_source_type,
            primary_source_id,
            source_count,
            active_source_count,
            theme_id,
            theme_name,
            theme_state,
            theme_role,
            market_readiness_status,
            tick_age_sec,
            vwap_ready,
            bar_1m_ready,
            bar_3m_ready,
            bar_5m_ready,
            reason_codes_json,
            metadata_json
        )
        VALUES (?, '2026-06-27', '005930', '삼성전자', 2, 'CONTEXT_READY',
            NULL, ?, ?, ?, NULL, 'THEME_LEADER', 'theme-005930', 1, 1,
            'theme-005930', '반도체', 'LEADING', 'LEADER_CANDIDATE', 'FRESH',
            1.0, 1, 1, 1, 1, '[]', '{}')
        """,
        (second_id, now, now, now),
    )
    strategy = evaluate_candidate_strategy(connection, first_id, settings=settings)
    save_strategy_observation(connection, strategy)
    before_state = connection.execute(
        "SELECT state FROM candidates WHERE candidate_instance_id = ?",
        (first_id,),
    ).fetchone()["state"]

    check = check_duplicate_cooldown(
        connection,
        load_risk_input_context(connection, first_id, settings=settings),
        settings,
    )
    after_state = connection.execute(
        "SELECT state FROM candidates WHERE candidate_instance_id = ?",
        (first_id,),
    ).fetchone()["state"]
    second_state = connection.execute(
        "SELECT state FROM candidates WHERE candidate_instance_id = ?",
        (second_id,),
    ).fetchone()["state"]
    connection.close()

    assert check.status is RiskCheckStatus.CAUTION_OBSERVED
    assert RiskReasonCode.DUPLICATE_ACTIVE_CANDIDATE.value in check.reason_codes
    assert after_state == before_state
    assert second_state == CandidateState.CONTEXT_READY.value


def test_account_limits_block_when_dry_run_position_capacity_is_exhausted(tmp_path) -> None:
    connection = initialize_database(tmp_path / "risk_account_limits.sqlite3")
    settings = _settings(dry_run_max_active_positions=1)
    candidate_id = _insert_strategy_fixture(connection)
    strategy = evaluate_candidate_strategy(connection, candidate_id, settings=settings)
    save_strategy_observation(connection, strategy)
    _insert_dry_run_position(connection, candidate_id=candidate_id)

    observation = evaluate_risk_for_candidate(connection, candidate_id, settings=settings)
    check = _check_by_category(observation, RiskCategory.ACCOUNT_LIMITS.value)
    connection.close()

    assert check.status is RiskCheckStatus.BLOCK_OBSERVED
    assert observation.overall_status is RiskObservationStatus.OBSERVE_BLOCK
    assert RiskReasonCode.ACTIVE_POSITION_LIMIT_EXCEEDED.value in check.reason_codes
    assert RiskReasonCode.CODE_CONCENTRATION_LIMIT_EXCEEDED.value in check.reason_codes
    assert check.evidence_json["dry_run"]["active_position_count"] == 1
    assert check.evidence_json["dry_run"]["code_open_position_count"] == 1


def test_live_sim_daily_loss_limit_ignores_under_limit_loss(tmp_path) -> None:
    connection = initialize_database(tmp_path / "risk_live_sim_loss_under.sqlite3")
    settings = _settings(
        live_sim_kill_switch=False,
        live_sim_max_daily_loss=100_000,
    )
    candidate_id = _insert_strategy_fixture(connection)
    strategy = evaluate_candidate_strategy(connection, candidate_id, settings=settings)
    save_strategy_observation(connection, strategy)
    _insert_live_sim_position(
        connection,
        trade_date="2026-06-27",
        status="CLOSED",
        quantity=0,
        realized_pnl=-50_000,
    )

    observation = evaluate_risk_for_candidate(connection, candidate_id, settings=settings)
    check = _check_by_category(observation, RiskCategory.ACCOUNT_LIMITS.value)
    connection.close()

    assert check.status is RiskCheckStatus.PASS_OBSERVED
    assert RiskReasonCode.DAILY_LOSS_LIMIT_EXCEEDED.value not in check.reason_codes
    assert check.evidence_json["live_sim"]["daily_pnl"] == -50_000


def test_live_sim_daily_loss_limit_blocks_realized_loss(tmp_path) -> None:
    connection = initialize_database(tmp_path / "risk_live_sim_realized_loss.sqlite3")
    settings = _settings(
        live_sim_kill_switch=False,
        live_sim_max_daily_loss=100_000,
    )
    candidate_id = _insert_strategy_fixture(connection)
    strategy = evaluate_candidate_strategy(connection, candidate_id, settings=settings)
    save_strategy_observation(connection, strategy)
    _insert_live_sim_position(
        connection,
        trade_date="2026-06-27",
        status="CLOSED",
        quantity=0,
        realized_pnl=-120_000,
    )

    observation = evaluate_risk_for_candidate(connection, candidate_id, settings=settings)
    check = _check_by_category(observation, RiskCategory.ACCOUNT_LIMITS.value)
    connection.close()

    assert check.status is RiskCheckStatus.BLOCK_OBSERVED
    assert check.severity is RiskSeverity.CRITICAL
    assert observation.overall_status is RiskObservationStatus.OBSERVE_BLOCK
    assert RiskReasonCode.DAILY_LOSS_LIMIT_EXCEEDED.value in check.reason_codes
    assert check.evidence_json["live_sim"]["realized_pnl"] == -120_000
    assert check.evidence_json["live_sim"]["unrealized_pnl"] == 0
    assert check.evidence_json["live_sim"]["daily_pnl"] == -120_000


def test_live_sim_daily_loss_limit_blocks_unrealized_loss_in_open_positions(tmp_path) -> None:
    connection = initialize_database(tmp_path / "risk_live_sim_unrealized_loss.sqlite3")
    settings = _settings(
        live_sim_kill_switch=False,
        live_sim_max_active_positions=10,
        live_sim_position_allow_scale_in=True,
        live_sim_max_daily_loss_pct=30.0,
    )
    candidate_id = _insert_strategy_fixture(connection)
    strategy = evaluate_candidate_strategy(connection, candidate_id, settings=settings)
    save_strategy_observation(connection, strategy)
    _insert_live_sim_position(
        connection,
        trade_date="2026-06-27",
        status="OPEN",
        quantity=1,
        realized_pnl=-20_000,
        unrealized_pnl=-80_000,
    )

    observation = evaluate_risk_for_candidate(connection, candidate_id, settings=settings)
    check = _check_by_category(observation, RiskCategory.ACCOUNT_LIMITS.value)
    connection.close()

    assert check.status is RiskCheckStatus.BLOCK_OBSERVED
    assert check.severity is RiskSeverity.CRITICAL
    assert RiskReasonCode.DAILY_LOSS_LIMIT_EXCEEDED.value in check.reason_codes
    assert check.evidence_json["live_sim"]["realized_pnl"] == -20_000
    assert check.evidence_json["live_sim"]["unrealized_pnl"] == -80_000
    assert check.evidence_json["live_sim"]["daily_pnl"] == -100_000
    assert check.evidence_json["live_sim"]["effective_daily_loss_limit"] == 90_000


def test_batch_evaluation_records_counts_and_errors(tmp_path) -> None:
    connection = initialize_database(tmp_path / "risk_batch.sqlite3")
    settings = _settings()
    candidate_id = _insert_strategy_fixture(connection, candidate_id="CAND-2026-06-27-005930-1")
    strategy = evaluate_candidate_strategy(connection, candidate_id, settings=settings)
    save_strategy_observation(connection, strategy)
    connection.execute(
        """
        INSERT INTO strategy_observations_latest (
            candidate_instance_id,
            strategy_observation_id,
            trade_date,
            code,
            name,
            evaluated_at,
            overall_status,
            primary_setup_type,
            primary_setup_status,
            score,
            confidence,
            reason_codes_json,
            config_version,
            observe_only
        )
        VALUES ('missing-candidate', 'missing-strategy', '2026-06-27', '000660',
            'SK hynix', ?, 'MATCHED_OBSERVATION', NULL, NULL, 0, 0, '[]',
            'observe_v1', 1)
        """,
        (datetime_to_wire(utc_now()),),
    )

    result = evaluate_risk_observations(
        connection,
        trade_date="2026-06-27",
        strategy_status="MATCHED_OBSERVATION",
        settings=settings,
    )
    latest_count = connection.execute(
        "SELECT COUNT(*) AS count FROM risk_observations_latest"
    ).fetchone()["count"]
    error_count = connection.execute(
        "SELECT COUNT(*) AS count FROM risk_evaluation_errors"
    ).fetchone()["count"]
    connection.close()

    assert result.strategy_observation_count == 2
    assert result.evaluated_count == 1
    assert result.observe_pass_count == 1
    assert result.error_count == 1
    assert result.status == "COMPLETED_WITH_ERRORS"
    assert latest_count == 1
    assert error_count == 1


def test_overall_block_for_overheat_and_stale_for_candidate_state(tmp_path) -> None:
    connection = initialize_database(tmp_path / "risk_overall.sqlite3")
    overheat_settings = _settings(risk_gate_max_change_rate=1.0)
    candidate_id = _insert_strategy_fixture(connection)
    strategy = evaluate_candidate_strategy(connection, candidate_id, settings=overheat_settings)
    save_strategy_observation(connection, strategy)
    overheat = evaluate_risk_for_candidate(connection, candidate_id, settings=overheat_settings)

    stale_id = _insert_strategy_fixture(
        connection,
        candidate_id="CAND-2026-06-27-000660-1",
        code="000660",
        name="SK하이닉스",
        state=CandidateState.STALE.value,
    )
    stale_strategy = evaluate_candidate_strategy(connection, stale_id, settings=_settings())
    save_strategy_observation(connection, stale_strategy)
    stale = evaluate_risk_for_candidate(connection, stale_id, settings=_settings())
    connection.close()

    assert overheat.overall_status is RiskObservationStatus.OBSERVE_BLOCK
    assert stale.overall_status is RiskObservationStatus.STALE_CONTEXT


def _settings(**overrides) -> Settings:
    values = {
        "market_data_tick_stale_sec": 999_999_999,
        "market_data_degraded_tick_stale_sec": 999_999_999,
        "candidate_source_stale_sec": 999_999_999,
        "candidate_tick_stale_sec": 999_999_999,
        "candidate_episode_ttl_sec": 999_999_999,
        "strategy_engine_stale_tick_sec": 999_999_999,
        "risk_gate_stale_tick_sec": 999_999_999,
        "risk_gate_strategy_stale_sec": 999_999_999,
    }
    values.update(overrides)
    return Settings(**values)


def _context(**overrides) -> RiskInputContext:
    values = {
        "candidate_instance_id": "CAND-2026-06-27-005930-1",
        "strategy_observation_id": "strategy-observation-1",
        "trade_date": "2026-06-27",
        "code": "005930",
        "name": "Samsung",
        "candidate_state": CandidateState.CONTEXT_READY.value,
        "strategy_status": "MATCHED_OBSERVATION",
        "strategy_evaluated_at": datetime_to_wire(utc_now()),
        "theme_id": "theme-005930",
        "theme_name": "semiconductor",
        "theme_state": "LEADING",
        "theme_role": "LEADER_CANDIDATE",
        "theme_fresh_coverage_ratio": 1.0,
        "theme_rising_ratio": 0.6,
        "market_readiness_status": "FRESH",
        "tick_age_sec": 1.0,
        "price": 97_000,
        "change_rate": 2.0,
        "day_high": 100_000,
        "spread_ticks": 1,
        "cumulative_trade_value": 97_000_000,
        "trade_value_delta_1m": 10_000_000,
        "trade_value_delta_3m": 20_000_000,
        "execution_strength": 120.0,
        "vwap": 96_500,
        "above_vwap": True,
        "source_count": 1,
        "active_source_count": 1,
        "bar_1m_ready": True,
        "raw_context": {
            "candidate_missing": False,
            "theme_latest_snapshot": {"leading_code": "005930"},
        },
    }
    values.update(overrides)
    return RiskInputContext(**values)


def _check_by_category(observation, category: str):
    for check in observation.check_observations:
        if check.category.value == category:
            return check
    raise AssertionError(f"check category not found: {category}")


def _insert_live_sim_order(
    connection,
    *,
    status: str,
    notional: float,
) -> None:
    now = datetime_to_wire(utc_now())
    connection.execute(
        """
        INSERT INTO live_sim_orders (
            live_sim_order_id,
            live_sim_intent_id,
            trade_date,
            account_id,
            code,
            name,
            side,
            order_type,
            quantity,
            limit_price,
            notional,
            status,
            filled_quantity,
            remaining_quantity,
            idempotency_key,
            created_at
        )
        VALUES (
            'expired-before-dispatch',
            'intent-expired-before-dispatch',
            '2026-06-27',
            'SIM-12345678',
            '005930',
            '삼성전자',
            'BUY',
            'LIMIT',
            1,
            ?,
            ?,
            ?,
            0,
            0,
            'key-expired-before-dispatch',
            ?
        )
        """,
        (notional, notional, status, now),
    )
    connection.commit()


def _insert_cross_exchange_observation(
    connection,
    *,
    code: str = "005930",
    divergence_bp: float = 120.0,
) -> None:
    now = datetime_to_wire(utc_now())
    connection.execute(
        """
        INSERT INTO market_cross_exchange_observations (
            code,
            bucket_start,
            krx_last_price,
            nxt_last_price,
            divergence_bp,
            krx_volume,
            nxt_volume,
            krx_volume_share,
            nxt_volume_share,
            krx_tick_count,
            nxt_tick_count,
            total_tick_count,
            updated_at,
            metadata_json
        )
        VALUES (?, ?, 10000, 10120, ?, 100, 40, 0.7142857, 0.2857143, 1, 1, 2, ?, ?)
        """,
        (
            code,
            now,
            divergence_bp,
            now,
            json.dumps({"observe_only": True, "not_order_signal": True}),
        ),
    )


def _insert_dry_run_position(connection, *, candidate_id: str) -> None:
    row = connection.execute(
        """
        SELECT trade_date, code, name
        FROM candidates
        WHERE candidate_instance_id = ?
        """,
        (candidate_id,),
    ).fetchone()
    now = datetime_to_wire(utc_now())
    connection.execute(
        """
        INSERT INTO dry_run_positions (
            dry_run_position_id,
            trade_date,
            code,
            name,
            quantity,
            avg_price,
            invested_notional,
            status,
            opened_at,
            updated_at
        )
        VALUES (?, ?, ?, ?, 1, 97000, 97000, 'OPEN', ?, ?)
        """,
        (
            f"dry-position-{row['code']}",
            row["trade_date"],
            row["code"],
            row["name"],
            now,
            now,
        ),
    )
    connection.commit()


def _insert_live_sim_position(
    connection,
    *,
    trade_date: str,
    status: str,
    quantity: int,
    realized_pnl: float,
    unrealized_pnl: float = 0.0,
    code: str = "005930",
) -> None:
    now = datetime_to_wire(utc_now())
    connection.execute(
        """
        INSERT INTO live_sim_positions (
            position_id,
            account_id,
            trade_date,
            code,
            name,
            side,
            quantity,
            available_quantity,
            avg_entry_price,
            total_entry_notional,
            realized_pnl,
            unrealized_pnl,
            opened_at,
            closed_at,
            last_price,
            last_price_at,
            status,
            created_at,
            updated_at
        )
        VALUES (?, 'SIM-12345678', ?, ?, '삼성전자', 'LONG', ?, ?, 97000, ?, ?, ?,
            ?, ?, 97000, ?, ?, ?, ?)
        """,
        (
            f"live-position-{code}-{status.lower()}",
            trade_date,
            code,
            quantity,
            quantity,
            97_000 * quantity,
            realized_pnl,
            unrealized_pnl,
            now,
            now if status == "CLOSED" else None,
            now,
            status,
            now,
            now,
        ),
    )
    connection.commit()
