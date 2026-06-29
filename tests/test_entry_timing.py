from __future__ import annotations

from pathlib import Path

from domain.broker.utils import datetime_to_wire, utc_now
from domain.risk.status import RiskObservationStatus
from domain.strategy.status import StrategyObservationStatus
from services.config import Settings
from services.entry_timing.engine import EntryTimingEngine
from services.entry_timing.models import (
    EntryTimingInput,
    EntryTimingState,
    OrderPlanStatus,
    PriceLocationState,
    SetupType,
)
from services.entry_timing.order_plan import (
    OrderPlanDraftBuilder,
    calculate_limit_price,
    make_order_plan_idempotency_key,
)
from services.entry_timing.price_location import PriceLocationClassifier
from services.entry_timing.service import (
    evaluate_entry_timing,
    get_entry_timing_status,
    get_order_plan_draft,
    list_latest_order_plan_drafts,
)
from services.risk_gate import evaluate_risk_for_candidate, save_risk_observation
from services.strategy_engine import evaluate_candidate_strategy, save_strategy_observation
from storage.sqlite import initialize_database
from tests.test_strategy_service import _insert_strategy_fixture


def test_price_location_classifier_distinguishes_pullback_vwap_and_overextended() -> None:
    settings = _settings()
    classifier = PriceLocationClassifier(settings=settings)

    pullback = classifier.classify(_entry_input(current_price=96_000, vwap=93_600))
    near_vwap = classifier.classify(
        _entry_input(current_price=99_800, day_high=103_000, vwap=99_500)
    )
    overextended = classifier.classify(_entry_input(current_price=100_000, vwap=94_000))

    assert pullback.state is PriceLocationState.PULLBACK_FROM_HIGH
    assert near_vwap.state is PriceLocationState.NEAR_VWAP
    assert overextended.state is PriceLocationState.EXTENDED_FROM_VWAP


def test_leader_leading_good_pullback_creates_plan_ready_draft() -> None:
    settings = _settings()
    item = _entry_input(momentum_1m=-0.1, momentum_3m=0.0)
    evaluation = EntryTimingEngine(settings=settings).evaluate(item)
    draft = OrderPlanDraftBuilder(settings=settings).build(item, evaluation)

    assert evaluation.entry_timing_state is EntryTimingState.GOOD_PULLBACK
    assert evaluation.setup_type is SetupType.THEME_LEADER_PULLBACK
    assert draft is not None
    assert draft.status is OrderPlanStatus.PLAN_READY
    assert draft.side == "BUY"
    assert draft.observe_only is True
    assert draft.not_order_intent is True
    assert draft.limit_price > 0


def test_co_leader_spreading_vwap_reclaim_creates_ready_draft() -> None:
    settings = _settings()
    item = _entry_input(
        theme_state="SPREADING",
        stock_role="CO_LEADER",
        current_price=100_000,
        day_high=103_000,
        day_low=96_000,
        vwap=99_700,
        momentum_1m=0.2,
        momentum_3m=0.4,
    )
    evaluation = EntryTimingEngine(settings=settings).evaluate(item)
    draft = OrderPlanDraftBuilder(settings=settings).build(item, evaluation)

    assert evaluation.entry_timing_state is EntryTimingState.VWAP_RECLAIM
    assert evaluation.setup_type is SetupType.VWAP_RECLAIM
    assert draft is not None
    assert draft.status is OrderPlanStatus.PLAN_READY


def test_follower_is_blocked_in_leader_only_theme() -> None:
    settings = _settings()
    item = _entry_input(theme_state="LEADER_ONLY", stock_role="FOLLOWER")

    evaluation = EntryTimingEngine(settings=settings).evaluate(item)
    draft = OrderPlanDraftBuilder(settings=settings).build(item, evaluation)

    assert evaluation.entry_timing_state is EntryTimingState.BLOCKED_CONTEXT
    assert evaluation.status is OrderPlanStatus.NO_PLAN
    assert draft is None


def test_chase_high_and_vwap_overextended_do_not_create_drafts() -> None:
    settings = _settings()
    chase = EntryTimingEngine(settings=settings).evaluate(
        _entry_input(current_price=99_500, day_high=100_000, vwap=98_000)
    )
    overextended = EntryTimingEngine(settings=settings).evaluate(
        _entry_input(current_price=100_000, day_high=104_000, vwap=94_000)
    )
    builder = OrderPlanDraftBuilder(settings=settings)

    assert chase.entry_timing_state is EntryTimingState.CHASE_HIGH
    assert (
        builder.build(
            _entry_input(current_price=99_500, day_high=100_000, vwap=98_000),
            chase,
        )
        is None
    )
    assert overextended.entry_timing_state is EntryTimingState.VWAP_OVEREXTENDED
    assert (
        builder.build(
            _entry_input(current_price=100_000, day_high=104_000, vwap=94_000),
            overextended,
        )
        is None
    )


def test_vwap_missing_and_momentum_warmup_are_near_miss_data_wait() -> None:
    settings = _settings()
    engine = EntryTimingEngine(settings=settings)
    missing_vwap = engine.evaluate(_entry_input(vwap=None))
    missing_momentum = engine.evaluate(
        _entry_input(momentum_1m=None, momentum_3m=None, momentum_5m=None)
    )
    builder = OrderPlanDraftBuilder(settings=settings)

    vwap_draft = builder.build(_entry_input(vwap=None), missing_vwap)
    momentum_draft = builder.build(
        _entry_input(momentum_1m=None, momentum_3m=None, momentum_5m=None),
        missing_momentum,
    )

    assert missing_vwap.entry_timing_state is EntryTimingState.DATA_WAIT
    assert missing_momentum.entry_timing_state is EntryTimingState.DATA_WAIT
    assert vwap_draft is not None
    assert vwap_draft.status is OrderPlanStatus.DATA_WAIT
    assert momentum_draft is not None
    assert momentum_draft.status is OrderPlanStatus.DATA_WAIT


def test_stale_vi_upper_limit_and_risk_block_prevent_plan_ready() -> None:
    settings = _settings()
    engine = EntryTimingEngine(settings=settings)
    stale = engine.evaluate(_entry_input(stale=True))
    vi = engine.evaluate(_entry_input(vi_active=True))
    upper = engine.evaluate(_entry_input(upper_limit_near=True))
    risk_block_item = _entry_input(risk_observation_status="OBSERVE_BLOCK")
    risk_block = engine.evaluate(risk_block_item)
    builder = OrderPlanDraftBuilder(settings=settings)
    risk_status, _ = builder.resolve_status(risk_block_item, risk_block)

    assert stale.status is OrderPlanStatus.BLOCKED_STALE
    assert builder.build(_entry_input(stale=True), stale) is None
    assert vi.status is OrderPlanStatus.BLOCKED_OVERHEAT
    assert upper.status is OrderPlanStatus.BLOCKED_OVERHEAT
    assert risk_status is OrderPlanStatus.BLOCKED_RISK
    assert builder.build(risk_block_item, risk_block) is None


def test_strategy_missing_and_low_liquidity_wait_retry_not_hard_fail() -> None:
    settings = _settings(entry_timing_min_turnover_krw=500_000_000)
    item = _entry_input(strategy_observation_status=None, turnover_krw=10_000_000)
    evaluation = EntryTimingEngine(settings=settings).evaluate(item)
    draft = OrderPlanDraftBuilder(settings=settings).build(item, evaluation)

    assert draft is not None
    assert draft.status is OrderPlanStatus.WAIT_RETRY
    assert "STRATEGY_OBSERVATION_MISSING" in draft.reason_codes
    assert "TURNOVER_BELOW_MIN" in draft.reason_codes


def test_idempotency_key_and_limit_price_are_stable_and_limit_only() -> None:
    settings = _settings(entry_timing_price_offset_ticks=1)
    item = _entry_input()
    evaluation = EntryTimingEngine(settings=settings).evaluate(item)
    key1 = make_order_plan_idempotency_key(item, evaluation)
    key2 = make_order_plan_idempotency_key(item, evaluation)
    limit_price = calculate_limit_price(item, settings=settings)

    assert key1 == key2
    assert limit_price.limit_price is not None
    assert limit_price.limit_price > 0
    assert limit_price.source == "BEST_ASK"
    assert settings.entry_timing_allow_market_order is False


def test_service_persists_latest_draft_without_order_side_effects(tmp_path) -> None:
    connection = initialize_database(tmp_path / "entry_timing.sqlite3")
    settings = _settings()
    candidate_id = _insert_strategy_fixture(connection)
    _raise_fixture_turnover(connection)
    strategy = evaluate_candidate_strategy(connection, candidate_id, settings=settings)
    save_strategy_observation(connection, strategy)
    risk = evaluate_risk_for_candidate(connection, candidate_id, settings=settings)
    save_risk_observation(connection, risk)

    result = evaluate_entry_timing(
        connection,
        candidate_instance_id=candidate_id,
        settings=settings,
    )
    plans = list_latest_order_plan_drafts(connection, limit=10)
    plan = get_order_plan_draft(connection, result.order_plan_drafts[0].order_plan_id)
    gateway_count = connection.execute("SELECT COUNT(*) AS count FROM gateway_commands").fetchone()[
        "count"
    ]
    live_sim_count = connection.execute(
        "SELECT COUNT(*) AS count FROM live_sim_intents"
    ).fetchone()["count"]
    connection.close()

    assert result.evaluated_count == 1
    assert result.plan_ready_count == 1
    assert len(result.order_plan_drafts) == 1
    assert plans[0]["order_plan_id"] == result.order_plan_drafts[0].order_plan_id
    assert plan is not None
    assert plan["observe_only"] is True
    assert plan["not_order_intent"] is True
    assert gateway_count == 0
    assert live_sim_count == 0
    assert risk.overall_status is RiskObservationStatus.OBSERVE_PASS
    assert strategy.overall_status is StrategyObservationStatus.MATCHED_OBSERVATION


def test_service_upserts_duplicate_draft_by_stable_key(tmp_path) -> None:
    connection = initialize_database(tmp_path / "entry_timing_dedupe.sqlite3")
    settings = _settings()
    candidate_id = _insert_strategy_fixture(connection)
    _raise_fixture_turnover(connection)
    strategy = evaluate_candidate_strategy(connection, candidate_id, settings=settings)
    save_strategy_observation(connection, strategy)
    risk = evaluate_risk_for_candidate(connection, candidate_id, settings=settings)
    save_risk_observation(connection, risk)

    first = evaluate_entry_timing(connection, candidate_instance_id=candidate_id, settings=settings)
    second = evaluate_entry_timing(
        connection,
        candidate_instance_id=candidate_id,
        settings=settings,
    )
    latest_count = connection.execute(
        "SELECT COUNT(*) AS count FROM order_plan_drafts_latest"
    ).fetchone()["count"]
    draft_count = connection.execute("SELECT COUNT(*) AS count FROM order_plan_drafts").fetchone()[
        "count"
    ]
    connection.close()

    assert first.order_plan_drafts[0].idempotency_key == second.order_plan_drafts[0].idempotency_key
    assert latest_count == 1
    assert draft_count == 1


def test_entry_timing_status_counts_no_plan_evaluations(tmp_path) -> None:
    connection = initialize_database(tmp_path / "entry_timing_status.sqlite3")
    connection.execute(
        """
        INSERT INTO entry_timing_evaluations (
            entry_timing_evaluation_id,
            trade_date,
            candidate_instance_id,
            code,
            name,
            evaluated_at,
            setup_type,
            entry_timing_state,
            price_location_state,
            status,
            reason_codes_json,
            evidence_json,
            observe_only,
            not_order_intent
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 1)
        """,
        (
            "eval_no_plan",
            "2026-06-29",
            "candidate_no_plan",
            "005930",
            "Samsung",
            datetime_to_wire(utc_now()),
            "NONE",
            "BLOCKED_CONTEXT",
            "UNKNOWN",
            "NO_PLAN",
            "[]",
            "{}",
        ),
    )

    status = get_entry_timing_status(connection, settings=_settings())
    connection.close()

    assert status["evaluation_count"] == 1
    assert status["latest_plan_count"] == 0
    assert status["no_plan_count"] == 1
    assert status["observe_only"] is True
    assert status["not_order_intent"] is True


def test_entry_timing_core_has_no_forbidden_imports() -> None:
    root = Path("services/entry_timing")
    forbidden = (
        "PyQt5",
        "QAxWidget",
        "send_order",
        "cancel_order",
        "modify_order",
        "GatewayCommand",
        "create_live_sim_intent",
    )
    for path in root.glob("*.py"):
        text = path.read_text(encoding="utf-8")
        assert not any(token in text for token in forbidden), path


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
        "entry_timing_stale_max_seconds": 999_999_999,
        "entry_timing_min_turnover_krw": 1_000,
        "entry_timing_min_execution_strength": 1,
    }
    values.update(overrides)
    return Settings(**values)


def _entry_input(**overrides) -> EntryTimingInput:
    now = datetime_to_wire(utc_now())
    values = {
        "trade_date": "2026-06-27",
        "candidate_instance_id": "CAND-2026-06-27-005930-1",
        "code": "005930",
        "name": "삼성전자",
        "theme_id": "theme-005930",
        "theme_name": "반도체",
        "theme_state": "LEADING",
        "theme_rank": 1,
        "stock_role": "LEADER",
        "theme_priority_score": 80.0,
        "current_price": 96_000,
        "prev_close": 94_000,
        "open_price": 95_000,
        "day_high": 100_000,
        "day_low": 92_000,
        "change_rate_pct": 2.0,
        "turnover_krw": 600_000_000,
        "execution_strength": 120.0,
        "momentum_1m": -0.1,
        "momentum_3m": 0.0,
        "momentum_5m": 0.2,
        "vwap": 93_500,
        "pullback_from_high_pct": None,
        "spread_ticks": 1,
        "stale": False,
        "vi_active": False,
        "upper_limit_near": False,
        "theme_reason_codes": ["WATCHSET_SELECTED"],
        "candidate_state": "CONTEXT_READY",
        "strategy_observation_status": "MATCHED_OBSERVATION",
        "strategy_setup_type": "THEME_LEADER_PULLBACK",
        "strategy_score": 0.8,
        "strategy_confidence": 0.7,
        "risk_observation_status": "OBSERVE_PASS",
        "risk_reason_codes": [],
        "observed_at": now,
        "best_bid": 95_900,
        "best_ask": 96_000,
        "tick_age_sec": 1.0,
    }
    values.update(overrides)
    return EntryTimingInput(**values)


def _raise_fixture_turnover(connection) -> None:
    connection.execute(
        """
        UPDATE market_ticks_latest
        SET cumulative_trade_value = 600000000,
            execution_strength = 130.0
        WHERE code = '005930'
        """
    )
    connection.execute(
        """
        UPDATE theme_snapshot_members
        SET cumulative_trade_value = 600000000,
            execution_strength = 130.0
        WHERE code = '005930'
        """
    )
    connection.commit()
