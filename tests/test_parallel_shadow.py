from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from services.parallel_shadow.engine import (
    ParallelShadowFrame,
    load_parallel_shadow_frame,
    run_parallel_shadow,
)
from services.parallel_shadow.models import (
    LiveSimObservation,
    ShadowPlan,
    ShadowPreflight,
    canonical_sha256,
)
from services.profit_lab.engine import ExecutionTick
from services.profit_lab.models import ProfitLabConfig
from tools.ops_parallel_shadow import (
    build_parallel_shadow_report,
    write_parallel_shadow_report,
)


def test_parallel_shadow_uses_profit_lab_model_and_compares_live_canary() -> None:
    frame = _frame(live=(_matching_live(),))

    first = run_parallel_shadow(frame, config=_complete_config(), commit_sha="abc123")
    second = run_parallel_shadow(frame, config=_complete_config(), commit_sha="abc123")

    execution = first.executions[0]
    comparison = first.comparisons[0]
    assert first.status == "PASS"
    assert execution.entry_fill_price == 100
    assert execution.exit_reason == "TAKE_PROFIT"
    assert execution.gross_pnl == 5.0
    assert execution.net_pnl == 2.69
    assert comparison.linkage_complete is True
    assert comparison.fill_disagreement is False
    assert comparison.fill_time_delta_sec == 0.0
    assert comparison.slippage_ticks == 0
    assert comparison.slippage_pct == 0.0
    assert comparison.exit_reason_disagreement is False
    assert comparison.gross_pnl_delta == 0.0
    assert comparison.net_pnl_delta == 0.0
    assert first.metrics["live_canary_plan_count"] == 1
    assert first.result_sha256 == second.result_sha256
    assert first.no_trading_side_effects is True


def test_shadow_continues_when_preflight_blocks_live_buy() -> None:
    frame = _frame(
        preflight=ShadowPreflight(
            status="BLOCK",
            kill_switch_active=True,
            live_buy_allowed=False,
            reason_codes=("OPERATOR_KILL_SWITCH",),
        )
    )

    result = run_parallel_shadow(frame, config=_complete_config(), commit_sha="abc123")

    assert result.status == "WARN"
    assert result.metrics["coherent_plan_ready_count"] == 1
    assert result.metrics["shadow_execution_count"] == 1
    assert result.metrics["shadow_retained_when_live_blocked"] is True
    assert result.metrics["live_buy_count_when_blocked"] == 0
    assert "NO_LIVE_CANARY_COMPARISON" in result.warnings


def test_live_buy_during_block_and_more_than_one_canary_fail_closed() -> None:
    second_plan = _plan("plan-2", at=_day() + timedelta(milliseconds=10))
    live = (
        _matching_live(),
        replace(
            _matching_live(),
            order_plan_id="plan-2",
            live_sim_intent_id="live-intent-2",
            live_sim_order_id="live-order-2",
            execution_ids=("live-execution-2",),
            position_id="live-position-2",
        ),
    )
    frame = _frame(
        plans=(_plan("plan-1"), second_plan),
        live=live,
        preflight=ShadowPreflight(
            status="BLOCK",
            kill_switch_active=True,
            live_buy_allowed=False,
        ),
    )

    result = run_parallel_shadow(frame, config=_complete_config(), commit_sha="abc123")

    assert result.status == "BLOCKED"
    assert "LIVE_CANARY_LIMIT_EXCEEDED" in result.blocker_reasons
    assert "LIVE_BUY_PRESENT_WHEN_BLOCKED" in result.blocker_reasons
    assert result.metrics["shadow_execution_count"] == 2


def test_ai_influence_cost_missing_and_linkage_gap_are_blockers() -> None:
    frame = _frame(
        plans=(replace(_plan("plan-1"), ai_influenced=True),),
        live=(
            LiveSimObservation(
                order_plan_id="missing-plan",
                live_sim_intent_id="intent-missing",
                live_sim_order_id="order-missing",
                requested_quantity=1,
                filled_quantity=0,
            ),
        ),
        ai_advisory_only=False,
    )

    result = run_parallel_shadow(frame, config=ProfitLabConfig(), commit_sha="abc123")

    assert result.status == "BLOCKED"
    assert "AI_INFLUENCE_DETECTED" in result.blocker_reasons
    assert "COST_MODEL_MISSING" in result.blocker_reasons
    assert "COMPARISON_LINKAGE_GAP" in result.blocker_reasons


def test_fill_disagreement_and_partial_fill_are_reported_without_hiding_shadow() -> None:
    live = replace(
        _matching_live(),
        requested_quantity=2,
        filled_quantity=1,
    )
    plan = replace(_plan("plan-1"), quantity=2)
    frame = _frame(plans=(plan,), live=(live,))

    result = run_parallel_shadow(frame, config=_complete_config(), commit_sha="abc123")

    assert result.status == "WARN"
    assert result.metrics["partial_fill_count"] == 1
    assert result.metrics["shadow_execution_count"] == 1
    assert "LIVE_PARTIAL_FILL_OBSERVED" in result.warnings


def test_parallel_shadow_rejects_plan_and_tick_identity_tampering(tmp_path) -> None:
    frame = _frame()
    input_path = tmp_path / "shadow-input.json"
    value = frame.to_dict()
    value["source_plan_ids_sha256"] = "0" * 64
    input_path.write_text(json.dumps(value), encoding="utf-8")

    with pytest.raises(ValueError, match="source_plan_ids_sha256"):
        load_parallel_shadow_frame(input_path)

    with pytest.raises(ValueError, match="availability must be monotonic"):
        replace(_frame(), ticks=tuple(reversed(_ticks())))


def test_parallel_shadow_report_writer_preserves_safety_evidence(tmp_path) -> None:
    result = run_parallel_shadow(
        _frame(live=(_matching_live(),)),
        config=_complete_config(),
        commit_sha="abc123",
    )
    report = build_parallel_shadow_report(result.to_dict(), run_id="shadow-test-run")

    paths = write_parallel_shadow_report(report, out_dir=tmp_path)
    raw = json.loads(paths["raw_json"].read_text(encoding="utf-8"))

    assert raw["verdict"]["status"] == "PASS"
    assert raw["verdict"]["no_trading_side_effects"] is True
    assert raw["safety"]["operational_db_opened"] is False
    assert "FAST-3 Parallel Shadow" in paths["summary_md"].read_text(encoding="utf-8")


def _day() -> datetime:
    return datetime(2026, 7, 20, 0, 0, tzinfo=UTC)


def _plan(plan_id: str, *, at: datetime | None = None) -> ShadowPlan:
    watermark = {"market_data": {"event_id": "tick-0", "rowid": 10}}
    return ShadowPlan(
        order_plan_id=plan_id,
        trade_date="2026-07-20",
        code="005930",
        created_at=at or _day(),
        limit_price=100,
        quantity=1,
        entry_timing_evaluation_id=f"entry-{plan_id}",
        strategy_observation_id=f"strategy-{plan_id}",
        risk_observation_id=f"risk-{plan_id}",
        source_run_id="source-run-1",
        source_watermark=watermark,
        source_watermark_hash=canonical_sha256(watermark),
        setup_type="BREAKOUT",
        regime="RISK_ON",
        theme="SEMICONDUCTOR",
    )


def _ticks() -> tuple[ExecutionTick, ...]:
    day = _day()
    values = (
        (day + timedelta(milliseconds=300), 99),
        (day + timedelta(milliseconds=600), 110),
        (day + timedelta(milliseconds=900), 110),
    )
    return tuple(
        ExecutionTick(
            sequence=index,
            event_id=f"tick-{index}",
            code="005930",
            exchange="KRX",
            price=price,
            event_at=available_at - timedelta(milliseconds=10),
            available_at=available_at,
        )
        for index, (available_at, price) in enumerate(values, start=1)
    )


def _matching_live() -> LiveSimObservation:
    return LiveSimObservation(
        order_plan_id="plan-1",
        live_sim_intent_id="live-intent-1",
        live_sim_order_id="live-order-1",
        requested_quantity=1,
        filled_quantity=1,
        execution_ids=("live-execution-1",),
        position_id="live-position-1",
        avg_fill_price=100,
        first_filled_at=_day() + timedelta(milliseconds=300),
        exit_reason="TAKE_PROFIT",
        closed_at=_day() + timedelta(milliseconds=900),
        holding_sec=0.6,
        gross_pnl=5.0,
        net_pnl=2.69,
    )


def _frame(
    *,
    plans: tuple[ShadowPlan, ...] | None = None,
    live: tuple[LiveSimObservation, ...] = (),
    preflight: ShadowPreflight | None = None,
    ai_advisory_only: bool = True,
) -> ParallelShadowFrame:
    resolved_plans = plans or (_plan("plan-1"),)
    plan_ids = sorted(item.order_plan_id for item in resolved_plans)
    return ParallelShadowFrame(
        snapshot_id="snapshot-1",
        generated_at=_day() + timedelta(seconds=2),
        trade_date="2026-07-20",
        plan_coverage_complete=True,
        source_plan_count=len(resolved_plans),
        source_plan_ids_sha256=canonical_sha256(plan_ids),
        plans=resolved_plans,
        ticks=_ticks(),
        preflight=preflight
        or ShadowPreflight(
            status="PASS",
            kill_switch_active=False,
            live_buy_allowed=True,
        ),
        live_sim_observations=live,
        ai_advisory_only=ai_advisory_only,
    )


def _complete_config() -> ProfitLabConfig:
    return ProfitLabConfig(
        cost_model_version="fixture-cost/v1",
        cost_model_confirmed=True,
        buy_commission_rate=0.001,
        sell_commission_rate=0.001,
        sell_tax_rate=0.001,
        buy_slippage_ticks=1,
        sell_slippage_ticks=1,
        entry_latency_ms=250,
        exit_latency_ms=250,
        entry_ttl_sec=2,
        exit_ttl_sec=2,
        minimum_filled_trades=1,
        minimum_distinct_trade_dates=1,
        eod_flatten_enabled=False,
    )
