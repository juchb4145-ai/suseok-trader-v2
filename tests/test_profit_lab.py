from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from domain.broker.events import GatewayEvent
from services.profit_lab.engine import load_profit_lab_signals, run_profit_lab
from services.profit_lab.models import (
    PROFIT_LAB_SIGNAL_FORMAT,
    ProfitLabConfig,
    ProfitLabSignal,
)
from services.runtime.projection_replay import export_replay_bundle
from storage.event_store import append_gateway_event
from storage.sqlite import initialize_database


def test_profit_lab_requires_later_ticks_and_uses_conservative_limit_prices(tmp_path) -> None:
    day = datetime(2026, 7, 20, 0, 0, tzinfo=UTC)
    bundle = _bundle(
        tmp_path,
        [
            (day, 99),
            (day + timedelta(milliseconds=100), 99),
            (day + timedelta(milliseconds=300), 98),
            (day + timedelta(milliseconds=600), 110),
            (day + timedelta(milliseconds=700), 110),
            (day + timedelta(milliseconds=900), 110),
        ],
    )
    manifest = _manifest(
        tmp_path,
        bundle,
        [_signal("signal-1", day, "2026-07-20")],
    )

    first = run_profit_lab(
        bundle_dir=bundle.bundle_dir,
        alpha_replay_report=_alpha_report(bundle, qualified=True),
        signal_manifest=manifest,
        config=_complete_config(minimum_filled_trades=1),
        commit_sha="abc123",
    )
    second = run_profit_lab(
        bundle_dir=bundle.bundle_dir,
        alpha_replay_report=_alpha_report(bundle, qualified=True),
        signal_manifest=manifest,
        config=_complete_config(minimum_filled_trades=1),
        commit_sha="abc123",
    )

    trade = first.trades[0]
    assert trade.status == "CLOSED"
    assert trade.entry_market_price == 98
    assert trade.entry_fill_price == 100
    assert trade.entry_filled_at.endswith("00:00:00.300000Z")
    assert trade.exit_triggered_at.endswith("00:00:00.600000Z")
    assert trade.exit_filled_at.endswith("00:00:00.900000Z")
    assert trade.exit_market_price == 110
    assert trade.exit_fill_price == 105
    assert trade.gross_pnl == 5.0
    assert trade.slippage_cost == 2.0
    assert first.metrics["entry_fill_count"] == 1
    assert first.metrics["fill_rate"] == 1.0
    assert first.result_sha256 == second.result_sha256
    assert first.no_trading_side_effects is True


def test_stop_sell_uses_first_later_gap_tick_not_stop_price(tmp_path) -> None:
    day = datetime(2026, 7, 20, 0, 0, tzinfo=UTC)
    bundle = _bundle(
        tmp_path,
        [
            (day + timedelta(milliseconds=300), 99),
            (day + timedelta(milliseconds=600), 97),
            (day + timedelta(milliseconds=900), 80),
        ],
    )
    manifest = _manifest(tmp_path, bundle, [_signal("gap-stop", day, "2026-07-20")])

    result = run_profit_lab(
        bundle_dir=bundle.bundle_dir,
        alpha_replay_report=_alpha_report(bundle, qualified=True),
        signal_manifest=manifest,
        config=_complete_config(minimum_filled_trades=1),
        commit_sha="abc123",
    )

    trade = result.trades[0]
    assert trade.exit_trigger_type == "STOP_LOSS"
    assert trade.exit_trigger_price == 97
    assert trade.exit_market_price == 80
    assert trade.exit_fill_price == 80
    assert trade.exit_fill_price < trade.exit_trigger_price


def test_buy_limit_expires_without_fill_and_never_uses_same_tick(tmp_path) -> None:
    day = datetime(2026, 7, 20, 0, 0, tzinfo=UTC)
    bundle = _bundle(
        tmp_path,
        [
            (day, 90),
            (day + timedelta(milliseconds=300), 101),
            (day + timedelta(seconds=3), 90),
        ],
    )
    manifest = _manifest(tmp_path, bundle, [_signal("ttl", day, "2026-07-20")])

    result = run_profit_lab(
        bundle_dir=bundle.bundle_dir,
        alpha_replay_report=_alpha_report(bundle, qualified=True),
        signal_manifest=manifest,
        config=_complete_config(entry_ttl_sec=2),
        commit_sha="abc123",
    )

    assert result.trades[0].status == "ENTRY_NO_FILL"
    assert result.trades[0].reason_codes == ("BUY_LIMIT_TTL_NO_FILL",)
    assert result.metrics["entry_no_fill_count"] == 1


def test_profit_lab_qualification_uses_date_splits_cost_and_risk_metrics(tmp_path) -> None:
    dates = [datetime(2026, 7, day, 0, 0, tzinfo=UTC) for day in (20, 21, 22)]
    ticks: list[tuple[datetime, int]] = []
    for index, day in enumerate(dates):
        ticks.append((day + timedelta(milliseconds=300), 99))
        if index == 0:
            ticks.extend(
                [
                    (day + timedelta(milliseconds=600), 97),
                    (day + timedelta(milliseconds=900), 98),
                ]
            )
        else:
            ticks.extend(
                [
                    (day + timedelta(milliseconds=600), 110),
                    (day + timedelta(milliseconds=900), 110),
                ]
            )
    bundle = _bundle(tmp_path, ticks)
    manifest = _manifest(
        tmp_path,
        bundle,
        [
            _signal(f"signal-{index}", day, day.date().isoformat())
            for index, day in enumerate(dates)
        ],
    )
    config = _complete_config(
        minimum_filled_trades=3,
        minimum_distinct_trade_dates=3,
    )

    result = run_profit_lab(
        bundle_dir=bundle.bundle_dir,
        alpha_replay_report=_alpha_report(bundle, qualified=True),
        signal_manifest=manifest,
        config=config,
        commit_sha="abc123",
    )

    assert result.qualification == "ALPHA_QUALIFIED"
    assert result.status == "PASS"
    assert result.metrics["closed_trade_count"] == 3
    assert result.metrics["distinct_trade_dates"] == 3
    assert result.metrics["profit_factor"] >= 1.15
    assert result.grouped_metrics["dataset_split"]["VALIDATION"]["net_expectancy"] > 0
    assert result.grouped_metrics["dataset_split"]["TEST"]["net_expectancy"] > 0
    assert len(result.stress_matrix) == 4


def test_profit_lab_fails_closed_for_cost_and_fast2a_data_quality(tmp_path) -> None:
    day = datetime(2026, 7, 20, 0, 0, tzinfo=UTC)
    bundle = _bundle(tmp_path, [(day + timedelta(milliseconds=300), 99)])
    manifest = _manifest(tmp_path, bundle, [_signal("signal", day, "2026-07-20")])

    cost_missing = run_profit_lab(
        bundle_dir=bundle.bundle_dir,
        alpha_replay_report=_alpha_report(bundle, qualified=True),
        signal_manifest=manifest,
        config=ProfitLabConfig(minimum_filled_trades=1, minimum_distinct_trade_dates=1),
        commit_sha="abc123",
    )
    data_blocked = run_profit_lab(
        bundle_dir=bundle.bundle_dir,
        alpha_replay_report=_alpha_report(bundle, qualified=False),
        signal_manifest=manifest,
        config=_complete_config(minimum_filled_trades=1),
        commit_sha="abc123",
    )

    assert cost_missing.qualification == "COST_MODEL_MISSING"
    assert "COST_MODEL_MISSING" in cost_missing.qualification_reasons
    assert data_blocked.qualification == "DATA_QUALITY_BLOCKED"
    assert "FAST_2A_DATA_QUALITY_NOT_QUALIFIED" in data_blocked.qualification_reasons


def test_profit_lab_rejects_alpha_and_signal_identity_mismatch(tmp_path) -> None:
    day = datetime(2026, 7, 20, 0, 0, tzinfo=UTC)
    bundle = _bundle(tmp_path, [(day + timedelta(milliseconds=300), 99)])
    manifest = _manifest(tmp_path, bundle, [_signal("signal", day, "2026-07-20")])
    bad_alpha = _alpha_report(bundle, qualified=True)
    bad_alpha["replay"]["identity"]["source_record_sha256"] = "bad"

    with pytest.raises(ValueError, match="source record hash"):
        run_profit_lab(
            bundle_dir=bundle.bundle_dir,
            alpha_replay_report=bad_alpha,
            signal_manifest=manifest,
            config=_complete_config(minimum_filled_trades=1),
            commit_sha="abc123",
        )

    with pytest.raises(ValueError, match="signal manifest SHA-256"):
        run_profit_lab(
            bundle_dir=bundle.bundle_dir,
            alpha_replay_report=_alpha_report(bundle, qualified=True),
            signal_manifest=replace(manifest, signals_sha256="0" * 64),
            config=_complete_config(minimum_filled_trades=1),
            commit_sha="abc123",
        )


def test_profit_lab_signal_trade_date_must_match_seoul_date() -> None:
    with pytest.raises(ValueError, match="trade_date must match"):
        _signal(
            "wrong-date",
            datetime(2026, 7, 20, 15, 30, tzinfo=UTC),
            "2026-07-20",
        )


def _bundle(tmp_path: Path, ticks: list[tuple[datetime, int]]):
    source = tmp_path / "source.sqlite3"
    connection = initialize_database(source)
    try:
        for index, (available_at, price) in enumerate(ticks, start=1):
            event_at = available_at - timedelta(milliseconds=10)
            event = GatewayEvent(
                event_id=f"tick-{index}",
                event_type="price_tick",
                source="fixture",
                ts=event_at,
                payload=_price_payload(price, event_at),
            )
            result = append_gateway_event(connection, event)
            assert result.accepted is True, result.error_message
            wire = available_at.isoformat().replace("+00:00", "Z")
            connection.execute(
                "UPDATE raw_events SET received_at = ? WHERE event_id = ?",
                (wire, event.event_id),
            )
            connection.execute(
                "UPDATE gateway_events SET received_at = ? WHERE event_id = ?",
                (wire, event.event_id),
            )
            connection.commit()
    finally:
        connection.close()
    return export_replay_bundle(source_db_path=source, bundle_dir=tmp_path / "bundle")


def _manifest(tmp_path: Path, bundle, signals: list[ProfitLabSignal]):
    path = tmp_path / "signals.json"
    path.write_text(
        json.dumps(
            {
                "format": PROFIT_LAB_SIGNAL_FORMAT,
                "source_record_sha256": bundle.record_sha256,
                "source_event_order_sha256": bundle.event_order_sha256,
                "signals": [signal.to_dict() for signal in signals],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return load_profit_lab_signals(path)


def _signal(signal_id: str, at: datetime, trade_date: str) -> ProfitLabSignal:
    return ProfitLabSignal(
        signal_id=signal_id,
        trade_date=trade_date,
        code="005930",
        signal_at=at,
        limit_price=100,
        quantity=1,
        setup_type="BREAKOUT",
        regime="RISK_ON",
        theme="SEMICONDUCTOR",
        order_plan_id=f"plan-{signal_id}",
        source_lineage={"source_run_id": "fixture-run"},
    )


def _price_payload(price: int, ts: datetime) -> dict[str, object]:
    return {
        "code": "005930",
        "name": "Samsung Electronics",
        "price": price,
        "change_rate": 0.1,
        "volume": 1000,
        "trade_value": price * 1000,
        "execution_strength": 101.0,
        "best_bid": max(price - 1, 1),
        "best_ask": price,
        "spread_ticks": 1,
        "day_high": max(price, 110),
        "day_low": min(price, 80),
        "trade_time": ts,
        "ts": ts,
        "metadata": {"exchange": "KRX"},
    }


def _alpha_report(bundle, *, qualified: bool) -> dict:
    return {
        "format": "point-in-time-alpha-replay-report/v1",
        "replay": {
            "result_sha256": "alpha-result-hash",
            "identity": {
                "source_record_sha256": bundle.record_sha256,
                "source_event_order_sha256": bundle.event_order_sha256,
            },
            "alpha_qualified": qualified,
            "point_in_time_violation_count": 0,
            "scan_coverage": "COMPLETE" if qualified else "NOT_PRESENT",
            "missing_sources": [] if qualified else ["market_scan_tr_response"],
        },
    }


def _complete_config(**overrides) -> ProfitLabConfig:
    values = {
        "cost_model_version": "fixture-cost/v1",
        "cost_model_confirmed": True,
        "buy_commission_rate": 0.001,
        "sell_commission_rate": 0.001,
        "sell_tax_rate": 0.001,
        "buy_slippage_ticks": 1,
        "sell_slippage_ticks": 1,
        "entry_latency_ms": 250,
        "exit_latency_ms": 250,
        "entry_ttl_sec": 2,
        "exit_ttl_sec": 2,
        "minimum_filled_trades": 1,
        "minimum_distinct_trade_dates": 1,
        "eod_flatten_enabled": False,
    }
    values.update(overrides)
    return ProfitLabConfig(**values)
