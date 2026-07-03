from __future__ import annotations

import json
from datetime import timedelta

from domain.broker.events import GatewayEvent
from domain.broker.market import BrokerPriceTick
from domain.broker.utils import utc_now
from domain.candidate.state import CandidateState
from domain.strategy.status import StrategyObservationStatus
from gateway.event_factory import make_price_tick_event
from services.candidate_service import get_candidate, list_candidates, refresh_candidate_context
from services.config import Settings, candidate_timezone
from services.market_data_service import process_gateway_event
from services.runtime.market_open_observe_cycle import run_market_open_observe_cycle_once
from services.strategy_engine import evaluate_candidate_strategy, load_strategy_candidate_context
from services.theme_diagnostics import build_theme_data_wait_diagnostics
from services.theme_leadership import ThemeState, rebuild_theme_leadership
from services.theme_service import calculate_theme_snapshot, import_theme_memberships
from storage.event_store import append_gateway_event
from storage.sqlite import initialize_database


def test_large_theme_diagnostic_marks_reference_coverage_impossible(tmp_path) -> None:
    connection = initialize_database(tmp_path / "theme-diagnostics.sqlite3")
    settings = _settings(
        realtime_subscription_max_per_theme=5,
        theme_observable_coverage_enabled=False,
    )
    members = _large_theme_members()
    import_theme_memberships(connection, _theme_payload(members))
    _project_observable_ticks(connection, members[:5], settings)

    snapshot = calculate_theme_snapshot(connection, "large_theme", settings=settings)
    diagnostics = build_theme_data_wait_diagnostics(connection, settings=settings)
    theme = diagnostics["themes"][0]
    connection.close()

    assert snapshot.state.value == "DATA_WAIT"
    assert "LOW_FRESH_COVERAGE" in snapshot.reason_codes
    assert theme["active_member_count"] == 30
    assert theme["tick_coverage"]["tick_count"] == 5
    assert theme["subscription_capacity"]["coverage_impossible"] is True
    assert diagnostics["subscription_capacity"]["coverage_impossible_theme_count"] == 1


def test_observable_coverage_allows_theme_snapshot_to_exit_data_wait(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "observable-theme-snapshot.sqlite3")
    settings = _settings(realtime_subscription_max_per_theme=5)
    members = _large_theme_members()
    import_theme_memberships(connection, _theme_payload(members))
    _project_observable_ticks(connection, members[:5], settings)
    snapshot = calculate_theme_snapshot(connection, "large_theme", settings=settings)

    result = rebuild_theme_leadership(connection, settings=settings)
    leadership_snapshot = result.snapshots[0]
    connection.close()

    assert snapshot.state.value == "LEADING"
    assert "THEME_OBSERVABLE_COVERAGE_USED" in snapshot.reason_codes
    assert snapshot.observable_member_count == 5
    assert snapshot.observable_fresh_member_count == 3
    assert snapshot.observable_fresh_coverage_ratio == 0.6
    assert snapshot.fresh_coverage_ratio < settings.theme_min_fresh_coverage_ratio
    assert snapshot.metadata["coverage"]["basis"] == "OBSERVABLE"
    assert leadership_snapshot.state is ThemeState.SPREADING
    assert leadership_snapshot.observable_member_count == 5
    assert leadership_snapshot.valid_member_count == 5
    assert leadership_snapshot.fresh_member_count == 3
    assert leadership_snapshot.full_member_count == 30
    assert (
        leadership_snapshot.full_fresh_coverage_ratio
        < settings.theme_min_fresh_coverage_ratio
    )


def test_theme_snapshot_keeps_data_wait_when_observable_minimum_not_met(tmp_path) -> None:
    connection = initialize_database(tmp_path / "observable-theme-min.sqlite3")
    settings = _settings(realtime_subscription_max_per_theme=5)
    members = _large_theme_members()
    import_theme_memberships(connection, _theme_payload(members))
    _project_observable_ticks(connection, members[:2], settings)

    snapshot = calculate_theme_snapshot(connection, "large_theme", settings=settings)
    connection.close()

    assert snapshot.state.value == "DATA_WAIT"
    assert snapshot.quality_status.value == "PARTIAL"
    assert snapshot.observable_member_count == 2
    assert "INSUFFICIENT_OBSERVABLE_MEMBERS" in snapshot.reason_codes


def test_theme_leadership_keeps_data_wait_when_observable_min_valid_not_met(tmp_path) -> None:
    connection = initialize_database(tmp_path / "observable-min-valid.sqlite3")
    settings = _settings()
    members = _large_theme_members()
    import_theme_memberships(connection, _theme_payload(members))
    _append_and_project(
        connection,
        make_price_tick_event(
            code=members[0][0],
            name=members[0][1],
            change_rate=2.0,
            trade_value=200_000_000,
        ),
        settings,
    )

    result = rebuild_theme_leadership(connection, settings=settings)
    connection.close()

    assert result.snapshots[0].state is ThemeState.DATA_WAIT
    assert result.snapshots[0].observable_member_count == 5
    assert result.snapshots[0].valid_member_count == 1
    assert "INSUFFICIENT_VALID_MEMBERS" in result.snapshots[0].reason_codes


def test_rt_tls_candidate_payload_and_strategy_fallback_when_legacy_data_wait(tmp_path) -> None:
    connection = initialize_database(tmp_path / "rt-tls-fallback.sqlite3")
    settings = _settings(
        realtime_subscription_max_per_theme=5,
        theme_leadership_write_candidate_sources=True,
        theme_observable_coverage_enabled=False,
    )
    trade_date = _trade_date(settings)
    members = _large_theme_members()
    import_theme_memberships(connection, _theme_payload(members))
    _project_observable_ticks(connection, members[:5], settings)
    legacy = calculate_theme_snapshot(connection, "large_theme", settings=settings)

    leadership = rebuild_theme_leadership(
        connection,
        trade_date=trade_date,
        write_candidate_sources=True,
        settings=settings,
    )
    source_rows = connection.execute(
        "SELECT payload_json FROM candidate_source_events ORDER BY observed_at, code"
    ).fetchall()
    candidates = list_candidates(connection, trade_date=trade_date, active_only=True)
    candidate_id = candidates[0]["candidate_instance_id"]

    refresh_candidate_context(connection, candidate_id, settings=settings)
    refresh_candidate_context(connection, candidate_id, settings=settings)
    candidate = get_candidate(connection, candidate_id, include_context=True)
    strategy_context = load_strategy_candidate_context(connection, candidate_id, settings)
    observation = evaluate_candidate_strategy(connection, candidate_id, settings=settings)
    connection.close()

    payloads = [json.loads(row["payload_json"]) for row in source_rows]
    assert legacy.state.value == "DATA_WAIT"
    assert leadership.watchset.items
    assert all(payload["observe_only"] is True for payload in payloads)
    assert all(payload["not_order_signal"] is True for payload in payloads)
    assert all("state" in payload and "member_role" in payload for payload in payloads)
    assert candidate is not None
    assert candidate["state"] == CandidateState.CONTEXT_READY.value
    assert candidate["context"]["theme_context"]["context_source"] == (
        "theme_leadership_source_context"
    )
    assert strategy_context.raw_context["theme_latest_snapshot"]["state"] == "DATA_WAIT"
    assert strategy_context.theme_state == "SPREADING"
    assert strategy_context.theme_role in {
        "LEADER_CANDIDATE",
        "CO_LEADER_CANDIDATE",
        "FOLLOWER_CANDIDATE",
    }
    assert "RT_TLS_THEME_CONTEXT_FALLBACK_USED" in strategy_context.reason_codes
    assert observation.overall_status is not StrategyObservationStatus.DATA_WAIT


def test_strategy_fallback_still_waits_when_bar_readiness_is_missing(tmp_path) -> None:
    connection = initialize_database(tmp_path / "rt-tls-readiness.sqlite3")
    settings = _settings(
        realtime_subscription_max_per_theme=5,
        theme_leadership_write_candidate_sources=True,
    )
    trade_date = _trade_date(settings)
    members = _large_theme_members()
    import_theme_memberships(connection, _theme_payload(members))
    _project_observable_ticks(connection, members[:5], settings)
    calculate_theme_snapshot(connection, "large_theme", settings=settings)
    rebuild_theme_leadership(
        connection,
        trade_date=trade_date,
        write_candidate_sources=True,
        settings=settings,
    )
    candidate_id = list_candidates(connection, trade_date=trade_date)[0]["candidate_instance_id"]
    connection.execute("DELETE FROM market_minute_bars WHERE code = ?", (members[0][0],))
    connection.commit()
    refresh_candidate_context(connection, candidate_id, settings=settings)

    observation = evaluate_candidate_strategy(connection, candidate_id, settings=settings)
    connection.close()

    assert observation.overall_status is StrategyObservationStatus.DATA_WAIT
    assert "BAR_1M_MISSING" in observation.reason_codes


def test_observe_cycle_keeps_order_delta_zero_and_counts_realtime_commands(tmp_path) -> None:
    connection = initialize_database(tmp_path / "observe-realtime-delta.sqlite3")
    settings = _settings(
        realtime_subscription_queue_commands=True,
        realtime_subscription_max_total=10,
    )

    result = run_market_open_observe_cycle_once(
        connection,
        trade_date=_trade_date(settings),
        settings=settings,
        write_run=False,
    )
    connection.close()

    assert result.order_command_delta == {
        "cancel_order": 0,
        "modify_order": 0,
        "send_order": 0,
    }
    assert result.realtime_command_delta["register_realtime"] == 2
    assert result.stages["CommandSafety"].status == "PASS"
    assert result.stages["RealtimeSubscription"].counts["queued_realtime_command_count"] == 2


def _settings(**overrides) -> Settings:
    values = {
        "market_data_tick_stale_sec": 10,
        "market_data_degraded_tick_stale_sec": 20,
        "candidate_source_stale_sec": 999_999_999,
        "candidate_tick_stale_sec": 999_999_999,
        "candidate_episode_ttl_sec": 999_999_999,
        "strategy_engine_stale_tick_sec": 999_999_999,
        "realtime_subscription_stale_sec": 999_999_999,
        "realtime_subscription_remove_stale_after_sec": 999_999_999,
    }
    values.update(overrides)
    return Settings(**values)


def _trade_date(settings: Settings) -> str:
    return (
        utc_now()
        .astimezone(candidate_timezone(settings.candidate_trade_date_timezone))
        .date()
        .isoformat()
    )


def _large_theme_members() -> list[tuple[str, str]]:
    return [(f"{100000 + index:06d}", f"종목{index:02d}") for index in range(30)]


def _theme_payload(members: list[tuple[str, str]]) -> dict[str, object]:
    return {
        "source_type": "MOCK",
        "source_name": "observable_fixture",
        "themes": [
            {
                "theme_id": "large_theme",
                "theme_name": "대형테마",
                "members": [
                    {
                        "code": code,
                        "name": name,
                        "metadata": {"rank": index + 1},
                    }
                    for index, (code, name) in enumerate(members)
                ],
            }
        ],
    }


def _project_observable_ticks(
    connection,
    members: list[tuple[str, str]],
    settings: Settings,
) -> None:
    for index, (code, name) in enumerate(members):
        if index < 3:
            event = make_price_tick_event(
                code=code,
                name=name,
                price=10_000 + index * 100,
                change_rate=2.5 - index * 0.2,
                volume=20_000 + index * 100,
                trade_value=250_000_000 + index * 20_000_000,
                execution_strength=125.0 - index,
            )
        else:
            event = _old_price_tick_event(
                code=code,
                name=name,
                price=10_000 + index * 100,
                change_rate=1.0,
                trade_value=100_000_000,
            )
        _append_and_project(connection, event, settings)


def _old_price_tick_event(
    *,
    code: str,
    name: str,
    price: int,
    change_rate: float,
    trade_value: int,
) -> GatewayEvent:
    old_ts = utc_now() - timedelta(seconds=60)
    tick = BrokerPriceTick(
        code=code,
        name=name,
        price=price,
        change_rate=change_rate,
        volume=max(int(trade_value / price), 1),
        trade_value=trade_value,
        execution_strength=100.0,
        best_bid=max(price - 10, 1),
        best_ask=price,
        spread_ticks=1,
        day_high=price + 500,
        day_low=max(price - 500, 1),
        trade_time=old_ts,
        ts=old_ts,
    )
    return GatewayEvent(
        event_id=f"evt-old-{code}",
        event_type="price_tick",
        source="test-gateway",
        payload=tick.to_dict(),
        ts=old_ts,
    )


def _append_and_project(connection, event: GatewayEvent, settings: Settings) -> None:
    append_result = append_gateway_event(connection, event)
    assert append_result.status == "ACCEPTED"
    result = process_gateway_event(connection, event, settings=settings)
    assert result.status == "APPLIED"
