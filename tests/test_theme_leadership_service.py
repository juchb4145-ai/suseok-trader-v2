from __future__ import annotations

from datetime import timedelta
from pathlib import Path

from domain.broker.events import GatewayEvent
from domain.broker.market import BrokerPriceTick
from domain.broker.utils import utc_now
from gateway.event_factory import make_condition_event, make_price_tick_event
from services.config import Settings, candidate_timezone
from services.market_data_service import process_gateway_event
from services.theme_leadership import (
    StockRole,
    ThemeLeadershipService,
    ThemeState,
    build_candidate_source_events,
    rebuild_theme_leadership,
)
from services.theme_leadership.bootstrap_watchset import (
    DEFAULT_BOOTSTRAP_SOURCE,
    queue_bootstrap_realtime_registration,
    select_bootstrap_realtime_codes,
)
from services.theme_leadership.classifier import ThemeStateClassifier, ThemeStateInput
from services.theme_leadership.ranker import ThemeLeadershipRanker
from services.theme_leadership.snapshot import RealtimeSnapshotBuilder
from services.theme_leadership.stock_role import StockRoleClassifier
from services.theme_leadership.universe import ThemeUniverseBuilder
from services.theme_leadership.watchset import WatchsetSelector
from services.theme_service import import_theme_memberships
from storage.event_store import append_gateway_event
from storage.sqlite import initialize_database


def test_theme_leadership_builders_rank_leading_theme_and_watchset(tmp_path) -> None:
    connection = initialize_database(tmp_path / "rt_tls.sqlite3")
    settings = _settings()
    import_theme_memberships(
        connection,
        _theme_payload(
            "semiconductor",
            "반도체",
            [
                ("005930", "삼성전자"),
                ("000660", "SK하이닉스"),
                ("035420", "NAVER"),
            ],
        ),
    )
    _append_and_project(
        connection,
        make_price_tick_event(
            code="005930",
            name="삼성전자",
            change_rate=3.0,
            volume=4000,
            trade_value=280_000_000,
            execution_strength=120.0,
        ),
        settings,
    )
    _append_and_project(
        connection,
        make_price_tick_event(
            code="000660",
            name="SK하이닉스",
            price=120000,
            change_rate=2.0,
            volume=2500,
            trade_value=300_000_000,
            execution_strength=112.0,
        ),
        settings,
    )
    _append_and_project(
        connection,
        make_price_tick_event(
            code="035420",
            name="NAVER",
            price=220000,
            change_rate=1.1,
            volume=600,
            trade_value=132_000_000,
            execution_strength=105.0,
        ),
        settings,
    )
    _append_and_project(connection, make_condition_event(code="005930", name="삼성전자"), settings)

    universe = ThemeUniverseBuilder().build(connection)
    stock_snapshots = RealtimeSnapshotBuilder(settings=settings).build_for_universe(
        connection,
        universe,
    )
    ranked = ThemeLeadershipRanker(settings=settings).rank(universe, stock_snapshots)
    watchset = WatchsetSelector(settings=settings).select(ranked)
    connection.close()

    assert len(universe) == 3
    assert stock_snapshots["005930"].source_flags["condition_include"] is True
    assert ranked[0].state is ThemeState.LEADING
    assert ranked[0].leader_count == 1
    assert {member.role for member in ranked[0].members} >= {
        StockRole.LEADER,
        StockRole.CO_LEADER,
        StockRole.FOLLOWER,
    }
    assert len(watchset.items) == 3
    assert all(item.source_type.startswith("THEME_") for item in watchset.items)


def test_ranker_prioritizes_observed_zero_score_theme_over_empty_theme(tmp_path) -> None:
    connection = initialize_database(tmp_path / "observed-zero-score.sqlite3")
    settings = _settings()
    import_theme_memberships(
        connection,
        _theme_payload(
            "empty_theme",
            "가나다 미관측",
            [("011000", "미관측A"), ("011001", "미관측B")],
        ),
    )
    import_theme_memberships(
        connection,
        _theme_payload(
            "weak_semiconductor",
            "반도체 약세",
            [("005930", "삼성전자"), ("000660", "SK하이닉스")],
        ),
    )
    _append_and_project(
        connection,
        make_price_tick_event(
            code="005930",
            name="삼성전자",
            change_rate=-5.0,
            trade_value=0,
        ),
        settings,
    )
    _append_and_project(
        connection,
        make_price_tick_event(
            code="000660",
            name="SK하이닉스",
            price=120000,
            change_rate=-5.0,
            trade_value=0,
        ),
        settings,
    )

    result = rebuild_theme_leadership(connection, settings=settings)
    connection.close()

    assert result.snapshots[0].theme_id == "weak_semiconductor"
    assert result.snapshots[0].state is ThemeState.WEAK
    assert result.snapshots[0].valid_member_count == 2


def test_leader_only_theme_keeps_followers_out_of_watchset(tmp_path) -> None:
    connection = initialize_database(tmp_path / "leader_only.sqlite3")
    settings = _settings()
    import_theme_memberships(
        connection,
        _theme_payload(
            "robotics",
            "로봇",
            [
                ("005930", "삼성전자"),
                ("000660", "SK하이닉스"),
                ("035420", "NAVER"),
            ],
        ),
    )
    _append_and_project(
        connection,
        make_price_tick_event(
            code="005930", name="삼성전자", change_rate=6.0, trade_value=500_000_000
        ),
        settings,
    )
    _append_and_project(
        connection,
        make_price_tick_event(code="000660", name="SK하이닉스", price=120000, change_rate=-0.4),
        settings,
    )
    _append_and_project(
        connection,
        make_price_tick_event(code="035420", name="NAVER", price=220000, change_rate=-0.2),
        settings,
    )

    result = rebuild_theme_leadership(connection, settings=settings)
    connection.close()

    assert result.snapshots[0].state is ThemeState.LEADER_ONLY
    assert all(item.stock_role is not StockRole.FOLLOWER for item in result.watchset.items)


def test_data_wait_is_retained_and_condition_only_does_not_become_leading(tmp_path) -> None:
    connection = initialize_database(tmp_path / "data_wait.sqlite3")
    settings = _settings()
    import_theme_memberships(
        connection,
        _theme_payload(
            "condition_theme",
            "조건테마",
            [
                ("005930", "삼성전자"),
                ("000660", "SK하이닉스"),
            ],
        ),
    )
    _append_and_project(connection, make_condition_event(code="005930", name="삼성전자"), settings)

    result = ThemeLeadershipService(settings=settings).rebuild(connection)
    connection.close()

    assert result.snapshots[0].state is ThemeState.DATA_WAIT
    assert "INSUFFICIENT_VALID_MEMBERS" in result.snapshots[0].reason_codes
    assert len(result.watchset.items) == 0
    assert len(result.candidate_source_events) == 0


def test_watchset_excludes_overheated_late_laggard_and_stale_members(tmp_path) -> None:
    connection = initialize_database(tmp_path / "excluded.sqlite3")
    settings = _settings(
        market_data_tick_stale_sec=30,
        market_data_degraded_tick_stale_sec=90,
    )
    import_theme_memberships(
        connection,
        _theme_payload(
            "battery",
            "2차전지",
            [
                ("005930", "삼성전자"),
                ("000660", "SK하이닉스"),
                ("035420", "NAVER"),
                ("051910", "LG화학"),
            ],
        ),
    )
    _append_and_project(
        connection,
        make_price_tick_event(
            code="005930", name="삼성전자", change_rate=3.0, trade_value=300_000_000
        ),
        settings,
    )
    _append_and_project(
        connection,
        make_price_tick_event(
            code="000660",
            name="SK하이닉스",
            price=120000,
            change_rate=26.0,
            trade_value=260_000_000,
        ),
        settings,
    )
    _append_and_project(
        connection,
        make_price_tick_event(
            code="035420", name="NAVER", price=220000, change_rate=0.0, trade_value=30_000_000
        ),
        settings,
    )
    _append_and_project(connection, _old_price_tick_event(), settings)

    result = rebuild_theme_leadership(connection, settings=settings)
    excluded_roles = {item["stock_role"] for item in result.watchset.excluded}
    connection.close()

    assert StockRole.OVERHEATED.value in excluded_roles
    assert StockRole.LATE_LAGGARD.value in excluded_roles
    assert StockRole.STALE.value in excluded_roles


def test_limits_and_candidate_source_events_are_observe_only_without_orders(tmp_path) -> None:
    connection = initialize_database(tmp_path / "candidate_sources.sqlite3")
    settings = _settings(
        theme_leadership_top_theme_count=2,
        theme_leadership_max_stocks_per_theme=1,
        theme_leadership_max_total_watchset=2,
        theme_leadership_write_candidate_sources=True,
    )
    for index, (theme_id, theme_name) in enumerate(
        [
            ("semiconductor", "반도체"),
            ("battery", "2차전지"),
            ("robotics", "로봇"),
        ]
    ):
        import_theme_memberships(
            connection,
            _theme_payload(
                theme_id,
                theme_name,
                [
                    (f"00{5930 + index}", f"종목{index}A"),
                    (f"00{6600 + index}", f"종목{index}B"),
                ],
            ),
        )
    for code in ["005930", "006600", "005931", "006601", "005932", "006602"]:
        _append_and_project(
            connection,
            make_price_tick_event(
                code=code, name=f"종목{code}", change_rate=2.0, trade_value=150_000_000
            ),
            settings,
        )

    result = rebuild_theme_leadership(connection, write_candidate_sources=True, settings=settings)
    candidate_event_count = connection.execute(
        "SELECT COUNT(*) AS count FROM candidate_source_events"
    ).fetchone()["count"]
    gateway_command_count = connection.execute(
        "SELECT COUNT(*) AS count FROM gateway_commands"
    ).fetchone()["count"]
    dry_run_intent_count = connection.execute(
        "SELECT COUNT(*) AS count FROM dry_run_intents"
    ).fetchone()["count"]
    live_sim_intent_count = connection.execute(
        "SELECT COUNT(*) AS count FROM live_sim_intents"
    ).fetchone()["count"]
    connection.close()

    assert len(result.snapshots) == 2
    assert len(result.watchset.items) == 2
    assert candidate_event_count == 2
    assert gateway_command_count == 0
    assert dry_run_intent_count == 0
    assert live_sim_intent_count == 0
    assert all(event.payload["observe_only"] is True for event in result.candidate_source_events)


def test_bootstrap_watchset_queues_only_realtime_registration(tmp_path) -> None:
    connection = initialize_database(tmp_path / "bootstrap-watchset.sqlite3")
    settings = _settings(theme_leadership_max_total_watchset=3)
    import_theme_memberships(
        connection,
        _theme_payload(
            "semiconductor",
            "반도체",
            [
                ("005930", "삼성전자"),
                ("000660", "SK하이닉스"),
                ("035420", "NAVER"),
            ],
        ),
    )
    import_theme_memberships(
        connection,
        _theme_payload(
            "it",
            "IT",
            [
                ("005930", "삼성전자"),
                ("034220", "LG디스플레이"),
                ("066570", "LG전자"),
            ],
        ),
    )

    selection = select_bootstrap_realtime_codes(
        connection,
        settings=settings,
        anchor_codes=["005930"],
        max_codes=3,
    )
    result = queue_bootstrap_realtime_registration(
        connection,
        settings=settings,
        anchor_codes=["005930"],
        max_codes=3,
    )
    row = connection.execute(
        "SELECT command_type, source, payload_json FROM gateway_commands"
    ).fetchone()
    connection.close()

    assert selection.selected_codes
    assert result.status == "QUEUED"
    assert row["command_type"] == "register_realtime"
    assert row["source"] == DEFAULT_BOOTSTRAP_SOURCE
    assert "send_order" not in row["payload_json"]
    assert '"observe_only":true' in row["payload_json"]


def test_classifiers_and_candidate_event_builder_contract() -> None:
    state, reasons = ThemeStateClassifier().classify(
        ThemeStateInput(
            valid_member_count=1,
            fresh_coverage_ratio=0.3,
            rising_count=1,
            rising_ratio=1.0,
            weighted_return_pct=3.0,
            total_turnover_krw=100_000_000,
            leader_score=30.0,
            score=70.0,
            reason_codes=[],
            min_valid_members=2,
            min_fresh_coverage_ratio=0.4,
        )
    )
    assert state is ThemeState.DATA_WAIT
    assert "INSUFFICIENT_VALID_MEMBERS" in reasons
    assert StockRoleClassifier() is not None
    assert build_candidate_source_events([], trade_date=_trade_date(_settings())) == []


def test_theme_leadership_core_has_no_kiwoom_or_pyqt_imports() -> None:
    root = Path("services/theme_leadership")
    forbidden = ("PyQt5", "QAxWidget", "Kiwoom", "send_order", "cancel_order", "modify_order")
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


def _theme_payload(
    theme_id: str,
    theme_name: str,
    members: list[tuple[str, str]],
) -> dict[str, object]:
    return {
        "source_type": "MOCK",
        "source_name": "rt_tls_fixture",
        "themes": [
            {
                "theme_id": theme_id,
                "theme_name": theme_name,
                "members": [{"code": code, "name": name, "weight": 1.0} for code, name in members],
            }
        ],
    }


def _old_price_tick_event() -> GatewayEvent:
    old_ts = utc_now() - timedelta(seconds=60)
    tick = BrokerPriceTick(
        code="051910",
        name="LG화학",
        price=400000,
        change_rate=2.0,
        volume=500,
        trade_value=200_000_000,
        execution_strength=100.0,
        best_bid=399500,
        best_ask=400000,
        spread_ticks=1,
        day_high=405000,
        day_low=390000,
        trade_time=old_ts,
        ts=old_ts,
    )
    return GatewayEvent(
        event_id="evt_old_rt_tls_051910",
        event_type="price_tick",
        source="test-gateway",
        payload=tick.to_dict(),
        ts=old_ts,
    )


def _append_and_project(
    connection,
    event: GatewayEvent,
    settings: Settings,
) -> None:
    append_result = append_gateway_event(connection, event)
    assert append_result.status == "ACCEPTED"
    result = process_gateway_event(connection, event, settings=settings)
    assert result.status == "APPLIED"
