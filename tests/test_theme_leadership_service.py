from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import services.theme_leadership.service as leadership_service
from domain.broker.events import GatewayEvent
from domain.broker.market import BrokerPriceTick
from domain.broker.utils import utc_now
from gateway.event_factory import make_condition_event, make_price_tick_event
from services.candidate_service import CandidateSourceApplyResult
from services.config import Settings, candidate_timezone
from services.market_data_service import process_gateway_event
from services.market_scan_service import process_market_scan_event
from services.theme_leadership import (
    StockRole,
    ThemeLeadershipService,
    ThemeLeadershipSnapshot,
    ThemeMemberLeadership,
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
from services.theme_service import calculate_theme_snapshot, import_theme_memberships
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


def test_theme_leadership_source_write_rolls_back_all_events_on_late_failure(
    tmp_path,
    monkeypatch,
) -> None:
    connection = initialize_database(tmp_path / "theme-leadership-source-atomic.sqlite3")
    call_count = 0

    def apply_source(actual_connection, source_event, *, settings):
        nonlocal call_count
        call_count += 1
        if call_count == 11:
            raise RuntimeError("synthetic late source failure")
        actual_connection.execute(
            """
            INSERT INTO candidate_projection_errors (
                candidate_instance_id,
                source_event_id,
                code,
                error_message,
                payload_json
            )
            VALUES (NULL, NULL, NULL, 'leadership-chunk-probe', '{}')
            """
        )
        return CandidateSourceApplyResult(source_event_count=1)

    monkeypatch.setattr(
        leadership_service,
        "create_or_merge_candidate_from_source",
        apply_source,
    )

    try:
        leadership_service._write_candidate_source_events(
            connection,
            [object() for _ in range(11)],
            settings=_settings(),
        )
    except RuntimeError as exc:
        assert "synthetic late source failure" in str(exc)
    else:
        raise AssertionError("expected late source failure")
    durable_count = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM candidate_projection_errors
        WHERE error_message = 'leadership-chunk-probe'
        """
    ).fetchone()["count"]
    in_transaction = connection.in_transaction
    connection.close()

    assert call_count == 11
    assert durable_count == 0
    assert in_transaction is False


def test_theme_leadership_fence_loss_rolls_back_entire_rebuild(
    tmp_path,
    monkeypatch,
) -> None:
    connection = initialize_database(tmp_path / "theme-leadership-fence.sqlite3")
    connection.execute(
        "CREATE TABLE leadership_source_probe (event_id TEXT PRIMARY KEY)"
    )
    connection.commit()
    events = [f"leadership-event-{index}" for index in range(11)]
    call_count = 0

    def apply_source(actual_connection, source_event, *, settings):
        nonlocal call_count
        call_count += 1
        actual_connection.execute(
            "INSERT INTO leadership_source_probe (event_id) VALUES (?)",
            (source_event,),
        )
        return CandidateSourceApplyResult(source_event_count=1)

    monkeypatch.setattr(
        leadership_service,
        "create_or_merge_candidate_from_source",
        apply_source,
    )
    monkeypatch.setattr(
        leadership_service,
        "assert_runtime_execution_fence",
        lambda connection: (_ for _ in ()).throw(
            RuntimeError("EVALUATION_RUN_FENCE_LOST")
        ),
    )

    try:
        leadership_service._write_candidate_source_events(
            connection,
            events,
            settings=_settings(),
        )
    except RuntimeError as exc:
        assert "EVALUATION_RUN_FENCE_LOST" in str(exc)
    else:
        raise AssertionError("expected fence loss before rebuild commit")
    durable_after_loss = connection.execute(
        "SELECT COUNT(*) AS count FROM leadership_source_probe"
    ).fetchone()["count"]
    in_transaction = connection.in_transaction
    connection.close()

    assert call_count == 11
    assert durable_after_loss == 0
    assert in_transaction is False


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


def test_rebuild_watchset_selection_pool_uses_eligible_themes_beyond_data_wait_top_n(
    tmp_path,
) -> None:
    connection = initialize_database(tmp_path / "watchset-eligible-pool.sqlite3")
    settings = _settings(theme_leadership_top_theme_count=5)
    snapshots = [
        _leadership_snapshot(rank=index, state=ThemeState.DATA_WAIT)
        for index in range(1, 6)
    ]
    snapshots.extend(
        [
            _leadership_snapshot(rank=6, state=ThemeState.LEADING),
            _leadership_snapshot(rank=7, state=ThemeState.SPREADING),
            _leadership_snapshot(rank=8, state=ThemeState.LEADER_ONLY),
        ]
    )
    service = ThemeLeadershipService(settings=settings)
    _install_fake_rebuild_inputs(service, snapshots)

    result = service.rebuild(connection)
    connection.close()

    assert result.diagnostic_top_theme_count == 5
    assert result.eligible_theme_count == 3
    assert result.watchset_selection_source == "eligible_ranked"
    assert result.watchset_selection_theme_count == 3
    assert result.warning == "DATA_WAIT_TOP_THEMES_SKIPPED_FOR_WATCHSET"
    assert result.watchset.items
    assert result.watchset.reason_summary["DATA_WAIT_TOP_THEMES_SKIPPED_FOR_WATCHSET"] == 1


def test_rebuild_watchset_selection_pool_reports_no_eligible_theme(tmp_path) -> None:
    connection = initialize_database(tmp_path / "watchset-no-eligible.sqlite3")
    settings = _settings(theme_leadership_top_theme_count=5)
    snapshots = [
        _leadership_snapshot(rank=1, state=ThemeState.DATA_WAIT),
        _leadership_snapshot(rank=2, state=ThemeState.WEAK),
    ]
    service = ThemeLeadershipService(settings=settings)
    _install_fake_rebuild_inputs(service, snapshots)

    result = service.rebuild(connection)
    connection.close()

    assert result.eligible_theme_count == 0
    assert result.watchset_selection_source == "diagnostic_top"
    assert result.watchset.items == []
    assert result.warning == "THEME_LEADERSHIP_NO_ELIGIBLE_THEME"
    assert result.watchset.reason_summary["THEME_LEADERSHIP_NO_ELIGIBLE_THEME"] == 1


def test_market_scan_flow_uses_observed_universe_for_watchset(tmp_path) -> None:
    connection = initialize_database(tmp_path / "market-scan-flow-watchset.sqlite3")
    settings = _settings(
        market_scan_enabled=True,
        theme_leadership_write_candidate_sources=True,
        theme_observable_coverage_enabled=False,
    )
    members = [(f"{100000 + index:06d}", f"스캔종목{index:02d}") for index in range(30)]
    import_theme_memberships(
        connection,
        _theme_payload("large_scan_theme", "대형스캔테마", members),
    )
    _project_market_scan(
        connection,
        "scan-flow",
        rows=[
            _scan_row(members[0][0], members[0][1], rank=1, change_rate=3.0),
            _scan_row(members[1][0], members[1][1], rank=2, change_rate=2.6),
            _scan_row(members[2][0], members[2][1], rank=3, change_rate=2.2),
        ],
        settings=settings,
    )

    legacy = calculate_theme_snapshot(connection, "large_scan_theme", settings=settings)
    result = rebuild_theme_leadership(
        connection,
        write_candidate_sources=True,
        settings=settings,
    )
    candidate_event_count = connection.execute(
        "SELECT COUNT(*) AS count FROM candidate_source_events"
    ).fetchone()["count"]
    connection.close()

    snapshot = result.snapshots[0]
    assert legacy.state == "DATA_WAIT"
    assert "LOW_FRESH_COVERAGE" in legacy.reason_codes
    assert legacy.scan_coverage_ratio < settings.theme_min_fresh_coverage_ratio
    assert snapshot.state is ThemeState.SPREADING
    assert snapshot.observable_member_count == 3
    assert snapshot.fresh_coverage_ratio == 1.0
    assert snapshot.full_fresh_coverage_ratio < settings.theme_min_fresh_coverage_ratio
    assert "LOW_FULL_SCAN_COVERAGE" in snapshot.reason_codes
    assert "LOW_FRESH_COVERAGE" not in snapshot.reason_codes
    assert result.watchset.items
    assert len(result.candidate_source_events) == candidate_event_count


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


def _project_market_scan(
    connection,
    suffix: str,
    *,
    rows: list[dict[str, object]],
    settings: Settings,
) -> None:
    event = GatewayEvent(
        event_id=f"evt_market_scan_{suffix}",
        event_type="tr_response",
        source="mock_gateway",
        payload={
            "request_id": f"market_scan:TRADE_VALUE:KOSPI:{suffix}",
            "tr_code": "OPT10032",
            "request_name": "market_scan_trade_value_kospi",
            "success": True,
            "rows": rows,
        },
    )
    result = process_market_scan_event(connection, event, settings=settings)
    assert result.status == "APPLIED"


def _scan_row(
    code: str,
    name: str,
    *,
    rank: int,
    change_rate: float,
) -> dict[str, object]:
    return {
        "code": code,
        "name": name,
        "rank": rank,
        "price": 10_000,
        "change_rate": change_rate,
        "trade_value": 500_000_000 - rank * 10_000_000,
        "volume": 100_000 - rank,
    }


def _leadership_snapshot(*, rank: int, state: ThemeState) -> ThemeLeadershipSnapshot:
    code = f"{100000 + rank:06d}"
    member = ThemeMemberLeadership(
        code=code,
        name=f"종목{rank}",
        role=StockRole.LEADER,
        member_score=100.0 - rank,
        change_rate_pct=2.0,
        turnover_krw=100_000_000.0,
        execution_strength=110.0,
        momentum_1m=1.0,
        momentum_3m=2.0,
        momentum_5m=3.0,
        vwap=10_000.0,
        pullback_from_high_pct=1.0,
        stale=False,
        reason_codes=[],
    )
    return ThemeLeadershipSnapshot(
        theme_id=f"theme-{rank}",
        theme_name=f"테마{rank}",
        state=state,
        score=1000.0 - rank,
        rank=rank,
        observable_member_count=1,
        valid_member_count=1,
        fresh_member_count=1,
        fresh_coverage_ratio=1.0,
        rising_count=1,
        rising_ratio=1.0,
        leader_count=1,
        co_leader_count=0,
        follower_count=0,
        total_turnover_krw=100_000_000.0,
        turnover_share=0.1,
        weighted_return_pct=2.0,
        leader_code=code,
        leader_name=f"종목{rank}",
        members=[member],
        reason_codes=[],
    )


def _install_fake_rebuild_inputs(
    service: ThemeLeadershipService,
    snapshots: list[ThemeLeadershipSnapshot],
) -> None:
    class FakeUniverseBuilder:
        def build(self, connection):
            return []

    class FakeSnapshotBuilder:
        def build_for_universe(self, connection, universe):
            return {}

    class FakeRanker:
        def rank(self, universe, stock_snapshots, *, created_at=None):
            return snapshots

    service.universe_builder = FakeUniverseBuilder()
    service.snapshot_builder = FakeSnapshotBuilder()
    service.ranker = FakeRanker()


def _append_and_project(
    connection,
    event: GatewayEvent,
    settings: Settings,
) -> None:
    append_result = append_gateway_event(connection, event)
    assert append_result.status == "ACCEPTED"
    result = process_gateway_event(connection, event, settings=settings)
    assert result.status == "APPLIED"
