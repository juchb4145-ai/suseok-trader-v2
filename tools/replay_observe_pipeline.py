# ruff: noqa: E402
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from domain.broker.events import GatewayEvent
from domain.broker.utils import datetime_to_wire, parse_timestamp, utc_now
from domain.strategy.status import StrategyObservationStatus
from services.candidate_service import rebuild_candidates_from_observations
from services.config import Settings, TradingMode, TradingProfile, candidate_timezone, load_settings
from services.entry_timing.models import OrderPlanStatus
from services.entry_timing.service import evaluate_entry_timing
from services.market_data_service import clear_market_data_projection, process_gateway_event
from services.risk_gate import evaluate_risk_observations
from services.strategy_engine import evaluate_candidates
from services.theme_leadership import rebuild_theme_leadership
from services.theme_service import calculate_all_theme_snapshots
from storage.sqlite import initialize_database

DEFAULT_REPORT_ROOT = ROOT_DIR / "reports" / "replay"
REPLAY_STALE_SEC = 999_999_999
FORBIDDEN_EXACT_TABLES = frozenset(
    {
        "gateway_commands",
        "gateway_command_events",
        "gateway_command_dedupe_keys",
    }
)
FORBIDDEN_PREFIXES = ("dry_run_", "live_sim_")
ENTRY_HORIZONS_MINUTES = (5, 15, 30)

REPLAY_RESET_TABLES = (
    "market_index_ticks_latest",
    "market_index_tick_samples",
    "market_index_bars",
    "market_index_projection_errors",
    "market_scan_snapshots",
    "market_scan_latest",
    "market_scan_errors",
    "market_regime_snapshots",
    "theme_snapshots",
    "theme_snapshot_members",
    "theme_latest_snapshots",
    "theme_projection_errors",
    "candidate_condition_fusion",
    "candidate_context_latest",
    "candidate_sources_latest",
    "candidate_source_events",
    "candidate_state_transitions",
    "candidate_projection_errors",
    "candidates",
    "strategy_setup_observations",
    "strategy_observations_latest",
    "strategy_observations",
    "strategy_evaluation_runs",
    "strategy_evaluation_errors",
    "risk_check_observations",
    "risk_observations_latest",
    "risk_observations",
    "risk_evaluation_runs",
    "risk_evaluation_errors",
    "entry_timing_evaluations",
    "entry_timing_evaluation_errors",
    "order_plan_drafts_latest",
    "order_plan_drafts",
    "incremental_evaluation_queue",
    "market_open_observe_cycle_runs",
)


@dataclass(frozen=True, kw_only=True)
class ReplayEvent:
    event_rowid: int
    event_id: str
    event_type: str
    source: str
    event_ts: datetime
    received_at: str
    payload: Mapping[str, Any]
    command_id: str | None = None
    idempotency_key: str | None = None

    def to_gateway_event(self) -> GatewayEvent:
        return GatewayEvent(
            event_id=self.event_id,
            event_type=self.event_type,
            source=self.source,
            command_id=self.command_id,
            idempotency_key=self.idempotency_key,
            ts=self.event_ts,
            payload=self.payload,
        )


@dataclass(frozen=True, kw_only=True)
class VirtualEntrySignal:
    signal_id: str
    event_id: str
    signal_ts: datetime
    code: str
    name: str
    candidate_instance_id: str
    setup_type: str
    entry_timing_state: str
    status: str
    limit_price: float


@dataclass(frozen=True, kw_only=True)
class VirtualEntryReturn:
    signal: VirtualEntrySignal
    fill_ts: str | None
    fill_price: float | None
    returns_pct: Mapping[int, float | None]


@dataclass(frozen=True, kw_only=True)
class ReplayRunResult:
    trade_date: str
    db_path: Path
    report_path: Path
    event_count: int
    processed_event_count: int
    market_applied_count: int
    market_ignored_count: int
    market_error_count: int
    pipeline_run_count: int
    matched_by_setup: Mapping[str, int]
    observe_pass_count: int
    matched_observation_count: int
    risk_evaluated_count: int
    virtual_entry_count: int
    return_distributions: Mapping[int, Mapping[str, float | int | None]]
    top_reason_codes: Sequence[tuple[str, int]]
    forbidden_table_counts_before: Mapping[str, int]
    forbidden_table_counts_after: Mapping[str, int]
    forbidden_table_delta: Mapping[str, int]
    started_at: str = field(default_factory=lambda: datetime_to_wire(utc_now()))
    completed_at: str = field(default_factory=lambda: datetime_to_wire(utc_now()))

    @property
    def observe_pass_rate(self) -> float:
        if self.matched_observation_count <= 0:
            return 0.0
        return self.observe_pass_count / self.matched_observation_count

    @property
    def no_forbidden_writes(self) -> bool:
        return all(delta == 0 for delta in self.forbidden_table_delta.values())

    def to_dict(self) -> dict[str, Any]:
        return {
            "trade_date": self.trade_date,
            "db_path": str(self.db_path),
            "report_path": str(self.report_path),
            "event_count": self.event_count,
            "processed_event_count": self.processed_event_count,
            "market_applied_count": self.market_applied_count,
            "market_ignored_count": self.market_ignored_count,
            "market_error_count": self.market_error_count,
            "pipeline_run_count": self.pipeline_run_count,
            "matched_by_setup": dict(self.matched_by_setup),
            "observe_pass_count": self.observe_pass_count,
            "matched_observation_count": self.matched_observation_count,
            "risk_evaluated_count": self.risk_evaluated_count,
            "observe_pass_rate": self.observe_pass_rate,
            "virtual_entry_count": self.virtual_entry_count,
            "return_distributions": {
                str(key): dict(value) for key, value in self.return_distributions.items()
            },
            "top_reason_codes": [
                {"reason_code": reason, "count": count}
                for reason, count in self.top_reason_codes
            ],
            "forbidden_table_counts_before": dict(self.forbidden_table_counts_before),
            "forbidden_table_counts_after": dict(self.forbidden_table_counts_after),
            "forbidden_table_delta": dict(self.forbidden_table_delta),
            "no_forbidden_writes": self.no_forbidden_writes,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
        }


def replay_observe_pipeline(
    *,
    trade_date: str,
    db_path: str | Path,
    speed: float = 0.0,
    report_root: str | Path = DEFAULT_REPORT_ROOT,
    settings: Settings | None = None,
) -> ReplayRunResult:
    replay_db_path = _resolve_replay_db_path(db_path)
    base_settings = settings or load_settings()
    _reject_operational_db_path(replay_db_path, base_settings)
    replay_settings = _replay_settings(base_settings, replay_db_path)

    connection = initialize_database(replay_db_path)
    try:
        _install_forbidden_write_guard(connection)
        forbidden_before = _forbidden_table_counts(connection)
        _reset_replay_projections(connection)
        events = _load_replay_events(connection, trade_date, settings=replay_settings)

        reason_counter: Counter[str] = Counter()
        seen_entry_keys: set[str] = set()
        virtual_entries: list[VirtualEntrySignal] = []
        processed_count = 0
        market_applied = 0
        market_ignored = 0
        market_errors = 0
        pipeline_runs = 0
        previous_event_ts: datetime | None = None

        for replay_event in events:
            _sleep_for_speed(previous_event_ts, replay_event.event_ts, speed)
            previous_event_ts = replay_event.event_ts
            gateway_event = replay_event.to_gateway_event()
            market_result = process_gateway_event(
                connection,
                gateway_event,
                settings=replay_settings,
            )
            processed_count += 1
            market_applied += market_result.applied_count
            market_ignored += market_result.ignored_count
            market_errors += market_result.error_count

            entry_result = _run_observe_pipeline_once(
                connection,
                trade_date=trade_date,
                settings=replay_settings,
            )
            pipeline_runs += 1
            reason_counter.update(_entry_result_reason_codes(entry_result))
            virtual_entries.extend(
                _new_virtual_entries(
                    entry_result,
                    replay_event=replay_event,
                    seen_entry_keys=seen_entry_keys,
                )
            )

        db_summary = _database_summary(connection, trade_date=trade_date)
        reason_counter.update(db_summary["reason_codes"])
        virtual_returns = _evaluate_virtual_entry_returns(connection, virtual_entries)
        distributions = _return_distributions(virtual_returns)
        forbidden_after = _forbidden_table_counts(connection)
    finally:
        connection.close()

    forbidden_delta = _count_delta(forbidden_before, forbidden_after)
    result_without_report = ReplayRunResult(
        trade_date=trade_date,
        db_path=replay_db_path,
        report_path=Path(),
        event_count=len(events),
        processed_event_count=processed_count,
        market_applied_count=market_applied,
        market_ignored_count=market_ignored,
        market_error_count=market_errors,
        pipeline_run_count=pipeline_runs,
        matched_by_setup=db_summary["matched_by_setup"],
        observe_pass_count=db_summary["observe_pass_count"],
        matched_observation_count=db_summary["matched_observation_count"],
        risk_evaluated_count=db_summary["risk_evaluated_count"],
        virtual_entry_count=len(virtual_entries),
        return_distributions=distributions,
        top_reason_codes=reason_counter.most_common(20),
        forbidden_table_counts_before=forbidden_before,
        forbidden_table_counts_after=forbidden_after,
        forbidden_table_delta=forbidden_delta,
    )
    report_path = write_replay_summary(result_without_report, report_root=report_root)
    result = replace(result_without_report, report_path=report_path)
    if not result.no_forbidden_writes:
        raise RuntimeError(
            "Replay touched forbidden GatewayCommand/LIVE_SIM/DRY_RUN tables: "
            f"{dict(result.forbidden_table_delta)}"
        )
    return result


def write_replay_summary(
    result: ReplayRunResult,
    *,
    report_root: str | Path = DEFAULT_REPORT_ROOT,
) -> Path:
    output_dir = Path(report_root) / result.trade_date
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "summary.md"
    path.write_text(_render_summary_markdown(result), encoding="utf-8")
    return path


def _run_observe_pipeline_once(
    connection: sqlite3.Connection,
    *,
    trade_date: str,
    settings: Settings,
):
    calculate_all_theme_snapshots(connection, settings=settings)
    rebuild_theme_leadership(
        connection,
        trade_date=trade_date,
        write_candidate_sources=True,
        settings=settings,
    )
    rebuild_candidates_from_observations(connection, trade_date=trade_date, settings=settings)
    evaluate_candidates(
        connection,
        trade_date=trade_date,
        candidate_state=None,
        settings=settings,
        manage_run_lock=False,
    )
    evaluate_risk_observations(
        connection,
        trade_date=trade_date,
        strategy_status=StrategyObservationStatus.MATCHED_OBSERVATION,
        settings=settings,
        manage_run_lock=False,
    )
    return evaluate_entry_timing(
        connection,
        trade_date=trade_date,
        write_order_plan_drafts=True,
        settings=settings,
        manage_run_lock=False,
    )


def _replay_settings(settings: Settings, db_path: Path) -> Settings:
    return replace(
        settings,
        trading_profile=TradingProfile.OBSERVE,
        trading_mode=TradingMode.OBSERVE,
        trading_db_path=db_path,
        trading_allow_live_sim=False,
        trading_allow_live_real=False,
        market_data_tick_stale_sec=REPLAY_STALE_SEC,
        market_data_degraded_tick_stale_sec=REPLAY_STALE_SEC,
        market_index_stale_sec=REPLAY_STALE_SEC,
        market_scan_enabled=False,
        realtime_subscription_enabled=False,
        realtime_subscription_queue_commands=False,
        realtime_subscription_stale_sec=REPLAY_STALE_SEC,
        realtime_subscription_remove_stale_after_sec=REPLAY_STALE_SEC,
        theme_snapshot_stale_sec=REPLAY_STALE_SEC,
        theme_leadership_write_candidate_sources=True,
        candidate_source_stale_sec=REPLAY_STALE_SEC,
        candidate_tick_stale_sec=REPLAY_STALE_SEC,
        candidate_episode_ttl_sec=REPLAY_STALE_SEC,
        strategy_engine_stale_tick_sec=REPLAY_STALE_SEC,
        risk_gate_stale_tick_sec=REPLAY_STALE_SEC,
        risk_gate_strategy_stale_sec=REPLAY_STALE_SEC,
        risk_gate_observation_cooldown_sec=0,
        entry_timing_write_order_plan_drafts=True,
        entry_timing_stale_max_seconds=REPLAY_STALE_SEC,
        dry_run_oms_enabled=False,
        dry_run_intent_creation_enabled=False,
        dry_run_simulated_fill_enabled=False,
        dry_run_order_routing_enabled=False,
        dry_run_gateway_command_enabled=False,
        dry_run_exit_engine_enabled=False,
        dry_run_exit_intent_creation_enabled=False,
        dry_run_exit_order_creation_enabled=False,
        dry_run_exit_simulated_fill_enabled=False,
        dry_run_exit_order_routing_enabled=False,
        dry_run_exit_gateway_command_enabled=False,
        live_sim_enabled=False,
        live_sim_order_routing_enabled=False,
        live_sim_gateway_command_enabled=False,
        live_sim_kill_switch=False,
        live_sim_pilot_pipeline_enabled=False,
        live_sim_pilot_auto_queue_command=False,
        live_sim_order_plan_routing_enabled=False,
        live_sim_cancel_enabled=False,
        live_sim_cancel_unfilled_enabled=False,
        live_sim_exit_engine_enabled=False,
        live_sim_exit_order_creation_enabled=False,
        live_sim_exit_gateway_command_enabled=False,
        live_sim_reconcile_request_broker_snapshot_enabled=False,
        live_sim_operating_cycle_enabled=False,
        live_sim_operating_write_runs=False,
        live_sim_position_allow_scale_in=True,
        live_sim_max_order_notional=1_000_000_000,
        live_sim_max_daily_order_count=1_000_000,
        live_sim_max_daily_notional=1_000_000_000_000,
        live_sim_max_active_orders=1_000_000,
        live_sim_max_active_positions=1_000_000,
    )


def _resolve_replay_db_path(db_path: str | Path) -> Path:
    path = Path(db_path).expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(f"replay db-path does not exist: {path}")
    if not path.is_file():
        raise ValueError(f"replay db-path must be a sqlite file: {path}")
    return path


def _reject_operational_db_path(db_path: Path, settings: Settings) -> None:
    operational_path = Path(settings.trading_db_path).expanduser().resolve()
    if db_path == operational_path:
        raise ValueError(
            "--db-path must point to a replay sqlite copy, not the configured operational DB"
        )


def _install_forbidden_write_guard(connection: sqlite3.Connection) -> None:
    write_actions = {
        sqlite3.SQLITE_INSERT,
        sqlite3.SQLITE_UPDATE,
        sqlite3.SQLITE_DELETE,
        sqlite3.SQLITE_DROP_TABLE,
        sqlite3.SQLITE_ALTER_TABLE,
    }

    def authorizer(
        action: int,
        arg1: str | None,
        arg2: str | None,
        database: str | None,
        trigger: str | None,
    ) -> int:
        del arg2, database, trigger
        if action in write_actions and _is_forbidden_table(arg1):
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    connection.set_authorizer(authorizer)


def _is_forbidden_table(table_name: str | None) -> bool:
    normalized = str(table_name or "").lower()
    return normalized in FORBIDDEN_EXACT_TABLES or normalized.startswith(FORBIDDEN_PREFIXES)


def _forbidden_table_counts(connection: sqlite3.Connection) -> dict[str, int]:
    return {
        table_name: _count_rows(connection, table_name)
        for table_name in _existing_tables(connection)
        if _is_forbidden_table(table_name)
    }


def _existing_tables(connection: sqlite3.Connection) -> list[str]:
    rows = connection.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
        ORDER BY name ASC
        """
    ).fetchall()
    return [str(row["name"]) for row in rows]


def _reset_replay_projections(connection: sqlite3.Connection) -> None:
    clear_market_data_projection(connection)
    try:
        connection.execute("BEGIN IMMEDIATE")
        existing = set(_existing_tables(connection))
        for table_name in REPLAY_RESET_TABLES:
            if table_name in existing:
                connection.execute(f"DELETE FROM {table_name}")
        connection.commit()
    except Exception:
        connection.rollback()
        raise


def _load_replay_events(
    connection: sqlite3.Connection,
    trade_date: str,
    *,
    settings: Settings,
) -> list[ReplayEvent]:
    rows = connection.execute(
        """
        SELECT
            rowid AS event_rowid,
            event_id,
            event_type,
            source,
            command_id,
            idempotency_key,
            event_ts,
            received_at,
            payload_json
        FROM gateway_events
        WHERE status = 'ACCEPTED'
            AND event_type IN ('price_tick', 'condition_event')
        """
    ).fetchall()
    timezone = candidate_timezone(settings.candidate_trade_date_timezone)
    events: list[ReplayEvent] = []
    for row in rows:
        event_ts = parse_timestamp(row["event_ts"], "event_ts")
        if event_ts.astimezone(timezone).date().isoformat() != trade_date:
            continue
        events.append(
            ReplayEvent(
                event_rowid=int(row["event_rowid"]),
                event_id=row["event_id"],
                event_type=row["event_type"],
                source=row["source"],
                command_id=row["command_id"],
                idempotency_key=row["idempotency_key"],
                event_ts=event_ts,
                received_at=row["received_at"],
                payload=json.loads(row["payload_json"]),
            )
        )
    events.sort(key=lambda item: (item.event_ts, item.received_at, item.event_rowid))
    return events


def _sleep_for_speed(
    previous_event_ts: datetime | None,
    event_ts: datetime,
    speed: float,
) -> None:
    if speed <= 0 or previous_event_ts is None:
        return
    gap = max((event_ts - previous_event_ts).total_seconds(), 0.0)
    if gap > 0:
        time.sleep(gap / speed)


def _entry_result_reason_codes(entry_result: Any) -> Counter[str]:
    counter: Counter[str] = Counter()
    for evaluation in getattr(entry_result, "evaluations", ()):
        counter.update(str(reason).upper() for reason in evaluation.reason_codes)
    for draft in getattr(entry_result, "order_plan_drafts", ()):
        counter.update(str(reason).upper() for reason in draft.reason_codes)
    return counter


def _new_virtual_entries(
    entry_result: Any,
    *,
    replay_event: ReplayEvent,
    seen_entry_keys: set[str],
) -> list[VirtualEntrySignal]:
    signals: list[VirtualEntrySignal] = []
    for draft in getattr(entry_result, "order_plan_drafts", ()):
        if draft.status is not OrderPlanStatus.PLAN_READY:
            continue
        signal_id = draft.idempotency_key
        if signal_id in seen_entry_keys:
            continue
        seen_entry_keys.add(signal_id)
        signals.append(
            VirtualEntrySignal(
                signal_id=signal_id,
                event_id=replay_event.event_id,
                signal_ts=replay_event.event_ts,
                code=draft.code,
                name=draft.name,
                candidate_instance_id=draft.candidate_instance_id,
                setup_type=draft.setup_type.value,
                entry_timing_state=draft.entry_timing_state.value,
                status=draft.status.value,
                limit_price=float(draft.limit_price),
            )
        )
    return signals


def _database_summary(
    connection: sqlite3.Connection,
    *,
    trade_date: str,
) -> dict[str, Any]:
    matched_by_setup = _matched_by_setup(connection, trade_date=trade_date)
    matched_count = sum(matched_by_setup.values())
    risk_status_counts = _status_counts(
        connection,
        "risk_observations",
        "overall_status",
        trade_date=trade_date,
    )
    return {
        "matched_by_setup": matched_by_setup,
        "matched_observation_count": matched_count,
        "risk_evaluated_count": sum(risk_status_counts.values()),
        "observe_pass_count": int(risk_status_counts.get("OBSERVE_PASS", 0)),
        "reason_codes": _reason_codes_from_db(connection, trade_date=trade_date),
    }


def _matched_by_setup(connection: sqlite3.Connection, *, trade_date: str) -> dict[str, int]:
    rows = connection.execute(
        """
        SELECT COALESCE(primary_setup_type, 'UNKNOWN') AS setup_type, COUNT(*) AS count
        FROM strategy_observations
        WHERE trade_date = ?
            AND overall_status = 'MATCHED_OBSERVATION'
        GROUP BY COALESCE(primary_setup_type, 'UNKNOWN')
        ORDER BY count DESC, setup_type ASC
        """,
        (trade_date,),
    ).fetchall()
    return {str(row["setup_type"]): int(row["count"] or 0) for row in rows}


def _status_counts(
    connection: sqlite3.Connection,
    table_name: str,
    column_name: str,
    *,
    trade_date: str,
) -> dict[str, int]:
    rows = connection.execute(
        f"""
        SELECT {column_name} AS status, COUNT(*) AS count
        FROM {table_name}
        WHERE trade_date = ?
        GROUP BY {column_name}
        """,
        (trade_date,),
    ).fetchall()
    return {str(row["status"]): int(row["count"] or 0) for row in rows}


def _reason_codes_from_db(connection: sqlite3.Connection, *, trade_date: str) -> Counter[str]:
    counter: Counter[str] = Counter()
    specs = (
        ("candidates", "reason_codes_json"),
        ("candidate_source_events", "reason_codes_json"),
        ("strategy_observations", "reason_codes_json"),
        ("strategy_setup_observations", "reason_codes_json"),
        ("risk_observations", "reason_codes_json"),
        ("risk_check_observations", "reason_codes_json"),
        ("entry_timing_evaluations", "reason_codes_json"),
        ("order_plan_drafts", "reason_codes_json"),
    )
    existing = set(_existing_tables(connection))
    for table_name, column_name in specs:
        if table_name not in existing:
            continue
        query = f"SELECT {column_name} FROM {table_name}"
        params: tuple[str, ...] = ()
        if _table_has_column(connection, table_name, "trade_date"):
            query += " WHERE trade_date = ?"
            params = (trade_date,)
        for row in connection.execute(query, params).fetchall():
            counter.update(_json_reason_codes(row[column_name]))
    return counter


def _table_has_column(connection: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    rows = connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    return any(str(row["name"]) == column_name for row in rows)


def _json_reason_codes(value: object) -> list[str]:
    if not value:
        return []
    try:
        loaded = json.loads(str(value))
    except json.JSONDecodeError:
        return []
    if not isinstance(loaded, Sequence) or isinstance(loaded, str):
        return []
    return [str(item).upper() for item in loaded if str(item).strip()]


def _evaluate_virtual_entry_returns(
    connection: sqlite3.Connection,
    signals: Sequence[VirtualEntrySignal],
) -> list[VirtualEntryReturn]:
    ticks_by_code = _ticks_by_code(connection)
    results: list[VirtualEntryReturn] = []
    for signal in signals:
        ticks = ticks_by_code.get(signal.code, [])
        fill_tick = _first_tick_after(ticks, signal.signal_ts)
        if fill_tick is None:
            results.append(
                VirtualEntryReturn(
                    signal=signal,
                    fill_ts=None,
                    fill_price=None,
                    returns_pct={minutes: None for minutes in ENTRY_HORIZONS_MINUTES},
                )
            )
            continue
        fill_price = float(fill_tick["price"])
        fill_ts = fill_tick["event_ts"]
        returns: dict[int, float | None] = {}
        for minutes in ENTRY_HORIZONS_MINUTES:
            horizon_tick = _first_tick_at_or_after(
                ticks,
                parse_timestamp(fill_ts, "fill_ts") + timedelta(minutes=minutes),
            )
            returns[minutes] = (
                None
                if horizon_tick is None or fill_price <= 0
                else (float(horizon_tick["price"]) - fill_price) / fill_price * 100.0
            )
        results.append(
            VirtualEntryReturn(
                signal=signal,
                fill_ts=fill_ts,
                fill_price=fill_price,
                returns_pct=returns,
            )
        )
    return results


def _ticks_by_code(connection: sqlite3.Connection) -> dict[str, list[dict[str, Any]]]:
    rows = connection.execute(
        """
        SELECT code, price, event_ts, event_id
        FROM market_tick_samples
        ORDER BY event_ts ASC, event_id ASC
        """
    ).fetchall()
    ticks: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        ticks[str(row["code"])].append(
            {
                "code": row["code"],
                "price": float(row["price"]),
                "event_ts": row["event_ts"],
                "event_id": row["event_id"],
            }
        )
    return ticks


def _first_tick_after(
    ticks: Sequence[Mapping[str, Any]],
    timestamp: datetime,
) -> Mapping[str, Any] | None:
    for tick in ticks:
        if parse_timestamp(tick["event_ts"], "event_ts") > timestamp:
            return tick
    return None


def _first_tick_at_or_after(
    ticks: Sequence[Mapping[str, Any]],
    timestamp: datetime,
) -> Mapping[str, Any] | None:
    for tick in ticks:
        if parse_timestamp(tick["event_ts"], "event_ts") >= timestamp:
            return tick
    return None


def _return_distributions(
    returns: Sequence[VirtualEntryReturn],
) -> dict[int, dict[str, float | int | None]]:
    distributions: dict[int, dict[str, float | int | None]] = {}
    for minutes in ENTRY_HORIZONS_MINUTES:
        values = [
            float(value)
            for item in returns
            for value in (item.returns_pct.get(minutes),)
            if value is not None
        ]
        distributions[minutes] = _distribution(values)
    return distributions


def _distribution(values: Sequence[float]) -> dict[str, float | int | None]:
    if not values:
        return {
            "count": 0,
            "min": None,
            "p25": None,
            "median": None,
            "mean": None,
            "p75": None,
            "max": None,
        }
    ordered = sorted(values)
    return {
        "count": len(ordered),
        "min": round(ordered[0], 6),
        "p25": round(_percentile(ordered, 0.25), 6),
        "median": round(_percentile(ordered, 0.50), 6),
        "mean": round(sum(ordered) / len(ordered), 6),
        "p75": round(_percentile(ordered, 0.75), 6),
        "max": round(ordered[-1], 6),
    }


def _percentile(values: Sequence[float], percentile: float) -> float:
    if len(values) == 1:
        return values[0]
    position = (len(values) - 1) * percentile
    lower = int(position)
    upper = min(lower + 1, len(values) - 1)
    weight = position - lower
    return values[lower] * (1 - weight) + values[upper] * weight


def _render_summary_markdown(result: ReplayRunResult) -> str:
    lines = [
        "# Replay Observe Pipeline Summary",
        "",
        f"- trade_date: `{result.trade_date}`",
        f"- db_path: `{result.db_path}`",
        f"- processed_event_count: `{result.processed_event_count}` / `{result.event_count}`",
        f"- pipeline_run_count: `{result.pipeline_run_count}`",
        f"- market_applied_count: `{result.market_applied_count}`",
        f"- market_ignored_count: `{result.market_ignored_count}`",
        f"- market_error_count: `{result.market_error_count}`",
        f"- no_forbidden_writes: `{result.no_forbidden_writes}`",
        "",
        "## Matched Observation By Setup",
        "",
        "| Setup | MATCHED_OBSERVATION |",
        "| --- | ---: |",
    ]
    if result.matched_by_setup:
        for setup, count in result.matched_by_setup.items():
            lines.append(f"| {_md(setup)} | {count} |")
    else:
        lines.append("| None | 0 |")
    lines.extend(
        [
            "",
            "## OBSERVE_PASS Conversion",
            "",
            f"- matched_observation_count: `{result.matched_observation_count}`",
            f"- risk_evaluated_count: `{result.risk_evaluated_count}`",
            f"- observe_pass_count: `{result.observe_pass_count}`",
            f"- observe_pass_rate: `{result.observe_pass_rate:.2%}`",
            "",
            "## Virtual Entry Return Distribution",
            "",
            "- fill assumption: next price_tick after PLAN_READY signal",
            "",
            "| Horizon | Count | Min % | P25 % | Median % | Mean % | P75 % | Max % |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for minutes in ENTRY_HORIZONS_MINUTES:
        dist = result.return_distributions.get(minutes, {})
        lines.append(
            "| {horizon}m | {count} | {min} | {p25} | {median} | {mean} | {p75} | {max} |".format(
                horizon=minutes,
                count=_cell(dist.get("count")),
                min=_cell(dist.get("min")),
                p25=_cell(dist.get("p25")),
                median=_cell(dist.get("median")),
                mean=_cell(dist.get("mean")),
                p75=_cell(dist.get("p75")),
                max=_cell(dist.get("max")),
            )
        )
    lines.extend(
        [
            "",
            f"- virtual_entry_count: `{result.virtual_entry_count}`",
            "",
            "## Top Reason Codes",
            "",
            "| Reason code | Count |",
            "| --- | ---: |",
        ]
    )
    if result.top_reason_codes:
        for reason, count in result.top_reason_codes:
            lines.append(f"| `{_md(reason)}` | {count} |")
    else:
        lines.append("| None | 0 |")
    lines.extend(
        [
            "",
            "## Forbidden Table Delta",
            "",
            "| Table | Before | After | Delta |",
            "| --- | ---: | ---: | ---: |",
        ]
    )
    for table_name in sorted(result.forbidden_table_counts_before):
        before = result.forbidden_table_counts_before.get(table_name, 0)
        after = result.forbidden_table_counts_after.get(table_name, 0)
        delta = result.forbidden_table_delta.get(table_name, 0)
        lines.append(
            f"| {_md(table_name)} | {before} | {after} | {delta} |"
        )
    lines.append("")
    return "\n".join(lines)


def _count_rows(connection: sqlite3.Connection, table_name: str) -> int:
    row = connection.execute(f"SELECT COUNT(*) AS count FROM {table_name}").fetchone()
    return int(row["count"] if row else 0)


def _count_delta(
    before: Mapping[str, int],
    after: Mapping[str, int],
) -> dict[str, int]:
    return {key: int(after.get(key, 0)) - int(before.get(key, 0)) for key in sorted(before)}


def _cell(value: object) -> str:
    if value is None:
        return "-"
    return str(value)


def _md(value: object) -> str:
    return _cell(value).replace("|", "\\|").replace("\n", " ")


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Replay historical gateway price_tick/condition_event rows and evaluate the "
            "observe-only Theme->Candidate->Strategy->Risk->EntryTiming pipeline."
        )
    )
    parser.add_argument("--trade-date", required=True)
    parser.add_argument(
        "--db-path",
        required=True,
        help="Path to a replay sqlite copy. The configured operational DB is rejected.",
    )
    parser.add_argument(
        "--speed",
        type=float,
        default=0.0,
        help="Replay speed multiplier. 0 means max speed; 1 means wall-clock event gaps.",
    )
    args = parser.parse_args()
    if args.speed < 0:
        parser.error("--speed must be >= 0")

    result = replay_observe_pipeline(
        trade_date=args.trade_date,
        db_path=args.db_path,
        speed=args.speed,
    )
    print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
