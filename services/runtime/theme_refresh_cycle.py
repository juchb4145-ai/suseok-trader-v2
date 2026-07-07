from __future__ import annotations

import sqlite3
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from json import loads
from typing import Any

from domain.broker.utils import datetime_to_wire, new_message_id, normalize_value, utc_now
from storage.gateway_command_store import canonical_json, get_command_type_counts

from services.candidate_quote_refresh import run_candidate_quote_refresh_once
from services.config import Settings, load_settings
from services.market_scan_service import run_market_scan_once
from services.realtime_subscription import run_realtime_subscription_once
from services.runtime.evaluation_run_guard import runtime_execution_lock
from services.runtime.incremental_evaluation import (
    enqueue_incremental_evaluation_for_fresh_candidates,
)
from services.theme_leadership import rebuild_theme_leadership
from services.theme_service import calculate_all_theme_snapshots

THEME_REFRESH_LOCK = "theme_refresh"


@dataclass(frozen=True, kw_only=True)
class ThemeRefreshCycleRunResult:
    run_id: str
    trade_date: str | None
    status: str
    market_scan: Mapping[str, Any]
    theme_snapshots: Mapping[str, Any]
    leadership: Mapping[str, Any]
    realtime_subscription: Mapping[str, Any]
    candidate_quote_refresh: Mapping[str, Any]
    incremental_backfill: Mapping[str, Any]
    command_type_counts_before: Mapping[str, int]
    command_type_counts_after: Mapping[str, int]
    errors: Sequence[Mapping[str, Any]] = field(default_factory=tuple)
    created_at: str = field(default_factory=lambda: datetime_to_wire(utc_now()))
    observe_only: bool = True
    no_order_side_effects: bool = True
    live_real_allowed: bool = False

    @property
    def order_command_delta(self) -> dict[str, int]:
        return _command_delta(
            _order_command_counts(self.command_type_counts_before),
            _order_command_counts(self.command_type_counts_after),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "trade_date": self.trade_date,
            "status": self.status,
            "market_scan": normalize_value(dict(self.market_scan)),
            "theme_snapshots": normalize_value(dict(self.theme_snapshots)),
            "leadership": normalize_value(dict(self.leadership)),
            "realtime_subscription": normalize_value(dict(self.realtime_subscription)),
            "candidate_quote_refresh": normalize_value(dict(self.candidate_quote_refresh)),
            "incremental_backfill": normalize_value(dict(self.incremental_backfill)),
            "command_type_counts_before": dict(self.command_type_counts_before),
            "command_type_counts_after": dict(self.command_type_counts_after),
            "gateway_command_delta": _command_delta(
                self.command_type_counts_before,
                self.command_type_counts_after,
            ),
            "order_command_delta": self.order_command_delta,
            "errors": normalize_value(list(self.errors)),
            "created_at": self.created_at,
            "observe_only": True,
            "not_order_intent": True,
            "no_order_side_effects": self.no_order_side_effects,
            "live_real_allowed": False,
            "real_order_allowed": False,
        }


def run_theme_refresh_cycle_once(
    connection: sqlite3.Connection,
    *,
    trade_date: str | None = None,
    settings: Settings | None = None,
    queue_market_scan_commands: bool | None = None,
    queue_realtime_commands: bool | None = None,
) -> ThemeRefreshCycleRunResult:
    resolved_settings = settings or load_settings()
    with runtime_execution_lock(
        connection,
        THEME_REFRESH_LOCK,
        details={"run_type": "theme_refresh_cycle", "trade_date": trade_date},
    ):
        result = _run_theme_refresh_cycle_once(
            connection,
            trade_date=trade_date,
            settings=resolved_settings,
            queue_market_scan_commands=queue_market_scan_commands,
            queue_realtime_commands=queue_realtime_commands,
        )
        store_theme_refresh_cycle_run(connection, result)
        return result


def store_theme_refresh_cycle_run(
    connection: sqlite3.Connection,
    result: ThemeRefreshCycleRunResult,
) -> None:
    payload = result.to_dict()
    started = not connection.in_transaction
    if started:
        connection.execute("BEGIN IMMEDIATE")
    try:
        connection.execute(
            """
            INSERT INTO theme_refresh_cycle_runs (
                run_id,
                trade_date,
                status,
                created_at,
                order_command_delta_json,
                gateway_command_delta_json,
                no_order_side_effects,
                result_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id) DO UPDATE SET
                trade_date = excluded.trade_date,
                status = excluded.status,
                created_at = excluded.created_at,
                order_command_delta_json = excluded.order_command_delta_json,
                gateway_command_delta_json = excluded.gateway_command_delta_json,
                no_order_side_effects = excluded.no_order_side_effects,
                result_json = excluded.result_json
            """,
            (
                result.run_id,
                result.trade_date,
                result.status,
                result.created_at,
                canonical_json(payload["order_command_delta"]),
                canonical_json(payload["gateway_command_delta"]),
                1 if result.no_order_side_effects else 0,
                canonical_json(payload),
            ),
        )
        if started:
            connection.commit()
    except Exception:
        if started:
            connection.rollback()
        raise


def get_latest_theme_refresh_cycle_run(
    connection: sqlite3.Connection,
) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT *
        FROM theme_refresh_cycle_runs
        ORDER BY created_at DESC, run_id DESC
        LIMIT 1
        """
    ).fetchone()
    if row is None:
        return None
    return _theme_refresh_run_row_to_dict(row)


def _run_theme_refresh_cycle_once(
    connection: sqlite3.Connection,
    *,
    trade_date: str | None,
    settings: Settings,
    queue_market_scan_commands: bool | None,
    queue_realtime_commands: bool | None,
) -> ThemeRefreshCycleRunResult:
    run_id = new_message_id("theme_refresh_cycle_run")
    before = get_command_type_counts(connection)
    errors: list[dict[str, Any]] = []
    market_scan_payload: dict[str, Any] = {}
    theme_payload: dict[str, Any] = {}
    leadership_payload: dict[str, Any] = {}
    subscription_payload: dict[str, Any] = {}
    quote_refresh_payload: dict[str, Any] = {}
    incremental_backfill_payload: dict[str, Any] = {}

    try:
        market_scan_payload = run_market_scan_once(
            connection,
            settings=settings,
            queue_commands=(
                settings.market_scan_enabled
                if queue_market_scan_commands is None
                else queue_market_scan_commands
            ),
        ).to_dict()
    except Exception as exc:
        errors.append({"stage": "MarketScan", "error": str(exc)})
        market_scan_payload = {"status": "ERROR", "error": str(exc)}

    try:
        theme_payload = calculate_all_theme_snapshots(connection, settings=settings).to_dict()
    except Exception as exc:
        errors.append({"stage": "ThemeSnapshot", "error": str(exc)})
        theme_payload = {"status": "ERROR", "error": str(exc)}

    watchset_codes: list[str] = []
    try:
        leadership_result = rebuild_theme_leadership(
            connection,
            trade_date=trade_date,
            write_candidate_sources=settings.theme_leadership_write_candidate_sources,
            settings=settings,
        )
        leadership_payload = leadership_result.to_dict(include_members=False)
        watchset_codes = [item.code for item in leadership_result.watchset.items]
    except Exception as exc:
        errors.append({"stage": "ThemeLeadership", "error": str(exc)})
        leadership_payload = {"status": "ERROR", "error": str(exc)}

    try:
        subscription_payload = run_realtime_subscription_once(
            connection,
            trade_date=trade_date,
            manual_seed_codes=watchset_codes,
            settings=settings,
            queue_commands=(
                settings.realtime_subscription_queue_commands
                if queue_realtime_commands is None
                else queue_realtime_commands
            ),
        ).to_dict()
    except Exception as exc:
        errors.append({"stage": "RealtimeSubscription", "error": str(exc)})
        subscription_payload = {"status": "ERROR", "error": str(exc)}

    try:
        quote_refresh_payload = run_candidate_quote_refresh_once(
            connection,
            trade_date=trade_date,
            settings=settings,
            queue_commands=(
                settings.realtime_subscription_queue_commands
                if queue_realtime_commands is None
                else queue_realtime_commands
            ),
        ).to_dict()
    except Exception as exc:
        errors.append({"stage": "CandidateQuoteRefresh", "error": str(exc)})
        quote_refresh_payload = {"status": "ERROR", "error": str(exc)}

    try:
        incremental_backfill_payload = enqueue_incremental_evaluation_for_fresh_candidates(
            connection,
            trade_date=trade_date,
            settings=settings,
        ).to_dict()
    except Exception as exc:
        errors.append({"stage": "IncrementalBackfill", "error": str(exc)})
        incremental_backfill_payload = {"status": "ERROR", "error": str(exc)}

    after = get_command_type_counts(connection)
    order_delta = _command_delta(_order_command_counts(before), _order_command_counts(after))
    no_order_side_effects = all(value == 0 for value in order_delta.values())
    if not no_order_side_effects:
        errors.append(
            {
                "stage": "CommandSafety",
                "error": "order command was created during theme refresh cycle",
                "order_command_delta": order_delta,
            }
        )
    status = "COMPLETED" if not errors else "COMPLETED_WITH_ERRORS"
    return ThemeRefreshCycleRunResult(
        run_id=run_id,
        trade_date=trade_date,
        status=status,
        market_scan=market_scan_payload,
        theme_snapshots=theme_payload,
        leadership=leadership_payload,
        realtime_subscription=subscription_payload,
        candidate_quote_refresh=quote_refresh_payload,
        incremental_backfill=incremental_backfill_payload,
        command_type_counts_before=before,
        command_type_counts_after=after,
        errors=tuple(errors),
        no_order_side_effects=no_order_side_effects,
    )


def _order_command_counts(counts: Mapping[str, int]) -> dict[str, int]:
    send_type = "send" + "_order"
    cancel_type = "cancel" + "_order"
    amend_type = "modify" + "_order"
    command_types = (send_type, cancel_type, amend_type)
    return {command_type: int(counts.get(command_type, 0)) for command_type in command_types}


def _command_delta(
    before: Mapping[str, int],
    after: Mapping[str, int],
) -> dict[str, int]:
    keys = sorted(set(before) | set(after))
    return {key: int(after.get(key, 0)) - int(before.get(key, 0)) for key in keys}


def _theme_refresh_run_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    payload = loads(str(row["result_json"]))
    payload["run_id"] = str(row["run_id"])
    payload["trade_date"] = row["trade_date"]
    payload["status"] = str(row["status"])
    payload["created_at"] = str(row["created_at"])
    payload["order_command_delta"] = loads(str(row["order_command_delta_json"]))
    payload["gateway_command_delta"] = loads(str(row["gateway_command_delta_json"]))
    payload["no_order_side_effects"] = bool(row["no_order_side_effects"])
    return payload
