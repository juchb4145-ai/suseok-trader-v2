from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from domain.broker.utils import normalize_value, parse_timestamp, utc_now
from storage.event_store import get_gateway_status_values
from storage.gateway_command_store import get_command_status_counts

from services.ai_advisory.storage import build_status as build_ai_advisory_status
from services.config import Settings, TradingMode, load_settings
from services.entry_timing.service import get_entry_timing_status
from services.live_sim.live_sim_service import (
    get_latest_live_sim_reconcile,
    get_live_sim_status,
)
from services.live_sim.safety_gate import check_live_sim_safety_gate, is_simulation_like
from services.operator.no_buy_sentinel import get_latest_no_buy_sentinel_snapshot
from services.theme_leadership import rebuild_theme_leadership


class OperatingMode(StrEnum):
    OBSERVE_CYCLE = "OBSERVE_CYCLE"
    PILOT_BUY_ONLY = "PILOT_BUY_ONLY"
    PILOT_FULL_LIFECYCLE = "PILOT_FULL_LIFECYCLE"
    PROTECT_ONLY = "PROTECT_ONLY"

    @classmethod
    def coerce(cls, value: OperatingMode | str | None, settings: Settings) -> OperatingMode:
        if value is None:
            value = settings.live_sim_operating_default_mode
        if isinstance(value, OperatingMode):
            return value
        return cls(str(value).strip().upper())

    @property
    def includes_buy(self) -> bool:
        return self in {OperatingMode.PILOT_BUY_ONLY, OperatingMode.PILOT_FULL_LIFECYCLE}

    @property
    def includes_lifecycle_commands(self) -> bool:
        return self in {OperatingMode.PILOT_FULL_LIFECYCLE, OperatingMode.PROTECT_ONLY}

    @property
    def observes_only(self) -> bool:
        return self is OperatingMode.OBSERVE_CYCLE


class PreflightStatus(StrEnum):
    PASS = "PASS"
    WARN = "WARN"
    BLOCK = "BLOCK"


@dataclass(frozen=True, kw_only=True)
class PreflightCheck:
    name: str
    status: PreflightStatus
    message: str
    details: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status.value,
            "message": self.message,
            "details": normalize_value(dict(self.details)),
        }


@dataclass(frozen=True, kw_only=True)
class LiveSimPreflightResult:
    status: PreflightStatus
    mode: OperatingMode
    queue_commands: bool
    checks: Sequence[PreflightCheck] = field(default_factory=tuple)
    warnings: Sequence[str] = field(default_factory=tuple)
    blocking_reasons: Sequence[str] = field(default_factory=tuple)
    counts: Mapping[str, Any] = field(default_factory=dict)
    gateway: Mapping[str, Any] = field(default_factory=dict)
    safety_gate: Mapping[str, Any] = field(default_factory=dict)
    latest_reconcile: Mapping[str, Any] | None = None
    ai_summary: Mapping[str, Any] = field(default_factory=dict)
    no_buy_summary: Mapping[str, Any] = field(default_factory=dict)
    live_sim_only: bool = True
    live_real_allowed: bool = False
    no_order_side_effects: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status.value,
            "mode": self.mode.value,
            "queue_commands": self.queue_commands,
            "checks": [check.to_dict() for check in self.checks],
            "warnings": list(self.warnings),
            "blocking_reasons": list(self.blocking_reasons),
            "counts": normalize_value(dict(self.counts)),
            "gateway": normalize_value(dict(self.gateway)),
            "safety_gate": normalize_value(dict(self.safety_gate)),
            "latest_reconcile": (
                None
                if self.latest_reconcile is None
                else normalize_value(dict(self.latest_reconcile))
            ),
            "ai_summary": normalize_value(dict(self.ai_summary)),
            "no_buy_summary": normalize_value(dict(self.no_buy_summary)),
            "live_sim_only": True,
            "live_real_allowed": False,
            "broker_order_path": "LIVE_SIM_ONLY",
            "real_order_allowed": False,
            "no_order_side_effects": True,
        }


def run_live_sim_preflight(
    connection: sqlite3.Connection,
    *,
    mode: OperatingMode | str | None = None,
    queue_commands: bool = False,
    trade_date: str | None = None,
    include_ai: bool = True,
    include_no_buy: bool = True,
    settings: Settings | None = None,
) -> LiveSimPreflightResult:
    resolved_settings = settings or load_settings()
    resolved_mode = OperatingMode.coerce(mode, resolved_settings)
    checks: list[PreflightCheck] = []

    def add(
        name: str,
        status: PreflightStatus,
        message: str,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        checks.append(
            PreflightCheck(
                name=name,
                status=status,
                message=message,
                details=dict(details or {}),
            )
        )

    try:
        connection.execute("SELECT 1").fetchone()
        add("core_db", PreflightStatus.PASS, "Core DB is accessible.")
    except sqlite3.Error as exc:
        add("core_db", PreflightStatus.BLOCK, "Core DB is not accessible.", {"error": str(exc)})

    gateway_values = _safe_gateway_status(connection)
    heartbeat_at = gateway_values.get("last_heartbeat_at")
    heartbeat_age_sec = _age_seconds(heartbeat_at)
    heartbeat_fresh = heartbeat_age_sec <= resolved_settings.live_sim_stale_tick_sec * 4
    add(
        "gateway_heartbeat",
        PreflightStatus.PASS if heartbeat_fresh else PreflightStatus.BLOCK,
        "Gateway heartbeat is fresh." if heartbeat_fresh else "Gateway heartbeat is stale.",
        {
            "last_heartbeat_at": heartbeat_at,
            "heartbeat_age_sec": None if heartbeat_age_sec == float("inf") else heartbeat_age_sec,
        },
    )

    gateway_orderable = _bool_value(gateway_values.get("gateway_orderable"))
    add(
        "gateway_orderable",
        PreflightStatus.PASS
        if gateway_orderable
        else PreflightStatus.BLOCK
        if queue_commands
        else PreflightStatus.WARN,
        "Gateway is orderable."
        if gateway_orderable
        else "Gateway is not orderable; command queueing is blocked when requested.",
        {"gateway_orderable": gateway_orderable},
    )

    live_real_disabled = (
        resolved_settings.trading_mode is not TradingMode.LIVE_REAL
        and not resolved_settings.live_real_allowed
    )
    add(
        "live_real_disabled",
        PreflightStatus.PASS if live_real_disabled else PreflightStatus.BLOCK,
        "LIVE_REAL is disabled." if live_real_disabled else "LIVE_REAL is enabled or allowed.",
        {
            "trading_mode": resolved_settings.trading_mode.value,
            "trading_allow_live_real": resolved_settings.live_real_allowed,
        },
    )

    account_configured = bool(resolved_settings.live_sim_account_id.strip())
    add(
        "live_sim_account_configured",
        PreflightStatus.PASS
        if account_configured
        else PreflightStatus.BLOCK
        if queue_commands
        else PreflightStatus.WARN,
        "LIVE_SIM account is configured."
        if account_configured
        else "LIVE_SIM account is not configured.",
        {"account_configured": account_configured},
    )

    account_mode = _gateway_or_config(
        gateway_values,
        "account_mode",
        resolved_settings.live_sim_account_mode,
    )
    broker_env = _gateway_or_config(
        gateway_values,
        "broker_env",
        resolved_settings.live_sim_broker_env,
    )
    server_mode = _gateway_or_config(
        gateway_values,
        "server_mode",
        resolved_settings.live_sim_server_mode,
    )
    simulation_like = all(
        is_simulation_like(value)
        for value in (
            account_mode,
            broker_env,
            server_mode,
            resolved_settings.live_sim_account_mode,
            resolved_settings.live_sim_broker_env,
            resolved_settings.live_sim_server_mode,
        )
    )
    add(
        "simulation_like_modes",
        PreflightStatus.PASS if simulation_like else PreflightStatus.BLOCK,
        "Account/server/broker modes are simulation-like."
        if simulation_like
        else "Account/server/broker modes are not simulation-like.",
        {
            "account_mode": account_mode,
            "broker_env": broker_env,
            "server_mode": server_mode,
            "configured_account_mode": resolved_settings.live_sim_account_mode,
            "configured_broker_env": resolved_settings.live_sim_broker_env,
            "configured_server_mode": resolved_settings.live_sim_server_mode,
        },
    )

    kill_switch = bool(resolved_settings.live_sim_kill_switch)
    add(
        "live_sim_kill_switch",
        PreflightStatus.PASS
        if not kill_switch
        else PreflightStatus.BLOCK
        if queue_commands
        else PreflightStatus.WARN,
        "LIVE_SIM kill switch is off."
        if not kill_switch
        else "LIVE_SIM kill switch is on; command queueing is blocked.",
        {"live_sim_kill_switch": kill_switch, "queue_commands": queue_commands},
    )

    enabled_details = {
        "operating_cycle_enabled": resolved_settings.live_sim_operating_cycle_enabled,
        "trading_allow_live_sim": resolved_settings.live_sim_allowed,
        "live_sim_enabled": resolved_settings.live_sim_enabled,
        "live_sim_order_routing_enabled": resolved_settings.live_sim_order_routing_enabled,
        "live_sim_gateway_command_enabled": resolved_settings.live_sim_gateway_command_enabled,
        "pilot_pipeline_enabled": resolved_settings.live_sim_pilot_pipeline_enabled,
        "order_plan_routing_enabled": resolved_settings.live_sim_order_plan_routing_enabled,
    }
    enabled_flags_ok = bool(
        enabled_details["operating_cycle_enabled"]
        and enabled_details["trading_allow_live_sim"]
        and enabled_details["live_sim_enabled"]
    )
    buy_pipeline_flags_ok = bool(
        resolved_settings.live_sim_pilot_pipeline_enabled
        and resolved_settings.live_sim_order_plan_routing_enabled
    )
    enabled_status = PreflightStatus.PASS
    if not resolved_settings.live_sim_operating_cycle_enabled:
        enabled_status = PreflightStatus.BLOCK
    elif not enabled_flags_ok or (resolved_mode.includes_buy and not buy_pipeline_flags_ok):
        enabled_status = (
            PreflightStatus.BLOCK
            if queue_commands and resolved_mode.includes_buy
            else PreflightStatus.WARN
        )
    add(
        "live_sim_enabled_flags",
        enabled_status,
        "LIVE_SIM operating flags are compatible with the requested mode."
        if enabled_status is PreflightStatus.PASS
        else "One or more LIVE_SIM operating flags are disabled.",
        enabled_details,
    )

    _add_theme_checks(connection, add, resolved_settings)
    _add_entry_timing_check(connection, add, resolved_settings, resolved_mode)

    safety_gate = _safe_safety_gate(connection, resolved_settings)
    safety_status = (
        PreflightStatus.PASS
        if safety_gate.get("passed")
        else PreflightStatus.BLOCK
        if queue_commands
        else PreflightStatus.WARN
    )
    add(
        "live_sim_safety_gate_preview",
        safety_status,
        "LIVE_SIM safety gate preview passed."
        if safety_status is PreflightStatus.PASS
        else "LIVE_SIM safety gate preview did not pass.",
        safety_gate,
    )

    latest_reconcile = _safe_latest_reconcile(connection)
    _add_reconcile_check(add, latest_reconcile, resolved_settings, resolved_mode)

    live_sim_status = _safe_live_sim_status(connection, resolved_settings)
    counts = _counts_from_live_sim_status(connection, live_sim_status)
    _add_count_checks(add, counts, resolved_settings, resolved_mode)

    _add_naver_import_check(connection, add)
    _add_fee_tax_check(add, resolved_settings)
    _add_eod_flatten_check(add, resolved_settings)
    if resolved_mode is OperatingMode.PILOT_FULL_LIFECYCLE:
        _add_full_lifecycle_flag_warnings(add, resolved_settings)

    ai_summary = _add_ai_checks(connection, add, resolved_settings, include_ai=include_ai)
    no_buy_summary = _add_no_buy_check(
        connection,
        add,
        trade_date=trade_date,
        include_no_buy=include_no_buy,
    )

    status = _overall_status(checks)
    warnings = [check.message for check in checks if check.status is PreflightStatus.WARN]
    blocking_reasons = [
        f"{check.name}: {check.message}"
        for check in checks
        if check.status is PreflightStatus.BLOCK
    ]
    gateway = {
        "last_heartbeat_at": heartbeat_at,
        "heartbeat_age_sec": None if heartbeat_age_sec == float("inf") else heartbeat_age_sec,
        "gateway_orderable": gateway_orderable,
        "command_queue_healthy": _bool_value(gateway_values.get("command_queue_healthy"), True),
        "account_mode": account_mode,
        "broker_env": broker_env,
        "server_mode": server_mode,
    }
    return LiveSimPreflightResult(
        status=status,
        mode=resolved_mode,
        queue_commands=bool(queue_commands),
        checks=tuple(checks),
        warnings=tuple(warnings),
        blocking_reasons=tuple(blocking_reasons),
        counts=counts,
        gateway=gateway,
        safety_gate=safety_gate,
        latest_reconcile=latest_reconcile,
        ai_summary=ai_summary,
        no_buy_summary=no_buy_summary,
    )


def _add_theme_checks(
    connection: sqlite3.Connection,
    add: Any,
    settings: Settings,
) -> None:
    try:
        row = connection.execute(
            "SELECT COUNT(*) AS count FROM theme_members WHERE active = 1"
        ).fetchone()
        active_members = int(row["count"] if row else 0)
    except sqlite3.Error as exc:
        add(
            "theme_membership",
            PreflightStatus.WARN,
            "Theme membership could not be inspected.",
            {"error": str(exc)},
        )
        active_members = 0
    else:
        add(
            "theme_membership",
            PreflightStatus.PASS if active_members > 0 else PreflightStatus.WARN,
            "Theme membership exists."
            if active_members > 0
            else "No active theme membership exists.",
            {"active_member_count": active_members},
        )

    try:
        result = rebuild_theme_leadership(
            connection,
            write_candidate_sources=False,
            settings=settings,
        )
        snapshots = [snapshot.to_dict(include_members=False) for snapshot in result.snapshots]
        data_wait_count = sum(1 for snapshot in snapshots if snapshot.get("state") == "DATA_WAIT")
        add(
            "theme_leadership",
            PreflightStatus.PASS,
            "ThemeLeadership inspected; DATA_WAIT themes are advisory."
            if data_wait_count
            else "ThemeLeadership is calculable.",
            {
                "status": result.status,
                "snapshot_count": len(snapshots),
                "watchset_count": len(result.watchset.items),
                "data_wait_count": data_wait_count,
            },
        )
    except Exception as exc:
        add(
            "theme_leadership",
            PreflightStatus.WARN,
            "ThemeLeadership calculation failed.",
            {"error": str(exc)},
        )


def _add_entry_timing_check(
    connection: sqlite3.Connection,
    add: Any,
    settings: Settings,
    mode: OperatingMode,
) -> None:
    try:
        status = get_entry_timing_status(connection, settings=settings)
    except sqlite3.Error as exc:
        add(
            "entry_timing",
            PreflightStatus.WARN,
            "EntryTiming status could not be inspected.",
            {"error": str(exc)},
        )
        return
    enabled = bool(status.get("enabled"))
    plan_ready_count = int(status.get("plan_ready_count") or 0)
    if not enabled:
        check_status = PreflightStatus.BLOCK if mode.includes_buy else PreflightStatus.WARN
        message = "EntryTiming is disabled."
    elif plan_ready_count <= 0:
        check_status = PreflightStatus.WARN
        message = "EntryTiming has zero PLAN_READY order plans."
    else:
        check_status = PreflightStatus.PASS
        message = "EntryTiming is enabled and has PLAN_READY order plans."
    add("entry_timing", check_status, message, status)


def _add_reconcile_check(
    add: Any,
    latest_reconcile: Mapping[str, Any] | None,
    settings: Settings,
    mode: OperatingMode,
) -> None:
    if latest_reconcile is None:
        add(
            "reconcile_latest_status",
            PreflightStatus.WARN,
            "No LIVE_SIM reconcile snapshot exists yet.",
        )
        return
    blocking_new_buy = _reconcile_blocks_new_buy(latest_reconcile)
    status = str(latest_reconcile.get("status") or "UNKNOWN")
    snapshot = _json_object(latest_reconcile.get("snapshot_json"))
    broker_snapshot_available = bool(snapshot.get("broker_snapshot_available"))
    if blocking_new_buy and mode.includes_buy:
        check_status = PreflightStatus.BLOCK
        message = "Latest reconcile blocks new BUY."
    elif (
        not broker_snapshot_available
        and settings.live_sim_reconcile_request_broker_snapshot_enabled
    ):
        check_status = PreflightStatus.WARN
        message = "Broker snapshot is unavailable; latest reconcile is local-only."
    elif not broker_snapshot_available:
        check_status = PreflightStatus.PASS
        message = "Broker snapshot is disabled; local-only reconcile is accepted."
    else:
        check_status = PreflightStatus.PASS
        message = "Latest reconcile does not block the requested mode."
    add(
        "reconcile_latest_status",
        check_status,
        message,
        {"status": status, "blocking_new_buy": blocking_new_buy, **dict(latest_reconcile)},
    )


def _add_count_checks(
    add: Any,
    counts: Mapping[str, Any],
    settings: Settings,
    mode: OperatingMode,
) -> None:
    open_orders = int(counts.get("open_order_count") or 0)
    open_positions = int(counts.get("open_position_count") or 0)
    add(
        "open_order_count",
        PreflightStatus.BLOCK
        if mode.includes_buy and open_orders >= settings.live_sim_max_active_orders
        else PreflightStatus.PASS,
        "Open order count is within limit."
        if open_orders < settings.live_sim_max_active_orders or not mode.includes_buy
        else "Open order count reached the active order limit for BUY mode.",
        {"open_order_count": open_orders, "limit": settings.live_sim_max_active_orders},
    )
    add(
        "open_position_count",
        PreflightStatus.BLOCK
        if mode.includes_buy and open_positions >= settings.live_sim_max_active_positions
        else PreflightStatus.PASS,
        "Open position count is within limit."
        if open_positions < settings.live_sim_max_active_positions or not mode.includes_buy
        else "Open position count reached the active position limit for BUY mode.",
        {
            "open_position_count": open_positions,
            "limit": settings.live_sim_max_active_positions,
        },
    )
    add(
        "active_cancel_count",
        PreflightStatus.PASS,
        "Active cancel count inspected.",
        {"active_cancel_count": counts.get("active_cancel_count", 0)},
    )
    add(
        "active_exit_count",
        PreflightStatus.PASS,
        "Active exit count inspected.",
        {"active_exit_count": counts.get("active_exit_count", 0)},
    )
    lifecycle_error_count = int(counts.get("lifecycle_error_count") or 0)
    add(
        "lifecycle_error_count",
        PreflightStatus.BLOCK
        if lifecycle_error_count and mode.includes_buy
        else PreflightStatus.WARN
        if lifecycle_error_count
        else PreflightStatus.PASS,
        "Unresolved lifecycle errors block BUY mode."
        if lifecycle_error_count and mode.includes_buy
        else "Lifecycle errors exist."
        if lifecycle_error_count
        else "No unresolved lifecycle errors were found.",
        {"lifecycle_error_count": lifecycle_error_count},
    )


def _add_naver_import_check(connection: sqlite3.Connection, add: Any) -> None:
    try:
        row = connection.execute(
            """
            SELECT imported_at, status, theme_count, member_count
            FROM theme_import_batches
            WHERE source_name = 'naver_theme'
            ORDER BY imported_at DESC, batch_id DESC
            LIMIT 1
            """
        ).fetchone()
    except sqlite3.Error as exc:
        add(
            "naver_import_recent",
            PreflightStatus.WARN,
            "Naver theme importer status could not be inspected.",
            {"error": str(exc)},
        )
        return
    if row is None:
        add(
            "naver_import_recent",
            PreflightStatus.PASS,
            "Naver theme importer has not run; this is advisory for preflight.",
            {"optional": True, "source_name": "naver_theme"},
        )
        return
    age_sec = _age_seconds(row["imported_at"])
    recent = age_sec <= 24 * 60 * 60 and str(row["status"]).upper() == "SUCCESS"
    add(
        "naver_import_recent",
        PreflightStatus.PASS,
        "Naver theme importer ran recently."
        if recent
        else "Naver theme importer is stale or did not succeed; this is advisory for preflight.",
        {
            "optional": True,
            "imported_at": row["imported_at"],
            "age_sec": None if age_sec == float("inf") else age_sec,
            "status": row["status"],
            "theme_count": row["theme_count"],
            "member_count": row["member_count"],
        },
    )


def _add_fee_tax_check(add: Any, settings: Settings) -> None:
    fee_tax_zero = settings.live_sim_fee_rate == 0 or settings.live_sim_tax_rate == 0
    add(
        "fee_tax_config",
        PreflightStatus.WARN if fee_tax_zero else PreflightStatus.PASS,
        "LIVE_SIM fee/tax uses a zero default."
        if fee_tax_zero
        else "LIVE_SIM fee/tax values are configured.",
        {"fee_rate": settings.live_sim_fee_rate, "tax_rate": settings.live_sim_tax_rate},
    )


def _add_eod_flatten_check(add: Any, settings: Settings) -> None:
    warn = settings.live_sim_exit_engine_enabled and not settings.live_sim_exit_eod_flatten_enabled
    add(
        "eod_flatten_config",
        PreflightStatus.WARN if warn else PreflightStatus.PASS,
        "LIVE_SIM exit engine is enabled but EOD flatten is disabled."
        if warn
        else "LIVE_SIM EOD flatten setting is compatible with the exit engine.",
        {
            "exit_engine_enabled": settings.live_sim_exit_engine_enabled,
            "eod_flatten_enabled": settings.live_sim_exit_eod_flatten_enabled,
            "eod_flatten_time": settings.live_sim_exit_eod_flatten_time,
        },
    )


def _add_full_lifecycle_flag_warnings(add: Any, settings: Settings) -> None:
    disabled = []
    if not settings.live_sim_cancel_enabled or not settings.live_sim_cancel_unfilled_enabled:
        disabled.append("LIVE_SIM_CANCEL_ENABLED/LIVE_SIM_CANCEL_UNFILLED_ENABLED")
    if (
        not settings.live_sim_exit_engine_enabled
        or not settings.live_sim_exit_order_creation_enabled
    ):
        disabled.append("LIVE_SIM_EXIT_ENGINE_ENABLED/LIVE_SIM_EXIT_ORDER_CREATION_ENABLED")
    if not settings.live_sim_exit_gateway_command_enabled:
        disabled.append("LIVE_SIM_EXIT_GATEWAY_COMMAND_ENABLED")
    add(
        "full_lifecycle_flags",
        PreflightStatus.WARN if disabled else PreflightStatus.PASS,
        "Cancel/exit flags are disabled in full lifecycle mode."
        if disabled
        else "Cancel/exit flags are compatible with full lifecycle mode.",
        {"disabled_flags": disabled},
    )


def _add_ai_checks(
    connection: sqlite3.Connection,
    add: Any,
    settings: Settings,
    *,
    include_ai: bool,
) -> dict[str, Any]:
    if not include_ai:
        add("ai_advisory", PreflightStatus.PASS, "AI advisory preflight is skipped.")
        return {"included": False, "advisory_only": True, "no_order_side_effects": True}
    try:
        status = build_ai_advisory_status(connection, settings=settings)
    except sqlite3.Error as exc:
        add(
            "ai_advisory",
            PreflightStatus.WARN,
            "AI advisory status unavailable.",
            {"error": str(exc)},
        )
        return {"available": False, "error": str(exc), "advisory_only": True}
    latest_status = (
        None if status.get("latest_run") is None else str(status["latest_run"].get("status"))
    )
    unavailable = latest_status in {
        "TIMEOUT",
        "INVALID_SCHEMA",
        "FAILED",
        "ERROR",
        "PROVIDER_ERROR",
    }
    if unavailable:
        ai_status = PreflightStatus.WARN
        message = "AI advisory is unavailable or recently failed."
    elif not status.get("enabled"):
        ai_status = PreflightStatus.WARN
        message = "AI Candidate Scorer is disabled."
    else:
        ai_status = PreflightStatus.PASS
        message = "AI advisory status is available."
    add("ai_advisory", ai_status, message, status)
    provider = settings.ai_candidate_scorer_provider.strip().lower()
    external_required = provider in {"external", "external_http", "openai"}
    external_status = (
        PreflightStatus.PASS
        if settings.ai_external_llm_enabled or not external_required
        else PreflightStatus.WARN
    )
    if settings.ai_external_llm_enabled:
        external_message = "External LLM is enabled."
    elif external_required:
        external_message = "External LLM is disabled for an external AI provider."
    else:
        external_message = "External LLM is not required for the configured AI provider."
    add(
        "external_llm",
        external_status,
        external_message,
        {
            "candidate_scorer_provider": provider,
            "external_llm_required": external_required,
            "external_llm_enabled": settings.ai_external_llm_enabled,
            "external_llm_provider": settings.ai_external_llm_provider,
            "external_llm_allow_network": settings.ai_external_llm_allow_network,
        },
    )
    return status


def _add_no_buy_check(
    connection: sqlite3.Connection,
    add: Any,
    *,
    trade_date: str | None,
    include_no_buy: bool,
) -> dict[str, Any]:
    if not include_no_buy:
        add("no_buy_sentinel", PreflightStatus.PASS, "No-Buy Sentinel preflight is skipped.")
        return {"included": False, "read_only": True, "no_order_side_effects": True}
    try:
        latest = get_latest_no_buy_sentinel_snapshot(connection, trade_date=trade_date)
    except sqlite3.Error as exc:
        add(
            "no_buy_sentinel",
            PreflightStatus.WARN,
            "No-Buy Sentinel status unavailable.",
            {"error": str(exc)},
        )
        return {"available": False, "error": str(exc)}
    if latest is None:
        add("no_buy_sentinel", PreflightStatus.WARN, "No-Buy Sentinel snapshot does not exist.")
        return {"available": False, "read_only": True, "no_order_side_effects": True}
    add(
        "no_buy_sentinel",
        PreflightStatus.PASS,
        "No-Buy Sentinel latest snapshot is available.",
        latest,
    )
    return latest


def _safe_gateway_status(connection: sqlite3.Connection) -> dict[str, str]:
    try:
        get_command_status_counts(connection)
        return get_gateway_status_values(connection)
    except sqlite3.Error:
        return {}


def _safe_safety_gate(connection: sqlite3.Connection, settings: Settings) -> dict[str, Any]:
    try:
        return check_live_sim_safety_gate(
            connection,
            settings,
            purpose="LIFECYCLE",
        ).to_dict()
    except Exception as exc:
        return {
            "passed": False,
            "status": "ERROR",
            "reason_codes": ["SAFETY_GATE_ERROR"],
            "error": str(exc),
        }


def _safe_live_sim_status(connection: sqlite3.Connection, settings: Settings) -> dict[str, Any]:
    try:
        return get_live_sim_status(connection, settings=settings)
    except Exception as exc:
        return {"error": str(exc)}


def _safe_latest_reconcile(connection: sqlite3.Connection) -> dict[str, Any] | None:
    try:
        return get_latest_live_sim_reconcile(connection)
    except sqlite3.Error:
        return None


def _counts_from_live_sim_status(
    connection: sqlite3.Connection,
    live_sim_status: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "open_order_count": int(live_sim_status.get("open_order_count") or 0),
        "open_position_count": int(live_sim_status.get("open_position_count") or 0),
        "active_cancel_count": int(live_sim_status.get("cancel_pending_count") or 0),
        "active_exit_count": int(live_sim_status.get("active_exit_signal_count") or 0),
        "lifecycle_error_count": _lifecycle_error_count(connection),
    }


def _lifecycle_error_count(connection: sqlite3.Connection) -> int:
    try:
        row = connection.execute(
            """
            SELECT COUNT(*) AS count
            FROM live_sim_lifecycle_events
            WHERE event_type IN ('RECONCILE_MISMATCH', 'LIFECYCLE_ERROR')
            """
        ).fetchone()
    except sqlite3.Error:
        return 0
    return int(row["count"] if row else 0)


def _overall_status(checks: Sequence[PreflightCheck]) -> PreflightStatus:
    if any(check.status is PreflightStatus.BLOCK for check in checks):
        return PreflightStatus.BLOCK
    if any(check.status is PreflightStatus.WARN for check in checks):
        return PreflightStatus.WARN
    return PreflightStatus.PASS


def _gateway_or_config(values: Mapping[str, str], key: str, fallback: str) -> str:
    return str(values.get(key) or fallback).strip().upper()


def _bool_value(value: object, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y", "on", "ok", "orderable"}:
        return True
    if normalized in {"0", "false", "no", "n", "off", "blocked"}:
        return False
    try:
        loaded = json.loads(str(value))
    except json.JSONDecodeError:
        return default
    return loaded if isinstance(loaded, bool) else default


def _age_seconds(value: object) -> float:
    if not value:
        return float("inf")
    try:
        return max((utc_now() - parse_timestamp(value, "timestamp")).total_seconds(), 0.0)
    except ValueError:
        return float("inf")


def _json_object(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if not value:
        return {}
    try:
        loaded = json.loads(str(value))
    except json.JSONDecodeError:
        return {}
    return loaded if isinstance(loaded, dict) else {}


def _reconcile_blocks_new_buy(reconcile: Mapping[str, Any]) -> bool:
    return bool(reconcile.get("blocking_new_buy")) or (
        str(reconcile.get("status") or "").upper() == "RECONCILE_MISMATCH"
        and int(reconcile.get("mismatch_count") or 0) > 0
    )
