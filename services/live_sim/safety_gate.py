from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from typing import Any

from domain.broker.utils import parse_timestamp, utc_now
from domain.live_sim.reasons import LiveSimReasonCode
from storage.gateway_command_store import get_command_status_counts

from services.config import Settings, TradingMode, load_settings

SIMULATION_LIKE_MODES = {"SIMULATION", "MOCK", "PAPER", "MOCK_TRADING", "LIVE_SIM"}


@dataclass(frozen=True, kw_only=True)
class LiveSimSafetyGateResult:
    passed: bool
    status: str
    reason_codes: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    trading_mode: str
    live_sim_enabled: bool
    live_real_disabled: bool
    account_mode: str
    broker_env: str
    server_mode: str
    gateway_heartbeat_ok: bool
    gateway_orderable: bool
    simulation_account_confirmed: bool
    simulation_server_confirmed: bool
    dry_run_prerequisite_confirmed: bool
    risk_prerequisite_confirmed: bool
    kill_switch_active: bool
    max_notional: float
    daily_limit_remaining: int
    no_trading_side_effects: bool = False
    live_real_allowed: bool = False
    gateway_command_queue_healthy: bool = True
    openai_tools_disabled: bool = True
    order_tools_disabled: bool = True
    dashboard_order_controls_unavailable: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "status": self.status,
            "reason_codes": list(self.reason_codes),
            "warnings": list(self.warnings),
            "trading_mode": self.trading_mode,
            "live_sim_enabled": self.live_sim_enabled,
            "live_real_disabled": self.live_real_disabled,
            "account_mode": self.account_mode,
            "broker_env": self.broker_env,
            "server_mode": self.server_mode,
            "gateway_heartbeat_ok": self.gateway_heartbeat_ok,
            "gateway_orderable": self.gateway_orderable,
            "simulation_account_confirmed": self.simulation_account_confirmed,
            "simulation_server_confirmed": self.simulation_server_confirmed,
            "dry_run_prerequisite_confirmed": self.dry_run_prerequisite_confirmed,
            "risk_prerequisite_confirmed": self.risk_prerequisite_confirmed,
            "kill_switch_active": self.kill_switch_active,
            "max_notional": self.max_notional,
            "daily_limit_remaining": self.daily_limit_remaining,
            "no_trading_side_effects": False,
            "live_real_allowed": False,
            "gateway_command_queue_healthy": self.gateway_command_queue_healthy,
            "openai_tools_disabled": self.openai_tools_disabled,
            "order_tools_disabled": self.order_tools_disabled,
            "dashboard_order_controls_unavailable": self.dashboard_order_controls_unavailable,
        }


def check_live_sim_safety_gate(
    connection: sqlite3.Connection,
    settings: Settings | None = None,
) -> LiveSimSafetyGateResult:
    resolved_settings = settings or load_settings()
    gateway_values = _gateway_status_values(connection)
    reason_codes: list[str] = []
    warnings: list[str] = []

    trading_mode_ok = (
        resolved_settings.trading_mode is TradingMode.LIVE_SIM or resolved_settings.live_sim_enabled
    )
    live_sim_enabled = resolved_settings.live_sim_enabled and resolved_settings.live_sim_allowed
    live_real_disabled = (
        resolved_settings.trading_mode is not TradingMode.LIVE_REAL
        and not resolved_settings.live_real_allowed
    )
    order_routing_enabled = resolved_settings.live_sim_order_routing_enabled
    gateway_command_enabled = resolved_settings.live_sim_gateway_command_enabled
    kill_switch_active = resolved_settings.live_sim_kill_switch
    gateway_heartbeat_ok = _heartbeat_is_fresh(
        gateway_values.get("last_heartbeat_at"), resolved_settings
    )
    gateway_orderable = _bool_status(gateway_values, "gateway_orderable", "orderable")
    account_mode = _gateway_or_config(
        gateway_values,
        "account_mode",
        resolved_settings.live_sim_account_mode,
    )
    broker_env = _gateway_or_config(
        gateway_values, "broker_env", resolved_settings.live_sim_broker_env
    )
    server_mode = _gateway_or_config(
        gateway_values,
        "server_mode",
        resolved_settings.live_sim_server_mode,
    )
    simulation_account_confirmed = (
        bool(resolved_settings.live_sim_account_id.strip())
        and _is_simulation_like(account_mode)
        and _is_simulation_like(resolved_settings.live_sim_account_mode)
    )
    simulation_server_confirmed = (
        _is_simulation_like(broker_env)
        and _is_simulation_like(server_mode)
        and _is_simulation_like(resolved_settings.live_sim_broker_env)
        and _is_simulation_like(resolved_settings.live_sim_server_mode)
    )
    command_counts = get_command_status_counts(connection)
    queue_healthy = _bool_status(gateway_values, "command_queue_healthy", default=True)

    if not trading_mode_ok or not live_sim_enabled:
        reason_codes.append(LiveSimReasonCode.LIVE_SIM_DISABLED.value)
    if not live_real_disabled:
        reason_codes.append(LiveSimReasonCode.LIVE_REAL_NOT_ALLOWED.value)
    if not order_routing_enabled:
        reason_codes.append(LiveSimReasonCode.ORDER_ROUTING_DISABLED.value)
    if not gateway_command_enabled:
        reason_codes.append(LiveSimReasonCode.GATEWAY_COMMAND_DISABLED.value)
    if kill_switch_active:
        reason_codes.append(LiveSimReasonCode.LIVE_SIM_KILL_SWITCH_ACTIVE.value)
    if not gateway_heartbeat_ok:
        reason_codes.append(LiveSimReasonCode.GATEWAY_HEARTBEAT_STALE.value)
    if not gateway_orderable:
        reason_codes.append(LiveSimReasonCode.GATEWAY_NOT_ORDERABLE.value)
    if not simulation_account_confirmed:
        reason_codes.append(LiveSimReasonCode.ACCOUNT_NOT_SIMULATION.value)
    if not _is_simulation_like(broker_env):
        reason_codes.append(LiveSimReasonCode.BROKER_ENV_NOT_SIMULATION.value)
    if not _is_simulation_like(server_mode):
        reason_codes.append(LiveSimReasonCode.SERVER_MODE_NOT_SIMULATION.value)
    if not simulation_server_confirmed:
        reason_codes.append(LiveSimReasonCode.SERVER_MODE_NOT_SIMULATION.value)
    if not queue_healthy:
        reason_codes.append(LiveSimReasonCode.GATEWAY_COMMAND_QUEUE_UNHEALTHY.value)
    if (
        resolved_settings.ai_sidecar_tools_enabled
        or resolved_settings.ai_sidecar_order_tools_enabled
    ):
        reason_codes.append(LiveSimReasonCode.AI_ORDER_TOOLS_ENABLED.value)
    if command_counts.get("FAILED", 0) or command_counts.get("REJECTED", 0):
        warnings.append(
            "Gateway command queue has failed/rejected history; inspect before LIVE_SIM."
        )

    daily_count = _daily_live_sim_order_count(connection)
    daily_limit_remaining = max(resolved_settings.live_sim_max_daily_order_count - daily_count, 0)
    if daily_limit_remaining <= 0:
        reason_codes.append(LiveSimReasonCode.DAILY_ORDER_LIMIT_EXCEEDED.value)

    reason_codes = _merge_reasons(reason_codes)
    passed = not reason_codes
    return LiveSimSafetyGateResult(
        passed=passed,
        status="PASSED" if passed else "BLOCKED",
        reason_codes=reason_codes,
        warnings=warnings,
        trading_mode=resolved_settings.trading_mode.value,
        live_sim_enabled=live_sim_enabled,
        live_real_disabled=live_real_disabled,
        account_mode=account_mode,
        broker_env=broker_env,
        server_mode=server_mode,
        gateway_heartbeat_ok=gateway_heartbeat_ok,
        gateway_orderable=gateway_orderable,
        simulation_account_confirmed=simulation_account_confirmed,
        simulation_server_confirmed=simulation_server_confirmed,
        dry_run_prerequisite_confirmed=True,
        risk_prerequisite_confirmed=True,
        kill_switch_active=kill_switch_active,
        max_notional=resolved_settings.live_sim_max_order_notional,
        daily_limit_remaining=daily_limit_remaining,
        gateway_command_queue_healthy=queue_healthy,
        openai_tools_disabled=not resolved_settings.ai_sidecar_tools_enabled,
        order_tools_disabled=not resolved_settings.ai_sidecar_order_tools_enabled,
        dashboard_order_controls_unavailable=True,
    )


def is_simulation_like(value: str | None) -> bool:
    return _is_simulation_like(value)


def _gateway_status_values(connection: sqlite3.Connection) -> dict[str, str]:
    rows = connection.execute("SELECT key, value FROM gateway_status").fetchall()
    return {str(row["key"]): str(row["value"]) for row in rows}


def _heartbeat_is_fresh(value: str | None, settings: Settings) -> bool:
    if not value:
        return False
    try:
        age = (utc_now() - parse_timestamp(value, "last_heartbeat_at")).total_seconds()
    except ValueError:
        return False
    return age <= settings.live_sim_stale_tick_sec * 4


def _bool_status(values: dict[str, str], *keys: str, default: bool = False) -> bool:
    for key in keys:
        raw = values.get(key)
        if raw is None:
            continue
        normalized = raw.strip().lower()
        if normalized in {"1", "true", "yes", "y", "on", "ok", "orderable"}:
            return True
        if normalized in {"0", "false", "no", "n", "off", "blocked"}:
            return False
        try:
            loaded = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(loaded, bool):
            return loaded
    return default


def _gateway_or_config(values: dict[str, str], key: str, fallback: str) -> str:
    return values.get(key, fallback).strip().upper()


def _is_simulation_like(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip().upper() in SIMULATION_LIKE_MODES


def _daily_live_sim_order_count(connection: sqlite3.Connection) -> int:
    row = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM live_sim_orders
        WHERE trade_date = date('now', 'localtime')
        """
    ).fetchone()
    return int(row["count"])


def _merge_reasons(reasons: list[str]) -> list[str]:
    return [*dict.fromkeys(str(reason).upper() for reason in reasons if str(reason).strip())]
