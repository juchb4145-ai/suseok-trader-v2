from __future__ import annotations

import sqlite3
from typing import Any

from services.config import Settings

ACTIVE_POSITION_STATUSES = ("OPEN", "CLOSING", "RECONCILE_MISMATCH")
DAILY_LOSS_LIMIT_REASON = "DAILY_LOSS_LIMIT_EXCEEDED"


def build_live_sim_daily_loss_evidence(
    connection: sqlite3.Connection,
    *,
    trade_date: str,
    settings: Settings,
) -> dict[str, Any]:
    realized_pnl = _sum_live_sim_positions(
        connection,
        "realized_pnl",
        "trade_date = ?",
        trade_date,
    )
    active_statuses_sql = _placeholders(ACTIVE_POSITION_STATUSES)
    unrealized_pnl = _sum_live_sim_positions(
        connection,
        "unrealized_pnl",
        f"trade_date = ? AND status IN ({active_statuses_sql}) AND quantity > 0",
        trade_date,
        *ACTIVE_POSITION_STATUSES,
    )
    daily_pnl = realized_pnl + unrealized_pnl
    pct_loss_limit = (
        settings.live_sim_max_daily_notional * settings.live_sim_max_daily_loss_pct / 100.0
        if settings.live_sim_max_daily_loss_pct > 0
        else 0.0
    )
    enabled_limits = [
        limit
        for limit in (settings.live_sim_max_daily_loss, pct_loss_limit)
        if limit > 0
    ]
    effective_loss_limit = min(enabled_limits) if enabled_limits else 0.0
    daily_loss = max(-daily_pnl, 0.0)
    limit_exceeded = bool(effective_loss_limit > 0 and daily_loss >= effective_loss_limit)
    return {
        "trade_date": trade_date,
        "realized_pnl": realized_pnl,
        "unrealized_pnl": unrealized_pnl,
        "daily_pnl": daily_pnl,
        "daily_loss": daily_loss,
        "max_daily_loss": settings.live_sim_max_daily_loss,
        "max_daily_loss_pct": settings.live_sim_max_daily_loss_pct,
        "pct_loss_limit": pct_loss_limit,
        "effective_loss_limit": effective_loss_limit,
        "daily_loss_limit_enabled": effective_loss_limit > 0,
        "daily_loss_limit_exceeded": limit_exceeded,
        "reason_code": DAILY_LOSS_LIMIT_REASON if limit_exceeded else None,
        "live_sim_only": True,
        "live_real_allowed": False,
    }


def _sum_live_sim_positions(
    connection: sqlite3.Connection,
    column_name: str,
    where_sql: str,
    *params: Any,
) -> float:
    row = connection.execute(
        f"""
        SELECT COALESCE(SUM({column_name}), 0) AS total
        FROM live_sim_positions
        WHERE {where_sql}
        """,
        params,
    ).fetchone()
    return float(row["total"] or 0.0)


def _placeholders(values: tuple[str, ...]) -> str:
    return ",".join("?" for _ in values)
