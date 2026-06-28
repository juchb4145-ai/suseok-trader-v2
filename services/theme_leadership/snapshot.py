from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping, Sequence
from typing import Any

from services.market_data_service import get_market_data_readiness
from services.theme_leadership.models import RealtimeStockSnapshot, ThemeUniverseMember


class RealtimeSnapshotBuilder:
    """Reads existing market projection tables and builds per-stock RT snapshots."""

    def __init__(self, *, settings: Any | None = None) -> None:
        self.settings = settings

    def build_for_universe(
        self,
        connection: sqlite3.Connection,
        universe: Sequence[ThemeUniverseMember],
    ) -> dict[str, RealtimeStockSnapshot]:
        snapshots: dict[str, RealtimeStockSnapshot] = {}
        names = {member.code: member.name for member in universe}
        for code, fallback_name in sorted(names.items()):
            snapshots[code] = self.build_for_code(connection, code, fallback_name=fallback_name)
        return snapshots

    def build_for_code(
        self,
        connection: sqlite3.Connection,
        code: str,
        *,
        fallback_name: str,
    ) -> RealtimeStockSnapshot:
        tick = _latest_tick(connection, code)
        readiness = get_market_data_readiness(connection, code, settings=self.settings)
        bars = {
            60: _latest_bar(connection, code, 60),
            180: _latest_bar(connection, code, 180),
            300: _latest_bar(connection, code, 300),
        }
        condition_flags = _condition_flags(connection, code)
        reason_codes = list(readiness.get("reason_codes", ()))

        if tick is None:
            return RealtimeStockSnapshot(
                code=code,
                name=fallback_name,
                market=None,
                current_price=None,
                change_rate_pct=None,
                turnover_krw=None,
                cum_volume=None,
                execution_strength=None,
                best_bid=None,
                best_ask=None,
                spread_ticks=None,
                day_high=None,
                day_low=None,
                open_price=None,
                prev_close=None,
                momentum_1m=None,
                momentum_3m=None,
                momentum_5m=None,
                vwap=None,
                pullback_from_high_pct=None,
                stale=True,
                vi_active=False,
                upper_limit_near=False,
                data_quality=str(readiness.get("quality_status") or "MISSING"),
                source_flags=condition_flags,
                reason_codes=[*reason_codes, "TICK_MISSING"],
            )

        current_price = int(tick["price"])
        day_high = int(tick["day_high"])
        day_low = int(tick["day_low"])
        open_price = _first_bar_open(bars)
        prev_close = _prev_close(current_price, float(tick["change_rate"]))
        pullback = _pct(day_high - current_price, day_high) if day_high > 0 else None
        vwap = _first_not_none(*(bar["vwap"] for bar in bars.values() if bar is not None))
        vi_active = _has_vi_flag(condition_flags)
        upper_limit_near = float(tick["change_rate"]) >= 25.0
        stale = str(readiness.get("quality_status") or "").upper() != "FRESH"
        if stale and "SNAPSHOT_NOT_FRESH" not in reason_codes:
            reason_codes.append("SNAPSHOT_NOT_FRESH")
        if current_price <= 0:
            reason_codes.append("INVALID_PRICE")
        if vi_active:
            reason_codes.append("VI_ACTIVE")
        if upper_limit_near:
            reason_codes.append("UPPER_LIMIT_NEAR")
        if int(tick["spread_ticks"]) >= 10:
            reason_codes.append("ABNORMAL_SPREAD")

        return RealtimeStockSnapshot(
            code=code,
            name=str(tick["name"] or fallback_name),
            market=None,
            current_price=current_price,
            change_rate_pct=float(tick["change_rate"]),
            turnover_krw=float(tick["cumulative_trade_value"]),
            cum_volume=int(tick["cumulative_volume"]),
            execution_strength=float(tick["execution_strength"]),
            best_bid=int(tick["best_bid"]),
            best_ask=int(tick["best_ask"]),
            spread_ticks=int(tick["spread_ticks"]),
            day_high=day_high,
            day_low=day_low,
            open_price=open_price,
            prev_close=prev_close,
            momentum_1m=_momentum_from_bar(current_price, bars[60]),
            momentum_3m=_momentum_from_bar(current_price, bars[180]),
            momentum_5m=_momentum_from_bar(current_price, bars[300]),
            vwap=float(vwap) if vwap is not None else None,
            pullback_from_high_pct=pullback,
            stale=stale,
            vi_active=vi_active,
            upper_limit_near=upper_limit_near,
            data_quality=str(readiness.get("quality_status") or "MISSING"),
            source_flags=condition_flags,
            reason_codes=reason_codes,
        )


def _latest_tick(connection: sqlite3.Connection, code: str) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT *
        FROM market_ticks_latest
        WHERE code = ?
        """,
        (code,),
    ).fetchone()


def _latest_bar(connection: sqlite3.Connection, code: str, interval_sec: int) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT *
        FROM market_minute_bars
        WHERE code = ? AND interval_sec = ?
        ORDER BY bucket_start DESC
        LIMIT 1
        """,
        (code, interval_sec),
    ).fetchone()


def _condition_flags(connection: sqlite3.Connection, code: str) -> dict[str, Any]:
    rows = connection.execute(
        """
        SELECT *
        FROM market_condition_latest
        WHERE code = ? AND action = 'ENTER'
        ORDER BY event_ts DESC, condition_id ASC
        """,
        (code,),
    ).fetchall()
    conditions = []
    for row in rows:
        try:
            metadata = json.loads(row["metadata_json"] or "{}")
        except json.JSONDecodeError:
            metadata = {"metadata_decode_error": True}
        conditions.append(
            {
                "condition_id": row["condition_id"],
                "condition_name": row["condition_name"],
                "event_ts": row["event_ts"],
                "metadata": metadata,
            }
        )
    return {
        "condition_include": bool(conditions),
        "condition_latest": conditions,
    }


def _has_vi_flag(flags: Mapping[str, Any]) -> bool:
    for condition in flags.get("condition_latest", ()):
        name = str(condition.get("condition_name") or "").upper()
        metadata = condition.get("metadata") or {}
        if "VI" in name or bool(metadata.get("vi_active")):
            return True
    return False


def _first_bar_open(bars: Mapping[int, sqlite3.Row | None]) -> int | None:
    for interval in (60, 180, 300):
        bar = bars.get(interval)
        if bar is not None:
            return int(bar["open"])
    return None


def _prev_close(current_price: int, change_rate_pct: float) -> int | None:
    denominator = 1.0 + change_rate_pct / 100.0
    if denominator <= 0:
        return None
    return int(round(current_price / denominator))


def _momentum_from_bar(current_price: int, bar: sqlite3.Row | None) -> float | None:
    if bar is None:
        return None
    open_price = int(bar["open"])
    if open_price <= 0:
        return None
    return _pct(current_price - open_price, open_price)


def _pct(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return numerator / denominator * 100.0


def _first_not_none(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None
