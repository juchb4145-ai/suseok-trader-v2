from __future__ import annotations

from typing import Any

from services.config import Settings, load_settings
from services.entry_timing.models import (
    EntryTimingInput,
    PriceLocationResult,
    PriceLocationState,
)


class PriceLocationClassifier:
    def __init__(self, *, settings: Settings | None = None) -> None:
        self.settings = settings or load_settings()

    def classify(self, item: EntryTimingInput) -> PriceLocationResult:
        metrics = calculate_price_location_metrics(item)
        reasons: list[str] = []
        current = item.current_price
        if current is None or current <= 0:
            return PriceLocationResult(
                state=PriceLocationState.UNKNOWN,
                metrics=metrics,
                reason_codes=["PRICE_MISSING"],
            )

        pullback = metrics.get("pullback_from_high_pct")
        price_vs_vwap = metrics.get("price_vs_vwap_pct")

        if price_vs_vwap is not None and (
            price_vs_vwap > self.settings.entry_timing_vwap_overextended_pct
        ):
            reasons.append("VWAP_OVEREXTENDED")
            return PriceLocationResult(
                state=PriceLocationState.EXTENDED_FROM_VWAP,
                metrics=metrics,
                reason_codes=reasons,
            )

        if pullback is not None and pullback <= self.settings.entry_timing_chase_near_high_pct:
            reasons.append("NEAR_DAY_HIGH")
            return PriceLocationResult(
                state=PriceLocationState.NEAR_DAY_HIGH,
                metrics=metrics,
                reason_codes=reasons,
            )

        if price_vs_vwap is not None and (
            abs(price_vs_vwap) <= self.settings.entry_timing_vwap_reclaim_tolerance_pct
        ):
            reasons.append("NEAR_VWAP")
            return PriceLocationResult(
                state=PriceLocationState.NEAR_VWAP,
                metrics=metrics,
                reason_codes=reasons,
            )

        if pullback is not None and (
            self.settings.entry_timing_pullback_min_pct
            <= pullback
            <= self.settings.entry_timing_pullback_max_pct
        ):
            reasons.append("PULLBACK_FROM_HIGH")
            return PriceLocationResult(
                state=PriceLocationState.PULLBACK_FROM_HIGH,
                metrics=metrics,
                reason_codes=reasons,
            )

        if pullback is not None and pullback > self.settings.entry_timing_pullback_max_pct:
            reasons.append("DEEP_PULLBACK")
            return PriceLocationResult(
                state=PriceLocationState.DEEP_PULLBACK,
                metrics=metrics,
                reason_codes=reasons,
            )

        if price_vs_vwap is not None:
            if price_vs_vwap > 0:
                reasons.append("ABOVE_VWAP")
                return PriceLocationResult(
                    state=PriceLocationState.ABOVE_VWAP,
                    metrics=metrics,
                    reason_codes=reasons,
                )
            reasons.append("BELOW_VWAP")
            return PriceLocationResult(
                state=PriceLocationState.BELOW_VWAP,
                metrics=metrics,
                reason_codes=reasons,
            )

        return PriceLocationResult(
            state=PriceLocationState.UNKNOWN,
            metrics=metrics,
            reason_codes=["PRICE_LOCATION_UNKNOWN"],
        )


def calculate_price_location_metrics(item: EntryTimingInput) -> dict[str, Any]:
    current = item.current_price
    metrics: dict[str, Any] = {
        "pullback_from_high_pct": item.pullback_from_high_pct,
        "price_vs_vwap_pct": None,
        "price_vs_open_pct": None,
        "day_range_position": None,
        "momentum_1m": item.momentum_1m,
        "momentum_3m": item.momentum_3m,
        "momentum_5m": item.momentum_5m,
        "turnover_krw": item.turnover_krw,
        "execution_strength": item.execution_strength,
        "spread_ticks": item.spread_ticks,
        "stale": item.stale,
        "vi_active": item.vi_active,
        "upper_limit_near": item.upper_limit_near,
    }
    if current is None or current <= 0:
        return metrics
    if item.day_high is not None and item.day_high > 0:
        metrics["pullback_from_high_pct"] = (
            item.pullback_from_high_pct
            if item.pullback_from_high_pct is not None
            else max((item.day_high - current) / item.day_high * 100.0, 0.0)
        )
    if item.vwap is not None and item.vwap > 0:
        metrics["price_vs_vwap_pct"] = (current - item.vwap) / item.vwap * 100.0
    if item.open_price is not None and item.open_price > 0:
        metrics["price_vs_open_pct"] = (current - item.open_price) / item.open_price * 100.0
    if (
        item.day_high is not None
        and item.day_low is not None
        and item.day_high > item.day_low
    ):
        metrics["day_range_position"] = (current - item.day_low) / (item.day_high - item.day_low)
    return metrics
