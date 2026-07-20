from __future__ import annotations

import hashlib
import json
import statistics
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, cast

from domain.broker.events import GatewayEvent
from domain.broker.utils import datetime_to_wire, parse_timestamp
from domain.exit.policy import (
    ExitOrderStyle,
    ExitTrigger,
    LongPositionSnapshot,
    evaluate_long_exit_policy,
)
from storage.gateway_command_store import canonical_json

from services.entry_timing.tick_size import add_ticks, normalize_tick_price, subtract_ticks
from services.profit_lab.models import (
    PROFIT_LAB_REPORT_FORMAT,
    PROFIT_LAB_SIGNAL_FORMAT,
    ProfitLabConfig,
    ProfitLabSignal,
    ProfitLabTrade,
)
from services.runtime.projection_replay import ReplayBundleResult, validate_replay_bundle

_SEOUL_TIMEZONE = timezone(timedelta(hours=9), name="Asia/Seoul")


@dataclass(frozen=True, kw_only=True)
class ProfitLabSignalManifest:
    source_record_sha256: str
    source_event_order_sha256: str
    signals: tuple[ProfitLabSignal, ...]
    signals_sha256: str


@dataclass(frozen=True, kw_only=True)
class AlphaReplayEvidence:
    result_sha256: str
    source_record_sha256: str
    source_event_order_sha256: str
    alpha_qualified: bool
    point_in_time_violation_count: int
    scan_coverage: str
    missing_sources: tuple[str, ...]


@dataclass(frozen=True, kw_only=True)
class _ReplayTick:
    sequence: int
    event_id: str
    code: str
    exchange: str
    price: int
    event_at: datetime
    available_at: datetime


@dataclass(frozen=True, kw_only=True)
class ProfitLabResult:
    status: str
    qualification: str
    qualification_reasons: tuple[str, ...]
    warnings: tuple[str, ...]
    bundle: ReplayBundleResult
    alpha_evidence: AlphaReplayEvidence
    config: ProfitLabConfig
    config_sha256: str
    signals_sha256: str
    commit_sha: str
    deterministic_identity_sha256: str
    result_sha256: str
    signal_count: int
    tick_count: int
    trades: tuple[ProfitLabTrade, ...]
    metrics: Mapping[str, Any]
    grouped_metrics: Mapping[str, Mapping[str, Any]]
    stress_matrix: Sequence[Mapping[str, Any]]
    point_in_time_violation_count: int = 0
    operational_db_write_count: int = 0
    gateway_command_write_count: int = 0
    live_sim_write_count: int = 0
    dry_run_write_count: int = 0

    @property
    def no_trading_side_effects(self) -> bool:
        return (
            self.operational_db_write_count == 0
            and self.gateway_command_write_count == 0
            and self.live_sim_write_count == 0
            and self.dry_run_write_count == 0
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "format": PROFIT_LAB_REPORT_FORMAT,
            "status": self.status,
            "qualification": self.qualification,
            "qualification_reasons": list(self.qualification_reasons),
            "warnings": list(self.warnings),
            "identity": {
                "source_record_sha256": self.bundle.record_sha256,
                "source_event_order_sha256": self.bundle.event_order_sha256,
                "alpha_replay_result_sha256": self.alpha_evidence.result_sha256,
                "config_sha256": self.config_sha256,
                "signals_sha256": self.signals_sha256,
                "commit_sha": self.commit_sha,
                "deterministic_identity_sha256": self.deterministic_identity_sha256,
            },
            "result_sha256": self.result_sha256,
            "config": self.config.to_dict(),
            "cost_model_complete": self.config.cost_model_complete,
            "signal_count": self.signal_count,
            "tick_count": self.tick_count,
            "trades": [trade.to_dict() for trade in self.trades],
            "metrics": dict(self.metrics),
            "grouped_metrics": {
                group: dict(values) for group, values in self.grouped_metrics.items()
            },
            "stress_matrix": [dict(item) for item in self.stress_matrix],
            "source_quality": {
                "alpha_qualified": self.alpha_evidence.alpha_qualified,
                "point_in_time_violation_count": (
                    self.alpha_evidence.point_in_time_violation_count
                ),
                "scan_coverage": self.alpha_evidence.scan_coverage,
                "missing_sources": list(self.alpha_evidence.missing_sources),
            },
            "safety": {
                "point_in_time_violation_count": self.point_in_time_violation_count,
                "operational_db_write_count": self.operational_db_write_count,
                "gateway_command_write_count": self.gateway_command_write_count,
                "live_sim_write_count": self.live_sim_write_count,
                "dry_run_write_count": self.dry_run_write_count,
                "no_order_side_effects": self.no_trading_side_effects,
                "no_trading_side_effects": self.no_trading_side_effects,
                "observe_only": True,
                "production_db_writes_allowed": False,
                "live_sim_allowed": False,
                "live_real_allowed": False,
            },
        }


def load_profit_lab_signals(
    path: str | Path,
) -> ProfitLabSignalManifest:
    signal_path = Path(path).expanduser().resolve()
    if not signal_path.is_file():
        raise FileNotFoundError(f"Profit Lab signal manifest is missing: {signal_path}")
    value = json.loads(signal_path.read_text(encoding="utf-8"))
    if not isinstance(value, Mapping) or value.get("format") != PROFIT_LAB_SIGNAL_FORMAT:
        raise ValueError("unsupported Profit Lab signal manifest format")
    raw_signals = value.get("signals")
    if not isinstance(raw_signals, list):
        raise ValueError("Profit Lab signal manifest signals must be a list")
    signals = tuple(ProfitLabSignal.from_mapping(_mapping(item, "signal")) for item in raw_signals)
    _validate_unique_signals(signals)
    return ProfitLabSignalManifest(
        source_record_sha256=str(value.get("source_record_sha256") or ""),
        source_event_order_sha256=str(value.get("source_event_order_sha256") or ""),
        signals=signals,
        signals_sha256=_sha256_json([signal.to_dict() for signal in signals]),
    )


def empty_profit_lab_signals(bundle: ReplayBundleResult) -> ProfitLabSignalManifest:
    return ProfitLabSignalManifest(
        source_record_sha256=bundle.record_sha256,
        source_event_order_sha256=bundle.event_order_sha256,
        signals=(),
        signals_sha256=_sha256_json([]),
    )


def load_alpha_replay_evidence(
    value: str | Path | Mapping[str, Any],
) -> AlphaReplayEvidence:
    if isinstance(value, Mapping):
        report = value
    else:
        path = Path(value).expanduser().resolve()
        if not path.is_file():
            raise FileNotFoundError(f"FAST-2A report is missing: {path}")
        parsed = json.loads(path.read_text(encoding="utf-8"))
        report = _mapping(parsed, "alpha replay report")
    if report.get("format") != "point-in-time-alpha-replay-report/v1":
        raise ValueError("unsupported FAST-2A report format")
    replay = _mapping(report.get("replay"), "replay")
    identity = _mapping(replay.get("identity"), "replay.identity")
    return AlphaReplayEvidence(
        result_sha256=str(replay.get("result_sha256") or ""),
        source_record_sha256=str(identity.get("source_record_sha256") or ""),
        source_event_order_sha256=str(identity.get("source_event_order_sha256") or ""),
        alpha_qualified=bool(replay.get("alpha_qualified")),
        point_in_time_violation_count=int(replay.get("point_in_time_violation_count") or 0),
        scan_coverage=str(replay.get("scan_coverage") or "UNKNOWN"),
        missing_sources=tuple(str(item) for item in replay.get("missing_sources") or []),
    )


def run_profit_lab(
    *,
    bundle_dir: str | Path,
    alpha_replay_report: str | Path | Mapping[str, Any],
    signal_manifest: ProfitLabSignalManifest | None = None,
    config: ProfitLabConfig | None = None,
    commit_sha: str = "UNKNOWN",
) -> ProfitLabResult:
    bundle = validate_replay_bundle(bundle_dir)
    evidence = load_alpha_replay_evidence(alpha_replay_report)
    _validate_alpha_identity(bundle, evidence)
    manifest = signal_manifest or empty_profit_lab_signals(bundle)
    _validate_signal_identity(bundle, manifest)
    resolved_config = config or ProfitLabConfig()
    config_sha256 = _sha256_json(resolved_config.to_dict())
    normalized_commit = str(commit_sha or "UNKNOWN").strip() or "UNKNOWN"
    identity = {
        "source_record_sha256": bundle.record_sha256,
        "source_event_order_sha256": bundle.event_order_sha256,
        "alpha_replay_result_sha256": evidence.result_sha256,
        "signals_sha256": manifest.signals_sha256,
        "config_sha256": config_sha256,
        "commit_sha": normalized_commit,
    }
    identity_sha256 = _sha256_json(identity)
    ticks = _load_replay_ticks(bundle.events_path)
    split_by_date = _date_splits(
        [signal.trade_date for signal in manifest.signals],
        train_ratio=resolved_config.train_ratio,
        validation_ratio=resolved_config.validation_ratio,
    )
    trades = tuple(
        _simulate(
            manifest.signals,
            ticks,
            config=resolved_config,
            split_by_date=split_by_date,
        )
    )
    metrics = _metrics(trades, eligible_signal_count=_eligible_count(manifest.signals))
    grouped = _grouped_metrics(trades)
    stress_matrix = _stress_matrix(
        manifest.signals,
        ticks,
        config=resolved_config,
        split_by_date=split_by_date,
    )
    qualification, reasons, warnings = _qualify(
        evidence=evidence,
        config=resolved_config,
        metrics=metrics,
        grouped=grouped,
        stress_matrix=stress_matrix,
        signal_count=len(manifest.signals),
    )
    if normalized_commit == "UNKNOWN":
        warnings.append("COMMIT_IDENTITY_UNKNOWN")
    status = "PASS" if qualification == "ALPHA_QUALIFIED" else "WARN"
    deterministic_result = {
        "identity_sha256": identity_sha256,
        "qualification": qualification,
        "qualification_reasons": sorted(set(reasons)),
        "warnings": sorted(set(warnings)),
        "trades": [trade.to_dict() for trade in trades],
        "metrics": metrics,
        "grouped_metrics": grouped,
        "stress_matrix": stress_matrix,
    }
    return ProfitLabResult(
        status=status,
        qualification=qualification,
        qualification_reasons=tuple(sorted(set(reasons))),
        warnings=tuple(sorted(set(warnings))),
        bundle=bundle,
        alpha_evidence=evidence,
        config=resolved_config,
        config_sha256=config_sha256,
        signals_sha256=manifest.signals_sha256,
        commit_sha=normalized_commit,
        deterministic_identity_sha256=identity_sha256,
        result_sha256=_sha256_json(deterministic_result),
        signal_count=len(manifest.signals),
        tick_count=len(ticks),
        trades=trades,
        metrics=metrics,
        grouped_metrics=grouped,
        stress_matrix=tuple(stress_matrix),
    )


def _load_replay_ticks(events_path: Path) -> tuple[_ReplayTick, ...]:
    ticks: list[_ReplayTick] = []
    previous_available: datetime | None = None
    with events_path.open("r", encoding="utf-8") as stream:
        for line in stream:
            record = json.loads(line)
            event = cast(GatewayEvent, GatewayEvent.from_dict(record["event"]))
            if event.event_type.strip().lower() != "price_tick":
                continue
            available_at = parse_timestamp(record["source_received_at"], "source_received_at")
            if previous_available is not None and available_at < previous_available:
                raise ValueError("price tick source availability is not monotonic")
            previous_available = available_at
            if event.ts > available_at:
                raise ValueError(f"future price tick detected: {event.event_id}")
            metadata = event.payload.get("metadata")
            metadata = metadata if isinstance(metadata, Mapping) else {}
            exchange = str(metadata.get("exchange") or "KRX").upper()
            if exchange not in {"KRX", "NXT"}:
                continue
            price = int(float(event.payload.get("price") or 0))
            if price <= 0:
                continue
            ticks.append(
                _ReplayTick(
                    sequence=int(record["sequence"]),
                    event_id=event.event_id,
                    code=str(event.payload.get("code") or ""),
                    exchange=exchange,
                    price=price,
                    event_at=event.ts,
                    available_at=available_at,
                )
            )
    return tuple(ticks)


def _simulate(
    signals: Sequence[ProfitLabSignal],
    ticks: Sequence[_ReplayTick],
    *,
    config: ProfitLabConfig,
    split_by_date: Mapping[str, str],
) -> list[ProfitLabTrade]:
    by_instrument: dict[tuple[str, str], list[_ReplayTick]] = defaultdict(list)
    for tick in ticks:
        by_instrument[(tick.code, tick.exchange)].append(tick)
    ordered_signals = sorted(
        signals,
        key=lambda item: (parse_timestamp(item.signal_at, "signal_at"), item.signal_id),
    )
    return [
        _simulate_signal(
            signal,
            by_instrument.get((signal.code, signal.exchange), []),
            config=config,
            dataset_split=split_by_date.get(signal.trade_date, "UNASSIGNED"),
        )
        for signal in ordered_signals
    ]


def _simulate_signal(
    signal: ProfitLabSignal,
    ticks: Sequence[_ReplayTick],
    *,
    config: ProfitLabConfig,
    dataset_split: str,
) -> ProfitLabTrade:
    signal_at = parse_timestamp(signal.signal_at, "signal_at")
    signal_wire = datetime_to_wire(signal_at)
    requested_limit = normalize_tick_price(signal.limit_price)
    base: dict[str, Any] = {
        "signal_id": signal.signal_id,
        "trade_date": signal.trade_date,
        "signal_at": signal_wire,
        "dataset_split": dataset_split,
        "code": signal.code,
        "exchange": signal.exchange,
        "setup_type": signal.setup_type,
        "regime": signal.regime,
        "theme": signal.theme,
        "requested_entry_limit": requested_limit,
        "quantity": signal.quantity,
    }
    if not signal.coherent:
        return ProfitLabTrade(
            **base,
            status="INELIGIBLE",
            reason_codes=("SIGNAL_NOT_COHERENT",),
        )
    eligible_at = signal_at + timedelta(milliseconds=config.entry_latency_ms)
    expires_at = signal_at + timedelta(seconds=config.entry_ttl_sec)
    entry_index: int | None = None
    for index, tick in enumerate(ticks):
        if tick.available_at <= signal_at or tick.available_at < eligible_at:
            continue
        if tick.available_at > expires_at:
            break
        if tick.price <= requested_limit:
            entry_index = index
            break
    if entry_index is None:
        return ProfitLabTrade(
            **base,
            status="ENTRY_NO_FILL",
            reason_codes=("BUY_LIMIT_TTL_NO_FILL",),
        )

    entry_tick = ticks[entry_index]
    # A limit order cannot fill beyond its own limit. Model adverse slippage as an
    # explicit cost so the economic result remains conservative without violating
    # the exchange price constraint.
    entry_fill_price = requested_limit
    highest_price = max(float(entry_fill_price), float(entry_tick.price))
    lowest_price = min(float(entry_fill_price), float(entry_tick.price))
    policy = config.exit_policy()
    exit_trigger: ExitTrigger | None = None
    exit_trigger_index: int | None = None
    for index in range(entry_index + 1, len(ticks)):
        tick = ticks[index]
        highest_price = max(highest_price, float(tick.price))
        lowest_price = min(lowest_price, float(tick.price))
        decision = evaluate_long_exit_policy(
            LongPositionSnapshot(
                entry_price=float(entry_fill_price),
                current_price=float(tick.price),
                highest_price=highest_price,
                quantity=signal.quantity,
                opened_at=entry_tick.available_at,
                observed_at=tick.available_at,
            ),
            policy,
        )
        if decision.primary_trigger is not None:
            exit_trigger = decision.primary_trigger
            exit_trigger_index = index
            break
    if exit_trigger is None or exit_trigger_index is None:
        return ProfitLabTrade(
            **base,
            status="OPEN_AT_REPLAY_END",
            entry_filled_at=datetime_to_wire(entry_tick.available_at),
            entry_market_price=entry_tick.price,
            entry_fill_price=entry_fill_price,
            mae_pct=_pct(lowest_price - entry_fill_price, entry_fill_price),
            mfe_pct=_pct(highest_price - entry_fill_price, entry_fill_price),
            reason_codes=("EXIT_TRIGGER_NOT_OBSERVED",),
        )

    trigger_tick = ticks[exit_trigger_index]
    fill = _find_exit_fill(
        ticks,
        trigger_index=exit_trigger_index,
        trigger=exit_trigger,
        config=config,
    )
    trigger_price = (
        normalize_tick_price(exit_trigger.trigger_price)
        if exit_trigger.trigger_price is not None
        else None
    )
    if fill is None:
        return ProfitLabTrade(
            **base,
            status="EXIT_NO_FILL",
            entry_filled_at=datetime_to_wire(entry_tick.available_at),
            entry_market_price=entry_tick.price,
            entry_fill_price=entry_fill_price,
            exit_triggered_at=datetime_to_wire(trigger_tick.available_at),
            exit_trigger_type=exit_trigger.trigger_type.value,
            exit_order_style=exit_trigger.order_style.value,
            exit_trigger_price=trigger_price,
            mae_pct=_pct(lowest_price - entry_fill_price, entry_fill_price),
            mfe_pct=_pct(highest_price - entry_fill_price, entry_fill_price),
            reason_codes=("SELL_EXIT_TTL_NO_FILL",),
        )

    fill_tick, exit_fill_price = fill
    highest_price = max(highest_price, float(fill_tick.price))
    lowest_price = min(lowest_price, float(fill_tick.price))
    quantity = signal.quantity
    buy_notional = float(entry_fill_price * quantity)
    sell_notional = float(exit_fill_price * quantity)
    buy_commission = buy_notional * config.buy_commission_rate
    sell_commission = sell_notional * config.sell_commission_rate
    sell_tax = sell_notional * config.sell_tax_rate
    buy_slippage_cost = (
        add_ticks(entry_fill_price, config.buy_slippage_ticks) - entry_fill_price
    ) * quantity
    sell_slippage_cost = (
        exit_fill_price - subtract_ticks(exit_fill_price, config.sell_slippage_ticks)
    ) * quantity
    slippage_cost = float(buy_slippage_cost + sell_slippage_cost)
    total_cost = buy_commission + sell_commission + sell_tax + slippage_cost
    gross_pnl = (exit_fill_price - entry_fill_price) * quantity
    net_pnl = gross_pnl - total_cost
    initial_risk = entry_fill_price * (config.stop_loss_pct / 100) * quantity
    return ProfitLabTrade(
        **base,
        status="CLOSED",
        entry_filled_at=datetime_to_wire(entry_tick.available_at),
        entry_market_price=entry_tick.price,
        entry_fill_price=entry_fill_price,
        exit_triggered_at=datetime_to_wire(trigger_tick.available_at),
        exit_filled_at=datetime_to_wire(fill_tick.available_at),
        exit_trigger_type=exit_trigger.trigger_type.value,
        exit_order_style=exit_trigger.order_style.value,
        exit_trigger_price=trigger_price,
        exit_market_price=fill_tick.price,
        exit_fill_price=exit_fill_price,
        gross_pnl=_round(gross_pnl),
        buy_commission=_round(buy_commission),
        sell_commission=_round(sell_commission),
        sell_tax=_round(sell_tax),
        slippage_cost=_round(slippage_cost),
        total_cost=_round(total_cost),
        net_pnl=_round(net_pnl),
        net_r=_round(net_pnl / initial_risk) if initial_risk > 0 else None,
        mae_pct=_pct(lowest_price - entry_fill_price, entry_fill_price),
        mfe_pct=_pct(highest_price - entry_fill_price, entry_fill_price),
        holding_sec=_round((fill_tick.available_at - entry_tick.available_at).total_seconds()),
        reason_codes=("CONSERVATIVE_EXIT_FILLED",),
    )


def _find_exit_fill(
    ticks: Sequence[_ReplayTick],
    *,
    trigger_index: int,
    trigger: ExitTrigger,
    config: ProfitLabConfig,
) -> tuple[_ReplayTick, int] | None:
    trigger_tick = ticks[trigger_index]
    eligible_at = trigger_tick.available_at + timedelta(milliseconds=config.exit_latency_ms)
    expires_at = trigger_tick.available_at + timedelta(seconds=config.exit_ttl_sec)
    limit_price = (
        normalize_tick_price(trigger.trigger_price) if trigger.trigger_price is not None else None
    )
    for tick in ticks[trigger_index + 1 :]:
        if tick.available_at <= trigger_tick.available_at or tick.available_at < eligible_at:
            continue
        if tick.available_at > expires_at:
            return None
        if trigger.order_style is ExitOrderStyle.SELL_LIMIT:
            if limit_price is None or tick.price < limit_price:
                continue
            return tick, limit_price
        return tick, tick.price
    return None


def _metrics(
    trades: Sequence[ProfitLabTrade],
    *,
    eligible_signal_count: int,
) -> dict[str, Any]:
    filled = [trade for trade in trades if trade.entry_filled]
    closed = [trade for trade in trades if trade.closed]
    entry_no_fill = [trade for trade in trades if trade.status == "ENTRY_NO_FILL"]
    net_values = [float(trade.net_pnl or 0.0) for trade in closed]
    gross_values = [float(trade.gross_pnl or 0.0) for trade in closed]
    net_r_values = [float(trade.net_r or 0.0) for trade in closed]
    wins = [value for value in net_values if value > 0]
    losses = [value for value in net_values if value < 0]
    positive_sum = sum(wins)
    negative_sum = abs(sum(losses))
    profit_factor = positive_sum / negative_sum if negative_sum > 0 else None
    holding = [float(trade.holding_sec or 0.0) for trade in closed]
    mae = [float(trade.mae_pct or 0.0) for trade in closed]
    mfe = [float(trade.mfe_pct or 0.0) for trade in closed]
    return {
        "signal_count": len(trades),
        "eligible_signal_count": eligible_signal_count,
        "entry_fill_count": len(filled),
        "entry_no_fill_count": len(entry_no_fill),
        "closed_trade_count": len(closed),
        "open_or_exit_no_fill_count": len(filled) - len(closed),
        "fill_rate": _round(len(filled) / eligible_signal_count) if eligible_signal_count else 0.0,
        "close_rate": _round(len(closed) / len(filled)) if filled else 0.0,
        "gross_pnl": _round(sum(gross_values)),
        "net_pnl": _round(sum(net_values)),
        "net_expectancy": _round(statistics.fmean(net_values)) if net_values else None,
        "expectancy_r": _round(statistics.fmean(net_r_values)) if net_r_values else None,
        "win_rate": _round(len(wins) / len(closed)) if closed else None,
        "profit_factor": _round(profit_factor) if profit_factor is not None else None,
        "mae_pct_average": _round(statistics.fmean(mae)) if mae else None,
        "mae_pct_worst": _round(min(mae)) if mae else None,
        "mfe_pct_average": _round(statistics.fmean(mfe)) if mfe else None,
        "mfe_pct_best": _round(max(mfe)) if mfe else None,
        "holding_sec_average": _round(statistics.fmean(holding)) if holding else None,
        "holding_sec_median": _round(statistics.median(holding)) if holding else None,
        "max_drawdown_r": _round(_max_drawdown(net_r_values)),
        "max_consecutive_loss": _max_consecutive_loss(net_values),
        "distinct_trade_dates": len({trade.trade_date for trade in closed}),
        "daily_profit_concentration_ratio": _daily_profit_concentration(closed),
    }


def _grouped_metrics(trades: Sequence[ProfitLabTrade]) -> dict[str, dict[str, Any]]:
    dimensions = {
        "setup": lambda trade: trade.setup_type,
        "regime": lambda trade: trade.regime,
        "theme": lambda trade: trade.theme,
        "entry_hour_kst": _entry_hour,
        "dataset_split": lambda trade: trade.dataset_split,
    }
    result: dict[str, dict[str, Any]] = {}
    for dimension, getter in dimensions.items():
        groups: dict[str, list[ProfitLabTrade]] = defaultdict(list)
        for trade in trades:
            groups[str(getter(trade) or "UNKNOWN")].append(trade)
        result[dimension] = {
            key: _compact_group_metrics(items) for key, items in sorted(groups.items())
        }
    return result


def _compact_group_metrics(trades: Sequence[ProfitLabTrade]) -> dict[str, Any]:
    closed = [trade for trade in trades if trade.closed]
    net = [float(trade.net_pnl or 0.0) for trade in closed]
    return {
        "signal_count": len(trades),
        "entry_fill_count": sum(trade.entry_filled for trade in trades),
        "closed_trade_count": len(closed),
        "net_pnl": _round(sum(net)),
        "net_expectancy": _round(statistics.fmean(net)) if net else None,
        "win_rate": _round(sum(value > 0 for value in net) / len(net)) if net else None,
    }


def _stress_matrix(
    signals: Sequence[ProfitLabSignal],
    ticks: Sequence[_ReplayTick],
    *,
    config: ProfitLabConfig,
    split_by_date: Mapping[str, str],
) -> list[dict[str, Any]]:
    scenarios = (
        ("BASELINE", config),
        (
            "SLIPPAGE_PLUS_1",
            replace(
                config,
                buy_slippage_ticks=config.buy_slippage_ticks + 1,
                sell_slippage_ticks=config.sell_slippage_ticks + 1,
            ),
        ),
        (
            "LATENCY_X2",
            replace(
                config,
                entry_latency_ms=config.entry_latency_ms * 2,
                exit_latency_ms=config.exit_latency_ms * 2,
            ),
        ),
        (
            "SLIPPAGE_PLUS_1_LATENCY_X2",
            replace(
                config,
                buy_slippage_ticks=config.buy_slippage_ticks + 1,
                sell_slippage_ticks=config.sell_slippage_ticks + 1,
                entry_latency_ms=config.entry_latency_ms * 2,
                exit_latency_ms=config.exit_latency_ms * 2,
            ),
        ),
    )
    result: list[dict[str, Any]] = []
    eligible = _eligible_count(signals)
    for scenario, scenario_config in scenarios:
        trades = _simulate(
            signals,
            ticks,
            config=scenario_config,
            split_by_date=split_by_date,
        )
        metrics = _metrics(trades, eligible_signal_count=eligible)
        result.append(
            {
                "scenario": scenario,
                "buy_slippage_ticks": scenario_config.buy_slippage_ticks,
                "sell_slippage_ticks": scenario_config.sell_slippage_ticks,
                "entry_latency_ms": scenario_config.entry_latency_ms,
                "exit_latency_ms": scenario_config.exit_latency_ms,
                "entry_fill_count": metrics["entry_fill_count"],
                "closed_trade_count": metrics["closed_trade_count"],
                "fill_rate": metrics["fill_rate"],
                "net_pnl": metrics["net_pnl"],
                "net_expectancy": metrics["net_expectancy"],
                "expectancy_r": metrics["expectancy_r"],
            }
        )
    return result


def _qualify(
    *,
    evidence: AlphaReplayEvidence,
    config: ProfitLabConfig,
    metrics: Mapping[str, Any],
    grouped: Mapping[str, Mapping[str, Any]],
    stress_matrix: Sequence[Mapping[str, Any]],
    signal_count: int,
) -> tuple[str, list[str], list[str]]:
    reasons: list[str] = []
    warnings: list[str] = []
    if not evidence.alpha_qualified or evidence.point_in_time_violation_count:
        reasons.append("FAST_2A_DATA_QUALITY_NOT_QUALIFIED")
    if not config.cost_model_complete:
        reasons.append("COST_MODEL_MISSING")
    if signal_count == 0:
        warnings.append("NO_REPLAY_SIGNALS")
    concentration = metrics.get("daily_profit_concentration_ratio")
    if concentration is not None and float(concentration) > (
        config.daily_profit_concentration_warn_ratio
    ):
        warnings.append("DAILY_PROFIT_CONCENTRATION_HIGH")
    baseline_expectancy = _optional_number(stress_matrix[0].get("net_expectancy"))
    stressed_expectancy = _optional_number(stress_matrix[1].get("net_expectancy"))
    if baseline_expectancy and stressed_expectancy is not None:
        drop_ratio = (baseline_expectancy - stressed_expectancy) / abs(baseline_expectancy)
        if stressed_expectancy <= 0 or drop_ratio > config.stress_expectancy_drop_warn_ratio:
            warnings.append("PLUS_ONE_TICK_EXPECTANCY_FRAGILE")

    if "FAST_2A_DATA_QUALITY_NOT_QUALIFIED" in reasons:
        return "DATA_QUALITY_BLOCKED", reasons, warnings
    if "COST_MODEL_MISSING" in reasons:
        return "COST_MODEL_MISSING", reasons, warnings
    if (
        int(metrics.get("closed_trade_count") or 0) < config.minimum_filled_trades
        or int(metrics.get("distinct_trade_dates") or 0) < config.minimum_distinct_trade_dates
    ):
        reasons.append("MINIMUM_SAMPLE_NOT_MET")
        return "INSUFFICIENT_SAMPLE", reasons, warnings

    split_metrics = grouped.get("dataset_split") or {}
    for split in ("VALIDATION", "TEST"):
        item = split_metrics.get(split)
        item = item if isinstance(item, Mapping) else {}
        expectancy = _optional_number(item.get("net_expectancy"))
        if expectancy is None or expectancy <= 0:
            reasons.append(f"{split}_NET_EXPECTANCY_NOT_POSITIVE")
    profit_factor = _optional_number(metrics.get("profit_factor"))
    if profit_factor is None or profit_factor < config.minimum_profit_factor:
        reasons.append("PROFIT_FACTOR_BELOW_MINIMUM")
    drawdown = float(metrics.get("max_drawdown_r") or 0.0)
    if drawdown > config.maximum_drawdown_r:
        reasons.append("MAX_DRAWDOWN_R_ABOVE_LIMIT")
    return (
        "ALPHA_UNQUALIFIED" if reasons else "ALPHA_QUALIFIED",
        reasons,
        warnings,
    )


def _date_splits(
    trade_dates: Sequence[str],
    *,
    train_ratio: float,
    validation_ratio: float,
) -> dict[str, str]:
    dates = sorted(set(trade_dates))
    if not dates:
        return {}
    if len(dates) < 3:
        return {value: "TRAIN" for value in dates}
    train_count = max(1, int(len(dates) * train_ratio))
    validation_count = max(1, int(len(dates) * validation_ratio))
    if train_count + validation_count >= len(dates):
        train_count = max(1, len(dates) - 2)
        validation_count = 1
    result: dict[str, str] = {}
    for index, value in enumerate(dates):
        if index < train_count:
            result[value] = "TRAIN"
        elif index < train_count + validation_count:
            result[value] = "VALIDATION"
        else:
            result[value] = "TEST"
    return result


def _entry_hour(trade: ProfitLabTrade) -> str:
    value = trade.entry_filled_at or trade.signal_at
    parsed = parse_timestamp(value, "entry_or_signal_at").astimezone(_SEOUL_TIMEZONE)
    return f"{parsed.hour:02d}"


def _max_drawdown(values: Sequence[float]) -> float:
    equity = 0.0
    peak = 0.0
    drawdown = 0.0
    for value in values:
        equity += value
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)
    return drawdown


def _max_consecutive_loss(values: Sequence[float]) -> int:
    current = 0
    maximum = 0
    for value in values:
        current = current + 1 if value < 0 else 0
        maximum = max(maximum, current)
    return maximum


def _daily_profit_concentration(trades: Sequence[ProfitLabTrade]) -> float | None:
    daily: dict[str, float] = defaultdict(float)
    for trade in trades:
        daily[trade.trade_date] += float(trade.net_pnl or 0.0)
    positive = [value for value in daily.values() if value > 0]
    if not positive:
        return None
    return _round(max(positive) / sum(positive))


def _eligible_count(signals: Sequence[ProfitLabSignal]) -> int:
    return sum(bool(signal.coherent) for signal in signals)


def _validate_unique_signals(signals: Sequence[ProfitLabSignal]) -> None:
    seen: set[str] = set()
    for signal in signals:
        if signal.signal_id in seen:
            raise ValueError(f"duplicate Profit Lab signal_id: {signal.signal_id}")
        seen.add(signal.signal_id)


def _validate_alpha_identity(
    bundle: ReplayBundleResult,
    evidence: AlphaReplayEvidence,
) -> None:
    if not evidence.result_sha256:
        raise ValueError("FAST-2A result_sha256 is required")
    if evidence.source_record_sha256 != bundle.record_sha256:
        raise ValueError("FAST-2A source record hash does not match replay bundle")
    if evidence.source_event_order_sha256 != bundle.event_order_sha256:
        raise ValueError("FAST-2A event order hash does not match replay bundle")


def _validate_signal_identity(
    bundle: ReplayBundleResult,
    manifest: ProfitLabSignalManifest,
) -> None:
    _validate_unique_signals(manifest.signals)
    actual_signals_sha256 = _sha256_json([signal.to_dict() for signal in manifest.signals])
    if manifest.signals_sha256 != actual_signals_sha256:
        raise ValueError("signal manifest SHA-256 does not match signals")
    if manifest.source_record_sha256 != bundle.record_sha256:
        raise ValueError("signal source record hash does not match replay bundle")
    if manifest.source_event_order_sha256 != bundle.event_order_sha256:
        raise ValueError("signal event order hash does not match replay bundle")


def _mapping(value: object, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be an object")
    return value


def _optional_number(value: object) -> float | None:
    if value is None:
        return None
    if not isinstance(value, (int, float, str)):
        raise ValueError(f"expected numeric value, got {type(value).__name__}")
    return float(value)


def _pct(numerator: float, denominator: float) -> float:
    return _round(numerator / denominator * 100) if denominator else 0.0


def _round(value: float) -> float:
    return round(float(value), 10)


def _sha256_json(value: object) -> str:
    return hashlib.sha256(canonical_json({"value": value}).encode("utf-8")).hexdigest()
