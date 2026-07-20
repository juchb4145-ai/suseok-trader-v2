from __future__ import annotations

import json
import statistics
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from domain.broker.utils import (
    datetime_to_wire,
    parse_bool,
    parse_timestamp,
    require_non_empty_str,
)

from services.entry_timing.tick_size import price_tick_distance
from services.parallel_shadow.models import (
    PARALLEL_SHADOW_INPUT_FORMAT,
    PARALLEL_SHADOW_REPORT_FORMAT,
    LiveSimObservation,
    ShadowExecution,
    ShadowLiveComparison,
    ShadowPlan,
    ShadowPreflight,
    canonical_sha256,
    validate_plan_identity,
)
from services.profit_lab.engine import ExecutionTick, simulate_conservative_execution
from services.profit_lab.models import ProfitLabConfig, ProfitLabTrade

_SEOUL_TIMEZONE = timezone(timedelta(hours=9), name="Asia/Seoul")


@dataclass(frozen=True, kw_only=True)
class ParallelShadowFrame:
    snapshot_id: str
    generated_at: datetime | str
    trade_date: str
    plan_coverage_complete: bool
    source_plan_count: int
    source_plan_ids_sha256: str
    plans: tuple[ShadowPlan, ...]
    ticks: tuple[ExecutionTick, ...]
    preflight: ShadowPreflight
    live_sim_observations: tuple[LiveSimObservation, ...] = ()
    ai_advisory_only: bool = True
    live_real_allowed: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "snapshot_id",
            require_non_empty_str(self.snapshot_id, "snapshot_id"),
        )
        generated_at = parse_timestamp(self.generated_at, "generated_at")
        object.__setattr__(self, "generated_at", generated_at)
        parsed_trade_date = date.fromisoformat(self.trade_date)
        if generated_at.astimezone(_SEOUL_TIMEZONE).date() != parsed_trade_date:
            raise ValueError("trade_date must match generated_at in Asia/Seoul")
        object.__setattr__(
            self,
            "plan_coverage_complete",
            parse_bool(self.plan_coverage_complete, "plan_coverage_complete"),
        )
        object.__setattr__(self, "source_plan_count", int(self.source_plan_count))
        if self.source_plan_count < 0:
            raise ValueError("source_plan_count must be >= 0")
        object.__setattr__(
            self,
            "source_plan_ids_sha256",
            require_non_empty_str(
                self.source_plan_ids_sha256,
                "source_plan_ids_sha256",
            ).lower(),
        )
        validate_plan_identity(
            self.plans,
            source_plan_count=self.source_plan_count,
            source_plan_ids_sha256=self.source_plan_ids_sha256,
        )
        if any(plan.trade_date != self.trade_date for plan in self.plans):
            raise ValueError("all plans must match frame trade_date")
        _validate_ticks(self.ticks, generated_at=generated_at)
        live_plan_ids = [item.order_plan_id for item in self.live_sim_observations]
        if len(live_plan_ids) != len(set(live_plan_ids)):
            raise ValueError("only one LIVE_SIM observation is allowed per order plan")
        object.__setattr__(
            self,
            "ai_advisory_only",
            parse_bool(self.ai_advisory_only, "ai_advisory_only"),
        )
        if parse_bool(self.live_real_allowed, "live_real_allowed"):
            raise ValueError("LIVE_REAL is not allowed in parallel shadow input")
        object.__setattr__(self, "live_real_allowed", False)

    @property
    def input_sha256(self) -> str:
        return canonical_sha256(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        return {
            "format": PARALLEL_SHADOW_INPUT_FORMAT,
            "snapshot_id": self.snapshot_id,
            "generated_at": datetime_to_wire(
                parse_timestamp(self.generated_at, "generated_at")
            ),
            "trade_date": self.trade_date,
            "plan_coverage_complete": self.plan_coverage_complete,
            "source_plan_count": self.source_plan_count,
            "source_plan_ids_sha256": self.source_plan_ids_sha256,
            "plans": [plan.to_dict() for plan in self.plans],
            "ticks": [tick.to_dict() for tick in self.ticks],
            "preflight": self.preflight.to_dict(),
            "live_sim_observations": [
                observation.to_dict() for observation in self.live_sim_observations
            ],
            "ai_advisory_only": self.ai_advisory_only,
            "live_real_allowed": False,
        }


@dataclass(frozen=True, kw_only=True)
class ParallelShadowResult:
    status: str
    blocker_reasons: tuple[str, ...]
    warnings: tuple[str, ...]
    frame: ParallelShadowFrame
    config: ProfitLabConfig
    config_sha256: str
    commit_sha: str
    deterministic_identity_sha256: str
    result_sha256: str
    executions: tuple[ShadowExecution, ...]
    comparisons: tuple[ShadowLiveComparison, ...]
    metrics: Mapping[str, Any]
    operational_db_write_count: int = 0
    gateway_command_write_count: int = 0
    live_sim_write_count: int = 0
    broker_call_count: int = 0

    @property
    def no_trading_side_effects(self) -> bool:
        return (
            self.operational_db_write_count
            + self.gateway_command_write_count
            + self.live_sim_write_count
            + self.broker_call_count
            == 0
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "format": PARALLEL_SHADOW_REPORT_FORMAT,
            "status": self.status,
            "blocker_reasons": list(self.blocker_reasons),
            "warnings": list(self.warnings),
            "identity": {
                "input_sha256": self.frame.input_sha256,
                "config_sha256": self.config_sha256,
                "commit_sha": self.commit_sha,
                "deterministic_identity_sha256": self.deterministic_identity_sha256,
            },
            "result_sha256": self.result_sha256,
            "snapshot_id": self.frame.snapshot_id,
            "trade_date": self.frame.trade_date,
            "preflight": self.frame.preflight.to_dict(),
            "config": self.config.to_dict(),
            "cost_model_complete": self.config.cost_model_complete,
            "executions": [item.to_dict() for item in self.executions],
            "comparisons": [item.to_dict() for item in self.comparisons],
            "metrics": dict(self.metrics),
            "safety": {
                "file_only": True,
                "observe_only": True,
                "ai_advisory_only": self.frame.ai_advisory_only,
                "operational_db_write_count": self.operational_db_write_count,
                "gateway_command_write_count": self.gateway_command_write_count,
                "live_sim_write_count": self.live_sim_write_count,
                "broker_call_count": self.broker_call_count,
                "no_trading_side_effects": self.no_trading_side_effects,
                "live_sim_allowed": False,
                "live_real_allowed": False,
            },
        }


def load_parallel_shadow_frame(path: str | Path) -> ParallelShadowFrame:
    input_path = Path(path).expanduser().resolve()
    if not input_path.is_file():
        raise FileNotFoundError(f"parallel shadow input is missing: {input_path}")
    parsed = json.loads(input_path.read_text(encoding="utf-8"))
    value = _mapping(parsed, "parallel shadow input")
    if value.get("format") != PARALLEL_SHADOW_INPUT_FORMAT:
        raise ValueError("unsupported parallel shadow input format")
    plans_raw = value.get("plans")
    ticks_raw = value.get("ticks")
    observations_raw = value.get("live_sim_observations") or []
    if not isinstance(plans_raw, list):
        raise ValueError("plans must be a list")
    if not isinstance(ticks_raw, list):
        raise ValueError("ticks must be a list")
    if not isinstance(observations_raw, list):
        raise ValueError("live_sim_observations must be a list")
    return ParallelShadowFrame(
        snapshot_id=str(value.get("snapshot_id") or ""),
        generated_at=str(value.get("generated_at") or ""),
        trade_date=str(value.get("trade_date") or ""),
        plan_coverage_complete=value.get("plan_coverage_complete", False),
        source_plan_count=int(value.get("source_plan_count") or 0),
        source_plan_ids_sha256=str(value.get("source_plan_ids_sha256") or ""),
        plans=tuple(ShadowPlan.from_mapping(_mapping(item, "plan")) for item in plans_raw),
        ticks=tuple(
            ExecutionTick.from_mapping(_mapping(item, "tick")) for item in ticks_raw
        ),
        preflight=ShadowPreflight.from_mapping(_mapping(value.get("preflight"), "preflight")),
        live_sim_observations=tuple(
            LiveSimObservation.from_mapping(_mapping(item, "live_sim_observation"))
            for item in observations_raw
        ),
        ai_advisory_only=value.get("ai_advisory_only", True),
        live_real_allowed=value.get("live_real_allowed", False),
    )


def run_parallel_shadow(
    frame: ParallelShadowFrame,
    *,
    config: ProfitLabConfig | None = None,
    commit_sha: str = "UNKNOWN",
) -> ParallelShadowResult:
    resolved_config = config or ProfitLabConfig()
    normalized_commit = str(commit_sha or "UNKNOWN").strip() or "UNKNOWN"
    config_sha256 = canonical_sha256(resolved_config.to_dict())
    identity = {
        "input_sha256": frame.input_sha256,
        "config_sha256": config_sha256,
        "commit_sha": normalized_commit,
        "execution_model_version": resolved_config.execution_model_version,
        "exit_policy_version": resolved_config.exit_policy_version,
        "cost_model_version": resolved_config.cost_model_version,
    }
    identity_sha256 = canonical_sha256(identity)
    eligible_plans = tuple(plan for plan in frame.plans if plan.shadow_eligible)
    trades = simulate_conservative_execution(
        [plan.to_profit_lab_signal() for plan in eligible_plans],
        frame.ticks,
        config=resolved_config,
        split_by_date={frame.trade_date: "SHADOW"},
    )
    plan_by_id = {plan.order_plan_id: plan for plan in eligible_plans}
    executions = tuple(
        _shadow_execution(
            plan_by_id[trade.signal_id],
            trade,
            deterministic_identity_sha256=identity_sha256,
        )
        for trade in trades
    )
    execution_by_plan = {item.order_plan_id: item for item in executions}
    comparisons = tuple(
        _compare(execution_by_plan.get(item.order_plan_id), item)
        for item in frame.live_sim_observations
    )
    metrics = _metrics(frame, eligible_plans, executions, comparisons)
    blockers, warnings = _verdict(
        frame,
        resolved_config,
        metrics=metrics,
        comparisons=comparisons,
        commit_sha=normalized_commit,
    )
    status = "BLOCKED" if blockers else "WARN" if warnings else "PASS"
    deterministic_result = {
        "identity_sha256": identity_sha256,
        "status": status,
        "blocker_reasons": sorted(set(blockers)),
        "warnings": sorted(set(warnings)),
        "executions": [item.to_dict() for item in executions],
        "comparisons": [item.to_dict() for item in comparisons],
        "metrics": metrics,
    }
    return ParallelShadowResult(
        status=status,
        blocker_reasons=tuple(sorted(set(blockers))),
        warnings=tuple(sorted(set(warnings))),
        frame=frame,
        config=resolved_config,
        config_sha256=config_sha256,
        commit_sha=normalized_commit,
        deterministic_identity_sha256=identity_sha256,
        result_sha256=canonical_sha256(deterministic_result),
        executions=executions,
        comparisons=comparisons,
        metrics=metrics,
    )


def _shadow_execution(
    plan: ShadowPlan,
    trade: ProfitLabTrade,
    *,
    deterministic_identity_sha256: str,
) -> ShadowExecution:
    base = {
        "identity_sha256": deterministic_identity_sha256,
        "order_plan_id": plan.order_plan_id,
    }
    execution_id = f"shadow_exec_{canonical_sha256(base)[:24]}"
    fill_id = (
        f"shadow_fill_{canonical_sha256(base | {'entity': 'fill'})[:24]}"
        if trade.entry_filled
        else None
    )
    position_id = (
        f"shadow_pos_{canonical_sha256(base | {'entity': 'position'})[:24]}"
        if trade.entry_filled
        else None
    )
    return ShadowExecution(
        shadow_execution_id=execution_id,
        shadow_fill_id=fill_id,
        shadow_position_id=position_id,
        order_plan_id=plan.order_plan_id,
        entry_timing_evaluation_id=plan.entry_timing_evaluation_id,
        strategy_observation_id=plan.strategy_observation_id,
        risk_observation_id=plan.risk_observation_id,
        source_run_id=plan.source_run_id,
        source_watermark_hash=plan.source_watermark_hash,
        status=trade.status,
        entry_filled_at=trade.entry_filled_at,
        entry_fill_price=trade.entry_fill_price,
        quantity=trade.quantity,
        exit_reason=trade.exit_trigger_type,
        exit_filled_at=trade.exit_filled_at,
        holding_sec=trade.holding_sec,
        gross_pnl=trade.gross_pnl,
        net_pnl=trade.net_pnl,
    )


def _compare(
    shadow: ShadowExecution | None,
    live: LiveSimObservation,
) -> ShadowLiveComparison:
    gaps: list[str] = []
    if shadow is None:
        gaps.append("SHADOW_EXECUTION_MISSING")
    if live.filled and not live.execution_ids:
        gaps.append("LIVE_SIM_EXECUTION_LINK_MISSING")
    if live.filled and live.position_id is None:
        gaps.append("LIVE_SIM_POSITION_LINK_MISSING")
    if shadow is not None and live.requested_quantity != shadow.quantity:
        gaps.append("REQUEST_QUANTITY_LINK_MISMATCH")
    shadow_filled = bool(shadow and shadow.filled)
    fill_disagreement = shadow_filled != live.filled
    fill_time_delta: float | None = None
    slippage_ticks: int | None = None
    slippage_pct: float | None = None
    if shadow and shadow.entry_filled_at and live.first_filled_at is not None:
        fill_time_delta = _round(
            (
                parse_timestamp(live.first_filled_at, "first_filled_at")
                - parse_timestamp(shadow.entry_filled_at, "shadow.entry_filled_at")
            ).total_seconds()
        )
    if shadow and shadow.entry_fill_price is not None and live.avg_fill_price is not None:
        slippage_ticks = price_tick_distance(shadow.entry_fill_price, live.avg_fill_price)
        slippage_pct = _round(
            (live.avg_fill_price - shadow.entry_fill_price)
            / shadow.entry_fill_price
            * 100
        )
    shadow_exit_reason = shadow.exit_reason if shadow else None
    exit_reason_disagreement = (
        shadow_exit_reason is not None or live.exit_reason is not None
    ) and shadow_exit_reason != live.exit_reason
    return ShadowLiveComparison(
        order_plan_id=live.order_plan_id,
        shadow_execution_id=shadow.shadow_execution_id if shadow else None,
        shadow_fill_id=shadow.shadow_fill_id if shadow else None,
        shadow_position_id=shadow.shadow_position_id if shadow else None,
        live_sim_intent_id=live.live_sim_intent_id,
        live_sim_order_id=live.live_sim_order_id,
        live_sim_execution_ids=live.execution_ids,
        live_sim_position_id=live.position_id,
        linkage_complete=not gaps,
        linkage_gaps=tuple(gaps),
        shadow_filled=shadow_filled,
        live_filled=live.filled,
        fill_disagreement=fill_disagreement,
        fill_time_delta_sec=fill_time_delta,
        slippage_ticks=slippage_ticks,
        slippage_pct=slippage_pct,
        partial_fill=live.partial_fill,
        shadow_exit_reason=shadow_exit_reason,
        live_exit_reason=live.exit_reason,
        exit_reason_disagreement=exit_reason_disagreement,
        holding_time_delta_sec=_optional_delta(
            live.holding_sec,
            shadow.holding_sec if shadow else None,
        ),
        gross_pnl_delta=_optional_delta(live.gross_pnl, shadow.gross_pnl if shadow else None),
        net_pnl_delta=_optional_delta(live.net_pnl, shadow.net_pnl if shadow else None),
    )


def _metrics(
    frame: ParallelShadowFrame,
    eligible_plans: Sequence[ShadowPlan],
    executions: Sequence[ShadowExecution],
    comparisons: Sequence[ShadowLiveComparison],
) -> dict[str, Any]:
    live_count = len(frame.live_sim_observations)
    fill_time_deltas = [
        abs(item.fill_time_delta_sec)
        for item in comparisons
        if item.fill_time_delta_sec is not None
    ]
    return {
        "source_plan_count": frame.source_plan_count,
        "plan_ready_count": sum(plan.status == "PLAN_READY" for plan in frame.plans),
        "coherent_plan_ready_count": len(eligible_plans),
        "shadow_execution_count": len(executions),
        "shadow_fill_count": sum(item.filled for item in executions),
        "duplicate_shadow_plan_count": len(executions)
        - len({item.order_plan_id for item in executions}),
        "shadow_plan_coverage_gap_count": max(len(eligible_plans) - len(executions), 0),
        "live_canary_plan_count": live_count,
        "live_buy_count_when_blocked": (
            live_count if frame.preflight.live_buy_blocked else 0
        ),
        "shadow_retained_when_live_blocked": (
            not frame.preflight.live_buy_blocked or len(executions) == len(eligible_plans)
        ),
        "ai_influenced_plan_count": sum(plan.ai_influenced for plan in eligible_plans),
        "comparison_count": len(comparisons),
        "comparison_linkage_gap_count": sum(not item.linkage_complete for item in comparisons),
        "fill_disagreement_count": sum(item.fill_disagreement for item in comparisons),
        "partial_fill_count": sum(item.partial_fill for item in comparisons),
        "exit_reason_disagreement_count": sum(
            item.exit_reason_disagreement for item in comparisons
        ),
        "fill_time_delta_sec_average_abs": (
            _round(statistics.fmean(fill_time_deltas)) if fill_time_deltas else None
        ),
    }


def _verdict(
    frame: ParallelShadowFrame,
    config: ProfitLabConfig,
    *,
    metrics: Mapping[str, Any],
    comparisons: Sequence[ShadowLiveComparison],
    commit_sha: str,
) -> tuple[list[str], list[str]]:
    blockers: list[str] = []
    warnings: list[str] = []
    if not frame.plan_coverage_complete:
        blockers.append("PLAN_COVERAGE_INCOMPLETE")
    if not config.cost_model_complete:
        blockers.append("COST_MODEL_MISSING")
    if int(metrics["duplicate_shadow_plan_count"]):
        blockers.append("DUPLICATE_SHADOW_PLAN")
    if int(metrics["shadow_plan_coverage_gap_count"]):
        blockers.append("SHADOW_PLAN_COVERAGE_GAP")
    if int(metrics["live_canary_plan_count"]) > 1:
        blockers.append("LIVE_CANARY_LIMIT_EXCEEDED")
    if int(metrics["live_buy_count_when_blocked"]):
        blockers.append("LIVE_BUY_PRESENT_WHEN_BLOCKED")
    if not bool(metrics["shadow_retained_when_live_blocked"]):
        blockers.append("SHADOW_NOT_RETAINED_WHEN_LIVE_BLOCKED")
    if not frame.ai_advisory_only or int(metrics["ai_influenced_plan_count"]):
        blockers.append("AI_INFLUENCE_DETECTED")
    if int(metrics["comparison_linkage_gap_count"]):
        blockers.append("COMPARISON_LINKAGE_GAP")
    if not frame.plans:
        warnings.append("NO_SOURCE_PLANS")
    elif not int(metrics["coherent_plan_ready_count"]):
        warnings.append("NO_COHERENT_PLAN_READY")
    if not comparisons:
        warnings.append("NO_LIVE_CANARY_COMPARISON")
    if int(metrics["fill_disagreement_count"]):
        warnings.append("FILL_DISAGREEMENT_OBSERVED")
    if int(metrics["partial_fill_count"]):
        warnings.append("LIVE_PARTIAL_FILL_OBSERVED")
    if int(metrics["exit_reason_disagreement_count"]):
        warnings.append("EXIT_REASON_DISAGREEMENT_OBSERVED")
    if commit_sha == "UNKNOWN":
        warnings.append("COMMIT_IDENTITY_UNKNOWN")
    return blockers, warnings


def _validate_ticks(ticks: Sequence[ExecutionTick], *, generated_at: datetime) -> None:
    sequences = [tick.sequence for tick in ticks]
    event_ids = [tick.event_id for tick in ticks]
    if len(sequences) != len(set(sequences)):
        raise ValueError("parallel shadow tick sequence must be unique")
    if len(event_ids) != len(set(event_ids)):
        raise ValueError("parallel shadow tick event_id must be unique")
    previous_available: datetime | None = None
    for tick in ticks:
        available_at = parse_timestamp(tick.available_at, "available_at")
        if previous_available is not None and available_at < previous_available:
            raise ValueError("parallel shadow tick availability must be monotonic")
        if available_at > generated_at:
            raise ValueError("parallel shadow input contains a future tick")
        previous_available = available_at


def _optional_delta(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    return _round(float(left) - float(right))


def _round(value: float) -> float:
    return round(float(value), 10)


def _mapping(value: object, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be an object")
    return value
