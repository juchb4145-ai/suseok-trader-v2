from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from domain.broker.utils import normalize_value


class NoBuyStatus(StrEnum):
    OK_TRADING_ACTIVITY = "OK_TRADING_ACTIVITY"
    NO_CANDIDATE = "NO_CANDIDATE"
    GATEWAY_REALTIME_STALLED = "GATEWAY_REALTIME_STALLED"
    MARKET_NO_TRADE = "MARKET_NO_TRADE"
    THEME_DATA_WAIT = "THEME_DATA_WAIT"
    ENTRY_TIMING_WAIT = "ENTRY_TIMING_WAIT"
    ORDER_PLAN_NOT_READY = "ORDER_PLAN_NOT_READY"
    LIVE_SIM_SAFETY_BLOCK = "LIVE_SIM_SAFETY_BLOCK"
    RECONCILE_BLOCK = "RECONCILE_BLOCK"
    DUPLICATE_OR_POSITION_BLOCK = "DUPLICATE_OR_POSITION_BLOCK"
    CONFIG_DISABLED = "CONFIG_DISABLED"
    GATEWAY_UNAVAILABLE = "GATEWAY_UNAVAILABLE"
    AI_NO_TRADE = "AI_NO_TRADE"
    MIXED_BLOCKS = "MIXED_BLOCKS"
    UNKNOWN = "UNKNOWN"


class StageCategory(StrEnum):
    THEME = "THEME"
    CANDIDATE = "CANDIDATE"
    ENTRY_TIMING = "ENTRY_TIMING"
    STRATEGY = "STRATEGY"
    RISK = "RISK"
    ORDER_PLAN = "ORDER_PLAN"
    LIVE_SIM_SAFETY = "LIVE_SIM_SAFETY"
    EXECUTION_LIFECYCLE = "EXECUTION_LIFECYCLE"
    RECONCILE = "RECONCILE"
    DUPLICATE_POSITION = "DUPLICATE_POSITION"
    LIMIT = "LIMIT"
    CONFIG = "CONFIG"
    GATEWAY = "GATEWAY"
    AI_ADVISORY = "AI_ADVISORY"
    DATA_WAIT = "DATA_WAIT"
    UNKNOWN = "UNKNOWN"


class BlockType(StrEnum):
    HARD_BLOCK = "HARD_BLOCK"
    SOFT_WAIT = "SOFT_WAIT"
    DATA_WAIT = "DATA_WAIT"
    THRESHOLD_MISS = "THRESHOLD_MISS"
    SAFETY_BLOCK = "SAFETY_BLOCK"
    CONFIG_DISABLED = "CONFIG_DISABLED"
    AI_NO_TRADE = "AI_NO_TRADE"
    NOT_APPLICABLE = "NOT_APPLICABLE"


@dataclass(frozen=True, kw_only=True)
class ReasonClassification:
    reason_code: str
    stage: StageCategory = StageCategory.UNKNOWN
    block_type: BlockType = BlockType.NOT_APPLICABLE
    operator_hint: str = "관련 원천 데이터와 최근 오류를 확인합니다."

    def to_dict(self) -> dict[str, Any]:
        return {
            "reason_code": self.reason_code,
            "stage": self.stage.value,
            "block_type": self.block_type.value,
            "operator_hint": self.operator_hint,
        }


@dataclass(frozen=True, kw_only=True)
class NoBuySentinelSnapshot:
    snapshot_id: str
    trade_date: str
    evaluated_at: str
    market_session: str
    status: NoBuyStatus
    no_buy_detected: bool
    intent_count: int = 0
    order_count: int = 0
    command_count: int = 0
    plan_ready_count: int = 0
    buy_eligible_count: int = 0
    ai_selected_count: int = 0
    top_near_miss: Sequence[Mapping[str, Any]] = field(default_factory=tuple)
    stage_summary: Mapping[str, Any] = field(default_factory=dict)
    stage_funnel: Mapping[str, Any] = field(default_factory=dict)
    reason_summary: Mapping[str, Any] = field(default_factory=dict)
    ai_summary: Mapping[str, Any] = field(default_factory=dict)
    system_summary: Mapping[str, Any] = field(default_factory=dict)
    operator_checklist: Sequence[str] = field(default_factory=tuple)
    created_at: str = ""
    read_only: bool = True
    no_order_side_effects: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "trade_date": self.trade_date,
            "evaluated_at": self.evaluated_at,
            "market_session": self.market_session,
            "status": self.status.value,
            "no_buy_detected": bool(self.no_buy_detected),
            "intent_count": int(self.intent_count),
            "order_count": int(self.order_count),
            "command_count": int(self.command_count),
            "plan_ready_count": int(self.plan_ready_count),
            "buy_eligible_count": int(self.buy_eligible_count),
            "ai_selected_count": int(self.ai_selected_count),
            "top_near_miss": normalize_value(list(self.top_near_miss)),
            "stage_summary": normalize_value(dict(self.stage_summary)),
            "stage_funnel": normalize_value(dict(self.stage_funnel)),
            "reason_summary": normalize_value(dict(self.reason_summary)),
            "ai_summary": normalize_value(dict(self.ai_summary)),
            "system_summary": normalize_value(dict(self.system_summary)),
            "operator_checklist": list(self.operator_checklist),
            "created_at": self.created_at or self.evaluated_at,
            "read_only": True,
            "no_order_side_effects": True,
        }
