from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from enum import StrEnum

from domain.broker.utils import parse_timestamp

_SEOUL_TIMEZONE = timezone(timedelta(hours=9), name="Asia/Seoul")


class ExitTriggerType(StrEnum):
    STOP_LOSS = "STOP_LOSS"
    TAKE_PROFIT = "TAKE_PROFIT"
    TRAILING_STOP = "TRAILING_STOP"
    MAX_HOLD = "MAX_HOLD"
    EOD_FLATTEN = "EOD_FLATTEN"


class ExitOrderStyle(StrEnum):
    SELL_LIMIT = "SELL_LIMIT"
    STOP_SELL = "STOP_SELL"


EXIT_TRIGGER_PRIORITY: tuple[ExitTriggerType, ...] = (
    ExitTriggerType.STOP_LOSS,
    ExitTriggerType.TRAILING_STOP,
    ExitTriggerType.EOD_FLATTEN,
    ExitTriggerType.MAX_HOLD,
    ExitTriggerType.TAKE_PROFIT,
)


@dataclass(frozen=True, kw_only=True)
class ExitPolicyConfig:
    stop_loss_pct: float
    take_profit_pct: float
    trailing_activation_pct: float
    trailing_stop_pct: float
    minimum_hold_sec: int
    maximum_hold_sec: int
    eod_flatten_enabled: bool
    eod_flatten_time: str
    close_only: bool = True
    allow_short: bool = False
    policy_version: str = "shared_exit_policy/v1"

    def __post_init__(self) -> None:
        for field_name in (
            "stop_loss_pct",
            "take_profit_pct",
            "trailing_stop_pct",
        ):
            if float(getattr(self, field_name)) <= 0:
                raise ValueError(f"{field_name} must be > 0")
        if self.trailing_activation_pct < 0:
            raise ValueError("trailing_activation_pct must be >= 0")
        if self.minimum_hold_sec < 0:
            raise ValueError("minimum_hold_sec must be >= 0")
        if self.maximum_hold_sec < 1:
            raise ValueError("maximum_hold_sec must be >= 1")
        if self.maximum_hold_sec < self.minimum_hold_sec:
            raise ValueError("maximum_hold_sec must be >= minimum_hold_sec")
        time.fromisoformat(self.eod_flatten_time)
        if not self.close_only:
            raise ValueError("shared exit policy must remain close-only")
        if self.allow_short:
            raise ValueError("shared exit policy does not allow short positions")
        if not self.policy_version.strip():
            raise ValueError("policy_version is required")


@dataclass(frozen=True, kw_only=True)
class LongPositionSnapshot:
    entry_price: float
    current_price: float
    highest_price: float
    quantity: int
    opened_at: datetime | str
    observed_at: datetime | str

    def __post_init__(self) -> None:
        if self.entry_price <= 0 or self.current_price <= 0 or self.highest_price <= 0:
            raise ValueError("position prices must be > 0")
        if self.quantity <= 0:
            raise ValueError("long position quantity must be > 0")
        opened_at = parse_timestamp(self.opened_at, "opened_at")
        observed_at = parse_timestamp(self.observed_at, "observed_at")
        if opened_at > observed_at:
            raise ValueError("opened_at must not be after observed_at")
        object.__setattr__(self, "opened_at", opened_at)
        object.__setattr__(self, "observed_at", observed_at)
        object.__setattr__(
            self,
            "highest_price",
            max(float(self.highest_price), float(self.current_price)),
        )


@dataclass(frozen=True, kw_only=True)
class ExitTrigger:
    trigger_type: ExitTriggerType
    order_style: ExitOrderStyle
    trigger_price: float | None
    current_price: float
    hold_sec: float
    evidence: dict[str, float | str | bool]


@dataclass(frozen=True, kw_only=True)
class ExitPolicyDecision:
    triggers: tuple[ExitTrigger, ...]
    primary_trigger: ExitTrigger | None
    hold_sec: float
    close_only: bool = True
    allow_short: bool = False


def evaluate_long_exit_policy(
    position: LongPositionSnapshot,
    policy: ExitPolicyConfig,
) -> ExitPolicyDecision:
    opened_at = parse_timestamp(position.opened_at, "opened_at")
    observed_at = parse_timestamp(position.observed_at, "observed_at")
    hold_sec = max((observed_at - opened_at).total_seconds(), 0.0)
    entry_price = float(position.entry_price)
    current_price = float(position.current_price)
    highest_price = float(position.highest_price)
    triggers: list[ExitTrigger] = []

    stop_price = entry_price * (1 - policy.stop_loss_pct / 100)
    if current_price <= stop_price:
        triggers.append(
            ExitTrigger(
                trigger_type=ExitTriggerType.STOP_LOSS,
                order_style=ExitOrderStyle.STOP_SELL,
                trigger_price=stop_price,
                current_price=current_price,
                hold_sec=hold_sec,
                evidence={"stop_loss_pct": policy.stop_loss_pct},
            )
        )

    take_profit_price = entry_price * (1 + policy.take_profit_pct / 100)
    if current_price >= take_profit_price:
        triggers.append(
            ExitTrigger(
                trigger_type=ExitTriggerType.TAKE_PROFIT,
                order_style=ExitOrderStyle.SELL_LIMIT,
                trigger_price=take_profit_price,
                current_price=current_price,
                hold_sec=hold_sec,
                evidence={"take_profit_pct": policy.take_profit_pct},
            )
        )

    trailing_activated = highest_price >= entry_price * (1 + policy.trailing_activation_pct / 100)
    trailing_stop_price = highest_price * (1 - policy.trailing_stop_pct / 100)
    if trailing_activated and current_price <= trailing_stop_price:
        triggers.append(
            ExitTrigger(
                trigger_type=ExitTriggerType.TRAILING_STOP,
                order_style=ExitOrderStyle.STOP_SELL,
                trigger_price=trailing_stop_price,
                current_price=current_price,
                hold_sec=hold_sec,
                evidence={
                    "trailing_activation_pct": policy.trailing_activation_pct,
                    "trailing_stop_pct": policy.trailing_stop_pct,
                    "highest_price": highest_price,
                },
            )
        )

    if hold_sec >= policy.maximum_hold_sec and hold_sec >= policy.minimum_hold_sec:
        triggers.append(
            ExitTrigger(
                trigger_type=ExitTriggerType.MAX_HOLD,
                order_style=ExitOrderStyle.STOP_SELL,
                trigger_price=None,
                current_price=current_price,
                hold_sec=hold_sec,
                evidence={
                    "minimum_hold_sec": float(policy.minimum_hold_sec),
                    "maximum_hold_sec": float(policy.maximum_hold_sec),
                },
            )
        )

    if policy.eod_flatten_enabled and _eod_due(
        observed_at,
        eod_flatten_time=policy.eod_flatten_time,
    ):
        triggers.append(
            ExitTrigger(
                trigger_type=ExitTriggerType.EOD_FLATTEN,
                order_style=ExitOrderStyle.STOP_SELL,
                trigger_price=None,
                current_price=current_price,
                hold_sec=hold_sec,
                evidence={"eod_flatten_time": policy.eod_flatten_time},
            )
        )

    ordered = tuple(
        sorted(
            triggers,
            key=lambda item: EXIT_TRIGGER_PRIORITY.index(item.trigger_type),
        )
    )
    return ExitPolicyDecision(
        triggers=ordered,
        primary_trigger=ordered[0] if ordered else None,
        hold_sec=hold_sec,
    )


def _eod_due(observed_at: datetime, *, eod_flatten_time: str) -> bool:
    local_time = observed_at.astimezone(_SEOUL_TIMEZONE).time().replace(tzinfo=None)
    cutoff = time.fromisoformat(eod_flatten_time)
    return cutoff <= local_time < time(23, 59, 59, 999999)
