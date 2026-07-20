from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

from domain.broker.utils import parse_timestamp, require_non_empty_str, validate_stock_code
from domain.exit.policy import ExitPolicyConfig

PROFIT_LAB_SIGNAL_FORMAT = "profit-lab-signals/v1"
PROFIT_LAB_REPORT_FORMAT = "conservative-profit-lab-report/v1"
_SEOUL_TIMEZONE = timezone(timedelta(hours=9), name="Asia/Seoul")


@dataclass(frozen=True, kw_only=True)
class ProfitLabConfig:
    execution_model_version: str = "conservative_limit/v1"
    cost_model_version: str = "UNCONFIRMED"
    cost_model_confirmed: bool = False
    buy_commission_rate: float = 0.0
    sell_commission_rate: float = 0.0
    sell_tax_rate: float = 0.0
    buy_slippage_ticks: int = 0
    sell_slippage_ticks: int = 0
    entry_latency_ms: int = 250
    exit_latency_ms: int = 250
    entry_ttl_sec: int = 90
    exit_ttl_sec: int = 90
    stop_loss_pct: float = 3.0
    take_profit_pct: float = 5.0
    trailing_activation_pct: float = 2.0
    trailing_stop_pct: float = 2.5
    minimum_hold_sec: int = 30
    maximum_hold_sec: int = 1800
    eod_flatten_enabled: bool = True
    eod_flatten_time: str = "15:15:00"
    exit_policy_version: str = "shared_exit_policy/v1"
    minimum_filled_trades: int = 100
    minimum_distinct_trade_dates: int = 10
    minimum_profit_factor: float = 1.15
    maximum_drawdown_r: float = 8.0
    daily_profit_concentration_warn_ratio: float = 0.35
    stress_expectancy_drop_warn_ratio: float = 0.50
    train_ratio: float = 0.60
    validation_ratio: float = 0.20

    def __post_init__(self) -> None:
        for field_name in (
            "execution_model_version",
            "cost_model_version",
            "exit_policy_version",
        ):
            if not str(getattr(self, field_name)).strip():
                raise ValueError(f"{field_name} is required")
        for field_name in (
            "buy_commission_rate",
            "sell_commission_rate",
            "sell_tax_rate",
        ):
            value = float(getattr(self, field_name))
            if value < 0 or value >= 0.1:
                raise ValueError(f"{field_name} must be in [0, 0.1)")
        for field_name in (
            "buy_slippage_ticks",
            "sell_slippage_ticks",
            "entry_latency_ms",
            "exit_latency_ms",
        ):
            if int(getattr(self, field_name)) < 0:
                raise ValueError(f"{field_name} must be >= 0")
        for field_name in ("entry_ttl_sec", "exit_ttl_sec"):
            if int(getattr(self, field_name)) < 1:
                raise ValueError(f"{field_name} must be >= 1")
        if self.minimum_filled_trades < 1:
            raise ValueError("minimum_filled_trades must be >= 1")
        if self.minimum_distinct_trade_dates < 1:
            raise ValueError("minimum_distinct_trade_dates must be >= 1")
        if self.minimum_profit_factor <= 0:
            raise ValueError("minimum_profit_factor must be > 0")
        if self.maximum_drawdown_r <= 0:
            raise ValueError("maximum_drawdown_r must be > 0")
        for field_name in (
            "daily_profit_concentration_warn_ratio",
            "stress_expectancy_drop_warn_ratio",
            "train_ratio",
            "validation_ratio",
        ):
            value = float(getattr(self, field_name))
            if value < 0 or value > 1:
                raise ValueError(f"{field_name} must be in [0, 1]")
        if self.train_ratio <= 0 or self.validation_ratio <= 0:
            raise ValueError("train_ratio and validation_ratio must be > 0")
        if self.train_ratio + self.validation_ratio >= 1:
            raise ValueError("train_ratio + validation_ratio must be < 1")
        self.exit_policy()

    @property
    def cost_model_complete(self) -> bool:
        normalized_version = self.cost_model_version.strip().upper()
        return (
            self.cost_model_confirmed
            and normalized_version not in {"", "UNKNOWN", "UNCONFIRMED", "TBD"}
            and self.buy_commission_rate > 0
            and self.sell_commission_rate > 0
            and self.sell_tax_rate > 0
            and self.buy_slippage_ticks > 0
            and self.sell_slippage_ticks > 0
        )

    def exit_policy(self) -> ExitPolicyConfig:
        return ExitPolicyConfig(
            stop_loss_pct=self.stop_loss_pct,
            take_profit_pct=self.take_profit_pct,
            trailing_activation_pct=self.trailing_activation_pct,
            trailing_stop_pct=self.trailing_stop_pct,
            minimum_hold_sec=self.minimum_hold_sec,
            maximum_hold_sec=self.maximum_hold_sec,
            eod_flatten_enabled=self.eod_flatten_enabled,
            eod_flatten_time=self.eod_flatten_time,
            policy_version=self.exit_policy_version,
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> ProfitLabConfig:
        return cls(**dict(value))


@dataclass(frozen=True, kw_only=True)
class ProfitLabSignal:
    signal_id: str
    trade_date: str
    code: str
    signal_at: datetime | str
    limit_price: float
    quantity: int
    exchange: str = "KRX"
    side: str = "BUY"
    order_type: str = "LIMIT"
    coherent: bool = True
    setup_type: str = "UNKNOWN"
    regime: str = "UNKNOWN"
    theme: str = "UNKNOWN"
    order_plan_id: str | None = None
    source_lineage: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "signal_id", require_non_empty_str(self.signal_id, "signal_id"))
        object.__setattr__(
            self,
            "trade_date",
            require_non_empty_str(self.trade_date, "trade_date"),
        )
        parsed_trade_date = date.fromisoformat(self.trade_date)
        object.__setattr__(self, "code", validate_stock_code(self.code))
        signal_at = parse_timestamp(self.signal_at, "signal_at")
        if signal_at.astimezone(_SEOUL_TIMEZONE).date() != parsed_trade_date:
            raise ValueError("trade_date must match signal_at in Asia/Seoul")
        object.__setattr__(self, "signal_at", signal_at)
        if float(self.limit_price) <= 0:
            raise ValueError("limit_price must be > 0")
        if int(self.quantity) <= 0:
            raise ValueError("quantity must be > 0")
        exchange = str(self.exchange).strip().upper()
        if exchange not in {"KRX", "NXT"}:
            raise ValueError("exchange must be KRX or NXT")
        object.__setattr__(self, "exchange", exchange)
        if str(self.side).strip().upper() != "BUY":
            raise ValueError("Profit Lab accepts BUY signals only")
        if str(self.order_type).strip().upper() != "LIMIT":
            raise ValueError("Profit Lab accepts LIMIT signals only")
        object.__setattr__(self, "side", "BUY")
        object.__setattr__(self, "order_type", "LIMIT")
        for field_name in ("setup_type", "regime", "theme"):
            normalized = str(getattr(self, field_name) or "UNKNOWN").strip() or "UNKNOWN"
            object.__setattr__(self, field_name, normalized.upper())
        if self.order_plan_id is not None:
            object.__setattr__(
                self,
                "order_plan_id",
                require_non_empty_str(self.order_plan_id, "order_plan_id"),
            )
        object.__setattr__(self, "source_lineage", dict(self.source_lineage or {}))

    def to_dict(self) -> dict[str, Any]:
        signal_at = parse_timestamp(self.signal_at, "signal_at")
        return {
            "signal_id": self.signal_id,
            "trade_date": self.trade_date,
            "code": self.code,
            "signal_at": signal_at.isoformat().replace("+00:00", "Z"),
            "limit_price": float(self.limit_price),
            "quantity": int(self.quantity),
            "exchange": self.exchange,
            "side": self.side,
            "order_type": self.order_type,
            "coherent": bool(self.coherent),
            "setup_type": self.setup_type,
            "regime": self.regime,
            "theme": self.theme,
            "order_plan_id": self.order_plan_id,
            "source_lineage": dict(self.source_lineage or {}),
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> ProfitLabSignal:
        return cls(**dict(value))


@dataclass(frozen=True, kw_only=True)
class ProfitLabTrade:
    signal_id: str
    trade_date: str
    signal_at: str
    dataset_split: str
    code: str
    exchange: str
    setup_type: str
    regime: str
    theme: str
    status: str
    requested_entry_limit: int
    quantity: int
    entry_filled_at: str | None = None
    entry_market_price: int | None = None
    entry_fill_price: int | None = None
    exit_triggered_at: str | None = None
    exit_filled_at: str | None = None
    exit_trigger_type: str | None = None
    exit_order_style: str | None = None
    exit_trigger_price: int | None = None
    exit_market_price: int | None = None
    exit_fill_price: int | None = None
    gross_pnl: float | None = None
    buy_commission: float | None = None
    sell_commission: float | None = None
    sell_tax: float | None = None
    slippage_cost: float | None = None
    total_cost: float | None = None
    net_pnl: float | None = None
    net_r: float | None = None
    mae_pct: float | None = None
    mfe_pct: float | None = None
    holding_sec: float | None = None
    reason_codes: tuple[str, ...] = ()

    @property
    def entry_filled(self) -> bool:
        return self.entry_fill_price is not None

    @property
    def closed(self) -> bool:
        return self.status == "CLOSED"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self) | {
            "entry_filled": self.entry_filled,
            "closed": self.closed,
            "reason_codes": list(self.reason_codes),
        }
