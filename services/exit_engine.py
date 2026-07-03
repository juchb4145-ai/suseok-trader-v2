from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from domain.broker.utils import (
    datetime_to_wire,
    new_message_id,
    normalize_value,
    parse_timestamp,
    require_non_empty_str,
    utc_now,
    validate_stock_code,
)
from domain.exit.models import (
    DryRunExitEvaluation,
    DryRunExitExecution,
    DryRunExitIntent,
    DryRunExitOrder,
    DryRunExitSignal,
)
from domain.exit.reasons import DryRunExitReasonCode
from domain.exit.rules import (
    CAUTION_SIGNAL_TYPES,
    RISK_DETERIORATED_STATUSES,
    SIGNAL_REASON_BY_TYPE,
    STRATEGY_INVALIDATED_STATUSES,
    THEME_WEAK_STATES,
)
from domain.exit.status import (
    DryRunExitEvaluationStatus,
    DryRunExitIntentStatus,
    DryRunExitOrderStatus,
    DryRunExitSignalType,
)
from domain.market.quality import tick_age_seconds

from services.config import Settings, TradingMode, load_settings
from services.oms.safety_gate import check_pr10_safety_gate

ACTIVE_POSITION_STATUSES = {"OPEN"}
ACTIVE_EXIT_ORDER_STATUSES = {
    DryRunExitOrderStatus.CREATED.value,
    DryRunExitOrderStatus.SIMULATED_SUBMITTED.value,
}


@dataclass(frozen=True, kw_only=True)
class DryRunExitRunResult:
    run_id: str
    trade_date: str | None = None
    evaluated_position_count: int = 0
    exit_signal_count: int = 0
    exit_intent_count: int = 0
    exit_order_count: int = 0
    exit_execution_count: int = 0
    rejection_count: int = 0
    error_count: int = 0
    status: str = "COMPLETED"
    dry_run_only: bool = True
    close_only: bool = True
    live_order_allowed: bool = False
    gateway_command_allowed: bool = False
    broker_order_sent: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "trade_date": self.trade_date,
            "evaluated_position_count": self.evaluated_position_count,
            "exit_signal_count": self.exit_signal_count,
            "exit_intent_count": self.exit_intent_count,
            "exit_order_count": self.exit_order_count,
            "exit_execution_count": self.exit_execution_count,
            "rejection_count": self.rejection_count,
            "error_count": self.error_count,
            "status": self.status,
            "dry_run_only": True,
            "close_only": True,
            "live_order_allowed": False,
            "gateway_command_allowed": False,
            "broker_order_sent": False,
        }


def evaluate_dry_run_exit_for_position(
    connection: sqlite3.Connection,
    dry_run_position_id: str,
    settings: Settings | None = None,
) -> DryRunExitEvaluation:
    resolved_settings = settings or load_settings()
    position_id = require_non_empty_str(dry_run_position_id, "dry_run_position_id")
    now = utc_now()
    observed_at = datetime_to_wire(now)
    position = _position_row(connection, position_id)

    if position is None:
        evaluation = DryRunExitEvaluation(
            exit_evaluation_id=new_message_id("dry_run_exit_eval"),
            dry_run_position_id=position_id,
            trade_date="UNKNOWN",
            code="000000",
            name="UNKNOWN",
            evaluated_at=observed_at,
            status=DryRunExitEvaluationStatus.INVALID_POSITION,
            quantity=0,
            avg_price=0,
            reason_codes=[DryRunExitReasonCode.POSITION_NOT_FOUND.value],
            evidence_json=_base_evidence()
            | {"dry_run_position_id": position_id, "position_found": False},
            config_version=resolved_settings.dry_run_exit_config_version,
        )
        _insert_exit_evaluation(connection, evaluation)
        connection.commit()
        return evaluation

    quantity = int(position["quantity"])
    avg_price = float(position["avg_price"])
    code = validate_stock_code(position["code"])
    status = str(position["status"]).upper()
    invalid_reasons: list[str] = []
    if status not in ACTIVE_POSITION_STATUSES:
        invalid_reasons.append(DryRunExitReasonCode.POSITION_ALREADY_CLOSED.value)
    if quantity <= 0:
        invalid_reasons.append(DryRunExitReasonCode.POSITION_QUANTITY_INVALID.value)
    if invalid_reasons:
        evaluation = DryRunExitEvaluation(
            exit_evaluation_id=new_message_id("dry_run_exit_eval"),
            dry_run_position_id=position_id,
            trade_date=str(position["trade_date"]),
            code=code,
            name=str(position["name"]),
            evaluated_at=observed_at,
            status=DryRunExitEvaluationStatus.INVALID_POSITION,
            quantity=max(quantity, 0),
            avg_price=max(avg_price, 0),
            last_price=_optional_float(position["last_price"]),
            reason_codes=invalid_reasons,
            evidence_json=_base_evidence() | {"position": _row_to_dict(position)},
            config_version=resolved_settings.dry_run_exit_config_version,
        )
        _insert_exit_evaluation(connection, evaluation)
        connection.commit()
        return evaluation

    tick = _latest_tick_row(connection, code)
    if tick is None:
        evaluation = DryRunExitEvaluation(
            exit_evaluation_id=new_message_id("dry_run_exit_eval"),
            dry_run_position_id=position_id,
            trade_date=str(position["trade_date"]),
            code=code,
            name=str(position["name"]),
            evaluated_at=observed_at,
            status=DryRunExitEvaluationStatus.DATA_WAIT,
            quantity=quantity,
            avg_price=avg_price,
            last_price=_optional_float(position["last_price"]),
            reason_codes=[DryRunExitReasonCode.LATEST_TICK_MISSING.value],
            evidence_json=_base_evidence() | {"position": _position_evidence(position)},
            config_version=resolved_settings.dry_run_exit_config_version,
        )
        _insert_exit_evaluation(connection, evaluation)
        connection.commit()
        return evaluation

    last_price = float(tick["price"])
    unrealized_pnl = (last_price - avg_price) * quantity
    unrealized_pnl_pct = ((last_price - avg_price) / avg_price * 100) if avg_price > 0 else 0.0
    metrics = update_position_high_low_watermark(connection, position_id, last_price)
    high_watermark = _optional_float(metrics.get("high_watermark_price"))
    drawdown_from_high_pct = (
        (high_watermark - last_price) / high_watermark * 100
        if high_watermark and high_watermark > 0
        else 0.0
    )
    hold_sec = _hold_seconds(position["opened_at"], now=now)
    tick_age = tick_age_seconds(tick["event_ts"], now=now)

    signals: list[DryRunExitSignal] = []
    reason_codes: list[str] = []
    evidence = _base_evidence() | {
        "position": _position_evidence(position),
        "latest_tick": _tick_evidence(tick, tick_age),
        "unrealized_pnl": unrealized_pnl,
        "unrealized_pnl_pct": unrealized_pnl_pct,
        "high_watermark_price": high_watermark,
        "drawdown_from_high_pct": drawdown_from_high_pct,
        "hold_sec": hold_sec,
    }

    if tick_age > resolved_settings.dry_run_exit_stale_tick_sec:
        _append_signal(
            signals,
            position_id=position_id,
            signal_type=DryRunExitSignalType.DATA_STALE_EXIT_CAUTION,
            current_price=last_price,
            threshold_value=float(resolved_settings.dry_run_exit_stale_tick_sec),
            evidence={"tick_age_sec": tick_age},
            observed_at=observed_at,
        )
        reason_codes.append(DryRunExitReasonCode.LATEST_TICK_STALE.value)

    stop_threshold = avg_price * (1 - resolved_settings.dry_run_exit_stop_loss_pct / 100)
    if avg_price > 0 and last_price <= stop_threshold:
        _append_signal(
            signals,
            position_id=position_id,
            signal_type=DryRunExitSignalType.STOP_LOSS,
            trigger_price=stop_threshold,
            current_price=last_price,
            threshold_value=resolved_settings.dry_run_exit_stop_loss_pct,
            evidence={"unrealized_pnl_pct": unrealized_pnl_pct},
            observed_at=observed_at,
        )

    take_threshold = avg_price * (1 + resolved_settings.dry_run_exit_take_profit_pct / 100)
    if avg_price > 0 and last_price >= take_threshold:
        _append_signal(
            signals,
            position_id=position_id,
            signal_type=DryRunExitSignalType.TAKE_PROFIT,
            trigger_price=take_threshold,
            current_price=last_price,
            threshold_value=resolved_settings.dry_run_exit_take_profit_pct,
            evidence={"unrealized_pnl_pct": unrealized_pnl_pct},
            observed_at=observed_at,
        )

    if (
        high_watermark
        and high_watermark > 0
        and drawdown_from_high_pct >= resolved_settings.dry_run_exit_trailing_stop_pct
    ):
        _append_signal(
            signals,
            position_id=position_id,
            signal_type=DryRunExitSignalType.TRAILING_STOP,
            trigger_price=high_watermark,
            current_price=last_price,
            threshold_value=resolved_settings.dry_run_exit_trailing_stop_pct,
            evidence={"drawdown_from_high_pct": drawdown_from_high_pct},
            observed_at=observed_at,
        )

    if (
        hold_sec is not None
        and hold_sec >= resolved_settings.dry_run_exit_max_hold_sec
        and hold_sec >= resolved_settings.dry_run_exit_min_hold_sec
    ):
        _append_signal(
            signals,
            position_id=position_id,
            signal_type=DryRunExitSignalType.MAX_HOLD,
            current_price=last_price,
            threshold_value=float(resolved_settings.dry_run_exit_max_hold_sec),
            evidence={"hold_sec": hold_sec},
            observed_at=observed_at,
        )

    theme = _latest_theme_for_code(connection, code)
    if theme is not None:
        evidence["theme"] = _row_to_dict(theme)
        if _theme_is_weak(theme, resolved_settings):
            _append_signal(
                signals,
                position_id=position_id,
                signal_type=DryRunExitSignalType.THEME_WEAKENING,
                current_price=last_price,
                evidence={
                    "state": theme["state"],
                    "fresh_coverage_ratio": theme["fresh_coverage_ratio"],
                    "rising_ratio": theme["rising_ratio"],
                },
                observed_at=observed_at,
            )

    risk = _latest_risk_for_code(connection, code, str(position["trade_date"]))
    if risk is not None:
        evidence["risk"] = _row_to_dict(risk)
        if str(risk["overall_status"]).upper() in RISK_DETERIORATED_STATUSES:
            _append_signal(
                signals,
                position_id=position_id,
                signal_type=DryRunExitSignalType.RISK_DETERIORATION,
                current_price=last_price,
                evidence={"overall_status": risk["overall_status"]},
                observed_at=observed_at,
            )

    strategy = _latest_strategy_for_code(connection, code, str(position["trade_date"]))
    if strategy is not None:
        evidence["strategy"] = _row_to_dict(strategy)
        if str(strategy["overall_status"]).upper() in STRATEGY_INVALIDATED_STATUSES:
            _append_signal(
                signals,
                position_id=position_id,
                signal_type=DryRunExitSignalType.STRATEGY_INVALIDATED,
                current_price=last_price,
                evidence={"overall_status": strategy["overall_status"]},
                observed_at=observed_at,
            )

    signal_count = len(
        [signal for signal in signals if signal.signal_type not in CAUTION_SIGNAL_TYPES]
    )
    caution_count = len(signals) - signal_count
    hold_count = 0 if signals else 1
    if signal_count:
        evaluation_status = DryRunExitEvaluationStatus.EXIT_SIGNAL_OBSERVED
    elif caution_count:
        evaluation_status = DryRunExitEvaluationStatus.EXIT_CAUTION_OBSERVED
    else:
        evaluation_status = DryRunExitEvaluationStatus.HOLD_OBSERVED
        reason_codes.append(DryRunExitReasonCode.NO_EXIT_SIGNAL.value)

    for signal in signals:
        reason_codes.extend(signal.reason_codes)
    primary_signal_type = _primary_signal_type(signals)
    evaluation = DryRunExitEvaluation(
        exit_evaluation_id=new_message_id("dry_run_exit_eval"),
        dry_run_position_id=position_id,
        trade_date=str(position["trade_date"]),
        code=code,
        name=str(position["name"]),
        evaluated_at=observed_at,
        status=evaluation_status,
        primary_signal_type=primary_signal_type,
        signal_count=signal_count,
        caution_count=caution_count,
        hold_count=hold_count,
        last_price=last_price,
        avg_price=avg_price,
        quantity=quantity,
        unrealized_pnl=unrealized_pnl,
        unrealized_pnl_pct=unrealized_pnl_pct,
        high_watermark_price=high_watermark,
        drawdown_from_high_pct=drawdown_from_high_pct,
        hold_sec=hold_sec,
        reason_codes=_merge_reasons(reason_codes),
        evidence_json=evidence,
        config_version=resolved_settings.dry_run_exit_config_version,
    )
    _insert_exit_evaluation(connection, evaluation)
    for signal in signals:
        _insert_exit_signal(
            connection,
            DryRunExitSignal(
                exit_signal_id=signal.exit_signal_id,
                exit_evaluation_id=evaluation.exit_evaluation_id,
                dry_run_position_id=position_id,
                signal_type=signal.signal_type,
                status=signal.status,
                severity=signal.severity,
                reason_codes=signal.reason_codes,
                trigger_price=signal.trigger_price,
                current_price=signal.current_price,
                threshold_value=signal.threshold_value,
                evidence_json=signal.evidence_json,
                observed_at=signal.observed_at,
            ),
        )
    connection.commit()
    return evaluation


def evaluate_all_dry_run_exits(
    connection: sqlite3.Connection,
    trade_date: str | None = None,
    limit: int | None = None,
    settings: Settings | None = None,
) -> DryRunExitRunResult:
    resolved_settings = settings or load_settings()
    run_id = new_message_id("dry_run_exit_run")
    started_at = datetime_to_wire(utc_now())
    _insert_run(connection, run_id=run_id, trade_date=trade_date, started_at=started_at)
    targets = _active_position_targets(connection, trade_date=trade_date, limit=limit or 100)
    evaluated_count = 0
    signal_count = 0
    error_count = 0
    for target in targets:
        position_id = str(target["dry_run_position_id"])
        try:
            evaluation = evaluate_dry_run_exit_for_position(
                connection,
                position_id,
                settings=resolved_settings,
            )
            evaluated_count += 1
            signal_count += evaluation.signal_count + evaluation.caution_count
        except Exception as exc:
            _record_error(
                connection,
                run_id=run_id,
                dry_run_position_id=position_id,
                code=target.get("code"),
                error_message=str(exc),
                payload=target,
            )
            error_count += 1
    status = "COMPLETED_WITH_ERRORS" if error_count else "COMPLETED"
    _complete_run(
        connection,
        run_id=run_id,
        evaluated_position_count=evaluated_count,
        exit_signal_count=signal_count,
        status=status,
        error_count=error_count,
    )
    connection.commit()
    return DryRunExitRunResult(
        run_id=run_id,
        trade_date=trade_date,
        evaluated_position_count=evaluated_count,
        exit_signal_count=signal_count,
        error_count=error_count,
        status=status,
    )


def create_dry_run_exit_intent(
    connection: sqlite3.Connection,
    dry_run_position_id: str,
    exit_evaluation_id: str | None = None,
    settings: Settings | None = None,
) -> DryRunExitIntent:
    resolved_settings = settings or load_settings()
    position_id = require_non_empty_str(dry_run_position_id, "dry_run_position_id")
    position = _position_row(connection, position_id)
    if not resolved_settings.dry_run_exit_engine_enabled:
        return _reject_exit_intent(
            connection,
            position=position,
            dry_run_position_id=position_id,
            exit_evaluation_id=exit_evaluation_id,
            reason_codes=[DryRunExitReasonCode.DRY_RUN_EXIT_DISABLED.value],
        )
    if not resolved_settings.dry_run_exit_intent_creation_enabled:
        return _reject_exit_intent(
            connection,
            position=position,
            dry_run_position_id=position_id,
            exit_evaluation_id=exit_evaluation_id,
            reason_codes=[DryRunExitReasonCode.EXIT_INTENT_CREATION_DISABLED.value],
        )
    safety_gate = check_pr11_exit_safety_gate(connection, resolved_settings)
    if resolved_settings.dry_run_exit_require_safety_gate and not safety_gate["passed"]:
        return _reject_exit_intent(
            connection,
            position=position,
            dry_run_position_id=position_id,
            exit_evaluation_id=exit_evaluation_id,
            reason_codes=[DryRunExitReasonCode.SAFETY_GATE_FAILED.value]
            + list(safety_gate["reason_codes"]),
            evidence={"safety_gate": safety_gate},
        )
    if position is None:
        return _reject_exit_intent(
            connection,
            position=None,
            dry_run_position_id=position_id,
            exit_evaluation_id=exit_evaluation_id,
            reason_codes=[DryRunExitReasonCode.POSITION_NOT_FOUND.value],
        )
    if not _position_is_active(position):
        return _reject_exit_intent(
            connection,
            position=position,
            dry_run_position_id=position_id,
            exit_evaluation_id=exit_evaluation_id,
            reason_codes=[DryRunExitReasonCode.POSITION_ALREADY_CLOSED.value],
        )

    evaluation = (
        get_exit_evaluation(connection, exit_evaluation_id, include_signals=True)
        if exit_evaluation_id is not None
        else _latest_exit_evaluation_for_position(connection, position_id)
    )
    if evaluation is None:
        evaluation_model = evaluate_dry_run_exit_for_position(
            connection,
            position_id,
            settings=resolved_settings,
        )
        evaluation = get_exit_evaluation(
            connection,
            evaluation_model.exit_evaluation_id,
            include_signals=True,
        )
    if evaluation is None:
        return _reject_exit_intent(
            connection,
            position=position,
            dry_run_position_id=position_id,
            exit_evaluation_id=exit_evaluation_id,
            reason_codes=[DryRunExitReasonCode.EXIT_EVALUATION_NOT_FOUND.value],
        )
    if evaluation["status"] != DryRunExitEvaluationStatus.EXIT_SIGNAL_OBSERVED.value:
        return _reject_exit_intent(
            connection,
            position=position,
            dry_run_position_id=position_id,
            exit_evaluation_id=evaluation["exit_evaluation_id"],
            reason_codes=[DryRunExitReasonCode.EXIT_SIGNAL_REQUIRED.value],
            evidence={"evaluation_status": evaluation["status"]},
        )

    quantity = int(position["quantity"])
    if quantity <= 0:
        return _reject_exit_intent(
            connection,
            position=position,
            dry_run_position_id=position_id,
            exit_evaluation_id=evaluation["exit_evaluation_id"],
            reason_codes=[DryRunExitReasonCode.POSITION_QUANTITY_INVALID.value],
        )
    intended_price = float(evaluation.get("last_price") or position["last_price"] or 0)
    if intended_price <= 0:
        return _reject_exit_intent(
            connection,
            position=position,
            dry_run_position_id=position_id,
            exit_evaluation_id=evaluation["exit_evaluation_id"],
            reason_codes=[DryRunExitReasonCode.LATEST_TICK_MISSING.value],
        )

    now = utc_now()
    intent = DryRunExitIntent(
        dry_run_exit_intent_id=new_message_id("dry_run_exit_intent"),
        exit_evaluation_id=evaluation["exit_evaluation_id"],
        dry_run_position_id=position_id,
        trade_date=str(position["trade_date"]),
        code=str(position["code"]),
        name=str(position["name"]),
        quantity=quantity,
        intended_price=intended_price,
        notional=intended_price * quantity,
        status=DryRunExitIntentStatus.CREATED,
        reason_codes=[
            DryRunExitReasonCode.DRY_RUN_ONLY.value,
            DryRunExitReasonCode.LIVE_ORDER_FORBIDDEN.value,
            DryRunExitReasonCode.GATEWAY_COMMAND_FORBIDDEN.value,
            DryRunExitReasonCode.BROKER_ORDER_FORBIDDEN.value,
        ],
        evidence_json={
            "evaluation": evaluation,
            "safety_gate": safety_gate,
            "close_only": True,
        },
        created_at=now,
        expires_at=now + timedelta(seconds=resolved_settings.dry_run_exit_intent_ttl_sec),
    )
    _insert_exit_intent(connection, intent)
    _insert_ledger(
        connection,
        trade_date=intent.trade_date,
        event_type="EXIT_INTENT_CREATED",
        related_entity_type="dry_run_exit_intent",
        related_entity_id=intent.dry_run_exit_intent_id,
        code=intent.code,
        amount=intent.notional,
        quantity=intent.quantity,
        payload=intent.to_dict(),
    )
    connection.commit()
    return intent


def convert_exit_intent_to_dry_run_order(
    connection: sqlite3.Connection,
    dry_run_exit_intent_id: str,
    settings: Settings | None = None,
) -> DryRunExitOrder:
    resolved_settings = settings or load_settings()
    if not resolved_settings.dry_run_exit_order_creation_enabled:
        raise ValueError(DryRunExitReasonCode.EXIT_ORDER_CREATION_DISABLED.value)
    safety_gate = check_pr11_exit_safety_gate(connection, resolved_settings)
    if resolved_settings.dry_run_exit_require_safety_gate and not safety_gate["passed"]:
        raise ValueError(DryRunExitReasonCode.SAFETY_GATE_FAILED.value)
    intent = get_exit_intent(connection, dry_run_exit_intent_id)
    if intent is None:
        raise ValueError(DryRunExitReasonCode.EXIT_INTENT_NOT_FOUND.value)
    if intent["status"] != DryRunExitIntentStatus.CREATED.value:
        raise ValueError(DryRunExitReasonCode.INVALID_EXIT_INTENT_STATUS.value)
    position = _position_row(connection, intent["dry_run_position_id"])
    if position is None:
        raise ValueError(DryRunExitReasonCode.POSITION_NOT_FOUND.value)
    if not _position_is_active(position):
        raise ValueError(DryRunExitReasonCode.POSITION_ALREADY_CLOSED.value)

    now = utc_now()
    order = DryRunExitOrder(
        dry_run_exit_order_id=new_message_id("dry_run_exit_order"),
        dry_run_exit_intent_id=intent["dry_run_exit_intent_id"],
        dry_run_position_id=intent["dry_run_position_id"],
        trade_date=intent["trade_date"],
        code=intent["code"],
        name=intent["name"],
        quantity=int(intent["quantity"]),
        requested_price=float(intent["intended_price"]),
        filled_quantity=0,
        remaining_quantity=int(intent["quantity"]),
        status=DryRunExitOrderStatus.CREATED,
        created_at=now,
        expires_at=intent.get("expires_at"),
        reason_codes=[
            DryRunExitReasonCode.DRY_RUN_ONLY.value,
            DryRunExitReasonCode.LIVE_ORDER_FORBIDDEN.value,
            DryRunExitReasonCode.GATEWAY_COMMAND_FORBIDDEN.value,
            DryRunExitReasonCode.BROKER_ORDER_FORBIDDEN.value,
        ],
        evidence_json={"intent": intent, "safety_gate": safety_gate},
    )
    _insert_exit_order(connection, order)
    connection.execute(
        """
        UPDATE dry_run_exit_intents
        SET status = ?
        WHERE dry_run_exit_intent_id = ?
        """,
        (DryRunExitIntentStatus.CONVERTED_TO_EXIT_ORDER.value, dry_run_exit_intent_id),
    )
    _insert_ledger(
        connection,
        trade_date=order.trade_date,
        event_type="EXIT_ORDER_CREATED",
        related_entity_type="dry_run_exit_order",
        related_entity_id=order.dry_run_exit_order_id,
        code=order.code,
        amount=order.requested_price * order.quantity,
        quantity=order.quantity,
        payload=order.to_dict(),
    )
    connection.commit()
    return order


def simulate_fill_dry_run_exit_order(
    connection: sqlite3.Connection,
    dry_run_exit_order_id: str,
    settings: Settings | None = None,
) -> DryRunExitExecution:
    resolved_settings = settings or load_settings()
    if not resolved_settings.dry_run_exit_simulated_fill_enabled:
        raise ValueError(DryRunExitReasonCode.SIMULATED_EXIT_FILL_DISABLED.value)
    safety_gate = check_pr11_exit_safety_gate(connection, resolved_settings)
    if resolved_settings.dry_run_exit_require_safety_gate and not safety_gate["passed"]:
        raise ValueError(DryRunExitReasonCode.SAFETY_GATE_FAILED.value)
    order = get_exit_order(connection, dry_run_exit_order_id)
    if order is None:
        raise ValueError(DryRunExitReasonCode.EXIT_ORDER_NOT_FOUND.value)
    if order["status"] not in ACTIVE_EXIT_ORDER_STATUSES:
        raise ValueError(DryRunExitReasonCode.INVALID_EXIT_ORDER_STATUS.value)
    position = _position_row(connection, order["dry_run_position_id"])
    if position is None:
        raise ValueError(DryRunExitReasonCode.POSITION_NOT_FOUND.value)
    if not _position_is_active(position):
        raise ValueError(DryRunExitReasonCode.POSITION_ALREADY_CLOSED.value)
    tick = _latest_tick_row(connection, order["code"])
    if tick is None:
        raise ValueError(DryRunExitReasonCode.LATEST_TICK_MISSING.value)

    position_quantity = int(position["quantity"])
    requested_quantity = int(order["remaining_quantity"] or order["quantity"])
    quantity = min(requested_quantity, position_quantity)
    if quantity <= 0:
        raise ValueError(DryRunExitReasonCode.POSITION_QUANTITY_INVALID.value)
    fill_price = float(tick["price"] or order["requested_price"])
    notional = fill_price * quantity
    commission = notional * resolved_settings.dry_run_commission_rate
    tax = notional * resolved_settings.dry_run_tax_rate
    avg_price = float(position["avg_price"])
    realized_pnl = (fill_price - avg_price) * quantity - commission - tax
    executed_at = utc_now()
    execution = DryRunExitExecution(
        dry_run_exit_execution_id=new_message_id("dry_run_exit_execution"),
        dry_run_exit_order_id=order["dry_run_exit_order_id"],
        dry_run_exit_intent_id=order["dry_run_exit_intent_id"],
        dry_run_position_id=order["dry_run_position_id"],
        trade_date=order["trade_date"],
        code=order["code"],
        quantity=quantity,
        price=fill_price,
        notional=notional,
        realized_pnl=realized_pnl,
        commission=commission,
        tax=tax,
        executed_at=executed_at,
    )
    _insert_exit_execution(connection, execution)
    executed_at_wire = datetime_to_wire(executed_at)
    connection.execute(
        """
        UPDATE dry_run_exit_orders
        SET status = ?,
            simulated_fill_price = ?,
            filled_quantity = ?,
            remaining_quantity = 0,
            simulated_submitted_at = COALESCE(simulated_submitted_at, ?),
            simulated_filled_at = ?
        WHERE dry_run_exit_order_id = ?
        """,
        (
            DryRunExitOrderStatus.SIMULATED_FILLED.value,
            fill_price,
            quantity,
            executed_at_wire,
            executed_at_wire,
            order["dry_run_exit_order_id"],
        ),
    )
    position_event = _reduce_or_close_position(
        connection,
        position,
        quantity=quantity,
        fill_price=fill_price,
        realized_pnl=realized_pnl,
        executed_at=executed_at_wire,
    )
    _insert_ledger(
        connection,
        trade_date=execution.trade_date,
        event_type="SIMULATED_EXIT_FILL",
        related_entity_type="dry_run_exit_execution",
        related_entity_id=execution.dry_run_exit_execution_id,
        code=execution.code,
        amount=execution.notional,
        quantity=execution.quantity,
        payload=execution.to_dict() | {"safety_gate": safety_gate},
    )
    _insert_ledger(
        connection,
        trade_date=execution.trade_date,
        event_type=position_event["event_type"],
        related_entity_type="dry_run_position",
        related_entity_id=position_event["dry_run_position_id"],
        code=execution.code,
        amount=execution.notional,
        quantity=execution.quantity,
        payload=position_event,
    )
    connection.commit()
    return execution


def update_position_high_low_watermark(
    connection: sqlite3.Connection,
    dry_run_position_id: str,
    latest_price: float,
) -> dict[str, Any]:
    position_id = require_non_empty_str(dry_run_position_id, "dry_run_position_id")
    position = _position_row(connection, position_id)
    if position is None:
        raise ValueError(DryRunExitReasonCode.POSITION_NOT_FOUND.value)
    price = float(latest_price)
    current_pnl = (price - float(position["avg_price"])) * int(position["quantity"])
    existing = _position_metrics_row(connection, position_id)
    previous_high = _optional_float(existing["high_watermark_price"]) if existing else None
    previous_low = _optional_float(existing["low_watermark_price"]) if existing else None
    position_last = _optional_float(position["last_price"])
    high_watermark = max(
        value for value in (previous_high, position_last, price) if value is not None
    )
    low_watermark = min(
        value for value in (previous_low, position_last, price) if value is not None
    )
    previous_max_pnl = _optional_float(existing["max_unrealized_pnl"]) if existing else None
    previous_min_pnl = _optional_float(existing["min_unrealized_pnl"]) if existing else None
    max_pnl = max(previous_max_pnl or 0.0, current_pnl)
    min_pnl = min(previous_min_pnl or 0.0, current_pnl)
    now = datetime_to_wire(utc_now())
    metadata = _json_object(existing["metadata_json"]) if existing else {}
    connection.execute(
        """
        INSERT INTO dry_run_position_metrics (
            dry_run_position_id,
            trade_date,
            code,
            high_watermark_price,
            low_watermark_price,
            max_unrealized_pnl,
            min_unrealized_pnl,
            last_evaluated_at,
            metadata_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(dry_run_position_id) DO UPDATE SET
            trade_date = excluded.trade_date,
            code = excluded.code,
            high_watermark_price = excluded.high_watermark_price,
            low_watermark_price = excluded.low_watermark_price,
            max_unrealized_pnl = excluded.max_unrealized_pnl,
            min_unrealized_pnl = excluded.min_unrealized_pnl,
            last_evaluated_at = excluded.last_evaluated_at,
            metadata_json = excluded.metadata_json
        """,
        (
            position_id,
            position["trade_date"],
            position["code"],
            high_watermark,
            low_watermark,
            max_pnl,
            min_pnl,
            now,
            _json_dumps(metadata | {"latest_price": price}),
        ),
    )
    return {
        "dry_run_position_id": position_id,
        "trade_date": position["trade_date"],
        "code": position["code"],
        "high_watermark_price": high_watermark,
        "low_watermark_price": low_watermark,
        "max_unrealized_pnl": max_pnl,
        "min_unrealized_pnl": min_pnl,
        "last_evaluated_at": now,
        "metadata_json": metadata | {"latest_price": price},
    }


def check_pr11_exit_safety_gate(
    connection: sqlite3.Connection,
    settings: Settings | None = None,
) -> dict[str, Any]:
    resolved_settings = settings or load_settings()
    pr10 = check_pr10_safety_gate(connection, resolved_settings)
    reason_codes = list(pr10.reason_codes)
    live_flags_disabled = (
        resolved_settings.trading_mode is TradingMode.OBSERVE
        and not resolved_settings.live_sim_allowed
        and not resolved_settings.live_real_allowed
    )
    exit_routing_disabled = not resolved_settings.dry_run_exit_order_routing_enabled
    exit_gateway_disabled = not resolved_settings.dry_run_exit_gateway_command_enabled
    short_disabled = not resolved_settings.dry_run_exit_allow_short
    sell_close_only = resolved_settings.dry_run_exit_allow_sell_close_only
    if not live_flags_disabled:
        reason_codes.append(DryRunExitReasonCode.LIVE_ORDER_FORBIDDEN.value)
    if not exit_routing_disabled:
        reason_codes.append(DryRunExitReasonCode.LIVE_ORDER_FORBIDDEN.value)
    if not exit_gateway_disabled:
        reason_codes.append(DryRunExitReasonCode.GATEWAY_COMMAND_FORBIDDEN.value)
    if not short_disabled:
        reason_codes.append(DryRunExitReasonCode.SHORT_SELL_FORBIDDEN.value)
    if not sell_close_only:
        reason_codes.append(DryRunExitReasonCode.CLOSE_ONLY_REQUIRED.value)
    reason_codes = _merge_reasons(reason_codes)
    passed = pr10.passed and not reason_codes
    return {
        "passed": passed,
        "status": "PASSED" if passed else "BLOCKED",
        "reason_codes": reason_codes,
        "pr10_safety_gate": pr10.to_dict(),
        "live_flags_disabled": live_flags_disabled,
        "exit_order_routing_disabled": exit_routing_disabled,
        "exit_gateway_command_disabled": exit_gateway_disabled,
        "sell_close_only": sell_close_only,
        "short_disabled": short_disabled,
        "dry_run_only": True,
        "close_only": True,
        "live_order_allowed": False,
        "gateway_command_allowed": False,
        "broker_order_sent": False,
    }


def get_exit_status(
    connection: sqlite3.Connection,
    settings: Settings | None = None,
) -> dict[str, Any]:
    resolved_settings = settings or load_settings()
    safety_gate = check_pr11_exit_safety_gate(connection, resolved_settings)
    return {
        "enabled": resolved_settings.dry_run_exit_engine_enabled,
        "intent_creation_enabled": resolved_settings.dry_run_exit_intent_creation_enabled,
        "order_creation_enabled": resolved_settings.dry_run_exit_order_creation_enabled,
        "simulated_fill_enabled": resolved_settings.dry_run_exit_simulated_fill_enabled,
        "require_safety_gate": resolved_settings.dry_run_exit_require_safety_gate,
        "order_routing_enabled": False,
        "gateway_command_enabled": False,
        "gateway_command_allowed": False,
        "live_order_allowed": False,
        "broker_order_sent": False,
        "sell_close_only": True,
        "short_allowed": False,
        "active_position_count": _active_position_count(connection),
        "evaluation_count": _count_rows(connection, "dry_run_exit_evaluations"),
        "signal_count": _count_rows(connection, "dry_run_exit_signals"),
        "exit_intent_count": _count_rows(connection, "dry_run_exit_intents"),
        "exit_order_count": _count_rows(connection, "dry_run_exit_orders"),
        "exit_execution_count": _count_rows(connection, "dry_run_exit_executions"),
        "run_count": _count_rows(connection, "dry_run_exit_runs"),
        "error_count": _count_rows(connection, "dry_run_exit_errors"),
        "config_version": resolved_settings.dry_run_exit_config_version,
        "safety_gate": safety_gate,
        "dry_run_only": True,
        "close_only": True,
    }


def list_exit_evaluations(
    connection: sqlite3.Connection,
    *,
    trade_date: str | None = None,
    status: DryRunExitEvaluationStatus | str | None = None,
    code: str | None = None,
    dry_run_position_id: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    clauses, params = _common_filters(trade_date=trade_date, status=status, code=code)
    if dry_run_position_id is not None:
        clauses.append("dry_run_position_id = ?")
        params.append(require_non_empty_str(dry_run_position_id, "dry_run_position_id"))
    return _list_rows(
        connection,
        "dry_run_exit_evaluations",
        clauses=clauses,
        params=params,
        order_by="evaluated_at DESC, exit_evaluation_id DESC",
        limit=limit,
        mapper=_evaluation_row_to_dict,
    )


def get_exit_evaluation(
    connection: sqlite3.Connection,
    exit_evaluation_id: str,
    *,
    include_signals: bool = True,
) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT *
        FROM dry_run_exit_evaluations
        WHERE exit_evaluation_id = ?
        """,
        (require_non_empty_str(exit_evaluation_id, "exit_evaluation_id"),),
    ).fetchone()
    if row is None:
        return None
    item = _evaluation_row_to_dict(row)
    if include_signals:
        item["signals"] = list_exit_signals(
            connection,
            exit_evaluation_id=item["exit_evaluation_id"],
            limit=100,
        )
    return item


def list_exit_signals(
    connection: sqlite3.Connection,
    *,
    exit_evaluation_id: str | None = None,
    dry_run_position_id: str | None = None,
    signal_type: DryRunExitSignalType | str | None = None,
    status: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if exit_evaluation_id is not None:
        clauses.append("exit_evaluation_id = ?")
        params.append(require_non_empty_str(exit_evaluation_id, "exit_evaluation_id"))
    if dry_run_position_id is not None:
        clauses.append("dry_run_position_id = ?")
        params.append(require_non_empty_str(dry_run_position_id, "dry_run_position_id"))
    if signal_type is not None:
        clauses.append("signal_type = ?")
        params.append(str(getattr(signal_type, "value", signal_type)).upper())
    if status is not None:
        clauses.append("status = ?")
        params.append(require_non_empty_str(status, "status").upper())
    return _list_rows(
        connection,
        "dry_run_exit_signals",
        clauses=clauses,
        params=params,
        order_by="observed_at DESC, exit_signal_id DESC",
        limit=limit,
        mapper=_signal_row_to_dict,
    )


def list_exit_intents(
    connection: sqlite3.Connection,
    *,
    trade_date: str | None = None,
    status: DryRunExitIntentStatus | str | None = None,
    code: str | None = None,
    dry_run_position_id: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    clauses, params = _common_filters(trade_date=trade_date, status=status, code=code)
    if dry_run_position_id is not None:
        clauses.append("dry_run_position_id = ?")
        params.append(require_non_empty_str(dry_run_position_id, "dry_run_position_id"))
    return _list_rows(
        connection,
        "dry_run_exit_intents",
        clauses=clauses,
        params=params,
        order_by="created_at DESC, dry_run_exit_intent_id DESC",
        limit=limit,
        mapper=_intent_row_to_dict,
    )


def get_exit_intent(
    connection: sqlite3.Connection,
    dry_run_exit_intent_id: str,
) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT *
        FROM dry_run_exit_intents
        WHERE dry_run_exit_intent_id = ?
        """,
        (require_non_empty_str(dry_run_exit_intent_id, "dry_run_exit_intent_id"),),
    ).fetchone()
    return None if row is None else _intent_row_to_dict(row)


def list_exit_orders(
    connection: sqlite3.Connection,
    *,
    trade_date: str | None = None,
    status: DryRunExitOrderStatus | str | None = None,
    code: str | None = None,
    dry_run_position_id: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    clauses, params = _common_filters(trade_date=trade_date, status=status, code=code)
    if dry_run_position_id is not None:
        clauses.append("dry_run_position_id = ?")
        params.append(require_non_empty_str(dry_run_position_id, "dry_run_position_id"))
    return _list_rows(
        connection,
        "dry_run_exit_orders",
        clauses=clauses,
        params=params,
        order_by="created_at DESC, dry_run_exit_order_id DESC",
        limit=limit,
        mapper=_order_row_to_dict,
    )


def get_exit_order(
    connection: sqlite3.Connection,
    dry_run_exit_order_id: str,
) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT *
        FROM dry_run_exit_orders
        WHERE dry_run_exit_order_id = ?
        """,
        (require_non_empty_str(dry_run_exit_order_id, "dry_run_exit_order_id"),),
    ).fetchone()
    return None if row is None else _order_row_to_dict(row)


def list_exit_executions(
    connection: sqlite3.Connection,
    *,
    trade_date: str | None = None,
    code: str | None = None,
    dry_run_position_id: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    clauses, params = _common_filters(trade_date=trade_date, code=code)
    if dry_run_position_id is not None:
        clauses.append("dry_run_position_id = ?")
        params.append(require_non_empty_str(dry_run_position_id, "dry_run_position_id"))
    return _list_rows(
        connection,
        "dry_run_exit_executions",
        clauses=clauses,
        params=params,
        order_by="executed_at DESC, dry_run_exit_execution_id DESC",
        limit=limit,
        mapper=_execution_row_to_dict,
    )


def list_exit_runs(
    connection: sqlite3.Connection,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    return _list_rows(
        connection,
        "dry_run_exit_runs",
        clauses=[],
        params=[],
        order_by="started_at DESC, run_id DESC",
        limit=limit,
        mapper=_row_to_dict,
    )


def list_exit_errors(
    connection: sqlite3.Connection,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    return _list_rows(
        connection,
        "dry_run_exit_errors",
        clauses=[],
        params=[],
        order_by="created_at DESC, id DESC",
        limit=limit,
        mapper=_error_row_to_dict,
    )


def _append_signal(
    signals: list[DryRunExitSignal],
    *,
    position_id: str,
    signal_type: DryRunExitSignalType,
    observed_at: str,
    current_price: float,
    trigger_price: float | None = None,
    threshold_value: float | None = None,
    evidence: Mapping[str, Any] | None = None,
) -> None:
    reason = SIGNAL_REASON_BY_TYPE[signal_type].value
    severity = "LOW" if signal_type in CAUTION_SIGNAL_TYPES else _signal_severity(signal_type)
    signals.append(
        DryRunExitSignal(
            exit_signal_id=new_message_id("dry_run_exit_signal"),
            exit_evaluation_id="PENDING",
            dry_run_position_id=position_id,
            signal_type=signal_type,
            status="OBSERVED",
            severity=severity,
            reason_codes=[reason],
            trigger_price=trigger_price,
            current_price=current_price,
            threshold_value=threshold_value,
            evidence_json=evidence or {},
            observed_at=observed_at,
        )
    )


def _signal_severity(signal_type: DryRunExitSignalType) -> str:
    if signal_type in {
        DryRunExitSignalType.STOP_LOSS,
        DryRunExitSignalType.TRAILING_STOP,
        DryRunExitSignalType.RISK_DETERIORATION,
    }:
        return "HIGH"
    return "MEDIUM"


def _primary_signal_type(signals: list[DryRunExitSignal]) -> DryRunExitSignalType | None:
    for signal in signals:
        if signal.signal_type not in CAUTION_SIGNAL_TYPES:
            return signal.signal_type
    return signals[0].signal_type if signals else None


def _reject_exit_intent(
    connection: sqlite3.Connection,
    *,
    position: sqlite3.Row | None,
    dry_run_position_id: str,
    exit_evaluation_id: str | None,
    reason_codes: list[str],
    evidence: Mapping[str, Any] | None = None,
) -> DryRunExitIntent:
    now = utc_now()
    position_id = require_non_empty_str(dry_run_position_id, "dry_run_position_id")
    trade_date = str(position["trade_date"]) if position is not None else "UNKNOWN"
    code = str(position["code"]) if position is not None else "000000"
    name = str(position["name"]) if position is not None else "UNKNOWN"
    quantity = int(position["quantity"]) if position is not None else 0
    price = _optional_float(position["last_price"]) if position is not None else 0.0
    intent = DryRunExitIntent(
        dry_run_exit_intent_id=new_message_id("dry_run_exit_intent_rejected"),
        exit_evaluation_id=exit_evaluation_id or "UNKNOWN",
        dry_run_position_id=position_id,
        trade_date=trade_date,
        code=code,
        name=name,
        quantity=max(quantity, 0),
        intended_price=price or 0.0,
        notional=(price or 0.0) * max(quantity, 0),
        status=DryRunExitIntentStatus.REJECTED,
        reason_codes=_merge_reasons(reason_codes),
        evidence_json=_base_evidence() | dict(evidence or {}),
        created_at=now,
    )
    _insert_exit_intent(connection, intent)
    _record_error(
        connection,
        run_id=None,
        dry_run_position_id=position_id,
        code=code if code != "000000" else None,
        error_message=";".join(intent.reason_codes),
        payload=intent.to_dict(),
    )
    connection.commit()
    return intent


def _insert_exit_evaluation(
    connection: sqlite3.Connection,
    evaluation: DryRunExitEvaluation,
) -> None:
    data = evaluation.to_dict()
    connection.execute(
        """
        INSERT INTO dry_run_exit_evaluations (
            exit_evaluation_id,
            dry_run_position_id,
            trade_date,
            code,
            name,
            evaluated_at,
            status,
            primary_signal_type,
            signal_count,
            caution_count,
            hold_count,
            last_price,
            avg_price,
            quantity,
            unrealized_pnl,
            unrealized_pnl_pct,
            high_watermark_price,
            drawdown_from_high_pct,
            hold_sec,
            reason_codes_json,
            evidence_json,
            config_version,
            dry_run_only,
            broker_order_allowed,
            gateway_command_allowed
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 0, 0)
        """,
        (
            data["exit_evaluation_id"],
            data["dry_run_position_id"],
            data["trade_date"],
            data["code"],
            data["name"],
            data["evaluated_at"],
            data["status"],
            data["primary_signal_type"],
            data["signal_count"],
            data["caution_count"],
            data["hold_count"],
            data["last_price"],
            data["avg_price"],
            data["quantity"],
            data["unrealized_pnl"],
            data["unrealized_pnl_pct"],
            data["high_watermark_price"],
            data["drawdown_from_high_pct"],
            data["hold_sec"],
            _json_dumps(data["reason_codes"]),
            _json_dumps(data["evidence_json"]),
            data["config_version"],
        ),
    )
    _insert_ledger(
        connection,
        trade_date=data["trade_date"],
        event_type="EXIT_EVALUATION",
        related_entity_type="dry_run_exit_evaluation",
        related_entity_id=data["exit_evaluation_id"],
        code=data["code"],
        amount=data["unrealized_pnl"],
        quantity=data["quantity"],
        payload=data,
    )


def _insert_exit_signal(connection: sqlite3.Connection, signal: DryRunExitSignal) -> None:
    data = signal.to_dict()
    connection.execute(
        """
        INSERT INTO dry_run_exit_signals (
            exit_signal_id,
            exit_evaluation_id,
            dry_run_position_id,
            signal_type,
            status,
            severity,
            reason_codes_json,
            trigger_price,
            current_price,
            threshold_value,
            evidence_json,
            observed_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            data["exit_signal_id"],
            data["exit_evaluation_id"],
            data["dry_run_position_id"],
            data["signal_type"],
            data["status"],
            data["severity"],
            _json_dumps(data["reason_codes"]),
            data["trigger_price"],
            data["current_price"],
            data["threshold_value"],
            _json_dumps(data["evidence_json"]),
            data["observed_at"],
        ),
    )


def _insert_exit_intent(connection: sqlite3.Connection, intent: DryRunExitIntent) -> None:
    data = intent.to_dict()
    connection.execute(
        """
        INSERT INTO dry_run_exit_intents (
            dry_run_exit_intent_id,
            exit_evaluation_id,
            dry_run_position_id,
            trade_date,
            code,
            name,
            side,
            quantity,
            intended_price,
            notional,
            status,
            reason_codes_json,
            evidence_json,
            created_at,
            expires_at,
            dry_run_only,
            close_only,
            live_order_allowed,
            gateway_command_allowed,
            broker_order_sent
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 1, 0, 0, 0)
        """,
        (
            data["dry_run_exit_intent_id"],
            data["exit_evaluation_id"],
            data["dry_run_position_id"],
            data["trade_date"],
            data["code"],
            data["name"],
            data["side"],
            data["quantity"],
            data["intended_price"],
            data["notional"],
            data["status"],
            _json_dumps(data["reason_codes"]),
            _json_dumps(data["evidence_json"]),
            data["created_at"],
            data["expires_at"],
        ),
    )


def _insert_exit_order(connection: sqlite3.Connection, order: DryRunExitOrder) -> None:
    data = order.to_dict()
    connection.execute(
        """
        INSERT INTO dry_run_exit_orders (
            dry_run_exit_order_id,
            dry_run_exit_intent_id,
            dry_run_position_id,
            trade_date,
            code,
            name,
            side,
            quantity,
            requested_price,
            simulated_fill_price,
            filled_quantity,
            remaining_quantity,
            status,
            reason_codes_json,
            evidence_json,
            dry_run_only,
            close_only,
            broker_order_sent,
            created_at,
            simulated_submitted_at,
            simulated_filled_at,
            expires_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 1, 0, ?, ?, ?, ?)
        """,
        (
            data["dry_run_exit_order_id"],
            data["dry_run_exit_intent_id"],
            data["dry_run_position_id"],
            data["trade_date"],
            data["code"],
            data["name"],
            data["side"],
            data["quantity"],
            data["requested_price"],
            data["simulated_fill_price"],
            data["filled_quantity"],
            data["remaining_quantity"],
            data["status"],
            _json_dumps(data["reason_codes"]),
            _json_dumps(data["evidence_json"]),
            data["created_at"],
            data["simulated_submitted_at"],
            data["simulated_filled_at"],
            data["expires_at"],
        ),
    )


def _insert_exit_execution(
    connection: sqlite3.Connection,
    execution: DryRunExitExecution,
) -> None:
    data = execution.to_dict()
    connection.execute(
        """
        INSERT INTO dry_run_exit_executions (
            dry_run_exit_execution_id,
            dry_run_exit_order_id,
            dry_run_exit_intent_id,
            dry_run_position_id,
            trade_date,
            code,
            side,
            quantity,
            price,
            notional,
            realized_pnl,
            commission,
            tax,
            executed_at,
            execution_type,
            dry_run_only,
            broker_order_sent
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 0)
        """,
        (
            data["dry_run_exit_execution_id"],
            data["dry_run_exit_order_id"],
            data["dry_run_exit_intent_id"],
            data["dry_run_position_id"],
            data["trade_date"],
            data["code"],
            data["side"],
            data["quantity"],
            data["price"],
            data["notional"],
            data["realized_pnl"],
            data["commission"],
            data["tax"],
            data["executed_at"],
            data["execution_type"],
        ),
    )


def _reduce_or_close_position(
    connection: sqlite3.Connection,
    position: sqlite3.Row,
    *,
    quantity: int,
    fill_price: float,
    realized_pnl: float,
    executed_at: str,
) -> dict[str, Any]:
    old_quantity = int(position["quantity"])
    avg_price = float(position["avg_price"])
    new_quantity = max(old_quantity - quantity, 0)
    invested_notional = avg_price * new_quantity
    unrealized_pnl = (fill_price - avg_price) * new_quantity
    cumulative_realized = float(position["realized_pnl"] or 0) + realized_pnl
    status = "CLOSED" if new_quantity == 0 else "OPEN"
    closed_at = executed_at if new_quantity == 0 else position["closed_at"]
    connection.execute(
        """
        UPDATE dry_run_positions
        SET quantity = ?,
            invested_notional = ?,
            realized_pnl = ?,
            unrealized_pnl = ?,
            last_price = ?,
            status = ?,
            updated_at = ?,
            closed_at = ?
        WHERE dry_run_position_id = ?
        """,
        (
            new_quantity,
            invested_notional,
            cumulative_realized,
            unrealized_pnl,
            fill_price,
            status,
            executed_at,
            closed_at,
            position["dry_run_position_id"],
        ),
    )
    return {
        "event_type": "POSITION_CLOSED" if new_quantity == 0 else "POSITION_REDUCED",
        "dry_run_position_id": position["dry_run_position_id"],
        "trade_date": position["trade_date"],
        "code": position["code"],
        "name": position["name"],
        "closed_quantity": quantity,
        "remaining_quantity": new_quantity,
        "avg_price": avg_price,
        "fill_price": fill_price,
        "realized_pnl": realized_pnl,
        "cumulative_realized_pnl": cumulative_realized,
        "unrealized_pnl": unrealized_pnl,
        "status": status,
        "updated_at": executed_at,
        "closed_at": closed_at,
        "dry_run_only": True,
        "close_only": True,
        "broker_order_sent": False,
    }


def _insert_ledger(
    connection: sqlite3.Connection,
    *,
    trade_date: str,
    event_type: str,
    related_entity_type: str,
    related_entity_id: str,
    code: str | None,
    amount: float,
    quantity: int,
    payload: Mapping[str, Any],
) -> None:
    connection.execute(
        """
        INSERT INTO dry_run_ledger (
            ledger_id,
            trade_date,
            event_type,
            related_entity_type,
            related_entity_id,
            code,
            amount,
            quantity,
            payload_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            new_message_id("dry_run_ledger"),
            trade_date,
            event_type.upper(),
            related_entity_type,
            related_entity_id,
            validate_stock_code(code) if code is not None and code != "000000" else code,
            amount,
            quantity,
            _json_dumps(payload),
        ),
    )


def _position_row(
    connection: sqlite3.Connection,
    dry_run_position_id: str,
) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT *
        FROM dry_run_positions
        WHERE dry_run_position_id = ?
        """,
        (require_non_empty_str(dry_run_position_id, "dry_run_position_id"),),
    ).fetchone()


def _position_metrics_row(
    connection: sqlite3.Connection,
    dry_run_position_id: str,
) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT *
        FROM dry_run_position_metrics
        WHERE dry_run_position_id = ?
        """,
        (require_non_empty_str(dry_run_position_id, "dry_run_position_id"),),
    ).fetchone()


def _latest_tick_row(connection: sqlite3.Connection, code: str | None) -> sqlite3.Row | None:
    if code is None:
        return None
    return connection.execute(
        """
        SELECT *
        FROM market_ticks_latest
        WHERE code = ? AND exchange = 'KRX'
        """,
        (validate_stock_code(code),),
    ).fetchone()


def _latest_theme_for_code(connection: sqlite3.Connection, code: str) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT
            s.*,
            m.member_role,
            m.readiness_status,
            m.tick_age_sec AS member_tick_age_sec
        FROM theme_snapshot_members AS m
        JOIN theme_latest_snapshots AS s
            ON s.snapshot_id = m.snapshot_id
            AND s.theme_id = m.theme_id
        WHERE m.code = ?
        ORDER BY s.calculated_at DESC, s.snapshot_id DESC
        LIMIT 1
        """,
        (validate_stock_code(code),),
    ).fetchone()


def _latest_risk_for_code(
    connection: sqlite3.Connection,
    code: str,
    trade_date: str,
) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT *
        FROM risk_observations_latest
        WHERE code = ?
            AND trade_date = ?
        ORDER BY evaluated_at DESC, risk_observation_id DESC
        LIMIT 1
        """,
        (validate_stock_code(code), require_non_empty_str(trade_date, "trade_date")),
    ).fetchone()


def _latest_strategy_for_code(
    connection: sqlite3.Connection,
    code: str,
    trade_date: str,
) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT *
        FROM strategy_observations_latest
        WHERE code = ?
            AND trade_date = ?
        ORDER BY evaluated_at DESC, strategy_observation_id DESC
        LIMIT 1
        """,
        (validate_stock_code(code), require_non_empty_str(trade_date, "trade_date")),
    ).fetchone()


def _active_position_targets(
    connection: sqlite3.Connection,
    *,
    trade_date: str | None,
    limit: int,
) -> list[dict[str, Any]]:
    clauses = ["status IN ('OPEN')", "quantity > 0"]
    params: list[Any] = []
    if trade_date is not None:
        clauses.append("trade_date = ?")
        params.append(require_non_empty_str(trade_date, "trade_date"))
    rows = connection.execute(
        f"""
        SELECT dry_run_position_id, trade_date, code
        FROM dry_run_positions
        WHERE {" AND ".join(clauses)}
        ORDER BY updated_at DESC, dry_run_position_id DESC
        LIMIT ?
        """,
        (*params, _bounded_limit(limit)),
    ).fetchall()
    return [_row_to_dict(row) for row in rows]


def _latest_exit_evaluation_for_position(
    connection: sqlite3.Connection,
    dry_run_position_id: str,
) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT *
        FROM dry_run_exit_evaluations
        WHERE dry_run_position_id = ?
        ORDER BY evaluated_at DESC, exit_evaluation_id DESC
        LIMIT 1
        """,
        (require_non_empty_str(dry_run_position_id, "dry_run_position_id"),),
    ).fetchone()
    return None if row is None else _evaluation_row_to_dict(row)


def _theme_is_weak(row: sqlite3.Row, settings: Settings) -> bool:
    state = str(row["state"]).upper()
    fresh_ratio = float(row["fresh_coverage_ratio"])
    rising_ratio = float(row["rising_ratio"])
    return (
        state in THEME_WEAK_STATES
        or fresh_ratio < settings.theme_min_fresh_coverage_ratio
        or rising_ratio < settings.theme_spreading_rising_ratio
    )


def _position_is_active(position: sqlite3.Row) -> bool:
    return (
        str(position["status"]).upper() in ACTIVE_POSITION_STATUSES
        and int(position["quantity"]) > 0
    )


def _hold_seconds(opened_at: object, *, now) -> float | None:
    if opened_at is None:
        return None
    opened = parse_timestamp(opened_at, "opened_at")
    return max((parse_timestamp(now, "now") - opened).total_seconds(), 0.0)


def _insert_run(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    trade_date: str | None,
    started_at: str,
) -> None:
    connection.execute(
        """
        INSERT INTO dry_run_exit_runs (run_id, trade_date, started_at, status)
        VALUES (?, ?, ?, 'RUNNING')
        """,
        (run_id, trade_date, started_at),
    )


def _complete_run(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    evaluated_position_count: int,
    exit_signal_count: int,
    status: str,
    exit_intent_count: int = 0,
    exit_order_count: int = 0,
    exit_execution_count: int = 0,
    rejection_count: int = 0,
    error_count: int = 0,
    error_message: str | None = None,
) -> None:
    connection.execute(
        """
        UPDATE dry_run_exit_runs
        SET completed_at = ?,
            evaluated_position_count = ?,
            exit_signal_count = ?,
            exit_intent_count = ?,
            exit_order_count = ?,
            exit_execution_count = ?,
            rejection_count = ?,
            error_count = ?,
            status = ?,
            error_message = ?
        WHERE run_id = ?
        """,
        (
            datetime_to_wire(utc_now()),
            evaluated_position_count,
            exit_signal_count,
            exit_intent_count,
            exit_order_count,
            exit_execution_count,
            rejection_count,
            error_count,
            status,
            error_message,
            run_id,
        ),
    )


def _record_error(
    connection: sqlite3.Connection,
    *,
    run_id: str | None,
    dry_run_position_id: str | None,
    code: str | None,
    error_message: str,
    payload: Mapping[str, Any],
) -> None:
    connection.execute(
        """
        INSERT INTO dry_run_exit_errors (
            run_id,
            dry_run_position_id,
            code,
            error_message,
            payload_json
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            run_id,
            dry_run_position_id,
            validate_stock_code(code) if code is not None else None,
            error_message,
            _json_dumps(payload),
        ),
    )


def _list_rows(
    connection: sqlite3.Connection,
    table_name: str,
    *,
    clauses: list[str],
    params: list[Any],
    order_by: str,
    limit: int,
    mapper,
) -> list[dict[str, Any]]:
    where_sql = "" if not clauses else "WHERE " + " AND ".join(clauses)
    rows = connection.execute(
        f"""
        SELECT *
        FROM {table_name}
        {where_sql}
        ORDER BY {order_by}
        LIMIT ?
        """,
        (*params, _bounded_limit(limit)),
    ).fetchall()
    return [mapper(row) for row in rows]


def _common_filters(
    *,
    trade_date: str | None = None,
    status: object | None = None,
    code: str | None = None,
) -> tuple[list[str], list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if trade_date is not None:
        clauses.append("trade_date = ?")
        params.append(require_non_empty_str(trade_date, "trade_date"))
    if status is not None:
        clauses.append("status = ?")
        params.append(str(getattr(status, "value", status)).upper())
    if code is not None:
        clauses.append("code = ?")
        params.append(validate_stock_code(code))
    return clauses, params


def _evaluation_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    item = _row_to_dict(row)
    item["reason_codes"] = _json_array(item.pop("reason_codes_json"))
    item["evidence_json"] = _json_object(item.pop("evidence_json"))
    item["dry_run_only"] = bool(item["dry_run_only"])
    item["broker_order_allowed"] = bool(item["broker_order_allowed"])
    item["gateway_command_allowed"] = bool(item["gateway_command_allowed"])
    item["live_order_allowed"] = False
    item["broker_order_sent"] = False
    return item


def _signal_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    item = _row_to_dict(row)
    item["reason_codes"] = _json_array(item.pop("reason_codes_json"))
    item["evidence_json"] = _json_object(item.pop("evidence_json"))
    item["dry_run_only"] = True
    item["close_only"] = True
    item["live_order_allowed"] = False
    item["gateway_command_allowed"] = False
    item["broker_order_sent"] = False
    return item


def _intent_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    item = _row_to_dict(row)
    item["reason_codes"] = _json_array(item.pop("reason_codes_json"))
    item["evidence_json"] = _json_object(item.pop("evidence_json"))
    for field_name in (
        "dry_run_only",
        "close_only",
        "live_order_allowed",
        "gateway_command_allowed",
        "broker_order_sent",
    ):
        item[field_name] = bool(item[field_name])
    return item


def _order_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    item = _row_to_dict(row)
    item["reason_codes"] = _json_array(item.pop("reason_codes_json"))
    item["evidence_json"] = _json_object(item.pop("evidence_json"))
    item["dry_run_only"] = bool(item["dry_run_only"])
    item["close_only"] = bool(item["close_only"])
    item["broker_order_sent"] = bool(item["broker_order_sent"])
    item["live_order_allowed"] = False
    item["gateway_command_allowed"] = False
    return item


def _execution_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    item = _row_to_dict(row)
    item["dry_run_only"] = bool(item["dry_run_only"])
    item["broker_order_sent"] = bool(item["broker_order_sent"])
    item["close_only"] = True
    item["live_order_allowed"] = False
    item["gateway_command_allowed"] = False
    return item


def _error_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    item = _row_to_dict(row)
    item["payload"] = _json_object(item.pop("payload_json"))
    return item


def _position_evidence(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "dry_run_position_id": row["dry_run_position_id"],
        "trade_date": row["trade_date"],
        "code": row["code"],
        "name": row["name"],
        "quantity": row["quantity"],
        "avg_price": row["avg_price"],
        "status": row["status"],
        "opened_at": row["opened_at"],
    }


def _tick_evidence(row: sqlite3.Row, tick_age: float) -> dict[str, Any]:
    return {
        "code": row["code"],
        "name": row["name"],
        "price": row["price"],
        "event_ts": row["event_ts"],
        "quality_status": row["quality_status"],
        "tick_age_sec": tick_age,
    }


def _base_evidence() -> dict[str, Any]:
    return {
        "dry_run_only": True,
        "close_only": True,
        "live_order_allowed": False,
        "gateway_command_allowed": False,
        "broker_order_sent": False,
        "ai_output_used": False,
        "rca_output_used": False,
        "codex_prompt_used": False,
    }


def _active_position_count(connection: sqlite3.Connection) -> int:
    row = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM dry_run_positions
        WHERE status = 'OPEN'
            AND quantity > 0
        """
    ).fetchone()
    return int(row["count"])


def _count_rows(connection: sqlite3.Connection, table_name: str) -> int:
    row = connection.execute(f"SELECT COUNT(*) AS count FROM {table_name}").fetchone()
    return int(row["count"])


def _bounded_limit(limit: int) -> int:
    return min(max(int(limit), 1), 500)


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    return float(value)


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}


def _json_array(value: str | None) -> list[str]:
    if not value:
        return []
    loaded = json.loads(value)
    if not isinstance(loaded, list):
        return []
    return [str(item) for item in loaded]


def _json_object(value: object) -> dict[str, Any]:
    if not value:
        return {}
    if isinstance(value, Mapping):
        return dict(value)
    loaded = json.loads(str(value))
    return loaded if isinstance(loaded, dict) else {}


def _json_dumps(value: object) -> str:
    return json.dumps(
        normalize_value(value),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _merge_reasons(reasons: list[str]) -> list[str]:
    return [*dict.fromkeys(str(reason).upper() for reason in reasons if str(reason).strip())]
