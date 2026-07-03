from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

DEFAULT_REPORT_ROOT = ROOT_DIR / "reports" / "live_sim_pilot"
EXIT_REASON_BUCKETS = ("STOP_LOSS", "TAKE_PROFIT", "TRAILING", "MAX_HOLD", "EOD")


def build_live_sim_pilot_kpi(
    connection: sqlite3.Connection,
    trade_date: str,
) -> dict[str, Any]:
    from domain.broker.utils import (
        datetime_to_wire,
        normalize_value,
        require_non_empty_str,
        utc_now,
    )
    from services.ai_sidecar.live_sim_review_store import list_live_sim_review_reports

    resolved_trade_date = require_non_empty_str(trade_date, "trade_date")
    candidates = _fetch_all(
        connection,
        """
        SELECT candidate_instance_id, code, name, state, reason_codes_json
        FROM candidates
        WHERE trade_date = ?
        """,
        (resolved_trade_date,),
    )
    strategy_rows = _fetch_all(
        connection,
        """
        SELECT *
        FROM strategy_observations_latest
        WHERE trade_date = ?
        """,
        (resolved_trade_date,),
    )
    risk_rows = _fetch_all(
        connection,
        """
        SELECT *
        FROM risk_observations_latest
        WHERE trade_date = ?
        """,
        (resolved_trade_date,),
    )
    order_plans = _fetch_all(
        connection,
        """
        SELECT *
        FROM order_plan_drafts_latest
        WHERE trade_date = ?
        """,
        (resolved_trade_date,),
    )
    intents = _fetch_all(
        connection,
        """
        SELECT *
        FROM live_sim_intents
        WHERE trade_date = ?
            AND side = 'BUY'
        """,
        (resolved_trade_date,),
    )
    orders = _fetch_all(
        connection,
        """
        SELECT *
        FROM live_sim_orders
        WHERE trade_date = ?
            AND side = 'BUY'
        """,
        (resolved_trade_date,),
    )
    executions = _fetch_all(
        connection,
        """
        SELECT e.*
        FROM live_sim_executions AS e
        JOIN live_sim_orders AS o
            ON o.live_sim_order_id = e.live_sim_order_id
        WHERE o.trade_date = ?
            AND o.side = 'BUY'
            AND e.side = 'BUY'
        """,
        (resolved_trade_date,),
    )
    rejections = _fetch_all(
        connection,
        """
        SELECT *
        FROM live_sim_rejections
        WHERE trade_date = ?
        """,
        (resolved_trade_date,),
    )
    cancel_intents = _fetch_all(
        connection,
        """
        SELECT c.*
        FROM live_sim_cancel_intents AS c
        JOIN live_sim_orders AS o
            ON o.live_sim_order_id = c.live_sim_order_id
        WHERE o.trade_date = ?
        """,
        (resolved_trade_date,),
    )

    enriched_intents = [_with_evidence(row) for row in intents]
    initial_intents = [row for row in enriched_intents if not _is_reprice_intent(row)]
    reprice_intents = [row for row in enriched_intents if _is_reprice_intent(row)]
    orders_by_intent = _group_by(orders, "live_sim_intent_id")
    executions_by_order = _group_by(executions, "live_sim_order_id")
    initial_intent_ids = {str(row["live_sim_intent_id"]) for row in initial_intents}
    initial_orders = [
        row for row in orders if str(row["live_sim_intent_id"]) in initial_intent_ids
    ]
    initial_command_orders = [row for row in initial_orders if row.get("gateway_command_id")]
    initial_filled_orders = [row for row in initial_orders if _int(row.get("filled_quantity")) > 0]

    funnel = _build_funnel(
        candidates=candidates,
        strategy_rows=strategy_rows,
        risk_rows=risk_rows,
        order_plans=order_plans,
        initial_intents=initial_intents,
        initial_orders=initial_orders,
        initial_command_orders=initial_command_orders,
        initial_filled_orders=initial_filled_orders,
        rejections=rejections,
        cancel_intents=cancel_intents,
    )
    fills = _build_fill_section(
        orders=orders,
        executions_by_order=executions_by_order,
        cancel_intents=cancel_intents,
        reprice_intents=reprice_intents,
        orders_by_intent=orders_by_intent,
    )
    strategy = _build_strategy_section(connection, resolved_trade_date)
    review_reports = list_live_sim_review_reports(
        connection,
        trade_date=resolved_trade_date,
        limit=500,
    )

    payload = {
        "trade_date": resolved_trade_date,
        "generated_at": datetime_to_wire(utc_now()),
        "schema_version": "live-sim-pilot-kpi.v1",
        "read_only": True,
        "review_only": True,
        "order_action_allowed": False,
        "gateway_command_allowed": False,
        "live_real_allowed": False,
        "broker_order_path": "LIVE_SIM_ONLY",
        "funnel": funnel,
        "fills": fills,
        "strategy": strategy,
        "review_store": {
            "live_sim_review_report_count": len(review_reports),
            "latest_review_ids": [row["review_id"] for row in review_reports[:10]],
        },
    }
    return normalize_value(payload)


def write_live_sim_pilot_report(
    payload: Mapping[str, Any],
    *,
    report_root: str | Path = DEFAULT_REPORT_ROOT,
) -> dict[str, Path]:
    trade_date = str(payload["trade_date"])
    output_dir = Path(report_root) / trade_date
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "kpi.json"
    md_path = output_dir / "kpi.md"
    json_path.write_text(
        json.dumps(dict(payload), ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    md_path.write_text(_render_markdown(payload), encoding="utf-8")
    return {"kpi_json": json_path, "kpi_md": md_path}


def open_readonly_connection(db_path: str | Path) -> sqlite3.Connection:
    path = Path(db_path)
    if not path.exists():
        raise FileNotFoundError(f"trading DB not found: {path}")
    connection = sqlite3.connect(
        f"{path.resolve().as_uri()}?mode=ro",
        timeout=5.0,
        uri=True,
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout=5000")
    connection.execute("PRAGMA query_only=ON")
    return connection


def _build_funnel(
    *,
    candidates: Sequence[Mapping[str, Any]],
    strategy_rows: Sequence[Mapping[str, Any]],
    risk_rows: Sequence[Mapping[str, Any]],
    order_plans: Sequence[Mapping[str, Any]],
    initial_intents: Sequence[Mapping[str, Any]],
    initial_orders: Sequence[Mapping[str, Any]],
    initial_command_orders: Sequence[Mapping[str, Any]],
    initial_filled_orders: Sequence[Mapping[str, Any]],
    rejections: Sequence[Mapping[str, Any]],
    cancel_intents: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    from services.operator.reason_classifier import aggregate_reason_summary

    strategy_by_candidate = {str(row["candidate_instance_id"]): row for row in strategy_rows}
    risk_by_candidate = {str(row["candidate_instance_id"]): row for row in risk_rows}
    plans_by_candidate = _group_by(order_plans, "candidate_instance_id")
    intents_by_candidate = _group_by(initial_intents, "candidate_instance_id")

    candidate_ids = {str(row["candidate_instance_id"]) for row in candidates}
    matched_strategy = [
        row for row in strategy_rows if str(row.get("overall_status")) == "MATCHED_OBSERVATION"
    ]
    matched_ids = {str(row["candidate_instance_id"]) for row in matched_strategy}
    risk_pass = [row for row in risk_rows if str(row.get("overall_status")) == "OBSERVE_PASS"]
    risk_pass_ids = {str(row["candidate_instance_id"]) for row in risk_pass}
    plan_ready = [row for row in order_plans if str(row.get("status")) == "PLAN_READY"]
    plan_ready_ids = {str(row["candidate_instance_id"]) for row in plan_ready}

    strategy_drop_reasons = []
    for candidate_id in sorted(candidate_ids):
        row = strategy_by_candidate.get(candidate_id)
        if row is None:
            strategy_drop_reasons.append("STRATEGY_OBSERVATION_MISSING")
        elif str(row.get("overall_status")) != "MATCHED_OBSERVATION":
            strategy_drop_reasons.extend(_reason_codes(row, fallback=row.get("overall_status")))

    risk_drop_reasons = []
    for candidate_id in sorted(matched_ids):
        row = risk_by_candidate.get(candidate_id)
        if row is None:
            risk_drop_reasons.append("RISK_OBSERVATION_MISSING")
        elif str(row.get("overall_status")) != "OBSERVE_PASS":
            risk_drop_reasons.extend(_reason_codes(row, fallback=row.get("overall_status")))

    entry_drop_reasons = []
    for candidate_id in sorted(risk_pass_ids):
        plans = plans_by_candidate.get(candidate_id, [])
        if not plans:
            entry_drop_reasons.append("ORDER_PLAN_MISSING")
        for plan in plans:
            if str(plan.get("status")) != "PLAN_READY":
                entry_drop_reasons.extend(_reason_codes(plan, fallback=plan.get("status")))

    intent_drop_reasons = []
    for row in rejections:
        intent_drop_reasons.extend(_reason_codes(row, fallback="LIVE_SIM_REJECTED"))
    for candidate_id in sorted(plan_ready_ids):
        if not intents_by_candidate.get(candidate_id):
            intent_drop_reasons.append("LIVE_SIM_INTENT_NOT_CREATED")

    initial_order_ids = {str(row["live_sim_order_id"]) for row in initial_orders}
    command_order_ids = {str(row["live_sim_order_id"]) for row in initial_command_orders}
    command_drop_reasons = []
    for intent in initial_intents:
        if not _group_by(initial_orders, "live_sim_intent_id").get(
            str(intent["live_sim_intent_id"]),
            [],
        ):
            command_drop_reasons.extend(_reason_codes(intent, fallback="LIVE_SIM_ORDER_MISSING"))
    for order in initial_orders:
        if not order.get("gateway_command_id"):
            command_drop_reasons.append(str(order.get("status") or "GATEWAY_COMMAND_MISSING"))

    fill_drop_reasons = []
    ttl_cancel_by_order = {
        str(row["live_sim_order_id"]): row
        for row in cancel_intents
        if str(row.get("reason") or "").upper() == "TTL_EXPIRED"
    }
    for order in initial_orders:
        order_id = str(order["live_sim_order_id"])
        if order_id not in command_order_ids or _int(order.get("filled_quantity")) > 0:
            continue
        cancel = ttl_cancel_by_order.get(order_id)
        if cancel is not None:
            fill_drop_reasons.append(f"TTL_CANCEL_{str(cancel.get('status') or 'CREATED')}")
        elif order.get("broker_message"):
            fill_drop_reasons.append(str(order["broker_message"]).upper())
        else:
            fill_drop_reasons.append(str(order.get("status") or "NOT_FILLED"))

    counts = {
        "candidate": len(candidates),
        "strategy_matched": len(matched_strategy),
        "risk_observe_pass": len(risk_pass),
        "entry_plan_ready": len(plan_ready),
        "live_sim_intent": len(initial_intents),
        "live_sim_command": len(initial_command_orders),
        "live_sim_filled_order": len(initial_filled_orders),
    }
    stage_specs = [
        ("candidate", "Candidate"),
        ("strategy_matched", "Strategy MATCHED_OBSERVATION"),
        ("risk_observe_pass", "Risk OBSERVE_PASS"),
        ("entry_plan_ready", "Entry Timing PLAN_READY"),
        ("live_sim_intent", "LIVE_SIM Intent"),
        ("live_sim_command", "LIVE_SIM Command"),
        ("live_sim_filled_order", "LIVE_SIM Filled Order"),
    ]
    drop_reasons = {
        "strategy_matched": _top_reasons(strategy_drop_reasons),
        "risk_observe_pass": _top_reasons(risk_drop_reasons),
        "entry_plan_ready": _top_reasons(entry_drop_reasons),
        "live_sim_intent": _top_reasons(intent_drop_reasons),
        "live_sim_command": _top_reasons(command_drop_reasons),
        "live_sim_filled_order": _top_reasons(fill_drop_reasons),
    }
    all_drop_reasons = [
        *strategy_drop_reasons,
        *risk_drop_reasons,
        *entry_drop_reasons,
        *intent_drop_reasons,
        *command_drop_reasons,
        *fill_drop_reasons,
    ]

    stages = []
    previous_count: int | None = None
    candidate_count = counts["candidate"]
    for key, label in stage_specs:
        count = counts[key]
        stages.append(
            {
                "stage": key,
                "label": label,
                "count": count,
                "from_previous_count": previous_count,
                "conversion_from_previous": _safe_div(count, previous_count),
                "conversion_from_candidate": _safe_div(count, candidate_count),
                "drop_reasons_top": drop_reasons.get(key, []),
            }
        )
        previous_count = count

    return {
        "counts": counts,
        "stages": stages,
        "drop_reasons_top10": drop_reasons,
        "drop_reason_summary": aggregate_reason_summary(all_drop_reasons),
        "initial_order_count": len(initial_order_ids),
    }


def _build_fill_section(
    *,
    orders: Sequence[Mapping[str, Any]],
    executions_by_order: Mapping[str, Sequence[Mapping[str, Any]]],
    cancel_intents: Sequence[Mapping[str, Any]],
    reprice_intents: Sequence[Mapping[str, Any]],
    orders_by_intent: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, Any]:
    command_orders = [row for row in orders if row.get("gateway_command_id")]
    filled_orders = [row for row in orders if _int(row.get("filled_quantity")) > 0]
    time_to_fill_values = []
    slippage_ticks = []
    slippage_pct = []
    slippage_items = []

    for order in filled_orders:
        order_id = str(order["live_sim_order_id"])
        first_execution = _first_execution(executions_by_order.get(order_id, []))
        if first_execution is not None:
            start = order.get("command_queued_at") or order.get("created_at")
            elapsed = _seconds_between(start, first_execution.get("executed_at"))
            if elapsed is not None:
                time_to_fill_values.append(elapsed)
        limit_price = _float(order.get("limit_price"))
        avg_fill_price = _float(order.get("avg_fill_price")) or _weighted_avg_price(
            executions_by_order.get(order_id, [])
        )
        if limit_price > 0 and avg_fill_price > 0:
            tick_size = _krx_tick_size(limit_price)
            raw_slippage = avg_fill_price - limit_price
            tick_slippage = raw_slippage / tick_size if tick_size > 0 else 0.0
            pct_slippage = raw_slippage / limit_price
            slippage_ticks.append(tick_slippage)
            slippage_pct.append(pct_slippage)
            slippage_items.append(
                {
                    "live_sim_order_id": order_id,
                    "limit_price": limit_price,
                    "avg_fill_price": avg_fill_price,
                    "slippage_price": raw_slippage,
                    "slippage_ticks": tick_slippage,
                    "slippage_pct": pct_slippage,
                }
            )

    ttl_cancel_intents = [
        row for row in cancel_intents if str(row.get("reason") or "").upper() == "TTL_EXPIRED"
    ]
    reprice_success_count = 0
    for intent in reprice_intents:
        intent_orders = orders_by_intent.get(str(intent["live_sim_intent_id"]), [])
        if any(_int(order.get("filled_quantity")) > 0 for order in intent_orders):
            reprice_success_count += 1

    return {
        "buy_order_count": len(orders),
        "buy_command_count": len(command_orders),
        "buy_filled_order_count": len(filled_orders),
        "buy_execution_count": sum(
            len(executions_by_order.get(str(row["live_sim_order_id"]), []))
            for row in orders
        ),
        "buy_fill_rate": _safe_div(len(filled_orders), len(command_orders)),
        "avg_time_to_fill_sec": _mean(time_to_fill_values),
        "time_to_fill_sec_distribution": _distribution(time_to_fill_values),
        "slippage": {
            "sample_count": len(slippage_items),
            "avg_ticks": _mean(slippage_ticks),
            "avg_pct": _mean(slippage_pct),
            "ticks_distribution": _distribution(slippage_ticks),
            "pct_distribution": _distribution(slippage_pct),
            "items": slippage_items[:50],
        },
        "ttl_cancel_count": len(ttl_cancel_intents),
        "ttl_cancel_acked_count": sum(
            1 for row in ttl_cancel_intents if str(row.get("status") or "").upper() == "ACKED"
        ),
        "reprice_attempt_count": len(reprice_intents),
        "reprice_success_count": reprice_success_count,
    }


def _build_strategy_section(
    connection: sqlite3.Connection,
    trade_date: str,
) -> dict[str, Any]:
    positions = _fetch_all(
        connection,
        """
        SELECT *
        FROM live_sim_positions
        WHERE trade_date = ?
            AND status = 'CLOSED'
        ORDER BY closed_at ASC, position_id ASC
        """,
        (trade_date,),
    )
    trades = [_closed_trade_record(connection, position) for position in positions]
    setup_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    exit_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for trade in trades:
        setup_groups[trade["setup_type"]].append(trade)
        exit_groups[trade["exit_reason"]].append(trade)

    by_setup = []
    for setup_type, items in sorted(setup_groups.items()):
        pnls = [_float(item["net_pnl"]) for item in items]
        wins = [item for item in items if _float(item["net_pnl"]) > 0]
        losses = [item for item in items if _float(item["net_pnl"]) <= 0]
        by_setup.append(
            {
                "setup_type": setup_type,
                "trade_count": len(items),
                "win_count": len(wins),
                "loss_count": len(losses),
                "win_rate": _safe_div(len(wins), len(items)),
                "avg_net_pnl": _mean(pnls),
                "expectancy_net": _expectancy(items),
            }
        )

    exit_reason_distribution = {}
    for reason in EXIT_REASON_BUCKETS:
        items = exit_groups.get(reason, [])
        exit_reason_distribution[reason] = {
            "count": len(items),
            "share": _safe_div(len(items), len(trades)),
        }
    unknown_items = exit_groups.get("UNKNOWN", [])
    if unknown_items:
        exit_reason_distribution["UNKNOWN"] = {
            "count": len(unknown_items),
            "share": _safe_div(len(unknown_items), len(trades)),
        }

    mfe_mae_by_exit_reason = {}
    for reason, items in sorted(exit_groups.items()):
        mfe_values = [_float(item["mfe"]) for item in items if item.get("mfe") is not None]
        mae_values = [_float(item["mae"]) for item in items if item.get("mae") is not None]
        mfe_mae_by_exit_reason[reason] = {
            "count": len(items),
            "mfe": _distribution(mfe_values),
            "mae": _distribution(mae_values),
            "mfe_pct": _pct_distribution(mfe_values),
            "mae_pct": _pct_distribution(mae_values),
        }

    stop_loss_mfe = [
        _float(item["mfe"])
        for item in exit_groups.get("STOP_LOSS", [])
        if item.get("mfe") is not None
    ]
    winning_mae = [
        _float(item["mae"])
        for item in trades
        if _float(item["net_pnl"]) > 0 and item.get("mae") is not None
    ]

    return {
        "closed_position_count": len(trades),
        "by_setup": by_setup,
        "exit_reason_distribution": exit_reason_distribution,
        "holding_time_sec_distribution": _distribution(
            [
                item["holding_time_sec"]
                for item in trades
                if item.get("holding_time_sec") is not None
            ]
        ),
        "mfe_mae_by_exit_reason": mfe_mae_by_exit_reason,
        "stop_loss_mfe_distribution": _distribution(stop_loss_mfe),
        "stop_loss_mfe_pct_distribution": _pct_distribution(stop_loss_mfe),
        "winning_trade_mae_distribution": _distribution(winning_mae),
        "winning_trade_mae_pct_distribution": _pct_distribution(winning_mae),
        "trades": trades[:200],
    }


def _closed_trade_record(
    connection: sqlite3.Connection,
    position: Mapping[str, Any],
) -> dict[str, Any]:
    close_event = _fetch_one(
        connection,
        """
        SELECT *
        FROM live_sim_position_events
        WHERE position_id = ?
            AND event_type = 'POSITION_CLOSED'
        ORDER BY created_at DESC, event_id DESC
        LIMIT 1
        """,
        (position["position_id"],),
    )
    source_intent = None
    if position.get("source_live_sim_intent_id"):
        source_intent = _fetch_one(
            connection,
            "SELECT * FROM live_sim_intents WHERE live_sim_intent_id = ?",
            (position["source_live_sim_intent_id"],),
        )
    strategy = None
    if source_intent is not None:
        strategy = _fetch_one(
            connection,
            """
            SELECT *
            FROM strategy_observations_latest
            WHERE candidate_instance_id = ?
            """,
            (source_intent["candidate_instance_id"],),
        )

    close_evidence = _json_object(
        None if close_event is None else close_event.get("evidence_json")
    )
    source_evidence = _json_object(
        None if source_intent is None else source_intent.get("evidence_json")
    )
    exit_reason = _exit_reason_for_close(connection, close_event, close_evidence)
    setup_type = (
        str(source_evidence.get("setup_type") or "")
        or str(_json_object(source_evidence.get("order_plan_draft")).get("setup_type") or "")
        or (str(strategy["primary_setup_type"]) if strategy is not None else "")
        or "UNKNOWN"
    )
    mfe, mae = _excursions_for_position(position, close_evidence)
    holding_time = _seconds_between(position.get("opened_at"), position.get("closed_at"))
    return {
        "position_id": position["position_id"],
        "code": position["code"],
        "name": position["name"],
        "setup_type": setup_type,
        "exit_reason": exit_reason,
        "net_pnl": _float(position.get("realized_pnl")),
        "quantity": _int(position.get("quantity")),
        "avg_entry_price": _float(position.get("avg_entry_price")),
        "highest_price": _float(position.get("highest_price")),
        "lowest_price": _float(position.get("lowest_price")),
        "mfe": mfe,
        "mae": mae,
        "mfe_pct": None if mfe is None else mfe * 100,
        "mae_pct": None if mae is None else mae * 100,
        "holding_time_sec": holding_time,
        "opened_at": position.get("opened_at"),
        "closed_at": position.get("closed_at"),
        "close_live_sim_order_id": (
            None if close_event is None else close_event.get("live_sim_order_id")
        ),
    }


def _exit_reason_for_close(
    connection: sqlite3.Connection,
    close_event: Mapping[str, Any] | None,
    close_evidence: Mapping[str, Any],
) -> str:
    raw_reason = close_evidence.get("exit_reason") or close_evidence.get("reason")
    if close_event is not None and close_event.get("live_sim_order_id"):
        exit_intent = _fetch_one(
            connection,
            """
            SELECT *
            FROM live_sim_exit_intents
            WHERE live_sim_order_id = ?
            ORDER BY created_at DESC, exit_intent_id DESC
            LIMIT 1
            """,
            (close_event["live_sim_order_id"],),
        )
        if exit_intent is not None:
            raw_reason = exit_intent.get("reason") or raw_reason
            if exit_intent.get("exit_signal_id"):
                signal = _fetch_one(
                    connection,
                    "SELECT * FROM live_sim_exit_signals WHERE exit_signal_id = ?",
                    (exit_intent["exit_signal_id"],),
                )
                if signal is not None:
                    raw_reason = signal.get("reason") or raw_reason
    return _normalize_exit_reason(raw_reason)


def _excursions_for_position(
    position: Mapping[str, Any],
    close_evidence: Mapping[str, Any],
) -> tuple[float | None, float | None]:
    mfe_value = _optional_float(
        close_evidence.get("mfe", close_evidence.get("mfe_ratio"))
    )
    mae_value = _optional_float(
        close_evidence.get("mae", close_evidence.get("mae_ratio"))
    )
    if mfe_value is not None and mae_value is not None:
        return mfe_value, mae_value
    entry = _float(position.get("avg_entry_price"))
    if entry <= 0:
        return mfe_value, mae_value
    highest = _optional_float(position.get("highest_price"))
    lowest = _optional_float(position.get("lowest_price"))
    if mfe_value is None and highest is not None:
        mfe_value = (highest - entry) / entry
    if mae_value is None and lowest is not None:
        mae_value = (lowest - entry) / entry
    return mfe_value, mae_value


def _render_markdown(payload: Mapping[str, Any]) -> str:
    funnel = _mapping(payload.get("funnel"))
    fills = _mapping(payload.get("fills"))
    strategy = _mapping(payload.get("strategy"))
    lines = [
        "# LIVE_SIM Pilot Daily KPI",
        "",
        f"- trade_date: `{_cell(payload.get('trade_date'))}`",
        f"- generated_at: `{_cell(payload.get('generated_at'))}`",
        f"- read_only: `{_cell(payload.get('read_only'))}`",
        f"- gateway_command_allowed: `{_cell(payload.get('gateway_command_allowed'))}`",
        f"- live_real_allowed: `{_cell(payload.get('live_real_allowed'))}`",
        "",
        "## Funnel",
        "",
        "| Stage | Count | Prev conversion | Candidate conversion | Top drop reasons |",
        "| --- | ---: | ---: | ---: | --- |",
    ]
    for stage in _list(funnel.get("stages")):
        stage_map = _mapping(stage)
        lines.append(
            "| {label} | {count} | {prev} | {total} | {reasons} |".format(
                label=_md_cell(stage_map.get("label")),
                count=_md_cell(stage_map.get("count")),
                prev=_md_cell(_format_pct(stage_map.get("conversion_from_previous"))),
                total=_md_cell(_format_pct(stage_map.get("conversion_from_candidate"))),
                reasons=_md_cell(_format_reason_counts(stage_map.get("drop_reasons_top"))),
            )
        )

    slippage = _mapping(fills.get("slippage"))
    lines.extend(
        [
            "",
            "## Fills",
            "",
            f"- BUY fill rate: `{_format_pct(fills.get('buy_fill_rate'))}`",
            "- BUY commands / filled orders: "
            f"`{_cell(fills.get('buy_command_count'))}` / "
            f"`{_cell(fills.get('buy_filled_order_count'))}`",
            f"- avg time-to-fill sec: `{_format_number(fills.get('avg_time_to_fill_sec'))}`",
            f"- avg slippage ticks: `{_format_number(slippage.get('avg_ticks'))}`",
            f"- avg slippage pct: `{_format_pct(slippage.get('avg_pct'))}`",
            f"- TTL cancel count: `{_cell(fills.get('ttl_cancel_count'))}`",
            f"- TTL cancel ACK count: `{_cell(fills.get('ttl_cancel_acked_count'))}`",
            "- reprice attempts / success: "
            f"`{_cell(fills.get('reprice_attempt_count'))}` / "
            f"`{_cell(fills.get('reprice_success_count'))}`",
            "",
            "## Strategy",
            "",
            f"- closed position count: `{_cell(strategy.get('closed_position_count'))}`",
            "- holding time sec distribution: "
            f"`{_json_excerpt(strategy.get('holding_time_sec_distribution'))}`",
            "",
            "### Setup KPI",
            "",
            "| Setup | Trades | Win rate | Avg net PnL | Expectancy net |",
            "| --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in _list(strategy.get("by_setup")):
        item = _mapping(row)
        lines.append(
            "| {setup} | {count} | {win_rate} | {avg} | {expectancy} |".format(
                setup=_md_cell(item.get("setup_type")),
                count=_md_cell(item.get("trade_count")),
                win_rate=_md_cell(_format_pct(item.get("win_rate"))),
                avg=_md_cell(_format_number(item.get("avg_net_pnl"))),
                expectancy=_md_cell(_format_number(item.get("expectancy_net"))),
            )
        )

    lines.extend(
        [
            "",
            "### Exit Reasons",
            "",
            "| Exit reason | Count | Share | MFE median | MAE median |",
            "| --- | ---: | ---: | ---: | ---: |",
        ]
    )
    exit_distribution = _mapping(strategy.get("exit_reason_distribution"))
    mfe_mae = _mapping(strategy.get("mfe_mae_by_exit_reason"))
    for reason, raw_item in exit_distribution.items():
        item = _mapping(raw_item)
        excursion = _mapping(mfe_mae.get(reason))
        lines.append(
            "| {reason} | {count} | {share} | {mfe} | {mae} |".format(
                reason=_md_cell(reason),
                count=_md_cell(item.get("count")),
                share=_md_cell(_format_pct(item.get("share"))),
                mfe=_md_cell(_format_pct(_mapping(excursion.get("mfe")).get("median"))),
                mae=_md_cell(_format_pct(_mapping(excursion.get("mae")).get("median"))),
            )
        )

    lines.extend(
        [
            "",
            "### 손절 트레이드의 MFE 분포",
            "",
            f"- ratio: `{_json_excerpt(strategy.get('stop_loss_mfe_distribution'))}`",
            f"- pct: `{_json_excerpt(strategy.get('stop_loss_mfe_pct_distribution'))}`",
            "",
            "### 승리 트레이드의 MAE 분포",
            "",
            f"- ratio: `{_json_excerpt(strategy.get('winning_trade_mae_distribution'))}`",
            f"- pct: `{_json_excerpt(strategy.get('winning_trade_mae_pct_distribution'))}`",
            "",
        ]
    )
    return "\n".join(lines)


def _fetch_all(
    connection: sqlite3.Connection,
    sql: str,
    params: Sequence[Any] = (),
) -> list[dict[str, Any]]:
    return [_row_to_dict(row) for row in connection.execute(sql, tuple(params)).fetchall()]


def _fetch_one(
    connection: sqlite3.Connection,
    sql: str,
    params: Sequence[Any] = (),
) -> dict[str, Any] | None:
    row = connection.execute(sql, tuple(params)).fetchone()
    return None if row is None else _row_to_dict(row)


def _row_to_dict(row: sqlite3.Row | Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(row, sqlite3.Row):
        return {key: row[key] for key in row.keys()}
    return dict(row)


def _with_evidence(row: Mapping[str, Any]) -> dict[str, Any]:
    item = dict(row)
    item["evidence"] = _json_object(item.get("evidence_json"))
    return item


def _is_reprice_intent(intent: Mapping[str, Any]) -> bool:
    evidence = _mapping(intent.get("evidence"))
    return (
        str(evidence.get("source") or "").lower() == "live_sim_reprice"
        or bool(_json_object(evidence.get("reprice")))
    )


def _reason_codes(row: Mapping[str, Any], *, fallback: object) -> list[str]:
    reasons = _json_array(row.get("reason_codes_json"))
    if not reasons and row.get("reason_codes") is not None:
        reasons = _json_array(row.get("reason_codes"))
    if reasons:
        return [str(reason).upper() for reason in reasons]
    fallback_text = str(fallback or "UNKNOWN").upper()
    return [fallback_text] if fallback_text else ["UNKNOWN"]


def _top_reasons(reasons: Sequence[str], *, limit: int = 10) -> list[dict[str, Any]]:
    counter = Counter(str(reason).upper() for reason in reasons if str(reason).strip())
    return [
        {"reason": reason, "count": count}
        for reason, count in counter.most_common(max(min(limit, 50), 1))
    ]


def _group_by(
    rows: Sequence[Mapping[str, Any]],
    key: str,
) -> dict[str, list[Mapping[str, Any]]]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        value = row.get(key)
        if value is not None:
            grouped[str(value)].append(row)
    return grouped


def _first_execution(
    executions: Sequence[Mapping[str, Any]],
) -> Mapping[str, Any] | None:
    if not executions:
        return None
    return min(executions, key=lambda row: str(row.get("executed_at") or ""))


def _weighted_avg_price(executions: Sequence[Mapping[str, Any]]) -> float:
    quantity = sum(_int(row.get("quantity")) for row in executions)
    if quantity <= 0:
        return 0.0
    notional = sum(_float(row.get("price")) * _int(row.get("quantity")) for row in executions)
    return notional / quantity


def _seconds_between(start: object, end: object) -> float | None:
    from domain.broker.utils import parse_timestamp

    if start is None or end is None:
        return None
    try:
        elapsed = parse_timestamp(end, "end") - parse_timestamp(start, "start")
        return max(elapsed.total_seconds(), 0.0)
    except ValueError:
        return None


def _expectancy(items: Sequence[Mapping[str, Any]]) -> float | None:
    if not items:
        return None
    wins = [_float(item.get("net_pnl")) for item in items if _float(item.get("net_pnl")) > 0]
    losses = [_float(item.get("net_pnl")) for item in items if _float(item.get("net_pnl")) <= 0]
    win_rate = len(wins) / len(items)
    loss_rate = len(losses) / len(items)
    return win_rate * (_mean(wins) or 0.0) + loss_rate * (_mean(losses) or 0.0)


def _distribution(values: Sequence[float]) -> dict[str, Any]:
    clean = sorted(float(value) for value in values if value is not None)
    if not clean:
        return {
            "count": 0,
            "min": None,
            "p25": None,
            "median": None,
            "p75": None,
            "max": None,
            "mean": None,
        }
    return {
        "count": len(clean),
        "min": clean[0],
        "p25": _percentile(clean, 0.25),
        "median": _percentile(clean, 0.50),
        "p75": _percentile(clean, 0.75),
        "max": clean[-1],
        "mean": _mean(clean),
    }


def _pct_distribution(values: Sequence[float]) -> dict[str, Any]:
    return _distribution([value * 100 for value in values])


def _percentile(values: Sequence[float], quantile: float) -> float:
    if len(values) == 1:
        return values[0]
    position = (len(values) - 1) * quantile
    lower = int(position)
    upper = min(lower + 1, len(values) - 1)
    weight = position - lower
    return values[lower] * (1 - weight) + values[upper] * weight


def _normalize_exit_reason(value: object) -> str:
    normalized = str(value or "").upper()
    if normalized == "TRAILING_STOP":
        return "TRAILING"
    if normalized == "EOD_FLATTEN":
        return "EOD"
    if normalized in EXIT_REASON_BUCKETS:
        return normalized
    return "UNKNOWN"


def _json_object(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if not value:
        return {}
    try:
        loaded = json.loads(str(value))
    except json.JSONDecodeError:
        return {}
    return loaded if isinstance(loaded, dict) else {}


def _json_array(value: object) -> list[Any]:
    if isinstance(value, list):
        return value
    if not value:
        return []
    try:
        loaded = json.loads(str(value))
    except json.JSONDecodeError:
        return [str(value)] if str(value).strip() else []
    if isinstance(loaded, Sequence) and not isinstance(loaded, str | bytes):
        return list(loaded)
    return []


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _list(value: object) -> list[Any]:
    return value if isinstance(value, list) else []


def _safe_div(numerator: int | float, denominator: int | float | None) -> float | None:
    if denominator is None or denominator == 0:
        return None
    return float(numerator) / float(denominator)


def _mean(values: Sequence[float]) -> float | None:
    clean = [float(value) for value in values if value is not None]
    if not clean:
        return None
    return sum(clean) / len(clean)


def _float(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _optional_float(value: object) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int(value: object) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _krx_tick_size(price: float) -> int:
    from services.entry_timing.tick_size import krx_tick_size

    try:
        return krx_tick_size(price)
    except ValueError:
        return 0


def _cell(value: object) -> str:
    if value is None:
        return "-"
    return str(value)


def _md_cell(value: object) -> str:
    return _cell(value).replace("|", "\\|").replace("\n", " ")


def _format_pct(value: object) -> str:
    if value is None:
        return "-"
    try:
        return f"{float(value) * 100:.2f}%"
    except (TypeError, ValueError):
        return "-"


def _format_number(value: object) -> str:
    if value is None:
        return "-"
    try:
        return f"{float(value):.4f}"
    except (TypeError, ValueError):
        return str(value)


def _format_reason_counts(value: object) -> str:
    items = _list(value)
    if not items:
        return "-"
    return ", ".join(
        f"{_mapping(item).get('reason')}={_mapping(item).get('count')}" for item in items
    )


def _json_excerpt(value: object, *, max_chars: int = 240) -> str:
    try:
        rendered = json.dumps(value, ensure_ascii=False, sort_keys=True)
    except TypeError:
        rendered = str(value)
    return rendered if len(rendered) <= max_chars else f"{rendered[: max_chars - 3]}..."


def main() -> int:
    from services.config import load_settings

    parser = argparse.ArgumentParser(
        description="Build a read-only LIVE_SIM pilot daily KPI report."
    )
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--report-root", default=str(DEFAULT_REPORT_ROOT))
    args = parser.parse_args()

    settings = load_settings()
    connection = open_readonly_connection(settings.trading_db_path)
    try:
        payload = build_live_sim_pilot_kpi(connection, args.trade_date)
    finally:
        connection.close()
    paths = write_live_sim_pilot_report(payload, report_root=args.report_root)
    result = {
        "ok": True,
        "trade_date": payload["trade_date"],
        "kpi_json": str(paths["kpi_json"]),
        "kpi_md": str(paths["kpi_md"]),
        "read_only": True,
        "order_action_allowed": False,
        "gateway_command_allowed": False,
        "live_real_allowed": False,
        "generated_at": datetime.now(tz=UTC).isoformat(),
    }
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
