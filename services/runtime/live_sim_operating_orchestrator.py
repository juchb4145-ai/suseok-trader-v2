from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import Any

from domain.broker.utils import datetime_to_wire, new_message_id, normalize_value, utc_now

from services.ai_advisory.service import score_ai_candidates
from services.config import Settings, load_settings
from services.entry_timing.service import evaluate_entry_timing
from services.live_sim.live_sim_service import (
    get_latest_live_sim_reconcile,
    get_live_sim_status,
    reconcile_live_sim,
    run_live_sim_cancel_unfilled_once,
    run_live_sim_exit_once,
    run_live_sim_reprice_once,
)
from services.operator.no_buy_sentinel import build_no_buy_sentinel_snapshot
from services.runtime.live_sim_pilot_pipeline import run_live_sim_pilot_pipeline_once
from services.runtime.preflight import (
    LiveSimPreflightResult,
    OperatingMode,
    PreflightStatus,
    run_live_sim_preflight,
)
from services.theme_leadership import rebuild_theme_leadership


@dataclass(frozen=True, kw_only=True)
class LiveSimOperatingRunResult:
    run_id: str
    trade_date: str | None
    mode: OperatingMode
    queue_commands: bool
    preflight: LiveSimPreflightResult
    status: str = "COMPLETED"
    buy_evaluated_count: int = 0
    buy_command_count: int = 0
    cancel_candidate_count: int = 0
    cancel_command_count: int = 0
    exit_signal_count: int = 0
    exit_command_count: int = 0
    reconcile_status: str | None = None
    no_buy_status: str | None = None
    ai_run_status: str | None = None
    warnings: Sequence[str] = field(default_factory=tuple)
    errors: Sequence[Mapping[str, Any]] = field(default_factory=tuple)
    reason_summary: Mapping[str, Any] = field(default_factory=dict)
    stages: Mapping[str, Any] = field(default_factory=dict)
    operator_summary: Mapping[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=lambda: datetime_to_wire(utc_now()))
    live_sim_only: bool = True
    live_real_allowed: bool = False
    no_order_side_effects: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "trade_date": self.trade_date,
            "mode": self.mode.value,
            "queue_commands": self.queue_commands,
            "preflight_status": self.preflight.status.value,
            "status": self.status,
            "buy_evaluated_count": self.buy_evaluated_count,
            "buy_command_count": self.buy_command_count,
            "cancel_candidate_count": self.cancel_candidate_count,
            "cancel_command_count": self.cancel_command_count,
            "exit_signal_count": self.exit_signal_count,
            "exit_command_count": self.exit_command_count,
            "reconcile_status": self.reconcile_status,
            "no_buy_status": self.no_buy_status,
            "ai_run_status": self.ai_run_status,
            "warnings": normalize_value(list(self.warnings)),
            "errors": normalize_value(list(self.errors)),
            "reason_summary": normalize_value(dict(self.reason_summary)),
            "preflight": self.preflight.to_dict(),
            "stages": normalize_value(dict(self.stages)),
            "operator_summary": normalize_value(dict(self.operator_summary)),
            "created_at": self.created_at,
            "live_sim_only": True,
            "live_real_allowed": False,
            "broker_order_path": "LIVE_SIM_ONLY",
            "real_order_allowed": False,
            "no_order_side_effects": self.buy_command_count
            + self.cancel_command_count
            + self.exit_command_count
            == 0,
        }


def run_live_sim_operating_cycle_once(
    connection: sqlite3.Connection,
    *,
    mode: OperatingMode | str | None = None,
    queue_commands: bool = False,
    trade_date: str | None = None,
    limit: int | None = None,
    include_ai: bool | None = None,
    include_no_buy: bool | None = None,
    settings: Settings | None = None,
) -> LiveSimOperatingRunResult:
    resolved_settings = settings or load_settings()
    resolved_mode = OperatingMode.coerce(mode, resolved_settings)
    resolved_include_ai = (
        resolved_settings.live_sim_operating_include_ai
        if include_ai is None
        else bool(include_ai)
    )
    resolved_include_no_buy = (
        resolved_settings.live_sim_operating_include_no_buy
        if include_no_buy is None
        else bool(include_no_buy)
    )
    run_id = new_message_id("live_sim_operating_run")
    created_at = datetime_to_wire(utc_now())
    stages: dict[str, Any] = {}
    errors: list[dict[str, Any]] = []
    warnings: list[str] = []
    reason_summary: dict[str, Any] = {
        "mode": resolved_mode.value,
        "queue_commands_requested": bool(queue_commands),
        "command_budget": _command_budget(resolved_settings),
        "command_policy": _mode_policy(resolved_mode),
        "blocking_reasons": [],
    }

    preflight = run_live_sim_preflight(
        connection,
        mode=resolved_mode,
        queue_commands=queue_commands,
        trade_date=trade_date,
        include_ai=resolved_include_ai,
        include_no_buy=resolved_include_no_buy,
        settings=resolved_settings,
    )
    stages["preflight"] = preflight.to_dict()
    warnings.extend(preflight.warnings)
    reason_summary["blocking_reasons"].extend(preflight.blocking_reasons)

    budgeted_settings = _settings_with_command_budgets(resolved_settings)
    queue_policy = _queue_policy(
        mode=resolved_mode,
        queue_commands=queue_commands,
        preflight=preflight,
        settings=resolved_settings,
    )
    reason_summary["queue_policy"] = queue_policy

    reconcile_status = None
    latest_reconcile: dict[str, Any] | None = None
    try:
        if resolved_settings.live_sim_reconcile_enabled:
            reconcile = reconcile_live_sim(connection, settings=resolved_settings)
            latest_reconcile = reconcile.to_dict()
            reconcile_status = reconcile.status
        else:
            warnings.append("LIVE_SIM_RECONCILE_ENABLED is false; reconcile stage skipped.")
            latest_reconcile = get_latest_live_sim_reconcile(connection)
            reconcile_status = (
                None if latest_reconcile is None else str(latest_reconcile.get("status"))
            )
    except Exception as exc:
        errors.append(_stage_error("reconcile", exc))
        latest_reconcile = get_latest_live_sim_reconcile(connection)
        reconcile_status = None if latest_reconcile is None else str(latest_reconcile.get("status"))
    stages["reconcile"] = latest_reconcile

    post_reconcile_blocks_buy = bool(
        latest_reconcile
        and resolved_mode.includes_buy
        and _reconcile_blocks_new_buy(latest_reconcile)
    )
    if post_reconcile_blocks_buy:
        reason = "post_reconcile: latest reconcile blocks new BUY."
        reason_summary["blocking_reasons"].append(reason)
        queue_policy["buy_commands_allowed"] = False

    cancel_result = None
    cancel_queue = (
        queue_policy["base_commands_allowed"]
        and queue_policy["lifecycle_commands_allowed"]
        and resolved_settings.live_sim_operating_max_cancel_commands_per_cycle > 0
    )
    try:
        cancel_result = run_live_sim_cancel_unfilled_once(
            connection,
            settings=budgeted_settings,
            dry_run=not cancel_queue,
            queue_commands=cancel_queue,
            limit=limit,
        )
        stages["cancel"] = cancel_result.to_dict()
    except Exception as exc:
        errors.append(_stage_error("cancel", exc))
        stages["cancel"] = {"status": "ERROR", "error": str(exc)}

    exit_result = None
    exit_queue = (
        queue_policy["base_commands_allowed"]
        and queue_policy["lifecycle_commands_allowed"]
        and resolved_settings.live_sim_operating_max_exit_commands_per_cycle > 0
    )
    try:
        exit_result = run_live_sim_exit_once(
            connection,
            settings=budgeted_settings,
            dry_run=not exit_queue,
            queue_commands=exit_queue,
            limit=limit,
        )
        stages["exit"] = exit_result.to_dict()
    except Exception as exc:
        errors.append(_stage_error("exit", exc))
        stages["exit"] = {"status": "ERROR", "error": str(exc)}

    try:
        theme_result = rebuild_theme_leadership(
            connection,
            trade_date=trade_date,
            write_candidate_sources=resolved_settings.theme_leadership_write_candidate_sources,
            settings=resolved_settings,
        )
        stages["theme_leadership"] = theme_result.to_dict(include_members=False)
    except Exception as exc:
        errors.append(_stage_error("theme_leadership", exc))
        stages["theme_leadership"] = {"status": "ERROR", "error": str(exc)}

    try:
        entry_result = evaluate_entry_timing(
            connection,
            trade_date=trade_date,
            limit=limit,
            settings=resolved_settings,
        )
        stages["entry_timing"] = entry_result.to_dict()
    except Exception as exc:
        errors.append(_stage_error("entry_timing", exc))
        stages["entry_timing"] = {"status": "ERROR", "error": str(exc)}

    ai_run_status = None
    if resolved_include_ai:
        try:
            ai_result = score_ai_candidates(
                connection,
                trade_date=trade_date,
                limit=limit,
                allow_external=resolved_settings.ai_external_llm_allow_network,
                settings=resolved_settings,
            )
            stages["ai_advisory"] = ai_result.to_dict()
            ai_run_status = ai_result.status
        except Exception as exc:
            errors.append(_stage_error("ai_advisory", exc))
            stages["ai_advisory"] = {"status": "ERROR", "error": str(exc)}
            ai_run_status = "ERROR"
    else:
        stages["ai_advisory"] = {
            "status": "SKIPPED",
            "advisory_only": True,
            "no_order_side_effects": True,
        }

    buy_result = None
    reprice_result = None
    buy_queue = (
        queue_policy["base_commands_allowed"]
        and queue_policy["buy_commands_allowed"]
        and not post_reconcile_blocks_buy
        and resolved_settings.live_sim_operating_max_buy_commands_per_cycle > 0
    )
    if resolved_mode.includes_buy and not post_reconcile_blocks_buy:
        remaining_buy_commands = resolved_settings.live_sim_operating_max_buy_commands_per_cycle
        if resolved_settings.live_sim_reprice_enabled:
            try:
                reprice_result = run_live_sim_reprice_once(
                    connection,
                    settings=budgeted_settings,
                    dry_run=not buy_queue,
                    queue_commands=buy_queue,
                    limit=remaining_buy_commands,
                )
                stages["reprice"] = reprice_result.to_dict()
                remaining_buy_commands = max(
                    remaining_buy_commands - int(reprice_result.command_count or 0),
                    0,
                )
            except Exception as exc:
                errors.append(_stage_error("reprice", exc))
                stages["reprice"] = {"status": "ERROR", "error": str(exc)}
        if buy_queue and remaining_buy_commands <= 0:
            stages["buy"] = {
                "status": "SKIPPED",
                "reason": "reprice_consumed_buy_budget",
                "command_count": 0,
                "live_sim_only": True,
                "live_real_allowed": False,
            }
        else:
            buy_settings = (
                budgeted_settings
                if remaining_buy_commands
                >= resolved_settings.live_sim_operating_max_buy_commands_per_cycle
                else replace(
                    budgeted_settings,
                    live_sim_order_plan_max_commands_per_run=min(
                        budgeted_settings.live_sim_order_plan_max_commands_per_run,
                        max(remaining_buy_commands, 0),
                    ),
                )
            )
            try:
                buy_result = run_live_sim_pilot_pipeline_once(
                    connection,
                    settings=buy_settings,
                    trade_date=trade_date,
                    limit=limit,
                    queue_commands=buy_queue,
                )
                stages["buy"] = buy_result.to_dict()
            except Exception as exc:
                errors.append(_stage_error("buy", exc))
                stages["buy"] = {"status": "ERROR", "error": str(exc)}
    else:
        stages["buy"] = {
            "status": "SKIPPED",
            "reason": "mode_disallows_buy"
            if not resolved_mode.includes_buy
            else "reconcile_blocks_new_buy",
            "command_count": 0,
            "live_sim_only": True,
            "live_real_allowed": False,
        }

    no_buy_status = None
    if resolved_include_no_buy:
        try:
            no_buy_snapshot = build_no_buy_sentinel_snapshot(
                connection,
                settings=resolved_settings,
                trade_date=trade_date,
                manual=True,
                limit=limit,
                include_ai=resolved_include_ai,
                write_snapshot=resolved_settings.no_buy_sentinel_write_snapshots,
            )
            no_buy_payload = no_buy_snapshot.to_dict()
            stages["no_buy_sentinel"] = no_buy_payload
            no_buy_status = no_buy_payload["status"]
        except Exception as exc:
            errors.append(_stage_error("no_buy_sentinel", exc))
            stages["no_buy_sentinel"] = {"status": "ERROR", "error": str(exc)}
            no_buy_status = "ERROR"
    else:
        stages["no_buy_sentinel"] = {
            "status": "SKIPPED",
            "read_only": True,
            "no_order_side_effects": True,
        }

    counts = _stage_counts(
        buy_result=buy_result,
        reprice_result=reprice_result,
        cancel_result=cancel_result,
        exit_result=exit_result,
    )
    command_total = (
        counts["buy_command_count"]
        + counts["cancel_command_count"]
        + counts["exit_command_count"]
    )
    if preflight.status is PreflightStatus.BLOCK and command_total:
        errors.append(
            {
                "stage": "command_policy",
                "error": "Commands were created despite preflight BLOCK.",
            }
        )
    reason_summary["command_counts"] = {
        "buy": counts["buy_command_count"],
        "reprice_buy": counts["reprice_command_count"],
        "cancel": counts["cancel_command_count"],
        "exit": counts["exit_command_count"],
        "total": command_total,
    }
    operator_summary = _operator_summary(
        mode=resolved_mode,
        queue_commands=queue_commands,
        preflight=preflight,
        queue_policy=queue_policy,
        counts=counts,
        stages=stages,
        warnings=warnings,
        errors=errors,
    )
    status = _run_status(preflight, errors, post_reconcile_blocks_buy)
    result = LiveSimOperatingRunResult(
        run_id=run_id,
        trade_date=trade_date,
        mode=resolved_mode,
        queue_commands=bool(queue_commands),
        preflight=preflight,
        status=status,
        buy_evaluated_count=counts["buy_evaluated_count"],
        buy_command_count=counts["buy_command_count"],
        cancel_candidate_count=counts["cancel_candidate_count"],
        cancel_command_count=counts["cancel_command_count"],
        exit_signal_count=counts["exit_signal_count"],
        exit_command_count=counts["exit_command_count"],
        reconcile_status=reconcile_status,
        no_buy_status=no_buy_status,
        ai_run_status=ai_run_status,
        warnings=tuple(_dedupe(warnings)),
        errors=tuple(errors),
        reason_summary=reason_summary,
        stages=stages,
        operator_summary=operator_summary,
        created_at=created_at,
    )
    if resolved_settings.live_sim_operating_write_runs:
        try:
            save_live_sim_operating_run(connection, result)
            connection.commit()
        except sqlite3.Error as exc:
            errors.append(_stage_error("save_operating_run", exc))
            connection.rollback()
            result = replace(
                result,
                errors=tuple(errors),
                status=_run_status(preflight, errors, post_reconcile_blocks_buy),
            )
    return result


def build_live_sim_operator_status(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
    trade_date: str | None = None,
) -> dict[str, Any]:
    resolved_settings = settings or load_settings()
    latest_run = get_latest_live_sim_operating_run(connection)
    preflight = run_live_sim_preflight(
        connection,
        mode=resolved_settings.live_sim_operating_default_mode,
        queue_commands=False,
        trade_date=trade_date,
        include_ai=resolved_settings.live_sim_operating_include_ai,
        include_no_buy=resolved_settings.live_sim_operating_include_no_buy,
        settings=resolved_settings,
    )
    live_sim_status = get_live_sim_status(connection, settings=resolved_settings)
    return {
        "latest_run": latest_run,
        "current_operating_mode": resolved_settings.live_sim_operating_default_mode,
        "preflight": preflight.to_dict(),
        "preflight_status": preflight.status.value,
        "warnings": preflight.warnings,
        "blocking_reasons": preflight.blocking_reasons,
        "command_counts_last_run": _command_counts_from_run(latest_run),
        "no_buy_status": None if latest_run is None else latest_run.get("no_buy_status"),
        "ai_advisory_status": None if latest_run is None else latest_run.get("ai_run_status"),
        "reconcile_status": None if latest_run is None else latest_run.get("reconcile_status"),
        "open_order_count": live_sim_status.get("open_order_count", 0),
        "open_position_count": live_sim_status.get("open_position_count", 0),
        "live_sim_only": True,
        "live_real_allowed": False,
        "read_only": True,
        "no_order_side_effects": True,
        "run_buttons_available": False,
        "order_controls_available": False,
        "settings_controls_available": False,
    }


def save_live_sim_operating_run(
    connection: sqlite3.Connection,
    result: LiveSimOperatingRunResult,
) -> None:
    connection.execute(
        """
        INSERT INTO live_sim_operating_runs (
            run_id,
            trade_date,
            mode,
            queue_commands,
            preflight_status,
            status,
            buy_evaluated_count,
            buy_command_count,
            cancel_candidate_count,
            cancel_command_count,
            exit_signal_count,
            exit_command_count,
            reconcile_status,
            no_buy_status,
            ai_run_status,
            reason_summary_json,
            warnings_json,
            errors_json,
            created_at,
            live_sim_only,
            live_real_allowed
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 0)
        """,
        (
            result.run_id,
            result.trade_date,
            result.mode.value,
            1 if result.queue_commands else 0,
            result.preflight.status.value,
            result.status,
            result.buy_evaluated_count,
            result.buy_command_count,
            result.cancel_candidate_count,
            result.cancel_command_count,
            result.exit_signal_count,
            result.exit_command_count,
            result.reconcile_status,
            result.no_buy_status,
            result.ai_run_status,
            _json_dumps(result.reason_summary),
            _json_dumps(list(result.warnings)),
            _json_dumps(list(result.errors)),
            result.created_at,
        ),
    )


def list_live_sim_operating_runs(
    connection: sqlite3.Connection,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT *
        FROM live_sim_operating_runs
        ORDER BY created_at DESC, run_id DESC
        LIMIT ?
        """,
        (_bounded_limit(limit),),
    ).fetchall()
    return [_operating_run_row_to_dict(row) for row in rows]


def get_latest_live_sim_operating_run(
    connection: sqlite3.Connection,
) -> dict[str, Any] | None:
    rows = list_live_sim_operating_runs(connection, limit=1)
    return rows[0] if rows else None


def _queue_policy(
    *,
    mode: OperatingMode,
    queue_commands: bool,
    preflight: LiveSimPreflightResult,
    settings: Settings,
) -> dict[str, Any]:
    preflight_allows_queue = (
        preflight.status is PreflightStatus.PASS
        if settings.live_sim_operating_require_preflight_pass_for_queue
        else preflight.status is not PreflightStatus.BLOCK
    )
    base_commands_allowed = bool(
        queue_commands
        and settings.live_sim_operating_cycle_enabled
        and preflight_allows_queue
        and not mode.observes_only
    )
    buy_allowed = bool(base_commands_allowed and mode.includes_buy)
    lifecycle_allowed = bool(base_commands_allowed and mode.includes_lifecycle_commands)
    return {
        "queue_commands_requested": bool(queue_commands),
        "require_preflight_pass_for_queue": (
            settings.live_sim_operating_require_preflight_pass_for_queue
        ),
        "preflight_allows_queue": preflight_allows_queue,
        "base_commands_allowed": base_commands_allowed,
        "buy_commands_allowed": buy_allowed,
        "lifecycle_commands_allowed": lifecycle_allowed,
        "observe_cycle_blocks_commands": mode.observes_only,
        "protect_only_blocks_buy": mode is OperatingMode.PROTECT_ONLY,
        "live_sim_only": True,
        "live_real_allowed": False,
    }


def _settings_with_command_budgets(settings: Settings) -> Settings:
    updates: dict[str, int] = {}
    if settings.live_sim_operating_max_buy_commands_per_cycle > 0:
        updates["live_sim_order_plan_max_commands_per_run"] = min(
            settings.live_sim_order_plan_max_commands_per_run,
            settings.live_sim_operating_max_buy_commands_per_cycle,
        )
    if settings.live_sim_operating_max_cancel_commands_per_cycle > 0:
        updates["live_sim_cancel_max_commands_per_run"] = min(
            settings.live_sim_cancel_max_commands_per_run,
            settings.live_sim_operating_max_cancel_commands_per_cycle,
        )
    if settings.live_sim_operating_max_exit_commands_per_cycle > 0:
        updates["live_sim_exit_max_commands_per_run"] = min(
            settings.live_sim_exit_max_commands_per_run,
            settings.live_sim_operating_max_exit_commands_per_cycle,
        )
    return replace(settings, **updates) if updates else settings


def _command_budget(settings: Settings) -> dict[str, int]:
    return {
        "buy": settings.live_sim_operating_max_buy_commands_per_cycle,
        "cancel": settings.live_sim_operating_max_cancel_commands_per_cycle,
        "exit": settings.live_sim_operating_max_exit_commands_per_cycle,
    }


def _mode_policy(mode: OperatingMode) -> dict[str, bool]:
    return {
        "observe_only": mode.observes_only,
        "buy_pipeline_allowed": mode.includes_buy,
        "lifecycle_commands_allowed": mode.includes_lifecycle_commands,
        "protect_only_blocks_buy": mode is OperatingMode.PROTECT_ONLY,
    }


def _stage_counts(
    *,
    buy_result: Any,
    reprice_result: Any,
    cancel_result: Any,
    exit_result: Any,
) -> dict[str, int]:
    reprice_evaluated = int(getattr(reprice_result, "evaluated_count", 0) or 0)
    reprice_commands = int(getattr(reprice_result, "command_count", 0) or 0)
    return {
        "buy_evaluated_count": int(getattr(buy_result, "evaluated_count", 0) or 0)
        + reprice_evaluated,
        "buy_command_count": int(getattr(buy_result, "command_count", 0) or 0)
        + reprice_commands,
        "reprice_evaluated_count": reprice_evaluated,
        "reprice_command_count": reprice_commands,
        "cancel_candidate_count": int(getattr(cancel_result, "evaluated_count", 0) or 0),
        "cancel_command_count": int(getattr(cancel_result, "command_count", 0) or 0),
        "exit_signal_count": int(getattr(exit_result, "evaluated_count", 0) or 0),
        "exit_command_count": int(getattr(exit_result, "command_count", 0) or 0),
    }


def _operator_summary(
    *,
    mode: OperatingMode,
    queue_commands: bool,
    preflight: LiveSimPreflightResult,
    queue_policy: Mapping[str, Any],
    counts: Mapping[str, int],
    stages: Mapping[str, Any],
    warnings: Sequence[str],
    errors: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    no_buy = _dict_or_empty(stages.get("no_buy_sentinel"))
    ai = _dict_or_empty(stages.get("ai_advisory"))
    return {
        "mode": mode.value,
        "queue_commands_requested": bool(queue_commands),
        "preflight_status": preflight.status.value,
        "warnings": list(_dedupe(warnings)),
        "blocking_reasons": list(preflight.blocking_reasons),
        "open_order_count": preflight.counts.get("open_order_count", 0),
        "open_position_count": preflight.counts.get("open_position_count", 0),
        "buy_evaluated_count": counts["buy_evaluated_count"],
        "buy_command_count": counts["buy_command_count"],
        "cancel_candidate_count": counts["cancel_candidate_count"],
        "cancel_command_count": counts["cancel_command_count"],
        "exit_signal_count": counts["exit_signal_count"],
        "exit_command_count": counts["exit_command_count"],
        "no_buy_summary": {
            "status": no_buy.get("status"),
            "no_buy_detected": no_buy.get("no_buy_detected"),
            "plan_ready_count": no_buy.get("plan_ready_count"),
            "buy_eligible_count": no_buy.get("buy_eligible_count"),
        },
        "ai_summary": {
            "status": ai.get("status"),
            "selected_count": ai.get("selected_count"),
            "advisory_only": True,
            "no_order_side_effects": True,
        },
        "queue_policy": dict(queue_policy),
        "errors": normalize_value(list(errors)),
        "next_operator_checks": _next_operator_checks(preflight, queue_policy, counts),
        "live_sim_only": True,
        "live_real_allowed": False,
        "no_order_side_effects": (
            counts["buy_command_count"]
            + counts["cancel_command_count"]
            + counts["exit_command_count"]
            == 0
        ),
    }


def _next_operator_checks(
    preflight: LiveSimPreflightResult,
    queue_policy: Mapping[str, Any],
    counts: Mapping[str, int],
) -> list[str]:
    checks = [
        "Dashboard에는 실행/매수/매도/취소 버튼이 없으므로 CLI/API run result를 확인합니다.",
        "LIVE_REAL 관련 flag는 계속 false인지 확인합니다.",
    ]
    if preflight.blocking_reasons:
        checks.append("preflight blocking_reasons를 먼저 해소합니다.")
    if preflight.warnings:
        checks.append("warnings는 queue policy와 함께 검토합니다.")
    if not queue_policy.get("base_commands_allowed"):
        checks.append("이번 cycle은 command queue가 허용되지 않았습니다.")
    if counts.get("buy_command_count", 0) or counts.get("cancel_command_count", 0) or counts.get(
        "exit_command_count",
        0,
    ):
        checks.append("Gateway command queue와 broker mock ack/fill 이벤트를 확인합니다.")
    return checks


def _run_status(
    preflight: LiveSimPreflightResult,
    errors: Sequence[Mapping[str, Any]],
    post_reconcile_blocks_buy: bool,
) -> str:
    if errors:
        return "COMPLETED_WITH_ERRORS"
    if preflight.status is PreflightStatus.BLOCK or post_reconcile_blocks_buy:
        return "BLOCKED"
    if preflight.status is PreflightStatus.WARN:
        return "COMPLETED_WITH_WARNINGS"
    return "COMPLETED"


def _command_counts_from_run(run: Mapping[str, Any] | None) -> dict[str, int]:
    if run is None:
        return {"buy": 0, "cancel": 0, "exit": 0, "total": 0}
    buy = int(run.get("buy_command_count") or 0)
    cancel = int(run.get("cancel_command_count") or 0)
    exit_count = int(run.get("exit_command_count") or 0)
    return {"buy": buy, "cancel": cancel, "exit": exit_count, "total": buy + cancel + exit_count}


def _operating_run_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    item = {key: row[key] for key in row.keys()}
    item["queue_commands"] = bool(item["queue_commands"])
    item["reason_summary"] = _json_object(item.pop("reason_summary_json"))
    item["warnings"] = _json_array(item.pop("warnings_json"))
    item["errors"] = _json_array(item.pop("errors_json"))
    item["live_sim_only"] = bool(item["live_sim_only"])
    item["live_real_allowed"] = bool(item["live_real_allowed"])
    item["broker_order_path"] = "LIVE_SIM_ONLY"
    item["real_order_allowed"] = False
    item["read_only"] = True
    return item


def _stage_error(stage: str, exc: Exception) -> dict[str, Any]:
    return {"stage": stage, "error": str(exc)}


def _reconcile_blocks_new_buy(reconcile: Mapping[str, Any]) -> bool:
    return bool(reconcile.get("blocking_new_buy")) or (
        str(reconcile.get("status") or "").upper() == "RECONCILE_MISMATCH"
        and int(reconcile.get("mismatch_count") or 0) > 0
    )


def _dict_or_empty(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _dedupe(values: Sequence[str]) -> list[str]:
    return [*dict.fromkeys(str(value) for value in values if str(value).strip())]


def _json_dumps(value: object) -> str:
    return json.dumps(
        normalize_value(value),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


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
        return []
    return loaded if isinstance(loaded, list) else []


def _bounded_limit(limit: int) -> int:
    return min(max(int(limit), 1), 500)
