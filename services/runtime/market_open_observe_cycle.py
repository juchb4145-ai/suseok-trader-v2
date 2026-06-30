from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from domain.broker.utils import datetime_to_wire, new_message_id, normalize_value, utc_now
from domain.strategy.status import StrategyObservationStatus
from storage.gateway_command_store import get_command_status_counts

from services.candidate_service import (
    get_candidate_status,
    rebuild_candidates_from_observations,
)
from services.config import Settings, load_settings
from services.entry_timing.service import evaluate_entry_timing, get_entry_timing_status
from services.live_sim.live_sim_service import get_live_sim_status
from services.realtime_subscription import build_realtime_subscription_plan
from services.risk_gate import evaluate_risk_observations, get_risk_status
from services.runtime.preflight import OperatingMode, run_live_sim_preflight
from services.strategy_engine import evaluate_candidates, get_strategy_status
from services.theme_leadership import rebuild_theme_leadership
from services.theme_service import calculate_all_theme_snapshots, get_theme_status

STAGE_PASS = "PASS"
STAGE_WARN = "WARN"
STAGE_BLOCK = "BLOCK"
STAGE_UNKNOWN = "UNKNOWN"


@dataclass(frozen=True, kw_only=True)
class ObserveCycleStageResult:
    stage: str
    status: str
    reason_codes: Sequence[str] = field(default_factory=tuple)
    summary: str = ""
    counts: Mapping[str, Any] = field(default_factory=dict)
    details: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "stage": self.stage,
            "status": self.status,
            "reason_codes": list(self.reason_codes),
            "summary": self.summary,
            "counts": normalize_value(dict(self.counts)),
            "details": normalize_value(dict(self.details)),
        }


@dataclass(frozen=True, kw_only=True)
class MarketOpenObserveCycleRunResult:
    run_id: str
    trade_date: str | None
    status: str
    stages: Mapping[str, ObserveCycleStageResult]
    command_counts_before: Mapping[str, int]
    command_counts_after: Mapping[str, int]
    send_order_count_before: int
    send_order_count_after: int
    warnings: Sequence[str] = field(default_factory=tuple)
    errors: Sequence[Mapping[str, Any]] = field(default_factory=tuple)
    created_at: str = field(default_factory=lambda: datetime_to_wire(utc_now()))
    observe_only: bool = True
    no_order_side_effects: bool = True
    live_real_allowed: bool = False

    @property
    def send_order_delta(self) -> int:
        return self.send_order_count_after - self.send_order_count_before

    def to_dict(self) -> dict[str, Any]:
        command_counts_after = dict(self.command_counts_after)
        command_counts_before = dict(self.command_counts_before)
        return {
            "run_id": self.run_id,
            "trade_date": self.trade_date,
            "status": self.status,
            "stage_summary": {
                key: value.to_dict() for key, value in self.stages.items()
            },
            "command_counts_before": command_counts_before,
            "command_counts_after": command_counts_after,
            "send_order_count_before": self.send_order_count_before,
            "send_order_count_after": self.send_order_count_after,
            "send_order_delta": self.send_order_delta,
            "gateway_command_delta": _command_delta(command_counts_before, command_counts_after),
            "warnings": list(self.warnings),
            "errors": normalize_value(list(self.errors)),
            "created_at": self.created_at,
            "observe_only": True,
            "not_order_intent": True,
            "no_order_side_effects": self.no_order_side_effects,
            "live_real_allowed": False,
            "real_order_allowed": False,
            "queue_commands": False,
            "order_controls_available": False,
        }


def run_market_open_observe_cycle_once(
    connection: sqlite3.Connection,
    *,
    trade_date: str | None = None,
    limit: int | None = None,
    settings: Settings | None = None,
    write_run: bool = True,
) -> MarketOpenObserveCycleRunResult:
    resolved_settings = settings or load_settings()
    run_id = new_message_id("market_open_observe_cycle_run")
    created_at = datetime_to_wire(utc_now())
    command_counts_before = _command_counts(connection)
    send_order_before = _send_order_count(connection)
    stages: dict[str, ObserveCycleStageResult] = {}
    warnings: list[str] = []
    errors: list[dict[str, Any]] = []

    theme_snapshot_payload: dict[str, Any] = {}
    leadership_payload: dict[str, Any] = {}
    try:
        theme_before = get_theme_status(connection, settings=resolved_settings)
        snapshot_result = calculate_all_theme_snapshots(
            connection,
            settings=resolved_settings,
        )
        leadership_result = rebuild_theme_leadership(
            connection,
            trade_date=trade_date,
            write_candidate_sources=resolved_settings.theme_leadership_write_candidate_sources,
            settings=resolved_settings,
        )
        theme_after = get_theme_status(connection, settings=resolved_settings)
        theme_snapshot_payload = snapshot_result.to_dict()
        leadership_payload = leadership_result.to_dict(include_members=False)
        stages["Theme"] = _theme_stage(
            theme_before=theme_before,
            theme_after=theme_after,
            snapshot_result=theme_snapshot_payload,
            leadership_result=leadership_payload,
        )
    except Exception as exc:
        error = _stage_error("Theme", exc)
        errors.append(error)
        stages["Theme"] = _blocked_stage("Theme", "THEME_SNAPSHOT_NOT_BUILT", str(exc))

    try:
        realtime_subscription_plan = build_realtime_subscription_plan(
            connection,
            trade_date=trade_date,
            settings=resolved_settings,
            queue_commands=False,
        )
        stages["RealtimeSubscription"] = _realtime_subscription_stage(
            realtime_subscription_plan.to_dict()
        )
    except Exception as exc:
        error = _stage_error("RealtimeSubscription", exc)
        errors.append(error)
        stages["RealtimeSubscription"] = _blocked_stage(
            "RealtimeSubscription",
            "REALTIME_SUBSCRIPTION_PLAN_FAILED",
            str(exc),
        )

    try:
        candidate_result = rebuild_candidates_from_observations(
            connection,
            trade_date=trade_date,
            settings=resolved_settings,
        )
        candidate_status = get_candidate_status(connection, settings=resolved_settings)
        stages["Candidate"] = _candidate_stage(
            result=candidate_result.to_dict(),
            status=candidate_status,
        )
    except Exception as exc:
        error = _stage_error("Candidate", exc)
        errors.append(error)
        stages["Candidate"] = _blocked_stage("Candidate", "CANDIDATE_REBUILD_NOT_RUN", str(exc))

    try:
        strategy_result = evaluate_candidates(
            connection,
            trade_date=trade_date,
            candidate_state=None,
            limit=limit,
            settings=resolved_settings,
        )
        strategy_status = get_strategy_status(connection, resolved_settings)
        stages["Strategy"] = _strategy_stage(
            result=strategy_result.to_dict(),
            status=strategy_status,
        )
    except Exception as exc:
        error = _stage_error("Strategy", exc)
        errors.append(error)
        stages["Strategy"] = _blocked_stage("Strategy", "STRATEGY_EVALUATE_NOT_RUN", str(exc))

    try:
        risk_result = evaluate_risk_observations(
            connection,
            trade_date=trade_date,
            strategy_status=StrategyObservationStatus.MATCHED_OBSERVATION,
            limit=limit,
            settings=resolved_settings,
        )
        risk_status = get_risk_status(connection, resolved_settings)
        stages["Risk"] = _risk_stage(
            result=risk_result.to_dict(),
            status=risk_status,
        )
    except Exception as exc:
        error = _stage_error("Risk", exc)
        errors.append(error)
        stages["Risk"] = _blocked_stage("Risk", "RISK_EVALUATE_NOT_RUN", str(exc))

    try:
        entry_result = evaluate_entry_timing(
            connection,
            trade_date=trade_date,
            limit=limit,
            write_order_plan_drafts=True,
            settings=resolved_settings,
        )
        entry_status = get_entry_timing_status(connection, settings=resolved_settings)
        stages["EntryTiming"] = _entry_timing_stage(
            result=entry_result.to_dict(),
            status=entry_status,
        )
    except Exception as exc:
        error = _stage_error("EntryTiming", exc)
        errors.append(error)
        stages["EntryTiming"] = _blocked_stage("EntryTiming", "ENTRY_TIMING_NO_INPUT", str(exc))

    try:
        live_sim_status = get_live_sim_status(connection, settings=resolved_settings)
        live_sim_preflight = run_live_sim_preflight(
            connection,
            settings=resolved_settings,
            mode=OperatingMode.OBSERVE_CYCLE,
            queue_commands=False,
            trade_date=trade_date,
            include_ai=False,
            include_no_buy=False,
        )
        stages["LiveSim"] = _live_sim_stage(
            live_sim_status=live_sim_status,
            preflight=live_sim_preflight.to_dict(),
        )
    except Exception as exc:
        error = _stage_error("LiveSim", exc)
        errors.append(error)
        stages["LiveSim"] = _blocked_stage("LiveSim", "LIVE_SIM_DISABLED_EXPECTED", str(exc))

    command_counts_after = _command_counts(connection)
    send_order_after = _send_order_count(connection)
    send_order_delta = send_order_after - send_order_before
    if send_order_delta:
        errors.append(
            {
                "stage": "CommandSafety",
                "error": "send_order command was created during observe cycle",
                "send_order_delta": send_order_delta,
            }
        )
        stages["CommandSafety"] = ObserveCycleStageResult(
            stage="CommandSafety",
            status=STAGE_BLOCK,
            reason_codes=("ORDER_COMMAND_ZERO_EXPECTED",),
            summary="Observe cycle created send_order commands.",
            counts={"send_order_delta": send_order_delta},
        )
    else:
        stages["CommandSafety"] = ObserveCycleStageResult(
            stage="CommandSafety",
            status=STAGE_PASS,
            reason_codes=("ORDER_COMMAND_ZERO_EXPECTED",),
            summary="No send_order GatewayCommand was created.",
            counts={"send_order_delta": 0},
        )

    warnings.extend(_stage_warnings(stages))
    status = _overall_status(stages, errors)
    result = MarketOpenObserveCycleRunResult(
        run_id=run_id,
        trade_date=trade_date,
        status=status,
        stages=stages,
        command_counts_before=command_counts_before,
        command_counts_after=command_counts_after,
        send_order_count_before=send_order_before,
        send_order_count_after=send_order_after,
        warnings=tuple(_dedupe(warnings)),
        errors=tuple(errors),
        created_at=created_at,
        no_order_side_effects=(send_order_delta == 0 and not errors),
    )
    if write_run:
        save_market_open_observe_cycle_run(connection, result)
        connection.commit()
    return result


def save_market_open_observe_cycle_run(
    connection: sqlite3.Connection,
    result: MarketOpenObserveCycleRunResult,
) -> None:
    connection.execute(
        """
        INSERT INTO market_open_observe_cycle_runs (
            run_id,
            trade_date,
            status,
            stage_summary_json,
            command_counts_json,
            warnings_json,
            errors_json,
            created_at,
            observe_only,
            no_order_side_effects,
            live_real_allowed
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, 0)
        """,
        (
            result.run_id,
            result.trade_date,
            result.status,
            _json_dumps(result.to_dict()["stage_summary"]),
            _json_dumps(
                {
                    "before": dict(result.command_counts_before),
                    "after": dict(result.command_counts_after),
                    "send_order_count_before": result.send_order_count_before,
                    "send_order_count_after": result.send_order_count_after,
                    "send_order_delta": result.send_order_delta,
                }
            ),
            _json_dumps(list(result.warnings)),
            _json_dumps(list(result.errors)),
            result.created_at,
            1 if result.no_order_side_effects else 0,
        ),
    )


def list_market_open_observe_cycle_runs(
    connection: sqlite3.Connection,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT *
        FROM market_open_observe_cycle_runs
        ORDER BY created_at DESC, run_id DESC
        LIMIT ?
        """,
        (_bounded_limit(limit),),
    ).fetchall()
    return [_run_row_to_dict(row) for row in rows]


def get_latest_market_open_observe_cycle_run(
    connection: sqlite3.Connection,
) -> dict[str, Any] | None:
    rows = list_market_open_observe_cycle_runs(connection, limit=1)
    return rows[0] if rows else None


def _theme_stage(
    *,
    theme_before: Mapping[str, Any],
    theme_after: Mapping[str, Any],
    snapshot_result: Mapping[str, Any],
    leadership_result: Mapping[str, Any],
) -> ObserveCycleStageResult:
    reason_codes: list[str] = []
    status = STAGE_PASS
    member_count = int(theme_after.get("member_count") or 0)
    active_theme_count = int(theme_after.get("active_theme_count") or 0)
    snapshot_count = int(snapshot_result.get("snapshot_count") or 0)
    error_count = int(snapshot_result.get("error_count") or 0)
    watchset_count = len(_dict_or_empty(leadership_result.get("watchset")).get("items", []))
    if member_count <= 0 or active_theme_count <= 0:
        status = STAGE_BLOCK
        reason_codes.append("THEME_MEMBERSHIP_EMPTY")
    elif snapshot_count <= 0:
        status = STAGE_WARN
        reason_codes.append("THEME_SNAPSHOT_NOT_BUILT")
    if error_count:
        status = STAGE_BLOCK
        reason_codes.append("MARKET_PROJECTION_ERROR")
    summary = (
        f"members={member_count}, snapshots={snapshot_count}, watchset={watchset_count}"
    )
    return ObserveCycleStageResult(
        stage="Theme",
        status=status,
        reason_codes=tuple(_dedupe(reason_codes)),
        summary=summary,
        counts={
            "theme_count_before": theme_before.get("theme_count"),
            "theme_count_after": theme_after.get("theme_count"),
            "member_count": member_count,
            "active_theme_count": active_theme_count,
            "snapshot_count": snapshot_count,
            "watchset_count": watchset_count,
            "error_count": error_count,
        },
        details={
            "snapshot_result": snapshot_result,
            "leadership_result": {
                "status": leadership_result.get("status"),
                "watchset_count": watchset_count,
                "candidate_apply_result": leadership_result.get("candidate_apply_result"),
            },
        },
    )


def _candidate_stage(
    *,
    result: Mapping[str, Any],
    status: Mapping[str, Any],
) -> ObserveCycleStageResult:
    reason_codes: list[str] = []
    stage_status = STAGE_PASS
    active_count = int(status.get("active_candidate_count") or 0)
    source_count = int(result.get("source_event_count") or 0)
    error_count = int(result.get("error_count") or 0)
    if error_count:
        stage_status = STAGE_BLOCK
        reason_codes.append("CANDIDATE_REBUILD_NOT_RUN")
    elif active_count <= 0:
        stage_status = STAGE_WARN
        reason_codes.append("CANDIDATE_EMPTY" if source_count else "CANDIDATE_DATA_WAIT")
    return ObserveCycleStageResult(
        stage="Candidate",
        status=stage_status,
        reason_codes=tuple(_dedupe(reason_codes)),
        summary=f"active={active_count}, source_events={source_count}",
        counts={
            "candidate_count": status.get("candidate_count"),
            "active_candidate_count": active_count,
            "source_event_count": source_count,
            "context_refreshed_count": result.get("context_refreshed_count"),
            "error_count": error_count,
        },
        details={"result": result, "status": status},
    )


def _realtime_subscription_stage(plan: Mapping[str, Any]) -> ObserveCycleStageResult:
    counts = _dict_or_empty(plan.get("counts"))
    planned_register_count = int(counts.get("planned_register_count") or 0)
    planned_remove_count = int(counts.get("planned_remove_count") or 0)
    status = STAGE_PASS
    reason_codes: list[str] = []
    if str(plan.get("status") or "").upper() == "DISABLED":
        status = STAGE_WARN
        reason_codes.append("REALTIME_SUBSCRIPTION_DISABLED")
    elif planned_register_count <= 0 and planned_remove_count <= 0:
        reason_codes.append("REALTIME_SUBSCRIPTION_NOOP")
    return ObserveCycleStageResult(
        stage="RealtimeSubscription",
        status=status,
        reason_codes=tuple(_dedupe(reason_codes)),
        summary=(
            f"register={planned_register_count}, remove={planned_remove_count}, "
            f"registered={counts.get('already_registered_count', 0)}"
        ),
        counts={
            "planned_register_count": planned_register_count,
            "planned_remove_count": planned_remove_count,
            "already_registered_count": counts.get("already_registered_count", 0),
            "anchor_count": counts.get("anchor_count", 0),
            "condition_count": counts.get("condition_count", 0),
            "candidate_count": counts.get("candidate_count", 0),
            "theme_watchset_count": counts.get("theme_watchset_count", 0),
            "queue_commands": False,
        },
        details={
            "plan": plan,
            "read_only": True,
            "observe_only": True,
            "queue_commands": False,
            "no_order_side_effects": True,
        },
    )


def _strategy_stage(
    *,
    result: Mapping[str, Any],
    status: Mapping[str, Any],
) -> ObserveCycleStageResult:
    reason_codes: list[str] = []
    stage_status = STAGE_PASS
    evaluated_count = int(result.get("evaluated_count") or 0)
    error_count = int(result.get("error_count") or 0)
    if str(result.get("status") or "").upper() == "DISABLED":
        stage_status = STAGE_WARN
        reason_codes.append("STRATEGY_EVALUATE_NOT_RUN")
    elif error_count:
        stage_status = STAGE_BLOCK
        reason_codes.append("STRATEGY_EVALUATE_NOT_RUN")
    elif evaluated_count <= 0:
        stage_status = STAGE_WARN
        reason_codes.append("STRATEGY_EMPTY")
    return ObserveCycleStageResult(
        stage="Strategy",
        status=stage_status,
        reason_codes=tuple(_dedupe(reason_codes)),
        summary=(
            f"evaluated={evaluated_count}, matched={result.get('matched_observation_count', 0)}"
        ),
        counts={
            "candidate_count": result.get("candidate_count"),
            "evaluated_count": evaluated_count,
            "matched_observation_count": result.get("matched_observation_count"),
            "latest_observation_count": status.get("latest_observation_count"),
            "error_count": error_count,
        },
        details={"result": result, "status": status},
    )


def _risk_stage(
    *,
    result: Mapping[str, Any],
    status: Mapping[str, Any],
) -> ObserveCycleStageResult:
    reason_codes: list[str] = []
    stage_status = STAGE_PASS
    evaluated_count = int(result.get("evaluated_count") or 0)
    error_count = int(result.get("error_count") or 0)
    if str(result.get("status") or "").upper() == "DISABLED":
        stage_status = STAGE_WARN
        reason_codes.append("RISK_EVALUATE_NOT_RUN")
    elif error_count:
        stage_status = STAGE_BLOCK
        reason_codes.append("RISK_EVALUATE_NOT_RUN")
    elif evaluated_count <= 0:
        stage_status = STAGE_WARN
        reason_codes.append("RISK_EMPTY")
    return ObserveCycleStageResult(
        stage="Risk",
        status=stage_status,
        reason_codes=tuple(_dedupe(reason_codes)),
        summary=f"evaluated={evaluated_count}, pass={result.get('observe_pass_count', 0)}",
        counts={
            "strategy_observation_count": result.get("strategy_observation_count"),
            "evaluated_count": evaluated_count,
            "observe_pass_count": result.get("observe_pass_count"),
            "block_count": result.get("block_count"),
            "latest_observation_count": status.get("latest_observation_count"),
            "error_count": error_count,
        },
        details={"result": result, "status": status},
    )


def _entry_timing_stage(
    *,
    result: Mapping[str, Any],
    status: Mapping[str, Any],
) -> ObserveCycleStageResult:
    reason_codes: list[str] = []
    stage_status = STAGE_PASS
    evaluated_count = int(result.get("evaluated_count") or 0)
    plan_ready_count = int(result.get("plan_ready_count") or 0)
    draft_count = len(result.get("order_plan_drafts") or [])
    error_count = int(result.get("error_count") or 0)
    if str(result.get("status") or "").upper() == "DISABLED":
        stage_status = STAGE_WARN
        reason_codes.append("ENTRY_TIMING_NO_INPUT")
    elif error_count:
        stage_status = STAGE_BLOCK
        reason_codes.append("ENTRY_TIMING_NO_INPUT")
    elif evaluated_count <= 0:
        stage_status = STAGE_WARN
        reason_codes.append("ENTRY_TIMING_NO_INPUT")
    elif draft_count <= 0:
        stage_status = STAGE_WARN
        reason_codes.append("ORDER_PLAN_EMPTY")
    return ObserveCycleStageResult(
        stage="EntryTiming",
        status=stage_status,
        reason_codes=tuple(_dedupe(reason_codes)),
        summary=f"evaluated={evaluated_count}, drafts={draft_count}, ready={plan_ready_count}",
        counts={
            "candidate_count": result.get("candidate_count"),
            "evaluated_count": evaluated_count,
            "order_plan_draft_count": draft_count,
            "plan_ready_count": plan_ready_count,
            "latest_plan_count": status.get("latest_plan_count"),
            "error_count": error_count,
        },
        details={"result": result, "status": status},
    )


def _live_sim_stage(
    *,
    live_sim_status: Mapping[str, Any],
    preflight: Mapping[str, Any],
) -> ObserveCycleStageResult:
    reason_codes = ["ORDER_COMMAND_ZERO_EXPECTED"]
    if not bool(live_sim_status.get("enabled")):
        reason_codes.append("LIVE_SIM_DISABLED_EXPECTED")
    if bool(live_sim_status.get("kill_switch")):
        reason_codes.append("LIVE_SIM_KILL_SWITCH_ON_EXPECTED")
    summary = (
        "read-only preflight="
        f"{preflight.get('status')}, enabled={live_sim_status.get('enabled')}, "
        f"kill={live_sim_status.get('kill_switch')}"
    )
    return ObserveCycleStageResult(
        stage="LiveSim",
        status=STAGE_PASS,
        reason_codes=tuple(_dedupe(reason_codes)),
        summary=summary,
        counts={
            "intent_count": live_sim_status.get("intent_count"),
            "order_count": live_sim_status.get("order_count"),
            "open_order_count": live_sim_status.get("open_order_count"),
            "open_position_count": live_sim_status.get("open_position_count"),
            "preflight_status": preflight.get("status"),
        },
        details={
            "status": live_sim_status,
            "preflight": preflight,
            "read_only": True,
            "queue_commands": False,
        },
    )


def _blocked_stage(stage: str, reason_code: str, message: str) -> ObserveCycleStageResult:
    return ObserveCycleStageResult(
        stage=stage,
        status=STAGE_BLOCK,
        reason_codes=(reason_code,),
        summary=message,
        details={"error": message},
    )


def _overall_status(
    stages: Mapping[str, ObserveCycleStageResult],
    errors: Sequence[Mapping[str, Any]],
) -> str:
    statuses = [stage.status for stage in stages.values()]
    if errors:
        return "COMPLETED_WITH_ERRORS"
    if STAGE_BLOCK in statuses:
        return "COMPLETED_WITH_BLOCKS"
    if STAGE_WARN in statuses:
        return "COMPLETED_WITH_WARNINGS"
    if STAGE_UNKNOWN in statuses:
        return "COMPLETED_WITH_UNKNOWN"
    return "COMPLETED"


def _stage_warnings(stages: Mapping[str, ObserveCycleStageResult]) -> list[str]:
    warnings = []
    for stage in stages.values():
        if stage.status in {STAGE_WARN, STAGE_BLOCK, STAGE_UNKNOWN}:
            reason = ",".join(stage.reason_codes) or stage.status
            warnings.append(f"{stage.stage}: {reason}")
    return warnings


def _command_counts(connection: sqlite3.Connection) -> dict[str, int]:
    return {key: int(value) for key, value in get_command_status_counts(connection).items()}


def _send_order_count(connection: sqlite3.Connection) -> int:
    row = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM gateway_commands
        WHERE LOWER(command_type) = 'send_order'
        """
    ).fetchone()
    return int(row["count"] if row else 0)


def _command_delta(before: Mapping[str, int], after: Mapping[str, int]) -> dict[str, int]:
    keys = sorted(set(before) | set(after))
    return {key: int(after.get(key, 0)) - int(before.get(key, 0)) for key in keys}


def _stage_error(stage: str, exc: Exception) -> dict[str, Any]:
    return {"stage": stage, "error": str(exc)}


def _run_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    item = {key: row[key] for key in row.keys()}
    item["stage_summary"] = _json_object(item.pop("stage_summary_json"))
    item["command_counts"] = _json_object(item.pop("command_counts_json"))
    item["warnings"] = _json_array(item.pop("warnings_json"))
    item["errors"] = _json_array(item.pop("errors_json"))
    item["observe_only"] = bool(item["observe_only"])
    item["no_order_side_effects"] = bool(item["no_order_side_effects"])
    item["live_real_allowed"] = bool(item["live_real_allowed"])
    item["real_order_allowed"] = False
    item["queue_commands"] = False
    return item


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


def _dict_or_empty(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _dedupe(values: Sequence[str]) -> list[str]:
    return [*dict.fromkeys(str(value) for value in values if str(value).strip())]


def _bounded_limit(limit: int) -> int:
    return min(max(int(limit), 1), 500)
