from __future__ import annotations

import json
import sqlite3
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import timedelta
from typing import Any

from domain.broker.commands import GatewayCommand
from domain.broker.utils import (
    BrokerValidationError,
    normalize_value,
    parse_timestamp,
    utc_now,
    validate_stock_code,
)
from domain.candidate.state import CandidateState
from storage.event_store import get_gateway_status_values
from storage.gateway_command_store import EnqueueCommandResult, enqueue_command

from services.config import Settings, candidate_timezone, load_settings

REALTIME_SUBSCRIPTION_SOURCE = "realtime_subscription_planner"
_REGISTER_ACTION = "register"
_REMOVE_ACTION = "remove"
_ALLOWED_ACTIONS = frozenset({_REGISTER_ACTION, _REMOVE_ACTION})
_ACTIVE_CANDIDATE_STATES = (
    CandidateState.WATCHING.value,
    CandidateState.CONTEXT_READY.value,
    CandidateState.DATA_WAIT.value,
)
_THEME_STATE_PRIORITY = {
    "LEADING": 0,
    "SPREADING": 1,
    "LEADER_ONLY": 2,
    "WATCH": 3,
    "DATA_WAIT": 4,
    "WEAK": 5,
    "FADING": 6,
}
_THEME_ROLE_PRIORITY = {
    "LEADER_CANDIDATE": 0,
    "CO_LEADER_CANDIDATE": 1,
    "FOLLOWER_CANDIDATE": 2,
    "LEADER": 0,
    "CO_LEADER": 1,
    "FOLLOWER": 2,
    "UNKNOWN": 5,
    "STALE": 9,
}
_EXCHANGE_SCHEMA_WARNING = (
    "market_ticks_latest is keyed by code only. Do not mix KRX/NXT in one "
    "MarketData projection until exchange/session-aware schema support is added."
)


@dataclass(frozen=True, kw_only=True)
class RealtimeSubscriptionPlan:
    status: str
    trade_date: str
    exchange: str
    registered_realtime_codes: Sequence[str] = field(default_factory=tuple)
    anchor_codes: Sequence[str] = field(default_factory=tuple)
    register_targets: Sequence[Mapping[str, Any]] = field(default_factory=tuple)
    remove_targets: Sequence[Mapping[str, Any]] = field(default_factory=tuple)
    keep_targets: Sequence[Mapping[str, Any]] = field(default_factory=tuple)
    excluded: Sequence[Mapping[str, Any]] = field(default_factory=tuple)
    near_miss: Sequence[Mapping[str, Any]] = field(default_factory=tuple)
    stale_registered_codes: Sequence[Mapping[str, Any]] = field(default_factory=tuple)
    missing_candidate_subscriptions: Sequence[Mapping[str, Any]] = field(default_factory=tuple)
    reason_summary: Mapping[str, int] = field(default_factory=dict)
    counts: Mapping[str, Any] = field(default_factory=dict)
    metadata: Mapping[str, Any] = field(default_factory=dict)
    warnings: Sequence[str] = field(default_factory=tuple)
    command_results: Sequence[Mapping[str, Any]] = field(default_factory=tuple)
    command_count: int = 0
    queue_commands: bool = False
    read_only: bool = True
    observe_only: bool = True
    no_order_side_effects: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "trade_date": self.trade_date,
            "exchange": self.exchange,
            "registered_realtime_codes": list(self.registered_realtime_codes),
            "anchor_codes": list(self.anchor_codes),
            "register_targets": normalize_value(list(self.register_targets)),
            "remove_targets": normalize_value(list(self.remove_targets)),
            "keep_targets": normalize_value(list(self.keep_targets)),
            "excluded": normalize_value(list(self.excluded)),
            "near_miss": normalize_value(list(self.near_miss)),
            "stale_registered_codes": normalize_value(list(self.stale_registered_codes)),
            "missing_candidate_subscriptions": normalize_value(
                list(self.missing_candidate_subscriptions)
            ),
            "reason_summary": dict(self.reason_summary),
            "counts": normalize_value(dict(self.counts)),
            "metadata": normalize_value(dict(self.metadata)),
            "warnings": list(self.warnings),
            "command_results": normalize_value(list(self.command_results)),
            "command_count": int(self.command_count),
            "queue_commands": bool(self.queue_commands),
            "read_only": bool(self.read_only),
            "observe_only": bool(self.observe_only),
            "no_order_side_effects": bool(self.no_order_side_effects),
            "not_order_intent": True,
            "no_order_controls": True,
            "order_controls_available": False,
            "real_order_allowed": False,
        }


class RealtimeSubscriptionPlanner:
    def __init__(self, *, settings: Settings | None = None) -> None:
        self.settings = settings or load_settings()

    def plan(
        self,
        connection: sqlite3.Connection,
        *,
        trade_date: str | None = None,
        registered_codes: Iterable[str] | None = None,
        manual_seed_codes: Iterable[str] | None = None,
        queue_commands: bool | None = None,
    ) -> RealtimeSubscriptionPlan:
        resolved_trade_date = trade_date or _trade_date(self.settings)
        exchange = self.settings.realtime_subscription_exchange
        queue_requested = (
            self.settings.realtime_subscription_queue_commands
            if queue_commands is None
            else bool(queue_commands)
        )
        anchors = _dedupe(_normalize_codes(self.settings.realtime_subscription_anchor_codes))
        manual_seeds = _dedupe(_normalize_codes(manual_seed_codes))
        registered = _dedupe(
            _normalize_codes(registered_codes)
            if registered_codes is not None
            else _registered_codes_from_gateway_status(connection)
        )
        registered_set = set(registered)
        reason_counter: Counter[str] = Counter()
        near_miss: list[dict[str, Any]] = []
        excluded: list[dict[str, Any]] = []
        warnings = [_EXCHANGE_SCHEMA_WARNING]
        if exchange in {"NXT", "ALL"}:
            warnings.append(
                "REALTIME_SUBSCRIPTION_EXCHANGE is not KRX; keep KRX/NXT separated "
                "until MarketData is exchange-aware."
            )

        if not self.settings.realtime_subscription_enabled:
            reason_counter["REALTIME_SUBSCRIPTION_DISABLED"] += 1
            return RealtimeSubscriptionPlan(
                status="DISABLED",
                trade_date=resolved_trade_date,
                exchange=exchange,
                registered_realtime_codes=registered,
                anchor_codes=anchors,
                reason_summary=dict(reason_counter),
                counts={
                    "planned_register_count": 0,
                    "planned_remove_count": 0,
                    "already_registered_count": len(registered),
                    "anchor_count": len(anchors),
                    "condition_count": 0,
                    "candidate_count": 0,
                    "theme_watchset_count": 0,
                },
                metadata=_metadata(exchange),
                warnings=warnings,
                queue_commands=queue_requested,
            )

        desired: dict[str, dict[str, Any]] = {}

        def add_target(
            code: str,
            *,
            name: str | None = None,
            priority: float,
            source_type: str,
            reason_codes: Sequence[str],
            details: Mapping[str, Any] | None = None,
        ) -> None:
            item = desired.setdefault(
                code,
                {
                    "code": code,
                    "name": name or code,
                    "priority": float(priority),
                    "source_types": [],
                    "reason_codes": [],
                    "details": [],
                },
            )
            if name and item.get("name") == code:
                item["name"] = name
            item["priority"] = max(float(item.get("priority") or 0.0), float(priority))
            if source_type not in item["source_types"]:
                item["source_types"].append(source_type)
            for reason in reason_codes:
                normalized_reason = str(reason).upper()
                if normalized_reason not in item["reason_codes"]:
                    item["reason_codes"].append(normalized_reason)
                reason_counter[normalized_reason] += 1
            if details:
                item["details"].append(dict(details))

        for code in anchors:
            add_target(
                code,
                priority=10_000,
                source_type="ANCHOR",
                reason_codes=("ANCHOR_CODE",),
                details={"source": "REALTIME_SUBSCRIPTION_ANCHOR_CODES"},
            )
        for code in manual_seeds:
            add_target(
                code,
                priority=9_800,
                source_type="MANUAL_SEED",
                reason_codes=("MANUAL_SEED",),
                details={"source": "manual_seed"},
            )

        condition_items = _recent_condition_enter_items(connection, settings=self.settings)
        for item in condition_items:
            add_target(
                item["code"],
                name=item["name"],
                priority=9_000,
                source_type="CONDITION_ENTER",
                reason_codes=("CONDITION_ENTER", "RECENT_CONDITION_ENTER"),
                details=item,
            )

        candidate_items = _active_candidate_items(connection, trade_date=resolved_trade_date)
        for item in candidate_items:
            add_target(
                item["code"],
                name=item["name"],
                priority=8_000,
                source_type="ACTIVE_CANDIDATE",
                reason_codes=("ACTIVE_CANDIDATE", f"CANDIDATE_{item['state']}"),
                details=item,
            )

        theme_items, theme_near_miss = _theme_subscription_items(
            connection,
            registered_codes=registered_set,
            desired_codes=set(desired),
            max_per_theme=self.settings.realtime_subscription_max_per_theme,
            max_theme_count=self.settings.theme_leadership_top_theme_count,
        )
        near_miss.extend(theme_near_miss)
        for item in theme_near_miss:
            for reason in item.get("reason_codes", []):
                reason_counter[str(reason).upper()] += 1
        for item in theme_items:
            add_target(
                item["code"],
                name=item["name"],
                priority=float(item["priority"]),
                source_type="THEME_WATCHSET",
                reason_codes=item["reason_codes"],
                details=item,
            )

        desired_items = sorted(
            desired.values(),
            key=lambda item: (-float(item["priority"]), item["code"]),
        )
        keep_targets = _keep_targets(registered, desired)
        stale_registered = _stale_registered_codes(
            connection,
            registered,
            desired_codes=set(desired),
            anchor_codes=set(anchors),
            settings=self.settings,
        )
        remove_targets = [
            item
            for item in stale_registered
            if "STALE_REGISTERED_REMOVE_CANDIDATE" in item.get("reason_codes", [])
        ]
        if remove_targets and not self.settings.realtime_subscription_allow_remove:
            for item in remove_targets:
                item["command_allowed"] = False
                item["reason_codes"] = _dedupe(
                    [*item.get("reason_codes", []), "REMOVE_COMMAND_DISABLED"]
                )
                reason_counter["REMOVE_COMMAND_DISABLED"] += 1

        capacity = max(self.settings.realtime_subscription_max_total - len(registered), 0)
        register_targets: list[dict[str, Any]] = []
        for item in desired_items:
            code = str(item["code"])
            if code in registered_set:
                reason_counter["ALREADY_REGISTERED"] += 1
                continue
            if len(register_targets) >= capacity:
                near = _target_view(
                    item,
                    action=_REGISTER_ACTION,
                    exchange=exchange,
                    extra_reasons=("MAX_TOTAL_REACHED",),
                )
                near_miss.append(near)
                reason_counter["MAX_TOTAL_REACHED"] += 1
                continue
            register_targets.append(
                _target_view(item, action=_REGISTER_ACTION, exchange=exchange)
            )

        missing_candidate_subscriptions = [
            {
                **item,
                "selected_for_register": item["code"]
                in {target["code"] for target in register_targets},
                "reason_codes": [
                    "MISSING_CANDIDATE_SUBSCRIPTION",
                    f"CANDIDATE_{item['state']}",
                ],
            }
            for item in candidate_items
            if item["code"] not in registered_set
        ]
        for _ in missing_candidate_subscriptions:
            reason_counter["MISSING_CANDIDATE_SUBSCRIPTION"] += 1

        status = "PLAN_READY"
        if not register_targets and not remove_targets:
            status = "NOOP"
        elif register_targets and not queue_requested:
            status = "PLAN_ONLY"
        counts = {
            "planned_register_count": len(register_targets),
            "planned_remove_count": len(remove_targets),
            "already_registered_count": len(keep_targets),
            "registered_count": len(registered),
            "anchor_count": len(anchors),
            "manual_seed_count": len(manual_seeds),
            "condition_count": len(condition_items),
            "candidate_count": len(candidate_items),
            "theme_watchset_count": len({str(item["code"]) for item in theme_items}),
            "stale_registered_count": len(stale_registered),
            "missing_candidate_subscription_count": len(missing_candidate_subscriptions),
            "max_total": self.settings.realtime_subscription_max_total,
            "max_per_theme": self.settings.realtime_subscription_max_per_theme,
        }
        return RealtimeSubscriptionPlan(
            status=status,
            trade_date=resolved_trade_date,
            exchange=exchange,
            registered_realtime_codes=registered,
            anchor_codes=anchors,
            register_targets=tuple(register_targets),
            remove_targets=tuple(remove_targets),
            keep_targets=tuple(keep_targets),
            excluded=tuple(excluded),
            near_miss=tuple(near_miss),
            stale_registered_codes=tuple(stale_registered),
            missing_candidate_subscriptions=tuple(missing_candidate_subscriptions),
            reason_summary=dict(reason_counter),
            counts=counts,
            metadata=_metadata(exchange),
            warnings=tuple(_dedupe(warnings)),
            queue_commands=queue_requested,
        )

    def run_once(
        self,
        connection: sqlite3.Connection,
        *,
        trade_date: str | None = None,
        registered_codes: Iterable[str] | None = None,
        manual_seed_codes: Iterable[str] | None = None,
        queue_commands: bool | None = None,
    ) -> RealtimeSubscriptionPlan:
        plan = self.plan(
            connection,
            trade_date=trade_date,
            registered_codes=registered_codes,
            manual_seed_codes=manual_seed_codes,
            queue_commands=queue_commands,
        )
        if not plan.queue_commands or plan.status == "DISABLED":
            return plan

        command_results: list[dict[str, Any]] = []
        command_count = 0
        for target in plan.register_targets:
            result = _queue_subscription_command(
                connection,
                action=_REGISTER_ACTION,
                target=target,
                trade_date=plan.trade_date,
                exchange=plan.exchange,
            )
            command_results.append(_enqueue_result_dict(result, target=target))
            if result.accepted:
                command_count += 1

        if self.settings.realtime_subscription_allow_remove:
            for target in plan.remove_targets:
                result = _queue_subscription_command(
                    connection,
                    action=_REMOVE_ACTION,
                    target=target,
                    trade_date=plan.trade_date,
                    exchange=plan.exchange,
                )
                command_results.append(_enqueue_result_dict(result, target=target))
                if result.accepted:
                    command_count += 1

        status = "QUEUED" if command_count else plan.status
        return replace(
            plan,
            status=status,
            command_results=tuple(command_results),
            command_count=command_count,
        )


def build_realtime_subscription_plan(
    connection: sqlite3.Connection,
    *,
    trade_date: str | None = None,
    registered_codes: Iterable[str] | None = None,
    manual_seed_codes: Iterable[str] | None = None,
    queue_commands: bool | None = None,
    settings: Settings | None = None,
) -> RealtimeSubscriptionPlan:
    return RealtimeSubscriptionPlanner(settings=settings).plan(
        connection,
        trade_date=trade_date,
        registered_codes=registered_codes,
        manual_seed_codes=manual_seed_codes,
        queue_commands=queue_commands,
    )


def run_realtime_subscription_once(
    connection: sqlite3.Connection,
    *,
    trade_date: str | None = None,
    registered_codes: Iterable[str] | None = None,
    manual_seed_codes: Iterable[str] | None = None,
    queue_commands: bool | None = None,
    settings: Settings | None = None,
) -> RealtimeSubscriptionPlan:
    return RealtimeSubscriptionPlanner(settings=settings).run_once(
        connection,
        trade_date=trade_date,
        registered_codes=registered_codes,
        manual_seed_codes=manual_seed_codes,
        queue_commands=queue_commands,
    )


def _recent_condition_enter_items(
    connection: sqlite3.Connection,
    *,
    settings: Settings,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT condition_id, condition_name, code, name, action, event_ts, received_at, source
        FROM market_condition_latest
        WHERE action = ?
        ORDER BY event_ts DESC, condition_id ASC, code ASC
        """,
        (settings.candidate_condition_action_enter,),
    ).fetchall()
    items: list[dict[str, Any]] = []
    for row in rows:
        code = _safe_code(row["code"])
        if code is None:
            continue
        age = _age_seconds(row["event_ts"])
        if age is not None and age > settings.realtime_subscription_stale_sec:
            continue
        items.append(
            {
                "code": code,
                "name": str(row["name"] or code),
                "condition_id": row["condition_id"],
                "condition_name": row["condition_name"],
                "action": row["action"],
                "event_ts": row["event_ts"],
                "received_at": row["received_at"],
                "source": row["source"],
                "age_sec": age,
            }
        )
    return items


def _active_candidate_items(
    connection: sqlite3.Connection,
    *,
    trade_date: str,
) -> list[dict[str, Any]]:
    placeholders = ",".join("?" for _ in _ACTIVE_CANDIDATE_STATES)
    rows = connection.execute(
        f"""
        SELECT
            candidate_instance_id,
            trade_date,
            code,
            name,
            state,
            last_seen_at,
            theme_id,
            theme_name,
            theme_state,
            theme_role
        FROM candidates
        WHERE trade_date = ? AND state IN ({placeholders})
        ORDER BY last_seen_at DESC, candidate_instance_id ASC
        """,
        (trade_date, *_ACTIVE_CANDIDATE_STATES),
    ).fetchall()
    items: list[dict[str, Any]] = []
    for row in rows:
        code = _safe_code(row["code"])
        if code is None:
            continue
        items.append(
            {
                "code": code,
                "name": str(row["name"] or code),
                "candidate_instance_id": row["candidate_instance_id"],
                "trade_date": row["trade_date"],
                "state": str(row["state"]).upper(),
                "last_seen_at": row["last_seen_at"],
                "theme_id": row["theme_id"],
                "theme_name": row["theme_name"],
                "theme_state": row["theme_state"],
                "theme_role": row["theme_role"],
            }
        )
    return items


def _theme_subscription_items(
    connection: sqlite3.Connection,
    *,
    registered_codes: set[str],
    desired_codes: set[str],
    max_per_theme: int,
    max_theme_count: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows = connection.execute(
        """
        SELECT
            s.theme_id,
            s.theme_name,
            s.state,
            s.calculated_at,
            s.leading_code,
            s.fresh_coverage_ratio,
            s.scan_coverage_ratio,
            s.flow_score,
            s.rising_ratio,
            s.total_trade_value,
            m.code,
            m.name,
            m.readiness_status,
            m.member_role,
            m.change_rate,
            m.cumulative_trade_value,
            tm.metadata_json AS membership_metadata_json
        FROM theme_latest_snapshots AS s
        JOIN theme_snapshot_members AS m ON m.snapshot_id = s.snapshot_id
        LEFT JOIN theme_members AS tm ON tm.theme_id = s.theme_id AND tm.code = m.code
        ORDER BY
            CASE s.state
                WHEN 'LEADING' THEN 0
                WHEN 'SPREADING' THEN 1
                WHEN 'LEADER_ONLY' THEN 2
                WHEN 'WATCH' THEN 3
                WHEN 'DATA_WAIT' THEN 4
                WHEN 'WEAK' THEN 5
                WHEN 'FADING' THEN 6
                ELSE 9
            END ASC,
            s.flow_score DESC,
            s.total_trade_value DESC,
            s.scan_coverage_ratio DESC,
            s.fresh_coverage_ratio DESC,
            s.theme_name ASC,
            m.code ASC
        """
    ).fetchall()
    items: list[dict[str, Any]] = []
    near_miss: list[dict[str, Any]] = []
    selected_per_theme: Counter[str] = Counter()
    theme_order: dict[str, int] = {}
    for row in rows:
        code = _safe_code(row["code"])
        if code is None:
            continue
        theme_id = str(row["theme_id"])
        if theme_id not in theme_order and len(theme_order) >= max_theme_count:
            continue
        theme_order.setdefault(theme_id, len(theme_order))
        state = str(row["state"] or "UNKNOWN").upper()
        role = str(row["member_role"] or "UNKNOWN").upper()
        readiness = str(row["readiness_status"] or "UNKNOWN").upper()
        if code not in registered_codes and code not in desired_codes:
            if selected_per_theme[theme_id] >= max_per_theme:
                near_miss.append(
                    {
                        "code": code,
                        "name": str(row["name"] or code),
                        "theme_id": theme_id,
                        "theme_name": row["theme_name"],
                        "theme_state": state,
                        "member_role": role,
                        "readiness_status": readiness,
                        "reason_codes": ["MAX_PER_THEME_REACHED"],
                    }
                )
                continue
            selected_per_theme[theme_id] += 1
        reason_codes = [
            "THEME_WATCHSET",
            f"THEME_{state}",
            f"THEME_MEMBER_{readiness}",
        ]
        if _float_or_zero(row["flow_score"]) > 0:
            reason_codes.append("THEME_FLOW_SCORE")
        if state == "DATA_WAIT":
            reason_codes.append("THEME_DATA_COVERAGE")
        if code == _safe_code(row["leading_code"]):
            reason_codes.append("THEME_LEADING_CODE")
        priority = (
            6_000
            - (_THEME_STATE_PRIORITY.get(state, 9) * 100)
            - theme_order[theme_id]
            - (_THEME_ROLE_PRIORITY.get(role, 6) * 5)
            + _float_or_zero(row["change_rate"])
        )
        items.append(
            {
                "code": code,
                "name": str(row["name"] or code),
                "theme_id": theme_id,
                "theme_name": row["theme_name"],
                "theme_state": state,
                "member_role": role,
                "readiness_status": readiness,
                "fresh_coverage_ratio": row["fresh_coverage_ratio"],
                "scan_coverage_ratio": row["scan_coverage_ratio"],
                "flow_score": row["flow_score"],
                "rising_ratio": row["rising_ratio"],
                "total_trade_value": row["total_trade_value"],
                "calculated_at": row["calculated_at"],
                "membership_rank": _membership_rank(row["membership_metadata_json"]),
                "priority": priority,
                "reason_codes": _dedupe(reason_codes),
            }
        )
    items.sort(
        key=lambda item: (
            -float(item["priority"]),
            item["membership_rank"],
            item["theme_name"],
            item["code"],
        )
    )
    return items, near_miss


def _keep_targets(
    registered: Sequence[str],
    desired: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for code in registered:
        base = dict(desired.get(code) or {})
        reason_codes = list(base.get("reason_codes") or [])
        if base:
            reason_codes.append("ALREADY_REGISTERED")
        else:
            reason_codes.append("REGISTERED_NOT_SELECTED")
        items.append(
            {
                "code": code,
                "name": base.get("name") or code,
                "action": "keep",
                "source_types": base.get("source_types") or ["CURRENT_REGISTRATION"],
                "reason_codes": _dedupe(reason_codes),
                "priority": base.get("priority", 0),
            }
        )
    return items


def _stale_registered_codes(
    connection: sqlite3.Connection,
    registered: Sequence[str],
    *,
    desired_codes: set[str],
    anchor_codes: set[str],
    settings: Settings,
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for code in registered:
        row = connection.execute(
            """
            SELECT code, name, event_ts, updated_at, quality_status
            FROM market_ticks_latest
            WHERE code = ?
            """,
            (code,),
        ).fetchone()
        reference_ts = row["event_ts"] if row is not None else None
        age = _age_seconds(reference_ts)
        missing = row is None
        stale = (
            missing
            or age is None
            or age > settings.realtime_subscription_remove_stale_after_sec
        )
        if not stale:
            continue
        reasons = ["REGISTERED_TICK_MISSING" if missing else "REGISTERED_TICK_STALE"]
        if code in anchor_codes:
            reasons.append("ANCHOR_STALE_KEEP")
        elif code in desired_codes:
            reasons.append("DESIRED_STALE_KEEP")
        else:
            reasons.append("STALE_REGISTERED_REMOVE_CANDIDATE")
        result.append(
            {
                "code": code,
                "name": row["name"] if row is not None else code,
                "action": _REMOVE_ACTION,
                "event_ts": reference_ts,
                "age_sec": age,
                "quality_status": row["quality_status"] if row is not None else "MISSING",
                "reason_codes": _dedupe(reasons),
                "command_allowed": bool(settings.realtime_subscription_allow_remove),
            }
        )
    return result


def _target_view(
    item: Mapping[str, Any],
    *,
    action: str,
    exchange: str,
    extra_reasons: Sequence[str] = (),
) -> dict[str, Any]:
    return {
        "code": item["code"],
        "name": item.get("name") or item["code"],
        "action": action,
        "exchange": exchange,
        "source_types": list(item.get("source_types") or []),
        "reason_codes": _dedupe([*item.get("reason_codes", []), *extra_reasons]),
        "priority": item.get("priority", 0),
        "details": normalize_value(list(item.get("details") or [])),
        "observe_only": True,
        "not_order_signal": True,
    }


def _queue_subscription_command(
    connection: sqlite3.Connection,
    *,
    action: str,
    target: Mapping[str, Any],
    trade_date: str,
    exchange: str,
) -> EnqueueCommandResult:
    if action not in _ALLOWED_ACTIONS:
        raise ValueError(f"unsupported realtime subscription action: {action}")
    code = validate_stock_code(target["code"])
    command_type = "register_realtime" if action == _REGISTER_ACTION else "remove_realtime"
    payload = {
        "code": code,
        "codes": [code],
        "exchange": exchange,
        "source": REALTIME_SUBSCRIPTION_SOURCE,
        "purpose": "observe_realtime_subscription",
        "observe_only": True,
        "not_order_signal": True,
        "no_order_side_effects": True,
        "reason_codes": list(target.get("reason_codes") or []),
        "metadata": {
            "trade_date": trade_date,
            "action": action,
            "source_types": list(target.get("source_types") or []),
            "market_data_schema_warning": _EXCHANGE_SCHEMA_WARNING,
        },
    }
    command = GatewayCommand(
        command_type=command_type,
        source=REALTIME_SUBSCRIPTION_SOURCE,
        payload=payload,
        idempotency_key=f"{REALTIME_SUBSCRIPTION_SOURCE}:{trade_date}:{exchange}:{action}:{code}",
    )
    return enqueue_command(
        connection,
        command,
        expires_at=utc_now() + timedelta(seconds=120),
    )


def _enqueue_result_dict(
    result: EnqueueCommandResult,
    *,
    target: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "code": target.get("code"),
        "action": target.get("action"),
        "accepted": result.accepted,
        "command_id": result.command_id,
        "status": result.status.value,
        "duplicate": result.duplicate,
        "error_message": result.error_message,
        "payload_hash": result.payload_hash,
    }


def _registered_codes_from_gateway_status(connection: sqlite3.Connection) -> list[str]:
    values = get_gateway_status_values(connection)
    return _parse_code_list_value(values.get("realtime_registered_codes"))


def _parse_code_list_value(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return []
        try:
            loaded = json.loads(stripped)
        except json.JSONDecodeError:
            loaded = stripped.split(",")
        if isinstance(loaded, str):
            return _normalize_codes([loaded])
        return _normalize_codes(loaded if isinstance(loaded, Iterable) else [loaded])
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes, bytearray)):
        return _normalize_codes(value)
    return []


def _metadata(exchange: str) -> dict[str, Any]:
    return {
        "exchange": exchange,
        "market_data_projection_key": "code",
        "market_data_schema_warning": _EXCHANGE_SCHEMA_WARNING,
        "todo": (
            "Make MarketData exchange/session-aware before mixed KRX/NXT realtime "
            "subscriptions are allowed in one projection."
        ),
    }


def _trade_date(settings: Settings) -> str:
    market_tz = candidate_timezone(settings.candidate_trade_date_timezone)
    return utc_now().astimezone(market_tz).date().isoformat()


def _age_seconds(value: object) -> float | None:
    if value is None:
        return None
    try:
        parsed = parse_timestamp(value, "timestamp")
    except (BrokerValidationError, ValueError):
        return None
    return max((utc_now() - parsed).total_seconds(), 0.0)


def _membership_rank(raw_metadata: object) -> int:
    try:
        metadata = json.loads(str(raw_metadata or "{}"))
    except json.JSONDecodeError:
        return 999_999
    for value in (
        metadata.get("rank"),
        (metadata.get("raw") or {}).get("naver_member_rank")
        if isinstance(metadata.get("raw"), Mapping)
        else None,
    ):
        try:
            rank = int(value)
        except (TypeError, ValueError):
            continue
        if rank > 0:
            return rank
    return 999_999


def _safe_code(value: object) -> str | None:
    try:
        return validate_stock_code(value)
    except (BrokerValidationError, ValueError):
        return None


def _normalize_codes(values: Iterable[object] | None) -> list[str]:
    if values is None:
        return []
    result: list[str] = []
    for value in values:
        code = _safe_code(value)
        if code is not None:
            result.append(code)
    return result


def _dedupe(values: Iterable[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = str(value).upper()
        if normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    return result


def _float_or_zero(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
