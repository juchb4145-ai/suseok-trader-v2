from __future__ import annotations

import re
import sqlite3
from typing import Any

from domain.broker.utils import datetime_to_wire, utc_now
from storage.event_retention import get_event_retention_status
from storage.event_store import get_gateway_status_values
from storage.gateway_order_broker_boundary import get_order_broker_boundary_status
from storage.live_sim_order_plan_uniqueness import (
    get_live_sim_order_plan_uniqueness_status,
)
from storage.projection_watermarks import get_projection_watermark_status

from services.ai_advisory.storage import build_status as build_ai_advisory_status
from services.candidate_service import get_candidate_status
from services.config import Settings, load_settings
from services.dashboard_service import build_safety_section
from services.entry_timing.service import get_entry_timing_status
from services.live_sim.execution_lifecycle_status import (
    build_live_sim_execution_lifecycle_status,
)
from services.live_sim.live_sim_service import (
    get_latest_live_sim_reconcile,
    get_live_sim_status,
)
from services.market_data_service import get_market_data_status
from services.market_index_service import get_market_index_status
from services.market_index_tr_bootstrap import get_market_index_tr_bootstrap_status
from services.market_regime_service import get_market_regime_status
from services.market_scan_service import get_market_scan_status
from services.operator.no_buy_sentinel import (
    build_no_buy_sentinel_snapshot,
    get_latest_no_buy_sentinel_snapshot,
)
from services.pipeline_coherency import build_pipeline_coherency_status
from services.realtime_subscription import build_realtime_subscription_plan
from services.runtime.append_only_readiness import (
    build_append_only_readiness_status,
)
from services.runtime.evaluation_run_guard import get_runtime_execution_lock_status
from services.runtime.gateway_live_sim_lifecycle_routing import (
    build_live_sim_lifecycle_cutover_status,
)
from services.runtime.gateway_market_index_routing import (
    get_latest_market_index_append_only_routing_status,
)
from services.runtime.gateway_market_regime_routing import (
    get_latest_market_regime_append_only_routing_status,
)
from services.runtime.gateway_market_scan_routing import (
    get_latest_market_scan_append_only_routing_status,
)
from services.runtime.incremental_evaluation import get_incremental_evaluation_status
from services.runtime.live_sim_pilot_pipeline import list_live_sim_pilot_runs
from services.runtime.market_index_projection_reconcile import (
    get_latest_market_index_projection_reconcile,
)
from services.runtime.market_regime_projection_reconcile import (
    get_latest_market_regime_projection_reconcile,
)
from services.runtime.market_scan_projection_reconcile import (
    get_latest_market_scan_projection_reconcile,
)
from services.runtime.projection_replay import get_projection_replay_status
from services.theme_coherency import build_theme_coherency_status
from services.theme_diagnostics import build_theme_data_wait_diagnostics
from services.theme_service import get_theme_status

_PUBLIC_LIFECYCLE_STATUS_KEYS = (
    "status",
    "qualification_status",
    "qualification_reason_codes",
    "canonical_status",
    "canonical_reason_codes",
    "canonical",
    "classification_counts",
    "raw_error_count",
    "mirror_lifecycle_count",
    "logical_subject_count",
    "active_lifecycle_blocker_count",
    "historical_runtime_status_audit_count",
    "manual_review_blocker_count",
    "active_reconcile_blocker_count",
    "historical_reconcile_event_count",
    "reconcile_manual_review_count",
    "effective_blocker_count",
    "mirrored_pair_count",
    "mirror_consistent",
    "reconcile",
    "code_filter",
    "code_filter_diagnostic_only",
    "limit",
    "offset",
    "returned_count",
    "full_count",
    "has_more",
    "next_offset",
    "pagination",
    "inventory_count_consistent",
    "inventory_digest",
    "scanned_inventory_digest",
    "ending_inventory_digest",
    "read_only",
    "observe_only",
    "no_order_side_effects",
    "real_order_allowed",
)
_PUBLIC_LIFECYCLE_ITEM_KEYS = (
    "subject_id",
    "subject_fingerprint",
    "classification",
    "reason_codes",
    "mirror_status",
    "error_surface_count",
    "lifecycle_surface_count",
    "created_at",
    "code",
    "inner_event_type",
    "payload_sha256",
    "event_metadata_consistent",
    "identifier_free",
)
_PUBLIC_LIFECYCLE_INNER_EVENT_TYPES = frozenset({"heartbeat", "orderability"})
_SENSITIVE_LIFECYCLE_VALUE_PATTERNS = (
    re.compile(r"(?i)(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9]{20,}"),
    re.compile(r"(?i)github_pat_[A-Za-z0-9_]{20,}"),
    re.compile(r"(?i)\bsk-[A-Za-z0-9_-]{20,}\b"),
    re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._~+/-]{16,}=*"),
    re.compile(r"(?i)(?:acct|account|계좌)[_.:@/-]?\d{6,16}"),
    re.compile(r"(?<![A-Za-z0-9])\d{8,16}(?![A-Za-z0-9])"),
)
_HYPHENATED_ACCOUNT_PATTERN = re.compile(
    r"(?<!\d)\d{3,6}-\d{2,6}(?:-\d{1,6})?(?!\d)"
)
_ISO_DATE_PATTERN = re.compile(r"(?<!\d)\d{4}-\d{2}-\d{2}(?!\d)")


def build_operator_status(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
    trade_date: str | None = None,
    include_no_buy_rebuild: bool = False,
) -> dict[str, Any]:
    resolved_settings = settings or load_settings()
    generated_at = datetime_to_wire(utc_now())
    gateway_values = get_gateway_status_values(connection)
    live_sim_status = get_live_sim_status(connection, resolved_settings)
    latest_reconcile = get_latest_live_sim_reconcile(connection)
    no_buy_latest = get_latest_no_buy_sentinel_snapshot(connection, trade_date=trade_date)
    if no_buy_latest is None and include_no_buy_rebuild:
        no_buy_latest = build_no_buy_sentinel_snapshot(
            connection,
            settings=resolved_settings,
            trade_date=trade_date,
            manual=True,
            write_snapshot=False,
        ).to_dict()

    realtime_plan = build_realtime_subscription_plan(
        connection,
        settings=resolved_settings,
        trade_date=trade_date,
        queue_commands=False,
    ).to_dict()
    theme_diagnostics = build_theme_data_wait_diagnostics(
        connection,
        settings=resolved_settings,
        limit=20,
    )
    execution_lifecycle = build_live_sim_execution_lifecycle_public_status(
        connection,
        limit=1,
        offset=0,
    )
    for page_key in (
        "items",
        "code_filter",
        "limit",
        "offset",
        "returned_count",
        "has_more",
        "next_offset",
        "pagination",
    ):
        execution_lifecycle.pop(page_key, None)
    execution_lifecycle["summary_only"] = True

    return {
        "generated_at": generated_at,
        "read_only": True,
        "no_order_side_effects": True,
        "execution_controls_available": False,
        "core": {
            "api_health": "ok",
            "trading_profile": resolved_settings.trading_profile.value,
            "trading_mode": resolved_settings.trading_mode.value,
            "token_required": bool(resolved_settings.trading_core_token),
            "database_path": str(resolved_settings.trading_db_path),
        },
        "safety": build_safety_section(resolved_settings),
        "gateway": {
            "last_event_received_at": gateway_values.get("last_event_received_at"),
            "last_heartbeat_at": gateway_values.get("last_heartbeat_at"),
            "gateway_orderable": gateway_values.get("gateway_orderable"),
            "command_queue_healthy": gateway_values.get("command_queue_healthy"),
            "account_mode": gateway_values.get("account_mode"),
            "broker_env": gateway_values.get("broker_env"),
            "server_mode": gateway_values.get("server_mode"),
        },
        "runtime_execution_locks": get_runtime_execution_lock_status(connection),
        "live_sim_order_plan_uniqueness": (
            get_live_sim_order_plan_uniqueness_status(connection)
        ),
        "order_broker_boundaries": get_order_broker_boundary_status(connection),
        "append_only_readiness": build_append_only_readiness_status(
            connection,
            settings=resolved_settings,
        ),
        "live_sim_lifecycle_consumer": build_live_sim_lifecycle_cutover_status(
            connection,
            settings=resolved_settings,
        ),
        "incremental_evaluation": get_incremental_evaluation_status(
            connection,
            settings=resolved_settings,
        ),
        "pipeline_coherency": build_pipeline_coherency_status(
            connection,
            max_age_sec=resolved_settings.entry_timing_stale_max_seconds,
            limit=100,
        ),
        "theme_coherency": build_theme_coherency_status(
            connection,
            settings=resolved_settings,
            limit=10,
        ),
        "projection_replay": get_projection_replay_status(),
        "projection_watermarks": get_projection_watermark_status(connection),
        "event_retention": get_event_retention_status(
            connection,
            settings=resolved_settings,
        ),
        "live_sim": {
            "status": live_sim_status,
            "kill_switch": live_sim_status.get("kill_switch"),
            "account_mode": live_sim_status.get("account_mode"),
            "broker_env": live_sim_status.get("broker_env"),
            "server_mode": live_sim_status.get("server_mode"),
            "pilot": {
                "enabled": resolved_settings.live_sim_pilot_pipeline_enabled,
                "auto_queue_command": resolved_settings.live_sim_pilot_auto_queue_command,
                "order_plan_routing_enabled": (
                    resolved_settings.live_sim_order_plan_routing_enabled
                ),
                "latest_runs": list_live_sim_pilot_runs(connection, limit=5),
            },
            "execution_lifecycle": execution_lifecycle,
            "reconcile_latest": latest_reconcile,
        },
        "market_data": get_market_data_status(connection, settings=resolved_settings),
        "market_index": {
            "status": get_market_index_status(connection, settings=resolved_settings),
            "tr_bootstrap": get_market_index_tr_bootstrap_status(
                connection,
                settings=resolved_settings,
            ),
            "projection_reconcile": get_latest_market_index_projection_reconcile(
                connection
            ),
            "append_only_routing": (
                get_latest_market_index_append_only_routing_status(
                    connection,
                    settings=resolved_settings,
                )
            ),
        },
        "market_index_tr_bootstrap": get_market_index_tr_bootstrap_status(
            connection,
            settings=resolved_settings,
        ),
        "market_regime": {
            "status": get_market_regime_status(
                connection,
                settings=resolved_settings,
            ),
            "projection_reconcile": get_latest_market_regime_projection_reconcile(
                connection
            ),
            "append_only_routing": (
                get_latest_market_regime_append_only_routing_status(
                    connection,
                    settings=resolved_settings,
                )
            ),
        },
        "market_scan": {
            "status": get_market_scan_status(
                connection,
                settings=resolved_settings,
            ),
            "projection_reconcile": get_latest_market_scan_projection_reconcile(
                connection
            ),
            "append_only_routing": (
                get_latest_market_scan_append_only_routing_status(
                    connection,
                    settings=resolved_settings,
                )
            ),
        },
        "theme_leadership": get_theme_status(connection, settings=resolved_settings),
        "theme_data_wait_diagnostics": {
            "state_quality_distribution": theme_diagnostics["state_quality_distribution"],
            "data_wait_reason_counts": theme_diagnostics["data_wait_reason_counts"],
            "root_cause_summary": theme_diagnostics["root_cause_summary"],
            "subscription_capacity": theme_diagnostics["subscription_capacity"],
            "top_data_wait_themes": theme_diagnostics["top_data_wait_themes"],
        },
        "realtime_subscription_warmup": {
            "status": realtime_plan.get("status"),
            "counts": realtime_plan.get("counts", {}),
            "pending_register_count": int(
                (realtime_plan.get("counts") or {}).get("planned_register_count") or 0
            ),
            "registered_count": int(
                (realtime_plan.get("counts") or {}).get("registered_count") or 0
            ),
            "missing_subscription_count": int(
                (realtime_plan.get("counts") or {}).get(
                    "missing_candidate_subscription_count",
                    0,
                )
                or 0
            ),
            "queue_commands": False,
            "read_only": True,
            "observe_only": True,
        },
        "candidates": get_candidate_status(connection, settings=resolved_settings),
        "entry_timing": get_entry_timing_status(connection, settings=resolved_settings),
        "ai_advisory": build_ai_advisory_status(connection, settings=resolved_settings),
        "no_buy_sentinel": no_buy_latest,
    }


def build_live_sim_execution_lifecycle_public_status(
    connection: sqlite3.Connection,
    *,
    limit: int = 100,
    offset: int = 0,
    code: str | None = None,
) -> dict[str, Any]:
    status = build_live_sim_execution_lifecycle_status(
        connection,
        limit=limit,
        offset=offset,
        code=code,
    )
    if not isinstance(status, dict):
        raise TypeError("execution lifecycle classifier must return a mapping")
    missing_status_keys = set(_PUBLIC_LIFECYCLE_STATUS_KEYS).difference(status)
    if missing_status_keys:
        raise ValueError("execution lifecycle classifier status contract is incomplete")
    public = {
        key: _sanitize_lifecycle_public_value(status.get(key), key=key)
        for key in _PUBLIC_LIFECYCLE_STATUS_KEYS
        if key in status
    }
    items = status.get("items")
    if not isinstance(items, list):
        raise TypeError("execution lifecycle classifier items must be a list")
    if (
        not all(isinstance(item, dict) for item in items)
        or any(
            set(_PUBLIC_LIFECYCLE_ITEM_KEYS).difference(item)
            for item in items
            if isinstance(item, dict)
        )
        or status.get("returned_count") != len(items)
    ):
        raise ValueError("execution lifecycle classifier item contract is invalid")
    public["items"] = [
        {
            key: _sanitize_lifecycle_public_value(item.get(key), key=key)
            for key in _PUBLIC_LIFECYCLE_ITEM_KEYS
            if key in item
        }
        for item in items
        if isinstance(item, dict)
    ]
    public["raw_payload_exposed"] = False
    public["account_identifier_exposed"] = False
    public["token_exposed"] = False
    return public


def _sanitize_lifecycle_public_value(value: Any, *, key: str) -> Any:
    lowered = key.lower()
    if lowered == "inner_event_type":
        return (
            value
            if isinstance(value, str)
            and value in _PUBLIC_LIFECYCLE_INNER_EVENT_TYPES
            else None
        )
    if (
        lowered in {"payload", "raw_payload", "error_message"}
        or "account" in lowered
        or "token" in lowered
        or "password" in lowered
        or "secret" in lowered
    ) and not lowered.endswith("_sha256"):
        return "[REDACTED]"
    if isinstance(value, dict):
        return {
            str(child_key): _sanitize_lifecycle_public_value(
                child_value,
                key=str(child_key),
            )
            for child_key, child_value in value.items()
        }
    if isinstance(value, list):
        return [
            _sanitize_lifecycle_public_value(item, key=key)
            for item in value
        ]
    if isinstance(value, tuple):
        return [
            _sanitize_lifecycle_public_value(item, key=key)
            for item in value
        ]
    if isinstance(value, str):
        if any(
            pattern.search(value) is not None
            for pattern in _SENSITIVE_LIFECYCLE_VALUE_PATTERNS
        ):
            return "[REDACTED]"
        without_iso_dates = _ISO_DATE_PATTERN.sub("", value)
        if _HYPHENATED_ACCOUNT_PATTERN.search(without_iso_dates) is not None:
            return "[REDACTED]"
    return value
