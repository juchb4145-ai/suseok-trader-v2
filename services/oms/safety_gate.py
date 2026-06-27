from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from typing import Any

from domain.broker.utils import normalize_payload
from domain.oms.reasons import DryRunRejectionReason

from services.config import Settings, TradingMode, load_settings


@dataclass(frozen=True, kw_only=True)
class SafetyGateResult:
    passed: bool
    status: str
    reason_codes: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    sidecar_review_only_confirmed: bool = True
    rca_review_only_confirmed: bool = True
    codex_prompt_review_only_confirmed: bool = True
    order_routing_disabled: bool = True
    live_flags_disabled: bool = True
    gateway_order_commands_disabled: bool = True
    openai_tools_disabled: bool = True
    order_tools_disabled: bool = True
    dashboard_order_controls_unavailable: bool = True
    safety_review_draft_exists: bool = False
    orders_enqueue_endpoint_absent: bool = True
    gateway_command_types_allowed: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "status": self.status,
            "reason_codes": list(self.reason_codes),
            "warnings": list(self.warnings),
            "sidecar_review_only_confirmed": self.sidecar_review_only_confirmed,
            "rca_review_only_confirmed": self.rca_review_only_confirmed,
            "codex_prompt_review_only_confirmed": self.codex_prompt_review_only_confirmed,
            "order_routing_disabled": self.order_routing_disabled,
            "live_flags_disabled": self.live_flags_disabled,
            "gateway_order_commands_disabled": self.gateway_order_commands_disabled,
            "openai_tools_disabled": self.openai_tools_disabled,
            "order_tools_disabled": self.order_tools_disabled,
            "dashboard_order_controls_unavailable": self.dashboard_order_controls_unavailable,
            "safety_review_draft_exists": self.safety_review_draft_exists,
            "orders_enqueue_endpoint_absent": self.orders_enqueue_endpoint_absent,
            "gateway_command_types_allowed": self.gateway_command_types_allowed,
            "dry_run_only": True,
            "live_order_allowed": False,
            "gateway_command_allowed": False,
            "broker_order_sent": False,
        }


def check_pr10_safety_gate(
    connection: sqlite3.Connection,
    settings: Settings | None = None,
) -> SafetyGateResult:
    resolved_settings = settings or load_settings()
    reason_codes: list[str] = []
    warnings: list[str] = []

    live_flags_disabled = (
        resolved_settings.trading_mode is TradingMode.OBSERVE
        and not resolved_settings.live_sim_allowed
        and not resolved_settings.live_real_allowed
    )
    order_routing_disabled = not resolved_settings.dry_run_order_routing_enabled
    gateway_order_commands_disabled = not resolved_settings.dry_run_gateway_command_enabled
    openai_tools_disabled = not resolved_settings.ai_sidecar_tools_enabled
    order_tools_disabled = not resolved_settings.ai_sidecar_order_tools_enabled
    safety_review_draft_exists = _pr10_safety_review_draft_exists(connection)

    if not live_flags_disabled:
        reason_codes.append(DryRunRejectionReason.LIVE_FLAGS_ENABLED.value)
    if not order_routing_disabled:
        reason_codes.append(DryRunRejectionReason.ORDER_ROUTING_DISABLED.value)
    if not gateway_order_commands_disabled:
        reason_codes.append(DryRunRejectionReason.GATEWAY_COMMAND_FORBIDDEN.value)
    if not openai_tools_disabled:
        reason_codes.append(DryRunRejectionReason.AI_OUTPUT_NOT_ALLOWED.value)
    if not order_tools_disabled:
        reason_codes.append(DryRunRejectionReason.AI_OUTPUT_NOT_ALLOWED.value)
    if (
        resolved_settings.dry_run_require_safety_gate
        and not safety_review_draft_exists
        and not resolved_settings.dry_run_allow_without_safety_draft_for_tests
    ):
        reason_codes.append(DryRunRejectionReason.SAFETY_GATE_FAILED.value)
    elif not safety_review_draft_exists:
        warnings.append("PR10 safety-review draft is bypassed by test-only dry-run setting.")

    reason_codes = [*dict.fromkeys(reason_codes)]
    passed = not reason_codes
    return SafetyGateResult(
        passed=passed,
        status="PASSED" if passed else "BLOCKED",
        reason_codes=reason_codes,
        warnings=warnings,
        sidecar_review_only_confirmed=True,
        rca_review_only_confirmed=True,
        codex_prompt_review_only_confirmed=True,
        order_routing_disabled=order_routing_disabled,
        live_flags_disabled=live_flags_disabled,
        gateway_order_commands_disabled=gateway_order_commands_disabled,
        openai_tools_disabled=openai_tools_disabled,
        order_tools_disabled=order_tools_disabled,
        dashboard_order_controls_unavailable=True,
        safety_review_draft_exists=safety_review_draft_exists,
        orders_enqueue_endpoint_absent=True,
        gateway_command_types_allowed=False,
    )


def _pr10_safety_review_draft_exists(connection: sqlite3.Connection) -> bool:
    rows = connection.execute(
        """
        SELECT metadata_json, prompt_text
        FROM ai_codex_prompt_drafts
        WHERE target_area = 'SAFETY_REVIEW'
        ORDER BY generated_at DESC
        LIMIT 20
        """
    ).fetchall()
    for row in rows:
        metadata = _json_object(row["metadata_json"])
        if metadata.get("pr10_safety_review") is True:
            return True
        prompt_text = str(row["prompt_text"] or "")
        if "PR10 OMS + DRY_RUN" in prompt_text:
            return True
    return False


def _json_object(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        loaded = json.loads(value)
    except json.JSONDecodeError:
        return {}
    if not isinstance(loaded, dict):
        return {}
    return normalize_payload(loaded)
