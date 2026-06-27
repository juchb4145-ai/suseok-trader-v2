from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any

from domain.ai_sidecar.codex_prompt import ensure_codex_prompt_policy
from domain.ai_sidecar.schemas import (
    AISidecarBaseOutput,
    AISidecarValidationError,
    CandidateBlockRCAOutput,
    CodexPromptDraftOutput,
    DailyMarketBriefOutput,
    NoTradeRCAOutput,
    OpsIncidentSummaryOutput,
    ThemeBriefOutput,
    TradeReviewOutput,
)
from domain.ai_sidecar.tasks import AISidecarTaskType

FORBIDDEN_ACTIONS: tuple[str, ...] = (
    "send_order",
    "cancel_order",
    "modify_order",
    "enqueue_order",
    "order_intent",
    "gateway_command",
    "live_real",
    "live_sim_enable",
    "trading_allow_live",
    "risk_limit_update",
    "strategy_threshold_update",
    "position_size_update",
)

ACTION_LIKE_FIELDS: tuple[str, ...] = (
    "action",
    "actions",
    "api_call",
    "api_calls",
    "command",
    "commands",
    "function",
    "function_name",
    "intent",
    "intents",
    "operation",
    "operations",
    "requested_action",
    "requested_actions",
    "recommended_action",
    "recommended_actions",
    "tool",
    "tools",
    "tool_name",
)

TASK_OUTPUT_SCHEMAS: dict[AISidecarTaskType, type[AISidecarBaseOutput]] = {
    AISidecarTaskType.DAILY_MARKET_BRIEF: DailyMarketBriefOutput,
    AISidecarTaskType.THEME_BRIEF: ThemeBriefOutput,
    AISidecarTaskType.CANDIDATE_BLOCK_RCA: CandidateBlockRCAOutput,
    AISidecarTaskType.NO_TRADE_RCA: NoTradeRCAOutput,
    AISidecarTaskType.TRADE_REVIEW: TradeReviewOutput,
    AISidecarTaskType.OPS_INCIDENT_SUMMARY: OpsIncidentSummaryOutput,
    AISidecarTaskType.CODEX_PROMPT_DRAFT: CodexPromptDraftOutput,
}


def validate_task_allowed(task_type: AISidecarTaskType | str) -> AISidecarTaskType:
    if isinstance(task_type, AISidecarTaskType):
        return task_type
    if isinstance(task_type, str):
        normalized = task_type.strip().upper()
        for allowed_task in AISidecarTaskType:
            if normalized in {allowed_task.name.upper(), allowed_task.value.upper()}:
                return allowed_task
    allowed = ", ".join(task.value for task in AISidecarTaskType)
    raise AISidecarValidationError(f"task_type must be one of: {allowed}")


def validate_output_schema(
    task_type: AISidecarTaskType | str,
    output: AISidecarBaseOutput | Mapping[str, Any],
) -> AISidecarBaseOutput:
    task = validate_task_allowed(task_type)
    schema_type = TASK_OUTPUT_SCHEMAS[task]

    if isinstance(output, schema_type):
        validated = output
    elif isinstance(output, Mapping):
        assert_no_trading_action(output)
        validated = schema_type.from_dict(output)
    else:
        raise AISidecarValidationError("output must be a mapping or expected output model")

    assert_no_trading_action(validated.to_dict())
    if task is AISidecarTaskType.CODEX_PROMPT_DRAFT:
        ensure_codex_prompt_policy(validated.to_dict()["prompt_draft"])
    return validated


def assert_no_trading_action(output: Mapping[str, Any]) -> None:
    if contains_forbidden_action(output):
        raise AISidecarValidationError("AI Sidecar output contains a forbidden trading action")


def contains_forbidden_action(value: object) -> bool:
    return _contains_forbidden_action(value, inspect_strings=isinstance(value, str))


def get_allowed_tasks() -> list[str]:
    return [task.value for task in AISidecarTaskType]


def get_forbidden_actions() -> list[str]:
    return list(FORBIDDEN_ACTIONS)


def _contains_forbidden_action(value: object, *, inspect_strings: bool) -> bool:
    if isinstance(value, Mapping):
        for raw_key, item in value.items():
            key = str(raw_key)
            if _matches_forbidden_token(key):
                return True
            if _is_action_like_field(key):
                if _contains_forbidden_action(item, inspect_strings=True):
                    return True
            elif _contains_forbidden_action(item, inspect_strings=False):
                return True
        return False

    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return any(
            _contains_forbidden_action(item, inspect_strings=inspect_strings) for item in value
        )

    if inspect_strings and isinstance(value, str):
        return _matches_forbidden_token(value)

    return False


def _is_action_like_field(value: str) -> bool:
    action_like_fields = {_canonicalize(field_name) for field_name in ACTION_LIKE_FIELDS}
    return _canonicalize(value) in action_like_fields


def _matches_forbidden_token(value: str) -> bool:
    canonical_value = _canonicalize(value)
    return any(_canonicalize(action) in canonical_value for action in FORBIDDEN_ACTIONS)


def _canonicalize(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())
