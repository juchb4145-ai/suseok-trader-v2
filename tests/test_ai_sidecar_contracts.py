from __future__ import annotations

import pytest
from domain.ai_sidecar.policy import (
    contains_forbidden_action,
    get_allowed_tasks,
    get_forbidden_actions,
    validate_output_schema,
)
from domain.ai_sidecar.schemas import AISidecarValidationError, DailyMarketBriefOutput
from domain.ai_sidecar.tasks import AISidecarTaskType


def valid_output() -> dict[str, object]:
    return {
        "summary": "Market breadth is mixed and needs operator review.",
        "severity": "LOW",
        "root_cause": "Theme momentum weakened after the open.",
        "operator_action": "Review the dashboard and keep observe-only posture.",
        "suggested_checks": ["Check candidate blocks", "Review risk observations"],
        "confidence": 0.72,
        "forbidden_actions_confirmed": True,
    }


def test_allowed_task_and_forbidden_action_lists_are_stable() -> None:
    assert get_allowed_tasks() == [task.value for task in AISidecarTaskType]
    assert "DAILY_MARKET_BRIEF" in get_allowed_tasks()
    assert "send_order" in get_forbidden_actions()
    assert "gateway_command" in get_forbidden_actions()


def test_output_schema_validation_accepts_valid_output() -> None:
    output = validate_output_schema(AISidecarTaskType.DAILY_MARKET_BRIEF, valid_output())

    assert isinstance(output, DailyMarketBriefOutput)
    assert output.confidence == 0.72


def test_output_schema_validation_rejects_forbidden_tool_action() -> None:
    output = valid_output() | {"tool": "send_order"}

    with pytest.raises(AISidecarValidationError, match="forbidden trading action"):
        validate_output_schema("DAILY_MARKET_BRIEF", output)


def test_policy_rejects_nested_tool_like_forbidden_action() -> None:
    executable_shape = {"metadata": {"tools": [{"function_name": "cancel_order"}]}}

    assert contains_forbidden_action(executable_shape) is True


def test_policy_does_not_reject_safety_explanation_text() -> None:
    output = valid_output()
    output["summary"] = "The sidecar cannot call send_order or create gateway_command."

    validated = validate_output_schema("DAILY_MARKET_BRIEF", output)

    assert validated.summary == "The sidecar cannot call send_order or create gateway_command."


@pytest.mark.parametrize("field_name", ["order_intent", "gateway_command", "send_order"])
def test_order_intent_and_gateway_command_like_fields_are_rejected(field_name: str) -> None:
    output = valid_output() | {field_name: {"code": "005930"}}

    with pytest.raises(AISidecarValidationError, match="forbidden trading action"):
        validate_output_schema("DAILY_MARKET_BRIEF", output)


def test_codex_prompt_draft_is_human_copy_only() -> None:
    output = valid_output() | {
        "prompt_draft": "Draft a code review prompt for the operator to copy manually."
    }

    validated = validate_output_schema("CODEX_PROMPT_DRAFT", output)

    assert validated.to_dict()["prompt_draft"].startswith("Draft a code review prompt")
