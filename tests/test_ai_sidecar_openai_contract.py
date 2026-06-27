from __future__ import annotations

import pytest
from domain.ai_sidecar.context import AISidecarContextPacket
from domain.ai_sidecar.schemas import AISidecarValidationError
from domain.ai_sidecar.tasks import AISidecarTaskType
from services.ai_sidecar.openai_client import get_openai_client_status
from services.ai_sidecar.output_schema import (
    READ_ONLY_OPERATOR_ACTIONS,
    get_openai_text_format_for_task,
    get_output_schema_for_task,
    get_output_schema_name_for_task,
    validate_structured_output,
)
from services.ai_sidecar.prompt_registry import build_prompt, get_prompt_template
from services.config import Settings
from storage.gateway_command_store import canonical_json


def _packet(
    task_type: AISidecarTaskType = AISidecarTaskType.NO_TRADE_RCA,
) -> AISidecarContextPacket:
    return AISidecarContextPacket(
        context_id="ctx-test",
        task_type=task_type,
        schema_version="ai-sidecar-context.v1",
        trade_date="2026-06-27",
        related_entity_type=None,
        related_entity_id=None,
        generated_at="2026-06-27T00:00:00Z",
        source_sections=[],
        context_hash="hash-test",
        size_chars=100,
        max_size_chars=12000,
        redaction_applied=True,
        order_context_included=False,
        payload={"summary": "redacted context", "secret": "[REDACTED]"},
    )


def _valid_output() -> dict[str, object]:
    return {
        "summary": "Observation context needs review.",
        "severity": "LOW",
        "root_cause": "Data freshness is mixed.",
        "operator_action": "REVIEW_ONLY",
        "suggested_checks": ["Check data freshness"],
        "confidence": 0.5,
        "forbidden_actions_confirmed": True,
    }


def test_openai_client_status_is_unavailable_without_key_or_model(monkeypatch) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    default_status = get_openai_client_status(Settings())
    model_only_status = get_openai_client_status(
        Settings(ai_sidecar_enabled_value=True, ai_sidecar_model="gpt-test")
    )

    assert default_status["available"] is False
    assert default_status["model_configured"] is False
    assert model_only_status["available"] is False
    assert model_only_status["api_key_available"] is False
    assert model_only_status["tools_enabled"] is False
    assert model_only_status["order_tools_enabled"] is False


def test_output_schema_exists_for_every_task_and_is_strict() -> None:
    for task_type in AISidecarTaskType:
        schema = get_output_schema_for_task(task_type)
        text_format = get_openai_text_format_for_task(task_type)
        schema_json = canonical_json(schema)

        assert get_output_schema_name_for_task(task_type).endswith("_output_v1")
        assert schema["type"] == "object"
        assert schema["additionalProperties"] is False
        assert set(schema["required"]) >= {
            "summary",
            "severity",
            "root_cause",
            "operator_action",
            "suggested_checks",
            "confidence",
            "forbidden_actions_confirmed",
        }
        assert text_format["type"] == "json_schema"
        assert text_format["strict"] is True
        assert "order_intent" not in schema_json
        assert "gateway_command" not in schema_json
        assert "send_order" not in schema_json
        assert "cancel_order" not in schema_json
        assert "modify_order" not in schema_json


@pytest.mark.parametrize(
    "output",
    [
        _valid_output() | {"operator_action": "BUY"},
        _valid_output() | {"confidence": 2.0},
        {key: value for key, value in _valid_output().items() if key != "summary"},
        _valid_output() | {"forbidden_actions_confirmed": False},
    ],
)
def test_local_schema_validation_rejects_invalid_structured_outputs(output) -> None:
    with pytest.raises(AISidecarValidationError):
        validate_structured_output("NO_TRADE_RCA", output)


def test_prompt_registry_covers_every_task_and_hash_is_deterministic() -> None:
    for task_type in AISidecarTaskType:
        template = get_prompt_template(task_type)
        first = build_prompt(task_type, _packet(task_type))
        second = build_prompt(task_type, _packet(task_type))

        assert "read-only" in template.system_prompt
        assert "tools" in template.system_prompt
        assert "function calls" in template.system_prompt
        assert "operator_action" in template.system_prompt
        assert all(action in template.system_prompt for action in READ_ONLY_OPERATOR_ACTIONS)
        assert first.prompt_hash == second.prompt_hash
        assert '"secret":"[REDACTED]"' in first.user_prompt
        assert "raw_secret_bypass" not in first.user_prompt


def test_prompt_uses_payload_not_unknown_context_bypass_fields() -> None:
    packet = _packet().to_dict() | {"raw_secret_bypass": "do not leak"}

    prompt = build_prompt("NO_TRADE_RCA", packet)

    assert "do not leak" not in prompt.user_prompt
    assert "redacted context" in prompt.user_prompt
