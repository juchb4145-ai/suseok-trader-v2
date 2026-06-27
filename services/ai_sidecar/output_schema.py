from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from typing import Any

from domain.ai_sidecar.policy import validate_task_allowed
from domain.ai_sidecar.schemas import AISidecarInsightSeverity, AISidecarValidationError
from domain.ai_sidecar.tasks import AISidecarTaskType

READ_ONLY_OPERATOR_ACTIONS: tuple[str, ...] = (
    "WATCH_ONLY",
    "REVIEW_ONLY",
    "CHECK_DATA",
    "CHECK_PIPELINE",
    "CHECK_POLICY",
    "NO_ACTION",
)

TASK_SCHEMA_NAMES: dict[AISidecarTaskType, str] = {
    AISidecarTaskType.DAILY_MARKET_BRIEF: "daily_market_brief_output_v1",
    AISidecarTaskType.THEME_BRIEF: "theme_brief_output_v1",
    AISidecarTaskType.CANDIDATE_BLOCK_RCA: "candidate_block_rca_output_v1",
    AISidecarTaskType.NO_TRADE_RCA: "no_trade_rca_output_v1",
    AISidecarTaskType.TRADE_REVIEW: "trade_review_output_v1",
    AISidecarTaskType.OPS_INCIDENT_SUMMARY: "ops_incident_summary_output_v1",
    AISidecarTaskType.CODEX_PROMPT_DRAFT: "codex_prompt_draft_output_v1",
}

BASE_REQUIRED_FIELDS: tuple[str, ...] = (
    "summary",
    "severity",
    "root_cause",
    "operator_action",
    "suggested_checks",
    "confidence",
    "forbidden_actions_confirmed",
)

EXECUTABLE_FIELD_TOKENS: tuple[str, ...] = (
    "order_intent",
    "gateway_command",
    "send_order",
    "cancel_order",
    "modify_order",
)


def get_output_schema_name_for_task(task_type: AISidecarTaskType | str) -> str:
    task = validate_task_allowed(task_type)
    return TASK_SCHEMA_NAMES[task]


def get_output_schema_for_task(task_type: AISidecarTaskType | str) -> dict[str, Any]:
    task = validate_task_allowed(task_type)
    required = list(BASE_REQUIRED_FIELDS)
    properties: dict[str, Any] = {
        "summary": {"type": "string", "minLength": 1},
        "severity": {
            "type": "string",
            "enum": [severity.value for severity in AISidecarInsightSeverity],
        },
        "root_cause": {"type": "string", "minLength": 1},
        "operator_action": {
            "type": "string",
            "enum": list(READ_ONLY_OPERATOR_ACTIONS),
        },
        "suggested_checks": {
            "type": "array",
            "items": {"type": "string", "minLength": 1},
            "minItems": 1,
        },
        "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0},
        "forbidden_actions_confirmed": {"type": "boolean", "enum": [True]},
    }
    if task is AISidecarTaskType.CODEX_PROMPT_DRAFT:
        required.append("prompt_draft")
        properties["prompt_draft"] = {"type": "string", "minLength": 1}

    schema = {
        "type": "object",
        "additionalProperties": False,
        "required": required,
        "properties": properties,
    }
    _assert_no_executable_fields(schema)
    return schema


def get_openai_text_format_for_task(task_type: AISidecarTaskType | str) -> dict[str, Any]:
    task = validate_task_allowed(task_type)
    return {
        "type": "json_schema",
        "name": get_output_schema_name_for_task(task),
        "strict": True,
        "schema": get_output_schema_for_task(task),
    }


def validate_structured_output(
    task_type: AISidecarTaskType | str,
    output: Mapping[str, Any],
) -> dict[str, Any]:
    schema = get_output_schema_for_task(task_type)
    normalized = dict(output)
    _validate_object(schema, normalized, path="$")
    return deepcopy(normalized)


def _validate_object(schema: Mapping[str, Any], value: object, *, path: str) -> None:
    if not isinstance(value, Mapping):
        raise AISidecarValidationError(f"{path} must be an object")

    required = tuple(str(field) for field in schema.get("required", ()))
    missing = [field for field in required if field not in value]
    if missing:
        raise AISidecarValidationError(f"{path} missing required field(s): {', '.join(missing)}")

    properties = schema.get("properties", {})
    if not isinstance(properties, Mapping):
        raise AISidecarValidationError(f"{path}.properties must be an object")

    if schema.get("additionalProperties") is False:
        unknown = sorted(str(field) for field in value if field not in properties)
        if unknown:
            raise AISidecarValidationError(f"{path} unknown field(s): {', '.join(unknown)}")

    for field, field_schema in properties.items():
        if field in value:
            _validate_value(field_schema, value[field], path=f"{path}.{field}")


def _validate_value(schema: object, value: object, *, path: str) -> None:
    if not isinstance(schema, Mapping):
        raise AISidecarValidationError(f"{path} schema must be an object")
    expected_type = schema.get("type")
    enum_values = schema.get("enum")
    if enum_values is not None and value not in enum_values:
        raise AISidecarValidationError(f"{path} must be one of: {enum_values}")

    if expected_type == "string":
        if not isinstance(value, str):
            raise AISidecarValidationError(f"{path} must be a string")
        if int(schema.get("minLength", 0)) > 0 and not value.strip():
            raise AISidecarValidationError(f"{path} must not be empty")
        return

    if expected_type == "number":
        if isinstance(value, bool) or not isinstance(value, int | float):
            raise AISidecarValidationError(f"{path} must be a number")
        if "minimum" in schema and float(value) < float(schema["minimum"]):
            raise AISidecarValidationError(f"{path} must be >= {schema['minimum']}")
        if "maximum" in schema and float(value) > float(schema["maximum"]):
            raise AISidecarValidationError(f"{path} must be <= {schema['maximum']}")
        return

    if expected_type == "boolean":
        if not isinstance(value, bool):
            raise AISidecarValidationError(f"{path} must be a boolean")
        return

    if expected_type == "array":
        if isinstance(value, str) or not isinstance(value, list | tuple):
            raise AISidecarValidationError(f"{path} must be an array")
        if len(value) < int(schema.get("minItems", 0)):
            raise AISidecarValidationError(f"{path} must contain at least one item")
        item_schema = schema.get("items", {})
        for index, item in enumerate(value):
            _validate_value(item_schema, item, path=f"{path}[{index}]")
        return

    if expected_type == "object":
        _validate_object(schema, value, path=path)
        return

    raise AISidecarValidationError(f"{path} has unsupported schema type: {expected_type}")


def _assert_no_executable_fields(value: object) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            normalized = str(key).lower()
            if any(token in normalized for token in EXECUTABLE_FIELD_TOKENS):
                raise AISidecarValidationError("output schema contains an executable field")
            _assert_no_executable_fields(item)
        return
    if isinstance(value, list | tuple):
        for item in value:
            _assert_no_executable_fields(item)
