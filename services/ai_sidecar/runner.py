from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from domain.ai_sidecar.context import AISidecarContextPacket
from domain.ai_sidecar.policy import validate_output_schema, validate_task_allowed
from domain.ai_sidecar.schemas import AISidecarInsight, AISidecarValidationError
from domain.ai_sidecar.tasks import AISidecarTaskType

from services.ai_sidecar.context_builder import build_context_packet
from services.ai_sidecar.context_store import (
    get_context_packet,
    save_context_build_error,
    save_context_packet,
)
from services.ai_sidecar.openai_client import (
    AISidecarModelClient,
    AISidecarModelRequest,
    OpenAIResponsesClient,
    get_openai_client_status,
)
from services.ai_sidecar.output_schema import (
    get_openai_text_format_for_task,
    get_output_schema_name_for_task,
    validate_structured_output,
)
from services.ai_sidecar.prompt_registry import build_prompt
from services.ai_sidecar.request_store import (
    AIRequestStatus,
    create_ai_request,
    save_ai_insight,
    update_ai_request_status,
)
from services.config import Settings, load_settings


@dataclass(frozen=True, kw_only=True)
class AISidecarRunResult:
    ok: bool
    request_id: str
    context_id: str | None
    task_type: str
    status: str
    insight_id: str | None = None
    error_message: str | None = None
    validation_error: str | None = None
    latency_ms: float | None = None
    model: str = ""
    observe_only: bool = True
    no_trading_side_effects: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "request_id": self.request_id,
            "insight_id": self.insight_id,
            "context_id": self.context_id,
            "task_type": self.task_type,
            "status": self.status,
            "error_message": self.error_message,
            "validation_error": self.validation_error,
            "latency_ms": self.latency_ms,
            "model": self.model,
            "observe_only": self.observe_only,
            "no_trading_side_effects": self.no_trading_side_effects,
        }


def run_ai_sidecar_task(
    connection: sqlite3.Connection,
    task_type: AISidecarTaskType | str,
    *,
    trade_date: str | None = None,
    related_entity_type: str | None = None,
    related_entity_id: str | None = None,
    context_id: str | None = None,
    persist_context: bool = True,
    model_client: AISidecarModelClient | None = None,
    settings: Settings | None = None,
) -> AISidecarRunResult:
    settings = settings or load_settings()
    task = validate_task_allowed(task_type)
    model = settings.ai_sidecar_model.strip()
    output_schema_name = get_output_schema_name_for_task(task)
    request_id = create_ai_request(
        connection,
        task_type=task,
        trade_date=trade_date,
        related_entity_type=related_entity_type,
        related_entity_id=related_entity_id,
        context_id=context_id,
        output_schema_name=output_schema_name,
        model=model,
        status=AIRequestStatus.QUEUED,
        metadata={"manual_execution": True, "persist_context": persist_context},
    )

    if not settings.ai_sidecar_enabled or not settings.ai_sidecar_allow_manual_run:
        return _finish_failure(
            connection,
            request_id=request_id,
            task=task,
            context_id=context_id,
            status=AIRequestStatus.AI_DISABLED,
            model=model,
            error_message="AI Sidecar execution is disabled",
        )

    if not model:
        return _finish_failure(
            connection,
            request_id=request_id,
            task=task,
            context_id=context_id,
            status=AIRequestStatus.CLIENT_UNAVAILABLE,
            model=model,
            error_message="AI Sidecar model is not configured",
        )

    if model_client is None:
        client_status = get_openai_client_status(settings)
        if not client_status["available"]:
            status = (
                AIRequestStatus.API_KEY_MISSING
                if not client_status["api_key_available"]
                else AIRequestStatus.CLIENT_UNAVAILABLE
            )
            return _finish_failure(
                connection,
                request_id=request_id,
                task=task,
                context_id=context_id,
                status=status,
                model=model,
                error_message=_client_unavailable_message(client_status),
            )
        model_client = OpenAIResponsesClient(settings=settings)

    update_ai_request_status(connection, request_id, AIRequestStatus.RUNNING, model=model)

    packet = _load_or_build_context_packet(
        connection,
        task=task,
        trade_date=trade_date,
        related_entity_type=related_entity_type,
        related_entity_id=related_entity_id,
        context_id=context_id,
        persist_context=persist_context,
        settings=settings,
        request_id=request_id,
        model=model,
    )
    if isinstance(packet, AISidecarRunResult):
        return packet
    if packet.order_context_included and not settings.ai_sidecar_order_context_allowed:
        return _finish_failure(
            connection,
            request_id=request_id,
            task=task,
            context_id=packet.context_id,
            status=AIRequestStatus.POLICY_REJECTED,
            model=model,
            error_message="Context packet includes order context while order context is disabled",
        )

    prompt = build_prompt(task, packet)
    output_schema = get_openai_text_format_for_task(task)
    update_ai_request_status(
        connection,
        request_id,
        AIRequestStatus.RUNNING,
        context_id=packet.context_id,
        prompt_hash=prompt.prompt_hash,
        context_hash=packet.context_hash,
        output_schema_name=output_schema_name,
        model=model,
        input_chars=prompt.input_chars,
        metadata={"manual_execution": True, "prompt_version": prompt.version},
    )

    model_response = model_client.generate_structured_output(
        AISidecarModelRequest(
            request_id=request_id,
            task_type=task,
            model=model,
            system_prompt=prompt.system_prompt,
            user_prompt=prompt.user_prompt,
            context_packet=packet.to_dict(),
            output_schema=output_schema,
            timeout_sec=settings.ai_sidecar_request_timeout_sec,
            metadata={
                "prompt_hash": prompt.prompt_hash,
                "context_hash": packet.context_hash,
                "output_schema_name": output_schema_name,
                "tools_enabled": False,
                "order_tools_enabled": False,
            },
        )
    )
    if not model_response.ok:
        status = _model_failure_status(model_response.status)
        return _finish_failure(
            connection,
            request_id=request_id,
            task=task,
            context_id=packet.context_id,
            status=status,
            model=model_response.model or model,
            error_message=model_response.error_message or "AI model request failed",
            latency_ms=model_response.latency_ms,
            input_chars=model_response.input_chars,
            output_chars=model_response.output_chars,
        )

    try:
        if not isinstance(model_response.output, Mapping):
            raise AISidecarValidationError("AI output must be a JSON object")
        schema_validated = validate_structured_output(task, model_response.output)
        validated = validate_output_schema(task, schema_validated)
    except AISidecarValidationError as exc:
        return _finish_failure(
            connection,
            request_id=request_id,
            task=task,
            context_id=packet.context_id,
            status=_validation_status(exc),
            model=model_response.model or model,
            validation_error=str(exc),
            latency_ms=model_response.latency_ms,
            input_chars=model_response.input_chars,
            output_chars=model_response.output_chars,
            raw_response=model_response.raw_response,
        )

    output = validated.to_dict()
    insight = AISidecarInsight(
        request_id=request_id,
        task_type=task,
        trade_date=trade_date or packet.trade_date,
        related_entity_type=related_entity_type or packet.related_entity_type,
        related_entity_id=related_entity_id or packet.related_entity_id,
        summary=validated.summary,
        root_cause=validated.root_cause,
        severity=validated.severity,
        operator_action=validated.operator_action,
        output=output,
    )
    insight_id = save_ai_insight(connection, insight)
    update_ai_request_status(
        connection,
        request_id,
        AIRequestStatus.COMPLETED,
        context_id=packet.context_id,
        prompt_hash=prompt.prompt_hash,
        context_hash=packet.context_hash,
        output_schema_name=output_schema_name,
        model=model_response.model or model,
        latency_ms=model_response.latency_ms,
        input_chars=model_response.input_chars,
        output_chars=model_response.output_chars,
        raw_response=(
            model_response.raw_response if settings.ai_sidecar_store_raw_response else None
        ),
    )
    return AISidecarRunResult(
        ok=True,
        request_id=request_id,
        insight_id=insight_id,
        context_id=packet.context_id,
        task_type=task.value,
        status=AIRequestStatus.COMPLETED.value,
        latency_ms=model_response.latency_ms,
        model=model_response.model or model,
    )


def _load_or_build_context_packet(
    connection: sqlite3.Connection,
    *,
    task: AISidecarTaskType,
    trade_date: str | None,
    related_entity_type: str | None,
    related_entity_id: str | None,
    context_id: str | None,
    persist_context: bool,
    settings: Settings,
    request_id: str,
    model: str,
) -> AISidecarContextPacket | AISidecarRunResult:
    try:
        if context_id:
            row = get_context_packet(connection, context_id)
            if row is None:
                return _finish_failure(
                    connection,
                    request_id=request_id,
                    task=task,
                    context_id=context_id,
                    status=AIRequestStatus.CONTEXT_ERROR,
                    model=model,
                    error_message="AI context packet not found",
                )
            packet_data = dict(row)
            packet_data.pop("created_at", None)
            return AISidecarContextPacket.from_dict(packet_data)

        packet = build_context_packet(
            connection,
            task,
            trade_date=trade_date,
            related_entity_type=related_entity_type,
            related_entity_id=related_entity_id,
            settings=settings,
        )
        if persist_context:
            save_context_packet(connection, packet)
        return packet
    except Exception as exc:
        save_context_build_error(
            connection,
            task_type=task,
            trade_date=trade_date,
            related_entity_type=related_entity_type,
            related_entity_id=related_entity_id,
            error_message=str(exc),
            payload={"runner_request_id": request_id},
        )
        return _finish_failure(
            connection,
            request_id=request_id,
            task=task,
            context_id=context_id,
            status=AIRequestStatus.CONTEXT_ERROR,
            model=model,
            error_message=str(exc),
        )


def _finish_failure(
    connection: sqlite3.Connection,
    *,
    request_id: str,
    task: AISidecarTaskType,
    context_id: str | None,
    status: AIRequestStatus,
    model: str,
    error_message: str | None = None,
    validation_error: str | None = None,
    latency_ms: float | None = None,
    input_chars: int | None = None,
    output_chars: int | None = None,
    raw_response: Mapping[str, Any] | None = None,
) -> AISidecarRunResult:
    update_ai_request_status(
        connection,
        request_id,
        status,
        context_id=context_id,
        model=model,
        error_message=error_message,
        validation_error=validation_error,
        latency_ms=latency_ms,
        input_chars=input_chars,
        output_chars=output_chars,
        raw_response=raw_response,
    )
    return AISidecarRunResult(
        ok=False,
        request_id=request_id,
        context_id=context_id,
        task_type=task.value,
        status=status.value,
        error_message=error_message,
        validation_error=validation_error,
        latency_ms=latency_ms,
        model=model,
    )


def _model_failure_status(status: str | None) -> AIRequestStatus:
    normalized = (status or "").strip().upper()
    if normalized == AIRequestStatus.TIMEOUT.value:
        return AIRequestStatus.TIMEOUT
    if normalized == AIRequestStatus.CLIENT_UNAVAILABLE.value:
        return AIRequestStatus.CLIENT_UNAVAILABLE
    if normalized == AIRequestStatus.API_KEY_MISSING.value:
        return AIRequestStatus.API_KEY_MISSING
    if normalized == AIRequestStatus.MODEL_ERROR.value:
        return AIRequestStatus.MODEL_ERROR
    return AIRequestStatus.FAILED


def _validation_status(exc: AISidecarValidationError) -> AIRequestStatus:
    if "forbidden trading action" in str(exc).lower():
        return AIRequestStatus.POLICY_REJECTED
    return AIRequestStatus.AI_OUTPUT_INVALID


def _client_unavailable_message(status: Mapping[str, Any]) -> str:
    if not status.get("model_configured"):
        return "AI Sidecar model is not configured"
    if not status.get("api_key_available"):
        return "OpenAI API key is missing"
    if not status.get("sdk_available"):
        return "OpenAI SDK is not installed"
    if not status.get("responses_api_enabled"):
        return "OpenAI Responses API is disabled"
    if not status.get("structured_outputs_enabled"):
        return "Structured outputs are disabled"
    return "OpenAI client is unavailable"
