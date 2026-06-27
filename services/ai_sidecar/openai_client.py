from __future__ import annotations

import importlib
import importlib.util
import json
import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from time import perf_counter
from typing import Any, Protocol

from domain.ai_sidecar.context import canonical_context_json
from domain.ai_sidecar.tasks import AISidecarTaskType
from domain.broker.utils import normalize_value

from services.config import Settings


@dataclass(frozen=True, kw_only=True)
class AISidecarModelRequest:
    request_id: str
    task_type: AISidecarTaskType
    model: str
    system_prompt: str
    user_prompt: str
    context_packet: Mapping[str, Any]
    output_schema: Mapping[str, Any]
    timeout_sec: int
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, kw_only=True)
class AISidecarModelResponse:
    ok: bool
    output: Mapping[str, Any] | None = None
    raw_response: Mapping[str, Any] | None = None
    model: str = ""
    latency_ms: float = 0.0
    input_chars: int = 0
    output_chars: int = 0
    error_message: str | None = None
    status: str | None = None


class AISidecarModelClient(Protocol):
    def generate_structured_output(
        self,
        request: AISidecarModelRequest,
    ) -> AISidecarModelResponse:
        """Return task-specific structured JSON without trading side effects."""


class OpenAIResponsesClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def generate_structured_output(
        self,
        request: AISidecarModelRequest,
    ) -> AISidecarModelResponse:
        started = perf_counter()
        status = get_openai_client_status(self._settings)
        if not status["available"]:
            return AISidecarModelResponse(
                ok=False,
                model=request.model,
                latency_ms=_elapsed_ms(started),
                input_chars=_input_chars(request),
                error_message=_availability_error(status),
                status="CLIENT_UNAVAILABLE",
            )

        try:
            client = self._build_client(request.timeout_sec)
            response = self._create_response(client, request)
            raw_response = _response_to_dict(response)
            output = _extract_structured_json(response, raw_response)
            output_chars = len(canonical_context_json(output))
            return AISidecarModelResponse(
                ok=True,
                output=output,
                raw_response=_cap_raw_response(raw_response, self._settings),
                model=request.model,
                latency_ms=_elapsed_ms(started),
                input_chars=_input_chars(request),
                output_chars=output_chars,
            )
        except TimeoutError as exc:
            return AISidecarModelResponse(
                ok=False,
                model=request.model,
                latency_ms=_elapsed_ms(started),
                input_chars=_input_chars(request),
                error_message=str(exc) or "OpenAI request timed out",
                status="TIMEOUT",
            )
        except Exception as exc:
            status_name = "TIMEOUT" if "timeout" in type(exc).__name__.lower() else "MODEL_ERROR"
            return AISidecarModelResponse(
                ok=False,
                model=request.model,
                latency_ms=_elapsed_ms(started),
                input_chars=_input_chars(request),
                error_message=str(exc),
                status=status_name,
            )

    def _build_client(self, timeout_sec: int) -> Any:
        openai_module = importlib.import_module("openai")
        client_type = openai_module.OpenAI
        api_key = os.environ[self._settings.ai_sidecar_openai_api_key_env]
        kwargs: dict[str, Any] = {"api_key": api_key, "timeout": timeout_sec}
        base_url = self._settings.ai_sidecar_openai_base_url.strip()
        if base_url:
            kwargs["base_url"] = base_url
        return client_type(**kwargs)

    def _create_response(self, client: Any, request: AISidecarModelRequest) -> Any:
        last_error: Exception | None = None
        attempts = self._settings.ai_sidecar_max_retries + 1
        for _ in range(attempts):
            try:
                return client.responses.create(
                    model=request.model,
                    input=[
                        {"role": "system", "content": request.system_prompt},
                        {"role": "user", "content": request.user_prompt},
                    ],
                    text={"format": dict(request.output_schema)},
                    store=False,
                    timeout=request.timeout_sec,
                )
            except Exception as exc:
                last_error = exc
                if "timeout" in type(exc).__name__.lower():
                    raise TimeoutError(str(exc)) from exc
        if last_error is not None:
            raise last_error
        raise RuntimeError("OpenAI response was not created")


class MockAISidecarModelClient:
    def __init__(
        self,
        *,
        output: Mapping[str, Any] | None = None,
        status: str = "success",
        error_message: str | None = None,
        latency_ms: float = 1.0,
    ) -> None:
        self.output = dict(output) if output is not None else None
        self.status = status
        self.error_message = error_message
        self.latency_ms = latency_ms
        self.requests: list[AISidecarModelRequest] = []

    def generate_structured_output(
        self,
        request: AISidecarModelRequest,
    ) -> AISidecarModelResponse:
        self.requests.append(request)
        if self.status == "timeout":
            return AISidecarModelResponse(
                ok=False,
                model=request.model,
                latency_ms=self.latency_ms,
                input_chars=_input_chars(request),
                error_message=self.error_message or "mock timeout",
                status="TIMEOUT",
            )
        if self.status == "model_error":
            return AISidecarModelResponse(
                ok=False,
                model=request.model,
                latency_ms=self.latency_ms,
                input_chars=_input_chars(request),
                error_message=self.error_message or "mock model error",
                status="MODEL_ERROR",
            )
        output = (
            self.output
            if self.output is not None
            else _valid_output_for_task(request.task_type)
        )
        return AISidecarModelResponse(
            ok=True,
            output=output,
            raw_response={"mock": True, "request_id": request.request_id},
            model=request.model,
            latency_ms=self.latency_ms,
            input_chars=_input_chars(request),
            output_chars=len(canonical_context_json(output)),
        )


def get_openai_client_status(settings: Settings) -> dict[str, Any]:
    sdk_available = importlib.util.find_spec("openai") is not None
    api_key_available = bool(os.environ.get(settings.ai_sidecar_openai_api_key_env, "").strip())
    model_configured = bool(settings.ai_sidecar_model.strip())
    configured = settings.ai_sidecar_enabled and model_configured
    available = (
        configured
        and api_key_available
        and sdk_available
        and settings.ai_sidecar_use_responses_api
        and settings.ai_sidecar_structured_outputs_enabled
        and not settings.ai_sidecar_tools_enabled
        and not settings.ai_sidecar_order_tools_enabled
    )
    return {
        "configured": configured,
        "sdk_available": sdk_available,
        "api_key_env": settings.ai_sidecar_openai_api_key_env,
        "api_key_available": api_key_available,
        "model_configured": model_configured,
        "responses_api_enabled": settings.ai_sidecar_use_responses_api,
        "structured_outputs_enabled": settings.ai_sidecar_structured_outputs_enabled,
        "strict_schema": settings.ai_sidecar_strict_schema,
        "tools_enabled": False,
        "order_tools_enabled": False,
        "available": available,
    }


def _valid_output_for_task(task_type: AISidecarTaskType) -> dict[str, Any]:
    output = {
        "summary": f"{task_type.value} requires operator review only.",
        "severity": "LOW",
        "root_cause": "Mock structured output for read-only analysis.",
        "operator_action": "REVIEW_ONLY",
        "suggested_checks": ["Review dashboard observations", "Check source data freshness"],
        "confidence": 0.72,
        "forbidden_actions_confirmed": True,
    }
    if task_type is AISidecarTaskType.CODEX_PROMPT_DRAFT:
        output["prompt_draft"] = "Draft a human-reviewed Codex prompt from the context packet."
    return output


def _response_to_dict(response: Any) -> dict[str, Any]:
    for method_name in ("model_dump", "to_dict", "dict"):
        method = getattr(response, method_name, None)
        if callable(method):
            data = method()
            if isinstance(data, Mapping):
                return normalize_value(data)
    if isinstance(response, Mapping):
        return normalize_value(response)
    return {"repr": repr(response)}


def _extract_structured_json(response: Any, raw_response: Mapping[str, Any]) -> Mapping[str, Any]:
    parsed = getattr(response, "output_parsed", None)
    if isinstance(parsed, Mapping):
        return normalize_value(parsed)
    text = getattr(response, "output_text", None)
    if isinstance(text, str) and text.strip():
        parsed_text = json.loads(text)
        if isinstance(parsed_text, Mapping):
            return normalize_value(parsed_text)
    candidate = _find_structured_mapping(raw_response)
    if candidate is not None:
        return candidate
    raise ValueError("OpenAI response did not contain structured JSON output")


def _find_structured_mapping(value: object) -> Mapping[str, Any] | None:
    if isinstance(value, Mapping):
        if {"summary", "severity", "root_cause"}.issubset(set(value)):
            return normalize_value(value)
        for key in ("parsed", "json", "content", "output"):
            item = value.get(key)
            if isinstance(item, str) and item.strip():
                try:
                    parsed = json.loads(item)
                except json.JSONDecodeError:
                    parsed = None
                if isinstance(parsed, Mapping):
                    return normalize_value(parsed)
            found = _find_structured_mapping(item)
            if found is not None:
                return found
        for item in value.values():
            found = _find_structured_mapping(item)
            if found is not None:
                return found
    if isinstance(value, list | tuple):
        for item in value:
            found = _find_structured_mapping(item)
            if found is not None:
                return found
    return None


def _cap_raw_response(
    raw_response: Mapping[str, Any],
    settings: Settings,
) -> Mapping[str, Any] | None:
    if not settings.ai_sidecar_store_raw_response:
        return None
    raw_json = canonical_context_json(raw_response)
    if len(raw_json) <= settings.ai_sidecar_max_output_chars:
        return raw_response
    return {
        "truncated": True,
        "raw_response_prefix": raw_json[: settings.ai_sidecar_max_output_chars],
    }


def _availability_error(status: Mapping[str, Any]) -> str:
    if not status.get("configured"):
        return "AI Sidecar OpenAI client is not configured"
    if not status.get("model_configured"):
        return "AI Sidecar model is not configured"
    if not status.get("api_key_available"):
        return "OpenAI API key is missing"
    if not status.get("sdk_available"):
        return "OpenAI SDK is not installed"
    return "AI Sidecar OpenAI client is unavailable"


def _input_chars(request: AISidecarModelRequest) -> int:
    return len(request.system_prompt) + len(request.user_prompt)


def _elapsed_ms(started: float) -> float:
    return round((perf_counter() - started) * 1000.0, 3)
