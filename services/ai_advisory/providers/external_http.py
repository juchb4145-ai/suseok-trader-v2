from __future__ import annotations

import json
import os
import time
from collections.abc import Mapping
from time import perf_counter
from typing import Any

import httpx
from domain.broker.utils import normalize_value

from services.ai_advisory.context_builder import build_candidate_scoring_prompt
from services.ai_advisory.models import (
    AiProviderRawResult,
    CandidatePrompt,
    CandidateScoringContext,
)
from services.ai_advisory.providers.base import (
    CandidateScorerProviderError,
    ExternalHttpTransport,
    ExternalHttpTransportResponse,
)
from services.ai_advisory.schema import get_candidate_scorer_output_schema
from services.config import Settings

DEFAULT_OPENAI_CHAT_COMPLETIONS_URL = "https://api.openai.com/v1/chat/completions"
TRANSIENT_STATUSES = {408, 409, 429, 500, 502, 503, 504}


class ExternalHTTPCandidateScorerProvider:
    name = "external_http"

    def __init__(
        self,
        *,
        settings: Settings,
        allow_external: bool = False,
        transport: ExternalHttpTransport | None = None,
    ) -> None:
        self._settings = settings
        self._allow_external = allow_external
        self._transport = transport or HttpxExternalHttpTransport()

    def score_candidates(
        self,
        context: CandidateScoringContext,
        *,
        prompt: CandidatePrompt | None = None,
        settings: Settings,
    ) -> AiProviderRawResult:
        started = perf_counter()
        resolved_prompt = prompt or build_candidate_scoring_prompt(context, settings=settings)
        availability = _external_availability(settings, allow_external=self._allow_external)
        provider_name = _provider_name(settings)
        model = _model_name(settings)
        if availability is not None:
            return AiProviderRawResult(
                provider=provider_name,
                model=model,
                status=availability["status"],
                latency_ms=_elapsed_ms(started),
                error_message=availability["message"],
                external_call_enabled=False,
                external_call_attempted=False,
            )

        url = _base_url(settings)
        headers = {
            "Authorization": f"Bearer {os.environ[settings.ai_external_llm_api_key_env]}",
            "Content-Type": "application/json",
        }
        payload = _request_payload(resolved_prompt, settings=settings)
        max_attempts = max(1, settings.ai_external_llm_max_retries + 1)
        compatibility_retries_remaining = 2
        last_error: str | None = None
        attempts = 0

        for attempt in range(max_attempts + compatibility_retries_remaining):
            attempts = attempt + 1
            try:
                response = self._transport.post_json(
                    url,
                    headers=headers,
                    payload=payload,
                    timeout_seconds=settings.ai_external_llm_timeout_seconds,
                )
            except TimeoutError as exc:
                last_error = str(exc) or "external LLM request timed out"
                if attempt < max_attempts - 1:
                    _sleep_backoff(settings)
                    continue
                return _error_result(
                    provider=provider_name,
                    model=model,
                    status="TIMEOUT",
                    started=started,
                    attempts=attempts,
                    message=last_error,
                )
            except Exception as exc:
                last_error = str(exc)
                if attempt < max_attempts - 1:
                    _sleep_backoff(settings)
                    continue
                return _error_result(
                    provider=provider_name,
                    model=model,
                    status="PROVIDER_ERROR",
                    started=started,
                    attempts=attempts,
                    message=last_error,
                )

            if response.status_code in TRANSIENT_STATUSES and attempt < max_attempts - 1:
                last_error = _http_error_message(response)
                _sleep_backoff(settings)
                continue
            if (
                response.status_code == 400
                and "temperature" in payload
                and _is_unsupported_temperature_error(response)
                and compatibility_retries_remaining > 0
            ):
                last_error = _http_error_message(response)
                payload = _payload_without_temperature(payload)
                compatibility_retries_remaining -= 1
                _sleep_backoff(settings)
                continue
            if (
                response.status_code == 400
                and _is_invalid_json_schema_response_format_error(response)
                and _uses_json_schema_response_format(payload)
                and compatibility_retries_remaining > 0
            ):
                last_error = _http_error_message(response)
                payload = _payload_with_json_object_response_format(payload)
                compatibility_retries_remaining -= 1
                _sleep_backoff(settings)
                continue
            if response.status_code >= 500:
                return _error_result(
                    provider=provider_name,
                    model=model,
                    status="PROVIDER_ERROR",
                    started=started,
                    attempts=attempts,
                    message=_http_error_message(response),
                    request_id=_request_id(response),
                )
            if response.status_code >= 400:
                return _error_result(
                    provider=provider_name,
                    model=model,
                    status="CONFIG_OR_REQUEST_ERROR",
                    started=started,
                    attempts=attempts,
                    message=_http_error_message(response),
                    request_id=_request_id(response),
                )
            return _success_result(
                response,
                provider=provider_name,
                model=model,
                started=started,
                attempts=attempts,
                settings=settings,
            )

        return _error_result(
            provider=provider_name,
            model=model,
            status="PROVIDER_ERROR",
            started=started,
            attempts=attempts,
            message=last_error or "external LLM request failed",
        )

    def score(self, context: Mapping[str, Any], *, settings: Settings) -> object:
        raise CandidateScorerProviderError("external_http requires CandidateScoringContext")


class HttpxExternalHttpTransport:
    def post_json(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        payload: Mapping[str, Any],
        timeout_seconds: float,
    ) -> ExternalHttpTransportResponse:
        try:
            with httpx.Client(timeout=timeout_seconds) as client:
                response = client.post(url, headers=dict(headers), json=normalize_value(payload))
        except httpx.TimeoutException as exc:
            raise TimeoutError(str(exc)) from exc
        json_body: Mapping[str, Any] | None = None
        try:
            parsed = response.json()
        except ValueError:
            parsed = None
        if isinstance(parsed, Mapping):
            json_body = parsed
        return ExternalHttpTransportResponse(
            status_code=response.status_code,
            text=response.text,
            headers=dict(response.headers),
            json_body=json_body,
        )


def _external_availability(
    settings: Settings,
    *,
    allow_external: bool,
) -> dict[str, str] | None:
    if not settings.ai_external_llm_enabled:
        return {"status": "DISABLED", "message": "AI_EXTERNAL_LLM_ENABLED is false"}
    if _provider_name(settings) in {"", "none", "mock"}:
        return {"status": "DISABLED", "message": "AI_EXTERNAL_LLM_PROVIDER is not external"}
    if not allow_external or not settings.ai_external_llm_allow_network:
        return {
            "status": "DISABLED_NETWORK",
            "message": "external LLM network access is not allowed",
        }
    if not _model_name(settings):
        return {"status": "CONFIG_ERROR", "message": "AI external LLM model is not configured"}
    if not settings.ai_external_llm_api_key_env.strip():
        return {
            "status": "CONFIG_ERROR",
            "message": "AI external LLM API key env is not configured",
        }
    if not os.environ.get(settings.ai_external_llm_api_key_env, "").strip():
        return {"status": "CONFIG_ERROR", "message": "AI external LLM API key is missing"}
    if not _base_url(settings):
        return {"status": "CONFIG_ERROR", "message": "AI external LLM base URL is not configured"}
    if settings.ai_external_llm_per_run_call_limit < 1:
        return {"status": "CONFIG_ERROR", "message": "AI external LLM per-run call limit is zero"}
    return None


def _request_payload(prompt: CandidatePrompt, *, settings: Settings) -> dict[str, Any]:
    schema = get_candidate_scorer_output_schema()
    response_format: dict[str, Any]
    if settings.ai_external_llm_require_json_schema:
        response_format = {
            "type": "json_schema",
            "json_schema": {
                "name": "ai_candidate_scorer_advisory",
                "strict": True,
                "schema": schema,
            },
        }
    else:
        response_format = {"type": "json_object"}
    return {
        "model": _model_name(settings),
        "temperature": settings.ai_external_llm_temperature,
        "messages": [
            {"role": "system", "content": prompt.system_prompt},
            {"role": "user", "content": prompt.user_prompt},
        ],
        "response_format": response_format,
        "store": False,
    }


def _success_result(
    response: ExternalHttpTransportResponse,
    *,
    provider: str,
    model: str,
    started: float,
    attempts: int,
    settings: Settings,
) -> AiProviderRawResult:
    raw_body = (
        response.json_body
        if response.json_body is not None
        else _json_or_text(response.text)
    )
    raw_text = _extract_output_text(raw_body)
    if len(raw_text) > settings.ai_external_llm_max_response_chars:
        return _error_result(
            provider=provider,
            model=model,
            status="INVALID_SCHEMA",
            started=started,
            attempts=attempts,
            message="external LLM response exceeded AI_EXTERNAL_LLM_MAX_RESPONSE_CHARS",
            request_id=_request_id(response),
        )
    parsed_json = _parse_output_json(raw_text)
    if parsed_json is None and isinstance(raw_body, Mapping):
        parsed_json = _direct_advisory_json(raw_body)
    if parsed_json is None:
        return _error_result(
            provider=provider,
            model=model,
            status="INVALID_SCHEMA",
            started=started,
            attempts=attempts,
            message="external LLM response did not contain advisory JSON",
            request_id=_request_id(response),
        )
    return AiProviderRawResult(
        provider=provider,
        model=model,
        status="COMPLETED",
        raw_text=raw_text,
        parsed_json=parsed_json,
        latency_ms=_elapsed_ms(started),
        token_usage=_usage(raw_body),
        finish_reason=_finish_reason(raw_body),
        request_id=_request_id(response) or _raw_request_id(raw_body),
        external_call_enabled=True,
        external_call_attempted=True,
        attempts=attempts,
    )


def _error_result(
    *,
    provider: str,
    model: str,
    status: str,
    started: float,
    attempts: int,
    message: str,
    request_id: str | None = None,
) -> AiProviderRawResult:
    return AiProviderRawResult(
        provider=provider,
        model=model,
        status=status,
        latency_ms=_elapsed_ms(started),
        error_message=_sanitize_error_message(message),
        request_id=request_id,
        external_call_enabled=True,
        external_call_attempted=status != "DISABLED_NETWORK",
        attempts=attempts,
    )


def _extract_output_text(raw_body: object) -> str:
    if isinstance(raw_body, str):
        return raw_body.strip()
    if not isinstance(raw_body, Mapping):
        return json.dumps(normalize_value(raw_body), ensure_ascii=False, sort_keys=True)
    output_text = raw_body.get("output_text")
    if isinstance(output_text, str) and output_text.strip():
        return output_text.strip()
    choices = raw_body.get("choices")
    if isinstance(choices, list) and choices:
        first = choices[0]
        if isinstance(first, Mapping):
            message = first.get("message")
            if isinstance(message, Mapping):
                content = message.get("content")
                if isinstance(content, str):
                    return content.strip()
                if isinstance(content, list):
                    text_parts = []
                    for item in content:
                        if isinstance(item, Mapping) and isinstance(item.get("text"), str):
                            text_parts.append(str(item["text"]))
                    if text_parts:
                        return "\n".join(text_parts).strip()
    return json.dumps(normalize_value(raw_body), ensure_ascii=False, sort_keys=True)


def _parse_output_json(raw_text: str) -> Mapping[str, Any] | None:
    if not raw_text:
        return None
    try:
        loaded = json.loads(raw_text)
    except json.JSONDecodeError:
        start = raw_text.find("{")
        end = raw_text.rfind("}")
        if start < 0 or end <= start:
            return None
        try:
            loaded = json.loads(raw_text[start : end + 1])
        except json.JSONDecodeError:
            return None
    return normalize_value(loaded) if isinstance(loaded, Mapping) else None


def _direct_advisory_json(raw_body: Mapping[str, Any]) -> Mapping[str, Any] | None:
    required = {"selected", "analysis", "score", "confidence", "risk_reward"}
    if required.issubset(set(raw_body)):
        return normalize_value(raw_body)
    return None


def _json_or_text(text: str) -> object:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def _usage(raw_body: object) -> Mapping[str, Any] | None:
    if isinstance(raw_body, Mapping) and isinstance(raw_body.get("usage"), Mapping):
        return normalize_value(raw_body["usage"])
    return None


def _finish_reason(raw_body: object) -> str | None:
    if not isinstance(raw_body, Mapping):
        return None
    choices = raw_body.get("choices")
    if isinstance(choices, list) and choices:
        first = choices[0]
        if isinstance(first, Mapping) and first.get("finish_reason") is not None:
            return str(first["finish_reason"])
    return None


def _request_id(response: ExternalHttpTransportResponse) -> str | None:
    for key in ("x-request-id", "x-openai-request-id", "request-id"):
        value = response.headers.get(key)
        if value:
            return str(value)
    return None


def _raw_request_id(raw_body: object) -> str | None:
    if isinstance(raw_body, Mapping):
        for key in ("id", "request_id"):
            value = raw_body.get(key)
            if value:
                return str(value)
    return None


def _http_error_message(response: ExternalHttpTransportResponse) -> str:
    text = response.text.strip()
    if len(text) > 500:
        text = text[:500]
    return f"external LLM HTTP {response.status_code}: {text}"


def _is_unsupported_temperature_error(response: ExternalHttpTransportResponse) -> bool:
    text = response.text.lower()
    if "temperature" in text and "unsupported" in text:
        return True
    body = response.json_body
    if not isinstance(body, Mapping):
        return False
    error = body.get("error")
    if not isinstance(error, Mapping):
        return False
    return str(error.get("param") or "").lower() == "temperature" and (
        str(error.get("code") or "").lower() == "unsupported_value"
        or "unsupported" in str(error.get("message") or "").lower()
    )


def _payload_without_temperature(payload: Mapping[str, Any]) -> dict[str, Any]:
    updated = dict(payload)
    updated.pop("temperature", None)
    return updated


def _is_invalid_json_schema_response_format_error(
    response: ExternalHttpTransportResponse,
) -> bool:
    text = response.text.lower()
    if "response_format" in text and "invalid schema" in text:
        return True
    body = response.json_body
    if not isinstance(body, Mapping):
        return False
    error = body.get("error")
    if not isinstance(error, Mapping):
        return False
    return str(error.get("param") or "").lower() == "response_format" and (
        "invalid schema" in str(error.get("message") or "").lower()
        or "json schema" in str(error.get("message") or "").lower()
    )


def _uses_json_schema_response_format(payload: Mapping[str, Any]) -> bool:
    response_format = payload.get("response_format")
    return (
        isinstance(response_format, Mapping)
        and str(response_format.get("type") or "").lower() == "json_schema"
    )


def _payload_with_json_object_response_format(payload: Mapping[str, Any]) -> dict[str, Any]:
    updated = dict(payload)
    updated["response_format"] = {"type": "json_object"}
    return updated


def _sanitize_error_message(value: str) -> str:
    text = value.strip()
    for secret in ("authorization", "api_key", "apikey", "token", "bearer"):
        text = text.replace(secret, "[redacted]")
        text = text.replace(secret.upper(), "[redacted]")
    if len(text) > 500:
        return text[:500]
    return text


def _provider_name(settings: Settings) -> str:
    provider = settings.ai_external_llm_provider.strip().lower()
    if provider in {"openai", "external"}:
        return "external_http"
    return provider or "none"


def _model_name(settings: Settings) -> str:
    return settings.ai_external_llm_model.strip() or settings.ai_candidate_scorer_model.strip()


def _base_url(settings: Settings) -> str:
    configured = settings.ai_external_llm_base_url.strip()
    if configured:
        return configured
    if settings.ai_external_llm_provider.strip().lower() == "openai":
        return DEFAULT_OPENAI_CHAT_COMPLETIONS_URL
    return ""


def _sleep_backoff(settings: Settings) -> None:
    if settings.ai_external_llm_retry_backoff_seconds > 0:
        time.sleep(settings.ai_external_llm_retry_backoff_seconds)


def _elapsed_ms(started: float) -> float:
    return round((perf_counter() - started) * 1000.0, 3)
