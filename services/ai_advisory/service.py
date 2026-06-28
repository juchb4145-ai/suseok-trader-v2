from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from collections.abc import Mapping
from dataclasses import replace
from typing import Any

from domain.broker.utils import new_message_id, normalize_value

from services.ai_advisory.context_builder import (
    build_candidate_scoring_context,
    build_candidate_scoring_prompt,
)
from services.ai_advisory.models import (
    AiCandidateScoringResult,
    AiProviderRawResult,
    CandidatePrompt,
    CandidateScoringContext,
)
from services.ai_advisory.providers.mock import MockCandidateScorerProvider
from services.ai_advisory.scorer import (
    CandidateScorerProvider,
    CandidateScorerProviderError,
    ExternalHttpTransport,
    get_candidate_scorer_provider,
)
from services.ai_advisory.storage import (
    save_advisory_error,
    save_candidate_scores,
    save_risk_reward_suggestions,
    save_scoring_run,
)
from services.ai_advisory.validator import (
    AdvisoryValidationError,
    parse_advisory_json,
    validate_advisory_output,
)
from services.config import Settings, load_settings


def score_ai_candidates(
    connection: sqlite3.Connection,
    *,
    trade_date: str | None = None,
    limit: int | None = None,
    dry_run: bool = False,
    provider: CandidateScorerProvider | None = None,
    provider_name: str | None = None,
    allow_external: bool = False,
    store_raw_response: bool | None = None,
    transport: ExternalHttpTransport | None = None,
    settings: Settings | None = None,
) -> AiCandidateScoringResult:
    resolved_settings = _runtime_settings(
        settings or load_settings(),
        provider_name=provider_name,
        store_raw_response=store_raw_response,
    )
    run_id = new_message_id("AI-RUN")
    configured_provider_name = resolved_settings.ai_candidate_scorer_provider
    configured_model = _configured_model(resolved_settings)

    if not resolved_settings.ai_candidate_scorer_enabled:
        result = AiCandidateScoringResult(
            ok=True,
            run_id=run_id,
            status="DISABLED",
            trade_date=trade_date,
            provider=configured_provider_name,
            model=configured_model,
            summary="AI Candidate Scorer is disabled.",
            dry_run=dry_run,
            prompt_redacted=True,
        )
        if not dry_run:
            save_scoring_run(
                connection,
                run_id=run_id,
                trade_date=trade_date,
                provider=configured_provider_name,
                model=configured_model,
                status="DISABLED",
                candidate_count=0,
                selected_count=0,
                prompt_hash=None,
                raw_response_hash=None,
                summary=result.summary,
                no_trade_reason=None,
                prompt_redacted=True,
            )
            connection.commit()
        return result

    context = build_candidate_scoring_context(
        connection,
        trade_date=trade_date,
        limit=limit,
        settings=resolved_settings,
    )
    prompt = build_candidate_scoring_prompt(context, settings=resolved_settings)
    context_dict = context.to_dict()
    candidate_codes = [str(candidate["code"]) for candidate in context_dict["candidates"]]
    selected_provider = provider or get_candidate_scorer_provider(
        resolved_settings,
        allow_external=allow_external,
        transport=transport,
    )
    provider_label = getattr(selected_provider, "name", configured_provider_name)

    if _is_external_provider_name(provider_label) and not candidate_codes:
        mock_result = MockCandidateScorerProvider().score_candidates(
            context,
            prompt=prompt,
            settings=resolved_settings,
        )
        metadata_result = AiProviderRawResult(
            provider=provider_label,
            model=configured_model,
            status="COMPLETED",
            external_call_enabled=True,
            external_call_attempted=False,
        )
        return _finish_validating_provider_result(
            connection,
            run_id=run_id,
            provider_result=mock_result,
            metadata_result=metadata_result,
            trade_date=context.trade_date,
            candidate_codes=candidate_codes,
            context=context,
            prompt=prompt,
            settings=resolved_settings,
            dry_run=dry_run,
            fallback_provider=None,
            fallback_error_category=None,
            allow_validation_fallback=False,
        )

    if (
        provider is None
        and _is_external_provider_name(provider_label)
        and _daily_limit_exceeded(connection, resolved_settings)
    ):
        provider_result = AiProviderRawResult(
            provider=provider_label,
            model=configured_model,
            status="COST_GUARD_LIMIT",
            error_message="AI external LLM daily call limit reached",
            external_call_enabled=True,
            external_call_attempted=False,
        )
        return _fallback_or_failure(
            connection,
            run_id=run_id,
            external_result=provider_result,
            status="COST_GUARD_LIMIT",
            trade_date=context.trade_date,
            candidate_codes=candidate_codes,
            context=context,
            prompt=prompt,
            settings=resolved_settings,
            dry_run=dry_run,
            allow_fallback=provider is None,
        )

    try:
        provider_result = _invoke_provider(
            selected_provider,
            context,
            prompt=prompt,
            context_dict=context_dict,
            settings=resolved_settings,
            provider_label=provider_label,
        )
    except TimeoutError as exc:
        provider_result = AiProviderRawResult(
            provider=provider_label,
            model=configured_model,
            status="TIMEOUT",
            error_message=str(exc),
            external_call_enabled=_is_external_provider_name(provider_label),
            external_call_attempted=_is_external_provider_name(provider_label),
        )
    except CandidateScorerProviderError as exc:
        provider_result = AiProviderRawResult(
            provider=provider_label,
            model=configured_model,
            status="PROVIDER_ERROR",
            error_message=str(exc),
            external_call_enabled=_is_external_provider_name(provider_label),
            external_call_attempted=False,
        )
    except Exception as exc:
        provider_result = AiProviderRawResult(
            provider=provider_label,
            model=configured_model,
            status="PROVIDER_ERROR",
            error_message=str(exc),
            external_call_enabled=_is_external_provider_name(provider_label),
            external_call_attempted=_is_external_provider_name(provider_label),
        )

    if provider_result.status != "COMPLETED":
        return _fallback_or_failure(
            connection,
            run_id=run_id,
            external_result=provider_result,
            status=provider_result.status,
            trade_date=context.trade_date,
            candidate_codes=candidate_codes,
            context=context,
            prompt=prompt,
            settings=resolved_settings,
            dry_run=dry_run,
            allow_fallback=(
                provider is None and _is_external_provider_name(provider_result.provider)
            ),
        )

    return _finish_validating_provider_result(
        connection,
        run_id=run_id,
        provider_result=provider_result,
        metadata_result=provider_result,
        trade_date=context.trade_date,
        candidate_codes=candidate_codes,
        context=context,
        prompt=prompt,
        settings=resolved_settings,
        dry_run=dry_run,
        fallback_provider=None,
        fallback_error_category=None,
        allow_validation_fallback=provider is None
        and _is_external_provider_name(provider_result.provider),
    )


def _finish_validating_provider_result(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    provider_result: AiProviderRawResult,
    metadata_result: AiProviderRawResult,
    trade_date: str | None,
    candidate_codes: list[str],
    context: CandidateScoringContext,
    prompt: CandidatePrompt,
    settings: Settings,
    dry_run: bool,
    fallback_provider: str | None,
    fallback_error_category: str | None,
    allow_validation_fallback: bool,
    fallback_error_message: str | None = None,
) -> AiCandidateScoringResult:
    raw_payload = provider_result.raw_payload()
    raw_response_hash = _hash_raw(
        metadata_result.raw_payload() if metadata_result.raw_payload() else raw_payload
    )
    try:
        validated = validate_advisory_output(
            raw_payload,
            candidate_codes=candidate_codes,
            settings=settings,
        )
    except AdvisoryValidationError as exc:
        if allow_validation_fallback:
            failed_result = AiProviderRawResult(
                provider=metadata_result.provider,
                model=metadata_result.model,
                status="INVALID_SCHEMA",
                raw_text=metadata_result.raw_text,
                parsed_json=metadata_result.parsed_json,
                latency_ms=metadata_result.latency_ms,
                token_usage=metadata_result.token_usage,
                error_message=str(exc),
                finish_reason=metadata_result.finish_reason,
                request_id=metadata_result.request_id,
                external_call_enabled=metadata_result.external_call_enabled,
                external_call_attempted=metadata_result.external_call_attempted,
                attempts=metadata_result.attempts,
            )
            return _fallback_or_failure(
                connection,
                run_id=run_id,
                external_result=failed_result,
                status="INVALID_SCHEMA",
                trade_date=trade_date,
                candidate_codes=candidate_codes,
                context=context,
                prompt=prompt,
                settings=settings,
                dry_run=dry_run,
                allow_fallback=True,
                validation_error=str(exc),
            )
        raw_json = _try_raw_json(raw_payload)
        return _finish_failure(
            connection,
            run_id=run_id,
            status="INVALID_SCHEMA",
            trade_date=trade_date,
            provider=metadata_result.provider,
            model=metadata_result.model,
            candidate_count=len(candidate_codes),
            prompt_hash=prompt.prompt_hash,
            error_type="INVALID_SCHEMA",
            raw_response_hash=raw_response_hash,
            validation_error=str(exc),
            raw_response_json=_raw_response_to_store(raw_json, settings, metadata_result),
            metadata_result=metadata_result,
            prompt=prompt,
            context=context.to_dict(),
            dry_run=dry_run,
        )

    advisory = validated.to_dict()
    selected_count = len(advisory["selected"])
    raw_json = _try_raw_json(raw_payload)
    raw_response_json = _raw_response_to_store(raw_json, settings, metadata_result)
    raw_response_stored = raw_response_json is not None
    if not dry_run:
        save_scoring_run(
            connection,
            run_id=run_id,
            trade_date=trade_date,
            provider=metadata_result.provider,
            model=metadata_result.model,
            status="COMPLETED",
            candidate_count=len(candidate_codes),
            selected_count=selected_count,
            prompt_hash=prompt.prompt_hash,
            raw_response_hash=raw_response_hash,
            summary=advisory.get("summary"),
            no_trade_reason=advisory.get("no_trade_reason"),
            raw_response_json=raw_response_json,
            external_call_enabled=metadata_result.external_call_enabled,
            external_call_attempted=metadata_result.external_call_attempted,
            latency_ms=metadata_result.latency_ms,
            request_id=metadata_result.request_id,
            token_usage_json=metadata_result.token_usage,
            raw_response_stored=raw_response_stored,
            prompt_redacted=True,
            prompt_truncated=prompt.truncated,
            error_category=fallback_error_category,
            error_message=fallback_error_message,
            fallback_provider=fallback_provider,
        )
        save_candidate_scores(
            connection,
            run_id=run_id,
            candidates=context.to_dict()["candidates"],
            advisory=advisory,
        )
        save_risk_reward_suggestions(
            connection,
            run_id=run_id,
            candidates=context.to_dict()["candidates"],
            advisory=advisory,
        )
        connection.commit()

    return AiCandidateScoringResult(
        ok=True,
        run_id=run_id,
        status="COMPLETED",
        trade_date=trade_date,
        provider=metadata_result.provider,
        model=metadata_result.model,
        candidate_count=len(candidate_codes),
        selected_count=selected_count,
        prompt_hash=prompt.prompt_hash,
        raw_response_hash=raw_response_hash,
        summary=advisory.get("summary"),
        no_trade_reason=advisory.get("no_trade_reason"),
        error_message=fallback_error_message,
        context=context.to_dict() if dry_run else {},
        prompt=prompt.to_dict() if dry_run else {},
        advisory=advisory,
        dry_run=dry_run,
        latency_ms=metadata_result.latency_ms,
        request_id=metadata_result.request_id,
        token_usage=dict(metadata_result.token_usage or {}),
        external_call_enabled=metadata_result.external_call_enabled,
        external_call_attempted=metadata_result.external_call_attempted,
        fallback_provider=fallback_provider,
        error_category=fallback_error_category,
        raw_response_stored=raw_response_stored,
        prompt_redacted=True,
        prompt_truncated=prompt.truncated,
    )


def _fallback_or_failure(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    external_result: AiProviderRawResult,
    status: str,
    trade_date: str | None,
    candidate_codes: list[str],
    context: CandidateScoringContext,
    prompt: CandidatePrompt,
    settings: Settings,
    dry_run: bool,
    allow_fallback: bool,
    validation_error: str | None = None,
) -> AiCandidateScoringResult:
    error_message = validation_error or external_result.error_message or status
    if not dry_run and allow_fallback:
        save_advisory_error(
            connection,
            run_id=run_id,
            trade_date=trade_date,
            error_type=status,
            error_message=_sanitize_error_message(error_message),
            payload={
                "status": status,
                "fail_open": True,
                "provider": external_result.provider,
                "request_id": external_result.request_id,
                "fallback_provider": "mock" if allow_fallback else None,
            },
        )
        connection.commit()

    if allow_fallback and settings.ai_candidate_scorer_fail_open:
        mock_result = MockCandidateScorerProvider().score_candidates(
            context,
            prompt=prompt,
            settings=settings,
        )
        return _finish_validating_provider_result(
            connection,
            run_id=run_id,
            provider_result=mock_result,
            metadata_result=external_result,
            trade_date=trade_date,
            candidate_codes=candidate_codes,
            context=context,
            prompt=prompt,
            settings=settings,
            dry_run=dry_run,
            fallback_provider="mock",
            fallback_error_category=status,
            fallback_error_message=_sanitize_error_message(error_message),
            allow_validation_fallback=False,
        )

    return _finish_failure(
        connection,
        run_id=run_id,
        status=status,
        trade_date=trade_date,
        provider=external_result.provider,
        model=external_result.model,
        candidate_count=len(candidate_codes),
        prompt_hash=prompt.prompt_hash,
        error_type=status,
        error_message=external_result.error_message,
        validation_error=validation_error,
        raw_response_hash=_hash_raw(external_result.raw_payload())
        if external_result.raw_payload()
        else None,
        raw_response_json=_raw_response_to_store(
            _try_raw_json(external_result.raw_payload()),
            settings,
            external_result,
        ),
        metadata_result=external_result,
        prompt=prompt,
        context=context.to_dict(),
        dry_run=dry_run,
    )


def _finish_failure(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    status: str,
    trade_date: str | None,
    provider: str,
    model: str,
    candidate_count: int,
    prompt_hash: str | None,
    error_type: str,
    metadata_result: AiProviderRawResult,
    prompt: CandidatePrompt | None,
    error_message: str | None = None,
    validation_error: str | None = None,
    raw_response_hash: str | None = None,
    raw_response_json: Mapping[str, Any] | None = None,
    context: Mapping[str, Any] | None = None,
    dry_run: bool = False,
) -> AiCandidateScoringResult:
    sanitized_error = _sanitize_error_message(validation_error or error_message or status)
    raw_response_stored = raw_response_json is not None
    if not dry_run:
        save_scoring_run(
            connection,
            run_id=run_id,
            trade_date=trade_date,
            provider=provider,
            model=model,
            status=status,
            candidate_count=candidate_count,
            selected_count=0,
            prompt_hash=prompt_hash,
            raw_response_hash=raw_response_hash,
            summary=None,
            no_trade_reason=None,
            error_message=_sanitize_error_message(error_message) if error_message else None,
            validation_error=_sanitize_error_message(validation_error)
            if validation_error
            else None,
            raw_response_json=raw_response_json,
            external_call_enabled=metadata_result.external_call_enabled,
            external_call_attempted=metadata_result.external_call_attempted,
            latency_ms=metadata_result.latency_ms,
            request_id=metadata_result.request_id,
            token_usage_json=metadata_result.token_usage,
            raw_response_stored=raw_response_stored,
            prompt_redacted=True,
            prompt_truncated=False if prompt is None else prompt.truncated,
            error_category=error_type,
        )
        save_advisory_error(
            connection,
            run_id=run_id,
            trade_date=trade_date,
            error_type=error_type,
            error_message=sanitized_error,
            payload={"status": status, "fail_open": True, "provider": provider},
        )
        connection.commit()
    return AiCandidateScoringResult(
        ok=False,
        run_id=run_id,
        status=status,
        trade_date=trade_date,
        provider=provider,
        model=model,
        candidate_count=candidate_count,
        prompt_hash=prompt_hash,
        raw_response_hash=raw_response_hash,
        error_message=_sanitize_error_message(error_message) if error_message else None,
        validation_error=_sanitize_error_message(validation_error)
        if validation_error
        else None,
        context=dict(context or {}) if dry_run else {},
        prompt=prompt.to_dict() if dry_run and prompt is not None else {},
        dry_run=dry_run,
        latency_ms=metadata_result.latency_ms,
        request_id=metadata_result.request_id,
        token_usage=dict(metadata_result.token_usage or {}),
        external_call_enabled=metadata_result.external_call_enabled,
        external_call_attempted=metadata_result.external_call_attempted,
        error_category=error_type,
        raw_response_stored=raw_response_stored,
        prompt_redacted=True,
        prompt_truncated=False if prompt is None else prompt.truncated,
    )


def _invoke_provider(
    provider: CandidateScorerProvider,
    context: CandidateScoringContext,
    *,
    prompt: CandidatePrompt,
    context_dict: Mapping[str, Any],
    settings: Settings,
    provider_label: str,
) -> AiProviderRawResult:
    started = time.monotonic()
    if hasattr(provider, "score_candidates"):
        result = provider.score_candidates(context, prompt=prompt, settings=settings)
    else:
        raw_response = provider.score(context_dict, settings=settings)
        result = AiProviderRawResult(
            provider=provider_label,
            model=_configured_model(settings),
            status="COMPLETED",
            raw_text=raw_response if isinstance(raw_response, str) else None,
            parsed_json=raw_response if isinstance(raw_response, Mapping) else None,
            latency_ms=round((time.monotonic() - started) * 1000.0, 3),
            external_call_enabled=_is_external_provider_name(provider_label),
            external_call_attempted=_is_external_provider_name(provider_label),
        )
    elapsed = time.monotonic() - started
    if elapsed > settings.ai_candidate_scorer_timeout_seconds:
        raise TimeoutError("AI Candidate Scorer provider timeout")
    return result


def _runtime_settings(
    settings: Settings,
    *,
    provider_name: str | None,
    store_raw_response: bool | None,
) -> Settings:
    updates: dict[str, Any] = {}
    if provider_name:
        normalized_provider = provider_name.strip().lower()
        if normalized_provider == "external":
            normalized_provider = "external_http"
        updates["ai_candidate_scorer_provider"] = normalized_provider
    if store_raw_response is not None:
        updates["ai_candidate_scorer_store_raw_response"] = bool(store_raw_response)
        updates["ai_external_llm_store_response"] = bool(store_raw_response)
    return replace(settings, **updates) if updates else settings


def _try_raw_json(raw_response: object) -> dict[str, Any] | None:
    try:
        return parse_advisory_json(raw_response)
    except AdvisoryValidationError:
        return None


def _raw_response_to_store(
    raw_response: Mapping[str, Any] | None,
    settings: Settings,
    metadata_result: AiProviderRawResult,
) -> dict[str, Any] | None:
    if raw_response is None:
        return None
    if _is_external_provider_name(metadata_result.provider):
        should_store = (
            settings.ai_candidate_scorer_store_raw_response
            and settings.ai_external_llm_store_response
        )
    else:
        should_store = settings.ai_candidate_scorer_store_raw_response
    if not should_store:
        return None
    return _sanitize_raw_response(raw_response)


def _sanitize_raw_response(raw_response: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if raw_response is None:
        return None
    return _remove_sensitive(normalize_value(raw_response))


def _remove_sensitive(value: Any) -> Any:
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, item in value.items():
            normalized_key = str(key).lower()
            if normalized_key in {"account_id", "account_no", "account_number"}:
                continue
            if "broker" in normalized_key and "account" in normalized_key:
                continue
            if "raw" in normalized_key and "payload" in normalized_key:
                continue
            if any(token in normalized_key for token in ("api_key", "apikey", "secret", "token")):
                continue
            result[str(key)] = _remove_sensitive(item)
        return result
    if isinstance(value, list | tuple):
        return [_remove_sensitive(item) for item in value]
    return value


def _hash_raw(value: object) -> str:
    if isinstance(value, str):
        text = value
    else:
        text = json.dumps(normalize_value(value), ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _sanitize_error_message(value: str | None) -> str:
    text = "" if value is None else str(value).strip()
    for sensitive in ("account_id", "account_no", "account_number", "api_key", "token", "secret"):
        text = text.replace(sensitive, "[redacted]")
        text = text.replace(sensitive.upper(), "[redacted]")
    if len(text) > 500:
        return text[:500]
    return text


def _is_external_provider_name(provider_name: str) -> bool:
    return provider_name.strip().lower() in {"external", "external_http", "openai"}


def _configured_model(settings: Settings) -> str:
    return settings.ai_external_llm_model or settings.ai_candidate_scorer_model


def _daily_limit_exceeded(connection: sqlite3.Connection, settings: Settings) -> bool:
    if not settings.ai_external_llm_cost_guard_enabled:
        return False
    if settings.ai_external_llm_daily_call_limit <= 0:
        return True
    row = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM ai_candidate_scoring_runs
        WHERE external_call_attempted = 1
            AND date(created_at) = date('now')
        """
    ).fetchone()
    count = int(row["count"] if row else 0)
    return count >= settings.ai_external_llm_daily_call_limit
