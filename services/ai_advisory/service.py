from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from collections.abc import Mapping
from typing import Any

from domain.broker.utils import new_message_id, normalize_value

from services.ai_advisory.context_builder import (
    build_candidate_scoring_context,
    build_candidate_scoring_prompt,
)
from services.ai_advisory.models import AiCandidateScoringResult
from services.ai_advisory.scorer import (
    CandidateScorerProvider,
    CandidateScorerProviderError,
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
    settings: Settings | None = None,
) -> AiCandidateScoringResult:
    resolved_settings = settings or load_settings()
    run_id = new_message_id("AI-RUN")
    provider_name = resolved_settings.ai_candidate_scorer_provider
    model = resolved_settings.ai_candidate_scorer_model

    if not resolved_settings.ai_candidate_scorer_enabled:
        result = AiCandidateScoringResult(
            ok=True,
            run_id=run_id,
            status="DISABLED",
            trade_date=trade_date,
            provider=provider_name,
            model=model,
            summary="AI Candidate Scorer is disabled.",
            dry_run=dry_run,
        )
        if not dry_run:
            save_scoring_run(
                connection,
                run_id=run_id,
                trade_date=trade_date,
                provider=provider_name,
                model=model,
                status="DISABLED",
                candidate_count=0,
                selected_count=0,
                prompt_hash=None,
                raw_response_hash=None,
                summary=result.summary,
                no_trade_reason=None,
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
    selected_provider = provider or get_candidate_scorer_provider(resolved_settings)
    provider_name = getattr(selected_provider, "name", provider_name)

    try:
        start = time.monotonic()
        raw_response = selected_provider.score(context_dict, settings=resolved_settings)
        elapsed = time.monotonic() - start
        if elapsed > resolved_settings.ai_candidate_scorer_timeout_seconds:
            raise TimeoutError("AI Candidate Scorer provider timeout")
    except TimeoutError as exc:
        return _finish_failure(
            connection,
            run_id=run_id,
            status="TIMEOUT",
            trade_date=context.trade_date,
            provider=provider_name,
            model=model,
            candidate_count=len(candidate_codes),
            prompt_hash=prompt.prompt_hash,
            error_message=str(exc),
            error_type="TIMEOUT",
            context=context_dict,
            prompt=prompt.to_dict() if dry_run else {},
            dry_run=dry_run,
        )
    except CandidateScorerProviderError as exc:
        return _finish_failure(
            connection,
            run_id=run_id,
            status="FAILED",
            trade_date=context.trade_date,
            provider=provider_name,
            model=model,
            candidate_count=len(candidate_codes),
            prompt_hash=prompt.prompt_hash,
            error_message=str(exc),
            error_type="PROVIDER_FAILED",
            context=context_dict,
            prompt=prompt.to_dict() if dry_run else {},
            dry_run=dry_run,
        )
    except Exception as exc:
        return _finish_failure(
            connection,
            run_id=run_id,
            status="FAILED",
            trade_date=context.trade_date,
            provider=provider_name,
            model=model,
            candidate_count=len(candidate_codes),
            prompt_hash=prompt.prompt_hash,
            error_message=str(exc),
            error_type="PROVIDER_FAILED",
            context=context_dict,
            prompt=prompt.to_dict() if dry_run else {},
            dry_run=dry_run,
        )

    raw_response_hash = _hash_raw(raw_response)
    try:
        validated = validate_advisory_output(
            raw_response,
            candidate_codes=candidate_codes,
            settings=resolved_settings,
        )
    except AdvisoryValidationError as exc:
        raw_json = _try_raw_json(raw_response)
        return _finish_failure(
            connection,
            run_id=run_id,
            status="INVALID_SCHEMA",
            trade_date=context.trade_date,
            provider=provider_name,
            model=model,
            candidate_count=len(candidate_codes),
            prompt_hash=prompt.prompt_hash,
            raw_response_hash=raw_response_hash,
            validation_error=str(exc),
            error_type="INVALID_SCHEMA",
            raw_response_json=(
                _sanitize_raw_response(raw_json)
                if resolved_settings.ai_candidate_scorer_store_raw_response
                else None
            ),
            context=context_dict,
            prompt=prompt.to_dict() if dry_run else {},
            dry_run=dry_run,
        )

    advisory = validated.to_dict()
    selected_count = len(advisory["selected"])
    if not dry_run:
        raw_json = parse_advisory_json(raw_response)
        save_scoring_run(
            connection,
            run_id=run_id,
            trade_date=context.trade_date,
            provider=provider_name,
            model=model,
            status="COMPLETED",
            candidate_count=len(candidate_codes),
            selected_count=selected_count,
            prompt_hash=prompt.prompt_hash,
            raw_response_hash=raw_response_hash,
            summary=advisory.get("summary"),
            no_trade_reason=advisory.get("no_trade_reason"),
            raw_response_json=(
                _sanitize_raw_response(raw_json)
                if resolved_settings.ai_candidate_scorer_store_raw_response
                else None
            ),
        )
        save_candidate_scores(
            connection,
            run_id=run_id,
            candidates=context_dict["candidates"],
            advisory=advisory,
        )
        save_risk_reward_suggestions(
            connection,
            run_id=run_id,
            candidates=context_dict["candidates"],
            advisory=advisory,
        )
        connection.commit()

    return AiCandidateScoringResult(
        ok=True,
        run_id=run_id,
        status="COMPLETED",
        trade_date=context.trade_date,
        provider=provider_name,
        model=model,
        candidate_count=len(candidate_codes),
        selected_count=selected_count,
        prompt_hash=prompt.prompt_hash,
        raw_response_hash=raw_response_hash,
        summary=advisory.get("summary"),
        no_trade_reason=advisory.get("no_trade_reason"),
        context=context_dict if dry_run else {},
        prompt=prompt.to_dict() if dry_run else {},
        advisory=advisory,
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
    error_message: str | None = None,
    validation_error: str | None = None,
    raw_response_hash: str | None = None,
    raw_response_json: Mapping[str, Any] | None = None,
    context: Mapping[str, Any] | None = None,
    prompt: Mapping[str, Any] | None = None,
    dry_run: bool = False,
) -> AiCandidateScoringResult:
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
            error_message=error_message,
            validation_error=validation_error,
            raw_response_json=raw_response_json,
        )
        save_advisory_error(
            connection,
            run_id=run_id,
            trade_date=trade_date,
            error_type=error_type,
            error_message=validation_error or error_message or status,
            payload={"status": status, "fail_open": True},
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
        error_message=error_message,
        validation_error=validation_error,
        context=dict(context or {}) if dry_run else {},
        prompt=dict(prompt or {}) if dry_run else {},
        dry_run=dry_run,
    )


def _try_raw_json(raw_response: object) -> dict[str, Any] | None:
    try:
        return parse_advisory_json(raw_response)
    except AdvisoryValidationError:
        return None


def _sanitize_raw_response(raw_response: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if raw_response is None:
        return None
    return _remove_sensitive(normalize_value(raw_response))


def _remove_sensitive(value: Any) -> Any:
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, item in value.items():
            if str(key).lower() in {"account_id", "account_no", "account_number"}:
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

