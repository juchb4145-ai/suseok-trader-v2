from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from typing import Any

from domain.broker.utils import validate_stock_code

from services.ai_advisory.models import RiskRewardBounds, ValidatedAdvisory, ValidatedRiskReward
from services.ai_advisory.schema import (
    FORBIDDEN_OUTPUT_TOKENS,
    MAX_ANALYSIS_CHARS,
    MAX_AVOID_CHARS,
    MAX_FLAG_CHARS,
    MAX_SUMMARY_CHARS,
    REQUIRED_OUTPUT_FIELDS,
)
from services.config import Settings


class AdvisoryValidationError(ValueError):
    """Raised when a candidate scorer response violates the advisory schema."""


def risk_reward_bounds_from_settings(settings: Settings) -> RiskRewardBounds:
    return RiskRewardBounds(
        stop_loss_min=settings.ai_candidate_risk_reward_stop_loss_min,
        stop_loss_max=settings.ai_candidate_risk_reward_stop_loss_max,
        take_profit_min=settings.ai_candidate_risk_reward_take_profit_min,
        take_profit_max=settings.ai_candidate_risk_reward_take_profit_max,
        trailing_min=settings.ai_candidate_risk_reward_trailing_min,
        trailing_max=settings.ai_candidate_risk_reward_trailing_max,
        max_hold_min_sec=settings.ai_candidate_risk_reward_max_hold_min_sec,
        max_hold_max_sec=settings.ai_candidate_risk_reward_max_hold_max_sec,
    )


def parse_advisory_json(raw_response: object) -> dict[str, Any]:
    if isinstance(raw_response, Mapping):
        return dict(raw_response)
    if not isinstance(raw_response, str):
        raise AdvisoryValidationError("AI response must be a JSON object or JSON string")

    text = raw_response.strip()
    if not text:
        raise AdvisoryValidationError("AI response is empty")
    try:
        loaded = json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start < 0 or end <= start:
            raise AdvisoryValidationError("AI response is not valid JSON") from None
        try:
            loaded = json.loads(text[start : end + 1])
        except json.JSONDecodeError as exc:
            raise AdvisoryValidationError("AI response JSON recovery failed") from exc

    if not isinstance(loaded, Mapping):
        raise AdvisoryValidationError("AI response root must be an object")
    return dict(loaded)


def validate_advisory_output(
    raw_response: object,
    *,
    candidate_codes: Sequence[str],
    settings: Settings,
) -> ValidatedAdvisory:
    candidate_set = {validate_stock_code(code) for code in candidate_codes}
    data = parse_advisory_json(raw_response)
    _reject_forbidden_tokens(data)
    _validate_root_schema(data)

    warnings: list[str] = []
    selected = _selected(data["selected"], candidate_set, warnings)
    analysis = _string_map(
        data["analysis"],
        candidate_set,
        warnings,
        field_name="analysis",
        max_chars=MAX_ANALYSIS_CHARS,
    )
    score = _score_map(data["score"], candidate_set, warnings, field_name="score")
    confidence = _score_map(
        data["confidence"],
        candidate_set,
        warnings,
        field_name="confidence",
    )
    risk_reward = _risk_reward_map(
        data["risk_reward"],
        candidate_set,
        warnings,
        risk_reward_bounds_from_settings(settings),
    )
    candidate_flags = _flags_map(data["candidate_flags"], candidate_set, warnings)
    avoid = _string_map(
        data["avoid"],
        candidate_set,
        warnings,
        field_name="avoid",
        max_chars=MAX_AVOID_CHARS,
    )
    summary = _limited_string(data["summary"], "summary", MAX_SUMMARY_CHARS)
    no_trade_reason_value = data["no_trade_reason"]
    if no_trade_reason_value is None:
        no_trade_reason = None
    else:
        no_trade_reason = _limited_string(
            no_trade_reason_value,
            "no_trade_reason",
            MAX_SUMMARY_CHARS,
        )

    return ValidatedAdvisory(
        selected=tuple(selected),
        analysis=analysis,
        score=score,
        confidence=confidence,
        risk_reward=risk_reward,
        candidate_flags=candidate_flags,
        avoid=avoid,
        summary=summary,
        no_trade_reason=no_trade_reason,
        warnings=tuple(warnings),
    )


def _validate_root_schema(data: Mapping[str, Any]) -> None:
    missing = [field for field in REQUIRED_OUTPUT_FIELDS if field not in data]
    if missing:
        raise AdvisoryValidationError(
            f"AI response missing required field(s): {', '.join(missing)}"
        )
    unknown = sorted(str(field) for field in data if field not in REQUIRED_OUTPUT_FIELDS)
    if unknown:
        raise AdvisoryValidationError(
            f"AI response contains unknown field(s): {', '.join(unknown)}"
        )


def _selected(value: object, candidate_set: set[str], warnings: list[str]) -> list[str]:
    if isinstance(value, str) or not isinstance(value, Sequence):
        raise AdvisoryValidationError("selected must be an array")
    selected: list[str] = []
    seen: set[str] = set()
    for item in value:
        code = _normalize_code_or_none(item)
        if code is None or code not in candidate_set:
            warnings.append(f"UNKNOWN_SELECTED_CODE:{item}")
            continue
        if code not in seen:
            selected.append(code)
            seen.add(code)
    return selected


def _string_map(
    value: object,
    candidate_set: set[str],
    warnings: list[str],
    *,
    field_name: str,
    max_chars: int,
) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise AdvisoryValidationError(f"{field_name} must be an object")
    result: dict[str, str] = {}
    for raw_code, raw_text in value.items():
        code = _normalize_code_or_none(raw_code)
        if code is None or code not in candidate_set:
            warnings.append(f"UNKNOWN_{field_name.upper()}_CODE:{raw_code}")
            continue
        result[code] = _limited_string(raw_text, f"{field_name}.{code}", max_chars)
    return result


def _score_map(
    value: object,
    candidate_set: set[str],
    warnings: list[str],
    *,
    field_name: str,
) -> dict[str, int]:
    if not isinstance(value, Mapping):
        raise AdvisoryValidationError(f"{field_name} must be an object")
    result: dict[str, int] = {}
    for raw_code, raw_score in value.items():
        code = _normalize_code_or_none(raw_code)
        if code is None or code not in candidate_set:
            warnings.append(f"UNKNOWN_{field_name.upper()}_CODE:{raw_code}")
            continue
        if isinstance(raw_score, bool) or not isinstance(raw_score, int | float):
            raise AdvisoryValidationError(f"{field_name}.{code} must be a number")
        if not math.isfinite(float(raw_score)):
            raise AdvisoryValidationError(f"{field_name}.{code} must be finite")
        rounded = int(round(float(raw_score)))
        if rounded < 0 or rounded > 100:
            raise AdvisoryValidationError(f"{field_name}.{code} must be between 0 and 100")
        result[code] = rounded
    return result


def _risk_reward_map(
    value: object,
    candidate_set: set[str],
    warnings: list[str],
    bounds: RiskRewardBounds,
) -> dict[str, ValidatedRiskReward]:
    if not isinstance(value, Mapping):
        raise AdvisoryValidationError("risk_reward must be an object")
    result: dict[str, ValidatedRiskReward] = {}
    for raw_code, raw_suggestion in value.items():
        code = _normalize_code_or_none(raw_code)
        if code is None or code not in candidate_set:
            warnings.append(f"UNKNOWN_RISK_REWARD_CODE:{raw_code}")
            continue
        if not isinstance(raw_suggestion, Mapping):
            raise AdvisoryValidationError(f"risk_reward.{code} must be an object")
        result[code] = _risk_reward_item(code, raw_suggestion, bounds)
    return result


def _risk_reward_item(
    code: str,
    value: Mapping[str, Any],
    bounds: RiskRewardBounds,
) -> ValidatedRiskReward:
    required = (
        "stop_loss_pct",
        "take_profit_pct",
        "trailing_stop_pct",
        "max_hold_sec",
    )
    missing = [field for field in required if field not in value]
    if missing:
        raise AdvisoryValidationError(
            f"risk_reward.{code} missing field(s): {', '.join(missing)}"
        )
    unknown = sorted(str(field) for field in value if field not in required)
    if unknown:
        raise AdvisoryValidationError(
            f"risk_reward.{code} contains unknown field(s): {', '.join(unknown)}"
        )

    stop_loss, stop_reasons = _clamp_float(
        value["stop_loss_pct"],
        bounds.stop_loss_min,
        bounds.stop_loss_max,
        "STOP_LOSS",
        f"risk_reward.{code}.stop_loss_pct",
    )
    take_profit, take_reasons = _clamp_float(
        value["take_profit_pct"],
        bounds.take_profit_min,
        bounds.take_profit_max,
        "TAKE_PROFIT",
        f"risk_reward.{code}.take_profit_pct",
    )
    trailing, trailing_reasons = _clamp_float(
        value["trailing_stop_pct"],
        bounds.trailing_min,
        bounds.trailing_max,
        "TRAILING_STOP",
        f"risk_reward.{code}.trailing_stop_pct",
    )
    max_hold, hold_reasons = _clamp_int(
        value["max_hold_sec"],
        bounds.max_hold_min_sec,
        bounds.max_hold_max_sec,
        "MAX_HOLD_SEC",
        f"risk_reward.{code}.max_hold_sec",
    )
    reasons = [*stop_reasons, *take_reasons, *trailing_reasons, *hold_reasons]
    return ValidatedRiskReward(
        stop_loss_pct=stop_loss,
        take_profit_pct=take_profit,
        trailing_stop_pct=trailing,
        max_hold_sec=max_hold,
        clamped=bool(reasons),
        clamp_reason_codes=tuple(reasons),
    )


def _flags_map(
    value: object,
    candidate_set: set[str],
    warnings: list[str],
) -> dict[str, tuple[str, ...]]:
    if not isinstance(value, Mapping):
        raise AdvisoryValidationError("candidate_flags must be an object")
    result: dict[str, tuple[str, ...]] = {}
    for raw_code, raw_flags in value.items():
        code = _normalize_code_or_none(raw_code)
        if code is None or code not in candidate_set:
            warnings.append(f"UNKNOWN_CANDIDATE_FLAGS_CODE:{raw_code}")
            continue
        if isinstance(raw_flags, str) or not isinstance(raw_flags, Sequence):
            raise AdvisoryValidationError(f"candidate_flags.{code} must be an array")
        flags: list[str] = []
        for raw_flag in raw_flags:
            flag = _limited_string(raw_flag, f"candidate_flags.{code}", MAX_FLAG_CHARS)
            flags.append(flag.strip().upper())
        result[code] = tuple(flags)
    return result


def _limited_string(value: object, field_name: str, max_chars: int) -> str:
    if not isinstance(value, str):
        raise AdvisoryValidationError(f"{field_name} must be a string")
    normalized = value.strip()
    if len(normalized) > max_chars:
        return normalized[:max_chars]
    return normalized


def _clamp_float(
    value: object,
    minimum: float,
    maximum: float,
    token: str,
    field_name: str,
) -> tuple[float, list[str]]:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise AdvisoryValidationError(f"{field_name} must be a number")
    parsed = float(value)
    if not math.isfinite(parsed):
        raise AdvisoryValidationError(f"{field_name} must be finite")
    reasons: list[str] = []
    if parsed < minimum:
        parsed = minimum
        reasons.append(f"{token}_CLAMPED_MIN")
    if parsed > maximum:
        parsed = maximum
        reasons.append(f"{token}_CLAMPED_MAX")
    return round(parsed, 4), reasons


def _clamp_int(
    value: object,
    minimum: int,
    maximum: int,
    token: str,
    field_name: str,
) -> tuple[int, list[str]]:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise AdvisoryValidationError(f"{field_name} must be an integer")
    parsed_float = float(value)
    if not math.isfinite(parsed_float):
        raise AdvisoryValidationError(f"{field_name} must be finite")
    parsed = int(round(parsed_float))
    reasons: list[str] = []
    if parsed < minimum:
        parsed = minimum
        reasons.append(f"{token}_CLAMPED_MIN")
    if parsed > maximum:
        parsed = maximum
        reasons.append(f"{token}_CLAMPED_MAX")
    return parsed, reasons


def _normalize_code_or_none(value: object) -> str | None:
    try:
        return validate_stock_code(value)
    except Exception:
        return None


def _reject_forbidden_tokens(value: object) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            _reject_forbidden_string(str(key))
            _reject_forbidden_tokens(item)
        return
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        for item in value:
            _reject_forbidden_tokens(item)
        return
    if isinstance(value, str):
        _reject_forbidden_string(value)


def _reject_forbidden_string(value: str) -> None:
    normalized = value.upper().replace(" ", "").replace("-", "_")
    for token in FORBIDDEN_OUTPUT_TOKENS:
        if token in normalized:
            raise AdvisoryValidationError(f"AI response contains forbidden trading action: {token}")
