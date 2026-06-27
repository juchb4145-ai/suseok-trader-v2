from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any

from domain.broker.utils import normalize_value

MASKED = "***MASKED***"
SECRET_REDACTED = "***SECRET_REDACTED***"
PATH_REDACTED = "***PATH_REDACTED***"
ENV_REDACTED = "***ENV_REDACTED***"
HEADERS_REDACTED = "***HEADERS_REDACTED***"
NUMERIC_ID_REDACTED = "***NUMERIC_ID_REDACTED***"

_SECRET_KEYS = {
    "password",
    "passwd",
    "token",
    "apikey",
    "api_key",
    "secret",
    "authorization",
    "cookie",
    "xcoretoken",
    "xlocaltoken",
    "tradingcoretoken",
    "openaiapikey",
    "openai_api_key",
}
_ACCOUNT_KEYS = {
    "account",
    "accountid",
    "account_id",
    "accountno",
    "account_no",
}
_ENV_KEYS = {"env", "environ", "environment", "rawenv", "rawenviron", "rawenvironment"}
_HEADER_KEYS = {"headers", "rawheaders", "requestheaders", "responseheaders"}
_PRESERVE_NUMERIC_KEYS = {
    "code",
    "stockcode",
    "stock_code",
    "price",
    "volume",
    "cumulativevolume",
    "cumulative_volume",
    "tradevalue",
    "trade_value",
    "cumulativetradevalue",
    "cumulative_trade_value",
    "candidateinstanceid",
    "candidate_instance_id",
    "themeid",
    "theme_id",
    "strategyobservationid",
    "strategy_observation_id",
    "riskobservationid",
    "risk_observation_id",
}

_WINDOWS_PATH_RE = re.compile(r"(?i)\b[A-Z]:\\(?:Users|Windows|ProgramData|Temp)\\[^\s\"'<>]+")
_LINUX_PATH_RE = re.compile(r"(?<![\w/])/(?:home|Users|mnt/c|var|tmp)/[^\s\"'<>]+")
_LONG_NUMERIC_RE = re.compile(r"(?<!\d)\d{8,20}(?!\d)")


def redact_context(value: object) -> Any:
    return _redact_value(normalize_value(value), parent_key=None)


def redact_string(value: object, *, parent_key: str | None = None) -> str:
    text = str(value)
    canonical_key = _canonicalize(parent_key or "")
    if canonical_key in _SECRET_KEYS:
        return SECRET_REDACTED
    if canonical_key in _ACCOUNT_KEYS:
        return MASKED
    text = _WINDOWS_PATH_RE.sub(PATH_REDACTED, text)
    text = _LINUX_PATH_RE.sub(PATH_REDACTED, text)
    if canonical_key not in _PRESERVE_NUMERIC_KEYS and not _is_stock_code(text):
        text = _LONG_NUMERIC_RE.sub(NUMERIC_ID_REDACTED, text)
    return text


def redact_mapping(mapping: Mapping[str, Any]) -> dict[str, Any]:
    return _redact_mapping(normalize_value(mapping))


def collect_redaction_report(original: object, redacted: object) -> dict[str, bool]:
    return {"redaction_applied": normalize_value(original) != normalize_value(redacted)}


def _redact_value(value: object, *, parent_key: str | None) -> Any:
    if isinstance(value, Mapping):
        return _redact_mapping(value)
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return [_redact_value(item, parent_key=parent_key) for item in value]
    if isinstance(value, str):
        return redact_string(value, parent_key=parent_key)
    return value


def _redact_mapping(mapping: Mapping[str, Any]) -> dict[str, Any]:
    redacted: dict[str, Any] = {}
    for raw_key, raw_value in mapping.items():
        key = str(raw_key)
        canonical_key = _canonicalize(key)
        if canonical_key in _SECRET_KEYS:
            redacted[key] = SECRET_REDACTED
        elif canonical_key in _ACCOUNT_KEYS:
            redacted[key] = MASKED
        elif canonical_key in _ENV_KEYS:
            redacted[key] = ENV_REDACTED
        elif canonical_key in _HEADER_KEYS:
            redacted[key] = HEADERS_REDACTED
        else:
            redacted[key] = _redact_value(raw_value, parent_key=key)
    return redacted


def _canonicalize(value: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "", value.strip().lower())


def _is_stock_code(value: str) -> bool:
    return bool(re.fullmatch(r"\d{6}", value.strip()))
