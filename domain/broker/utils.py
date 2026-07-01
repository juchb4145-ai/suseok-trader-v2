from __future__ import annotations

import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import asdict, is_dataclass
from datetime import UTC, datetime, timedelta, timezone, tzinfo
from decimal import Decimal
from enum import Enum, StrEnum
from typing import Any, TypeVar
from uuid import uuid4
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


class BrokerValidationError(ValueError):
    """Raised when broker contract data violates the domain boundary."""


EnumT = TypeVar("EnumT", bound=StrEnum)

_CODE_RE = re.compile(r"^\d{6}$")
_MESSAGE_ID_PREFIX_RE = re.compile(r"[^a-zA-Z0-9_-]+")


def utc_now() -> datetime:
    """Return an aware UTC timestamp for contract objects."""

    return datetime.now(UTC)


def _market_timezone() -> tzinfo:
    try:
        return ZoneInfo("Asia/Seoul")
    except ZoneInfoNotFoundError:
        return timezone(timedelta(hours=9), name="Asia/Seoul")


MARKET_TIMEZONE = _market_timezone()


def market_now() -> datetime:
    """Return the current aware timestamp in the KRX market timezone (KST).

    Stored/wire timestamps stay UTC; this is only for calendar-date and
    session-clock decisions (trade_date, EOD cutoffs)."""

    return utc_now().astimezone(MARKET_TIMEZONE)


def market_today() -> str:
    """Return today's trade date (YYYY-MM-DD) in the KRX market timezone."""

    return market_now().date().isoformat()


def market_time_str() -> str:
    """Return the current market-local time of day as HH:MM:SS."""

    return market_now().strftime("%H:%M:%S")


def timestamp() -> str:
    """Return the current UTC timestamp in wire format."""

    return datetime_to_wire(utc_now())


def new_message_id(prefix: str = "msg") -> str:
    normalized = _MESSAGE_ID_PREFIX_RE.sub("_", prefix.strip()) or "msg"
    return f"{normalized}_{uuid4().hex}"


def datetime_to_wire(value: datetime) -> str:
    parsed = parse_timestamp(value, "timestamp")
    return parsed.isoformat().replace("+00:00", "Z")


def parse_timestamp(value: object, field_name: str) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    if isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            raise BrokerValidationError(f"{field_name} must not be empty")
        if normalized.endswith("Z"):
            normalized = f"{normalized[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError as exc:
            raise BrokerValidationError(f"{field_name} must be an ISO-8601 timestamp") from exc
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)

    raise BrokerValidationError(f"{field_name} must be a datetime or ISO-8601 string")


def normalize_payload(payload: object) -> dict[str, Any]:
    normalized = normalize_value(payload)
    if not isinstance(normalized, dict):
        raise BrokerValidationError("payload must normalize to a JSON object")
    return normalized


def normalize_value(value: object) -> Any:
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        return normalize_value(to_dict())

    if is_dataclass(value) and not isinstance(value, type):
        return normalize_value(asdict(value))

    if isinstance(value, datetime):
        return datetime_to_wire(value)

    if isinstance(value, Enum):
        return value.value

    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)

    if isinstance(value, Mapping):
        return {str(key): normalize_value(item) for key, item in value.items()}

    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [normalize_value(item) for item in value]

    if isinstance(value, (str, int, float, bool)) or value is None:
        return value

    raise BrokerValidationError(f"Unsupported payload value type: {type(value).__name__}")


def require_mapping(data: object, model_name: str) -> Mapping[str, Any]:
    if not isinstance(data, Mapping):
        raise BrokerValidationError(f"{model_name} must be created from a mapping")
    return data


def require_fields(data: Mapping[str, Any], fields: Sequence[str], model_name: str) -> None:
    missing = [field for field in fields if field not in data]
    if missing:
        joined = ", ".join(missing)
        raise BrokerValidationError(f"{model_name} missing required field(s): {joined}")


def require_non_empty_str(value: object, field_name: str) -> str:
    if not isinstance(value, str):
        raise BrokerValidationError(f"{field_name} must be a string")
    normalized = value.strip()
    if not normalized:
        raise BrokerValidationError(f"{field_name} must not be empty")
    return normalized


def optional_non_empty_str(value: object, field_name: str) -> str | None:
    if value is None:
        return None
    return require_non_empty_str(value, field_name)


def validate_stock_code(value: object, field_name: str = "code") -> str:
    code = require_non_empty_str(value, field_name).upper()
    if code.startswith("A") and len(code) == 7:
        code = code[1:]
    if not _CODE_RE.fullmatch(code):
        raise BrokerValidationError(f"{field_name} must be a 6-digit domestic stock code")
    return code


def parse_int(value: object, field_name: str, *, min_value: int | None = None) -> int:
    if isinstance(value, bool):
        raise BrokerValidationError(f"{field_name} must be an integer")

    if isinstance(value, int):
        parsed = value
    elif isinstance(value, float):
        if not math.isfinite(value) or not value.is_integer():
            raise BrokerValidationError(f"{field_name} must be an integer")
        parsed = int(value)
    elif isinstance(value, Decimal):
        if value != value.to_integral_value():
            raise BrokerValidationError(f"{field_name} must be an integer")
        parsed = int(value)
    elif isinstance(value, str):
        normalized = value.strip().replace(",", "")
        if not re.fullmatch(r"[+-]?\d+", normalized):
            raise BrokerValidationError(f"{field_name} must be an integer")
        parsed = int(normalized)
    else:
        raise BrokerValidationError(f"{field_name} must be an integer")

    if min_value is not None and parsed < min_value:
        raise BrokerValidationError(f"{field_name} must be >= {min_value}")
    return parsed


def parse_float(value: object, field_name: str, *, min_value: float | None = None) -> float:
    if isinstance(value, bool):
        raise BrokerValidationError(f"{field_name} must be a number")

    if isinstance(value, (int, float)):
        parsed = float(value)
    elif isinstance(value, Decimal):
        parsed = float(value)
    elif isinstance(value, str):
        normalized = value.strip().replace(",", "")
        if normalized.endswith("%"):
            normalized = normalized[:-1].strip()
        try:
            parsed = float(normalized)
        except ValueError as exc:
            raise BrokerValidationError(f"{field_name} must be a number") from exc
    else:
        raise BrokerValidationError(f"{field_name} must be a number")

    if not math.isfinite(parsed):
        raise BrokerValidationError(f"{field_name} must be finite")
    if min_value is not None and parsed < min_value:
        raise BrokerValidationError(f"{field_name} must be >= {min_value}")
    return parsed


def parse_str_enum(value: object, enum_type: type[EnumT], field_name: str) -> EnumT:
    if isinstance(value, enum_type):
        return value
    if isinstance(value, str):
        normalized = value.strip().upper()
        for member in enum_type:
            if normalized in {member.name.upper(), member.value.upper()}:
                return member
    allowed = ", ".join(member.value for member in enum_type)
    raise BrokerValidationError(f"{field_name} must be one of: {allowed}")


def parse_bool(value: object, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise BrokerValidationError(f"{field_name} must be a boolean")
    return value
