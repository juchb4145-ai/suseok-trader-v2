from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from domain.ai_sidecar.context import AISidecarContextSection
from domain.broker.utils import normalize_value

ORDER_CONTEXT_BLOCKED = "ORDER_CONTEXT_BLOCKED"
ORDER_ACTION_FIELD_DROPPED = "ORDER_ACTION_FIELD_DROPPED"

_RESTRICTED_KEYS = {
    "order",
    "orders",
    "orderintent",
    "order_intent",
    "orderrequest",
    "order_request",
    "gatewaycommand",
    "gateway_command",
    "sendorder",
    "send_order",
    "cancelorder",
    "cancel_order",
    "modifyorder",
    "modify_order",
    "positionsize",
    "position_size",
    "livereal",
    "live_real",
    "livesim",
    "live_sim",
    "account",
}
_ACTION_LIKE_FIELDS = {
    "action",
    "actions",
    "api_call",
    "api_calls",
    "command",
    "commands",
    "function",
    "function_name",
    "intent",
    "intents",
    "operation",
    "operations",
    "requested_action",
    "requested_actions",
    "recommended_action",
    "recommended_actions",
    "tool",
    "tools",
    "tool_name",
}
_FORBIDDEN_ACTIONS = {
    "sendorder",
    "cancelorder",
    "modifyorder",
    "orderintent",
    "gatewaycommand",
    "enqueueorder",
}


@dataclass(frozen=True, kw_only=True)
class ContextPolicyResult:
    value: Any
    dropped: bool = False
    warnings: Sequence[str] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        object.__setattr__(self, "dropped", bool(self.dropped))
        object.__setattr__(self, "warnings", tuple(str(item) for item in self.warnings))


def sanitize_order_context(
    value: object,
    *,
    allow_order_context: bool = False,
) -> ContextPolicyResult:
    if allow_order_context:
        return ContextPolicyResult(value=normalize_value(value), dropped=False, warnings=())
    sanitized, dropped = _sanitize_value(normalize_value(value), parent_key=None)
    warnings = (ORDER_CONTEXT_BLOCKED,) if dropped else ()
    return ContextPolicyResult(value=sanitized, dropped=dropped, warnings=warnings)


def sanitize_context_sections(
    sections: Sequence[AISidecarContextSection],
    *,
    allow_order_context: bool = False,
) -> tuple[list[AISidecarContextSection], list[str]]:
    if allow_order_context:
        return list(sections), []
    sanitized_sections: list[AISidecarContextSection] = []
    warnings: list[str] = []
    for section in sections:
        if _is_restricted_section(section.section_name) or _is_restricted_section(section.source):
            warnings.append(ORDER_CONTEXT_BLOCKED)
            continue
        result = sanitize_order_context(section.payload, allow_order_context=False)
        if result.dropped:
            warnings.extend(result.warnings)
        sanitized_sections.append(
            AISidecarContextSection(
                section_name=section.section_name,
                source=section.source,
                row_count=section.row_count,
                truncated=section.truncated,
                missing=section.missing,
                payload=result.value,
            )
        )
    return sanitized_sections, _unique(warnings)


def contains_order_action(value: object) -> bool:
    return _contains_order_action(normalize_value(value), inspect_strings=isinstance(value, str))


def _sanitize_value(value: object, *, parent_key: str | None) -> tuple[Any, bool]:
    if isinstance(value, Mapping):
        return _sanitize_mapping(value)
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        items = []
        dropped = False
        for item in value:
            sanitized_item, item_dropped = _sanitize_value(item, parent_key=parent_key)
            dropped = dropped or item_dropped
            if sanitized_item is not _DROP:
                items.append(sanitized_item)
        return items, dropped
    if parent_key is not None and _is_action_like_field(parent_key) and isinstance(value, str):
        if _matches_forbidden_action(value):
            return _DROP, True
    return value, False


def _sanitize_mapping(mapping: Mapping[str, Any]) -> tuple[dict[str, Any], bool]:
    sanitized: dict[str, Any] = {}
    dropped = False
    for raw_key, raw_value in mapping.items():
        key = str(raw_key)
        if _is_restricted_key(key):
            dropped = True
            continue
        if _is_action_like_field(key) and _contains_order_action(raw_value, inspect_strings=True):
            dropped = True
            continue
        value, value_dropped = _sanitize_value(raw_value, parent_key=key)
        dropped = dropped or value_dropped
        if value is not _DROP:
            sanitized[key] = value
    return sanitized, dropped


def _contains_order_action(value: object, *, inspect_strings: bool) -> bool:
    if isinstance(value, Mapping):
        for raw_key, item in value.items():
            key = str(raw_key)
            if _is_restricted_key(key):
                return True
            if _is_action_like_field(key):
                if _contains_order_action(item, inspect_strings=True):
                    return True
            elif _contains_order_action(item, inspect_strings=False):
                return True
        return False
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return any(_contains_order_action(item, inspect_strings=inspect_strings) for item in value)
    if inspect_strings and isinstance(value, str):
        return _matches_forbidden_action(value)
    return False


def _is_restricted_key(value: str) -> bool:
    canonical = _canonicalize(value)
    return canonical in {_canonicalize(key) for key in _RESTRICTED_KEYS}


def _is_restricted_section(value: str) -> bool:
    canonical = _canonicalize(value)
    return canonical in {"orders", "order", "orderintent", "orderrequest", "oms", "positions"}


def _is_action_like_field(value: str) -> bool:
    return _canonicalize(value) in {_canonicalize(field_name) for field_name in _ACTION_LIKE_FIELDS}


def _matches_forbidden_action(value: str) -> bool:
    canonical = _canonicalize(value)
    return any(action in canonical for action in _FORBIDDEN_ACTIONS)


def _canonicalize(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.strip().lower())


def _unique(values: Sequence[str]) -> list[str]:
    return [*dict.fromkeys(value for value in values if value)]


class _DropValue:
    pass


_DROP = _DropValue()
