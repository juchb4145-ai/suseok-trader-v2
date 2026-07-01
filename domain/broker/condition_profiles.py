from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, time, timedelta, timezone
from enum import StrEnum
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from domain.broker.utils import normalize_value


class ConditionRole(StrEnum):
    DISCOVERY = "DISCOVERY"
    LEADER = "LEADER"
    PULLBACK = "PULLBACK"
    BREAKOUT = "BREAKOUT"
    THEME_SPREAD = "THEME_SPREAD"
    RISK_BLOCK = "RISK_BLOCK"
    MANUAL = "MANUAL"


class PriceSubscribePolicy(StrEnum):
    NONE = "none"
    BATCH = "batch"
    IMMEDIATE = "immediate"
    REAL_ONLY = "real_only"


class ConditionSessionProfile(StrEnum):
    PREOPEN_NXT = "PREOPEN_NXT"
    OPENING_0900_0915 = "OPENING_0900_0915"
    MORNING_TREND = "MORNING_TREND"
    MIDDAY = "MIDDAY"
    AFTERNOON = "AFTERNOON"
    CLOSING = "CLOSING"


@dataclass(frozen=True, kw_only=True)
class ConditionProfile:
    condition_name: str
    profile_id: str = ""
    condition_index: int | None = None
    role: ConditionRole | str = ConditionRole.DISCOVERY
    realtime_search: bool = True
    price_subscribe_policy: PriceSubscribePolicy | str = PriceSubscribePolicy.BATCH
    priority: int = 100
    ttl_sec: int = 180
    max_initial: int = 50
    max_realtime_per_min: int = 10
    enabled: bool = True
    screen_no: str = ""

    def __post_init__(self) -> None:
        role = parse_condition_role(self.role)
        policy = parse_price_subscribe_policy(self.price_subscribe_policy)
        name = str(self.condition_name or "").strip()
        if not name:
            raise ValueError("condition_name must not be empty")
        profile_id = str(self.profile_id or "").strip() or _profile_id(
            name,
            self.condition_index,
            role.value,
        )
        screen_no = str(self.screen_no or "").strip()
        if screen_no and not screen_no.isdigit():
            raise ValueError("screen_no must be numeric")
        object.__setattr__(self, "condition_name", name)
        object.__setattr__(self, "profile_id", profile_id)
        object.__setattr__(
            self,
            "condition_index",
            int(self.condition_index) if self.condition_index is not None else None,
        )
        object.__setattr__(self, "role", role)
        object.__setattr__(self, "price_subscribe_policy", policy)
        object.__setattr__(self, "priority", int(self.priority))
        object.__setattr__(self, "ttl_sec", max(int(self.ttl_sec), 1))
        object.__setattr__(self, "max_initial", max(int(self.max_initial), 0))
        object.__setattr__(
            self,
            "max_realtime_per_min",
            max(int(self.max_realtime_per_min), 0),
        )
        object.__setattr__(self, "screen_no", screen_no)

    def with_resolution(
        self,
        *,
        condition_name: str | None = None,
        condition_index: int | None = None,
        screen_no: str | None = None,
    ) -> ConditionProfile:
        return replace(
            self,
            condition_name=condition_name or self.condition_name,
            condition_index=(
                int(condition_index)
                if condition_index is not None
                else self.condition_index
            ),
            screen_no=screen_no if screen_no is not None else self.screen_no,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "profile_id": self.profile_id,
            "condition_name": self.condition_name,
            "condition_index": self.condition_index,
            "role": self.role.value,
            "realtime_search": bool(self.realtime_search),
            "price_subscribe_policy": self.price_subscribe_policy.value,
            "priority": int(self.priority),
            "ttl_sec": int(self.ttl_sec),
            "max_initial": int(self.max_initial),
            "max_realtime_per_min": int(self.max_realtime_per_min),
            "enabled": bool(self.enabled),
            "screen_no": self.screen_no,
        }


def parse_condition_role(value: object) -> ConditionRole:
    raw_value = getattr(value, "value", value)
    text = str(raw_value or ConditionRole.DISCOVERY.value).strip().upper()
    aliases = {
        "DISCOVER": "DISCOVERY",
        "THEME_SPREADING": "THEME_SPREAD",
        "RISK": "RISK_BLOCK",
        "BLOCK": "RISK_BLOCK",
    }
    normalized = aliases.get(text, text)
    return ConditionRole(normalized)


def parse_price_subscribe_policy(value: object) -> PriceSubscribePolicy:
    raw_value = getattr(value, "value", value)
    text = str(raw_value or PriceSubscribePolicy.NONE.value).strip().lower()
    aliases = {
        "no": "none",
        "off": "none",
        "false": "none",
        "now": "immediate",
        "realtime": "real_only",
        "real": "real_only",
    }
    normalized = aliases.get(text, text)
    return PriceSubscribePolicy(normalized)


def parse_condition_profiles(raw: str | None) -> tuple[ConditionProfile, ...]:
    text = str(raw or "").strip()
    if not text:
        return ()
    loaded = json.loads(text)
    if isinstance(loaded, Mapping):
        items = loaded.get("profiles", [loaded])
    else:
        items = loaded
    if not isinstance(items, Sequence) or isinstance(items, (str, bytes, bytearray)):
        raise ValueError("condition profiles must be a JSON object or array")
    return tuple(ConditionProfile(**dict(item)) for item in items if isinstance(item, Mapping))


def condition_profiles_from_config(
    *,
    profiles: Iterable[ConditionProfile] | None = None,
    legacy_condition_name: str | None = None,
    legacy_condition_index: int | None = None,
    legacy_realtime: bool = True,
) -> tuple[ConditionProfile, ...]:
    explicit = tuple(profile for profile in (profiles or ()) if profile.enabled)
    if explicit:
        return explicit
    condition_name = str(legacy_condition_name or "").strip()
    if not condition_name and legacy_condition_index is None:
        return ()
    policy = (
        PriceSubscribePolicy.IMMEDIATE
        if legacy_realtime
        else PriceSubscribePolicy.NONE
    )
    return (
        ConditionProfile(
            profile_id="legacy_condition",
            condition_name=condition_name or f"Kiwoom Condition {legacy_condition_index}",
            condition_index=legacy_condition_index,
            role=ConditionRole.DISCOVERY,
            realtime_search=bool(legacy_realtime),
            price_subscribe_policy=policy,
            priority=100,
            ttl_sec=180,
            max_initial=50,
            max_realtime_per_min=10,
            enabled=True,
        ),
    )


def current_condition_session_profile(now: datetime | None = None) -> ConditionSessionProfile:
    tz = _seoul_timezone()
    local_now = (now or datetime.now(tz)).astimezone(tz)
    current = local_now.time()
    if current < time(9, 0):
        return ConditionSessionProfile.PREOPEN_NXT
    if current < time(9, 15):
        return ConditionSessionProfile.OPENING_0900_0915
    if current < time(10, 30):
        return ConditionSessionProfile.MORNING_TREND
    if current < time(13, 30):
        return ConditionSessionProfile.MIDDAY
    if current < time(15, 0):
        return ConditionSessionProfile.AFTERNOON
    return ConditionSessionProfile.CLOSING


def _seoul_timezone():
    try:
        return ZoneInfo("Asia/Seoul")
    except ZoneInfoNotFoundError:
        return timezone(timedelta(hours=9), name="Asia/Seoul")


def normalize_condition_profile_payload(profile: ConditionProfile) -> dict[str, Any]:
    return normalize_value(profile.to_dict())


def _profile_id(condition_name: str, condition_index: int | None, role: str) -> str:
    name = condition_name.strip().lower().replace(" ", "_")
    index = "auto" if condition_index is None else str(int(condition_index))
    return f"{role.lower()}:{index}:{name}"
