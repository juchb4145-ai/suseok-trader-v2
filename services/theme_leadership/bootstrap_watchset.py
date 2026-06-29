from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from domain.broker.commands import GatewayCommand
from domain.broker.utils import utc_now, validate_stock_code
from services.config import Settings, candidate_timezone, load_settings
from storage.gateway_command_store import EnqueueCommandResult, enqueue_command


DEFAULT_BOOTSTRAP_SOURCE = "live_sim_theme_bootstrap"


@dataclass(frozen=True, kw_only=True)
class BootstrapThemeSelection:
    theme_id: str
    theme_name: str
    member_count: int
    required_fresh_count: int
    anchor_codes: Sequence[str] = field(default_factory=tuple)
    selected_codes: Sequence[str] = field(default_factory=tuple)
    selected_names: Mapping[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "theme_id": self.theme_id,
            "theme_name": self.theme_name,
            "member_count": self.member_count,
            "required_fresh_count": self.required_fresh_count,
            "anchor_codes": list(self.anchor_codes),
            "selected_codes": list(self.selected_codes),
            "selected_names": dict(self.selected_names),
        }


@dataclass(frozen=True, kw_only=True)
class BootstrapRealtimeSelection:
    anchor_codes: Sequence[str] = field(default_factory=tuple)
    selected_codes: Sequence[str] = field(default_factory=tuple)
    selected_names: Mapping[str, str] = field(default_factory=dict)
    themes: Sequence[BootstrapThemeSelection] = field(default_factory=tuple)
    skipped_theme_count: int = 0
    observe_only: bool = True
    not_order_signal: bool = True
    no_order_side_effects: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "anchor_codes": list(self.anchor_codes),
            "selected_codes": list(self.selected_codes),
            "selected_names": dict(self.selected_names),
            "themes": [theme.to_dict() for theme in self.themes],
            "skipped_theme_count": self.skipped_theme_count,
            "observe_only": True,
            "not_order_signal": True,
            "no_order_side_effects": True,
        }


@dataclass(frozen=True, kw_only=True)
class BootstrapRealtimeRegistrationResult:
    status: str
    selection: BootstrapRealtimeSelection
    command_id: str | None = None
    enqueue_result: EnqueueCommandResult | None = None
    observe_only: bool = True
    not_order_signal: bool = True
    no_order_side_effects: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "selection": self.selection.to_dict(),
            "command_id": self.command_id,
            "enqueue_result": None
            if self.enqueue_result is None
            else {
                "accepted": self.enqueue_result.accepted,
                "command_id": self.enqueue_result.command_id,
                "status": self.enqueue_result.status.value,
                "payload_hash": self.enqueue_result.payload_hash,
                "duplicate": self.enqueue_result.duplicate,
                "error_message": self.enqueue_result.error_message,
            },
            "observe_only": True,
            "not_order_signal": True,
            "no_order_side_effects": True,
        }


@dataclass(frozen=True, kw_only=True)
class _ThemeMember:
    theme_id: str
    theme_name: str
    code: str
    name: str
    rank: int


def select_bootstrap_realtime_codes(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
    anchor_codes: Iterable[str] | None = None,
    max_codes: int | None = None,
    max_theme_size: int = 60,
    min_additional_per_anchor_theme: int = 1,
) -> BootstrapRealtimeSelection:
    resolved_settings = settings or load_settings()
    anchors = tuple(
        _dedupe(
            _normalize_codes(anchor_codes)
            if anchor_codes is not None
            else _latest_market_codes(connection)
        )
    )
    max_total = _bounded_count(
        max_codes
        if max_codes is not None
        else resolved_settings.theme_leadership_max_total_watchset,
        minimum=1,
        maximum=100,
    )
    max_members = _bounded_count(max_theme_size, minimum=2, maximum=500)
    min_additional = _bounded_count(min_additional_per_anchor_theme, minimum=0, maximum=20)
    theme_groups = _load_active_theme_members(connection)
    candidates: list[tuple[tuple[int, int, str, str], BootstrapThemeSelection]] = []
    skipped = 0

    for theme_id, members in theme_groups.items():
        if not members:
            skipped += 1
            continue
        member_count = len(members)
        if member_count > max_members:
            skipped += 1
            continue
        member_codes = {member.code for member in members}
        theme_anchors = tuple(code for code in anchors if code in member_codes)
        if anchors and not theme_anchors:
            skipped += 1
            continue
        required_count = max(
            resolved_settings.theme_leadership_min_valid_members,
            math.ceil(member_count * resolved_settings.theme_leadership_min_fresh_coverage_ratio),
        )
        target_count = min(
            member_count,
            max(required_count, len(theme_anchors) + min_additional),
        )
        missing_count = max(target_count - len(theme_anchors), min_additional if theme_anchors else 0)
        if missing_count <= 0:
            skipped += 1
            continue
        ranked_members = sorted(members, key=lambda member: (member.rank, member.code))
        selected = [
            member
            for member in ranked_members
            if member.code not in set(theme_anchors) and member.code not in set(anchors)
        ][:missing_count]
        if not selected:
            skipped += 1
            continue
        theme = BootstrapThemeSelection(
            theme_id=theme_id,
            theme_name=members[0].theme_name,
            member_count=member_count,
            required_fresh_count=required_count,
            anchor_codes=theme_anchors,
            selected_codes=tuple(member.code for member in selected),
            selected_names={member.code: member.name for member in selected},
        )
        candidates.append(
            (
                (
                    -len(theme_anchors),
                    member_count,
                    members[0].theme_name,
                    theme_id,
                ),
                theme,
            )
        )

    selected_codes: list[str] = []
    selected_names: dict[str, str] = {}
    selected_themes: list[BootstrapThemeSelection] = []
    seen = set(anchors)
    for _, theme in sorted(candidates, key=lambda item: item[0]):
        added_codes: list[str] = []
        added_names: dict[str, str] = {}
        for code in theme.selected_codes:
            if code in seen:
                continue
            selected_codes.append(code)
            selected_names[code] = str(theme.selected_names.get(code) or code)
            added_codes.append(code)
            added_names[code] = selected_names[code]
            seen.add(code)
            if len(selected_codes) >= max_total:
                break
        if added_codes:
            selected_themes.append(
                BootstrapThemeSelection(
                    theme_id=theme.theme_id,
                    theme_name=theme.theme_name,
                    member_count=theme.member_count,
                    required_fresh_count=theme.required_fresh_count,
                    anchor_codes=theme.anchor_codes,
                    selected_codes=tuple(added_codes),
                    selected_names=added_names,
                )
            )
        if len(selected_codes) >= max_total:
            break

    return BootstrapRealtimeSelection(
        anchor_codes=anchors,
        selected_codes=tuple(selected_codes),
        selected_names=selected_names,
        themes=tuple(selected_themes),
        skipped_theme_count=skipped,
    )


def queue_bootstrap_realtime_registration(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
    anchor_codes: Iterable[str] | None = None,
    max_codes: int | None = None,
    screen_no: str = "5002",
    ttl_sec: int = 1800,
) -> BootstrapRealtimeRegistrationResult:
    resolved_settings = settings or load_settings()
    selection = select_bootstrap_realtime_codes(
        connection,
        settings=resolved_settings,
        anchor_codes=anchor_codes,
        max_codes=max_codes,
    )
    if not selection.selected_codes:
        return BootstrapRealtimeRegistrationResult(status="NO_SELECTION", selection=selection)

    payload = {
        "codes": list(selection.selected_codes),
        "screen_no": str(screen_no),
        "source": DEFAULT_BOOTSTRAP_SOURCE,
        "purpose": "theme_realtime_bootstrap",
        "observe_only": True,
        "not_order_signal": True,
        "anchor_codes": list(selection.anchor_codes),
        "themes": [theme.to_dict() for theme in selection.themes],
    }
    command_hash = hashlib.sha256(
        json.dumps(
            {
                "codes": list(selection.selected_codes),
                "screen_no": str(screen_no),
                "trade_date": _trade_date(resolved_settings),
            },
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()[:16]
    command = GatewayCommand(
        command_type="register_realtime",
        source=DEFAULT_BOOTSTRAP_SOURCE,
        payload=payload,
        idempotency_key=(
            f"{DEFAULT_BOOTSTRAP_SOURCE}:{_trade_date(resolved_settings)}:"
            f"{screen_no}:{command_hash}"
        ),
    )
    enqueue_result = enqueue_command(
        connection,
        command,
        expires_at=utc_now() + timedelta(seconds=max(int(ttl_sec), 1)),
    )
    status = "QUEUED" if enqueue_result.accepted else enqueue_result.status.value
    return BootstrapRealtimeRegistrationResult(
        status=status,
        selection=selection,
        command_id=command.command_id,
        enqueue_result=enqueue_result,
    )


def _load_active_theme_members(
    connection: sqlite3.Connection,
) -> dict[str, list[_ThemeMember]]:
    rows = connection.execute(
        """
        SELECT
            t.theme_id,
            t.theme_name,
            m.code,
            m.name,
            m.metadata_json
        FROM theme_members AS m
        JOIN themes AS t ON t.theme_id = m.theme_id
        WHERE t.active = 1 AND m.active = 1
        ORDER BY t.theme_name ASC, m.code ASC
        """
    ).fetchall()
    groups: dict[str, list[_ThemeMember]] = defaultdict(list)
    for row in rows:
        code = _safe_stock_code(row["code"])
        if code is None:
            continue
        member = _ThemeMember(
            theme_id=str(row["theme_id"]),
            theme_name=str(row["theme_name"]),
            code=code,
            name=str(row["name"] or code),
            rank=_metadata_rank(row["metadata_json"]),
        )
        groups[member.theme_id].append(member)
    return dict(groups)


def _latest_market_codes(connection: sqlite3.Connection) -> list[str]:
    rows = connection.execute(
        """
        SELECT code
        FROM market_ticks_latest
        ORDER BY updated_at DESC, code ASC
        """
    ).fetchall()
    return _normalize_codes(row["code"] for row in rows)


def _metadata_rank(raw_metadata: object) -> int:
    try:
        metadata = json.loads(str(raw_metadata or "{}"))
    except json.JSONDecodeError:
        return 999_999
    for value in (
        metadata.get("rank"),
        (metadata.get("raw") or {}).get("naver_member_rank")
        if isinstance(metadata.get("raw"), Mapping)
        else None,
    ):
        try:
            rank = int(value)
        except (TypeError, ValueError):
            continue
        if rank > 0:
            return rank
    return 999_999


def _normalize_codes(codes: Iterable[str] | None) -> list[str]:
    if codes is None:
        return []
    result: list[str] = []
    for code in codes:
        normalized = _safe_stock_code(code)
        if normalized is not None:
            result.append(normalized)
    return result


def _safe_stock_code(code: object) -> str | None:
    try:
        return validate_stock_code(str(code or "").strip())
    except ValueError:
        return None


def _dedupe(values: Iterable[str]) -> list[str]:
    return [*dict.fromkeys(values)]


def _bounded_count(value: int, *, minimum: int, maximum: int) -> int:
    return min(max(int(value), minimum), maximum)


def _trade_date(settings: Settings) -> str:
    return datetime.now(candidate_timezone(settings.candidate_trade_date_timezone)).date().isoformat()
