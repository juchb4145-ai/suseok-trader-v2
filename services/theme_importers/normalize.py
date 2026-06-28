from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from domain.broker.utils import validate_stock_code

from services.theme_importers.models import (
    NAVER_REFERENCE_SOURCE_NAME,
    NAVER_REFERENCE_SOURCE_TYPE,
    NaverTheme,
    NaverThemeMember,
    ThemeImportEvidence,
    ThemeImportParserError,
)

_WHITESPACE_RE = re.compile(r"\s+")
_THEME_ID_SAFE_RE = re.compile(r"[^a-zA-Z0-9_]+")


@dataclass(frozen=True, kw_only=True)
class NormalizedNaverThemePayload:
    payload: Mapping[str, Any]
    duplicate_count: int = 0
    skipped_theme_count: int = 0
    errors: Sequence[ThemeImportParserError] = field(default_factory=tuple)

    @property
    def theme_count(self) -> int:
        return len(self.payload.get("themes", []))

    @property
    def member_count(self) -> int:
        return sum(len(theme.get("members", [])) for theme in self.payload.get("themes", []))


def normalize_theme_name(value: object) -> str:
    if value is None:
        return ""
    return _WHITESPACE_RE.sub(" ", str(value)).strip()


def canonicalize_theme_id(theme: NaverTheme | str, *, source_theme_id: str | None = None) -> str:
    if isinstance(theme, NaverTheme):
        source_id = theme.source_theme_id
        theme_name = theme.theme_name
    else:
        source_id = source_theme_id
        theme_name = str(theme)
    normalized_source_id = normalize_theme_name(source_id)
    if normalized_source_id:
        safe_source_id = _THEME_ID_SAFE_RE.sub("_", normalized_source_id).strip("_").lower()
        if safe_source_id:
            return f"naver_theme_{safe_source_id}"
    digest = hashlib.sha256(normalize_theme_name(theme_name).encode("utf-8")).hexdigest()[:12]
    return f"naver_theme_{digest}"


def normalize_stock_code(value: object) -> str:
    return validate_stock_code(value)


def normalize_naver_theme_payload(
    themes: Sequence[NaverTheme],
    members_by_source_theme_id: Mapping[str, Sequence[NaverThemeMember]],
    *,
    fetched_at: str,
    min_member_count: int = 2,
) -> NormalizedNaverThemePayload:
    normalized_themes: list[dict[str, Any]] = []
    errors: list[ThemeImportParserError] = []
    duplicate_count = 0
    skipped_theme_count = 0
    seen_theme_ids: set[str] = set()

    for theme in themes:
        theme_name = normalize_theme_name(theme.theme_name)
        theme_id = canonicalize_theme_id(theme)
        if theme_id in seen_theme_ids:
            duplicate_count += 1
            continue
        seen_theme_ids.add(theme_id)

        members: list[dict[str, Any]] = []
        seen_members: set[tuple[str, str]] = set()
        for member in members_by_source_theme_id.get(theme.source_theme_id, ()):
            try:
                code = normalize_stock_code(member.code)
            except ValueError as exc:
                errors.append(
                    ThemeImportParserError(
                        stage="normalize_member",
                        message=str(exc),
                        source_url=member.source_url,
                        theme_name=theme_name,
                        source_theme_id=theme.source_theme_id,
                        theme_id=theme_id,
                        payload=member.to_dict(),
                    )
                )
                continue
            name = normalize_theme_name(member.name)
            if not name:
                errors.append(
                    ThemeImportParserError(
                        stage="normalize_member",
                        message="member name is empty",
                        source_url=member.source_url,
                        theme_name=theme_name,
                        source_theme_id=theme.source_theme_id,
                        theme_id=theme_id,
                        code=code,
                        payload=member.to_dict(),
                    )
                )
                continue
            member_key = (theme_id, code)
            if member_key in seen_members:
                duplicate_count += 1
                continue
            seen_members.add(member_key)
            members.append(
                {
                    "code": code,
                    "name": name,
                    "active": True,
                    "weight": 1.0,
                    "metadata": _member_metadata(
                        theme=theme,
                        member=member,
                        code=code,
                        name=name,
                        fetched_at=fetched_at,
                    ),
                }
            )

        if len(members) < min_member_count:
            skipped_theme_count += 1
            errors.append(
                ThemeImportParserError(
                    stage="normalize_theme",
                    message=(
                        f"theme member count below minimum: "
                        f"{len(members)} < {min_member_count}"
                    ),
                    source_url=theme.source_url,
                    theme_name=theme_name,
                    source_theme_id=theme.source_theme_id,
                    theme_id=theme_id,
                    payload={"member_count": len(members)},
                )
            )
            continue

        normalized_themes.append(
            {
                "theme_id": theme_id,
                "theme_name": theme_name,
                "active": True,
                "metadata": _theme_metadata(theme, fetched_at=fetched_at),
                "members": members,
            }
        )

    return NormalizedNaverThemePayload(
        payload={
            "source_type": NAVER_REFERENCE_SOURCE_TYPE,
            "source_name": NAVER_REFERENCE_SOURCE_NAME,
            "metadata": {
                "source": NAVER_REFERENCE_SOURCE_NAME,
                "fetched_at": fetched_at,
                "not_intraday_signal": True,
                "not_order_signal": True,
            },
            "themes": normalized_themes,
        },
        duplicate_count=duplicate_count,
        skipped_theme_count=skipped_theme_count,
        errors=tuple(errors),
    )


def _theme_metadata(theme: NaverTheme, *, fetched_at: str) -> dict[str, Any]:
    return {
        "source_type": NAVER_REFERENCE_SOURCE_TYPE,
        "source_name": NAVER_REFERENCE_SOURCE_NAME,
        "source_theme_id": theme.source_theme_id,
        "source_url": theme.source_url,
        "rank": theme.rank,
        "change_rate_text": theme.change_rate_text,
        "fetched_at": fetched_at,
        "not_intraday_signal": True,
        "not_order_signal": True,
        "raw": dict(theme.metadata),
    }


def _member_metadata(
    *,
    theme: NaverTheme,
    member: NaverThemeMember,
    code: str,
    name: str,
    fetched_at: str,
) -> dict[str, Any]:
    confidence = 0.8 if member.reason_text else 0.7
    evidence = ThemeImportEvidence(
        theme_name=theme.theme_name,
        code=code,
        name=name,
        confidence=confidence,
        evidence_text=member.reason_text or "Naver theme membership reference",
        source_url=member.source_url,
        fetched_at=fetched_at,
    )
    return {
        "source_type": NAVER_REFERENCE_SOURCE_TYPE,
        "source_name": NAVER_REFERENCE_SOURCE_NAME,
        "source_theme_id": theme.source_theme_id,
        "source_url": member.source_url,
        "rank": member.rank,
        "confidence": confidence,
        "freshness": {
            "fetched_at": fetched_at,
            "source_kind": "reference_membership",
        },
        "reason_text": member.reason_text,
        "evidence": evidence.to_dict(),
        "not_intraday_signal": True,
        "not_order_signal": True,
        "raw": dict(member.metadata),
    }
