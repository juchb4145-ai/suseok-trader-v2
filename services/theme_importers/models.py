from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from domain.broker.utils import normalize_payload

NAVER_REFERENCE_SOURCE_TYPE = "NAVER_REFERENCE"
NAVER_REFERENCE_SOURCE_NAME = "naver_theme"


@dataclass(frozen=True, kw_only=True)
class NaverTheme:
    source_theme_id: str
    theme_name: str
    source_url: str
    rank: int
    change_rate_text: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_theme_id": self.source_theme_id,
            "theme_name": self.theme_name,
            "source_url": self.source_url,
            "rank": self.rank,
            "change_rate_text": self.change_rate_text,
            "metadata": normalize_payload(self.metadata),
        }


@dataclass(frozen=True, kw_only=True)
class NaverThemeMember:
    theme_name: str
    code: str
    name: str
    source_url: str
    reason_text: str | None = None
    rank: int | None = None
    source_theme_id: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "theme_name": self.theme_name,
            "code": self.code,
            "name": self.name,
            "source_url": self.source_url,
            "reason_text": self.reason_text,
            "rank": self.rank,
            "source_theme_id": self.source_theme_id,
            "metadata": normalize_payload(self.metadata),
        }


@dataclass(frozen=True, kw_only=True)
class ThemeImportEvidence:
    theme_name: str
    code: str
    name: str
    confidence: float
    evidence_text: str
    source_url: str
    fetched_at: str
    source_type: str = NAVER_REFERENCE_SOURCE_TYPE
    source_name: str = NAVER_REFERENCE_SOURCE_NAME

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_type": self.source_type,
            "source_name": self.source_name,
            "theme_name": self.theme_name,
            "code": self.code,
            "name": self.name,
            "confidence": self.confidence,
            "evidence_text": self.evidence_text,
            "source_url": self.source_url,
            "fetched_at": self.fetched_at,
        }


@dataclass(frozen=True, kw_only=True)
class ThemeImportParserError:
    stage: str
    message: str
    source_url: str | None = None
    theme_name: str | None = None
    source_theme_id: str | None = None
    theme_id: str | None = None
    code: str | None = None
    payload: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "stage": self.stage,
            "message": self.message,
            "source_url": self.source_url,
            "theme_name": self.theme_name,
            "source_theme_id": self.source_theme_id,
            "theme_id": self.theme_id,
            "code": self.code,
            "payload": normalize_payload(self.payload),
        }
