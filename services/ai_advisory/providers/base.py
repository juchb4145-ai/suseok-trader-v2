from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol

from services.ai_advisory.models import (
    AiProviderRawResult,
    CandidatePrompt,
    CandidateScoringContext,
)
from services.config import Settings


class CandidateScorerProvider(Protocol):
    name: str

    def score_candidates(
        self,
        context: CandidateScoringContext,
        *,
        prompt: CandidatePrompt | None = None,
        settings: Settings,
    ) -> AiProviderRawResult:
        """Return raw provider output. The service performs strict JSON validation."""

    def score(self, context: Mapping[str, Any], *, settings: Settings) -> object:
        """Backward-compatible raw response API for PR-6 callers/tests."""


class CandidateScorerProviderError(RuntimeError):
    pass


@dataclass(frozen=True, kw_only=True)
class ExternalHttpTransportResponse:
    status_code: int
    text: str
    headers: Mapping[str, str] = field(default_factory=dict)
    json_body: Mapping[str, Any] | None = None


class ExternalHttpTransport(Protocol):
    def post_json(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        payload: Mapping[str, Any],
        timeout_seconds: float,
    ) -> ExternalHttpTransportResponse:
        """POST a JSON payload and return an HTTP-shaped response."""
