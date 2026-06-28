from __future__ import annotations

from services.ai_advisory.providers import (
    CandidateScorerProvider,
    CandidateScorerProviderError,
    ExternalHTTPCandidateScorerProvider,
    ExternalHttpTransport,
    MockCandidateScorerProvider,
)
from services.config import Settings


def get_candidate_scorer_provider(
    settings: Settings,
    *,
    allow_external: bool = False,
    transport: ExternalHttpTransport | None = None,
) -> CandidateScorerProvider:
    provider = settings.ai_candidate_scorer_provider.strip().lower()
    if provider in {"", "mock", "fake"}:
        return MockCandidateScorerProvider()
    if provider in {"external", "external_http", "openai"}:
        return ExternalHTTPCandidateScorerProvider(
            settings=settings,
            allow_external=allow_external,
            transport=transport,
        )
    raise CandidateScorerProviderError(f"unsupported AI candidate scorer provider: {provider}")


__all__ = [
    "CandidateScorerProvider",
    "CandidateScorerProviderError",
    "ExternalHTTPCandidateScorerProvider",
    "ExternalHttpTransport",
    "MockCandidateScorerProvider",
    "get_candidate_scorer_provider",
]
