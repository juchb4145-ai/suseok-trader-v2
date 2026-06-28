from __future__ import annotations

import sqlite3
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from domain.broker.utils import normalize_payload

from services.config import Settings, load_settings
from services.theme_importers.models import (
    NAVER_REFERENCE_SOURCE_NAME,
    NAVER_REFERENCE_SOURCE_TYPE,
    ThemeImportParserError,
)
from services.theme_importers.naver_theme import NaverThemeFetcher, NaverThemeFetchResult
from services.theme_importers.normalize import (
    NormalizedNaverThemePayload,
    normalize_naver_theme_payload,
)
from services.theme_service import (
    ThemeImportResult,
    import_theme_memberships,
    record_theme_import_batch,
    record_theme_import_error,
)


@dataclass(frozen=True, kw_only=True)
class NaverThemeImportRunResult:
    status: str
    dry_run: bool
    replace: bool
    fetched_theme_count: int
    fetched_member_count: int
    normalized_theme_count: int
    normalized_member_count: int
    duplicate_count: int
    parser_error_count: int
    skipped_theme_count: int
    payload: Mapping[str, Any]
    errors: Sequence[ThemeImportParserError] = field(default_factory=tuple)
    batch_id: str | None = None
    import_result: ThemeImportResult | None = None
    aborted: bool = False

    def to_dict(self, *, include_payload: bool = False) -> dict[str, Any]:
        data: dict[str, Any] = {
            "status": self.status,
            "dry_run": self.dry_run,
            "replace": self.replace,
            "fetched_theme_count": self.fetched_theme_count,
            "fetched_member_count": self.fetched_member_count,
            "normalized_theme_count": self.normalized_theme_count,
            "normalized_member_count": self.normalized_member_count,
            "duplicate_count": self.duplicate_count,
            "parser_error_count": self.parser_error_count,
            "skipped_theme_count": self.skipped_theme_count,
            "batch_id": self.batch_id,
            "aborted": self.aborted,
            "sample_themes": self.sample_themes(),
            "sample_members": self.sample_members(),
            "errors": [error.to_dict() for error in self.errors],
        }
        if self.import_result is not None:
            data["import_result"] = self.import_result.to_dict()
        if include_payload:
            data["payload"] = normalize_payload(self.payload)
        return data

    def sample_themes(self, *, limit: int = 5) -> list[dict[str, Any]]:
        themes = self.payload.get("themes", [])
        return [
            {
                "theme_id": theme.get("theme_id"),
                "theme_name": theme.get("theme_name"),
                "member_count": len(theme.get("members", [])),
            }
            for theme in themes[:limit]
        ]

    def sample_members(self, *, limit: int = 5) -> list[dict[str, Any]]:
        samples: list[dict[str, Any]] = []
        for theme in self.payload.get("themes", []):
            for member in theme.get("members", []):
                samples.append(
                    {
                        "theme_id": theme.get("theme_id"),
                        "theme_name": theme.get("theme_name"),
                        "code": member.get("code"),
                        "name": member.get("name"),
                    }
                )
                if len(samples) >= limit:
                    return samples
        return samples


class NaverThemeImporter:
    def __init__(
        self,
        *,
        settings: Settings | None = None,
        fetcher: NaverThemeFetcher | None = None,
    ) -> None:
        self.settings = settings or load_settings()
        self.fetcher = fetcher or NaverThemeFetcher(
            base_url=self.settings.naver_theme_import_base_url,
            timeout_seconds=self.settings.naver_theme_import_timeout_seconds,
            request_sleep_seconds=self.settings.naver_theme_import_request_sleep_seconds,
        )

    def run(
        self,
        *,
        connection: sqlite3.Connection | None = None,
        dry_run: bool = False,
        limit_themes: int | None = None,
        replace: bool | None = None,
    ) -> NaverThemeImportRunResult:
        if not dry_run and connection is None:
            raise ValueError("connection is required when dry_run is false")

        effective_limit = self._effective_limit(limit_themes)
        effective_replace = self.settings.naver_theme_import_replace if replace is None else replace
        fetch_result = self.fetcher.fetch(limit_themes=effective_limit)
        normalized = normalize_naver_theme_payload(
            fetch_result.themes,
            fetch_result.members_by_source_theme_id,
            fetched_at=fetch_result.fetched_at,
            min_member_count=self.settings.naver_theme_import_min_member_count,
        )
        errors = [*fetch_result.errors, *normalized.errors]
        payload = normalize_payload(normalized.payload)
        if connection is not None:
            payload, conflict_errors = _without_theme_name_conflicts(connection, payload)
            errors.extend(conflict_errors)

        fetched_member_count = sum(
            len(members) for members in fetch_result.members_by_source_theme_id.values()
        )
        should_abort = (
            self.settings.naver_theme_import_abort_on_empty
            and (not payload.get("themes") or _payload_member_count(payload) == 0)
        )
        if should_abort:
            return self._abort_empty(
                connection=connection,
                dry_run=dry_run,
                replace=effective_replace,
                fetch_result=fetch_result,
                normalized=normalized,
                fetched_member_count=fetched_member_count,
                payload=payload,
                errors=errors,
            )

        if dry_run:
            return self._result(
                status="DRY_RUN",
                dry_run=True,
                replace=effective_replace,
                fetch_result=fetch_result,
                normalized=normalized,
                fetched_member_count=fetched_member_count,
                payload=payload,
                errors=errors,
            )

        assert connection is not None
        import_result = import_theme_memberships(
            connection,
            payload,
            source_type=NAVER_REFERENCE_SOURCE_TYPE,
            source_name=NAVER_REFERENCE_SOURCE_NAME,
            replace=effective_replace,
        )
        _record_errors(connection, import_result.batch_id, errors)
        connection.commit()
        return self._result(
            status="SUCCESS",
            dry_run=False,
            replace=effective_replace,
            fetch_result=fetch_result,
            normalized=normalized,
            fetched_member_count=fetched_member_count,
            payload=payload,
            errors=errors,
            batch_id=import_result.batch_id,
            import_result=import_result,
        )

    def _effective_limit(self, limit_themes: int | None) -> int:
        configured = self.settings.naver_theme_import_max_themes
        if limit_themes is None:
            return configured
        return min(max(int(limit_themes), 1), configured)

    def _abort_empty(
        self,
        *,
        connection: sqlite3.Connection | None,
        dry_run: bool,
        replace: bool,
        fetch_result: NaverThemeFetchResult,
        normalized: NormalizedNaverThemePayload,
        fetched_member_count: int,
        payload: Mapping[str, Any],
        errors: Sequence[ThemeImportParserError],
    ) -> NaverThemeImportRunResult:
        batch_id: str | None = None
        import_result: ThemeImportResult | None = None
        if connection is not None and not dry_run:
            import_result = record_theme_import_batch(
                connection,
                source_type=NAVER_REFERENCE_SOURCE_TYPE,
                source_name=NAVER_REFERENCE_SOURCE_NAME,
                theme_count=0,
                member_count=0,
                status="ABORTED",
                error_message="empty naver theme fetch aborted",
            )
            batch_id = import_result.batch_id
            _record_errors(connection, batch_id, errors)
            connection.commit()
        return self._result(
            status="ABORTED_EMPTY_FETCH",
            dry_run=dry_run,
            replace=replace,
            fetch_result=fetch_result,
            normalized=normalized,
            fetched_member_count=fetched_member_count,
            payload=payload,
            errors=errors,
            batch_id=batch_id,
            import_result=import_result,
            aborted=True,
        )

    def _result(
        self,
        *,
        status: str,
        dry_run: bool,
        replace: bool,
        fetch_result: NaverThemeFetchResult,
        normalized: NormalizedNaverThemePayload,
        fetched_member_count: int,
        payload: Mapping[str, Any],
        errors: Sequence[ThemeImportParserError],
        batch_id: str | None = None,
        import_result: ThemeImportResult | None = None,
        aborted: bool = False,
    ) -> NaverThemeImportRunResult:
        return NaverThemeImportRunResult(
            status=status,
            dry_run=dry_run,
            replace=replace,
            fetched_theme_count=len(fetch_result.themes),
            fetched_member_count=fetched_member_count,
            normalized_theme_count=len(payload.get("themes", [])),
            normalized_member_count=_payload_member_count(payload),
            duplicate_count=normalized.duplicate_count,
            parser_error_count=len(errors),
            skipped_theme_count=normalized.skipped_theme_count,
            payload=payload,
            errors=tuple(errors),
            batch_id=batch_id,
            import_result=import_result,
            aborted=aborted,
        )


def _payload_member_count(payload: Mapping[str, Any]) -> int:
    return sum(len(theme.get("members", [])) for theme in payload.get("themes", []))


def _record_errors(
    connection: sqlite3.Connection,
    batch_id: str | None,
    errors: Sequence[ThemeImportParserError],
) -> None:
    for error in errors:
        record_theme_import_error(
            connection,
            batch_id=batch_id,
            source_type=NAVER_REFERENCE_SOURCE_TYPE,
            source_name=NAVER_REFERENCE_SOURCE_NAME,
            stage=error.stage,
            theme_id=error.theme_id,
            theme_name=error.theme_name,
            code=error.code,
            source_url=error.source_url,
            error_message=error.message,
            payload=error.payload,
        )


def _without_theme_name_conflicts(
    connection: sqlite3.Connection,
    payload: Mapping[str, Any],
) -> tuple[dict[str, Any], list[ThemeImportParserError]]:
    resolved = normalize_payload(payload)
    themes = list(resolved.get("themes", []))
    kept_themes: list[dict[str, Any]] = []
    errors: list[ThemeImportParserError] = []
    for theme in themes:
        row = connection.execute(
            """
            SELECT theme_id, source_type, source_name
            FROM themes
            WHERE theme_name = ?
            """,
            (theme.get("theme_name"),),
        ).fetchone()
        if row is not None and row["theme_id"] != theme.get("theme_id"):
            errors.append(
                ThemeImportParserError(
                    stage="persist_theme",
                    message=(
                        "theme_name already exists with another theme_id; "
                        "skipping to avoid source overwrite"
                    ),
                    theme_id=str(theme.get("theme_id")),
                    theme_name=str(theme.get("theme_name")),
                    payload={
                        "existing_theme_id": row["theme_id"],
                        "existing_source_type": row["source_type"],
                        "existing_source_name": row["source_name"],
                    },
                )
            )
            continue
        kept_themes.append(theme)
    resolved["themes"] = kept_themes
    return resolved, errors
