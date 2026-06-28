from __future__ import annotations

import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from bs4 import BeautifulSoup
from domain.broker.utils import datetime_to_wire, utc_now

from services.theme_importers.models import (
    NaverTheme,
    NaverThemeMember,
    ThemeImportParserError,
)
from services.theme_importers.normalize import normalize_theme_name

DEFAULT_NAVER_THEME_BASE_URL = "https://finance.naver.com/sise/theme.naver"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "suseok-trader-v2 naver-theme-reference-importer"
)

MappingSequence = dict[str, Sequence[NaverThemeMember]]


@dataclass(frozen=True, kw_only=True)
class NaverThemeFetchResult:
    fetched_at: str
    themes: Sequence[NaverTheme] = field(default_factory=tuple)
    members_by_source_theme_id: MappingSequence = field(default_factory=dict)
    errors: Sequence[ThemeImportParserError] = field(default_factory=tuple)


class NaverThemeFetcher:
    def __init__(
        self,
        *,
        base_url: str = DEFAULT_NAVER_THEME_BASE_URL,
        timeout_seconds: float = 10.0,
        request_sleep_seconds: float = 0.3,
        retries: int = 2,
        user_agent: str = DEFAULT_USER_AGENT,
    ) -> None:
        self.base_url = base_url
        self.timeout_seconds = timeout_seconds
        self.request_sleep_seconds = request_sleep_seconds
        self.retries = max(int(retries), 0)
        self.user_agent = user_agent

    def fetch(self, *, limit_themes: int | None = None) -> NaverThemeFetchResult:
        fetched_at = datetime_to_wire(utc_now())
        errors: list[ThemeImportParserError] = []
        try:
            list_html = self._fetch_text(self.base_url)
        except Exception as exc:
            return NaverThemeFetchResult(
                fetched_at=fetched_at,
                themes=(),
                members_by_source_theme_id={},
                errors=(
                    ThemeImportParserError(
                        stage="fetch_list",
                        message=str(exc),
                        source_url=self.base_url,
                    ),
                ),
            )

        themes, list_errors = parse_theme_list(list_html, base_url=self.base_url)
        errors.extend(list_errors)
        if limit_themes is not None:
            themes = themes[: max(int(limit_themes), 0)]

        members_by_source_theme_id: dict[str, Sequence[NaverThemeMember]] = {}
        for index, theme in enumerate(themes):
            if index > 0 and self.request_sleep_seconds > 0:
                time.sleep(self.request_sleep_seconds)
            try:
                detail_html = self._fetch_text(theme.source_url)
            except Exception as exc:
                errors.append(
                    ThemeImportParserError(
                        stage="fetch_detail",
                        message=str(exc),
                        source_url=theme.source_url,
                        theme_name=theme.theme_name,
                        source_theme_id=theme.source_theme_id,
                    )
                )
                continue
            members, detail_errors = parse_theme_detail(
                detail_html,
                theme_name=theme.theme_name,
                source_url=theme.source_url,
                source_theme_id=theme.source_theme_id,
            )
            members_by_source_theme_id[theme.source_theme_id] = members
            errors.extend(detail_errors)

        return NaverThemeFetchResult(
            fetched_at=fetched_at,
            themes=themes,
            members_by_source_theme_id=members_by_source_theme_id,
            errors=tuple(errors),
        )

    def _fetch_text(self, url: str) -> str:
        last_error: Exception | None = None
        for attempt in range(self.retries + 1):
            try:
                request = urllib.request.Request(url, headers={"User-Agent": self.user_agent})
                with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                    body = response.read()
                    charset = response.headers.get_content_charset()
                return _decode_response_body(body, charset)
            except (TimeoutError, urllib.error.URLError, urllib.error.HTTPError) as exc:
                last_error = exc
                if attempt < self.retries:
                    time.sleep(min(0.5 * (attempt + 1), 2.0))
        raise RuntimeError(f"failed to fetch {url}: {last_error}") from last_error


def parse_theme_list(
    html: str,
    *,
    base_url: str = DEFAULT_NAVER_THEME_BASE_URL,
) -> tuple[list[NaverTheme], list[ThemeImportParserError]]:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", class_=_has_theme_table_class)
    rows = table.find_all("tr") if table is not None else soup.find_all("tr")
    errors: list[ThemeImportParserError] = []
    themes: list[NaverTheme] = []

    if table is None:
        errors.append(
            ThemeImportParserError(
                stage="parse_list",
                message="theme list table not found",
                source_url=base_url,
            )
        )

    for row in rows:
        anchor = row.find("a", href=_is_theme_detail_href)
        if anchor is None:
            continue
        href = str(anchor.get("href") or "")
        source_theme_id = _query_value(href, "no")
        theme_name = normalize_theme_name(anchor.get_text(" ", strip=True))
        if not source_theme_id or not theme_name:
            errors.append(
                ThemeImportParserError(
                    stage="parse_list",
                    message="theme row missing source id or name",
                    source_url=urllib.parse.urljoin(base_url, href),
                    payload={"href": href, "theme_name": theme_name},
                )
            )
            continue
        cells = row.find_all("td")
        change_rate_text = _cell_text(cells, 1)
        rank = len(themes) + 1
        themes.append(
            NaverTheme(
                source_theme_id=source_theme_id,
                theme_name=theme_name,
                source_url=urllib.parse.urljoin(base_url, href),
                rank=rank,
                change_rate_text=change_rate_text,
                metadata={
                    "naver_rank": rank,
                    "naver_change_rate_text": change_rate_text,
                    "source_link_path": href,
                },
            )
        )

    if not themes:
        errors.append(
            ThemeImportParserError(
                stage="parse_list",
                message="no naver theme rows found",
                source_url=base_url,
            )
        )
    return themes, errors


def parse_theme_detail(
    html: str,
    *,
    theme_name: str,
    source_url: str,
    source_theme_id: str | None = None,
) -> tuple[list[NaverThemeMember], list[ThemeImportParserError]]:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", class_=_has_detail_table_class)
    rows = table.find_all("tr") if table is not None else soup.find_all("tr")
    errors: list[ThemeImportParserError] = []
    members: list[NaverThemeMember] = []

    if table is None:
        errors.append(
            ThemeImportParserError(
                stage="parse_detail",
                message="theme detail member table not found",
                source_url=source_url,
                theme_name=theme_name,
                source_theme_id=source_theme_id,
            )
        )

    for row in rows:
        anchor = row.find("a", href=_is_stock_item_href)
        if anchor is None:
            continue
        href = str(anchor.get("href") or "")
        code = _query_value(href, "code")
        name = normalize_theme_name(anchor.get_text(" ", strip=True))
        if not code or not name:
            errors.append(
                ThemeImportParserError(
                    stage="parse_detail",
                    message="member row missing code or name",
                    source_url=source_url,
                    theme_name=theme_name,
                    source_theme_id=source_theme_id,
                    payload={"href": href, "name": name},
                )
            )
            continue
        reason_text = _reason_text(row)
        rank = len(members) + 1
        members.append(
            NaverThemeMember(
                theme_name=theme_name,
                code=code,
                name=name,
                source_url=source_url,
                reason_text=reason_text,
                rank=rank,
                source_theme_id=source_theme_id,
                metadata={
                    "naver_member_rank": rank,
                    "source_link_path": href,
                },
            )
        )

    if not members:
        errors.append(
            ThemeImportParserError(
                stage="parse_detail",
                message="no naver theme members found",
                source_url=source_url,
                theme_name=theme_name,
                source_theme_id=source_theme_id,
            )
        )
    return members, errors


def _decode_response_body(body: bytes, charset: str | None) -> str:
    candidates = [charset, "euc-kr", "cp949", "utf-8"]
    for candidate in candidates:
        if not candidate:
            continue
        try:
            return body.decode(candidate)
        except UnicodeDecodeError:
            continue
    return body.decode("utf-8", errors="replace")


def _has_theme_table_class(value: Any) -> bool:
    classes = _class_tokens(value)
    return "theme" in classes and "type_1" in classes


def _has_detail_table_class(value: Any) -> bool:
    return "type_5" in _class_tokens(value)


def _class_tokens(value: Any) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, str):
        return set(value.split())
    if isinstance(value, Sequence):
        return {str(item) for item in value}
    return {str(value)}


def _is_theme_detail_href(value: Any) -> bool:
    href = str(value or "")
    if "sise_group_detail.naver" not in href:
        return False
    query = urllib.parse.parse_qs(urllib.parse.urlsplit(href).query)
    return query.get("type", [""])[0] == "theme" and bool(query.get("no"))


def _is_stock_item_href(value: Any) -> bool:
    href = str(value or "")
    if "item/main.naver" not in href:
        return False
    return bool(_query_value(href, "code"))


def _query_value(href: str, key: str) -> str | None:
    query = urllib.parse.parse_qs(urllib.parse.urlsplit(href).query)
    values = query.get(key)
    if not values:
        return None
    normalized = values[0].strip()
    return normalized or None


def _cell_text(cells: Sequence[Any], index: int) -> str | None:
    if index >= len(cells):
        return None
    text = normalize_theme_name(cells[index].get_text(" ", strip=True))
    return text or None


def _reason_text(row: Any) -> str | None:
    reason = row.find("p", class_="info_txt")
    if reason is None:
        return None
    text = normalize_theme_name(reason.get_text(" ", strip=True))
    return text or None
