from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from services.config import Settings
from services.theme_importers.models import NaverTheme, NaverThemeMember, ThemeImportParserError
from services.theme_importers.naver_theme import (
    NaverThemeFetchResult,
    parse_theme_detail,
    parse_theme_list,
)
from services.theme_importers.normalize import normalize_naver_theme_payload
from services.theme_importers.service import NaverThemeImporter
from services.theme_leadership.service import ThemeLeadershipService
from services.theme_leadership.universe import ThemeUniverseBuilder
from services.theme_service import import_theme_memberships, list_theme_members
from storage.sqlite import initialize_database

FIXTURES = Path(__file__).parent / "fixtures"


def test_naver_theme_list_and_detail_parse_fixture_html() -> None:
    list_html = (FIXTURES / "naver_theme_list_sample.html").read_text(encoding="utf-8")
    detail_html = (FIXTURES / "naver_theme_detail_sample.html").read_text(encoding="utf-8")

    themes, list_errors = parse_theme_list(
        list_html,
        base_url="https://finance.naver.com/sise/theme.naver",
    )
    members, detail_errors = parse_theme_detail(
        detail_html,
        theme_name=themes[0].theme_name,
        source_url=themes[0].source_url,
        source_theme_id=themes[0].source_theme_id,
    )

    assert list_errors == []
    assert detail_errors == []
    assert [theme.source_theme_id for theme in themes] == ["101", "202"]
    assert themes[0].theme_name == "반도체"
    assert themes[0].change_rate_text == "+3.20%"
    assert [member.code for member in members] == ["005930", "A000660", "005930"]
    assert members[0].reason_text == "메모리 반도체와 파운드리 사업을 영위."


def test_naver_normalize_validates_codes_and_removes_duplicates() -> None:
    theme = NaverTheme(
        source_theme_id="101",
        theme_name=" 반도체 ",
        source_url="https://finance.naver.com/sise/sise_group_detail.naver?type=theme&no=101",
        rank=1,
        change_rate_text="+3.20%",
    )
    members = [
        NaverThemeMember(
            theme_name="반도체",
            code="A005930",
            name=" 삼성전자 ",
            source_url=theme.source_url,
            reason_text="메모리 반도체",
            rank=1,
            source_theme_id="101",
        ),
        NaverThemeMember(
            theme_name="반도체",
            code="005930",
            name="삼성전자",
            source_url=theme.source_url,
            rank=2,
            source_theme_id="101",
        ),
        NaverThemeMember(
            theme_name="반도체",
            code="BAD",
            name="잘못된코드",
            source_url=theme.source_url,
            rank=3,
            source_theme_id="101",
        ),
        NaverThemeMember(
            theme_name="반도체",
            code="000660",
            name="SK하이닉스",
            source_url=theme.source_url,
            rank=4,
            source_theme_id="101",
        ),
    ]

    result = normalize_naver_theme_payload(
        [theme],
        {"101": members},
        fetched_at="2026-06-28T00:00:00Z",
        min_member_count=1,
    )
    normalized_theme = result.payload["themes"][0]

    assert result.duplicate_count == 1
    assert len(result.errors) == 1
    assert normalized_theme["theme_id"] == "naver_theme_101"
    assert normalized_theme["metadata"]["change_rate_text"] == "+3.20%"
    assert [member["code"] for member in normalized_theme["members"]] == ["005930", "000660"]
    assert normalized_theme["members"][0]["metadata"]["confidence"] == 0.8
    assert normalized_theme["members"][0]["metadata"]["not_order_signal"] is True


def test_naver_importer_dry_run_does_not_modify_database(tmp_path) -> None:
    connection = initialize_database(tmp_path / "dry_run.sqlite3")
    connection.close()
    importer = NaverThemeImporter(
        settings=_settings(tmp_path),
        fetcher=_FakeFetcher(_fetch_result()),
    )

    result = importer.run(dry_run=True, limit_themes=1)

    connection = initialize_database(tmp_path / "dry_run.sqlite3")
    try:
        theme_count = connection.execute("SELECT COUNT(*) AS count FROM themes").fetchone()[
            "count"
        ]
        batch_count = connection.execute(
            "SELECT COUNT(*) AS count FROM theme_import_batches"
        ).fetchone()["count"]
    finally:
        connection.close()

    assert result.status == "DRY_RUN"
    assert result.normalized_theme_count == 1
    assert result.normalized_member_count == 2
    assert theme_count == 0
    assert batch_count == 0


def test_naver_importer_upsert_replace_scope_and_leadership_connection(tmp_path) -> None:
    connection = initialize_database(tmp_path / "naver.sqlite3")
    settings = _settings(tmp_path, naver_theme_import_min_member_count=1)
    manual_payload = {
        "source_type": "MANUAL",
        "source_name": "operator",
        "themes": [
            {
                "theme_id": "manual_watch",
                "theme_name": "수동관심",
                "members": [{"code": "035420", "name": "NAVER"}],
            }
        ],
    }
    import_theme_memberships(connection, manual_payload)

    first = NaverThemeImporter(settings=settings, fetcher=_FakeFetcher(_fetch_result())).run(
        connection=connection
    )
    duplicate = NaverThemeImporter(settings=settings, fetcher=_FakeFetcher(_fetch_result())).run(
        connection=connection
    )
    append = NaverThemeImporter(
        settings=settings,
        fetcher=_FakeFetcher(_fetch_result(members=[("005930", "삼성전자")])),
    ).run(connection=connection, replace=False)
    after_append = list_theme_members(connection, "naver_theme_101")
    replaced = NaverThemeImporter(
        settings=settings,
        fetcher=_FakeFetcher(_fetch_result(members=[("005930", "삼성전자")])),
    ).run(connection=connection, replace=True)
    after_replace = list_theme_members(connection, "naver_theme_101")
    manual_members = list_theme_members(connection, "manual_watch")
    universe = ThemeUniverseBuilder().build(connection)
    leadership = ThemeLeadershipService(settings=settings).rebuild(connection)
    side_effect_counts = {
        table: connection.execute(f"SELECT COUNT(*) AS count FROM {table}").fetchone()["count"]
        for table in ("gateway_commands", "live_sim_intents", "live_sim_orders")
    }
    connection.close()

    assert first.status == "SUCCESS"
    assert duplicate.status == "SUCCESS"
    assert append.status == "SUCCESS"
    assert replaced.status == "SUCCESS"
    assert len(after_append) == 2
    assert sum(1 for member in after_append if member["active"]) == 2
    assert len(after_replace) == 2
    assert sum(1 for member in after_replace if member["active"]) == 1
    assert manual_members[0]["active"] is True
    assert any(member.source_type == "NAVER_REFERENCE" for member in universe)
    assert any(snapshot.theme_id == "naver_theme_101" for snapshot in leadership.snapshots)
    assert side_effect_counts == {
        "gateway_commands": 0,
        "live_sim_intents": 0,
        "live_sim_orders": 0,
    }


def test_naver_importer_empty_fetch_safe_aborts_and_records_errors(tmp_path) -> None:
    connection = initialize_database(tmp_path / "empty.sqlite3")
    fetch_result = NaverThemeFetchResult(
        fetched_at="2026-06-28T00:00:00Z",
        themes=(),
        members_by_source_theme_id={},
        errors=(
            ThemeImportParserError(
                stage="parse_list",
                message="no naver theme rows found",
                source_url="https://finance.naver.com/sise/theme.naver",
            ),
        ),
    )
    importer = NaverThemeImporter(settings=_settings(tmp_path), fetcher=_FakeFetcher(fetch_result))

    result = importer.run(connection=connection)
    batch = connection.execute("SELECT * FROM theme_import_batches").fetchone()
    error_count = connection.execute(
        "SELECT COUNT(*) AS count FROM theme_import_errors"
    ).fetchone()["count"]
    member_count = connection.execute("SELECT COUNT(*) AS count FROM theme_members").fetchone()[
        "count"
    ]
    connection.close()

    assert result.status == "ABORTED_EMPTY_FETCH"
    assert result.aborted is True
    assert batch["status"] == "ABORTED"
    assert error_count >= 1
    assert member_count == 0


class _FakeFetcher:
    def __init__(self, result: NaverThemeFetchResult) -> None:
        self.result = result

    def fetch(self, *, limit_themes: int | None = None) -> NaverThemeFetchResult:
        if limit_themes is None:
            return self.result
        themes = tuple(self.result.themes[:limit_themes])
        allowed = {theme.source_theme_id for theme in themes}
        members = {
            source_theme_id: values
            for source_theme_id, values in self.result.members_by_source_theme_id.items()
            if source_theme_id in allowed
        }
        return NaverThemeFetchResult(
            fetched_at=self.result.fetched_at,
            themes=themes,
            members_by_source_theme_id=members,
            errors=self.result.errors,
        )


def _fetch_result(
    *,
    members: Sequence[tuple[str, str]] = (("005930", "삼성전자"), ("000660", "SK하이닉스")),
) -> NaverThemeFetchResult:
    theme = NaverTheme(
        source_theme_id="101",
        theme_name="반도체",
        source_url="https://finance.naver.com/sise/sise_group_detail.naver?type=theme&no=101",
        rank=1,
        change_rate_text="+3.20%",
    )
    return NaverThemeFetchResult(
        fetched_at="2026-06-28T00:00:00Z",
        themes=(theme,),
        members_by_source_theme_id={
            "101": tuple(
                NaverThemeMember(
                    theme_name="반도체",
                    code=code,
                    name=name,
                    source_url=theme.source_url,
                    reason_text=f"{name} reference",
                    rank=index + 1,
                    source_theme_id="101",
                )
                for index, (code, name) in enumerate(members)
            )
        },
        errors=(),
    )


def _settings(tmp_path: Path, **overrides: object) -> Settings:
    values = {
        "trading_db_path": tmp_path / "naver.sqlite3",
        "naver_theme_import_request_sleep_seconds": 0.0,
        "naver_theme_import_max_themes": 10,
        "naver_theme_import_min_member_count": 2,
    }
    values.update(overrides)
    return Settings(**values)
