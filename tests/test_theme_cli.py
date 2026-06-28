from __future__ import annotations

import json
import sys

from services.theme_importers.service import NaverThemeImportRunResult
from storage.sqlite import open_connection
from tools.import_naver_themes import main as import_naver_themes_main
from tools.import_theme_memberships import main as import_theme_memberships_main
from tools.rebuild_theme_snapshots import main as rebuild_theme_snapshots_main


def test_theme_import_and_rebuild_tools_use_configured_db(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "theme_cli.sqlite3"
    payload_path = tmp_path / "themes.json"
    payload_path.write_text(json.dumps(_payload(), ensure_ascii=False), encoding="utf-8")
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    monkeypatch.setattr(
        sys,
        "argv",
        ["import_theme_memberships", "--file", str(payload_path)],
    )

    assert import_theme_memberships_main() == 0

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "rebuild_theme_snapshots",
            "--theme-id",
            "semiconductor",
            "--calculated-at",
            "2026-06-26T00:00:00Z",
        ],
    )

    assert rebuild_theme_snapshots_main() == 0

    connection = open_connection(db_path)
    try:
        theme_count = connection.execute("SELECT COUNT(*) AS count FROM themes").fetchone()[
            "count"
        ]
        snapshot_count = connection.execute(
            "SELECT COUNT(*) AS count FROM theme_snapshots"
        ).fetchone()["count"]
    finally:
        connection.close()

    assert theme_count == 1
    assert snapshot_count == 1


def test_naver_theme_import_tool_dry_run_does_not_open_db(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "naver_cli.sqlite3"
    output_path = tmp_path / "naver_output.json"
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    monkeypatch.setattr("tools.import_naver_themes.NaverThemeImporter", _FakeNaverImporter)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "import_naver_themes",
            "--dry-run",
            "--limit-themes",
            "1",
            "--output",
            str(output_path),
        ],
    )

    assert import_naver_themes_main() == 0

    output = json.loads(output_path.read_text(encoding="utf-8"))
    assert output["status"] == "DRY_RUN"
    assert output["normalized_member_count"] == 2
    assert not db_path.exists()


def _payload() -> dict[str, object]:
    return {
        "source_type": "MOCK",
        "source_name": "cli_fixture",
        "themes": [
            {
                "theme_id": "semiconductor",
                "theme_name": "반도체",
                "members": [
                    {"code": "005930", "name": "삼성전자"},
                    {"code": "000660", "name": "SK하이닉스"},
                ],
            }
        ],
    }


class _FakeNaverImporter:
    def __init__(self, *, settings) -> None:
        self.settings = settings

    def run(self, *, connection=None, dry_run=False, limit_themes=None, replace=None):
        assert connection is None
        assert dry_run is True
        assert limit_themes == 1
        return NaverThemeImportRunResult(
            status="DRY_RUN",
            dry_run=True,
            replace=False,
            fetched_theme_count=1,
            fetched_member_count=2,
            normalized_theme_count=1,
            normalized_member_count=2,
            duplicate_count=0,
            parser_error_count=0,
            skipped_theme_count=0,
            payload={
                "source_type": "NAVER_REFERENCE",
                "source_name": "naver_theme",
                "themes": [
                    {
                        "theme_id": "naver_theme_101",
                        "theme_name": "반도체",
                        "members": [
                            {"code": "005930", "name": "삼성전자"},
                            {"code": "000660", "name": "SK하이닉스"},
                        ],
                    }
                ],
            },
        )
