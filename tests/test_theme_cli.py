from __future__ import annotations

import json
import sys

from storage.sqlite import open_connection
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
