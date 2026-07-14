from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest
from tools.archive_failed_append_only_daily_evidence import (
    archive_failed_evidence_session,
)


def _fixture(tmp_path: Path, *, verdict: str = "FAIL") -> dict[str, Path]:
    db_path = tmp_path / "append-only-10day.sqlite3"
    connection = sqlite3.connect(db_path)
    connection.execute("CREATE TABLE evidence (value TEXT NOT NULL)")
    connection.execute("INSERT INTO evidence VALUES ('preserved')")
    connection.commit()
    connection.close()
    session_path = Path(f"{db_path}.session.json")
    session_path.write_text(
        json.dumps(
            {
                "format": "append-only-daily-session/v1",
                "trade_date": "2026-07-13",
                "database_path": str(db_path),
            }
        ),
        encoding="utf-8",
    )
    latest_path = tmp_path / "latest.json"
    latest_path.write_text(
        json.dumps(
            {
                "trade_date": "2026-07-13",
                "expected_db_path": str(db_path),
                "verdict": {"status": verdict, "failures": ["TEST_FAILURE"]},
            }
        ),
        encoding="utf-8",
    )
    summary_path = tmp_path / "summary.md"
    summary_path.write_text("# Failed evidence\n", encoding="utf-8")
    return {
        "db": db_path,
        "session": session_path,
        "latest": latest_path,
        "summary": summary_path,
        "archive": tmp_path / "archive",
    }


def test_failed_evidence_archive_dry_run_keeps_canonical_files(tmp_path) -> None:
    paths = _fixture(tmp_path)

    result = archive_failed_evidence_session(
        db_path=paths["db"],
        session_state_path=paths["session"],
        latest_report_path=paths["latest"],
        summary_report_path=paths["summary"],
        archive_root=paths["archive"],
        apply=False,
    )

    assert result["status"] == "DRY_RUN"
    assert paths["db"].exists()
    assert paths["session"].exists()
    assert not Path(result["archive_dir"]).exists()


def test_failed_evidence_archive_moves_verified_source_and_session(tmp_path) -> None:
    paths = _fixture(tmp_path)

    result = archive_failed_evidence_session(
        db_path=paths["db"],
        session_state_path=paths["session"],
        latest_report_path=paths["latest"],
        summary_report_path=paths["summary"],
        archive_root=paths["archive"],
        apply=True,
    )

    archived_db = Path(result["archived_db_path"])
    assert result["status"] == "COMPLETED"
    assert result["source_quick_check"] == "ok"
    assert result["archived_quick_check"] == "ok"
    assert result["source_sha256"] == result["archived_sha256"]
    assert not paths["db"].exists()
    assert not paths["session"].exists()
    assert archived_db.exists()
    assert Path(result["manifest_path"]).exists()
    connection = sqlite3.connect(f"file:{archived_db.as_posix()}?mode=ro", uri=True)
    try:
        assert connection.execute("SELECT value FROM evidence").fetchone()[0] == "preserved"
    finally:
        connection.close()


def test_failed_evidence_archive_rejects_pass_report(tmp_path) -> None:
    paths = _fixture(tmp_path, verdict="PASS")

    with pytest.raises(ValueError, match="only a failed evidence report"):
        archive_failed_evidence_session(
            db_path=paths["db"],
            session_state_path=paths["session"],
            latest_report_path=paths["latest"],
            summary_report_path=paths["summary"],
            archive_root=paths["archive"],
            apply=True,
        )
