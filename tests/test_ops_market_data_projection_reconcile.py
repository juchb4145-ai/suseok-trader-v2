from __future__ import annotations

from tools import ops_market_data_projection_reconcile as tool


def test_ops_market_data_projection_reconcile_writes_summary(tmp_path, monkeypatch) -> None:
    calls: list[tuple[str, str]] = []

    def fake_fetch_json(url: str, *, token: str, method: str, timeout_sec: float):
        calls.append((method, url))
        if method == "POST":
            return {
                "ok": True,
                "status_code": 200,
                "url": url,
                "data": {"status": "PASS", "run_id": "run_api"},
            }
        return {
            "ok": True,
            "status_code": 200,
            "url": url,
            "data": {
                "latest_run": {
                    "run_id": "run_api",
                    "status": "PASS",
                    "append_only_ready": True,
                    "checked_event_count": 3,
                    "event_rowid_min": 10,
                    "event_rowid_max": 12,
                    "outbox_pending_count": 0,
                    "outbox_error_count": 0,
                    "outbox_dead_letter_count": 0,
                    "missing_projection_count": 0,
                    "watermark_risk_count": 0,
                    "synthetic_child_event_issue_count": 0,
                    "reason_codes": [],
                },
                "issues": [],
            },
        }

    monkeypatch.setattr(tool, "fetch_json", fake_fetch_json)

    report = tool.run_reconcile_report(
        core_url="http://127.0.0.1:8000",
        token="test-token",
        run_once=True,
        limit=3,
        timeout_sec=1,
        out_dir=tmp_path,
    )

    summary_path = report["report_paths"]["summary_md"]
    summary = tmp_path.glob("*/summary.md")
    summary_file = next(summary)
    assert summary_path == str(summary_file)
    assert "status: `PASS`" in summary_file.read_text(encoding="utf-8")
    assert calls[0][0] == "POST"
    assert "limit=3" in calls[0][1]
    assert calls[1][0] == "GET"


def test_render_console_summary_handles_missing_latest() -> None:
    assert (
        tool.render_console_summary({"latest": {"data": {"latest_run": None}}})
        == "market_data projection reconcile: no latest run"
    )
