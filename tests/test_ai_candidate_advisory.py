from __future__ import annotations

import json
from collections.abc import Mapping

from apps.core_api import app
from fastapi.testclient import TestClient
from services.ai_advisory.context_builder import (
    build_candidate_scoring_context,
    build_candidate_scoring_prompt,
)
from services.ai_advisory.scorer import MockCandidateScorerProvider
from services.ai_advisory.service import score_ai_candidates
from services.ai_advisory.storage import list_latest_scores
from services.ai_advisory.validator import AdvisoryValidationError, validate_advisory_output
from services.config import Settings
from storage.sqlite import initialize_database
from tests.test_live_sim_order_plan_pipeline import _pilot_settings, _prepared_order_plan_connection


def test_ai_candidate_settings_default_safe_off() -> None:
    settings = Settings()

    assert settings.ai_candidate_scorer_enabled is False
    assert settings.ai_candidate_scorer_provider == "mock"
    assert settings.ai_candidate_scorer_store_raw_response is False
    assert settings.ai_candidate_scorer_allow_order_actions is False
    assert settings.ai_candidate_scorer_fail_open is True
    assert settings.ai_candidate_scorer_attach_to_order_plan is False
    assert settings.ai_candidate_scorer_attach_to_live_sim_run is False


def test_sqlite_initialization_creates_ai_advisory_tables(tmp_path) -> None:
    connection = initialize_database(tmp_path / "ai-advisory-schema.sqlite3")
    table_rows = connection.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
            AND name IN (
                'ai_candidate_scoring_runs',
                'ai_candidate_scores',
                'ai_risk_reward_suggestions',
                'ai_advisory_errors'
            )
        """
    ).fetchall()
    connection.close()

    assert {row["name"] for row in table_rows} == {
        "ai_candidate_scoring_runs",
        "ai_candidate_scores",
        "ai_risk_reward_suggestions",
        "ai_advisory_errors",
    }


def test_context_builder_handles_zero_one_and_ten_candidates(tmp_path) -> None:
    empty = initialize_database(tmp_path / "empty.sqlite3")
    empty_context = build_candidate_scoring_context(
        empty,
        settings=Settings(ai_candidate_scorer_enabled=True),
    )
    empty.close()

    one, _ = _prepared_order_plan_connection(tmp_path / "one.sqlite3")
    one_context = build_candidate_scoring_context(
        one,
        trade_date="2026-06-27",
        settings=_enabled_pilot_settings(),
    )
    one_json = json.dumps(one_context.to_dict(), ensure_ascii=False)
    one.close()

    many = initialize_database(tmp_path / "many.sqlite3")
    for index in range(12):
        _insert_latest_order_plan(many, index)
    ten_context = build_candidate_scoring_context(
        many,
        trade_date="2026-06-27",
        settings=Settings(ai_candidate_scorer_enabled=True, ai_candidate_scorer_max_candidates=10),
    )
    many.close()

    assert empty_context.to_dict()["candidate_count"] == 0
    assert one_context.to_dict()["candidate_count"] == 1
    assert "account_id" not in one_json
    assert "SIM-12345678" not in one_json
    assert ten_context.to_dict()["candidate_count"] == 10


def test_prompt_size_limit_is_applied(tmp_path) -> None:
    connection = initialize_database(tmp_path / "prompt.sqlite3")
    for index in range(10):
        _insert_latest_order_plan(connection, index)
    settings = Settings(
        ai_candidate_scorer_enabled=True,
        ai_candidate_scorer_max_candidates=10,
        ai_candidate_scorer_max_prompt_chars=1000,
    )

    context = build_candidate_scoring_context(connection, settings=settings)
    prompt = build_candidate_scoring_prompt(context, settings=settings)
    connection.close()

    assert prompt.input_chars <= 1000
    assert prompt.truncated is True


def test_mock_provider_is_deterministic() -> None:
    settings = Settings(ai_candidate_scorer_enabled=True)
    context = {
        "candidates": [
            {
                "code": "005930",
                "name": "삼성전자",
                "order_plan_status": "PLAN_READY",
                "strategy_status": "MATCHED_OBSERVATION",
                "risk_status": "OBSERVE_PASS",
                "entry_timing_state": "GOOD_PULLBACK",
            }
        ]
    }
    provider = MockCandidateScorerProvider()

    first = provider.score(context, settings=settings)
    second = provider.score(context, settings=settings)

    assert first == second
    assert "005930" in str(first)


def test_validator_accepts_selected_empty_and_clamps_risk_reward() -> None:
    settings = Settings(ai_candidate_scorer_enabled=True)
    advisory = validate_advisory_output(
        {
            "selected": [],
            "analysis": {"005930": "관망"},
            "score": {"005930": 64},
            "confidence": {"005930": 55},
            "risk_reward": {
                "005930": {
                    "stop_loss_pct": 0.2,
                    "take_profit_pct": 99,
                    "trailing_stop_pct": 0.5,
                    "max_hold_sec": 99,
                },
                "999999": {
                    "stop_loss_pct": 2,
                    "take_profit_pct": 4,
                    "trailing_stop_pct": 2,
                    "max_hold_sec": 1000,
                },
            },
            "candidate_flags": {"005930": ["good_pullback"]},
            "avoid": {"999999": "unknown code"},
            "summary": "관망",
            "no_trade_reason": "임계값 미달",
        },
        candidate_codes=["005930"],
        settings=settings,
    )
    payload = advisory.to_dict()

    assert payload["selected"] == []
    assert payload["risk_reward"]["005930"]["stop_loss_pct"] == 1.0
    assert payload["risk_reward"]["005930"]["take_profit_pct"] == 8.0
    assert payload["risk_reward"]["005930"]["max_hold_sec"] == 300
    assert "999999" not in payload["avoid"]


def test_validator_rejects_invalid_json_score_range_and_forbidden_action() -> None:
    settings = Settings(ai_candidate_scorer_enabled=True)
    valid_base = {
        "selected": ["005930"],
        "analysis": {"005930": "ok"},
        "score": {"005930": 82},
        "confidence": {"005930": 70},
        "risk_reward": {
            "005930": {
                "stop_loss_pct": 2,
                "take_profit_pct": 4,
                "trailing_stop_pct": 2,
                "max_hold_sec": 1000,
            }
        },
        "candidate_flags": {"005930": ["LEADER"]},
        "avoid": {},
        "summary": "ok",
        "no_trade_reason": None,
    }

    try:
        validate_advisory_output("not-json", candidate_codes=["005930"], settings=settings)
        raise AssertionError("expected invalid json")
    except AdvisoryValidationError:
        pass

    bad_score = dict(valid_base)
    bad_score["score"] = {"005930": 101}
    try:
        validate_advisory_output(bad_score, candidate_codes=["005930"], settings=settings)
        raise AssertionError("expected bad score")
    except AdvisoryValidationError:
        pass

    forbidden = dict(valid_base)
    forbidden["analysis"] = {"005930": "SEND_ORDER now"}
    try:
        validate_advisory_output(forbidden, candidate_codes=["005930"], settings=settings)
        raise AssertionError("expected forbidden action")
    except AdvisoryValidationError:
        pass


def test_scoring_run_persists_scores_and_has_no_order_side_effects(tmp_path) -> None:
    connection, order_plan_id = _prepared_order_plan_connection(tmp_path / "run.sqlite3")
    settings = _enabled_pilot_settings()
    before = _table_counts(
        connection,
        [
            "order_plan_drafts",
            "order_plan_drafts_latest",
            "live_sim_intents",
            "live_sim_orders",
            "gateway_commands",
        ],
    )

    result = score_ai_candidates(
        connection,
        trade_date="2026-06-27",
        settings=settings,
    )
    latest = list_latest_scores(connection)
    raw_response = connection.execute(
        "SELECT raw_response_json FROM ai_candidate_scoring_runs WHERE run_id = ?",
        (result.run_id,),
    ).fetchone()
    after = _table_counts(connection, before.keys())
    connection.close()

    assert result.status == "COMPLETED"
    assert result.selected_count >= 1
    assert latest["run"]["status"] == "COMPLETED"
    assert latest["scores"][0]["order_plan_id"] == order_plan_id
    assert latest["scores"][0]["score"] is not None
    assert latest["risk_reward_suggestions"]
    assert raw_response["raw_response_json"] is None
    assert before == after


def test_invalid_schema_is_stored_fail_open_without_order_side_effects(tmp_path) -> None:
    connection, _ = _prepared_order_plan_connection(tmp_path / "invalid.sqlite3")
    before = _table_counts(
        connection,
        ["order_plan_drafts", "live_sim_intents", "gateway_commands"],
    )

    result = score_ai_candidates(
        connection,
        trade_date="2026-06-27",
        settings=_enabled_pilot_settings(),
        provider=_InvalidSchemaProvider(),
    )
    error_count = connection.execute(
        "SELECT COUNT(*) AS count FROM ai_advisory_errors"
    ).fetchone()["count"]
    after = _table_counts(connection, before.keys())
    connection.close()

    assert result.status == "INVALID_SCHEMA"
    assert result.ok is False
    assert int(error_count) == 1
    assert before == after


def test_provider_timeout_is_fail_open(tmp_path) -> None:
    connection, _ = _prepared_order_plan_connection(tmp_path / "timeout.sqlite3")

    result = score_ai_candidates(
        connection,
        trade_date="2026-06-27",
        settings=_enabled_pilot_settings(),
        provider=_TimeoutProvider(),
    )
    run = connection.execute(
        "SELECT status FROM ai_candidate_scoring_runs WHERE run_id = ?",
        (result.run_id,),
    ).fetchone()
    connection.close()

    assert result.status == "TIMEOUT"
    assert run["status"] == "TIMEOUT"


def test_disabled_scoring_records_disabled_without_context(tmp_path) -> None:
    connection = initialize_database(tmp_path / "disabled.sqlite3")

    result = score_ai_candidates(connection, settings=Settings())
    row = connection.execute("SELECT status FROM ai_candidate_scoring_runs").fetchone()
    connection.close()

    assert result.status == "DISABLED"
    assert row["status"] == "DISABLED"


def test_ai_advisory_api_and_dashboard_are_read_only(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "api.sqlite3"
    connection, _ = _prepared_order_plan_connection(db_path)
    connection.close()
    _set_ai_advisory_env(monkeypatch, db_path)

    with TestClient(app) as client:
        status = client.get("/api/ai-advisory/status")
        unauthorized = client.post("/api/ai-advisory/score-candidates?trade_date=2026-06-27")
        created = client.post(
            "/api/ai-advisory/score-candidates?trade_date=2026-06-27&limit=10",
            headers={"X-Local-Token": "secret-token"},
        )
        latest = client.get("/api/ai-advisory/candidate-scores/latest")
        dashboard = client.get("/api/dashboard/snapshot")

    assert status.status_code == 200
    assert status.json()["advisory_only"] is True
    assert unauthorized.status_code == 401
    assert created.status_code == 200
    assert created.json()["no_order_side_effects"] is True
    assert latest.status_code == 200
    assert latest.json()["scores"]
    assert dashboard.status_code == 200
    assert dashboard.json()["ai_advisory"]["no_order_side_effects"] is True
    assert dashboard.json()["ai_advisory"]["order_controls_available"] is False


class _InvalidSchemaProvider:
    name = "invalid"

    def score(self, context: Mapping[str, object], *, settings: Settings) -> object:
        return {
            "selected": ["005930"],
            "analysis": {"005930": "ok"},
            "score": {"005930": 999},
            "confidence": {"005930": 70},
            "risk_reward": {
                "005930": {
                    "stop_loss_pct": 2,
                    "take_profit_pct": 4,
                    "trailing_stop_pct": 2,
                    "max_hold_sec": 1000,
                }
            },
            "candidate_flags": {"005930": ["LEADER"]},
            "avoid": {},
            "summary": "ok",
            "no_trade_reason": None,
        }


class _TimeoutProvider:
    name = "timeout"

    def score(self, context: Mapping[str, object], *, settings: Settings) -> object:
        raise TimeoutError("provider timeout")


def _enabled_pilot_settings(**overrides) -> Settings:
    values = {
        "ai_candidate_scorer_enabled": True,
        "ai_candidate_scorer_min_score": 0,
        "ai_candidate_scorer_min_confidence": 0,
    }
    values.update(overrides)
    return _pilot_settings(**values)


def _insert_latest_order_plan(connection, index: int) -> None:
    code = f"{5930 + index:06d}"
    status = "PLAN_READY" if index < 6 else ("WAIT_RETRY" if index < 9 else "DATA_WAIT")
    connection.execute(
        """
        INSERT INTO order_plan_drafts_latest (
            idempotency_key,
            order_plan_id,
            trade_date,
            candidate_instance_id,
            code,
            name,
            side,
            status,
            setup_type,
            entry_timing_state,
            price_location_state,
            theme_id,
            theme_name,
            theme_state,
            theme_rank,
            stock_role,
            priority_score,
            current_price,
            limit_price,
            limit_price_source,
            limit_price_offset_ticks,
            suggested_quantity,
            suggested_notional,
            max_notional,
            risk_budget_source,
            expires_at,
            reason_codes_json,
            evidence_json,
            observe_only,
            not_order_intent,
            created_at
        )
        VALUES (?, ?, '2026-06-27', ?, ?, ?, 'BUY', ?, 'PULLBACK',
            'GOOD_PULLBACK', 'VWAP_OK', 'theme-ai', 'AI테마', 'LEADING', 1,
            'LEADER_CANDIDATE', ?, 10000, 10000, 'BEST_ASK', 0, 1, 10000,
            100000, 'CONFIG', '2999-01-01T00:00:00Z', '[]', '{}', 1, 1,
            '2026-06-27T00:00:00Z')
        """,
        (
            f"idem-{index}",
            f"plan-{index}",
            f"candidate-{index}",
            code,
            f"종목{index}",
            status,
            100 - index,
        ),
    )
    connection.commit()


def _table_counts(connection, table_names) -> dict[str, int]:
    result: dict[str, int] = {}
    for table_name in table_names:
        row = connection.execute(f"SELECT COUNT(*) AS count FROM {table_name}").fetchone()
        result[str(table_name)] = int(row["count"])
    return result


def _set_ai_advisory_env(monkeypatch, db_path) -> None:
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    monkeypatch.setenv("TRADING_CORE_TOKEN", "secret-token")
    monkeypatch.setenv("TRADING_PROFILE", "LIVE_SIM_PILOT")
    monkeypatch.setenv("TRADING_MODE", "LIVE_SIM")
    monkeypatch.setenv("TRADING_ALLOW_LIVE_SIM", "true")
    monkeypatch.setenv("TRADING_ALLOW_LIVE_REAL", "false")
    monkeypatch.setenv("LIVE_SIM_ENABLED", "true")
    monkeypatch.setenv("LIVE_SIM_ORDER_ROUTING_ENABLED", "true")
    monkeypatch.setenv("LIVE_SIM_GATEWAY_COMMAND_ENABLED", "true")
    monkeypatch.setenv("LIVE_SIM_ACCOUNT_ID", "SIM-12345678")
    monkeypatch.setenv("LIVE_SIM_KILL_SWITCH", "false")
    monkeypatch.setenv("LIVE_SIM_STALE_TICK_SEC", "999999999")
    monkeypatch.setenv("LIVE_SIM_PILOT_PIPELINE_ENABLED", "true")
    monkeypatch.setenv("LIVE_SIM_ORDER_PLAN_ROUTING_ENABLED", "true")
    monkeypatch.setenv("LIVE_SIM_ORDER_PLAN_STALE_SEC", "999999999")
    monkeypatch.setenv("AI_CANDIDATE_SCORER_ENABLED", "true")
    monkeypatch.setenv("AI_CANDIDATE_SCORER_PROVIDER", "mock")
    monkeypatch.setenv("AI_CANDIDATE_SCORER_MIN_SCORE", "0")
    monkeypatch.setenv("AI_CANDIDATE_SCORER_MIN_CONFIDENCE", "0")
