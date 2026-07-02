from __future__ import annotations

from api.routes import ai_sidecar as ai_sidecar_routes
from apps.core_api import app
from domain.ai_sidecar.context import AISidecarContextPacket
from fastapi.testclient import TestClient
from services.ai_sidecar.context_store import save_context_packet
from services.ai_sidecar.openai_client import MockAISidecarModelClient
from services.ai_sidecar.request_store import get_ai_request, list_ai_insights
from services.ai_sidecar.runner import run_ai_sidecar_task
from services.config import Settings
from storage.sqlite import initialize_database


def _settings() -> Settings:
    return Settings(ai_sidecar_enabled_value=True, ai_sidecar_model="mock-model")


def _packet(
    *,
    context_id: str = "ctx-stored",
    order_context_included: bool = False,
) -> AISidecarContextPacket:
    return AISidecarContextPacket(
        context_id=context_id,
        task_type="NO_TRADE_RCA",
        schema_version="ai-sidecar-context.v1",
        trade_date="2026-06-27",
        related_entity_type=None,
        related_entity_id=None,
        generated_at="2026-06-27T00:00:00Z",
        source_sections=[],
        context_hash="ctx-hash",
        size_chars=100,
        max_size_chars=12000,
        redaction_applied=True,
        order_context_included=order_context_included,
        payload={"summary": "stored redacted context"},
    )


def _valid_output() -> dict[str, object]:
    return {
        "summary": "Candidate is blocked by missing context.",
        "severity": "LOW",
        "root_cause": "Required observation data is missing.",
        "operator_action": "REVIEW_ONLY",
        "suggested_checks": ["Check candidate context"],
        "confidence": 0.64,
        "forbidden_actions_confirmed": True,
    }


def test_runner_success_with_mock_client_saves_request_and_insight(tmp_path) -> None:
    connection = initialize_database(tmp_path / "ai-runner.sqlite3")
    mock = MockAISidecarModelClient(output=_valid_output())

    result = run_ai_sidecar_task(
        connection,
        "CANDIDATE_BLOCK_RCA",
        related_entity_type="candidate",
        related_entity_id="missing-candidate",
        model_client=mock,
        settings=_settings(),
    )
    request = get_ai_request(connection, result.request_id)
    insights = list_ai_insights(connection, include_output=True)
    connection.close()

    assert result.ok is True
    assert result.status == "COMPLETED"
    assert result.observe_only is True
    assert result.no_trading_side_effects is True
    assert request["status"] == "COMPLETED"
    assert request["context_id"] == result.context_id
    assert request["output_schema_name"] == "candidate_block_rca_output_v1"
    assert len(insights) == 1
    assert insights[0]["request_id"] == result.request_id
    assert mock.requests[0].output_schema["strict"] is True
    assert "tools" not in mock.requests[0].output_schema


def test_runner_invalid_output_records_failure_without_insight(tmp_path) -> None:
    connection = initialize_database(tmp_path / "ai-invalid.sqlite3")
    invalid_output = {key: value for key, value in _valid_output().items() if key != "summary"}
    mock = MockAISidecarModelClient(output=invalid_output)

    result = run_ai_sidecar_task(
        connection,
        "NO_TRADE_RCA",
        trade_date="2026-06-27",
        model_client=mock,
        settings=_settings(),
    )
    request = get_ai_request(connection, result.request_id)
    insights = list_ai_insights(connection)
    connection.close()

    assert result.ok is False
    assert result.status == "AI_OUTPUT_INVALID"
    assert request["status"] == "AI_OUTPUT_INVALID"
    assert request["validation_error"]
    assert insights == []


def test_runner_forbidden_action_output_is_rejected_without_insight(tmp_path) -> None:
    connection = initialize_database(tmp_path / "ai-forbidden.sqlite3")
    mock = MockAISidecarModelClient(output=_valid_output() | {"operator_action": "BUY"})

    result = run_ai_sidecar_task(
        connection,
        "NO_TRADE_RCA",
        trade_date="2026-06-27",
        model_client=mock,
        settings=_settings(),
    )
    insights = list_ai_insights(connection)
    connection.close()

    assert result.ok is False
    assert result.status in {"AI_OUTPUT_INVALID", "POLICY_REJECTED"}
    assert insights == []


def test_runner_timeout_and_model_error_do_not_create_insights(tmp_path) -> None:
    for status in ("timeout", "model_error"):
        connection = initialize_database(tmp_path / f"ai-{status}.sqlite3")
        mock = MockAISidecarModelClient(status=status)

        result = run_ai_sidecar_task(
            connection,
            "NO_TRADE_RCA",
            trade_date="2026-06-27",
            model_client=mock,
            settings=_settings(),
        )
        insights = list_ai_insights(connection)
        connection.close()

        assert result.ok is False
        assert result.status in {"TIMEOUT", "MODEL_ERROR"}
        assert insights == []


def test_runner_uses_stored_context_id_and_rejects_order_context_when_disabled(tmp_path) -> None:
    connection = initialize_database(tmp_path / "ai-context-id.sqlite3")
    save_context_packet(connection, _packet())
    mock = MockAISidecarModelClient(output=_valid_output())

    result = run_ai_sidecar_task(
        connection,
        "NO_TRADE_RCA",
        context_id="ctx-stored",
        model_client=mock,
        settings=_settings(),
    )

    save_context_packet(
        connection,
        _packet(context_id="ctx-order-context", order_context_included=True),
    )
    blocked_mock = MockAISidecarModelClient(output=_valid_output())
    blocked = run_ai_sidecar_task(
        connection,
        "NO_TRADE_RCA",
        context_id="ctx-order-context",
        model_client=blocked_mock,
        settings=_settings(),
    )
    connection.close()

    assert result.ok is True
    assert result.context_id == "ctx-stored"
    assert blocked.ok is False
    assert blocked.status == "POLICY_REJECTED"
    assert blocked_mock.requests == []


def test_runner_missing_context_records_context_error(tmp_path) -> None:
    connection = initialize_database(tmp_path / "ai-missing-context.sqlite3")
    mock = MockAISidecarModelClient(output=_valid_output())

    result = run_ai_sidecar_task(
        connection,
        "NO_TRADE_RCA",
        context_id="missing-context",
        model_client=mock,
        settings=_settings(),
    )
    connection.close()

    assert result.ok is False
    assert result.status == "CONTEXT_ERROR"
    assert mock.requests == []


def test_run_api_requires_token_when_configured_and_can_use_mock_client(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "ai-api-token.sqlite3"))
    monkeypatch.setenv("TRADING_CORE_TOKEN", "secret-token")
    monkeypatch.setenv("AI_SIDECAR_ENABLED", "true")
    monkeypatch.setenv("AI_SIDECAR_MODEL", "mock-model")
    ai_sidecar_routes._MODEL_CLIENT_OVERRIDE = MockAISidecarModelClient(output=_valid_output())
    try:
        with TestClient(app) as client:
            missing = client.post("/api/ai-sidecar/run", json={"task_type": "NO_TRADE_RCA"})
            wrong = client.post(
                "/api/ai-sidecar/run",
                json={"task_type": "NO_TRADE_RCA"},
                headers={"X-Local-Token": "wrong"},
            )
            accepted = client.post(
                "/api/ai-sidecar/run",
                json={"task_type": "NO_TRADE_RCA"},
                headers={"X-Core-Token": "secret-token"},
            )
            requests = client.get("/api/ai-sidecar/requests")
            insights = client.get("/api/ai-sidecar/insights")
            request_detail = client.get(
                f"/api/ai-sidecar/requests/{accepted.json()['request_id']}"
            )
            insight_detail = client.get(
                f"/api/ai-sidecar/insights/{accepted.json()['insight_id']}"
            )
    finally:
        ai_sidecar_routes._MODEL_CLIENT_OVERRIDE = None

    assert missing.status_code == 401
    assert wrong.status_code == 403
    assert accepted.status_code == 200
    assert accepted.json()["status"] == "COMPLETED"
    assert requests.json()["requests"][0]["status"] == "COMPLETED"
    assert insights.json()["insights"][0]["request_id"] == accepted.json()["request_id"]
    assert request_detail.json()["request_id"] == accepted.json()["request_id"]
    assert insight_detail.json()["insight_id"] == accepted.json()["insight_id"]
    assert "output" in insight_detail.json()


def test_run_api_disabled_records_failure_without_model_call(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "ai-api-disabled.sqlite3"))
    monkeypatch.setenv("AI_SIDECAR_ENABLED", "false")
    monkeypatch.setenv("AI_SIDECAR_MODEL", "mock-model")
    mock = MockAISidecarModelClient(output=_valid_output())
    ai_sidecar_routes._MODEL_CLIENT_OVERRIDE = mock
    try:
        with TestClient(app) as client:
            run = client.post(
                "/api/ai-sidecar/run",
                json={"task_type": "NO_TRADE_RCA"},
                headers={"X-Local-Token": "test-token"},
            )
            requests = client.get("/api/ai-sidecar/requests")
    finally:
        ai_sidecar_routes._MODEL_CLIENT_OVERRIDE = None

    assert run.status_code == 200
    assert run.json()["ok"] is False
    assert run.json()["status"] == "AI_DISABLED"
    assert requests.json()["requests"][0]["status"] == "AI_DISABLED"
    assert mock.requests == []


def test_run_api_missing_api_key_records_failure_but_status_stays_available(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "ai-api-key.sqlite3"))
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.setenv("AI_SIDECAR_ENABLED", "true")
    monkeypatch.setenv("AI_SIDECAR_MODEL", "gpt-test")

    with TestClient(app) as client:
        run = client.post(
            "/api/ai-sidecar/run",
            json={"task_type": "NO_TRADE_RCA"},
            headers={"X-Local-Token": "test-token"},
        )
        health = client.get("/health")
        core_status = client.get("/api/status")
        execution_status = client.get("/api/ai-sidecar/execution/status")

    assert run.status_code == 200
    assert run.json()["ok"] is False
    assert run.json()["status"] == "API_KEY_MISSING"
    assert health.status_code == 200
    assert core_status.status_code == 200
    assert execution_status.status_code == 200
    assert execution_status.json()["openai_client_available"] is False
