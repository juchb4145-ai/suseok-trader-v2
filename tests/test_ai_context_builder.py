from __future__ import annotations

from apps.core_api import app
from domain.ai_sidecar.context import (
    AISidecarContextPacket,
    AISidecarContextSection,
    calculate_context_hash,
    packet_hash_material,
)
from fastapi.testclient import TestClient
from services.ai_sidecar.context_builder import _finalize_packet, build_context_packet
from services.ai_sidecar.context_policy import sanitize_order_context
from services.ai_sidecar.context_store import (
    get_context_packet,
    list_context_build_errors,
    list_context_packets,
    save_context_build_error,
    save_context_packet,
)
from services.ai_sidecar.redaction import redact_context
from services.config import Settings
from storage.sqlite import initialize_database


def test_context_packet_model_round_trip_and_hash_are_deterministic() -> None:
    section = AISidecarContextSection(
        section_name="dashboard_safety",
        source="dashboard",
        row_count=1,
        payload={"read_only": True, "warnings": ["OBSERVE_PASS is not order approval."]},
    )
    packet = AISidecarContextPacket(
        context_id="ctx-test",
        task_type="NO_TRADE_RCA",
        schema_version="ai-sidecar-context.v1",
        trade_date="2026-06-27",
        related_entity_type=None,
        related_entity_id=None,
        generated_at="2026-06-27T00:00:00Z",
        source_sections=[section],
        context_hash="placeholder",
        size_chars=1,
        max_size_chars=12000,
        missing_sections=["TRADE_TABLE_UNAVAILABLE"],
        warnings=["NO_ORDER_PATH_BY_DESIGN"],
        payload={"dashboard_safety": section.payload},
    )
    context_hash = calculate_context_hash(packet_hash_material(packet))
    packet = AISidecarContextPacket.from_dict(packet.to_dict() | {"context_hash": context_hash})

    restored = AISidecarContextPacket.from_dict(packet.to_dict())

    assert restored.to_dict() == packet.to_dict()
    assert calculate_context_hash(packet_hash_material(restored)) == context_hash
    assert restored.order_context_included is False


def test_redaction_masks_sensitive_values_without_touching_stock_code_or_market_numbers() -> None:
    redacted = redact_context(
        {
            "account_id": "1234567890",
            "token": "Bearer abc",
            "x-core-token": "local-secret",
            "path": r"C:\Users\tester\project\file.txt",
            "code": "005930",
            "price": 70000,
            "volume": 123456789,
            "candidate_instance_id": "CAND-2026-06-27-005930-1",
        }
    )

    assert redacted["account_id"] == "***MASKED***"
    assert redacted["token"] == "***SECRET_REDACTED***"
    assert redacted["x-core-token"] == "***SECRET_REDACTED***"
    assert redacted["path"] == "***PATH_REDACTED***"
    assert redacted["code"] == "005930"
    assert redacted["price"] == 70000
    assert redacted["volume"] == 123456789
    assert redacted["candidate_instance_id"].startswith("CAND-")


def test_order_context_restriction_drops_order_like_fields_but_keeps_safety_text() -> None:
    result = sanitize_order_context(
        {
            "order_intent": {"code": "005930"},
            "tool": "send_order",
            "note": "order disabled by design",
            "safe": {"command_status_counts": {"QUEUED": 0}},
        },
        allow_order_context=False,
    )

    assert result.dropped is True
    assert "order_intent" not in result.value
    assert "tool" not in result.value
    assert result.value["note"] == "order disabled by design"
    assert result.value["safe"]["command_status_counts"]["QUEUED"] == 0


def test_context_size_limit_truncates_optional_sections() -> None:
    settings = Settings(ai_sidecar_max_context_chars=1800)
    packet = _finalize_packet(
        task_type="NO_TRADE_RCA",
        trade_date="2026-06-27",
        related_entity_type=None,
        related_entity_id=None,
        sections=[
            AISidecarContextSection(
                section_name="dashboard_safety",
                source="dashboard",
                payload={"read_only": True},
            ),
            AISidecarContextSection(
                section_name="recent_errors",
                source="projection_errors",
                row_count=100,
                payload={"items": [{"error": "x" * 200} for _ in range(30)]},
            ),
        ],
        settings=settings,
    )

    assert packet.truncated is True
    assert "CONTEXT_TRUNCATED" in packet.warnings
    assert packet.context_hash == calculate_context_hash(packet_hash_material(packet))


def test_task_builders_create_read_only_packets_on_empty_database(tmp_path) -> None:
    connection = initialize_database(tmp_path / "context-builders.sqlite3")
    settings = Settings()

    packets = [
        build_context_packet(
            connection,
            "DAILY_MARKET_BRIEF",
            trade_date="2026-06-27",
            settings=settings,
        ),
        build_context_packet(
            connection,
            "THEME_BRIEF",
            related_entity_id="missing-theme",
            settings=settings,
        ),
        build_context_packet(
            connection,
            "CANDIDATE_BLOCK_RCA",
            related_entity_id="missing-candidate",
            settings=settings,
        ),
        build_context_packet(
            connection,
            "NO_TRADE_RCA",
            trade_date="2026-06-27",
            settings=settings,
        ),
        build_context_packet(connection, "TRADE_REVIEW", related_entity_id=None, settings=settings),
        build_context_packet(connection, "OPS_INCIDENT_SUMMARY", settings=settings),
        build_context_packet(connection, "CODEX_PROMPT_DRAFT", settings=settings),
    ]
    connection.close()

    assert [packet.task_type.value for packet in packets] == [
        "DAILY_MARKET_BRIEF",
        "THEME_BRIEF",
        "CANDIDATE_BLOCK_RCA",
        "NO_TRADE_RCA",
        "TRADE_REVIEW",
        "OPS_INCIDENT_SUMMARY",
        "CODEX_PROMPT_DRAFT",
    ]
    assert all(packet.schema_version == "ai-sidecar-context.v1" for packet in packets)
    assert all(packet.order_context_included is False for packet in packets)
    assert "TRADE_TABLE_UNAVAILABLE" in packets[4].missing_sections
    assert "OMS_UNAVAILABLE" in packets[4].missing_sections


def test_context_persistence_round_trip_and_error_listing(tmp_path) -> None:
    connection = initialize_database(tmp_path / "context-store.sqlite3")
    packet = build_context_packet(
        connection,
        "NO_TRADE_RCA",
        trade_date="2026-06-27",
        settings=Settings(),
    )

    context_id = save_context_packet(connection, packet)
    save_context_build_error(
        connection,
        task_type="NO_TRADE_RCA",
        trade_date="2026-06-27",
        error_message="fixture error",
        payload={"fixture": True},
    )
    restored = get_context_packet(connection, context_id)
    packets = list_context_packets(connection, task_type="NO_TRADE_RCA")
    errors = list_context_build_errors(connection)
    connection.close()

    assert restored is not None
    assert restored["context_id"] == context_id
    assert packets[0]["context_id"] == context_id
    assert errors[0]["error_message"] == "fixture error"


def test_ai_context_api_endpoints_are_get_read_only_without_api_key(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "context-api.sqlite3"))
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    with TestClient(app) as client:
        status = client.get("/api/ai-sidecar/context/status")
        preview = client.get(
            "/api/ai-sidecar/context/preview",
            params={"task_type": "NO_TRADE_RCA", "trade_date": "2026-06-27", "persist": True},
        )
        packets = client.get("/api/ai-sidecar/context/packets")
        detail = client.get(f"/api/ai-sidecar/context/packets/{preview.json()['context_id']}")
        errors = client.get("/api/ai-sidecar/context/errors")
        candidate = client.get("/api/ai-sidecar/context/candidate/missing-candidate")
        theme = client.get("/api/ai-sidecar/context/theme/missing-theme")
        no_trade = client.get("/api/ai-sidecar/context/no-trade/2026-06-27")

    assert status.status_code == 200
    assert status.json()["openai_client_available"] is False
    assert status.json()["execution_api_available"] is False
    assert status.json()["allow_order_context"] is False
    assert preview.status_code == 200
    assert preview.json()["task_type"] == "NO_TRADE_RCA"
    assert packets.status_code == 200
    assert packets.json()["packets"]
    assert detail.status_code == 200
    assert detail.json()["context_id"] == preview.json()["context_id"]
    assert errors.status_code == 200
    assert candidate.status_code == 200
    assert candidate.json()["task_type"] == "CANDIDATE_BLOCK_RCA"
    assert theme.status_code == 200
    assert theme.json()["task_type"] == "THEME_BRIEF"
    assert no_trade.status_code == 200
    assert no_trade.json()["task_type"] == "NO_TRADE_RCA"


def test_safety_regression_no_ai_execution_or_order_routes() -> None:
    route_methods = {
        route.path: route.methods
        for route in app.routes
        if route.path.startswith("/api/ai-sidecar")
    }

    assert "/api/ai-sidecar/run" not in route_methods
    assert all("POST" not in methods for methods in route_methods.values())
