from __future__ import annotations

import json
from pathlib import Path

from storage.gateway_command_store import canonical_json, hash_payload_json
from storage.gateway_order_broker_boundary import (
    RESOLUTION_TABLE,
    ensure_gateway_order_broker_boundary_schema,
)
from storage.sqlite import initialize_database, open_connection
from tools.resolve_order_broker_boundary import main

COMMAND_ID = "cmd-r1-unconfirmed"
RAW_ACCOUNT = "1234567890"
RAW_IDEMPOTENCY = "idem-account-1234567890"


def test_preview_is_read_only_and_redacts_raw_identifiers(
    tmp_path: Path,
    capsys,
) -> None:
    db_path = tmp_path / "preview.sqlite3"
    _seed_unconfirmed_boundary(db_path)
    before = _counts(db_path)

    exit_code = main(["--db", str(db_path), "--command-id", COMMAND_ID])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["status"] == "PREVIEW"
    assert payload["read_only"] is True
    assert payload["query_only"] is True
    assert payload["preview"]["raw_state"] == "UNCONFIRMED"
    assert len(payload["preview"]["source_boundary_fingerprint"]) == 64
    assert payload["preview"]["eligible"] is True
    assert _counts(db_path) == before
    rendered = json.dumps(payload, ensure_ascii=False)
    assert RAW_ACCOUNT not in rendered
    assert RAW_IDEMPOTENCY not in rendered


def test_apply_requires_all_acknowledgements_and_performs_no_write_on_reject(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    db_path = tmp_path / "ack-reject.sqlite3"
    _seed_unconfirmed_boundary(db_path)
    safe_env = _write_safe_env(tmp_path, db_path)
    monkeypatch.setenv("TRADING_ENV_FILE", str(safe_env))
    evidence = tmp_path / "artifact.json"
    evidence.write_text('{"orders":[]}', encoding="utf-8")
    fingerprint = _preview_fingerprint(db_path, capsys)

    args = _apply_args(db_path, evidence, fingerprint)
    args.remove("--acknowledge-routing-gate-change")
    exit_code = main(args)
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 2
    assert payload["status"] == "REJECTED"
    assert "ROUTING_GATE_CHANGE_ACK_REQUIRED" in payload["reason_codes"]
    assert _counts(db_path)["resolution"] == 0


def test_apply_is_append_only_idempotent_and_never_prints_evidence_path(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    db_path = tmp_path / "apply.sqlite3"
    _seed_unconfirmed_boundary(db_path)
    safe_env = _write_safe_env(tmp_path, db_path)
    monkeypatch.setenv("TRADING_ENV_FILE", str(safe_env))
    evidence_content = '{"verified":"broker-not-reached"}'
    evidence = tmp_path / f"HTS-{RAW_ACCOUNT}-private.json"
    evidence.write_text(evidence_content, encoding="utf-8")
    fingerprint = _preview_fingerprint(db_path, capsys)
    args = _apply_args(db_path, evidence, fingerprint)

    first_exit = main(args)
    first = json.loads(capsys.readouterr().out)
    second_exit = main(args)
    second = json.loads(capsys.readouterr().out)

    assert first_exit == 0
    assert second_exit == 0
    assert first["status"] == "APPLIED"
    assert second["status"] == "REPLAYED_EFFECTIVE"
    assert first["command_count_delta"] == 0
    assert first["order_command_count_delta"] == 0
    assert first["raw_boundary_changed"] is False
    assert first["preview_after"]["raw_state"] == "UNCONFIRMED"
    assert first["preview_after"]["effective_state"] == (
        "RESOLVED_BROKER_NOT_REACHED"
    )
    assert second["result"]["idempotent_replay"] is True
    counts = _counts(db_path)
    assert counts == {"command": 1, "boundary": 1, "resolution": 1}
    rendered = json.dumps([first, second], ensure_ascii=False)
    assert str(evidence) not in rendered
    assert evidence.name not in rendered
    assert evidence_content not in rendered
    assert RAW_ACCOUNT not in rendered
    assert RAW_IDEMPOTENCY not in rendered

    connection = open_connection(db_path)
    try:
        raw_state = connection.execute(
            "SELECT state FROM gateway_order_broker_boundaries WHERE command_id = ?",
            (COMMAND_ID,),
        ).fetchone()["state"]
    finally:
        connection.close()
    assert raw_state == "UNCONFIRMED"


def test_apply_rejects_implicit_or_default_environment_without_db_write(
    tmp_path: Path,
    capsys,
) -> None:
    db_path = tmp_path / "unsafe-env.sqlite3"
    _seed_unconfirmed_boundary(db_path)
    evidence = tmp_path / "artifact.json"
    evidence.write_text('{"orders":[]}', encoding="utf-8")
    fingerprint = _preview_fingerprint(db_path, capsys)

    exit_code = main(_apply_args(db_path, evidence, fingerprint))
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 2
    assert payload["status"] == "REJECTED"
    assert "TRADING_ENV_FILE_NOT_FOUND" in payload["reason_codes"]
    assert _counts(db_path)["resolution"] == 0


def test_apply_rejects_theme_market_scan_command_producer(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    db_path = tmp_path / "theme-producer.sqlite3"
    _seed_unconfirmed_boundary(db_path)
    safe_env = _write_safe_env(tmp_path, db_path)
    safe_env.write_text(
        safe_env.read_text(encoding="utf-8").replace(
            "THEME_REFRESH_QUEUE_MARKET_SCAN_COMMANDS=false",
            "THEME_REFRESH_QUEUE_MARKET_SCAN_COMMANDS=true",
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("TRADING_ENV_FILE", str(safe_env))
    evidence = tmp_path / "artifact.json"
    evidence.write_text('{"orders":[]}', encoding="utf-8")
    fingerprint = _preview_fingerprint(db_path, capsys)

    exit_code = main(_apply_args(db_path, evidence, fingerprint))
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 2
    assert payload["status"] == "REJECTED"
    assert (
        "COMMAND_PRODUCER_ENABLED:THEME_REFRESH_QUEUE_MARKET_SCAN_COMMANDS"
        in payload["reason_codes"]
    )
    assert _counts(db_path)["resolution"] == 0


def test_revoke_is_append_only_idempotent_and_reblocks_effective_boundary(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    db_path = tmp_path / "revoke.sqlite3"
    _seed_unconfirmed_boundary(db_path)
    safe_env = _write_safe_env(tmp_path, db_path)
    monkeypatch.setenv("TRADING_ENV_FILE", str(safe_env))
    evidence = tmp_path / "initial-artifact.json"
    evidence.write_text('{"orders":[]}', encoding="utf-8")
    fingerprint = _preview_fingerprint(db_path, capsys)

    resolve_args = _apply_args(db_path, evidence, fingerprint)
    assert main(resolve_args) == 0
    resolution = json.loads(capsys.readouterr().out)
    resolution_id = str(resolution["result"]["resolution_id"])
    revoke_fingerprint = _preview_fingerprint(db_path, capsys)
    correction_content = '{"correction":"operator-withdrawal"}'
    correction = tmp_path / f"correction-{RAW_ACCOUNT}-private.json"
    correction.write_text(correction_content, encoding="utf-8")
    revoke_args = _apply_args(
        db_path,
        correction,
        revoke_fingerprint,
        request_id="request.r1.revoke",
        evidence_ref="HTS_CORRECTION_ALPHA",
        revoke_resolution_id=resolution_id,
    )

    first_exit = main(revoke_args)
    first = json.loads(capsys.readouterr().out)
    second_exit = main(revoke_args)
    second = json.loads(capsys.readouterr().out)

    assert first_exit == 0
    assert second_exit == 0
    assert first["status"] == "REVOKED"
    assert second["status"] == "REPLAYED_EFFECTIVE"
    assert first["mode"] == "APPEND_ONLY_REVOCATION"
    assert first["reason_code"] == "OPERATOR_REVOKED_BROKER_NOT_REACHED"
    assert first["evidence_type"] == "SIMULATION_HTS_ORDER_HISTORY_CORRECTION"
    assert first["preview_after"]["raw_state"] == "UNCONFIRMED"
    assert first["preview_after"]["effective_state"] == "UNCONFIRMED"
    assert first["preview_after"]["resolution"] is None
    assert first["preview_after"]["resolution_event_count"] == 2
    assert second["result"]["idempotent_replay"] is True
    replayed_resolve_exit = main(resolve_args)
    replayed_resolve = json.loads(capsys.readouterr().out)
    assert replayed_resolve_exit == 2
    assert replayed_resolve["status"] == "REPLAYED_NOT_EFFECTIVE"
    assert replayed_resolve["result"]["idempotent_replay"] is True
    assert replayed_resolve["result"]["idempotent_replay_effective"] is False
    assert _counts(db_path) == {"command": 1, "boundary": 1, "resolution": 2}
    rendered = json.dumps([first, second], ensure_ascii=False)
    assert str(correction) not in rendered
    assert correction.name not in rendered
    assert correction_content not in rendered
    assert RAW_ACCOUNT not in rendered
    assert RAW_IDEMPOTENCY not in rendered


def test_apply_rejects_account_like_digits_in_opaque_labels(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    db_path = tmp_path / "account-label-reject.sqlite3"
    _seed_unconfirmed_boundary(db_path)
    safe_env = _write_safe_env(tmp_path, db_path)
    monkeypatch.setenv("TRADING_ENV_FILE", str(safe_env))
    evidence = tmp_path / "artifact.json"
    evidence.write_text('{"orders":[]}', encoding="utf-8")
    fingerprint = _preview_fingerprint(db_path, capsys)
    args = _apply_args(
        db_path,
        evidence,
        fingerprint,
        request_id="request.account1234-5678",
    )

    exit_code = main(args)
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 2
    assert payload["status"] == "REJECTED"
    assert "REQUEST_ID_INVALID" in payload["reason_codes"]
    assert _counts(db_path)["resolution"] == 0


def _seed_unconfirmed_boundary(db_path: Path) -> None:
    connection = initialize_database(db_path)
    now = "2026-07-07T00:22:33.942712Z"
    payload_json = canonical_json(
        {
            "account_id": RAW_ACCOUNT,
            "code": "005930",
            "side": "BUY",
            "mode": "LIVE_SIM",
            "live_sim_only": True,
            "live_real_allowed": False,
        }
    )
    connection.execute(
        """
        INSERT INTO gateway_commands (
            command_id,
            command_type,
            source,
            status,
            idempotency_key,
            payload_json,
            payload_hash,
            created_at,
            dispatched_at,
            expires_at,
            attempts,
            last_error
        )
        VALUES (?, 'send_order', 'live_sim', 'UNCONFIRMED', ?, ?, ?, ?, ?, ?, 1, ?)
        """,
        (
            COMMAND_ID,
            RAW_IDEMPOTENCY,
            payload_json,
            hash_payload_json(payload_json),
            now,
            now,
            now,
            "Gateway order dispatch timed out; reconciliation required.",
        ),
    )
    ensure_gateway_order_broker_boundary_schema(connection)
    connection.commit()
    connection.close()


def _write_safe_env(tmp_path: Path, db_path: Path) -> Path:
    env_path = tmp_path / "fast0-r1-safe.env"
    env_path.write_text(
        "\n".join(
            [
                f"TRADING_DB_PATH={db_path}",
                "TRADING_PROFILE=OBSERVE",
                "TRADING_MODE=OBSERVE",
                "TRADING_ALLOW_LIVE_SIM=false",
                "TRADING_ALLOW_LIVE_REAL=false",
                "LIVE_SIM_ENABLED=false",
                "LIVE_SIM_ORDER_ROUTING_ENABLED=false",
                "LIVE_SIM_GATEWAY_COMMAND_ENABLED=false",
                "LIVE_SIM_REPRICE_ENABLED=false",
                "LIVE_SIM_PILOT_PIPELINE_ENABLED=false",
                "LIVE_SIM_PILOT_AUTO_QUEUE_COMMAND=false",
                "LIVE_SIM_ORDER_PLAN_ROUTING_ENABLED=false",
                "LIVE_SIM_CANCEL_ENABLED=false",
                "LIVE_SIM_CANCEL_UNFILLED_ENABLED=false",
                "LIVE_SIM_EXIT_ENGINE_ENABLED=false",
                "LIVE_SIM_EXIT_ORDER_CREATION_ENABLED=false",
                "LIVE_SIM_EXIT_GATEWAY_COMMAND_ENABLED=false",
                "LIVE_SIM_EXIT_EOD_FLATTEN_ENABLED=false",
                "LIVE_SIM_RECONCILE_REQUEST_BROKER_SNAPSHOT_ENABLED=false",
                "LIVE_SIM_OPERATING_CYCLE_ENABLED=false",
                "LIVE_SIM_OPERATING_LOOP_ENABLED=false",
                "LIVE_SIM_OPERATING_LOOP_QUEUE_COMMANDS=false",
                "REALTIME_SUBSCRIPTION_QUEUE_COMMANDS=false",
                "DRY_RUN_ORDER_ROUTING_ENABLED=false",
                "DRY_RUN_GATEWAY_COMMAND_ENABLED=false",
                "DRY_RUN_EXIT_ORDER_ROUTING_ENABLED=false",
                "DRY_RUN_EXIT_GATEWAY_COMMAND_ENABLED=false",
                "THEME_REFRESH_QUEUE_MARKET_SCAN_COMMANDS=false",
                "LIVE_SIM_KILL_SWITCH=true",
            ]
        ),
        encoding="utf-8",
    )
    return env_path


def _preview_fingerprint(db_path: Path, capsys) -> str:
    exit_code = main(["--db", str(db_path), "--command-id", COMMAND_ID])
    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    return str(payload["preview"]["source_boundary_fingerprint"])


def _apply_args(
    db_path: Path,
    evidence: Path,
    fingerprint: str,
    *,
    request_id: str = "request.r1.alpha",
    evidence_ref: str = "HTS_EXPORT_ALPHA",
    revoke_resolution_id: str | None = None,
) -> list[str]:
    args = [
        "--db",
        str(db_path),
        "--command-id",
        COMMAND_ID,
        "--apply",
        "--request-id",
        request_id,
        "--expected-fingerprint",
        fingerprint,
        "--evidence-file",
        str(evidence),
        "--evidence-ref",
        evidence_ref,
        "--operator-id",
        "operator.alpha",
        "--acknowledge-late-evidence-precedence",
        "--acknowledge-routing-gate-change",
    ]
    if revoke_resolution_id is not None:
        args.extend(
            [
                "--revoke-resolution-id",
                revoke_resolution_id,
                "--acknowledge-correction-or-contradiction",
            ]
        )
    else:
        args.append("--confirm-no-broker-order-or-execution")
    return args


def _counts(db_path: Path) -> dict[str, int]:
    connection = open_connection(db_path)
    try:
        return {
            "command": int(
                connection.execute(
                    "SELECT COUNT(*) AS count FROM gateway_commands"
                ).fetchone()["count"]
            ),
            "boundary": int(
                connection.execute(
                    "SELECT COUNT(*) AS count FROM gateway_order_broker_boundaries"
                ).fetchone()["count"]
            ),
            "resolution": int(
                connection.execute(
                    f"SELECT COUNT(*) AS count FROM {RESOLUTION_TABLE}"
                ).fetchone()["count"]
            ),
        }
    finally:
        connection.close()
