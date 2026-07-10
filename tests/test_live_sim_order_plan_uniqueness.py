from __future__ import annotations

import json
import sqlite3

import pytest
from services.live_sim.order_plan_eligibility import (
    find_live_sim_intent_by_order_plan,
)
from services.live_sim.order_plan_intent import (
    create_live_sim_intent_from_order_plan,
)
from storage.live_sim_order_plan_uniqueness import (
    LIVE_SIM_ORDER_PLAN_UNIQUE_INDEX,
    LiveSimOrderPlanUniquenessError,
    get_live_sim_order_plan_uniqueness_status,
)
from storage.sqlite import initialize_database
from tests.test_live_sim_order_plan_pipeline import (
    _pilot_settings,
    _prepared_order_plan_connection,
)


def test_legacy_order_plan_ids_are_backfilled_with_partial_unique_index_and_reentrant(
    tmp_path,
) -> None:
    db_path = tmp_path / "legacy-live-sim-order-plan.sqlite3"
    _create_legacy_live_sim_intents(
        db_path,
        [
            {
                "intent_id": "legacy-plan-intent",
                "idempotency_key": "legacy-plan-key",
                "evidence": {"order_plan_id": "plan-legacy-1"},
            },
            {
                "intent_id": "legacy-generic-intent",
                "idempotency_key": "legacy-generic-key",
                "evidence": {"source": "candidate"},
            },
        ],
    )

    connection = initialize_database(db_path)
    migrated = connection.execute(
        """
        SELECT live_sim_intent_id, order_plan_id
        FROM live_sim_intents
        ORDER BY live_sim_intent_id
        """
    ).fetchall()
    index = _index_row(connection)
    status = get_live_sim_order_plan_uniqueness_status(connection)
    connection.close()

    assert [(row["live_sim_intent_id"], row["order_plan_id"]) for row in migrated] == [
        ("legacy-generic-intent", None),
        ("legacy-plan-intent", "plan-legacy-1"),
    ]
    assert bool(index["unique"]) is True
    assert bool(index["partial"]) is True
    assert status["status"] == "PASS"
    assert status["missing_backfill_count"] == 0

    reinitialized = initialize_database(db_path)
    rerun_status = get_live_sim_order_plan_uniqueness_status(reinitialized)
    row_count = reinitialized.execute(
        "SELECT COUNT(*) AS count FROM live_sim_intents"
    ).fetchone()["count"]
    reinitialized.close()

    assert rerun_status["status"] == "PASS"
    assert row_count == 2


def test_legacy_duplicate_order_plan_id_fails_closed_and_rolls_back_migration(
    tmp_path,
) -> None:
    db_path = tmp_path / "duplicate-live-sim-order-plan.sqlite3"
    _create_legacy_live_sim_intents(
        db_path,
        [
            {
                "intent_id": "duplicate-one",
                "idempotency_key": "duplicate-key-one",
                "evidence": {"order_plan_id": "plan-duplicate"},
            },
            {
                "intent_id": "duplicate-two",
                "idempotency_key": "duplicate-key-two",
                "evidence": {"order_plan_id": "plan-duplicate"},
            },
        ],
    )

    with pytest.raises(LiveSimOrderPlanUniquenessError) as raised:
        initialize_database(db_path)

    verification = sqlite3.connect(db_path)
    columns = {
        row[1]
        for row in verification.execute("PRAGMA table_info(live_sim_intents)").fetchall()
    }
    index = verification.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = ?",
        (LIVE_SIM_ORDER_PLAN_UNIQUE_INDEX,),
    ).fetchone()
    verification.close()

    assert "DUPLICATE_ORDER_PLAN_ID" in raised.value.reason_codes
    assert "order_plan_id" not in columns
    assert index is None


def test_existing_order_plan_column_evidence_mismatch_fails_closed(tmp_path) -> None:
    db_path = tmp_path / "mismatch-live-sim-order-plan.sqlite3"
    _create_legacy_live_sim_intents(
        db_path,
        [
            {
                "intent_id": "mismatch-intent",
                "idempotency_key": "mismatch-key",
                "order_plan_id": "plan-column",
                "evidence": {"order_plan_id": "plan-evidence"},
            }
        ],
        with_order_plan_id=True,
    )

    with pytest.raises(LiveSimOrderPlanUniquenessError) as raised:
        initialize_database(db_path)

    verification = sqlite3.connect(db_path)
    row = verification.execute(
        "SELECT order_plan_id, evidence_json FROM live_sim_intents"
    ).fetchone()
    index = verification.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = ?",
        (LIVE_SIM_ORDER_PLAN_UNIQUE_INDEX,),
    ).fetchone()
    verification.close()

    assert "ORDER_PLAN_ID_EVIDENCE_MISMATCH" in raised.value.reason_codes
    assert row == ("plan-column", json.dumps({"order_plan_id": "plan-evidence"}))
    assert index is None


def test_direct_lookup_survives_more_than_500_newer_rows_and_unique_index_blocks_duplicate(
    tmp_path,
) -> None:
    connection, order_plan_id = _prepared_order_plan_connection(
        tmp_path / "direct-order-plan-lookup.sqlite3"
    )
    intent = create_live_sim_intent_from_order_plan(
        connection,
        order_plan_id,
        settings=_pilot_settings(),
    )
    source_row = connection.execute(
        "SELECT * FROM live_sim_intents WHERE live_sim_intent_id = ?",
        (intent.live_sim_intent_id,),
    ).fetchone()
    columns = [row["name"] for row in connection.execute("PRAGMA table_info(live_sim_intents)")]
    insert_sql = (
        f"INSERT INTO live_sim_intents ({', '.join(columns)}) "
        f"VALUES ({', '.join('?' for _ in columns)})"
    )
    generic_rows = []
    for index in range(501):
        values = {column: source_row[column] for column in columns}
        values["live_sim_intent_id"] = f"generic-intent-{index:03d}"
        values["idempotency_key"] = f"generic-key-{index:03d}"
        values["order_plan_id"] = None
        values["evidence_json"] = "{}"
        generic_rows.append(tuple(values[column] for column in columns))
    connection.executemany(insert_sql, generic_rows)
    connection.commit()

    traced_statements: list[str] = []
    connection.set_trace_callback(traced_statements.append)
    found = find_live_sim_intent_by_order_plan(connection, order_plan_id)
    connection.set_trace_callback(None)

    duplicate = {column: source_row[column] for column in columns}
    duplicate["live_sim_intent_id"] = "duplicate-plan-intent"
    duplicate["idempotency_key"] = "duplicate-plan-key"
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(insert_sql, tuple(duplicate[column] for column in columns))
    connection.rollback()
    status = get_live_sim_order_plan_uniqueness_status(connection)
    connection.close()

    assert found is not None
    assert found["live_sim_intent_id"] == intent.live_sim_intent_id
    assert found["order_plan_id"] == order_plan_id
    assert any("WHERE order_plan_id =" in statement for statement in traced_statements)
    assert all("LIMIT 500" not in statement for statement in traced_statements)
    assert status["intent_count"] == 502
    assert status["status"] == "PASS"


def _create_legacy_live_sim_intents(
    db_path,
    rows: list[dict],
    *,
    with_order_plan_id: bool = False,
) -> None:
    order_plan_column = "order_plan_id TEXT," if with_order_plan_id else ""
    connection = sqlite3.connect(db_path)
    connection.execute(
        f"""
        CREATE TABLE live_sim_intents (
            live_sim_intent_id TEXT PRIMARY KEY,
            candidate_instance_id TEXT NOT NULL,
            strategy_observation_id TEXT NOT NULL,
            risk_observation_id TEXT NOT NULL,
            {order_plan_column}
            dry_run_intent_id TEXT,
            dry_run_order_id TEXT,
            trade_date TEXT NOT NULL,
            account_id TEXT NOT NULL,
            code TEXT NOT NULL,
            name TEXT NOT NULL,
            side TEXT NOT NULL,
            order_type TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            limit_price REAL,
            notional REAL NOT NULL,
            status TEXT NOT NULL,
            reason_codes_json TEXT NOT NULL DEFAULT '[]',
            evidence_json TEXT NOT NULL DEFAULT '{{}}',
            idempotency_key TEXT NOT NULL UNIQUE,
            gateway_command_id TEXT,
            live_sim_only INTEGER NOT NULL DEFAULT 1,
            live_real_allowed INTEGER NOT NULL DEFAULT 0,
            broker_order_sent INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            expires_at TEXT
        )
        """
    )
    columns = [
        "live_sim_intent_id",
        "candidate_instance_id",
        "strategy_observation_id",
        "risk_observation_id",
    ]
    if with_order_plan_id:
        columns.append("order_plan_id")
    columns.extend(
        [
            "trade_date",
            "account_id",
            "code",
            "name",
            "side",
            "order_type",
            "quantity",
            "limit_price",
            "notional",
            "status",
            "evidence_json",
            "idempotency_key",
            "created_at",
        ]
    )
    insert_sql = (
        f"INSERT INTO live_sim_intents ({', '.join(columns)}) "
        f"VALUES ({', '.join('?' for _ in columns)})"
    )
    for row in rows:
        values = {
            "live_sim_intent_id": row["intent_id"],
            "candidate_instance_id": "candidate-legacy",
            "strategy_observation_id": "strategy-legacy",
            "risk_observation_id": "risk-legacy",
            "order_plan_id": row.get("order_plan_id"),
            "trade_date": "2026-07-10",
            "account_id": "SIM-LEGACY",
            "code": "005930",
            "name": "legacy",
            "side": "BUY",
            "order_type": "LIMIT",
            "quantity": 1,
            "limit_price": 1000,
            "notional": 1000,
            "status": "CREATED",
            "evidence_json": json.dumps(row["evidence"]),
            "idempotency_key": row["idempotency_key"],
            "created_at": "2026-07-10T00:00:00Z",
        }
        connection.execute(insert_sql, tuple(values[column] for column in columns))
    connection.commit()
    connection.close()


def _index_row(connection: sqlite3.Connection):
    return next(
        row
        for row in connection.execute("PRAGMA index_list(live_sim_intents)")
        if row["name"] == LIVE_SIM_ORDER_PLAN_UNIQUE_INDEX
    )
