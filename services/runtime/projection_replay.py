from __future__ import annotations

import hashlib
import json
import sqlite3
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Literal

from domain.broker.events import GatewayEvent
from domain.broker.utils import (
    datetime_to_wire,
    new_message_id,
    parse_timestamp,
    utc_now,
)
from storage.event_store import append_gateway_event
from storage.gateway_command_store import canonical_json, hash_payload_json
from storage.projection_outbox import enqueue_projection_jobs_for_gateway_event
from storage.sqlite import initialize_database, open_connection

from services.config import Settings, TradingMode, TradingProfile, load_settings
from services.market_data_service import MARKET_DATA_EVENT_TYPES, MARKET_PROJECTION_TABLES
from services.market_index_service import (
    MARKET_INDEX_PROJECTION_TABLES,
    is_market_index_projection_event,
    process_market_index_event,
)
from services.runtime.market_data_projection_reconcile import (
    run_market_data_projection_reconcile,
)
from services.runtime.market_index_projection_reconcile import (
    run_market_index_projection_reconcile,
)
from services.runtime.projection_outbox_worker import process_projection_outbox_batch

BUNDLE_FORMAT = "projection-replay-bundle/v1"
REPORT_FORMAT = "projection-replay-report/v1"
SAFE_REPLAY_EVENT_TYPES = frozenset(
    {
        *MARKET_DATA_EVENT_TYPES,
        "market_symbols",
        "market_index_tick",
    }
)
ORDER_EVENT_TYPES = frozenset(
    {
        "command_started",
        "command_ack",
        "command_failed",
        "execution_event",
        "order_pre_ack",
        "order_broker_unconfirmed",
        "kiwoom_order_chejan",
        "kiwoom_balance_chejan",
        "kiwoom_special_chejan",
    }
)
REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_REPORT_ROOT = REPOSITORY_ROOT / "reports" / "projection_replay"
DEFAULT_WORK_ROOT = REPOSITORY_ROOT / "storage" / "replay" / "projection_parity"

_EVENTS_FILE_NAME = "events.jsonl"
_MANIFEST_FILE_NAME = "manifest.json"
_NONDETERMINISTIC_COLUMNS = frozenset(
    {
        "created_at",
        "updated_at",
        "last_processed_at",
        "last_success_processed_at",
        "last_error_processed_at",
    }
)
_REPLAY_WRITABLE_TABLES = frozenset(
    {
        "raw_events",
        "gateway_events",
        "gateway_status",
        "projection_outbox",
        "projection_event_results",
        "projection_watermarks",
        "sqlite_sequence",
        *MARKET_PROJECTION_TABLES,
        *MARKET_INDEX_PROJECTION_TABLES,
    }
)
_SIDE_EFFECT_EXACT_TABLES = frozenset(
    {
        "gateway_commands",
        "gateway_command_events",
        "gateway_command_dedupe_keys",
        "gateway_order_broker_boundaries",
        "gateway_order_broker_boundary_resolutions",
        "incremental_evaluation_queue",
        "order_plan_drafts",
        "order_plan_drafts_latest",
        "candidate_condition_fusion",
    }
)
_SIDE_EFFECT_PREFIXES = (
    "dry_run_",
    "live_sim_",
    "entry_timing_",
    "strategy_",
    "risk_",
    "candidate_",
    "theme_",
    "exit_",
)
_WRITE_ACTIONS = frozenset({sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_DELETE})


@dataclass(frozen=True, kw_only=True)
class ReplayBundleResult:
    bundle_dir: Path
    manifest_path: Path
    events_path: Path
    event_count: int
    event_types: tuple[str, ...]
    trade_date: str | None
    record_sha256: str
    event_order_sha256: str
    venue_counts: Mapping[str, int]

    def to_dict(self) -> dict[str, Any]:
        return {
            "bundle_dir": str(self.bundle_dir),
            "manifest_path": str(self.manifest_path),
            "events_path": str(self.events_path),
            "event_count": self.event_count,
            "event_types": list(self.event_types),
            "trade_date": self.trade_date,
            "record_sha256": self.record_sha256,
            "event_order_sha256": self.event_order_sha256,
            "venue_counts": dict(self.venue_counts),
        }


@dataclass(frozen=True, kw_only=True)
class ReplayImportResult:
    bundle: ReplayBundleResult
    target_db_path: Path
    imported_event_count: int
    imported_event_order_sha256: str
    order_preserved: bool
    side_effect_table_counts_before: Mapping[str, int]
    side_effect_table_counts_after: Mapping[str, int]
    side_effect_table_delta: Mapping[str, int]
    blocked_write_attempts: Sequence[Mapping[str, Any]]

    @property
    def no_trading_side_effects(self) -> bool:
        return not self.blocked_write_attempts and all(
            value == 0 for value in self.side_effect_table_delta.values()
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "bundle": self.bundle.to_dict(),
            "target_db_path": str(self.target_db_path),
            "imported_event_count": self.imported_event_count,
            "imported_event_order_sha256": self.imported_event_order_sha256,
            "order_preserved": self.order_preserved,
            "side_effect_table_counts_before": dict(self.side_effect_table_counts_before),
            "side_effect_table_counts_after": dict(self.side_effect_table_counts_after),
            "side_effect_table_delta": dict(self.side_effect_table_delta),
            "blocked_write_attempts": [dict(item) for item in self.blocked_write_attempts],
            "no_order_side_effects": self.no_trading_side_effects,
            "no_trading_side_effects": self.no_trading_side_effects,
        }


@dataclass(frozen=True, kw_only=True)
class ProjectionSnapshot:
    overall_sha256: str
    tables: Mapping[str, Mapping[str, Any]]
    projection_counts_by_venue: Mapping[str, int]

    def to_dict(self) -> dict[str, Any]:
        return {
            "overall_sha256": self.overall_sha256,
            "tables": {key: dict(value) for key, value in self.tables.items()},
            "projection_counts_by_venue": dict(self.projection_counts_by_venue),
        }


@dataclass(frozen=True, kw_only=True)
class ProjectionReplayPathResult:
    mode: Literal["inline", "worker"]
    db_path: Path
    imported_event_count: int
    event_order_sha256: str
    order_preserved: bool
    inline_applied_count: int
    inline_ignored_count: int
    inline_error_count: int
    worker_batch_count: int
    worker_claimed_count: int
    worker_applied_count: int
    worker_error_count: int
    worker_dead_letter_count: int
    market_data_outbox_counts: Mapping[str, int]
    projection_outbox_counts: Mapping[str, Mapping[str, int]]
    reconcile_status: str
    reconcile_runs: Sequence[Mapping[str, Any]]
    market_index_reconcile_status: str
    market_index_reconcile_run: Mapping[str, Any] | None
    snapshot: ProjectionSnapshot
    side_effect_table_counts_before: Mapping[str, int]
    side_effect_table_counts_after: Mapping[str, int]
    side_effect_table_delta: Mapping[str, int]
    blocked_write_attempts: Sequence[Mapping[str, Any]]

    @property
    def no_trading_side_effects(self) -> bool:
        return not self.blocked_write_attempts and all(
            value == 0 for value in self.side_effect_table_delta.values()
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "db_path": str(self.db_path),
            "imported_event_count": self.imported_event_count,
            "event_order_sha256": self.event_order_sha256,
            "order_preserved": self.order_preserved,
            "inline_applied_count": self.inline_applied_count,
            "inline_ignored_count": self.inline_ignored_count,
            "inline_error_count": self.inline_error_count,
            "worker_batch_count": self.worker_batch_count,
            "worker_claimed_count": self.worker_claimed_count,
            "worker_applied_count": self.worker_applied_count,
            "worker_error_count": self.worker_error_count,
            "worker_dead_letter_count": self.worker_dead_letter_count,
            "market_data_outbox_counts": dict(self.market_data_outbox_counts),
            "projection_outbox_counts": {
                key: dict(value) for key, value in self.projection_outbox_counts.items()
            },
            "reconcile_status": self.reconcile_status,
            "reconcile_runs": [dict(item) for item in self.reconcile_runs],
            "market_index_reconcile_status": self.market_index_reconcile_status,
            "market_index_reconcile_run": (
                None
                if self.market_index_reconcile_run is None
                else dict(self.market_index_reconcile_run)
            ),
            "snapshot": self.snapshot.to_dict(),
            "side_effect_table_counts_before": dict(self.side_effect_table_counts_before),
            "side_effect_table_counts_after": dict(self.side_effect_table_counts_after),
            "side_effect_table_delta": dict(self.side_effect_table_delta),
            "blocked_write_attempts": [dict(item) for item in self.blocked_write_attempts],
            "no_order_side_effects": self.no_trading_side_effects,
            "no_trading_side_effects": self.no_trading_side_effects,
        }


@dataclass(frozen=True, kw_only=True)
class ProjectionReplayParityResult:
    run_id: str
    status: str
    bundle: ReplayBundleResult
    work_dir: Path
    inline: ProjectionReplayPathResult
    worker: ProjectionReplayPathResult
    projection_hash_match: bool
    mismatched_tables: tuple[str, ...]
    failures: tuple[str, ...]
    warnings: tuple[str, ...]
    created_at: str = field(default_factory=lambda: datetime_to_wire(utc_now()))

    @property
    def no_trading_side_effects(self) -> bool:
        return self.inline.no_trading_side_effects and self.worker.no_trading_side_effects

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "status": self.status,
            "bundle": self.bundle.to_dict(),
            "work_dir": str(self.work_dir),
            "inline": self.inline.to_dict(),
            "worker": self.worker.to_dict(),
            "projection_hash_match": self.projection_hash_match,
            "mismatched_tables": list(self.mismatched_tables),
            "failures": list(self.failures),
            "warnings": list(self.warnings),
            "observe_only": True,
            "live_sim_allowed": False,
            "live_real_allowed": False,
            "production_db_writes_allowed": False,
            "no_order_side_effects": self.no_trading_side_effects,
            "no_trading_side_effects": self.no_trading_side_effects,
            "created_at": self.created_at,
        }


def export_replay_bundle(
    *,
    source_db_path: str | Path,
    bundle_dir: str | Path,
    trade_date: str | None = None,
    event_types: Iterable[str] = MARKET_DATA_EVENT_TYPES,
) -> ReplayBundleResult:
    source_path = _require_existing_file(source_db_path, "source_db_path")
    output_dir = _require_new_directory_path(bundle_dir, "bundle_dir")
    normalized_event_types = _normalize_safe_event_types(event_types)
    bounds = _trade_date_bounds(trade_date)
    output_dir.mkdir(parents=True)
    events_path = output_dir / _EVENTS_FILE_NAME
    manifest_path = output_dir / _MANIFEST_FILE_NAME
    record_hasher = hashlib.sha256()
    order_hasher = hashlib.sha256()
    venue_counts: Counter[str] = Counter()
    exported_event_types: set[str] = set()
    event_count = 0
    first_event_rowid: int | None = None
    last_event_rowid: int | None = None

    connection = _open_read_only_connection(source_path)
    try:
        connection.row_factory = sqlite3.Row
        query, params = _export_query(normalized_event_types, bounds=bounds)
        cursor = connection.execute(query, params)
        with events_path.open("w", encoding="utf-8", newline="\n") as stream:
            for row in cursor:
                record = _export_record(row, sequence=event_count + 1)
                _validate_record_safety(record)
                line = canonical_json(record)
                stream.write(line)
                stream.write("\n")
                _update_hash(record_hasher, line)
                _update_hash(order_hasher, _order_component(record))
                venue_counts[_event_venue(record["event"])] += 1
                exported_event_types.add(str(record["event"]["event_type"]).lower())
                event_count += 1
                source_rowid = int(record["source_event_rowid"])
                first_event_rowid = first_event_rowid or source_rowid
                last_event_rowid = source_rowid
    finally:
        connection.close()

    manifest = {
        "format": BUNDLE_FORMAT,
        "created_at": datetime_to_wire(utc_now()),
        "source_db_path": str(source_path),
        "trade_date": trade_date,
        "event_types": sorted(exported_event_types),
        "requested_event_types": list(normalized_event_types),
        "event_count": event_count,
        "first_source_event_rowid": first_event_rowid,
        "last_source_event_rowid": last_event_rowid,
        "record_sha256": record_hasher.hexdigest(),
        "event_order_sha256": order_hasher.hexdigest(),
        "venue_counts": dict(sorted(venue_counts.items())),
        "accepted_events_only": True,
        "order_event_types_excluded": True,
        "production_db_writes_allowed": False,
        "no_order_side_effects": True,
    }
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return _bundle_result(output_dir, manifest)


def validate_replay_bundle(bundle_dir: str | Path) -> ReplayBundleResult:
    resolved_dir = _require_existing_directory(bundle_dir, "bundle_dir")
    manifest = _read_manifest(resolved_dir)
    events_path = resolved_dir / _EVENTS_FILE_NAME
    if not events_path.is_file():
        raise FileNotFoundError(f"replay bundle events file is missing: {events_path}")

    record_hasher = hashlib.sha256()
    order_hasher = hashlib.sha256()
    venue_counts: Counter[str] = Counter()
    event_types: set[str] = set()
    event_count = 0
    previous_source_rowid = 0
    seen_event_ids: set[str] = set()
    for record in _iter_bundle_records(events_path):
        event_count += 1
        if int(record.get("sequence") or 0) != event_count:
            raise ValueError(f"bundle sequence gap at record {event_count}")
        source_rowid = int(record.get("source_event_rowid") or 0)
        if source_rowid <= previous_source_rowid:
            raise ValueError("source_event_rowid must be strictly increasing")
        previous_source_rowid = source_rowid
        _validate_record_safety(record)
        event = GatewayEvent.from_dict(_mapping(record.get("event"), "event"))
        if event.event_id in seen_event_ids:
            raise ValueError(f"duplicate event_id in replay bundle: {event.event_id}")
        seen_event_ids.add(event.event_id)
        payload_hash = hash_payload_json(canonical_json(event.payload))
        if payload_hash != str(record.get("payload_hash") or ""):
            raise ValueError(f"payload hash mismatch for event_id={event.event_id}")
        received_at = str(record.get("source_received_at") or "")
        parse_timestamp(received_at, "source_received_at")
        line = canonical_json(record)
        _update_hash(record_hasher, line)
        _update_hash(order_hasher, _order_component(record))
        event_types.add(event.event_type.strip().lower())
        venue_counts[_event_venue(event.to_dict())] += 1

    expected_count = int(manifest.get("event_count") or 0)
    if event_count != expected_count:
        raise ValueError(
            f"bundle event_count mismatch: manifest={expected_count} actual={event_count}"
        )
    if record_hasher.hexdigest() != str(manifest.get("record_sha256") or ""):
        raise ValueError("bundle record_sha256 mismatch")
    if order_hasher.hexdigest() != str(manifest.get("event_order_sha256") or ""):
        raise ValueError("bundle event_order_sha256 mismatch")
    manifest_types = tuple(sorted(str(item).lower() for item in manifest["event_types"]))
    if tuple(sorted(event_types)) != manifest_types:
        raise ValueError("bundle event_types do not match events.jsonl")
    if dict(sorted(venue_counts.items())) != dict(manifest.get("venue_counts") or {}):
        raise ValueError("bundle venue_counts do not match events.jsonl")
    return _bundle_result(resolved_dir, manifest)


def import_replay_bundle(
    *,
    bundle_dir: str | Path,
    target_db_path: str | Path,
    operational_db_path: str | Path | None = None,
) -> ReplayImportResult:
    bundle = validate_replay_bundle(bundle_dir)
    target_path = _resolve_target_db_path(
        target_db_path,
        operational_db_path=operational_db_path,
    )
    connection = initialize_database(target_path)
    blocked_attempts = install_projection_replay_write_guard(connection)
    before = _side_effect_table_counts(connection)
    imported_count = 0
    try:
        for record in _iter_bundle_records(bundle.events_path):
            event = GatewayEvent.from_dict(_mapping(record["event"], "event"))
            result = append_gateway_event(connection, event)
            if not result.accepted or result.duplicate or result.status != "ACCEPTED":
                raise ValueError(
                    "replay import rejected event "
                    f"{event.event_id}: status={result.status} duplicate={result.duplicate}"
                )
            received_at = str(record["source_received_at"])
            connection.execute(
                "UPDATE raw_events SET received_at = ? WHERE event_id = ?",
                (received_at, event.event_id),
            )
            connection.execute(
                "UPDATE gateway_events SET received_at = ? WHERE event_id = ?",
                (received_at, event.event_id),
            )
            connection.commit()
            imported_count += 1
        imported_order_hash = _database_event_order_sha256(connection)
        after = _side_effect_table_counts(connection)
    finally:
        connection.close()
    delta = _count_delta(before, after)
    order_preserved = imported_order_hash == bundle.event_order_sha256
    result = ReplayImportResult(
        bundle=bundle,
        target_db_path=target_path,
        imported_event_count=imported_count,
        imported_event_order_sha256=imported_order_hash,
        order_preserved=order_preserved,
        side_effect_table_counts_before=before,
        side_effect_table_counts_after=after,
        side_effect_table_delta=delta,
        blocked_write_attempts=tuple(blocked_attempts),
    )
    if not order_preserved:
        raise RuntimeError("replay import did not preserve authoritative event order")
    if not result.no_trading_side_effects:
        raise RuntimeError("replay import attempted or produced a trading side effect")
    return result


def run_projection_replay_parity(
    *,
    bundle_dir: str | Path,
    work_root: str | Path = DEFAULT_WORK_ROOT,
    operational_db_path: str | Path | None = None,
    settings: Settings | None = None,
    batch_size: int = 50,
) -> ProjectionReplayParityResult:
    bundle = validate_replay_bundle(bundle_dir)
    bounded_batch_size = min(max(int(batch_size), 1), 500)
    run_id = new_message_id("projection_replay")
    root = Path(work_root).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    run_dir = root / run_id
    run_dir.mkdir()
    base_settings = settings or load_settings()
    resolved_operational_path = operational_db_path or base_settings.trading_db_path

    inline = _run_projection_path(
        mode="inline",
        bundle=bundle,
        db_path=run_dir / "inline.sqlite3",
        operational_db_path=resolved_operational_path,
        base_settings=base_settings,
        batch_size=bounded_batch_size,
    )
    worker = _run_projection_path(
        mode="worker",
        bundle=bundle,
        db_path=run_dir / "worker.sqlite3",
        operational_db_path=resolved_operational_path,
        base_settings=base_settings,
        batch_size=bounded_batch_size,
    )
    mismatched_tables = tuple(
        sorted(
            table
            for table in inline.snapshot.tables
            if inline.snapshot.tables[table] != worker.snapshot.tables.get(table)
        )
    )
    projection_hash_match = (
        inline.snapshot.overall_sha256 == worker.snapshot.overall_sha256 and not mismatched_tables
    )
    failures: list[str] = []
    warnings: list[str] = []
    if bundle.event_count == 0:
        failures.append("EMPTY_REPLAY_BUNDLE")
    if not inline.order_preserved or not worker.order_preserved:
        failures.append("EVENT_ORDER_NOT_PRESERVED")
    if not projection_hash_match:
        failures.append("INLINE_WORKER_PROJECTION_HASH_MISMATCH")
    for path_result in (inline, worker):
        prefix = path_result.mode.upper()
        if path_result.reconcile_status != "PASS":
            failures.append(f"{prefix}_RECONCILE_NOT_PASS")
        if path_result.market_index_reconcile_status not in {"NOT_RUN", "PASS"}:
            failures.append(f"{prefix}_MARKET_INDEX_RECONCILE_NOT_PASS")
        if path_result.worker_error_count or path_result.worker_dead_letter_count:
            failures.append(f"{prefix}_OUTBOX_ERROR_OR_DEAD_LETTER")
        for projection_name, counts in path_result.projection_outbox_counts.items():
            projection_code = projection_name.upper()
            if int(counts.get("PENDING") or 0):
                failures.append(f"{prefix}_{projection_code}_OUTBOX_PENDING")
            if int(counts.get("ERROR") or 0) or int(counts.get("DEAD_LETTER") or 0):
                failures.append(
                    f"{prefix}_{projection_code}_OUTBOX_ERROR_OR_DEAD_LETTER"
                )
        if not path_result.no_trading_side_effects:
            failures.append(f"{prefix}_TRADING_SIDE_EFFECT_DETECTED")
    if not any(key == "NXT" for key in bundle.venue_counts):
        warnings.append("NXT_EVENT_NOT_PRESENT")
    status = "FAIL" if failures else "WARN" if warnings else "PASS"
    return ProjectionReplayParityResult(
        run_id=run_id,
        status=status,
        bundle=bundle,
        work_dir=run_dir,
        inline=inline,
        worker=worker,
        projection_hash_match=projection_hash_match,
        mismatched_tables=mismatched_tables,
        failures=tuple(sorted(set(failures))),
        warnings=tuple(sorted(set(warnings))),
    )


def install_projection_replay_write_guard(
    connection: sqlite3.Connection,
) -> list[dict[str, Any]]:
    blocked_attempts: list[dict[str, Any]] = []

    def authorizer(
        action_code: int,
        arg1: str | None,
        arg2: str | None,
        database_name: str | None,
        trigger_name: str | None,
    ) -> int:
        del arg2, trigger_name
        table_name = str(arg1 or "").strip().lower()
        if action_code in _WRITE_ACTIONS and table_name not in _REPLAY_WRITABLE_TABLES:
            blocked_attempts.append(
                {
                    "action_code": int(action_code),
                    "table_name": table_name,
                    "database_name": database_name,
                }
            )
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    connection.set_authorizer(authorizer)
    return blocked_attempts


def get_projection_replay_status(
    report_root: str | Path | None = None,
) -> dict[str, Any]:
    root = Path(report_root or DEFAULT_REPORT_ROOT).expanduser().resolve()
    candidates = sorted(root.glob("*/raw.json"), reverse=True) if root.is_dir() else []
    base = {
        "read_only": True,
        "observe_only": True,
        "replay_execution_available": False,
        "production_db_writes_allowed": False,
        "live_sim_allowed": False,
        "live_real_allowed": False,
        "no_order_side_effects": True,
        "no_trading_side_effects": True,
        "report_root": str(root),
    }
    if not candidates:
        return {
            **base,
            "status": "NOT_RUN",
            "latest_report_path": None,
            "latest_run": None,
            "reason_codes": ["PROJECTION_REPLAY_REPORT_NOT_FOUND"],
        }
    latest_path = candidates[0]
    try:
        payload = json.loads(latest_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict) or payload.get("format") != REPORT_FORMAT:
            raise ValueError("unsupported projection replay report format")
        verdict = _mapping(payload.get("verdict"), "verdict")
        parity = _mapping(payload.get("parity"), "parity")
        bundle = _mapping(parity.get("bundle"), "bundle")
        return {
            **base,
            "status": str(verdict.get("status") or "INVALID"),
            "no_order_side_effects": bool(verdict.get("no_order_side_effects", False)),
            "no_trading_side_effects": bool(verdict.get("no_trading_side_effects", False)),
            "latest_report_path": str(latest_path),
            "latest_run": {
                "generated_at": payload.get("generated_at"),
                "run_id": parity.get("run_id"),
                "status": parity.get("status"),
                "event_count": bundle.get("event_count"),
                "trade_date": bundle.get("trade_date"),
                "venue_counts": bundle.get("venue_counts") or {},
                "projection_hash_match": parity.get("projection_hash_match"),
                "mismatched_tables": parity.get("mismatched_tables") or [],
                "inline_reconcile_status": _nested_value(parity, "inline", "reconcile_status"),
                "worker_reconcile_status": _nested_value(parity, "worker", "reconcile_status"),
                "failures": verdict.get("failures") or [],
                "warnings": verdict.get("warnings") or [],
            },
            "reason_codes": list(verdict.get("failures") or [])
            + list(verdict.get("warnings") or []),
        }
    except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
        return {
            **base,
            "status": "INVALID",
            "latest_report_path": str(latest_path),
            "latest_run": None,
            "reason_codes": ["PROJECTION_REPLAY_REPORT_INVALID"],
            "error_message": str(exc),
        }


def _run_projection_path(
    *,
    mode: Literal["inline", "worker"],
    bundle: ReplayBundleResult,
    db_path: Path,
    operational_db_path: str | Path,
    base_settings: Settings,
    batch_size: int,
) -> ProjectionReplayPathResult:
    imported = import_replay_bundle(
        bundle_dir=bundle.bundle_dir,
        target_db_path=db_path,
        operational_db_path=operational_db_path,
    )
    connection = open_connection(db_path)
    blocked_attempts = install_projection_replay_write_guard(connection)
    replay_settings = _projection_replay_settings(
        base_settings,
        db_path=db_path,
        worker_apply=mode == "worker",
    )
    inline_applied = 0
    inline_ignored = 0
    inline_errors = 0
    worker_batch_count = 0
    worker_claimed = 0
    worker_applied = 0
    worker_errors = 0
    worker_dead_letters = 0
    try:
        for row in _iter_imported_gateway_events(connection):
            event = _gateway_event_from_row(row)
            enqueue_projection_jobs_for_gateway_event(
                connection,
                event,
                event_rowid=int(row["event_rowid"]),
            )
            if mode == "inline" and event.event_type in MARKET_DATA_EVENT_TYPES:
                result = _process_inline_projection(connection, event, replay_settings)
                inline_applied += result[0]
                inline_ignored += result[1]
                inline_errors += result[2]
            if mode == "inline" and is_market_index_projection_event(
                event,
                settings=replay_settings,
            ):
                result = process_market_index_event(
                    connection,
                    event,
                    settings=replay_settings,
                )
                inline_applied += result.applied_count
                inline_ignored += result.ignored_count
                inline_errors += result.error_count

        for projection_name in ("market_data", "market_index", "market_regime"):
            while _projection_pending_count(connection, projection_name) > 0:
                batch = process_projection_outbox_batch(
                    connection,
                    settings=replay_settings,
                    limit=batch_size,
                    owner_id=f"{mode}-projection-replay-{projection_name}",
                    apply_projection=mode == "worker",
                    projection_name=projection_name,
                )
                worker_batch_count += 1
                worker_claimed += batch.claimed_count
                worker_applied += batch.applied_count
                worker_errors += batch.error_count
                worker_dead_letters += batch.dead_letter_count
                if batch.claimed_count == 0:
                    break
        projection_outbox_counts = _projection_outbox_counts(connection)
        outbox_counts = projection_outbox_counts["market_data"]
        reconcile_runs = _run_reconcile_chunks(connection, settings=replay_settings)
        reconcile_status = _aggregate_reconcile_status(reconcile_runs)
        market_index_reconcile_run = _run_market_index_reconcile(
            connection,
            settings=replay_settings,
        )
        market_index_reconcile_status = (
            "NOT_RUN"
            if market_index_reconcile_run is None
            else str(market_index_reconcile_run.get("status") or "FAIL")
        )
        snapshot = _projection_snapshot(connection)
        after = _side_effect_table_counts(connection)
    finally:
        connection.close()
    combined_blocked = tuple(imported.blocked_write_attempts) + tuple(blocked_attempts)
    combined_before = imported.side_effect_table_counts_before
    combined_after = after
    combined_delta = _count_delta(combined_before, combined_after)
    return ProjectionReplayPathResult(
        mode=mode,
        db_path=db_path,
        imported_event_count=imported.imported_event_count,
        event_order_sha256=imported.imported_event_order_sha256,
        order_preserved=imported.order_preserved,
        inline_applied_count=inline_applied,
        inline_ignored_count=inline_ignored,
        inline_error_count=inline_errors,
        worker_batch_count=worker_batch_count,
        worker_claimed_count=worker_claimed,
        worker_applied_count=worker_applied,
        worker_error_count=worker_errors,
        worker_dead_letter_count=worker_dead_letters,
        market_data_outbox_counts=outbox_counts,
        projection_outbox_counts=projection_outbox_counts,
        reconcile_status=reconcile_status,
        reconcile_runs=tuple(reconcile_runs),
        market_index_reconcile_status=market_index_reconcile_status,
        market_index_reconcile_run=market_index_reconcile_run,
        snapshot=snapshot,
        side_effect_table_counts_before=combined_before,
        side_effect_table_counts_after=combined_after,
        side_effect_table_delta=combined_delta,
        blocked_write_attempts=combined_blocked,
    )


def _projection_replay_settings(
    settings: Settings,
    *,
    db_path: Path,
    worker_apply: bool,
) -> Settings:
    return replace(
        settings,
        trading_profile=TradingProfile.OBSERVE,
        trading_mode=TradingMode.OBSERVE,
        trading_db_path=db_path,
        trading_allow_live_sim=False,
        trading_allow_live_real=False,
        market_data_enabled=True,
        projection_outbox_worker_enabled=False,
        projection_outbox_apply_projection_enabled=worker_apply,
        projection_outbox_market_data_apply_enabled=worker_apply,
        projection_outbox_market_reference_apply_enabled=False,
        projection_outbox_market_index_apply_enabled=worker_apply,
        projection_outbox_market_regime_apply_enabled=False,
        projection_outbox_shadow_min_age_sec=0,
        projection_outbox_apply_min_age_sec=0,
        projection_outbox_market_index_apply_min_age_sec=0,
        projection_outbox_market_regime_apply_min_age_sec=0,
        projection_outbox_run_once_max_wall_ms=600_000,
        market_index_tr_bootstrap_enabled=True,
        market_regime_enabled=False,
        incremental_evaluation_enabled=False,
        incremental_evaluation_worker_enabled=False,
        condition_fusion_event_incremental_enabled=False,
        realtime_subscription_enabled=False,
        realtime_subscription_queue_commands=False,
        dry_run_oms_enabled=False,
        dry_run_intent_creation_enabled=False,
        dry_run_order_routing_enabled=False,
        dry_run_gateway_command_enabled=False,
        live_sim_enabled=False,
        live_sim_order_routing_enabled=False,
        live_sim_gateway_command_enabled=False,
        live_sim_pilot_pipeline_enabled=False,
        live_sim_pilot_auto_queue_command=False,
        live_sim_order_plan_routing_enabled=False,
        live_sim_cancel_enabled=False,
        live_sim_exit_engine_enabled=False,
        live_sim_exit_order_creation_enabled=False,
        live_sim_exit_gateway_command_enabled=False,
        live_sim_operating_cycle_enabled=False,
        live_sim_operating_write_runs=False,
    )


def _process_inline_projection(
    connection: sqlite3.Connection,
    event: GatewayEvent,
    settings: Settings,
) -> tuple[int, int, int]:
    from services.market_data_service import process_gateway_event

    result = process_gateway_event(connection, event, settings=settings)
    return result.applied_count, result.ignored_count, result.error_count


def _run_reconcile_chunks(
    connection: sqlite3.Connection,
    *,
    settings: Settings,
) -> list[dict[str, Any]]:
    chunks: list[dict[str, Any]] = []
    last_rowid = 0
    while True:
        rows = connection.execute(
            """
            SELECT rowid
            FROM gateway_events
            WHERE status = 'ACCEPTED'
              AND event_type IN ('price_tick', 'condition_event', 'tr_response')
              AND rowid > ?
            ORDER BY rowid ASC
            LIMIT 5000
            """,
            (last_rowid,),
        ).fetchall()
        if not rows:
            break
        min_rowid = int(rows[0]["rowid"])
        max_rowid = int(rows[-1]["rowid"])
        result = run_market_data_projection_reconcile(
            connection,
            settings=settings,
            limit=len(rows),
            min_event_rowid=min_rowid,
            max_event_rowid=max_rowid,
            persist=False,
        )
        chunks.append(result.to_dict())
        last_rowid = max_rowid
    return chunks


def _aggregate_reconcile_status(runs: Sequence[Mapping[str, Any]]) -> str:
    if not runs:
        return "NOT_RUN"
    statuses = {str(run.get("status") or "") for run in runs}
    if "FAIL" in statuses:
        return "FAIL"
    if "WARN" in statuses:
        return "WARN"
    return "PASS" if statuses == {"PASS"} else "FAIL"


def _run_market_index_reconcile(
    connection: sqlite3.Connection,
    *,
    settings: Settings,
) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM gateway_events
        WHERE status = 'ACCEPTED'
          AND (
                event_type = 'market_index_tick'
                OR (
                    event_type = 'tr_response'
                    AND lower(COALESCE(json_extract(payload_json, '$.request_id'), ''))
                        LIKE 'market_index_tr_bootstrap:%'
                )
          )
        """
    ).fetchone()
    event_count = int(row["count"] if row is not None else 0)
    if event_count == 0:
        return None
    return run_market_index_projection_reconcile(
        connection,
        settings=settings,
        limit=event_count,
        persist=False,
    ).to_dict()


def _projection_snapshot(connection: sqlite3.Connection) -> ProjectionSnapshot:
    tables: dict[str, dict[str, Any]] = {}
    for table_name in (*MARKET_PROJECTION_TABLES, *MARKET_INDEX_PROJECTION_TABLES):
        tables[table_name] = _table_snapshot(connection, table_name)
    tables["projection_watermarks"] = _table_snapshot(
        connection,
        "projection_watermarks",
        where="projection_name = 'market_data'",
    )
    overall_sha256 = hashlib.sha256(canonical_json(tables).encode("utf-8")).hexdigest()
    return ProjectionSnapshot(
        overall_sha256=overall_sha256,
        tables=tables,
        projection_counts_by_venue=_projection_counts_by_venue(connection),
    )


def _table_snapshot(
    connection: sqlite3.Connection,
    table_name: str,
    *,
    where: str | None = None,
) -> dict[str, Any]:
    columns = connection.execute(f'PRAGMA table_info("{table_name}")').fetchall()
    included = [
        str(column["name"])
        for column in columns
        if str(column["name"]) not in _NONDETERMINISTIC_COLUMNS
    ]
    primary_key = [
        str(column["name"])
        for column in sorted(columns, key=lambda item: int(item["pk"] or 0))
        if int(column["pk"] or 0) > 0 and str(column["name"]) in included
    ]
    order_columns = primary_key or included
    selected = ", ".join(f'"{column}"' for column in included)
    order_by = ", ".join(f'"{column}"' for column in order_columns)
    query = f'SELECT {selected} FROM "{table_name}"'
    if where:
        query += f" WHERE {where}"
    if order_by:
        query += f" ORDER BY {order_by}"
    hasher = hashlib.sha256()
    count = 0
    for row in connection.execute(query):
        payload = {column: row[column] for column in included}
        _update_hash(hasher, canonical_json(payload))
        count += 1
    return {
        "row_count": count,
        "sha256": hasher.hexdigest(),
        "columns": included,
    }


def _projection_counts_by_venue(connection: sqlite3.Connection) -> dict[str, int]:
    rows = connection.execute(
        """
        SELECT exchange, COUNT(*) AS count
        FROM market_tick_samples
        GROUP BY exchange
        ORDER BY exchange
        """
    ).fetchall()
    return {str(row["exchange"]): int(row["count"]) for row in rows}


def _iter_imported_gateway_events(
    connection: sqlite3.Connection,
) -> Iterable[sqlite3.Row]:
    return connection.execute(
        """
        SELECT
            rowid AS event_rowid,
            event_id,
            event_type,
            source,
            command_id,
            idempotency_key,
            event_ts,
            payload_json
        FROM gateway_events
        WHERE status = 'ACCEPTED'
        ORDER BY rowid ASC
        """
    )


def _gateway_event_from_row(row: Mapping[str, Any]) -> GatewayEvent:
    return GatewayEvent(
        event_id=str(row["event_id"]),
        event_type=str(row["event_type"]),
        source=str(row["source"]),
        command_id=row["command_id"],
        idempotency_key=row["idempotency_key"],
        ts=parse_timestamp(row["event_ts"], "event_ts"),
        payload=json.loads(row["payload_json"]),
    )


def _projection_pending_count(
    connection: sqlite3.Connection,
    projection_name: str,
) -> int:
    row = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM projection_outbox
        WHERE projection_name = ? AND status = 'PENDING'
        """,
        (projection_name,),
    ).fetchone()
    return int(row["count"] if row else 0)


def _projection_outbox_counts(
    connection: sqlite3.Connection,
) -> dict[str, dict[str, int]]:
    statuses = ("PENDING", "PROCESSING", "APPLIED", "SKIPPED", "ERROR", "DEAD_LETTER")
    counts = {
        projection_name: {status: 0 for status in statuses}
        for projection_name in ("market_data", "market_index", "market_regime")
    }
    rows = connection.execute(
        """
        SELECT projection_name, status, COUNT(*) AS count
        FROM projection_outbox
        WHERE projection_name IN ('market_data', 'market_index', 'market_regime')
        GROUP BY projection_name, status
        """
    ).fetchall()
    for row in rows:
        counts[str(row["projection_name"])][str(row["status"])] = int(row["count"])
    return counts


def _database_event_order_sha256(connection: sqlite3.Connection) -> str:
    hasher = hashlib.sha256()
    rows = connection.execute(
        """
        SELECT event_id, event_type, event_ts, received_at
        FROM gateway_events
        WHERE status = 'ACCEPTED'
        ORDER BY rowid ASC
        """
    )
    for row in rows:
        _update_hash(
            hasher,
            canonical_json(
                {
                    "event_id": row["event_id"],
                    "event_type": row["event_type"],
                    "event_ts": row["event_ts"],
                    "received_at": row["received_at"],
                }
            ),
        )
    return hasher.hexdigest()


def _side_effect_table_counts(connection: sqlite3.Connection) -> dict[str, int]:
    rows = connection.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
    ).fetchall()
    table_names = [str(row["name"]) for row in rows if _is_side_effect_table(str(row["name"]))]
    return {
        table_name: int(
            connection.execute(f'SELECT COUNT(*) AS count FROM "{table_name}"').fetchone()["count"]
        )
        for table_name in table_names
    }


def _is_side_effect_table(table_name: str) -> bool:
    normalized = table_name.strip().lower()
    return normalized in _SIDE_EFFECT_EXACT_TABLES or normalized.startswith(_SIDE_EFFECT_PREFIXES)


def _count_delta(before: Mapping[str, int], after: Mapping[str, int]) -> dict[str, int]:
    return {
        table: int(after.get(table, 0)) - int(before.get(table, 0))
        for table in sorted(set(before) | set(after))
    }


def _export_query(
    event_types: Sequence[str],
    *,
    bounds: tuple[str, str] | None,
) -> tuple[str, tuple[Any, ...]]:
    placeholders = ",".join("?" for _ in event_types)
    clauses = ["ge.status = 'ACCEPTED'", f"ge.event_type IN ({placeholders})"]
    params: list[Any] = list(event_types)
    if bounds is not None:
        clauses.extend(["ge.event_ts >= ?", "ge.event_ts < ?"])
        params.extend(bounds)
    query = f"""
        SELECT
            ge.rowid AS event_rowid,
            ge.event_id,
            ge.event_type,
            ge.source,
            ge.command_id,
            ge.idempotency_key,
            ge.event_ts,
            ge.received_at,
            ge.payload_json,
            ge.status,
            ge.error_message,
            re.payload_hash
        FROM gateway_events AS ge
        JOIN raw_events AS re ON re.event_id = ge.event_id
        WHERE {" AND ".join(clauses)}
        ORDER BY ge.rowid ASC
    """
    return query, tuple(params)


def _export_record(row: Mapping[str, Any], *, sequence: int) -> dict[str, Any]:
    event = {
        "event_id": row["event_id"],
        "event_type": row["event_type"],
        "source": row["source"],
        "ts": row["event_ts"],
        "payload": json.loads(row["payload_json"]),
    }
    if row["command_id"] is not None:
        event["command_id"] = row["command_id"]
    if row["idempotency_key"] is not None:
        event["idempotency_key"] = row["idempotency_key"]
    return {
        "sequence": sequence,
        "source_event_rowid": int(row["event_rowid"]),
        "source_received_at": row["received_at"],
        "source_status": row["status"],
        "payload_hash": row["payload_hash"],
        "event": event,
    }


def _validate_record_safety(record: Mapping[str, Any]) -> None:
    event = _mapping(record.get("event"), "event")
    event_type = str(event.get("event_type") or "").strip().lower()
    if event_type in ORDER_EVENT_TYPES or event_type not in SAFE_REPLAY_EVENT_TYPES:
        raise ValueError(f"event_type is not safe for projection replay: {event_type}")
    if str(record.get("source_status") or "") != "ACCEPTED":
        raise ValueError("projection replay accepts only ACCEPTED gateway events")


def _event_venue(event: Mapping[str, Any]) -> str:
    event_type = str(event.get("event_type") or "").strip().lower()
    payload = event.get("payload")
    payload = payload if isinstance(payload, Mapping) else {}
    if event_type == "condition_event":
        return "KRX"
    venues: set[str] = set()
    _collect_venue(venues, payload.get("exchange"))
    metadata = payload.get("metadata")
    if isinstance(metadata, Mapping):
        _collect_venue(venues, metadata.get("exchange"))
        _collect_venue(venues, metadata.get("venue"))
    rows = payload.get("rows")
    if isinstance(rows, list):
        for row in rows:
            if isinstance(row, Mapping):
                _collect_venue(venues, row.get("exchange"))
                _collect_venue(venues, row.get("venue"))
    if len(venues) > 1:
        return "MIXED"
    if venues:
        return next(iter(venues))
    return "KRX" if event_type == "price_tick" else "UNKNOWN"


def _collect_venue(venues: set[str], value: object) -> None:
    normalized = str(value or "").strip().upper()
    if normalized in {"KRX", "NXT"}:
        venues.add(normalized)
    elif normalized:
        venues.add("UNKNOWN")


def _order_component(record: Mapping[str, Any]) -> str:
    event = _mapping(record.get("event"), "event")
    return canonical_json(
        {
            "event_id": event.get("event_id"),
            "event_type": event.get("event_type"),
            "event_ts": event.get("ts"),
            "received_at": record.get("source_received_at"),
        }
    )


def _update_hash(hasher: Any, value: str) -> None:
    hasher.update(value.encode("utf-8"))
    hasher.update(b"\n")


def _iter_bundle_records(events_path: Path) -> Iterable[dict[str, Any]]:
    with events_path.open("r", encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, start=1):
            if not line.strip():
                raise ValueError(f"blank replay record at line {line_number}")
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid replay JSON at line {line_number}: {exc}") from exc
            if not isinstance(value, dict):
                raise ValueError(f"replay record must be an object at line {line_number}")
            yield value


def _read_manifest(bundle_dir: Path) -> dict[str, Any]:
    path = bundle_dir / _MANIFEST_FILE_NAME
    if not path.is_file():
        raise FileNotFoundError(f"replay bundle manifest is missing: {path}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid replay manifest JSON: {exc}") from exc
    if not isinstance(value, dict) or value.get("format") != BUNDLE_FORMAT:
        raise ValueError("unsupported replay bundle format")
    if not isinstance(value.get("event_types"), list):
        raise ValueError("replay manifest event_types must be a list")
    if value["event_types"]:
        _normalize_safe_event_types(value["event_types"])
    if value.get("accepted_events_only") is not True:
        raise ValueError("replay bundle must contain accepted events only")
    if value.get("order_event_types_excluded") is not True:
        raise ValueError("replay bundle must explicitly exclude order event types")
    return value


def _bundle_result(bundle_dir: Path, manifest: Mapping[str, Any]) -> ReplayBundleResult:
    return ReplayBundleResult(
        bundle_dir=bundle_dir,
        manifest_path=bundle_dir / _MANIFEST_FILE_NAME,
        events_path=bundle_dir / _EVENTS_FILE_NAME,
        event_count=int(manifest.get("event_count") or 0),
        event_types=tuple(sorted(str(item) for item in manifest.get("event_types") or [])),
        trade_date=str(manifest["trade_date"]) if manifest.get("trade_date") else None,
        record_sha256=str(manifest.get("record_sha256") or ""),
        event_order_sha256=str(manifest.get("event_order_sha256") or ""),
        venue_counts={
            str(key): int(value) for key, value in dict(manifest.get("venue_counts") or {}).items()
        },
    )


def _normalize_safe_event_types(event_types: Iterable[str]) -> tuple[str, ...]:
    normalized = tuple(
        sorted({str(item).strip().lower() for item in event_types if str(item).strip()})
    )
    if not normalized:
        raise ValueError("at least one replay event_type is required")
    unsafe = sorted(set(normalized) - SAFE_REPLAY_EVENT_TYPES)
    if unsafe:
        raise ValueError(f"unsafe or unsupported replay event_types: {unsafe}")
    return normalized


def _trade_date_bounds(trade_date: str | None) -> tuple[str, str] | None:
    if trade_date is None:
        return None
    parsed = date.fromisoformat(trade_date)
    zone = timezone(timedelta(hours=9), name="Asia/Seoul")
    start = datetime.combine(parsed, time.min, tzinfo=zone).astimezone(UTC)
    end = datetime.combine(parsed + timedelta(days=1), time.min, tzinfo=zone).astimezone(UTC)
    return datetime_to_wire(start), datetime_to_wire(end)


def _open_read_only_connection(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True, timeout=15.0)
    connection.execute("PRAGMA query_only=ON")
    return connection


def _resolve_target_db_path(
    target_db_path: str | Path,
    *,
    operational_db_path: str | Path | None,
) -> Path:
    target = Path(target_db_path).expanduser().resolve()
    if operational_db_path is None:
        operational_db_path = load_settings().trading_db_path
    operational = Path(operational_db_path).expanduser().resolve()
    if target == operational:
        raise ValueError("replay target must not be the configured operational DB")
    if target.exists():
        raise FileExistsError(f"replay target DB already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    return target


def _require_existing_file(value: str | Path, field_name: str) -> Path:
    path = Path(value).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(f"{field_name} does not exist: {path}")
    return path


def _require_existing_directory(value: str | Path, field_name: str) -> Path:
    path = Path(value).expanduser().resolve()
    if not path.is_dir():
        raise FileNotFoundError(f"{field_name} does not exist: {path}")
    return path


def _require_new_directory_path(value: str | Path, field_name: str) -> Path:
    path = Path(value).expanduser().resolve()
    if path.exists():
        raise FileExistsError(f"{field_name} already exists: {path}")
    return path


def _mapping(value: object, field_name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be an object")
    return dict(value)


def _nested_value(payload: Mapping[str, Any], section: str, field_name: str) -> Any:
    value = payload.get(section)
    return value.get(field_name) if isinstance(value, Mapping) else None
