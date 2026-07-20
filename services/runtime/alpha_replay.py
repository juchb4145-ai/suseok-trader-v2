from __future__ import annotations

import hashlib
import json
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Literal, cast

from domain.broker.events import GatewayEvent
from domain.broker.utils import datetime_to_wire, parse_timestamp
from storage.gateway_command_store import canonical_json

from services.config import Settings, load_settings
from services.market_data_service import market_session_for_tick
from services.runtime.projection_replay import import_replay_bundle, validate_replay_bundle

ALPHA_REPLAY_REPORT_FORMAT = "point-in-time-alpha-replay-report/v1"
ReplayMode = Literal["STRUCTURAL_REPLAY", "ALPHA_REPLAY"]
REPLAY_MODES: tuple[ReplayMode, ...] = ("STRUCTURAL_REPLAY", "ALPHA_REPLAY")

ALPHA_REPLAY_INPUT_SOURCES = (
    "price_tick",
    "condition_event",
    "candidate_quote_refresh_tr_response",
    "market_symbols",
    "market_index_tick",
    "market_index_tr_bootstrap",
    "market_scan_tr_response",
    "theme_membership_lineage",
    "config_lineage",
)

VIRTUAL_CLOCK_TARGETS = (
    "tick_freshness",
    "index_freshness",
    "theme_freshness",
    "candidate_freshness",
    "strategy_age",
    "risk_age",
    "entry_timing_expiration",
    "order_plan_expiration",
    "cooldown",
    "entry_window",
    "minimum_hold",
    "maximum_hold",
    "eod",
)
_SEOUL_TIMEZONE = timezone(timedelta(hours=9), name="Asia/Seoul")


class PointInTimeViolation(RuntimeError):
    """Raised when replay code attempts to observe data after the virtual clock."""


@dataclass
class VirtualClock:
    _now: datetime | None = None

    @property
    def now(self) -> datetime:
        if self._now is None:
            raise RuntimeError("virtual clock has not started")
        return self._now

    def advance_to(self, value: str | datetime) -> datetime:
        target = parse_timestamp(value, "virtual_clock_target")
        if self._now is not None and target < self._now:
            raise PointInTimeViolation(
                "virtual clock cannot move backwards: "
                f"current={datetime_to_wire(self._now)} target={datetime_to_wire(target)}"
            )
        self._now = target
        return target

    def age_seconds(self, observed_at: str | datetime) -> float:
        observed = parse_timestamp(observed_at, "observed_at")
        if observed > self.now:
            raise PointInTimeViolation(
                "future observation requested: "
                f"observed_at={datetime_to_wire(observed)} "
                f"virtual_now={datetime_to_wire(self.now)}"
            )
        return max((self.now - observed).total_seconds(), 0.0)

    def is_fresh(self, observed_at: str | datetime, *, stale_after_sec: int) -> bool:
        return self.age_seconds(observed_at) <= float(stale_after_sec)

    def is_expired(self, expires_at: str | datetime) -> bool:
        return parse_timestamp(expires_at, "expires_at") <= self.now

    def cooldown_elapsed(
        self,
        last_observed_at: str | datetime,
        *,
        cooldown_sec: int,
    ) -> bool:
        return self.age_seconds(last_observed_at) >= float(cooldown_sec)

    def inside_entry_window(self, *, start: str, end: str) -> bool:
        local = self.now.astimezone(_SEOUL_TIMEZONE).time().replace(tzinfo=None)
        return _parse_time(start) <= local < _parse_time(end)

    def hold_seconds(self, opened_at: str | datetime) -> float:
        return self.age_seconds(opened_at)

    def minimum_hold_elapsed(
        self,
        opened_at: str | datetime,
        *,
        minimum_hold_sec: int,
    ) -> bool:
        return self.hold_seconds(opened_at) >= float(minimum_hold_sec)

    def maximum_hold_elapsed(
        self,
        opened_at: str | datetime,
        *,
        maximum_hold_sec: int,
    ) -> bool:
        return self.hold_seconds(opened_at) >= float(maximum_hold_sec)

    def eod_due(self, *, eod_time: str) -> bool:
        local = self.now.astimezone(_SEOUL_TIMEZONE).time().replace(tzinfo=None)
        return local >= _parse_time(eod_time)


@dataclass(frozen=True, kw_only=True)
class _TimedValue:
    available_at: datetime
    event_at: datetime
    event_id: str
    payload_hash: str


@dataclass
class PointInTimeState:
    _latest: dict[tuple[str, str], _TimedValue] = field(default_factory=dict)

    def apply(
        self,
        *,
        category: str,
        key: str,
        event: GatewayEvent,
        available_at: datetime,
        payload_hash: str,
        clock: VirtualClock,
    ) -> None:
        if available_at > clock.now or event.ts > clock.now:
            raise PointInTimeViolation(f"cannot apply future {category} event_id={event.event_id}")
        self._latest[(category, key)] = _TimedValue(
            available_at=available_at,
            event_at=event.ts,
            event_id=event.event_id,
            payload_hash=payload_hash,
        )

    def latest(self, category: str, key: str, *, clock: VirtualClock) -> _TimedValue | None:
        value = self._latest.get((category, key))
        if value is None:
            return None
        if value.available_at > clock.now or value.event_at > clock.now:
            raise PointInTimeViolation(
                f"future {category} state read for key={key} event_id={value.event_id}"
            )
        return value

    def assert_visible_at(self, *, clock: VirtualClock) -> None:
        for category, key in sorted(self._latest):
            self.latest(category, key, clock=clock)

    @property
    def size(self) -> int:
        return len(self._latest)


@dataclass(frozen=True, kw_only=True)
class AlphaReplayResult:
    mode: ReplayMode
    status: str
    bundle_dir: Path
    isolated_db_path: Path
    trade_date: str | None
    source_record_sha256: str
    source_event_order_sha256: str
    config_sha256: str
    commit_sha: str
    deterministic_identity_sha256: str
    result_sha256: str
    event_count: int
    first_virtual_at: str | None
    last_virtual_at: str | None
    event_type_coverage: Mapping[str, int]
    input_source_coverage: Mapping[str, Mapping[str, Any]]
    missing_sources: tuple[str, ...]
    scan_coverage: str
    point_in_time_violation_count: int
    point_in_time_violations: Sequence[Mapping[str, Any]]
    virtual_clock_targets: Mapping[str, str]
    freshness_observations: Mapping[str, Mapping[str, int]]
    session_coverage: Mapping[str, int]
    imported_event_count: int
    order_preserved: bool
    blocked_write_attempts: Sequence[Mapping[str, Any]]
    side_effect_table_delta: Mapping[str, int]
    operational_db_write_count: int
    alpha_qualified: bool
    qualification_reasons: tuple[str, ...]
    failures: tuple[str, ...]
    warnings: tuple[str, ...]

    @property
    def no_trading_side_effects(self) -> bool:
        return (
            not self.blocked_write_attempts
            and all(value == 0 for value in self.side_effect_table_delta.values())
            and self.operational_db_write_count == 0
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "format": ALPHA_REPLAY_REPORT_FORMAT,
            "mode": self.mode,
            "status": self.status,
            "bundle_dir": str(self.bundle_dir),
            "isolated_db_path": str(self.isolated_db_path),
            "trade_date": self.trade_date,
            "identity": {
                "source_record_sha256": self.source_record_sha256,
                "source_event_order_sha256": self.source_event_order_sha256,
                "config_sha256": self.config_sha256,
                "commit_sha": self.commit_sha,
                "deterministic_identity_sha256": self.deterministic_identity_sha256,
            },
            "result_sha256": self.result_sha256,
            "event_count": self.event_count,
            "first_virtual_at": self.first_virtual_at,
            "last_virtual_at": self.last_virtual_at,
            "event_type_coverage": dict(self.event_type_coverage),
            "input_source_coverage": {
                key: dict(value) for key, value in self.input_source_coverage.items()
            },
            "missing_sources": list(self.missing_sources),
            "scan_coverage": self.scan_coverage,
            "point_in_time_violation_count": self.point_in_time_violation_count,
            "point_in_time_violations": [dict(item) for item in self.point_in_time_violations],
            "virtual_clock": {
                "wall_clock_reads": 0,
                "targets": dict(self.virtual_clock_targets),
                "freshness_observations": {
                    key: dict(value) for key, value in self.freshness_observations.items()
                },
                "session_coverage": dict(self.session_coverage),
            },
            "isolation": {
                "imported_event_count": self.imported_event_count,
                "order_preserved": self.order_preserved,
                "blocked_write_attempts": [dict(item) for item in self.blocked_write_attempts],
                "side_effect_table_delta": dict(self.side_effect_table_delta),
                "operational_db_write_count": self.operational_db_write_count,
                "production_db_writes_allowed": False,
                "gateway_command_writes": 0,
                "live_sim_writes": 0,
                "dry_run_writes": 0,
            },
            "alpha_qualified": self.alpha_qualified,
            "qualification_reasons": list(self.qualification_reasons),
            "failures": list(self.failures),
            "warnings": list(self.warnings),
            "observe_only": True,
            "live_sim_allowed": False,
            "live_real_allowed": False,
            "no_order_side_effects": self.no_trading_side_effects,
            "no_trading_side_effects": self.no_trading_side_effects,
        }


def run_point_in_time_alpha_replay(
    *,
    bundle_dir: str | Path,
    isolated_db_path: str | Path,
    operational_db_path: str | Path,
    mode: ReplayMode = "ALPHA_REPLAY",
    settings: Settings | None = None,
    commit_sha: str = "UNKNOWN",
) -> AlphaReplayResult:
    normalized_mode = _normalize_mode(mode)
    bundle = validate_replay_bundle(bundle_dir)
    resolved_settings = settings or load_settings()
    time_policy = _time_policy(resolved_settings)
    config_sha256 = _sha256_json(time_policy)
    normalized_commit = str(commit_sha or "UNKNOWN").strip() or "UNKNOWN"
    identity = {
        "mode": normalized_mode,
        "source_record_sha256": bundle.record_sha256,
        "source_event_order_sha256": bundle.event_order_sha256,
        "config_sha256": config_sha256,
        "commit_sha": normalized_commit,
    }
    identity_sha256 = _sha256_json(identity)
    imported = import_replay_bundle(
        bundle_dir=bundle.bundle_dir,
        target_db_path=isolated_db_path,
        operational_db_path=operational_db_path,
    )

    replay = _replay_records(
        bundle.events_path,
        mode=normalized_mode,
        settings=resolved_settings,
        identity_sha256=identity_sha256,
    )
    failures = list(replay["failures"])
    if not imported.order_preserved:
        failures.append("AUTHORITATIVE_EVENT_ORDER_NOT_PRESERVED")
    if not imported.no_trading_side_effects:
        failures.append("TRADING_SIDE_EFFECT_DETECTED")

    missing_sources = tuple(
        source
        for source in ALPHA_REPLAY_INPUT_SOURCES
        if int(replay["source_counts"].get(source, 0)) == 0
    )
    warnings = [f"MISSING_SOURCE:{source}" for source in missing_sources]
    scan_coverage = str(replay["scan_coverage"])
    if scan_coverage != "COMPLETE":
        warnings.append(f"MARKET_SCAN_COVERAGE:{scan_coverage}")
    if normalized_commit == "UNKNOWN":
        warnings.append("COMMIT_IDENTITY_UNKNOWN")

    qualification_reasons: list[str] = []
    if normalized_mode != "ALPHA_REPLAY":
        qualification_reasons.append("STRUCTURAL_REPLAY_ONLY")
    if missing_sources:
        qualification_reasons.append("REQUIRED_INPUT_SOURCE_MISSING")
    if scan_coverage != "COMPLETE":
        qualification_reasons.append("MARKET_SCAN_NOT_COMPLETE")
    if failures:
        qualification_reasons.append("REPLAY_SAFETY_FAILURE")
    alpha_qualified = not qualification_reasons and normalized_mode == "ALPHA_REPLAY"
    status = "FAIL" if failures else "WARN" if warnings else "PASS"

    coverage = {
        source: {
            "count": int(replay["source_counts"].get(source, 0)),
            "status": ("PRESENT" if int(replay["source_counts"].get(source, 0)) else "MISSING"),
        }
        for source in ALPHA_REPLAY_INPUT_SOURCES
    }
    deterministic_result = {
        "identity_sha256": identity_sha256,
        "mode": normalized_mode,
        "event_count": int(replay["event_count"]),
        "event_type_coverage": replay["event_type_counts"],
        "input_source_coverage": coverage,
        "scan_coverage": scan_coverage,
        "point_in_time_violations": replay["violations"],
        "frame_sha256": replay["frame_sha256"],
        "freshness_observations": replay["freshness"],
        "session_coverage": replay["sessions"],
        "alpha_qualified": alpha_qualified,
        "qualification_reasons": sorted(set(qualification_reasons)),
    }

    return AlphaReplayResult(
        mode=normalized_mode,
        status=status,
        bundle_dir=bundle.bundle_dir,
        isolated_db_path=Path(isolated_db_path).expanduser().resolve(),
        trade_date=bundle.trade_date,
        source_record_sha256=bundle.record_sha256,
        source_event_order_sha256=bundle.event_order_sha256,
        config_sha256=config_sha256,
        commit_sha=normalized_commit,
        deterministic_identity_sha256=identity_sha256,
        result_sha256=_sha256_json(deterministic_result),
        event_count=int(replay["event_count"]),
        first_virtual_at=replay["first_virtual_at"],
        last_virtual_at=replay["last_virtual_at"],
        event_type_coverage=dict(replay["event_type_counts"]),
        input_source_coverage=coverage,
        missing_sources=missing_sources,
        scan_coverage=scan_coverage,
        point_in_time_violation_count=len(replay["violations"]),
        point_in_time_violations=tuple(replay["violations"]),
        virtual_clock_targets={target: "VIRTUAL_CLOCK" for target in VIRTUAL_CLOCK_TARGETS},
        freshness_observations={key: dict(value) for key, value in replay["freshness"].items()},
        session_coverage=dict(replay["sessions"]),
        imported_event_count=imported.imported_event_count,
        order_preserved=imported.order_preserved,
        blocked_write_attempts=tuple(imported.blocked_write_attempts),
        side_effect_table_delta=dict(imported.side_effect_table_delta),
        operational_db_write_count=0,
        alpha_qualified=alpha_qualified,
        qualification_reasons=tuple(sorted(set(qualification_reasons))),
        failures=tuple(sorted(set(failures))),
        warnings=tuple(sorted(set(warnings))),
    )


def _replay_records(
    events_path: Path,
    *,
    mode: ReplayMode,
    settings: Settings,
    identity_sha256: str,
) -> dict[str, Any]:
    del mode
    clock = VirtualClock()
    state = PointInTimeState()
    event_type_counts: Counter[str] = Counter()
    source_counts: Counter[str] = Counter({"config_lineage": 1})
    freshness: dict[str, Counter[str]] = {
        "tick": Counter(),
        "index": Counter(),
        "candidate": Counter(),
        "theme": Counter(),
    }
    sessions: Counter[str] = Counter()
    violations: list[dict[str, Any]] = []
    failures: list[str] = []
    scan_payloads: list[Mapping[str, Any]] = []
    frame_hasher = hashlib.sha256()
    first_virtual_at: str | None = None
    last_virtual_at: str | None = None
    previous_available: datetime | None = None
    event_count = 0

    with events_path.open("r", encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, start=1):
            record = json.loads(line)
            event = cast(GatewayEvent, GatewayEvent.from_dict(record["event"]))
            available_at = parse_timestamp(record["source_received_at"], "source_received_at")
            event_count += 1
            event_type = event.event_type.strip().lower()
            event_type_counts[event_type] += 1

            if previous_available is not None and available_at < previous_available:
                violations.append(
                    _violation(
                        line_number,
                        event.event_id,
                        "NON_MONOTONIC_SOURCE_AVAILABILITY",
                        available_at=available_at,
                        reference_at=previous_available,
                    )
                )
                failures.append("POINT_IN_TIME_VIOLATION")
            target = max(available_at, previous_available or available_at)
            try:
                clock.advance_to(target)
            except PointInTimeViolation as exc:
                violations.append(
                    _violation(
                        line_number,
                        event.event_id,
                        "VIRTUAL_CLOCK_BACKWARD",
                        message=str(exc),
                    )
                )
                failures.append("POINT_IN_TIME_VIOLATION")
            previous_available = target
            wire_now = datetime_to_wire(clock.now)
            first_virtual_at = first_virtual_at or wire_now
            last_virtual_at = wire_now

            if event.ts > clock.now:
                violations.append(
                    _violation(
                        line_number,
                        event.event_id,
                        "EVENT_TIMESTAMP_AFTER_AVAILABILITY",
                        available_at=clock.now,
                        reference_at=event.ts,
                    )
                )
                failures.append("POINT_IN_TIME_VIOLATION")
                continue

            sources = _classify_sources(event)
            source_counts.update(sources)
            if "market_scan_tr_response" in sources:
                scan_payloads.append(event.payload)
            category, key = _state_key(event)
            try:
                state.apply(
                    category=category,
                    key=key,
                    event=event,
                    available_at=available_at,
                    payload_hash=str(record["payload_hash"]),
                    clock=clock,
                )
                state.assert_visible_at(clock=clock)
                freshness_name, stale_after = _freshness_policy(
                    event,
                    sources=sources,
                    settings=settings,
                )
                fresh_status = "NOT_APPLICABLE"
                if freshness_name is not None and stale_after is not None:
                    fresh_status = (
                        "FRESH"
                        if clock.is_fresh(event.ts, stale_after_sec=stale_after)
                        else "STALE"
                    )
                    freshness[freshness_name][fresh_status] += 1
                session = _event_session(event, clock=clock)
                sessions[session] += 1
                _hash_line(
                    frame_hasher,
                    canonical_json(
                        {
                            "identity_sha256": identity_sha256,
                            "sequence": int(record["sequence"]),
                            "virtual_at": wire_now,
                            "event_id": event.event_id,
                            "event_type": event_type,
                            "category": category,
                            "key": key,
                            "freshness": fresh_status,
                            "session": session,
                            "state_size": state.size,
                        }
                    ),
                )
            except PointInTimeViolation as exc:
                violations.append(
                    _violation(line_number, event.event_id, "FUTURE_STATE_ACCESS", message=str(exc))
                )
                failures.append("POINT_IN_TIME_VIOLATION")

    return {
        "event_count": event_count,
        "event_type_counts": dict(sorted(event_type_counts.items())),
        "source_counts": dict(sorted(source_counts.items())),
        "freshness": {key: dict(sorted(value.items())) for key, value in sorted(freshness.items())},
        "sessions": dict(sorted(sessions.items())),
        "violations": violations,
        "failures": failures,
        "frame_sha256": frame_hasher.hexdigest(),
        "first_virtual_at": first_virtual_at,
        "last_virtual_at": last_virtual_at,
        "scan_coverage": _scan_coverage(scan_payloads),
    }


def _classify_sources(event: GatewayEvent) -> tuple[str, ...]:
    event_type = event.event_type.strip().lower()
    payload = event.payload
    sources: list[str] = []
    if event_type in {"price_tick", "condition_event", "market_symbols", "market_index_tick"}:
        sources.append(event_type)
    if event_type == "tr_response":
        request_id = str(payload.get("request_id") or "").lower()
        request_name = str(payload.get("request_name") or "").lower()
        metadata = payload.get("metadata")
        metadata = metadata if isinstance(metadata, Mapping) else {}
        metadata_source = str(metadata.get("source") or "").lower()
        combined = " ".join((request_id, request_name, metadata_source))
        if "candidate_quote_refresh" in combined:
            sources.append("candidate_quote_refresh_tr_response")
        if "market_index_tr_bootstrap" in combined:
            sources.append("market_index_tr_bootstrap")
        if "market_scan" in combined:
            sources.append("market_scan_tr_response")
    if _contains_lineage(payload, ("theme", "membership")):
        sources.append("theme_membership_lineage")
    return tuple(sorted(set(sources)))


def _contains_lineage(value: object, needles: tuple[str, ...]) -> bool:
    if isinstance(value, Mapping):
        for key, nested in value.items():
            normalized = str(key).lower()
            if "lineage" in normalized and any(needle in normalized for needle in needles):
                return True
            if _contains_lineage(nested, needles):
                return True
    elif isinstance(value, list):
        return any(_contains_lineage(item, needles) for item in value)
    return False


def _state_key(event: GatewayEvent) -> tuple[str, str]:
    payload = event.payload
    event_type = event.event_type.strip().lower()
    if event_type == "price_tick":
        metadata = payload.get("metadata")
        metadata = metadata if isinstance(metadata, Mapping) else {}
        exchange = str(metadata.get("exchange") or "KRX").upper()
        return "tick", f"{payload.get('code') or 'UNKNOWN'}:{exchange}"
    if event_type == "condition_event":
        return "condition", (
            f"{payload.get('condition_id') or 'UNKNOWN'}:{payload.get('code') or 'UNKNOWN'}"
        )
    if event_type == "market_index_tick":
        return "market_index", str(payload.get("index_code") or "UNKNOWN").upper()
    if event_type == "market_symbols":
        return "market_symbols", "ALL"
    if event_type == "tr_response":
        return "tr_response", str(payload.get("request_id") or event.event_id)
    return event_type, event.event_id


def _freshness_policy(
    event: GatewayEvent,
    *,
    sources: Sequence[str],
    settings: Settings,
) -> tuple[str | None, int | None]:
    event_type = event.event_type.strip().lower()
    if event_type == "price_tick":
        return "tick", settings.market_data_tick_stale_sec
    if event_type == "market_index_tick" or "market_index_tr_bootstrap" in sources:
        return "index", settings.market_index_stale_sec
    if event_type == "condition_event" or "candidate_quote_refresh_tr_response" in sources:
        return "candidate", settings.candidate_source_stale_sec
    if "theme_membership_lineage" in sources:
        return "theme", settings.theme_snapshot_stale_sec
    return None, None


def _event_session(event: GatewayEvent, *, clock: VirtualClock) -> str:
    if event.event_type.strip().lower() == "price_tick":
        metadata = event.payload.get("metadata")
        metadata = metadata if isinstance(metadata, Mapping) else {}
        exchange = str(metadata.get("exchange") or "KRX")
        return market_session_for_tick(clock.now, exchange)
    local_time = clock.now.astimezone(_SEOUL_TIMEZONE).time().replace(tzinfo=None)
    return "REGULAR" if time(9, 0) <= local_time < time(15, 30) else "OFF_HOURS"


def _scan_coverage(payloads: Sequence[Mapping[str, Any]]) -> str:
    if not payloads:
        return "NOT_PRESENT"
    for payload in payloads:
        metadata = payload.get("metadata")
        metadata = metadata if isinstance(metadata, Mapping) else {}
        page_lineage = metadata.get("page_lineage")
        if metadata.get("pagination_complete") is not True:
            return "FIRST_PAGE_ONLY"
        if not isinstance(page_lineage, list) or not page_lineage:
            return "FIRST_PAGE_ONLY"
    return "COMPLETE"


def _time_policy(settings: Settings) -> dict[str, Any]:
    return {
        "market_data_tick_stale_sec": settings.market_data_tick_stale_sec,
        "market_index_stale_sec": settings.market_index_stale_sec,
        "theme_snapshot_stale_sec": settings.theme_snapshot_stale_sec,
        "candidate_source_stale_sec": settings.candidate_source_stale_sec,
        "candidate_tick_stale_sec": settings.candidate_tick_stale_sec,
        "strategy_engine_stale_tick_sec": settings.strategy_engine_stale_tick_sec,
        "risk_gate_stale_tick_sec": settings.risk_gate_stale_tick_sec,
        "risk_gate_strategy_stale_sec": settings.risk_gate_strategy_stale_sec,
        "entry_timing_stale_max_seconds": settings.entry_timing_stale_max_seconds,
        "entry_timing_plan_ttl_seconds": settings.entry_timing_plan_ttl_seconds,
        "risk_gate_observation_cooldown_sec": settings.risk_gate_observation_cooldown_sec,
        "live_sim_duplicate_cooldown_sec": settings.live_sim_duplicate_cooldown_sec,
        "live_sim_order_ttl_sec": settings.live_sim_order_ttl_sec,
        "live_sim_entry_window_start": settings.live_sim_entry_window_start,
        "live_sim_entry_window_end": settings.live_sim_entry_window_end,
        "live_sim_exit_min_hold_sec": settings.live_sim_exit_min_hold_sec,
        "live_sim_exit_max_hold_sec": settings.live_sim_exit_max_hold_sec,
        "live_sim_exit_eod_flatten_time": settings.live_sim_exit_eod_flatten_time,
        "timezone": "Asia/Seoul",
    }


def _violation(
    sequence: int,
    event_id: str,
    reason_code: str,
    *,
    available_at: datetime | None = None,
    reference_at: datetime | None = None,
    message: str | None = None,
) -> dict[str, Any]:
    return {
        "sequence": sequence,
        "event_id": event_id,
        "reason_code": reason_code,
        "available_at": datetime_to_wire(available_at) if available_at else None,
        "reference_at": datetime_to_wire(reference_at) if reference_at else None,
        "message": message,
    }


def _normalize_mode(value: str) -> ReplayMode:
    normalized = str(value or "").strip().upper()
    if normalized not in REPLAY_MODES:
        raise ValueError(f"unsupported replay mode: {value}")
    return normalized  # type: ignore[return-value]


def _parse_time(value: str) -> time:
    return time.fromisoformat(value)


def _sha256_json(value: object) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _hash_line(hasher: Any, value: str) -> None:
    hasher.update(value.encode("utf-8"))
    hasher.update(b"\n")
