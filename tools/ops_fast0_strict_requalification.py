# ruff: noqa: E501 -- generated fixed SHA-256 manifests intentionally stay one entry per line.

from __future__ import annotations

import argparse
import ctypes
import hashlib
import json
import os
import re
import shutil
import sqlite3
import sys
from collections import Counter
from collections.abc import Mapping, Sequence
from contextlib import AbstractContextManager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, BinaryIO
from urllib.parse import quote

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from domain.broker.utils import datetime_to_wire  # noqa: E402
from services.pipeline_coherency import build_pipeline_coherency_rca_status  # noqa: E402
from services.pipeline_coherency_disposition import (  # noqa: E402
    is_pipeline_coherency_disposition_schema_ready,
    resolve_pipeline_coherency_dispositions,
)
from services.runtime.incremental_evaluation_dead_letter_resolution import (  # noqa: E402
    build_incremental_evaluation_dead_letter_effective_status,
)
from services.runtime.projection_outbox_bulk_retire import (  # noqa: E402
    _count_condition_event_bulk_retire_eligible_fast,
    _count_effective_skip_pending,
    _count_pending_condition_events,
    _fast_bulk_retire_counts,
)
from storage.gateway_order_broker_boundary import (  # noqa: E402
    get_order_broker_boundary_status,
)
from storage.sqlite import APP_NAME, SCHEMA_VERSION  # noqa: E402
from tools import ops_live_sim_execution_lifecycle_check as lifecycle_tool  # noqa: E402

_EXPECTED_SCHEMA_VERSION = "63"
_PAGE_LIMIT = 500
_DEFAULT_INCREMENTAL_BACKLOG_WARN_COUNT = 100
_DEFAULT_INCREMENTAL_BACKLOG_FAIL_COUNT = 1000
_DEFAULT_INCREMENTAL_STALE_WARN_SEC = 30
_DEFAULT_INCREMENTAL_STALE_FAIL_SEC = 300
_POLICY_PIPELINE_MAX_AGE_SEC = 60.0
_POLICY_INCREMENTAL_RETRY_LIMIT = 3
_POLICY_PROJECTION_STALE_PROCESSING_SEC = 120
_POLICY_PROJECTION_BLOCKING_OLDER_THAN_SEC = 60
_POLICY_PROJECTION_BACKLOG_WARN_PENDING_COUNT = 1000
_POLICY_PROJECTION_BACKLOG_FAIL_PENDING_COUNT = 10000
_POLICY_PROJECTION_RECENT_WINDOW_SEC = 300
_POLICY_PROJECTION_RECENT_FAIL_COUNT = 100
_POLICY_PROJECTION_CONDITION_MAX_PENDING = 100
_POLICY_PROJECTION_CONDITION_RECENT_MAX_PENDING = 10
_POLICY_MIN_FREE_BYTES = 1024 * 1024 * 1024
_MAX_RUNTIME_LOG_FILES = 32
_MAX_RUNTIME_LOG_TOTAL_BYTES = 1024 * 1024 * 1024
_MIGRATION_APPLY_FUNCTION = "storage.sqlite.migrate_schema_62_to_63"
_MIGRATION_SOURCE_SCHEMA = "62"
_MIGRATION_PREFLIGHT_CONTRACT = "exact-62-to-63-fence-events-v1"
_MIGRATION_LEASE_KEYS = frozenset(
    {
        "live_sim_lifecycle_inbox_processing_or_locked",
        "projection_outbox_processing_or_locked",
        "runtime_execution_locks",
    }
)
_MIGRATION_TARGET_PROBES = frozenset()
_DATA_FILE_SUFFIXES = ("", "-wal", "-shm", "-journal")
_RUNTIME_MARKERS = ("EVALUATION_RUN_FENCE_LOST", "OWNER_ALIVE_AFTER_TTL")
_PIPELINE_CLASSIFICATIONS = (
    "HISTORICAL_CLOSED",
    "MISSING_CANDIDATE_MANUAL_REVIEW",
    "STALE_OTHER_DATE_MANUAL_REVIEW",
    "ACTIVE_CURRENT",
)
_PIPELINE_DISPOSITION_STATUSES = (
    "PENDING",
    "INVALID_CHAIN",
    "EFFECTIVE",
    "INVALID_CAS",
    "MISSING",
)
_BROKER_RAW_STATES = (
    "CLAIMED",
    "GATEWAY_STARTED",
    "PRE_ACK_RECORDED",
    "BROKER_ACCEPTED",
    "CHEJAN_CONFIRMED",
    "UNCONFIRMED",
)
_BROKER_EFFECTIVE_STATES = (*_BROKER_RAW_STATES, "RESOLVED_BROKER_NOT_REACHED")
_LIFECYCLE_CLASSIFICATIONS = (
    "ACTIVE_LIFECYCLE_BLOCKER",
    "HISTORICAL_RUNTIME_STATUS_AUDIT",
    "MANUAL_REVIEW_BLOCKER",
)
_LIFECYCLE_QUALIFICATION_REASONS = frozenset(
    {
        "LIVE_SIM_ACTIVE_LIFECYCLE_BLOCKERS_PRESENT",
        "LIVE_SIM_LIFECYCLE_MANUAL_REVIEW_REQUIRED",
        "LIVE_SIM_ACTIVE_RECONCILE_BLOCKER_PRESENT",
        "LIVE_SIM_RECONCILE_MANUAL_REVIEW_REQUIRED",
        "LIVE_SIM_LIFECYCLE_MIRROR_INCONSISTENT",
        "LIVE_SIM_LIFECYCLE_INVENTORY_CHANGED_DURING_READ",
    }
)
_LIFECYCLE_CANONICAL_REASONS = frozenset(
    {
        "LIVE_SIM_RAW_ERROR_ROWS_PRESENT",
        "LIVE_SIM_RAW_LIFECYCLE_ERRORS_PRESENT",
        "LIVE_SIM_RAW_RECONCILE_MISMATCH_EVENTS_PRESENT",
        "LIVE_SIM_ACTIVE_RECONCILE_MISMATCH_PRESENT",
        "LIVE_SIM_RECONCILE_EVIDENCE_MANUAL_REVIEW_REQUIRED",
    }
)
_LIFECYCLE_SAFE_SCALAR_KEYS = (
    "status",
    "qualification_status",
    "canonical_status",
    "raw_error_count",
    "mirror_lifecycle_count",
    "logical_subject_count",
    "active_lifecycle_blocker_count",
    "historical_runtime_status_audit_count",
    "manual_review_blocker_count",
    "active_reconcile_blocker_count",
    "historical_reconcile_event_count",
    "reconcile_manual_review_count",
    "effective_blocker_count",
    "mirrored_pair_count",
    "mirror_consistent",
    "code_filter",
    "full_count",
    "inventory_count_consistent",
    "inventory_digest",
    "scanned_inventory_digest",
    "ending_inventory_digest",
    "read_only",
    "observe_only",
    "no_order_side_effects",
    "real_order_allowed",
    "collected_count",
    "unique_subject_count",
    "invalid_item_count",
    "item_classification_counts_consistent",
    "page_count",
    "raw_rows_recorded",
    "filter_global_contract_consistent",
    "requested_code_filter",
)
_LIFECYCLE_SAFE_PAGE_KEYS = (
    "offset",
    "returned_count",
    "full_count",
    "has_more",
    "next_offset",
    "inventory_count_consistent",
    "inventory_digest",
    "scanned_inventory_digest",
    "ending_inventory_digest",
    "qualification_status",
    "effective_blocker_count",
)
_PIPELINE_GLOBAL_KEYS = (
    "qualification_status",
    "qualification_reason_codes",
    "canonical_status",
    "canonical_reason_codes",
    "classification_counts",
    "manual_review_count",
    "manual_review_pending_count",
    "current_source_drift_count",
    "current_source_drift_pending_count",
    "current_source_drift_unknown_count",
    "current_source_drift_unknown_pending_count",
    "disposition_required_count",
    "disposition_pending_count",
    "plan_expiry_unknown_count",
    "schema_ready",
    "full_count",
    "inventory_count_consistent",
    "inventory_digest",
    "inventory_end_digest",
)
_OUTBOX_STATUSES = (
    "PENDING",
    "PROCESSING",
    "APPLIED",
    "SKIPPED",
    "ERROR",
    "DEAD_LETTER",
)
_LINEAGE_COLUMNS = frozenset(
    {
        "source_run_id",
        "source_watermark",
        "source_watermark_hash",
        "source_event_id",
        "source_observed_at",
        "data_age_sec",
        "generated_by",
    }
)
_LINEAGE_TABLE_COLUMNS: dict[str, frozenset[str]] = {
    "strategy_observations_latest": _LINEAGE_COLUMNS,
    "risk_observations_latest": _LINEAGE_COLUMNS | {"strategy_observation_id"},
    "entry_timing_evaluations": _LINEAGE_COLUMNS
    | {"strategy_observation_id", "risk_observation_id"},
    "order_plan_drafts_latest": _LINEAGE_COLUMNS
    | {
        "entry_timing_evaluation_id",
        "strategy_observation_id",
        "risk_observation_id",
    },
}

# SHA-256 of normalized schema-62 SQL (uppercase, whitespace removed,
# IF NOT EXISTS removed).  These objects are safety boundaries rather than
# optional query accelerators, so name-only checks are insufficient.
_SCHEMA_OBJECT_MANIFEST: dict[str, tuple[str, str]] = {
    "gateway_order_broker_boundaries": (
        "table",
        "b3184cc9042e128fdc9a580936269827f8f03f489fdbb2a85491970fd1e76f3c",
    ),
    "gateway_order_broker_boundary_resolutions": (
        "table",
        "da19450c9f231aadc142caaadf909083a0397ff38463d80d1dd414a7e9f7d59d",
    ),
    "uq_gateway_order_boundary_idempotency": (
        "index",
        "bb6b9f6b1340ff485f0f006fe6973a793c4a81a90fe9e07510615b324ed7a566",
    ),
    "idx_gateway_order_boundary_state_updated": (
        "index",
        "5eee4ab28632deba780a1f8576a221a9ecf8b32fdb3702221f06c152cacdbf39",
    ),
    "idx_gateway_order_boundary_broker_order_no": (
        "index",
        "f3a2321ccc0d061212ff40c390f525530ee69e908596ff40824b299250551e53",
    ),
    "idx_gateway_events_command_event": (
        "index",
        "8c544bf80c3fe9a1f79ac2b092e1603b3ab1f743371af8ef1c42d4ade7aaf683",
    ),
    "idx_gateway_command_events_command_event": (
        "index",
        "cca6c1f862dfd0def9b02040b161d6e367897c3ee8502e227e448a06971b3e98",
    ),
    "idx_gateway_order_boundary_resolutions_created": (
        "index",
        "7d5c35d05f1af321dd78216894987b78179c3c282aa4ba7612b610fc5b27bfb2",
    ),
    "uq_gateway_order_boundary_resolutions_request_id": (
        "index",
        "ab990fa05a3f9ad4f585e4e8879ccc73e40aa8579ef60042e358c66f9dac3930",
    ),
    "uq_gateway_order_boundary_resolutions_command_sequence": (
        "index",
        "e9d789eb5559a1b3725cbb6e0600592c5955dcfba875b60d272aed05dbd583af",
    ),
    "trg_gateway_order_boundary_resolutions_no_update": (
        "trigger",
        "1770a94273632fa0f3f29438bc6f044649e1f80c02b7251048c23c900cb10dc8",
    ),
    "trg_gateway_order_boundary_resolutions_no_delete": (
        "trigger",
        "5b286f70771e803d41ef97adb03a7520b2ab240b67cca7c5a21bc6262974dfd4",
    ),
    "incremental_evaluation_dead_letters": (
        "table",
        "9dfd31b8a5a5381a3c5aa052b8a9ed40a0201aeb2f7b70acba4553c214738b8c",
    ),
    "incremental_evaluation_dead_letter_dispositions": (
        "table",
        "0c10bf4a153d6c240e0e1f91a8de249424a148ff0b93e92270ad4d137fa69e01",
    ),
    "idx_incremental_evaluation_dead_letter_candidate_time": (
        "index",
        "5703fdbf067ce2305522b916e3e30626f1548cadf529d4ebdb56b9da4027d512",
    ),
    "idx_incremental_evaluation_dead_letter_status_time": (
        "index",
        "5be5141de29263e5242f85ec1f7cd1b7b673f56e1728d98c651b6edefac83b7f",
    ),
    "idx_incremental_dead_letter_disposition_effective": (
        "index",
        "f52089d11d12cae19852fe15ebe810f396494603272c1bf393365ca01804a670",
    ),
    "idx_incremental_dead_letter_disposition_session": (
        "index",
        "80a91ebc89dbc21a16153bcecb65c6e2b39e1c924cd38df5d0a3ade4b1e75933",
    ),
    "trg_incremental_evaluation_dead_letters_no_update": (
        "trigger",
        "c824843303391d0e8edc617fdc01df0e6dc9ac7fe3db6ff9023dfaf5e0d8a90d",
    ),
    "trg_incremental_evaluation_dead_letters_no_delete": (
        "trigger",
        "004e6641b3c0e5074f03ec9ec62b538a4b78f38776c710808cb7d730a0a15d40",
    ),
    "trg_incremental_dead_letter_dispositions_no_update": (
        "trigger",
        "42673b95101a11ec3a900879ed153eae6506c3a4949a90fb04c370c8ccae4d37",
    ),
    "trg_incremental_dead_letter_dispositions_no_delete": (
        "trigger",
        "6372a7c9c69cf63b712146bf971715c3705aa44b6ef4790428d3e4a82e6c880b",
    ),
    "runtime_execution_locks": (
        "table",
        "6ba0f871d73d4f145f10cabfc7ee120bbfb4a46e90cea34c62d88ab79711458c",
    ),
    "runtime_execution_lock_fences": (
        "table",
        "986d664cf9c25cf96d56f1bc7426f8f0610455907a4d724100882719269a53af",
    ),
    "idx_runtime_execution_locks_expires": (
        "index",
        "50fc0ce23f3e5575f89bb5a3eb9defe6e9feba817b648eff87c10cf4fbe94fa1",
    ),
    "idx_runtime_execution_locks_heartbeat": (
        "index",
        "fc4f342abcdcf7b69f5f92d5c94e8475943b062e26c33b123b5b9bfd5287cb99",
    ),
    "idx_runtime_execution_locks_process": (
        "index",
        "05c1ced8fb96244021ce15dc9787adc7c9f89b4ee012e773431220aee6ab6704",
    ),
    "uq_live_sim_intents_order_plan_id": (
        "index",
        "17787b5ac500ea9cd0a724b7e2f457fac2377c6505268a62cdadcecc8a092b18",
    ),
    "idx_strategy_latest_source_watermark": (
        "index",
        "37ed3b0290633a2dd7368888097de4d1a903c84cace9ff5643fd8d95d2f40b8c",
    ),
    "idx_risk_latest_source_watermark": (
        "index",
        "0f9bcb5a69dee14d1efbe58659d2fcbc702d2f33628342a1dff7b5f5ecd731da",
    ),
    "idx_entry_timing_candidate_lineage": (
        "index",
        "52f082cb73f46db7dcac6dbbde22dd8b375cff922ddf0bdd5713a8e1a3347d24",
    ),
    "idx_order_plan_latest_candidate_lineage": (
        "index",
        "6d7c2db12eb8cc07350357a2d84402261d511d5b5b7d1f69c22ff5027fc33c8c",
    ),
    "projection_outbox": (
        "table",
        "c02ac6f935b49d7b9106466549219a21db78903aca1159788687bfc5c7bcf2d4",
    ),
    "projection_watermarks": (
        "table",
        "e7dacd2dbddeda5a175b3080840a23578e3ae6d26e8cf20b31e460005d34dab9",
    ),
    "projection_event_results": (
        "table",
        "09ba730795d1baef589dc3bb1664229bd3f14bd817d24ff75df2070abadcc430",
    ),
    "idx_projection_watermarks_updated_at": (
        "index",
        "f3268c0921a3bbf1931586a91cc7539e373315a3a63868df88104cacbaebc70e",
    ),
    "idx_projection_event_results_event": (
        "index",
        "da6926bcb3bf31e2f45471915c82fb8cf6363ab1e4c763b78785e5c0602030d6",
    ),
    "idx_projection_event_results_status": (
        "index",
        "94757a220e1bfdb0526f477a4fa923c5e8edb5e4166c5256374c9cbd2c35d33a",
    ),
    "idx_projection_outbox_status_available": (
        "index",
        "632aeb4effd2e9376ec011ae2adae229f9bdcac3ce896434407dae400dd76b37",
    ),
    "idx_projection_outbox_projection_status": (
        "index",
        "534bc278d062d8bb0cbc437fa66a2455163004c5a3e22b9028184ecd9f21123b",
    ),
    "idx_projection_outbox_event_rowid": (
        "index",
        "63ed7e7c9ff5937867f857c123a408c7baaa223e49ce2a96552ee10507579811",
    ),
    "idx_projection_outbox_event_status_projection": (
        "index",
        "fbc36176c7444db0e28c8de08c761ebbc5970639ff34d4448aa04e74cbd82fe1",
    ),
    "idx_projection_outbox_updated_at": (
        "index",
        "5c01834234b3f0985d1fc52b9fab126cdb71a2e2ed3300c81a90c138c2ac29ee",
    ),
    "market_data_projection_reconcile_runs": (
        "table",
        "7d16692dbf9b381408bd1e873cca0dccd8f767ee130e35f4ab74a1c6aaf1cc20",
    ),
    "market_data_projection_routing_decisions": (
        "table",
        "f70508f43635b6d0598b673c6e62ab55d5c3e71514be8fe826603ef14ac1b80a",
    ),
    "pipeline_coherency_dispositions": (
        "table",
        "ca63469c6854d5d505072106a36e0c2c3e8b53c8bb7023076eb80a32bd2da0d0",
    ),
    "idx_pipeline_coherency_disposition_effective": (
        "index",
        "c4f56dea6b2fb74d80fcf9678c4b04b8e5e0efb0f9d35b42e74b08a11686b1d1",
    ),
    "idx_pipeline_coherency_disposition_action_created": (
        "index",
        "f6eb61a80dee95bec6fec5c466a87f61338c40e096afd77f75ae382c5569d946",
    ),
    "trg_pipeline_coherency_dispositions_no_update": (
        "trigger",
        "cfdb07cae97e840e4973088e6b19232dec6e083bea65cfe7d592dff22373956e",
    ),
    "trg_pipeline_coherency_dispositions_no_delete": (
        "trigger",
        "7d900ca05a201d28d287b0dec108846586a0242a8e413d5cf61999d933ff08a4",
    ),
}

# Schema 59 added the runtime lease columns with ALTER TABLE.  SQLite preserves
# that historical column order in sqlite_master, so both normalized forms are
# exact schema-62 contracts with identical columns and constraints.
_SCHEMA_OBJECT_ALTERNATE_SHA256: dict[str, frozenset[str]] = {
    "runtime_execution_locks": frozenset(
        {"b63757451e05083160e38649e423865e34cc1935b43dac925cc8bd72df7c1fd8"}
    ),
    "market_data_projection_reconcile_runs": frozenset(
        {"d72c4b7c65159b838b05657784a342a94063286f7dac21a2c177dd4653d4a77d"}
    ),
    "market_data_projection_routing_decisions": frozenset(
        {"ef06232ccdd03d754198db1c386906d324af85be1b2a31ce51f3a0bb00f690da"}
    ),
}

_SCHEMA_OBJECT_TOKEN_SHA256: dict[str, str] = {
    "gateway_order_broker_boundaries": "af37fbf1f1ed9cb3ce61d79a12e5ebaebaf69b45bfbcf693f5b6598ea10dfa43",
    "gateway_order_broker_boundary_resolutions": "33d9fb9df36fbc1d76c7c77d4d5c89d9c7e54bb8dd725da122b64be44ce60745",
    "idx_entry_timing_candidate_lineage": "fcbaa89a1b3a3c26c65fef71e4d6e896bd85eb998a573a137ea31fe2a5289a99",
    "idx_gateway_command_events_command_event": "1722e6a687495ca86941faa8f0df9f20f5c7e6d0cbe7192117df6db91e9fb12c",
    "idx_gateway_events_command_event": "22f6eed0d757181db4f38afb4d929de9502f77a770e149b06c67812d29f7761d",
    "idx_gateway_order_boundary_broker_order_no": "c6d3025d193d3950eeffb8b4171f8147878f9d0a9b0fc3c515b068f4e87c7c21",
    "idx_gateway_order_boundary_resolutions_created": "7f1a2da4891797abe098a81d4b2f3e982e48805c8c02abf62a499953c50f68c6",
    "idx_gateway_order_boundary_state_updated": "6e738ab42252443edca136b622dda8ee007d61d14f796a7ff3bd2400a6ed1979",
    "idx_incremental_dead_letter_disposition_effective": "b0b525eabda3d6662e52e76c98ed9521a67ec47beef34213ff6834a142d9135a",
    "idx_incremental_dead_letter_disposition_session": "f3c542a5ea8da29e927a9503e346adb0bc33d0e98f201a69dc291d333b16a5b7",
    "idx_incremental_evaluation_dead_letter_candidate_time": "7799fcb0b0f63931a9df01b4b6b6da53f09c0370a7015aec76cdf92c0cc1a78f",
    "idx_incremental_evaluation_dead_letter_status_time": "84148221c267263c8d8402fa494cafcf9b798c9bb65af78efc140fa5c196bfa9",
    "idx_order_plan_latest_candidate_lineage": "172fe5e11968098aa0ec4b610ffc293f02e982666643394a0c6310abd51a295e",
    "idx_pipeline_coherency_disposition_action_created": "caafabeba18086e48af168273a3932012ed26e8b3384ea57466c80d675c6e856",
    "idx_pipeline_coherency_disposition_effective": "ce36211d301da56cee9f401de4faaf7ba42fababfb8f256ba445ec3de47f8f9f",
    "idx_projection_event_results_event": "860a9c0466330e061817c2587eb844fd8a2d82a9022caa0da8199716bd398c5e",
    "idx_projection_event_results_status": "d84009e2c5b8536e6b99bc06fa555f9929973a27750e2bb044df94ba533dd9c1",
    "idx_projection_outbox_event_rowid": "c83748a31808c6a754d56761dfd8a2c0cc60aabaf8a3d93e7d322e57e72eea43",
    "idx_projection_outbox_event_status_projection": "398245c574d89f4ce030e4f1f28d4c0d106f4fa670109de8df14c7b04849ff2a",
    "idx_projection_outbox_projection_status": "e527f846b7cf0f3ace3f8db22b67275f5c3110a95741b82e2154e893a9a2a8f4",
    "idx_projection_outbox_status_available": "79e94472dacd36551192cf36bca7721c7625c3b8d3858277fc775a851a7f0606",
    "idx_projection_outbox_updated_at": "786b59aadc2ee475856bda238fa520b2f173c57c448f931c59c08c2181f3f1b5",
    "idx_projection_watermarks_updated_at": "6647db722125749a3bd8a0a49efd9c146df9c17c75869268f2bc8089d722ae28",
    "idx_risk_latest_source_watermark": "f5186cd81a19eaaef4803c840c787d7fba43427ecc15b0ed027f0c3278ddc173",
    "idx_runtime_execution_locks_expires": "599c7f409fd3573bc49a2e07d35b93c8414f8c996ab2f7a16b019784df841839",
    "idx_runtime_execution_locks_heartbeat": "5542036a038211983e6d7516f70b16a90b32deb00a890cce68ff0790e48ddd2e",
    "idx_runtime_execution_locks_process": "97c242d39b1d685568fcea38d0be34a7c8784a06956fba6717bb07a84fb823ec",
    "idx_strategy_latest_source_watermark": "e09442094c7cf543fda93844eddb37ce32046dcba162b98a08b978a431dc63b7",
    "incremental_evaluation_dead_letter_dispositions": "2ddc314741f526fb5ef09060025d6e707298ad75ebe9f23ed02a50ba7385f420",
    "incremental_evaluation_dead_letters": "c14d8d5c6c209ee7800e22e0a87b9a5c838952dd133df64e2751316983d0fa5d",
    "market_data_projection_reconcile_runs": "76bfb938ca25cb20644e338626da2d03a6d94adb0671228aea8939b798537556",
    "market_data_projection_routing_decisions": "6803bb4fc50c15cffb89ce13a01b92ef0dfcf08977fde9f197e69b14c68e2cab",
    "pipeline_coherency_dispositions": "ed40f77fe05c6edef7822bdfe0ef09b0eec1e70731efc14132212d40fea8ef85",
    "projection_event_results": "f5e09facd716b5243f27eb44339c899b44259d83d099b61b66dc50fd4744628a",
    "projection_outbox": "1cdedbcb7d8fad2fe474bd00bac3e0ee2d98f9187730caeb57b193b0fc194e04",
    "projection_watermarks": "eeecfd08ab4757d235403e371e6d725f4d8e310e3d3f51d9c872005d363e556e",
    "runtime_execution_lock_fences": "cd121e71c01bcd16f10eeead9e1f988b63e9e9912451cc109b7f999e0d2f1343",
    "runtime_execution_locks": "c3ee6bbdf37899f89637b3538261f9d7376932b711aca0faca4b38f5728985e2",
    "trg_gateway_order_boundary_resolutions_no_delete": "b2ba7f48d2a1f928857aa62417db04ed402dbb3f0050dc8d916f1fd252873333",
    "trg_gateway_order_boundary_resolutions_no_update": "b59d8f3f2368e8ff9d2a40900cc384d53c5a86567df3a95c225015b593a56fba",
    "trg_incremental_dead_letter_dispositions_no_delete": "81a89c9aed5005e760f35a40ef42f2ed86cd9b6d272bc5f495964a770256ce48",
    "trg_incremental_dead_letter_dispositions_no_update": "83d053eaa289fa62570478ea069a51535e9625e7d36b1c3a5d1814bd8519ae85",
    "trg_incremental_evaluation_dead_letters_no_delete": "f39d3fc34e3ec86994c0760e76bfb86d819a9754dad5897b5cd5b72d68b9a18f",
    "trg_incremental_evaluation_dead_letters_no_update": "f307456530338ea5f6a024302c31bd7179de1a103b363c1b7b70fd728b46f951",
    "trg_pipeline_coherency_dispositions_no_delete": "8c0647ca07d2ebcf912c269b90232de9569f4ba51c6f0a7006902007c4952332",
    "trg_pipeline_coherency_dispositions_no_update": "874a295dc5751fd6161a97d6a088f55fc1ca4d59fe44461905a5e2a922230a18",
    "uq_gateway_order_boundary_idempotency": "2b01893a2bdec9b9519193fd6de46ae6b52602cec267ad557482e520897a4fcf",
    "uq_gateway_order_boundary_resolutions_command_sequence": "c0be1155f47f26e608d641f64085d5a824964d1a16bbd1c763cbb2fc94348a69",
    "uq_gateway_order_boundary_resolutions_request_id": "3dc6e349cb17e0bd8f3092ae9846a84dc4596236128ea56247d46ed85bfc439e",
    "uq_live_sim_intents_order_plan_id": "0cf81c64d9b046b2a2b0278c6fb4881905dc9ac5d15bed389bb8390b891cb24d",
}

_SCHEMA_OBJECT_OWNER: dict[str, str] = {
    "idx_entry_timing_candidate_lineage": "entry_timing_evaluations",
    "idx_gateway_command_events_command_event": "gateway_command_events",
    "idx_gateway_events_command_event": "gateway_events",
    "idx_gateway_order_boundary_broker_order_no": "gateway_order_broker_boundaries",
    "idx_gateway_order_boundary_resolutions_created": "gateway_order_broker_boundary_resolutions",
    "idx_gateway_order_boundary_state_updated": "gateway_order_broker_boundaries",
    "idx_incremental_dead_letter_disposition_effective": "incremental_evaluation_dead_letter_dispositions",
    "idx_incremental_dead_letter_disposition_session": "incremental_evaluation_dead_letter_dispositions",
    "idx_incremental_evaluation_dead_letter_candidate_time": "incremental_evaluation_dead_letters",
    "idx_incremental_evaluation_dead_letter_status_time": "incremental_evaluation_dead_letters",
    "idx_order_plan_latest_candidate_lineage": "order_plan_drafts_latest",
    "idx_pipeline_coherency_disposition_action_created": "pipeline_coherency_dispositions",
    "idx_pipeline_coherency_disposition_effective": "pipeline_coherency_dispositions",
    "idx_projection_event_results_event": "projection_event_results",
    "idx_projection_event_results_status": "projection_event_results",
    "idx_projection_outbox_event_rowid": "projection_outbox",
    "idx_projection_outbox_event_status_projection": "projection_outbox",
    "idx_projection_outbox_projection_status": "projection_outbox",
    "idx_projection_outbox_status_available": "projection_outbox",
    "idx_projection_outbox_updated_at": "projection_outbox",
    "idx_projection_watermarks_updated_at": "projection_watermarks",
    "idx_risk_latest_source_watermark": "risk_observations_latest",
    "idx_runtime_execution_locks_expires": "runtime_execution_locks",
    "idx_runtime_execution_locks_heartbeat": "runtime_execution_locks",
    "idx_runtime_execution_locks_process": "runtime_execution_locks",
    "idx_strategy_latest_source_watermark": "strategy_observations_latest",
    "trg_gateway_order_boundary_resolutions_no_delete": "gateway_order_broker_boundary_resolutions",
    "trg_gateway_order_boundary_resolutions_no_update": "gateway_order_broker_boundary_resolutions",
    "trg_incremental_dead_letter_dispositions_no_delete": "incremental_evaluation_dead_letter_dispositions",
    "trg_incremental_dead_letter_dispositions_no_update": "incremental_evaluation_dead_letter_dispositions",
    "trg_incremental_evaluation_dead_letters_no_delete": "incremental_evaluation_dead_letters",
    "trg_incremental_evaluation_dead_letters_no_update": "incremental_evaluation_dead_letters",
    "trg_pipeline_coherency_dispositions_no_delete": "pipeline_coherency_dispositions",
    "trg_pipeline_coherency_dispositions_no_update": "pipeline_coherency_dispositions",
    "uq_gateway_order_boundary_idempotency": "gateway_order_broker_boundaries",
    "uq_gateway_order_boundary_resolutions_command_sequence": "gateway_order_broker_boundary_resolutions",
    "uq_gateway_order_boundary_resolutions_request_id": "gateway_order_broker_boundary_resolutions",
    "uq_live_sim_intents_order_plan_id": "live_sim_intents",
}

_SCHEMA_OBJECT_ALTERNATE_TOKEN_SHA256: dict[str, frozenset[str]] = {
    "runtime_execution_locks": frozenset(
        {"7dd199f86808694ee1349e7fe3a8223f280f9cf0a08b038c2ef194b8e29977a9"}
    ),
    "market_data_projection_reconcile_runs": frozenset(
        {"0f542a092ce0a105b9f707034f7d1c51c0c4ef3836dac7b6102337d508267ce2"}
    ),
    "market_data_projection_routing_decisions": frozenset(
        {"e2e149935ef83c80bd3435e4f373e66337f186eb781ea3d3becb8ac26ca4b434"}
    ),
}
_EXPECTED_PERSISTENT_TRIGGERS = frozenset(
    name
    for name, (object_type, _legacy_hash) in _SCHEMA_OBJECT_MANIFEST.items()
    if object_type == "trigger"
) | frozenset(
    {
        "trg_gateway_order_boundary_fence_events_no_update",
        "trg_gateway_order_boundary_fence_events_no_delete",
    }
)

# The legacy normalized SQL hash intentionally ignores formatting and keyword
# case.  This second manifest preserves every quoted token byte-for-byte so a
# semantically different CHECK/default literal cannot collide with it.
_SCHEMA_OBJECT_QUOTED_TOKEN_SHA256: dict[str, str] = {
    "gateway_order_broker_boundaries": (
        "9b7d149cb5d7b73fae6519a8d54b82d772c0d7d9793bfdd293cbef462ad78874"
    ),
    "gateway_order_broker_boundary_resolutions": (
        "d43d0818fe9cd7c7f46ebfb50d501e9324b4bcc112161cd8c14c002151f40daa"
    ),
    "trg_gateway_order_boundary_resolutions_no_update": (
        "e59499633e7cc27ffe4edd71db59a1b01e2fa6d89671b099146e556777835f32"
    ),
    "trg_gateway_order_boundary_resolutions_no_delete": (
        "e59499633e7cc27ffe4edd71db59a1b01e2fa6d89671b099146e556777835f32"
    ),
    "incremental_evaluation_dead_letters": (
        "1969f41830c045507399992ff3d946bf7fd2fcd35fa799d005a735f45cfc198b"
    ),
    "incremental_evaluation_dead_letter_dispositions": (
        "dccefe96d5c74d5546361c6e1b710c526fc3d2028ccf8105b5b03a75873dcc49"
    ),
    "trg_incremental_evaluation_dead_letters_no_update": (
        "f1229bb120e7f4d5573a28aa551c85ddb6332b5ee8c9a71b7000f95e2a84e5c1"
    ),
    "trg_incremental_evaluation_dead_letters_no_delete": (
        "f1229bb120e7f4d5573a28aa551c85ddb6332b5ee8c9a71b7000f95e2a84e5c1"
    ),
    "trg_incremental_dead_letter_dispositions_no_update": (
        "a4dec666ea9f68963b1c0c3594b4ec132736b4b7065dfd5372c2c59babdaddad"
    ),
    "trg_incremental_dead_letter_dispositions_no_delete": (
        "a4dec666ea9f68963b1c0c3594b4ec132736b4b7065dfd5372c2c59babdaddad"
    ),
    "runtime_execution_locks": ("0cf96a4d5c23d24eccca4db5c0d047aa7cdadba41b6a617c7019001857e646b2"),
    "runtime_execution_lock_fences": (
        "2de9deef20c614b068e3c6e1591284ecd1526411b7e53d13d5352ef13c64d863"
    ),
    "projection_outbox": ("2958cc8fa911e5f5e40bad725b286e5c12afb3d64e3f49eef3a820ff0a1f4f86"),
    "projection_watermarks": ("c05f89c97f7eb6c027f0adc6ac93be07429ed58f2e0fe88dc0c1a3a20e4f8d7e"),
    "projection_event_results": (
        "ab5757501264e195f63b3cf5a87c6f9f7f8fcbcd17dc941e32f100b445ce91df"
    ),
    "market_data_projection_reconcile_runs": (
        "7cd8ea117244dc4e49a96ba91e30e9b8e9407d2b675dd67145c1b7f3eaa3140a"
    ),
    "market_data_projection_routing_decisions": (
        "3fe1da1ce486958d31698c4d4dc51a46c9789c6e44df4be52bdadb78253b5c55"
    ),
    "pipeline_coherency_dispositions": (
        "877c26f8eb36edbf26787c1e808546d78755ed51e169577936005f56a7c72b66"
    ),
    "trg_pipeline_coherency_dispositions_no_update": (
        "1d5878b2fcf41e165d0e41b0449b9aa4ce169d8be47b9e14d4652ebfd24aa852"
    ),
    "trg_pipeline_coherency_dispositions_no_delete": (
        "1d5878b2fcf41e165d0e41b0449b9aa4ce169d8be47b9e14d4652ebfd24aa852"
    ),
}

_SCHEMA_OBJECT_ALTERNATE_QUOTED_TOKEN_SHA256: dict[str, frozenset[str]] = {
    "market_data_projection_routing_decisions": frozenset(
        {"7fb1b1a9a0b8555fd3238c5faa64f4d8a46458c17219c3943e56e9136b3e8324"}
    )
}

_CRITICAL_TRIGGER_MANIFEST: dict[str, frozenset[str]] = {
    "gateway_order_broker_boundaries": frozenset(),
    "gateway_order_broker_boundary_resolutions": frozenset(
        {
            "trg_gateway_order_boundary_resolutions_no_delete",
            "trg_gateway_order_boundary_resolutions_no_update",
        }
    ),
    "incremental_evaluation_dead_letters": frozenset(
        {
            "trg_incremental_evaluation_dead_letters_no_delete",
            "trg_incremental_evaluation_dead_letters_no_update",
        }
    ),
    "incremental_evaluation_dead_letter_dispositions": frozenset(
        {
            "trg_incremental_dead_letter_dispositions_no_delete",
            "trg_incremental_dead_letter_dispositions_no_update",
        }
    ),
    "runtime_execution_locks": frozenset(),
    "runtime_execution_lock_fences": frozenset(),
    "live_sim_intents": frozenset(),
    "projection_outbox": frozenset(),
    "projection_watermarks": frozenset(),
    "projection_event_results": frozenset(),
    "pipeline_coherency_dispositions": frozenset(
        {
            "trg_pipeline_coherency_dispositions_no_delete",
            "trg_pipeline_coherency_dispositions_no_update",
        }
    ),
}

_DB_MARKER_COLUMNS: dict[str, tuple[str, ...]] = {
    "gateway_commands": ("last_error",),
    "gateway_events": ("error_message",),
    "incremental_evaluation_queue": ("last_error",),
    "incremental_evaluation_dead_letters": ("last_error",),
    "projection_outbox": ("last_error",),
    "projection_event_results": ("error_message",),
    "live_sim_errors": ("error_message", "payload_json"),
    "live_sim_runs": ("error_message",),
    "dry_run_errors": ("error_message", "payload_json"),
    "strategy_evaluation_errors": ("error_message", "payload_json"),
    "risk_evaluation_errors": ("error_message", "payload_json"),
    "entry_timing_evaluation_errors": ("error_message", "payload_json"),
    "live_sim_operating_runs": ("errors_json",),
}

_SENSITIVE_VALUE_PATTERNS = (
    re.compile(r"(?i)(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9]{20,}"),
    re.compile(r"(?i)github_pat_[A-Za-z0-9_]{20,}"),
    re.compile(r"(?i)\bsk-[A-Za-z0-9_-]{20,}\b"),
    re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._~+/-]{16,}=*"),
    re.compile(r"(?i)(?:token|password|secret|api[_-]?key|account)\s*[:=]\s*\S+"),
    re.compile(r"(?i)(?:acct|account|계좌)[_.:@/-]?\d{6,16}"),
    re.compile(r"(?<![A-Za-z0-9])\d{8,16}(?![A-Za-z0-9])"),
)
_HYPHENATED_ACCOUNT_PATTERN = re.compile(r"(?<!\d)\d{3,6}-\d{2,6}(?:-\d{1,6})?(?!\d)")
_ISO_DATE_PATTERN = re.compile(r"(?<!\d)\d{4}-\d{2}-\d{2}(?!\d)")


class Fast0StrictRequalificationError(RuntimeError):
    pass


class _PinnedDatabaseIdentity(AbstractContextManager["_PinnedDatabaseIdentity"]):
    """Keep the selected main file identity stable for hashing and SQLite reads."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.sqlite_source = str(path)
        self.method = ""
        self._initial_stat: os.stat_result | None = None
        self._stream: BinaryIO | None = None
        self._windows_handle: int | None = None
        self._completed = False

    def __enter__(self) -> _PinnedDatabaseIdentity:
        if os.name == "nt":
            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            create_file = kernel32.CreateFileW
            create_file.argtypes = [
                ctypes.c_wchar_p,
                ctypes.c_uint32,
                ctypes.c_uint32,
                ctypes.c_void_p,
                ctypes.c_uint32,
                ctypes.c_uint32,
                ctypes.c_void_p,
            ]
            create_file.restype = ctypes.c_void_p
            # Allow concurrent readers only.  Write/delete/rename handles are
            # denied while the aggregate snapshot and both hashes are built.
            handle = create_file(str(self.path), 0x80000000, 0x00000001, None, 3, 0x80, None)
            invalid = ctypes.c_void_p(-1).value
            if handle == invalid:
                raise Fast0StrictRequalificationError("DATABASE_IDENTITY_PIN_FAILED")
            self._windows_handle = int(handle)
            self.method = "WINDOWS_READ_HANDLE_DENY_WRITE_DELETE"
            self._initial_stat = self.path.stat()
        else:
            try:
                self._stream = self.path.open("rb")
            except OSError as exc:
                raise Fast0StrictRequalificationError("DATABASE_IDENTITY_PIN_FAILED") from exc
            descriptor = self._stream.fileno()
            descriptor_paths = (f"/proc/self/fd/{descriptor}", f"/dev/fd/{descriptor}")
            selected = next((value for value in descriptor_paths if Path(value).exists()), None)
            if selected is None:
                self._stream.close()
                self._stream = None
                raise Fast0StrictRequalificationError(
                    "DATABASE_PINNED_DESCRIPTOR_PATH_UNAVAILABLE"
                )
            self.sqlite_source = selected
            self.method = "POSIX_PINNED_DESCRIPTOR_PATH"
            self._initial_stat = os.fstat(self._stream.fileno())
        self.assert_path_identity()
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        try:
            if exc_type is None:
                self.assert_path_identity()
                self._completed = True
        finally:
            if self._stream is not None:
                self._stream.close()
                self._stream = None
            if self._windows_handle is not None:
                kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
                kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
                kernel32.CloseHandle(ctypes.c_void_p(self._windows_handle))
                self._windows_handle = None
        return None

    def assert_path_identity(self) -> None:
        if self._initial_stat is None:
            raise Fast0StrictRequalificationError("DATABASE_IDENTITY_PIN_NOT_ACTIVE")
        try:
            current = self.path.stat()
        except OSError as exc:
            raise Fast0StrictRequalificationError("DATABASE_PATH_IDENTITY_CHANGED") from exc
        if _stat_identity(current) != _stat_identity(self._initial_stat):
            raise Fast0StrictRequalificationError("DATABASE_PATH_IDENTITY_CHANGED")

    def fingerprint(self) -> dict[str, Any]:
        if self._initial_stat is None:
            raise Fast0StrictRequalificationError("DATABASE_IDENTITY_PIN_NOT_ACTIVE")
        if self._stream is None:
            # The Windows handle denies write/delete/rename while this pathname
            # reader is open, so a separate buffered read cannot change identity.
            return _fingerprint_regular_file(self.path)
        before = os.fstat(self._stream.fileno())
        self._stream.seek(0)
        digest = hashlib.sha256()
        while chunk := self._stream.read(1024 * 1024):
            digest.update(chunk)
        after = os.fstat(self._stream.fileno())
        if _stat_snapshot(before) != _stat_snapshot(after):
            raise Fast0StrictRequalificationError("DATABASE_FILE_CHANGED_DURING_HASH")
        return {
            "exists": True,
            "size": int(after.st_size),
            "mtime_ns": int(after.st_mtime_ns),
            "sha256": digest.hexdigest(),
        }

    def public_status(self) -> dict[str, Any]:
        return {
            "status": "PASS" if self._completed else "FAIL",
            "method": self.method,
            "held_across_snapshot": self._completed,
            "path_identity_stable": self._completed,
            "raw_identity_recorded": False,
        }


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Build aggregate-only FAST-0 strict requalification evidence from one "
            "immutable schema-62 SQLite snapshot."
        )
    )
    parser.add_argument("--db", required=True)
    parser.add_argument("--migration-apply-report", required=True)
    parser.add_argument("--migration-apply-report-sha256", required=True)
    parser.add_argument("--runtime-log", action="append", default=[])
    parser.add_argument("--pipeline-max-age-sec", type=float, default=60.0)
    parser.add_argument("--incremental-retry-limit", type=int, default=3)
    parser.add_argument(
        "--incremental-backlog-warn-count",
        type=int,
        default=_DEFAULT_INCREMENTAL_BACKLOG_WARN_COUNT,
    )
    parser.add_argument(
        "--incremental-backlog-fail-count",
        type=int,
        default=_DEFAULT_INCREMENTAL_BACKLOG_FAIL_COUNT,
    )
    parser.add_argument(
        "--incremental-stale-warn-sec",
        type=int,
        default=_DEFAULT_INCREMENTAL_STALE_WARN_SEC,
    )
    parser.add_argument(
        "--incremental-stale-fail-sec",
        type=int,
        default=_DEFAULT_INCREMENTAL_STALE_FAIL_SEC,
    )
    parser.add_argument(
        "--projection-stale-processing-sec",
        type=int,
        default=_POLICY_PROJECTION_STALE_PROCESSING_SEC,
    )
    parser.add_argument("--min-free-bytes", type=int, default=1024 * 1024 * 1024)
    parser.add_argument(
        "--out-dir",
        default=str(ROOT_DIR / "reports" / "fast0_strict_requalification"),
    )
    args = parser.parse_args()
    try:
        report = run_report(
            db_path=Path(args.db),
            migration_apply_report=Path(args.migration_apply_report),
            runtime_logs=[Path(value) for value in args.runtime_log],
            pipeline_max_age_sec=max(float(args.pipeline_max_age_sec), 0.0),
            incremental_retry_limit=max(int(args.incremental_retry_limit), 1),
            projection_stale_processing_sec=max(int(args.projection_stale_processing_sec), 0),
            min_free_bytes=max(int(args.min_free_bytes), 0),
            run_quick_check=True,
            out_dir=Path(args.out_dir),
            expected_migration_apply_report_sha256=str(args.migration_apply_report_sha256).lower(),
            incremental_backlog_warn_count=max(int(args.incremental_backlog_warn_count), 1),
            incremental_backlog_fail_count=max(int(args.incremental_backlog_fail_count), 1),
            incremental_stale_warn_sec=max(int(args.incremental_stale_warn_sec), 1),
            incremental_stale_fail_sec=max(int(args.incremental_stale_fail_sec), 1),
        )
    except Exception as exc:
        print(
            f"FAST-0 strict requalification: ERROR error_type={type(exc).__name__}",
            file=sys.stderr,
        )
        return 2
    print(render_console_summary(report))
    verdict = _mapping(report.get("verdict"))
    return 0 if verdict.get("status") == "PASS" else 2


def run_report(
    *,
    db_path: Path,
    migration_apply_report: Path,
    runtime_logs: Sequence[Path],
    pipeline_max_age_sec: float,
    incremental_retry_limit: int,
    projection_stale_processing_sec: int,
    min_free_bytes: int,
    run_quick_check: bool,
    out_dir: Path,
    incremental_backlog_warn_count: int = _DEFAULT_INCREMENTAL_BACKLOG_WARN_COUNT,
    incremental_backlog_fail_count: int = _DEFAULT_INCREMENTAL_BACKLOG_FAIL_COUNT,
    incremental_stale_warn_sec: int = _DEFAULT_INCREMENTAL_STALE_WARN_SEC,
    incremental_stale_fail_sec: int = _DEFAULT_INCREMENTAL_STALE_FAIL_SEC,
    expected_migration_apply_report_sha256: str | None = None,
) -> dict[str, Any]:
    if run_quick_check is not True:
        raise Fast0StrictRequalificationError("DATABASE_QUICK_CHECK_IS_MANDATORY")
    if str(SCHEMA_VERSION) != _EXPECTED_SCHEMA_VERSION:
        raise Fast0StrictRequalificationError("CODE_TARGET_SCHEMA_MISMATCH")
    if incremental_backlog_fail_count < incremental_backlog_warn_count:
        raise Fast0StrictRequalificationError("INCREMENTAL_BACKLOG_THRESHOLDS_INVALID")
    if incremental_stale_fail_sec < incremental_stale_warn_sec:
        raise Fast0StrictRequalificationError("INCREMENTAL_STALE_THRESHOLDS_INVALID")
    if (
        min(
            incremental_backlog_warn_count,
            incremental_backlog_fail_count,
            incremental_stale_warn_sec,
            incremental_stale_fail_sec,
        )
        <= 0
    ):
        raise Fast0StrictRequalificationError("INCREMENTAL_THRESHOLDS_NOT_POSITIVE")
    if (
        pipeline_max_age_sec != _POLICY_PIPELINE_MAX_AGE_SEC
        or incremental_retry_limit != _POLICY_INCREMENTAL_RETRY_LIMIT
        or projection_stale_processing_sec != _POLICY_PROJECTION_STALE_PROCESSING_SEC
        or incremental_backlog_warn_count != _DEFAULT_INCREMENTAL_BACKLOG_WARN_COUNT
        or incremental_backlog_fail_count != _DEFAULT_INCREMENTAL_BACKLOG_FAIL_COUNT
        or incremental_stale_warn_sec != _DEFAULT_INCREMENTAL_STALE_WARN_SEC
        or incremental_stale_fail_sec != _DEFAULT_INCREMENTAL_STALE_FAIL_SEC
        or min_free_bytes < _POLICY_MIN_FREE_BYTES
    ):
        raise Fast0StrictRequalificationError("FAST0_POLICY_RELAXATION_REJECTED")
    if not _is_sha256(expected_migration_apply_report_sha256):
        raise Fast0StrictRequalificationError("MIGRATION_REPORT_EXPECTED_SHA256_INVALID")

    resolved_path = _validated_database_path(db_path)
    _assert_no_sidecars(resolved_path)
    writer_probe_before = _probe_no_other_open_handles(resolved_path)
    disk = _disk_space_status(resolved_path, min_free_bytes=min_free_bytes)
    file_log_scan = _scan_runtime_logs(runtime_logs)
    observed_at = datetime.now(UTC)
    identity_pin = _PinnedDatabaseIdentity(resolved_path)
    with identity_pin:
        files_before = _file_fingerprints(
            resolved_path,
            pinned_main=identity_pin.fingerprint(),
        )
        baseline = _read_migration_baseline(
            migration_apply_report,
            expected_report_sha256=str(expected_migration_apply_report_sha256),
            current_path=resolved_path,
            current_main=files_before["main"],
        )

        connection = _open_strict_read_only(
            resolved_path,
            sqlite_source=identity_pin.sqlite_source,
        )
        connection.execute("BEGIN DEFERRED")
        try:
            quick_check_raw = [
                str(row[0]) for row in connection.execute("PRAGMA quick_check(1)")
            ]
            quick_check = ["ok"] if quick_check_raw == ["ok"] else ["not_ok"]
            database_list = [
                {"seq": int(row[0]), "name": str(row[1]), "file_present": bool(row[2])}
                for row in connection.execute("PRAGMA database_list").fetchall()
                if bool(row[2])
            ]
            identity = _read_database_identity(connection)
            schema_manifest = _validate_schema_manifest(connection)
            lineage_schema = _validate_lineage_schema(connection)
            order_state_inventory = {
                "preservation_contract_valid": (
                    baseline.get("preservation_contract_valid") is True
                ),
                "continuity_contract": "MIGRATION_CHAIN_PLUS_CURRENT_PHYSICAL_HASH",
                "raw_rows_recorded": False,
            }
            order_plan = _collect_order_plan_status(connection)
            broker_boundary = _collect_broker_boundary_status(connection)
            incremental = _collect_incremental_status(
                connection,
                retry_limit=incremental_retry_limit,
                backlog_warn_count=incremental_backlog_warn_count,
                backlog_fail_count=incremental_backlog_fail_count,
                stale_warn_sec=incremental_stale_warn_sec,
                stale_fail_sec=incremental_stale_fail_sec,
                observed_at=observed_at,
            )
            pipeline = _read_pipeline_all_pages(
                connection,
                max_age_sec=pipeline_max_age_sec,
                as_of=observed_at,
            )
            lifecycle = _sanitize_lifecycle_report(
                lifecycle_tool._read_all_pages(connection, code=None)
            )
            projection = _collect_projection_status(
                connection,
                stale_processing_sec=projection_stale_processing_sec,
                observed_at=observed_at,
            )
            runtime = _collect_runtime_status(connection, observed_at=observed_at)
            database_marker_scan = _scan_database_markers(connection)
            modify_order = _collect_modify_order_counts(connection)
        finally:
            connection.rollback()
            connection.close()

        identity_pin.assert_path_identity()
        _validated_database_path(resolved_path)
        _assert_no_sidecars(resolved_path)
        files_after = _file_fingerprints(
            resolved_path,
            pinned_main=identity_pin.fingerprint(),
        )
        baseline["current_after_matches_migration"] = _fingerprint_matches(
            baseline.get("migration_database"),
            files_after.get("main"),
        )
        table_inventory = _baseline_logical_preservation(baseline)
    writer_probe_after = _probe_no_other_open_handles(resolved_path)
    identity_pin_status = identity_pin.public_status()

    report: dict[str, Any] = {
        "contract": "fast0-strict-offline-requalification.v1",
        "policy": {
            "contract": "fast0-strict-offline-policy.v1",
            "pipeline_max_age_sec": pipeline_max_age_sec,
            "incremental_retry_limit": incremental_retry_limit,
            "incremental_backlog_warn_count": incremental_backlog_warn_count,
            "incremental_backlog_fail_count": incremental_backlog_fail_count,
            "incremental_stale_warn_sec": incremental_stale_warn_sec,
            "incremental_stale_fail_sec": incremental_stale_fail_sec,
            "projection_stale_processing_sec": projection_stale_processing_sec,
            "projection_blocking_older_than_sec": (_POLICY_PROJECTION_BLOCKING_OLDER_THAN_SEC),
            "projection_backlog_warn_pending_count": (
                _POLICY_PROJECTION_BACKLOG_WARN_PENDING_COUNT
            ),
            "projection_backlog_fail_pending_count": (
                _POLICY_PROJECTION_BACKLOG_FAIL_PENDING_COUNT
            ),
            "projection_recent_window_sec": _POLICY_PROJECTION_RECENT_WINDOW_SEC,
            "projection_recent_fail_count": _POLICY_PROJECTION_RECENT_FAIL_COUNT,
            "projection_condition_max_pending": _POLICY_PROJECTION_CONDITION_MAX_PENDING,
            "projection_condition_recent_max_pending": (
                _POLICY_PROJECTION_CONDITION_RECENT_MAX_PENDING
            ),
            "min_free_bytes": min_free_bytes,
            "runtime_log_max_files": _MAX_RUNTIME_LOG_FILES,
            "runtime_log_max_total_bytes": _MAX_RUNTIME_LOG_TOTAL_BYTES,
        },
        "generated_at": datetime_to_wire(observed_at),
        "database": {
            "identity": identity,
            "database_list": database_list,
            "quick_check": quick_check,
            "connection": {
                "mode": "ro",
                "immutable": True,
                "query_only": True,
                "single_deferred_snapshot": True,
            },
            "files_before": files_before,
            "files_after": files_after,
            "sidecars_absent_before": True,
            "sidecars_absent_after": True,
            "writer_probe_before": writer_probe_before,
            "writer_probe_after": writer_probe_after,
            "identity_pin": identity_pin_status,
            "writer_quiescence_scope": (
                "PINNED_IDENTITY_PLUS_POINT_IN_TIME_PROBES_AND_PHYSICAL_HASH"
            ),
            "whole_window_writer_absence_proven": False,
            "disk": disk,
        },
        "migration_baseline": baseline,
        "schema_manifest": schema_manifest,
        "pipeline_lineage_schema": lineage_schema,
        "logical_table_inventory": table_inventory,
        "order_state_inventory": order_state_inventory,
        "order_plan_uniqueness": order_plan,
        "broker_boundary": broker_boundary,
        "incremental_evaluation": incremental,
        "pipeline_coherency": pipeline,
        "execution_lifecycle": lifecycle,
        "projection": projection,
        "runtime_execution": runtime,
        "database_marker_scan": database_marker_scan,
        "file_log_marker_scan": file_log_scan,
        "modify_order": modify_order,
        "observe_smoke": {
            "status": "NOT_RUN_OUT_OF_SCOPE",
            "core_started": False,
            "gateway_started": False,
            "command_delta_verified": False,
        },
        "read_only": True,
        "observe_only": True,
        "raw_rows_recorded": False,
        "raw_payloads_recorded": False,
        "identifiers_recorded": False,
        "no_order_side_effects": True,
        "no_trading_side_effects": True,
        "core_started": False,
        "gateway_started": False,
        "live_sim_allowed": False,
        "live_real_allowed": False,
    }
    report["execution_lifecycle"]["strict_verdict"] = lifecycle_tool.evaluate_report(
        {
            "database": report["database"],
            "execution_lifecycle": report["execution_lifecycle"],
            "read_only": True,
            "observe_only": True,
            "raw_rows_recorded": False,
            "no_order_side_effects": True,
            "no_trading_side_effects": True,
        }
    )
    report["verdict"] = evaluate_report(report)
    paths = write_report(report, out_dir=out_dir)
    report["report_paths"] = {key: str(value) for key, value in paths.items()}
    return report


def evaluate_report(report: Mapping[str, Any]) -> dict[str, Any]:
    evidence_failures: list[str] = []
    blockers: list[str] = []
    warnings: list[str] = []
    database = _mapping(report.get("database"))
    policy = _mapping(report.get("policy"))
    identity = _mapping(database.get("identity"))
    connection = _mapping(database.get("connection"))
    baseline = _mapping(report.get("migration_baseline"))
    schema = _mapping(report.get("schema_manifest"))
    lineage = _mapping(report.get("pipeline_lineage_schema"))
    inventory = _mapping(report.get("logical_table_inventory"))
    order_state_inventory = _mapping(report.get("order_state_inventory"))
    order_plan = _mapping(report.get("order_plan_uniqueness"))
    broker = _mapping(report.get("broker_boundary"))
    incremental = _mapping(report.get("incremental_evaluation"))
    incremental_thresholds = _mapping(incremental.get("thresholds"))
    pipeline = _mapping(report.get("pipeline_coherency"))
    lifecycle = _mapping(report.get("execution_lifecycle"))
    projection = _mapping(report.get("projection"))
    projection_thresholds = _mapping(projection.get("thresholds"))
    runtime = _mapping(report.get("runtime_execution"))
    db_markers = _mapping(report.get("database_marker_scan"))
    file_markers = _mapping(report.get("file_log_marker_scan"))
    modify_order = _mapping(report.get("modify_order"))

    expected_policy = {
        "contract": "fast0-strict-offline-policy.v1",
        "pipeline_max_age_sec": _POLICY_PIPELINE_MAX_AGE_SEC,
        "incremental_retry_limit": _POLICY_INCREMENTAL_RETRY_LIMIT,
        "incremental_backlog_warn_count": _DEFAULT_INCREMENTAL_BACKLOG_WARN_COUNT,
        "incremental_backlog_fail_count": _DEFAULT_INCREMENTAL_BACKLOG_FAIL_COUNT,
        "incremental_stale_warn_sec": _DEFAULT_INCREMENTAL_STALE_WARN_SEC,
        "incremental_stale_fail_sec": _DEFAULT_INCREMENTAL_STALE_FAIL_SEC,
        "projection_stale_processing_sec": _POLICY_PROJECTION_STALE_PROCESSING_SEC,
        "projection_blocking_older_than_sec": _POLICY_PROJECTION_BLOCKING_OLDER_THAN_SEC,
        "projection_backlog_warn_pending_count": (_POLICY_PROJECTION_BACKLOG_WARN_PENDING_COUNT),
        "projection_backlog_fail_pending_count": (_POLICY_PROJECTION_BACKLOG_FAIL_PENDING_COUNT),
        "projection_recent_window_sec": _POLICY_PROJECTION_RECENT_WINDOW_SEC,
        "projection_recent_fail_count": _POLICY_PROJECTION_RECENT_FAIL_COUNT,
        "projection_condition_max_pending": _POLICY_PROJECTION_CONDITION_MAX_PENDING,
        "projection_condition_recent_max_pending": (
            _POLICY_PROJECTION_CONDITION_RECENT_MAX_PENDING
        ),
        "runtime_log_max_files": _MAX_RUNTIME_LOG_FILES,
        "runtime_log_max_total_bytes": _MAX_RUNTIME_LOG_TOTAL_BYTES,
    }
    if (
        any(policy.get(key) != value for key, value in expected_policy.items())
        or int(policy.get("min_free_bytes") or 0) < _POLICY_MIN_FREE_BYTES
    ):
        evidence_failures.append("FAST0_POLICY_CONTRACT_INVALID")
    expected_incremental_thresholds = {
        "backlog_warn_count": _DEFAULT_INCREMENTAL_BACKLOG_WARN_COUNT,
        "backlog_fail_count": _DEFAULT_INCREMENTAL_BACKLOG_FAIL_COUNT,
        "stale_warn_sec": _DEFAULT_INCREMENTAL_STALE_WARN_SEC,
        "stale_fail_sec": _DEFAULT_INCREMENTAL_STALE_FAIL_SEC,
    }
    if (
        incremental.get("retry_limit") != _POLICY_INCREMENTAL_RETRY_LIMIT
        or incremental_thresholds != expected_incremental_thresholds
    ):
        evidence_failures.append("INCREMENTAL_THRESHOLD_CONTRACT_INVALID")
    expected_projection_thresholds = {
        "stale_processing_sec": _POLICY_PROJECTION_STALE_PROCESSING_SEC,
        "blocking_older_than_sec": _POLICY_PROJECTION_BLOCKING_OLDER_THAN_SEC,
        "backlog_warn_pending_count": _POLICY_PROJECTION_BACKLOG_WARN_PENDING_COUNT,
        "backlog_fail_pending_count": _POLICY_PROJECTION_BACKLOG_FAIL_PENDING_COUNT,
        "recent_window_sec": _POLICY_PROJECTION_RECENT_WINDOW_SEC,
        "recent_fail_count": _POLICY_PROJECTION_RECENT_FAIL_COUNT,
        "condition_max_pending": _POLICY_PROJECTION_CONDITION_MAX_PENDING,
        "condition_recent_max_pending": _POLICY_PROJECTION_CONDITION_RECENT_MAX_PENDING,
    }
    if projection_thresholds != expected_projection_thresholds:
        evidence_failures.append("PROJECTION_THRESHOLD_CONTRACT_INVALID")

    if database.get("files_before") != database.get("files_after"):
        evidence_failures.append("DATABASE_DATA_FILE_CHANGED")
    if database.get("quick_check") != ["ok"]:
        evidence_failures.append("DATABASE_QUICK_CHECK_FAILED")
    if connection != {
        "mode": "ro",
        "immutable": True,
        "query_only": True,
        "single_deferred_snapshot": True,
    }:
        evidence_failures.append("DATABASE_CONNECTION_CONTRACT_INVALID")
    if identity.get("app_name") != APP_NAME or identity.get("app_name_row_count") != 1:
        evidence_failures.append("DATABASE_APP_IDENTITY_INVALID")
    if (
        identity.get("schema_version") != _EXPECTED_SCHEMA_VERSION
        or identity.get("schema_version_row_count") != 1
    ):
        evidence_failures.append("DATABASE_SCHEMA_IDENTITY_INVALID")
    database_list = database.get("database_list")
    if database_list != [{"seq": 0, "name": "main", "file_present": True}]:
        evidence_failures.append("DATABASE_ATTACHMENT_CONTRACT_INVALID")
    for key in ("writer_probe_before", "writer_probe_after"):
        if _mapping(database.get(key)).get("status") != "PASS":
            evidence_failures.append(f"{key.upper()}_FAILED")
    identity_pin = _mapping(database.get("identity_pin"))
    if (
        identity_pin.get("status") != "PASS"
        or identity_pin.get("held_across_snapshot") is not True
        or identity_pin.get("path_identity_stable") is not True
        or identity_pin.get("raw_identity_recorded") is not False
    ):
        evidence_failures.append("DATABASE_IDENTITY_PIN_INVALID")
    if (
        database.get("writer_quiescence_scope")
        != "PINNED_IDENTITY_PLUS_POINT_IN_TIME_PROBES_AND_PHYSICAL_HASH"
        or database.get("whole_window_writer_absence_proven") is not False
    ):
        evidence_failures.append("WRITER_QUIESCENCE_SCOPE_INVALID")
    if _mapping(database.get("disk")).get("sufficient") is not True:
        evidence_failures.append("DATABASE_DISK_SPACE_INSUFFICIENT")
    if baseline.get("contract_ready") is not True:
        evidence_failures.append("MIGRATION_BASELINE_CONTRACT_INVALID")
    if baseline.get("current_before_matches_migration") is not True:
        evidence_failures.append("MIGRATION_BASELINE_BEFORE_MISMATCH")
    if baseline.get("current_after_matches_migration") is not True:
        evidence_failures.append("MIGRATION_BASELINE_AFTER_MISMATCH")
    if schema.get("ready") is not True:
        evidence_failures.append("SCHEMA_MANIFEST_INVALID")
    if lineage.get("ready") is not True:
        evidence_failures.append("PIPELINE_LINEAGE_SCHEMA_INVALID")
    if (
        inventory.get("verification_complete") is not True
        or inventory.get("verification_method")
        != "MIGRATION_CHAIN_AND_CURRENT_PHYSICAL_HASH"
        or inventory.get("full_table_row_scan_performed") is not False
        or not _is_sha256(inventory.get("inventory_digest"))
        or int(inventory.get("table_count") or 0) <= 0
    ):
        evidence_failures.append("LOGICAL_TABLE_INVENTORY_INVALID")
    if order_state_inventory.get("preservation_contract_valid") is not True:
        evidence_failures.append("ORDER_STATE_PRESERVATION_INVALID")
    if order_plan.get("exact_index_contract") is not True:
        evidence_failures.append("ORDER_PLAN_UNIQUE_INDEX_CONTRACT_INVALID")
    if broker.get("structure_ready") is not True:
        evidence_failures.append("BROKER_BOUNDARY_STRUCTURE_INVALID")
    if broker.get("state_bucket_contract_valid") is not True:
        evidence_failures.append("BROKER_BOUNDARY_STATE_BUCKET_INVALID")
    if incremental.get("schema_ready") is not True:
        evidence_failures.append("INCREMENTAL_DISPOSITION_SCHEMA_INVALID")
    if incremental.get("raw_status_contract_valid") is not True:
        evidence_failures.append("INCREMENTAL_RAW_STATUS_INVALID")
    if incremental.get("bucket_conservation_valid") is not True:
        evidence_failures.append("INCREMENTAL_BUCKET_CONSERVATION_INVALID")
    if incremental.get("queue_timestamp_contract_valid") is not True:
        evidence_failures.append("INCREMENTAL_QUEUE_TIMESTAMP_INVALID")
    if pipeline.get("schema_ready") is not True:
        evidence_failures.append("PIPELINE_DISPOSITION_SCHEMA_INVALID")
    if (
        pipeline.get("classification_bucket_contract_valid") is not True
        or pipeline.get("disposition_bucket_contract_valid") is not True
    ):
        evidence_failures.append("PIPELINE_BUCKET_CONTRACT_INVALID")
    for condition, reason in (
        (pipeline.get("inventory_count_consistent") is True, "PIPELINE_INVENTORY_DRIFT"),
        (pipeline.get("page_contract_consistent") is True, "PIPELINE_PAGE_CONTRACT_INVALID"),
        (
            pipeline.get("aggregate_contract_consistent") is True,
            "PIPELINE_AGGREGATE_CONTRACT_INVALID",
        ),
        (pipeline.get("invalid_item_count") == 0, "PIPELINE_ITEM_CONTRACT_INVALID"),
        (
            pipeline.get("full_count") == pipeline.get("collected_count"),
            "PIPELINE_FULL_COUNT_MISMATCH",
        ),
        (
            pipeline.get("collected_count") == pipeline.get("unique_subject_count"),
            "PIPELINE_DUPLICATE_SUBJECT",
        ),
    ):
        if not condition:
            evidence_failures.append(reason)
    lifecycle_verdict = _mapping(lifecycle.get("strict_verdict"))
    if lifecycle_verdict.get("status") != "PASS":
        evidence_failures.append("EXECUTION_LIFECYCLE_STRICT_CHECK_FAILED")
    if lifecycle.get("classification_bucket_contract_valid") is not True:
        evidence_failures.append("EXECUTION_LIFECYCLE_BUCKET_CONTRACT_INVALID")
    if lifecycle.get("reason_code_contract_valid") is not True:
        evidence_failures.append("EXECUTION_LIFECYCLE_REASON_CODE_CONTRACT_INVALID")
    if lifecycle.get("privacy_contract_valid") is not True:
        evidence_failures.append("EXECUTION_LIFECYCLE_PRIVACY_CONTRACT_INVALID")
    if projection.get("schema_ready") is not True:
        evidence_failures.append("PROJECTION_SCHEMA_INVALID")
    if projection.get("aggregate_contract_valid") is not True:
        evidence_failures.append("PROJECTION_AGGREGATE_CONTRACT_INVALID")
    if projection.get("readiness_status") not in {"PASS", "WARN", "FAIL"}:
        evidence_failures.append("PROJECTION_READINESS_CONTRACT_INVALID")
    if runtime.get("schema_ready") is not True:
        evidence_failures.append("RUNTIME_EXECUTION_SCHEMA_INVALID")
    if runtime.get("fencing_contract_valid") is not True:
        evidence_failures.append("RUNTIME_FENCING_CONTRACT_INVALID")
    if db_markers.get("scan_complete") is not True:
        evidence_failures.append("DATABASE_MARKER_SCAN_INCOMPLETE")
    if file_markers.get("scan_complete") is not True:
        evidence_failures.append("FILE_LOG_MARKER_SCAN_INCOMPLETE")
    if file_markers.get("coverage_provided") is not True:
        evidence_failures.append("RUNTIME_LOG_EVIDENCE_NOT_PROVIDED")
    if report.get("read_only") is not True or report.get("raw_rows_recorded") is not False:
        evidence_failures.append("REPORT_READ_ONLY_CONTRACT_INVALID")
    if report.get("identifiers_recorded") is not False:
        evidence_failures.append("REPORT_IDENTIFIER_POLICY_INVALID")

    if order_plan.get("status") != "PASS":
        blockers.append("ORDER_PLAN_UNIQUENESS_NOT_PASS")
    if int(broker.get("raw_unconfirmed_count") or 0) > 0:
        blockers.append("BROKER_RAW_UNCONFIRMED_PRESENT")
    if broker.get("semantic_contract_clear") is not True:
        blockers.append("BROKER_BOUNDARY_SEMANTIC_BLOCKER")
    if (
        int(incremental.get("queued_count") or 0)
        >= _DEFAULT_INCREMENTAL_BACKLOG_FAIL_COUNT
    ):
        blockers.append("INCREMENTAL_QUEUE_BACKLOG_FAIL")
    if int(incremental.get("stale_fail_count") or 0) > 0:
        blockers.append("INCREMENTAL_QUEUE_STALE_FAIL")
    if int(incremental.get("retry_exhausted_count") or 0) > 0:
        blockers.append("INCREMENTAL_RETRY_EXHAUSTED_PRESENT")
    if int(incremental.get("effective_dead_letter_count") or 0) > 0:
        blockers.append("INCREMENTAL_EFFECTIVE_DEAD_LETTER_PRESENT")
    if pipeline.get("qualification_status") != "PASS":
        blockers.append("PIPELINE_QUALIFICATION_NOT_PASS")
    if lifecycle.get("qualification_status") != "PASS":
        blockers.append("EXECUTION_LIFECYCLE_QUALIFICATION_NOT_PASS")
    for field, reason in (
        ("outbox_error_count", "PROJECTION_OUTBOX_ERROR_PRESENT"),
        ("outbox_dead_letter_count", "PROJECTION_OUTBOX_DEAD_LETTER_PRESENT"),
        ("outbox_processing_count", "QUIESCENT_PROJECTION_PROCESSING_PRESENT"),
        ("stale_processing_count", "PROJECTION_STALE_PROCESSING_PRESENT"),
        ("event_result_error_count", "PROJECTION_EVENT_ERROR_PRESENT"),
    ):
        if int(projection.get(field) or 0) > 0:
            blockers.append(reason)
    if projection.get("readiness_status") == "FAIL":
        blockers.append("PROJECTION_BACKLOG_READINESS_FAIL")
    if int(runtime.get("lock_count") or 0) > 0:
        blockers.append("RUNTIME_EXECUTION_LOCK_PRESENT")
    if any(int(value or 0) > 0 for value in _mapping(db_markers.get("marker_counts")).values()):
        blockers.append("DATABASE_RUNTIME_MARKER_PRESENT")
    if file_markers.get("coverage_provided") is not True:
        blockers.append("RUNTIME_LOG_EVIDENCE_NOT_PROVIDED")
    elif file_markers.get("coverage_authoritative") is not True:
        blockers.append("RUNTIME_LOG_COVERAGE_NOT_AUTHORITATIVE")
    if any(int(value or 0) > 0 for value in _mapping(file_markers.get("marker_counts")).values()):
        blockers.append("RUNTIME_LOG_MARKER_PRESENT")
    if any(int(value or 0) > 0 for value in modify_order.values()):
        blockers.append("MODIFY_ORDER_COMMAND_PRESENT")
    if _mapping(report.get("observe_smoke")).get("command_delta_verified") is not True:
        blockers.append("FINAL_OBSERVE_SMOKE_NOT_RUN")
    if int(projection.get("outbox_pending_count") or 0) > 0:
        warnings.append("PROJECTION_PENDING_ROWS_PRESERVED")
    if int(projection.get("applied_without_success_count") or 0) > 0:
        warnings.append("PROJECTION_APPLIED_WITHOUT_RESULT_DIAGNOSTIC")
    if int(incremental.get("raw_dead_letter_status_count") or 0) > 0:
        warnings.append("INCREMENTAL_RAW_DEAD_LETTER_HISTORY_PRESERVED")
    if int(incremental.get("queued_count") or 0) > 0:
        warnings.append("INCREMENTAL_QUEUE_ROWS_PRESERVED")
    if (
        int(incremental.get("queued_count") or 0)
        >= _DEFAULT_INCREMENTAL_BACKLOG_WARN_COUNT
    ):
        warnings.append("INCREMENTAL_QUEUE_BACKLOG_WARN")
    if int(incremental.get("stale_warn_count") or 0) > 0:
        warnings.append("INCREMENTAL_QUEUE_STALE_WARN")
    if pipeline.get("canonical_status") != "PASS":
        warnings.append("PIPELINE_CANONICAL_AUDIT_NON_PASS_PRESERVED")
    if lifecycle.get("canonical_status") not in {None, "PASS"}:
        warnings.append("LIFECYCLE_CANONICAL_AUDIT_NON_PASS_PRESERVED")

    evidence_failures = sorted(set(evidence_failures))
    blockers = sorted(set(blockers))
    warnings = sorted(set(warnings))
    evidence_status = "PASS" if not evidence_failures else "FAIL"
    fast0_status = "PASS" if not blockers and evidence_status == "PASS" else "BLOCKED"
    return {
        "status": fast0_status,
        "evidence_status": evidence_status,
        "fast0_status": fast0_status,
        "evidence_failures": evidence_failures,
        "qualification_blockers": blockers,
        "warnings": warnings,
        "database_files_unchanged": database.get("files_before") == database.get("files_after"),
        "raw_rows_recorded": False,
        "read_only": True,
        "no_order_side_effects": True,
        "no_trading_side_effects": True,
    }


def _read_database_identity(connection: sqlite3.Connection) -> dict[str, Any]:
    rows = connection.execute(
        """
        SELECT key, value
        FROM app_metadata
        WHERE key IN ('app_name', 'schema_version')
        ORDER BY key
        """
    ).fetchall()
    values: dict[str, list[str]] = {"app_name": [], "schema_version": []}
    for row in rows:
        key = str(row["key"])
        if key in values:
            values[key].append(str(row["value"]))
    app_name_valid = values["app_name"] == [APP_NAME]
    schema_version_valid = values["schema_version"] == [_EXPECTED_SCHEMA_VERSION]
    return {
        "app_name": APP_NAME if app_name_valid else None,
        "app_name_row_count": len(values["app_name"]),
        "app_name_value_valid": app_name_valid,
        "schema_version": _EXPECTED_SCHEMA_VERSION if schema_version_valid else None,
        "schema_version_row_count": len(values["schema_version"]),
        "schema_version_value_valid": schema_version_valid,
        "expected_app_name": APP_NAME,
        "expected_schema_version": _EXPECTED_SCHEMA_VERSION,
    }


def _validate_schema_manifest(connection: sqlite3.Connection) -> dict[str, Any]:
    objects: dict[str, dict[str, Any]] = {}
    for name, (expected_type, expected_sha256) in _SCHEMA_OBJECT_MANIFEST.items():
        row = connection.execute(
            "SELECT type, tbl_name, sql FROM sqlite_master WHERE name = ?",
            (name,),
        ).fetchone()
        actual_type = None if row is None else str(row["type"])
        actual_sha256 = None if row is None else _normalized_sql_sha256(row["sql"])
        actual_token_sha256 = None if row is None else _sql_token_sha256(row["sql"])
        expected_owner = name if expected_type == "table" else _SCHEMA_OBJECT_OWNER.get(name)
        actual_owner = None if row is None else str(row["tbl_name"])
        expected_quoted_sha256 = _SCHEMA_OBJECT_QUOTED_TOKEN_SHA256.get(name)
        actual_quoted_sha256 = None if row is None else _quoted_token_sha256(row["sql"])
        accepted_hashes = {
            expected_sha256,
            *_SCHEMA_OBJECT_ALTERNATE_SHA256.get(name, frozenset()),
        }
        accepted_token_hashes = {
            _SCHEMA_OBJECT_TOKEN_SHA256.get(name),
            *_SCHEMA_OBJECT_ALTERNATE_TOKEN_SHA256.get(name, frozenset()),
        }
        accepted_quoted_hashes = {
            expected_quoted_sha256,
            *_SCHEMA_OBJECT_ALTERNATE_QUOTED_TOKEN_SHA256.get(name, frozenset()),
        }
        objects[name] = {
            "present": row is not None,
            "type_valid": actual_type == expected_type,
            "owner_valid": expected_owner is not None and actual_owner == expected_owner,
            "sql_valid": actual_sha256 in accepted_hashes,
            "token_sql_valid": actual_token_sha256 in accepted_token_hashes,
            "quoted_tokens_valid": (
                expected_quoted_sha256 is None
                or actual_quoted_sha256 in accepted_quoted_hashes
            ),
        }
    invalid_objects = sorted(
        name
        for name, status in objects.items()
        if not all(bool(value) for value in status.values())
    )
    pipeline_ready = is_pipeline_coherency_disposition_schema_ready(connection)
    invalid_trigger_tables: list[str] = []
    for table_name, expected_triggers in _CRITICAL_TRIGGER_MANIFEST.items():
        actual_triggers = frozenset(
            str(row[0])
            for row in connection.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type = 'trigger' AND tbl_name = ?
                """,
                (table_name,),
            ).fetchall()
        )
        if actual_triggers != expected_triggers:
            invalid_trigger_tables.append(table_name)
    critical_trigger_sets_valid = not invalid_trigger_tables
    persistent_triggers = frozenset(
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'trigger'"
        ).fetchall()
    )
    persistent_trigger_set_valid = persistent_triggers == _EXPECTED_PERSISTENT_TRIGGERS
    temp_trigger_count = int(
        connection.execute(
            "SELECT COUNT(*) FROM sqlite_temp_master WHERE type = 'trigger'"
        ).fetchone()[0]
    )
    return {
        "expected_object_count": len(objects),
        "present_object_count": sum(item["present"] for item in objects.values()),
        "valid_object_count": sum(
            all(bool(value) for value in item.values()) for item in objects.values()
        ),
        "invalid_objects": invalid_objects,
        "critical_trigger_sets_valid": critical_trigger_sets_valid,
        "invalid_critical_trigger_table_count": len(invalid_trigger_tables),
        "invalid_critical_trigger_tables": sorted(invalid_trigger_tables),
        "persistent_trigger_set_valid": persistent_trigger_set_valid,
        "unexpected_persistent_trigger_count": len(
            persistent_triggers - _EXPECTED_PERSISTENT_TRIGGERS
        ),
        "missing_persistent_trigger_count": len(
            _EXPECTED_PERSISTENT_TRIGGERS - persistent_triggers
        ),
        "temp_trigger_count": temp_trigger_count,
        "pipeline_disposition_exact_schema_ready": pipeline_ready,
        "ready": bool(
            not invalid_objects
            and pipeline_ready
            and critical_trigger_sets_valid
            and persistent_trigger_set_valid
            and temp_trigger_count == 0
        ),
    }


def _validate_lineage_schema(connection: sqlite3.Connection) -> dict[str, Any]:
    table_ready: dict[str, bool] = {}
    for table_name, required in _LINEAGE_TABLE_COLUMNS.items():
        columns = {
            str(row["name"])
            for row in connection.execute(
                f"PRAGMA table_info({_quote_identifier(table_name)})"
            ).fetchall()
        }
        table_ready[table_name] = required <= columns
    return {
        "table_count": len(table_ready),
        "ready_table_count": sum(table_ready.values()),
        "tables": table_ready,
        "ready": all(table_ready.values()),
    }


def _collect_order_plan_status(connection: sqlite3.Connection) -> dict[str, Any]:
    table_exists = (
        connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'live_sim_intents'"
        ).fetchone()
        is not None
    )
    columns = (
        {
            str(row["name"])
            for row in connection.execute("PRAGMA table_info(live_sim_intents)").fetchall()
        }
        if table_exists
        else set()
    )
    column_exists = "order_plan_id" in columns
    audit: dict[str, Any]
    if table_exists and column_exists:
        row = connection.execute(
            """
            WITH base AS (
                SELECT
                    NULLIF(trim(CAST(order_plan_id AS TEXT)), '') AS column_id,
                    evidence_json,
                    json_valid(evidence_json) AS json_is_valid,
                    CASE WHEN json_valid(evidence_json) = 1
                         THEN json_type(evidence_json, '$') END AS root_type
                FROM live_sim_intents
            ), classified AS (
                SELECT *,
                    CASE WHEN root_type = 'object'
                         THEN json_type(evidence_json, '$.order_plan_id')
                    END AS evidence_value_type
                FROM base
            ), normalized AS (
                SELECT *,
                    CASE WHEN root_type = 'object' AND evidence_value_type = 'text'
                         THEN NULLIF(
                             trim(CAST(json_extract(evidence_json, '$.order_plan_id') AS TEXT)),
                             ''
                         )
                    END AS evidence_id,
                    CASE
                        WHEN json_is_valid != 1
                             AND instr(evidence_json, 'order_plan_id') > 0
                            THEN 'INVALID_ORDER_PLAN_ID'
                        WHEN json_is_valid != 1 OR root_type != 'object' THEN 'INVALID_JSON'
                        WHEN evidence_value_type IS NULL OR evidence_value_type = 'null' THEN NULL
                        WHEN evidence_value_type != 'text' THEN 'INVALID_ORDER_PLAN_ID'
                        ELSE NULL
                    END AS evidence_state
                FROM classified
            ), resolved AS (
                SELECT *, COALESCE(column_id, evidence_id) AS resolved_id
                FROM normalized
            ), duplicate_groups AS (
                SELECT resolved_id, COUNT(*) AS row_count
                FROM resolved
                WHERE resolved_id IS NOT NULL
                GROUP BY resolved_id
                HAVING COUNT(*) > 1
            )
            SELECT
                (SELECT COUNT(*) FROM resolved) AS intent_count,
                (SELECT COUNT(*) FROM resolved WHERE resolved_id IS NOT NULL)
                    AS order_plan_intent_count,
                (SELECT COUNT(*) FROM resolved WHERE column_id IS NOT NULL)
                    AS column_order_plan_id_count,
                (SELECT COUNT(*) FROM resolved WHERE evidence_id IS NOT NULL)
                    AS json_order_plan_id_count,
                (SELECT COUNT(*) FROM duplicate_groups) AS duplicate_group_count,
                (SELECT COALESCE(SUM(row_count), 0) FROM duplicate_groups)
                    AS duplicate_row_count,
                (SELECT COUNT(*) FROM resolved
                  WHERE column_id IS NOT NULL AND evidence_id IS NOT NULL
                    AND column_id != evidence_id) AS mismatch_count,
                (SELECT COUNT(*) FROM resolved
                  WHERE column_id IS NULL AND evidence_id IS NOT NULL)
                    AS missing_backfill_count,
                (SELECT COUNT(*) FROM resolved
                  WHERE column_id IS NOT NULL AND evidence_id IS NULL)
                    AS column_without_evidence_count,
                (SELECT COUNT(*) FROM resolved
                  WHERE evidence_state = 'INVALID_JSON') AS invalid_evidence_json_count,
                (SELECT COUNT(*) FROM resolved
                  WHERE evidence_state = 'INVALID_ORDER_PLAN_ID')
                    AS invalid_evidence_order_plan_id_count
            """
        ).fetchone()
        audit = {key: int(row[key] or 0) for key in row.keys()}
    else:
        audit = {
            "intent_count": 0,
            "order_plan_intent_count": 0,
            "column_order_plan_id_count": 0,
            "json_order_plan_id_count": 0,
            "duplicate_group_count": 0,
            "duplicate_row_count": 0,
            "mismatch_count": 0,
            "missing_backfill_count": 0,
            "column_without_evidence_count": 0,
            "invalid_evidence_json_count": 0,
            "invalid_evidence_order_plan_id_count": 0,
        }
    index_list = (
        connection.execute("PRAGMA index_list(live_sim_intents)").fetchall() if table_exists else []
    )
    index_row = next(
        (row for row in index_list if str(row["name"]) == "uq_live_sim_intents_order_plan_id"),
        None,
    )
    unique_index_exists = index_row is not None
    unique_index_is_unique = bool(index_row is not None and int(index_row["unique"]) == 1)
    unique_index_is_partial = bool(index_row is not None and int(index_row["partial"]) == 1)
    index_rows = connection.execute(
        "PRAGMA index_xinfo(uq_live_sim_intents_order_plan_id)"
    ).fetchall()
    key_rows = [row for row in index_rows if int(row["key"] or 0) == 1]
    key_columns = [str(row["name"]) for row in key_rows]
    descending = [bool(row["desc"]) for row in key_rows]
    schema_row = connection.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?",
        ("uq_live_sim_intents_order_plan_id",),
    ).fetchone()
    predicate_valid = bool(
        schema_row is not None
        and _sql_token_sha256(schema_row["sql"])
        == _SCHEMA_OBJECT_TOKEN_SHA256["uq_live_sim_intents_order_plan_id"]
    )
    exact_index_contract = bool(
        unique_index_exists
        and unique_index_is_unique
        and unique_index_is_partial
        and key_columns == ["order_plan_id"]
        and descending == [False]
        and predicate_valid
    )
    reason_codes: list[str] = []
    warning_codes: list[str] = []
    if not table_exists:
        reason_codes.append("LIVE_SIM_INTENTS_TABLE_MISSING")
    if not column_exists:
        reason_codes.append("ORDER_PLAN_ID_COLUMN_MISSING")
    if not exact_index_contract:
        reason_codes.append("ORDER_PLAN_UNIQUE_INDEX_CONTRACT_INVALID")
    for field, reason in (
        ("duplicate_group_count", "DUPLICATE_ORDER_PLAN_ID"),
        ("mismatch_count", "ORDER_PLAN_ID_EVIDENCE_MISMATCH"),
        ("missing_backfill_count", "ORDER_PLAN_ID_BACKFILL_MISSING"),
        ("invalid_evidence_order_plan_id_count", "ORDER_PLAN_ID_EVIDENCE_INVALID"),
    ):
        if int(audit[field]) > 0:
            reason_codes.append(reason)
    if int(audit["invalid_evidence_json_count"]) > 0:
        warning_codes.append("INVALID_EVIDENCE_JSON_WITHOUT_ORDER_PLAN_ID")
    if int(audit["column_without_evidence_count"]) > 0:
        warning_codes.append("ORDER_PLAN_ID_COLUMN_WITHOUT_EVIDENCE")
    status = "FAIL" if reason_codes else "WARN" if warning_codes else "PASS"
    return {
        "status": status,
        "reason_codes": sorted(set(reason_codes)),
        "warning_codes": sorted(set(warning_codes)),
        "table_exists": table_exists,
        "column_exists": column_exists,
        "unique_index_exists": unique_index_exists,
        "unique_index_is_unique": unique_index_is_unique,
        "unique_index_is_partial": unique_index_is_partial,
        **audit,
        "index_key_contract_valid": key_columns == ["order_plan_id"],
        "index_sort_contract_valid": descending == [False],
        "predicate_valid": predicate_valid,
        "exact_index_contract": exact_index_contract,
        "raw_rows_recorded": False,
    }


def _collect_broker_boundary_status(connection: sqlite3.Connection) -> dict[str, Any]:
    status = get_order_broker_boundary_status(connection)
    safe_keys = (
        "status",
        "raw_status",
        "effective_status",
        "fast_0_status",
        "reason_codes",
        "warning_codes",
        "table_exists",
        "required_indexes_present",
        "resolution_schema_ready",
        "resolution_source_schema_ready",
        "resolution_source_schema_reason_codes",
        "resolution_table_exists",
        "resolution_required_indexes_present",
        "resolution_append_only_triggers_present",
        "order_command_count",
        "active_order_command_count",
        "unknown_command_status_count",
        "expected_boundary_count",
        "boundary_count",
        "missing_boundary_count",
        "durable_pre_ack_count",
        "durable_pre_ack_gap_count",
        "duplicate_idempotency_count",
        "command_state_mismatch_count",
        "unknown_state_count",
        "orphan_boundary_count",
        "unexpected_boundary_count",
        "linked_command_type_invalid_count",
        "linked_command_type_mismatch_count",
        "invalid_command_type_count",
        "invalid_scope_count",
        "raw_unconfirmed_count",
        "effective_unconfirmed_count",
        "effective_resolution_count",
        "invalidated_resolution_count",
        "resolution_maintenance_fence_active_count",
        "invalid_resolution_chain_count",
        "resolution_event_count",
    )
    result = {key: status.get(key) for key in safe_keys}
    raw_states = _fixed_count_buckets(status.get("raw_state_counts"), _BROKER_RAW_STATES)
    effective_states = _fixed_count_buckets(
        status.get("effective_state_counts"),
        _BROKER_EFFECTIVE_STATES,
    )
    boundary_count = int(status.get("boundary_count") or 0)
    raw_state_conservation_valid = (
        sum(raw_states["counts"].values()) + int(raw_states["unknown_row_count"]) == boundary_count
    )
    effective_state_conservation_valid = (
        sum(effective_states["counts"].values()) + int(effective_states["unknown_row_count"])
        == boundary_count
    )
    semantic_contract_clear = bool(
        not status.get("reason_codes")
        and int(status.get("effective_unconfirmed_count") or 0) == 0
        and int(status.get("invalidated_resolution_count") or 0) == 0
        and int(status.get("active_order_command_count") or 0) == 0
        and not status.get("resolution_maintenance_fence_active")
    )
    return {
        **result,
        "raw_state_counts": raw_states["counts"],
        "effective_state_counts": effective_states["counts"],
        "raw_state_unknown_row_count": raw_states["unknown_row_count"],
        "raw_state_unknown_distinct_count": raw_states["unknown_distinct_count"],
        "effective_state_unknown_row_count": effective_states["unknown_row_count"],
        "effective_state_unknown_distinct_count": effective_states["unknown_distinct_count"],
        "state_bucket_contract_valid": bool(
            raw_states["contract_valid"]
            and effective_states["contract_valid"]
            and raw_state_conservation_valid
            and effective_state_conservation_valid
        ),
        "raw_state_conservation_valid": raw_state_conservation_valid,
        "effective_state_conservation_valid": effective_state_conservation_valid,
        "structure_ready": bool(
            status.get("table_exists")
            and status.get("required_indexes_present")
            and status.get("resolution_schema_ready")
            and status.get("resolution_source_schema_ready")
        ),
        "semantic_contract_clear": semantic_contract_clear,
        "raw_rows_recorded": False,
    }


def _collect_incremental_status(
    connection: sqlite3.Connection,
    *,
    retry_limit: int,
    backlog_warn_count: int,
    backlog_fail_count: int,
    stale_warn_sec: int,
    stale_fail_sec: int,
    observed_at: datetime,
) -> dict[str, Any]:
    effective = build_incremental_evaluation_dead_letter_effective_status(connection)
    raw_counts = connection.execute(
        """
        SELECT COUNT(*) AS total_count,
               SUM(CASE WHEN status = 'DEAD_LETTER' THEN 1 ELSE 0 END) AS dead_count,
               SUM(CASE WHEN status = 'RESET' THEN 1 ELSE 0 END) AS reset_count
        FROM incremental_evaluation_dead_letters
        """
    ).fetchone()
    stale_warn_cutoff = datetime_to_wire(observed_at - timedelta(seconds=max(stale_warn_sec, 1)))
    stale_fail_cutoff = datetime_to_wire(observed_at - timedelta(seconds=max(stale_fail_sec, 1)))
    queue = connection.execute(
        """
        SELECT COUNT(*) AS queued_count,
               SUM(CASE WHEN attempts >= ? THEN 1 ELSE 0 END) AS exhausted_count,
               SUM(CASE WHEN julianday(updated_at) <= julianday(?) THEN 1 ELSE 0 END)
                   AS stale_warn_count,
               SUM(CASE WHEN julianday(updated_at) <= julianday(?) THEN 1 ELSE 0 END)
                   AS stale_fail_count,
               SUM(CASE WHEN julianday(updated_at) IS NULL THEN 1 ELSE 0 END)
                   AS invalid_timestamp_count,
               MAX(attempts) AS max_attempts
        FROM incremental_evaluation_queue
        """,
        (retry_limit, stale_warn_cutoff, stale_fail_cutoff),
    ).fetchone()
    ledger_count = int(
        connection.execute(
            "SELECT COUNT(*) FROM incremental_evaluation_dead_letter_dispositions"
        ).fetchone()[0]
    )
    bucket_fields = (
        "active_unresolved_dead_letter_count",
        "historical_pending_disposition_count",
        "historical_disposed_dead_letter_count",
        "manual_review_dead_letter_count",
        "recovery_pending_count",
        "recovery_verified_count",
        "invalid_disposition_count",
    )
    bucket_counts = {key: int(effective.get(key) or 0) for key in bucket_fields}
    raw_dead_count = int(raw_counts["dead_count"] or 0)
    raw_total_count = int(raw_counts["total_count"] or 0)
    raw_reset_count = int(raw_counts["reset_count"] or 0)
    raw_unknown_status_count = max(raw_total_count - raw_dead_count - raw_reset_count, 0)
    queued_count = int(queue["queued_count"] or 0)
    retry_exhausted_count = int(queue["exhausted_count"] or 0)
    stale_warn_count = int(queue["stale_warn_count"] or 0)
    stale_fail_count = int(queue["stale_fail_count"] or 0)
    queue_reason_codes: list[str] = []
    queue_status = "PASS"
    if queued_count >= backlog_fail_count:
        queue_status = "FAIL"
        queue_reason_codes.append("INCREMENTAL_QUEUE_BACKLOG_FAIL")
    elif queued_count >= backlog_warn_count:
        queue_status = "WARN"
        queue_reason_codes.append("INCREMENTAL_QUEUE_BACKLOG_WARN")
    if stale_fail_count > 0:
        queue_status = "FAIL"
        queue_reason_codes.append("INCREMENTAL_QUEUE_STALE_FAIL")
    elif stale_warn_count > 0:
        if queue_status == "PASS":
            queue_status = "WARN"
        queue_reason_codes.append("INCREMENTAL_QUEUE_STALE_WARN")
    if retry_exhausted_count > 0:
        queue_status = "FAIL"
        queue_reason_codes.append("INCREMENTAL_QUEUE_RETRY_EXHAUSTED_ACTIVE")
    return {
        "raw_total_count": raw_total_count,
        "raw_dead_letter_status_count": raw_dead_count,
        "raw_legacy_reset_count": raw_reset_count,
        "raw_unknown_status_count": raw_unknown_status_count,
        "raw_status_contract_valid": raw_unknown_status_count == 0,
        "disposition_ledger_count": ledger_count,
        "queued_count": queued_count,
        "retry_exhausted_count": retry_exhausted_count,
        "stale_warn_count": stale_warn_count,
        "stale_fail_count": stale_fail_count,
        "invalid_queue_timestamp_count": int(queue["invalid_timestamp_count"] or 0),
        "queue_timestamp_contract_valid": int(queue["invalid_timestamp_count"] or 0) == 0,
        "max_attempts": int(queue["max_attempts"] or 0),
        "queue_status": queue_status,
        "queue_reason_codes": queue_reason_codes,
        "retry_limit": retry_limit,
        "thresholds": {
            "backlog_warn_count": backlog_warn_count,
            "backlog_fail_count": backlog_fail_count,
            "stale_warn_sec": stale_warn_sec,
            "stale_fail_sec": stale_fail_sec,
        },
        "effective_dead_letter_count": int(effective.get("effective_dead_letter_count") or 0),
        "bucket_counts": bucket_counts,
        "bucket_conservation_valid": sum(bucket_counts.values()) == raw_dead_count,
        "raw_status": effective.get("raw_status"),
        "effective_status": effective.get("effective_status"),
        "qualification_reason_codes": list(effective.get("reason_codes") or []),
        "schema_ready": effective.get("schema_ready") is True,
        "raw_rows_recorded": False,
    }


def _pipeline_global_contract_digest(page: Mapping[str, Any]) -> str:
    payload = {
        key: page.get(key) for key in _PIPELINE_GLOBAL_KEYS if key != "classification_counts"
    }
    payload["classification_counts"] = _fixed_count_buckets(
        page.get("classification_counts"),
        _PIPELINE_CLASSIFICATIONS,
    )
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _read_pipeline_all_pages(
    connection: sqlite3.Connection,
    *,
    max_age_sec: float,
    as_of: datetime,
) -> dict[str, Any]:
    first: dict[str, Any] | None = None
    page_contracts: list[dict[str, Any]] = []
    subject_digests: set[bytes] = set()
    collected_count = 0
    classifications: Counter[str] = Counter()
    effective_statuses = {status: 0 for status in _PIPELINE_DISPOSITION_STATUSES}
    unknown_effective_statuses: set[str] = set()
    unknown_effective_status_row_count = 0
    invalid_item_count = 0
    independent: Counter[str] = Counter()
    offset = 0
    while True:
        page = build_pipeline_coherency_rca_status(
            connection,
            trade_date=None,
            candidate_instance_id=None,
            max_age_sec=max_age_sec,
            limit=_PAGE_LIMIT,
            offset=offset,
            disposition_resolver=(
                lambda trade_date, subjects: resolve_pipeline_coherency_dispositions(
                    connection,
                    trade_date,
                    subjects,
                    as_of=as_of,
                )
            ),
            as_of=as_of,
        )
        if first is None:
            first = dict(page)
        items = page.get("items")
        if not isinstance(items, Sequence) or isinstance(items, (str, bytes)):
            raise Fast0StrictRequalificationError("PIPELINE_RCA_ITEMS_INVALID")
        for item in items:
            if not isinstance(item, Mapping):
                invalid_item_count += 1
                continue
            subject_id = item.get("candidate_instance_id")
            classification = str(item.get("classification") or "")
            if (
                not isinstance(subject_id, str)
                or not subject_id
                or classification not in _PIPELINE_CLASSIFICATIONS
            ):
                invalid_item_count += 1
                continue
            collected_count += 1
            subject_digests.add(hashlib.sha256(subject_id.encode("utf-8")).digest())
            classifications[classification] += 1
            effective_disposition = _mapping(item.get("effective_disposition"))
            effective = effective_disposition.get("effective") is True
            effective_status = str(effective_disposition.get("status") or "MISSING")
            if effective_status in effective_statuses:
                effective_statuses[effective_status] += 1
            else:
                unknown_effective_status_row_count += 1
                unknown_effective_statuses.add(effective_status)
            if item.get("manual_review_required") is True:
                independent["manual_review_count"] += 1
                if not effective:
                    independent["manual_review_pending_count"] += 1
            if item.get("current_source_drift") is True:
                independent["current_source_drift_count"] += 1
                if not effective:
                    independent["current_source_drift_pending_count"] += 1
            if item.get("current_source_drift") is None:
                independent["current_source_drift_unknown_count"] += 1
                if not effective:
                    independent["current_source_drift_unknown_pending_count"] += 1
            if item.get("disposition_required") is True:
                independent["disposition_required_count"] += 1
                if not effective:
                    independent["disposition_pending_count"] += 1
            if item.get("active_recovery_required") is True:
                independent["active_current_non_pass_count"] += 1
            if item.get("latest_plan_unexpired") is True:
                independent["unexpired_plan_count"] += 1
                if str(item.get("latest_plan_status") or "").upper() == "PLAN_READY":
                    independent["unexpired_plan_ready_count"] += 1
            if (
                item.get("latest_plan_present") is True
                and item.get("latest_plan_unexpired") is None
            ):
                independent["plan_expiry_unknown_count"] += 1
        page_contracts.append(
            {
                "offset": page.get("offset"),
                "returned_count": page.get("returned_count"),
                "item_count": len(items),
                "full_count": page.get("full_count"),
                "has_more": page.get("has_more"),
                "next_offset": page.get("next_offset"),
                "inventory_count_consistent": page.get("inventory_count_consistent"),
                "inventory_digest": page.get("inventory_digest"),
                "inventory_end_digest": page.get("inventory_end_digest"),
                "qualification_status": page.get("qualification_status"),
                "global_contract_digest": _pipeline_global_contract_digest(page),
            }
        )
        if not bool(page.get("has_more")):
            break
        next_offset = page.get("next_offset")
        if (
            not isinstance(next_offset, int)
            or isinstance(next_offset, bool)
            or next_offset <= offset
        ):
            raise Fast0StrictRequalificationError("PIPELINE_RCA_PAGINATION_STALLED")
        offset = next_offset
    if first is None:
        raise Fast0StrictRequalificationError("PIPELINE_RCA_PAGE_MISSING")
    first_classifications = _fixed_count_buckets(
        first.get("classification_counts"),
        _PIPELINE_CLASSIFICATIONS,
    )
    first_contract = {
        key: first.get(key) for key in _PIPELINE_GLOBAL_KEYS if key != "classification_counts"
    }
    first_contract["classification_counts"] = first_classifications["counts"]
    expected_offset = 0
    first_global_digest = _pipeline_global_contract_digest(first)
    page_contract_consistent = True
    for page_index, page_contract in enumerate(page_contracts):
        returned_count = page_contract.get("returned_count")
        full_count = page_contract.get("full_count")
        if (
            type(returned_count) is not int
            or returned_count < 0
            or returned_count > _PAGE_LIMIT
            or type(full_count) is not int
            or full_count < 0
            or page_contract.get("offset") != expected_offset
            or page_contract.get("item_count") != returned_count
            or page_contract.get("inventory_count_consistent") is not True
            or page_contract.get("global_contract_digest") != first_global_digest
        ):
            page_contract_consistent = False
            break
        expected_offset += returned_count
        expected_has_more = expected_offset < full_count
        if page_contract.get("has_more") is not expected_has_more:
            page_contract_consistent = False
            break
        expected_next = expected_offset if expected_has_more else None
        if page_contract.get("next_offset") != expected_next:
            page_contract_consistent = False
            break
        if page_index < len(page_contracts) - 1 and not expected_has_more:
            page_contract_consistent = False
            break
    if expected_offset != int(first.get("full_count") or 0):
        page_contract_consistent = False
    independent_counts = {
        key: int(independent[key])
        for key in (
            "manual_review_count",
            "manual_review_pending_count",
            "current_source_drift_count",
            "current_source_drift_pending_count",
            "current_source_drift_unknown_count",
            "current_source_drift_unknown_pending_count",
            "disposition_required_count",
            "disposition_pending_count",
            "active_current_non_pass_count",
            "unexpired_plan_count",
            "unexpired_plan_ready_count",
            "plan_expiry_unknown_count",
        )
    }
    comparable_keys = (
        "manual_review_count",
        "manual_review_pending_count",
        "current_source_drift_count",
        "current_source_drift_pending_count",
        "current_source_drift_unknown_count",
        "current_source_drift_unknown_pending_count",
        "disposition_required_count",
        "disposition_pending_count",
        "plan_expiry_unknown_count",
    )
    aggregate_contract_consistent = bool(
        all(
            int(classifications[key]) == int(first_classifications["counts"][key])
            for key in _PIPELINE_CLASSIFICATIONS
        )
        and first_classifications["contract_valid"] is True
        and unknown_effective_status_row_count == 0
        and all(independent_counts[key] == int(first.get(key) or 0) for key in comparable_keys)
    )
    qualification_has_independent_blocker = any(
        independent_counts[key] > 0
        for key in (
            "manual_review_pending_count",
            "current_source_drift_pending_count",
            "current_source_drift_unknown_pending_count",
            "disposition_pending_count",
            "active_current_non_pass_count",
            "unexpired_plan_ready_count",
            "plan_expiry_unknown_count",
        )
    )
    if first.get("qualification_status") == "PASS" and qualification_has_independent_blocker:
        aggregate_contract_consistent = False
    return {
        **first_contract,
        "schema_ready": is_pipeline_coherency_disposition_schema_ready(connection),
        "collected_count": collected_count,
        "unique_subject_count": len(subject_digests),
        "invalid_item_count": invalid_item_count,
        "collected_classification_counts": {
            key: int(classifications[key]) for key in _PIPELINE_CLASSIFICATIONS
        },
        "effective_disposition_status_counts": effective_statuses,
        "unknown_effective_disposition_status_row_count": (unknown_effective_status_row_count),
        "unknown_effective_disposition_status_distinct_count": len(unknown_effective_statuses),
        "classification_bucket_contract_valid": first_classifications["contract_valid"],
        "disposition_bucket_contract_valid": unknown_effective_status_row_count == 0,
        "independent_counts": independent_counts,
        "aggregate_contract_consistent": aggregate_contract_consistent,
        "page_contract_consistent": page_contract_consistent,
        "page_count": len(page_contracts),
        "pages": page_contracts,
        "raw_rows_recorded": False,
    }


def _sanitize_lifecycle_report(value: Mapping[str, Any]) -> dict[str, Any]:
    source = dict(value)
    known_input_keys = frozenset(
        {
            *_LIFECYCLE_SAFE_SCALAR_KEYS,
            "qualification_reason_codes",
            "canonical_reason_codes",
            "canonical",
            "reconcile",
            "classification_counts",
            "pages",
            "code_filter_diagnostic_only",
        }
    )
    result = {key: source.get(key) for key in _LIFECYCLE_SAFE_SCALAR_KEYS}
    classifications = _fixed_count_buckets(
        source.get("classification_counts"),
        _LIFECYCLE_CLASSIFICATIONS,
    )
    result["classification_counts"] = classifications["counts"]
    result["classification_unknown_row_count"] = classifications["unknown_row_count"]
    result["classification_unknown_distinct_count"] = classifications["unknown_distinct_count"]
    result["classification_bucket_contract_valid"] = classifications["contract_valid"]
    qualification_reasons, qualification_reasons_valid = _fixed_reason_codes(
        source.get("qualification_reason_codes"),
        allowed=_LIFECYCLE_QUALIFICATION_REASONS,
    )
    canonical_reasons, canonical_reasons_valid = _fixed_reason_codes(
        source.get("canonical_reason_codes"),
        allowed=_LIFECYCLE_CANONICAL_REASONS,
    )
    result["qualification_reason_codes"] = qualification_reasons
    result["canonical_reason_codes"] = canonical_reasons
    result["reason_code_contract_valid"] = bool(
        qualification_reasons_valid and canonical_reasons_valid
    )
    raw_pages = source.get("pages")
    sanitized_pages: list[dict[str, Any]] = []
    page_contract_valid = bool(
        isinstance(raw_pages, list)
        and all(
            isinstance(page, Mapping)
            and frozenset(page) == frozenset(_LIFECYCLE_SAFE_PAGE_KEYS)
            for page in raw_pages
        )
    )
    if isinstance(raw_pages, list):
        for raw_page in raw_pages:
            if isinstance(raw_page, Mapping):
                sanitized_pages.append(
                    {key: raw_page.get(key) for key in _LIFECYCLE_SAFE_PAGE_KEYS}
                )
    result["pages"] = sanitized_pages
    result["privacy_contract_valid"] = bool(
        frozenset(source) == known_input_keys
        and page_contract_valid
        and result["reason_code_contract_valid"] is True
    )
    if classifications["contract_valid"] is not True:
        result["item_classification_counts_consistent"] = False
    return result


def _fixed_reason_codes(
    value: Any,
    *,
    allowed: frozenset[str],
) -> tuple[list[str], bool]:
    if not isinstance(value, list):
        return [], False
    safe = [item for item in value if isinstance(item, str) and item in allowed]
    valid = len(safe) == len(value) and len(safe) == len(set(safe))
    return safe, valid


def _collect_projection_status(
    connection: sqlite3.Connection,
    *,
    stale_processing_sec: int,
    observed_at: datetime,
) -> dict[str, Any]:
    outbox_total_direct = int(
        connection.execute("SELECT COUNT(*) FROM projection_outbox").fetchone()[0]
    )
    outbox_rows = connection.execute(
        """
        SELECT status, COUNT(*) AS count
        FROM projection_outbox
        GROUP BY status
        """
    ).fetchall()
    outbox_counts = {status: 0 for status in _OUTBOX_STATUSES}
    unknown_outbox = 0
    for row in outbox_rows:
        status = str(row["status"])
        if status in outbox_counts:
            outbox_counts[status] = int(row["count"])
        else:
            unknown_outbox += int(row["count"])
    event_rows = connection.execute(
        """
        SELECT status, COUNT(*) AS count
        FROM projection_event_results
        GROUP BY status
        """
    ).fetchall()
    event_total_direct = int(
        connection.execute("SELECT COUNT(*) FROM projection_event_results").fetchone()[0]
    )
    event_counts = {"SUCCESS": 0, "ERROR": 0}
    unknown_event = 0
    for row in event_rows:
        status = str(row["status"])
        if status in event_counts:
            event_counts[status] = int(row["count"])
        else:
            unknown_event += int(row["count"])
    stale_cutoff = datetime_to_wire(observed_at - timedelta(seconds=max(stale_processing_sec, 0)))
    recent_cutoff = datetime_to_wire(
        observed_at - timedelta(seconds=_POLICY_PROJECTION_RECENT_WINDOW_SEC)
    )
    blocking_cutoff = datetime_to_wire(
        observed_at - timedelta(seconds=_POLICY_PROJECTION_BLOCKING_OLDER_THAN_SEC)
    )
    observed_at_wire = datetime_to_wire(observed_at)
    quality = connection.execute(
        """
        SELECT
          (SELECT COUNT(*)
             FROM projection_outbox
             WHERE status = 'PROCESSING'
               AND locked_at IS NOT NULL
               AND julianday(locked_at) <= julianday(?)) AS stale_processing_count,
          (SELECT COUNT(*) FROM projection_outbox
           WHERE (status = 'PROCESSING' AND (
                    locked_at IS NULL OR locked_by IS NULL OR trim(locked_by) = ''))
              OR (
                  status != 'PROCESSING'
                  AND (locked_at IS NOT NULL OR locked_by IS NOT NULL)
              )) AS lock_state_invalid_count,
          (SELECT COUNT(*) FROM projection_outbox
            WHERE CASE
                    WHEN json_valid(metadata_json) != 1 THEN 1
                    WHEN json_type(metadata_json, '$') != 'object' THEN 1
                    ELSE 0
                  END = 1) AS outbox_invalid_metadata_count,
          (SELECT COUNT(*) FROM projection_event_results
            WHERE CASE
                    WHEN json_valid(metadata_json) != 1 THEN 1
                    WHEN json_type(metadata_json, '$') != 'object' THEN 1
                    ELSE 0
                  END = 1) AS event_invalid_metadata_count,
          (SELECT COUNT(*) FROM projection_watermarks
            WHERE CASE
                    WHEN json_valid(metadata_json) != 1 THEN 1
                    WHEN json_type(metadata_json, '$') != 'object' THEN 1
                    ELSE 0
                  END = 1) AS watermark_invalid_metadata_count,
          (SELECT COUNT(*)
             FROM projection_outbox po
             LEFT JOIN projection_event_results per
               ON per.projection_name = po.projection_name AND per.event_id = po.event_id
            WHERE po.status = 'APPLIED'
              AND COALESCE(per.status, '') != 'SUCCESS'
          ) AS applied_without_success_count,
          (SELECT COUNT(*)
             FROM projection_outbox po
             JOIN gateway_events ge ON ge.event_id = po.event_id
            WHERE po.event_rowid IS NOT NULL
              AND po.event_rowid != ge.rowid
          ) AS outbox_event_rowid_mismatch_count,
          (SELECT COUNT(*)
             FROM projection_event_results per
             JOIN gateway_events ge ON ge.event_id = per.event_id
            WHERE per.event_rowid != ge.rowid) AS result_event_rowid_mismatch_count
        """,
        (stale_cutoff,),
    ).fetchone()
    pending_quality = connection.execute(
        """
        SELECT
          SUM(CASE WHEN status = 'PENDING'
                    AND julianday(COALESCE(available_at, created_at)) >= julianday(?)
                   THEN 1 ELSE 0 END) AS recent_pending_count,
          SUM(CASE WHEN status = 'PENDING'
                    AND julianday(COALESCE(available_at, created_at)) <= julianday(?)
                   THEN 1 ELSE 0 END) AS eligible_pending_count,
          SUM(CASE WHEN status = 'PENDING' AND event_type = 'condition_event'
                   THEN 1 ELSE 0 END) AS condition_event_pending_count,
          SUM(CASE WHEN status = 'PENDING' AND event_type = 'condition_event'
                    AND julianday(COALESCE(available_at, created_at)) >= julianday(?)
                   THEN 1 ELSE 0 END) AS condition_event_recent_pending_count,
          SUM(CASE WHEN status = 'PENDING'
                    AND julianday(COALESCE(available_at, created_at)) IS NULL
                   THEN 1 ELSE 0 END) AS invalid_pending_timestamp_count
        FROM projection_outbox
        """,
        (recent_cutoff, observed_at_wire, recent_cutoff),
    ).fetchone()
    pending_gateway_payload_invalid_count = int(
        connection.execute(
            """
            SELECT COUNT(*)
            FROM projection_outbox AS o
            JOIN gateway_events AS g ON g.event_id = o.event_id
            WHERE o.status = 'PENDING'
              AND CASE
                    WHEN json_valid(g.payload_json) != 1 THEN 1
                    WHEN json_type(g.payload_json, '$') != 'object' THEN 1
                    ELSE 0
                  END = 1
            """
        ).fetchone()[0]
    )
    latest_reconcile_row = connection.execute(
        """
        SELECT status
        FROM market_data_projection_reconcile_runs
        ORDER BY created_at DESC, run_id DESC
        LIMIT 1
        """
    ).fetchone()
    raw_latest_reconcile_status = (
        None if latest_reconcile_row is None else str(latest_reconcile_row["status"])
    )
    latest_reconcile_status_contract_valid = latest_reconcile_row is None or (
        raw_latest_reconcile_status in {"PASS", "WARN", "FAIL"}
    )
    latest_reconcile_status = (
        raw_latest_reconcile_status if latest_reconcile_status_contract_valid else None
    )
    routing_counts = connection.execute(
        """
        SELECT
          SUM(CASE WHEN effective_skip_inline = 1
                    AND event_type = 'condition_event'
                   THEN 1 ELSE 0 END) AS condition_event_effective_skip_count,
          SUM(CASE WHEN effective_skip_inline = 1
                    AND event_type NOT IN ('price_tick', 'tr_response', 'condition_event')
                   THEN 1 ELSE 0 END) AS invalid_effective_skip_count
        FROM market_data_projection_routing_decisions
        """
    ).fetchone()
    condition_event_effective_skip_count = int(
        routing_counts["condition_event_effective_skip_count"] or 0
    )
    invalid_effective_skip_count = int(routing_counts["invalid_effective_skip_count"] or 0)
    live_ingest_detected = (
        connection.execute(
            """
            SELECT 1
            FROM gateway_events
            WHERE julianday(received_at) >= julianday(?)
            LIMIT 1
            """,
            (recent_cutoff,),
        ).fetchone()
        is not None
    )
    total_pending_count = outbox_counts["PENDING"]
    condition_event_pending_count = int(pending_quality["condition_event_pending_count"] or 0)
    if pending_gateway_payload_invalid_count == 0:
        retire_counts = _fast_bulk_retire_counts(
            connection,
            older_than=blocking_cutoff,
            exclude_recent_condition_events=True,
        )
        condition_event_eligible_count = _count_condition_event_bulk_retire_eligible_fast(
            connection,
            recent_cutoff=blocking_cutoff,
            exclude_recent_condition_events=True,
        )
        effective_skip_pending_count = _count_effective_skip_pending(connection)
        # Cross-check the direct condition inventory used by the producer helper.
        condition_inventory_consistent = (
            _count_pending_condition_events(connection) == condition_event_pending_count
        )
        bulk_retire_eligible_count = int(retire_counts["bulk_retire_eligible_count"])
    else:
        # The producer classifier calls JSON functions on gateway payloads.  If
        # any input is malformed, fail closed without executing that classifier.
        condition_event_eligible_count = 0
        effective_skip_pending_count = 0
        condition_inventory_consistent = False
        bulk_retire_eligible_count = 0
    blocking_pending_count = max(total_pending_count - bulk_retire_eligible_count, 0)
    condition_event_blocking_pending_count = max(
        condition_event_pending_count - condition_event_eligible_count,
        0,
    )
    recent_pending_count = int(pending_quality["recent_pending_count"] or 0)
    condition_event_recent_pending_count = int(
        pending_quality["condition_event_recent_pending_count"] or 0
    )
    readiness_reasons: list[str] = []
    if outbox_counts["ERROR"] > 0:
        readiness_reasons.append("PROJECTION_OUTBOX_ERROR")
    if outbox_counts["DEAD_LETTER"] > 0:
        readiness_reasons.append("PROJECTION_OUTBOX_DEAD_LETTER")
    if int(quality["stale_processing_count"] or 0) > 0:
        readiness_reasons.append("STALE_OUTBOX_PROCESSING")
    if recent_pending_count > _POLICY_PROJECTION_RECENT_FAIL_COUNT:
        readiness_reasons.append("RECENT_OUTBOX_BACKLOG")
    if blocking_pending_count >= _POLICY_PROJECTION_BACKLOG_FAIL_PENDING_COUNT:
        readiness_reasons.append("OUTBOX_BLOCKING_PENDING_FAIL_THRESHOLD")
    if condition_event_blocking_pending_count > _POLICY_PROJECTION_CONDITION_MAX_PENDING:
        readiness_reasons.append("CONDITION_EVENT_BLOCKING_OUTBOX_BACKLOG")
    if condition_event_recent_pending_count > _POLICY_PROJECTION_CONDITION_RECENT_MAX_PENDING:
        readiness_reasons.append("CONDITION_EVENT_RECENT_OUTBOX_BACKLOG")
    if latest_reconcile_row is None:
        readiness_reasons.append("LATEST_RECONCILE_MISSING")
    elif latest_reconcile_status != "PASS":
        readiness_reasons.append("LATEST_RECONCILE_NOT_PASS")
    if invalid_effective_skip_count > 0:
        readiness_reasons.append("INVALID_EFFECTIVE_SKIP_EVENT_TYPE")
    readiness_warnings: list[str] = []
    if not readiness_reasons:
        if blocking_pending_count >= _POLICY_PROJECTION_BACKLOG_WARN_PENDING_COUNT:
            readiness_warnings.append("OUTBOX_BLOCKING_BACKLOG_DRAIN_RECOMMENDED")
        elif total_pending_count >= _POLICY_PROJECTION_BACKLOG_WARN_PENDING_COUNT:
            readiness_warnings.append("NON_BLOCKING_SHADOW_BACKLOG_BULK_RETIRE_RECOMMENDED")
        if effective_skip_pending_count > 0:
            readiness_warnings.append("EFFECTIVE_SKIP_PENDING_WITHIN_SLA")
        if live_ingest_detected and recent_pending_count > 0:
            readiness_warnings.append("LIVE_INGEST_PENDING_BACKLOG")
        if latest_reconcile_status == "PASS" and total_pending_count > 0:
            readiness_warnings.append("LATEST_RECONCILE_PASS_WITH_BACKLOG")
    readiness_status = "FAIL" if readiness_reasons else "WARN" if readiness_warnings else "PASS"
    aggregate_contract_valid = bool(
        unknown_outbox == 0
        and unknown_event == 0
        and sum(outbox_counts.values()) + unknown_outbox == outbox_total_direct
        and sum(event_counts.values()) + unknown_event == event_total_direct
        and int(quality["lock_state_invalid_count"] or 0) == 0
        and int(quality["outbox_invalid_metadata_count"] or 0) == 0
        and int(quality["event_invalid_metadata_count"] or 0) == 0
        and int(quality["watermark_invalid_metadata_count"] or 0) == 0
        and int(quality["outbox_event_rowid_mismatch_count"] or 0) == 0
        and int(quality["result_event_rowid_mismatch_count"] or 0) == 0
        and int(pending_quality["invalid_pending_timestamp_count"] or 0) == 0
        and pending_gateway_payload_invalid_count == 0
        and condition_inventory_consistent
        and latest_reconcile_status_contract_valid
    )
    return {
        "schema_ready": _schema_objects_valid(
            connection,
            (
                "projection_outbox",
                "projection_watermarks",
                "projection_event_results",
                "market_data_projection_reconcile_runs",
                "market_data_projection_routing_decisions",
            ),
        ),
        "outbox_total_count": outbox_total_direct,
        "outbox_status_conservation_valid": (
            sum(outbox_counts.values()) + unknown_outbox == outbox_total_direct
        ),
        "outbox_status_counts": outbox_counts,
        "outbox_pending_count": outbox_counts["PENDING"],
        "outbox_processing_count": outbox_counts["PROCESSING"],
        "outbox_error_count": outbox_counts["ERROR"],
        "outbox_dead_letter_count": outbox_counts["DEAD_LETTER"],
        "unknown_outbox_status_count": unknown_outbox,
        "event_result_status_counts": event_counts,
        "event_result_total_count": event_total_direct,
        "event_result_status_conservation_valid": (
            sum(event_counts.values()) + unknown_event == event_total_direct
        ),
        "event_result_error_count": event_counts["ERROR"],
        "unknown_event_result_status_count": unknown_event,
        "stale_processing_count": int(quality["stale_processing_count"] or 0),
        "recent_pending_count": recent_pending_count,
        "eligible_pending_count": int(pending_quality["eligible_pending_count"] or 0),
        "blocking_pending_count": blocking_pending_count,
        "non_blocking_shadow_pending_count": bulk_retire_eligible_count,
        "bulk_retire_eligible_count": bulk_retire_eligible_count,
        "condition_event_pending_count": condition_event_pending_count,
        "condition_event_recent_pending_count": condition_event_recent_pending_count,
        "condition_event_blocking_pending_count": condition_event_blocking_pending_count,
        "effective_skip_pending_count": effective_skip_pending_count,
        "live_ingest_detected": live_ingest_detected,
        "latest_reconcile_present": latest_reconcile_row is not None,
        "latest_reconcile_status": latest_reconcile_status,
        "latest_reconcile_unknown_status_count": int(
            latest_reconcile_row is not None
            and latest_reconcile_status_contract_valid is not True
        ),
        "latest_reconcile_status_contract_valid": (
            latest_reconcile_status_contract_valid
        ),
        "condition_event_effective_skip_count": condition_event_effective_skip_count,
        "invalid_effective_skip_count": invalid_effective_skip_count,
        "invalid_pending_timestamp_count": int(
            pending_quality["invalid_pending_timestamp_count"] or 0
        ),
        "pending_gateway_payload_invalid_count": pending_gateway_payload_invalid_count,
        "readiness_status": readiness_status,
        "readiness_reason_codes": sorted(readiness_reasons or readiness_warnings),
        "thresholds": {
            "stale_processing_sec": stale_processing_sec,
            "blocking_older_than_sec": _POLICY_PROJECTION_BLOCKING_OLDER_THAN_SEC,
            "backlog_warn_pending_count": _POLICY_PROJECTION_BACKLOG_WARN_PENDING_COUNT,
            "backlog_fail_pending_count": _POLICY_PROJECTION_BACKLOG_FAIL_PENDING_COUNT,
            "recent_window_sec": _POLICY_PROJECTION_RECENT_WINDOW_SEC,
            "recent_fail_count": _POLICY_PROJECTION_RECENT_FAIL_COUNT,
            "condition_max_pending": _POLICY_PROJECTION_CONDITION_MAX_PENDING,
            "condition_recent_max_pending": (_POLICY_PROJECTION_CONDITION_RECENT_MAX_PENDING),
        },
        "lock_state_invalid_count": int(quality["lock_state_invalid_count"] or 0),
        "outbox_invalid_metadata_count": int(quality["outbox_invalid_metadata_count"] or 0),
        "event_invalid_metadata_count": int(quality["event_invalid_metadata_count"] or 0),
        "watermark_invalid_metadata_count": int(quality["watermark_invalid_metadata_count"] or 0),
        "applied_without_success_count": int(quality["applied_without_success_count"] or 0),
        "outbox_event_rowid_mismatch_count": int(quality["outbox_event_rowid_mismatch_count"] or 0),
        "result_event_rowid_mismatch_count": int(quality["result_event_rowid_mismatch_count"] or 0),
        "aggregate_contract_valid": aggregate_contract_valid,
        "raw_rows_recorded": False,
    }


def _collect_runtime_status(
    connection: sqlite3.Connection,
    *,
    observed_at: datetime,
) -> dict[str, Any]:
    now = datetime_to_wire(observed_at)
    counts = connection.execute(
        """
        SELECT COUNT(*) AS lock_count,
               SUM(CASE
                       WHEN julianday(expires_at) > julianday(?) THEN 1
                       ELSE 0
                   END) AS active_count,
               SUM(CASE
                       WHEN julianday(expires_at) <= julianday(?) THEN 1
                       ELSE 0
                   END) AS expired_count,
               SUM(CASE WHEN julianday(expires_at) IS NULL
                         OR julianday(acquired_at) IS NULL
                         OR (heartbeat_at IS NOT NULL AND julianday(heartbeat_at) IS NULL)
                        THEN 1 ELSE 0 END) AS invalid_timestamp_count,
               SUM(CASE WHEN json_valid(detail_json) != 1 THEN 1 ELSE 0 END) AS invalid_json_count,
               SUM(CASE WHEN process_id <= 0 OR thread_id <= 0 OR fencing_token <= 0
                        THEN 1 ELSE 0 END) AS invalid_owner_contract_count
        FROM runtime_execution_locks
        """,
        (now, now),
    ).fetchone()
    fence = connection.execute(
        """
        SELECT
          (SELECT COUNT(*) FROM runtime_execution_lock_fences) AS fence_count,
          (SELECT COUNT(*) FROM runtime_execution_lock_fences
            WHERE last_fencing_token <= 0) AS nonpositive_fence_count,
          (SELECT COUNT(*)
             FROM runtime_execution_locks l
             LEFT JOIN runtime_execution_lock_fences f ON f.lock_name = l.lock_name
            WHERE f.lock_name IS NULL) AS missing_fence_count,
          (SELECT COUNT(*)
             FROM runtime_execution_locks l
             JOIN runtime_execution_lock_fences f ON f.lock_name = l.lock_name
            WHERE l.fencing_token != f.last_fencing_token) AS fence_mismatch_count
        """
    ).fetchone()
    fencing_contract_valid = all(
        int(fence[key] or 0) == 0
        for key in (
            "nonpositive_fence_count",
            "missing_fence_count",
            "fence_mismatch_count",
        )
    ) and all(
        int(counts[key] or 0) == 0
        for key in (
            "invalid_timestamp_count",
            "invalid_json_count",
            "invalid_owner_contract_count",
        )
    )
    return {
        "schema_ready": _schema_objects_valid(
            connection,
            ("runtime_execution_locks", "runtime_execution_lock_fences"),
        ),
        "lock_count": int(counts["lock_count"] or 0),
        "active_count": int(counts["active_count"] or 0),
        "expired_count": int(counts["expired_count"] or 0),
        "invalid_timestamp_count": int(counts["invalid_timestamp_count"] or 0),
        "invalid_json_count": int(counts["invalid_json_count"] or 0),
        "invalid_owner_contract_count": int(counts["invalid_owner_contract_count"] or 0),
        "fence_count": int(fence["fence_count"] or 0),
        "nonpositive_fence_count": int(fence["nonpositive_fence_count"] or 0),
        "missing_fence_count": int(fence["missing_fence_count"] or 0),
        "fence_mismatch_count": int(fence["fence_mismatch_count"] or 0),
        "fencing_contract_valid": fencing_contract_valid,
        "owner_liveness_probed": False,
        "raw_rows_recorded": False,
    }


def _scan_database_markers(connection: sqlite3.Connection) -> dict[str, Any]:
    marker_counts = {marker: 0 for marker in _RUNTIME_MARKERS}
    scanned_column_count = 0
    for table_name, columns in _DB_MARKER_COLUMNS.items():
        actual_columns = {
            str(row["name"])
            for row in connection.execute(
                f"PRAGMA table_info({_quote_identifier(table_name)})"
            ).fetchall()
        }
        for column in columns:
            if column not in actual_columns:
                return {
                    "scan_complete": False,
                    "scanned_column_count": scanned_column_count,
                    "marker_counts": marker_counts,
                    "raw_values_recorded": False,
                }
            scanned_column_count += 1
            for marker in _RUNTIME_MARKERS:
                count = connection.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM {_quote_identifier(table_name)}
                    WHERE instr(COALESCE({_quote_identifier(column)}, ''), ?) > 0
                    """,
                    (marker,),
                ).fetchone()[0]
                marker_counts[marker] += int(count or 0)
    return {
        "scan_complete": True,
        "scanned_column_count": scanned_column_count,
        "marker_counts": marker_counts,
        "raw_values_recorded": False,
    }


def _collect_modify_order_counts(connection: sqlite3.Connection) -> dict[str, int]:
    return {
        "gateway_commands_count": int(
            connection.execute(
                """
                SELECT COUNT(*) FROM gateway_commands
                WHERE lower(trim(command_type)) = 'modify_order'
                """
            ).fetchone()[0]
        ),
        "gateway_command_dedupe_keys_count": int(
            connection.execute(
                """
                SELECT COUNT(*) FROM gateway_command_dedupe_keys
                WHERE lower(trim(command_type)) = 'modify_order'
                """
            ).fetchone()[0]
        ),
        "gateway_order_broker_boundaries_count": int(
            connection.execute(
                """
                SELECT COUNT(*) FROM gateway_order_broker_boundaries
                WHERE lower(trim(command_type)) = 'modify_order'
                """
            ).fetchone()[0]
        ),
    }


def _read_migration_baseline(
    path: Path,
    *,
    expected_report_sha256: str,
    current_path: Path,
    current_main: Mapping[str, Any],
) -> dict[str, Any]:
    payload, report_sha256 = _read_stable_json(
        path,
        missing_reason="MIGRATION_REPORT_NOT_FOUND",
        invalid_reason="MIGRATION_REPORT_INVALID",
    )
    source = _mapping(payload.get("source"))
    migration = _mapping(payload.get("migration"))
    policy = _mapping(payload.get("evidence_policy"))
    safety = _mapping(payload.get("safety"))
    backup = _mapping(payload.get("backup"))
    counts = _mapping(payload.get("counts"))
    preflight = _mapping(payload.get("preflight"))
    migration_database = _mapping(source.get("post_main"))
    exclusive_database = _mapping(source.get("exclusive_post_commit_main"))
    preflight_main = _mapping(source.get("preflight_main"))
    migration_database_size = migration_database.get("size")
    existing_table_count = counts.get("existing_table_count")
    current_path_matches = _same_resolved_path(source.get("path"), current_path)
    policy_safe = bool(
        policy.get("raw_rows_included") is False
        and policy.get("secrets_included") is False
        and policy.get("account_numbers_included") is False
        and policy.get("hashes_and_counts_only") is True
    )
    producer_contract_valid = bool(
        payload.get("status") == "PASS"
        and payload.get("committed") is True
        and report_sha256 == expected_report_sha256
        and migration.get("source_schema") == _MIGRATION_SOURCE_SCHEMA
        and migration.get("target_schema") == _EXPECTED_SCHEMA_VERSION
        and migration.get("exact_function") == _MIGRATION_APPLY_FUNCTION
        and migration.get("acknowledgement_matched") is True
        and type(migration.get("authorizer_denial_count")) is int
        and migration.get("authorizer_denial_count") == 0
        and source.get("schema_before") == _MIGRATION_SOURCE_SCHEMA
        and source.get("schema_after") == _EXPECTED_SCHEMA_VERSION
        and source.get("strict_read_only_quick_check") == ["ok"]
        and source.get("post_commit_quick_check") == ["ok"]
        and source.get("exclusive_post_commit_quick_check") == ["ok"]
        and type(source.get("hardlink_count")) is int
        and source.get("hardlink_count") == 1
        and type(source.get("sidecar_count_after")) is int
        and source.get("sidecar_count_after") == 0
        and current_path_matches
        and _is_sha256(migration_database.get("sha256"))
        and type(migration_database_size) is int
        and migration_database_size >= 0
        and _fingerprints_exact(migration_database, exclusive_database)
        and policy_safe
    )
    preservation_flags = (
        "exact_precommit_postcommit_snapshot_equal",
        "postcommit_readonly_exclusive_snapshot_equal",
        "exclusive_postcommit_final_fingerprint_equal",
        "existing_table_content_preserved",
        "sqlite_sequence_preserved",
        "projection_outbox_preserved",
        "target_ledger_empty",
        "exclusive_transaction_acquired",
        "exclusive_post_commit_transaction_acquired",
    )
    lease_contract_valid = all(
        _zero_exact_lease_map(safety.get(key))
        for key in (
            "lease_counts",
            "exclusive_lease_counts",
            "exclusive_post_commit_lease_counts",
        )
    )
    target_probes = _mapping(safety.get("target_contract_probes"))
    target_probe_contract_valid = bool(
        frozenset(target_probes) == _MIGRATION_TARGET_PROBES
        and all(value is True for value in target_probes.values())
    )
    backup_contract_valid = bool(
        backup.get("schema_version") == _MIGRATION_SOURCE_SCHEMA
        and backup.get("quick_check") == ["ok"]
        and backup.get("byte_identical") is True
        and type(backup.get("hardlink_count")) is int
        and backup.get("hardlink_count") == 1
        and type(backup.get("sidecar_count")) is int
        and backup.get("sidecar_count") == 0
        and _fingerprints_content_equal(backup, preflight_main)
    )
    preservation_contract_valid = bool(
        all(safety.get(key) is True for key in preservation_flags)
        and lease_contract_valid
        and type(safety.get("exclusive_post_commit_runtime_lease_count")) is int
        and safety.get("exclusive_post_commit_runtime_lease_count") == 0
        and type(safety.get("source_sidecar_count_before")) is int
        and safety.get("source_sidecar_count_before") == 0
        and type(existing_table_count) is int
        and existing_table_count > 0
        and type(counts.get("target_ledger_row_count")) is int
        and counts.get("target_ledger_row_count") == 0
        and target_probe_contract_valid
        and backup_contract_valid
    )

    preflight_payload: Mapping[str, Any] = {}
    preflight_report_sha256: str | None = None
    preflight_path_value = preflight.get("path")
    if isinstance(preflight_path_value, str) and preflight_path_value:
        preflight_path = Path(preflight_path_value)
        preflight_payload, preflight_report_sha256 = _read_stable_json(
            preflight_path,
            missing_reason="MIGRATION_PREFLIGHT_REPORT_NOT_FOUND",
            invalid_reason="MIGRATION_PREFLIGHT_REPORT_INVALID",
        )
    preflight_source = _mapping(preflight_payload.get("source"))
    preflight_verdict = _mapping(preflight_payload.get("verdict"))
    preflight_files_after = _mapping(preflight_source.get("files_after"))
    preflight_after_main = _mapping(preflight_files_after.get("main"))
    required_preflight_true = (
        "backup_tables_preserved",
        "backup_table_content_preserved",
        "target_table_set_exact",
        "existing_table_content_preserved",
        "migration_table_content_preserved",
        "order_state_preserved",
        "projection_outbox_preserved",
        "source_data_files_unchanged",
        "sqlite_sequence_preserved",
        "target_objects_absent_at_source",
        "target_objects_exact",
        "target_contract_valid",
        "target_ledger_empty",
        "initializer_noop",
        "quick_checks_ok",
    )
    preflight_age = preflight.get("age_sec")
    preflight_chain_valid = bool(
        preflight.get("verdict") == "PASS"
        and _is_sha256(preflight.get("sha256"))
        and preflight_report_sha256 == preflight.get("sha256")
        and isinstance(preflight_age, (int, float))
        and not isinstance(preflight_age, bool)
        and 0 <= float(preflight_age) <= 3600
        and preflight_payload.get("preflight_contract_version") == _MIGRATION_PREFLIGHT_CONTRACT
        and preflight_payload.get("required_source_schema") == _MIGRATION_SOURCE_SCHEMA
        and preflight_payload.get("target_schema_version") == _EXPECTED_SCHEMA_VERSION
        and preflight_source.get("opened_read_only") is True
        and preflight_source.get("query_only") is True
        and preflight_source.get("immutable") is True
        and preflight_source.get("quick_check") == ["ok"]
        and _same_resolved_path(preflight_source.get("path"), current_path)
        and _fingerprints_content_equal(preflight_after_main, preflight_main)
        and _fingerprints_content_equal(preflight_after_main, backup)
        and preflight_verdict.get("status") == "PASS"
        and preflight_verdict.get("failures") == []
        and preflight_verdict.get("operating_database_mutated") is False
        and preflight_verdict.get("source_schema_version") == _MIGRATION_SOURCE_SCHEMA
        and preflight_verdict.get("target_schema_version") == _EXPECTED_SCHEMA_VERSION
        and preflight_verdict.get("quick_check") == ["ok"]
        and preflight_verdict.get("exact_quick_check") == ["ok"]
        and all(preflight_verdict.get(key) is True for key in required_preflight_true)
    )
    current_before_matches_migration = _fingerprints_exact(
        migration_database,
        current_main,
    )
    contract_ready = bool(
        producer_contract_valid
        and preservation_contract_valid
        and preflight_chain_valid
        and current_before_matches_migration
    )
    return {
        "report_sha256": report_sha256,
        "expected_report_sha256": expected_report_sha256,
        "report_sha256_authorized": report_sha256 == expected_report_sha256,
        "preflight_report_sha256": preflight_report_sha256,
        "declared_status_pass": payload.get("status") == "PASS",
        "declared_committed": payload.get("committed") is True,
        "source_schema_valid": migration.get("source_schema") == _MIGRATION_SOURCE_SCHEMA,
        "target_schema_valid": migration.get("target_schema") == _EXPECTED_SCHEMA_VERSION,
        "evidence_policy_safe": policy_safe,
        "producer_contract_valid": producer_contract_valid,
        "preservation_contract_valid": preservation_contract_valid,
        "preflight_chain_valid": preflight_chain_valid,
        "target_probe_contract_valid": target_probe_contract_valid,
        "lease_contract_valid": lease_contract_valid,
        "backup_contract_valid": backup_contract_valid,
        "existing_table_count": counts.get("existing_table_count"),
        "migration_database": {
            "sha256": (
                migration_database.get("sha256")
                if _is_sha256(migration_database.get("sha256"))
                else None
            ),
            "size": (
                migration_database.get("size")
                if type(migration_database.get("size")) is int
                else None
            ),
            "mtime_ns": (
                migration_database.get("mtime_ns")
                if type(migration_database.get("mtime_ns")) is int
                else None
            ),
        },
        "contract_ready": contract_ready,
        "current_before_matches_migration": current_before_matches_migration,
        "paths_recorded": False,
    }


def _read_stable_json(
    path: Path,
    *,
    missing_reason: str,
    invalid_reason: str,
) -> tuple[Mapping[str, Any], str]:
    resolved = _validated_regular_file(path, reason=missing_reason)
    try:
        before = resolved.stat()
        payload_bytes = resolved.read_bytes()
        after = resolved.stat()
        if before.st_size != after.st_size or before.st_mtime_ns != after.st_mtime_ns:
            raise Fast0StrictRequalificationError(f"{invalid_reason}_CHANGED_DURING_READ")

        def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
            result: dict[str, Any] = {}
            for key, value in pairs:
                if key in result:
                    raise ValueError("duplicate JSON key")
                result[key] = value
            return result

        def reject_constant(value: str) -> None:
            raise ValueError(f"non-finite JSON number: {value}")

        payload = json.loads(
            payload_bytes.decode("utf-8"),
            object_pairs_hook=reject_duplicate_keys,
            parse_constant=reject_constant,
        )
    except (OSError, UnicodeError, ValueError) as exc:
        raise Fast0StrictRequalificationError(invalid_reason) from exc
    if not isinstance(payload, Mapping):
        raise Fast0StrictRequalificationError(invalid_reason)
    return payload, hashlib.sha256(payload_bytes).hexdigest()


def _same_resolved_path(value: Any, expected: Path) -> bool:
    if not isinstance(value, str) or not value:
        return False
    try:
        return Path(value).expanduser().resolve(strict=False) == expected.resolve(strict=True)
    except OSError:
        return False


def _fingerprints_exact(expected: Any, actual: Any) -> bool:
    left = _mapping(expected)
    right = _mapping(actual)
    return bool(
        _is_sha256(left.get("sha256"))
        and left.get("sha256") == right.get("sha256")
        and type(left.get("size")) is int
        and left.get("size") == right.get("size")
        and type(left.get("mtime_ns")) is int
        and left.get("mtime_ns") == right.get("mtime_ns")
    )


def _fingerprints_content_equal(expected: Any, actual: Any) -> bool:
    left = _mapping(expected)
    right = _mapping(actual)
    return bool(
        _is_sha256(left.get("sha256"))
        and left.get("sha256") == right.get("sha256")
        and type(left.get("size")) is int
        and left.get("size") == right.get("size")
    )


def _zero_exact_lease_map(value: Any) -> bool:
    leases = _mapping(value)
    return bool(
        frozenset(leases) == _MIGRATION_LEASE_KEYS
        and all(type(count) is int and count == 0 for count in leases.values())
    )


def _baseline_logical_preservation(baseline: Mapping[str, Any]) -> dict[str, Any]:
    digest_payload = {
        "report_sha256": baseline.get("report_sha256"),
        "preflight_report_sha256": baseline.get("preflight_report_sha256"),
        "migration_database": baseline.get("migration_database"),
        "preservation_contract_valid": baseline.get("preservation_contract_valid"),
    }
    return {
        "verification_complete": bool(
            baseline.get("contract_ready") is True
            and baseline.get("current_after_matches_migration") is True
        ),
        "verification_method": "MIGRATION_CHAIN_AND_CURRENT_PHYSICAL_HASH",
        "full_table_row_scan_performed": False,
        "table_count": int(baseline.get("existing_table_count") or 0),
        "inventory_digest": hashlib.sha256(
            json.dumps(digest_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest(),
        "preservation_contract_valid": baseline.get("preservation_contract_valid") is True,
        "raw_rows_recorded": False,
    }


def _scan_runtime_logs(paths: Sequence[Path]) -> dict[str, Any]:
    if len(paths) > _MAX_RUNTIME_LOG_FILES:
        raise Fast0StrictRequalificationError("RUNTIME_LOG_FILE_LIMIT_EXCEEDED")
    marker_counts = {marker: 0 for marker in _RUNTIME_MARKERS}
    file_fingerprints: list[dict[str, Any]] = []
    resolved_paths: set[Path] = set()
    total_bytes = 0
    for path in paths:
        resolved = _validated_regular_file(path, reason="RUNTIME_LOG_NOT_FOUND")
        if resolved in resolved_paths:
            raise Fast0StrictRequalificationError("RUNTIME_LOG_DUPLICATE_PATH")
        resolved_paths.add(resolved)
        path_before = resolved.stat()
        digest = hashlib.sha256()
        with resolved.open("rb") as stream:
            handle_before = os.fstat(stream.fileno())
            if _stat_identity(path_before) != _stat_identity(handle_before):
                raise Fast0StrictRequalificationError("RUNTIME_LOG_IDENTITY_CHANGED")
            if total_bytes + int(handle_before.st_size) > _MAX_RUNTIME_LOG_TOTAL_BYTES:
                raise Fast0StrictRequalificationError("RUNTIME_LOG_BYTE_LIMIT_EXCEEDED")
            counts = _scan_binary_stream(stream, digest=digest)
            handle_after = os.fstat(stream.fileno())
        path_after = resolved.stat()
        if not (
            _stat_snapshot(path_before)
            == _stat_snapshot(handle_before)
            == _stat_snapshot(handle_after)
            == _stat_snapshot(path_after)
        ):
            raise Fast0StrictRequalificationError("RUNTIME_LOG_CHANGED_DURING_SCAN")
        for marker, count in counts.items():
            marker_counts[marker] += count
        total_bytes += int(handle_after.st_size)
        file_fingerprints.append(
            {
                "size": int(handle_after.st_size),
                "mtime_ns": int(handle_after.st_mtime_ns),
                "sha256": digest.hexdigest(),
            }
        )
    inventory_digest = hashlib.sha256(
        json.dumps(
            file_fingerprints,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    return {
        "coverage_provided": bool(paths),
        # A collection of historical files can prove marker presence, but file
        # selection alone cannot prove a complete or current runtime window.
        "coverage_authoritative": False,
        "coverage_scope": "BOUNDED_HISTORICAL_FILES",
        "scan_complete": True,
        "file_count": len(paths),
        "total_bytes": total_bytes,
        "inventory_digest": inventory_digest,
        "marker_counts": marker_counts,
        "encodings_scanned": ["UTF8_ASCII_COMPATIBLE", "UTF16_LE", "UTF16_BE"],
        "matching_lines_recorded": False,
        "paths_recorded": False,
    }


def _scan_binary_stream(stream: BinaryIO, *, digest: Any) -> dict[str, int]:
    marker_bytes = {
        marker: (
            marker.encode("ascii"),
            marker.encode("utf-16-le"),
            marker.encode("utf-16-be"),
        )
        for marker in _RUNTIME_MARKERS
    }
    counts = {marker: 0 for marker in _RUNTIME_MARKERS}
    overlap = max(len(value) for values in marker_bytes.values() for value in values) - 1
    tail = b""
    while chunk := stream.read(1024 * 1024):
        digest.update(chunk)
        data = tail + chunk
        safe_end = max(len(data) - overlap, 0)
        for marker, values in marker_bytes.items():
            for value in values:
                start = 0
                while True:
                    index = data.find(value, start)
                    if index < 0 or index >= safe_end:
                        break
                    counts[marker] += 1
                    start = index + len(value)
        tail = data[safe_end:]
    for marker, values in marker_bytes.items():
        counts[marker] += sum(tail.count(value) for value in values)
    return counts


def _validated_database_path(path: Path) -> Path:
    resolved = _validated_regular_file(path, reason="DATABASE_NOT_FOUND")
    if int(resolved.stat().st_nlink) != 1:
        raise Fast0StrictRequalificationError("DATABASE_HARDLINK_ALIAS_UNSAFE")
    return resolved


def _validated_regular_file(path: Path, *, reason: str) -> Path:
    try:
        resolved = path.expanduser().resolve(strict=True)
    except OSError as exc:
        raise Fast0StrictRequalificationError(reason) from exc
    if not resolved.is_file():
        raise Fast0StrictRequalificationError(reason)
    return resolved


def _open_strict_read_only(
    path: Path,
    *,
    sqlite_source: str | None = None,
) -> sqlite3.Connection:
    _assert_no_sidecars(path)
    uri_path = quote(sqlite_source or path.as_posix(), safe="/:")
    connection = sqlite3.connect(
        f"file:{uri_path}?mode=ro&immutable=1",
        uri=True,
        timeout=30.0,
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    if int(connection.execute("PRAGMA query_only").fetchone()[0]) != 1:
        connection.close()
        raise Fast0StrictRequalificationError("DATABASE_QUERY_ONLY_NOT_ENABLED")
    return connection


def _assert_no_sidecars(path: Path) -> None:
    if any(Path(f"{path}{suffix}").exists() for suffix in ("-wal", "-shm", "-journal")):
        raise Fast0StrictRequalificationError("STRICT_READ_ONLY_REQUIRES_CHECKPOINTED_DATABASE")


def _stat_identity(value: os.stat_result) -> tuple[int, int]:
    return int(value.st_dev), int(value.st_ino)


def _stat_snapshot(value: os.stat_result) -> tuple[int, int, int, int]:
    return (*_stat_identity(value), int(value.st_size), int(value.st_mtime_ns))


def _fingerprint_regular_file(path: Path) -> dict[str, Any]:
    path_before = path.stat()
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        handle_before = os.fstat(stream.fileno())
        if _stat_identity(path_before) != _stat_identity(handle_before):
            raise Fast0StrictRequalificationError("FILE_IDENTITY_CHANGED_BEFORE_READ")
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
        handle_after = os.fstat(stream.fileno())
    path_after = path.stat()
    if not (
        _stat_snapshot(path_before)
        == _stat_snapshot(handle_before)
        == _stat_snapshot(handle_after)
        == _stat_snapshot(path_after)
    ):
        raise Fast0StrictRequalificationError("DATABASE_FILE_CHANGED_DURING_HASH")
    return {
        "exists": True,
        "size": int(handle_after.st_size),
        "mtime_ns": int(handle_after.st_mtime_ns),
        "sha256": digest.hexdigest(),
    }


def _file_fingerprints(
    path: Path,
    *,
    pinned_main: Mapping[str, Any] | None = None,
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for suffix in _DATA_FILE_SUFFIXES:
        candidate = Path(f"{path}{suffix}")
        key = "main" if not suffix else suffix.removeprefix("-")
        if not suffix and pinned_main is not None:
            result[key] = dict(pinned_main)
            continue
        if not candidate.exists():
            result[key] = {"exists": False, "size": 0, "mtime_ns": None, "sha256": None}
            continue
        result[key] = _fingerprint_regular_file(candidate)
    return result


def _probe_no_other_open_handles(path: Path) -> dict[str, Any]:
    if os.name == "nt":
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        create_file = kernel32.CreateFileW
        create_file.argtypes = [
            ctypes.c_wchar_p,
            ctypes.c_uint32,
            ctypes.c_uint32,
            ctypes.c_void_p,
            ctypes.c_uint32,
            ctypes.c_uint32,
            ctypes.c_void_p,
        ]
        create_file.restype = ctypes.c_void_p
        handle = create_file(str(path), 0x80000000, 0, None, 3, 0x80, None)
        invalid = ctypes.c_void_p(-1).value
        if handle == invalid:
            return {
                "status": "BLOCKED",
                "method": "WINDOWS_EXCLUSIVE_READ_HANDLE",
                "no_other_open_handles": False,
                "error_code": int(ctypes.get_last_error()),
            }
        kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
        kernel32.CloseHandle(handle)
        return {
            "status": "PASS",
            "method": "WINDOWS_EXCLUSIVE_READ_HANDLE",
            "no_other_open_handles": True,
            "error_code": 0,
        }
    if sys.platform.startswith("linux"):
        target = str(path.resolve())
        open_handle_count = 0
        unscannable_process_count = 0
        proc = Path("/proc")
        for process_dir in proc.iterdir():
            if not process_dir.name.isdigit() or int(process_dir.name) == os.getpid():
                continue
            fd_dir = process_dir / "fd"
            try:
                descriptors = list(fd_dir.iterdir())
            except (OSError, PermissionError):
                unscannable_process_count += 1
                continue
            for descriptor in descriptors:
                try:
                    linked = os.path.realpath(descriptor)
                except OSError:
                    continue
                if linked == target:
                    open_handle_count += 1
        return {
            "status": (
                "PASS" if open_handle_count == 0 and unscannable_process_count == 0 else "BLOCKED"
            ),
            "method": "LINUX_PROC_FD_SCAN",
            "no_other_open_handles": open_handle_count == 0,
            "open_handle_count": open_handle_count,
            "unscannable_process_count": unscannable_process_count,
        }
    return {
        "status": "BLOCKED",
        "method": "UNSUPPORTED_PLATFORM",
        "no_other_open_handles": False,
    }


def _disk_space_status(path: Path, *, min_free_bytes: int) -> dict[str, Any]:
    usage = shutil.disk_usage(path.parent)
    return {
        "free_bytes": int(usage.free),
        "required_free_bytes": int(min_free_bytes),
        "sufficient": int(usage.free) >= int(min_free_bytes),
    }


def _fingerprint_matches(expected: Any, actual: Any) -> bool:
    expected_mapping = _mapping(expected)
    actual_mapping = _mapping(actual)
    return bool(
        _is_sha256(expected_mapping.get("sha256"))
        and expected_mapping.get("sha256") == actual_mapping.get("sha256")
        and int(expected_mapping.get("size") or -1) == int(actual_mapping.get("size") or -2)
    )


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _normalized_sql_sha256(value: Any) -> str:
    return hashlib.sha256(_normalize_sql(value).encode("utf-8")).hexdigest()


def _quoted_token_sha256(value: Any) -> str:
    sql = str(value or "")
    tokens: list[str] = []
    index = 0
    closing = {"'": "'", '"': '"', "`": "`", "[": "]"}
    while index < len(sql):
        opening = sql[index]
        if opening not in closing:
            index += 1
            continue
        end = closing[opening]
        start = index
        index += 1
        terminated = False
        while index < len(sql):
            if sql[index] != end:
                index += 1
                continue
            if end != "]" and index + 1 < len(sql) and sql[index + 1] == end:
                index += 2
                continue
            index += 1
            terminated = True
            break
        if not terminated:
            tokens.append("<UNTERMINATED>" + sql[start:])
            break
        tokens.append(sql[start:index])
    encoded = json.dumps(
        tokens,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _sql_token_sha256(value: Any) -> str:
    sql = str(value or "")
    if "\x00" in sql:
        raise Fast0StrictRequalificationError("SCHEMA_SQL_NUL_INVALID")
    tokens: list[tuple[str, bytes]] = []
    index = 0
    closing = {"'": "'", '"': '"', "`": "`", "[": "]"}
    quote_kind = {"'": "STRING", '"': "QUOTED", "`": "QUOTED", "[": "QUOTED"}
    while index < len(sql):
        char = sql[index]
        if char.isspace():
            index += 1
            continue
        if sql.startswith("--", index):
            newline = sql.find("\n", index + 2)
            index = len(sql) if newline < 0 else newline + 1
            continue
        if sql.startswith("/*", index):
            end_comment = sql.find("*/", index + 2)
            if end_comment < 0:
                raise Fast0StrictRequalificationError("SCHEMA_SQL_COMMENT_UNTERMINATED")
            index = end_comment + 2
            continue
        if char in closing:
            end = closing[char]
            start = index
            index += 1
            terminated = False
            while index < len(sql):
                if sql[index] != end:
                    index += 1
                    continue
                if end != "]" and index + 1 < len(sql) and sql[index + 1] == end:
                    index += 2
                    continue
                index += 1
                terminated = True
                break
            if not terminated:
                raise Fast0StrictRequalificationError("SCHEMA_SQL_QUOTE_UNTERMINATED")
            tokens.append((quote_kind[char], sql[start:index].encode("utf-8")))
            continue
        if char.isalnum() or char in {"_", "$"} or ord(char) >= 128:
            start = index
            index += 1
            while index < len(sql):
                current = sql[index]
                if not (current.isalnum() or current in {"_", "$"} or ord(current) >= 128):
                    break
                index += 1
            raw_word = sql[start:index]
            canonical_word = "".join(
                chr(ord(part) - 32) if "a" <= part <= "z" else part for part in raw_word
            )
            tokens.append(("WORD", canonical_word.encode("utf-8")))
            continue
        tokens.append(("SYMBOL", char.encode("utf-8")))
        index += 1
    if tokens and tokens[-1] == ("SYMBOL", b";"):
        tokens.pop()
    digest = hashlib.sha256(b"sqlite-token-v1\x00")
    for kind, payload in tokens:
        kind_bytes = kind.encode("ascii")
        digest.update(len(kind_bytes).to_bytes(2, "big"))
        digest.update(kind_bytes)
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    return digest.hexdigest()


def _normalize_sql(value: Any) -> str:
    return "".join(str(value or "").upper().split()).replace("IFNOTEXISTS", "").rstrip(";")


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _schema_objects_valid(
    connection: sqlite3.Connection,
    names: Sequence[str],
) -> bool:
    for name in names:
        expected = _SCHEMA_OBJECT_MANIFEST.get(name)
        if expected is None:
            return False
        expected_type, expected_sha256 = expected
        row = connection.execute(
            "SELECT type, tbl_name, sql FROM sqlite_master WHERE name = ?",
            (name,),
        ).fetchone()
        accepted_hashes = {
            expected_sha256,
            *_SCHEMA_OBJECT_ALTERNATE_SHA256.get(name, frozenset()),
        }
        expected_token_sha256 = _SCHEMA_OBJECT_TOKEN_SHA256.get(name)
        accepted_token_hashes = {
            expected_token_sha256,
            *_SCHEMA_OBJECT_ALTERNATE_TOKEN_SHA256.get(name, frozenset()),
        }
        expected_quoted_sha256 = _SCHEMA_OBJECT_QUOTED_TOKEN_SHA256.get(name)
        accepted_quoted_hashes = {
            expected_quoted_sha256,
            *_SCHEMA_OBJECT_ALTERNATE_QUOTED_TOKEN_SHA256.get(name, frozenset()),
        }
        expected_owner = name if expected_type == "table" else _SCHEMA_OBJECT_OWNER.get(name)
        if (
            row is None
            or str(row["type"]) != expected_type
            or str(row["tbl_name"]) != expected_owner
            or _normalized_sql_sha256(row["sql"]) not in accepted_hashes
            or expected_token_sha256 is None
            or _sql_token_sha256(row["sql"]) not in accepted_token_hashes
            or (
                expected_quoted_sha256 is not None
                and _quoted_token_sha256(row["sql"]) not in accepted_quoted_hashes
            )
        ):
            return False
    return True


def _is_sha256(value: Any) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def write_report(report: Mapping[str, Any], *, out_dir: Path) -> dict[str, Path]:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S.%fZ")
    report_dir = out_dir / stamp
    report_dir.mkdir(parents=True, exist_ok=False)
    raw_path = report_dir / "raw.json"
    summary_path = report_dir / "summary.md"
    commands_path = report_dir / "commands.txt"
    safe_report = _mapping(_redact(report))
    raw_path.write_text(
        json.dumps(safe_report, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    summary_path.write_text(render_markdown_summary(safe_report), encoding="utf-8")
    commands_path.write_text(
        " ".join(
            (
                "python -B -m tools.ops_fast0_strict_requalification",
                "--db <redacted-schema-62-db>",
                "--migration-apply-report <redacted-migration-report>",
                "--migration-apply-report-sha256 <approved-migration-report-sha256>",
                "--runtime-log <redacted-runtime-log>",
                "--out-dir <redacted-evidence-directory>",
            )
        )
        + "\n",
        encoding="utf-8",
    )
    return {
        "raw_json": raw_path,
        "summary_md": summary_path,
        "commands_txt": commands_path,
    }


def render_markdown_summary(report: Mapping[str, Any]) -> str:
    verdict = _mapping(report.get("verdict"))
    broker = _mapping(report.get("broker_boundary"))
    incremental = _mapping(report.get("incremental_evaluation"))
    pipeline = _mapping(report.get("pipeline_coherency"))
    projection = _mapping(report.get("projection"))
    return "\n".join(
        [
            "# FAST-0 Strict Offline Requalification",
            "",
            f"- generated_at: `{report.get('generated_at')}`",
            f"- evidence_status: `{verdict.get('evidence_status')}`",
            f"- fast0_status: `{verdict.get('fast0_status')}`",
            (
                "- broker raw/effective UNCONFIRMED: "
                f"`{broker.get('raw_unconfirmed_count')}/"
                f"{broker.get('effective_unconfirmed_count')}`"
            ),
            (
                "- incremental raw/effective dead-letter: "
                f"`{incremental.get('raw_dead_letter_status_count')}/"
                f"{incremental.get('effective_dead_letter_count')}`"
            ),
            (
                "- pipeline canonical/qualification: "
                f"`{pipeline.get('canonical_status')}/"
                f"{pipeline.get('qualification_status')}`"
            ),
            (
                "- projection ERROR/DEAD_LETTER/result ERROR: "
                f"`{projection.get('outbox_error_count')}/"
                f"{projection.get('outbox_dead_letter_count')}/"
                f"{projection.get('event_result_error_count')}`"
            ),
            f"- evidence failures: `{', '.join(verdict.get('evidence_failures') or []) or '-'}`",
            (
                "- qualification blockers: "
                f"`{', '.join(verdict.get('qualification_blockers') or []) or '-'}`"
            ),
            "",
            "The operating database was opened immutable/query-only in one deferred snapshot.",
            "Core and Gateway were not started; no order or broker action was performed.",
        ]
    )


def render_console_summary(report: Mapping[str, Any]) -> str:
    verdict = _mapping(report.get("verdict"))
    return (
        "FAST-0 strict requalification: "
        f"evidence={verdict.get('evidence_status')} "
        f"fast0={verdict.get('fast0_status')} "
        f"blockers={len(verdict.get('qualification_blockers') or [])}"
    )


def _redact(value: Any, *, key: str = "") -> Any:
    lowered = key.lower()
    if any(part in lowered for part in ("account", "token", "password", "secret")):
        return "[REDACTED]"
    if isinstance(value, Mapping):
        redacted: dict[str, Any] = {}
        original_keys = {str(child) for child in value}
        used_keys: set[str] = set()
        redacted_key_index = 0
        for child, item in value.items():
            child_key = str(child)
            key_is_safe = bool(
                isinstance(child, str)
                and not child_key.startswith("[REDACTED_KEY_")
                and not _contains_sensitive_value(child_key)
                and child_key not in used_keys
            )
            if key_is_safe:
                output_key = child_key
            else:
                while True:
                    redacted_key_index += 1
                    output_key = f"[REDACTED_KEY_{redacted_key_index}]"
                    if output_key not in original_keys and output_key not in used_keys:
                        break
            used_keys.add(output_key)
            redacted[output_key] = _redact(item, key=child_key)
        return redacted
    if isinstance(value, list):
        return [_redact(item, key=key) for item in value]
    if isinstance(value, tuple):
        return [_redact(item, key=key) for item in value]
    if isinstance(value, str) and _contains_sensitive_value(value):
        return "[REDACTED]"
    return value


def _contains_sensitive_value(value: str) -> bool:
    if any(pattern.search(value) is not None for pattern in _SENSITIVE_VALUE_PATTERNS):
        return True
    without_dates = _ISO_DATE_PATTERN.sub("", value)
    return _HYPHENATED_ACCOUNT_PATTERN.search(without_dates) is not None


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _fixed_count_buckets(value: Any, allowed: Sequence[str]) -> dict[str, Any]:
    source = _mapping(value)
    counts = {key: 0 for key in allowed}
    unknown_row_count = 0
    unknown_distinct_count = 0
    invalid_entry_count = 0
    for key, raw_count in source.items():
        if type(raw_count) is not int or raw_count < 0:
            invalid_entry_count += 1
            continue
        if isinstance(key, str) and key in counts:
            counts[key] = raw_count
        else:
            unknown_distinct_count += 1
            unknown_row_count += raw_count
    return {
        "counts": counts,
        "unknown_row_count": unknown_row_count,
        "unknown_distinct_count": unknown_distinct_count,
        "invalid_entry_count": invalid_entry_count,
        "bucket_conservation_valid": invalid_entry_count == 0,
        "contract_valid": (
            unknown_row_count == 0 and unknown_distinct_count == 0 and invalid_entry_count == 0
        ),
    }


if __name__ == "__main__":
    raise SystemExit(main())
