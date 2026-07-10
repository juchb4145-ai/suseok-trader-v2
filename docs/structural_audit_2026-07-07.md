# Structural Audit - 2026-07-07

대상: `suseok-trader-v2` main  
범위: Gateway ingestion, runtime lock, incremental evaluation, market index/regime, LIVE_SIM order lifecycle, replay/retention/watermark, dashboard coherency  
최종 업데이트: 2026-07-10 (PR-20 market_scan worker/reconcile/dry-run 준비 구현 완료)
안전 원칙: append-only 전환은 기본 disabled와 strict feature flag를 유지한다. `LIVE_REAL` 활성화, 주문 정책 완화, 매수 기준 완화는 하지 않는다.

## 이미 개선된 점

- Gateway event store는 `raw_events`/`gateway_events`에 `event_id` PK와 payload hash를 저장하고, 동일 payload 중복은 `duplicate_count`로 흡수하며 payload 충돌은 `CONFLICT`로 거부한다. 근거: `storage/event_store.py`.
- Gateway command는 `gateway_command_dedupe_keys.idempotency_key` PK를 사용해 active idempotency 중복 enqueue를 차단한다. LIVE_SIM `send_order`/`cancel_order`는 simulation-like mode, `live_sim_only=true`, `live_real_allowed=false`, idempotency 일치 검사를 통과해야 enqueue된다. 근거: `storage/gateway_command_store.py`.
- LIVE_SIM intent/order 주요 키는 DB 제약이 있다. `live_sim_intents.live_sim_intent_id` PK, `idempotency_key` UNIQUE, nullable `order_plan_id` partial UNIQUE, `live_sim_orders.live_sim_order_id` PK, `live_sim_orders.idempotency_key` UNIQUE, `order_plan_drafts.order_plan_id` PK, `order_plan_drafts.idempotency_key` UNIQUE. 근거: `storage/sqlite.py`, `storage/live_sim_order_plan_uniqueness.py`.
- Market index parser의 `parser_status`는 latest row와 dashboard status에 노출된다. `PILOT_UNVERIFIED` 같은 상태를 감지할 수 있다. 근거: `services/market_index_service.py`, `gateway/kiwoom_client.py`.
- Dashboard top theme 표시는 단순 latest sample만 쓰지 않고 state-filtered query를 별도로 사용한다. 기존 테스트가 DATA_WAIT 최신 표본 뒤에 숨은 LEADING/SPREADING 테마를 검증한다. 근거: `services/dashboard_service.py`, `tests/test_dashboard_service.py`.
- Order command는 `CLAIMED -> GATEWAY_STARTED -> PRE_ACK_RECORDED -> BROKER_ACCEPTED -> CHEJAN_CONFIRMED` 경계를 사용한다. timeout은 `UNCONFIRMED`로 격리되고, durable pre-ack이 Core DB에 commit되지 않으면 Kiwoom broker method를 호출하지 않는다. 근거: `storage/gateway_command_store.py`, `storage/gateway_order_broker_boundary.py`, `gateway/kiwoom_command_handlers.py`.
- P0-2는 수정 완료됐다. Candidate quote refresh `tr_response`의 synthetic `price_tick`은 `parent_event_id:synthetic_price_tick:row_index:code:exchange` 기반 deterministic child `event_id`를 사용하며, payload metadata에 parent event/command/TR/request/row 정보를 남긴다. 근거: `services/market_data_service.py`, `tests/test_structural_audit_guards.py`.

## P0-1 진행 요약

- P0-1 mitigation step 1: `projection_outbox` skeleton을 추가했다. Gateway POST의 기존 inline projection은 유지되며, outbox는 shadow 준비용 PENDING job 관측 기반으로만 동작한다. 주문/LIVE_SIM 동작, `LIVE_REAL`, 매수 gate, safety gate 정책은 변경하지 않았다.
- PR-3: `projection_outbox` shadow verification worker를 추가했다. worker는 inline projection을 대체하지 않고 projection table을 직접 갱신하지 않으며, outbox job 상태만 `APPLIED`/`SKIPPED`/`ERROR`/`DEAD_LETTER`로 정리한다. background worker는 기본 disabled다.
- PR-4: `projection_outbox` market_data-only apply pilot을 추가했다. 기본값은 disabled이며, `PROJECTION_OUTBOX_APPLY_PROJECTION_ENABLED=true`와 `PROJECTION_OUTBOX_MARKET_DATA_APPLY_ENABLED=true`가 모두 켜지고 operator가 `apply_projection=true`로 run-once를 호출할 때만 `market_data` projection 재적용이 허용된다. `market_reference`/`market_index`/`market_regime`/`market_scan`/`condition_fusion` apply는 여전히 차단되고, Gateway POST inline projection과 주문/LIVE_SIM 동작은 변경하지 않았다. 운영 절차는 `docs/runbook_projection_outbox_apply_market_data_ko.md`에 정리했다.
- PR-5: `market_data` projection dual-run reconciliation을 추가했다. accepted `price_tick`/`condition_event`/`tr_response`와 inline projection artifact, `projection_outbox`, watermark, synthetic child event id를 대조해 append-only 전환 준비도를 `PASS`/`WARN`/`FAIL`로 리포트한다. 이 PR도 append-only 전환이 아니며 Gateway inline projection, worker default disabled, 주문/LIVE_SIM/LIVE_REAL 정책은 그대로 유지한다. 운영 절차는 `docs/runbook_market_data_projection_reconcile_ko.md`에 정리했다.
- PR-6: `market_data` append-only dry-run routing decision을 추가했다. Gateway inline `process_gateway_event()`는 계속 실행되며, dry-run flag가 켜진 경우에만 reconcile PASS/outbox readiness 조건을 만족한 이벤트에 대해 `would_skip_inline=True`를 기록한다. `effective_skip_inline`은 PR-6에서 항상 `False`이며, cutover flag가 켜져도 `EFFECTIVE_SKIP_DISABLED_IN_PR6` evidence만 남긴다. 운영 절차는 `docs/runbook_market_data_append_only_routing_ko.md`에 정리했다.
- PR-7: `price_tick`에 한정한 `market_data` inline skip을 엄격한 feature flag 뒤에 추가했다. 기본값에서는 `effective_skip_inline=False`이며, `dry_run + global cutover + price_tick cutover + worker apply enabled + reconcile PASS/fresh + outbox ready + per-minute skip budget > 0` 조건을 모두 만족해야만 Gateway request path의 `process_gateway_event()`를 건너뛴다. `condition_event`/`tr_response`는 계속 inline projection을 유지하며, skipped `price_tick`의 incremental evaluation enqueue는 `projection_outbox` worker가 market_data apply 성공 후 deferred side effect evidence로 기록한다. 운영 절차는 `docs/runbook_market_data_price_tick_cutover_ko.md`에 정리했다.
- PR-8: `tr_response` candidate quote refresh incremental enqueue를 공통 side-effect 서비스로 분리하고, worker가 `market_data` `tr_response`를 직접 apply한 경우에만 deferred side-effect evidence를 기록하도록 준비했다. 이번 PR에서도 `tr_response`와 `condition_event`의 `effective_skip_inline`은 금지되어 있으며 Gateway inline projection은 유지된다. 운영 절차는 `docs/runbook_market_data_tr_response_side_effect_migration_ko.md`에 정리했다.
- PR-9: `tr_response` limited inline skip과 worker-side deferred candidate quote refresh를 strict flag, fresh reconcile, worker apply, synthetic child guard, per-minute budget 뒤에서 허용했다. 기본값은 disabled다.
- PR-10: `condition_event` 후속 `condition_fusion` refresh를 공통 side-effect 서비스로 분리하고 worker apply 경로의 deferred refresh evidence를 추가했다. 이 단계에서는 `condition_event` inline skip을 계속 금지했다.
- PR-10.5~10.8: dashboard fast path, SQLite lock retry/`LOCKED_RETRYABLE`, outbox backlog readiness/drain, safe bulk shadow retire를 추가해 장중 검증과 backlog 판정을 안정화했다. projection 또는 주문 side effect를 새로 허용하지 않는다.
- PR-11: `condition_event` limited inline skip과 worker-side deferred `condition_fusion` refresh를 strict flag, reconcile/backlog readiness, per-minute budget 뒤에서 허용했다. worker의 candidate ingest는 계속 금지한다.
- PR-12: `price_tick`/`tr_response`/`condition_event` cutover를 중앙 `operating_mode`, global kill switch, global budget, auto rollback gate로 통제하는 MarketData append-only controller를 추가했다. 기본 mode는 `OFF`, kill switch는 enabled다.
- PR-13: `market_reference` worker apply 준비, reconcile, dry-run routing, operator/dashboard/ops evidence를 추가했다. Gateway의 `process_market_symbols_event()`는 항상 실행되고 `effective_skip_inline=False`이므로 아직 cutover가 아니다.
- PR-14: `market_reference` limited cutover를 global kill switch, 원자적 `1/min` budget, fresh reconcile, worker apply, outbox/membership health, 즉시 inline rollback 뒤에서 허용했다. 기본값은 cutover OFF, kill switch ON, budget 0이다.
- PR-15: `market_index` worker apply/reconcile/dry-run을 준비했다. parser confidence와 data usability, `REALTIME`/`TR_BOOTSTRAP`/`UNKNOWN` source를 분리하고 PR-15에서는 effective skip을 강제 차단했다.
- PR-16: `market_index` limited cutover를 global kill switch, 원자적 per-minute budget, fresh reconcile/data/parser/realtime/worker/regime gate와 즉시 inline rollback 뒤에서 허용했다. worker는 effective-skip event의 `market_index` projection 후 `source_event_id`가 연결된 `market_regime` snapshot과 sibling outbox를 닫아야 한다. 기본값은 cutover OFF, kill switch ON, budget 0, PR-15 legacy guard ON이다.
- PR-17: schema 51에 trade_date/market/index watermark 단위 KOSPI/KOSDAQ 공통 `market_context` snapshot/latest pointer를 추가했다. Gateway index inline 경로와 PR-16 worker 복구 경로가 같은 builder를 사용하며, candidate refresh와 per-code regime API는 snapshot id를 read-only 참조하고 후보별 regime snapshot을 더 만들지 않는다. parser confidence/data quality/data usability는 별도 lineage이고 missing/stale/unverified는 fail-closed한다.
- PR-18: schema 52에 `market_regime` projection reconcile/routing evidence를 추가하고, standalone regime worker가 PR-17 common builder로 global regime 1개와 KOSPI/KOSDAQ context pair를 생성하도록 준비했다. index dependency 누락은 retry/dead-letter, superseded event는 verify-only로 fail-closed하며 Gateway dry-run의 `effective_skip_inline`은 항상 false다.
- PR-19: schema 53에 market-regime cutover/controller evidence와 원자적 `1/min` budget을 추가했다. KRX REGULAR/index parser-data-source/Gateway health, 직전 event를 덮는 fresh reconcile, prior effective-skip worker closure를 모두 통과한 경우에만 market-regime/context inline builder를 worker로 이관하며 기본은 cutover OFF, kill switch ON, budget 0, PR-18 guard ON이다.
- PR-20: schema 54에 market-scan source lineage, reconcile/routing evidence를 추가하고 scan worker apply를 준비했다. 같은 event의 market_data dependency가 먼저 완료돼야 하며 parser VERIFIED/data usable/row terminal coverage/fresh prior reconcile을 모두 만족해도 `effective_skip_inline`은 강제로 false다.
- Replay 기반: accepted event export/import, source rowid/received_at 및 order hash 보존, 새 격리 DB 강제, inline-shadow와 worker-apply projection hash/reconcile 비교, strict SQLite write authorizer, KRX/NXT 분리 evidence를 구현했다. operator/API Dashboard는 report read-only status만 제공하고 replay 실행 endpoint는 없다.
- 현재 판정: MarketData PR-12와 MarketReference PR-14 장중 검증, P0-3 runtime execution lock fencing, P1-6 LIVE_SIM order-plan uniqueness, P0-4 durable broker boundary, 격리 replay, P1-2/P2-4 projection watermark/retention RCA, PR-15/16 market_index, PR-17 common context, PR-18/19 market_regime, PR-20 market_scan 준비 구현을 완료했다. market_scan inline path와 PR-20 effective-skip guard는 유지된다. 다음 구현 범위는 market_scan limited cutover이며 market-regime 운영 gate는 `PENDING_KRX_SESSION`/`PENDING_KOA_STUDIO_CONFIRMATION`이다.

## P0

| ID | 증상 | 코드 근거 | 운영 영향 | 최소 수정 방향 | 테스트/SQL 검증 |
|---|---|---|---|---|---|
| P0-1 Gateway ingestion request path에 inline projection이 남아 있음 (부분 완화) | `api/routes/gateway.py::post_gateway_event()`는 append/outbox 기록 후 routing decision을 평가한다. `market_data`는 PR-7/9/11/12, `market_reference`는 PR-14, `market_index`는 PR-16, `market_regime`/common context는 PR-19 guarded mode에서 inline projection을 건너뛸 수 있다. PR-20은 `market_scan` worker/reconcile/dry-run을 준비했지만 effective skip은 금지한다. `market_scan`과 `handle_live_sim_gateway_event()`는 여전히 request path에 남아 있다. | 제한된 market_data/reference/index/regime event는 worker로 이관할 수 있지만, scan/lifecycle inline projection 비용과 DB write lock 경합은 여전히 Gateway POST latency와 같은 critical path에 있다. | market_scan limited cutover를 별도 PR로 진행하고, 이후 lifecycle consumer의 durable retry/watermark를 확정한다. 최종적으로 POST는 raw append + outbox enqueue만 수행한다. | MarketData: `docs/runbook_market_data_append_only_controller_ko.md`. MarketReference: `docs/runbook_market_reference_projection_ko.md`. MarketIndex: `docs/runbook_market_index_projection_ko.md`. MarketRegime: `docs/runbook_market_regime_projection_ko.md`. MarketScan: `docs/runbook_market_scan_projection_ko.md`. SQL: `SELECT projection_name,event_type,status,COUNT(*) FROM projection_outbox GROUP BY 1,2,3;` |
| P0-2 Candidate quote refresh TR response synthetic price_tick `event_id` collision | 수정 완료. `_process_tr_response()`는 row별 synthetic child `GatewayEvent.event_id`를 deterministic하게 생성하고, parent metadata를 보존한다. 근거: `services/market_data_service.py::_synthetic_price_tick_event_id()`, `_with_synthetic_price_tick_metadata()`. | 다중 code row TR response가 `market_tick_samples.event_id` PK 충돌 없이 projection된다. | 유지보수 방향: child id 포맷과 metadata contract를 replay/reconcile 문서에 계속 고정한다. | 테스트: `tests/test_structural_audit_guards.py::test_synthetic_tr_response_multiple_ticks_uses_unique_child_event_ids`. SQL: `SELECT event_id, metadata_json FROM market_tick_samples WHERE event_id LIKE '%synthetic_price_tick%';` |
| P0-3 runtime execution lock lease/fencing | 수정 완료. lock row에 `process_id`, `thread_id`, `heartbeat_at`, `fencing_token`을 저장하고 별도 monotonic fence sequence를 유지한다. daemon heartbeat가 lease를 갱신하며 TTL이 지나도 owner process/thread가 살아 있으면 takeover를 차단한다. evaluation transaction과 cycle commit 경계는 현재 owner/token을 검증한다. startup cleanup은 expired/dead-owner 또는 self-owned row만 삭제한다. | 장시간 run의 TTL overlap, 다른 Core startup의 active-lock 삭제, stale owner latest-row write를 fail-closed로 차단한다. | 운영 상태는 operator API/dashboard fast/ops report로 확인한다. active owner를 수동 전체 삭제하지 않는다. | 테스트: `tests/test_evaluation_run_guard.py`, `tests/test_live_sim_operating_loop.py`, `tests/test_ops_runtime_execution_lock_check.py`. Runbook: `docs/runbook_runtime_execution_lock_ko.md`. |
| P0-4 order command broker boundary와 durable DB pre-ack | 수정 완료. schema 47의 `gateway_order_broker_boundaries`가 `CLAIMED`, `GATEWAY_STARTED`, `PRE_ACK_RECORDED`, `BROKER_ACCEPTED`, `CHEJAN_CONFIRMED`, `UNCONFIRMED`를 저장한다. Gateway는 local journal 이후 Core에 `order_pre_ack`를 동기 POST하고 `accepted=true`, `durable_pre_ack_recorded=true`를 모두 확인한 경우에만 Kiwoom 주문/cancel method를 호출한다. | Core 응답 실패/유실은 `DURABLE_DB_PRE_ACK_FAILED`로 fail-closed한다. timeout은 `UNCONFIRMED`로 격리하고 신규 BUY routing을 차단하되 lifecycle cancel은 허용한다. 늦은 ack/Chejan은 상태를 복구하며 out-of-order lower state는 역행시키지 않는다. | operator API, Dashboard fast section, ops report로 missing/gap/duplicate/mismatch와 historical unconfirmed를 확인한다. 기존 미확정 row를 evidence 없이 종결하지 않는다. | 테스트: `tests/test_gateway_order_broker_boundary.py`, `tests/test_ops_order_broker_boundary_check.py`, `tests/test_structural_audit_guards.py::test_order_command_lifecycle_detects_claimed_without_pre_ack`. Runbook: `docs/runbook_order_broker_boundary_ko.md`. Reports: `reports/order_broker_boundary_migration/20260710T012452Z/summary.md`, `reports/order_broker_boundary/20260710T013001Z/summary.md`. |

## P1

| ID | 증상 | 코드 근거 | 운영 영향 | 최소 수정 방향 | 테스트/SQL 검증 |
|---|---|---|---|---|---|
| P1-1 Incremental evaluation은 Candidate -> Strategy -> Risk까지만 처리하고 EntryTiming/OrderPlan은 별도 loop에 남아 있음 | `services/runtime/incremental_evaluation.py::process_incremental_evaluation_batch()`는 `refresh_candidate_context()`, `evaluate_candidate_strategy()`, `evaluate_risk_for_candidate()` 후 queue row를 삭제한다. `evaluate_entry_timing()`은 `services/runtime/market_open_observe_cycle.py`와 `services/runtime/live_sim_operating_orchestrator.py`에서 별도 호출된다. | price tick 하나의 변경이 order_plan까지 같은 run/snapshot으로 이어지지 않는다. Strategy/Risk latest와 EntryTiming/OrderPlan latest의 source 시점이 달라질 수 있다. | incremental worker에 선택적 EntryTiming/OrderPlan stage를 붙이거나, order_plan 생성 시 strategy/risk/candidate/tick source ids를 검증한다. `source_run_id`, `source_watermark`, `data_age_sec`를 latest row에 저장한다. | Dashboard mismatch guard: `test_dashboard_snapshot_mixed_latest_rows_are_detectable_by_guard_query`. SQL: strategy/risk/order_plan latest를 candidate_id로 join해 observation id 불일치를 집계한다. |
| P1-2 projection별 success/error watermark와 retention gate | 수정 완료. schema 48은 기존 `last_event_*` 호환 컬럼과 별도로 `last_success_*`, `last_error_*`를 저장하고 `(projection_name,event_id)` 최신 `SUCCESS/ERROR` ledger를 둔다. market-data inline 오류는 success watermark를 전진시키지 않으며 outbox terminal 상태와 result update는 같은 commit이다. | accepted replayable event는 모든 required outbox가 `APPLIED`, result가 `SUCCESS`, projection success watermark가 event rowid 이상일 때만 retention eligible이다. evidence 없는 legacy event, ERROR/DEAD_LETTER/SKIPPED/missing result는 fail-closed한다. | `EVENT_STORE_RETENTION_ENABLED=false`, `PROJECTION_EVENT_RESULT_BACKFILL_ENABLED=false`가 기본이다. 실제 prune은 retention enabled와 old blocker 0을 모두 요구하며, blocker 하나가 전체 apply를 HTTP 409로 막는다. legacy backfill은 APPLIED outbox만 대상으로 dry-run 기본이다. | 테스트: `tests/test_projection_watermarks_retention.py`, `tests/test_event_retention.py`. API: `/api/operator/projection-watermarks/*`, `/api/operator/event-retention/status`. Runbook: `docs/runbook_projection_watermark_retention_ko.md`. |
| P1-3 MarketRegime snapshot이 공통 market context가 아니라 후보별 refresh에서 rebuild될 수 있음 | 수정 완료. schema 51의 common snapshot/reference 계약과 PR-18 worker를 사용하며, PR-19 effective-skip worker는 cadence를 무시하고 current event-linked global regime 1개와 KOSPI/KOSDAQ context pair를 강제 생성한다. 일반 Candidate/API read는 snapshot을 생성하지 않는다. | 같은 평가 run의 후보가 공통 market 시점과 lineage를 공유한다. PR-19 fixture의 effective event는 linked regime/context `1/2`를 남겼고, prior worker closure 전에는 다음 skip을 거부한다. | legacy inline builder는 10거래일 evidence 후 flag cleanup까지 긴급 rollback으로 유지한다. KRX 장중/KOA Studio evidence 없이 기본값을 활성화하지 않는다. | 테스트: `tests/test_market_context_service.py`, `tests/test_market_regime_projection_worker.py`, `tests/test_gateway_market_regime_append_only_routing.py`. API: `/api/operator/market-regime-projection-reconcile/*`. Ops: `tools/ops_market_regime_projection_check.py`. Reports: `reports/market_regime_projection/20260710T063020Z/summary.md`, `reports/market_regime_cutover/20260710T070226Z/summary.md`. |
| P1-4 Market index parser_status가 UNVERIFIED일 때 후보/Risk 정책과 운영 표시가 충분히 분리되지 않음 (부분 수정) | PR-17 common context는 `parser_confidence_status`, `data_quality_status`, `trading_data_usable`을 별도 컬럼/evidence로 저장하고 candidate context/API/Dashboard/ops에 전달한다. 두 지수 data usable + FRESH regime quality + parser VERIFIED가 아니면 `trading_eligible=false`다. metadata 없는 legacy event는 계속 `UNKNOWN`이며 실제 adapter mapping은 `PILOT_UNVERIFIED_FID_MAP`이다. | data freshness가 좋아도 parser confidence가 검증되지 않으면 eligible로 승격되지 않고 operator status가 WARN이다. 다만 실제 KOA Studio FID 확인과 downstream Risk의 명시적 parser reason-code 정책은 아직 남아 있다. | KOA Studio로 FID mapping evidence를 남기고 market_regime worker/reconcile 단계에서 parser-unverified reason과 Risk/entry fail-closed 계약을 고정한다. | 테스트: `tests/test_market_context_service.py`, `tests/test_market_index_projection_reconcile.py`, `tests/test_gateway_market_index_limited_cutover.py`. Reports: `reports/market_context/20260710T055744Z/summary.md`, `reports/market_index_cutover_fixture/20260710T050254Z/summary.md`. |
| P1-5 Dashboard snapshot은 서로 다른 latest table을 같은 화면에 섞어 보여준다 | `services/dashboard_service.py::build_dashboard_snapshot()`이 market_data/theme/candidate/strategy/risk/entry/live_sim latest를 독립 조회한다. pipeline summary에는 `generated_at`만 있고 section별 `source_run_id`, `source_watermark`, `trade_date`, `data_age_sec`, `generated_by`가 없다. | "화면 상태가 동일 평가 run 기준인가?"를 판단할 수 없다. 오래된 order_plan과 최신 risk가 함께 PASS처럼 보일 수 있다. | dashboard `coherency` section을 추가하고 section별 source metadata를 표준화한다. stage row에도 `source_run_id`, `source_watermark`, `data_age_sec`, `trade_date`, `generated_by`를 넣는다. | 추가 테스트: `test_dashboard_snapshot_mixed_latest_rows_are_detectable_by_guard_query`. |
| P1-6 `order_plan_id` 기반 duplicate 방지가 JSON evidence scan에 의존 | 수정 완료. schema 46은 `live_sim_intents.order_plan_id` nullable 컬럼과 `uq_live_sim_intents_order_plan_id` partial UNIQUE index를 추가한다. 기존 JSON evidence는 savepoint migration으로 backfill하고 duplicate, 컬럼/JSON mismatch, invalid order-plan evidence가 있으면 startup을 fail-closed한다. 조회는 `WHERE order_plan_id = ?` 직접 index lookup이다. | 최근 500건 제한과 evidence scan에 의존하지 않고 DB가 order plan당 intent 1건을 강제한다. candidate 기반 일반 intent의 `NULL`은 여러 건 허용한다. | operator API, Dashboard fast section, ops report로 column/index/backfill/duplicate 상태를 상시 확인한다. 운영 duplicate를 임의 삭제하지 않는다. | 테스트: `tests/test_live_sim_order_plan_uniqueness.py`, `tests/test_ops_live_sim_order_plan_uniqueness_check.py`. Runbook: `docs/runbook_live_sim_order_plan_uniqueness_ko.md`. Report: `reports/live_sim_order_plan_uniqueness/20260710T004658Z/summary.md`. |

## P2

| ID | 증상 | 코드 근거 | 운영 영향 | 최소 수정 방향 | 테스트/SQL 검증 |
|---|---|---|---|---|---|
| P2-1 incremental queue status는 backlog/stale age alert가 없다 | `services/runtime/incremental_evaluation.py::get_incremental_evaluation_status()`는 `queued_count`, `retry_exhausted_count`, `oldest_enqueued_at`, `max_attempts`를 반환하지만 stale threshold 평가와 reason code는 없다. | queue가 오래 쌓여도 dashboard/operator가 즉시 `STALE_QUEUE`로 해석하기 어렵다. | status에 `oldest_age_sec`, `stale_queue_count`, `backlog_status`, `reason_codes`를 추가한다. retry exhausted row는 별도 dead-letter/retry reset 운영 경로를 둔다. | 추가 테스트: `test_incremental_queue_backlog_and_stale_rows_are_detectable`. SQL: `SELECT COUNT(*),MIN(enqueued_at),MAX(attempts) FROM incremental_evaluation_queue;` |
| P2-2 top theme DB/leadership source가 둘 다 표시되지만 coherency warning은 제한적 | `services/dashboard_service.py`는 DB top tradable과 leadership fallback을 함께 다루며 `DASHBOARD_SAMPLE_LIMIT_HIDES_TRADABLE_THEME`는 있다. 그러나 DB snapshot과 leadership snapshot의 생성 run/watermark는 표시하지 않는다. | 운영자가 DB top theme와 leadership top theme가 같은 관측 universe인지 구분하기 어렵다. | theme/leadership section에 `source`, `snapshot_id`, `calculated_at`, `data_age_sec`, `watchset_selection_source`를 동일 포맷으로 표시한다. | 기존 테스트: `tests/test_dashboard_service.py::test_dashboard_top_theme_query_does_not_hide_tradable_themes_behind_latest_sample`. |
| P2-3 Market index TR bootstrap 설정은 status에 보이지만 Core projection과 bootstrap replay 경계가 약함 (부분 수정) | PR-15는 event source를 `REALTIME/TR_BOOTSTRAP/UNKNOWN`으로 분류하고 Dashboard에 realtime source와 `CONFIGURED_NOT_IMPLEMENTED` bootstrap 상태를 별도로 표시한다. 현재 Gateway `_market_index_adapter_health()`도 TR-only 설정을 `TR_BOOTSTRAP_NOT_IMPLEMENTED`로 판정한다. | realtime 결과를 bootstrap evidence로 오인하지 않으며, 구현되지 않은 TR bootstrap event는 reconcile FAIL이다. 다만 실제 TR child event/projection은 아직 없다. | bootstrap TR 응답을 deterministic `market_index_tick` child event 또는 별도 snapshot으로 구현하고 replay/source lineage를 추가한다. | 테스트: `tests/test_market_index_projection_reconcile.py::test_market_index_reconcile_rejects_tr_bootstrap_until_adapter_exists`. |
| P2-4 event별 projection-retention RCA | 수정 완료. `projection_retention_event_rca` SQL view와 read-only RCA service가 Gateway/raw event, required outbox, result, success/error watermark, replay availability, KRX/NXT venue, eligibility reason을 event_id 단위로 결합한다. | 운영자는 `PROJECTION_OUTBOX_MISSING/NOT_APPLIED`, `PROJECTION_RESULT_MISSING/ERROR`, `PROJECTION_SUCCESS_WATERMARK_BEHIND`, raw/command 보호 사유를 API/Dashboard에서 바로 확인한다. | Dashboard full/fast `projection_watermarks`, `projection_retention`, errors RCA card와 ops report를 추가했다. fast status는 bounded blocker probe, exact count는 명시적 ops 요청으로 분리한다. | API: `/api/operator/projection-retention/rca`. Ops: `tools/ops_projection_retention_check.py`. Report: `reports/projection_retention/20260710T031456Z/summary.md`. |

## 추가된 Guard Tests

이번 audit 및 PR-2에서 보강한 파일: `tests/test_structural_audit_guards.py`

- `test_synthetic_tr_response_multiple_ticks_uses_unique_child_event_ids`
- `test_projection_outbox_enqueues_shadow_jobs_for_projection_events`
- `test_projection_outbox_duplicate_event_id_does_not_create_duplicate_job`
- `test_projection_outbox_enqueues_market_scan_for_scan_related_tr_response`
- `test_projection_outbox_excludes_non_projection_gateway_events`
- `test_runtime_execution_lock_can_be_reacquired_after_ttl_while_owner_still_running`
- `test_dashboard_snapshot_mixed_latest_rows_are_detectable_by_guard_query`
- `test_order_command_lifecycle_detects_dispatched_without_pre_ack`
- `test_incremental_queue_backlog_and_stale_rows_are_detectable`

실행:

```powershell
python -m pytest tests\test_structural_audit_guards.py -q
```

결과:

```text
9 passed
```

PR-6 추가 테스트:

- `tests/test_gateway_market_data_append_only_routing.py`
- `tests/test_ops_market_data_append_only_routing_check.py`

핵심 검증:

- dry-run disabled 기본 동작에서 inline projection 유지
- reconcile PASS/outbox ready일 때만 `would_skip_inline=True`
- reconcile missing/FAIL/stale이면 BLOCKED
- cutover flag가 켜져도 PR-6에서는 `effective_skip_inline=False`
- condition_event/tr_response도 allowlist 안에서 decision 기록
- operator status/dashboard/ops script 판정 포함

PR-7 추가 테스트:

- `tests/test_gateway_market_data_price_tick_cutover.py`
- `tests/test_projection_outbox_price_tick_deferred_incremental.py`
- `tests/test_ops_market_data_price_tick_cutover_check.py`

핵심 검증:

- 기본 설정에서는 `price_tick`도 inline projection 유지
- strict flags, fresh reconcile PASS, worker apply enabled, skip budget 조건에서만 `price_tick` effective skip 발생
- `condition_event`/`tr_response`는 cutover flag가 켜져도 inline projection 유지
- budget exhausted, reconcile fail, worker apply disabled 시 inline fallback
- worker apply 후 `market_tick_samples`, `market_ticks_latest`, `incremental_evaluation_queue`와 deferred evidence 기록
- operator status/dashboard/ops script가 invalid effective skip과 deferred enqueue 누락을 감지

PR-8 추가 테스트:

- `tests/test_market_data_tr_response_side_effects.py`
- `tests/test_projection_outbox_tr_response_deferred_side_effect.py`
- `tests/test_ops_market_data_tr_response_side_effect_check.py`

핵심 검증:

- candidate quote refresh `tr_response` side-effect 서비스가 code별 incremental queue를 생성하고 empty/partial error를 구분
- Gateway inline `tr_response` projection과 기존 incremental enqueue 응답 의미 유지
- worker가 직접 `tr_response` market_data projection을 적용한 경우에만 deferred candidate quote refresh side-effect 기록
- inline already applied verify 경로에서는 duplicate side-effect 미실행
- PR-8 기준에서는 `tr_response`/`condition_event` cutover flag가 켜져도 `effective_skip_inline=0`
- operator status/dashboard/ops script가 PR-8 금지 조건과 side-effect error를 감지

PR-9 진행 상태:

- `tr_response` 한정 market_data inline skip을 strict feature flags, fresh reconcile PASS, worker apply enabled, synthetic child guard, per-minute budget 뒤에서 허용
- 기본값은 `GATEWAY_MARKET_DATA_APPEND_ONLY_TR_RESPONSE_CUTOVER_ENABLED=false`, `GATEWAY_MARKET_DATA_APPEND_ONLY_TR_RESPONSE_MAX_SKIP_PER_MINUTE=0`으로 production behavior 변경 없음
- `condition_event`는 PR-9에서도 inline 유지, effective skip 발견 시 reconcile/operator check에서 FAIL
- skipped `tr_response`의 candidate quote refresh enqueue는 projection_outbox worker가 market_data apply 성공 후 deferred side-effect로 수행
- operator status/dashboard/reconcile/ops script에 tr_response budget, pending worker, worker applied, deferred quote refresh, invalid skip counters 추가
- 추가 테스트:
  - `tests/test_gateway_market_data_tr_response_cutover.py`
  - `tests/test_projection_outbox_tr_response_cutover_worker.py`
  - `tests/test_market_data_reconcile_tr_response_cutover.py`
  - `tests/test_ops_market_data_tr_response_cutover_check.py`

PR-10 진행 상태:

- `condition_event`의 후속 `condition_fusion` refresh를 `services/runtime/market_data_projection_side_effects.py` service로 분리
- Gateway inline `condition_event` projection과 `condition_fusion` refresh는 그대로 유지
- projection_outbox worker가 직접 `condition_event` market_data projection을 적용한 경우에만 deferred `condition_fusion` refresh evidence를 기록
- inline already-applied `condition_event`는 worker verify-only로 처리하며 side-effect를 중복 실행하지 않음
- worker evidence에 `candidate_ingest_executed=false`, `no_order_side_effects=true`, `no_trading_side_effects=true`를 기록
- `condition_event effective_skip_inline`은 PR-10에서도 금지되며, 발견 시 operator/reconcile/ops script에서 FAIL
- `candidate_service.ingest_condition_sources()`는 worker에서 호출하지 않으며 후보 ingest는 기존 pipeline에 남김
- operator status/dashboard/reconcile/ops script에 condition_event worker-side readiness, deferred fusion refresh, duplicate side-effect, candidate ingest guard counters 추가
- 추가 테스트:
  - `tests/test_market_data_condition_event_side_effects.py`
  - `tests/test_projection_outbox_condition_event_deferred_side_effect.py`
  - `tests/test_market_data_reconcile_condition_event_side_effect.py`
  - `tests/test_ops_market_data_condition_event_side_effect_check.py`

PR-10.5 진행 상태:

- Dashboard snapshot API가 `sections`, `fast`, `timeout_budget_ms` query를 지원하도록 추가했다.
- `sections`/`fast=true` 요청은 필요한 read-only 섹션만 계산하는 fast path를 사용한다. 기존 full dashboard snapshot은 호환 유지된다.
- cache key는 `db_path/detail/limit/sections/fast` 기준으로 분리했고, cache lock은 read/write 순간에만 잡는다.
- PR-10 ops script는 targeted fast snapshot을 사용하며, core condition_event/outbox/reconcile 실패와 dashboard API timeout을 verdict에서 분리한다.
- 주문/LIVE_SIM/LIVE_REAL, market_data projection routing, cutover 정책은 변경하지 않았다.
- 추가 테스트:
  - `tests/test_dashboard_snapshot_sections.py`
  - `tests/test_dashboard_snapshot_fast_path.py`
  - `tests/test_ops_dashboard_timeout_handling.py`

PR-10.6 진행 상태:

- 장중 ingest 중 operator run-once가 SQLite write lock과 경합할 때 HTTP 500으로 종료되지 않도록 `storage/sqlite_locking.py` retry helper와 `LOCKED_RETRYABLE` 응답을 추가했다.
- `/api/operator/projection-outbox/run-once`는 live-safe 기본값으로 batch를 `PROJECTION_OUTBOX_LIVE_RUN_ONCE_BATCH_SIZE`까지 clamp하고, lock retry 횟수와 stale PROCESSING reset 수를 결과에 노출한다.
- operator run-once connection은 `OPERATOR_SQLITE_BUSY_TIMEOUT_MS`를 사용해 SQLite lock을 길게 붙잡지 않고 retry/`LOCKED_RETRYABLE` 경로로 빠르게 넘긴다.
- `/api/operator/market-data-projection-reconcile/run-once`는 `live_safe=true`에서 기본 persist를 끄고, persist 요청 중 lock이 발생하면 read-only reconcile fallback을 시도한다. fallback도 lock이면 `LOCKED_RETRYABLE`로 반환한다.
- PR-7/8/9/10 ops scripts는 `409` 또는 body status `LOCKED_RETRYABLE`을 짧게 재시도하며, 재시도 소진 시 core 검증 실패가 아니라 WARN + `block_next_pr=true`로 분류한다.
- 주문/LIVE_SIM/LIVE_REAL, market_data cutover policy, buy/safety gate는 변경하지 않았다.
- 운영 절차는 `docs/runbook_sqlite_lock_contention_ko.md`에 정리했다.
- 추가 테스트:
  - `tests/test_operator_sqlite_lock_handling.py`
  - `tests/test_projection_outbox_lock_retry.py`
  - `tests/test_market_data_reconcile_lock_fallback.py`
  - `tests/test_ops_sqlite_locked_retryable.py`

PR-10.7 진행 상태:

- `projection_outbox` backlog를 projection_name/event_type/status/age 기준으로 진단하는 read-only service를 추가했다.
- `/api/operator/projection-outbox/backlog`는 backlog readiness, recent pending, stale PROCESSING, condition_event pending, latest reconcile/routing guard 상태를 한 번에 반환한다.
- `/api/operator/projection-outbox/drain-once`는 token 보호 하에 live_safe batch를 반복 실행할 수 있지만, 기존 worker apply 설정을 완화하지 않는다.
- operator status/dashboard fast path/pipeline_summary에 `backlog_readiness_status`, `pr11_condition_event_cutover_ready`, recent/condition_event pending, stale count, operator action을 노출한다.
- PR-7/9/10 ops script는 core PASS와 backlog WARN을 분리해 표시하며, PR-11 condition_event cutover 가능 여부를 별도 필드로 표시한다.
- 새 drain 운영 스크립트 `tools/ops_projection_outbox_backlog_drain.py`와 `docs/runbook_projection_outbox_backlog_ko.md`를 추가했다.
- 주문/LIVE_SIM/LIVE_REAL, safety gate, buy gate, price_tick/tr_response/condition_event cutover policy는 변경하지 않았다.
- 추가 테스트:
  - `tests/test_projection_outbox_backlog_status.py`
  - `tests/test_projection_outbox_drain_once_api.py`
  - `tests/test_ops_projection_outbox_backlog_drain.py`
  - `tests/test_dashboard_snapshot_fast_path.py`

PR-10.8 진행 상태:

- `projection_outbox` bulk shadow retire를 추가했다. 이미 Gateway inline projection artifact 또는 projection error로 처리 여부를 확인할 수 있는 old shadow job만 `APPLIED`/`SKIPPED`로 terminal 처리한다.
- bulk retire는 projection table을 생성/수정하지 않고 `projection_outbox.status`, `processed_at`, `updated_at`, `metadata_json`만 갱신한다.
- `effective_skip_inline=True`, prior error attempt, missing/failed gateway event, artifact/error/skip-safe evidence가 없는 job은 bulk retire 대상에서 제외한다.
- backlog readiness를 raw pending 중심에서 `blocking_pending_count`, `non_blocking_shadow_pending_count`, `bulk_retire_eligible_count`, `condition_event_blocking_pending_count`, `effective_skip_pending_count` 기준으로 분리했다.
- PR-11 `condition_event` cutover readiness는 전체 pending이 아니라 blocking pending, recent pending, effective skip, latest reconcile, error/dead_letter 기준으로 판단한다.
- `/api/operator/projection-outbox/bulk-retire`와 `tools/ops_projection_outbox_bulk_retire.py`를 추가했다. 기본값은 dry-run이며, apply도 주문/LIVE_SIM/LIVE_REAL side effect 없이 outbox metadata/status만 갱신한다.
- dashboard fast path/pipeline_summary에 bulk retire eligibility, blocking backlog, recommended action을 노출한다.
- 운영 절차는 `docs/runbook_projection_outbox_bulk_retire_ko.md`에 정리했다.
- 2026-07-09 장중 실제 Kiwoom 데이터로 OBSERVE safe env(`TRADING_MODE=OBSERVE`, `TRADING_ALLOW_LIVE_SIM=false`, `TRADING_ALLOW_LIVE_REAL=false`)에서 검증했다. dry-run 500건, apply 500건이 완료됐고 apply 결과는 `APPLIED=498`, `SKIPPED=2`, net pending `-370`이었다. `market_projection_errors`/`market_index_projection_errors` 최근 10분 0건, recent gateway command 0건이었다. 단, live ingest 유입으로 `RECENT_OUTBOX_BACKLOG`, `CONDITION_EVENT_*_BACKLOG`가 남아 PR-11 readiness는 계속 `FAIL`이다.
- 주문/LIVE_SIM/LIVE_REAL, safety gate, buy gate, price_tick/tr_response/condition_event cutover policy, Gateway ingest path는 변경하지 않았다.
- 추가 테스트:
  - `tests/test_projection_outbox_bulk_retire.py`
  - `tests/test_projection_outbox_backlog_blocking_readiness.py`
  - `tests/test_projection_outbox_drain_once_api.py`
  - `tests/test_ops_projection_outbox_bulk_retire.py`
  - `tests/test_ops_projection_outbox_backlog_drain.py`

PR-11 진행 상태:

- `condition_event`에 한해서 market_data inline projection skip을 strict feature flag, fresh reconcile PASS, append_only_ready, projection_outbox worker apply enabled, condition_fusion enabled, backlog readiness, per-minute skip budget 뒤에서 허용했다.
- 기본값은 `GATEWAY_MARKET_DATA_APPEND_ONLY_CONDITION_EVENT_CUTOVER_ENABLED=false`, `GATEWAY_MARKET_DATA_APPEND_ONLY_CONDITION_EVENT_MAX_SKIP_PER_MINUTE=0`으로 유지해 production behavior는 변하지 않는다.
- Gateway path에서 effective skipped `condition_event`는 `process_gateway_event()`와 Gateway-side `condition_fusion` refresh를 실행하지 않고, `projection_outbox` worker가 market_data apply 후 deferred `condition_fusion` refresh를 수행한다.
- worker metadata와 routing decision evidence에 `candidate_ingest_executed=false`, `no_order_side_effects=true`, `no_trading_side_effects=true`, condition code/action, budget/backlog 상태를 기록한다.
- candidate ingest는 worker에서 실행하지 않는다. `candidate_service.ingest_condition_sources()` 호출은 기존 pipeline 밖으로 이동하지 않았다.
- reconcile은 effective skipped `condition_event`의 worker PENDING/PROCESSING within SLA를 WARN으로, terminal APPLIED/SKIPPED 이후 artifact missing이나 deferred fusion evidence missing/error를 FAIL로 분리한다.
- operator status/dashboard/ops script에 PR-11 condition_event cutover status, pending worker, worker applied, deferred fusion refresh, candidate ingest forbidden, artifact missing, rollback hint를 노출했다.
- price_tick PR-7, tr_response PR-9, market_reference/index/regime/scan/live_sim Gateway handling, 주문/LIVE_SIM/LIVE_REAL, safety/buy gate는 변경하지 않았다.
- 운영 절차는 `docs/runbook_market_data_condition_event_cutover_ko.md`에 정리했다.
- 2026-07-09 장중 OBSERVE-safe 검증은 최종 `PASS`였다. `effective_skip=3`, worker `APPLIED=3`, deferred fusion refresh `3`, candidate ingest `0`, pending worker `0`, fusion error `0`, artifact missing `0`, backlog readiness `PASS`, PR-11 cutover ready `True`를 확인했다. 근거: `reports/market_data_condition_event_cutover/20260709T042254Z/summary.md`.
- 추가 테스트:
  - `tests/test_gateway_market_data_condition_event_cutover.py`
  - `tests/test_projection_outbox_condition_event_cutover_worker.py`
  - `tests/test_market_data_reconcile_condition_event_cutover.py`
  - `tests/test_ops_market_data_condition_event_cutover_check.py`

PR-12 진행 상태:

- MarketData append-only controller를 추가해 `price_tick`/`tr_response`/`condition_event` limited cutover를 `OFF`/`DRY_RUN`/event-only/`MARKET_DATA_LIMITED`/`MARKET_DATA_FULL_GUARDED` operating mode 아래에서 중앙 제어한다.
- 기본값은 `GATEWAY_MARKET_DATA_APPEND_ONLY_OPERATING_MODE=OFF`, `GATEWAY_MARKET_DATA_APPEND_ONLY_GLOBAL_KILL_SWITCH=true`, `GATEWAY_MARKET_DATA_APPEND_ONLY_GLOBAL_MAX_SKIP_PER_MINUTE=0`으로 유지해 production behavior는 변하지 않는다.
- global kill switch와 auto rollback gate는 event-specific cutover flag보다 우선하며, controller가 PASS하지 않으면 routing service가 effective skip을 막고 inline fallback으로 되돌린다.
- auto rollback은 projection outbox ERROR/DEAD_LETTER, invalid effective skip, candidate ingest in worker, reconcile FAIL, append_only_not_ready after skip, backlog FAIL, stale PROCESSING, deferred side-effect error를 DB event로 기록한다. 설정 파일은 자동 수정하지 않는다.
- operator API와 dashboard fast path에 `market_data_append_only_controller` status, event-type gate, global budget, rollback hint, no trading/order side-effect evidence를 추가했다.
- 신규 ops script `tools/ops_market_data_append_only_controller_check.py`와 운영 절차 `docs/runbook_market_data_append_only_controller_ko.md`를 추가했다.
- price_tick PR-7, tr_response PR-9, condition_event PR-11 개별 cutover는 유지하되 중앙 controller를 통과해야 한다. market_reference/index/regime/scan 분리, `handle_live_sim_gateway_event`, 주문/LIVE_SIM/LIVE_REAL, safety/buy gate는 변경하지 않았다.
- 2026-07-09 Core를 scenario별 OBSERVE-safe로 재기동하고 API reconcile을 포함한 matrix를 검증했다. `PRICE_TICK_ONLY`, `TR_RESPONSE_ONLY`, `CONDITION_EVENT_ONLY`는 각 budget `1/min`, `MARKET_DATA_LIMITED`는 global budget `3/min`에서 `PASS`; reconcile `PASS/append_only_ready=True`, backlog `PASS`, outbox `ERROR/DEAD_LETTER=0/0`, rollback 없음이었다. `DRY_RUN`은 effective gate가 닫힌 예상 동작으로 `WARN`이었다. `.env`와 LIVE_SIM/LIVE_REAL 설정은 변경하지 않았다. 근거: `reports/market_data_append_only_controller_api_matrix/20260709_144713/summary.md`.
- 추가 테스트:
  - `tests/test_market_data_append_only_controller.py`
  - `tests/test_gateway_market_data_append_only_controller_routing.py`
  - `tests/test_dashboard_market_data_append_only_controller.py`
  - `tests/test_ops_market_data_append_only_controller_check.py`

PR-13 진행 상태:

- `market_reference` projection worker apply 준비 경로를 추가했다. `projection_outbox` job 중 `projection_name='market_reference'`, `event_type='market_symbols'`만 처리하며, 기본 verify-only에서는 `market_symbol_memberships` inline artifact만 확인한다.
- worker가 `process_market_symbols_event()`를 호출하는 조건은 `PROJECTION_OUTBOX_APPLY_PROJECTION_ENABLED=true`와 `PROJECTION_OUTBOX_MARKET_REFERENCE_APPLY_ENABLED=true`가 모두 켜진 run-once apply mode뿐이다. 기본값은 disabled다.
- Gateway path의 `process_market_symbols_event()` inline 호출은 그대로 유지했다. PR-13은 market_reference cutover가 아니며, `market_reference effective_skip_inline`은 항상 `False`다.
- accepted `market_symbols` event, membership coverage, payload shape(dict/list/other), outbox status, latest freshness를 대조하는 `market_reference_projection_reconcile`을 추가했다.
- `market_reference` append-only dry-run routing decision을 추가했다. 조건이 모두 맞으면 `would_skip_inline=True` evidence만 남기고, cutover flag가 켜져도 `MARKET_REFERENCE_EFFECTIVE_SKIP_DISABLED_IN_PR13` reason과 함께 inline fallback을 유지한다.
- operator API, dashboard fast path, `tools/ops_market_reference_projection_check.py`, `docs/runbook_market_reference_projection_ko.md`를 추가했다.
- market_data append-only controller/price_tick/tr_response/condition_event, market_index/regime/scan/live_sim Gateway handling, 주문/LIVE_SIM/LIVE_REAL, safety/buy gate는 변경하지 않았다.
- OBSERVE 런처가 LIVE_SIM 파일럿 `.env`의 operating/cancel/exit/reprice 플래그를 상속하지 않도록 모든 주문 side-effect 플래그를 process-local override에서 강제로 차단했다. PR-13 검증 모드에서는 background projection worker, market_data apply, periodic fusion/incremental/retention worker도 끄고 reference 수동 run-once만 허용한다.
- 대용량 운영 DB reconcile을 위해 `gateway_events(event_type,status)` index를 추가하고 membership을 한 번만 preload한다. 과거 snapshot의 superseded evidence는 종목별이 아니라 event별로 집계하며, outbox rollout 이전 event는 legacy INFO로 분리한다. 실제 reconcile latency는 30초 timeout 초과에서 약 `348ms`로 줄었다.
- `/api/operator/projection-outbox/run-once`에 optional `projection_name` filter를 추가했다. `projection_name=market_reference` batch는 오래된 market_data backlog를 claim하거나 terminal 처리하지 않는다.
- 2026-07-10 08:22 KST NXT 세션에서 실제 Kiwoom Gateway `exchange=NXT`, login/heartbeat, realtime callback을 확인했다. latest routing은 `would_skip_inline=true`, `effective_skip_inline=false`, 주문 command queue `0`, `order_commands_allowed=false`였다.
- reference-only worker `limit=1` 결과는 `APPLIED_BY_VERIFY=1`, error/dead-letter `0`, `no_trading_side_effects=true`였다. 후속 reconcile은 `PASS/append_only_ready=true`, membership `4053`, missing `0`, outbox `APPLIED=7`, `SKIPPED=15`, `ERROR/DEAD_LETTER=0/0`이었다.
- 공식 ops report는 `PASS`이며 worker mode/result까지 포함한다. 근거: `reports/market_reference_projection/20260709T232734Z/summary.md`.
- market_reference 및 관련 outbox/Gateway/dashboard/controller/structural/config/SQLite 18개 테스트 파일 회귀 배치를 통과했다.
- 추가 테스트:
  - `tests/test_market_reference_projection_worker.py`
  - `tests/test_market_reference_projection_reconcile.py`
  - `tests/test_gateway_market_reference_append_only_routing.py`
  - `tests/test_dashboard_market_reference_section.py`
  - `tests/test_ops_market_reference_projection_check.py`

PR-14 진행 상태:

- production 기본값은 `cutover=false`, `global_kill_switch=true`, `max_skip_per_minute=0`, legacy PR-13 guard ON이다. `-MarketReferenceLimitedCutover` launcher switch에서만 process-local로 cutover ON, kill switch OFF, budget `1/min`, worker apply ON을 설정한다.
- schema version 44에 venue와 무관한 단일 `market_reference_append_only_budget_state` row를 추가했다. SQLite upsert reservation은 같은 minute bucket에서 limit을 넘는 increment를 원자적으로 거부하므로 NXT/KRX 전환이나 Gateway 재시작으로 budget이 초기화되지 않는다.
- effective skip gate는 accepted/non-empty `market_symbols`, current outbox ready, fresh reconcile `PASS/append_only_ready`, membership minimum, worker apply, market_reference pending/processing/error/dead-letter health, 이전 effective skip의 durable `APPLIED_BY_WORKER` evidence를 모두 요구한다.
- gate 실패, routing exception, worker pending/error, latest artifact missing 시 `process_market_symbols_event()` inline fallback을 유지한다. latest snapshot이 과거 event_id를 supersede하는 정상 동작은 durable worker evidence로 판별해 오탐 rollback하지 않는다.
- Dashboard fast summary와 operator routing status는 controller `PASS/WARN/FAIL`, kill switch, decision/current-minute budget, rollback reasons, effective-skip worker health를 노출한다. ops script의 `--run-worker --expect-effective-skip`는 latest actual decision과 latest market_symbols event id, venue/login, decision budget `1/1`, worker `APPLIED_BY_WORKER`, dashboard count 일치를 검증한다.
- 2026-07-10 08:53 KST 실제 Kiwoom NXT Gateway에서 `kiwoom_logged_in=true`, `server_mode=SIMULATION`, `realtime_exchange=NXT`, heartbeat fresh를 확인했다. actual `market_symbols` event `evt_b0538e276e4344c984ef676f0620019a`가 `effective_skip_inline=true`였고 decision evidence는 budget limit/used `1/1`이었다.
- skip 직후 controller는 worker 미적용을 감지해 fail-closed `FAIL`이었고, reference-only `limit=1` worker가 `APPLIED_BY_WORKER=1`로 membership을 생성한 뒤 reconcile/controller가 `PASS`로 복구됐다. membership `4053`, missing `0`, effective-skip pending/artifact/evidence-missing `0/0/0`, outbox pending/error/dead-letter `0/0/0`, rollback `false`였다.
- limited Core 시작 시각 이후 신규 Gateway command와 주문형 command는 각각 `0/0`이었다. command queue `0`, `order_commands_allowed=false`, worker `no_trading_side_effects=true`를 확인했으며 검증 후 Core/Gateway PID와 8000 listener를 모두 종료했다.
- 공식 ops report는 `PASS`다. 근거: `reports/market_reference_projection/20260710T000023Z/summary.md`; decision budget/venue raw evidence는 같은 디렉터리 `raw.json`에 있다.
- market_reference/Gateway/outbox/dashboard/config/SQLite 영향 범위 회귀와 full pytest suite를 통과했다. runbook: `docs/runbook_market_reference_projection_ko.md`.

P0-3 runtime execution lock 진행 상태:

- schema version 45에서 `runtime_execution_locks`에 `process_id`, `thread_id`, `heartbeat_at`, `fencing_token`을 추가하고 `runtime_execution_lock_fences`에 lock별 monotonic token을 보존한다. release와 cleanup은 fence sequence를 삭제하지 않는다.
- `runtime_execution_lock()`은 별도 SQLite connection을 사용하는 daemon heartbeat로 lease를 갱신한다. TTL이 수동 만료돼도 owner PID/thread가 살아 있으면 `OWNER_ALIVE_AFTER_TTL`로 takeover를 거부한다.
- lock takeover가 발생한 경우 stale lease의 `immediate_transaction()`과 context 종료는 `EVALUATION_RUN_FENCE_LOST`로 차단된다. Strategy/Risk/EntryTiming transaction, observe-cycle final run write, incremental candidate commit, theme-refresh run write에 fence 검증을 연결했다.
- Core startup cleanup은 현재 process 소유 residual row와 expired/dead-owner row만 제거한다. active 타 process row와 expired-but-alive row는 보존한다. 기존 live-sim stop/runbook의 "모든 residual lock 자동 삭제" 문구도 수정했다.
- `GET /api/operator/runtime-execution-locks/status`, operator aggregate status, Dashboard fast `runtime_execution_locks`/pipeline summary, `tools/ops_runtime_execution_lock_check.py`를 추가했다. 상태는 `ACTIVE`, `EXPIRED_OWNER_ALIVE`, `STALE_EXPIRED`를 구분한다.
- heartbeat renewal, live-owner TTL takeover 거부, release 이후 token 증가, stale-owner write rollback, old schema migration, startup selective cleanup, API/dashboard/ops 계약을 테스트했다. lock 영향 범위 회귀 157개 테스트를 통과했다.
- 2026-07-10 09:21 KST OBSERVE-safe Core API에서 idle lock `PASS`, lock/active/stale/expired-alive `0/0/0/0`, no trading side effects를 확인했다. 공식 report: `reports/runtime_execution_lock/20260710T002121Z/summary.md`.
- 검증 후 Core supervisor와 inherited socket child를 모두 종료했다. 최종 상태는 health unreachable, 8000 listener `0`, Python runtime process `0`이다.

P1-6 LIVE_SIM order-plan uniqueness 진행 상태:

- schema version 46에서 `live_sim_intents.order_plan_id` nullable 컬럼과 `order_plan_id IS NOT NULL` partial UNIQUE index `uq_live_sim_intents_order_plan_id`를 추가했다. candidate 기반 일반 intent는 `NULL`을 유지한다.
- migration은 savepoint 안에서 기존 `evidence_json.order_plan_id`를 검사한다. duplicate, 컬럼/JSON mismatch, invalid order-plan evidence가 있으면 backfill과 index 생성을 rollback하고 Core startup을 fail-closed한다. 구버전 DB migration과 재실행 호환을 테스트했다.
- order-plan intent 모델과 insert가 컬럼을 직접 기록하며 `find_live_sim_intent_by_order_plan()`은 최근 500건 JSON scan 대신 `WHERE order_plan_id = ? LIMIT 1`을 사용한다. 501개 newer generic intent 뒤의 기존 계획도 직접 조회되고 두 번째 non-null 계획 intent는 DB UNIQUE가 거부한다.
- `GET /api/operator/live-sim/order-plan-uniqueness/status`, operator aggregate status, Dashboard fast `live_sim_order_plan_uniqueness`/pipeline summary, `tools/ops_live_sim_order_plan_uniqueness_check.py`를 추가했다. runbook은 `docs/runbook_live_sim_order_plan_uniqueness_ko.md`다.
- 운영 DB를 read-only SQLite backup으로 복제한 격리 DB에서 7건 backfill, partial UNIQUE 생성, schema 46 재실행을 검증했다. 1차/2차 상태 모두 `PASS`, duplicate/mismatch/missing backfill `0/0/0`이었다.
- full pytest suite 863개와 LIVE_SIM/dashboard/SQLite 영향 범위 회귀를 통과했다. duplicate, mismatch, nullable partial UNIQUE, direct lookup, API/dashboard/ops fail-closed 계약을 포함한다.
- 2026-07-10 09:46 KST OBSERVE-safe Core API에서 `profile/mode=OBSERVE/OBSERVE`, LIVE_SIM/LIVE_REAL false, kill switch true, uniqueness/dashboard `PASS`, intent/backfill `7/7`, duplicate/mismatch/missing `0/0/0`을 확인했다. 과거 order command 5건은 유지됐고 점검 전후 total/order command delta는 `0/0`이었다. 공식 report: `reports/live_sim_order_plan_uniqueness/20260710T004658Z/summary.md`.
- 검증 후 Core PID를 종료했다. 최종 상태는 health unreachable, 8000 listener `0`, Python runtime process `0`이었다. P1-6 진입 조건 `PASS`를 충족해 후속 P0-4 broker boundary로 진행했다.

P0-4 durable order broker boundary 진행 상태:

- schema version 47에 `gateway_order_broker_boundaries`와 nullable idempotency partial UNIQUE, state/update, broker order number index를 추가했다. 500만 건 이상의 market-only event를 불필요하게 색인하지 않도록 `gateway_events(command_id,event_type,received_at) WHERE command_id IS NOT NULL` partial index를 사용한다.
- Core poll은 order command를 `QUEUED -> CLAIMED`로 전환한다. `command_started`, `order_pre_ack`, `command_ack`, `kiwoom_order_chejan`/`execution_event`는 각각 `GATEWAY_STARTED`, `PRE_ACK_RECORDED`, `BROKER_ACCEPTED`, `CHEJAN_CONFIRMED`로 기록하며 lower/out-of-order event가 상태를 되돌리지 않는다.
- Kiwoom `send_order`/`cancel_order` handler는 local pre-ack journal을 기록한 뒤 Core `/api/gateway/events`에 동기 POST한다. Core가 `accepted=true`와 `durable_pre_ack_recorded=true`를 모두 반환하지 않으면 `DURABLE_DB_PRE_ACK_FAILED`로 종료하고 broker method를 호출하지 않는다. 이 경계는 비동기 Core worker queue를 우회한다.
- pre-ack 이후 Kiwoom broker method가 예외를 던지거나 broker ack 전 timeout이 발생하면 `order_broker_unconfirmed`/timeout evidence와 함께 `UNCONFIRMED`로 보존한다. `UNCONFIRMED` 또는 durable gap이 있으면 신규 `send_order` claim과 NEW_BUY safety gate를 차단하지만 reconcile/lifecycle cancel은 막지 않는다. 늦은 ack/Chejan만 `BROKER_ACCEPTED`/`CHEJAN_CONFIRMED`로 복구할 수 있다.
- mock Gateway도 `command_started -> order_pre_ack -> command_ack` 계약을 사용해 replay와 실제 adapter의 상태 의미를 맞췄다. `order_pre_ack`는 LIVE_SIM lifecycle unknown-event error를 만들지 않는 broker-boundary event로 분리했다.
- `GET /api/operator/gateway/order-broker-boundaries/status`, read-only list API, operator aggregate status, Dashboard full/fast `order_broker_boundaries`/pipeline summary, Gateway heartbeat의 durable pre-ack metrics, `tools/ops_order_broker_boundary_check.py`를 추가했다. runbook은 `docs/runbook_order_broker_boundary_ko.md`다.
- 구버전 migration, 재실행, duplicate/idempotency, out-of-order, broker-call exception/timeout과 late Chejan recovery, pre-ack callback failure에서 broker call 0, send/cancel 공통 경계, API/dashboard/ops 계약을 테스트했다. 구조 감사 guard는 과거 `DISPATCHED` 대신 `CLAIMED/GATEWAY_STARTED` pre-ack 누락을 탐지한다. 최종 full pytest suite `882`개와 scoped ruff를 통과했다.
- 26.7GB 운영 DB를 read-only SQLite online backup으로 복제한 격리 DB에서 schema `46 -> 47`을 적용했다. 1차 `37.039s`, 재실행 `0.059s`, `quick_check=ok`, partial index query plan 사용을 확인했다. 5개 order command 중 미발송 `EXPIRED=2`, 기존 evidence 없는 `UNCONFIRMED=3`은 그대로 보존됐고 missing/gap/duplicate/mismatch는 `0/0/0/0`이었다. 근거: `reports/order_broker_boundary_migration/20260710T012452Z/summary.md`.
- 최초 Core 실행은 process env보다 `.env`를 나중에 적용하는 config precedence 때문에 ops가 `CORE_NOT_OBSERVE/LIVE_SIM_ALLOWED`로 즉시 FAIL 판정했다. Gateway는 없었고 total/order command delta는 `0/0`이었다. 해당 Core를 종료한 뒤 별도 temp safe env를 `TRADING_ENV_FILE`로 지정해 재검증했다.
- 2026-07-10 10:30 KST 최종 Core API는 `mode=OBSERVE`, LIVE_SIM/LIVE_REAL false였다. boundary/dashboard는 기존 `UNCONFIRMED=3` 때문에 예상 `WARN`, 신규 BUY routing block `true`, missing/gap/duplicate/mismatch `0/0/0/0`, 점검 전후 total/order command delta `0/0`이었다. 이 WARN은 과거 broker evidence를 조작하지 않은 결과이며 다음 replay PR을 차단하지 않는다. 공식 report: `reports/order_broker_boundary/20260710T013001Z/summary.md`.
- 검증 후 Core supervisor/child PID를 종료하고 8000 listener `0`을 확인했다. temp safe env와 26.7GB 격리 backup도 경로 검증 후 삭제했다. 다음 PR 진입 판정은 `WARN_ALLOWED_HISTORICAL_UNCONFIRMED`이며 replay 기반으로 진행한다.

## 다음 PR 권장 순서

1. market_scan limited cutover를 global kill switch, 원자적 budget, prior worker closure 뒤에서 별도 PR로 진행한다.
2. LIVE_SIM lifecycle을 durable idempotent consumer로 옮긴다.
3. incremental queue stale/backlog/dead-letter/retry-reset 운영 경로를 추가한다.
4. pipeline source lineage/freshness guard와 dashboard/theme coherency를 완성한다.
5. 모든 consumer 안정화 후 Gateway POST를 raw append + durable enqueue로 제한한다.
6. 연속 10거래일 exit criteria를 충족한 뒤에만 append-only scaffolding flag와 최종 inline 경로를 정리한다.

## Replay 검증 완료

- `services/runtime/projection_replay.py`와 `tools/ops_projection_replay.py`가 accepted event를 `manifest.json`+`events.jsonl` bundle로 export/import한다. sequence, source rowid, source `received_at`, payload hash, 전체 record hash와 authoritative event-order hash를 검증하며 unsafe/order event type을 거부한다.
- import/parity target은 존재하지 않는 새 DB만 허용하고 설정된 운영 DB와 같은 경로 또는 기존 파일을 fail-closed한다. source export는 SQLite `mode=ro`+`query_only`다.
- 동일 bundle의 (a) inline projection+outbox shadow verify, (b) outbox worker apply를 두 격리 DB에서 실행한다. market-data projection table과 logical watermark의 canonical hash, table별 hash/count, KRX/NXT sample count 및 5,000 event chunk별 기존 reconcile을 비교한다.
- strict authorizer allowlist 밖의 `gateway_commands`, broker boundary, order plan, incremental/candidate/theme/strategy/risk/entry/exit, DRY_RUN/LIVE_SIM table write는 차단하며 blocked attempt 자체를 FAIL 처리한다. replay settings는 `OBSERVE`, LIVE_SIM/LIVE_REAL false, candidate ingest 및 incremental/condition-fusion side effect disabled다.
- `tr_response` synthetic price tick이 실행 시각을 사용하던 비결정성을 찾아 parent event의 저장 `event_ts`/`received_at`을 상속하도록 수정했다.
- 반복 fixture `tests/fixtures/projection_replay/market_data_events.json`은 KRX price, 명시적 NXT price, KRX condition, TR response를 포함한다. targeted tests는 export/import order 보존, tamper/운영 DB 거부, exact parity, write guard, operator/dashboard read-only 계약을 검증하며 최종 full pytest suite `887`개를 통과했다.
- operator `GET /api/operator/projection-replay/status`, Dashboard full/fast `projection_replay`는 최신 report만 읽고 실행 control을 제공하지 않는다. Runbook: `docs/runbook_market_data_replay_verification_ko.md`.
- 2026-07-10 fixture evidence는 event `4`(KRX `2`, NXT `1`, UNKNOWN `1`), inline/worker hash 일치, 양쪽 reconcile `PASS`, market_data outbox `APPLIED=4`, PENDING/PROCESSING/ERROR/DEAD_LETTER `0`, 주문/trading side effect `0`이다. Report: `reports/projection_replay/20260710T021342Z_660e648e/summary.md`.
- 다음 PR 진입 판정은 `PASS`다. 이 완료는 장중 cutover나 10거래일 flag cleanup 근거를 대체하지 않는다. 이후 P1-2/P2-4 watermark/retention/RCA와 PR-15/16 market_index 준비 및 제한적 cutover 구현까지 완료했다.

## Projection watermark / retention RCA 완료

- schema 48은 projection별 `last_success_*`/`last_error_*` watermark, `(projection_name,event_id)` 단위 `projection_event_results`, event별 `projection_retention_event_rca` view를 추가했다. 기존 `last_event_*`는 호환 목적으로 유지한다. inline/worker 성공은 success watermark와 result를 기록하고, 오류는 error watermark만 전진시킨다. outbox의 `APPLIED` 및 `ERROR/DEAD_LETTER` terminal 전이와 result/watermark 갱신은 같은 commit이다.
- accepted replayable event의 retention은 raw event가 존재하고, required outbox가 모두 `APPLIED`, result가 모두 `SUCCESS`, 각 success watermark가 event rowid 이상인 경우에만 허용한다. missing outbox/result, ERROR/DEAD_LETTER/SKIPPED, watermark lag, command 연결 event는 event별 reason code로 fail-closed한다.
- 운영 기본값은 `EVENT_STORE_RETENTION_ENABLED=false`, `PROJECTION_EVENT_RESULT_BACKFILL_ENABLED=false`다. backfill API는 APPLIED legacy outbox만 대상으로 dry-run이 기본이며 apply flag 없이는 HTTP 409다. retention apply는 enabled와 old blocker 0을 동시에 요구하고 blocker 하나라도 있으면 전체 prune을 거부한다.
- Dashboard full/fast와 operator aggregate에 `projection_watermarks`/`projection_retention`을 추가했다. fast status는 partial index를 사용하는 bounded blocker probe이고, 정확한 eligible/blocked count는 명시적 ops 요청으로 분리했다. event RCA는 KRX/NXT/UNKNOWN/MIXED venue와 replay availability를 함께 표시한다.
- 26.7GB 운영 DB를 read-only SQLite online backup으로 격리했다. source는 backup 시점에 이미 schema `48`(`schema_metadata.updated_at=2026-07-10 02:40:45Z`)이었고 `projection_event_results=0`이었다. 이 additive schema 변경의 실행 주체는 확인되지 않았다. source에 migration, backfill apply, retention apply는 실행하지 않았고 source `quick_check`는 제한 시간 안에 끝나지 않아 성공으로 기록하지 않는다.
- 실제 구버전 migration 검증은 격리 copy에서 신규 객체를 제거해 schema 47 상태를 재현한 뒤 수행했다. `47 -> 48`은 `9.619s`, 재실행은 `0.043s`, 최종 격리 DB `quick_check=ok`(`524.195s`)였다. 30일 status는 `0.001s`, 1일 fast bounded status는 5,020,589 age-eligible event에서 `0.525s`, 1일 exact는 `116.080s`였고 eligible/blocked는 `1,251,085/3,769,504`였다. query plan은 retention partial index를 사용했다. 근거: `reports/projection_retention_migration/20260710T024503Z/summary.md`.
- OBSERVE-safe Core를 격리 migrated DB에만 띄워 API matrix를 확인했다. `LIVE_SIM/LIVE_REAL=false`, projection/background apply와 retention/backfill apply는 disabled였고, legacy APPLIED backfill candidate `65,159`, applied `0`, unresolved projection error `0`, command/order-command delta `0/0`이었다. 실제 NXT event `evt_3bd65e3120e147a1b075f8d7192ec51d`의 RCA venue count `NXT=1`을 확인했다.
- 최종 API verdict는 `WARN`이며 유일한 reason은 의도한 안전 기본값 `EVENT_RETENTION_DISABLED_SAFE_DEFAULT`다. failures는 없고 점검 후 Core와 port 8018 listener를 종료했다. 근거: `reports/projection_retention/20260710T031456Z/summary.md`, runbook: `docs/runbook_projection_watermark_retention_ko.md`.
- targeted migration/idempotency/atomicity/fail-closed/API/dashboard/ops tests와 당시 full pytest suite `895`개를 통과했다. 다음 PR 진입 판정은 `WARN_ALLOWED_RETENTION_DISABLED_SAFE_DEFAULT`였으며 이후 PR-15/16 market_index 준비 및 제한적 cutover 구현을 완료했다.

## PR-15 Market index worker/reconcile/dry-run 준비 완료

- schema 49에 `market_index_projection_reconcile_runs/issues`와 `market_index_projection_routing_decisions`를 additive로 추가했다. schema 48 상태에서 migration과 재실행을 검증했고 기존 index projection table을 rebuild하지 않는다.
- `PROJECTION_OUTBOX_MARKET_INDEX_APPLY_ENABLED=false`가 기본이다. global apply와 index apply가 모두 true이고 `projection_name=market_index`인 경우에만 worker가 raw accepted event를 `process_market_index_event()`로 적용한다. worker는 `market_regime` job을 claim/mutate하지 않으며 older event는 `SKIPPED`, implausible/error event는 retry/dead-letter로 fail-closed한다.
- Gateway는 `market_index_tick`마다 dry-run routing evidence를 기록하지만 PR-15에서 `effective_skip_inline`은 항상 false다. cutover flag를 켜도 index와 regime inline projection을 그대로 실행한다. operator API, Dashboard full/fast index/reconcile/routing section, `tools/ops_market_index_projection_check.py`를 추가했다.
- projection reconcile은 accepted event, event-id sample, projection error, index outbox status, KOSPI/KOSDAQ coverage를 대조한다. `data_usable`, `parser_verified`, source `REALTIME/TR_BOOTSTRAP/UNKNOWN`을 독립 집계한다. parser unverified는 projection 정합성과 별도 INFO지만 기본 append-only readiness를 false로 만든다.
- 현재 TR bootstrap adapter는 구현되지 않았음을 source contract와 Dashboard에 명시했다. `TR_BOOTSTRAP` source event는 FAIL이고 realtime evidence로 대체하지 않는다. NXT tick도 KRX market-index evidence로 인정하지 않는다.
- schema-49 격리 fixture에서 KOSPI inline+shadow 1건과 KOSDAQ raw+worker apply 1건을 OBSERVE-safe Core API로 검증했다. reconcile `PASS`, data usable `2`, parser verified `2`, realtime source `2`, index outbox `APPLIED=2`, ERROR/DEAD_LETTER `0`, effective skip `0`, command/order-command delta `0/0`이었다. Core 종료 후 port 8019 listener `0`을 확인했다. Report: `reports/market_index_projection/20260710T040055Z/summary.md`.
- 운영 DB는 `mode=ro/query_only`로만 조회했다. schema는 여전히 `48`, accepted index event `13,598`, 최신 KOSPI/KOSDAQ event는 2026-07-09 13:20 KST의 명시적 realtime source였지만 parser는 `PILOT_UNVERIFIED_FID_MAP`이다. 최신 heartbeat도 2026-07-10 08:56 KST로 stale했고 `market_index_adapter_health=PARSE_ERROR`, callback/parsed/error=`8/0/8`이었다. 운영 DB에 schema migration이나 worker/reconcile write를 실행하지 않았다.
- 신규 14개 테스트를 포함한 최종 full pytest suite `909`개와 scoped ruff/compile을 통과했다. 구현 gate는 `PASS`, 실제 KRX 운영 gate는 `PENDING_KRX_SESSION`, parser gate는 `PENDING_KOA_STUDIO_CONFIRMATION`이다. PR-16 코드는 offline으로 준비할 수 있지만 effective cutover 활성화는 금지한다. 상세 근거: `reports/market_index_projection_fixture/20260710T040009Z/summary.md`, runbook: `docs/runbook_market_index_projection_ko.md`.

## PR-16 Market index limited cutover 구현 및 격리 검증 완료

- schema 50에 `market_index_append_only_budget_state`와 nullable regime lineage(`source_event_id`, `source_projection`, `generated_by`) 및 partial source-event index를 additive로 추가했다. 분 단위 effective skip budget은 원자적으로 소비하며 기본값은 `GATEWAY_MARKET_INDEX_APPEND_ONLY_CUTOVER_ENABLED=false`, global kill switch와 PR-15 legacy guard ON, max skip `0/min`이다.
- cutover는 trading profile/mode OBSERVE와 LIVE_SIM/LIVE_REAL 허용 false를 코드에서 강제한다. Event는 기본 age 30초/future skew 5초 이내의 KRX 평일 REGULAR session이어야 하며 NXT/off-hours는 거부한다. Gateway heartbeat와 latest index tick도 30초 이내이고 realtime enabled, adapter `CALLBACK_ACTIVE`, parsed tick 존재여야 한다. DRY_RUN/global/legacy gate 외에도 reconcile/data/parser/regime/gateway-health fail-closed guard가 모두 ON이어야 하고, fresh reconcile PASS, data usable, parser verified, explicit realtime source, worker apply, index/regime outbox 무오류와 SLA 내 backlog, prior effective-skip artifact/regime health를 모두 요구한다. 안전 모드 이탈, guard 비활성화, pending/error/evidence/artifact/regime gap이나 budget 초과가 있으면 현재 요청부터 inline projection으로 즉시 rollback한다.
- effective skip event는 worker가 `market_index`를 apply한 뒤 `source_event_id`와 `source_projection=market_index` lineage를 가진 `market_regime` snapshot을 생성해야 한다. sibling `market_regime` outbox는 `MARKET_INDEX_REGIME_VERIFY`로 그 snapshot을 검증한 뒤에만 `APPLIED`가 된다. refresh 오류와 lineage 누락은 fail-closed한다.
- index write 후 regime refresh만 실패하면 index job은 retryable `PENDING`으로 돌아간다. 재시도는 effective-skip event의 기존 artifact를 worker-origin recovery evidence로 닫고 linked regime를 보충하며, 반복 실패가 retry limit에 도달하면 `DEAD_LETTER`로 종료한다. 공용 projection의 기존 ERROR 정책은 변경하지 않았다.
- enqueue commit과 routing decision 사이의 worker race를 닫기 위해 같은 event에서는 `market_index` job을 sibling regime보다 먼저 claim하되 다른 event의 전역 priority는 바꾸지 않는다. cutover armed worker가 routing보다 먼저 실행돼도 event-linked regime를 생성하며, 이미 APPLIED인 index가 worker evidence/artifact/linked regime/regime outbox를 모두 갖추지 못하면 Gateway는 effective skip을 거부하고 inline fallback한다.
- schema-50 격리 fixture의 OBSERVE-safe Core API에서 cutover ON, kill switch/legacy guard OFF, budget `1/min`으로 event 1건을 effective skip했다. Event age/future skew는 `0.008s/0s`, KRX weekday `REGULAR`, Gateway heartbeat/latest tick age는 `0.060s`, adapter `CALLBACK_ACTIVE`였다. index/regime outbox는 각각 `APPLIED_BY_WORKER`/`APPLIED_BY_VERIFY`, linked regime snapshot present, partial lineage index 사용, effective-skip health 누락 0, reconcile `PASS` 3건, controller `PASS`, open/ERROR/DEAD_LETTER `0/0/0`, command/order-command delta `0/0`, `quick_check=ok`이었다. Core 종료 후 port 8020 listener `0`을 확인했다.
- fixture regime은 `NEUTRAL/DEGRADED`였다. 이 결과는 worker lineage, retry/dead-letter, health/session gate와 fail-closed routing만 증명하며 current operating KRX market-quality evidence로 사용하지 않는다. NXT event도 KRX market-index evidence를 대체하지 않는다.
- 운영 DB는 최종 점검에서 SQLite `mode=ro/query_only`로만 조회했다. schema `50`, `schema_version.updated_at=2026-07-10 04:24:30Z`였으나 migration 실행 주체는 확인되지 않았다. operating routing/effective-skip/budget/non-null lineage row는 모두 0이다. pytest가 기본 DB path로 빠지는 재발을 막기 위해 autouse fixture가 `tmp_path/default-test.sqlite3`를 강제하도록 수정했고, full suite 전후 operating schema timestamp가 동일함을 read-only로 확인했다.
- 실제 adapter는 heartbeat age 약 `18,510s`, `PARSE_ERROR`, parsed/error `0/8`, latest parsed tick 없음이고 최신 accepted event parser도 `PILOT_UNVERIFIED_FID_MAP`이다. 따라서 운영 활성화 gate는 계속 `PENDING_KRX_SESSION`/`PENDING_KOA_STUDIO_CONFIRMATION`이다. Reports: `reports/market_index_projection/20260710T050421Z/summary.md`, `reports/market_index_cutover_fixture/20260710T050254Z/summary.md`; runbook: `docs/runbook_market_index_projection_ko.md`.
- 신규 제한 cutover/rollback/API/ops/race/retry/health/session tests를 포함한 최종 full pytest suite `939`개와 scoped ruff/compile을 통과했다. `mypy`는 현재 venv에 설치돼 있지 않아 실행하지 못했다. 다음 구현 범위는 공통 `market_context` snapshot과 `market_regime` worker/reconcile/cutover다.

## PR-17 공통 Market Context snapshot 구현 및 격리 검증 완료

- schema 51은 `market_context_snapshots`, market별 `market_context_latest`, `candidate_context_latest.market_context_snapshot_id`와 partial reference index를 additive로 추가한다. schema 48 형태를 재현한 migration과 재실행에서 기존 market-index/regime lineage와 신규 context table/column/index를 함께 검증했다.
- KOSPI/KOSDAQ snapshot pair는 최신 index event의 immutable `event_id`, source rowid, event/received timestamp, parser status, data source로 만든 같은 SHA-256 watermark hash를 공유한다. 동적으로 변하는 tick age/freshness는 hash identity에서 제외해 같은 원천의 반복 rebuild가 새 snapshot을 만들지 않는다. `(market, source_watermark_hash)` UNIQUE와 latest pointer upsert가 duplicate/race를 흡수하고 예외 시 context transaction을 rollback한다.
- parser confidence, data quality, trading data usability를 별도 lineage로 저장한다. 두 지수 data usable, regime quality FRESH, regime DATA_WAIT 아님을 모두 만족해야 `trading_data_usable=true`이고 parser까지 VERIFIED여야 `trading_eligible=true`다. membership unknown, pair missing, stale, unverified는 read-only resolver에서 `DATA_WAIT` 또는 ineligible로 fail-closed한다.
- Gateway index inline projection과 PR-16 market-index worker continuity가 같은 common builder를 사용한다. Gateway는 정상 pair의 새 watermark를 5초 cadence로 제한하되 data usability 또는 parser confidence가 개선되면 즉시 rebuild한다. Worker는 retry에서 event-linked global regime이 이미 있어도 누락된 context pair를 복구하고 `mutated_projection_names`에 `market_context`를 명시한다. refresh exception과 lineage 누락은 기존 retry/dead-letter 및 inline rollback 계약을 유지한다.
- Candidate refresh는 공통 snapshot id를 저장하고 cached JSON에 watermark/parser/data lineage를 남길 뿐 market context/regime snapshot을 생성하지 않는다. 같은 market 후보 2개의 refresh 전후 context/regime count가 `2/1 -> 2/1`로 유지되고 같은 snapshot id를 참조하는 테스트를 추가했다. 기존 `/api/market-regime/for-code/{code}`도 common snapshot read로 전환해 GET이 per-code regime row를 만들지 않는다.
- read-only `/api/operator/market-context/status`, 인증된 OBSERVE-safe `/api/operator/market-context/rebuild`, Dashboard full/fast `market_context`, `tools/ops_market_context_check.py`, `docs/runbook_market_context_ko.md`를 추가했다. Rebuild는 OBSERVE profile/mode, LIVE_SIM/LIVE_REAL false, `live_safe=true`를 코드에서 강제하며 SQLite lock retry와 rollback evidence를 노출한다.
- schema-51 신규 격리 DB의 Core를 port 8021에서 OBSERVE-safe로 실행하고 Gateway API로 market_symbols 1건과 synthetic parser VERIFIED KOSPI/KOSDAQ index event 2건을 입력했다. 최종 latest pair는 동일 hash와 동일 source regime id, parser VERIFIED, data quality FRESH, data usable true였고 context/regime snapshot은 `4/2`, source-regime missing `0`, `quick_check=ok`였다. Ops verdict `PASS`, rebuild `APPLIED_BY_VERIFY`, candidate reference/missing/unreferenced `0/0/0`, outbox `ERROR/DEAD_LETTER=0/0`, command/order-command delta `0/0`이었다. Core 종료 후 listener `0`을 확인하고 fixture DB/WAL/SHM은 경로 검증 후 삭제했으며 로그와 report만 보존했다. Reports: `reports/market_context/20260710T055744Z/summary.md`, `reports/market_context_fixture/20260710T055725Z/core.stderr.log`.
- 운영 DB는 pytest와 fixture 검증 후에도 SQLite `mode=ro/query_only`로만 조회했으며 schema `50`, `schema_version.updated_at=2026-07-10 04:24:30Z`로 PR-16 최종 확인값과 동일했다. PR-17 schema 51 migration은 운영 DB에 적용하지 않았다.
- targeted 회귀와 SQLite lock retry/idempotency/cadence/missing-stale/parser separation/candidate reuse/API/dashboard/ops/migration tests, 최종 full pytest suite `950`개, scoped ruff, compileall, `git diff --check`를 통과했다. 구현 gate와 다음 준비 PR 진입 조건은 `PASS`다. 단 synthetic VERIFIED fixture는 실제 KOA Studio parser 검증 또는 KRX 장중 evidence가 아니므로 실제 regime cutover gate는 계속 `PENDING_KRX_SESSION`/`PENDING_KOA_STUDIO_CONFIRMATION`이다. 다음 논리 PR은 market_regime worker apply/reconcile/dry-run 준비다.

## PR-18 Market regime worker/reconcile/dry-run 준비 및 격리 검증 완료

- schema 52는 `market_regime_projection_reconcile_runs/issues`와 `market_regime_projection_routing_decisions`를 additive로 추가한다. schema 51 형태에서 migration/re-run을 검증했고 기존 market-index/context table을 rebuild하지 않는다.
- `PROJECTION_OUTBOX_MARKET_REGIME_APPLY_ENABLED=false`가 기본이다. global apply와 regime apply가 모두 true이고 operator가 `projection_name=market_regime`, `apply_projection=true`로 호출한 경우에만 accepted source의 index artifact를 확인한 뒤 PR-17 common builder를 실행한다. dependency 누락은 retry/dead-letter로 닫고, 더 최신 index event가 존재하는 superseded source는 latest context verify-only로 snapshot 폭증을 막는다.
- reconcile은 accepted `market_index_tick`, event-id index sample, regime outbox, KOSPI/KOSDAQ latest watermark coverage, common context/source-regime 구조, parser/data readiness를 대조한다. outbox ERROR/DEAD_LETTER, context watermark lag, dependency/lineage 누락은 FAIL이고 stale/unverified/data-unusable는 WARN으로 append-only readiness를 닫는다.
- Gateway는 regime dry-run decision을 기록하지만 PR-18의 `effective_skip_inline`은 항상 false다. fresh reconcile PASS, append-only ready, accepted source/outbox/index artifact/context, worker apply, OBSERVE-safe가 모두 맞으면 `would_skip_inline=true` 와 `EFFECTIVE_SKIP_DISABLED_IN_PR18`만 남기고 inline builder를 실행한다. operator API, Dashboard full/fast, ops script, runbook을 함께 추가했다.
- schema-52 격리 DB의 OBSERVE-safe Core API에서 synthetic parser VERIFIED KOSPI/KOSDAQ 2건을 index artifact까지 seed한 후 regime worker를 실행했다. claimed/applied `2/2`, `APPLIED_BY_WORKER/APPLIED_BY_VERIFY=1/1`, mutation `market_context,market_regime`, 초기 context/regime snapshot `2/1`이었다. 추가 Gateway event의 dry-run은 would/effective `1/0`이고 inline builder를 유지했다. 최종 reconcile `PASS`, append-only ready true, checked event `3`, outbox ERROR/DEAD_LETTER `0/0`, command/order-command delta `0/0`, `quick_check=ok`였다. Reports: `reports/market_regime_projection/20260710T063020Z/summary.md`, `reports/market_regime_projection_fixture/20260710T062849Z/summary.md`.
- 첫 Core fixture 기동에서 일반 process env보다 repository `.env`가 우선된다는 설정 계약을 놓쳤다. `/api/status`가 `LIVE_SIM`과 기본 operating DB를 반환한 즉시 event/worker/order 호출 없이 Core를 종료했다. 다만 startup `initialize_database()`가 26.7GB operating DB에 additive schema `50 -> 52`를 적용하고 `schema_version.updated_at=2026-07-10 06:26:26Z`로 변경했다. 신규 reconcile/routing row는 `0/0`이며 임의 destructive rollback은 하지 않았다. 재기동은 격리 `TRADING_ENV_FILE`을 사용하고 seed 전 status/DB path를 확인했다. Incident: `reports/market_regime_projection_fixture/20260710T062624Z/incident.md`.
- 신규 worker/reconcile/routing/migration/API/dashboard/ops/fail-closed/idempotency/superseded tests와 market-index/context 회귀 104개, 최종 full pytest suite `965`개, scoped Ruff, compileall을 통과했다. 구현 gate는 `PASS`이지만 synthetic fixture는 실제 KRX/KOA Studio evidence가 아니므로 limited cutover gate는 `PENDING_KRX_SESSION`/`PENDING_KOA_STUDIO_CONFIRMATION`이다.

## PR-19 Market regime limited cutover 구현 및 offline 격리 검증 완료

- schema 53은 `market_regime_projection_routing_decisions`에 cutover/kill-switch/budget/observe/index-routing/rollback/controller evidence를 additive column으로 추가하고 `market_regime_append_only_budget_state`를 만든다. schema 52 형태에서 column/index/budget migration과 재실행을 검증했다. 기본은 cutover OFF, global kill switch ON, budget 0, PR-18 legacy guard ON이다.
- Gateway는 index inline projection이 성공한 후 current index routing evidence의 parser VERIFIED, data usable, explicit REALTIME, KRX 평일 REGULAR, event age/future skew, fresh Gateway heartbeat/latest parsed tick을 재사용한다. regime latest reconcile이 current event 직전 accepted index event까지 덮지 않거나 guard 설정 하나라도 비활성화되면 budget을 소비하지 않고 inline builder를 실행한다. NXT는 market-regime KRX evidence로 인정하지 않는다.
- effective skip은 dry-run/cutover ON, kill switch/PR-18 guard OFF, `1/min` 이상 budget, worker apply, fresh reconcile/context, regime outbox backlog SLA를 모두 통과해야 원자적으로 예약된다. 같은 event 재평가는 budget을 중복 소비하지 않는다. 이전 effective skip의 outbox APPLIED, `APPLIED_BY_WORKER`, event-linked regime, context pair 중 하나라도 누락되면 다음 event부터 즉시 inline rollback한다.
- regime worker는 effective-skip event에서 PR-17 5초 cadence를 무시하고 current `source_event_id`가 연결된 global regime 1개와 KOSPI/KOSDAQ context pair를 강제로 생성한다. worker 전에 source가 superseded되거나 linked snapshot/context가 누락되면 retry/dead-letter로 fail-closed하고 후속 skip을 차단한다. Gateway API 통합 테스트는 effective skip 시 inline context count가 늘지 않고 operator worker 후 2개가 추가되며 command count가 0임을 검증했다.
- KRX 정규장 종료 후 schema-53 격리 Core를 OBSERVE-safe port 8023에서 기동했다. synthetic KRX/index-routing evidence로 offline effective skip 1건을 만들고 API worker로 닫았다. would/effective `1/1`, budget `1/1`, controller `PASS`, linked regime/context `1/2`, 최종 reconcile `PASS` 3건, outbox ERROR/DEAD_LETTER `0/0`, command/order-command delta `0/0`, `quick_check=ok`였다. Core 종료 후 listener와 fixture DB/WAL/SHM을 정리했다. Reports: `reports/market_regime_cutover/20260710T070226Z/summary.md`, `reports/market_regime_cutover_fixture/20260710T070117Z/summary.md`.
- 위 fixture의 KRX session/Gateway health는 offline synthetic injection이며 실제 장중 evidence가 아니다. 따라서 구현 gate는 `PASS`, 운영 cutover gate는 `PENDING_KRX_SESSION`/`PENDING_KOA_STUDIO_CONFIRMATION`으로 분리한다. 운영 DB는 PR-18 startup incident로 schema `52`, timestamp `2026-07-10 06:26:26Z`인 상태에서 read-only 확인했고 schema 53을 적용하지 않았다. reconcile/routing row는 계속 `0/0`이다.
- market-regime cutover/budget/rollback/Gateway API/worker/migration/ops 테스트와 영향 범위 회귀, 최종 full pytest suite `972`개, scoped Ruff, compileall, `git diff --check`를 통과했다. 다음 논리 PR은 market_scan worker apply/reconcile/dry-run 준비다.

## PR-20 Market scan worker/reconcile/dry-run 준비 완료

- schema 54는 `market_scan_snapshots/latest`에 `source_event_id`, `request_id`, `parser_status`, `generated_by`를 additive하게 추가하고 reconcile run/issue 및 routing decision table을 만든다. scan id는 process별 Python hash 대신 SHA-256 기반 결정적 id를 사용하며 늦은 과거 worker event가 latest pointer를 역행시키지 않는다.
- `PROJECTION_OUTBOX_MARKET_SCAN_APPLY_ENABLED=false`가 기본이다. 명시적 operator apply에서만 scan worker가 실행되며 같은 event의 `market_data` artifact 또는 APPLIED sibling outbox를 먼저 요구한다. dependency 누락은 retryable PENDING으로 복귀하고 scan mutation 범위는 `market_scan` 하나다. candidate ingest와 주문 side effect는 실행하지 않는다.
- reconcile은 accepted scan TR source row, event-linked/request-linked snapshot, row error terminal coverage, parser verification, data usability, market_data dependency, scan outbox를 대조한다. 최신 stream parser가 `PILOT_UNVERIFIED`, 응답이 empty/failed, row error가 있으면 WARN이고 coverage/dependency/outbox 오류는 FAIL이다.
- Gateway는 prior event를 덮는 fresh reconcile PASS, worker apply, OBSERVE-safe, parser VERIFIED, usable rows, market_data dependency와 outbox SLA를 모두 만족할 때만 `would_skip_inline=true`를 기록한다. PR-20의 `effective_skip_inline`은 항상 false이고 `MARKET_SCAN_EFFECTIVE_SKIP_DISABLED_IN_PR20` evidence와 inline fallback을 유지한다.
- operator reconcile/routing API, operator aggregate status, Dashboard full/fast sections, `tools/ops_market_scan_projection_check.py`, `docs/runbook_market_scan_projection_ko.md`를 추가했다. 기본 KOSPI/KOSDAQ scan TR parser/coverage는 NXT tick으로 대체할 수 없고 NXT는 exchange가 명시된 venue-neutral ordering/replay 검증에만 사용할 수 있다.
- schema-54 격리 DB의 OBSERVE-safe Core를 port 8024에서 기동하고 synthetic KRX scan TR 2건을 API로 입력했다. 첫 event worker closure/reconcile PASS 뒤 둘째 event는 dry-run `would/effective=1/0`, inline scan `APPLIED`였고 worker verify 후 최종 reconcile PASS, checked 2, snapshot/source-event lineage `2/2`, scan outbox APPLIED/ERROR/DEAD_LETTER `2/0/0`, command/order-command delta `0/0`, candidate ingest `0`, `quick_check=ok`였다. Core 종료 후 listener와 fixture DB/WAL/SHM을 정리했다. Reports: `reports/market_scan_projection/20260710T073413Z/summary.md`, `reports/market_scan_projection_fixture/20260710T073245Z/summary.md`.
- fixture의 `KOA_STUDIO_VERIFIED`는 synthetic metadata이므로 실제 parser/장중 evidence가 아니다. 운영 cutover gate는 `PENDING_KRX_SESSION`/`PENDING_KOA_STUDIO_CONFIRMATION`이다. 운영 DB는 `mode=ro/query_only`로 schema `52`, timestamp `2026-07-10 06:26:26Z`, market-scan projection table `0`을 확인했고 schema 54를 적용하지 않았다.
- worker dependency/retry/idempotency, reconcile PASS/WARN, dry-run/guard, schema 53 migration/re-run, operator/dashboard/ops와 기존 outbox/schema/dashboard 회귀, 최종 full pytest suite `984`개, scoped Ruff, compileall, `git diff --check`를 통과했다. 구현 gate는 `PASS`, 다음 논리 PR은 market_scan limited cutover다.

## Flag 정리 계획 (최종 순서 8)

- 전제 조건 (exit criteria): `price_tick`/`tr_response`/`condition_event` cutover가 모두 기본 경로로 동작하고, 연속 10거래일 reconcile PASS, invalid effective skip 0건, deferred side-effect 누락 0건.
- 1단계: `GATEWAY_MARKET_DATA_APPEND_ONLY_*` 계열 scaffolding flag(dry-run, per-type cutover, budget, require-*, reconcile max age 등 약 20개)와 `PROJECTION_OUTBOX_SHADOW_MODE`/`PROJECTION_OUTBOX_APPLY_PROJECTION_ENABLED`/`PROJECTION_OUTBOX_MARKET_DATA_APPLY_ENABLED` apply 게이트를 제거하고, append-only를 기본 동작으로 만든다. inline projection 경로는 긴급 rollback용 단일 flag 하나 뒤에만 유지한다.
- 2단계: 안정 확인 후 inline projection 코드 경로와 rollback flag를 삭제한다. 이 시점이 P0-1의 최종 상태(POST는 raw append + outbox enqueue만 수행)다.
- 유지 대상: worker 운영 파라미터(`PROJECTION_OUTBOX_WORKER_ENABLED`, `PROJECTION_OUTBOX_WORKER_INTERVAL_SEC`, `PROJECTION_OUTBOX_BATCH_SIZE`, `PROJECTION_OUTBOX_RETRY_LIMIT`, `PROJECTION_OUTBOX_PROCESSING_TTL_SEC`)는 flag가 아니라 운영 설정으로 남긴다.
- 각 제거 PR은 해당 flag를 참조하는 runbook(현재 6개), ops script, operator status/dashboard counter를 함께 정리한다. flag만 지우고 죽은 runbook/counter를 남기지 않는다.
- 후속: flag 정리 완료 후 dual-run reconcile은 상시 운영 체크에서 replay 검증 도구로 역할을 좁힌다.
