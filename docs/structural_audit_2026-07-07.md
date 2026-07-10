# Structural Audit - 2026-07-07

대상: `suseok-trader-v2` main  
범위: Gateway ingestion, runtime lock, incremental evaluation, market index/regime, LIVE_SIM order lifecycle, replay/retention/watermark, dashboard coherency  
최종 업데이트: 2026-07-10 (`PR-14` market_reference NXT limited cutover 검증 완료)
안전 원칙: append-only 전환은 기본 disabled와 strict feature flag를 유지한다. `LIVE_REAL` 활성화, 주문 정책 완화, 매수 기준 완화는 하지 않는다.

## 이미 개선된 점

- Gateway event store는 `raw_events`/`gateway_events`에 `event_id` PK와 payload hash를 저장하고, 동일 payload 중복은 `duplicate_count`로 흡수하며 payload 충돌은 `CONFLICT`로 거부한다. 근거: `storage/event_store.py`.
- Gateway command는 `gateway_command_dedupe_keys.idempotency_key` PK를 사용해 active idempotency 중복 enqueue를 차단한다. LIVE_SIM `send_order`/`cancel_order`는 simulation-like mode, `live_sim_only=true`, `live_real_allowed=false`, idempotency 일치 검사를 통과해야 enqueue된다. 근거: `storage/gateway_command_store.py`.
- LIVE_SIM intent/order 주요 키는 DB 제약이 있다. `live_sim_intents.live_sim_intent_id` PK, `live_sim_intents.idempotency_key` UNIQUE, `live_sim_orders.live_sim_order_id` PK, `live_sim_orders.idempotency_key` UNIQUE, `order_plan_drafts.order_plan_id` PK, `order_plan_drafts.idempotency_key` UNIQUE. 근거: `storage/sqlite.py`.
- Market index parser의 `parser_status`는 latest row와 dashboard status에 노출된다. `PILOT_UNVERIFIED` 같은 상태를 감지할 수 있다. 근거: `services/market_index_service.py`, `gateway/kiwoom_client.py`.
- Dashboard top theme 표시는 단순 latest sample만 쓰지 않고 state-filtered query를 별도로 사용한다. 기존 테스트가 DATA_WAIT 최신 표본 뒤에 숨은 LEADING/SPREADING 테마를 검증한다. 근거: `services/dashboard_service.py`, `tests/test_dashboard_service.py`.
- Stale `DISPATCHED` order command는 timeout 후 `UNCONFIRMED`로 전환되고 운영자 종결 도구가 `command_started`/`command_ack`/CHEJAN/체결 evidence를 확인한다. 근거: `storage/gateway_command_store.py`, `tools/resolve_live_sim_order.py`.
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
- 현재 판정: MarketData PR-12와 MarketReference PR-14의 장중 OBSERVE-safe 검증을 완료했다. 다음 우선순위는 P0-3 runtime execution lock fencing이다.

## P0

| ID | 증상 | 코드 근거 | 운영 영향 | 최소 수정 방향 | 테스트/SQL 검증 |
|---|---|---|---|---|---|
| P0-1 Gateway ingestion request path에 inline projection이 남아 있음 (부분 완화) | `api/routes/gateway.py::post_gateway_event()`는 append/outbox 기록 후 routing decision을 평가한다. `market_data`는 PR-7/9/11/12의 guarded mode에서, `market_reference`는 PR-14 guarded mode에서만 inline projection을 건너뛸 수 있다. market_index/regime/scan과 `handle_live_sim_gateway_event()`는 request path에 남아 있다. | 제한된 market_data/reference event는 worker로 이관할 수 있지만, 나머지 inline projection 비용과 DB write lock 경합은 여전히 Gateway POST latency와 같은 critical path에 있다. | projection별 worker/watermark/error/retry 정책과 lifecycle consumer를 확정한다. 최종적으로 POST는 raw append + outbox enqueue만 수행한다. | MarketData: `docs/runbook_market_data_append_only_controller_ko.md`. MarketReference: `docs/runbook_market_reference_projection_ko.md`. SQL: `SELECT projection_name,event_type,status,COUNT(*) FROM projection_outbox GROUP BY 1,2,3;` |
| P0-2 Candidate quote refresh TR response synthetic price_tick `event_id` collision | 수정 완료. `_process_tr_response()`는 row별 synthetic child `GatewayEvent.event_id`를 deterministic하게 생성하고, parent metadata를 보존한다. 근거: `services/market_data_service.py::_synthetic_price_tick_event_id()`, `_with_synthetic_price_tick_metadata()`. | 다중 code row TR response가 `market_tick_samples.event_id` PK 충돌 없이 projection된다. | 유지보수 방향: child id 포맷과 metadata contract를 replay/reconcile 문서에 계속 고정한다. | 테스트: `tests/test_structural_audit_guards.py::test_synthetic_tr_response_multiple_ticks_uses_unique_child_event_ids`. SQL: `SELECT event_id, metadata_json FROM market_tick_samples WHERE event_id LIKE '%synthetic_price_tick%';` |
| P0-3 `runtime_execution_lock` TTL 만료 시 첫 owner 실행 중에도 두 번째 owner가 lock 획득 가능 | `services/runtime/evaluation_run_guard.py::_acquire_lock()`은 `expires_at <= now`이면 같은 `lock_name` row를 새 owner로 upsert한다. lock holder의 heartbeat/lease renewal/fencing token이 없다. `apps/core_api.py::lifespan()`은 시작 시 `clear_runtime_execution_locks()`로 모든 lock을 삭제한다. | long-running observe cycle/entry timing/LIVE_SIM loop가 TTL을 넘기면 incremental worker가 같은 `evaluation_pipeline`을 재획득할 수 있다. multi-process 배포에서 한 Core process startup이 다른 process의 active lock을 삭제할 수 있다. latest tables가 서로 다른 run의 결과로 덮인다. | lock row에 `process_id`, `thread_id`, `heartbeat_at`, `fencing_token`을 추가한다. 실행 중 lease renewal을 하고, startup clear는 expired/self-owned lock만 대상으로 제한한다. run write 시 fencing token을 검증한다. | 추가 테스트: `test_runtime_execution_lock_can_be_reacquired_after_ttl_while_owner_still_running`. SQL: `SELECT * FROM runtime_execution_locks;` |
| P0-4 order command가 `DISPATCHED`로 claim된 뒤 Gateway가 SendOrder 전 죽으면 broker boundary 상태가 불명확 | `storage/gateway_command_store.py::_dispatch_ready_commands()`가 poll 시 `QUEUED -> DISPATCHED`를 커밋한다. `gateway/kiwoom_command_handlers.py::_handle_send_order()`에서 그 뒤 `command_started`, `order_pre_ack`, SendOrder, `command_ack` 순으로 이벤트가 생성된다. | Core에는 `DISPATCHED`만 남고 `order_pre_ack`가 없으면 broker 도달 전/후를 DB 상태만으로 구분하기 어렵다. timeout 후 `UNCONFIRMED`로 가더라도 operator가 broker absent 여부를 수동 확인해야 한다. | command 상태 모델에 `CLAIMED`, `GATEWAY_STARTED`, `PRE_ACK_RECORDED`, `BROKER_ACCEPTED`, `CHEJAN_CONFIRMED`, `UNCONFIRMED`를 분리한다. `order_pre_ack`를 file journal뿐 아니라 DB pre_ack table/unique key로 durable하게 저장한다. | 추가 테스트: `test_order_command_lifecycle_detects_dispatched_without_pre_ack`. SQL: `SELECT c.command_id FROM gateway_commands c WHERE c.status='DISPATCHED' AND c.command_type IN ('send_order','cancel_order') AND NOT EXISTS (SELECT 1 FROM gateway_events e WHERE e.command_id=c.command_id AND e.event_type='order_pre_ack');` |

## P1

| ID | 증상 | 코드 근거 | 운영 영향 | 최소 수정 방향 | 테스트/SQL 검증 |
|---|---|---|---|---|---|
| P1-1 Incremental evaluation은 Candidate -> Strategy -> Risk까지만 처리하고 EntryTiming/OrderPlan은 별도 loop에 남아 있음 | `services/runtime/incremental_evaluation.py::process_incremental_evaluation_batch()`는 `refresh_candidate_context()`, `evaluate_candidate_strategy()`, `evaluate_risk_for_candidate()` 후 queue row를 삭제한다. `evaluate_entry_timing()`은 `services/runtime/market_open_observe_cycle.py`와 `services/runtime/live_sim_operating_orchestrator.py`에서 별도 호출된다. | price tick 하나의 변경이 order_plan까지 같은 run/snapshot으로 이어지지 않는다. Strategy/Risk latest와 EntryTiming/OrderPlan latest의 source 시점이 달라질 수 있다. | incremental worker에 선택적 EntryTiming/OrderPlan stage를 붙이거나, order_plan 생성 시 strategy/risk/candidate/tick source ids를 검증한다. `source_run_id`, `source_watermark`, `data_age_sec`를 latest row에 저장한다. | Dashboard mismatch guard: `test_dashboard_snapshot_mixed_latest_rows_are_detectable_by_guard_query`. SQL: strategy/risk/order_plan latest를 candidate_id로 join해 observation id 불일치를 집계한다. |
| P1-2 projection watermark가 `market_data` 하나에 집중되어 있고 retention gating도 price_tick 중심 | `storage/projection_watermarks.py`는 generic table이지만 `services/market_data_service.py`만 적극 사용한다. `storage/event_retention.py`의 `WATERMARK_GATED_EVENT_TYPES`는 `{"price_tick"}`뿐이다. market_index/reference/scan/regime에는 projection watermark가 없다. | future replay에서 market_symbols, market_index_tick, tr_response, scan event가 retention으로 삭제되면 해당 projection rebuild가 완전하지 않을 수 있다. market_data도 tr_response projection 실패 후 watermark 전진 위험이 있다. | projection별 watermark를 추가하고 event retention은 모든 replayable event type을 각 projection watermark로 gate한다. failed projection event는 retry queue로 남기고 성공 watermark와 error watermark를 분리한다. | SQL: `SELECT * FROM projection_watermarks;`, `SELECT event_type,COUNT(*) FROM gateway_events GROUP BY event_type;` |
| P1-3 MarketRegime snapshot이 공통 market context가 아니라 후보별 refresh에서 rebuild될 수 있음 | `services/market_regime_service.py::get_market_regime_for_code()`가 `rebuild_market_regime_snapshot(connection, code)`를 직접 호출한다. `candidate_context_latest.market_context_json`은 candidate별 저장이다. | 후보 refresh 수만큼 market_regime snapshot이 생성되고, 동일 평가 run 안에서도 후보별 regime 시점이 달라질 수 있다. index tick missing/stale이면 다수 후보가 `DATA_WAIT`로 동시에 밀린다. | `market_context_snapshots`를 trade_date/market/index watermark 단위로 만들고 후보 context는 snapshot id만 참조한다. KOSPI/KOSDAQ missing은 global DATA_WAIT와 per-market fallback을 구분한다. | SQL: `SELECT target_code,COUNT(*) FROM market_regime_snapshots GROUP BY target_code;` |
| P1-4 Market index parser_status가 UNVERIFIED일 때 후보/Risk 정책과 운영 표시가 충분히 분리되지 않음 | `gateway/kiwoom_client.py::market_index_parser_evidence()`는 `mapping_status="UNVERIFIED_PILOT"`를 반환하고, tick metadata `parser_status`는 `MARKET_INDEX_PARSER_STATUS`를 담는다. `services/market_index_service.py::_has_unverified_index_parser()`는 status에 `unverified`만 노출한다. | KOSPI/KOSDAQ index tick/bar missing 또는 unverified parser 상태가 시장 전체 `DATA_WAIT`/`RISK_OFF`로 오인될 수 있다. | parser_status를 market_regime quality에 반영하고, `UNVERIFIED`는 trading block이 아니라 adapter confidence warning으로 분리한다. dashboard에 "index data usable vs parser verified"를 별도 표시한다. | 기존 테스트: `tests/test_market_index_service.py::test_market_index_records_unverified_and_implausible_guard`. SQL: `SELECT index_code,metadata_json FROM market_index_ticks_latest;` |
| P1-5 Dashboard snapshot은 서로 다른 latest table을 같은 화면에 섞어 보여준다 | `services/dashboard_service.py::build_dashboard_snapshot()`이 market_data/theme/candidate/strategy/risk/entry/live_sim latest를 독립 조회한다. pipeline summary에는 `generated_at`만 있고 section별 `source_run_id`, `source_watermark`, `trade_date`, `data_age_sec`, `generated_by`가 없다. | "화면 상태가 동일 평가 run 기준인가?"를 판단할 수 없다. 오래된 order_plan과 최신 risk가 함께 PASS처럼 보일 수 있다. | dashboard `coherency` section을 추가하고 section별 source metadata를 표준화한다. stage row에도 `source_run_id`, `source_watermark`, `data_age_sec`, `trade_date`, `generated_by`를 넣는다. | 추가 테스트: `test_dashboard_snapshot_mixed_latest_rows_are_detectable_by_guard_query`. |
| P1-6 `order_plan_id` 기반 duplicate 방지가 JSON evidence scan에 의존 | `services/live_sim/order_plan_eligibility.py::find_live_sim_intent_by_order_plan()`은 최근 `live_sim_intents` 500개를 읽어 `evidence_json.order_plan_id`를 찾는다. `live_sim_intents`에는 `order_plan_id` 컬럼/UNIQUE가 없다. | idempotency key UNIQUE는 있지만 order_plan_id 자체의 일대일 보장은 DB 제약이 아니다. evidence schema 변경이나 500개 제한에서 duplicate intent 감지가 약해진다. | `live_sim_intents.order_plan_id` nullable 컬럼과 partial unique index를 추가한다. 기존 evidence는 migration으로 backfill한다. | SQL: `PRAGMA table_info(live_sim_intents);`, `SELECT json_extract(evidence_json,'$.order_plan_id'),COUNT(*) FROM live_sim_intents GROUP BY 1 HAVING COUNT(*)>1;` |

## P2

| ID | 증상 | 코드 근거 | 운영 영향 | 최소 수정 방향 | 테스트/SQL 검증 |
|---|---|---|---|---|---|
| P2-1 incremental queue status는 backlog/stale age alert가 없다 | `services/runtime/incremental_evaluation.py::get_incremental_evaluation_status()`는 `queued_count`, `retry_exhausted_count`, `oldest_enqueued_at`, `max_attempts`를 반환하지만 stale threshold 평가와 reason code는 없다. | queue가 오래 쌓여도 dashboard/operator가 즉시 `STALE_QUEUE`로 해석하기 어렵다. | status에 `oldest_age_sec`, `stale_queue_count`, `backlog_status`, `reason_codes`를 추가한다. retry exhausted row는 별도 dead-letter/retry reset 운영 경로를 둔다. | 추가 테스트: `test_incremental_queue_backlog_and_stale_rows_are_detectable`. SQL: `SELECT COUNT(*),MIN(enqueued_at),MAX(attempts) FROM incremental_evaluation_queue;` |
| P2-2 top theme DB/leadership source가 둘 다 표시되지만 coherency warning은 제한적 | `services/dashboard_service.py`는 DB top tradable과 leadership fallback을 함께 다루며 `DASHBOARD_SAMPLE_LIMIT_HIDES_TRADABLE_THEME`는 있다. 그러나 DB snapshot과 leadership snapshot의 생성 run/watermark는 표시하지 않는다. | 운영자가 DB top theme와 leadership top theme가 같은 관측 universe인지 구분하기 어렵다. | theme/leadership section에 `source`, `snapshot_id`, `calculated_at`, `data_age_sec`, `watchset_selection_source`를 동일 포맷으로 표시한다. | 기존 테스트: `tests/test_dashboard_service.py::test_dashboard_top_theme_query_does_not_hide_tradable_themes_behind_latest_sample`. |
| P2-3 Market index TR bootstrap 설정은 status에 보이지만 Core projection과 bootstrap replay 경계가 약함 | `gateway/kiwoom_runtime.py`는 `market_index_tr_bootstrap_enabled`를 heartbeat/status에 싣지만, Core projection은 `market_index_tick` 이벤트 중심이다. | realtime callback이 막힐 때 index bootstrap이 runtime status와 projection freshness를 어떻게 보완하는지 운영자가 확인하기 어렵다. | bootstrap TR 응답을 명시적 `market_index_tick` child event 또는 `market_index_bootstrap_snapshot` projection으로 분리하고 source를 dashboard에 표시한다. | SQL: `SELECT key,value FROM gateway_status WHERE key LIKE 'market_index%';` |
| P2-4 projection 오류와 retention 상태가 dashboard에서 root cause 단위로 묶이지 않음 | `services/dashboard_service.py`는 projection errors를 나열하지만 watermark, retention candidate, projection error recent window를 하나의 RCA card로 묶지는 않는다. | "이 event가 projection 실패 후 retention 대상인지", "replay 가능성이 남아 있는지"를 수동 SQL로 봐야 한다. | event_id 기준 projection status view를 만들고 dashboard errors에 retention eligibility와 watermark position을 함께 표시한다. | SQL: `SELECT ge.rowid,ge.event_id,pw.last_event_rowid FROM gateway_events ge CROSS JOIN projection_watermarks pw WHERE pw.projection_name='market_data' ORDER BY ge.rowid DESC LIMIT 20;` |

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

## 다음 PR 권장 순서

1. runtime lock에 heartbeat/lease renewal/fencing token을 추가하고 startup clear를 expired/self-owned lock으로 제한한다.
2. `live_sim_intents.order_plan_id` backfill 및 partial UNIQUE index를 추가한 뒤 JSON scan을 직접 조회로 바꾼다.
3. order lifecycle state와 durable DB pre-ack을 broker boundary 중심으로 세분화한다.
4. Replay 검증을 도입해 inline/worker parity를 격리 DB에서 반복 검증한다.
5. projection별 success/error watermark와 retention/RCA contract를 확정한다.
6. market_index apply 준비와 limited cutover를 분리하고 parser confidence/bootstrap source를 명시한다.
7. 공통 market_context snapshot 이후 market_regime worker/reconcile/cutover를 단계화한다.
8. market_scan worker apply/reconcile/dry-run과 limited cutover를 분리한다.
9. P0-4 완료 후 LIVE_SIM lifecycle을 durable idempotent consumer로 옮긴다.
10. incremental queue stale/backlog/dead-letter/retry-reset 운영 경로를 추가한다.
11. pipeline source lineage/freshness guard와 dashboard/theme coherency를 완성한다.
12. 모든 consumer 안정화 후 Gateway POST를 raw append + durable enqueue로 제한한다.
13. 연속 10거래일 exit criteria를 충족한 뒤에만 append-only scaffolding flag와 최종 inline 경로를 정리한다.

## Replay 검증 계획 (순서 3)

- 목적: cutover PR 검증이 "장중 run-once + reconcile" 1일 1회 사이클에 묶이지 않도록, 기록된 이벤트를 오프라인에서 재주입해 하루에 여러 PR을 검증할 수 있게 한다.
- 방식: `raw_events`/`gateway_events`에서 하루치 이벤트를 export하고, 격리된 replay DB에 두 경로를 재실행한다 — (a) Gateway POST inline projection 경로, (b) `projection_outbox` worker apply 경로. 결과는 기존 `market_data` dual-run reconcile(PR-5)로 대조한다.
- 산출물: event export/재주입 스크립트, replay 전용 DB fixture, `docs/runbook_market_data_replay_verification_ko.md`.
- 안전 제약: replay는 검증 전용이며 production DB에 쓰지 않는다. 주문/LIVE_SIM/LIVE_REAL side effect는 replay에서 항상 차단한다. 안전 원칙(주문 정책 완화 없음)은 replay 환경에도 동일 적용한다.
- 연계: P1-2 projection watermark/retention 정책과 event export 포맷을 공유한다. retention gating이 확정되기 전에는 replay 대상 이벤트가 삭제되지 않도록 export를 먼저 수행한다.

## Flag 정리 계획 (순서 5)

- 전제 조건 (exit criteria): `price_tick`/`tr_response`/`condition_event` cutover가 모두 기본 경로로 동작하고, 연속 10거래일 reconcile PASS, invalid effective skip 0건, deferred side-effect 누락 0건.
- 1단계: `GATEWAY_MARKET_DATA_APPEND_ONLY_*` 계열 scaffolding flag(dry-run, per-type cutover, budget, require-*, reconcile max age 등 약 20개)와 `PROJECTION_OUTBOX_SHADOW_MODE`/`PROJECTION_OUTBOX_APPLY_PROJECTION_ENABLED`/`PROJECTION_OUTBOX_MARKET_DATA_APPLY_ENABLED` apply 게이트를 제거하고, append-only를 기본 동작으로 만든다. inline projection 경로는 긴급 rollback용 단일 flag 하나 뒤에만 유지한다.
- 2단계: 안정 확인 후 inline projection 코드 경로와 rollback flag를 삭제한다. 이 시점이 P0-1의 최종 상태(POST는 raw append + outbox enqueue만 수행)다.
- 유지 대상: worker 운영 파라미터(`PROJECTION_OUTBOX_WORKER_ENABLED`, `PROJECTION_OUTBOX_WORKER_INTERVAL_SEC`, `PROJECTION_OUTBOX_BATCH_SIZE`, `PROJECTION_OUTBOX_RETRY_LIMIT`, `PROJECTION_OUTBOX_PROCESSING_TTL_SEC`)는 flag가 아니라 운영 설정으로 남긴다.
- 각 제거 PR은 해당 flag를 참조하는 runbook(현재 6개), ops script, operator status/dashboard counter를 함께 정리한다. flag만 지우고 죽은 runbook/counter를 남기지 않는다.
- 후속: flag 정리 완료 후 dual-run reconcile은 상시 운영 체크에서 replay 검증 도구로 역할을 좁힌다.
