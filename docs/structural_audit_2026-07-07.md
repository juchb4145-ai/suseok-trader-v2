# Structural Audit - 2026-07-07

대상: `suseok-trader-v2` main  
범위: Gateway ingestion, runtime lock, incremental evaluation, market index/regime, LIVE_SIM order lifecycle, replay/retention/watermark, dashboard coherency  
안전 원칙: 이번 점검은 구조 진단과 guard/test 추가만 다룬다. `LIVE_REAL` 활성화, 주문 정책 완화, 매수 기준 완화는 하지 않는다.

## 이미 개선된 점

- Gateway event store는 `raw_events`/`gateway_events`에 `event_id` PK와 payload hash를 저장하고, 동일 payload 중복은 `duplicate_count`로 흡수하며 payload 충돌은 `CONFLICT`로 거부한다. 근거: `storage/event_store.py`.
- Gateway command는 `gateway_command_dedupe_keys.idempotency_key` PK를 사용해 active idempotency 중복 enqueue를 차단한다. LIVE_SIM `send_order`/`cancel_order`는 simulation-like mode, `live_sim_only=true`, `live_real_allowed=false`, idempotency 일치 검사를 통과해야 enqueue된다. 근거: `storage/gateway_command_store.py`.
- LIVE_SIM intent/order 주요 키는 DB 제약이 있다. `live_sim_intents.live_sim_intent_id` PK, `live_sim_intents.idempotency_key` UNIQUE, `live_sim_orders.live_sim_order_id` PK, `live_sim_orders.idempotency_key` UNIQUE, `order_plan_drafts.order_plan_id` PK, `order_plan_drafts.idempotency_key` UNIQUE. 근거: `storage/sqlite.py`.
- Market index parser의 `parser_status`는 latest row와 dashboard status에 노출된다. `PILOT_UNVERIFIED` 같은 상태를 감지할 수 있다. 근거: `services/market_index_service.py`, `gateway/kiwoom_client.py`.
- Dashboard top theme 표시는 단순 latest sample만 쓰지 않고 state-filtered query를 별도로 사용한다. 기존 테스트가 DATA_WAIT 최신 표본 뒤에 숨은 LEADING/SPREADING 테마를 검증한다. 근거: `services/dashboard_service.py`, `tests/test_dashboard_service.py`.
- Stale `DISPATCHED` order command는 timeout 후 `UNCONFIRMED`로 전환되고 운영자 종결 도구가 `command_started`/`command_ack`/CHEJAN/체결 evidence를 확인한다. 근거: `storage/gateway_command_store.py`, `tools/resolve_live_sim_order.py`.

## P0-1 진행 상태

- P0-1 mitigation step 1: `projection_outbox` skeleton을 추가했다. Gateway POST의 기존 inline projection은 유지되며, outbox는 shadow 준비용 PENDING job 관측 기반으로만 동작한다. 주문/LIVE_SIM 동작, `LIVE_REAL`, 매수 gate, safety gate 정책은 변경하지 않았다.
- PR-3: `projection_outbox` shadow verification worker를 추가했다. worker는 inline projection을 대체하지 않고 projection table을 직접 갱신하지 않으며, outbox job 상태만 `APPLIED`/`SKIPPED`/`ERROR`/`DEAD_LETTER`로 정리한다. background worker는 기본 disabled다.

## P0

| ID | 증상 | 코드 근거 | 운영 영향 | 최소 수정 방향 | 테스트/SQL 검증 |
|---|---|---|---|---|---|
| P0-1 Gateway ingestion이 append-only가 아니라 request path에서 projection을 동기 처리 | `api/routes/gateway.py::post_gateway_event()`가 `_gateway_event_write_lock` 안에서 `append_gateway_event()` 후 `process_gateway_event()`, `process_market_symbols_event()`, `process_market_index_event()`, `rebuild_market_regime_snapshot()`, `process_market_scan_event()`, incremental enqueue, `handle_live_sim_gateway_event()`까지 호출한다. | 이벤트 POST latency가 projection 비용과 DB write lock에 묶인다. 한 projection 실패가 event ingest 응답과 같은 critical path에 들어온다. SQLite `BEGIN IMMEDIATE` 경합 시 Gateway callback 수집이 밀릴 수 있다. | POST는 raw append + outbox 기록까지만 수행한다. projection별 worker를 분리하고, 각 projection은 독립 watermark/error table을 갖는다. POST 응답에는 append 결과와 enqueue 결과만 반환한다. | `tests/test_structural_audit_guards.py::test_synthetic_tr_response_multiple_ticks_detects_event_id_collision`로 projection 실패가 append 이후 발생함을 확인. SQL: `SELECT event_id,status FROM gateway_events ORDER BY rowid DESC LIMIT 20;` |
| P0-2 Candidate quote refresh TR response의 synthetic price_tick이 parent `event_id`를 재사용 | `services/market_data_service.py::_process_tr_response()`가 row별 synthetic `GatewayEvent(event_id=event.event_id, event_type="price_tick")`를 만들고, `_process_price_tick()`은 `market_tick_samples.event_id` PK에 그대로 INSERT한다. | 단일 TR response에 여러 code row가 있으면 두 번째 synthetic tick에서 `UNIQUE constraint failed: market_tick_samples.event_id`가 발생한다. transaction rollback 뒤 `market_projection_errors`만 남고 `market_data` watermark는 parent event까지 전진하므로 future incremental replay가 해당 accepted event를 건너뛸 수 있다. | synthetic tick에는 `parent_event_id:row_index:code` 기반 child event id를 부여하거나, `market_tick_samples` PK를 `(event_id, code, exchange)`로 바꾸고 parent event id를 별도 컬럼으로 둔다. projection error 시 watermark 전진 정책도 재검토한다. | 추가 테스트: `test_synthetic_tr_response_multiple_ticks_detects_event_id_collision`. SQL: `SELECT event_id,error_message FROM market_projection_errors WHERE event_id LIKE 'evt_candidate_quote_refresh%';` |
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

## 다음 PR 권장 순서

1. Gateway ingest를 append-only + projection outbox/worker로 분리하고, projection별 watermark/error/retry 정책을 확정한다.
2. market data TR synthetic tick child event id 또는 sample PK를 수정하고, error watermark와 success watermark를 분리한다.
3. runtime lock에 heartbeat/fencing token을 추가하고 startup clear 정책을 expired/self-owned lock만 대상으로 제한한다.
4. order lifecycle state를 broker boundary 중심으로 세분화하고 DB pre_ack journal/unique key를 추가한다.
5. incremental evaluation과 EntryTiming/OrderPlan의 source metadata를 연결하고 dashboard coherency section을 추가한다.
6. market_context snapshot을 공통화하고 candidate context는 snapshot id를 참조하도록 바꾼다.
