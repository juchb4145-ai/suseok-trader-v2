# FAST Track LIVE_SIM 수익모델 로드맵 — 2026-07

## 0. 문서 목적

이 문서는 `codex/structural-audit-completion` 병합 이후의 `main`을 기준으로,
이미 완료된 구조 개선을 반복하지 않고 다음 순서로 진행하기 위한 실행 문서다.

```text
Post-Merge Qualification
  → Pure Preview
  → 수동 LIVE_SIM 1건
  → Point-in-Time Alpha Replay
  → Profit Lab
  → Parallel Shadow
  → Broker Snapshot Reconcile
  → 제한적 자동 LIVE_SIM Canary
  → Champion / Challenger 운영
```

이 문서가 병합된 뒤에는 다음 자료의 우선순위를 구분한다.

1. **현재 실행 순서와 승급 기준:** 이 문서
2. **구조 감사 완료 근거:** `docs/structural_audit_2026-07-07.md`
3. **기존 제품 기능 이력:** `docs/roadmap.md`
4. **장기 구조 부채:** `docs/redesign_roadmap_2026-07_ko.md`

## 1. 기준선

### 1.1 구조 감사 병합으로 완료된 항목

다음은 완료된 기반으로 간주하며 새 FAST PR에서 다시 구현하지 않는다.

- runtime execution lease, heartbeat, fencing token, 제한적 startup cleanup
- `Candidate → Strategy → Risk → EntryTiming → OrderPlan` source lineage/coherency
- `live_sim_intents.order_plan_id` partial unique constraint
- `CLAIMED → GATEWAY_STARTED → PRE_ACK_RECORDED → BROKER_ACCEPTED → CHEJAN_CONFIRMED`
  broker boundary와 durable DB pre-ack
- 공통 KOSPI/KOSDAQ market-context snapshot
- market-data/reference/index/regime/scan guarded append-only 경로
- LIVE_SIM lifecycle durable consumer와 ordered retry/dead-letter
- incremental evaluation dead-letter/reset 운영 경로
- projection replay parity, watermark, retention RCA
- Gateway → Core batch data plane과 비등록 realtime callback 차단
- theme source/freshness/coherency 진단

### 1.2 현재 확인해야 할 잔여 기준선

FAST 기능 개발 전 다음 항목을 실제 현재 `main`과 운영 환경에서 다시 고정한다.

- clean checkout에서 Ruff, mypy, 전체 pytest
- 운영 SQLite 실제 schema와 현재 코드 target migration 상태
- 운영 DB `quick_check(1)`
- pipeline coherency, runtime lock, order-plan uniqueness, broker boundary 상태
- projection/lifecycle `ERROR`, `DEAD_LETTER`, stale backlog
- Gateway/Core OBSERVE smoke에서 주문 command delta 0

### 1.3 알려진 미완료 항목

- 완전 무기록 LIVE_SIM 통합 Preview API
- 실제 체결·비용·청산을 반영하는 Alpha Replay/Profit Lab
- Profit Lab과 동일 모델을 사용하는 장중 Parallel Shadow
- Kiwoom 모의계좌 미체결/보유/체결 broker snapshot 요청과 reconcile
- Alpha gate와 broker reconcile을 통과한 제한적 자동 BUY canary

## 2. 공통 안전 계약

모든 단계에서 다음을 지킨다.

- `LIVE_REAL`은 범위 밖이며 항상 금지한다.
- `TRADING_ALLOW_LIVE_REAL=false`를 유지한다.
- AI/LLM 결과는 advisory-only이며 selection, sizing, routing, exit에 사용하지 않는다.
- 전략·Risk·EntryTiming threshold를 운영 증거 없이 완화하지 않는다.
- `.env`를 PR에서 자동 수정하지 않는다.
- 운영 DB migration, retention, bulk repair는 명시적 운영 절차 없이 실행하지 않는다.
- 테스트 삭제, skip 추가, assertion 약화로 PASS를 만들지 않는다.
- 기존 emergency inline fallback과 append-only kill switch를 10거래일 exit criteria 전까지 유지한다.
- `queue_commands=false`를 무기록 Preview로 간주하지 않는다.
- 한 PR은 한 목적만 수행하고 다음 단계 구현을 섞지 않는다.

## 3. 두 개의 병렬 트랙

### Track A — 수익모델/LIVE_SIM FAST Track

이 문서의 FAST-0~FAST-6을 진행한다.

### Track B — Append-only 장기 Evidence

영속 OBSERVE evidence를 거래일별로 계속 적재한다.

- 목적: raw append + durable enqueue 기본화, scaffolding flag 정리, inline fallback 제거
- 요구: 연속 10거래일 qualified evidence
- FAST 수동 canary 차단 여부: **아님**
- 자동 flag cleanup 허용 여부: **아님**
- LIVE_SIM 세션과 동일 Kiwoom 세션을 공유해야 하면 FAST 운영을 우선하고 evidence 누락을 정직하게 기록한다.

## 4. 단계 상태판

| 단계 | 상태 | 의존성 | 핵심 산출물 |
| --- | --- | --- | --- |
| FAST-0 Post-Merge Qualification | `IN_PROGRESS` | R1~R7 병합/evidence 완료, blocker 6개 | 검증 report, 운영 baseline·처리 manifest |
| FAST-1 Pure Preview | `BLOCKED_BY_FAST_0` | FAST-0 PASS | 무기록 preview API |
| Operational Gate C1 | `BLOCKED_BY_FAST_1` | Preview PASS, 장중 KRX | 수동 LIVE_SIM 1건 lifecycle |
| FAST-2A Point-in-Time Replay | `BLOCKED_BY_FAST_0` | FAST-0 PASS | virtual-clock replay |
| FAST-2B Profit Lab | `BLOCKED_BY_FAST_2A` | replay deterministic PASS | 보수적 fill/exit/cost 성과 |
| FAST-3 Parallel Shadow | `BLOCKED_BY_FAST_2B` | Profit Lab model 고정 | shadow/live comparison |
| FAST-4 Broker Snapshot Reconcile | `BLOCKED_BY_FAST_1` | Gateway query contract | broker/local reconcile |
| FAST-5 Automatic Canary | `BLOCKED` | C1 + Alpha + Broker reconcile | 일 1~2건 자동 canary |
| FAST-6 Champion/Challenger | `BLOCKED_BY_FAST_5` | canary evidence | 모델 운영 체계 |

상태값은 다음만 사용한다.

```text
NOT_STARTED
NEXT
IN_PROGRESS
BLOCKED
BLOCKED_BY_<STAGE>
READY_FOR_REVIEW
DONE
ROLLED_BACK
```

## 5. FAST-0 — Post-Merge Qualification

### 목표

병합된 구조를 실제 현재 `main`과 운영 DB에서 검증하고, 이후 모든 FAST PR이 공유할
재현 가능한 기준선을 만든다.

### 권장 브랜치/PR

```text
branch: codex/fast-0-post-merge-qualification
PR: FAST-0: Qualify post-audit main and operating database
```

기능 결함이 없고 문서/report만 생성된다면 코드 PR 없이 운영 qualification으로 종결할 수 있다.
Ruff 또는 실제 결함이 있으면 해당 결함만 최소 수정한다.

### 작업 범위

1. `main` clean checkout 확인
2. GitHub Actions 실패 재현 및 Ruff 원인 수정
3. `python -m ruff check .`
4. `python -m mypy`
5. `python -m pytest`
6. 운영 DB read-only fingerprint 및 schema 확인
7. clone migration preflight
8. 승인된 절차로 운영 DB를 현재 코드 target schema까지 적용하거나 이미 target임을 확인
9. OBSERVE Core/Gateway smoke
10. 구조 status API/ops 도구 일괄 점검

### 필수 점검

```text
/api/operator/runtime-execution-locks/status
/api/operator/live-sim/order-plan-uniqueness/status
/api/operator/live-sim/execution-lifecycle/status
/api/operator/gateway/order-broker-boundaries/status
/api/operator/pipeline-coherency/status
/api/operator/incremental-evaluation/status
/api/operator/theme-coherency/status
/api/operator/append-only-readiness/status
/api/live-sim/operator/preflight
/api/live-sim/reconcile/latest
```

실제 route 이름이 다르면 현재 코드의 route를 source of truth로 사용하고 report에 기록한다.

### 운영 DB 규칙

- 먼저 `mode=ro`, `query_only=ON`으로 확인한다.
- clone migration이 PASS하기 전 운영 DB를 변경하지 않는다.
- Core/Gateway/theme/evidence process를 모두 종료한 뒤 migration한다.
- backup, schema, outbox count, WAL/SHM, file fingerprint를 기록한다.
- exact migration은 재실행하지 않는다. clone이 target schema에 도달한 뒤 `initialize_database`
  logical no-op와 `quick_check(1)=ok`를 확인한다.

### PASS 기준

- Ruff, mypy, 전체 pytest PASS
- 운영 DB가 현재 코드 target schema와 정확히 일치
- `quick_check(1)=ok`
- runtime active lock 이상 0
- unresolved order-plan uniqueness conflict 0
- unresolved broker-boundary gap/UNCONFIRMED 0
- pipeline coherency qualification `PASS`이고 active/pending blocker `0`; canonical historical
  `FAIL`은 삭제하지 않고 warning/audit로 보존 가능
- projection `ERROR=0`, `DEAD_LETTER=0`
- lifecycle active/manual/effective blocker `0`; canonical historical audit는 R4 classifier로
  검증되고 raw/mirror logical-subject count가 일치
- OBSERVE smoke의 `send_order/cancel_order/modify_order` delta 0
- Core/Gateway profile이 OBSERVE이며 LIVE_SIM/LIVE_REAL false

### 허용 WARN

- legacy non-ready row의 lineage 누락
- append-only evidence `1/10` 등 기간 미충족
- 기능 오류가 아닌 현재 장외 freshness

### 2026-07-15 현재 복원 상태

이 절은 최초 작성 당시의 schema 59 예시보다 우선하는 현재 실행 기록이다.

- PR #10(FAST-0), #11(FAST-0R1), #12(FAST-0R2), #13(FAST-0R3), #14(FAST-0R4)는
  `main`에 병합됐다.
- 운영 DB schema `61→62` migration은 clone preflight, backup, apply 후 무결성 검증을
  거쳐 완료됐다. preflight evidence는
  `reports/database_migration_preflight/20260715T143959Z/raw.json`, apply evidence는
  `reports/database_migration_apply/20260715T151225.565090Z/raw.json`이다.
- 현재 운영 DB schema는 `62`다. FAST-0R4 strict read-only evidence는
  `reports/live_sim_execution_lifecycle_check/20260715T174834Z/raw.json`이다.
- broker boundary raw `UNCONFIRMED=3`은 보존되고 검증된 append-only resolution 기준
  effective `UNCONFIRMED=0`이다. raw count를 삭제하거나 성공으로 오인하지 않는다.
- incremental dead-letter raw `38`은 보존돼 있고 disposition ledger가 비어 있어 effective
  blocker도 `38`이다. 별도 승인 전에는 apply하지 않는다.
- canonical pipeline `FAIL=159`는 그대로 유지한다. FAST-0R3 당시 snapshot은 historical
  closed `149`, manual `10`, active/current `0`을 별도 RCA view에서 분류했으며 canonical
  결과를 낮추지 않았다. 현재 판정은 아래 R5 snapshot의 `149/9/1` 분류가 대체한다.
- FAST-0R4 운영 재검증에서 lifecycle raw/mirrored/matched pair는 `346/346/346`,
  historical reconcile은 `265`, 전체 logical subject는 `611`이었다. active lifecycle,
  active reconcile, manual review, effective blocker는 모두 `0`이고 qualification은 `PASS`다.
  canonical은 과거 감사 이력을 보존해 `BLOCKED`이며 raw audit row를 UPDATE/DELETE하지 않는다.
- FAST-0은 broker boundary raw `UNCONFIRMED=3`, incremental effective dead-letter `38`,
  pipeline qualification blocker, authoritative runtime coverage 부재와 최종 OBSERVE smoke
  미완료 때문에 `BLOCKED`다. append-only
  resolution의 effective count가 `0`이어도 raw `UNCONFIRMED`를 FAST-0 PASS로 오인하지
  않으며 FAST-1을 시작하지 않는다.
- FAST-0R5 strict offline requalification은 2026-07-15 완료됐다. evidence는
  `reports/fast_track/fast_0_strict_requalification/20260715T211029.344659Z/`이며
  `evidence_status=PASS`, `fast0_status=BLOCKED`, exit `2`다. pinned DB identity, DB 전후
  fingerprint와 sidecar 부재, quick check, schema manifest `50/50`, migration evidence
  chain이 모두 PASS다.
- FAST-0R6 blocker resolution plan은 2026-07-16 strict read-only로 완료됐다. evidence는
  `reports/fast_track/fast_0_blocker_resolution_plan/20260715T232358.741099Z/raw.json`, SHA-256은
  `a51303b49062c0e7e3ab28cdfc2d4c414844384900a010df47c8023dfa75996e`다. evidence/plan은
  `PASS/COMPLETE`이고 DB 전후 fingerprint가 같으며 이 도구가 수행한 write·프로세스 시작·주문/
  broker operation은 0이다. 외부 process/broker 상태는 이 plan이 검증하지 않으며 Phase A에서
  별도 확인한다.
  이 결과는 처리 계획만 승인 가능한 상태이며 운영 apply를 승인하지 않는다.

### FAST-0R4 lifecycle qualification 계약

FAST-0R4는 raw `live_sim_errors`와 mirrored `live_sim_lifecycle_events`를 합산하지 않고
logical subject inventory로 분류한다. canonical count/status는 보존하고, 현재 승급 판단에는
active lifecycle/reconcile blocker와 malformed/manual-review blocker만 사용하는 별도
qualification을 제공한다.

- API: `/api/operator/live-sim/execution-lifecycle/status`
- strict tool: `python -B -m tools.ops_live_sim_execution_lifecycle_check --db <schema-62-db>`
- `code` filter는 diagnostic-only이며 global verdict/count/digest를 축소하지 않는다.
- endpoint 누락 또는 malformed payload는 market-open RCA에서 `BLOCK`이다.
- strict tool은 sidecar 부재, immutable/query-only, `quick_check(1)`, schema/app identity,
  전후 file fingerprint, full pagination과 inventory digest를 모두 검증한다.
- report에는 raw 행, raw payload, error message, 계좌 식별자, token을 기록하지 않는다.

세부 절차는 `docs/runbook_live_sim_execution_lifecycle_qualification_ko.md`를 따른다.

### FAST-0R5 strict offline requalification 계약 및 실행 상태

FAST-0R5는 schema-62 운영 DB와 승인된 migration apply report/runtime log를 입력으로
사용하는 strict read-only 최종 재점검이다. Core/Gateway/worker를 시작하거나 운영 DB를
변경하지 않는다.

- runbook: `docs/runbook_fast0_strict_requalification_ko.md`
- strict tool: `python -B -m tools.ops_fast0_strict_requalification`
- DB는 writer 종료와 checkpoint를 먼저 확인하고 `mode=ro&immutable=1`,
  `query_only=ON`으로만 연다. 하나의 pinned read handle/file identity를 hash·SQLite
  snapshot·최종 hash 전체에 유지한다.
- `quick_check(1)`, schema/app identity, pinned identity/path stability, sidecar 부재와 전후
  main file fingerprint를 필수로 검증한다.
- schema `61→62` migration을 다시 실행하지 않고 승인된 apply `raw.json`과 그 report가
  참조하는 preflight `raw.json`을 read-only evidence로 검증한다. apply report에는 승인
  기록에서 독립적으로 고정한 SHA-256을 `--migration-apply-report-sha256`로 전달한다.
- runtime log가 여러 개면 `--runtime-log <path>`를 반복하며, raw line 대신
  `EVALUATION_RUN_FENCE_LOST`, `OWNER_ALIVE_AFTER_TTL` marker count만 기록한다. 파일은 최대
  32개/합계 1 GiB이고 이 bounded historical scan은 authoritative runtime window나 현재
  owner liveness를 증명하지 않는다.
- projection outbox와 watermark unresolved error를 별도 aggregate로 검증한다.
- order-plan partial UNIQUE index는 column/predicate까지 확인하고, normalized
  `modify_order`를 gateway command/dedupe key/broker boundary 세 표면에서 각각 `0` 계약으로
  둔다.
- pipeline age 60초, incremental retry 3/backlog 100·1000/stale 30·300초, projection stale
  PROCESSING 120초와 최소 disk 1 GiB는 fixed policy이며 CLI로 완화하지 않는다.
- report는 aggregate count/boolean/digest만 포함하며 raw row, raw payload, error message,
  token 또는 원 계좌번호를 기록하지 않는다.

R5는 `evidence_status`와 `fast0_status`를 분리한다. evidence 수집과 파일 불변 검증이
`PASS`여도 checker는 runtime log coverage를 non-authoritative로 기록하고 최종 OBSERVE
smoke를 실행하지 않으므로 단독 FAST-0 결과는 `BLOCKED`, process exit code는 `2`다.
기준 완화나 DB write로 exit code를 없애지 않는다.

2026-07-15 실행 결과는 다음과 같다.

- R5 evidence/FAST-0/process exit: **`PASS` / `BLOCKED` / `2`**
- broker raw/effective UNCONFIRMED: `3/0`
- incremental raw/effective dead-letter: `38/38`; queue/retry-exhausted/stale-fail `0/0/0`
- pipeline canonical/qualification: `FAIL/BLOCKED`; historical closed `149`, missing-candidate
  manual review `9`, active-current `1`, disposition pending `158`
- lifecycle canonical/qualification/strict: `BLOCKED/PASS/PASS`; effective blocker `0`
- projection readiness/latest reconcile: `WARN/PASS`; pending `717`, processing/error/
  dead-letter/event-result-error `0/0/0/0`, invalid effective skip `0`
- order-plan exact partial UNIQUE PASS; `modify_order` 세 표면 `0`; runtime lock `0`, fence `1`
- DB identity pin은 snapshot 전체 유지/path stable PASS이고 raw file ID는 기록하지 않음
- DB와 6개 bounded log의 두 runtime marker count 모두 `0`; coverage는 non-authoritative
- 최종 OBSERVE smoke: 별도 승인 단계 미실행

qualification blocker는 `BROKER_BOUNDARY_SEMANTIC_BLOCKER`,
`BROKER_RAW_UNCONFIRMED_PRESENT`, `INCREMENTAL_EFFECTIVE_DEAD_LETTER_PRESENT`,
`PIPELINE_QUALIFICATION_NOT_PASS`, `RUNTIME_LOG_COVERAGE_NOT_AUTHORITATIVE`,
`FINAL_OBSERVE_SMOKE_NOT_RUN`의 6개다.

### FAST-0R6 dead-letter / pipeline blocker 처리 계획

R6는 승인된 R5 raw report와 같은 schema-62 DB fingerprint를 입력으로 고정하고, 한 번의 pinned
strict read-only snapshot에서 전체 대상과 preview eligibility를 집계한다. 도구는 원 dead-letter
ID, candidate ID, payload, error, 계좌·token 또는 실제 경로를 report에 기록하지 않는다.

- incremental dead-letter: raw/effective/pending `38/38/38`, preview eligible/blocked `38/0`.
  U01~U38 campaign manifest는
  `845b5b9bf82f1a2cebdda9ddf262627f3eda1e91fa53d1b027983284be43f31d`다.
- pipeline inventory: `159`, historical/missing/active `149/9/1`, current-source drift unknown `159`.
- expected action은 expired `PLAN_READY` 11, orphan 9지만 실제 preview eligible은 orphan 9뿐이다.
- expired-plan 11은 모두 candidate active-source blocker가 있고, 그중 unsafe gateway command 2와
  broker boundary unknown 1이 중첩된다. 조정 증거 없이 disposition하지 않는다.
- legacy non-ready 138은 현 계약상 eligible 0이다. active-source zero 미입증과 current-source
  drift unknown이 각각 138이므로 authoritative evidence contract/resolver 보강이 먼저다.
- active-current 1은 disposition하지 않고 audited OBSERVE environment의 targeted rebuild preview로
  분리한다.

운영 순서는 byte-identical backup/writer quiescence -> incremental 38건 건별 append -> orphan 9건
수동 증거·건별 append -> expired-plan 11건 source/command/boundary 조정 -> legacy 138건 계약 구현·
strict 재검증 -> active 1건 targeted rebuild preview -> 전체 FAST-0R5 재실행이다. 각 write 단계는
별도 승인 전까지 시작하지 않는다. 자세한 절차는
`docs/runbook_fast0_blocker_resolution_plan_ko.md`를 따른다.

FAST-0R7은 위 pipeline 분기를 한 번의 pinned snapshot에서 다시 분류하는 strict read-only
reconciliation evidence다. R7은 eligibility/effective 상태를 바꾸거나 apply를 승인하지 않는다.
승인 report는
`reports/fast_track/fast_0_pipeline_blocker_reconciliation/20260716T011001.053891Z/raw.json`,
SHA-256은 `469e827cd586cc3550913afbdc3db5307c48188b71707cc58896fe68a6498849`다. 결과는
`PASS/COMPLETE/PREPARATION_REQUIRED`, target 20, invalid 0이다. expired-plan 11건은 eligible 0,
orphan 9건은 preview eligible 9지만 모두 외부 authoritative evidence가 필요하다. source active 10과
projection 불일치 1은 별도 운영 처리 설계가 필요하다. 구현 계약은
`docs/runbook_pipeline_blocker_reconciliation_ko.md`를 따른다.

### 중단 조건

- 운영 DB migration 실패 또는 fingerprint 변화
- CI failure를 테스트 약화로 우회
- `LIVE_REAL` 허용값 발견
- unresolved order command 또는 local/broker ambiguity
- projection error/dead-letter 또는 lifecycle active/manual blocker

### Evidence

```text
reports/fast_track/fast_0_post_merge_qualification/<timestamp>/
  summary.md
  raw.json
  commands.txt
  environment_redacted.json
  test_results.txt
  database_preflight.json
```

## 6. FAST-1 — Pure LIVE_SIM Preview

### 목표

기존 `PLAN_READY`와 현재 eligibility/safety 상태를 조회하되 어떠한 run, rejection,
intent, order, command도 저장하지 않는 통합 Preview를 추가한다.

### 권장 브랜치/PR

```text
branch: codex/fast-1-pure-live-sim-preview
PR: FAST-1: Add side-effect-free LIVE_SIM canary preview
```

### 신규 API

```text
POST /api/live-sim/pilot/preview
```

### 반환 항목

- selected existing `PLAN_READY` rows
- 각 plan의 order-plan eligibility
- pipeline lineage/coherency
- safety gate
- reconcile blocker
- broker-boundary blocker
- duplicate intent 여부
- active order/position
- deterministic top candidate
- `canary_ready`
- `preview_only=true`
- `no_order_side_effects=true`
- `live_sim_only=true`
- `live_real_allowed=false`

### Preview에서 금지할 호출

- `evaluate_entry_timing()`
- `create_live_sim_intent_from_order_plan()`
- rejection 저장
- pilot/operating run 저장
- GatewayCommand 생성
- DRY_RUN intent/order 생성

### 금지 테이블 delta

다음 전후 count가 모두 0이어야 한다.

```text
entry_timing_evaluations
order_plan_drafts
live_sim_intents
live_sim_orders
live_sim_runs
live_sim_operating_runs
live_sim_rejections
gateway_commands
gateway_command_events
gateway_command_dedupe_keys
dry_run_*
```

### PASS 기준

- Preview 금지 테이블 delta 0
- 기존 eligibility와 preview 결과 일치
- lineage FAIL, duplicate intent, reconcile mismatch, UNCONFIRMED plan 선택 불가
- stable tie-breaker로 동일 입력의 top candidate 동일
- AI advisory 변경이 selection에 영향 없음
- 기존 `queue_commands=false` 호환 동작을 변경하지 않고 의미 차이를 문서화

### Evidence

```text
reports/fast_track/fast_1_preview/<timestamp>/
```

## 7. Operational Gate C1 — 수동 LIVE_SIM 1건

### 목표

수익성을 판단하지 않고 Kiwoom 모의투자 주문 경계를 한 건의 complete lifecycle로 검증한다.

### 선행 조건

- FAST-0 PASS
- FAST-1 Preview `canary_ready=true`
- KRX regular session
- Kiwoom `server_mode=SIMULATION`
- unresolved broker-boundary/reconcile/pipeline blocker 0

### 초기 한도

```text
KRX only
LIMIT BUY only
시장가 금지
주문금액 최대 100,000원
일 최대 신규 BUY 1건
active order 1
active position 1
scale-in false
operating loop false
수동 run-once only
```

### lifecycle

```text
OrderPlan
→ LiveSimIntent
→ GatewayCommand
→ CLAIMED
→ GATEWAY_STARTED
→ PRE_ACK_RECORDED
→ BROKER_ACCEPTED 또는 명시적 REJECTED
→ CHEJAN_CONFIRMED
→ local position
→ TTL cancel 또는 close-only exit
→ reconcile
```

### PASS 기준

- BUY command 정확히 1건
- durable pre-ack 전에 broker method 호출 없음
- duplicate command/intent 0
- ack/reject/chejan이 event-id 및 command-id로 연결
- 미체결 시 TTL cancel
- 체결 시 local position 생성
- exit 시 close-only SELL이며 short/over-sell 없음
- 종료 후 reconcile blocker 0
- UNCONFIRMED 0
- LIVE_REAL command 0

### 운영 원칙

첫 lifecycle이 닫히기 전에 두 번째 신규 BUY를 생성하지 않는다.

### Evidence

```text
reports/fast_track/c1_manual_canary/<trade_date>/<timestamp>/
```

## 8. FAST-2A — Point-in-Time Alpha Replay Foundation

### 목표

기존 projection replay의 read-only export, manifest, deterministic order, isolated DB,
SQLite authorizer를 재사용해 미래 데이터 누수 없는 Alpha Replay를 만든다.

### 권장 브랜치/PR

```text
branch: codex/fast-2a-point-in-time-alpha-replay
PR: FAST-2A: Add deterministic point-in-time Alpha replay
```

### 모드

```text
STRUCTURAL_REPLAY
ALPHA_REPLAY
```

`ALPHA_REPLAY`는 운영 freshness와 session rule을 사용하고 모든 시간 판단을 virtual clock으로 처리한다.

### 입력 event

- `price_tick`
- `condition_event`
- candidate quote refresh `tr_response`
- `market_symbols`
- `market_index_tick`
- valid market-index TR bootstrap
- market scan TR
- 당시 유효 theme/membership/config lineage

### Virtual Clock 적용 대상

- tick/index/theme/candidate freshness
- Strategy/Risk age
- EntryTiming/OrderPlan expiration
- cooldown
- entry window
- max hold/min hold
- EOD

### Point-in-time 금지

시각 `t`에서 `t` 이후의 tick, theme, market context, Strategy/Risk latest,
market scan 결과를 조회하면 테스트 실패로 처리한다.

### Market scan coverage

자동 continuation/pagination이 완전하지 않으면 `FIRST_PAGE_ONLY`를 manifest와 report에 표시하고,
scan completeness를 요구하는 전략의 `ALPHA_QUALIFIED` 판정을 금지한다.

### PASS 기준

- 동일 source/config/commit의 deterministic result hash 동일
- point-in-time violation 0
- 운영 DB write 0
- GatewayCommand/LIVE_SIM/DRY_RUN write 0
- event type별 coverage와 누락 source 명시

## 9. FAST-2B — Conservative Profit Lab

### 목표

Replay signal을 보수적 주문 체결·청산·비용 모델로 평가한다.

### 권장 브랜치/PR

```text
branch: codex/fast-2b-profit-lab
PR: FAST-2B: Add conservative execution and cost Profit Lab
```

### 체결 모델

#### BUY LIMIT

- signal과 같은 tick 체결 금지
- configured latency 이후 event만 사용
- 미래 tick `price <= limit`일 때만 체결
- 보수적으로 limit price를 fill price로 사용
- TTL 이후 `NO_FILL`

#### SELL LIMIT

- 미래 tick `price >= limit`일 때만 체결

#### STOP SELL

- stop 발생 이후 첫 유효 tick
- gap down이면 stop price가 아니라 불리한 실제 tick 적용

### 공통 Exit policy

Replay 전용 복제본이 아니라 DRY_RUN/LIVE_SIM과 공유 가능한 pure policy core로 분리한다.

- stop-loss
- take-profit
- trailing activation/stop
- min/max hold
- EOD flatten
- close-only
- no-short

### 비용 모델

- buy commission
- sell commission
- sell tax
- slippage ticks
- tick-size rounding
- `execution_model_version`
- `cost_model_version`

비용이 0이거나 미확정이면 net metric을 계산하더라도 `ALPHA_QUALIFIED`를 금지하고
`COST_MODEL_MISSING`으로 판정한다.

### 지표

- signal/eligible/fill/no-fill/fill-rate
- gross/net PnL
- expectancy, expectancy R
- win rate, profit factor
- MAE/MFE
- holding time
- max drawdown
- consecutive loss
- setup/regime/time/theme별 성과
- slippage/latency/fill-rate stress matrix
- 날짜 기반 TRAIN/VALIDATION/TEST

### 판정

```text
ALPHA_QUALIFIED
ALPHA_UNQUALIFIED
INSUFFICIENT_SAMPLE
COST_MODEL_MISSING
DATA_QUALITY_BLOCKED
```

초기 qualification 기본값:

- filled trades 100 이상
- 서로 다른 거래일 10일 이상
- validation/test net expectancy 양수
- profit factor 1.15 이상
- max drawdown 8R 이하
- 단일 거래일 이익 집중 35% 초과 시 WARN
- +1 tick stress에서 기대값 급락 여부 표시

기준은 config version으로 기록하되 자동 threshold 완화는 금지한다.

## 10. FAST-3 — Parallel Shadow Execution

### 목표

Profit Lab의 동일 execution/exit/cost policy로 모든 coherent plan을 장중 Shadow 처리하고,
선택된 한 건의 LIVE_SIM과 비교한다.

### 권장 브랜치/PR

```text
branch: codex/fast-3-parallel-shadow
PR: FAST-3: Add parallel shadow and LIVE_SIM comparison
```

### 실행 모델

```text
모든 coherent PLAN_READY → Shadow
stable top 1 → 수동 LIVE_SIM canary
```

### 연결 lineage

- order plan ID
- entry timing evaluation ID
- strategy/risk observation ID
- source run ID/watermark
- shadow execution/fill/position ID
- live-sim intent/order/execution/position ID

### 비교 항목

- fill/no-fill disagreement
- fill time delta
- slippage ticks/pct
- partial fill
- exit reason
- holding time
- gross/net PnL delta

### PASS 기준

- 동일 plan shadow 중복 0
- live canary 최대 1
- preflight BLOCK/kill switch에서 shadow는 유지하고 live BUY는 0
- AI 영향 0
- comparison linkage gap 0

## 11. FAST-4 — Kiwoom Simulation Broker Snapshot Reconcile

### 목표

키움 모의계좌의 미체결, 보유, 당일 체결을 read-only 조회해 Core local state와 대조한다.

### 권장 브랜치/PR

```text
branch: codex/fast-4-broker-snapshot-reconcile
PR: FAST-4: Add Kiwoom simulation broker snapshot reconciliation
```

### 범위

- Gateway read-only snapshot request command
- continuation/page lineage
- snapshot assembly 상태: `REQUESTED/COLLECTING/COMPLETE/INCOMPLETE/FAILED/STALE`
- local order/execution/position 비교
- account ID 마스킹
- REAL server fail-closed

### mismatch

- broker-only/local-only order
- broker-only/local-only position
- order quantity/status mismatch
- execution sum mismatch
- position quantity/average-price mismatch
- stale/incomplete snapshot

### 정책

- mismatch/incomplete/stale → 신규 BUY 차단
- cancel/close-only exit 보호 경로는 가능한 범위에서 유지
- 자동 local DB repair 금지

### PASS 기준

- COMPLETE fresh snapshot
- local/broker exact match 또는 정의된 tolerance PASS
- duplicate snapshot/request 0
- REAL server request 0
- mismatch fixture에서 신규 BUY command 0

## 12. FAST-5 — Guarded Automatic LIVE_SIM Canary

### 진입 조건

- FAST-0 DONE
- FAST-1 DONE
- 수동 C1 complete lifecycle PASS
- FAST-2B `ALPHA_QUALIFIED`
- FAST-3 치명적 fill disagreement 없음
- FAST-4 broker reconcile PASS
- unresolved broker boundary/UNCONFIRMED 0
- pipeline coherency qualification PASS

### 초기 운영 한도

```text
KRX only
LIMIT BUY only
cycle당 BUY 최대 1
일 최대 BUY 2
주문당 100,000원
active order 1
active position 1
scale-in false
AI routing 영향 0
kill switch 유지
```

### 자동 rollback

다음 발생 시 신규 BUY를 즉시 차단하고 `PROTECT_ONLY`로 전환한다.

- broker snapshot mismatch/stale
- UNCONFIRMED
- lifecycle dead-letter/gap
- pipeline coherency qualification non-PASS
- duplicate intent/command
- daily loss limit
- Gateway heartbeat/orderable/simulation mode failure

## 13. FAST-6 — Champion / Challenger 운영

| 역할 | 개수 | 실행 경로 |
| --- | ---: | --- |
| Champion | 1 | Replay + Shadow + LIVE_SIM |
| Challenger | 1~2 | Replay + Shadow |
| 기타 setup | 제한 | OBSERVE 통계 |

실험에서는 진입, stop, take-profit, theme threshold, market regime 중 한 축만 변경한다.
각 experiment는 다음을 기록한다.

- experiment ID
- git commit SHA
- strategy/risk/entry config hash
- data range와 split
- execution/cost model version
- qualification verdict

## 14. 공통 승급 게이트

### Gate S0 — Code

- Ruff/mypy/pytest PASS
- exact migration one-shot/CAS guard + target-schema `initialize_database` logical no-op
- default-safe config

### Gate S1 — Operational Baseline

- 해당 단계의 코드가 요구하는 current target schema exact 일치 (현재 FAST-0R5: schema 62)
- OBSERVE smoke PASS
- 주문 delta 0
- coherency/boundary/dead-letter blocker 0

### Gate S2 — Manual Execution

- Preview delta 0
- 수동 1건 lifecycle complete
- reconcile PASS

### Gate S3 — Alpha

- point-in-time deterministic
- 비용 후 out-of-sample expectancy
- qualification verdict

### Gate S4 — Broker Truth

- fresh COMPLETE broker snapshot
- local/broker reconcile PASS

### Gate S5 — Automation

- 제한 한도
- automatic rollback
- protect-only 경로

## 15. PR 및 Issue 운영 규칙

### 이름

```text
Issue: [FAST-0] Post-merge qualification
Branch: codex/fast-0-post-merge-qualification
PR: FAST-0: Qualify post-audit main and operating database
```

### 한 단계당 하나의 추적 Issue

다음 단계 Issue는 이전 단계가 `DONE` 또는 명시적 승인된 `READY_FOR_REVIEW`일 때 만든다.
미래 단계 구현 상세는 현재 코드가 바뀌면 낡을 수 있으므로 master roadmap Issue에는 체크리스트만 유지한다.

### PR 완료 시 이 문서에서 갱신할 것

- 단계 상태
- 실제 PR 번호
- 실제 merge commit
- test count/command
- evidence report 경로
- 남은 risk
- 다음 단계 진입 판정

## 16. Codex 작업 계약

각 FAST PR을 Codex에 전달할 때 다음을 공통으로 포함한다.

1. `main`과 해당 문서를 읽고 이미 구현된 기능을 먼저 확인한다.
2. 사용자 working tree의 미커밋 변경을 덮어쓰지 않는다.
3. 감으로 새 abstraction/table/flag를 만들지 않는다.
4. 기존 source-of-truth를 재사용하고 중복 truth를 만들지 않는다.
5. 설정 기본값과 LIVE_REAL 금지를 유지한다.
6. 전략/Risk threshold를 변경하지 않는다.
7. targeted test와 전체 test를 모두 실행한다.
8. 실제 실행 명령, 수치, row delta, report 경로를 남긴다.
9. acceptance criteria를 충족하지 못하면 `BLOCKED`로 보고한다.
10. 다음 FAST 단계를 같은 PR에서 구현하지 않는다.

### Codex 최종 보고 형식

```text
1. 최종 판정: PASS / WARN / BLOCKED
2. 기준 commit / branch
3. root cause 또는 구현 목표
4. 변경 파일과 schema/API
5. 안전 경계
6. 테스트 명령과 결과
7. 운영 또는 fixture evidence
8. 금지 side-effect delta
9. 남은 리스크
10. 다음 단계 진입 판정
```

## 17. 전역 중단 조건

다음 중 하나라도 발생하면 다음 단계로 넘어가지 않는다.

- LIVE_REAL 허용 또는 real server ambiguity
- unresolved `UNCONFIRMED`
- broker-boundary/pre-ack gap
- pipeline coherency qualification non-PASS 또는 active/pending blocker
- projection/lifecycle `ERROR` 또는 `DEAD_LETTER`
- 동일 order plan의 중복 intent/command
- 운영 DB 무단 mutation
- preview/shadow/replay가 금지 테이블에 쓰기
- 비용 모델 없는 Alpha PASS
- 미래 데이터 누수
- mismatch 상태에서 신규 BUY 생성

## 18. 완료 정의

FAST Track은 단순히 주문이 나갔다고 완료되지 않는다.

```text
재현 가능한 source lineage
+ 비용 후 검증된 Alpha
+ Shadow/모의체결 괴리 측정
+ broker snapshot truth reconcile
+ 제한된 자동 canary
+ fail-closed/protect-only 복구
```

위 조건을 만족하고도 `LIVE_REAL`은 자동 다음 단계가 아니다. 실계좌는 별도 safety project로만 검토한다.
