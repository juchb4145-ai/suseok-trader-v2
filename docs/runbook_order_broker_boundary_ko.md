# 주문 브로커 경계 Runbook

## 목적

`gateway_order_broker_boundaries`는 Core가 주문 명령을 claim한 시점부터 Gateway 시작, durable DB pre-ack, 브로커 접수, Chejan 확인까지를 명시적으로 기록한다. 이 기능은 LIVE_SIM/LIVE_REAL을 활성화하지 않으며 실제 주문 검증을 수행하지 않는다.

## 상태 계약

- `CLAIMED`: Core가 주문 명령을 Gateway에 전달 대상으로 claim했다.
- `GATEWAY_STARTED`: Gateway handler가 명령 처리를 시작했다.
- `PRE_ACK_RECORDED`: 주문 호출 직전 `order_pre_ack`가 Core DB에 commit됐다.
- `BROKER_ACCEPTED`: Kiwoom `SendOrder` 반환이 접수 성공을 나타냈다.
- `CHEJAN_CONFIRMED`: 주문 Chejan 또는 execution event가 확인됐다.
- `UNCONFIRMED`: claim/pre-ack 이후 제한 시간 안에 브로커 확인이 없어 reconcile이 필요하다.

상태 이벤트는 늦거나 순서가 바뀌어 도착해도 하위 상태로 되돌아가지 않는다. 원본 `UNCONFIRMED` row와 command 상태는 삭제하거나 덮어쓰지 않는다. 늦은 broker ack/Chejan은 raw boundary를 상위 상태로 전환할 수 있고, 별도 운영자 resolution은 raw 상태를 바꾸지 않은 채 effective 상태만 계산한다.

### Raw와 effective 상태

- `state`, `state_counts`, `unconfirmed_count`, `block_new_order_routing`은 기존 raw 계약이다.
- `effective_state`, `effective_state_counts`, `effective_unconfirmed_count`는 append-only resolution과 그 이후 도착한 broker evidence까지 반영한다.
- 감사 가능한 `BROKER_NOT_REACHED` resolution이 유효할 때 raw `UNCONFIRMED`는 보존되고 effective 상태만 `RESOLVED_BROKER_NOT_REACHED`가 된다.
- resolution 이후 pre-ack, ack, Chejan, execution 또는 broker order evidence가 발견되면 resolution row는 보존한다. `resolution_status`는 `OVERRIDDEN_BY_BROKER_EVIDENCE` 또는 raw 전이가 먼저 일어난 경우 `OVERRIDDEN_BY_RAW_STATE`가 되며, effective 상태는 현재 raw 상태를 따르고 qualification은 다시 차단된다.
- `qualification_block_new_order_routing`은 FAST-0 판정용 gate다. `effective_block_new_order_routing`은 실제 주문 routing gate이며 active resolution의 `routing_fence_active`도 포함한다.
- FAST-0R1 resolution 자체는 maintenance fence를 해제하지 않으므로 실제 주문 routing은 계속 차단된다. schema 63의 별도 append-only fence-event 절차와 건별 당일 승인이 없으면 resolution만으로 주문을 재개할 수 없다.
- 구조 오류, resolution schema/index 오류, 유효성을 판정할 수 없는 상태는 항상 fail closed한다.

기존 consumer가 raw 필드를 읽는 동작은 변경하지 않는다. 신규 주문 safety gate는 명시적으로 effective gate를 소비하며, FAST-0은 `effective_unconfirmed_count=0`만으로 단독 통과시키지 않고 구조 FAIL과 다른 qualification gate도 함께 확인한다.

## Durable pre-ack

Gateway는 `send_order`와 `cancel_order` 모두 다음 순서를 지킨다.

1. `command_started`를 생성한다.
2. 선택적으로 local JSONL pre-ack journal을 append한다.
3. Core `/api/gateway/events`에 `order_pre_ack`를 동기 POST한다.
4. 응답의 `accepted=true`와 `durable_pre_ack_recorded=true`를 모두 확인한다.
5. 확인된 경우에만 Kiwoom client의 주문 메서드를 호출한다.

Core timeout, HTTP 오류, malformed 응답, DB commit 미확인 시 `DURABLE_DB_PRE_ACK_FAILED`로 fail closed하며 broker 메서드는 호출하지 않는다. 비동기 Core worker queue는 이 경계에 사용하지 않는다.

durable pre-ack 확인 뒤 Kiwoom broker method 자체가 예외를 던지면 접수 여부를 단정할 수 없다. 이 경우 `order_broker_unconfirmed`를 기록해 command/boundary를 즉시 `UNCONFIRMED`로 전환하고 broker snapshot/Chejan reconcile 전까지 신규 BUY routing을 차단한다.

## Startup migration

Schema migration은 savepoint 안에서 table/index를 생성하고 기존 `gateway_commands`, `gateway_events`, `gateway_command_events`를 결정적 순서로 backfill한다. 과거 `ACKED`만 있고 `order_pre_ack` evidence가 없으면 `BROKER_ACCEPTED` 상태는 보존하지만 durable pre-ack 시각을 만들지 않는다. 이 경우 status는 `DURABLE_PRE_ACK_GAP`으로 FAIL이다.

`gateway_order_broker_boundary_resolutions`는 resolution과 revoke 이력을 append-only로 저장한다. UPDATE/DELETE trigger가 변경을 거부하고, 각 record는 source fingerprint, 외부 evidence SHA-256, 비식별 evidence reference, operator ID와 reason code를 보존한다. 원 계좌번호, order idempotency key, evidence 파일 경로 또는 파일 내용은 저장하지 않는다.

기존 `UNCONFIRMED`는 직접 UPDATE하거나 기존 LIVE_SIM 주문 종결 도구로 임의 해소하지 않는다. 검증된 HTS artifact와 source fingerprint를 갖춘 append-only resolution만 effective 상태에 반영한다.

## API 확인

```powershell
Invoke-RestMethod `
  -Uri "http://127.0.0.1:8000/api/operator/gateway/order-broker-boundaries/status" `
  -Headers @{"X-Local-Token"=$env:TRADING_CORE_TOKEN}

Invoke-RestMethod `
  -Uri "http://127.0.0.1:8000/api/operator/gateway/order-broker-boundaries?limit=100" `
  -Headers @{"X-Local-Token"=$env:TRADING_CORE_TOKEN}
```

FAST-0R1은 broker-boundary write POST API를 제공하지 않는다. Resolution 한 건을 기록하려고 Core를 새로 시작하지 말고, 아래 전용 CLI를 사용한다. 기존 GET은 status/list 확인용 read-only surface다.

정상 기준:

- `status=PASS`, 또는 기존 미확정 건만 존재하는 `WARN`
- `table_exists=true`
- `required_indexes_present=true`
- `missing_boundary_count=0`
- `durable_pre_ack_gap_count=0`
- `duplicate_idempotency_count=0`
- `command_state_mismatch_count=0`
- raw `UNCONFIRMED>0`이면 raw `block_new_order_routing=true`
- effective `UNCONFIRMED>0`이면 `effective_block_new_order_routing=true`
- active resolution이 있으면 `resolution_maintenance_fence_active=true`와 `effective_block_new_order_routing=true`
- FAST-0 qualification은 `qualification_block_new_order_routing=false`와 `fast_0_status=CLEAR`로 판정하며, 이는 실제 routing 허용을 의미하지 않는다.
- resolution 적용 뒤에도 raw count와 raw row는 감소하지 않는다.

Dashboard fast path:

```powershell
Invoke-RestMethod `
  -Uri "http://127.0.0.1:8000/api/dashboard/snapshot?fast=true&sections=order_broker_boundaries,pipeline_summary" `
  -Headers @{"X-Local-Token"=$env:TRADING_CORE_TOKEN}
```

## OBSERVE 점검

최초 운영 DB 점검 전에 Core를 새로 시작하지 않는다. Core startup은 schema 초기화, lock cleanup, 일반 RW 연결과 WAL pragma 등 DB write를 수행할 수 있으므로 API가 read-only여도 startup 자체는 비파괴 점검이 아니다. process environment 몇 개만 바꾸는 방식도 사용하지 않는다. repository `.env`가 값을 덮어쓸 수 있다.

먼저 strict read-only source schema/파일 fingerprint와 아래 CLI preview를 확보하고, migration은 복제 DB preflight로만 검증한다. API checker는 이미 별도 절차로 OBSERVE-safe startup이 검증된 Core가 실행 중인 경우에만 사용한다. 이 runbook의 점검 절차 자체는 Core/Gateway 시작을 승인하지 않으며, Gateway를 시작하거나 `/api/gateway/commands`를 polling하지 않는다.

나중에 별도 승인으로 Core startup이 필요하면 repository `.env`가 아닌 전용 safe env를 만들고 `TRADING_ENV_FILE`로 명시한다. 그 startup은 운영 DB 변경 가능성이 있는 별도 단계로 기록하며 최초 read-only 증거 수집과 섞지 않는다.

```powershell
& $python -B -m tools.ops_order_broker_boundary_check `
  --core-url http://127.0.0.1:8000 `
  --token $env:TRADING_CORE_TOKEN `
  --expected-db $db `
  --out-dir reports/order_broker_boundary
```

점검 도구는 read-only API만 호출하며 command를 enqueue/poll하지 않는다. 시작 전후 전체 command 수, 상태별 count map, command type별 count map과 order command 수가 모두 동일해야 한다.

FAST-0 qualification에서는 일반 WARN/exit-0만으로 통과 판정하지 않고 `--require-effective-clear`를 추가한다. 이 모드는 raw historical `UNCONFIRMED`를 WARN으로 보존하면서 effective `UNCONFIRMED`가 한 건이라도 남으면 실패한다.

```powershell
& $python -B -m tools.ops_order_broker_boundary_check `
  --core-url http://127.0.0.1:8000 `
  --token $env:TRADING_CORE_TOKEN `
  --expected-db $db `
  --out-dir reports/order_broker_boundary `
  --require-effective-clear
```

## FAST-0R1 운영자 resolution CLI

`tools.resolve_order_broker_boundary`는 Core/API/Gateway를 시작하지 않는다. 기본 동작은 SQLite URI `mode=ro`와 `query_only=ON`을 사용하는 preview다. preview는 schema migration, resolution INSERT, command poll, reconcile 또는 broker 호출을 수행하지 않는다.

```powershell
& $python -B -m tools.resolve_order_broker_boundary `
  --db $db `
  --command-id $commandId
```

preview에서 다음 항목을 확인한다.

- raw state가 `UNCONFIRMED`
- resolution eligibility와 모든 reason code
- `source_boundary_fingerprint`
- pre-ack, ack, Chejan, execution, broker order evidence가 0
- 현재 effective state, active resolution과 resolution event count

Apply는 코드 배포와 별도의 승인된 운영 단계다. 세 command를 한 번에 처리하지 않고 반드시 한 row씩 preview → evidence 확인 → apply → read-only preview 순서로 진행한다.

### Apply 전제

1. Core, Gateway, theme loop와 주문 producer를 모두 종료한다.
2. simulation HTS 주문/체결 이력을 해당 account/code/side/time window로 export하고 비식별 artifact를 만든다.
3. repository `.env`를 수정하거나 직접 사용하지 않는다. 별도 OBSERVE-safe env 파일을 만들고 `TRADING_ENV_FILE`로 명시한다.
4. 별도 env에는 운영 DB 경로와 함께 `TRADING_PROFILE=OBSERVE`, `TRADING_MODE=OBSERVE`, 양쪽 LIVE allow=false, 모든 routing/gateway/operating producer=false, `THEME_REFRESH_QUEUE_MARKET_SCAN_COMMANDS=false`, kill switch=true를 둔다.
5. preview에서 받은 fingerprint를 apply 직전에 다시 확인한다.

도구는 apply에서 다음 조건을 모두 검사한다.

- `TRADING_ENV_FILE`이 명시됐고 repository 기본 `.env`가 아님
- OBSERVE profile/mode, LIVE_SIM/LIVE_REAL allow=false
- LIVE_SIM, dry-run order routing, cancel/exit/reprice/operating loop를 포함한 command producer가 모두 off
- `THEME_REFRESH_QUEUE_MARKET_SCAN_COMMANDS`가 safe env에서 명시적으로 false
- LIVE_SIM kill switch가 on
- `--db`와 safe env의 DB 경로가 동일함
- evidence 파일이 존재하고 비어 있지 않음
- expected fingerprint가 64자리 lowercase SHA-256 형식임
- 세 acknowledgement flag가 모두 명시됨

```powershell
$env:TRADING_ENV_FILE = $safeEnv

& $python -B -m tools.resolve_order_broker_boundary `
  --db $db `
  --command-id $commandId `
  --apply `
  --request-id $requestId `
  --expected-fingerprint $sourceBoundaryFingerprint `
  --evidence-file $redactedHtsArtifact `
  --evidence-ref HTS_EXPORT_ALPHA `
  --operator-id operator.alpha `
  --confirm-no-broker-order-or-execution `
  --acknowledge-late-evidence-precedence `
  --acknowledge-routing-gate-change
```

`evidence-ref`와 `operator-id`에는 계좌번호, token, 파일 경로, 개인정보를 넣지 않는다. 도구는 evidence 파일을 streaming SHA-256으로 읽고 hash만 storage layer에 전달한다. 파일 경로와 내용은 성공·오류 출력 및 DB에 기록하지 않는다.

Apply는 `BEGIN IMMEDIATE` 기반 source-fingerprint CAS를 거친 append-only INSERT 한 건만 허용한다. 동일 request/hash 재시도는 idempotent하다. 현재도 유효한 요청은 `REPLAYED_EFFECTIVE`, revoke나 late evidence로 이미 무효화된 과거 요청은 `REPLAYED_NOT_EFFECTIVE`/nonzero로 구분한다. stale fingerprint, 다른 payload의 request 재사용, broker evidence 존재, LIVE_REAL 가능 설정은 거부한다. `gateway_commands`, `live_sim_orders`, `live_sim_intents`, raw boundary를 변경하거나 command를 생성하지 않는다.

Apply 직후 같은 명령을 `--apply` 없이 다시 실행하고 다음을 확인한다.

- raw row와 raw `UNCONFIRMED` count가 보존됨
- 해당 row의 effective state만 의도대로 변경됨
- command count와 order-command count delta가 0
- resolution maintenance fence가 유지되어 실제 `effective_block_new_order_routing`은 계속 true
- `qualification_block_new_order_routing=false`와 `fast_0_status=CLEAR`는 qualification 해소만 뜻하며 주문 routing 허용이 아님
- 늦은 broker evidence가 있으면 resolution보다 우선하여 fail closed됨

### Append-only revoke/correction

잘못된 artifact, 잘못된 operator 판정 또는 정책상 철회가 확인되면 기존 resolution을 UPDATE/DELETE하지 않는다. 새 correction artifact를 만들고 최신 preview fingerprint와 active `resolution_id`를 사용해 append-only revoke를 기록한다.

```powershell
& $python -B -m tools.resolve_order_broker_boundary `
  --db $db `
  --command-id $commandId `
  --apply `
  --revoke-resolution-id $activeResolutionId `
  --request-id $revokeRequestId `
  --expected-fingerprint $sourceBoundaryFingerprint `
  --evidence-file $redactedCorrectionArtifact `
  --evidence-ref HTS_CORRECTION_ALPHA `
  --operator-id operator.alpha `
  --acknowledge-correction-or-contradiction `
  --acknowledge-late-evidence-precedence `
  --acknowledge-routing-gate-change
```

`--apply`에 `--revoke-resolution-id`가 없으면 `BROKER_NOT_REACHED` resolution, 있으면 해당 active resolution의 revoke다. 한 요청은 두 action을 동시에 수행하지 않는다. Revoke는 고정 `reason_code=OPERATOR_REVOKED_BROKER_NOT_REACHED`, `evidence_type=SIMULATION_HTS_ORDER_HISTORY_CORRECTION`을 사용하며 같은 safe env, evidence SHA-256, CAS와 세 acknowledgement 계약을 그대로 적용한다. 성공 후 raw row는 계속 보존되고 effective 상태는 다시 `UNCONFIRMED`가 되어 차단된다. 동일 request/hash 재시도만 idempotent하다.

기존 `tools.resolve_live_sim_order`는 local order/intent 종결용이며 broker-boundary resolution 도구가 아니다. 두 도구를 한 작업으로 묶거나 기존 lifecycle event를 새 HTS artifact 대신 사용하지 않는다.

## FAST-5 maintenance fence release

`BROKER_NOT_REACHED` resolution은 과거 raw `UNCONFIRMED`를 qualification에서 제외하더라도 기본적으로
신규 주문 routing maintenance fence를 유지한다. FAST-5 bootstrap을 준비할 때 이 fence를 기존
resolution 행의 UPDATE/DELETE 또는 revoke로 없애지 않는다. schema 63의
`gateway_order_broker_boundary_fence_events`에 `RELEASE` 또는 `REINSTATE`를 append-only로 기록한다.

schema 62 운영 DB는 일반 Core startup으로 올리지 않는다. 먼저 프로세스와 writer가 없는 상태에서
WAL/SHM/journal sidecar가 없음을 확인하고, 별도 clone에 exact migration preflight를 수행한다.

```powershell
& $python -B -m tools.ops_database_migration_62_to_63_preflight `
  --source-db $db `
  --clone-db $clone `
  --out-dir "$evidence\database_migration_62_to_63_preflight"
```

운영 DB의 schema 62→63 apply는 위 PASS raw report와 별도 운영 승인을 받은 뒤에만 수행한다.
migration은 fence-event table/index/append-only trigger만 추가하고 기존 table 내용과 주문 상태를
보존해야 한다. 이 단계와 개별 fence release 승인은 서로 대체하지 않는다.

Apply 도구가 실패를 출력하더라도 `committed=true`이면 schema 63 변경은 이미 반영된 상태다.
같은 migration을 재실행하지 말고 운영 DB를 strict read-only로 재확인한 뒤 evidence 생성 실패를
별도 복구한다. `committed=false`인 사전 검증 실패만 미적용으로 취급한다.

Apply 승인은 preflight raw 파일 자체의 lowercase SHA-256에도 결속한다. 승인된 digest를
`--preflight-raw-sha256`으로 전달하지 않거나 파일 digest가 달라지면 운영 DB를 열기 전에 거부한다.

```powershell
& $python -B -m tools.ops_database_migration_62_to_63_apply `
  --source-db $db `
  --preflight-raw $preflightRaw `
  --preflight-raw-sha256 $approvedPreflightRawSha256 `
  --backup-db $backup `
  --acknowledge APPLY_EXACT_SCHEMA_62_TO_63_FENCE_EVENT_LEDGER `
  --out-dir "$evidence\database_migration_62_to_63_apply"
```

schema 63 적용 후 각 U alias를 한 건씩 strict read-only preflight한다. preflight는 명령 ID,
active resolution ID/request hash, source boundary fingerprint, 직전 fence event, 거래일, 운영 DB
instance identity digest, exact app/schema, 전체 Gateway command count/order count/full-state fingerprint와
증거 SHA를 하나의 공개 approval payload에 결속한다. 다른 경로의 clone DB에는 같은 승인을 재사용할
수 없다.

```powershell
& $python -B -m tools.release_order_broker_boundary_fence `
  --db $db `
  --action release `
  --command-id $commandId `
  --command-alias U01 `
  --approval-id fence-release-jul23-u01 `
  --request-id fence-release-request-u01 `
  --operator-id operator.bootstrap `
  --trade-date $tradeDate `
  --out "$evidence\U01_fence_release_preflight.json"
```

출력의 `status=APPROVAL_READY`, report SHA-256, approval `expected_sha256`을 확인한다. preflight만으로
apply가 승인되지 않는다. 각 alias마다 해당 approval SHA를 인용한 별도 승인이 필요하며, 세 건을
한 번에 승인하거나 자동 연속 처리하지 않는다.

승인된 한 건의 apply는 repository 기본 `.env`가 아닌 별도 OBSERVE-safe env를 사용하고 기존
resolution apply와 같은 producer-off/kill-switch 계약을 만족해야 한다.

```powershell
$env:TRADING_ENV_FILE = $safeEnv

& $python -B -m tools.release_order_broker_boundary_fence `
  --db $db `
  --apply `
  --preflight-report "$evidence\U01_fence_release_preflight.json" `
  --preflight-report-sha256 $preflightReportSha256 `
  --approval-sha256 $approvedSha256 `
  --acknowledge-append-only-fence-change `
  --acknowledge-raw-history-preserved `
  --acknowledge-late-evidence-fail-closed
```

Apply는 preflight 파일과 approval canonical JSON을 다시 hash하고, 저장소 계층의
`BEGIN IMMEDIATE` 안에서 당일·DB identity·app/schema·Gateway command snapshot·boundary CAS를 모두
재검증한 뒤 append-only event 한 건만 기록한다. 승인 뒤 어떤 명령 상태라도 바뀌었으면 새
preflight와 새 승인이 필요하다. raw boundary와 기존 resolution은 그대로 남는다. 늦은 broker
evidence, resolution/fingerprint drift, ledger 손상, DB identity 변경 또는 거래일 변경은 release를
자동으로 무효화해 maintenance fence를 복원한다. 활성 주문 명령과 runtime lock은 release ledger를
수정하지 않고 FAST-5 동적 gate에서 별도로 차단한다.

Fence 도구의 성공 상태는 신규 apply의 `APPLIED` 또는 현재도 유효한 exact replay의
`REPLAYED_EFFECTIVE`뿐이다. `APPLIED_WITH_VERIFICATION_FAILURE`와
`APPLIED_NOT_EFFECTIVE`는 event가 이미 commit된 실패이므로 같은 request를 새 승인처럼 재적용하지
않는다. ledger와 effective fence를 strict read-only로 확인한 뒤 새 preflight 여부를 결정한다.

잘못 해제했거나 bootstrap을 중단해야 하면 기존 `RELEASE`를 수정하지 않고 `--action reinstate`로
새 preflight와 별도 SHA 승인을 만든 뒤 `REINSTATE`를 append한다. 실제 broker snapshot 조회,
FAST-5 `PENDING` 전환, reservation과 주문 queue는 fence release 승인에 포함되지 않는다.

## 장애 처리

- `DURABLE_PRE_ACK_GAP`: 신규 주문 routing을 중지하고 원본 event/journal을 대조한다. 누락 evidence를 추정 생성하지 않는다.
- `UNCONFIRMED_ORDER_BOUNDARY_REQUIRES_RECONCILE`: broker snapshot/Chejan과 수동 reconcile evidence를 확보할 때까지 WARN과 routing block을 유지한다.
- `STALE_BOUNDARY_FINGERPRINT`: 새 preview를 수행하고 새 evidence를 대조한다. 기존 resolution row나 raw boundary를 수정하지 않는다.
- `BROKER_NOT_REACHED_NOT_PROVABLE`: 세부 broker-reach reason을 확인하고 late event, HTS 주문/체결, broker order number를 수동 조사한다.
- `OVERRIDDEN_BY_BROKER_EVIDENCE`: resolution을 삭제하지 말고 신규 주문을 차단한 상태에서 contradiction을 조사한다.
- `ORDER_BOUNDARY_IDEMPOTENCY_DUPLICATE`: 운영 DB row를 삭제하지 말고 복제 DB에서 migration을 재현한다.
- `ORDER_COMMAND_BOUNDARY_STATE_MISMATCH`: command event 순서와 terminal 상태를 확인하고 Core를 OBSERVE로 유지한다.

운영 DB 삭제, destructive migration, 실제 주문/취소 호출은 금지한다. 재현과 replay는 주문 side effect를 차단한 복제 DB에서만 수행한다.
