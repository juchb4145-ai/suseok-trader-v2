# FAST-5 Guarded Automatic LIVE_SIM Canary

## 목적

FAST-5는 검증된 `OrderPlanDraft`를 기존 LIVE_SIM 주문 경로로 cycle당 최대 한 건만
자동 전달한다. 이 단계는 새로운 주문 구현이 아니라 FAST-1~4와 기존 safety gate를 한 번 더
결속하는 최종 자동화 경계다.

현재 구현 완료는 운영 활성화를 뜻하지 않는다. 기본 설정은 자동 canary와 자동 queue를 모두
끄며, 별도 운영 승인 전에는 Gateway command, 브로커 호출, LIVE_SIM 주문이 발생하지 않는다.

## API

```text
GET  /api/live-sim/automation/canary/status
POST /api/live-sim/automation/canary/run-once?queue_commands=false
POST /api/live-sim/automation/canary/run-once?queue_commands=true
```

- status와 `queue_commands=false`는 read-only preview다.
- run-once는 local token을 요구한다.
- `queue_commands=true`여도 모든 gate가 `PASS`가 아니면 command를 만들지 않고
  `PROTECT_ONLY` rollback run만 기록한다.
- 기존 rollback latch가 미확인 상태면 반복 호출은 새 run이나 command를 만들지 않는다.

## 외부 evidence 결속

다음 세 증거는 상태 문자열만으로 통과하지 않는다. PASS 상태와 정확한 lowercase SHA-256을
함께 설정해야 한다.

```text
LIVE_SIM_FAST5_MANUAL_C1_STATUS=PASS
LIVE_SIM_FAST5_MANUAL_C1_EVIDENCE_SHA256=<64 hex>

LIVE_SIM_FAST5_ALPHA_STATUS=ALPHA_QUALIFIED
LIVE_SIM_FAST5_ALPHA_EVIDENCE_SHA256=<64 hex>

LIVE_SIM_FAST5_SHADOW_STATUS=PASS
LIVE_SIM_FAST5_SHADOW_EVIDENCE_SHA256=<64 hex>
```

SHA가 없거나 형식이 잘못되면 설정 로딩 또는 FAST-5 gate가 fail-closed한다.

## 동적 gate

매 cycle 직전에 다음 상태를 다시 계산한다.

- LIVE_SIM preflight `PASS`
- KRX, LIMIT BUY 전용, scale-in false
- AI order tool·AI order-plan attachment 비활성: routing 영향 `0`
- 현재 거래일 pipeline 전체 inventory가 500행 이하이고 조회 결과가 전체 inventory와 일치
- pipeline `PASS`, mismatch/missing-lineage/stale `0`
- broker-boundary effective `UNCONFIRMED=0`, maintenance fence 없음
- lifecycle qualification `PASS`, effective blocker `0`
- 최신 broker snapshot `COMPLETE`, fresh, 현재 거래일, local mismatch `0`
- Gateway heartbeat/orderable/simulation mode, kill switch, daily loss, 중복·한도 gate `PASS`

500행 제한을 전체 검증으로 오인하지 않는다. 실제 current inventory가 500행을 초과하거나
조회 수와 inventory count가 다르면 `FAST5_PIPELINE_NON_PASS`로 차단한다.

## 강제 한도

FAST-5 실행기는 일반 LIVE_SIM 설정이 더 넓어도 다음 값으로 낮춰 실행한다.

```text
exchange                   KRX
side                       BUY
order type                 LIMIT
BUY commands per cycle     1
BUY orders per day         <= 2
notional per order         <= 100,000 KRW
active orders              1
active positions           1
scale-in                   false
reprice                    false
AI routing effect          0
```

기존 OrderPlan eligibility와 실제 Gateway enqueue boundary도 다시 safety/reconcile/lifecycle를
검사하므로 FAST-5 gate 결과만으로 우회할 수 없다.

FAST-5 cycle 내부에서는 일반 local-only reconcile을 새로 기록하지 않는다. gate에서 결속한 최신
broker reconcile을 보존하고, 시간이 경과해 snapshot이 stale해지면 실제 enqueue boundary의
동적 freshness 검사가 신규 BUY를 다시 차단한다. 따라서
`LIVE_SIM_RECONCILE_REQUEST_BROKER_SNAPSHOT_ENABLED=true`도 필수다.

## 자동 rollback과 latch

gate non-PASS, 운영 cycle 오류, preflight 역전, command budget 위반이 발생하면 결과를
`PROTECT_ONLY`로 전환하고 `live_sim_operating_runs.reason_summary_json`에
`rollback_latched=true`를 기록한다. 이 상태에서는 신규 BUY가 계속 차단되고 cancel/close-only
보호 경로의 기존 계약은 유지된다.

자동 해제는 없다. 운영자가 원인과 해당 run을 확인한 뒤 정확한 run ID를 설정해야 한다.

```text
LIVE_SIM_FAST5_ROLLBACK_ACK_RUN_ID=<exact latched run id>
```

다른 ID, 빈 값 또는 이후 새 rollback run은 latch를 해제하지 않는다.

## 기본 설정

```text
LIVE_SIM_FAST5_AUTOMATIC_CANARY_ENABLED=false
LIVE_SIM_FAST5_AUTO_QUEUE_ENABLED=false
LIVE_SIM_FAST5_MAX_BUY_COMMANDS_PER_CYCLE=1
LIVE_SIM_FAST5_MAX_DAILY_BUY_COUNT=2
LIVE_SIM_FAST5_MAX_ORDER_NOTIONAL=100000
LIVE_SIM_FAST5_PIPELINE_MAX_AGE_SEC=60
```

실제 queue에는 기존 `LIVE_SIM_PILOT_AUTO_QUEUE_COMMAND=true`도 함께 필요하다. 어느 하나라도
false이면 `FAST5_AUTO_QUEUE_DISABLED`로 차단한다.

Core의 기존 operating loop는 FAST-5가 비활성일 때 `LIVE_SIM_OPERATING_LOOP_QUEUE_COMMANDS=true`
만으로 command를 만들 수 없다. 자동 queue 요청은 FAST-5 실행기로만 라우팅되며, FAST-5가
꺼져 있으면 기존 loop는 `queue_commands=false` 관측 cycle로 강제된다.

## 개발 검증 범위

- 기본 설정에서 `PROTECT_ONLY`, command `0`
- 외부 evidence SHA 미결속 차단
- 동적 gate 한 건이라도 non-PASS이면 BUY `0`
- blocked queue 요청은 rollback run 한 건만 append
- 동일 latch 반복 호출은 새 run/command `0`
- hard cap 강제와 AI/reprice/scale-in 비활성
- REAL 허용 `false`, broker path `LIVE_SIM_ONLY`

이번 개발 검증은 fixture DB에서만 수행한다. 운영 DB migration, Core/Gateway 시작, 모의·실제
브로커 요청 및 주문은 범위 밖이다.
