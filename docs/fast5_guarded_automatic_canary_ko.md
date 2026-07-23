# FAST-5 Guarded Automatic LIVE_SIM Canary

## 목적

FAST-5는 검증된 `OrderPlanDraft`를 기존 LIVE_SIM 주문 경로로 자동 전달한다. 최초 브로커
lifecycle 검증은 수동 주문 대신 **운영자가 당일 계약 해시에 승인한 자동 1회 bootstrap**으로
수행한다. 새로운 주문 구현이 아니라 FAST-1~4와 기존 safety gate를 실제 모의투자 경계에서
결속하는 단계다.

기본 설정은 모두 꺼져 있다. 이 문서와 코드의 구현만으로 Gateway command, 브로커 호출,
LIVE_SIM 주문이 발생하지 않으며 LIVE_REAL은 항상 금지된다.

## API

```text
GET  /api/live-sim/automation/canary/status
POST /api/live-sim/automation/canary/run-once?queue_commands=false
POST /api/live-sim/automation/canary/run-once?queue_commands=true
```

- status와 `queue_commands=false`는 read-only preview다.
- run-once는 local token을 요구한다.
- gate non-PASS는 command와 운영 run을 만들지 않고 `PROTECT_ONLY`로 거절한다.
- bootstrap 1회가 이미 예약됐으면 신규 BUY는 만들지 않고 `PROTECT_ONLY` 취소·청산
  lifecycle만 허용한다.
- 실제 실행 오류나 BUY budget 불변식 위반만 rollback latch를 기록한다.

## 1회 bootstrap 승인 계약

bootstrap은 종목을 임의로 지정하는 승인이 아니다. 현재 pipeline에서 모든 동적 gate를 통과한
plan 하나를 선택하되, 운영자가 다음 공개 payload 전체의 SHA-256에 승인한다.

```text
contract/policy version
approval id + trade date
LIVE_SIM only, LIVE_REAL false
KRX LIMIT BUY only
BUY command/day 1
order notional <= 100,000 KRW
active order/position 1
scale-in/reprice/AI routing false
```

권장 절차는 다음과 같다.

1. `BOOTSTRAP_STATUS=BLOCKED` 상태에서 거래일과 새로운 approval ID를 설정한다.
2. status API의 `bootstrap_approval.payload`, `canonical_json`, `expected_sha256`을 검토한다.
3. 별도 운영 승인 후 정확한 SHA를 설정하고 `BOOTSTRAP_STATUS=PENDING`으로 전환한다.
4. 장중 status가 `phase=BOOTSTRAP`, `ready=true`일 때 queue run-once를 한 번 실행한다.
5. complete lifecycle과 reconcile evidence를 검토한 후 evidence 파일 SHA를 결속하여
   `BOOTSTRAP_STATUS=PASS`로 승격한다.

```text
LIVE_SIM_FAST5_BOOTSTRAP_STATUS=BLOCKED|PENDING|PASS
LIVE_SIM_FAST5_BOOTSTRAP_TRADE_DATE=2026-07-20
LIVE_SIM_FAST5_BOOTSTRAP_APPROVAL_ID=<unique operator approval id>
LIVE_SIM_FAST5_BOOTSTRAP_APPROVAL_SHA256=<status가 계산한 64 hex>
LIVE_SIM_FAST5_BOOTSTRAP_EVIDENCE_SHA256=<complete lifecycle evidence 64 hex>
```

`PENDING`은 거래일, approval ID, lowercase approval SHA가 필수다. 설정 SHA가 현재 payload와
다르거나 요청 거래일이 다르면 차단된다. queue 전에 같은 SHA의 예약 run을 SQLite
`BEGIN IMMEDIATE`로 먼저 append하므로 동시 요청도 한 건만 신규 BUY 단계에 진입한다. 주문이
만들어지지 않았더라도 승인은 소비되며, 재시도에는 새 approval ID와 새 SHA가 필요하다.
기존 `LIVE_SIM_FAST5_MANUAL_C1_*` 변수는 더 이상 gate 입력이 아니며 bootstrap 변수로 명시적으로
전환하지 않으면 기본 `BLOCKED` 상태가 유지된다.

## bootstrap과 정규 자동화의 차이

최초 bootstrap의 목적은 수익성 재증명이 아니라 실제 SIM broker lifecycle 확인이다. 따라서
`PENDING` 단계에서는 Alpha와 Parallel Shadow가 `WARN`인 채로도 동적 안전 gate가 모두 PASS면
1회 실행할 수 있다.

bootstrap lifecycle이 PASS로 결속된 뒤의 정규 자동화는 다음 증거를 모두 요구한다.

```text
LIVE_SIM_FAST5_BOOTSTRAP_STATUS=PASS
LIVE_SIM_FAST5_BOOTSTRAP_EVIDENCE_SHA256=<64 hex>
LIVE_SIM_FAST5_ALPHA_STATUS=ALPHA_QUALIFIED
LIVE_SIM_FAST5_ALPHA_EVIDENCE_SHA256=<64 hex>
LIVE_SIM_FAST5_SHADOW_STATUS=PASS
LIVE_SIM_FAST5_SHADOW_EVIDENCE_SHA256=<64 hex>
```

## 동적 gate

신규 BUY 직전에 다음 상태를 매번 다시 계산한다.

- LIVE_SIM preflight `PASS`, Kiwoom simulation mode와 orderable heartbeat
- KRX, LIMIT BUY 전용, scale-in/market order false
- AI order tool·order-plan attachment 비활성: routing 영향 `0`
- 현재 거래일 pipeline 전체 inventory가 500행 이하이며 전수 진단 조회 결과와 전체 count 일치
- 최신 order plan 중 미만료 `PLAN_READY`가 1건 이상이고, 그 전부의 내부 FK와
  strategy/risk/entry/plan source run·watermark·현재 source·age 검증이 `PASS`
- broker-boundary effective `UNCONFIRMED=0`, maintenance fence 없음
- maintenance fence release가 현재 거래일에 유효하고 운영 DB identity·schema 63 계약과 일치
- lifecycle qualification `PASS`, effective blocker `0`
- 최신 broker snapshot `COMPLETE`, fresh, 현재 거래일, local mismatch `0`
- kill switch, daily loss, 중복, active order/position, notional gate `PASS`

500행 제한을 전체 검증으로 오인하지 않는다. current inventory가 500행을 초과하거나 조회 수와
inventory count가 다르면 `FAST5_PIPELINE_NON_PASS`로 차단한다.
전수 진단의 과거/non-ready stale·missing·mismatch는 계속 표시하되 FAST-5 신규 BUY gate를
직접 차단하지 않는다. 반대로 최신 미만료 `PLAN_READY`의 stale, lineage/FK missing, source
run/watermark mismatch는 한 건만 있어도 차단한다. 미만료 `PLAN_READY=0`, 잘못된 만료시각,
500행 selection truncation도 fail-closed다.

## 강제 한도

| 한도 | bootstrap | bootstrap PASS 이후 |
| --- | ---: | ---: |
| cycle당 신규 BUY | 1 | 1 |
| 일 신규 BUY | 1 | 최대 2 |
| 주문당 금액 | 최대 100,000원 | 최대 100,000원 |
| active order / position | 1 / 1 | 1 / 1 |

두 단계 모두 KRX LIMIT BUY, 시장가·scale-in·자동 reprice·AI routing 금지다. 기존 OrderPlan
eligibility와 Gateway enqueue boundary도 safety/reconcile/lifecycle를 다시 검사한다.
`LIVE_SIM_RECONCILE_REQUEST_BROKER_SNAPSHOT_ENABLED=true`가 필수다.

## rollback과 보호 동작

준비되지 않은 설정, stale 데이터, evidence 미결속 같은 gate 거절은 DB에 rollback latch를
남기지 않는다. 설정이나 시장 입력을 고친 뒤 새 preview로 다시 판단할 수 있다.

실제 queue 단계에 진입한 뒤 운영 cycle 오류나 command budget 위반이 발생하면
`rollback_latched=true`를 기록하고 신규 BUY를 차단한다. 자동 해제는 없으며 운영자가 원인과
run을 확인한 뒤 정확한 ID를 설정한다. queue 호출 중 예외는 command count가 0으로 보이더라도
`side_effects_unknown=true`, `no_order_side_effects=false`로 기록한다. 이전 v1/v2 latch도
pipeline qualification 범위를 명시한 v3에서 그대로 유효하다.

```text
LIVE_SIM_FAST5_ROLLBACK_ACK_RUN_ID=<exact latched run id>
```

bootstrap 예약 후 반복 cycle은 BUY 없이 `PROTECT_ONLY`로 기존 주문의 cancel/close-only
lifecycle을 진행할 수 있다. 보호 cycle 자체도 preflight를 우회하지 않는다.

## 기본 설정

```text
LIVE_SIM_FAST5_AUTOMATIC_CANARY_ENABLED=false
LIVE_SIM_FAST5_AUTO_QUEUE_ENABLED=false
LIVE_SIM_FAST5_BOOTSTRAP_STATUS=BLOCKED
LIVE_SIM_FAST5_MAX_BUY_COMMANDS_PER_CYCLE=1
LIVE_SIM_FAST5_MAX_DAILY_BUY_COUNT=2
LIVE_SIM_FAST5_MAX_ORDER_NOTIONAL=100000
LIVE_SIM_FAST5_PIPELINE_MAX_AGE_SEC=60
```

실제 queue에는 `LIVE_SIM_PILOT_AUTO_QUEUE_COMMAND=true`와 운영 run persistence도 필요하다.
Core loop는 FAST-5가 꺼져 있으면 기존 loop를 `queue_commands=false` 관측 cycle로 강제한다.

## 개발 검증 범위

- 기본 설정과 gate non-PASS에서 command/run/latch `0`
- 당일 payload SHA 불일치와 날짜 불일치 차단
- bootstrap에서 Alpha·Shadow advisory, 일 BUY 1건 강제
- approval SHA 선예약과 중복 예약 차단
- 소비된 bootstrap은 신규 BUY 0, 보호 lifecycle만 허용
- 정규 자동화에서 bootstrap/Alpha/Shadow evidence SHA 필수
- REAL 허용 `false`, broker path `LIVE_SIM_ONLY`

개발 검증은 fixture DB에서만 수행한다. 운영 DB, Core/Gateway, 모의·실제 브로커 요청 및 주문은
별도 운영 승인 범위다.
