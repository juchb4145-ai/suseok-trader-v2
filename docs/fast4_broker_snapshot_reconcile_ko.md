# FAST-4 Kiwoom Simulation Broker Snapshot Reconcile

## 목적과 안전 경계

FAST-4는 키움 모의서버 계좌의 미체결 주문, 당일 체결, 보유 종목을 읽기 전용 TR로
조회하고 Core의 LIVE_SIM 주문·체결·포지션과 비교한다. 이 기능은 local state를 자동
수정하지 않으며, 불일치·불완전·오래된 스냅샷이 있으면 신규 BUY만 차단한다.
미체결 BUY cancel과 보유 포지션 close-only SELL 경로는 기존 안전 계약을 유지한다.

다음 작업은 이 기능에 포함되지 않는다.

- LIVE_SIM 또는 LIVE_REAL 활성화
- Core/Gateway 프로세스 시작
- 실제 또는 모의 주문·정정·취소
- 운영 DB migration
- broker 결과로 local 주문·체결·포지션을 자동 수정하는 작업

기본값 `LIVE_SIM_RECONCILE_REQUEST_BROKER_SNAPSHOT_ENABLED=false`이므로 코드 배포만으로
계좌 조회가 발생하지 않는다.

## Gateway 요청 계약

Core는 `POST /api/live-sim/reconcile/broker-snapshot/request`에서
`command_type=broker_snapshot_request`를 한 건 queue한다. 요청은 다음 조건을 모두
만족해야 한다.

- `source=live_sim` 및 command-level idempotency key
- `account_mode/broker_env/server_mode`가 모두 simulation-like
- `live_sim_only=true`, `live_real_allowed=false`
- `broker_order_path=LIVE_SIM_ONLY`
- `read_only=true`, `automatic_local_repair=false`
- Gateway의 실제 `GetServerGubun()` 결과가 `SIMULATION`

실서버 또는 `UNKNOWN`이면 첫 TR 전에 `command_failed`로 종료한다. 같은 시점에 이미
활성 상태인 snapshot command가 있으면 새 요청도 queue하지 않는다. 이미 사용한
`snapshot_id`의 재사용도 command 상태와 무관하게 거부한다.

Gateway는 다음 순서로 조회한다.

| 순서 | 섹션 | TR | 주요 결과 |
| --- | --- | --- | --- |
| 1 | `OPEN_ORDERS` | `OPT10075` | 주문번호, 종목, 주문/체결/미체결 수량, 상태 |
| 2 | `EXECUTIONS` | `OPT10076` | 주문번호, 체결번호, 종목, 체결 수량·가격 |
| 3 | `POSITIONS` | `OPW00018` | 종목, 보유/매매가능 수량, 평균 매입가 |

`prev_next=2`이면 같은 섹션의 다음 페이지를 요청한다. 각 페이지는 `snapshot_id`,
section, TR, request ID, page number, 이전/다음 continuation, row count, 수신 시각을
`page_lineage`에 남긴다. 섹션당 최대 페이지 수를 넘기면 `INCOMPLETE`로 닫아 무한
연속조회를 막는다.

## 조립 상태와 이벤트

Gateway는 기존 `account_snapshot` event type을 사용하며 상태는 다음과 같다.

```text
REQUESTED → COLLECTING → COMPLETE
                      ↘ INCOMPLETE
                      ↘ FAILED
COMPLETE + freshness TTL 초과 → STALE
```

`REQUESTED`, `COLLECTING`, `INCOMPLETE`, `FAILED`, `STALE`는 모두 신규 BUY를 차단한다.
`COMPLETE`도 Core 대조에서 mismatch가 하나라도 발견되면 차단된다. terminal
`COMPLETE`만 command ACK가 되며, 그 외 terminal 상태는 command failure다.

계좌번호 원문은 Gateway TR 입력에만 사용한다. `account_snapshot`, reconcile 결과,
lifecycle evidence, API response에는 마지막 네 자리만 남긴 `***1234` 형식으로 저장한다.
비밀번호는 command payload에 넣지 않으며 Kiwoom의 저장된 계좌비밀번호 입력 경로를
사용한다.

## 비교 규칙

Core는 기존 `live_sim_reconcile_snapshots`를 재사용하고 새 schema를 만들지 않는다.

- broker-only/local-only open order
- 주문 종목, 원주문 수량, 미체결 수량, 상태 불일치
- broker-only/local-only position
- 보유 수량, 매매가능 수량, 평균 매입가 tolerance 초과
- broker 주문번호별 당일 체결 수량 합계 불일치
- account mask와 거래일 불일치
- snapshot status, required section completeness, freshness 불일치

비교 결과는 append-only reconcile snapshot과 lifecycle event로 남는다. local 주문,
체결, 포지션 row를 UPDATE/DELETE하는 repair 경로는 없다.

신규 BUY는 intent 평가뿐 아니라 `send_order` command를 실제 queue하기 직전에도 최신
reconcile과 진행 중 snapshot request를 다시 확인한다. 따라서 snapshot이 그 사이
오래되거나 새 mismatch가 생겨도 BUY command는 생성되지 않는다.

## 설정

```dotenv
LIVE_SIM_RECONCILE_REQUEST_BROKER_SNAPSHOT_ENABLED=false
LIVE_SIM_BROKER_SNAPSHOT_STALE_SEC=120
LIVE_SIM_BROKER_SNAPSHOT_REQUEST_TTL_SEC=180
LIVE_SIM_BROKER_SNAPSHOT_MAX_PAGES_PER_SECTION=20
```

기능 플래그 활성화와 실제 조회 호출은 별도 운영 승인 대상이다. 활성화된 상태에서
fresh `COMPLETE` snapshot이 없으면 BUY preflight는 fail-closed한다. OBSERVE/보호 경로는
경고와 증거를 남기되 주문 조회 자체가 주문 호출로 바뀌지는 않는다.

## 개발 검증

`tests/test_fast4_broker_snapshot.py`는 다음 계약을 fixture로 검증한다.

- continuation page lineage와 세 섹션 조립
- account ID 원문 비노출
- REAL server에서 TR request count 0
- 활성 snapshot request 중복 queue 0
- fresh `COMPLETE` exact match PASS
- stale, execution sum, average-price mismatch 차단
- mismatch fixture에서 신규 `send_order` command 0
