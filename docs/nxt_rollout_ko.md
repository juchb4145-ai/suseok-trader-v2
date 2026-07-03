# NXT Rollout

이 문서는 NXT(넥스트레이드) 활용 범위를 PR1부터 PR4까지 정리한다. 현재 기본값은 관찰 중심이며, PR4의 주문 라우팅 준비도 LIVE_SIM 모의투자 전용 gate 아래에서만 동작한다.

## PR1: Exchange-Aware Market Data

- Gateway/Core payload에 `exchange=KRX|NXT`를 분리해 저장한다.
- Kiwoom realtime 등록 코드는 KRX 기본, NXT `_NX`, 통합/SOR `_AL` suffix 계약을 사용한다.
- API 조회는 `exchange=KRX|NXT|ALL` 필터를 제공한다.
- 기존 exchange metadata가 없는 tick은 KRX로 귀속해 이전 관찰 파이프라인과 호환한다.

## PR2: NXT Observation Context

- NXT premarket tick은 opt-in snapshot으로 저장한다.
- KRX/NXT regular session 1분 단위 cross-exchange observation을 만든다.
- 이 데이터는 Theme, Candidate, EntryTiming의 관찰 컨텍스트일 뿐 주문 입력이 아니다.
- `MATCHED_OBSERVATION`은 매수 신호가 아니고 `OBSERVE_PASS`는 주문 승인이 아니다.

## PR3: Exchange-Aware Observe Pipeline

- NXT 관찰값을 dashboard/API/runbook에서 확인할 수 있게 한다.
- 관찰 결과와 readiness는 주문 가능 상태를 뜻하지 않는다.
- NXT aftermarket smoke는 `--realtime-exchange nxt`로 tick 수신과 projection만 검증한다.

## PR4: LIVE_SIM Routing Prep

- `LIVE_SIM_ORDER_EXCHANGE`를 추가한다. 기본값은 `KRX`라 기존 LIVE_SIM 주문 payload는 그대로 KRX로 나간다.
- 허용값은 `KRX`, `NXT`, `SOR`다. Kiwoom 주문 코드 변환은 `NXT -> _NX`, `SOR -> _AL`을 사용한다.
- `LIVE_SIM_NXT_SUPPORT_CONFIRMED=false`이면 `NXT` 또는 `SOR`는 preflight의 `nxt_order_support_verified`에서 `BLOCK`된다.
- safety gate도 `NXT_ORDER_SUPPORT_UNCONFIRMED`로 command 생성을 막는다.
- Kiwoom `GetServerGubun` simulation server 확인은 그대로 유지한다. REAL server면 order_exchange 값과 무관하게 Gateway adapter가 거부한다.
- session guard도 그대로다. `order_exchange=NXT`여도 `LIVE_SIM_ENTRY_WINDOW_START`/`LIVE_SIM_ENTRY_WINDOW_END` 밖에서는 entry가 막힌다.
- NXT 전용 hoga code는 구현하지 않는다. 기존 `LIVE_SIM_DEFAULT_HOGA` 경로를 사용한다.

## Kiwoom Mock NXT Support Confirmation

`LIVE_SIM_NXT_SUPPORT_CONFIRMED=true`를 켜기 전에 다음을 운영자가 수동 확인한다.

1. Mock Gateway 또는 Kiwoom 모의투자 환경에서 `order_exchange=NXT` command가 `005930_NX` 주문 코드로 변환되는지 확인한다.
2. `order_exchange=SOR` command가 `005930_AL` 주문 코드로 변환되는지 확인한다.
3. 같은 command가 `LIVE_SIM_NXT_SUPPORT_CONFIRMED=false`에서는 preflight `BLOCK`과 `NXT_ORDER_SUPPORT_UNCONFIRMED`로 차단되는지 확인한다.
4. Kiwoom `GetServerGubun`이 REAL인 경우 `order_exchange=NXT`여도 `send_order requires simulation server`로 거부되는지 확인한다.
5. entry window 밖에서는 NXT/SOR 설정과 무관하게 `ENTRY_WINDOW_CLOSED`가 유지되는지 확인한다.

확인 전 rollback 기본값:

```powershell
$env:LIVE_SIM_ORDER_EXCHANGE = "KRX"
$env:LIVE_SIM_NXT_SUPPORT_CONFIRMED = "false"
```

NXT 관찰 확대와 LIVE_SIM 라우팅 준비는 별개의 단계다. 관찰 데이터, `MATCHED_OBSERVATION`, `OBSERVE_PASS`를 LIVE_SIM 주문 승인으로 해석하지 않는다.
