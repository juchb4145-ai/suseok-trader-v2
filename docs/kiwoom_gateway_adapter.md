# Kiwoom Gateway Real Adapter

## 목적

PR13은 `suseok_ai`의 실제 Kiwoom OpenAPI+ Gateway 자산을 `suseok-trader-v2`의 broker-neutral Gateway 계약으로 이식한다. Core는 계속 64-bit로 실행할 수 있고, PyQt5/QAxWidget/Kiwoom ActiveX는 32-bit Gateway process 내부에서만 사용한다.

## 가져온 자산

- `QAxWidget("KHOPENAPI.KHOpenAPICtrl.1")` 기반 Kiwoom client wrapper
- `CommConnect`, `GetLoginInfo`, `GetCodeListByMarket`
- `SetRealReg`, `SetRealRemove`
- `GetConditionLoad`, `GetConditionNameList`, `SendCondition`, `SendConditionStop`
- `SetInputValue`, `CommRqData`, `GetRepeatCnt`, `GetCommData`
- `SendOrder` low-level wrapper
- FID 기반 `price_tick` parser
- 조건검색 TR/real event wiring
- Chejan raw capture와 최소 order/fill parser

## 가져오지 않은 것

- legacy strategy/runtime/dashboard
- theme/candidate hydration parser
- broker reconcile 대형 TR runtime
- background auto order worker
- generic order enqueue API
- generic `cancel_order` / `modify_order` 실행 경로. `cancel_order`는 LIVE_SIM 미체결 BUY TTL 취소 전용으로만 처리한다.
- LIVE_REAL 주문 경로

## Gateway/Core Event Mapping

| Kiwoom source | Gateway event_type | Core 처리 |
| --- | --- | --- |
| heartbeat timer | `heartbeat` | `gateway_status`와 LIVE_SIM safety gate 입력 |
| login callback | `login_status`, `orderability` | 상태 저장 |
| condition load callback | `condition_load_result`, `condition_loaded` | 상태 저장 |
| `OnReceiveTrCondition` | `condition_event` action `ENTER` | market condition projection |
| `OnReceiveRealCondition` `I/D` | `condition_event` action `ENTER/EXIT` | market condition projection |
| `OnReceiveRealData` | `price_tick` | latest tick, bars, readiness |
| `request_tr` command | `tr_response` | TR snapshot projection |
| `SendOrder` result | `command_ack` or `command_failed` | LIVE_SIM order state |
| `OnReceiveChejanData` fill | `execution_event` | LIVE_SIM execution state |

## Price Tick Mapping

필수 FID:

| FID | 의미 | v2 field |
| --- | --- | --- |
| 10 | 현재가 | `price` |
| 12 | 등락률 | `change_rate` |
| 13 | 누적거래량 | `volume` |
| 14 | 누적거래대금 | `trade_value` |
| 16 | 시가 | `metadata.open_price` |
| 17 | 고가 | `day_high` |
| 18 | 저가 | `day_low` |
| 20 | 체결시간 | `trade_time`, `metadata.trade_time` |
| 27 | 최우선매도호가 | `best_ask` |
| 28 | 최우선매수호가 | `best_bid` |
| 228 | 체결강도 | `execution_strength` |

FID 14는 Kiwoom에서 백만원 단위로 들어오는 것으로 처리하고 `metadata.trade_value_unit=million_krw`로 기록한다. 값이 없으면 `price * volume`으로 추정하고 `TRADE_VALUE_MISSING`, `TURNOVER_ESTIMATED` reason code를 남긴다.

보존하는 주요 reason code:

- `TRADE_VALUE_MISSING`
- `TURNOVER_ESTIMATED`
- `EXECUTION_STRENGTH_MISSING`
- `DAY_HIGH_LOW_MISSING`
- `BEST_BID_ASK_MISSING`
- `REAL_PARSE_FALLBACK`

## Condition Mapping

- Kiwoom code `A005930`은 `005930`으로 normalize한다.
- Kiwoom event type `I`, `include`, `ENTER`는 `ENTER`로 변환한다.
- Kiwoom event type `D`, `remove`, `EXIT`는 `EXIT`로 변환한다.
- `condition_index`는 `payload.metadata.condition_index`에 보존한다.
- v2 `condition_id`는 `kiwoom_condition_{index}` 형식으로 만든다.
- condition hit code는 `--condition-realtime`이 켜져 있으면 즉시 실시간 등록한다.

## TR Mapping

Core command:

```json
{
  "command_type": "request_tr",
  "payload": {
    "request_id": "tr1",
    "tr_code": "OPT10001",
    "request_name": "stock_basic",
    "params": {"종목코드": "005930"},
    "fields": ["종목코드", "종목명", "현재가"]
  }
}
```

Gateway 처리:

1. `SetInputValue`를 params만큼 호출
2. `CommRqData`
3. `OnReceiveTrData`
4. `GetRepeatCnt`, `GetCommData`로 rows 구성
5. `tr_response` event와 `command_ack` 전송

초기 안정 범위는 `opt10001` 같은 기본 TR이다. 대형 multi-page TR은 후속 PR에서 별도 rate-limit와 parser spec을 붙인다.

## LIVE_SIM Order Guard

Real Kiwoom Gateway의 `send_order`와 `cancel_order`는 다음 조건을 모두 만족해야 `SendOrder`를 호출한다.

- `command_type=send_order` 또는 PR-5 미체결 BUY TTL 전용 `cancel_order`
- `source=live_sim`
- command-level `idempotency_key`
- payload `mode=LIVE_SIM`, `live_mode=LIVE_SIM`
- payload `idempotency_key`와 command idempotency 일치
- `metadata.source=live_sim`
- `metadata.live_sim_only=true`
- `metadata.live_real_allowed=false`
- `metadata.live_sim_intent_id`
- `account_mode`, `broker_env`, `server_mode`가 simulation-like
- Kiwoom `GetServerGubun`이 simulation-like
- BUY entry `send_order`는 `side=BUY`
- SELL exit `send_order`는 open position close-only metadata(`position_id`, `exit_intent_id`, short 금지)
- `cancel_order`는 미체결 BUY 원주문 metadata(`cancel_intent_id`, `original_live_sim_order_id`, `original_order_no`)

`LIVE_REAL`, 실서버, metadata 누락, idempotency 누락, 신규 SELL/short, generic cancel, modify는 모두 reject된다.

## 실행

Core:

```powershell
python -m uvicorn apps.core_api:app --host 127.0.0.1 --port 8000
```

32-bit Kiwoom Gateway:

```powershell
python -m apps.kiwoom_gateway `
  --core-url http://127.0.0.1:8000 `
  --token $env:TRADING_CORE_TOKEN `
  --auto-login `
  --threaded-login `
  --poll-wait-sec 1 `
  --heartbeat-interval-sec 2 `
  --condition-name "YOUR_CONDITION_NAME" `
  --condition-realtime
```

필수 환경:

- 32-bit Python
- Kiwoom OpenAPI+ 설치 및 로그인 가능 상태
- 32-bit PyQt5
- Core와 동일한 local token

## Troubleshooting

| 증상 | 확인 |
| --- | --- |
| `Current Python is 64-bit` | 32-bit Python 환경으로 Gateway 실행 |
| `PyQt5 is required` | 32-bit Python에 PyQt5 설치 |
| ActiveX control not registered | Kiwoom OpenAPI+ 설치/등록 상태 확인 |
| heartbeat 없음 | Core URL/token, 방화벽, `/api/gateway/events/recent` 확인 |
| condition 없음 | 조건식 이름/index, Kiwoom 조건식 로드 성공 여부 확인 |
| tick 없음 | condition hit 또는 `--realtime-codes` 등록 여부 확인 |
| LIVE_SIM command rejected | `/api/gateway/events/recent`의 `command_failed.error_message` 확인 |
| REAL server detected | 모의투자 로그인인지 확인. REAL이면 주문은 의도적으로 거부됨 |
