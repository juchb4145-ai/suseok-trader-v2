# FAST-2B Conservative Profit Lab 실행 계약

## 목적

FAST-2A point-in-time replay 번들의 가격 tick과 그 번들에 결속된 BUY LIMIT signal을
보수적 체결·청산·비용 모델로 평가한다. 이 도구는 파일만 읽고 보고서 파일만 생성한다.
운영 DB, Gateway command, DRY_RUN/LIVE_SIM 상태, 브로커 API에는 접근하지 않는다.

## 입력 결속

필수 입력은 다음 두 가지다.

- 검증된 FAST-2A replay bundle 디렉터리
- 같은 bundle의 `source_record_sha256`와 `source_event_order_sha256`를 가진 FAST-2A 보고서

signal manifest를 지정하면 동일한 두 source hash를 포함해야 한다. signal의 정규화된
내용도 `signals_sha256`으로 다시 계산한다. 어느 identity라도 다르면 실행을 중단한다.
signal manifest가 없으면 빈 signal 집합으로 실행하며 `NO_REPLAY_SIGNALS` 경고를 남긴다.

```json
{
  "format": "profit-lab-signals/v1",
  "source_record_sha256": "<FAST-2A bundle record SHA-256>",
  "source_event_order_sha256": "<FAST-2A bundle order SHA-256>",
  "signals": [
    {
      "signal_id": "signal-20260720-001",
      "trade_date": "2026-07-20",
      "code": "005930",
      "signal_at": "2026-07-20T00:10:00Z",
      "limit_price": 70000,
      "quantity": 1,
      "exchange": "KRX",
      "side": "BUY",
      "order_type": "LIMIT",
      "coherent": true,
      "setup_type": "BREAKOUT",
      "regime": "RISK_ON",
      "theme": "SEMICONDUCTOR",
      "source_lineage": {"source_run_id": "<replay run id>"}
    }
  ]
}
```

`trade_date`는 `signal_at`의 Asia/Seoul 날짜와 같아야 하며 signal ID는 중복될 수 없다.

## 보수적 체결 계약

BUY LIMIT은 signal과 같은 tick을 절대 사용하지 않는다. configured latency가 지난 미래
tick에서 `price <= limit`인 경우에만 limit price로 체결하고, TTL까지 조건을 만족하지
못하면 `ENTRY_NO_FILL`이다.

SELL LIMIT도 trigger와 같은 tick을 사용하지 않는다. latency 이후 미래 tick에서
`price >= limit`인 경우에만 limit price로 체결한다. STOP SELL은 trigger 뒤 첫 유효
tick의 실제 가격을 사용하므로 gap-down 손실을 stop 가격으로 축소하지 않는다.

slippage tick은 limit 가격 제약을 위반하는 가상 fill 가격으로 만들지 않고 별도 비용으로
차감한다. commission, sell tax, slippage를 합한 `total_cost`가 gross PnL에서 빠져 net
PnL과 R을 만든다. KRX 가격 구간을 넘나드는 tick-size도 반영한다.

## 공통 Exit policy

`domain.exit.policy`의 pure long-only policy를 Profit Lab, DRY_RUN, LIVE_SIM이 공유한다.
지원 trigger는 stop-loss, take-profit, trailing-stop, max-hold, EOD flatten이다.
minimum-hold, close-only, no-short 제약을 포함하며 동일 관측에서 여러 조건이 발생하면
고정된 보수적 우선순위를 사용한다. Profit Lab은 trigger 뒤 별도의 execution latency와
TTL을 적용한다.

## 비용 설정과 fail-closed 판정

기본 설정은 실제 수수료를 추측하지 않는다.

```json
{
  "cost_model_version": "UNCONFIRMED",
  "cost_model_confirmed": false,
  "buy_commission_rate": 0.0,
  "sell_commission_rate": 0.0,
  "sell_tax_rate": 0.0,
  "buy_slippage_ticks": 0,
  "sell_slippage_ticks": 0
}
```

운영자가 근거를 확인한 값과 version을 별도 config JSON에 넣어야 한다. version이
미확정이거나 비용 항목이 0이면 metric은 계산하되 `ALPHA_QUALIFIED`를 금지하고
`COST_MODEL_MISSING`으로 판정한다. threshold를 데이터 결과에 맞춰 자동 완화하지 않는다.

## 지표와 판정

보고서는 signal/eligible/fill/no-fill/fill-rate, gross/net PnL, expectancy/R, win rate,
profit factor, MAE/MFE, holding time, drawdown, 연속 손실을 기록한다. setup, regime, theme,
진입 KST 시간, 날짜 기반 TRAIN/VALIDATION/TEST별 지표와 baseline, +1 tick slippage,
2배 latency, 결합 stress 결과도 기록한다.

판정값은 `ALPHA_QUALIFIED`, `ALPHA_UNQUALIFIED`, `INSUFFICIENT_SAMPLE`,
`COST_MODEL_MISSING`, `DATA_QUALITY_BLOCKED` 중 하나다. FAST-2A가 qualified가 아니거나
point-in-time violation이 있으면 다른 수치가 좋아도 `DATA_QUALITY_BLOCKED`다.

## 실행

```powershell
python -B -m tools.ops_profit_lab `
  --bundle-dir <FAST-2A-bundle-directory> `
  --alpha-report <FAST-2A-raw.json> `
  --signals-file <profit-lab-signals.json> `
  --config-file <verified-cost-and-policy.json> `
  --out-dir reports/profit_lab
```

`--signals-file`과 `--config-file`은 생략 가능하지만, 기본 실행은 signal 없음과 비용 미확정을
명시하므로 승격 증거가 아니다. 출력은 실행별 `raw.json`과 `summary.md`이며 계좌번호,
토큰, 비밀번호를 입력 또는 출력 파일에 기록해서는 안 된다.
