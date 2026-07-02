# Safety Principles KO

## 요약

이 문서는 `suseok-trader-v2`의 안전 원칙을 한곳에 모은다. 핵심은 “관찰과 검토는 허용하지만, 실행은 통제된 경로 외 금지”다. 이 문서의 기능은 실계좌 주문을 의미하지 않는다.

## 핵심 원칙

| 원칙 | 설명 |
| --- | --- |
| 기본은 `OBSERVE` | 시스템 기본값은 관찰이다. |
| Dashboard는 read-only | Dashboard는 조회 화면이며 실행 버튼이 없다. |
| AI Sidecar는 review-only | AI/RCA/Codex 결과는 사람이 읽는 자료다. |
| Candidate는 관찰 에피소드 | 매수 후보 확정이 아니다. |
| Strategy는 observation | `MATCHED_OBSERVATION`은 매수 신호가 아니다. |
| Risk는 observation | `OBSERVE_PASS`는 주문 승인이 아니다. |
| DRY_RUN은 내부 모의 회계 | 브로커 주문이 아니다. |
| LIVE_SIM은 모의투자 전용 | 실계좌 주문이 아니다. |
| LIVE_REAL은 future safety project | 현재 구현되어 있지 않으며 금지다. |

## TradingProfile Capability

`TRADING_PROFILE`은 각 profile이 가질 수 있는 capability의 상한이다. 개별 env flag는
운영 switch로 남아 있지만, profile이 허용하지 않는 capability는 사용할 수 없다.

| Profile | observation | dry-run shadow | LIVE_SIM intent | LIVE_SIM command | LIVE_REAL |
| --- | --- | --- | --- | --- | --- |
| `OBSERVE` | 허용 | 금지 | 금지 | 금지 | 금지 |
| `LIVE_SIM_PILOT` | 허용 | 허용 | 허용 | 모의투자 전용 허용 | 금지 |

코드에서는 `settings.trading_capabilities`를 단일 capability matrix로 읽는다.

도메인 모델의 `observe_only` 필드는 기본값이 `true`지만, 모델 생성자가 입력값을
무조건 `true`로 덮어쓰지 않는다. 주문 가능성은 `MATCHED_OBSERVATION`,
`OBSERVE_PASS`, `observe_only=false` 같은 단일 필드가 아니라 profile capability와
admission 결과가 함께 결정한다.

## 주문 관련 금지 원칙

- `GatewayCommand`는 safety gate 없이는 생성 금지다.
- `send_order`, `cancel_order`, `modify_order`는 통제된 경로 외 금지다.
- `POST /api/orders/enqueue` 같은 generic order endpoint는 존재하지 않는다.
- LIVE_SIM `cancel_order`는 미체결 BUY TTL 취소 전용이며, `modify_order`는 disabled다.
- LIVE_SIM `send_order`는 BUY entry 또는 open position close-only SELL exit 경로에서만 가능하다.
- LIVE_SIM command는 `live_sim_only=true`, `live_real_allowed=false` metadata를 유지해야 한다.
- DRY_RUN은 `GatewayCommand`를 만들지 않는다.

## AI/RCA/Codex 안전 원칙

- AI/RCA/Codex output은 Strategy/Risk/OMS 자동 입력이 아니다.
- OpenAI tools/function calling은 disabled다.
- order tools는 disabled다.
- AI failure는 trading state를 바꾸지 않는다.
- Codex Prompt Draft는 사람이 복사하는 초안이다.
- Codex Prompt Draft는 branch/commit/push/PR을 자동 생성하지 않는다.

## 상태값 안전 해석

| 값 | 안전한 해석 |
| --- | --- |
| `CONTEXT_READY` | Strategy 평가 입력 준비 |
| `MATCHED_OBSERVATION` | setup observation rule match |
| `OBSERVE_PASS` | risk block/caution이 관측되지 않음 |
| `STOP_LOSS` | DRY_RUN simulated exit signal |
| `TAKE_PROFIT` | DRY_RUN simulated exit signal |
| `LIVE_SIM_ENABLED=true` | 모의투자 기능 enable 조건 중 하나 |

각 값은 단독으로 주문 실행을 의미하지 않는다.

## Kill Switch

LIVE_SIM rollback의 첫 단계는 kill switch다.

```powershell
$env:LIVE_SIM_KILL_SWITCH = "true"
$env:LIVE_SIM_ORDER_ROUTING_ENABLED = "false"
$env:LIVE_SIM_GATEWAY_COMMAND_ENABLED = "false"
$env:LIVE_SIM_ENABLED = "false"
$env:TRADING_ALLOW_LIVE_SIM = "false"
```

그 다음 `/api/live-sim/status`에서 safety gate가 blocked인지 확인한다.

## 운영자 체크포인트

- 안전 문구가 문서에서 반복되는 것은 의도다.
- read-only/review-only surface와 manual POST endpoint를 구분한다.
- manual POST endpoint도 주문 endpoint인지 projection/report endpoint인지 확인한다.
- LIVE_REAL은 현재 구현되어 있지 않으며 별도 future safety project다.
