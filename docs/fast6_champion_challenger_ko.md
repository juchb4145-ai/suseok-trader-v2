# FAST-6 Champion / Challenger 실행 계약

## 목적과 현재 상태

FAST-6은 검증된 Champion 1개와 Challenger 1~2개를 동일한 Replay·Profit Lab·Parallel
Shadow 근거로 비교하는 오프라인 운영 체계다. 코드는 구현됐지만 FAST-5 운영 증거가 아직
충족되지 않았으므로 상태는 `BLOCKED_BY_FAST_5`다.

이 단계의 출력은 사람 검토용 승급 후보일 뿐이다. 전략 설정을 바꾸거나 LIVE_SIM을
활성화하지 않으며 주문, 브로커 호출, 운영 DB 접근 경로를 제공하지 않는다.

## 실험 단위

각 manifest에는 다음을 반드시 기록한다.

- experiment ID
- Champion 1개와 Challenger 1~2개
- 모든 candidate에 공통인 Git commit SHA
- strategy/risk/entry config SHA-256
- 데이터 시작일·종료일과 날짜별 TRAIN/VALIDATION/TEST split SHA-256
- execution model version과 cost model version
- FAST-5 상태와 PASS evidence SHA-256
- qualification verdict

각 candidate는 Profit Lab과 Parallel Shadow `raw.json`의 상대 경로와 파일 전체
SHA-256을 지정한다. 도구는 파일을 읽기 전에 SHA를 대조하고, 보고서 내부의 candidate
commit, execution model, cost model, 데이터 구간과 split도 manifest와 대조한다. 통제되지 않은
코드 차이를 숨길 수 없도록 모든 candidate는 동일한 Git commit을 사용해야 한다.

날짜 split SHA-256은 정렬된 다음 구조에 `canonical_sha256`을 적용한 값이다.

```json
[
  {"trade_date": "2026-07-01", "dataset_split": "TRAIN"},
  {"trade_date": "2026-07-02", "dataset_split": "VALIDATION"},
  {"trade_date": "2026-07-03", "dataset_split": "TEST"}
]
```

## 단일축 실험 계약

허용 축은 다음 다섯 개뿐이다.

```text
ENTRY
STOP
TAKE_PROFIT
THEME_THRESHOLD
MARKET_REGIME
```

Challenger는 Champion과 `changed_axis` 하나에서만 달라야 한다. 각 축의 config SHA뿐 아니라
연관된 상위 config SHA도 함께 바뀌어야 한다.

| 변경 축 | 변경 가능한 상위 config | 그대로여야 하는 config |
| --- | --- | --- |
| ENTRY | entry | strategy, risk |
| STOP / TAKE_PROFIT | risk | strategy, entry |
| THEME_THRESHOLD / MARKET_REGIME | strategy | risk, entry |

두 축 이상이 바뀌거나 무관한 상위 config hash까지 바뀌면 입력 단계에서 거부한다.
각 candidate의 Profit Lab과 Parallel Shadow는 완전히 같은 평가 config를 사용해야 한다.
Candidate 간 Profit Lab config도 동일해야 하며, `STOP`은 stop 관련 필드,
`TAKE_PROFIT`은 take-profit 필드 차이만 예외로 허용한다. 따라서 sample·qualification·비용
임계값을 Challenger에 유리하게 바꿔 비교하는 것은 불가능하다.

## 비교와 판정

기본 승급 검토 기준은 다음과 같다.

- Champion과 Challenger 모두 Profit Lab `ALPHA_QUALIFIED`
- 동일한 확정 비용·execution model
- point-in-time violation과 주문 부작용 0
- Shadow duplicate, coverage gap, comparison linkage gap, AI 영향 0
- validation과 test net expectancy가 각각 Champion 대비 5% 이상 개선
- profit factor가 Champion 이상
- max drawdown R 증가 0 이하

비교 기준은 manifest의 policy에 명시되며 실행 중 자동 완화하지 않는다. Challenger가 여러
개면 validation/test 중 더 낮은 개선율이 가장 높은 하나만 검토 후보로 표시한다.

결과 상태는 다음과 같다.

```text
REVIEW_READY
RETAIN_CHAMPION
BLOCKED_BY_FAST_5
BLOCKED
```

`BLOCKED_BY_FAST_5`에서도 오프라인 비교값은 보존하지만 실제 승급은 불가능하다. Champion의
Shadow 보고서에 LIVE_SIM canary 비교 근거가 없거나 FAST-5가 PASS가 아니면 승급 검토를
차단한다. Challenger의 Shadow는 LIVE_SIM 비교가 없어도 되므로
`NO_LIVE_CANARY_COMPARISON` 경고를 허용한다.

## 실행

```powershell
python -B -m tools.ops_champion_challenger `
  --manifest <champion-challenger-experiment.json> `
  --out-dir reports/champion_challenger
```

출력은 실행별 `raw.json`과 `summary.md`다. 도구 인수에는 DB 경로, API URL, 계좌, 주문,
승격 적용 옵션이 없다. manifest의 `promotion_mode`는 `REVIEW_ONLY`만 허용하고 결과에는 항상
다음이 기록된다.

```text
promotion_applied=false
live_sim_activation_changed=false
operational_db_write_count=0
gateway_command_write_count=0
broker_call_count=0
```

## 다음 운영 조건

코드 병합은 FAST-6 운영 승급을 뜻하지 않는다. 실제 Champion 교체를 검토하려면 먼저
FAST-5의 C1, Alpha, Shadow, broker reconcile과 자동 rollback 증거가 모두 PASS여야 한다.
그 뒤에도 이 보고서는 검토 자료일 뿐이며 설정 변경과 LIVE_SIM 활성화는 별도 승인된 운영
절차에서만 수행한다.
