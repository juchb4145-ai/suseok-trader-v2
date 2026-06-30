# 장초반 조건검색 Profile 운영 가이드

조건검색은 매수 신호가 아니라 시장 센서입니다. 조건식 hit는 관찰 evidence로만 쓰고, 주문 command나 주문 intent를 만들지 않습니다.

## 조건식 Pack

`configs/condition_profiles/market_open_profiles.json`은 다음 조건식명을 전제로 합니다.

- `주도테마_넓은후보`: DISCOVERY. 테마 후보군을 넓게 감지하되 단독 매수 후보로 쓰지 않습니다.
- `주도테마_거래대금주도`: LEADER. 거래대금과 체결 강도가 붙는 주도 후보를 감지합니다.
- `주도테마_첫눌림`: PULLBACK. 주도주가 첫 눌림 뒤 재수급을 받는 구간을 감지합니다.
- `주도테마_재돌파`: BREAKOUT. 전고점, 당일 고점, VWAP 등 주요 구간 재돌파 후보를 감지합니다.
- `리스크_과열위험`: RISK_BLOCK. 과열, 급등 피로, 변동성 확대를 차단 evidence로 남깁니다.
- `주도테마_테마확산`: THEME_SPREAD. 후순위 확산 후보 감지용이며 기본값은 disabled입니다.

## 운영 원칙

positive profile은 기본 `price_subscribe_policy=batch`를 씁니다. 초기 `tr_condition` 결과는 한 번에 모아 SetRealReg batch로 등록하고, 장중 `real_condition` 신규 편입은 즉시 등록합니다. 이렇게 해야 장초반 조건검색 결과가 종목별 SetRealReg 호출로 흩어지는 일을 피할 수 있습니다.

`RISK_BLOCK`은 가격 실시간을 등록하지 않습니다. 조건 이벤트와 fusion risk evidence만 남기며, 후보 승격이나 주문 쪽 side effect를 만들지 않습니다.

DISCOVERY 단독 hit는 매수 후보가 아닙니다. 우선 평가 후보는 `LEADER + PULLBACK`, `LEADER + BREAKOUT` 조합을 중심으로 봅니다.

`immediate` 정책은 수동/특수 profile에서만 제한적으로 씁니다. enabled profile에서 `immediate`와 `max_initial > 0`을 같이 쓰면 초기 TR 결과가 종목별 즉시 등록으로 번질 수 있어 validation warning을 냅니다.

## 실행 예

```powershell
.\tools\start_market_open_observe.ps1 -ConditionProfilesFile configs\condition_profiles\market_open_profiles.json
```

검증:

```powershell
python -m tools.validate_condition_profiles --file configs/condition_profiles/market_open_profiles.json
```
