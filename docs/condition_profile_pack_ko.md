# Market Open Condition Profile Pack

`configs/condition_profiles/market_open_profiles.json`은 장중 OBSERVE 운영용 예시 pack입니다.
Kiwoom 조건식 이름은 사용자 환경마다 다르므로 `condition_name`의 `REPLACE_WITH_*` 값을 실제 HTS 조건검색식 이름으로 바꿔서 사용합니다.

검증:

```powershell
python -m tools.validate_condition_profiles --file configs/condition_profiles/market_open_profiles.json
```

실행 helper:

```powershell
.\tools\start_market_open_observe.ps1 `
  -ConditionProfilesFile configs\condition_profiles\market_open_profiles.json
```

주의:

- 조건식 hit는 매수 신호가 아니며 sensor evidence입니다.
- `RISK_BLOCK` profile은 후보 관찰과 주문계획 승격을 차단하는 evidence입니다.
- `priority`와 Condition Fusion priority는 Strategy/EntryTiming 평가 순서와 관찰 점수에만 사용됩니다.
- Dashboard와 observe cycle은 read-only이며 주문 command를 만들지 않습니다.
