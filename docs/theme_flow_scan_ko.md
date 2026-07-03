# 스캔 기반 테마 자금흐름 운영 메모

PR-2 범위는 테마 판정을 실시간 구독 한도에서 분리한다.

- 관측 소스 우선순위는 `REALTIME_TICK > MARKET_SCAN`이다.
- `MARKET_SCAN_ENABLED=true`일 때 DATA_WAIT 판정은 `scan_coverage_ratio`를 사용한다.
- Theme Service의 레거시 스냅샷은 전체 active member 기준 커버리지 진단을 유지한다.
- Theme Leadership의 market-scan flow ranking은 관측된 멤버를 observable universe로 보고
  `fresh_member_count / observed_member_count` 기준으로 리더십 상태를 재판정한다.
  따라서 대형 테마가 레거시 스냅샷에서는 `DATA_WAIT`이어도, 스캔 관측 멤버가 충분하면
  `SPREADING`/`LEADING` watchset 후보가 될 수 있다.
  이때 전체 스캔 커버리지 부족은 `LOW_FULL_SCAN_COVERAGE` reason으로 보존된다.
- `realtime_coverage_ratio`는 진입 관측용으로 남긴다.
- 멤버 row에는 `observation_source`가 저장된다.

신규 flow 지표는 다음 컬럼에 저장된다.

- `flow_trade_value_delta`: 직전 스캔 대비 테마 합산 거래대금 증가분
- `flow_rank_inflow_count`: 스캔 top_n 안으로 신규 진입한 멤버 수
- `flow_score`: 멤버별 증가분 log 정규화, inflow breadth, 상승 비율, 집중도 패널티를 합성한 점수

대시보드 top theme 정렬은 `flow_score`를 우선하고, 기존 `total_trade_value`는 tie-breaker로 유지한다.
