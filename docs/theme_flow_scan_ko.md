# 스캔 기반 테마 자금흐름 운영 메모

PR-2 범위는 테마 판정을 실시간 구독 한도에서 분리한다.

- 관측 소스 우선순위는 `REALTIME_TICK > MARKET_SCAN`이다.
- `MARKET_SCAN_ENABLED=true`일 때 DATA_WAIT 판정은 `scan_coverage_ratio`를 사용한다.
- `realtime_coverage_ratio`는 진입 관측용으로 남긴다.
- 멤버 row에는 `observation_source`가 저장된다.

신규 flow 지표는 다음 컬럼에 저장된다.

- `flow_trade_value_delta`: 직전 스캔 대비 테마 합산 거래대금 증가분
- `flow_rank_inflow_count`: 스캔 top_n 안으로 신규 진입한 멤버 수
- `flow_score`: 멤버별 증가분 log 정규화, inflow breadth, 상승 비율, 집중도 패널티를 합성한 점수

대시보드 top theme 정렬은 `flow_score`를 우선하고, 기존 `total_trade_value`는 tie-breaker로 유지한다.
