# Strategy Audit - 2026-07-07

대상: `suseok-trader-v2` main  
범위: Strategy evaluator, EntryTiming/OrderPlan, LIVE_SIM exit 파라미터, 일일 손실 한도, 점수 체계, 전략 깔때기(funnel) 계측  
안전 원칙: 이번 점검은 전략 구조 진단만 다룬다. `LIVE_REAL` 활성화, 주문 정책 완화, 매수 기준 완화는 하지 않는다.  
관계 문서: 인프라/데이터 파이프라인 이슈는 `docs/structural_audit_2026-07-07.md`, 백테스트·supervision·exit SPOF는 `docs/redesign_roadmap_2026-07_ko.md`(§2.4, §2.6)가 이미 다루므로 중복하지 않는다.

## 잘 유지되고 있는 부분 (보존 대상)

- 모든 전략/리스크/진입 판정이 observe-only 기본값과 `live_real_allowed=false`를 각인하고, reason code 증적을 남긴다. 근거: `domain/strategy/evaluator.py`, `services/entry_timing/order_plan.py`.
- OrderPlan은 `idempotency_key` UNIQUE, `BUY_LIMIT_ONLY`, 시장가 금지(`entry_timing_allow_market_order=False` 강제)를 지킨다. 근거: `services/entry_timing/order_plan.py::calculate_limit_price()`, `storage/sqlite.py::order_plan_drafts`.
- LIVE_SIM 청산은 tick 데이터 유실 시 청산을 내지 않고 `EXIT_EVALUATION_DATA_WAIT`로 보수적으로 멈춘다. 데이터 장애가 강제 청산으로 전이되지 않는다. 근거: `services/live_sim/live_sim_service.py`.
- EOD flatten 활성(`LIVE_SIM_EXIT_EOD_FLATTEN_ENABLED=true`), 일일 주문 수(3)/명목(30만) 한도, 활성 주문·포지션 1개 제한이 걸려 있다. 근거: `.env`, `services/config.py`.
- MAE/MFE excursion이 포지션에 기록되고, pilot report가 setup별 win_rate/expectancy를 산출한다. 향후 파라미터 캘리브레이션의 데이터 기반이 이미 있다. 근거: `tools/build_live_sim_pilot_report.py`.
- MOMENTUM_CONTINUATION 루트의 사이즈 0.5×, TTL 0.5×는 실제로 적용된다. 근거: `services/entry_timing/order_plan.py::_route_risk_controls()`.

## P0

| ID | 증상 | 코드 근거 | 운영 영향 | 최소 수정 방향 | 테스트/SQL 검증 |
|---|---|---|---|---|---|
| P0-1 Flow 게이트 임계값 기본 0.0으로 사실상 항상 통과 → MATCHED_OBSERVATION 남발 | `domain/strategy/evaluator.py::_flow_observed()`가 `delta >= 임계값` 비교. `services/config.py`의 `strategy_min_trade_value_delta_1m/3m = 0.0`이 기본값이고 `.env`에 override 없음. `trade_value_delta`는 누적 거래대금 증분이라 존재하면 항상 ≥ 0이므로 거래 없는 분봉(delta=0)도 "수급 관찰됨" 판정. | MATCHED(0.86) vs FORMING(0.58) 구분이 무너진다. `live_sim_require_strategy_matched=True` 게이트가 의도보다 약해져, 수급 확인 없는 셋업이 주문 깔때기에 진입한다. | 최소 거래대금 증분을 0보다 크게 설정(절대 KRW 또는 종목별 분위수 기반)하고, 임계값 0일 때는 비교를 `>`로 바꾸거나 flow 게이트 비활성임을 reason code로 명시한다. 파라미터 변경은 관찰 지표 비교 후 적용한다. | 추가 테스트: `delta_1m=0.0`일 때 `flow_observed=False` 검증. SQL: `SELECT json_extract(evidence_json,'$.flow_observed'), COUNT(*) FROM strategy_setup_observations GROUP BY 1;` — 현재 flow_observed=1 편중 확인. |
| P0-2 진입 루트가 요구한 tight stop이 청산 엔진에서 미이행 | `services/entry_timing/order_plan.py::_route_risk_controls()`가 MOMENTUM_CONTINUATION에 `tight_stop_required: True`를 evidence로 기록하지만 소비처가 없다(테스트 assert 1건뿐). LIVE_SIM 청산은 모든 포지션에 전역 `live_sim_exit_stop_loss_pct=3.0` 일괄 적용. `live_sim_positions`에는 포지션별 stop/take 파라미터 컬럼이 없다(`trailing_stop_price` watermark만 존재). | 고위험 루트(고점 근처 추격 허용)에 절반 사이즈를 주고도 스탑은 일반 눌림목과 동일하다. 진입 시점의 리스크 통제 약속과 청산 시점의 실제 통제가 불일치 — 리스크 정합성 결함. | 진입 시 루트별 stop/take/trailing 파라미터를 `live_sim_positions`에 컬럼으로 저장(order plan evidence에서 전파)하고, 청산 엔진은 포지션별 값을 우선 사용, 전역값은 fallback으로 유지한다. | 추가 테스트: MOMENTUM_CONTINUATION 루트로 연 포지션이 더 타이트한 스탑으로 평가되는지 검증. SQL: `PRAGMA table_info(live_sim_positions);` — stop 파라미터 컬럼 부재 확인. `SELECT entry_timing_state, COUNT(*) FROM order_plan_drafts GROUP BY 1;` |
| P0-3 일일 손실 한도가 일일 투입 명목과 동일해 사실상 발동 불가능 | `.env`의 `LIVE_SIM_MAX_DAILY_LOSS=300000` = `live_sim_max_daily_notional=300000`. `services/live_sim/daily_loss_guard.py::build_live_sim_daily_loss_evidence()`는 `daily_loss >= effective_loss_limit`일 때만 차단. 주문당 10만 × 일 3건 × 스탑 3% 구조에서 현실적 일일 최대 손실은 슬리피지 포함 ~1.5만원 수준. | 한도가 -100% 손실에서만 발동하므로 갭 하락 외에는 장식이다. 연속 손실일에 대한 서킷브레이커 역할을 하지 못한다. | 손실 한도를 "건당 리스크(명목 × 스탑%) × N배"로 정의해 재설정(예: 3천원 × 3건 × 2~3배 ≈ 2~3만원). config validation에 `max_daily_loss >= max_daily_notional`일 때 경고(또는 거부)를 추가한다. | 추가 테스트: config validation — 손실 한도가 일일 명목 이상이면 경고. SQL: 발동 이력 부재 확인 — `SELECT COUNT(*) FROM live_sim_intents WHERE evidence_json LIKE '%DAILY_LOSS_LIMIT_EXCEEDED%';` |

## P1

| ID | 증상 | 코드 근거 | 운영 영향 | 최소 수정 방향 | 테스트/SQL 검증 |
|---|---|---|---|---|---|
| P1-1 동일 셋업이 두 엔진에 중복 정의되고 임계값이 모순 | 눌림목 범위: `strategy_pullback_min/max_pct` 0.3–5.0 vs `entry_timing_pullback_min/max_pct` 1.0–4.5. VWAP 허용폭: 1.0 vs 0.7. 고점 근접: `strategy_breakout_retest_near_high_pct=2.0`(매치) vs `entry_timing_chase_near_high_pct=0.7`(추격 차단). 근거: `domain/strategy/evaluator.py`, `services/entry_timing/engine.py`, `services/config.py`. | 실제 매매 가능 영역은 두 엔진의 교집합인데 이를 계산·검증하는 곳이 없다. 눌림 0.5%에서 Strategy는 MATCHED, EntryTiming은 CHASE_HIGH 차단 — "전략은 잡았는데 주문이 안 나가는" 깔때기 감쇠가 구조에 내장. 파라미터 튜닝 시 이름이 다른 두 세트를 함께 만져야 해 실수 유발. | 셋업 판정 임계값을 단일 도메인 모듈/네임스페이스로 통합하거나, 최소한 config validation에 "EntryTiming 허용 범위 ⊆ Strategy 매치 범위" 검증을 추가한다. 통합 전까지는 두 임계값 세트의 교집합을 문서/대시보드에 명시한다. | 추가 테스트: config validation — 두 엔진 envelope 교집합이 비퇴화인지 검증. SQL: `SELECT s.overall_status, e.status, COUNT(*) FROM strategy_observations_latest s JOIN entry_timing_evaluations e ON e.candidate_instance_id = s.candidate_instance_id GROUP BY 1,2;` — MATCHED인데 BLOCKED_CHASE인 조합 집계. |
| P1-2 단계별 깔때기 계측 부재 — "12:13 이후 주문 0건"의 원인 규명 불가 | `services/entry_timing/engine.py::_hard_context_state()`가 `theme_state ∉ {LEADING, SPREADING, LEADER_ONLY}`면 즉시 차단. 진입 윈도우는 09:05–14:30(`services/config.py`)이지만 오후 테마가 FADING/WATCH로 전환되면 윈도우 종료 전에 깔때기가 닫힌다. 시간대별 단계 통과율을 집계하는 곳이 없다. | 오후 주문 0건이 설계(테마 게이트)인지 결함(데이터 지연)인지 데이터로 판별할 수 없다. 모든 파라미터 튜닝이 근거 없이 진행된다. 로드맵 §2.3 "계측 없이 최적화 금지" 원칙의 전략 버전 위반. | 30분 버킷별 funnel 리포트 추가: 후보 → Strategy MATCHED → Risk OBSERVE_PASS → EntryTiming PLAN_READY → intent → order → fill 카운트와 차단 reason code 상위 N개. pilot report(`tools/build_live_sim_pilot_report.py`) 확장으로 구현. | SQL: `SELECT substr(evaluated_at,12,2) AS hh, status, COUNT(*) FROM entry_timing_evaluations GROUP BY 1,2 ORDER BY 1;` / `SELECT substr(created_at,12,2) AS hh, COUNT(*) FROM order_plan_drafts WHERE status='PLAN_READY' GROUP BY 1;` — 시간대별 감쇠 지점 확인. |

## P2

| ID | 증상 | 코드 근거 | 운영 영향 | 최소 수정 방향 | 테스트/SQL 검증 |
|---|---|---|---|---|---|
| P2-1 점수 체계가 캘리브레이션 없는 하드코딩 상수이고 스케일이 혼재 | setup별 score/confidence(0.86/0.84/0.80/0.78 등)가 `domain/strategy/evaluator.py`에 상수로 박혀 있고, regime multiplier(1.05/0.85/0.60)는 `services/strategy_engine.py::_market_regime_multiplier()`에 별도 존재. 주문 우선순위는 또 다른 스케일(`_order_plan_priority()`: theme_priority + fusion×0.02, cap 25)을 쓴다. strategy score는 하위 단계에서 소비되지 않는다. | 당장은 무해하나(score는 primary setup 선택과 표시용), 점수가 실측 성과와 무관하므로 대시보드/운영 판단에 오신호를 줄 수 있다. 스케일 혼재로 향후 우선순위 로직 개편 시 혼란. | score/confidence가 진단용임을 필드명 또는 문서로 명시한다. 백테스터(로드맵 §2.6 Phase 3) 가동 후 setup×regime별 실측 승률/expectancy로 캘리브레이션하고, 우선순위 스케일을 단일화한다. 자동 적용은 하지 않는다. | SQL: `SELECT primary_setup_type, AVG(score), COUNT(*) FROM strategy_observations_latest WHERE overall_status='MATCHED_OBSERVATION' GROUP BY 1;` — 점수가 상수임을 확인. pilot report의 setup별 expectancy와 대조. |
| P2-2 청산 파라미터가 종목 변동성·스프레드와 무관한 고정값 | `live_sim_exit_stop_loss_pct=3.0`, `take_profit_pct=5.0`, `trailing_stop_pct=2.5`(기본값), `.env`의 `LIVE_SIM_EXIT_MAX_HOLD_SEC=5400`. 저변동 종목과 고변동 테마주가 동일 스탑을 받는다. 근거: `services/config.py`, `services/live_sim/live_sim_service.py`. | 고변동 종목은 노이즈 스탑아웃, 저변동 종목은 과도한 손실 허용 — 실질 리스크가 종목별로 불균등. 손익비도 셋업 특성과 무관하게 고정. | 이미 기록 중인 MAE/MFE 분포에서 setup×변동성 구간별로 stop/take/trailing을 도출한다(백테스터 선행 필요, 로드맵 Phase 3). 그 전까지는 파라미터 변경 금지, 표본 축적에 집중. | SQL: pilot report의 MAE/MFE 분포를 setup별로 확인 — stop 3%가 승리 트레이드의 MAE 분포 바깥에 있는지 검증. |

## 진단 근거 요약

- **P0-1 vs P1-1 상호작용**: flow 게이트가 항상 열려 있어(P0-1) Strategy MATCHED가 과다 생성되고, 그 다수가 EntryTiming 모순 임계값(P1-1)에서 차단된다. 두 항목을 함께 고치지 않으면 한쪽 수정의 효과 측정이 왜곡된다 — P1-2 funnel 계측을 먼저 넣는 이유.
- **메모리에 남아 있던 미해결 질문**("12:13 이후 주문/체결 0건")의 유력 가설은 진입 윈도우(14:30 종료)가 아니라 P1-2의 테마 상태 하드 게이트다. funnel 계측으로 확정한다.

## 권장 Guard Tests (미작성 — 각 PR에 포함)

- `test_flow_gate_zero_delta_is_not_observed_flow` (P0-1)
- `test_momentum_continuation_position_gets_tighter_stop_than_global` (P0-2)
- `test_config_rejects_daily_loss_limit_not_below_daily_notional` (P0-3)
- `test_entry_timing_envelope_is_subset_of_strategy_envelope` (P1-1)
- `test_funnel_report_counts_stage_attrition_by_time_bucket` (P1-2)

## 다음 PR 권장 순서

1. **P1-2 funnel 계측**: 30분 버킷별 단계 통과율 + 차단 reason code 상위 N 리포트. 코드 변경 없이 관찰만 추가 — 이후 모든 수정의 효과 측정 기반이므로 최우선.
2. **P0-1 flow 게이트**: 최소 거래대금 증분 기본값을 0보다 크게 설정하고 zero-delta 통과를 차단. funnel 리포트로 전후 MATCHED 비율 비교.
3. **P0-3 일일 손실 한도 재설정**: 건당 리스크 배수 기반으로 `.env` 값 변경 + config validation 경고 추가. 코드 변경 최소.
4. **P0-2 포지션별 청산 파라미터**: `live_sim_positions`에 stop/take/trailing 컬럼 추가(migration), 진입 루트에서 전파, 청산 엔진이 포지션별 값 우선 사용.
5. **P1-1 셋업 정의 통합**: funnel 데이터로 교집합 감쇠를 확인한 뒤 임계값 네임스페이스 단일화 또는 envelope 포함관계 validation 추가.
6. **P2-1/P2-2 점수·청산 캘리브레이션**: 백테스터(로드맵 Phase 3) 가동 후 MAE/MFE·expectancy 기반으로 진행. 그 전에는 착수하지 않는다.
