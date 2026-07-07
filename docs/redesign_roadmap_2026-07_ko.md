# suseok-trader-v2 재설계 로드맵 (2026-07-07)

LIVE_SIM 파일럿 실가동 시점 기준으로 시스템 전체를 재점검하고, 재설계가 필요한 부분을
우선순위와 단계로 정리한 문서다. [아키텍처 진단 보고서(2026-07)](architecture_review_2026-07.md)의
후속 문서이며, 해당 보고서의 R0~R8 항목이 실제 코드에 반영됐는지 검증한 결과를 출발점으로 삼는다.

## 공통 안전 문구

- 기본은 `OBSERVE`다. `LIVE_SIM`은 키움 모의투자 전용이며 실계좌 주문이 아니다.
- `LIVE_REAL`은 이 로드맵의 어떤 단계에서도 자동으로 활성화되지 않는다. 별도 future safety project다.
- 이 문서의 어떤 항목도 Dashboard 실행 버튼, AI 자동 주문 입력, generic order enqueue를 도입하지 않는다.

---

## 1. 이전 진단(R0~R8) 반영 현황 — 코드 검증 결과

| 항목 | 내용 | 상태 | 검증 근거 |
| --- | --- | --- | --- |
| R0 | P0 5건(EOD 타임존, trade_date 불일치, Qt sleep, 무제한 버퍼, 재접속 부재) | ✅ 반영 | 커밋 이력 및 `domain/broker/utils.py` KST 유틸, `core_io_worker.py` 버퍼 상한 |
| R1 | settings 캐시 + 인증 미들웨어 + CI | ✅ 반영 | `load_settings` `@lru_cache`, `api/dependencies/auth.py` 전역 미들웨어(기본 차단 + read-only 화이트리스트 + 기동 시 토큰 강제 + `compare_digest`), `.github/workflows/ci.yml` |
| R2 | 레이트리밋 거버너 + 주문 pre-ack 저널 | ✅ 반영 | `gateway/kiwoom_command_handlers.py` `KiwoomRateLimitGovernor`(전역 5건/초 + 시간당 롤링 + 타입별 최소간격), `gateway/order_pre_ack_journal.py`, `PendingOrderRegistry`가 command_id 우선 매칭으로 교체됨 |
| R3 | 평가 런 원자성 + 단일 실행 락 | ✅ 반영 | `services/runtime/evaluation_run_guard.py`(`EVALUATION_PIPELINE_LOCK`, 재시도, 청크 처리) |
| R4 | 이벤트 구동 증분 평가 | ✅ 반영(부분) | `services/runtime/incremental_evaluation.py` dirty-set 큐(tick ingest 트리거) + Core lifespan 내 operating loop(최소 5초 주기). 단, entry-timing 이후 단계는 여전히 루프 주기 — §3.2 참조 |
| R5 | 리스크 게이트 계좌 한도 이관 | ✅ 반영 | `services/risk_gate.py`에 DRY_RUN/LIVE_SIM exposure·일손실 evidence(`daily_loss_guard`), preflight의 open position/order 한도 |
| R6 | projection watermark + 이벤트 보존정책 | ✅ 반영 | 커밋 `ea215d6`, `tools/prune_event_store.py` |
| R7 | 중첩 QEventLoop 제거 | ✅ 반영 | 커밋 `430a4b3` "Remove nested Kiwoom Qt waits" |
| R8 | venv/reports 정리, god 모듈 분해, Mock/Real Protocol 통일 | ⚠️ 부분 | venv_32/reports untrack 완료(`ba901ea`). **god 모듈은 오히려 성장** — §2.1 참조 |

결론: 안전성·정합성 계열(R0~R7)은 사실상 완료됐고, **구조 부채(R8)만 남은 채 파일럿
운영 수정이 그 위에 계속 쌓이고 있다.** 본 문서 작성 시점(2026-07-07, 미커밋 워킹트리
포함)에 `python -m pytest` 전체 스위트는 통과 상태다. 이번 로드맵의 초점은 (a) 파일럿 실운영에서
드러난 신규 리스크, (b) 구조 부채 해소, (c) 퀀트 개선 루프(백테스트/성과분석) 신설이다.

---

## 2. 재설계 필요 항목 — 신규 진단

### 2.1 [구조] god 모듈이 파일럿 수정 속도를 직접 저해 (S1, 최우선 구조 작업)

| 파일 | 2026-07 진단 시점 | 현재 | 증감 |
| --- | --- | --- | --- |
| `services/live_sim/live_sim_service.py` | 4,406줄 | 5,050줄 | +644 |
| `storage/sqlite.py` | 2,954줄 | 3,518줄 | +564 |
| `services/config.py` | 2,367줄 | 2,973줄 | +606 |
| `services/risk_gate.py` | (미측정) | 2,652줄 | — |

파일럿에서 발견되는 수정(entry window, EOD flatten, daily loss, excursion, exchange
gating…)이 전부 `live_sim_service.py` 한 파일로 들어가고 있다. 이 추세면 LIVE_REAL
논의 전에 회귀 리스크와 리뷰 비용이 임계점을 넘는다.

**방안** (동작 변경 없는 기계적 분해, 파일럿과 병행 가능한 소형 PR 연쇄):

- `live_sim_service.py` → `intent.py` / `order.py` / `execution.py`(체결·포지션·excursion) /
  `exit_policy.py`(stop·take·trailing·max_hold·EOD) / `cancel.py` / `reconcile.py` /
  `payloads.py`(API 직렬화)로 분해. 기존 공개 함수는 `live_sim_service.py`에서 재수출해
  API/도구/테스트 호환을 유지한다.
- `storage/sqlite.py` → 도메인별 스키마 모듈(`storage/schema/*.py`) + 마이그레이션
  레지스트리(버전 테이블 + 순서 보장). 현재는 전 도메인 DDL과 ad-hoc 마이그레이션이
  한 파일에 있어 스키마 변경 충돌이 잦다.
- `services/config.py` → 도메인 서브 설정(`GatewaySettings`, `LiveSimSettings`,
  `ThemeSettings`…) 합성으로 분리. 검증 로직을 서브 설정에 귀속.

### 2.2 [런타임] 루프 파편화와 크로스 프로세스 SQLite 쓰기 (S1)

현재 쓰기 주체가 4개다: ① Core API ingest(tick 단위), ② Core 내 operating loop(5초+),
③ **theme refresh 별도 프로세스**(동일 SQLite 직접 쓰기), ④ 수동 도구. WAL +
busy_timeout 15초 + DB 락 테이블로 버티고 있으나, theme refresh가 프로세스 밖에서
직접 DB를 여는 구조는 락 경합의 원인이자 단일 장애 지점 분산이다. 최근 커밋
(`fb491ad` 락 분리, `19e4fa1` 게이팅 완화)이 이 경합을 증상 단위로 손보는 중이다.

**방안**: theme refresh를 Core 내 스케줄 태스크(asyncio)로 흡수하거나, 최소한 쓰기를
Core API 경유(`POST /api/themes/...`)로 통일한다. 원칙: **운영 DB에 쓰는 프로세스는
Core 하나**. 별도 프로세스는 read-only(모니터)이거나 API 클라이언트여야 한다.

### 2.3 [런타임] 틱→주문 지연 예산이 정의·계측되지 않음 (S2)

ingest 시 Strategy/Risk 증분 평가는 tick 단위로 돌지만, entry-timing → order plan →
LIVE_SIM intent → command queue는 operating loop 주기(최소 5초 + 사이클 수행 시간)에
묶여 있다. 풀백/리클레임 계열 셋업(1~3분 봉 기준)에는 치명적이지 않으나, 현재는
"어느 단계에서 몇 초 걸리는지" 자체가 계측되지 않는다.

**방안**: 스테이지 타임스탬프 계측부터 시작한다. tick 수신 → projection → 증분 평가 →
entry-timing → intent → command dispatch 각 단계의 timestamp를 run 레코드에 남기고,
p50/p95 지연을 pilot report에 포함한다. 계측 결과가 나온 뒤에 루프 주기 단축 또는
entry-timing 증분화(dirty-set 확장)를 결정한다. **계측 없이 미리 최적화하지 않는다.**

### 2.4 [안전] Exit 경로의 단일 장애 지점 — Core 다운 시 보호 수단 부재 (S1)

stop-loss/trailing/EOD flatten이 전부 Core operating loop에서 평가된다. Core가 장중에
죽으면 (a) 신규 진입도 멈추지만 (b) **열린 포지션의 청산 판단도 멈춘다**. 또한 미체결
BUY의 TTL cancel도 Core가 만드는 command이므로 함께 멈춘다.

**방안** (경계 원칙 유지 — Gateway는 판단하지 않는다):

1. **Gateway dead-man cancel**: Gateway가 Core 폴링 실패를 N초 이상 감지하면 자기가
   pre-ack 저널에 기록한 **미체결 주문의 cancel만** 브로커에 낸다. cancel-only이므로
   "Gateway는 임의 주문을 만들지 않는다" 원칙과 충돌하지 않는다. 신규 주문·청산 판단은
   여전히 금지.
2. 포지션 청산은 자동화하지 않는다. 대신 운영자 알림(§2.7)과 수동 kill 절차 runbook을
   1차 방어선으로 삼고, LIVE_SIM에서 "Core 강제 종료 drill"을 정기 수행해 실측 복구
   시간을 기록한다.
3. exit 평가를 operating loop에서 분리해 더 짧은 주기의 전용 태스크로 승격하는 것은
   §2.3 계측 이후 결정한다.

### 2.5 [정합성] 브로커 스냅샷 reconcile이 비활성 — local-only 대사로 파일럿 진행 중 (S1)

launch report 기준 `reconcile_latest_status: Broker snapshot is disabled; local-only
reconcile is accepted`. 즉 현재 대사는 Core 내부 기록끼리의 검산이고, **키움 계좌
잔고/보유/미체결 TR과의 대사가 없다.** 체결 이벤트 유실·중복이 있어도 로컬 상태가
자기 일관성만 유지하면 통과한다.

**방안**: 장중 주기 + EOD 시점에 계좌 평가금/보유 종목/미체결 목록 TR 스냅샷을 받아
LIVE_SIM position/order와 대사하고, mismatch 시 신규 BUY를 차단(기존 정책 재사용)한다.
**mismatch 발생 → 차단 → 해소 drill 증거가 쌓이기 전에는 주문 명목 한도를 올리지
않는다.** 이 항목은 LIVE_REAL 게이트(§4)의 필수 전제다.

### 2.6 [퀀트] 전략 개선 루프 부재 — 백테스트·파라미터 평가 공백 (S2)

`tools/replay_observe_pipeline.py`는 observe 단계 재생 전용이라 체결이 없다. entry
timing 임계값, stop/take/trailing 파라미터를 파일럿 하루 단위 경험으로 조정하는 중이며,
이는 표본이 절대적으로 부족하다. MAE/MFE excursion 지표가 이미 포지션에 기록되므로
기반은 있다.

**방안**:

1. **이벤트 스토어 fill-model 백테스터**: 기존 replay 인프라를 확장해 OrderPlan까지
   재생하고, 보수적 체결 모델(지정가는 반대편 터치 시 체결, 부분체결·미체결 TTL 반영,
   수수료/세금/슬리피지 차감)로 가상 체결을 만든다. 운영 DB 쓰기 차단(authorizer)은
   기존 replay와 동일하게 유지.
2. **성과 분석 모듈**: LIVE_SIM 체결·excursion에서 setup/theme_state/stock_role/
   entry_timing_state별 승률·expectancy·보유시간·MAE/MFE 분포를 일별 자동 리포트로
   산출(기존 pilot report 확장).
3. **파라미터 평가**: 백테스터 위에 config 후보 세트를 walk-forward로 비교하는 도구.
   자동 적용은 하지 않고 리포트만 낸다(운영자가 .env 반영).

### 2.7 [운영] 프로세스 supervision·알림·EOD 자동화 부재 (S2)

현재 기동은 `start_live_sim_pilot.ps1` + `pids.json`, 감시는 신규 `monitor_live_sim_lifecycle.py`
(read-only JSONL)다. crash 시 자동 재기동이 없고, 운영자 알림 채널이 없으며, EOD 절차
(flatten 확인 → 리포트 → prune → 백업)가 수동이다.

**방안**:

1. **watchdog + 알림**: monitor를 확장해 (a) Core/Gateway/theme refresh 생존,
   (b) heartbeat 신선도, (c) 미체결 방치, (d) daily loss 접근을 감시하고 임계 시
   운영자 알림(Windows toast + 텔레그램/이메일 중 1개)을 보낸다.
2. **재기동 정책**: 관찰 프로세스(theme refresh, monitor)는 자동 재기동. **주문 가능
   프로세스(Core, Gateway)는 자동 재기동하되 preflight 전체 통과 전에는
   `queue_commands`가 켜지지 않게** launcher에 게이트를 둔다.
3. **EOD 자동화**: 15:30 이후 open position=0 확인 → pilot report/review sidecar 생성 →
   event store prune → SQLite 백업(일별 스냅샷) → 프로세스 종료를 스크립트 하나로 묶는다.
4. **DB 백업**: 현재 백업 절차가 없다. EOD 백업 + 장중 DB 파일 크기 모니터링을 추가한다.

### 2.8 [정합성] 게이트웨이 command 의미론 마무리 — 진행 중 작업의 완결 (S1, 진행 중)

미커밋 워킹트리(`storage/gateway_command_store.py` +136줄, expired/unstarted command
처리, command-critical 이벤트 큐 우선순위)가 바로 이 영역이다. 완결 조건을 명시한다:

- command TTL/expired/unstarted 상태 전이표를 `docs/gateway_transport.md`에 반영.
- Gateway 재기동 시 pre-ack 저널 재대사(미보고 주문 → 조회 TR 대사) drill을 LIVE_SIM에서
  1회 이상 수행하고 결과를 report로 남김.
- 이벤트 at-least-once / command at-most-once 비대칭과 그 보상 경로(저널, expired 처리)를
  한 문서에서 설명.

---

## 3. 잘 유지되고 있는 부분 (재설계 불필요, 보존 대상)

- **주문 이중 안전게이트 + LIVE_REAL 구조적 차단**: Core 측 command 검증 + Gateway 측
  `GetServerGubun` 실서버 거부. preflight 27항목 체크도 실효적으로 동작 중.
- **인증**: 전역 미들웨어 기본 차단 + 기동 시 토큰 강제. fail-open 문제 해소됨.
- **레이트리밋 거버너**: 전역/시간당/타입별 3중 캡 + 지연 흡수.
- **pre-ack 저널 + command_id 기반 체결 귀속**: 이전 진단의 P1-3 핵심이 해소됨.
- **증분 평가 dirty-set + 평가 파이프라인 락**: 배치 전량 재계산 문제 해소.
- **excursion(MAE/MFE) 기록**: 성과 분석(§2.6)의 데이터 기반이 이미 존재.
- **launcher의 단계별 안전 게이트**: 토큰 정렬, 중복 기동 방지, preflight 연동.

---

## 4. LIVE_REAL 게이트 (별도 프로젝트 유지 — 이 로드맵의 범위 밖)

LIVE_REAL은 여전히 미구현·금지이며, 아래 **정량 기준**이 전부 충족되기 전에는 착수
논의도 하지 않는다. 이 로드맵은 그 기준을 만드는 로드맵이지 LIVE_REAL 준비 로드맵이
아니다.

| # | 기준 | 근거 산출물 |
| --- | --- | --- |
| G1 | 브로커 스냅샷 reconcile(§2.5) 활성 상태로 20 거래일 무결 대사 | reconcile report |
| G2 | daily loss·exposure·건수 한도가 LIVE_SIM에서 실제 발동·차단한 이력 각 1회 이상 | safety gate 이벤트 |
| G3 | Core 강제 종료 drill + dead-man cancel(§2.4) 동작 확인 2회 이상 | drill report |
| G4 | Gateway 재기동 pre-ack 재대사 drill(§2.8) 1회 이상 | drill report |
| G5 | kill switch 활성→전 주문 경로 차단 drill 분기별 1회 | drill report |
| G6 | 백테스트-파일럿 성과 괴리 분석(§2.6) 1회 이상 | 성과 리포트 |

---

## 5. 단계별 실행 계획

원칙: 파일럿 운영을 멈추지 않는다. 모든 구조 PR은 동작 불변(기계적 분해) 또는
opt-in 플래그로 들어간다. 순서는 의존성 순이며, Phase 간 일부 병행 가능하다.

### Phase 1 — 파일럿 안전 완결 (즉시, ~1주)

| # | 작업 | 근거 | 크기 |
| --- | --- | --- | --- |
| 1-1 | 진행 중인 command expired/unstarted + 큐 우선순위 작업 완결·커밋 (§2.8) | 미커밋 워킹트리 | 소(진행 중) |
| 1-2 | 브로커 스냅샷 reconcile 활성화 + mismatch 차단 검증 (§2.5) | local-only 대사 | 중 |
| 1-3 | Gateway dead-man cancel (§2.4-1) | Core 다운 시 미체결 방치 | 소 |
| 1-4 | watchdog 알림 채널 1개 (§2.7-1) | 무감시 시간대 존재 | 소 |
| 1-5 | 스테이지 latency 계측 (§2.3) | 지연 예산 미정의 | 소 |

### Phase 2 — 구조 부채 해소 (~2주, 파일럿과 병행)

| # | 작업 | 근거 | 크기 |
| --- | --- | --- | --- |
| 2-1 | `live_sim_service.py` 7모듈 분해 (§2.1) — 재수출로 호환 유지 | 5,050줄 | 중(3~4 PR) |
| 2-2 | `storage/sqlite.py` 도메인 스키마 + 마이그레이션 레지스트리 (§2.1) | 3,518줄 | 중 |
| 2-3 | `config.py` 도메인 서브 설정 분리 + 기동 시 config snapshot 로깅 (§2.1) | 2,973줄 | 중 |
| 2-4 | theme refresh를 Core 내 태스크로 흡수 — 쓰기 주체 단일화 (§2.2) | 크로스 프로세스 경합 | 중 |

### Phase 3 — 퀀트 개선 루프 (~2주)

| # | 작업 | 근거 | 크기 |
| --- | --- | --- | --- |
| 3-1 | 성과 분석 모듈: setup/theme/role/timing별 expectancy·MAE/MFE 일별 리포트 (§2.6-2) | 데이터는 이미 기록 중 | 소 |
| 3-2 | fill-model 백테스터 (§2.6-1) | 파라미터 근거 부족 | 대 |
| 3-3 | walk-forward 파라미터 평가 도구 (§2.6-3) | 수동 튜닝 | 중 |

### Phase 4 — 상시 운영 전환 (~1주)

| # | 작업 | 근거 | 크기 |
| --- | --- | --- | --- |
| 4-1 | 재기동 정책 + preflight 게이트 launcher (§2.7-2) | 수동 재기동 | 소 |
| 4-2 | EOD 자동화 + SQLite 일별 백업 (§2.7-3/4) | 수동 절차 | 소 |
| 4-3 | Core 강제 종료 / Gateway 재기동 / kill switch drill 정례화 (§4 G3~G5) | 증거 축적 | 소(반복) |

### Phase 5 — LIVE_REAL 게이트 심사 (기준 충족 시)

§4의 G1~G6 충족 여부를 평가하는 것 자체가 산출물이다. 충족 전에는 LIVE_REAL 관련
코드를 작성하지 않는다.

---

## 6. 운영자 체크포인트

- 이 문서의 "완료/반영"은 관찰·모의투자 파이프라인 기준이며 실계좌 준비 완료가 아니다.
- Phase 2 구조 PR 병합 전후로 `python -m pytest` 전체 통과와 파일럿 launch report
  STARTED_OK를 함께 확인한다.
- 브로커 reconcile(1-2)이 켜지기 전까지는 `LIVE_SIM_MAX_ORDER_NOTIONAL`과 일일 건수
  한도를 올리지 않는다.
- dead-man cancel(1-3)은 cancel-only다. Gateway가 청산·신규 주문을 만들기 시작하면
  경계 위반이다.
- 백테스터(3-2)는 운영 DB에 절대 쓰지 않는다(기존 replay authorizer 정책 승계).
