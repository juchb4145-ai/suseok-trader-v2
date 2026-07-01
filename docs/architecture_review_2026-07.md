# suseok-trader-v2 아키텍처 진단 보고서 (2026-07)

Gateway(32-bit Kiwoom), Core(FastAPI/SQLite), API/테스트/도구 3개 영역을 전수 검토한 결과다.
심각도는 P0(치명: 즉시 수정), P1(아키텍처: 단계적 PR), P2(구조/위생: 정리 작업)로 나눈다.
P0 5건은 이 보고서와 같은 브랜치에서 코드로 수정했다. P1/P2는 로드맵으로 제안한다.

## 요약

| 심각도 | 건수 | 대표 항목 |
| --- | --- | --- |
| P0 | 5 | EOD 청산 타임존 버그, trade_date 3중 불일치, Qt 메인 스레드 블로킹, 무제한 이벤트 버퍼, 재접속 부재 |
| P1 | 7 | 배치 pull 파이프라인, 레이트리밋 거버너 부재, 주문 ack 유실 발산, SQLite 비원자성, 리스크 게이트 계좌 보호 부재 등 |
| P2 | 6 | venv_32 커밋, fail-open 인증, god 모듈, CI/mypy 부재, Mock/Real 드리프트, 도구 중복 |

잘 설계된 부분(보존 대상)도 명시한다: 주문 이중 안전게이트, CoreIoWorker 락 규율,
ingest 멱등성, config 기반 임계값, live_sim 수명주기/reconcile 모델링, AI sidecar 계층 분리.

---

## P0 — 치명적 정합성/안정성 결함 (이번 브랜치에서 수정됨)

### P0-1. EOD 청산이 장중에 절대 발동하지 않음 (타임존 버그)

- 위치: `services/live_sim/live_sim_service.py` `_is_eod_flatten_time()`
- 내용: **UTC 시각** 문자열(`datetime_to_wire(utc_now())[11:19]`)을 KST 의도의
  `live_sim_exit_eod_flatten_time="15:15:00"`(`services/config.py`)과 비교했다.
  KOSPI 장마감 청산 의도 시각 15:15 KST는 06:15 UTC이므로 비교는 15:15 **UTC**
  (= 다음날 00:15 KST)에야 참이 된다. LIVE_SIM exit이 활성화되는 순간
  장마감 미청산 포지션이 방치되는 실제 손실 경로다.
- 수정: `domain/broker/utils.py`에 `market_now()/market_today()/market_time_str()`
  (Asia/Seoul) 유틸을 추가하고 KST 시각으로 비교하도록 교체.

### P0-2. trade_date가 모듈마다 다른 달력을 사용 (3중 불일치)

- 후보 파이프라인: **Asia/Seoul** (`services/candidate_service.py` `_resolve_trade_date`,
  `candidate_trade_date_timezone` 설정)
- live_sim: **UTC 날짜** (`live_sim_service.py` `_today_trade_date`)
- 안전게이트: **서버 localtime** (`services/live_sim/safety_gate.py`
  `WHERE trade_date = date('now','localtime')`)
- 영향: 일일 주문 건수 한도(`live_sim_max_daily_order_count`), 일일 명목금액 한도,
  reconcile 대상 날짜가 KST 세션과 어긋난다. 서버 TZ 설정에 따라 안전게이트 카운트가
  live_sim이 기록한 trade_date와 다른 날짜를 조회할 수 있어 **일일 한도 우회**가 가능했다.
- 수정: 세 경로 모두 `market_today()`(KST) 기준으로 통일. 저장되는 이벤트 타임스탬프는
  기존대로 UTC 직렬화를 유지하고 "달력 날짜/장중 시각 판정"만 KST로 통일했다.

### P0-3. Qt 메인 스레드에서 `time.sleep()` — ActiveX 콜백 기아

- 위치: `gateway/kiwoom_runtime.py` `_send_configured_conditions`
- 내용: 조건검색 프로파일을 순차 전송하며 전송 간격을 맞추기 위해 메인 스레드에서
  `time.sleep(interval - elapsed)`을 호출했다. 이 함수는 `on_condition_loaded` →
  `load_conditions()`의 중첩 QEventLoop 내부, 즉 Qt 메인 스레드에서 실행되므로
  sleep 동안 Qt 이벤트 루프가 정지하고 `OnReceiveRealData`/`OnReceiveConditionVer` 등
  모든 ActiveX 콜백이 굶는다. `docs/kiwoom_gateway_callback_rca.md`가 경고한 바로 그
  실패 모드를 코드가 재도입한 상태였다.
- 수정: sleep을 제거하고 전송 대기열 + 지연 스케줄러(`schedule_delayed`, 실환경에서는
  `QTimer.singleShot` 주입) 기반 순차 전송으로 교체. 테스트에서는 가짜 스케줄러로 검증.

### P0-4. Core 장애 시 이벤트 버퍼 무제한 증가 (32-bit OOM 위험)

- 위치: `gateway/core_io_worker.py`
- 내용: Core가 다운되면 워커 deque가 무한 성장한다. coalescing은 `heartbeat`,
  `price_tick`, `quote_tick`, `market_index_tick` 4종에만 적용되고
  `command_ack`/`execution_event`/`condition_event`/`tr_response`/`gateway_log` 등은
  전부 누적된다. 32-bit 프로세스는 주소공간이 약 2GB이므로 장중 Core 장애가 길어지면
  Gateway 자체가 OOM으로 죽고, 버퍼는 RAM에만 있으므로 전량 유실된다.
- 수정: `max_buffer_size`(기본 10,000) 도입. 상한 초과 시 저가치 이벤트(tick류,
  heartbeat, gateway_log)를 오래된 것부터 드롭하고 주문 관련 이벤트(`command_ack`,
  `command_failed`, `execution_event`)는 보존한다. 드롭 카운트는 heartbeat payload의
  `core_io`에 노출해 Core 복구 후 유실 규모를 알 수 있게 했다.
- 잔여 리스크(P1): 디스크 스필/재전송 저널은 미구현. 주문 이벤트조차 프로세스 크래시
  시에는 유실된다. → 로드맵 R2 참조.

### P0-5. 세션 단절 시 자동 재접속 부재

- 위치: `gateway/kiwoom_runtime.py`(`reconnect_count: 0` 하드코딩),
  `apps/kiwoom_gateway.py`(시작 시 1회 로그인만 스케줄)
- 내용: 키움은 일일 점검/세션 만료로 강제 단절이 일상적인데, 단절 감지도 재로그인도
  없어 수동 재시작 전까지 조용히 데이터 수신이 멈춘다. `_recover_realtime_if_stalled`는
  로그인이 살아있을 때의 realtime 재등록만 수행하고 로그인 자체는 복구하지 못한다.
- 수정: watchdog에서 `GetConnectState`로 단절을 감지하고 지수 백오프(5s→…→300s 캡)로
  재로그인을 요청한다. `OnEventConnect` 단절 코드 수신도 동일 경로로 처리한다.
  재로그인 성공 시 realtime/조건검색을 재등록하고 `reconnect_count`를 실제 카운터로
  heartbeat에 노출한다.
- 실환경 검증 필요: 이 로직은 mock 주입 테스트로만 검증했다. 아래 "실환경 smoke 절차"
  를 반드시 수행할 것.

---

## P1 — 아키텍처 문제 (단계적 PR 로드맵)

### P1-1. 배치 pull 파이프라인 vs 장중 트레이딩의 근본 미스매치

tick 수신 시 즉시 갱신되는 것은 market data/index/regime projection뿐이다
(`api/routes/gateway.py`의 ingest 경로). Theme→Candidate→Strategy→Risk→EntryTiming은
운영자가 `run_market_open_observe_cycle`(또는 개별 `/evaluate` POST)을 호출할 때만,
그것도 매번 최대 500 후보 전체를 재계산한다(`services/runtime/market_open_observe_cycle.py`).
tick→리스크 재평가 지연 = 사이클 실행 주기(분 단위)다. 추격/과열·VWAP 이격·호가 스프레드
류의 장중 시그널을 분 단위 스냅샷으로 평가하는 구조다.

**방안**: Core에 이벤트 구동 증분 평가를 도입한다.
1단계로 ingest 경로에서 "해당 tick의 종목이 활성 후보인 경우에만" Strategy/Risk를
증분 재평가하는 dirty-set 큐를 만들고(전 종목 재스캔 제거), 2단계로 FastAPI lifespan에
백그라운드 평가 루프(asyncio task)를 두어 운영자 수동 트리거 의존을 없앤다.
observe-only 원칙은 유지된다 — 바뀌는 것은 관측의 신선도다.

### P1-2. 키움 API 레이트리밋 거버너 부재

`gateway/kiwoom_command_handlers.py`는 command 타입별 최소간격(`request_tr` 1.0s 등)만
검사하고, 위반 시 **큐잉이 아니라 거부**(`command_failed`)한다. 전역 5건/초 캡,
시간당 TR 캡의 롤링 카운터가 없고, 타입별 버킷이 독립이라 `send_order`+`send_condition`
+`request_tr`이 같은 초에 동시 발사될 수 있다. `kiwoom_tr.py`의 `request_delay_sec`은
저장만 되고 참조되지 않는 데드 코드다.

**방안**: 게이트웨이에 단일 토큰버킷 거버너(전역 N건/초 + 타입별 가중치 + 시간당 롤링
카운터)를 두고, 초과분은 거부 대신 지연 큐로 흡수한다. Core 재발행 의존을 제거한다.

### P1-3. 주문 ack 유실 시 Core-브로커 상태 발산

`send_order` 실행(`SendOrder` 발사) **후에** `command_ack` 이벤트를 큐에 넣는다.
ack가 Core에 닿지 못하면 Core는 dispatch timeout으로 command를 FAILED 처리하지만
브로커에는 주문이 살아있다. 또한 command 전달은 at-most-once(폴링 직후 게이트웨이
크래시 시 유실, `storage/gateway_command_store.py`의 DISPATCHED→FAILED 편도 전이),
이벤트 전달은 at-least-once(중복 가능)로 비대칭이다.

**방안**: (a) 주문 실행 직전 로컬 pre-ack 저널(디스크) 기록 → 재기동 시 미보고 주문을
`execution_event`/조회 TR로 재대사, (b) Core 측 FAILED 전이를 "UNCONFIRMED"로 세분화해
chejan/execution 경로와 대사 후 종결, (c) `PendingOrderRegistry`의 (account, code, side)
서명 충돌(동일 종목 동시 매수 시 체결 오귀속)을 command_id 기반 키로 교체.

### P1-4. SQLite 다중 문장 쓰기의 비원자성 + 동시 실행 무방비

평가 런이 청크 단위로 `commit()`하고(`services/risk_gate.py`), 사이클/개별 `/evaluate`의
동시 실행을 막는 락이 없다. 두 writer가 겹치면 한쪽이 5초 busy_timeout 후
`SQLITE_BUSY`로 죽는데, 이미 절반 커밋된 상태다. `*_latest` 테이블은 두 런이
비결정적으로 섞일 수 있다.

**방안**: 평가 런 단위 `BEGIN IMMEDIATE` 트랜잭션 + 애플리케이션 레벨 "사이클 단일
실행" 락(DB 락 테이블 또는 프로세스 내 asyncio lock). run row에 idempotency key를 두어
재실행 시 중복 런 생성을 방지한다.

### P1-5. projection 재빌드가 full-replay 전용 + 이벤트 스토어 무보존정책

`rebuild_market_data_projection`은 전체 이벤트 재생만 안전하다(누적 delta를
`market_ticks_latest` 현재값과의 diff로 계산하므로 부분 replay는 오염,
`market_tick_samples`의 event_id PK 충돌). `raw_events`/`gateway_events`는 tick마다
행이 쌓이는데 TTL/프루닝이 전혀 없어 재빌드 비용이 무한 증가한다.

**방안**: projection에 watermark(마지막 처리 event rowid) 컬럼을 두어 증분 replay를
가능하게 하고, 일 단위 파티션 정리 잡(예: N일 경과 raw tick 아카이브 후 삭제)을 추가한다.

### P1-6. 리스크 게이트에 계좌 보호가 없음

리스크 게이트의 체크는 전부 시장/데이터 품질 관측(추격/과열, 유동성/스프레드,
중복/쿨다운, 레짐)이고, 포지션 사이징·일손실 한도·노출 한도는
`check_portfolio_placeholder` 스텁(`NOT_EVALUATED`)이다. 정량 한도
(`live_sim_max_order_notional`, `live_sim_max_daily_*`)는 live_sim safety gate에만 있고
기본 always-block이라 실전 검증된 적이 없다.

**방안**: 계좌 수준 한도(1회 주문 명목, 일 손실, 총 노출, 종목당 집중도, kill switch)를
리스크 도메인으로 이관해 DRY_RUN 경로에서도 평가·기록되게 하고, safety gate는 이를
최종 재확인하는 이중 게이트로 유지한다. LIVE_REAL 전에 이 한도들이 시뮬레이션에서
실제로 발동한 이력이 쌓여야 한다.

### P1-7. 중첩 QEventLoop / processEvents busy-wait

`login()`/`load_conditions()`의 중첩 `QEventLoop.exec_()`(그것도 login 루프 안에서
condition 루프가 또 열리는 2중 중첩), `kiwoom_tr.py`의 `QApplication.processEvents()`
busy-wait(최대 20초)는 타이머 재진입으로 부분 초기화 상태의 런타임 코드가 실행될 수
있는 구조다.

**방안**: 로그인/조건로드를 상태머신 + 타이머 콜백(비차단 대기)으로 전환하고, TR 대기도
pending-map + `OnReceiveTrData` 콜백 완결 방식으로 바꾼다. (이번 P0-3 수정으로 조건
"전송" 경로는 비차단화됐고, "로드 대기" 경로가 남아 있다.)

---

## P2 — 구조/위생

| 항목 | 내용 | 방안 |
| --- | --- | --- |
| venv_32 커밋 | 3,518개 파일이 git 추적 중(저장소의 약 90%). `.gitignore`에 누락돼 있었다 | 이번에 `.gitignore`에 추가. 후속으로 `git rm -r --cached venv_32` 커밋, 히스토리 purge(`git filter-repo`)는 협업자 조율 후 별도 수행 |
| reports/ 커밋 | 운영 산출물 ~7.7MB(JSON 런 결과)가 버전 관리됨 | reports/를 gitignore하고 산출물은 로컬/오브젝트 스토리지로 |
| 인증 fail-open | `api/dependencies/auth.py`: 토큰 미설정 시 인증 전면 비활성. 엔드포인트별 수동 opt-in이라 dashboard/market_data 등 모듈 전체가 무인증 | 전역 미들웨어로 전환(기본 차단 + read-only 화이트리스트), 빈 토큰이면 기동 거부, `secrets.compare_digest` 사용 |
| god 모듈 | `live_sim_service.py` 4,406줄, `storage/sqlite.py` 2,954줄, `services/config.py` 2,367줄(300필드 + 600줄 검증이 **매 요청** 재실행) | `load_settings()`에 `lru_cache` 즉시 적용(1줄). live_sim은 평가/영속/페이로드/대사 4모듈로 분해. config는 도메인별 서브 설정으로 분리 |
| CI/타입체크 부재 | `.github/workflows` 없음, mypy 미도입(힌트는 전면 존재), ruff 규칙 최소(B,E,F,I,UP), black+ruff 이중 포매터 | pytest+ruff+mypy CI 워크플로 추가, black 제거(ruff format), 복잡도(C90)·보안(S) 규칙 도입 |
| Mock/Real 드리프트 | `MockKiwoomClient`가 Protocol 없이 표면 복제, mock 런타임은 QTimer/워커 동시성 모델을 검증하지 않음. 게이트웨이 설정 3원화(GatewaySettings/RuntimeConfig/argparse) | 공유 `Protocol` 정의, mock에도 실제 워커 경로 사용, 설정 단일화 |
| 도구 중복 | tools/ 65개가 API 202개 엔드포인트와 `load_settings+open_connection+try/finally` 보일러플레이트를 복붙 | 공용 CLI 헬퍼(컨텍스트 매니저) 하나로 수렴 |

---

## 보존해야 할 강점

- **주문 이중 안전게이트**: Core 측 LIVE_SIM metadata 검증(`storage/gateway_command_store.py`
  `validate_command_type_allowed` — idempotency key, mode=LIVE_SIM, live_real_allowed=false 강제)
  + Gateway 측 `GetServerGubun` 실서버 감지 거부. LIVE_REAL이 구조적으로 도달 불가능하다.
- **CoreIoWorker 락 규율**: 단일 조건변수로 이벤트/카운터를 보호하고 우선 heartbeat
  삽입 시 head 재확인까지 처리한 동시성 설계는 정확하다.
- **ingest 멱등성**: `storage/event_store.py`의 event_id+payload_hash 중복 제거와
  CONFLICT 감지.
- **config 기반 전략/리스크 임계값**: 수치 파라미터가 코드에서 분리돼 있다.
- **live_sim 주문/포지션 수명주기와 reconcile**: intent→order→execution→position 모델링,
  체결 합계 대사, 음수 잔량 감지 등 실질적이다.
- **AI sidecar 계층 분리**: domain/services/provider 3층, 네트워크 게이트, 리댁션 강제
  — 저장소에서 가장 규율 있는 서브시스템.

---

## 권장 진행 순서 (로드맵)

| 순번 | 작업 | 심각도 | 크기 |
| --- | --- | --- | --- |
| R0 | 본 브랜치의 P0 5건 수정 검토·병합 | P0 | 완료(이 브랜치) |
| R1 | `load_settings()` 캐시 + 인증 미들웨어 + CI(pytest/ruff/mypy) | P2 | 소 |
| R2 | 레이트리밋 거버너 + 주문 pre-ack 저널/재대사 (P1-2, P1-3) | P1 | 중 |
| R3 | 평가 런 원자성 + 사이클 단일 실행 락 (P1-4) | P1 | 중 |
| R4 | 이벤트 구동 증분 평가 파이프라인 (P1-1) | P1 | 대 |
| R5 | 리스크 게이트 계좌 한도 이관 (P1-6) | P1 | 중 |
| R6 | projection watermark + 이벤트 보존정책 (P1-5) | P1 | 중 |
| R7 | 중첩 QEventLoop 제거 (P1-7) | P1 | 중 |
| R8 | venv_32/reports 정리, god 모듈 분해, Mock/Real Protocol 통일 | P2 | 대(분할) |

LIVE_REAL 검토는 최소 R0~R5 완료 + LIVE_SIM에서 계좌 한도 발동 이력 확보 이후에만
논의할 것을 권고한다.

---

## 실환경 smoke 절차 (P0 수정 검증용, Windows 32-bit Kiwoom 환경)

이 저장소의 CI/테스트 환경에서는 실제 Kiwoom COM을 실행할 수 없으므로, P0-3/P0-5는
모의투자 서버 접속 환경에서 아래 절차로 확인한다.

1. **조건검색 비차단 전송(P0-3)**: 조건 프로파일 2개 이상 설정 후 게이트웨이 기동.
   `gateway/status`의 `condition_send` 관련 카운트가 프로파일 수만큼 증가하고,
   전송 구간 동안 `realtime_callback_count`가 계속 증가하는지(콜백 기아 없음) 확인.
2. **재접속(P0-5)**: 게이트웨이 기동·로그인 후 네트워크를 60초 차단하거나 키움 세션을
   강제 종료. heartbeat의 `login_connected=false` 전환 → `reconnect_count` 증가 →
   재로그인 성공 후 `realtime_registration_success_count` 재증가(재등록)와 tick 재수신을
   확인. 백오프 간격(5s→10s→…)이 로그에 남는지 확인.
3. **버퍼 상한(P0-4)**: Core를 내린 상태에서 장중 tick을 5분 수신시킨 뒤 Core 재기동.
   게이트웨이 heartbeat `core_io.dropped_event_count`가 노출되고 프로세스 메모리가
   상한에서 안정화되는지 확인. `command_ack`류가 드롭되지 않았는지 Core 수신 이벤트로 대사.
4. **EOD/trade_date(P0-1/2)**: 모의투자 장중 15:10 KST에 LIVE_SIM 포지션을 만든 뒤
   15:15 KST에 exit run_once 실행 → EOD flatten 사유의 exit signal이 생성되는지 확인.
   `live_sim/status`의 일일 카운트 trade_date가 KST 날짜와 일치하는지 확인.
