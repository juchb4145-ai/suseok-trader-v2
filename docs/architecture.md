# suseok-trader-v2 Architecture

## 요약

`suseok-trader-v2`는 Gateway와 Core를 분리한 국내 주식 자동매매 기반 시스템이다. 현재 구현의 핵심은 **관찰, 저장, 검토, 안전 경계**다. Core는 PyQt5, QAxWidget, Kiwoom OpenAPI+ concrete client를 import하지 않는다. Gateway는 broker 통신을 담당하지만 전략 판단과 리스크 판단을 하지 않는다.

## 전체 흐름

```text
Gateway
  -> Event Store
  -> Market Data Service
  -> Theme Service
  -> Candidate FSM
  -> Strategy Observation
  -> Risk Observation
  -> Dashboard
  -> AI Sidecar / RCA / Codex Prompt Draft
  -> DRY_RUN OMS
  -> DRY_RUN Exit Engine
  -> LIVE_SIM
```

## 프로세스 분리 원칙

| 영역 | 담당 | 금지 |
| --- | --- | --- |
| 32-bit Kiwoom Gateway | Kiwoom OpenAPI+ ActiveX/QAxWidget 접근, broker event 수집, command polling | 전략 판단, 리스크 판단, 임의 주문 생성 |
| 64-bit Core/API/Web Dashboard | state, persistence, projection, policy, API, operator view | PyQt5, QAxWidget, concrete Kiwoom client import |
| AI Sidecar | context, RCA, review, Codex prompt draft | Strategy/Risk/OMS 자동 입력, order tool |

기존 `juchb4145-ai/suseok_ai`는 경계와 안전 원칙 참고용이다. runtime code, legacy strategy logic, Kiwoom/PyQt integration을 이 저장소로 복사하지 않는다.

## Gateway/Core Transport

- Gateway to Core: `POST /api/gateway/events`
- Core to Gateway: `GET /api/gateway/commands`

PR 2B부터 Core는 broker-neutral HTTP surface를 제공한다. 실제 Kiwoom/PyQt Gateway는 Core 밖에 있어야 한다.

## Domain 경계

| 도메인 | 역할 | 주문 관련 경계 |
| --- | --- | --- |
| Event Store | Gateway event 원본과 정규화 event 저장 | 실행하지 않음 |
| Market Data Service | tick/latest/bar/VWAP/readiness projection | 주문 판단하지 않음 |
| Theme Service | membership과 snapshot | 투자 추천이나 주문 판단 아님 |
| Candidate FSM | 후보 관찰 에피소드 상태 관리 | 매수 후보 확정 아님 |
| Strategy Engine | setup observation 저장 | `MATCHED_OBSERVATION`은 매수 신호 아님 |
| Risk Gate | risk observation 저장 | `OBSERVE_PASS`는 주문 승인 아님 |
| OMS DRY_RUN | 내부 모의 회계 | 브로커 주문 아님 |
| LIVE_SIM | 키움 모의투자 전용 queue path | 실계좌 주문 아님 |
| AI Sidecar | review-only 분석 보조 | 자동 주문 입력 아님 |

## Project Layout

- `apps/`: 실행 entrypoint
- `api/`: FastAPI route module
- `domain/`: broker-neutral domain model과 policy
- `infrastructure/`: 필요할 때 추가되는 external adapter
- `services/`: application service
- `storage/`: SQLite connection과 persistence helper
- `web/`: Dashboard template/static asset
- `tests/`: automated tests
- `docs/`: architecture, contract, runbook
- `tools/`: local script

## 안전 기본값

- `TRADING_MODE=OBSERVE`
- `TRADING_ALLOW_LIVE_SIM=false`
- `TRADING_ALLOW_LIVE_REAL=false`
- `AI_SIDECAR_ENABLED=false`
- `DRY_RUN_OMS_ENABLED=false`
- `LIVE_SIM_ENABLED=false`
- `LIVE_SIM_KILL_SWITCH=true`

이 문서의 기능은 실계좌 주문을 의미하지 않는다. `LIVE_REAL`은 현재 구현되어 있지 않으며 별도 future safety project다.

## 명시적으로 제외된 legacy scope

- `runtime_factory.py`
- legacy runtime code
- hybrid strategy code
- dirty/shadow variant
- threshold A/B machinery
- Core 내부 Kiwoom OpenAPI+/PyQt implementation

## 운영자 체크포인트

- Core에서 PyQt/QAxWidget/Kiwoom concrete import가 보이면 경계 위반으로 본다.
- Gateway가 broker 통신 외 전략/리스크 판단을 시작하면 경계 위반이다.
- Dashboard와 AI Sidecar는 실행 주체가 아니다.
- Strategy/Risk/DRY_RUN/LIVE_SIM의 안전 경계를 서로 섞어 해석하지 않는다.
