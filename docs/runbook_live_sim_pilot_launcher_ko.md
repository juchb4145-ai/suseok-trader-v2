# LIVE_SIM 파일럿 런처 런북

`tools/start_live_sim_pilot.ps1`는 LIVE_SIM 파일럿 장중 기동을 한 번에 수행하는 PowerShell 런처다. Core와 Kiwoom Gateway를 분리 기동하고, Naver theme import, theme snapshot rebuild, Core API 기반 기동 검증, Markdown 리포트 생성을 순서대로 실행한다.

이 런처는 `GatewayCommand`나 주문을 직접 생성하지 않는다. 주문 가능 여부는 `.env`, safety gate, preflight, operating loop가 판단한다. LIVE_REAL 관련 플래그는 켜지 않는다.

## 기본 사용법

```powershell
pwsh -File .\tools\start_live_sim_pilot.ps1
```

옵션:

```powershell
pwsh -File .\tools\start_live_sim_pilot.ps1 -DryRun
pwsh -File .\tools\start_live_sim_pilot.ps1 -SkipNaverImport
pwsh -File .\tools\start_live_sim_pilot.ps1 -GatewayPython C:\Python39-32\python.exe
```

정리:

```powershell
pwsh -File .\tools\stop_live_sim_pilot.ps1
pwsh -File .\tools\stop_live_sim_pilot.ps1 -Force
```

`-Force`는 활성 LIVE_SIM 주문 조회가 실패했거나 활성 주문이 남아 있을 때도 프로세스를 종료해야 하는 운영자 판단이 있을 때만 사용한다.

## 아침 기동 체크리스트

| 단계 | 기대결과 | 실패 시 조치 |
| --- | --- | --- |
| `.env` 확인 | `TRADING_MODE=LIVE_SIM`, `TRADING_ALLOW_LIVE_REAL=false`, `TRADING_ALLOW_LIVE_SIM=true`, `LIVE_SIM_ENABLED=true` | `.env`를 수정하고 `-DryRun`으로 다시 확인 |
| 토큰 확인 | `TRADING_CORE_TOKEN`과 `GATEWAY_CORE_TOKEN` 일치 | 두 값을 같은 local token으로 맞춤 |
| 시뮬레이션 모드 | `LIVE_SIM_ACCOUNT_MODE/BROKER_ENV/SERVER_MODE=SIMULATION` | 실계좌 관련 값이 섞였는지 확인하고 중단 |
| 중복 기동 확인 | 포트 8000과 `apps.core_api:app`, `apps.kiwoom_gateway` 프로세스 없음 | `.\tools\stop_live_sim_pilot.ps1` 또는 수동 종료 |
| Core 기동 | `/health`가 30초 내 200 반환 | `logs/live_sim_pilot/runtime/core_*.err.log` 확인 |
| Naver theme import | 성공. 실패해도 런처는 경고 후 계속 | 실패하면 `naver_import_recent` preflight WARN 가능. 네트워크/파서 로그 확인 |
| Theme snapshot rebuild | 1회 성공 | theme membership과 market data projection 상태 확인 |
| Gateway 기동 | 32-bit Python, PyQt5, Kiwoom OpenAPI+ 환경에서 프로세스 시작 | `Current Python is 64-bit`, `PyQt5 is required`, ActiveX 등록 오류 확인 |
| Gateway login | `kiwoom_logged_in=true`, `server_mode=SIMULATION` | Kiwoom 로그인창, server_gubun, token/Core URL 확인 |
| 실시간 tick | latest price tick `event_ts`가 60초 이내 | 조건식/실시간 등록, `latest_realtime_callback_at`, event posting 확인 |
| 지수 실시간 | latest market index tick `event_ts`가 120초 이내 | `KIWOOM_MARKET_INDEX_REALTIME_ENABLED=true`, 필요 시 `KIWOOM_MARKET_INDEX_TR_BOOTSTRAP_ENABLED=true`로 재기동 |
| 조건식 로드 | `condition_load_state=LOADED` | 조건식 파일/이름, `OnReceiveConditionVer`, gateway event 로그 확인 |
| LIVE_SIM status | safety gate 요약 출력 | kill switch, heartbeat, gateway_orderable, simulation mode 확인 |
| Preflight | `PASS` 또는 원인 있는 `WARN`; `BLOCK`이면 주문 차단 | 출력된 `blocking_reasons`를 먼저 해소 |
| Operating loop | `LIVE_SIM_OPERATING_LOOP_ENABLED=true`이면 90초 내 새 run 생성 | Core loop 설정, market time window, evaluation lock 상태 확인 |

## 단계별 판정 기준

0단계 안전 게이트는 하나라도 실패하면 즉시 중단한다.

- `TRADING_ALLOW_LIVE_REAL=false`
- `TRADING_MODE`가 `LIVE_REAL`이 아님
- `TRADING_MODE=LIVE_SIM`
- `TRADING_ALLOW_LIVE_SIM=true`
- `LIVE_SIM_ENABLED=true`
- `LIVE_SIM_ACCOUNT_MODE=SIMULATION`
- `LIVE_SIM_BROKER_ENV=SIMULATION`
- `LIVE_SIM_SERVER_MODE=SIMULATION`
- `TRADING_CORE_TOKEN`과 `GATEWAY_CORE_TOKEN` 일치
- 포트 8000과 Core/Gateway 프로세스 중복 없음

`LIVE_SIM_KILL_SWITCH=true`는 실패가 아니다. 런처는 노란 경고를 내고 계속한다. 이 경우 기동은 가능하지만 preflight나 safety gate에서 주문 큐잉이 차단될 수 있다.

1단계 Core 기동은 반드시 `venv_64\Scripts\python.exe -m uvicorn apps.core_api:app --host 127.0.0.1 --port 8000` 형식으로 실행한다. `--reload`는 사용하지 않는다. 이 리포에서는 reload가 고아 락과 코드 미반영 사고를 만든 이력이 있다.

2단계 장전 준비는 `python -m tools.import_naver_themes`와 `python -m tools.rebuild_theme_snapshots`를 실행한다. Naver import만 실패 허용이며, 실패 시 `naver_import_recent` preflight가 WARN이 될 수 있음을 보고한다.

3단계 Gateway는 기본 `venv_32\Scripts\python.exe`를 사용한다. `-GatewayPython`으로 다른 32-bit Python을 지정할 수 있다. 핵심 인자는 `--auto-login`, `--no-threaded-login`, `--no-observe-only`다.

4단계 기동 검증은 최대 3분 동안 Core API를 폴링한다. 각 항목은 콘솔과 리포트에 `✅` 또는 `❌`로 기록된다.

- `GET /api/gateway/status`: `kiwoom_logged_in=true`, `server_mode=SIMULATION`
- `GET /api/market-data/ticks/latest?limit=1`: 최신 가격 tick 60초 이내
- `GET /api/market-indexes/latest?limit=1`: 최신 지수 tick 120초 이내
- `GET /api/gateway/status`: `condition_load_state=LOADED`
- `GET /api/live-sim/status`: safety gate 요약
- `GET /api/live-sim/operator/preflight`: overall status, WARN/BLOCK 사유
- `GET /api/live-sim/operator/runs/latest`: operating loop 새 run 확인

Preflight가 `BLOCK`이면 런처는 "기동은 완료됐지만 주문은 차단 상태"라고 표시한다. 이 상태에서는 Core/Gateway가 떠 있어도 BUY 큐잉은 진행되지 않는다.

5단계 리포트는 `reports/live_sim_pilot_launch/<yyyy-MM-dd_HHmmss>/launch_report.md`에 저장된다. 토큰은 마스킹된다.

## DryRun 수동 확인 절차

스크립트 자체는 pytest 대상이 아니므로 `-DryRun`으로 안전 게이트와 실행 계획을 수동 확인한다.

```powershell
pwsh -File .\tools\start_live_sim_pilot.ps1 -DryRun
```

확인할 것:

- 0단계 안전 게이트가 `✅`로 끝난다.
- 출력된 Core 명령에 `--reload`가 없다.
- 출력된 Gateway 명령에 `--no-observe-only`, `--auto-login`, `--no-threaded-login`이 있다.
- `TRADING_CORE_TOKEN`과 `GATEWAY_CORE_TOKEN` 불일치 시 실패하는지 확인한다.
- 포트 8000이나 기존 Gateway가 떠 있으면 중복 기동으로 실패하는지 확인한다.

## 산출물 위치

- PID 파일: `logs/live_sim_pilot/pids.json`
- Runtime 로그: `logs/live_sim_pilot/runtime/*.log`
- 기동 리포트: `reports/live_sim_pilot_launch/<yyyy-MM-dd_HHmmss>/launch_report.md`

## 종료 기준

정상 종료는 gateway를 먼저, core를 나중에 종료한다.

```powershell
pwsh -File .\tools\stop_live_sim_pilot.ps1
```

stop 스크립트는 종료 전에 `GET /api/live-sim/orders?limit=500`을 조회한다. `COMMAND_QUEUED`, `BROKER_ACKED`, `PARTIALLY_FILLED` 주문이 있으면 목록을 보여주고 `-Force` 없이는 종료를 거부한다.

`runtime_execution_locks` 잔여 락은 Core 재기동 시 자동 정리된다. stop 스크립트는 DB를 직접 수정하지 않는다.
