# Mock Gateway Runbook

## 요약

Mock Gateway는 실제 Kiwoom runtime 없이 Core/Gateway transport를 검증하는 로컬 도구다. heartbeat, deterministic price tick, condition event, command polling 흐름을 확인한다. 이 runbook의 절차는 실계좌 주문을 의미하지 않는다.

## Start Core

```powershell
python -m uvicorn apps.core_api:app --host 127.0.0.1 --port 8000 --reload
```

Helper:

```powershell
.\tools\start_core.ps1
```

확인:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/health
Invoke-RestMethod http://127.0.0.1:8000/api/status
```

## Run Once

```powershell
python -m apps.mock_gateway --core-url http://127.0.0.1:8000 --once
```

Helper:

```powershell
.\tools\start_mock_gateway_once.ps1
```

예상 event:

- `heartbeat`
- `price_tick`
- `condition_event`

## Run Loop

```powershell
python -m apps.mock_gateway --core-url http://127.0.0.1:8000 --interval-sec 1.0
```

Helper:

```powershell
.\tools\start_mock_gateway_loop.ps1
```

Loop mode는 heartbeat와 deterministic price tick을 주기적으로 보내고 Core command를 계속 polling한다.

## Token 설정

Core에 `TRADING_CORE_TOKEN`이 있으면 Gateway에도 같은 값을 전달한다.

```powershell
$env:TRADING_CORE_TOKEN = "change-me-local-token"
$env:GATEWAY_CORE_TOKEN = "change-me-local-token"
python -m apps.mock_gateway --once
```

Gateway는 `X-Core-Token` header를 사용한다.

## Core 상태 확인

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/gateway/status
Invoke-RestMethod http://127.0.0.1:8000/api/gateway/events/recent
Invoke-RestMethod http://127.0.0.1:8000/api/gateway/commands/status
Invoke-RestMethod http://127.0.0.1:8000/api/market-data/status
Invoke-RestMethod http://127.0.0.1:8000/api/market-data/ticks/005930
```

첫 mock run 후 `last_heartbeat_at`이 채워져야 한다.

## 관찰 파이프라인 확인

```powershell
python -m tools.import_theme_memberships --file data/themes/sample_themes.json
python -m tools.rebuild_theme_snapshots
python -m tools.rebuild_candidates
python -m tools.evaluate_strategy
python -m tools.evaluate_risk
Invoke-RestMethod http://127.0.0.1:8000/api/dashboard/snapshot
```

이 흐름은 observe-only다. `MATCHED_OBSERVATION`은 매수 신호가 아니고 `OBSERVE_PASS`는 주문 승인이 아니다.

## Command Polling Test

tests는 `storage.gateway_command_store.enqueue_command()`를 통해 command를 넣는다. Mock Gateway는 `GET /api/gateway/commands`를 polling하고 허용된 command만 처리한다.

Order-like command는 기본적으로 disabled다. forbidden command는 `command_failed`가 되고 주문 동작은 실행되지 않는다.

## Troubleshooting

| 증상 | 확인할 것 |
| --- | --- |
| `Core transport failed` | Core 실행 여부와 `--core-url` |
| `Core HTTP 401` | `GATEWAY_CORE_TOKEN` 또는 `--token` |
| `Core HTTP 403` | token 값이 `TRADING_CORE_TOKEN`과 같은지 |
| recent events 없음 | Core와 Gateway가 같은 `TRADING_DB_PATH`를 보는지 |
| market data 없음 | mock event가 accepted인지, projection error가 있는지 |

## 운영자 체크포인트

- Mock Gateway는 transport 검증 도구다.
- 실제 Kiwoom 주문, 실계좌 주문, 자동 실행 경로가 아니다.
- command 상태가 `ACKED`인지, `FAILED`인지 먼저 본다.
- LIVE_SIM acceptance와 혼동하지 않는다.
