# Local Observe Runbook KO

## 요약

이 runbook은 로컬에서 관찰 파이프라인을 확인하는 절차다. 모든 명령은 PowerShell 예시다. 이 절차는 실제 주문이 나가지 않는다. Dashboard는 읽기 전용이며 실행 버튼이 없다.

## 0. 사전 확인

```powershell
python --version
python -m pytest --version
```

선택적으로 local token 설정:

```powershell
$env:TRADING_CORE_TOKEN = "change-me-local-token"
```

## 1. Core 실행

```powershell
python -m uvicorn apps.core_api:app --host 127.0.0.1 --port 8000 --reload
```

별도 PowerShell 창에서 확인:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/health
Invoke-RestMethod http://127.0.0.1:8000/api/status
```

## 2. Mock Gateway 실행

```powershell
python -m apps.mock_gateway --core-url http://127.0.0.1:8000 --once
```

확인:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/gateway/status
Invoke-RestMethod http://127.0.0.1:8000/api/gateway/events/recent
```

## 3. Market Data 확인

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/market-data/status
Invoke-RestMethod http://127.0.0.1:8000/api/market-data/ticks/005930
Invoke-RestMethod "http://127.0.0.1:8000/api/market-data/bars/005930?interval_sec=60"
Invoke-RestMethod http://127.0.0.1:8000/api/market-data/readiness/005930
```

필요하면 projection rebuild:

```powershell
python -m tools.rebuild_market_data_projection --clear-projection
```

## 4. Theme import/rebuild

```powershell
python -m tools.import_theme_memberships --file data/themes/sample_themes.json
python -m tools.rebuild_theme_snapshots
```

확인:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/themes/status
Invoke-RestMethod http://127.0.0.1:8000/api/themes/snapshots/latest
```

수동 sample file은 bootstrap/testing only다.

## 5. Candidate rebuild

```powershell
python -m tools.rebuild_candidates
```

확인:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/candidates/status
Invoke-RestMethod http://127.0.0.1:8000/api/candidates
```

특정 candidate 확인:

```powershell
python -m tools.inspect_candidate --candidate-instance-id CAND-2026-06-27-005930-1 `
  --include-context --include-sources --include-transitions
```

Candidate는 매수 후보가 아니라 관찰 에피소드다.

## 6. Strategy evaluate

```powershell
python -m tools.evaluate_strategy
```

확인:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/strategy/status
Invoke-RestMethod http://127.0.0.1:8000/api/strategy/observations/latest
```

`MATCHED_OBSERVATION`은 매수 신호가 아니다.

## 7. Risk evaluate

```powershell
python -m tools.evaluate_risk
```

확인:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/risk/status
Invoke-RestMethod http://127.0.0.1:8000/api/risk/observations/latest
```

`OBSERVE_PASS`는 주문 승인이 아니다.

## 8. Dashboard 확인

브라우저:

- `http://127.0.0.1:8000/`
- `http://127.0.0.1:8000/dashboard`

API:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/dashboard/status
Invoke-RestMethod http://127.0.0.1:8000/api/dashboard/snapshot
Invoke-RestMethod http://127.0.0.1:8000/api/dashboard/funnel
Invoke-RestMethod http://127.0.0.1:8000/api/dashboard/errors
```

Dashboard는 읽기 전용이며 실행 버튼이 없다.

## 9. DRY_RUN 상태 확인

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/dry-run/status
Invoke-RestMethod http://127.0.0.1:8000/api/dry-run/eligibility
Invoke-RestMethod http://127.0.0.1:8000/api/dry-run/positions
```

DRY_RUN은 내부 모의 회계이며 브로커 주문이 아니다.

## 10. DRY_RUN Exit 상태 확인

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/dry-run/exits/status
Invoke-RestMethod http://127.0.0.1:8000/api/dry-run/exits/evaluations
Invoke-RestMethod http://127.0.0.1:8000/api/dry-run/exits/signals
```

Exit signal은 실제 매도 주문이 아니다.

## 11. LIVE_SIM status 확인

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/live-sim/status
Invoke-RestMethod http://127.0.0.1:8000/api/live-sim/rejections
Invoke-RestMethod http://127.0.0.1:8000/api/live-sim/reconcile
```

LIVE_SIM은 모의투자 전용이며 실계좌 주문이 아니다. 기본값은 disabled다.

## 12. AI/RCA/Codex 표시 확인

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/ai-sidecar/context/status
Invoke-RestMethod http://127.0.0.1:8000/api/ai-sidecar/execution/status
Invoke-RestMethod http://127.0.0.1:8000/api/dashboard/ai-explanations/status
```

AI/RCA/Codex 결과는 review-only이며 Strategy/Risk/OMS 자동 입력이 아니다.

## 13. Local checks

```powershell
python -m pytest
python -m ruff check .
```

## 운영자 체크포인트

- 이 runbook은 관찰 파이프라인 확인 절차다.
- 실제 주문이 나가지 않는다.
- Dashboard에 실행 버튼이 없는 것이 정상이다.
- `LIVE_REAL`은 현재 구현되어 있지 않다.
