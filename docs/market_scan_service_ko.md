# Market Scan Service 운영 메모

PR-1 범위의 `market_scan_service`는 전 시장 저빈도 TR 스캔을 수집하는 observe-only 서비스다.

- 기본값은 `MARKET_SCAN_ENABLED=false`다.
- 실행 함수는 `run_market_scan_once(connection, settings=..., queue_commands=...)`다.
- 생성하는 GatewayCommand는 `request_tr`뿐이다.
- 기본 TR은 `TRADE_VALUE=OPT10032`, `CHANGE_RATE=OPT10027`이며 `MARKET_SCAN_TR_CODES`로 바꿀 수 있다.
- 기본 시장은 `KOSPI,KOSDAQ`이고 시장 입력값은 `MARKET_SCAN_MARKET_CODES`로 분리되어 있다.
- TR 필드 매핑은 검증 전 상태이므로 row metadata에 `parser_status=PILOT_UNVERIFIED`를 남긴다.

응답 처리는 `process_market_scan_event`가 담당한다. 정상 row는 `market_scan_snapshots`와 `market_scan_latest`에 저장되고, 실패 row는 `market_scan_errors.reason_code`에 대문자 스네이크 케이스로 기록된다.

마이그레이션은 다음 명령으로 수행한다.

```powershell
python tools/migrate_market_scan_theme_flow.py
```

주문 관련 커맨드(`send_order`, `cancel_order`, `modify_order`)는 이 경로에서 만들지 않는다.
