# Market Scan Service 운영 메모

PR-1 범위의 `market_scan_service`는 전 시장 저빈도 TR 스캔을 수집하는 observe-only 서비스다.

- 기본값은 `MARKET_SCAN_ENABLED=false`다.
- 실행 함수는 `run_market_scan_once(connection, settings=..., queue_commands=...)`다.
- 생성하는 GatewayCommand는 `request_tr`뿐이다.
- 기본 TR은 `TRADE_VALUE=OPT10032`, `CHANGE_RATE=OPT10027`이며 `MARKET_SCAN_TR_CODES`로 바꿀 수 있다.
- 기본 시장은 `KOSPI,KOSDAQ`이고 시장 입력값은 `MARKET_SCAN_MARKET_CODES`로 분리되어 있다.
- `OPT10032`는 `거래대금상위` multi output과 `종목코드/종목명/현재순위/현재가/등락률/거래대금/현재거래량`을 사용한다.
- `OPT10027`은 `전일대비등락률상위` multi output과 `종목코드/종목명/현재가/등락률/현재거래량`을 사용한다.
- 2026-07-11 설치된 KOA contract와 Kiwoom 모의투자 실제 callback을 대조했다. KOSPI/KOSDAQ 각각 `OPT10032=100행`, `OPT10027=200행`이 같은 필드로 반환됐다.
- 전역 기본값은 여전히 `MARKET_SCAN_PARSER_STATUS=PILOT_UNVERIFIED`다. 해당 설치본의 KOA 계약을 확인한 검증 runtime에서만 `KOA_STUDIO_VERIFIED`를 명시한다.

broker input은 서비스 metadata가 아니라 KOA 입력 이름만 전달한다.

- `OPT10032`: `시장구분`, `관리종목포함=0`, `거래소구분=1`
- `OPT10027`: `시장구분`, `정렬구분=1`, `거래량조건=0`, `종목조건=1`, `신용조건=0`, `상하한포함=0`, `가격조건=0`, `거래대금조건=0`, `거래소구분=1`

실제 첫 페이지는 모두 `continuation_key=2`였다. 현재 market-scan은 자동 연속조회와 페이지 병합을 하지 않으므로 `MARKET_SCAN_TOP_N`을 자동 pagination 보장으로 해석하지 않는다. 첫 페이지보다 넓은 universe가 필요하면 별도 pagination PR에서 page lineage와 rank를 함께 설계한다.

응답 처리는 `process_market_scan_event`가 담당한다. 정상 row는 `market_scan_snapshots`와 `market_scan_latest`에 저장되고, 실패 row는 `market_scan_errors.reason_code`에 대문자 스네이크 케이스로 기록된다.

마이그레이션은 다음 명령으로 수행한다.

```powershell
python tools/migrate_market_scan_theme_flow.py
```

주문 관련 커맨드(`send_order`, `cancel_order`, `modify_order`)는 이 경로에서 만들지 않는다.

실제 callback과 projection/cutover 근거는 `reports/market_scan_tr_contract/20260710T231634Z/summary.md`와 `reports/market_scan_projection/20260710T231634Z/summary.md`에 있다.
