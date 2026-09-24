# FINAL EDGE 2 V1.0

신규상장·SPAC 실시간 분석 전용 엔진의 첫 독립 버전입니다.

## V1.0 포함 기능
- EDGE 1과 완전 분리된 Flask 앱
- provider-neutral 실시간 체결 ingest bridge
- 종목별 메모리 상태 유지
- 1분봉 실시간 생성
- 장중 시가/고가/저가/거래량/거래대금 집계
- V_REBOUND / BREAKOUT / CRASH 초기 신호 엔진
- watchlist API
- 실시간 상태 UI (1초 갱신)
- 주문 기능 없음 (`ORDERS_ENABLED=False`)

## 환경변수
- `EDGE2_INGEST_TOKEN`: 외부 실시간 bridge가 체결을 넣을 때 사용하는 토큰
- `EDGE2_MAX_WATCHLIST`: 동시 감시 종목 수, 기본 40
- `EDGE2_EVENT_RETENTION`: 종목당 최근 체결 보존 수, 기본 5000
- `EDGE2_BOOTSTRAP`: 선택적 초기 watchlist

예시:
`005930:삼성전자:KOSPI:WATCH,123456:신규종목:KOSDAQ:NEW_LISTING`

## API
- `GET /health`
- `GET /api/state`
- `GET|POST|DELETE /api/watchlist`
- `POST /api/ingest/tick`
- `POST /api/ingest/ticks`
- `GET /api/bars/<ticker>`
- `GET /api/signals`

## 중요한 설계 원칙
실제 증권사 WebSocket 연결부는 V1.1 이후 별도 bridge 모듈로 붙입니다. V1.0 엔진에는 주문/자동매매 기능이 없습니다.
