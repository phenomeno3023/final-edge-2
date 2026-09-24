# FINAL EDGE 2 V1.1.0 — KIS Real-Time Bridge

- KIS 실전 WebSocket 연결
- 국내주식 KRX 실시간 체결 H0STCNT0
- 국내주식 KRX 실시간 호가 H0STASP0
- 기존 1분봉 / V반전 / 돌파 / 급락 엔진 연결
- 주문 API 없음, ORDERS_ENABLED=False 유지

## Render 필수 환경변수
- KIS_APP_KEY
- KIS_APP_SECRET

선택: KIS_ENABLED=1, EDGE2_BOOTSTRAP=005930:삼성전자:KOSPI:WATCH

키는 GitHub 코드에 직접 입력하지 마세요.

## 확인
/health 또는 /api/state 에서 kis.connected, kis.approval_ready 확인.
