# FINAL EDGE 2 V1.1.2 — KIS Empty-Watchlist Connection Fix

- WATCHLIST가 0개여도 KIS bridge worker 시작
- WebSocket approval key 먼저 발급
- KIS WebSocket 연결 먼저 성립
- 감시종목 추가 시 체결/호가 구독
- 자동주문 계속 비활성화
- App Key/App Secret은 Render 환경변수에서만 읽고 로그/API에 출력하지 않음

배포 후 기대 상태:
- configured=true
- approval_ready=true
- connected=true
- watchlist가 비어 있으면 subscriptions/tick/quote가 0인 것은 정상
