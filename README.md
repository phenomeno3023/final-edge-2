# FINAL EDGE 2 V1.1.1 — KIS Connection Diagnostics

V1.1.0 기능은 유지하고 KIS 연결 진단을 강화한 버전입니다.

추가:
- Render 로그에 KIS bridge 시작 여부 출력
- WebSocket approval key 요청/HTTP 상태/성공 여부 출력
- WebSocket 접속 성공/오류/5초 재시도 출력
- `/api/kis/status` 진단 endpoint 추가
- App Key / App Secret 값 자체는 로그/API에 절대 출력하지 않음
- 주문 기능 없음 (`ORDERS_ENABLED = False`)

배포 후 확인:
1. Render Logs에서 `[EDGE2][KIS]` 검색
2. 브라우저에서 `/api/kis/status` 확인
3. `last_error`가 있으면 그 오류만 기준으로 다음 수정 진행
