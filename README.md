# FINAL EDGE 2 V1.1.3 — KIS Approval Diagnostics

이번 버전은 KIS WebSocket 승인키 발급 실패 원인 확인용입니다.

추가:
- 승인키 요청 예외를 `last_error`에 기록
- HTTP 상태코드 및 KIS 응답 일부 기록
- JSON 파싱 오류와 `approval_key` 누락 응답 기록
- `/api/kis/status`에 실제 사용 중인 approval_url / ws_url 표시
- App Key / App Secret 값 자체는 로그/API에 출력하지 않음
- 자동주문 없음

배포 후:
`https://final-edge-2.onrender.com/api/kis/status`

핵심 확인:
- approval_ready
- connected
- last_error
- bridge_thread_alive
- approval_url
- ws_url
