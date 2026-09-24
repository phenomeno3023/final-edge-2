# FINAL EDGE 2 V1.1.5 — Safe Logs + Subscription Verification

핵심 수정
- KIS 승인키 응답 로그에서 `approval_key` 완전 마스킹
- credential/token 계열 키를 재귀적으로 `***REDACTED***` 처리
- credential endpoint의 비-JSON 응답 본문은 로그에 출력하지 않음
- WebSocket 연결 성공/해제 로그 추가
- 종목별 체결+호가 구독 요청 로그 추가
- 구독 ACK 성공/실패 상태 기록
- `/api/kis/status`에 다음 안전한 진단값 추가:
  - ws_connected_at
  - subscription_requests
  - subscription_acks
  - last_subscribed_ticker
  - last_subscription_ack
- App Key / App Secret / approval_key 실제 값은 로그/API에 출력하지 않음
- 자동주문 없음

현재 테스트용 `EDGE2_BOOTSTRAP`을 유지하면 삼성전자(005930) 구독 검증에 사용할 수 있습니다.

배포 후 기대값
- VERSION 1.1.5
- KIS CONNECTED
- WATCH 1
- KIS SUBS 1
- `/api/kis/status`에서:
  - approval_ready=true
  - connected=true
  - bridge_thread_alive=true
  - subscriptions=1
  - subscription_requests=2
  - subscription_acks가 1 이상이면 KIS 구독 ACK 확인
