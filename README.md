# FINAL EDGE 2 V1.1.4 — Render/Gunicorn Worker Fix

원인
- 이전 버전은 모듈 import 시 KIS background thread를 시작했습니다.
- Render/Gunicorn 배포 과정에서는 앱이 실제 서비스 worker가 아닌 임시/부모 process에서 먼저 import될 수 있습니다.
- 그 process가 종료되면 KIS thread도 같이 사라져 `/api/kis/status`에서 `bridge_thread_alive=false`가 남았습니다.

수정
- 모듈 import 시 KIS thread 시작 제거
- 실제 HTTP 요청을 처리하는 worker에서 `ensure_kis_bridge()` 실행
- PID가 바뀌었거나 thread가 죽었으면 자동 재시작
- `/api/kis/status`에 `process_pid`, `bridge_pid`, `bridge_started_at` 추가
- KIS 승인키/HTTP 진단 기능은 V1.1.3 그대로 유지
- App Key / App Secret 실제 값은 로그/API에 출력하지 않음
- 자동주문 없음

배포 후 확인
1. FINAL EDGE 2를 새로고침
2. 3~5초 대기
3. `https://final-edge-2.onrender.com/api/kis/status`
4. 기대:
   - configured=true
   - bridge_thread_alive=true
   - process_pid == bridge_pid
   - approval_ready=true (승인키 성공 시)
   - connected=true (WebSocket 성공 시)
