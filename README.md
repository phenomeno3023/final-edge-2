# FINAL EDGE 2 V1.1.7 — KRX MDCSTAT20001 Fix

수정 내용
- KRX 신규상장 조회 bld 경로를 `dbms/MDC/STAT/issue/MDCSTAT20001`로 수정
- JSON 방식에 맞는 실제 기본 파라미터 적용:
  - mktId=ALL
  - isurCd=ALL
  - isurCd2=ALL
  - listClssCd=ALL
  - secugrpTp=ALL
  - cntrIsoCd=ALL
  - strtDd / endDd
- 신규상장 화면의 실제 menuId `MDC02021301` 기준 Referer 적용
- JSON 요청용 Accept / X-Requested-With 헤더 추가
- KRX 400 응답 시 본문 일부를 `last_error`에 기록
- 기존 KIS WebSocket, ACK, 안전 로그, 자동주문 OFF 유지

배포 후 확인
1. FINAL EDGE 2에서 VERSION 1.1.7
2. `/api/krx/status`
3. 기대:
   - last_error=""
   - last_ok_ts > 0
   - fetched >= 0
   - 오늘 휴장/신규상장 없음이면 today_targets=0은 정상
