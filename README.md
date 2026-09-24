# FINAL EDGE 2 V1.1.6 — KRX Auto Targets

- KRX 신규상장종목 현황(20001) 자동 조회
- 당일 상장 종목만 WATCHLIST 자동 등록
- 종목명에 스팩/SPAC이 있으면 SPAC, 나머지는 NEW_LISTING
- KIS 실시간 체결+호가 구독과 자동 연동
- 기존 EDGE2_BOOTSTRAP 병행 가능
- 자동주문 없음

환경변수:
- EDGE2_KRX_AUTO_TARGETS=1 (기본)
- EDGE2_KRX_SYNC_INTERVAL_SEC=300 (기본, 최소 60초)

상태 확인:
- /api/krx/status
- POST /api/krx/sync
