from __future__ import annotations

import requests
from typing import Dict, List

URL = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
REFERER = "https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC02021301"

def _pick(row: Dict, *keys: str) -> str:
    for k in keys:
        v = row.get(k)
        if v not in (None, ""):
            return str(v).strip()
    return ""

def _find_rows(payload) -> List[Dict]:
    if isinstance(payload, dict):
        for key in ("OutBlock_1", "output", "outBlock1", "OutBlock1"):
            v = payload.get(key)
            if isinstance(v, list):
                return [x for x in v if isinstance(x, dict)]
        for v in payload.values():
            if isinstance(v, list) and (not v or isinstance(v[0], dict)):
                return [x for x in v if isinstance(x, dict)]
    return []

def fetch_new_listings(start_date: str, end_date: str) -> List[Dict]:
    # KRX MDCSTAT20001 JSON defaults, matching the live screen contract.
    data = {
        "bld": "dbms/MDC/STAT/issue/MDCSTAT20001",
        "locale": "ko_KR",
        "mktId": "ALL",
        "isurCd": "ALL",
        "isurCd2": "ALL",
        "listClssCd": "ALL",
        "secugrpTp": "ALL",
        "cntrIsoCd": "ALL",
        "strtDd": start_date,
        "endDd": end_date,
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Referer": REFERER,
        "Origin": "https://data.krx.co.kr",
        "X-Requested-With": "XMLHttpRequest",
    }

    r = requests.post(URL, headers=headers, data=data, timeout=12)
    if not r.ok:
        preview = (r.text or "").strip().replace("\n", " ")[:300]
        raise RuntimeError(f"KRX_HTTP_{r.status_code}:{preview}")

    payload = r.json()
    rows = _find_rows(payload)

    out: List[Dict] = []
    for row in rows:
        ticker = _pick(row, "ISU_SRT_CD", "종목코드", "SHORT_CODE")
        if len(ticker) != 6 or not ticker.isdigit():
            ticker = ""

        out.append({
            "ticker": ticker,
            "name": _pick(row, "ISU_ABBRV", "ISU_NM", "종목명", "회사명"),
            "market": _pick(row, "MKT_NM", "MKT_TP_NM", "시장구분"),
            "listing_date": _pick(row, "LIST_DD", "LIST_DD_NM", "상장일"),
            "listing_type": _pick(row, "LIST_TP_NM", "상장유형"),
            "security_type": _pick(row, "SECUGRP_NM", "증권구분"),
        })
    return out
