from __future__ import annotations

import os
import time
import json
import threading
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone, timedelta
from collections import defaultdict, deque
from typing import Deque, Dict, List, Optional, Tuple

from flask import Flask, jsonify, request, render_template_string

KST = timezone(timedelta(hours=9))
APP_VERSION = "1.0.0"
APP_NAME = "FINAL EDGE 2"
ORDERS_ENABLED = False

app = Flask(__name__)

# -----------------------------
# Configuration
# -----------------------------
INGEST_TOKEN = os.getenv("EDGE2_INGEST_TOKEN", "").strip()
MAX_WATCHLIST = max(1, int(os.getenv("EDGE2_MAX_WATCHLIST", "40") or 40))
EVENT_RETENTION = max(200, int(os.getenv("EDGE2_EVENT_RETENTION", "5000") or 5000))

# -----------------------------
# In-memory real-time state
# -----------------------------
STATE_LOCK = threading.RLock()
STARTED_AT = time.time()


@dataclass
class Tick:
    ticker: str
    price: float
    volume: float
    ts: float
    side: str = ""
    source: str = "bridge"


@dataclass
class MinuteBar:
    minute: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    turnover: float


@dataclass
class Signal:
    ticker: str
    kind: str
    strength: int
    price: float
    ts: float
    reason: str


@dataclass
class InstrumentState:
    ticker: str
    name: str = ""
    market: str = ""
    category: str = "NEW_LISTING"  # NEW_LISTING / SPAC / WATCH
    enabled: bool = True
    last_price: float = 0.0
    last_ts: float = 0.0
    session_open: float = 0.0
    session_high: float = 0.0
    session_low: float = 0.0
    cumulative_volume: float = 0.0
    cumulative_turnover: float = 0.0
    tick_count: int = 0
    last_signal: str = ""
    last_signal_ts: float = 0.0
    bars: Deque[MinuteBar] = field(default_factory=lambda: deque(maxlen=240))
    recent_ticks: Deque[Tick] = field(default_factory=lambda: deque(maxlen=EVENT_RETENTION))


WATCHLIST: Dict[str, InstrumentState] = {}
SIGNALS: Deque[Signal] = deque(maxlen=500)
ENGINE_STATS = {
    "ticks_received": 0,
    "ticks_rejected": 0,
    "signals_emitted": 0,
    "last_event_ts": 0.0,
}


def now_kst() -> datetime:
    return datetime.now(KST)


def _minute_key(ts: float) -> str:
    return datetime.fromtimestamp(ts, KST).strftime("%Y-%m-%d %H:%M")


def _ensure_bar(s: InstrumentState, tick: Tick) -> MinuteBar:
    key = _minute_key(tick.ts)
    turnover = tick.price * tick.volume
    if not s.bars or s.bars[-1].minute != key:
        bar = MinuteBar(
            minute=key,
            open=tick.price,
            high=tick.price,
            low=tick.price,
            close=tick.price,
            volume=tick.volume,
            turnover=turnover,
        )
        s.bars.append(bar)
        return bar

    b = s.bars[-1]
    b.high = max(b.high, tick.price)
    b.low = min(b.low, tick.price)
    b.close = tick.price
    b.volume += tick.volume
    b.turnover += turnover
    return b


def _emit_signal(s: InstrumentState, kind: str, strength: int, reason: str, ts: float) -> None:
    # 20-second duplicate suppression for same signal type on same ticker.
    if s.last_signal == kind and ts - s.last_signal_ts < 20:
        return
    sig = Signal(
        ticker=s.ticker,
        kind=kind,
        strength=max(1, min(100, int(strength))),
        price=s.last_price,
        ts=ts,
        reason=reason,
    )
    SIGNALS.appendleft(sig)
    s.last_signal = kind
    s.last_signal_ts = ts
    ENGINE_STATS["signals_emitted"] += 1


def _evaluate_signals(s: InstrumentState, tick: Tick) -> None:
    """V1.0 signal engine: deterministic, read-only, no order execution.

    Initial detectors intentionally use only live price/volume state:
      - V_REBOUND: rebound from session low + short-term confirmation
      - BREAKOUT: new session high with recent turnover support
      - CRASH: sharp drop from session high

    Thresholds are starter defaults and should be calibrated with real sessions.
    """
    if s.session_open <= 0 or s.last_price <= 0:
        return

    recent = list(s.recent_ticks)[-50:]
    if len(recent) < 5:
        return

    low = s.session_low or s.last_price
    high = s.session_high or s.last_price
    rebound_pct = ((s.last_price / low) - 1.0) * 100 if low > 0 else 0.0
    drawdown_pct = ((s.last_price / high) - 1.0) * 100 if high > 0 else 0.0

    # Short-term confirmation: last price above average of recent tick prices.
    avg_recent = sum(t.price for t in recent) / len(recent)
    recent_turnover = sum(t.price * t.volume for t in recent)

    if rebound_pct >= 2.0 and s.last_price >= avg_recent * 1.003:
        strength = min(100, int(55 + rebound_pct * 6))
        _emit_signal(
            s,
            "V_REBOUND",
            strength,
            f"장중저점 대비 +{rebound_pct:.2f}% 반등, 최근 체결평균 상회",
            tick.ts,
        )

    # New high / breakout. Need at least meaningful recent turnover to avoid one-tick noise.
    if s.last_price >= high and recent_turnover >= 50_000_000:
        breakout_pct = ((s.last_price / s.session_open) - 1.0) * 100
        strength = min(100, int(58 + max(0.0, breakout_pct) * 3))
        _emit_signal(
            s,
            "BREAKOUT",
            strength,
            f"장중고점 갱신, 최근 체결거래대금 {recent_turnover/100_000_000:.2f}억",
            tick.ts,
        )

    if drawdown_pct <= -5.0:
        strength = min(100, int(60 + abs(drawdown_pct) * 4))
        _emit_signal(
            s,
            "CRASH",
            strength,
            f"장중고점 대비 {drawdown_pct:.2f}% 급락",
            tick.ts,
        )


def process_tick(tick: Tick) -> Tuple[bool, str]:
    with STATE_LOCK:
        s = WATCHLIST.get(tick.ticker)
        if not s or not s.enabled:
            ENGINE_STATS["ticks_rejected"] += 1
            return False, "ticker_not_watched"
        if tick.price <= 0 or tick.volume < 0:
            ENGINE_STATS["ticks_rejected"] += 1
            return False, "invalid_tick"

        if s.tick_count == 0:
            s.session_open = tick.price
            s.session_high = tick.price
            s.session_low = tick.price
        else:
            s.session_high = max(s.session_high, tick.price)
            s.session_low = min(s.session_low, tick.price)

        s.last_price = tick.price
        s.last_ts = tick.ts
        s.cumulative_volume += tick.volume
        s.cumulative_turnover += tick.price * tick.volume
        s.tick_count += 1
        s.recent_ticks.append(tick)
        _ensure_bar(s, tick)
        _evaluate_signals(s, tick)

        ENGINE_STATS["ticks_received"] += 1
        ENGINE_STATS["last_event_ts"] = tick.ts
        return True, "ok"


def state_summary(s: InstrumentState) -> dict:
    chg = ((s.last_price / s.session_open) - 1.0) * 100 if s.session_open else 0.0
    from_low = ((s.last_price / s.session_low) - 1.0) * 100 if s.session_low else 0.0
    from_high = ((s.last_price / s.session_high) - 1.0) * 100 if s.session_high else 0.0
    return {
        "ticker": s.ticker,
        "name": s.name,
        "market": s.market,
        "category": s.category,
        "enabled": s.enabled,
        "last_price": s.last_price,
        "session_open": s.session_open,
        "session_high": s.session_high,
        "session_low": s.session_low,
        "change_pct": round(chg, 3),
        "from_low_pct": round(from_low, 3),
        "from_high_pct": round(from_high, 3),
        "cumulative_volume": s.cumulative_volume,
        "cumulative_turnover": s.cumulative_turnover,
        "tick_count": s.tick_count,
        "bar_count": len(s.bars),
        "last_signal": s.last_signal,
        "last_signal_ts": s.last_signal_ts,
        "last_ts": s.last_ts,
    }


# -----------------------------
# API
# -----------------------------
@app.get("/health")
def health():
    return jsonify({
        "ok": True,
        "app": APP_NAME,
        "version": APP_VERSION,
        "orders_enabled": ORDERS_ENABLED,
        "uptime_sec": round(time.time() - STARTED_AT, 1),
        "watchlist_count": len(WATCHLIST),
    })


@app.get("/api/state")
def api_state():
    with STATE_LOCK:
        return jsonify({
            "ok": True,
            "app": APP_NAME,
            "version": APP_VERSION,
            "now": now_kst().isoformat(),
            "orders_enabled": ORDERS_ENABLED,
            "watchlist": [state_summary(s) for s in WATCHLIST.values()],
            "signals": [asdict(x) for x in list(SIGNALS)[:50]],
            "engine": dict(ENGINE_STATS),
            "bridge": {
                "configured": bool(INGEST_TOKEN),
                "mode": "provider-neutral ingest bridge",
                "max_watchlist": MAX_WATCHLIST,
            },
        })


@app.route("/api/watchlist", methods=["GET", "POST", "DELETE"])
def api_watchlist():
    if request.method == "GET":
        with STATE_LOCK:
            return jsonify({"ok": True, "items": [state_summary(s) for s in WATCHLIST.values()]})

    data = request.get_json(silent=True) or {}
    ticker = str(data.get("ticker", "")).strip().upper()
    if not ticker:
        return jsonify({"ok": False, "error": "ticker_required"}), 400

    with STATE_LOCK:
        if request.method == "DELETE":
            existed = WATCHLIST.pop(ticker, None)
            return jsonify({"ok": True, "deleted": bool(existed), "ticker": ticker})

        if ticker not in WATCHLIST and len(WATCHLIST) >= MAX_WATCHLIST:
            return jsonify({"ok": False, "error": "watchlist_limit", "limit": MAX_WATCHLIST}), 400
        s = WATCHLIST.get(ticker) or InstrumentState(ticker=ticker)
        s.name = str(data.get("name", s.name or ""))[:80]
        s.market = str(data.get("market", s.market or ""))[:20]
        s.category = str(data.get("category", s.category or "WATCH"))[:30].upper()
        s.enabled = bool(data.get("enabled", True))
        WATCHLIST[ticker] = s
        return jsonify({"ok": True, "item": state_summary(s)})


def _authorized() -> bool:
    if not INGEST_TOKEN:
        return False
    token = request.headers.get("X-EDGE2-TOKEN", "") or request.args.get("token", "")
    return token == INGEST_TOKEN


@app.post("/api/ingest/tick")
def ingest_tick():
    if not _authorized():
        return jsonify({"ok": False, "error": "unauthorized_or_bridge_not_configured"}), 401
    d = request.get_json(silent=True) or {}
    try:
        tick = Tick(
            ticker=str(d["ticker"]).strip().upper(),
            price=float(d["price"]),
            volume=float(d.get("volume", 0)),
            ts=float(d.get("ts", time.time())),
            side=str(d.get("side", ""))[:12],
            source=str(d.get("source", "bridge"))[:30],
        )
    except Exception as e:
        return jsonify({"ok": False, "error": "bad_tick", "detail": str(e)}), 400
    ok, msg = process_tick(tick)
    return jsonify({"ok": ok, "status": msg})


@app.post("/api/ingest/ticks")
def ingest_ticks():
    if not _authorized():
        return jsonify({"ok": False, "error": "unauthorized_or_bridge_not_configured"}), 401
    body = request.get_json(silent=True) or {}
    rows = body.get("ticks", [])
    if not isinstance(rows, list):
        return jsonify({"ok": False, "error": "ticks_must_be_list"}), 400
    accepted, rejected = 0, 0
    errors: List[str] = []
    for i, d in enumerate(rows[:2000]):
        try:
            tick = Tick(
                ticker=str(d["ticker"]).strip().upper(),
                price=float(d["price"]),
                volume=float(d.get("volume", 0)),
                ts=float(d.get("ts", time.time())),
                side=str(d.get("side", ""))[:12],
                source=str(d.get("source", "bridge"))[:30],
            )
            ok, msg = process_tick(tick)
            if ok:
                accepted += 1
            else:
                rejected += 1
                if len(errors) < 20:
                    errors.append(f"{i}:{msg}")
        except Exception as e:
            rejected += 1
            if len(errors) < 20:
                errors.append(f"{i}:{e}")
    return jsonify({"ok": True, "accepted": accepted, "rejected": rejected, "errors": errors})


@app.get("/api/bars/<ticker>")
def api_bars(ticker: str):
    ticker = ticker.strip().upper()
    with STATE_LOCK:
        s = WATCHLIST.get(ticker)
        if not s:
            return jsonify({"ok": False, "error": "not_found"}), 404
        return jsonify({"ok": True, "ticker": ticker, "bars": [asdict(b) for b in s.bars]})


@app.get("/api/signals")
def api_signals():
    with STATE_LOCK:
        return jsonify({"ok": True, "items": [asdict(x) for x in list(SIGNALS)[:200]]})


HTML = r"""
<!doctype html>
<html lang="ko">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width,initial-scale=1" />
<title>FINAL EDGE 2</title>
<style>
body{margin:0;background:#07111f;color:#dfe9f5;font-family:Arial,"Noto Sans KR",sans-serif}
.wrap{max-width:1180px;margin:24px auto;padding:0 18px}
.hero{background:#0c2038;border:1px solid #24435f;border-radius:16px;padding:20px}
h1{margin:0;color:#f2c14e;letter-spacing:.5px}.sub{opacity:.8;margin-top:6px}
.badges{display:flex;gap:8px;flex-wrap:wrap;margin-top:14px}.badge{padding:7px 10px;border-radius:999px;background:#102c4c;border:1px solid #2b5275;font-size:13px}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-top:14px}.card{background:#0a1a2d;border:1px solid #1b3854;border-radius:14px;padding:16px}
.card h3{margin-top:0;color:#f2c14e}.mono{font-family:Consolas,monospace;font-size:13px;white-space:pre-wrap}.muted{opacity:.65}.good{color:#65d6a6}.warn{color:#ffcc66}
table{width:100%;border-collapse:collapse;font-size:13px}th,td{padding:8px;border-bottom:1px solid #1b3854;text-align:left}th{color:#9fc4e3}
@media(max-width:800px){.grid{grid-template-columns:1fr}}
</style>
</head>
<body><div class="wrap">
<div class="hero"><h1>FINAL EDGE 2</h1><div class="sub">신규상장 · SPAC 실시간 분석 엔진 V1.0</div>
<div class="badges"><div class="badge">실시간 이벤트 처리</div><div class="badge">1분봉 메모리 생성</div><div class="badge">V반전 / 돌파 / 급락 탐지</div><div class="badge">자동주문 없음</div></div></div>
<div class="grid">
<div class="card"><h3>ENGINE</h3><div id="engine" class="mono">loading...</div></div>
<div class="card"><h3>LIVE SIGNALS</h3><div id="signals" class="mono">loading...</div></div>
</div>
<div class="card" style="margin-top:14px"><h3>WATCHLIST</h3><table><thead><tr><th>종목</th><th>구분</th><th>현재가</th><th>등락</th><th>저점대비</th><th>고점대비</th><th>체결</th><th>최근신호</th></tr></thead><tbody id="rows"></tbody></table></div>
<div class="card" style="margin-top:14px"><div class="muted">V1.0은 provider-neutral bridge 구조입니다. 실제 증권사 WebSocket 연결은 별도 bridge 모듈로 붙이며, 이 화면/엔진에는 주문 기능이 없습니다.</div></div>
</div>
<script>
function n(v,d=2){return Number(v||0).toLocaleString(undefined,{maximumFractionDigits:d})}
async function refresh(){
 const r=await fetch('/api/state',{cache:'no-store'}); const d=await r.json();
 document.getElementById('engine').textContent=`VERSION ${d.version}\nWATCH ${d.watchlist.length}\nTICKS ${d.engine.ticks_received}\nSIGNALS ${d.engine.signals_emitted}\nBRIDGE ${d.bridge.configured?'READY':'TOKEN NOT SET'}\nORDERS ${d.orders_enabled?'ON':'OFF'}`;
 document.getElementById('signals').innerHTML=(d.signals.slice(0,8).map(s=>`${s.ticker} · ${s.kind} · ${s.strength}\n${s.reason}`).join('\n\n')||'<span class="muted">신호 대기</span>');
 document.getElementById('rows').innerHTML=d.watchlist.map(x=>`<tr><td>${x.name||x.ticker}<br><span class="muted">${x.ticker}</span></td><td>${x.category}</td><td>${n(x.last_price,0)}</td><td>${n(x.change_pct)}%</td><td>${n(x.from_low_pct)}%</td><td>${n(x.from_high_pct)}%</td><td>${n(x.tick_count,0)}</td><td>${x.last_signal||'-'}</td></tr>`).join('');
}
refresh();setInterval(refresh,1000);
</script></body></html>
"""


@app.get("/")
def home():
    return render_template_string(HTML)


# Optional bootstrap watchlist through environment variable:
# EDGE2_BOOTSTRAP="005930:삼성전자:KOSPI:WATCH,123456:예시:KOSDAQ:NEW_LISTING"
def bootstrap_watchlist() -> None:
    raw = os.getenv("EDGE2_BOOTSTRAP", "").strip()
    if not raw:
        return
    with STATE_LOCK:
        for chunk in raw.split(","):
            parts = [x.strip() for x in chunk.split(":")]
            if not parts or not parts[0]:
                continue
            ticker = parts[0].upper()
            if len(WATCHLIST) >= MAX_WATCHLIST:
                break
            WATCHLIST[ticker] = InstrumentState(
                ticker=ticker,
                name=parts[1] if len(parts) > 1 else "",
                market=parts[2] if len(parts) > 2 else "",
                category=(parts[3] if len(parts) > 3 else "WATCH").upper(),
            )


bootstrap_watchlist()

if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port, threaded=True)
