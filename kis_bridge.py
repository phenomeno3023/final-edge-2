from __future__ import annotations
import asyncio, json, threading, time
from typing import Callable, Dict, Iterable, Optional
import requests, websockets
TR_TICK="H0STCNT0"
TR_QUOTE="H0STASP0"
TICK_FIELDS=["MKSC_SHRN_ISCD","STCK_CNTG_HOUR","STCK_PRPR","PRDY_VRSS_SIGN","PRDY_VRSS","PRDY_CTRT","WGHN_AVRG_STCK_PRC","STCK_OPRC","STCK_HGPR","STCK_LWPR","ASKP1","BIDP1","CNTG_VOL","ACML_VOL","ACML_TR_PBMN","SELN_CNTG_CSNU","SHNU_CNTG_CSNU","NTBY_CNTG_CSNU","CTTR","SELN_CNTG_SMTN","SHNU_CNTG_SMTN","CCLD_DVSN","SHNU_RATE","PRDY_VOL_VRSS_ACML_VOL_RATE","OPRC_HOUR","OPRC_VRSS_PRPR_SIGN","OPRC_VRSS_PRPR","HGPR_HOUR","HGPR_VRSS_PRPR_SIGN","HGPR_VRSS_PRPR","LWPR_HOUR","LWPR_VRSS_PRPR_SIGN","LWPR_VRSS_PRPR","BSOP_DATE","NEW_MKOP_CLS_CODE","TRHT_YN","ASKP_RSQN1","BIDP_RSQN1","TOTAL_ASKP_RSQN","TOTAL_BIDP_RSQN","VOL_TNRT","PRDY_SMNS_HOUR_ACML_VOL","PRDY_SMNS_HOUR_ACML_VOL_RATE","HOUR_CLS_CODE","MRKT_TRTM_CLS_CODE","VI_STND_PRC"]
QUOTE_FIELDS=["MKSC_SHRN_ISCD","BSOP_HOUR","HOUR_CLS_CODE","ASKP1","ASKP2","ASKP3","ASKP4","ASKP5","ASKP6","ASKP7","ASKP8","ASKP9","ASKP10","BIDP1","BIDP2","BIDP3","BIDP4","BIDP5","BIDP6","BIDP7","BIDP8","BIDP9","BIDP10","ASKP_RSQN1","ASKP_RSQN2","ASKP_RSQN3","ASKP_RSQN4","ASKP_RSQN5","ASKP_RSQN6","ASKP_RSQN7","ASKP_RSQN8","ASKP_RSQN9","ASKP_RSQN10","BIDP_RSQN1","BIDP_RSQN2","BIDP_RSQN3","BIDP_RSQN4","BIDP_RSQN5","BIDP_RSQN6","BIDP_RSQN7","BIDP_RSQN8","BIDP_RSQN9","BIDP_RSQN10","TOTAL_ASKP_RSQN","TOTAL_BIDP_RSQN","OVTM_TOTAL_ASKP_RSQN","OVTM_TOTAL_BIDP_RSQN","ANTC_CNPR","ANTC_CNQN","ANTC_VOL","ANTC_CNTG_VRSS","ANTC_CNTG_VRSS_SIGN","ANTC_CNTG_PRDY_CTRT","ACML_VOL","TOTAL_ASKP_RSQN_ICDC","TOTAL_BIDP_RSQN_ICDC","OVTM_TOTAL_ASKP_ICDC","OVTM_TOTAL_BIDP_ICDC","STCK_DEAL_CLS_CODE"]

def _num(v,d=0.0):
    try:return float(str(v).replace(",","").strip())
    except:return d

_SENSITIVE_KEYS = {
    "approval_key", "appkey", "app_key", "secretkey", "appsecret",
    "app_secret", "access_token", "token", "authorization"
}

def _sanitize(value):
    """Return a log-safe copy with credentials/tokens redacted."""
    if isinstance(value, dict):
        out = {}
        for k, v in value.items():
            if str(k).lower() in _SENSITIVE_KEYS:
                out[k] = "***REDACTED***"
            else:
                out[k] = _sanitize(v)
        return out
    if isinstance(value, list):
        return [_sanitize(x) for x in value]
    return value

def _safe_response_preview(response):
    try:
        data = response.json()
        return json.dumps(_sanitize(data), ensure_ascii=False)[:500]
    except Exception:
        # Do not emit raw non-JSON bodies from credential endpoints.
        return "<non-json response omitted>"

class KISRealtimeBridge:
    def __init__(self,app_key,app_secret,symbol_provider,on_tick,on_quote,status,ws_url="ws://ops.koreainvestment.com:21000",approval_url="https://openapi.koreainvestment.com:9443/oauth2/Approval"):
        self.app_key=app_key; self.app_secret=app_secret; self.symbol_provider=symbol_provider; self.on_tick=on_tick; self.on_quote=on_quote; self.status=status; self.ws_url=ws_url; self.approval_url=approval_url; self._stop=threading.Event(); self._thread=None
    def start(self):
        if self._thread and self._thread.is_alive(): return
        self._thread=threading.Thread(target=self._thread_main,name="kis-ws",daemon=True); self._thread.start()
    def stop(self): self._stop.set()
    def _thread_main(self):
        while not self._stop.is_set():
            try: asyncio.run(self._run_once())
            except Exception as e: self.status["connected"]=False; self.status["last_error"]=f"{type(e).__name__}: {e}"[:300]
            if not self._stop.is_set(): time.sleep(5)
    def _approval_key(self):
        print("[EDGE2][KIS] requesting WebSocket approval key", flush=True)
        payload = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "secretkey": self.app_secret,
        }
        try:
            r = requests.post(
                self.approval_url,
                headers={"content-type": "application/json"},
                data=json.dumps(payload),
                timeout=10,
            )
        except Exception as e:
            msg = f"approval_request_exception:{type(e).__name__}:{e}"
            self.status["approval_ready"] = False
            self.status["last_error"] = msg[:500]
            print(f"[EDGE2][KIS] {msg}", flush=True)
            raise

        body_preview = _safe_response_preview(r)
        print(f"[EDGE2][KIS] approval HTTP {r.status_code} body={body_preview}", flush=True)

        if not r.ok:
            msg = f"approval_http_error:{r.status_code}:{body_preview}"
            self.status["approval_ready"] = False
            self.status["last_error"] = msg[:500]
            raise RuntimeError(msg)

        try:
            data = r.json()
        except Exception as e:
            msg = f"approval_json_error:{type(e).__name__}:{body_preview}"
            self.status["approval_ready"] = False
            self.status["last_error"] = msg[:500]
            raise RuntimeError(msg)

        key = str(data.get("approval_key", "")).strip()
        if not key:
            safe_data = dict(data) if isinstance(data, dict) else {"response": str(data)}
            safe_data.pop("approval_key", None)
            msg = f"approval_key_missing:{safe_data}"
            self.status["approval_ready"] = False
            self.status["last_error"] = msg[:500]
            raise RuntimeError(msg)

        self.status["approval_ready"] = True
        self.status["last_error"] = ""
        print("[EDGE2][KIS] approval key READY", flush=True)
        return key
    @staticmethod
    def _sub_msg(key,tr_id,ticker):
        return json.dumps({"header":{"approval_key":key,"custtype":"P","tr_type":"1","content-type":"utf-8"},"body":{"input":{"tr_id":tr_id,"tr_key":ticker}}},ensure_ascii=False)
    async def _subscribe_symbol(self,ws,key,ticker):
        await ws.send(self._sub_msg(key,TR_TICK,ticker)); await asyncio.sleep(.08); await ws.send(self._sub_msg(key,TR_QUOTE,ticker)); await asyncio.sleep(.08)
    async def _run_once(self):
        self.status["approval_ready"]=False; key=await asyncio.to_thread(self._approval_key); subscribed=set()
        print(f"[EDGE2][KIS] connecting WebSocket {self.ws_url}", flush=True)
        async with websockets.connect(self.ws_url,ping_interval=None,close_timeout=5,open_timeout=10) as ws:
            self.status["connected"]=True
            self.status["last_error"]=""
            self.status["ws_connected_at"]=time.time()
            print("[EDGE2][KIS] WebSocket CONNECTED", flush=True)
            while not self._stop.is_set():
                desired={str(x).strip().upper() for x in self.symbol_provider() if str(x).strip()}
                for ticker in sorted(desired-subscribed):
                    await self._subscribe_symbol(ws,key,ticker)
                    subscribed.add(ticker)
                    self.status["subscriptions"]=len(subscribed)
                    self.status["subscription_requests"]=int(self.status.get("subscription_requests",0) or 0)+2
                    self.status["last_subscribed_ticker"]=ticker
                    print(f"[EDGE2][KIS] subscribe requested ticker={ticker} tick+quote", flush=True)
                try:data=await asyncio.wait_for(ws.recv(),timeout=2)
                except asyncio.TimeoutError:continue
                if not data:continue
                self.status["last_message_ts"]=time.time()
                if data[0] in ("0","1") and "|" in data:
                    parts=data.split("|",3)
                    if len(parts)<4:continue
                    tr_id=parts[1]
                    try:count=max(1,int(parts[2] or "1"))
                    except:count=1
                    if tr_id==TR_TICK:self._handle_ticks(parts[3],count)
                    elif tr_id==TR_QUOTE:self._handle_quotes(parts[3],count)
                    continue
                try:obj=json.loads(data)
                except:continue
                tr_id=str((obj.get("header") or {}).get("tr_id",""))
                if tr_id=="PINGPONG":await ws.send(data);continue
                body=obj.get("body") or {}
                rt_cd=str(body.get("rt_cd","0"))
                msg1=str(body.get("msg1",""))
                if tr_id in (TR_TICK,TR_QUOTE):
                    if rt_cd in ("0",""):
                        self.status["subscription_acks"]=int(self.status.get("subscription_acks",0) or 0)+1
                        self.status["last_subscription_ack"]=f"{tr_id}:OK"
                        print(f"[EDGE2][KIS] subscription ACK tr_id={tr_id} OK", flush=True)
                    else:
                        safe_msg=msg1[:200]
                        self.status["last_subscription_ack"]=f"{tr_id}:ERROR:{safe_msg}"
                        self.status["last_error"]=f"subscription_error:{tr_id}:{safe_msg}"[:300]
                        print(f"[EDGE2][KIS] subscription ACK tr_id={tr_id} ERROR {safe_msg}", flush=True)
                elif rt_cd not in ("0",""):
                    self.status["last_error"]=msg1[:300]
        self.status["connected"]=False
        print("[EDGE2][KIS] WebSocket DISCONNECTED", flush=True)
    def _handle_ticks(self,payload,count):
        vals=payload.split("^"); width=len(TICK_FIELDS); usable=min(count,len(vals)//width)
        for i in range(usable):
            row=dict(zip(TICK_FIELDS,vals[i*width:(i+1)*width])); ticker=row.get("MKSC_SHRN_ISCD","").strip(); price=_num(row.get("STCK_PRPR")); volume=abs(_num(row.get("CNTG_VOL")))
            if ticker and price>0:self.on_tick({"ticker":ticker,"price":price,"volume":volume,"ts":time.time(),"side":row.get("CCLD_DVSN","")})
    def _handle_quotes(self,payload,count):
        vals=payload.split("^"); width=len(QUOTE_FIELDS); usable=min(count,len(vals)//width)
        for i in range(usable):
            row=dict(zip(QUOTE_FIELDS,vals[i*width:(i+1)*width])); ticker=row.get("MKSC_SHRN_ISCD","").strip()
            if ticker:self.on_quote({"ticker":ticker,"ts":time.time(),"ask1":_num(row.get("ASKP1")),"bid1":_num(row.get("BIDP1")),"ask_qty1":_num(row.get("ASKP_RSQN1")),"bid_qty1":_num(row.get("BIDP_RSQN1")),"total_ask_qty":_num(row.get("TOTAL_ASKP_RSQN")),"total_bid_qty":_num(row.get("TOTAL_BIDP_RSQN"))})
