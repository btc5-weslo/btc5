"""FastAPI service: live dashboard API + static UI + automatic trading (DEMO by default; LIVE only after explicit user
confirmation, with wallet credentials read from environment variables only — never stored, logged or returned)."""
import asyncio, json, os, time, logging, math, hmac
from contextlib import asynccontextmanager
import httpx
from fastapi import FastAPI, HTTPException, Body, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from .config import Config, ROOT
from .db import DB
from .clock import SyncedClock
from .live import LiveData
from .polymarket import Polymarket, UA, round_start_for
from .engine import Engine, DEFAULT_SETTINGS
from . import features as F, analytics, backtest as BT, history
from .trading import Trader

logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(name)s %(message)s")
cfg = Config(); db = DB(cfg.db_path); clock = SyncedClock()
G = {}

async def clock_loop(client, eng):
    """Resync every 5 min, immediately after a detected host pause, and every 30 s while unsynced."""
    while True:
        ok = await clock.sync(client)
        d = clock.drift_check()
        if not ok or not d["ok"]: eng.log("CLOCK", "warning", f"Clock sync problem: {clock.error} {d}")
        t0 = time.time()
        while time.time() - t0 < (300 if ok else 30):
            await asyncio.sleep(1)
            if getattr(eng, "pause_detected", False):
                eng.pause_detected = False; break

def bg(coro):
    """Hold a strong reference to background tasks so they are not garbage-collected."""
    t = asyncio.create_task(coro); G.setdefault("bg", set()).add(t); t.add_done_callback(G["bg"].discard); return t

@asynccontextmanager
async def lifespan(app):
    client = httpx.AsyncClient(headers=UA, limits=httpx.Limits(max_connections=40))
    live = LiveData(cfg, client)
    poly = Polymarket(client, cfg)
    eng = Engine(cfg, db, clock, live, poly)
    live.logf = lambda src, lvl, msg: eng.log(src, lvl, msg)
    for n, en in (eng.settings.get("feeds") or {}).items():
        if n in live.feeds: live.feeds[n].enabled = bool(en)
    trader = Trader(cfg, db, clock, poly, log=eng.log); eng.trader = trader
    G.update(client=client, live=live, poly=poly, eng=eng, trader=trader, started=time.time())
    await clock.sync(client)
    await live.start()
    bg(clock_loop(client, eng)); bg(eng.run()); bg(trader.run())
    if trader.creds_configured(): bg(trader.connect())   # read-only wallet status; mode stays DEMO
    from . import cloud_tasks
    cloud_tasks.start(bg, client)   # no-op unless Render/HF env vars are set
    yield
    eng.running = False; live.running = False; trader.running = False
    await cloud_tasks.shutdown()
    await client.aclose()

app = FastAPI(title="POLYMARKET BTC-5", lifespan=lifespan)
READONLY = os.environ.get("BTC5_READONLY") == "1"

@app.middleware("http")
async def readonly_guard(request, call_next):
    """Public deployments (the HF Space) are read-only: no one on the internet can change settings or start jobs."""
    p = request.url.path
    if READONLY and request.method not in ("GET", "HEAD", "OPTIONS") and p.startswith("/api/trading/") and (p == "/api/trading/emergency-stop" or os.environ.get("BTC5_ADMIN_TOKEN")):
        return await call_next(request)      # trading writes on a public instance need the admin passcode (checked per endpoint); emergency stop is always allowed
    if READONLY and request.method not in ("GET", "HEAD", "OPTIONS"):
        return JSONResponse({"error": "read-only public deployment — settings and jobs can only be changed on a private instance"}, status_code=403)
    return await call_next(request)
STATIC = os.path.join(ROOT, "static")

@app.get("/")
def index(): return FileResponse(os.path.join(STATIC, "index.html"), headers={"Cache-Control": "no-store"})

_intel = {"ts": 0, "v": None}
def intel():
    """Live market intelligence (real data, updated each second). Uses data up to NOW for display; the locked
    prediction is never recomputed from it."""
    if time.time() - _intel["ts"] < 1 and _intel["v"]: return _intel["v"]
    live = G["live"]; eng = G["eng"]; now = clock.now(); T = round_start_for(now)
    bars, src, deg = live.primary_bars_window(T - 600, now)
    f, info = F.compute(T, bars, live.m1, [(t, p) for t, p in eng.state.get("crowd") or []], live.derivs.get("oi_series"), live.derivs.get("funding_series"), now)
    quotes = {}
    for ex in ("binance", "coinbase", "kraken"):
        q = live.quotes.get(ex); fd = live.feeds[ex]
        quotes[ex] = {"price": fd.last_price, "bid": q and q["bid"], "ask": q and q["ask"], "fresh": fd.fresh(cfg.feed_stale_s), "age_s": None if fd.age() is None else round(fd.age(), 2)}
    cl = live.feeds["chainlink"]
    quotes["chainlink"] = {"price": cl.last_price, "fresh": cl.fresh(cfg.feed_stale_s), "age_s": None if cl.age() is None else round(cl.age(), 2)}
    ref = cl.last_price
    for k, q in quotes.items(): q["vs_chainlink_usd"] = round(q["price"] - ref, 2) if q.get("price") and ref else None
    v = {"ts": time.time(), "regime": info.get("regime"), "vwap": info.get("vwap"), "ema": info.get("ema"), "rsi": info.get("rsi"), "macd": info.get("macd"), "adx": info.get("adx"),
         "atr": info.get("atr"), "vol_percentile": info.get("vol_percentile"), "realized_vol_ratio": round(math.exp(f["rv_ratio"]), 3), "sigma_1s_bps": round((info.get("sigma_1s") or 0) * 1e4, 3),
         "taker_buy_share": info.get("taker_buy_share"), "round_volume_btc": info.get("round_volume_btc"), "levels": info.get("levels"), "books": live.books,
         "exchanges": quotes, "derivs": {k: v for k, v in live.derivs.items() if not k.endswith("series")}, "big_trades": list(live.big_trades)[-8:],
         "bar_source": src, "degraded": deg, "data_quality": info.get("data_quality"), "missing": info.get("missing")}
    _intel.update(ts=time.time(), v=v); return v

def round_view():
    eng = G["eng"]; live = G["live"]; st = eng.state; now = clock.now()
    r = st.get("round") or {}
    T = r.get("start_ts") or round_start_for(now)
    px, src = live.composite()
    op = (st.get("open") or {}).get("official") or (st.get("open") or {}).get("chainlink_rtds")
    pred = st.get("prediction")
    if pred and pred.get("round_id") != r.get("round_id"): pred = None
    el = now - T
    phase = st.get("phase")
    if pred and pred.get("official_outcome"): phase = "ROUND COMPLETED"
    agents = None
    if pred and pred.get("agents"): agents = pred["agents"]
    else:
        pre = st.get("prelim")
        agents = {g: {"agent": g, "name": n, "status": "ANALYZING" if el < 80 else "FINALIZING", "direction": None, "p_up": None, "p_down": None,
                      "note": "Direction withheld until the T90 publication event"} for g, n in __import__("btc5.model", fromlist=["AGENT_GROUPS"]).AGENT_GROUPS}
        agents["VALIDATOR"] = {"agent": "VALIDATOR", "name": "AGENT 6 · QUANT FINAL VALIDATION", "status": "WAITING FOR T90", "direction": None}
        if pre: 
            for a in agents.values(): a["inputs_ready"] = True
    return {"round_id": r.get("round_id"), "start_ts": T, "decision_ts_target": T + 90, "end_ts": T + 300, "elapsed": round(el, 2),
            "phase": phase, "stage": st.get("stage"), "market": st.get("market"), "market_error": st.get("market_error"),
            "open_price": op, "open_source": "Polymarket official (Chainlink)" if (st.get("open") or {}).get("official") else ("Chainlink RTDS at T0" if op else None),
            "price": px, "price_source": src, "diff": round(px - op, 2) if px and op else None, "prediction": pred, "agents": agents,
            "poly_book": st.get("poly_book"), "crowd_last": (st.get("crowd") or [None])[-1] if pred else None}

def health():
    eng = G["eng"]; live = G["live"]
    feeds = live.snapshot()
    lag = list(eng.loop_lag)
    last = db.one("SELECT round_id, deviation_ms, status FROM rounds WHERE status!='MISSED' ORDER BY start_ts DESC LIMIT 1")
    ws_ok = [feeds[n]["fresh"] for n in ("binance", "coinbase", "kraken", "chainlink")]
    return {"ok": any(ws_ok) and clock.synced, "server_time": time.time(), "clock": clock.status(), "drift": clock.drift_check(), "feeds": feeds,
            "loop_lag_ms": {"p50": round(sorted(lag)[len(lag) // 2], 2) if lag else None, "max": round(max(lag), 2) if lag else None},
            "tasks": len(eng.tasks), "model": {"version": eng.brain.version, "arch": eng.brain.arch, "loaded": eng.brain.model is not None},
            "training": {k: v for k, v in eng.training.items() if k != "last"}, "challenger": db.kv_get("challenger_status"),
            "last_decision": last, "uptime_s": round(time.time() - G["started"]), "db_bytes": os.path.getsize(cfg.db_path),
            "mode": ("LIVE TRADING (real money)" if G["trader"].settings["mode"] == "live" else "DEMO trading (paper money, real Polymarket prices)") if G.get("trader") else "PREDICTION ONLY",
            "settings_mode": eng.settings.get("mode")}

@app.get("/api/health")
def api_health():
    # Render healthCheckPath: never 500 just because the engine is still starting.
    if "eng" not in G or "live" not in G:
        return {"ok": False, "starting": True, "server_time": time.time()}
    return health()

def latest_backtest():
    r = db.one("SELECT ts, results_json FROM backtests ORDER BY id DESC LIMIT 1")
    return json.loads(r["results_json"]) if r else None

@app.get("/api/state")
def api_state():
    eng = G["eng"]; bt = latest_backtest()
    rel = eng.brain.meta or {}
    return {"server_time": clock.now(), "round": round_view(), "intel": intel(), "health": health(),
            "model": {"version": eng.brain.version, "arch": eng.brain.arch, "walk_forward": rel.get("walk_forward"), "confidence_accuracy": rel.get("confidence_accuracy"),
                      "agent_reliability": rel.get("agent_reliability")},
            "logs": list(eng.logs)[-40:], "settings": eng.settings, "trading": G["trader"].summary() if G.get("trader") else None,
            "interactive": not READONLY}

@app.get("/api/candles")
def api_candles(tf: str = "1m", n: int = 120):
    live = G["live"]; now = clock.now()
    if tf == "1s":
        bars = live.bars["binance"].window(int(now) - min(n, 900), now)
        rows = [[b[0], b[1], b[2], b[3], b[4], b[5]] for b in bars]
    else:
        rows = [[b[0], b[1], b[2], b[3], b[4], b[5]] for b in live.m1[-(n + 200):]]
        m0 = int(now) // 60 * 60
        cur = live.bars["binance"].window(m0, now)
        if cur and (not rows or rows[-1][0] < m0):
            rows.append([m0, cur[0][1], max(b[2] for b in cur), min(b[3] for b in cur), cur[-1][4], sum(b[5] for b in cur)])
    import numpy as np
    c = [r[4] for r in rows]
    def ema(x, k):
        if not x: return []
        a = 2 / (k + 1); e = x[0]; o = []
        for v in x: e = a * v + (1 - a) * e; o.append(round(e, 2))
        return o
    out = {"tf": tf, "source": "Binance BTCUSDT", "candles": rows[-n:], "ema": {str(k): ema(c, k)[-n:] for k in (9, 20, 50, 200)}}
    if len(c) > 30:
        m = np.array(ema(c, 12)) - np.array(ema(c, 26)); s = ema(list(m), 9)
        out["macd"] = {"macd": [round(float(x), 2) for x in m[-n:]], "signal": s[-n:]}
        d = np.diff(c); g = np.where(d > 0, d, 0); l = np.where(d < 0, -d, 0); rsi = []
        ag = g[:14].mean(); al = l[:14].mean()
        for i in range(len(d)):
            if i >= 14: ag = (ag * 13 + g[i]) / 14; al = (al * 13 + l[i]) / 14
            rsi.append(None if i < 14 else round(100 - 100 / (1 + ag / al), 1) if al else 100)
        out["rsi"] = ([None] + rsi)[-n:]
    # session VWAP for the current round (1m) from 1s bars
    T = round_start_for(now); rb = live.bars["binance"].window(T, now); v = sum(b[5] for b in rb)
    out["round_vwap"] = round(sum((b[2] + b[3] + b[4]) / 3 * b[5] for b in rb) / v, 2) if v else None
    out["round_start"] = T
    return out

@app.get("/api/history")
def api_history(limit: int = 100):
    rows = db.q("SELECT * FROM rounds ORDER BY start_ts DESC LIMIT ?", (min(limit, 2000),))
    out = []
    for r in rows:
        p = Engine.public(r); p.pop("agents", None); out.append(p)
    return out

@app.get("/api/round/{rid}")
def api_round(rid: str):
    r = db.get_round(rid)
    if not r: raise HTTPException(404)
    return Engine.public(r)

@app.get("/api/performance")
def api_perf():
    eng = G["eng"]; return analytics.performance(db, (eng.brain.meta or {}).get("walk_forward"))

@app.get("/api/backtest")
def api_backtest():
    r = db.q("SELECT id, ts, params_json FROM backtests ORDER BY id DESC LIMIT 10")
    return {"latest": latest_backtest(), "runs": r}

@app.post("/api/backtest/run")
async def api_bt_run(body: dict = Body(default={})):
    days = body.get("days")
    days = float(days) if days else None
    eng = G["eng"]
    async def go():
        eng.log("BACKTEST", "info", f"Backtest started (days={days or 'all'})")
        r = await asyncio.get_running_loop().run_in_executor(eng.pool, _bt_job, days)
        eng.log("BACKTEST", "info", f"Backtest finished: {r}")
    bg(go()); return {"started": True, "days": days}

def _bt_job(days):
    from .config import Config as C
    from .db import DB as D
    d = D(C().db_path); res = BT.run(d, days)
    if "error" in res: return res["error"]
    d.x("INSERT INTO backtests(ts,params_json,results_json) VALUES (?,?,?)", (time.time(), json.dumps({"days": days, "trigger": "manual"}), json.dumps(res, default=str)))
    return f"{res['n_test']} OOS rounds, ensemble accuracy {res['models']['stack']['accuracy']}"

@app.post("/api/history/backfill")
async def api_backfill(body: dict = Body(default={})):
    days = float(body.get("days") or cfg.backfill_days); eng = G["eng"]
    bg(eng.train(backfill=True, days=days)); return {"started": True, "days": days, "note": "real-data backfill + retrain/evaluation job"}

@app.post("/api/model/retrain")
async def api_retrain():
    bg(G["eng"].train(backfill=False)); return {"started": True}

@app.get("/api/models")
def api_models():
    return {"champion": db.kv_get("champion"), "challenger_status": db.kv_get("challenger_status"),
            "versions": [dict(r, metrics=json.loads(r.pop("metrics_json"))) for r in db.q("SELECT version, created_ts, kind, role, n_train, metrics_json FROM model_versions ORDER BY created_ts DESC LIMIT 20")]}

@app.get("/api/logs")
def api_logs(limit: int = 200):
    return db.q("SELECT * FROM events ORDER BY id DESC LIMIT ?", (min(limit, 2000),))

@app.get("/api/settings")
def get_settings(): return G["eng"].settings

@app.post("/api/settings")
def set_settings(body: dict = Body(...)):
    eng = G["eng"]; s = eng.settings
    allowed = set(DEFAULT_SETTINGS)
    for k, v in body.items():
        if k not in allowed: raise HTTPException(400, f"unknown setting {k}")
        if k == "mode" and v not in ("automatic", "monitoring"): raise HTTPException(400, "mode must be automatic|monitoring")
        if k == "weighting" and v.get("mode") not in ("learned", "manual"): raise HTTPException(400, "weighting.mode must be learned|manual")
        if k == "model" and v.get("pin_version") and not db.one("SELECT 1 FROM model_versions WHERE version=?", (v["pin_version"],)): raise HTTPException(400, "unknown model version")
        s[k] = v
    db.kv_set("settings", s)
    for n, en in (s.get("feeds") or {}).items():
        if n in G["live"].feeds: G["live"].feeds[n].enabled = bool(en)
    eng.brain.version = None; eng.brain.reload()
    eng.log("SETTINGS", "info", f"Settings updated: {', '.join(body)}")
    return s

# ------------------------------------------------------------------ trading
def require_token(request: Request):
    """If BTC5_ADMIN_TOKEN is set, every trading/mode write needs header X-BTC5-Token (constant-time compare)."""
    tok = os.environ.get("BTC5_ADMIN_TOKEN")
    if tok and not hmac.compare_digest((request.headers.get("x-btc5-token") or "").encode(), tok.encode()):
        raise HTTPException(401, "admin passcode required (X-BTC5-Token)")

@app.get("/api/trading")
def api_trading(mode: str = None): return G["trader"].summary(mode if mode in ("demo", "live") else None, recent=20)

@app.get("/api/trading/trades")
def api_trades(mode: str = None, limit: int = 100): return G["trader"].trades(mode, limit)

@app.get("/api/trading/equity")
def api_equity(mode: str = "demo"): return G["trader"].equity("live" if mode == "live" else "demo")

@app.get("/api/trading/wallet")
async def api_wallet():
    t = G["trader"]
    if t.client is not None: await t.refresh_wallet()
    return t.wallet_status()

@app.post("/api/trading/settings")
def api_trading_settings(request: Request, body: dict = Body(...)):
    require_token(request)
    try: G["trader"].update_settings(body)
    except ValueError as e: raise HTTPException(400, str(e))
    return G["trader"].summary()

@app.post("/api/trading/mode")
async def api_trading_mode(request: Request, body: dict = Body(...)):
    require_token(request)
    try: r = await G["trader"].set_mode(body.get("mode"), bool(body.get("confirm")), body.get("limits"))
    except ValueError as e: raise HTTPException(400, str(e))
    except PermissionError as e: raise HTTPException(409, str(e))
    return dict(r, summary=G["trader"].summary())

@app.post("/api/trading/emergency-stop")
async def api_estop():
    """Always allowed (fail-safe direction only): auto-trade off, DEMO mode, cancel all open CLOB orders."""
    return await G["trader"].emergency_stop()

@app.post("/api/trading/demo/reset")
def api_demo_reset(request: Request, body: dict = Body(default={})):
    require_token(request)
    try: return G["trader"].reset_demo(body.get("bankroll"))
    except ValueError as e: raise HTTPException(400, str(e))

app.mount("/static", StaticFiles(directory=STATIC), name="static")
