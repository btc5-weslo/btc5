"""Historical dataset collection from REAL sources only:
 - Official outcomes: Polymarket Gamma (closed BTC 5m markets, 1/0 payout).
 - Binance BTCUSDT 1-second klines for [T0-600s, T0+90s) per round (open/high/low/close/volume/trades/taker-buy volume).
   Nothing after T0+90 is ever stored for a round, so backtests cannot leak post-decision data.
 - Binance 1-minute klines (indicator context), OKX BTC open-interest (5m) and funding history.
 - Polymarket CLOB price history for the UP token, points with t <= T0+90 only (crowd-implied probability).
Resumable and incremental; errors are recorded per round, never filled with invented values."""
import asyncio, json, time, logging
import httpx
from .db import pack
from .polymarket import Polymarket, parse_resolution, UA

log = logging.getLogger("btc5.history")

async def fetch_m1(c, cfg, db, start, end):
    t = start
    while t < end:
        r = await c.get(f"{cfg.binance_rest}/api/v3/klines", params={"symbol": "BTCUSDT", "interval": "1m", "startTime": t * 1000, "limit": 1000}, timeout=10)
        r.raise_for_status(); k = r.json()
        if not k: break
        db.many("INSERT OR REPLACE INTO m1 VALUES (?,?,?,?,?,?,?,?,?)",
                [(int(x[0]) // 1000, float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5]), float(x[7]), int(x[8]), float(x[9])) for x in k])
        t = int(k[-1][0]) // 1000 + 60
        await asyncio.sleep(0.1)

async def fetch_derivs(c, db):
    try:
        r = await c.get("https://www.okx.com/api/v5/rubik/stat/contracts/open-interest-volume", params={"ccy": "BTC", "period": "5m"}, timeout=10)
        d = r.json().get("data", [])
        db.many("INSERT OR REPLACE INTO oi5m VALUES (?,?,?)", [(int(x[0]) // 1000, float(x[1]), float(x[2])) for x in d])
    except Exception as e: log.warning("OI history: %s", e)
    try:
        after = None
        for _ in range(4):
            p = {"instId": "BTC-USDT-SWAP", "limit": 100}
            if after: p["after"] = after
            r = await c.get("https://www.okx.com/api/v5/public/funding-rate-history", params=p, timeout=10)
            d = r.json().get("data", [])
            if not d: break
            db.many("INSERT OR REPLACE INTO funding VALUES (?,?)", [(int(x["fundingTime"]) // 1000, float(x.get("realizedRate") or x["fundingRate"])) for x in d])
            after = d[-1]["fundingTime"]
    except Exception as e: log.warning("funding history: %s", e)

def s1_rows(k):
    return [[int(x[0]) // 1000, float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5]), int(x[8]), float(x[9])] for x in k]

async def fetch_round(c, poly, cfg, db, T, sem):
    async with sem:
        err = None; outcome = tok = None; crowd = []; s1 = None
        try:
            m = await poly.market_raw(T)
            if m:
                outcome = parse_resolution(m)
                tok = json.loads(m["clobTokenIds"])[0] if m.get("clobTokenIds") else None
            else: err = "no market"
            r = await c.get(f"{cfg.binance_rest}/api/v3/klines", timeout=10,
                            params={"symbol": "BTCUSDT", "interval": "1s", "startTime": (T - 600) * 1000, "endTime": (T + 90) * 1000 - 1, "limit": 1000})
            r.raise_for_status(); s1 = [b for b in s1_rows(r.json()) if b[0] < T + 90]
            if tok:
                try:
                    h = await poly.price_history(tok, T, T + 90)
                    crowd = [[int(p["t"]), float(p["p"])] for p in h if int(p["t"]) <= T + 90]
                except Exception as e: crowd = None
        except Exception as e:
            err = f"{type(e).__name__}: {e}"[:200]
        if outcome is None and err is None: err = "not resolved yet"
        db.x("INSERT OR REPLACE INTO hist_rounds VALUES (?,?,?,?,?,?,?,?)",
             (T, f"{cfg.slug_prefix}{T}", outcome, tok, json.dumps(crowd) if crowd is not None else None, pack(s1) if s1 else None, time.time(), err))

async def backfill(cfg, db, days=None, progress=None, client=None):
    days = days or cfg.backfill_days
    own = client is None
    c = client or httpx.AsyncClient(headers=UA, http2=False)
    poly = Polymarket(c, cfg)
    try:
        now = int(time.time()); last = now // 300 * 300 - 600   # rounds that ended >= 5 min ago
        first = last - int(days * 86400) // 300 * 300
        have = db.q("SELECT MIN(ts) a, MAX(ts) b FROM m1")[0]
        m1_start = first - 1000 * 60
        if have["a"] is None or have["a"] > m1_start: await fetch_m1(c, cfg, db, m1_start, now)
        else: await fetch_m1(c, cfg, db, have["b"] - 120, now)
        await fetch_derivs(c, db)
        done = {r["start_ts"] for r in db.q("SELECT start_ts FROM hist_rounds WHERE outcome IS NOT NULL AND s1 IS NOT NULL")}
        todo = [T for T in range(first, last + 1, 300) if T not in done]
        sem = asyncio.Semaphore(6); n = 0
        for i in range(0, len(todo), 60):
            await asyncio.gather(*(fetch_round(c, poly, cfg, db, T, sem) for T in todo[i:i + 60]))
            n += len(todo[i:i + 60])
            if progress: progress(n, len(todo))
        return len(todo)
    finally:
        if own: await c.aclose()

if __name__ == "__main__":
    import sys
    from .config import Config
    from .db import DB
    logging.basicConfig(level=logging.WARNING)
    cfg = Config(); db = DB(cfg.db_path)
    d = float(sys.argv[1]) if len(sys.argv) > 1 else cfg.backfill_days
    print("todo", asyncio.run(backfill(cfg, db, d, progress=lambda a, b: print(time.strftime("%T"), a, "/", b, flush=True))))
