import asyncio, math, random, time
from btc5.polymarket import MarketInfo
from btc5.live import FeedStatus

class FakeClock:
    def __init__(self, t): self.t = float(t); self.synced = True
    def now(self): return self.t
    def mono(self): return self.t
    def drift_check(self): return {"ok": True}

def make_sleep(clock):
    async def sleep(d):
        clock.t += max(float(d), 0.0005); await asyncio.sleep(0)
    return sleep

def synth_bars(T0, start, end, p0=60000.0, seed=1):
    rnd = random.Random(seed); out = []; p = p0
    for s in range(start, end):
        o = p; p = p * math.exp(rnd.gauss(0, 5e-5)); v = abs(rnd.gauss(1, 0.5))
        out.append([s, o, max(o, p), min(o, p), p, v, rnd.randint(1, 30), v * rnd.random()])
    return out

def synth_m1(T0, n=300, p0=60000.0, seed=2):
    rnd = random.Random(seed); out = []; p = p0
    for i in range(n):
        t = T0 - (n - i) * 60; o = p; p *= math.exp(rnd.gauss(0, 4e-4)); v = abs(rnd.gauss(20, 5))
        out.append([t, o, max(o, p) * 1.0002, min(o, p) * 0.9998, p, v, v * p, 500, v * 0.5])
    return out

def market(T, cfg):
    return MarketInfo(slug=f"{cfg.slug_prefix}{T}", market_id="1", condition_id="0x", question="Bitcoin Up or Down - test", start_ts=T, end_ts=T + 300,
                      up_token="u", down_token="d", resolution_source="https://data.chain.link/streams/btc-usd", rules="Chainlink rules", accepting_orders=True)

class FakeLive:
    def __init__(self, T):
        self.T = T; self.m1 = synth_m1(T); self.all = synth_bars(T, T - 600, T + 300)
        self.derivs = {"oi_series": [], "funding_series": []}; self.books = {}
        self.feeds = {n: FeedStatus(n) for n in ("binance", "coinbase", "kraken", "chainlink", "polymarket_clob")}
        for f in self.feeds.values(): f.last_msg = time.time()
        self.windows = []; self.bars = {"binance": self}
    def primary_bars_window(self, start, cutoff):
        self.windows.append(cutoff); return [b for b in self.all if start <= b[0] and b[0] + 1 <= cutoff], "binance", False
    def get(self, a, b): return [x for x in self.all if a <= x[0] < b]
    def chainlink_at(self, ts): return 60000.0
    def chainlink_last_before(self, ts): return (ts - 0.5, 60010.0)
    async def patch_gaps(self, a, b): return 0

class FakePoly:
    def __init__(self, cfg, outcome="UP", fail_market=False): self.cfg = cfg; self.outcome = outcome; self.fail = fail_market
    async def discover(self, T):
        from btc5.polymarket import MarketIdentificationError
        if self.fail: raise MarketIdentificationError("test: market not found")
        return market(T, self.cfg)
    async def book(self, tok): return {"mid": 0.55, "best_bid": 0.54, "best_ask": 0.56}
    async def chainlink_open_close(self, T): return {"openPrice": 60000.0, "closePrice": 60050.0}
    async def resolution(self, T): return self.outcome, {"closed": True}
