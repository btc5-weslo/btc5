"""Market Data Layer: WebSocket ingestion with automatic reconnection, exchange normalisation, data validation,
1-second bar construction, REST gap-patching from the same Binance 1s source used in training, and derivatives pollers.

Feeds (all public, no credentials):
  Binance BTCUSDT  aggTrade + bookTicker  wss://data-stream.binance.vision   (primary bars; binance.com is geo-blocked here)
  Coinbase BTC-USD matches + ticker       wss://ws-feed.exchange.coinbase.com (failover bars, cross-exchange)
  Kraken BTC/USD   trade + ticker (v2)     wss://ws.kraken.com/v2
  Chainlink BTC/USD via Polymarket RTDS    wss://ws-live-data.polymarket.com  (the contract's resolution source)
  REST: Binance depth / klines, Coinbase L2 book, OKX OI / funding / long-short / liquidations, Deribit perpetual."""
import asyncio, json, time, collections, math, logging
import httpx, websockets

log = logging.getLogger("btc5.live")

class FeedStatus:
    def __init__(self, name, kind="ws"):
        self.name = name; self.kind = kind; self.connected = False; self.last_msg = None; self.last_price = None; self.last_exch_ts = None
        self.reconnects = 0; self.errors = 0; self.last_error = None; self.enabled = True; self.connected_since = None
    def age(self): return None if self.last_msg is None else time.time() - self.last_msg
    def fresh(self, s=5.0): return self.enabled and self.last_msg is not None and time.time() - self.last_msg <= s
    def to_dict(self, stale_s=5.0):
        a = self.age()
        return {"name": self.name, "kind": self.kind, "enabled": self.enabled, "connected": self.connected, "fresh": self.fresh(stale_s),
                "age_s": None if a is None else round(a, 2), "price": self.last_price, "reconnects": self.reconnects, "errors": self.errors,
                "last_error": self.last_error, "status": "DISABLED" if not self.enabled else ("LIVE" if self.fresh(stale_s) else ("STALE" if self.last_msg else "DOWN"))}

class BarBuilder:
    """1-second OHLCV bars from executed trades: [t, o, h, l, c, v, n, taker_buy_v]. Seconds with no trades while the
    socket was connected are real 'no trade' observations (flat bar, v=0, same convention as Binance 1s klines).
    Seconds while disconnected are left missing (gaps) and later patched from REST if possible."""
    def __init__(self, maxlen=1800):
        self.bars = collections.OrderedDict(); self.maxlen = maxlen; self.cur = {}; self.connected_secs = set()
    def trade(self, ts, px, sz, taker_buy):
        s = int(ts)
        b = self.cur.get(s)
        if b is None:
            b = self.cur[s] = [s, px, px, px, px, 0.0, 0, 0.0]
        b[2] = max(b[2], px); b[3] = min(b[3], px); b[4] = px; b[5] += sz; b[6] += 1
        if taker_buy: b[7] += sz
    def mark_connected(self, s): self.connected_secs.add(int(s))
    def roll(self, now):
        """Finalize seconds strictly older than now-1 (allows late trades)."""
        lim = int(now) - 1
        for s in sorted(k for k in self.cur if k < lim):
            self.bars[s] = self.cur.pop(s)
        last = next(reversed(self.bars)) if self.bars else None
        if last is not None:
            for s in range(last + 1, lim):
                if s in self.connected_secs and s not in self.bars:
                    c = self.bars[s - 1][4] if (s - 1) in self.bars else None
                    if c is None: break
                    self.bars[s] = [s, c, c, c, c, 0.0, 0, 0.0]
            self.bars = collections.OrderedDict(sorted(self.bars.items()))
        while len(self.bars) > self.maxlen: self.bars.popitem(last=False)
        self.connected_secs = {s for s in self.connected_secs if s > lim - self.maxlen}
    def patch(self, rows):
        for r in rows:
            if r[0] not in self.bars: self.bars[r[0]] = r
        self.bars = collections.OrderedDict(sorted(self.bars.items()))
    def get(self, start, end): return [b for s, b in self.bars.items() if start <= s < end]
    def window(self, start, cutoff):
        """Bars for [start, cutoff): finalized bars + in-progress bars of seconds < cutoff (all their trades are timestamped
        before the cutoff), plus flat no-trade bars for seconds observed while connected. Never includes data >= cutoff."""
        out = {s: list(b) for s, b in self.bars.items() if start <= s and s + 1 <= cutoff}
        for s, b in list(self.cur.items()):
            if start <= s and s + 1 <= cutoff and s not in out: out[s] = list(b)
        if out:
            for s in range(min(out), int(cutoff)):
                if s not in out and s in self.connected_secs and (s - 1) in out:
                    c = out[s - 1][4]; out[s] = [s, c, c, c, c, 0.0, 0, 0.0]
        return [out[s] for s in sorted(out)]
    def gaps(self, start, end): return [s for s in range(start, end) if s not in self.bars]

class LiveData:
    def __init__(self, cfg, client: httpx.AsyncClient, logf=None):
        self.cfg = cfg; self.c = client; self.logf = logf or (lambda *a: None)
        self.feeds = {n: FeedStatus(n) for n in ("binance", "coinbase", "kraken", "chainlink")}
        for n in ("binance_depth", "coinbase_book", "okx", "deribit", "binance_1m", "polymarket_clob"): self.feeds[n] = FeedStatus(n, "rest")
        self.bars = {"binance": BarBuilder(), "coinbase": BarBuilder()}
        self.quotes = {}                       # exchange -> {bid, ask, ts}
        self.chainlink = collections.deque(maxlen=900)   # (ts_s, value)
        self.m1 = []                           # Binance 1m klines [t,o,h,l,c,v,qv,n,tbv]
        self.books = {}                        # exchange -> {bid_usd, ask_usd, imbalance, ts}
        self.derivs = {"oi_series": [], "funding_series": []}
        self.big_trades = collections.deque(maxlen=200)
        self.running = False
        self.tasks = []
        self._kicked = {}
        self._ws = {}                          # name -> open websocket (so a host-pause can force-close it)

    # ---------- websockets ----------
    async def _ws_loop(self, name, url, sub, handler):
        st = self.feeds[name]; backoff = 1
        while self.running:
            if not st.enabled: await asyncio.sleep(2); continue
            try:
                async with websockets.connect(url, open_timeout=10, ping_interval=20, ping_timeout=20, max_size=2 ** 22) as ws:
                    if sub: await ws.send(json.dumps(sub))
                    self._ws[name] = ws
                    st.connected = True; st.connected_since = time.time(); backoff = 1
                    self.logf("FEEDS", "info", f"{name} websocket connected")
                    while self.running and st.enabled:
                        msg = await asyncio.wait_for(ws.recv(), timeout=self.WS_SILENCE_S)
                        try: handler(msg)
                        except Exception as e: st.errors += 1; st.last_error = f"parse: {e}"[:160]
            except Exception as e:
                st.errors += 1; st.last_error = f"{type(e).__name__}: {e}"[:160]
            finally:
                self._ws.pop(name, None)
            if self._kicked.pop(name, False): backoff = 1    # forced reconnect after a host pause: retry immediately
            if st.connected: self.logf("FEEDS", "warning", f"{name} websocket disconnected ({st.last_error}); reconnecting")
            st.connected = False; st.reconnects += 1
            await asyncio.sleep(min(30, backoff) if backoff > 1 else 0.2); backoff *= 2

    WS_SILENCE_S = 12      # every subscribed stream ticks/heartbeats several times a second; 12 s of silence = dead socket

    def kick(self, reason="host pause"):
        """Force-close every open websocket so the loops reconnect now instead of waiting for the silence timeout.
        Used after a detected host/VM pause, when sockets are half-open and silently dead."""
        n = 0
        for name, ws in list(self._ws.items()):
            self._kicked[name] = True; n += 1
            tr = getattr(ws, "transport", None)
            if tr is not None: tr.abort()            # half-open socket: drop TCP now so recv() raises immediately
            else: asyncio.ensure_future(ws.close())
        if n: self.logf("FEEDS", "warning", f"Forced reconnect of {n} websockets ({reason})")
        return n

    def _binance(self, msg):
        m = json.loads(msg); d = m.get("data", m); now = time.time(); st = self.feeds["binance"]
        if d.get("e") == "aggTrade":
            ts = d["T"] / 1000; px = float(d["p"]); q = float(d["q"])
            if not (1000 < px < 10_000_000): raise ValueError("price out of range")
            self.bars["binance"].trade(ts, px, q, not d["m"])      # m=True -> buyer is maker -> taker SELL
            st.last_price = px; st.last_exch_ts = ts
            if px * q >= 250_000: self.big_trades.append({"ts": ts, "ex": "binance", "px": px, "usd": round(px * q), "side": "BUY" if not d["m"] else "SELL"})
        elif "b" in d and "a" in d and "u" in d:
            self.quotes["binance"] = {"bid": float(d["b"]), "ask": float(d["a"]), "ts": now}
        st.last_msg = now; self.bars["binance"].mark_connected(now)

    def _coinbase(self, msg):
        d = json.loads(msg); now = time.time(); st = self.feeds["coinbase"]; t = d.get("type")
        if t in ("match", "last_match"):
            from datetime import datetime
            ts = datetime.fromisoformat(d["time"].replace("Z", "+00:00")).timestamp(); px = float(d["price"]); sz = float(d["size"])
            if t == "match": self.bars["coinbase"].trade(ts, px, sz, d["side"] == "sell")   # side = maker side
            st.last_price = px; st.last_exch_ts = ts
            if px * sz >= 250_000: self.big_trades.append({"ts": ts, "ex": "coinbase", "px": px, "usd": round(px * sz), "side": "BUY" if d["side"] == "sell" else "SELL"})
        elif t == "ticker":
            self.quotes["coinbase"] = {"bid": float(d["best_bid"]), "ask": float(d["best_ask"]), "ts": now}; st.last_price = float(d["price"])
        st.last_msg = now; self.bars["coinbase"].mark_connected(now)

    def _kraken(self, msg):
        d = json.loads(msg); now = time.time(); st = self.feeds["kraken"]
        ch = d.get("channel")
        if ch == "trade":
            for t in d.get("data", []): st.last_price = float(t["price"])
        elif ch == "ticker":
            for t in d.get("data", []): self.quotes["kraken"] = {"bid": float(t["bid"]), "ask": float(t["ask"]), "ts": now}; st.last_price = float(t["last"])
        st.last_msg = now

    def _chainlink(self, msg):
        if not msg: return
        d = json.loads(msg); p = d.get("payload") or {}; st = self.feeds["chainlink"]
        if "data" in p:   # initial snapshot list
            for x in p["data"]: self.chainlink.append((x["timestamp"] / 1000, float(x["value"])))
        elif "value" in p and p.get("symbol") == "btc/usd":
            self.chainlink.append((p["timestamp"] / 1000, float(p["value"]))); st.last_price = float(p["value"]); st.last_exch_ts = p["timestamp"] / 1000
        else: return
        st.last_msg = time.time()

    # ---------- REST pollers ----------
    async def _poll(self, name, fn, every):
        st = self.feeds[name]
        while self.running:
            t0 = time.monotonic()
            if st.enabled:
                try: await fn(); st.last_msg = time.time(); st.connected = True
                except Exception as e: st.errors += 1; st.last_error = f"{type(e).__name__}: {e}"[:160]; st.connected = False
            await asyncio.sleep(max(0.2, every - (time.monotonic() - t0)))

    async def _binance_depth(self):
        r = await self.c.get(f"{self.cfg.binance_rest}/api/v3/depth", params={"symbol": "BTCUSDT", "limit": 100}, timeout=4); r.raise_for_status(); j = r.json()
        self._book("binance", [(float(p), float(q)) for p, q in j["bids"]], [(float(p), float(q)) for p, q in j["asks"]])

    async def _coinbase_book(self):
        r = await self.c.get("https://api.exchange.coinbase.com/products/BTC-USD/book", params={"level": 2}, timeout=4); r.raise_for_status(); j = r.json()
        self._book("coinbase", [(float(p), float(s)) for p, s, _ in j["bids"][:300]], [(float(p), float(s)) for p, s, _ in j["asks"][:300]])

    def _book(self, ex, bids, asks):
        if not bids or not asks: return
        mid = (bids[0][0] + asks[0][0]) / 2; band = mid * 0.001   # within 10 bps of mid
        bu = sum(p * q for p, q in bids if p >= mid - band); au = sum(p * q for p, q in asks if p <= mid + band)
        wall_b = max(bids, key=lambda x: x[1]); wall_a = max(asks, key=lambda x: x[1])
        prev = self.books.get(ex)
        self.books[ex] = {"mid": mid, "bid_usd_10bps": round(bu), "ask_usd_10bps": round(au), "imbalance": round((bu - au) / (bu + au), 4) if bu + au else None,
                          "bid_wall": [wall_b[0], round(wall_b[1], 3)], "ask_wall": [wall_a[0], round(wall_a[1], 3)], "ts": time.time(),
                          "liquidity_change": None if not prev else round((bu + au) / max(1, prev["bid_usd_10bps"] + prev["ask_usd_10bps"]) - 1, 4)}

    def m1_limit(self, now):
        """How many 1m klines to request so a gap (e.g. after a host pause) is fully back-filled, not just the last few."""
        if not self.m1: return 400
        missing = int((now - self.m1[-1][0]) // 60) + 3
        return max(5, min(1000, missing))

    async def _binance_1m(self):
        r = await self.c.get(f"{self.cfg.binance_rest}/api/v3/klines", params={"symbol": "BTCUSDT", "interval": "1m", "limit": self.m1_limit(time.time())}, timeout=6)
        r.raise_for_status()
        rows = [[int(x[0]) // 1000, float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5]), float(x[7]), int(x[8]), float(x[9])] for x in r.json()]
        d = {b[0]: b for b in self.m1}; d.update({b[0]: b for b in rows})
        self.m1 = [d[k] for k in sorted(d)][-600:]

    async def _okx(self):
        g = lambda u, p=None: self.c.get(u, params=p, timeout=6)
        oi, fh, fr, ls, lq = await asyncio.gather(
            g("https://www.okx.com/api/v5/rubik/stat/contracts/open-interest-volume", {"ccy": "BTC", "period": "5m"}),
            g("https://www.okx.com/api/v5/public/funding-rate-history", {"instId": "BTC-USDT-SWAP", "limit": 3}),
            g("https://www.okx.com/api/v5/public/funding-rate", {"instId": "BTC-USDT-SWAP"}),
            g("https://www.okx.com/api/v5/rubik/stat/contracts/long-short-account-ratio", {"ccy": "BTC", "period": "5m"}),
            g("https://www.okx.com/api/v5/public/liquidation-orders", {"instType": "SWAP", "uly": "BTC-USDT", "state": "filled", "limit": 100}))
        self.derivs["oi_series"] = sorted((int(x[0]) // 1000, float(x[1])) for x in oi.json()["data"][:60])
        self.derivs["funding_series"] = sorted((int(x["fundingTime"]) // 1000, float(x.get("realizedRate") or x["fundingRate"])) for x in fh.json()["data"])
        fj = fr.json()["data"][0]; self.derivs["funding_current"] = float(fj["fundingRate"]); self.derivs["next_funding_ts"] = int(fj["fundingTime"]) // 1000
        l = ls.json()["data"]; self.derivs["long_short_ratio"] = float(l[0][1]) if l else None
        self.derivs["long_short_ratio_1h_ago"] = float(l[12][1]) if len(l) > 12 else None
        det = (lq.json().get("data") or [{}])[0].get("details", [])
        now = time.time(); w = [d for d in det if now - int(d["ts"]) / 1000 <= 900]
        # OKX contract size for BTC-USDT-SWAP is 0.01 BTC
        self.derivs["liq_15m"] = {"long_liq_btc": round(sum(float(d["sz"]) * 0.01 for d in w if d["posSide"] == "long"), 3),
                                  "short_liq_btc": round(sum(float(d["sz"]) * 0.01 for d in w if d["posSide"] == "short"), 3), "n": len(w)}
        self.derivs["okx_ts"] = now

    async def _deribit(self):
        r = await self.c.get("https://www.deribit.com/api/v2/public/ticker", params={"instrument_name": "BTC-PERPETUAL"}, timeout=5); r.raise_for_status()
        j = r.json()["result"]
        self.derivs["deribit"] = {"mark": j["mark_price"], "index": j["index_price"], "basis_bps": round((j["mark_price"] / j["index_price"] - 1) * 1e4, 2),
                                  "funding_8h": j.get("funding_8h"), "open_interest_usd": j.get("open_interest"), "ts": time.time()}

    async def patch_gaps(self, start, end):
        """Fill missing 1s bars in [start, end) from the Binance REST 1s klines (same source as the training data). Real data only."""
        bb = self.bars["binance"]; g = bb.gaps(start, end)
        if not g: return 0
        r = await self.c.get(f"{self.cfg.binance_rest}/api/v3/klines", timeout=3,
                             params={"symbol": "BTCUSDT", "interval": "1s", "startTime": g[0] * 1000, "endTime": end * 1000 - 1, "limit": 1000})
        r.raise_for_status()
        rows = [[int(x[0]) // 1000, float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5]), int(x[8]), float(x[9])] for x in r.json()]
        rows = [x for x in rows if start <= x[0] < end and x[0] in set(g)]
        bb.patch(rows); return len(rows)

    async def _roller(self):
        while self.running:
            now = time.time()
            for b in self.bars.values(): b.roll(now)
            await asyncio.sleep(0.25)

    def chainlink_at(self, ts):
        """Chainlink value with timestamp exactly ts (s), else None."""
        for t, v in reversed(self.chainlink):
            if int(t) == int(ts): return v
            if t < ts - 2: break
        return None

    def chainlink_last_before(self, ts):
        for t, v in reversed(self.chainlink):
            if t <= ts: return t, v
        return None

    def composite(self):
        """Display price: Chainlink (resolution source) if fresh, else median of fresh exchange prices. None if nothing fresh."""
        if self.feeds["chainlink"].fresh(5): return self.feeds["chainlink"].last_price, "chainlink"
        px = sorted(f.last_price for n, f in self.feeds.items() if n in ("binance", "coinbase", "kraken") and f.fresh(5) and f.last_price)
        return (px[len(px) // 2], "median exchanges") if px else (None, None)

    def primary_bars(self, start, end):
        """Binance bars (training-consistent). Failover to Coinbase bars if Binance has >20% gaps; flagged as degraded."""
        b = self.bars["binance"].get(start, end)
        if len(b) >= 0.8 * (end - start) or not self.bars["coinbase"].get(start, end): return b, "binance", False
        cb = self.bars["coinbase"].get(start, end)
        return (cb, "coinbase (failover)", True) if len(cb) > len(b) else (b, "binance", True)

    def primary_bars_window(self, start, cutoff):
        b = self.bars["binance"].window(start, cutoff)
        n_round = sum(1 for x in b if x[0] >= cutoff - 90)
        if n_round >= 0.8 * min(90, cutoff - start): return b, "binance", False
        cb = self.bars["coinbase"].window(start, cutoff)
        if sum(1 for x in cb if x[0] >= cutoff - 90) > n_round: return cb, "coinbase (failover)", True
        return b, "binance", True

    async def seed(self):
        """Startup backfill of recent real bars so features are available immediately."""
        now = int(time.time())
        try:
            r = await self.c.get(f"{self.cfg.binance_rest}/api/v3/klines", timeout=8, params={"symbol": "BTCUSDT", "interval": "1s", "startTime": (now - 900) * 1000, "limit": 1000})
            self.bars["binance"].patch([[int(x[0]) // 1000, float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5]), int(x[8]), float(x[9])] for x in r.json() if int(x[0]) // 1000 < now - 1])
        except Exception as e: self.logf("FEEDS", "warning", f"1s seed failed: {e}")
        try: await self._binance_1m()
        except Exception as e: self.logf("FEEDS", "warning", f"1m seed failed: {e}")

    async def start(self):
        self.running = True
        await self.seed()
        S = lambda c: self.tasks.append(asyncio.create_task(c))
        S(self._ws_loop("binance", "wss://data-stream.binance.vision/stream?streams=btcusdt@aggTrade/btcusdt@bookTicker", None, self._binance))
        S(self._ws_loop("coinbase", "wss://ws-feed.exchange.coinbase.com", {"type": "subscribe", "product_ids": ["BTC-USD"], "channels": ["matches", "ticker"]}, self._coinbase))
        S(self._ws_loop("kraken", "wss://ws.kraken.com/v2", {"method": "subscribe", "params": {"channel": "ticker", "symbol": ["BTC/USD"]}}, self._kraken))
        S(self._ws_loop("chainlink", "wss://ws-live-data.polymarket.com", {"action": "subscribe", "subscriptions": [{"topic": "crypto_prices_chainlink", "type": "*", "filters": "{\"symbol\":\"btc/usd\"}"}]}, self._chainlink))
        S(self._poll("binance_depth", self._binance_depth, 2)); S(self._poll("coinbase_book", self._coinbase_book, 3))
        S(self._poll("binance_1m", self._binance_1m, 20)); S(self._poll("okx", self._okx, 30)); S(self._poll("deribit", self._deribit, 15))
        S(self._roller())

    def snapshot(self):
        st = self.cfg.feed_stale_s
        return {k: v.to_dict(st if v.kind == "ws" else 90) for k, v in self.feeds.items()}
