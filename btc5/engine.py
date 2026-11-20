"""Timing layer + round orchestration.

For each 5-minute round T0 (Polymarket eventStartTime, validated):
  T0..T80  ANALYZING   continuous data collection, preliminary analysis (kept server-side; never exposed as a direction)
  T80..T90 FINALIZING  gap patching, final validation, payload preparation
  T90      LOCKED      ONE decision event on the synchronised clock (monotonic wait); inputs frozen at cutoff T0+90;
                       exactly one immutable DB record per round (UNIQUE + triggers); decision/publish timestamps + deviation recorded
  T300+    COMPLETED   official Polymarket resolution (Gamma 1/0 payout) verified; result + failure analysis recorded
If the process was not running (box paused) at T90 the round is recorded MISSED with the reason — never back-dated."""
import asyncio, json, math, time, collections, statistics, traceback, concurrent.futures, multiprocessing
from .config import Config, confidence_class, CONFIDENCE_ORDER
from .polymarket import round_start_for, MarketIdentificationError
from . import features as F
from .agents import specialist_reports, validator_report
from .model import load, AGENT_GROUPS
from . import training

async def wait_until(clock, target_wall, sleep=asyncio.sleep):
    """Wait until the synchronised wall clock reaches target_wall.
    Coarse phase re-reads the wall clock every <=0.5 s, so a VM suspend (during which Linux's monotonic clock does not
    advance) can never make us oversleep; the final <=250 ms approach uses the monotonic clock for precision."""
    while True:
        rem = target_wall - clock.now()
        if rem <= 0: return
        if rem > 0.25:
            await sleep(min(rem - 0.2, 0.5)); continue
        tm = clock.mono() + rem
        while True:
            r = tm - clock.mono()
            if r <= 0: return
            await sleep(r - 0.02 if r > 0.05 else min(r, 0.001))

DEFAULT_SETTINGS = {"auto_detect": True, "mode": "automatic", "feeds": {}, "model": {"pin_version": None, "retrain_every": 12},
                    "weighting": {"mode": "learned", "manual": {"TREND": 1.0, "STRUCTURE": 0.0, "ORDERFLOW": 0.0, "VOLATILITY": 1.0, "DERIVS": 0.5}},
                    "chart": {"tf": "1m", "ema": [9, 20, 50, 200], "vwap": True}, "history": {"backfill_days": 10}, "backtest": {"days": 10},
                    "notifications": {"browser_on_lock": False}}

class Brain:
    """Champion model + reliability tables. Falls back to a documented deterministic model if no trained model exists."""
    def __init__(self, db):
        self.db = db; self.model = None; self.version = None; self.arch = None; self.meta = {}
    def reload(self):
        s = self.db.kv_get("settings", {}) or {}
        v = ((s.get("model") or {}).get("pin_version")) or self.db.kv_get("champion")
        if v and v != self.version:
            m = load(self.db, v)
            if m is not None:
                r = self.db.one("SELECT kind, metrics_json FROM model_versions WHERE version=?", (v,))
                self.model, self.version, self.arch, self.meta = m, v, r["kind"], json.loads(r["metrics_json"])
        rel = self.db.kv_get("reliability")
        if rel: self.meta = dict(self.meta, **rel)
        return self.version
    def predict(self, f, info, weighting=None):
        flags = []
        if self.model is None:
            # Documented fallback hierarchy: random-walk probability from real displacement -> crowd price -> 0.5 (tie-break)
            if info.get("price") is not None: p = info["p_random_walk"]; src = "FALLBACK: random-walk model on real displacement (no trained model)"
            elif info.get("crowd_up") is not None: p = info["crowd_up"]; src = "FALLBACK: Polymarket crowd price"
            else: p = 0.5; src = "FALLBACK: no data — deterministic tie-break"
            return p, {}, src, ["No trained champion model loaded — degraded fallback"]
        reg = info.get("regime") or "SIDEWAYS CONSOLIDATION"; dq = info.get("data_quality", 0)
        p, P = self.model.predict([f], [reg], [dq])
        probs = {g: float(P[0][i]) for i, (g, _) in enumerate(AGENT_GROUPS)} if P is not None else {}
        p = float(p[0]); src = f"{self.arch} champion {self.version}"
        if weighting and weighting.get("mode") == "manual" and probs:
            w = weighting.get("manual") or {}
            z = sum(float(w.get(g, 0)) * math.log(max(1e-4, q) / max(1e-4, 1 - q)) for g, q in probs.items())
            p = 1 / (1 + math.exp(-z)); src += " with MANUAL agent weights (user override, not calibrated)"; flags.append("Manual agent weights active — probabilities are not calibrated")
        return p, probs, src, flags

class Engine:
    def __init__(self, cfg: Config, db, clock, live, poly, sleep=asyncio.sleep):
        self.cfg, self.db, self.clock, self.live, self.poly, self.sleep = cfg, db, clock, live, poly, sleep
        self.brain = Brain(db)
        self.logs = collections.deque(maxlen=500)
        self.state = {"phase": "STARTING", "round": None, "market": None, "prediction": None, "prelim": None, "open": {}, "crowd": [], "poly_book": {}}
        self.handled = set(); self.resolving = set(); self.tasks = set(); self.running = False
        self.training = {"running": False, "last": None, "last_ts": None, "error": None}
        self.resolved_since_train = 0
        self.loop_lag = collections.deque(maxlen=240)
        self.pool = concurrent.futures.ProcessPoolExecutor(max_workers=1, mp_context=multiprocessing.get_context("spawn"))
        self.trader = None            # btc5.trading.Trader (attached by app.py); trades only AFTER the immutable lock

    # ------------------------------------------------------------ util
    def log(self, source, level, message, rid=None):
        rid = rid or ((self.state.get("round") or {}).get("round_id"))
        try: self.db.event(rid, source, level, message)
        except Exception: pass
        self.logs.append({"ts": time.time(), "round_id": rid, "source": source, "level": level, "message": message})

    def spawn(self, coro):
        t = asyncio.create_task(coro); self.tasks.add(t); t.add_done_callback(self.tasks.discard); return t

    @property
    def settings(self):
        s = self.db.kv_get("settings")
        if not s: s = json.loads(json.dumps(DEFAULT_SETTINGS)); self.db.kv_set("settings", s)
        for k, v in DEFAULT_SETTINGS.items(): s.setdefault(k, v)
        return s

    # ------------------------------------------------------------ main loop
    async def run(self):
        self.running = True
        self.brain.reload()
        if self.brain.model: self.log("MODEL", "info", f"Champion model loaded: {self.brain.version}")
        else: self.log("MODEL", "warning", "No trained model yet — training job scheduled; fallback model in use until then")
        self.spawn(self.lag_monitor())
        self.recover()
        if not self.brain.model or (time.time() - (self.db.kv_get("last_train_ts") or 0)) > 3600: self.spawn(self.train(backfill=True))
        last_T = None
        while self.running:
            now = self.clock.now(); T = round_start_for(now)
            if last_T is not None and T - last_T > 300:
                # the box was suspended across whole rounds: account for every skipped round, never back-date
                for Tm in range(last_T + 300, T, 300):
                    if Tm not in self.handled:
                        self.handled.add(Tm); self._missed(Tm, "box paused/suspended — no process was running at this round's T90")
            if T not in self.handled:
                self.handled.add(T); self.spawn(self.run_round(T))
            last_T = T
            await wait_until(self.clock, T + self.cfg.round_seconds + 0.0005, self.sleep)

    async def lag_monitor(self):
        """Event-loop lag + suspend detection (wall-clock jump much larger than monotonic elapsed)."""
        self.pause_detected = False
        while self.running:
            t = time.monotonic(); w = time.time(); await asyncio.sleep(0.5)
            self.loop_lag.append((time.monotonic() - t - 0.5) * 1000)
            jump = (time.time() - w) - (time.monotonic() - t)
            if (time.time() - w) > 2.5:
                self.pause_detected = True
                try: self.live.kick(f"host pause of {time.time()-w:.0f}s")
                except Exception as e: self.log("FEEDS", "warning", f"websocket kick failed: {e}")
                self.log("SYSTEM", "warning", f"Host pause detected: wall clock advanced {time.time()-w:.1f}s during a 0.5s sleep (monotonic {time.monotonic()-t:.1f}s) — resyncing clock, feeds will reconnect")

    def recover(self):
        """Persistent-state recovery after a restart/pause: re-attach pending resolutions and account for every round the
        system was offline for (recorded MISSED with reason, last 24 h)."""
        now = self.clock.now()
        for r in self.db.q("SELECT round_id,start_ts,end_ts FROM rounds WHERE official_outcome IS NULL AND end_ts > ? AND (result IS NULL OR result='PENDING')", (now - 86400,)):
            self.spawn(self.resolve(r["round_id"], r["start_ts"], r["end_ts"]))
        last = self.db.one("SELECT MAX(start_ts) t FROM rounds")["t"]
        cur = round_start_for(now)
        if last:
            n = 0
            for T in range(max(last + 300, cur - 86400), cur, 300):
                if self._missed(T, "system offline / box paused — no process was running at this round's T90", log=False): n += 1
            if n: self.log("RECOVERY", "warning", f"Recovered after downtime: {n} rounds recorded as MISSED (system was not running)")
        self.log("RECOVERY", "info", f"Recovery check complete; {self.db.one('SELECT COUNT(*) n FROM rounds')['n']} rounds in history")

    def _missed(self, T, reason, market=None, log=True, extra=None):
        rid = f"{self.cfg.slug_prefix}{T}"
        rec = {"round_id": rid, "start_ts": T, "end_ts": T + 300, "decision_target_ts": T + 90, "status": "MISSED", "missed_reason": reason, "result": "MISSED"}
        if market: rec.update(market_id=market.market_id, question=market.question, condition_id=market.condition_id)
        if extra: rec.update(extra)
        ok = self.db.insert_round(rec)
        if ok and log: self.log("TIMING", "error", f"Round MISSED: {reason}", rid)
        if ok and T + 300 > self.clock.now() - 86400: self.spawn(self.resolve(rid, T, T + 300))
        return ok

    # ------------------------------------------------------------ one round
    async def run_round(self, T):
        cfg = self.cfg; rid = f"{cfg.slug_prefix}{T}"; T90 = T + cfg.decision_offset_s; end = T + cfg.round_seconds
        self.state.update(round={"round_id": rid, "start_ts": T, "decision_ts_target": T90, "end_ts": end}, market=None, prediction=None, prelim=None,
                          open={}, crowd=[], poly_book={}, phase="ANALYZING", market_error=None)
        ex = self.db.get_round(rid)
        if ex:
            self.log("TIMING", "warning", "Round already recorded — duplicate prediction prevented", rid)
            self.state["prediction"] = self.public(ex); self.state["phase"] = "MISSED" if ex["status"] == "MISSED" else "PREDICTION LOCKED"
            self.spawn(self.display_context(T)); return
        if self.clock.now() > T90 + cfg.late_tolerance_ms / 1000:
            self._missed(T, f"engine became active at T+{self.clock.now()-T:.0f}s, after the T90 decision point"); self.state["phase"] = "MISSED"
            self.state["prediction"] = self.public(self.db.get_round(rid)); self.spawn(self.display_context(T)); return
        S = self.settings
        self.log("TIMING", "info", f"Round opened. Decision event scheduled for {time.strftime('%H:%M:%S', time.localtime(T90))}", rid)
        market = None
        # market detection & identity validation (retry until just before T90 — also covers rounds joined late after a host pause)
        async def discover():
            nonlocal market
            while market is None and self.clock.now() < T90 - 0.3 and S.get("auto_detect", True):
                try:
                    market = await self.poly.discover(T); self.state["market"] = market.to_dict()
                    self.log("MARKET", "info", f"Market verified: {market.question} | id {market.market_id} | source {market.resolution_source}", rid)
                except MarketIdentificationError as e:
                    self.state["market_error"] = str(e); self.log("MARKET", "error", f"Market identification failed: {e}", rid); await self.sleep(3)
                except Exception as e:
                    self.state["market_error"] = f"Gamma API error: {e}"; await self.sleep(2)
        self.spawn(discover())
        crowd = []; open_task_done = False; patched = False
        while self.clock.now() < T90 - 1.5:
            el = self.clock.now() - T
            self.state["phase"] = "ANALYZING" if el < 80 else "FINALIZING"
            self.state["stage"] = ("0-30s: detect market, establish reference, initialise streams" if el < 30 else "30-60s: update indicators, compare exchanges, update agents"
                                   if el < 60 else "60-80s: specialist analysis, ensemble probabilities, data validation" if el < 80 else "80-89s: final validation, freeze inputs at T90 cutoff")
            # official open (Chainlink) - RTDS exact-second value, then Polymarket's own published openPrice
            if "chainlink_rtds" not in self.state["open"]:
                v = self.live.chainlink_at(T)
                if v: self.state["open"]["chainlink_rtds"] = v
            if not open_task_done and el > 3 and int(el) % 4 == 0:
                try:
                    j = await self.poly.chainlink_open_close(T)
                    if j.get("openPrice"): self.state["open"]["official"] = float(j["openPrice"]); open_task_done = True
                except Exception: pass
            if market and int(el) % 2 == 0:
                try:
                    up = await self.poly.book(market.up_token)
                    self.state["poly_book"] = dict(up, ts=time.time())
                    if up.get("mid") is not None: crowd.append((self.clock.now(), up["mid"])); self.state["crowd"] = crowd[-60:]
                    self.live.feeds["polymarket_clob"].last_msg = time.time()
                except Exception as e: self.live.feeds["polymarket_clob"].errors += 1; self.live.feeds["polymarket_clob"].last_error = str(e)[:120]
            if el >= 82 and not patched:
                patched = True
                try:
                    n = await self.live.patch_gaps(T - 600, int(T + 82))
                    if n: self.log("FEEDS", "warning", f"Patched {n} missing 1s bars from Binance REST (real data, pre-cutoff)", rid)
                except Exception as e: self.log("FEEDS", "warning", f"Gap patch failed: {e}", rid)
            try:
                f, info, flags = self.inputs(T, T + el, crowd)
                p, probs, src, fl2 = self.brain.predict(f, info, S.get("weighting"))
                self.state["prelim"] = {"ts": time.time(), "p": p, "probs": probs, "info": info, "f": f}   # SERVER-SIDE ONLY (redacted in API)
            except Exception as e:
                self.log("ENGINE", "error", f"analysis error: {e}", rid)
            await wait_until(self.clock, min(self.clock.now() + 1.0, T90 - 1.5), self.sleep)
        if not patched and self.clock.now() < T90 - 0.4:
            # joined very late (e.g. box resumed at T+88..89): one bounded REST patch of real pre-cutoff 1s bars, never past T90
            try:
                n = await asyncio.wait_for(self.live.patch_gaps(T - 600, int(min(T + 89, self.clock.now()))), timeout=max(0.05, T90 - 0.4 - self.clock.now()))
                if n: self.log("FEEDS", "warning", f"Patched {n} missing 1s bars from Binance REST (real data, pre-cutoff, late join)", rid)
            except Exception as e: self.log("FEEDS", "warning", f"Late gap patch skipped: {type(e).__name__}", rid)
        # ---------------- T90 decision event ----------------
        await wait_until(self.clock, T90 - 0.25, self.sleep)
        await wait_until(self.clock, T90, self.sleep)
        decision_ts = self.clock.now()
        await self.decide(T, market, crowd, decision_ts, S)
        self.spawn(self.track(T))
        if market and self.state["open"].get("official"): self.spawn(self.poll_book(T, market))
        else: self.spawn(self.display_context(T, market))   # display-only: market card, open price, CLOB book (record stays as locked)

    async def poll_book(self, T, market):
        """Post-T90 display/health polling of the Polymarket CLOB book until round end (never feeds the locked prediction)."""
        st = self.live.feeds["polymarket_clob"]
        while self.running and self.clock.now() < T + 300 and (self.state.get("round") or {}).get("start_ts") == T:
            try:
                self.state["poly_book"] = dict(await self.poly.book(market.up_token), ts=time.time()); st.last_msg = time.time(); st.connected = True
            except Exception as e: st.errors += 1; st.last_error = str(e)[:120]
            await self.sleep(3)

    async def display_context(self, T, market=None):
        """Display-only context (market identity, official open, crowd book) for a round whose prediction is already
        recorded or was locked without it (late join after a host pause). Retries until round end; never used to predict
        and never changes the immutable record."""
        m = market; tries = 0
        while self.running and self.clock.now() < T + 300 and (self.state.get("round") or {}).get("start_ts") == T:
            try:
                if m is None:
                    m = await self.poly.discover(T); self.state["market"] = m.to_dict(); self.state["market_error"] = None
                if not self.state["open"].get("official"):
                    j = await self.poly.chainlink_open_close(T)
                    if j.get("openPrice"): self.state["open"]["official"] = float(j["openPrice"])
                if m is not None and self.state["open"].get("official"):
                    break
            except Exception as e:
                self.state["market_error"] = f"display context: {type(e).__name__}: {e}"[:200]
            tries += 1
            await self.sleep(min(10, 2 + tries))
        if m is not None: await self.poll_book(T, m)

    def inputs(self, T, cutoff, crowd):
        bars, src, degraded = self.live.primary_bars_window(T - 600, cutoff)
        f, info = F.compute(T, bars, self.live.m1, [(t, p) for t, p in crowd], self.live.derivs.get("oi_series"), self.live.derivs.get("funding_series"), cutoff)
        flags = []
        if degraded: flags.append(f"Primary feed degraded — bars from {src}")
        info["bar_source"] = src
        for n in ("binance", "coinbase", "kraken", "chainlink"):
            if not self.live.feeds[n].fresh(self.cfg.feed_stale_s): flags.append(f"{n} feed stale/unavailable")
        if not self.clock.synced: flags.append("Clock not synchronised to exchange time sources")
        info["degraded"] = bool(degraded or info.get("data_quality", 0) < 0.6)
        return f, info, flags

    async def decide(self, T, market, crowd, decision_ts, S):
        cfg = self.cfg; rid = f"{cfg.slug_prefix}{T}"; T90 = T + cfg.decision_offset_s
        dev_ms = (decision_ts - T90) * 1000
        if dev_ms < 0:   # must never happen; hard guard against an early decision
            await wait_until(self.clock, T90, self.sleep); decision_ts = self.clock.now(); dev_ms = (decision_ts - T90) * 1000
        if dev_ms > cfg.late_tolerance_ms:
            self._missed(T, f"decision event fired {dev_ms:.0f} ms late (event loop/box paused) — not recorded as a prediction", market, extra={"decision_ts": decision_ts, "deviation_ms": dev_ms})
            self.state["phase"] = "MISSED"; return None
        try:
            f, info, flags = self.inputs(T, T90, [c for c in crowd if c[0] <= T90])
            p, probs, src, fl2 = self.brain.predict(f, info, S.get("weighting")); flags += fl2
            tie = None
            if p > 0.5: direction = "UP"
            elif p < 0.5: direction = "DOWN"
            else: direction, tie = F.tie_break(f, info)
            if market is None: flags.insert(0, "MARKET IDENTITY NOT VERIFIED — excluded from performance statistics")
            if S.get("mode") == "monitoring": status = "MONITORING_ONLY"
            else: status = "LOCKED" if market else "MARKET_UNVERIFIED"
            reps = specialist_reports(f, info, probs, (self.brain.meta.get("agent_reliability") or {}), {"books": self.live.books, "derivs": self.live.derivs})
            val = validator_report(p, probs, self.brain.model.weights() if self.brain.model and hasattr(self.brain.model, "weights") else {}, info, f, flags, self.brain.arch, src)
            reps["VALIDATOR"] = val
            cl = self.live.chainlink_last_before(T90)
            op = self.state["open"].get("official") or self.state["open"].get("chainlink_rtds")
            rec = {"round_id": rid, "start_ts": T, "end_ts": T + 300, "decision_target_ts": T90, "status": status,
                   "market_id": market.market_id if market else None, "condition_id": market.condition_id if market else None,
                   "question": market.question if market else None, "up_token": market.up_token if market else None, "down_token": market.down_token if market else None,
                   "resolution_source": market.resolution_source if market else None, "rules": market.rules if market else None,
                   "decision_ts": decision_ts, "deviation_ms": round(dev_ms, 3), "cutoff_ts": T90,
                   "prediction": direction, "p_up": round(p, 6), "p_down": round(1 - p, 6), "edge": round(abs(p - 0.5) * 2, 4), "confidence": confidence_class(p),
                   "disagreement": val.get("disagreement"), "data_quality": info.get("data_quality"), "degraded": int(bool(info.get("degraded") or self.brain.model is None)),
                   "regime": info.get("regime"), "model_version": src, "tie_break": tie, "open_price": op,
                   "open_source": "Polymarket official (Chainlink)" if self.state["open"].get("official") else ("Chainlink RTDS at T0" if op else None),
                   "price_at_prediction": info.get("price"), "chainlink_at_prediction": cl[1] if cl else None, "crowd_up": info.get("crowd_up"),
                   "agents_json": json.dumps(reps, default=str), "evidence_json": json.dumps({"info": info, "flags": flags, "features": f}, default=str), "result": "PENDING"}
            if not self.db.insert_round(rec):
                self.log("TIMING", "error", "Duplicate prediction attempt blocked by database", rid); return None
            pub = self.clock.now()
            self.db.x("UPDATE rounds SET published_ts=?, publish_latency_ms=? WHERE round_id=? AND published_ts IS NULL", (pub, round((pub - decision_ts) * 1000, 3), rid))
            self.db.x("INSERT OR REPLACE INTO live_features VALUES (?,?,?,?)", (rid, T, json.dumps({"f": f, "books": self.live.books, "derivs": {k: v for k, v in self.live.derivs.items() if not k.endswith("series")}}, default=str), None))
            self.state["prediction"] = self.public(self.db.get_round(rid)); self.state["phase"] = "PREDICTION LOCKED"; self.state["prelim"] = None
            self.log("PREDICTION", "info", f"LOCKED {direction} · P(UP) {p:.3f} · {confidence_class(p)} · deviation {dev_ms:+.1f} ms · {status}", rid)
            if self.trader is not None: self.spawn(self._trade(rec, market))
            return rec
        except Exception as e:
            self.log("ENGINE", "error", f"decision failure: {e}\n{traceback.format_exc()[-600:]}", rid)
            self._missed(T, f"decision computation failed: {e}", market); return None

    async def _trade(self, rec, market):
        try: await self.trader.on_lock(rec, market)
        except Exception as e: self.log("TRADING", "error", f"trade decision failed: {type(e).__name__}: {e}", rec["round_id"])

    async def track(self, T):
        await self.resolve(f"{self.cfg.slug_prefix}{T}", T, T + 300)

    async def resolve(self, rid, T, end):
        if rid in self.resolving: return
        self.resolving.add(rid)
        try:
            await wait_until(self.clock, end + 3, self.sleep)
            deadline = end + self.cfg.resolution_timeout_s
            while self.running:
                try:
                    outcome, raw = await self.poly.resolution(T)
                except Exception as e: outcome, raw = None, {"error": str(e)}
                if outcome in ("UP", "DOWN"):
                    oc = {}
                    try: oc = await self.poly.chainlink_open_close(T)
                    except Exception: pass
                    rec = self.db.get_round(rid)
                    valid = rec and rec["status"] == "LOCKED"
                    result = ("CORRECT" if rec["prediction"] == outcome else "INCORRECT") if valid else rec["result"] if rec and rec["status"] == "MISSED" else "EXCLUDED"
                    fa = self.failure_analysis(rec, outcome) if result == "INCORRECT" else None
                    self.db.x("UPDATE rounds SET official_outcome=?, official_open=?, official_close=?, resolved_ts=?, resolution_json=?, result=?, failure_analysis=? WHERE round_id=? AND official_outcome IS NULL",
                              (outcome, oc.get("openPrice"), oc.get("closePrice"), time.time(), json.dumps(raw), result, json.dumps(fa) if fa else None, rid))
                    if rec and rec.get("agents_json"):
                        for g, a in json.loads(rec["agents_json"]).items():
                            if a.get("direction"): self.db.x("INSERT OR IGNORE INTO agent_results VALUES (?,?,?,?,?,?)", (rid, g, a["direction"], a.get("p_up"), int(a["direction"] == outcome), time.time()))
                    self.db.x("UPDATE live_features SET outcome=? WHERE round_id=?", (outcome, rid))
                    if self.trader is not None:
                        try: self.trader.settle(rid, outcome)
                        except Exception as e: self.log("TRADING", "error", f"settlement failed: {e}", rid)
                    if valid:
                        so, sc = oc.get("openPrice"), oc.get("closePrice")
                        note = "" if so is None or sc is None or (sc >= so) == (outcome == "UP") else " — note: spot close vs open points the other way; the contract settles on Chainlink's 60s TWAP, official outcome used"
                        self.log("RESOLUTION", "info", f"Official result {outcome} (Polymarket Gamma, closed + UMA resolved; Chainlink spot open {so} close {sc}) — prediction {result}{note}", rid)
                    if self.state.get("prediction") and self.state["prediction"]["round_id"] == rid:
                        self.state["prediction"] = self.public(self.db.get_round(rid)); self.state["phase"] = "ROUND COMPLETED"
                    self.resolved_since_train += 1
                    if self.resolved_since_train >= int((self.settings.get("model") or {}).get("retrain_every", self.cfg.retrain_every_rounds)):
                        self.spawn(self.train(backfill=True))
                    return result
                if self.clock.now() > deadline:
                    self.db.x("UPDATE rounds SET result='UNVERIFIED' WHERE round_id=? AND official_outcome IS NULL", (rid,))
                    self.log("RESOLUTION", "warning", "Official resolution not available within timeout — UNVERIFIED", rid); return "UNVERIFIED"
                await self.sleep(10)
        finally:
            self.resolving.discard(rid)

    def failure_analysis(self, rec, outcome):
        ev = json.loads(rec["evidence_json"] or "{}"); f = ev.get("features", {}); info = ev.get("info", {})
        reasons = []
        if (rec.get("data_quality") or 0) < 0.7 or rec.get("degraded"): reasons.append("Incorrect/incomplete market data (degraded inputs)")
        if f.get("breakout") and (f["breakout"] > 0) != (outcome == "UP"): reasons.append("False breakout")
        if f.get("mom30") and (f["mom30"] > 0) == (rec["prediction"] == "UP"): reasons.append("Momentum reversal after T90")
        bars = self.live.bars["binance"].get(rec["start_ts"] + 90, rec["end_ts"])
        if len(bars) > 60 and info.get("sigma_1s"):
            r = [math.log(b[4] / a[4]) for a, b in zip(bars[:-1], bars[1:])]
            if statistics.pstdev(r) > 2 * info["sigma_1s"]: reasons.append("Unexpected volatility after T90")
        if (rec.get("disagreement") or 0) >= 0.8: reasons.append("Model disagreement among specialists")
        if rec.get("confidence") in ("HIGH", "VERY HIGH"): reasons.append("Poor probability calibration (high-confidence miss)")
        if (rec.get("regime") or "").startswith("STRONG") and ("BULLISH" in rec["regime"]) != (outcome == "UP"): reasons.append("Market regime change")
        if not reasons: reasons.append("Low-edge round: outcome within normal noise for the stated probability")
        return reasons

    async def train(self, backfill=True, days=None):
        if self.training["running"]: return
        self.training.update(running=True, started=time.time(), error=None)
        self.log("MODEL", "info", "Training job started (separate process): incremental real-data backfill + walk-forward + champion/challenger")
        try:
            out = await asyncio.get_running_loop().run_in_executor(self.pool, training.job, backfill, days)
            self.training.update(last=out, last_ts=time.time()); self.db.kv_set("last_train_ts", time.time()); self.resolved_since_train = 0
            v = self.brain.reload()
            self.log("MODEL", "info", f"Training job finished: {out.get('promoted') and 'promoted ' + out['promoted'] or (out.get('challenger') or {}).get('decision') or out.get('error')}; champion {v}")
        except Exception as e:
            self.training["error"] = str(e); self.log("MODEL", "error", f"Training job failed: {e}")
        finally:
            self.training["running"] = False

    @staticmethod
    def public(rec):
        if not rec: return None
        keep = ("round_id", "start_ts", "end_ts", "status", "prediction", "p_up", "p_down", "edge", "confidence", "decision_ts", "decision_target_ts", "deviation_ms",
                "publish_latency_ms", "data_quality", "degraded", "regime", "model_version", "tie_break", "open_price", "open_source", "price_at_prediction",
                "chainlink_at_prediction", "crowd_up", "disagreement", "missed_reason", "official_outcome", "official_open", "official_close", "result", "question", "market_id")
        out = {k: rec.get(k) for k in keep}
        out["agents"] = json.loads(rec["agents_json"]) if rec.get("agents_json") else None
        ev = json.loads(rec["evidence_json"]) if rec.get("evidence_json") else {}
        out["flags"] = ev.get("flags"); out["failure_analysis"] = json.loads(rec["failure_analysis"]) if rec.get("failure_analysis") else None
        return out
