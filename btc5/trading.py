"""BTC-5 automatic trading: DEMO (paper money, real Polymarket prices) and LIVE (real Polymarket CLOB orders).

Safety model
  * The process ALWAYS boots in DEMO mode. LIVE is never resumed after a restart.
  * LIVE requires wallet credentials in environment variables / HF Secrets ONLY (never SQLite, code, logs or chat):
      POLYMARKET_PRIVATE_KEY, POLYMARKET_FUNDER (proxy/funder wallet; optional for signature type 0),
      POLYMARKET_SIGNATURE_TYPE (0 = EOA, 1 = Polymarket email/Magic proxy, 2 = browser-wallet proxy; default 1 with a funder, else 0),
      optional POLYMARKET_API_KEY / POLYMARKET_API_SECRET / POLYMARKET_API_PASSPHRASE (else derived with create_or_derive_api_creds).
  * Switching DEMO -> LIVE needs an explicit confirmation that echoes the active dollar limits; if BTC5_ADMIN_TOKEN is set the
    passcode header is required on every trading write (checked in app.py).
  * Hard limits on every order: stake cap, max daily loss (worst case: realized loss today + open cost + this stake), max open
    exposure, max entry price, Polymarket minimum order size, exactly one order per round (UNIQUE round_id, row reserved
    BEFORE the order is sent). Optional env hard cap BTC5_LIVE_MAX_STAKE_USD that the UI cannot raise.
  * Emergency stop: auto-trade off, back to DEMO, cancel every open CLOB order.

Prices are never invented: DEMO fills walk the REAL Polymarket CLOB ask ladder for the predicted token at T90 (fallback: the
Gamma market's best ask for that side). No real price -> the round is SKIPPED with the reason. Fees use the market's own
feeSchedule (taker fee = shares * rate * (p*(1-p))^exponent). Settlement uses only the official Polymarket outcome."""
import asyncio, json, math, os, time
from datetime import datetime
from zoneinfo import ZoneInfo
from .config import CONFIDENCE_ORDER

ET = ZoneInfo("America/New_York")
CLOB_HOST = "https://clob.polymarket.com"
DEFAULTS = {"mode": "demo", "auto_trade": True, "stake_usd": 20.0, "min_confidence": "VERY LOW", "max_daily_loss_usd": 100.0,
            "max_exposure_usd": 200.0, "max_entry_price": 0.90, "demo_bankroll": 1000.0, "demo_epoch": 1}
OPEN = "OPEN"

def day_start_et(ts):
    d = datetime.fromtimestamp(ts, ET); return d.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()

def fee_per_share(price, rate, exponent=1.0):
    return rate * (price * (1 - price)) ** exponent if rate else 0.0

def simulate_buy(asks, usd, limit_price, rate=0.0, exponent=1.0):
    """Walk the real ascending ask ladder [(price, size)] spending up to `usd` (cost + taker fee) at prices <= limit_price.
    Returns (shares, cost, fee, fills). 0.01-share granularity like the CLOB."""
    remaining = usd; shares = cost = fee = 0.0; fills = []
    for price, size in asks:
        if price > limit_price + 1e-12 or remaining <= 1e-9: break
        unit = price + fee_per_share(price, rate, exponent)
        take = math.floor(min(size, remaining / unit) * 100) / 100
        if take <= 0: break
        f = take * fee_per_share(price, rate, exponent)
        shares += take; cost += take * price; fee += f; remaining -= take * price + f; fills.append([price, take])
    return round(shares, 2), round(cost, 6), round(fee, 6), fills

def mask(addr):
    return f"{addr[:6]}…{addr[-4:]}" if addr and len(addr) > 12 else ("set" if addr else None)

class Trader:
    def __init__(self, cfg, db, clock, poly, env=None, client_factory=None, log=None):
        self.cfg, self.db, self.clock, self.poly = cfg, db, clock, poly
        self.env = env if env is not None else os.environ
        self.client_factory = client_factory or self._make_client
        self.logf = log or (lambda *a, **k: None)
        self.client = None; self.connect_error = None; self.wallet = {"ts": 0}
        self.running = False
        s = self.settings
        if s.get("mode") != "demo":
            self.log("warning", "Restart detected while LIVE was on — booting in DEMO (live mode is never resumed automatically)")
        s["mode"] = "demo"; self._save(s)

    # ----------------------------------------------------------------- settings / creds
    @property
    def settings(self):
        s = self.db.kv_get("trading") or {}
        for k, v in DEFAULTS.items(): s.setdefault(k, v)
        return s
    def _save(self, s): self.db.kv_set("trading", s)
    def log(self, level, msg, rid=None): self.logf("TRADING", level, msg, rid)

    def creds_configured(self):
        pk = (self.env.get("POLYMARKET_PRIVATE_KEY") or "").strip()
        return bool(pk) and (bool((self.env.get("POLYMARKET_FUNDER") or "").strip()) or self.sig_type() == 0)
    def sig_type(self):
        v = self.env.get("POLYMARKET_SIGNATURE_TYPE")
        if v not in (None, ""): return int(v)
        return 1 if (self.env.get("POLYMARKET_FUNDER") or "").strip() else 0
    def hard_cap(self):
        v = self.env.get("BTC5_LIVE_MAX_STAKE_USD"); return float(v) if v else None
    def _scrub(self, s):
        s = str(s); pk = (self.env.get("POLYMARKET_PRIVATE_KEY") or "").strip()
        for secret in filter(None, [pk, pk[2:] if pk.startswith("0x") else None, self.env.get("POLYMARKET_API_SECRET"), self.env.get("POLYMARKET_API_PASSPHRASE")]):
            s = s.replace(secret, "***")
        return s[:300]

    def _make_client(self):
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import ApiCreds
        e = self.env
        c = ClobClient(CLOB_HOST, key=e["POLYMARKET_PRIVATE_KEY"].strip(), chain_id=137, signature_type=self.sig_type(),
                       funder=(e.get("POLYMARKET_FUNDER") or "").strip() or None)
        if e.get("POLYMARKET_API_KEY") and e.get("POLYMARKET_API_SECRET") and e.get("POLYMARKET_API_PASSPHRASE"):
            c.set_api_creds(ApiCreds(api_key=e["POLYMARKET_API_KEY"], api_secret=e["POLYMARKET_API_SECRET"], api_passphrase=e["POLYMARKET_API_PASSPHRASE"]))
        else:
            c.set_api_creds(c.create_or_derive_api_creds())
        return c

    async def connect(self):
        if not self.creds_configured():
            self.connect_error = "wallet credentials not configured (env vars / HF Secrets)"; return False
        if self.client is not None: return True
        try:
            self.client = await asyncio.wait_for(asyncio.to_thread(self.client_factory), 25)
            self.connect_error = None; await self.refresh_wallet(force=True); return True
        except Exception as e:
            self.client = None; self.connect_error = f"connect failed: {type(e).__name__}: {self._scrub(e)}"; return False

    async def refresh_wallet(self, force=False):
        if self.client is None or (not force and time.time() - self.wallet.get("ts", 0) < 30): return self.wallet
        try:
            from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
            params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        except Exception:
            params = None
        try:
            r = await asyncio.wait_for(asyncio.to_thread(self.client.get_balance_allowance, params), 15)
            al = r.get("allowances") or {}
            allowance = max([float(v) for v in al.values()] or [float(r.get("allowance") or 0)]) / 1e6
            self.wallet = {"ts": time.time(), "usdc_balance": float(r.get("balance") or 0) / 1e6, "usdc_allowance": allowance, "error": None}
        except Exception as e:
            self.wallet = dict(self.wallet, ts=time.time(), error=f"{type(e).__name__}: {self._scrub(e)}")
        return self.wallet

    def wallet_status(self):
        addr = None
        try:
            if self.client is not None: addr = self.client.get_address()
        except Exception: pass
        return {"configured": self.creds_configured(), "connected": self.client is not None, "connect_error": self.connect_error,
                "signer": mask(addr), "funder": mask((self.env.get("POLYMARKET_FUNDER") or "").strip()), "signature_type": self.sig_type() if self.creds_configured() else None,
                "usdc_balance": self.wallet.get("usdc_balance"), "usdc_allowance": self.wallet.get("usdc_allowance"), "balance_ts": self.wallet.get("ts") or None,
                "balance_error": self.wallet.get("error"), "hard_stake_cap_usd": self.hard_cap(),
                "admin_token_required": bool(self.env.get("BTC5_ADMIN_TOKEN")),
                "how_to_enable": ["Set POLYMARKET_PRIVATE_KEY and POLYMARKET_FUNDER (and POLYMARKET_SIGNATURE_TYPE) as environment variables on the machine running BTC-5, or as private Secrets on the Hugging Face Space — never in chat, code or the database.",
                                  "Optionally set BTC5_ADMIN_TOKEN (a passcode required for every trading change) and BTC5_LIVE_MAX_STAKE_USD (a hard stake cap).",
                                  "Restart BTC-5. It boots in DEMO. Review your limits, then press LIVE and confirm."]}

    # ----------------------------------------------------------------- user actions
    def update_settings(self, body):
        s = self.settings; out = dict(s)
        for k, v in body.items():
            if k == "auto_trade": out[k] = bool(v)
            elif k == "min_confidence":
                if v not in CONFIDENCE_ORDER: raise ValueError(f"min_confidence must be one of {CONFIDENCE_ORDER}")
                out[k] = v
            elif k in ("stake_usd", "max_daily_loss_usd", "max_exposure_usd", "max_entry_price"):
                v = float(v)
                if not math.isfinite(v) or v <= 0: raise ValueError(f"{k} must be a positive number")
                if k == "max_entry_price" and not (0.02 <= v <= 0.99): raise ValueError("max_entry_price must be between 0.02 and 0.99")
                if k == "stake_usd" and v < 1: raise ValueError("stake_usd must be at least $1")
                out[k] = round(v, 4)
            else: raise ValueError(f"unknown trading setting {k}")
        if out["max_exposure_usd"] < out["stake_usd"]: raise ValueError("max_exposure_usd must be >= stake_usd")
        if out["max_daily_loss_usd"] < out["stake_usd"]: raise ValueError("max_daily_loss_usd must be >= stake_usd")
        cap = self.hard_cap()
        if out["mode"] == "live" and cap and out["stake_usd"] > cap: raise ValueError(f"stake exceeds hard cap BTC5_LIVE_MAX_STAKE_USD=${cap}")
        self._save(out); self.log("info", f"Trading settings updated: {', '.join(f'{k}={out[k]}' for k in body)}")
        return out

    def limits(self, s=None):
        s = s or self.settings
        return {k: s[k] for k in ("stake_usd", "max_daily_loss_usd", "max_exposure_usd", "max_entry_price", "min_confidence", "auto_trade")}

    async def set_mode(self, mode, confirm=False, limits=None):
        s = self.settings
        if mode == "demo":
            s["mode"] = "demo"; self._save(s); self.log("info", "Switched to DEMO mode"); return {"ok": True, "mode": "demo"}
        if mode != "live": raise ValueError("mode must be demo|live")
        if not self.creds_configured():
            raise PermissionError("Live mode unavailable: wallet credentials are not configured. Set POLYMARKET_PRIVATE_KEY and POLYMARKET_FUNDER as environment variables / HF Secrets, then restart.")
        if not confirm: raise PermissionError("Live mode requires explicit confirmation")
        cur = self.limits(s)
        if not isinstance(limits, dict) or any(abs(float(limits.get(k, -1)) - float(cur[k])) > 1e-9 for k in ("stake_usd", "max_daily_loss_usd", "max_exposure_usd", "max_entry_price")):
            raise PermissionError("Confirmation does not match the active dollar limits — reload and confirm again")
        cap = self.hard_cap()
        if cap and s["stake_usd"] > cap: raise PermissionError(f"Stake ${s['stake_usd']} exceeds hard cap BTC5_LIVE_MAX_STAKE_USD=${cap}")
        if not await self.connect(): raise PermissionError(f"Could not connect to Polymarket: {self.connect_error}")
        s = self.settings; s["mode"] = "live"; s["live_since"] = self.clock.now(); self._save(s)
        self.log("warning", f"LIVE MODE ENABLED by user confirmation — limits {cur}")
        return {"ok": True, "mode": "live", "limits": cur}

    async def emergency_stop(self, reason="user pressed EMERGENCY STOP"):
        s = self.settings; was = s["mode"]; s.update(auto_trade=False, mode="demo", emergency_ts=self.clock.now()); self._save(s)
        cancelled = None; err = None
        if self.client is not None:
            try: cancelled = await asyncio.wait_for(asyncio.to_thread(self.client.cancel_all), 15)
            except Exception as e: err = f"{type(e).__name__}: {self._scrub(e)}"
        self.log("error", f"EMERGENCY STOP ({reason}): auto-trade OFF, mode {was.upper()} -> DEMO, open CLOB orders cancel: {'error ' + err if err else ('sent' if self.client else 'no live client')}")
        return {"ok": True, "mode": "demo", "auto_trade": False, "cancel_result": cancelled, "cancel_error": err, "had_live_client": self.client is not None}

    def reset_demo(self, bankroll=None):
        s = self.settings
        if bankroll is not None:
            b = float(bankroll)
            if not (10 <= b <= 1e7): raise ValueError("bankroll must be between $10 and $10,000,000")
            s["demo_bankroll"] = round(b, 2)
        s["demo_epoch"] = int(s.get("demo_epoch", 1)) + 1; self._save(s)
        self.log("info", f"Demo balance reset to ${s['demo_bankroll']:,.2f} (epoch {s['demo_epoch']}; earlier demo trades kept in history)")
        return self.summary()

    # ----------------------------------------------------------------- accounting
    def _scope(self, mode):
        s = self.settings
        return ("mode='demo' AND epoch=?", (s["demo_epoch"],)) if mode == "demo" else ("mode='live'", ())
    def realized(self, mode, since=None):
        w, a = self._scope(mode)
        q = f"SELECT COALESCE(SUM(pnl),0) p FROM trades WHERE {w} AND status IN ('WON','LOST')" + (" AND settled_ts>=?" if since else "")
        return self.db.one(q, a + ((since,) if since else ()))["p"]
    def exposure(self, mode):
        w, a = self._scope(mode)
        return self.db.one(f"SELECT COALESCE(SUM(cost+fee),0) e FROM trades WHERE {w} AND status='OPEN'", a)["e"]
    def demo_balance(self):
        return self.settings["demo_bankroll"] + self.realized("demo") - self.exposure("demo")

    def summary(self, mode=None, recent=12):
        s = self.settings; mode = mode or s["mode"]; w, a = self._scope(mode); now = self.clock.now()
        rows = self.db.q(f"SELECT status, pnl, cost, fee, settled_ts FROM trades WHERE {w} AND status IN ('WON','LOST') ORDER BY settled_ts", a)
        wins = sum(r["status"] == "WON" for r in rows); n = len(rows)
        staked = sum((r["cost"] or 0) + (r["fee"] or 0) for r in rows); pnl = sum(r["pnl"] or 0 for r in rows)
        start = s["demo_bankroll"] if mode == "demo" else 0.0; eq = peak = start; mdd = 0.0
        for r in rows:
            eq += r["pnl"] or 0; peak = max(peak, eq); mdd = max(mdd, peak - eq)
        cnt = {r["status"]: r["n"] for r in self.db.q(f"SELECT status, COUNT(*) n FROM trades WHERE {w} GROUP BY status", a)}
        open_rows = self.db.q(f"SELECT * FROM trades WHERE {w} AND status='OPEN' ORDER BY start_ts", a)
        cur = self.db.one("SELECT * FROM trades ORDER BY start_ts DESC LIMIT 1")
        out = {"mode": s["mode"], "view_mode": mode, "auto_trade": s["auto_trade"], "limits": self.limits(s), "demo_bankroll": s["demo_bankroll"], "demo_epoch": s["demo_epoch"],
               "balance": round(self.demo_balance(), 2) if mode == "demo" else self.wallet.get("usdc_balance"),
               "balance_source": "demo bankroll + settled P&L − open cost" if mode == "demo" else ("Polymarket CLOB collateral balance" if self.wallet.get("usdc_balance") is not None else "wallet not connected"),
               "realized_pnl": round(pnl, 2), "today_pnl": round(self.realized(mode, day_start_et(now)), 2), "open_exposure": round(self.exposure(mode), 2),
               "open_positions": [self.public(r) for r in open_rows], "settled": n, "wins": wins, "losses": n - wins, "win_rate": round(wins / n, 4) if n else None,
               "roi": round(pnl / staked, 4) if staked else None, "staked": round(staked, 2), "fees": round(sum(r["fee"] or 0 for r in rows), 4), "max_drawdown": round(mdd, 2),
               "counts": cnt, "current": self.public(cur) if cur and cur["end_ts"] > now - 60 else None,
               "live": {"configured": self.creds_configured(), "connected": self.client is not None, "error": self.connect_error, "live_since": s.get("live_since") if s["mode"] == "live" else None},
               "emergency_ts": s.get("emergency_ts"), "admin_token_required": bool(self.env.get("BTC5_ADMIN_TOKEN"))}
        if recent: out["recent"] = self.trades(limit=recent)
        return out

    def trades(self, mode=None, limit=100):
        q = "SELECT * FROM trades" + (" WHERE mode=?" if mode in ("demo", "live") else "") + " ORDER BY start_ts DESC LIMIT ?"
        return [self.public(r) for r in self.db.q(q, ((mode,) if mode in ("demo", "live") else ()) + (min(int(limit), 2000),))]

    def equity(self, mode="demo"):
        s = self.settings; w, a = self._scope(mode); eq = s["demo_bankroll"] if mode == "demo" else 0.0
        pts = []
        for r in self.db.q(f"SELECT settled_ts, pnl FROM trades WHERE {w} AND status IN ('WON','LOST') ORDER BY settled_ts", a):
            eq += r["pnl"] or 0; pts.append([round(r["settled_ts"], 1), round(eq, 2)])
        return {"mode": mode, "start": s["demo_bankroll"] if mode == "demo" else 0.0, "points": pts[-500:]}

    @staticmethod
    def public(r):
        if not r: return None
        d = dict(r); d.pop("order_json", None); d.pop("token", None); return d

    # ----------------------------------------------------------------- per-round execution
    def _reserve(self, rec, mode, s):
        try:
            self.db.x("INSERT INTO trades(round_id,start_ts,end_ts,mode,epoch,side,confidence,p_side,status,created_ts) VALUES (?,?,?,?,?,?,?,?,?,?)",
                      (rec["round_id"], rec["start_ts"], rec["end_ts"], mode, s["demo_epoch"] if mode == "demo" else 0, rec.get("prediction"), rec.get("confidence"),
                       (rec.get("p_up") if rec.get("prediction") == "UP" else rec.get("p_down")), "PENDING", self.clock.now()))
            return True
        except Exception as e:
            if "UNIQUE" in str(e).upper(): return False
            raise
    def _upd(self, rid, **kw):
        self.db.x(f"UPDATE trades SET {','.join(k + '=?' for k in kw)} WHERE round_id=?", tuple(kw.values()) + (rid,))
    def _skip(self, rid, why, **kw):
        self._upd(rid, status="SKIPPED", error=why, **kw); self.log("info", f"No trade this round: {why}", rid); return self.public(self.db.one("SELECT * FROM trades WHERE round_id=?", (rid,)))

    async def entry_quote(self, market, side, T):
        """Real ask ladder for the predicted token (CLOB), else the Gamma market's best ask for that side. Never invented."""
        token = market["up_token"] if side == "UP" else market["down_token"]
        try:
            asks = await self.poly.ask_ladder(token)
            if asks: return token, asks, "Polymarket CLOB order book (ask ladder at T90)"
        except Exception as e:
            self.log("warning", f"CLOB book unavailable at T90 ({type(e).__name__}); trying Gamma price")
        try:
            m = await self.poly.market_raw(T)
            if m:
                if side == "UP": p = m.get("bestAsk")
                else: p = (1 - float(m["bestBid"])) if m.get("bestBid") not in (None, "") else None
                if p not in (None, "") and 0 < float(p) < 1: return token, [(round(float(p), 4), float("inf"))], "Polymarket Gamma best ask (CLOB book unavailable; depth unknown)"
        except Exception as e:
            self.log("warning", f"Gamma price unavailable ({type(e).__name__})")
        return token, None, None

    def preflight(self, mode, s, rec, market, now, stake):
        fails = []
        if rec.get("prediction") not in ("UP", "DOWN"): fails.append("no directional prediction")
        if rec.get("status") != "LOCKED": fails.append(f"round status {rec.get('status')} (market identity not verified or monitoring only)")
        if not market: fails.append("no verified Polymarket market")
        elif not market.get("accepting_orders", True): fails.append("market not accepting orders")
        if rec.get("confidence") in CONFIDENCE_ORDER and CONFIDENCE_ORDER.index(rec["confidence"]) < CONFIDENCE_ORDER.index(s["min_confidence"]):
            fails.append(f"confidence {rec['confidence']} below minimum {s['min_confidence']}")
        if now > rec["end_ts"] - 20: fails.append("too close to round end")
        exp = self.exposure(mode)
        if exp + stake > s["max_exposure_usd"] + 1e-9: fails.append(f"max exposure: open ${exp:.2f} + stake ${stake:.2f} > ${s['max_exposure_usd']:.2f}")
        lost_today = -min(0.0, self.realized(mode, day_start_et(now)))
        if lost_today + exp + stake > s["max_daily_loss_usd"] + 1e-9:
            fails.append(f"daily loss limit: lost ${lost_today:.2f} today + open ${exp:.2f} + stake ${stake:.2f} would exceed ${s['max_daily_loss_usd']:.2f}")
        if mode == "demo" and self.demo_balance() < stake - 1e-9: fails.append(f"demo balance ${self.demo_balance():.2f} < stake")
        if mode == "live":
            cap = self.hard_cap()
            if cap and stake > cap: fails.append(f"stake above hard cap ${cap}")
            if self.client is None: fails.append("live wallet not connected")
            bal = self.wallet.get("usdc_balance")
            if bal is not None and bal < stake: fails.append(f"wallet USDC ${bal:.2f} < stake")
        return fails

    async def on_lock(self, rec, market):
        """Called once right after the immutable T90 prediction is stored. One trade decision per round, ever."""
        s = self.settings; mode = s["mode"]; rid = rec["round_id"]
        if not s["auto_trade"]: return None
        if hasattr(market, "to_dict"): market = market.to_dict()
        if not self._reserve(rec, mode, s):
            self.log("warning", "Trade for this round already exists — duplicate order prevented", rid); return None
        now = self.clock.now(); stake = float(s["stake_usd"])
        fails = self.preflight(mode, s, rec, market, now, stake)
        if fails: return self._skip(rid, "; ".join(fails), stake=stake)
        side = rec["prediction"]
        token, asks, src = await self.entry_quote(market, side, rec["start_ts"])
        if not asks: return self._skip(rid, "no real Polymarket price available at T90 (CLOB and Gamma both failed)", stake=stake)
        best = asks[0][0]
        self._upd(rid, best_ask=best, price_source=src, token=token)
        if best > s["max_entry_price"] + 1e-9: return self._skip(rid, f"best ask {best:.3f} above max entry price {s['max_entry_price']:.2f}", stake=stake)
        rate = float(market.get("fee_rate") or 0); expo = float(market.get("fee_exponent") or 1); min_size = float(market.get("order_min_size") or 0)
        if mode == "demo":
            shares, cost, fee, fills = simulate_buy(asks, stake, s["max_entry_price"], rate, expo)
            if shares <= 0: return self._skip(rid, "no ask liquidity at or below max entry price", stake=stake)
            if shares < min_size: return self._skip(rid, f"fill of {shares} shares below Polymarket minimum order size {min_size}", stake=stake)
            avg = cost / shares
            self._upd(rid, status=OPEN, stake=stake, entry_price=round(avg, 6), shares=shares, cost=round(cost, 4), fee=round(fee, 4), placed_ts=self.clock.now(),
                      order_json=json.dumps({"fills": fills, "fee_rate": rate, "fee_exponent": expo, "partial": cost + fee < stake - 0.01}), error=None if cost + fee >= stake - 0.01 else "partial fill: book depth below stake")
            self.log("info", f"DEMO trade: BUY {side} {shares} sh @ {avg:.3f} (best ask {best:.3f}) = ${cost + fee:.2f} incl ${fee:.2f} fee — {src}", rid)
        else:
            res = await self.place_live(rid, token, side, stake, best, s, market, min_size)
            if res is None: return self.public(self.db.one("SELECT * FROM trades WHERE round_id=?", (rid,)))
        return self.public(self.db.one("SELECT * FROM trades WHERE round_id=?", (rid,)))

    async def place_live(self, rid, token, side, stake, best, s, market, min_size):
        tick = float(market.get("tick_size") or 0.01)
        limit = min(s["max_entry_price"], math.ceil((best + 2 * tick) / tick - 1e-9) * tick)
        limit = round(min(limit, 1 - tick), 4)
        if stake / limit < min_size: return self._skip(rid, f"stake ${stake} buys < minimum order size {min_size} shares", stake=stake)
        try:
            from py_clob_client.clob_types import MarketOrderArgs, OrderType, PartialCreateOrderOptions
            args = MarketOrderArgs(token_id=token, amount=round(stake, 2), side="BUY", price=limit, order_type=OrderType.FAK)
            opts = PartialCreateOrderOptions(tick_size=str(market.get("tick_size") or "0.01"), neg_risk=bool(market.get("neg_risk", False)))
            def go():
                signed = self.client.create_market_order(args, opts)
                return self.client.post_order(signed, OrderType.FAK)
            self._upd(rid, status="SUBMITTING", stake=stake, placed_ts=self.clock.now())
            resp = await asyncio.wait_for(asyncio.to_thread(go), 20)
        except Exception as e:
            self._upd(rid, status="FAILED", error=f"order rejected/failed: {type(e).__name__}: {self._scrub(e)}")
            self.log("error", f"LIVE order failed: {type(e).__name__}: {self._scrub(e)}", rid); return None
        resp = resp or {}
        oid = resp.get("orderID") or resp.get("orderId")
        if not resp.get("success") or not oid:
            self._upd(rid, status="FAILED", order_id=oid, order_json=json.dumps(resp, default=str)[:4000], error=f"exchange rejected: {self._scrub(resp.get('errorMsg') or resp)}")
            self.log("error", f"LIVE order rejected: {self._scrub(resp.get('errorMsg') or resp)}", rid); return None
        shares = float(resp.get("takingAmount") or 0); cost = float(resp.get("makingAmount") or 0)
        if shares <= 0:
            try:
                o = await asyncio.wait_for(asyncio.to_thread(self.client.get_order, oid), 10)
                shares = float(o.get("size_matched") or 0); px = float(o.get("price") or 0); cost = shares * px
            except Exception: pass
        if shares <= 0:
            self._upd(rid, status="UNFILLED", order_id=oid, order_json=json.dumps(resp, default=str)[:4000], error=f"order {resp.get('status')} with no fill (fill-and-kill)")
            self.log("warning", f"LIVE order {oid} not filled (status {resp.get('status')})", rid); return None
        self._upd(rid, status=OPEN, order_id=oid, shares=round(shares, 4), cost=round(cost, 4), fee=0.0, entry_price=round(cost / shares, 6),
                  order_json=json.dumps(resp, default=str)[:4000], error=None)
        self.log("warning", f"LIVE trade FILLED: BUY {side} {shares:.2f} sh for ${cost:.2f} (limit {limit:.2f}) order {oid}", rid)
        self.spawn_wallet_refresh()
        return resp

    def spawn_wallet_refresh(self):
        try: asyncio.get_running_loop().create_task(self.refresh_wallet(force=True))
        except RuntimeError: pass

    # ----------------------------------------------------------------- settlement
    def settle(self, rid, outcome):
        t = self.db.one("SELECT * FROM trades WHERE round_id=? AND status='OPEN'", (rid,))
        if not t or outcome not in ("UP", "DOWN"): return None
        won = t["side"] == outcome; payout = round(t["shares"] * 1.0, 4) if won else 0.0
        pnl = round(payout - (t["cost"] or 0) - (t["fee"] or 0), 4)
        self.db.x("UPDATE trades SET status=?, outcome=?, payout=?, pnl=?, settled_ts=? WHERE round_id=? AND status='OPEN'", ("WON" if won else "LOST", outcome, payout, pnl, self.clock.now(), rid))
        self.log("info", f"{t['mode'].upper()} trade settled on official outcome {outcome}: {'WON' if won else 'LOST'} {pnl:+.2f} USD"
                 + (" (winning shares must be redeemed on polymarket.com)" if t["mode"] == "live" and won else ""), rid)
        if t["mode"] == "live": self.spawn_wallet_refresh()
        return pnl

    async def sweep_once(self):
        n = 0
        for t in self.db.q("SELECT t.round_id, t.start_ts, t.end_ts, r.official_outcome FROM trades t LEFT JOIN rounds r ON r.round_id=t.round_id WHERE t.status='OPEN'"):
            if t["official_outcome"]: n += self.settle(t["round_id"], t["official_outcome"]) is not None
            elif t["end_ts"] < self.clock.now() - 600:
                try:
                    out, _ = await self.poly.resolution(t["start_ts"])
                    if out in ("UP", "DOWN"): n += self.settle(t["round_id"], out) is not None
                except Exception: pass
        # a PENDING row with no decision (process died mid-way) is closed honestly, never re-sent
        for t in self.db.q("SELECT round_id FROM trades WHERE status IN ('PENDING','SUBMITTING') AND created_ts < ?", (self.clock.now() - 120,)):
            self._upd(t["round_id"], status="FAILED", error="process interrupted before the order outcome was recorded — check Polymarket order history")
        return n

    async def run(self):
        self.running = True
        while self.running:
            try:
                await self.sweep_once()
                if self.client is not None: await self.refresh_wallet()
            except Exception as e: self.log("warning", f"settlement sweep error: {e}")
            await asyncio.sleep(15)
