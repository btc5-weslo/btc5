"""Automatic trading: demo fills at real (book) prices, settlement P&L, live-mode safety gates."""
import asyncio, json, os, tempfile, time
import pytest
from btc5.config import Config
from btc5.db import DB
from btc5.trading import Trader, simulate_buy, DEFAULTS
from helpers import FakeClock, FakeLive, FakePoly, make_sleep

T = 1_800_000_000 // 300 * 300
MK = {"up_token": "u", "down_token": "d", "accepting_orders": True, "fee_rate": 0.0, "fee_exponent": 1.0, "order_min_size": 5, "tick_size": "0.01"}

class Poly:
    def __init__(self, ladder=None, gamma=None, fail_book=False, outcome=None):
        self.ladder = ladder if ladder is not None else {"u": [(0.55, 1000)], "d": [(0.47, 1000)]}; self.gamma = gamma; self.fail = fail_book; self.outcome = outcome
    async def ask_ladder(self, tok):
        if self.fail: raise RuntimeError("book down")
        return self.ladder.get(tok, [])
    async def market_raw(self, T): return self.gamma
    async def resolution(self, T): return self.outcome, {}

class FakeClient:
    def __init__(self, resp=None, boom=False):
        self.orders = []; self.cancelled = 0; self.resp = resp; self.boom = boom
    def get_address(self): return "0x1234567890abcdef1234567890abcdef12345678"
    def get_balance_allowance(self, params): return {"balance": "500000000", "allowances": {"x": "1000000000"}}
    def create_market_order(self, args, opts): self.orders.append(args); return {"signed": args}
    def post_order(self, signed, ot):
        if self.boom: raise RuntimeError("network")
        return self.resp or {"success": True, "orderID": "0xabc", "status": "matched", "makingAmount": "20", "takingAmount": "35.71"}
    def cancel_all(self): self.cancelled += 1; return {"canceled": ["0xabc"]}
    def get_order(self, oid): return {"size_matched": "0", "price": "0.56"}

ENV = {"POLYMARKET_PRIVATE_KEY": "0x" + "11" * 32, "POLYMARKET_FUNDER": "0x" + "22" * 20, "POLYMARKET_SIGNATURE_TYPE": "1"}

def mk(env=None, poly=None, client=None, t=T + 90.01):
    cfg = Config(); db = DB(os.path.join(tempfile.mkdtemp(), "t.db")); clk = FakeClock(t)
    fc = client or FakeClient()
    tr = Trader(cfg, db, clk, poly or Poly(), env=dict(env or {}), client_factory=lambda: fc)
    return tr, db, clk, fc

def rec(T0=T, pred="UP", conf="HIGH", status="LOCKED", p_up=0.7):
    rid = f"btc-updown-5m-{T0}"
    return {"round_id": rid, "start_ts": T0, "end_ts": T0 + 300, "prediction": pred, "confidence": conf, "status": status, "p_up": p_up, "p_down": 1 - p_up}

def put_round(db, r, outcome=None):
    db.insert_round({"round_id": r["round_id"], "start_ts": r["start_ts"], "end_ts": r["end_ts"], "decision_target_ts": r["start_ts"] + 90, "status": r["status"], "prediction": r["prediction"], "result": "PENDING"})
    if outcome: db.x("UPDATE rounds SET official_outcome=? WHERE round_id=?", (outcome, r["round_id"]))

run = asyncio.run

# ---------------------------------------------------------------- defaults & demo
def test_defaults_demo_autotrade_every_round():
    tr, *_ = mk(); s = tr.settings
    assert s["mode"] == "demo" and s["auto_trade"] is True and s["min_confidence"] == "VERY LOW"
    assert s["stake_usd"] == 20 and s["demo_bankroll"] == 1000 and s["max_daily_loss_usd"] == 100 and s["max_exposure_usd"] == 200 and s["max_entry_price"] == 0.9

def test_demo_trade_uses_real_ask_and_payout_math():
    tr, db, clk, _ = mk()
    t = run(tr.on_lock(rec(), MK))
    assert t["status"] == "OPEN" and t["mode"] == "demo" and t["side"] == "UP"
    assert t["entry_price"] == pytest.approx(0.55) and t["best_ask"] == 0.55
    assert t["shares"] == pytest.approx(36.36, abs=0.01)       # floor(20/0.55, 0.01)
    assert "CLOB" in t["price_source"]
    pnl = tr.settle(t["round_id"], "UP")
    assert pnl == pytest.approx(36.36 - 36.36 * 0.55, abs=1e-3)
    s = tr.summary("demo")
    assert s["wins"] == 1 and s["win_rate"] == 1 and s["balance"] == pytest.approx(1000 + pnl, abs=0.01)

def test_demo_loss_settles_to_zero_payout_and_drawdown():
    tr, db, *_ = mk()
    t = run(tr.on_lock(rec(pred="DOWN"), MK))
    assert t["entry_price"] == pytest.approx(0.47)
    pnl = tr.settle(t["round_id"], "UP")
    r = db.one("SELECT * FROM trades WHERE round_id=?", (t["round_id"],))
    assert r["status"] == "LOST" and r["payout"] == 0 and pnl == pytest.approx(-r["cost"])
    s = tr.summary("demo"); assert s["max_drawdown"] == pytest.approx(-pnl, abs=0.01) and s["losses"] == 1
    assert tr.settle(t["round_id"], "DOWN") is None          # settles exactly once

def test_fees_from_market_fee_schedule():
    shares, cost, fee, fills = simulate_buy([(0.5, 1000)], 20, 0.9, rate=0.07, exponent=1)
    assert fee == pytest.approx(shares * 0.07 * 0.25, abs=1e-6) and cost + fee <= 20 + 1e-9

def test_ladder_walk_partial_and_price_cap():
    shares, cost, fee, fills = simulate_buy([(0.5, 10), (0.6, 10), (0.95, 100)], 20, 0.9)
    assert shares == 20 and cost == pytest.approx(11) and len(fills) == 2

def test_skip_when_ask_above_max_entry_price():
    tr, *_ = mk(poly=Poly(ladder={"u": [(0.95, 100)]}))
    t = run(tr.on_lock(rec(), MK)); assert t["status"] == "SKIPPED" and "max entry price" in t["error"]

def test_no_real_price_means_skip_never_fake():
    tr, *_ = mk(poly=Poly(fail_book=True, gamma=None))
    t = run(tr.on_lock(rec(), MK)); assert t["status"] == "SKIPPED" and "no real Polymarket price" in t["error"] and t["entry_price"] is None

def test_gamma_fallback_when_clob_down():
    tr, *_ = mk(poly=Poly(fail_book=True, gamma={"bestAsk": "0.61", "bestBid": "0.59"}))
    t = run(tr.on_lock(rec(pred="DOWN"), MK))
    assert t["status"] == "OPEN" and t["entry_price"] == pytest.approx(0.41) and "Gamma" in t["price_source"]

def test_min_confidence_filter_and_unverified_market():
    tr, *_ = mk(); tr.update_settings({"min_confidence": "HIGH"})
    assert run(tr.on_lock(rec(conf="LOW"), MK))["status"] == "SKIPPED"
    t = run(tr.on_lock(rec(T0=T + 300, status="MARKET_UNVERIFIED"), None)); assert t["status"] == "SKIPPED"

def test_auto_trade_off_places_nothing():
    tr, db, *_ = mk(); tr.update_settings({"auto_trade": False})
    assert run(tr.on_lock(rec(), MK)) is None and db.one("SELECT COUNT(*) n FROM trades")["n"] == 0

def test_one_order_per_round_idempotent():
    tr, db, clk, fc = mk(env=ENV); run(tr.connect())
    async def go(): return await asyncio.gather(*[tr.on_lock(rec(), MK) for _ in range(5)])
    run(go())
    assert db.one("SELECT COUNT(*) n FROM trades")["n"] == 1
    tr2, *_ = mk(env=ENV)
    tr2.db = db; assert run(tr2.on_lock(rec(), MK)) is None    # even a fresh process cannot place a second order

def test_exposure_and_daily_loss_limits():
    tr, db, clk, _ = mk(); tr.update_settings({"stake_usd": 20, "max_exposure_usd": 40, "max_daily_loss_usd": 50})
    a = run(tr.on_lock(rec(T0=T), MK)); b = run(tr.on_lock(rec(T0=T + 300), MK)); c = run(tr.on_lock(rec(T0=T + 600), MK))
    assert a["status"] == b["status"] == "OPEN" and c["status"] == "SKIPPED" and "max exposure" in c["error"]
    tr.settle(a["round_id"], "DOWN"); tr.settle(b["round_id"], "DOWN")        # lost ~$40 today
    d = run(tr.on_lock(rec(T0=T + 900), MK)); assert d["status"] == "SKIPPED" and "daily loss" in d["error"]

def test_demo_reset_new_epoch_keeps_history():
    tr, db, *_ = mk(); t = run(tr.on_lock(rec(), MK)); tr.settle(t["round_id"], "DOWN")
    s = tr.reset_demo(500); assert s["balance"] == 500 and s["settled"] == 0
    assert db.one("SELECT COUNT(*) n FROM trades")["n"] == 1
    with pytest.raises(Exception): db.x("DELETE FROM trades")

def test_sweep_settles_from_official_outcome():
    tr, db, clk, _ = mk(); r = rec(); put_round(db, r)
    run(tr.on_lock(r, MK)); db.x("UPDATE rounds SET official_outcome='UP' WHERE round_id=?", (r["round_id"],))
    assert run(tr.sweep_once()) == 1 and db.one("SELECT status FROM trades")["status"] == "WON"

def test_engine_places_demo_trade_at_lock_and_settles_on_resolution():
    from btc5.engine import Engine
    cfg = Config(); db = DB(os.path.join(tempfile.mkdtemp(), "e.db")); clk = FakeClock(T + 1)
    class P(FakePoly):
        async def ask_ladder(self, tok): return [(0.52, 500)]
        async def market_raw(self, T): return None
    poly = P(cfg, outcome="UP")
    eng = Engine(cfg, db, clk, FakeLive(T), poly, sleep=make_sleep(clk)); eng.running = True
    eng.trader = Trader(cfg, db, clk, poly, env={}, log=eng.log)
    async def go():
        await eng.run_round(T)
        for _ in range(50): await asyncio.sleep(0)
        for t in list(eng.tasks): t.cancel()
        await asyncio.sleep(0); eng.resolving.clear()
        await eng.resolve(f"{cfg.slug_prefix}{T}", T, T + 300)
    run(go())
    t = db.one("SELECT * FROM trades"); r = db.get_round(f"{cfg.slug_prefix}{T}")
    assert t and t["mode"] == "demo" and t["side"] == r["prediction"] and t["entry_price"] == pytest.approx(0.52)
    assert t["placed_ts"] >= r["decision_ts"]                       # never before the T90 lock
    assert t["status"] == ("WON" if r["prediction"] == "UP" else "LOST")

# ---------------------------------------------------------------- live safety gates
def test_always_boots_in_demo_even_if_live_was_saved():
    tr, db, *_ = mk(env=ENV); s = tr.settings; s["mode"] = "live"; tr._save(s)
    tr2 = Trader(tr.cfg, db, tr.clock, tr.poly, env=ENV, client_factory=lambda: FakeClient())
    assert tr2.settings["mode"] == "demo"

def test_live_refused_without_credentials():
    tr, *_ = mk(env={})
    with pytest.raises(PermissionError, match="not configured"): run(tr.set_mode("live", True, tr.limits()))
    assert tr.settings["mode"] == "demo"

def test_live_requires_confirmation_matching_limits():
    tr, *_ = mk(env=ENV)
    with pytest.raises(PermissionError): run(tr.set_mode("live", False, tr.limits()))
    bad = dict(tr.limits(), stake_usd=999)
    with pytest.raises(PermissionError, match="limits"): run(tr.set_mode("live", True, bad))
    assert tr.settings["mode"] == "demo"
    r = run(tr.set_mode("live", True, tr.limits())); assert r["mode"] == "live" and tr.settings["mode"] == "live"
    run(tr.set_mode("demo")); assert tr.settings["mode"] == "demo"          # one click back

def test_live_order_flow_and_hard_cap():
    env = dict(ENV, BTC5_LIVE_MAX_STAKE_USD="25")
    tr, db, clk, fc = mk(env=env, poly=Poly(ladder={"u": [(0.56, 1000)]}))
    run(tr.set_mode("live", True, tr.limits()))
    t = run(tr.on_lock(rec(), MK))
    assert t["status"] == "OPEN" and t["mode"] == "live" and t["order_id"] == "0xabc" and t["shares"] == pytest.approx(35.71)
    o = fc.orders[0]; assert o.side == "BUY" and o.amount == 20 and o.price <= 0.9 and o.token_id == "u" and o.order_type == "FAK"
    with pytest.raises(ValueError, match="hard cap"): tr.update_settings({"stake_usd": 30})
    assert tr.settle(t["round_id"], "UP") == pytest.approx(35.71 - 20)

def test_live_order_failure_recorded_not_retried():
    tr, db, clk, fc = mk(env=ENV, client=FakeClient(boom=True)); run(tr.set_mode("live", True, tr.limits()))
    t = run(tr.on_lock(rec(), MK)); assert t["status"] == "FAILED" and "network" in t["error"]
    assert run(tr.on_lock(rec(), MK)) is None and len(fc.orders) == 1
    tr2, db2, _, fc2 = mk(env=ENV, client=FakeClient(resp={"success": False, "errorMsg": "not enough balance"})); run(tr2.set_mode("live", True, tr2.limits()))
    t2 = run(tr2.on_lock(rec(), MK)); assert t2["status"] == "FAILED" and "not enough balance" in t2["error"]

def test_live_daily_loss_blocks_orders():
    tr, db, clk, fc = mk(env=ENV); tr.update_settings({"max_daily_loss_usd": 30}); run(tr.set_mode("live", True, tr.limits()))
    a = run(tr.on_lock(rec(T0=T), MK)); tr.settle(a["round_id"], "DOWN")          # -$20 live today
    b = run(tr.on_lock(rec(T0=T + 300), MK)); assert b["status"] == "SKIPPED" and "daily loss" in b["error"] and len(fc.orders) == 1

def test_emergency_stop_cancels_and_reverts():
    tr, db, clk, fc = mk(env=ENV); run(tr.set_mode("live", True, tr.limits()))
    r = run(tr.emergency_stop())
    assert r["mode"] == "demo" and r["auto_trade"] is False and fc.cancelled == 1
    assert tr.settings["mode"] == "demo" and tr.settings["auto_trade"] is False
    assert run(tr.on_lock(rec(), MK)) is None

def test_wallet_status_never_leaks_key():
    tr, *_ = mk(env=ENV); run(tr.connect()); w = tr.wallet_status()
    blob = json.dumps(w)
    assert w["configured"] and w["connected"] and w["usdc_balance"] == 500 and "11111111" not in blob and "2222222222" not in blob
    assert w["funder"].startswith("0x2222") and "…" in w["funder"]

def test_key_scrubbed_from_errors():
    pk = ENV["POLYMARKET_PRIVATE_KEY"]
    def boom(): raise RuntimeError(f"bad key {pk}")
    cfg = Config(); db = DB(os.path.join(tempfile.mkdtemp(), "t.db"))
    tr = Trader(cfg, db, FakeClock(T), Poly(), env=ENV, client_factory=boom)
    assert not run(tr.connect()) and pk not in tr.connect_error and pk[2:] not in tr.connect_error

# ---------------------------------------------------------------- HTTP layer
@pytest.fixture
def api(monkeypatch):
    from fastapi.testclient import TestClient
    import btc5.app as A
    tr, db, clk, fc = mk(env=ENV)
    monkeypatch.setattr(A, "G", dict(A.G, trader=tr)); monkeypatch.setattr(A, "READONLY", False)
    return TestClient(A.app), tr, A, monkeypatch

def test_api_admin_token_required_on_writes(api):
    c, tr, A, mp = api; mp.setenv("BTC5_ADMIN_TOKEN", "4821")
    assert c.post("/api/trading/settings", json={"stake_usd": 5}).status_code == 401
    assert c.post("/api/trading/mode", json={"mode": "live", "confirm": True, "limits": tr.limits()}).status_code == 401
    assert c.post("/api/trading/demo/reset", json={}).status_code == 401
    assert c.post("/api/trading/settings", json={"stake_usd": 5}, headers={"X-BTC5-Token": "wrong"}).status_code == 401
    r = c.post("/api/trading/settings", json={"stake_usd": 5}, headers={"X-BTC5-Token": "4821"}); assert r.status_code == 200 and r.json()["limits"]["stake_usd"] == 5
    assert c.post("/api/trading/emergency-stop").status_code == 200                 # fail-safe always allowed

def test_api_mode_switch_and_validation(api):
    c, tr, A, mp = api; mp.delenv("BTC5_ADMIN_TOKEN", raising=False)
    assert c.post("/api/trading/settings", json={"min_confidence": "SUPER"}).status_code == 400
    assert c.post("/api/trading/settings", json={"evil": 1}).status_code == 400
    assert c.post("/api/trading/mode", json={"mode": "live"}).status_code == 409
    r = c.post("/api/trading/mode", json={"mode": "live", "confirm": True, "limits": tr.limits()}); assert r.status_code == 200 and r.json()["mode"] == "live"
    assert c.post("/api/trading/mode", json={"mode": "demo"}).json()["mode"] == "demo"
    w = c.get("/api/trading/wallet").json(); assert "POLYMARKET_PRIVATE_KEY" not in json.dumps(w.get("signer") or "") and w["configured"]

def test_public_readonly_blocks_trading_without_token(monkeypatch):
    from fastapi.testclient import TestClient
    import btc5.app as A
    tr, *_ = mk(env=ENV)
    monkeypatch.setattr(A, "G", dict(A.G, trader=tr)); monkeypatch.setattr(A, "READONLY", True); monkeypatch.delenv("BTC5_ADMIN_TOKEN", raising=False)
    c = TestClient(A.app)
    assert c.post("/api/trading/mode", json={"mode": "live", "confirm": True, "limits": tr.limits()}).status_code == 403
    assert c.post("/api/trading/settings", json={"stake_usd": 5}).status_code == 403
    assert c.post("/api/trading/emergency-stop").status_code == 200
