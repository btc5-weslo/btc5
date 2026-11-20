"""Regression tests for the Nov 17 audit: late joins after a host pause, evidence-text crash, half-open websockets after
a pause, 1m kline gaps after a pause, and supervisor exit-code logging."""
import asyncio, os, tempfile, time, json, re
import httpx, pytest, websockets
from btc5.config import Config
from btc5.db import DB
from btc5.engine import Engine
from btc5.live import LiveData
from btc5.agents import specialist_reports
from btc5 import features as F
from helpers import FakeClock, make_sleep, FakeLive, FakePoly, market

T = 1_800_000_000 // 300 * 300

def mk(t_start, poly=None):
    cfg = Config(); cfg.db_path = os.path.join(tempfile.mkdtemp(), "t.db")
    db = DB(cfg.db_path); clk = FakeClock(t_start); live = FakeLive(T)
    eng = Engine(cfg, db, clk, live, poly or FakePoly(cfg), sleep=make_sleep(clk)); eng.running = True
    return cfg, db, clk, live, eng

async def finish(eng):
    for t in list(eng.tasks): t.cancel()
    await asyncio.sleep(0)

def test_round_joined_at_T87_after_pause_still_verifies_market():
    # 03:55 round: box resumed at T+86 s; discovery used to stop at T+85 -> MARKET_UNVERIFIED even though the market existed
    cfg, db, clk, live, eng = mk(T + 87.0)
    async def go(): await eng.run_round(T); await finish(eng)
    asyncio.run(go())
    r = db.get_round(f"{cfg.slug_prefix}{T}")
    assert r["status"] == "LOCKED" and r["market_id"] == "1" and r["decision_ts"] >= T + 90

def test_round_joined_at_T89_with_no_in_round_bars_still_locks():
    # 14:50 round: no 1s bars inside the round at cutoff crashed the evidence text -> round MISSED
    cfg, db, clk, live, eng = mk(T + 89.3)
    live.all = [b for b in live.all if b[0] < T]
    async def go(): await eng.run_round(T); await finish(eng)
    asyncio.run(go())
    r = db.get_round(f"{cfg.slug_prefix}{T}")
    assert r["status"] in ("LOCKED", "MARKET_UNVERIFIED") and r["prediction"] in ("UP", "DOWN"), r["missed_reason"]
    assert r["decision_ts"] >= T + 90

def test_specialist_reports_never_raise_on_empty_round():
    f, info = F.compute(T, [], [], cutoff=T + 90)
    probs = {g: 0.5 for g in ("TREND", "STRUCTURE", "ORDERFLOW", "VOLATILITY", "DERIVS")}
    reps = specialist_reports(f, info, probs, {}, {"books": {}, "derivs": {}})
    assert set(probs) <= set(reps) and all(reps[g]["p_up"] == 0.5 and reps[g]["evidence"] for g in probs)
    json.dumps(reps)

class FlakyPoly(FakePoly):
    """Gamma unreachable until after T90 (e.g. network not back yet after a resume)."""
    def __init__(self, cfg, clk): super().__init__(cfg); self.clk = clk
    async def discover(self, T):
        if self.clk.now() < T + 100: raise RuntimeError("gamma timeout")
        return market(T, self.cfg)

def test_unverified_round_gets_display_context_after_T90_without_changing_record():
    cfg = Config(); cfg.db_path = os.path.join(tempfile.mkdtemp(), "t.db"); db = DB(cfg.db_path); clk = FakeClock(T + 1)
    poly = FlakyPoly(cfg, clk); eng = Engine(cfg, db, clk, FakeLive(T), poly, sleep=make_sleep(clk)); eng.running = True
    async def go():
        await eng.run_round(T)
        for _ in range(400):
            await asyncio.sleep(0)
            if eng.state.get("market") and eng.state.get("poly_book"): break
        await finish(eng)
    asyncio.run(go())
    r = db.get_round(f"{cfg.slug_prefix}{T}")
    assert r["status"] == "MARKET_UNVERIFIED"                     # the locked record is honest and immutable
    assert eng.state["market"]["market_id"] == "1"                 # but the dashboard now shows the market ...
    assert eng.state["open"].get("official") == 60000.0            # ... the official open ...
    assert eng.state.get("poly_book", {}).get("mid") == 0.55       # ... and the CLOB feed is polled again

def test_m1_refresh_backfills_whole_gap_after_pause():
    live = LiveData(Config(), None)
    now = 1_800_000_000
    live.m1 = [[now - 3600 - 60 * i, 1, 1, 1, 1, 1, 1, 1, 1] for i in range(10)][::-1]
    assert live.m1_limit(now) >= 60                                # 1 h gap -> request >= 60 bars, not 5
    live.m1[-1][0] = now - 60
    assert live.m1_limit(now) == 5
    live.m1 = []
    assert live.m1_limit(now) == 400

def test_kick_forces_immediate_reconnect_of_silent_websocket():
    """A half-open socket after a host pause never errors by itself; kick() must make the loop reconnect at once."""
    async def go():
        conns = []
        async def handler(ws):
            conns.append(time.time())
            await ws.wait_closed()                                   # never sends anything (like a dead socket)
        async with websockets.serve(handler, "127.0.0.1", 0) as srv:
            port = srv.sockets[0].getsockname()[1]
            live = LiveData(Config(), None); live.running = True
            task = asyncio.create_task(live._ws_loop("binance", f"ws://127.0.0.1:{port}", None, lambda m: None))
            for _ in range(100):
                await asyncio.sleep(0.05)
                if live.feeds["binance"].connected: break
            assert live.feeds["binance"].connected and len(conns) == 1
            t0 = time.time(); assert live.kick("test") == 1
            for _ in range(60):
                await asyncio.sleep(0.05)
                if len(conns) >= 2 and live.feeds["binance"].connected: break
            dt = time.time() - t0
            live.running = False; task.cancel()
            assert len(conns) >= 2 and dt < 2.0, dt                # reconnected within 2 s, not the 12-30 s silence timeout
            srv.close()
    asyncio.run(asyncio.wait_for(go(), 20))

def test_ws_silence_timeout_is_short():
    assert LiveData.WS_SILENCE_S <= 15

def test_supervisor_logs_real_exit_codes():
    s = open(os.path.join(os.path.dirname(__file__), "..", "run.sh")).read()
    for name in ("server", "pusher"):
        line = next(l for l in s.splitlines() if f"{name} exited" in l)
        # $? must be captured before $(date ...) runs, otherwise it is always 0
        assert re.search(r"rc=\\\$\?;.*" + name + r" exited \(rc=", line), line
