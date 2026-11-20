"""Timing layer: T90 decision event, never early, one record per round, late wake => MISSED (never back-dated)."""
import asyncio, os, tempfile, time, json, pytest
from btc5.config import Config
from btc5.db import DB
from btc5.engine import Engine, wait_until
from btc5.clock import SyncedClock
from helpers import FakeClock, make_sleep, FakeLive, FakePoly

T = 1_800_000_000 // 300 * 300

def mk(t_start, **kw):
    cfg = Config(); cfg.db_path = os.path.join(tempfile.mkdtemp(), "t.db")
    db = DB(cfg.db_path); clk = FakeClock(t_start); live = FakeLive(T); poly = FakePoly(cfg, **kw)
    eng = Engine(cfg, db, clk, live, poly, sleep=make_sleep(clk)); eng.running = True
    return cfg, db, clk, live, eng

async def finish(eng):
    for t in list(eng.tasks): t.cancel()
    await asyncio.sleep(0)

def test_decision_exactly_at_T90_and_never_early():
    cfg, db, clk, live, eng = mk(T + 0.4)
    async def go():
        await eng.run_round(T); await finish(eng)
    asyncio.run(go())
    r = db.get_round(f"{cfg.slug_prefix}{T}")
    assert r["status"] == "LOCKED" and r["prediction"] in ("UP", "DOWN")
    assert r["decision_ts"] >= T + 90, "decision fired before T90"
    assert 0 <= r["deviation_ms"] < 2.0, r["deviation_ms"]
    assert r["cutoff_ts"] == T + 90
    assert r["published_ts"] >= r["decision_ts"]
    # the final inputs were frozen at exactly T0+90
    assert max(live.windows) <= T + 90

def test_one_prediction_per_round_even_if_rerun():
    cfg, db, clk, live, eng = mk(T + 1)
    async def go():
        await eng.run_round(T)
        clk.t = T + 10; eng.handled.discard(T)
        await eng.run_round(T)       # duplicate scheduling of the same round
        await finish(eng)
    asyncio.run(go())
    rows = db.q("SELECT * FROM rounds WHERE start_ts=?", (T,))
    assert len(rows) == 1

def test_late_start_records_missed_not_prediction():
    cfg, db, clk, live, eng = mk(T + 95)
    async def go(): await eng.run_round(T); await finish(eng)
    asyncio.run(go())
    r = db.get_round(f"{cfg.slug_prefix}{T}")
    assert r["status"] == "MISSED" and r["prediction"] is None and "after the T90" in r["missed_reason"]

def test_paused_event_loop_at_deadline_is_missed():
    cfg, db, clk, live, eng = mk(T + 1)
    async def go():
        await eng.decide(T, None, [], T + 90 + 5.0, eng.settings)   # decision event fired 5 s late (box paused)
        await finish(eng)
    asyncio.run(go())
    r = db.get_round(f"{cfg.slug_prefix}{T}")
    assert r["status"] == "MISSED" and r["prediction"] is None and r["deviation_ms"] == pytest.approx(5000, abs=1)

def test_unverified_market_is_flagged_not_counted():
    cfg, db, clk, live, eng = mk(T + 1, fail_market=True)
    async def go(): await eng.run_round(T); await finish(eng)
    asyncio.run(go())
    r = db.get_round(f"{cfg.slug_prefix}{T}")
    assert r["status"] == "MARKET_UNVERIFIED" and r["prediction"] in ("UP", "DOWN")
    assert "MARKET IDENTITY NOT VERIFIED" in r["evidence_json"]

def test_resolution_verifies_official_outcome():
    cfg, db, clk, live, eng = mk(T + 1, outcome="DOWN")
    async def go():
        await eng.run_round(T)
        await eng.resolve(f"{cfg.slug_prefix}{T}", T, T + 300); await finish(eng)
    asyncio.run(go())
    r = db.get_round(f"{cfg.slug_prefix}{T}")
    assert r["official_outcome"] == "DOWN"
    assert r["result"] == ("CORRECT" if r["prediction"] == "DOWN" else "INCORRECT")
    assert r["official_open"] == 60000.0

def test_real_clock_wait_until_never_returns_early():
    c = SyncedClock()
    async def go():
        for d in (0.05, 0.2, 0.31):
            target = c.now() + d; await wait_until(c, target)
            assert c.now() >= target
            assert c.now() - target < 0.03
    asyncio.run(go())

def test_recovery_accounts_for_offline_rounds_and_reattaches_resolution():
    cfg, db, clk, live, eng = mk(T + 1)
    db.insert_round({"round_id": f"{cfg.slug_prefix}{T-1500}", "start_ts": T - 1500, "end_ts": T - 1200, "decision_target_ts": T - 1410, "status": "LOCKED", "prediction": "UP", "p_up": 0.6, "result": "PENDING"})
    clk.t = T + 1
    async def go():
        eng.recover(); n = len(eng.tasks); await finish(eng); return n
    n = asyncio.run(go())
    missed = db.q("SELECT start_ts FROM rounds WHERE status='MISSED' ORDER BY start_ts")
    assert [m["start_ts"] for m in missed] == [T - 1200, T - 900, T - 600, T - 300]
    assert n >= 1   # pending resolution re-attached

class SuspendClock(FakeClock):
    """Wall clock jumps forward while the monotonic clock does not (Linux CLOCK_MONOTONIC excludes VM suspend)."""
    def __init__(self, t): super().__init__(t); self.m = 0.0
    def mono(self): return self.m

def test_wait_until_does_not_oversleep_after_suspend():
    c = SuspendClock(1000.0); woke = []
    async def sleep(d):
        d = max(d, 0.0005); c.t += d; c.m += d
        if 1004 < c.t < 1005 and not woke: c.t += 120; woke.append(1)    # 2-minute suspend: wall jumps, monotonic doesn't
        await asyncio.sleep(0)
    async def go(): await wait_until(c, 1010.0, sleep)
    asyncio.run(go())
    assert woke and 1010.0 <= c.t < 1010.0 + 125 and c.t - 1010.0 < 130
    assert c.t >= 1010.0

def test_scheduler_records_rounds_skipped_by_suspend():
    cfg, db, clk, live, eng = mk(T + 280)
    async def go():
        async def jump_sleep(d):
            clk.t += max(d, 0.0005)
            if T + 299 < clk.t < T + 400: clk.t = T + 3 * 300 + 120     # suspended across rounds T+300 and T+600
            await asyncio.sleep(0)
        eng.sleep = jump_sleep
        eng.recover = lambda: None
        eng.brain.reload = lambda: None
        async def no_train(**k): return None
        eng.train = no_train
        task = asyncio.create_task(eng.run())
        for _ in range(4000):
            await asyncio.sleep(0)
            if clk.t > T + 900 + 100: break
        eng.running = False; task.cancel(); await finish(eng)
    asyncio.run(go())
    rows = {r["start_ts"]: r["status"] for r in db.q("SELECT start_ts,status FROM rounds")}
    assert rows.get(T + 300) == "MISSED" and rows.get(T + 600) == "MISSED"
