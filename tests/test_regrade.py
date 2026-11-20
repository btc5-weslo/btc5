"""Regrade audit: stored grades are made to agree with Polymarket's official resolution; unresolved rounds stay PENDING."""
import asyncio, json, os, sqlite3, importlib
import pytest
from btc5.db import DB
from btc5.engine import Engine
from btc5.polymarket import parse_resolution
from btc5 import regrade

NOW = 1_800_000_000
def rid(T): return f"btc-updown-5m-{T}"

def market(T, up_won=True, status="resolved", closed=True, prices=None):
    return {"slug": rid(T), "closed": closed, "umaResolutionStatus": status, "outcomes": '["Up", "Down"]',
            "outcomePrices": json.dumps(prices or (["1", "0"] if up_won else ["0", "1"])), "closedTime": "x", "resolutionSource": "chainlink"}

def locked(db, T, prediction, official=None, result=None, spot=None):
    rec = {"round_id": rid(T), "start_ts": T, "end_ts": T + 300, "decision_target_ts": T + 90, "status": "LOCKED", "prediction": prediction,
           "p_up": 0.7 if prediction == "UP" else 0.3, "p_down": 0.3 if prediction == "UP" else 0.7, "confidence": "HIGH",
           "decision_ts": T + 90.001, "deviation_ms": 1.0, "result": result or "PENDING"}
    if official:
        rec.update(official_outcome=official, resolved_ts=T + 310, resolution_json=json.dumps({"umaResolutionStatus": "resolved"}),
                   result=result or ("CORRECT" if official == prediction else "INCORRECT"))
    if spot: rec.update(official_open=spot[0], official_close=spot[1])
    assert db.insert_round(rec)

def trade(db, T, side, status=None, outcome=None, shares=20.0, cost=10.0):
    won = outcome == side if outcome else None
    db.x("INSERT INTO trades(round_id,start_ts,end_ts,mode,epoch,side,stake,entry_price,shares,cost,fee,status,outcome,payout,pnl,created_ts) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
         (rid(T), T, T + 300, "demo", 1, side, cost, 0.5, shares, cost, 0.0, status or "OPEN", outcome,
          (shares if won else 0.0) if outcome else None, ((shares - cost) if won else -cost) if outcome else None, T + 91))

class Poly:
    def __init__(self, markets, fail=()): self.m = markets; self.fail = set(fail); self.calls = []
    async def market_raw(self, T):
        self.calls.append(T)
        if T in self.fail: raise RuntimeError("gamma timeout")
        return self.m.get(T)

async def nosleep(_): pass
CFG = {"days": 7, "every": 600, "full_every": 21600, "gap": 0, "first": 0, "settle_delay": 120}

@pytest.fixture
def db(tmp_path): return DB(str(tmp_path / "r.db"))

def test_strict_resolution_requires_uma_resolved():
    m = market(1, status=None)
    assert parse_resolution(m) == "UP"                       # historical data path unchanged
    assert parse_resolution(m, strict=True) is None          # grading: not confirmed -> not official
    assert parse_resolution(market(1, status="resolved"), strict=True) == "UP"
    assert parse_resolution(market(1, status="proposed"), strict=True) is None
    assert parse_resolution(market(1, closed=False), strict=True) is None

def test_correct_round_fixes_wrong_outcome_resettles_trade_and_keeps_triggers(db):
    T = NOW - 3000
    locked(db, T, "DOWN", official="UP")                     # recorded UP / INCORRECT, Polymarket says DOWN
    trade(db, T, "DOWN", status="LOST", outcome="UP")
    db.x("INSERT INTO agent_results VALUES (?,?,?,?,?,?)", (rid(T), "TREND", "DOWN", 0.3, 0, 1.0))
    ch = regrade.correct_round(db, rid(T), "DOWN", {"umaResolutionStatus": "resolved"}, now=NOW)
    assert ch["from_outcome"] == "UP" and ch["to_outcome"] == "DOWN" and ch["to_result"] == "CORRECT"
    r = db.get_round(rid(T))
    assert r["official_outcome"] == "DOWN" and r["result"] == "CORRECT"
    meta = json.loads(r["resolution_json"])["regrade"]
    assert meta["from_outcome"] == "UP" and meta["from_result"] == "INCORRECT" and meta["ts"] == NOW
    t = db.one("SELECT * FROM trades WHERE round_id=?", (rid(T),))
    assert t["status"] == "WON" and t["outcome"] == "DOWN" and t["pnl"] == 10.0 and t["payout"] == 20.0
    assert db.one("SELECT correct FROM agent_results WHERE round_id=?", (rid(T),))["correct"] == 1
    # protective triggers are back afterwards
    with pytest.raises(sqlite3.DatabaseError): db.x("UPDATE rounds SET official_outcome='UP' WHERE round_id=?", (rid(T),))
    with pytest.raises(sqlite3.DatabaseError): db.x("UPDATE trades SET pnl=1 WHERE round_id=?", (rid(T),))
    with pytest.raises(sqlite3.DatabaseError): db.x("UPDATE rounds SET prediction='UP' WHERE round_id=?", (rid(T),))
    assert regrade.correct_round(db, rid(T), "DOWN", now=NOW + 1) is None   # idempotent

def test_correct_round_ignores_missed_and_bad_outcomes(db):
    T = NOW - 3000
    db.insert_round({"round_id": rid(T), "start_ts": T, "end_ts": T + 300, "decision_target_ts": T + 90, "status": "MISSED", "result": "MISSED"})
    assert regrade.correct_round(db, rid(T), "UP") is None
    assert db.get_round(rid(T))["official_outcome"] is None
    locked(db, T + 300, "UP")
    assert regrade.correct_round(db, rid(T + 300), None) is None and regrade.correct_round(db, rid(T + 300), "FLAT") is None

def test_run_pass_grades_pending_keeps_unresolved_and_confirms(db):
    a, b, c, d, e, f = (NOW - 300 * k for k in (10, 9, 8, 7, 6, 1))
    locked(db, a, "UP", official="UP")                       # consistent
    locked(db, b, "UP", official="DOWN")                     # wrong: Polymarket says UP
    locked(db, c, "DOWN", result="UNVERIFIED")               # stuck: resolved now
    locked(db, d, "DOWN")                                    # stuck PENDING, still not resolved
    locked(db, e, "UP")                                      # gamma error
    locked(db, f, "UP")                                      # ended < 120 s ago: too fresh, not touched
    trade(db, c, "DOWN")                                     # OPEN trade: left to the trader sweep
    poly = Poly({a: market(a, True), b: market(b, True), c: market(c, False), d: market(d, True, status="proposed"), f: market(f, True)}, fail={e})
    s = asyncio.run(regrade.run_pass(db, poly, full=True, now=NOW, sleep=nosleep, cfg=CFG))
    assert (s["checked"], s["confirmed"], s["corrected"], s["graded_pending"], s["unresolved"], s["errors"]) == (4, 1, 1, 1, 1, 1)
    assert db.get_round(rid(b))["official_outcome"] == "UP" and db.get_round(rid(b))["result"] == "CORRECT"
    assert db.get_round(rid(c))["official_outcome"] == "DOWN" and db.get_round(rid(c))["result"] == "CORRECT"
    assert db.get_round(rid(d))["official_outcome"] is None and db.get_round(rid(d))["result"] == "PENDING"
    assert db.get_round(rid(e))["result"] == "PENDING" and db.get_round(rid(f))["result"] == "PENDING"
    assert f not in poly.calls
    assert db.one("SELECT status FROM trades WHERE round_id=?", (rid(c),))["status"] == "OPEN"
    # next (non-full) pass only asks Gamma about rounds not yet confirmed
    poly.calls.clear(); poly.fail.clear(); poly.m[d] = market(d, False); poly.m[e] = market(e, False)
    s2 = asyncio.run(regrade.run_pass(db, poly, full=False, now=NOW + 600, sleep=nosleep, cfg=CFG))
    assert sorted(poly.calls) == sorted([d, e, f])
    assert db.get_round(rid(d))["result"] == "CORRECT" and db.get_round(rid(e))["result"] == "INCORRECT"

def test_run_pass_skips_rounds_older_than_window(db):
    old = NOW - 8 * 86400
    locked(db, old, "UP", official="DOWN")
    poly = Poly({old: market(old, True)})
    asyncio.run(regrade.run_pass(db, poly, full=True, now=NOW, sleep=nosleep, cfg=CFG))
    assert poly.calls == [] and db.get_round(rid(old))["official_outcome"] == "DOWN"

def test_public_exposes_spot_vs_official(db):
    T = NOW - 3000
    locked(db, T, "DOWN", official="UP", spot=(62008.91, 61991.89))   # the Nov 20 5:50 AM ET shape: spot down, official UP
    p = Engine.public(db.get_round(rid(T)))
    assert p["spot_direction"] == "DOWN" and p["spot_disagrees"] is True and p["official_outcome"] == "UP"
    assert p["polymarket_url"].endswith(rid(T)) and p["regraded"] is None

def test_correction_survives_snapshot_restore_via_incremental(tmp_path, monkeypatch):
    """Snapshot taken BEFORE a correction + incremental written AFTER it -> restored DB carries the correction."""
    snap = str(tmp_path / "snap.db"); live = str(tmp_path / "btc5.db")
    T = NOW - 3000
    s = DB(snap); locked(s, T, "DOWN", official="UP"); trade(s, T, "DOWN", status="LOST", outcome="UP"); s.con.close()
    import shutil; shutil.copy(snap, live)
    L = DB(live); regrade.correct_round(L, rid(T), "DOWN", now=NOW); L.con.close()
    monkeypatch.setenv("BTC5_DB", live); monkeypatch.setenv("BTC5_DB_BACKUP", "0")
    import backup; B = importlib.reload(backup)
    corrected = B._bundle(rid(T)); assert B._richer(corrected, B._bundle(rid(T)) | {"round": dict(corrected["round"], resolution_json="{}")})
    shutil.copy(snap, live)                                  # "restore" the old snapshot
    act = B.apply_incremental_round(DB(live), corrected)
    assert act == "regraded"
    D = DB(live); r = D.get_round(rid(T))
    assert r["official_outcome"] == "DOWN" and r["result"] == "CORRECT"
    assert json.loads(r["resolution_json"])["regrade"]["ts"] == NOW
    assert D.one("SELECT status FROM trades WHERE round_id=?", (rid(T),))["status"] == "WON"
    assert B.apply_incremental_round(D, corrected) == "kept"   # idempotent
