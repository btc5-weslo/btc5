"""Data integrity, leakage, immutability, independence, statistics."""
import json, os, tempfile, sqlite3, math, random, time
import numpy as np, pytest
from btc5.config import Config, confidence_class
from btc5.db import DB
from btc5 import features as F, analytics
from btc5.polymarket import validate_market, parse_resolution, MarketIdentificationError
from btc5.live import BarBuilder, FeedStatus
from btc5.model import StackModel, AGENT_GROUPS, metrics
from helpers import synth_bars, synth_m1

T = 1_800_000_000 // 300 * 300
cfg = Config()

def newdb(): return DB(os.path.join(tempfile.mkdtemp(), "x.db"))
def rec(T0, **kw):
    r = {"round_id": f"btc-updown-5m-{T0}", "start_ts": T0, "end_ts": T0 + 300, "decision_target_ts": T0 + 90, "status": "LOCKED", "prediction": "UP", "p_up": 0.6, "p_down": 0.4,
         "confidence": "LOW", "decision_ts": T0 + 90.0005, "deviation_ms": 0.5, "result": "PENDING"}
    r.update(kw); return r

# ---------- immutability / duplicates / persistence
def test_duplicate_insert_blocked():
    db = newdb(); assert db.insert_round(rec(T)); assert not db.insert_round(rec(T, prediction="DOWN"))
    assert db.get_round(f"btc-updown-5m-{T}")["prediction"] == "UP"

@pytest.mark.parametrize("col,val", [("prediction", "'DOWN'"), ("p_up", "0.1"), ("decision_ts", "1"), ("confidence", "'HIGH'"), ("status", "'MISSED'")])
def test_locked_fields_immutable(col, val):
    db = newdb(); db.insert_round(rec(T))
    with pytest.raises(sqlite3.DatabaseError): db.x(f"UPDATE rounds SET {col}={val} WHERE start_ts=?", (T,))

def test_history_cannot_be_deleted_and_outcome_set_once():
    db = newdb(); db.insert_round(rec(T))
    with pytest.raises(sqlite3.DatabaseError): db.x("DELETE FROM rounds")
    db.x("UPDATE rounds SET official_outcome='UP' WHERE start_ts=?", (T,))
    with pytest.raises(sqlite3.DatabaseError): db.x("UPDATE rounds SET official_outcome='DOWN' WHERE start_ts=?", (T,))

def test_publication_timestamp_permanent():
    db = newdb(); db.insert_round(rec(T)); db.x("UPDATE rounds SET published_ts=? WHERE start_ts=?", (T + 90.01, T))
    with pytest.raises(sqlite3.DatabaseError): db.x("UPDATE rounds SET published_ts=? WHERE start_ts=?", (T + 99, T))

def test_records_persist_after_restart():
    p = os.path.join(tempfile.mkdtemp(), "p.db"); DB(p).insert_round(rec(T))
    assert DB(p).get_round(f"btc-updown-5m-{T}")["prediction"] == "UP"

# ---------- no future data
def test_features_ignore_all_post_cutoff_data():
    pre = synth_bars(T, T - 600, T + 90); fut = synth_bars(T, T + 90, T + 300, p0=pre[-1][4] * 1.01, seed=9)
    m1 = synth_m1(T); m1_fut = [[T + 60 * i, 1, 2, 0.5, 9e9, 1, 1, 1, 1] for i in range(1, 5)]
    crowd = [(T + 20, 0.5), (T + 80, 0.6)]; crowd_f = crowd + [(T + 91, 0.99), (T + 200, 0.01)]
    oi = [(T - 300 * i, 1e9 + i) for i in range(20, 0, -1)]; oi_f = oi + [(T, 5e9), (T + 300, 9e9)]
    fu = [(T - 3600, 1e-5)]; fu_f = fu + [(T + 100, 5e-3)]
    a = F.compute(T, pre, m1, crowd, oi, fu, T + 90); b = F.compute(T, pre + fut, m1 + m1_fut, crowd_f, oi_f, fu_f, T + 90)
    assert a == b

def test_partial_minute_bar_not_used():
    m1 = synth_m1(T) + [[T + 60, 1, 1, 1, 1, 1, 1, 1, 1]]   # minute bar [T+60, T+120) closes after the cutoff
    s, m, *_ = F.eligible(T, [], m1, [], [], [], T + 90)
    assert m[-1][0] + 60 <= T + 90

def test_bar_window_excludes_cutoff_second_and_later():
    bb = BarBuilder()
    for s in range(T, T + 95):
        bb.mark_connected(s); bb.trade(s + 0.5, 100 + s - T, 1.0, True)
    bb.roll(T + 93)                        # some seconds finalized, others still in progress
    w = bb.window(T, T + 90)
    assert w[-1][0] == T + 89 and all(b[0] + 1 <= T + 90 for b in w)

def test_historical_store_has_no_post_cutoff_bars():
    p = Config().db_path
    real = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "btc5.db")
    if not os.path.exists(real): pytest.skip("no real history")
    from btc5.db import unpack
    con = sqlite3.connect(f"file:{real}?mode=ro", uri=True)
    n = 0
    for st, blob in con.execute("SELECT start_ts, s1 FROM hist_rounds WHERE s1 IS NOT NULL ORDER BY RANDOM() LIMIT 300"):
        bars = unpack(blob); assert max(b[0] for b in bars) <= st + 89; n += 1
    assert n > 0

# ---------- always UP/DOWN, deterministic tie-break
def test_tie_break_deterministic_and_directional():
    f = {k: 0.0 for k in F.ALL_FEATURES}
    assert F.tie_break(f, {"disp_bps": 0}) == ("UP", "contract rule (>= open resolves UP)")
    assert F.tie_break(f, {"disp_bps": -1.2})[0] == "DOWN"
    f["tbr_round"] = 0.3; assert F.tie_break(f, {"disp_bps": 0})[0] == "UP"
    assert confidence_class(0.5) == "VERY LOW"

def test_no_data_yields_flagged_degraded_not_invented():
    f, info = F.compute(T, [], [], None, None, None, T + 90)
    assert info["data_quality"] == 0 and info["missing"] and "price" not in info

# ---------- market identity & resolution rules
def gamma(T0, **kw):
    m = {"slug": f"btc-updown-5m-{T0}", "id": 1, "conditionId": "0x1", "question": "Bitcoin Up or Down - x", "events": [{"series": [{"slug": "btc-up-or-down-5m"}]}],
         "eventStartTime": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(T0)), "endDate": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(T0 + 300)),
         "outcomes": '["Up", "Down"]', "clobTokenIds": '["1", "2"]', "resolutionSource": "https://data.chain.link/streams/btc-usd", "description": "Chainlink"}
    m.update(kw); return m

def test_market_validation():
    assert validate_market(gamma(T), T, cfg).start_ts == T
    for bad in ({"slug": "btc-updown-5m-1"}, {"eventStartTime": "2020-01-01T00:00:00Z"}, {"outcomes": '["Yes","No"]'}, {"question": "Ethereum Up or Down"},
                {"resolutionSource": "binance", "description": "Binance"}, {"events": [{"series": [{"slug": "eth-up-or-down-5m"}]}]}):
        with pytest.raises(MarketIdentificationError): validate_market(gamma(T, **bad), T, cfg)

def test_resolution_parsing_only_final():
    base = {"outcomes": '["Up","Down"]'}
    assert parse_resolution(dict(base, closed=True, outcomePrices='["1","0"]', umaResolutionStatus="resolved")) == "UP"
    assert parse_resolution(dict(base, closed=True, outcomePrices='["0","1"]')) == "DOWN"
    assert parse_resolution(dict(base, closed=False, outcomePrices='["1","0"]')) is None
    assert parse_resolution(dict(base, closed=True, outcomePrices='["0.6","0.4"]')) is None
    assert parse_resolution(dict(base, closed=True, outcomePrices='["1","0"]', umaResolutionStatus="proposed")) is None

# ---------- agents independent; ensemble learns from evidence
def synth_dataset(n=900, seed=3):
    rng = np.random.default_rng(seed); rows = []; y = []
    for i in range(n):
        f = {k: float(rng.normal()) for k in F.ALL_FEATURES}
        z = 1.5 * f["z_rem"] + 0.8 * f["mom30"]; y.append(int(rng.random() < 1 / (1 + math.exp(-z)))); rows.append(f)
    return rows, np.array(y)

def test_agents_use_only_their_own_feature_group():
    rows, y = synth_dataset(); m = StackModel().fit(rows, y, ["SIDEWAYS CONSOLIDATION"] * len(y), [1.0] * len(y))
    base = m.agent_probs(rows[:50])
    for j, (g, _) in enumerate(AGENT_GROUPS):
        pert = [dict(r) for r in rows[:50]]
        for r in pert:
            for k in F.ALL_FEATURES:
                if k not in F.GROUPS[g]: r[k] = r[k] + 3.0
        assert np.allclose(m.agent_probs(pert)[:, j], base[:, j])
    assert len(set(sum(F.GROUPS.values(), []))) == len(F.ALL_FEATURES)   # groups are disjoint

def test_validator_weights_track_predictive_value_not_votes():
    rows, y = synth_dataset(1500); m = StackModel().fit(rows, y, ["SIDEWAYS CONSOLIDATION"] * len(y), [1.0] * len(y))
    w = m.weights()
    assert w["VOLATILITY"] > 0.5 and w["TREND"] > 0.2 and w["VOLATILITY"] > w["TREND"]
    assert max(abs(w["STRUCTURE"]), abs(w["ORDERFLOW"]), abs(w["DERIVS"])) < 0.1 * w["VOLATILITY"]

def test_metrics_match_hand_calculation():
    m = metrics([0.9, 0.2, 0.6, 0.4], [1, 0, 0, 1])
    assert m["accuracy"] == 0.5 and m["n"] == 4
    assert m["brier"] == round((0.01 + 0.04 + 0.36 + 0.36) / 4, 4)

# ---------- performance statistics only from resolved rounds
def test_performance_counts_only_resolved():
    db = newdb()
    db.insert_round(rec(T, result="CORRECT", confidence="HIGH", regime="X")); db.x("UPDATE rounds SET official_outcome='UP' WHERE start_ts=?", (T,))
    db.insert_round(rec(T + 300, prediction="DOWN", result="INCORRECT", confidence="LOW", regime="X")); db.x("UPDATE rounds SET official_outcome='UP' WHERE start_ts=?", (T + 300,))
    db.insert_round(rec(T + 600, result="PENDING"))
    db.insert_round({"round_id": f"btc-updown-5m-{T+900}", "start_ts": T + 900, "end_ts": T + 1200, "decision_target_ts": T + 990, "status": "MISSED", "result": "MISSED"})
    db.insert_round(rec(T + 1200, status="MARKET_UNVERIFIED", result="EXCLUDED"))
    p = analytics.performance(db)
    assert p["overall"]["n"] == 2 and p["overall"]["accuracy"] == 0.5
    assert p["missed"] == 1 and p["pending"] == 1 and p["excluded"] == 1
    assert p["high_conf"]["n"] == 1 and p["down"]["accuracy"] == 0.0
    assert p["overall"]["conclusive"] is False

# ---------- failed connections are detected
def test_stale_feed_detected():
    f = FeedStatus("binance"); assert f.to_dict()["status"] == "DOWN"
    f.last_msg = time.time(); assert f.to_dict()["status"] == "LIVE"
    f.last_msg = time.time() - 30; assert f.to_dict()["status"] == "STALE" and not f.fresh()
    f.enabled = False; assert f.to_dict()["status"] == "DISABLED"
