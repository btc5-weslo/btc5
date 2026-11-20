"""Nov 19 audit: resolution parsing / grading against the official Polymarket outcome, and continuous learning."""
import asyncio, json, os, tempfile, pickle
import numpy as np, pytest
from btc5.config import Config
from btc5.db import DB
from btc5.engine import Engine
from btc5.polymarket import parse_resolution
from btc5 import features as F, training
from btc5.model import StackModel, GBDTModel, LRModel
from helpers import FakeClock, make_sleep, FakeLive, FakePoly, synth_bars, synth_m1

T = 1_800_000_000 // 300 * 300
def mk(**kw):
    return {"closed": True, "umaResolutionStatus": "resolved", "outcomes": json.dumps(kw.get("outcomes", ["Up", "Down"])),
            "outcomePrices": json.dumps(kw.get("prices", ["1", "0"])), **{k: v for k, v in kw.items() if k not in ("outcomes", "prices")}}

def test_parse_maps_by_outcome_name_not_position():
    assert parse_resolution(mk(prices=["1", "0"])) == "UP"
    assert parse_resolution(mk(prices=["0", "1"])) == "DOWN"
    assert parse_resolution(mk(outcomes=["Down", "Up"], prices=["1", "0"])) == "DOWN"
    assert parse_resolution(mk(outcomes=["Down", "Up"], prices=["0", "1"])) == "UP"

@pytest.mark.parametrize("m", [mk(closed=False), mk(umaResolutionStatus="proposed"), mk(umaResolutionStatus="disputed"),
                               mk(prices=["0.5", "0.5"]), mk(prices=["0.97", "0.03"]), mk(outcomes=["Yes", "No"]), {}, None])
def test_parse_rejects_anything_not_final(m):
    assert parse_resolution(m) is None

class SpotDisagreesPoly(FakePoly):
    """Official TWAP-settled outcome UP while Chainlink SPOT close < open (as in the 11-19 19:10 round)."""
    async def chainlink_open_close(self, T): return {"openPrice": 61975.78428645, "closePrice": 61970.24032243335}
    async def resolution(self, T): return "UP", {"closed": True, "outcomes": '["Up","Down"]', "outcomePrices": '["1","0"]', "umaResolutionStatus": "resolved"}

@pytest.mark.parametrize("pred_p,expected", [(0.8, "CORRECT"), (0.2, "INCORRECT")])
def test_grading_uses_official_outcome_not_spot_open_close(pred_p, expected):
    cfg = Config(); cfg.db_path = os.path.join(tempfile.mkdtemp(), "g.db"); db = DB(cfg.db_path); clk = FakeClock(T + 1)
    eng = Engine(cfg, db, clk, FakeLive(T), SpotDisagreesPoly(cfg), sleep=make_sleep(clk)); eng.running = True
    eng.brain.predict = lambda f, info, w=None: (pred_p, {g: pred_p for g in ("TREND", "STRUCTURE", "ORDERFLOW", "VOLATILITY", "DERIVS")}, "test", [])
    async def go():
        await eng.run_round(T)
        r = await eng.resolve(f"{cfg.slug_prefix}{T}", T, T + 300)
        for t in list(eng.tasks): t.cancel()
        return r
    assert asyncio.run(go()) == expected
    r = db.get_round(f"{cfg.slug_prefix}{T}")
    assert r["official_outcome"] == "UP" and r["official_close"] < r["official_open"] and r["result"] == expected
    assert json.loads(r["resolution_json"])["umaResolutionStatus"] == "resolved"

def test_twap_reference_feature_uses_only_pre_open_bars():
    bars = synth_bars(T, T - 600, T + 300); m1 = synth_m1(T)
    f1, i1 = F.compute(T, bars, m1, cutoff=T + 90)
    later = [b if b[0] < T + 90 else [b[0], b[1] * 2, b[2] * 2, b[3] * 2, b[4] * 2] + b[5:] for b in bars]
    f2, _ = F.compute(T, later, m1, cutoff=T + 90)
    assert f1["z_twap"] == f2["z_twap"] and f1["z_twap_rem"] == f2["z_twap_rem"]          # nothing after the cutoff
    pre = [b[4] for b in bars if T - 60 <= b[0] < T]
    assert i1["twap_ref"] == round(sum(pre) / len(pre), 2)

def _ds(n=700, seed=3):
    rng = np.random.default_rng(seed); rows = []; y = []
    for i in range(n):
        f = {k: float(rng.normal()) for k in F.ALL_FEATURES}; rows.append(f)
        y.append(int(f["z_twap"] + 0.5 * f["z90"] + rng.normal() * 0.8 > 0))
    return rows, np.array(y), ["SIDEWAYS CONSOLIDATION"] * n, [1.0] * n

@pytest.mark.parametrize("cls", [StackModel, GBDTModel, LRModel])
def test_models_trained_before_new_features_still_predict(cls, monkeypatch):
    import btc5.model as M
    rows, y, regs, dq = _ds()
    legacy = [{k: v for k, v in r.items() if k not in F.NEW_IN_V2} for r in rows]
    monkeypatch.setattr(M, "GROUPS", F.LEGACY_GROUPS); monkeypatch.setattr(M, "ALL_FEATURES", F.LEGACY_ALL)
    m = cls().fit(legacy, y, regs, dq)
    monkeypatch.undo()
    for attr in ("features", "groups"):
        if hasattr(m, attr): delattr(m, attr)
    if hasattr(m, "stack"): delattr(m.stack, "groups")
    m = pickle.loads(pickle.dumps(m))
    p, _ = m.predict(rows[:5], regs[:5], dq[:5])          # live rows now carry extra keys
    assert len(p) == 5 and np.all((p > 0) & (p < 1))

def test_challenger_is_refit_on_new_rounds_strictly_out_of_sample(monkeypatch):
    rows, y, regs, dq = _ds(600); h0 = 400; seen = []
    class Spy(StackModel):
        def fit(self, r, yy, rg, d): seen.append(len(yy)); return super().fit(r, yy, rg, d)
    monkeypatch.setitem(training.ARCH, "stack", Spy)
    px = training.challenger_walk_forward("stack", rows, y, regs, dq, h0, blocks=4)
    assert len(px) == 200 and not np.isnan(px).any()
    assert seen == [400, 450, 500, 550]                   # each block trained only on rounds BEFORE it, incl. new ones

def test_promotion_rule():
    good = {"mean": -0.012, "ci95": [-0.026, 0.0013], "b_significantly_better": False}
    assert training.promotion_decision(good, {"accuracy": 0.733}, {"accuracy": 0.754})[0]
    assert not training.promotion_decision(dict(good, mean=0.002, ci95=[-0.01, 0.014]), {"accuracy": 0.73}, {"accuracy": 0.75})[0]
    assert not training.promotion_decision(dict(good, ci95=[-0.03, 0.02]), {"accuracy": 0.73}, {"accuracy": 0.75})[0]   # too uncertain
    assert not training.promotion_decision(good, {"accuracy": 0.75}, {"accuracy": 0.73})[0]                           # loses accuracy
    assert training.promotion_decision({"mean": -0.03, "ci95": [-0.05, -0.01], "b_significantly_better": True}, {"accuracy": 0.75}, {"accuracy": 0.70})[0]
