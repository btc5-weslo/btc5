"""Historical replay + walk-forward backtest on REAL fetched data (see history.py).
Each round's features are computed by the same features.compute() used live, with cutoff T0+90; the stored 1s data for a
round ends at T0+90, the 1m/OI/funding/crowd series are cut at T0+90 by features.eligible()."""
import json, math, time, bisect
import numpy as np
from .db import unpack
from . import features as F
from .model import ARCH, metrics, AGENT_GROUPS, StackModel
from .config import confidence_class

def load_series(db):
    m1 = [[r["ts"], r["o"], r["h"], r["l"], r["c"], r["v"], r["qv"], r["n"], r["tbv"]] for r in db.q("SELECT * FROM m1 ORDER BY ts")]
    oi = [(r["ts"], r["oi_usd"]) for r in db.q("SELECT * FROM oi5m ORDER BY ts")]
    fu = [(r["ts"], r["rate"]) for r in db.q("SELECT * FROM funding ORDER BY ts")]
    return m1, oi, fu

def build_dataset(db, since_ts=0, max_rounds=None):
    m1, oi, fu = load_series(db)
    m1t = [b[0] for b in m1]; oit = [x[0] for x in oi]; fut = [x[0] for x in fu]
    out = []
    q = "SELECT start_ts, outcome, crowd_json, s1 FROM hist_rounds WHERE outcome IS NOT NULL AND s1 IS NOT NULL AND start_ts>=? ORDER BY start_ts"
    # Cap the window from the end so a 512 MB host does not unpack thousands of 1s-bar blobs at once.
    rows = db.q(q, (since_ts,))
    if max_rounds and len(rows) > max_rounds:
        rows = rows[-int(max_rounds):]
    for r in rows:
        T = r["start_ts"]; cut = T + 90
        i = bisect.bisect_left(m1t, cut); j = bisect.bisect_left(oit, cut); k = bisect.bisect_right(fut, cut)
        f, info = F.compute(T, unpack(r["s1"]), m1[max(0, i - 400):i + 2], json.loads(r["crowd_json"]) if r["crowd_json"] else None,
                            oi[max(0, j - 20):j + 1], fu[max(0, k - 3):k], cut)
        if info.get("bars_round", 0) < 30: continue
        out.append({"T": T, "y": 1 if r["outcome"] == "UP" else 0, "f": f, "regime": info.get("regime"), "dq": info.get("data_quality", 0), "vol": f["atr_pct"]})
    return out

def baselines(ds, rng_seed=42):
    rng = np.random.default_rng(rng_seed)
    g = lambda k: np.array([d["f"][k] for d in ds])
    sig = lambda x: np.where(x > 0, 0.55, np.where(x < 0, 0.45, 0.5))
    crowd = np.array([1 / (1 + math.exp(-d["f"]["crowd_logit"])) if d["f"]["crowd_avail"] else 0.5 for d in ds])
    return {"random": np.where(rng.random(len(ds)) > 0.5, 0.51, 0.49),
            "momentum_30s": sig(g("mom30")), "vwap": sig(g("vwap_dist")), "ema_9_20": sig(g("ema9_20")),
            "displacement_from_open": sig(g("z90")), "polymarket_crowd": crowd}

def walk_forward(ds, archs=("stack", "gbdt", "lr"), init_frac=0.4, step_frac=0.1):
    n = len(ds); rows = [d["f"] for d in ds]; y = np.array([d["y"] for d in ds]); regs = [d["regime"] for d in ds]; dq = [d["dq"] for d in ds]
    start = int(n * init_frac); step = max(20, int(n * step_frac))
    oos = {a: np.full(n, np.nan) for a in archs}; agent_oos = np.full((n, len(AGENT_GROUPS)), np.nan)
    folds = []
    for a0 in range(start, n, step):
        b0 = min(n, a0 + step)
        for a in archs:
            m = ARCH[a]().fit(rows[:a0], y[:a0], regs[:a0], dq[:a0])
            p, P = m.predict(rows[a0:b0], regs[a0:b0], dq[a0:b0]); oos[a][a0:b0] = p
            if a == "stack": agent_oos[a0:b0] = P
        folds.append({"train_n": a0, "test_from": ds[a0]["T"], "test_to": ds[b0 - 1]["T"], "test_n": b0 - a0})
    return oos, agent_oos, start, folds

def segment(p, y, keys):
    out = {}
    for k in sorted(set(keys)):
        m = np.array([kk == k for kk in keys])
        if m.sum(): out[k] = {"n": int(m.sum()), "accuracy": round(float(((p[m] >= 0.5) == y[m]).mean()), 4)}
    return out

def run(db, days=None, archs=("stack", "gbdt", "lr"), max_rounds=None, step_frac=0.1):
    t0 = time.time()
    since = 0 if not days else int(time.time() - days * 86400)
    ds = build_dataset(db, since, max_rounds=max_rounds)
    if len(ds) < 200: return {"error": f"only {len(ds)} real historical rounds available; need >= 200", "n": len(ds)}
    oos, agent_oos, start, folds = walk_forward(ds, archs, step_frac=step_frac)
    test = ds[start:]; y = np.array([d["y"] for d in test])
    res = {"generated_ts": time.time(), "n_rounds": len(ds), "n_test": len(test), "first_round": ds[0]["T"], "last_round": ds[-1]["T"],
           "test_from": test[0]["T"], "folds": folds, "method": "expanding-window walk-forward; each fold trains only on earlier rounds",
           "data": "Binance BTCUSDT 1s/1m klines, Polymarket Gamma official outcomes, Polymarket CLOB price history (<=T90), OKX OI & funding",
           "models": {}, "baselines": {}, "agents": {}}
    for a in archs:
        p = oos[a][start:]; m = metrics(p, y, confidence_class)
        m["by_regime"] = segment(p, y, [d["regime"] for d in test])
        vt = np.quantile([d["vol"] for d in test], [1 / 3, 2 / 3])
        m["by_volatility"] = segment(p, y, ["1 low" if d["vol"] < vt[0] else "2 mid" if d["vol"] < vt[1] else "3 high" for d in test])
        hits = ((p >= 0.5) == y).astype(float)
        m["rolling_100"] = [round(float(hits[max(0, i - 100):i].mean()), 4) for i in range(100, len(hits) + 1, 25)]
        res["models"][a] = m
    for k, p in baselines(ds).items():
        res["baselines"][k] = metrics(p[start:], y)
    for j, (g, name) in enumerate(AGENT_GROUPS):
        res["agents"][g] = {"name": name, **{k: v for k, v in metrics(agent_oos[start:, j], y).items() if k in ("n", "accuracy", "ci95", "brier", "log_loss")}}
    # incumbent = documented six-agent stack; replaced only if another architecture is SIGNIFICANTLY better out-of-sample
    tests = {a: paired_logloss_test(oos["stack"][start:], oos[a][start:], y) for a in archs if a != "stack"} if "stack" in archs else {}
    best = "stack" if "stack" in archs else archs[0]
    for a, t in tests.items():
        if t["b_significantly_better"] and (best == "stack" or t["mean"] < tests[best]["mean"]): best = a
    res["arch_tests"] = tests; res["champion_arch"] = best
    res["lowest_logloss_arch"] = min(archs, key=lambda a: res["models"][a]["log_loss"])
    bb = max(res["baselines"].items(), key=lambda kv: kv[1]["accuracy"])
    hb = ((oos[best][start:] >= 0.5) == y).astype(float); 
    bp = baselines(ds)[bb[0]][start:]; hbb = ((bp >= 0.5) == y).astype(float)
    d = hb - hbb; se = d.std(ddof=1) / math.sqrt(len(d))
    res["vs_best_baseline"] = {"baseline": bb[0], "diff_accuracy": round(float(d.mean()), 4), "ci95": [round(float(d.mean() - 1.96 * se), 4), round(float(d.mean() + 1.96 * se), 4)],
                               "significant": bool(d.mean() - 1.96 * se > 0)}
    # feature importance: standardized coefficients of a stack model trained on all data
    sm = StackModel().fit([d["f"] for d in ds], np.array([d["y"] for d in ds]), [d["regime"] for d in ds], [d["dq"] for d in ds])
    imp = {}
    for g, _ in AGENT_GROUPS:
        lr = sm.agents[g][-1]
        imp[g] = sorted(({"feature": f, "coef": round(float(c), 4)} for f, c in zip(sm._g(g), lr.coef_[0])), key=lambda x: -abs(x["coef"]))
    res["feature_importance"] = imp; res["validator_weights"] = sm.weights()
    res["runtime_s"] = round(time.time() - t0, 1)
    return res

def paired_logloss_test(p_a, p_b, y):
    """Paired difference of per-round log loss (b - a). Negative mean => b better. Returns mean and 95% CI."""
    eps = 1e-4
    la = -(y * np.log(np.clip(p_a, eps, 1 - eps)) + (1 - y) * np.log(np.clip(1 - p_a, eps, 1)))
    lb = -(y * np.log(np.clip(p_b, eps, 1 - eps)) + (1 - y) * np.log(np.clip(1 - p_b, eps, 1)))
    d = lb - la; se = d.std(ddof=1) / math.sqrt(len(d))
    return {"mean": round(float(d.mean()), 5), "ci95": [round(float(d.mean() - 1.96 * se), 5), round(float(d.mean() + 1.96 * se), 5)], "n": len(d),
            "b_significantly_better": bool(d.mean() + 1.96 * se < 0)}

def select_champion(db, res):
    """Uses the walk-forward result's significance-gated choice (see run())."""
    return res["champion_arch"], res.get("arch_tests", {}), build_dataset(db)
