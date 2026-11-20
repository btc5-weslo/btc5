"""Training / promotion job. Runs in a SEPARATE PROCESS so CPU work can never delay a T90 decision event.
Champion/challenger: a new model is promoted only after it is significantly better (paired log-loss CI < 0) on at least
`promotion_min_rounds` real rounds that occurred AFTER the champion's training data ended."""
import asyncio, json, time, math
import numpy as np
from .config import Config
from .db import DB
from . import backtest as BT, history
from .model import ARCH, save, load, metrics

def job(do_backfill=True, days=None):
    cfg = Config(); db = DB(cfg.db_path); out = {"started": time.time()}
    if do_backfill:
        try: out["backfilled"] = asyncio.run(history.backfill(cfg, db, days or cfg.backfill_days))
        except Exception as e: out["backfill_error"] = str(e)
    champ_ver = db.kv_get("champion")
    res = BT.run(db, days)
    if "error" in res: out["error"] = res["error"]; return out
    db.x("INSERT INTO backtests(ts,params_json,results_json) VALUES (?,?,?)", (time.time(), json.dumps({"days": days, "trigger": "training job"}), json.dumps(res, default=str)))
    winner, tests, ds = BT.select_champion(db, res)
    out["arch_tests"] = tests; out["arch_winner"] = winner
    rows = [d["f"] for d in ds]; y = np.array([d["y"] for d in ds]); regs = [d["regime"] for d in ds]; dq = [d["dq"] for d in ds]
    rel = res["agents"]
    def meta_for(arch):
        m = res["models"].get(arch) or res["models"][winner]
        return {"walk_forward": {k: m[k] for k in ("n", "accuracy", "ci95", "brier", "log_loss", "ece")}, "walk_forward_arch": arch,
                "agent_reliability": rel, "confidence_accuracy": m.get("by_confidence", {})}
    meta = meta_for(winner)
    if not champ_ver:
        m = ARCH[winner]().fit(rows, y, regs, dq)
        v = save(db, m, "champion", len(ds), meta, ds[-1]["T"]); db.kv_set("champion", v); db.kv_set("reliability", meta); out["promoted"] = v; out["reason"] = "initial champion (walk-forward selected)"
        return out
    cr = db.one("SELECT * FROM model_versions WHERE version=?", (champ_ver,)); cm = json.loads(cr["metrics_json"])
    train_end = cm.get("train_end_ts", 0)
    hold = [i for i, d in enumerate(ds) if d["T"] > train_end]
    status = {"champion": champ_ver, "champion_arch": cr["kind"], "challenger_arch": winner, "new_rounds_since_champion": len(hold), "required": cfg.promotion_min_rounds}
    deployed_arch = cr["kind"]
    if len(hold) >= cfg.promotion_min_rounds:
        h0 = hold[0]
        champ = load(db, champ_ver)
        pc, _ = champ.predict(rows[h0:], regs[h0:], dq[h0:])
        # Challenger = the walk-forward-selected architecture RE-FIT AS NEW DATA ARRIVES: for each block of the holdout it is
        # trained on every round before that block (incl. the new rounds) and predicts the block -> strictly out-of-sample,
        # and it measures exactly what promotion would deploy (a model that has learned from the latest rounds).
        px = challenger_walk_forward(winner, rows, y, regs, dq, h0)
        t = BT.paired_logloss_test(pc, px, y[h0:]); status["holdout_test"] = t
        mc = metrics(pc, y[h0:]); mx = metrics(px, y[h0:])
        status["champion_holdout"] = {k: v for k, v in mc.items() if k in ("n", "accuracy", "log_loss", "brier")}
        status["challenger_holdout"] = {k: v for k, v in mx.items() if k in ("n", "accuracy", "log_loss", "brier")}
        ok, why = promotion_decision(t, mc, mx)
        status["rule"] = why
        if ok:
            m = ARCH[winner]().fit(rows, y, regs, dq)
            v = save(db, m, "champion", len(ds), dict(meta, promoted_over=champ_ver, holdout_test=t), ds[-1]["T"])
            db.kv_set("champion", v); out["promoted"] = v; status["decision"] = "PROMOTED"; deployed_arch = winner
        else:
            status["decision"] = "challenger not better out-of-sample — champion retained"
    else:
        status["decision"] = f"accumulating out-of-sample evidence ({len(hold)}/{cfg.promotion_min_rounds} rounds)"
    # refresh reliability tables (evaluation only) for the architecture that is actually DEPLOYED
    db.kv_set("reliability", meta_for(deployed_arch)); db.kv_set("challenger_status", dict(status, ts=time.time()))
    out["challenger"] = status
    return out

NONINFERIORITY_LL = 0.01     # max tolerated upper-95% log-loss excess when the point estimate favours the challenger
MAX_ACC_DROP = 0.01

def promotion_decision(t, mc, mx):
    """Promote when the re-fit challenger is (a) significantly better, or (b) better on the out-of-sample point estimate
    of log loss, not worse beyond a small non-inferiority margin at 95%, and not losing > 1 pt of accuracy. Rule (b) lets the
    model keep learning from new rounds; it never accepts a model that is measurably worse out-of-sample."""
    if t["b_significantly_better"]: return True, "significantly better log loss (95% CI < 0)"
    if t["mean"] < 0 and t["ci95"][1] < NONINFERIORITY_LL and mx["accuracy"] >= mc["accuracy"] - MAX_ACC_DROP:
        return True, f"better log loss point estimate ({t['mean']:+.4f}), non-inferior (CI upper {t['ci95'][1]:+.4f} < {NONINFERIORITY_LL}), accuracy {mx['accuracy']:.3f} vs {mc['accuracy']:.3f}"
    return False, f"log loss diff {t['mean']:+.4f} CI {t['ci95']}; accuracy {mx['accuracy']:.3f} vs {mc['accuracy']:.3f}"

def challenger_walk_forward(arch, rows, y, regs, dq, h0, blocks=6, min_block=50):
    n = len(y); step = max(min_block, (n - h0 + blocks - 1) // blocks); px = np.full(n - h0, np.nan)
    for a in range(h0, n, step):
        b = min(n, a + step)
        m = ARCH[arch]().fit(rows[:a], y[:a], regs[:a], dq[:a])
        px[a - h0:b - h0] = m.predict(rows[a:b], regs[a:b], dq[a:b])[0]
    return px
