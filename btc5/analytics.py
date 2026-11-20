"""Performance Center: statistics computed ONLY from recorded, officially-resolved rounds. Sample sizes everywhere."""
import math, json
from .config import CONFIDENCE_ORDER

MIN_CONCLUSIVE = 100

def _acc(rows):
    n = len(rows)
    if not n: return {"n": 0, "accuracy": None}
    k = sum(1 for r in rows if r["result"] == "CORRECT"); a = k / n
    lo, hi = wilson(k, n)
    return {"n": n, "correct": k, "accuracy": round(a, 4), "ci95": [round(lo, 4), round(hi, 4)], "conclusive": n >= MIN_CONCLUSIVE}

def wilson(k, n, z=1.96):
    if n == 0: return (0, 0)
    p = k / n; d = 1 + z * z / n; c = (p + z * z / (2 * n)) / d; h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return c - h, c + h

def performance(db, backtest=None):
    allr = db.q("SELECT * FROM rounds ORDER BY start_ts")
    res = [r for r in allr if r["result"] in ("CORRECT", "INCORRECT")]
    rev = list(reversed(res))
    out = {"total_recorded": len(allr), "resolved_predictions": len(res),
           "pending": sum(1 for r in allr if r["result"] == "PENDING"), "missed": sum(1 for r in allr if r["status"] == "MISSED"),
           "excluded": sum(1 for r in allr if r["status"] in ("MARKET_UNVERIFIED", "MONITORING_ONLY")),
           "unverified": sum(1 for r in allr if r["result"] == "UNVERIFIED"),
           "overall": _acc(res), "last": {str(k): _acc(rev[:k]) for k in (10, 25, 50, 100)},
           "up": _acc([r for r in res if r["prediction"] == "UP"]), "down": _acc([r for r in res if r["prediction"] == "DOWN"]),
           "high_conf": _acc([r for r in res if r["confidence"] in ("HIGH", "VERY HIGH")]),
           "low_conf": _acc([r for r in res if r["confidence"] in ("VERY LOW", "LOW")]),
           "by_confidence": {c: _acc([r for r in res if r["confidence"] == c]) for c in CONFIDENCE_ORDER},
           "by_regime": {}, "calibration": [], "streaks": {}, "timing": {}, "agents": {}}
    for reg in sorted({r["regime"] for r in res if r["regime"]}): out["by_regime"][reg] = _acc([r for r in res if r["regime"] == reg])
    for lo in [i / 10 for i in range(10)]:
        b = [r for r in res if lo <= r["p_up"] < lo + 0.1 or (lo == 0.9 and r["p_up"] == 1)]
        if b: out["calibration"].append({"bin": f"{lo:.1f}-{lo+0.1:.1f}", "n": len(b), "mean_p": round(sum(r["p_up"] for r in b) / len(b), 4),
                                         "observed_up": round(sum(1 for r in b if r["official_outcome"] == "UP") / len(b), 4)})
    if res:
        eps = 1e-4
        y = [1 if r["official_outcome"] == "UP" else 0 for r in res]; p = [min(max(r["p_up"], eps), 1 - eps) for r in res]
        out["brier"] = round(sum((a - b) ** 2 for a, b in zip(p, y)) / len(y), 4)
        out["log_loss"] = round(-sum(b * math.log(a) + (1 - b) * math.log(1 - a) for a, b in zip(p, y)) / len(y), 4)
    cur = None; run = 0; best = {"CORRECT": 0, "INCORRECT": 0}
    for r in res:
        run = run + 1 if r["result"] == cur else 1; cur = r["result"]; best[cur] = max(best[cur], run)
    out["streaks"] = {"current": {"type": cur, "length": run} if cur else None, "max_correct": best["CORRECT"], "max_incorrect": best["INCORRECT"]}
    dev = [r["deviation_ms"] for r in allr if r["status"] != "MISSED" and r["deviation_ms"] is not None]
    if dev:
        s = sorted(dev)
        out["timing"] = {"n": len(dev), "mean_ms": round(sum(dev) / len(dev), 3), "median_ms": round(s[len(s) // 2], 3), "max_ms": round(max(dev), 3),
                         "min_ms": round(min(dev), 3), "early": sum(1 for d in dev if d < 0)}
    for a in db.q("SELECT agent, COUNT(*) n, SUM(correct) k FROM agent_results ar JOIN rounds r USING(round_id) WHERE r.status='LOCKED' GROUP BY agent"):
        out["agents"][a["agent"]] = {"n": a["n"], "accuracy": round(a["k"] / a["n"], 4) if a["n"] else None}
    # drift detection vs walk-forward expectation
    if backtest and res:
        exp = backtest.get("accuracy")
        last = rev[:50]; k = sum(1 for r in last if r["result"] == "CORRECT"); n = len(last)
        if exp and n >= 20:
            se = math.sqrt(exp * (1 - exp) / n); z = (k / n - exp) / se
            out["drift"] = {"window": n, "live_accuracy": round(k / n, 4), "expected": exp, "z": round(z, 2), "alert": z < -2.0}
    fa = {}
    for r in res:
        if r["failure_analysis"]:
            for x in json.loads(r["failure_analysis"]): fa[x] = fa.get(x, 0) + 1
    out["failure_causes"] = dict(sorted(fa.items(), key=lambda kv: -kv[1]))
    return out
