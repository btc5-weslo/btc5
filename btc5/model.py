"""Statistical models.

Architecture ("stack"):
  * Agents 1-5: one regularised logistic regression per DISJOINT feature group (features.GROUPS) -> calibrated P(UP) each.
    Disjoint inputs keep the agents independent: no agent sees another agent's features.
  * Agent 6 (validator): logistic regression stacked on the five agents' out-of-fold log-odds + market regime + data quality.
    Its coefficients ARE the learned reliability weights: an agent with no out-of-sample predictive value gets ~0 weight,
    and correlated agents cannot double-count because the stacker fits them jointly.
Challenger architectures: gradient-boosted trees on all features ("gbdt"), and a single logistic regression ("lr").
Selection = walk-forward out-of-sample log loss, never in-sample accuracy."""
import math, pickle, time, hashlib
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import make_pipeline
from sklearn.ensemble import HistGradientBoostingClassifier
from .features import GROUPS, ALL_FEATURES, REGIMES, LEGACY_GROUPS, LEGACY_ALL

AGENT_GROUPS = [("TREND", "AGENT 1 · TREND & MOMENTUM"), ("STRUCTURE", "AGENT 2 · MARKET STRUCTURE"), ("ORDERFLOW", "AGENT 3 · ORDER FLOW & LIQUIDITY"),
                ("VOLATILITY", "AGENT 4 · VOLATILITY & QUANT"), ("DERIVS", "AGENT 5 · DERIVATIVES & SENTIMENT")]
EPS = 1e-4

def logit(p): p = np.clip(p, EPS, 1 - EPS); return np.log(p / (1 - p))

def mat(rows, cols): return np.array([[r[c] for c in cols] for r in rows], dtype=float)

def regime_onehot(regs): return np.array([[1.0 if r == g else 0.0 for g in REGIMES] for r in regs])

def _lr(C): return make_pipeline(StandardScaler(), LogisticRegression(C=C, max_iter=2000))

class StackModel:
    kind = "stack"
    def __init__(self, C_agent=0.3, C_meta=1.0):
        self.C_agent, self.C_meta = C_agent, C_meta
        self.agents = {}; self.meta = None
    def _g(self, g): return getattr(self, "groups", LEGACY_GROUPS)[g]     # feature list frozen at fit time
    def _meta_X(self, P, regs, dq):
        return np.hstack([logit(P), regime_onehot(regs), np.array(dq)[:, None]])
    def fit(self, rows, y, regs, dq, inner=4):
        y = np.asarray(y); n = len(y); self.groups = {g: list(fs) for g, fs in GROUPS.items()}
        # out-of-fold agent predictions (time-ordered expanding folds) for the stacker
        oof = np.full((n, len(AGENT_GROUPS)), np.nan)
        edges = np.linspace(int(n * 0.3), n, inner + 1).astype(int)
        for a, b in zip(edges[:-1], edges[1:]):
            for j, (g, _) in enumerate(AGENT_GROUPS):
                m = _lr(self.C_agent).fit(mat(rows[:a], self._g(g)), y[:a]); oof[a:b, j] = m.predict_proba(mat(rows[a:b], self._g(g)))[:, 1]
        ok = ~np.isnan(oof[:, 0])
        self.logit_sd = logit(oof[ok]).std(axis=0)
        self.meta = LogisticRegression(C=self.C_meta, max_iter=2000).fit(self._meta_X(oof[ok], [regs[i] for i in np.where(ok)[0]], np.array(dq)[ok]), y[ok])
        for g, _ in AGENT_GROUPS: self.agents[g] = _lr(self.C_agent).fit(mat(rows, self._g(g)), y)
        return self
    def agent_probs(self, rows):
        return np.column_stack([self.agents[g].predict_proba(mat(rows, self._g(g)))[:, 1] for g, _ in AGENT_GROUPS])
    def predict(self, rows, regs, dq):
        P = self.agent_probs(rows)
        return self.meta.predict_proba(self._meta_X(P, regs, dq))[:, 1], P
    def weights(self):
        """Standardised influence of each specialist on the final log-odds: coefficient x SD of its out-of-fold log-odds.
        ~0 => no independent out-of-sample predictive contribution."""
        c = self.meta.coef_[0]; sd = getattr(self, "logit_sd", np.ones(len(AGENT_GROUPS)))
        return {g: round(float(c[i] * sd[i]), 4) for i, (g, _) in enumerate(AGENT_GROUPS)}

class GBDTModel:
    kind = "gbdt"
    def __init__(self): self.m = None; self.stack = StackModel()
    def _X(self, rows, regs, dq): return np.hstack([mat(rows, getattr(self, "features", LEGACY_ALL)), regime_onehot(regs), np.array(dq)[:, None]])
    def fit(self, rows, y, regs, dq):
        self.features = list(ALL_FEATURES)
        self.m = HistGradientBoostingClassifier(max_depth=3, learning_rate=0.04, max_iter=250, l2_regularization=1.0, min_samples_leaf=40).fit(self._X(rows, regs, dq), y)
        self.stack.fit(rows, y, regs, dq)     # agents still provide the specialist views
        return self
    def predict(self, rows, regs, dq):
        return self.m.predict_proba(self._X(rows, regs, dq))[:, 1], self.stack.agent_probs(rows)
    def weights(self): return self.stack.weights()

class LRModel:
    kind = "lr"
    def __init__(self): self.m = None
    def fit(self, rows, y, regs, dq):
        self.features = list(ALL_FEATURES); self.m = _lr(0.3).fit(np.hstack([mat(rows, self.features), regime_onehot(regs)]), y); return self
    def predict(self, rows, regs, dq): return self.m.predict_proba(np.hstack([mat(rows, getattr(self, "features", LEGACY_ALL)), regime_onehot(regs)]))[:, 1], None

ARCH = {"stack": StackModel, "gbdt": GBDTModel, "lr": LRModel}

def metrics(p, y, conf_fn=None):
    p = np.clip(np.asarray(p, float), EPS, 1 - EPS); y = np.asarray(y)
    n = len(y)
    if n == 0: return {"n": 0}
    pred = (p >= 0.5).astype(int)   # >= : ties resolve UP, mirroring the contract
    acc = float((pred == y).mean())
    se = math.sqrt(acc * (1 - acc) / n)
    out = {"n": n, "accuracy": round(acc, 4), "ci95": [round(acc - 1.96 * se, 4), round(acc + 1.96 * se, 4)],
           "brier": round(float(((p - y) ** 2).mean()), 4), "log_loss": round(float(-(y * np.log(p) + (1 - y) * np.log(1 - p)).mean()), 4),
           "pred_up_share": round(float(pred.mean()), 4), "actual_up_share": round(float(y.mean()), 4)}
    for side, v in (("up", 1), ("down", 0)):
        m = pred == v
        out[f"{side}_accuracy"] = {"n": int(m.sum()), "accuracy": round(float((y[m] == v).mean()), 4) if m.sum() else None}
    bins = []
    for lo in np.arange(0, 1, 0.1):
        m = (p >= lo) & (p < lo + 0.1 if lo < 0.9 else p <= 1)
        if m.sum(): bins.append({"bin": f"{lo:.1f}-{lo + 0.1:.1f}", "n": int(m.sum()), "mean_p": round(float(p[m].mean()), 4), "observed_up": round(float(y[m].mean()), 4)})
    out["calibration"] = bins
    out["ece"] = round(sum(b["n"] * abs(b["mean_p"] - b["observed_up"]) for b in bins) / n, 4)
    if conf_fn:
        by = {}
        for pi, yi in zip(p, y):
            c = conf_fn(pi); d = by.setdefault(c, [0, 0]); d[0] += 1; d[1] += int((pi >= 0.5) == yi)
        out["by_confidence"] = {k: {"n": v[0], "accuracy": round(v[1] / v[0], 4)} for k, v in by.items()}
    return out

def fingerprint(obj): return hashlib.sha1(pickle.dumps(obj)).hexdigest()[:8]

def save(db, model, role, n_train, metrics_, train_end_ts):
    blob = pickle.dumps(model)
    ver = f"{model.kind}-{time.strftime('%Y%m%d-%H%M%S')}-{hashlib.sha1(blob).hexdigest()[:6]}"
    metrics_ = dict(metrics_, train_end_ts=train_end_ts)
    import json
    db.x("INSERT INTO model_versions VALUES (?,?,?,?,?,?,?)", (ver, time.time(), model.kind, role, n_train, json.dumps(metrics_), blob))
    return ver

def load(db, version):
    r = db.one("SELECT * FROM model_versions WHERE version=?", (version,))
    return pickle.loads(r["blob"]) if r else None
