"""Feature engineering. ONE implementation is used by both the live engine and the backtester.

Information constraint: every function receives `cutoff` (= T0 + 90 s) and itself discards any input that is
timestamped at or after the cutoff (1 s bar [t, t+1) needs t+1 <= cutoff; 1 m bar needs ts+60 <= cutoff;
OI 5 m bar needs ts+300 <= cutoff; crowd/funding points need t <= cutoff). Tests verify that appending future
data never changes any feature.

Inputs (all real observations):
  s1:      Binance BTCUSDT 1-second bars [t, o, h, l, c, v, n_trades, taker_buy_v]
  m1:      Binance 1-minute bars [t, o, h, l, c, v, qv, n, taker_buy_v]
  crowd:   Polymarket UP-token prices [(t, p)]
  oi:      OKX BTC open interest 5 m [(t, oi_usd)]
  funding: OKX realized funding [(t, rate)]
Missing inputs produce feature value 0 plus an availability flag = 0, never an invented observation."""
import math
import numpy as np

GROUPS = {
    "TREND": ["mom30", "mom10", "accel", "ema9_20", "ema20_50", "px_ema200", "vwap_dist", "macd_h", "rsi", "stoch_rsi"],
    "STRUCTURE": ["range_pos", "dist_hi60", "dist_lo60", "breakout", "failed_bo", "hh_hl", "sweep", "wick", "adx_dir"],
    "ORDERFLOW": ["tbr_round", "tbr_30", "tbr_pre", "cvd", "relvol", "vol_accel", "trade_size"],
    "VOLATILITY": ["z_rem", "z90", "rv_ratio", "atr_pct", "skew", "jump", "disp_atr", "z_twap", "z_twap_rem"],
    "DERIVS": ["crowd_logit", "crowd_avail", "crowd_age", "crowd_delta", "oi15", "oi60", "oi_avail", "oi_px", "funding"],
}
ALL_FEATURES = [f for g in GROUPS.values() for f in g]
# Feature lists used by models trained before the TWAP-reference features existed (pickled models carry no list).
NEW_IN_V2 = {"z_twap", "z_twap_rem"}
LEGACY_GROUPS = {g: [f for f in fs if f not in NEW_IN_V2] for g, fs in GROUPS.items()}
LEGACY_ALL = [f for g in LEGACY_GROUPS.values() for f in g]
REGIMES = ["STRONG BULLISH TREND", "STRONG BEARISH TREND", "SIDEWAYS CONSOLIDATION", "HIGH-VOLATILITY BREAKOUT",
           "LOW-VOLATILITY COMPRESSION", "REVERSAL CONDITIONS", "ABNORMAL CONDITIONS"]

def _ema(x, n):
    a = 2 / (n + 1); e = x[0]; out = []
    for v in x: e = a * v + (1 - a) * e; out.append(e)
    return np.array(out)

def _rsi(c, n=14):
    if len(c) < n + 1: return None
    d = np.diff(c); g = np.where(d > 0, d, 0); l = np.where(d < 0, -d, 0)
    ag = g[:n].mean(); al = l[:n].mean(); out = []
    for i in range(n, len(d)):
        ag = (ag * (n - 1) + g[i]) / n; al = (al * (n - 1) + l[i]) / n
        out.append(100 if al == 0 else 100 - 100 / (1 + ag / al))
    return np.array(out) if out else None

def _atr(h, l, c, n=14):
    if len(c) < n + 1: return None
    tr = np.maximum(h[1:] - l[1:], np.maximum(abs(h[1:] - c[:-1]), abs(l[1:] - c[:-1])))
    return float(tr[-n:].mean())

def _adx(h, l, c, n=14):
    if len(c) < 2 * n + 1: return None, None
    up = h[1:] - h[:-1]; dn = l[:-1] - l[1:]
    pdm = np.where((up > dn) & (up > 0), up, 0); mdm = np.where((dn > up) & (dn > 0), dn, 0)
    tr = np.maximum(h[1:] - l[1:], np.maximum(abs(h[1:] - c[:-1]), abs(l[1:] - c[:-1])))
    k = np.ones(n) / n
    atr = np.convolve(tr, k, "valid"); pdi = 100 * np.convolve(pdm, k, "valid") / np.maximum(atr, 1e-9); mdi = 100 * np.convolve(mdm, k, "valid") / np.maximum(atr, 1e-9)
    dx = 100 * abs(pdi - mdi) / np.maximum(pdi + mdi, 1e-9)
    return float(dx[-n:].mean()), float(pdi[-1] - mdi[-1])

def clip(x, a=5.0):
    return float(max(-a, min(a, x))) if x is not None and math.isfinite(x) else 0.0

def eligible(T0, s1, m1, crowd, oi, funding, cutoff):
    """Strict information filter at the cutoff."""
    return ([b for b in s1 if b[0] + 1 <= cutoff], [b for b in m1 if b[0] + 60 <= cutoff],
            [p for p in (crowd or []) if p[0] <= cutoff], [p for p in (oi or []) if p[0] + 300 <= cutoff],
            [p for p in (funding or []) if p[0] <= cutoff])

def compute(T0, s1, m1, crowd=None, oi=None, funding=None, cutoff=None):
    """Return (features dict, info dict). info holds human-readable, real derived values (prices, levels, regime, quality)."""
    cutoff = T0 + 90 if cutoff is None else cutoff
    s1, m1, crowd, oi, funding = eligible(T0, s1, m1, crowd, oi, funding, cutoff)
    f = {k: 0.0 for k in ALL_FEATURES}
    info = {"cutoff": cutoff, "missing": []}
    pre = [b for b in s1 if T0 - 600 <= b[0] < T0]
    rnd = [b for b in s1 if T0 <= b[0] < cutoff]
    info["bars_round"] = len(rnd); info["bars_pre"] = len(pre)
    expected = max(1, int(cutoff - T0))
    info["coverage"] = round(min(1.0, len(rnd) / expected), 3)
    if not rnd:
        info["missing"].append("no BTC price bars inside the round before cutoff")
        info["data_quality"] = 0.0
        return f, info
    # reference open: first 1s bar open at T0 (or last close before T0)
    b0 = next((b for b in rnd if b[0] == T0), None)
    p0 = b0[1] if b0 else (pre[-1][4] if pre else rnd[0][1])
    P = rnd[-1][4]
    info.update(open_ref=p0, price=P, open_ref_source="Binance 1s bar at T0" if b0 else "Binance last close before T0 (T0 bar missing)")
    closes_pre = np.array([b[4] for b in pre]) if len(pre) > 30 else None
    rets_pre = np.diff(np.log(closes_pre)) if closes_pre is not None else np.array([])
    rc = np.array([p0] + [b[4] for b in rnd])
    rets_in = np.diff(np.log(rc))
    sig_pre = float(rets_pre.std()) if len(rets_pre) > 30 else None
    sig_in = float(rets_in.std()) if len(rets_in) > 10 else None
    sig = sig_pre or sig_in or 1e-4
    sig = max(sig, 1e-6)
    sig_eff = math.sqrt(0.6 * sig ** 2 + 0.4 * (sig_in or sig) ** 2)
    disp = math.log(P / p0)
    el = max(1.0, rnd[-1][0] + 1 - T0); rem = max(1.0, T0 + 300 - (rnd[-1][0] + 1))
    def ret_back(n):
        i = max(0, len(rc) - 1 - n); return math.log(rc[-1] / rc[i]), len(rc) - 1 - i
    r30, n30 = ret_back(30); r10, n10 = ret_back(10); r60, n60 = ret_back(60)
    f["mom30"] = clip(r30 / (sig * math.sqrt(max(n30, 1))))
    f["mom10"] = clip(r10 / (sig * math.sqrt(max(n10, 1))))
    f["accel"] = clip((r30 - (r60 - r30)) / (sig * math.sqrt(30)))
    # volatility / quant
    f["z90"] = clip(disp / (sig * math.sqrt(el)), 8)
    f["z_rem"] = clip(disp / (sig_eff * math.sqrt(rem)), 8)
    info["p_random_walk"] = round(0.5 * (1 + math.erf(f["z_rem"] / math.sqrt(2))), 4)
    # Resolution-aligned reference: the contract compares Chainlink's 60s-TWAP at the END with its value at the START
    # (= mean price over [T0-60, T0)). Measure the T90 price against that pre-open mean (real pre-T0 bars only).
    pre60 = [b[4] for b in pre if b[0] >= T0 - 60]
    ref = sum(pre60) / len(pre60) if len(pre60) >= 30 else p0
    dt = math.log(P / ref)
    f["z_twap"] = clip(dt / (sig * math.sqrt(el)), 8)
    f["z_twap_rem"] = clip(dt / (sig_eff * math.sqrt(max(30.0, rem - 30))), 8)
    info["twap_ref"] = round(ref, 2); info["twap_ref_source"] = "mean of Binance 1s closes in [T0-60,T0)" if len(pre60) >= 30 else "T0 open (pre-open bars missing)"
    f["rv_ratio"] = clip(math.log((sig_in or sig) / sig), 3)
    if len(rets_in) > 10 and rets_in.std() > 0:
        z = (rets_in - rets_in.mean()) / rets_in.std()
        f["skew"] = clip(float((z ** 3).mean()), 4)
        f["jump"] = clip(float(abs(rets_in).max() / sig) / 5, 4)
    # 1-minute context
    c1 = np.array([b[4] for b in m1[-300:]]); h1 = np.array([b[2] for b in m1[-300:]]); l1 = np.array([b[3] for b in m1[-300:]])
    atr = _atr(h1, l1, c1) if len(c1) > 15 else None
    if atr is None: info["missing"].append("1-minute history (EMA/ATR/RSI context)")
    atr = atr or (sig * P * math.sqrt(60))
    info["atr"] = round(atr, 2)
    f["atr_pct"] = clip(math.log(max(atr / P, 1e-6) / 6e-4), 3)
    f["disp_atr"] = clip((P - p0) / atr)
    if len(c1) >= 30:
        cx = np.append(c1, P)
        e9, e20, e50 = _ema(cx, 9)[-1], _ema(cx, 20)[-1], _ema(cx, 50)[-1]
        e200 = _ema(cx, 200)[-1] if len(cx) >= 200 else None
        f["ema9_20"] = clip((e9 - e20) / atr); f["ema20_50"] = clip((e20 - e50) / atr)
        f["px_ema200"] = clip((P - e200) / atr / 3) if e200 is not None else 0.0
        info["ema"] = {"ema9": round(e9, 2), "ema20": round(e20, 2), "ema50": round(e50, 2), "ema200": None if e200 is None else round(e200, 2)}
        m12, m26 = _ema(cx, 12), _ema(cx, 26); macd = m12 - m26; sigl = _ema(macd, 9)
        f["macd_h"] = clip((macd[-1] - sigl[-1]) / atr * 3)
        info["macd"] = {"macd": round(float(macd[-1]), 2), "signal": round(float(sigl[-1]), 2), "hist": round(float(macd[-1] - sigl[-1]), 2)}
        r = _rsi(cx)
        if r is not None:
            f["rsi"] = clip((r[-1] - 50) / 25); info["rsi"] = round(float(r[-1]), 1)
            if len(r) >= 14:
                w = r[-14:]; rng = w.max() - w.min()
                f["stoch_rsi"] = clip(((r[-1] - w.min()) / rng - 0.5) * 2 if rng > 0 else 0)
        adx, di = _adx(np.append(h1, max(P, rnd[-1][2])), np.append(l1, min(P, rnd[-1][3])), cx)
        if adx is not None:
            f["adx_dir"] = clip(adx / 25 * (1 if di > 0 else -1)); info["adx"] = round(adx, 1)
        prior60 = m1[-60:]; prior30 = m1[-30:]
        hi60 = max(b[2] for b in prior60); lo60 = min(b[3] for b in prior60)
        hi30 = max(b[2] for b in prior30); lo30 = min(b[3] for b in prior30)
        f["dist_hi60"] = clip((P - hi60) / atr); f["dist_lo60"] = clip((P - lo60) / atr)
        rh = max(b[2] for b in rnd); rl = min(b[3] for b in rnd)
        f["breakout"] = 1.0 if P > hi30 else (-1.0 if P < lo30 else 0.0)
        f["failed_bo"] = (-1.0 if rh > hi30 and P < hi30 else 0.0) + (1.0 if rl < lo30 and P > lo30 else 0.0)
        f["sweep"] = (1.0 if rl < lo60 and P > lo60 else 0.0) - (1.0 if rh > hi60 and P < hi60 else 0.0)
        info["levels"] = {"resistance_60m": round(hi60, 2), "support_60m": round(lo60, 2), "high_30m": round(hi30, 2), "low_30m": round(lo30, 2)}
        # regime
        slope = (_ema(cx, 20)[-1] - _ema(cx, 20)[-16]) / atr if len(cx) > 20 else 0
        atrs = [_atr(h1[i - 15:i], l1[i - 15:i], c1[i - 15:i]) for i in range(15, len(c1), 15)]
        atrs = [a for a in atrs if a]
        pct = float(np.mean(np.array(atrs) < atr)) if len(atrs) > 5 else 0.5
        info["vol_percentile"] = round(pct, 2)
        prev_slope = (_ema(cx, 20)[-16] - _ema(cx, 20)[-31]) / atr if len(cx) > 35 else 0
        if f["jump"] > 2.0 or info["coverage"] < 0.6: reg = "ABNORMAL CONDITIONS"
        elif pct > 0.85 and abs(f["breakout"]) == 1: reg = "HIGH-VOLATILITY BREAKOUT"
        elif slope > 1.2 and (adx or 0) > 22: reg = "STRONG BULLISH TREND"
        elif slope < -1.2 and (adx or 0) > 22: reg = "STRONG BEARISH TREND"
        elif abs(prev_slope) > 1.0 and slope * prev_slope < 0: reg = "REVERSAL CONDITIONS"
        elif pct < 0.2: reg = "LOW-VOLATILITY COMPRESSION"
        else: reg = "SIDEWAYS CONSOLIDATION"
        info["regime"] = reg
    else:
        info["regime"] = "ABNORMAL CONDITIONS" if info["coverage"] < 0.6 else "SIDEWAYS CONSOLIDATION"
        info["missing"].append("insufficient 1-minute bars for structure/indicators")
    rh = max(b[2] for b in rnd); rl = min(b[3] for b in rnd)
    f["range_pos"] = clip(((P - rl) / (rh - rl) - 0.5) * 2) if rh > rl else 0.0
    segs = [rnd[i::1][j * 30:(j + 1) * 30] for i in (0,) for j in range(3)]
    segs = [s for s in segs if s]
    if len(segs) >= 2:
        sc = 0
        for a, b in zip(segs[:-1], segs[1:]):
            sc += (1 if max(x[2] for x in b) > max(x[2] for x in a) else -1) + (1 if min(x[3] for x in b) > min(x[3] for x in a) else -1)
        f["hh_hl"] = sc / 4
    last = rnd[-30:]; o, c = last[0][1], last[-1][4]; hh = max(x[2] for x in last); ll = min(x[3] for x in last)
    f["wick"] = clip(((min(o, c) - ll) - (hh - max(o, c))) / (hh - ll)) if hh > ll else 0.0
    # VWAP of the round
    vv = sum(b[5] for b in rnd)
    if vv > 0:
        vwap = sum((b[2] + b[3] + b[4]) / 3 * b[5] for b in rnd) / vv
        f["vwap_dist"] = clip((P - vwap) / (P * sig * math.sqrt(el))); info["vwap"] = round(vwap, 2)
    # order flow (executed taker volume — not resting liquidity)
    def tbr(bars):
        v = sum(b[5] for b in bars); return (sum(b[7] for b in bars) / v - 0.5) if v > 0 else 0.0
    f["tbr_round"] = clip(tbr(rnd) * 4); f["tbr_30"] = clip(tbr(rnd[-30:]) * 4); f["tbr_pre"] = clip(tbr(pre) * 4)
    vpre = sum(b[5] for b in pre) / max(1, len(pre)); vin = vv / max(1, len(rnd))
    f["cvd"] = clip(sum(2 * b[7] - b[5] for b in rnd) / max(vpre * 90, 1e-9))
    f["relvol"] = clip(math.log(max(vin, 1e-9) / max(vpre, 1e-9)), 4) if vpre > 0 and vin > 0 else 0.0
    v30 = sum(b[5] for b in rnd[-30:]) / max(1, len(rnd[-30:])); v60 = sum(b[5] for b in rnd[:-30]) / max(1, len(rnd[:-30]))
    f["vol_accel"] = clip(math.log(v30 / v60), 4) if v30 > 0 and v60 > 0 else 0.0
    npre = sum(b[6] for b in pre); nin = sum(b[6] for b in rnd)
    f["trade_size"] = clip(math.log((vv / nin) / (sum(b[5] for b in pre) / npre)), 4) if nin and npre and vv > 0 and sum(b[5] for b in pre) > 0 else 0.0
    info["taker_buy_share"] = round(tbr(rnd) + 0.5, 4) if vv > 0 else None
    info["round_volume_btc"] = round(vv, 3)
    # derivatives & sentiment
    if crowd:
        p = min(max(crowd[-1][1], 0.01), 0.99)
        f["crowd_logit"] = clip(math.log(p / (1 - p))); f["crowd_avail"] = 1.0
        f["crowd_age"] = clip((cutoff - crowd[-1][0]) / 60, 3)
        f["crowd_delta"] = clip((crowd[-1][1] - crowd[0][1]) * 4)
        info["crowd_up"] = crowd[-1][1]; info["crowd_ts"] = crowd[-1][0]
    else: info["missing"].append("Polymarket crowd price")
    if len(oi) >= 13:
        o_ = [x[1] for x in oi]
        f["oi15"] = clip((o_[-1] / o_[-4] - 1) * 200); f["oi60"] = clip((o_[-1] / o_[-13] - 1) * 100); f["oi_avail"] = 1.0
        pchg = (P / m1[-15][4] - 1) if len(m1) >= 15 else 0
        f["oi_px"] = clip(np.sign(pchg) * f["oi15"])
        info["oi_usd"] = o_[-1]
    else: info["missing"].append("OKX open-interest history")
    if funding:
        f["funding"] = clip(funding[-1][1] * 1e4 / 2); info["funding"] = funding[-1][1]
    else: info["missing"].append("funding rate")
    q = info["coverage"] * (1.0 if len(pre) >= 300 else 0.7) * (1.0 if len(m1) >= 60 else 0.75) * (1.0 if crowd else 0.9)
    info["data_quality"] = round(q, 3)
    info["sigma_1s"] = sig; info["disp_bps"] = round(disp * 1e4, 2)
    return f, info

def tie_break(f, info):
    """Deterministic tie-break when P(UP) == P(DOWN) exactly:
    1) sign of displacement from the opening reference, 2) sign of round taker imbalance, 3) sign of last-10s momentum,
    4) UP — because the contract resolves UP when the end price is >= the opening price (ties resolve UP)."""
    for name, v in (("displacement", info.get("disp_bps", 0)), ("taker imbalance", f.get("tbr_round", 0)), ("10s momentum", f.get("mom10", 0))):
        if v and v != 0: return ("UP" if v > 0 else "DOWN"), name
    return "UP", "contract rule (>= open resolves UP)"
