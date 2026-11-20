"""Agent reports. Each specialist's probability comes from its own model (model.StackModel.agents[group]) fitted on its own
disjoint feature group; the text below only DESCRIBES real measured inputs. Nothing here invents a number."""
import math
from .config import confidence_class
from .model import AGENT_GROUPS

def _dir(p): return "UP" if p >= 0.5 else "DOWN"
def _fmt(x, n=2): return "n/a" if x is None else f"{x:,.{n}f}"

def _specialist_reports_raw(f, info, probs, reliability, live):
    """probs: {group: p_up}; reliability: {group: {accuracy, n, ...}} from walk-forward; live: live-only observations."""
    out = {}
    P = info.get("price"); disp = info.get("disp_bps")
    for g, name in AGENT_GROUPS:
        p = probs.get(g)
        rel = reliability.get(g) or {}
        rep = {"agent": g, "name": name, "p_up": None if p is None else round(p, 4), "p_down": None if p is None else round(1 - p, 4),
               "direction": None if p is None else _dir(p), "confidence": None if p is None else confidence_class(p),
               "reliability": {"oos_accuracy": rel.get("accuracy"), "n": rel.get("n"), "brier": rel.get("brier")},
               "status": "OK" if p is not None else "UNAVAILABLE", "evidence": [], "fields": {}}
        if g == "TREND":
            e = info.get("ema") or {}
            rep["fields"] = {"momentum_strength": round(abs(f["mom30"]), 2), "ema_alignment": "bullish" if f["ema9_20"] > 0 and f["ema20_50"] > 0 else "bearish" if f["ema9_20"] < 0 and f["ema20_50"] < 0 else "mixed"}
            rep["evidence"] = [f"30s momentum {f['mom30']:+.2f}σ, 10s {f['mom10']:+.2f}σ, acceleration {f['accel']:+.2f}σ",
                               f"EMA9 {_fmt(e.get('ema9'))} / EMA20 {_fmt(e.get('ema20'))} / EMA50 {_fmt(e.get('ema50'))} / EMA200 {_fmt(e.get('ema200'))}",
                               f"Price vs round VWAP {_fmt(info.get('vwap'))}: {f['vwap_dist']:+.2f}σ; RSI {info.get('rsi','n/a')}; MACD hist {(info.get('macd') or {}).get('hist','n/a')}"]
        elif g == "STRUCTURE":
            lv = info.get("levels") or {}
            cls = "breakout" if f["breakout"] > 0 else "breakdown" if f["breakout"] < 0 else ("failed breakout" if f["failed_bo"] < 0 else "failed breakdown" if f["failed_bo"] > 0 else "inside range")
            rep["fields"] = {"structure": cls, "key_levels": lv, "regime": info.get("regime")}
            rep["evidence"] = [f"Structure: {cls}; position in round range {f['range_pos']:+.2f} (−1 low … +1 high)",
                               f"60m resistance {_fmt(lv.get('resistance_60m'))}, support {_fmt(lv.get('support_60m'))}; higher-highs/lows score {f['hh_hl']:+.2f}",
                               f"Liquidity sweep {'bullish' if f['sweep']>0 else 'bearish' if f['sweep']<0 else 'none'}; ADX {info.get('adx','n/a')}"]
        elif g == "ORDERFLOW":
            bk = live.get("books") or {}
            imb = {k: v.get("imbalance") for k, v in bk.items()}
            rep["fields"] = {"buying_pressure": info.get("taker_buy_share"), "book_imbalance_10bps": imb,
                             "liquidity": "thin" if f["relvol"] < -0.7 else "heavy" if f["relvol"] > 0.7 else "normal"}
            rep["evidence"] = [f"Executed taker-buy share {_fmt((info.get('taker_buy_share') or 0)*100,1)}% of {info.get('round_volume_btc','n/a')} BTC (Binance, round so far)",
                               f"CVD {f['cvd']:+.2f} (vs 90s avg volume); relative volume {math.exp(f['relvol']):.2f}×; volume acceleration {f['vol_accel']:+.2f}",
                               "Resting book imbalance (observed, not model-weighted until enough live samples): " + ", ".join(f"{k} {v:+.2f}" for k, v in imb.items() if v is not None) if imb else "Order book: unavailable"]
        elif g == "VOLATILITY":
            rep["fields"] = {"volatility_regime": "expanding" if f["rv_ratio"] > 0.3 else "compressing" if f["rv_ratio"] < -0.3 else "stable",
                             "random_walk_p_up": info.get("p_random_walk"), "vol_percentile": info.get("vol_percentile")}
            rep["evidence"] = [f"Displacement from open {_fmt(disp)} bps = {f['z90']:+.2f}σ elapsed; z to expiry {f['z_rem']:+.2f} → random-walk P(UP) {info.get('p_random_walk')}",
                               f"ATR(14,1m) ${_fmt(info.get('atr'))}; in-round/pre vol ratio {math.exp(f['rv_ratio']):.2f}×; vol percentile {info.get('vol_percentile','n/a')}",
                               f"Return skew {f['skew']:+.2f}; largest 1s move {f['jump']*5:.1f}σ"]
        elif g == "DERIVS":
            d = live.get("derivs") or {}
            lq = d.get("liq_15m") or {}
            rep["fields"] = {"positioning": ("long build-up" if f["oi_px"] > 0.2 and f["oi15"] > 0 else "short build-up" if f["oi_px"] < -0.2 and f["oi15"] > 0 else "deleveraging" if f["oi15"] < -0.2 else "neutral") if f["oi_avail"] else "OI unavailable",
                             "liquidation_risk": lq or "unavailable", "crowd_up": info.get("crowd_up"), "long_short_ratio": d.get("long_short_ratio"),
                             "deribit_basis_bps": (d.get("deribit") or {}).get("basis_bps")}
            rep["evidence"] = [f"Polymarket crowd P(UP) {_fmt(info.get('crowd_up'),3)}" + (f" (age {f['crowd_age']*60:.0f}s)" if f["crowd_avail"] else " — unavailable"),
                               f"OKX OI 15m {f['oi15']/2:+.2f}%, 1h {f['oi60']:+.2f}%; funding {_fmt((info.get('funding') or 0)*100,4)}%" if f["oi_avail"] else "OKX open interest history unavailable",
                               f"Liquidations 15m (OKX): longs {lq.get('long_liq_btc','n/a')} BTC, shorts {lq.get('short_liq_btc','n/a')} BTC; L/S ratio {d.get('long_short_ratio','n/a')}; Deribit basis {(d.get('deribit') or {}).get('basis_bps','n/a')} bps"]
        out[g] = rep
    return out

def specialist_reports(f, info, probs, reliability, live):
    """Descriptive reports must never block or alter the locked prediction: if a value needed only for the evidence TEXT is
    missing (e.g. no in-round bars after a host pause), the agent's probability is kept and the text says what is unavailable."""
    try:
        return _specialist_reports_raw(f, info, probs, reliability, live)
    except Exception as e:
        out = {}
        for g, name in AGENT_GROUPS:
            p = probs.get(g); rel = reliability.get(g) or {}
            out[g] = {"agent": g, "name": name, "p_up": None if p is None else round(p, 4), "p_down": None if p is None else round(1 - p, 4),
                      "direction": None if p is None else _dir(p), "confidence": None if p is None else confidence_class(p),
                      "reliability": {"oos_accuracy": rel.get("accuracy"), "n": rel.get("n"), "brier": rel.get("brier")},
                      "status": "OK" if p is not None else "UNAVAILABLE", "fields": {},
                      "evidence": [f"Evidence detail unavailable ({type(e).__name__}): inputs incomplete at cutoff — " + "; ".join(info.get("missing") or ["see data quality"])]}
        return out

def validator_report(final_p, probs, weights, info, f, quality_flags, arch, version):
    dirs = {g: _dir(p) for g, p in probs.items() if p is not None}
    up = sum(1 for d in dirs.values() if d == "UP"); dn = len(dirs) - up
    fd = _dir(final_p)
    lo = [math.log(max(1e-4, p) / max(1e-4, 1 - p)) for p in probs.values() if p is not None]
    disagreement = round(float(__import__("statistics").pstdev([1 if d == "UP" else 0 for d in dirs.values()])) * 2, 3) if dirs else None
    checks = []
    if up and dn: checks.append(f"Conflict: {up} specialists UP vs {dn} DOWN — resolved by learned reliability weights, not majority")
    rw = info.get("p_random_walk")
    if rw is not None and (rw >= 0.5) != (final_p >= 0.5): checks.append(f"Ensemble disagrees with the random-walk baseline P(UP)={rw}")
    for g, w in (weights or {}).items():
        p = probs.get(g)
        if p is not None and abs(p - 0.5) > 0.2 and abs(w) < 0.1: checks.append(f"{g} is strongly directional but has ~zero out-of-sample weight ({w:+.2f}) — treated as noise")
    checks += quality_flags
    return {"agent": "VALIDATOR", "name": "AGENT 6 · QUANT FINAL VALIDATION", "p_up": round(final_p, 4), "p_down": round(1 - final_p, 4), "direction": fd,
            "confidence": confidence_class(final_p), "status": "OK", "weights": weights, "agreement": f"{max(up, dn)}/{len(dirs)}" if dirs else None,
            "votes": {"UP": up, "DOWN": dn}, "disagreement": disagreement, "checks": checks or ["No conflicts or stale inputs detected"],
            "evidence": [f"Champion model {version} ({arch}); final P(UP) {final_p:.3f}", f"Regime: {info.get('regime')}; data quality {info.get('data_quality')}",
                         f"Learned agent influence (log-odds per 1 SD of agent signal): " + ", ".join(f"{k} {v:+.2f}" for k, v in (weights or {}).items())]}
