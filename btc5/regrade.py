"""Official-result audit (regrade) for locked predictions.

Every locked round from the last BTC5_REGRADE_DAYS (default 7) is checked against Polymarket's OWN resolution
(Gamma market: closed + umaResolutionStatus == 'resolved' + a 1/0 outcomePrices payout, mapped by outcome NAME).
  * a recorded official_outcome/result that differs from Polymarket is corrected (old values kept in resolution_json.regrade);
  * demo/live trade settlement for that round is recomputed from the corrected outcome, so P&L and win/loss stats follow;
  * rounds stuck PENDING / UNVERIFIED are graded once Polymarket has resolved them; unresolved ones stay PENDING and are
    retried on the next pass. Nothing is ever guessed: no confirmed resolution -> no change.
Corrections are written to the HF incremental log immediately (and ride every later full snapshot).

Light by design for a 512 MB / 0.1 CPU host: one sequential Gamma request at a time with a fixed gap, a startup pass over
the whole window, then every BTC5_REGRADE_EVERY s (600) only rounds not yet confirmed (Polymarket resolutions are final),
plus a full re-check every BTC5_REGRADE_FULL_EVERY s (6 h)."""
import asyncio, json, os, time, logging
from .db import SCHEMA
from .polymarket import parse_resolution

log = logging.getLogger("btc5.regrade")
KV_VERIFIED = "regrade_verified"      # {round_id: ts confirmed to match Polymarket}
KV_STATUS = "regrade_status"
STATUS = {"enabled": False, "running": False, "last_pass": None, "passes": 0, "corrections_total": 0, "recent_corrections": []}

def _cfg():
    f = lambda k, d: float(os.environ.get(k, d))
    return {"days": f("BTC5_REGRADE_DAYS", 7), "every": f("BTC5_REGRADE_EVERY", 600), "full_every": f("BTC5_REGRADE_FULL_EVERY", 21600),
            "gap": f("BTC5_REGRADE_GAP_S", 0.35), "first": f("BTC5_REGRADE_FIRST", 45), "settle_delay": f("BTC5_REGRADE_MIN_AGE_S", 120)}

def enabled():
    v = os.environ.get("BTC5_REGRADE")
    if v is not None: return v == "1"
    return bool(os.environ.get("RENDER_EXTERNAL_URL"))   # on by default in the Render cloud app only

def regrade_meta(rec):
    try:
        rj = json.loads((rec or {}).get("resolution_json") or "{}")
        return rj.get("regrade") if isinstance(rj, dict) else None
    except Exception:
        return None

def regrade_ts(rec):
    m = regrade_meta(rec); return float(m.get("ts") or 0) if m else 0.0

def _with_triggers_off(db, names, fn):
    """Run fn() with the named protective triggers temporarily dropped (only for correcting a record to Polymarket's
    official value); the full trigger set is always recreated afterwards."""
    with db.lock:
        for n in names: db.con.execute(f"DROP TRIGGER IF EXISTS {n}")
        try: return fn()
        finally: db.con.executescript(SCHEMA)

def resettle_trades(db, rid, outcome, now=None):
    """Recompute settled trades (WON/LOST) of this round from the official outcome. OPEN trades are left to the trader's
    normal sweep (it reads rounds.official_outcome). Returns the list of changed trades."""
    changed = []
    for t in db.q("SELECT * FROM trades WHERE round_id=? AND status IN ('WON','LOST')", (rid,)):
        if t["outcome"] == outcome: continue
        won = t["side"] == outcome; payout = round((t["shares"] or 0) * 1.0, 4) if won else 0.0
        pnl = round(payout - (t["cost"] or 0) - (t["fee"] or 0), 4)
        def upd(): db.con.execute("UPDATE trades SET status=?, outcome=?, payout=?, pnl=? WHERE round_id=?", ("WON" if won else "LOST", outcome, payout, pnl, rid))
        _with_triggers_off(db, ["trades_settle_once"], upd)
        changed.append({"round_id": rid, "mode": t["mode"], "from": t["status"], "to": "WON" if won else "LOST", "pnl_from": t["pnl"], "pnl_to": pnl})
    return changed

def correct_round(db, rid, outcome, raw=None, meta=None, now=None):
    """Make the stored grade of a LOCKED round agree with Polymarket's official outcome. Returns a change dict or None.
    `meta` (optional) re-applies a regrade record that was already made elsewhere (incremental-backup replay)."""
    if outcome not in ("UP", "DOWN"): return None
    rec = db.get_round(rid)
    if not rec or rec["status"] != "LOCKED" or rec.get("prediction") not in ("UP", "DOWN"): return None
    result = "CORRECT" if rec["prediction"] == outcome else "INCORRECT"
    if rec.get("official_outcome") == outcome and rec.get("result") == result:
        resettle_trades(db, rid, outcome)    # keep trades consistent even if only they drifted
        return None
    now = now or time.time()
    try: rj = json.loads(rec.get("resolution_json") or "{}")
    except Exception: rj = {}
    if not isinstance(rj, dict): rj = {"previous": rj}
    if raw: rj.update({k: v for k, v in raw.items() if k != "regrade"})
    rj["regrade"] = meta or {"ts": now, "from_outcome": rec.get("official_outcome"), "from_result": rec.get("result"),
                             "to_outcome": outcome, "to_result": result, "source": "Polymarket Gamma (closed, UMA resolved, 1/0 payout)"}
    fa = rec.get("failure_analysis") if result == "INCORRECT" else None
    sql = "UPDATE rounds SET official_outcome=?, result=?, resolved_ts=COALESCE(resolved_ts, ?), resolution_json=?, failure_analysis=? WHERE round_id=?"
    args = (outcome, result, now, json.dumps(rj), fa, rid)
    if rec.get("official_outcome") is None: db.x(sql, args)
    else: _with_triggers_off(db, ["rounds_resolve_once"], lambda: db.con.execute(sql, args))
    db.x("UPDATE agent_results SET correct=CASE WHEN direction=? THEN 1 ELSE 0 END WHERE round_id=?", (outcome, rid))
    db.x("UPDATE live_features SET outcome=? WHERE round_id=?", (outcome, rid))
    trades = resettle_trades(db, rid, outcome, now)
    return {"round_id": rid, "start_ts": rec["start_ts"], "prediction": rec["prediction"], "from_outcome": rec.get("official_outcome"),
            "from_result": rec.get("result"), "to_outcome": outcome, "to_result": result, "trades": trades, "ts": now}

def candidates(db, now, days, min_age, verified, full):
    rows = db.q("SELECT round_id, start_ts, end_ts, official_outcome, result FROM rounds WHERE status='LOCKED' AND end_ts >= ? AND end_ts <= ? ORDER BY start_ts DESC",
                (now - days * 86400, now - min_age))
    return [r for r in rows if full or r["round_id"] not in verified or r["official_outcome"] is None]

async def run_pass(db, poly, full=False, now=None, sleep=asyncio.sleep, skip=(), on_change=None, cfg=None):
    """One audit pass. Returns a summary dict. Network errors leave the round untouched (retried next pass)."""
    cfg = cfg or _cfg(); now = now or time.time()
    verified = db.kv_get(KV_VERIFIED, {}) or {}
    cut = now - cfg["days"] * 86400
    verified = {k: v for k, v in verified.items() if v >= cut - 86400}
    todo = [r for r in candidates(db, now, cfg["days"], cfg["settle_delay"], verified, full) if r["round_id"] not in skip]
    s = {"checked": 0, "confirmed": 0, "corrected": 0, "graded_pending": 0, "unresolved": 0, "errors": 0, "changes": [], "full": full, "ts": now}
    for i, r in enumerate(todo):
        if i: await sleep(cfg["gap"])
        try:
            m = await poly.market_raw(r["start_ts"])
        except Exception as e:
            s["errors"] += 1; continue
        s["checked"] += 1
        outcome = parse_resolution(m, strict=True) if m else None
        if outcome is None:
            s["unresolved"] += 1; continue                 # not official yet: stays PENDING, retried next pass
        raw = {k: m.get(k) for k in ("closed", "outcomes", "outcomePrices", "umaResolutionStatus", "closedTime", "resolutionSource")}
        ch = correct_round(db, r["round_id"], outcome, raw, now=now)
        verified[r["round_id"]] = now
        if ch:
            if ch["from_outcome"] is None: s["graded_pending"] += 1
            else: s["corrected"] += 1
            s["changes"].append(ch)
            if on_change:
                try: await on_change(ch)
                except Exception as e: log.warning("on_change failed: %r", e)
        else: s["confirmed"] += 1
    db.kv_set(KV_VERIFIED, verified)
    return s

async def loop(eng, stop=None):
    """Startup pass (full window), then every `every` s; full re-check every `full_every` s."""
    cfg = _cfg(); STATUS["enabled"] = True
    async def wait(t):
        if stop is None: await asyncio.sleep(t); return False
        try: await asyncio.wait_for(stop.wait(), timeout=t); return True
        except asyncio.TimeoutError: return False
    async def on_change(ch):
        rid = ch["round_id"]
        what = "graded (was PENDING/UNVERIFIED)" if ch["from_outcome"] is None else f"CORRECTED from {ch['from_outcome']}/{ch['from_result']}"
        eng.log("RESOLUTION", "warning" if ch["from_outcome"] else "info",
                f"Regrade vs Polymarket: official {ch['to_outcome']} → prediction {ch['to_result']} — {what}" + (f"; {len(ch['trades'])} trade(s) re-settled" if ch["trades"] else ""), rid)
        st = eng.state.get("prediction")
        if st and st.get("round_id") == rid: eng.state["prediction"] = eng.public(eng.db.get_round(rid))
        try:
            import backup
            await asyncio.to_thread(backup.record_round, rid, None, False)   # local incremental now; one upload per pass
        except Exception as e:
            eng.log("BACKUP", "warning", f"regrade incremental write failed: {e!r}", rid)
    if await wait(cfg["first"]): return
    last_full = 0.0
    while eng.running if hasattr(eng, "running") else True:
        full = time.time() - last_full >= cfg["full_every"]
        STATUS["running"] = True
        try:
            s = await run_pass(eng.db, eng.poly, full=full, skip=set(getattr(eng, "resolving", ())), on_change=on_change, cfg=cfg)
            if full: last_full = time.time()
            STATUS.update(last_pass={k: v for k, v in s.items() if k != "changes"}, passes=STATUS["passes"] + 1)
            STATUS["corrections_total"] += s["corrected"] + s["graded_pending"]
            STATUS["recent_corrections"] = (s["changes"] + STATUS["recent_corrections"])[:20]
            eng.db.kv_set(KV_STATUS, {"last_pass": STATUS["last_pass"], "recent_corrections": STATUS["recent_corrections"]})
            if s["changes"]:
                try:
                    import backup
                    await asyncio.to_thread(backup.flush_incrementals)
                except Exception as e:
                    eng.log("BACKUP", "warning", f"regrade incremental upload failed: {e!r}")
            eng.log("RESOLUTION", "info", f"Regrade pass ({'full' if full else 'new'}): checked {s['checked']}, confirmed {s['confirmed']}, "
                    f"corrected {s['corrected']}, graded {s['graded_pending']}, still unresolved {s['unresolved']}, errors {s['errors']}")
        except Exception as e:
            eng.log("RESOLUTION", "error", f"Regrade pass failed: {e!r}")
        finally:
            STATUS["running"] = False
        if await wait(cfg["every"]): return

def snapshot():
    return {k: (v[:5] if k == "recent_corrections" else v) for k, v in STATUS.items()} | {"enabled": enabled()}
