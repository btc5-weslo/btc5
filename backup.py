#!/usr/bin/env python3
"""Cloud DB persistence for an ephemeral disk (Render free / HF Space).

  restore(): if data/btc5.db is missing, download db/btc5.db.gz; then ALWAYS replay
             db/incr.json so locks and grades written since the last full snapshot survive.
  record_round(rid): after each lock or grade, merge that round into a compact incremental
             file (local + bucket). Small and fast — unlike the 30+ MB full snapshot.
  backup_once(): full gzip snapshot every BTC5_BACKUP_EVERY s (default 600 = 10 min).
  flush_shutdown(): on SIGTERM, upload incrementals first (fast), then try a full snapshot.

Safety: a full snapshot never overwrites a remote backup that holds MORE rounds than the
local DB. Incremental replay never invents a prediction or re-locks a past round: it only
inserts records that already carry their original decision_ts / official_outcome.
"""
import gzip, json, os, shutil, sqlite3, sys, threading, time, urllib.request

os.environ.setdefault("HF_HUB_DISABLE_PROGRESS_BARS", "1")
HERE = os.path.dirname(os.path.abspath(__file__))
DB = os.environ.get("BTC5_DB", os.path.join(HERE, "data", "btc5.db"))
BUCKET = os.environ.get("BTC5_BUCKET", "weslo00700/btc5-feed")
EVERY = float(os.environ.get("BTC5_BACKUP_EVERY", "1200"))
URL = f"https://huggingface.co/buckets/{BUCKET}/resolve/"
INCR_REMOTE = "db/incr.json"
_LOCK = threading.RLock()
_TRAINING = False
STATUS = {"last_incr_ts": None, "last_incr_round": None, "last_incr_ok": None,
          "last_full_ts": None, "last_full_ok": None, "last_replay": None}

# Recreated after a surgical replace of a MISSED placeholder with a real LOCKED record.
_TRIGGERS = """
CREATE TRIGGER IF NOT EXISTS rounds_immutable BEFORE UPDATE OF round_id,start_ts,status,decision_ts,deviation_ms,cutoff_ts,prediction,p_up,p_down,confidence,agents_json,model_version ON rounds
BEGIN SELECT RAISE(ABORT,'locked prediction records are immutable'); END;
CREATE TRIGGER IF NOT EXISTS rounds_publish_once BEFORE UPDATE OF published_ts ON rounds WHEN OLD.published_ts IS NOT NULL
BEGIN SELECT RAISE(ABORT,'publication timestamp is permanent'); END;
CREATE TRIGGER IF NOT EXISTS rounds_resolve_once BEFORE UPDATE OF official_outcome ON rounds WHEN OLD.official_outcome IS NOT NULL
BEGIN SELECT RAISE(ABORT,'official outcome already recorded'); END;
CREATE TRIGGER IF NOT EXISTS rounds_nodelete BEFORE DELETE ON rounds BEGIN SELECT RAISE(ABORT,'prediction history cannot be deleted'); END;
"""

ROUND_COLS = (
    "round_id", "start_ts", "end_ts", "decision_target_ts", "status", "market_id", "condition_id", "question",
    "up_token", "down_token", "resolution_source", "rules", "decision_ts", "published_ts", "deviation_ms",
    "publish_latency_ms", "cutoff_ts", "prediction", "p_up", "p_down", "edge", "confidence", "disagreement",
    "data_quality", "degraded", "regime", "model_version", "tie_break", "open_price", "open_source",
    "price_at_prediction", "chainlink_at_prediction", "crowd_up", "agents_json", "evidence_json", "missed_reason",
    "official_outcome", "official_open", "official_close", "resolved_ts", "resolution_json", "result", "failure_analysis",
)
TRADE_COLS = (
    "round_id", "start_ts", "end_ts", "mode", "epoch", "side", "token", "confidence", "p_side", "stake", "best_ask",
    "entry_price", "price_source", "shares", "cost", "fee", "status", "outcome", "payout", "pnl", "order_id",
    "order_json", "error", "created_ts", "placed_ts", "settled_ts",
)


def log(*a):
    print(time.strftime("%F %T"), *a, flush=True)


def _db_path():
    return os.environ.get("BTC5_DB", DB)


def _incr_local():
    return os.path.join(os.path.dirname(os.path.abspath(_db_path())) or ".", "incr.json")


def set_training(active):
    """Full snapshots are skipped while a training job is running (avoids a RAM spike on a 512 MB host)."""
    global _TRAINING
    _TRAINING = bool(active)


def snapshot_status():
    return dict(STATUS)


def counts(path):
    c = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        ok = c.execute("PRAGMA quick_check").fetchone()[0] == "ok"
        return ok, c.execute("SELECT COUNT(*) FROM rounds").fetchone()[0], c.execute("SELECT COUNT(*) FROM hist_rounds").fetchone()[0]
    finally:
        c.close()


def remote_meta():
    try:
        with urllib.request.urlopen(urllib.request.Request(URL + f"db/btc5.meta.json?t={time.time_ns()}", headers={"User-Agent": "btc5-backup"}), timeout=20) as r:
            return json.load(r)
    except Exception:
        return None


def _fetch_json(rel, timeout=30):
    try:
        with urllib.request.urlopen(urllib.request.Request(URL + f"{rel}?t={time.time_ns()}", headers={"User-Agent": "btc5-backup"}), timeout=timeout) as r:
            return json.load(r)
    except Exception:
        return None


def restore():
    """Restore the full snapshot if the local DB is missing, then replay incrementals (always)."""
    ok = restore_snapshot()
    replay = replay_incrementals()
    STATUS["last_replay"] = replay
    return ok


def restore_snapshot():
    path = _db_path()
    if os.path.exists(path):
        log("restore: local DB present, nothing to do")
        return True
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".restore"
    for attempt in range(5):
        try:
            with urllib.request.urlopen(urllib.request.Request(URL + f"db/btc5.db.gz?t={time.time_ns()}", headers={"User-Agent": "btc5-backup"}), timeout=120) as r, \
                 gzip.GzipFile(fileobj=r) as g, open(tmp, "wb") as f:
                shutil.copyfileobj(g, f)
            ok, n, h = counts(tmp)
            if not ok:
                raise RuntimeError("integrity check failed")
            os.replace(tmp, path)
            log(f"restore: OK ({n} rounds, {h} historical rounds)")
            return True
        except Exception as e:
            log(f"restore attempt {attempt + 1} failed: {e!r}")
            time.sleep(5 * (attempt + 1))
    log("restore: FAILED - starting with an empty DB (backups are protected from being overwritten by it)")
    return False


def _status_rank(s):
    return {"MISSED": 0, "MONITORING_ONLY": 1, "MARKET_UNVERIFIED": 2, "LOCKED": 3}.get(s or "", 0)


def _richer(a, b):
    """True if bundle a is a more complete record of the same round than b (never 'richer' by inventing fields)."""
    ra, rb = (a or {}).get("round") or a or {}, (b or {}).get("round") or b or {}
    if _status_rank(ra.get("status")) != _status_rank(rb.get("status")):
        return _status_rank(ra.get("status")) > _status_rank(rb.get("status"))
    if bool(ra.get("official_outcome")) != bool(rb.get("official_outcome")):
        return bool(ra.get("official_outcome"))
    if bool(ra.get("prediction")) != bool(rb.get("prediction")):
        return bool(ra.get("prediction"))
    return False


def _round_cols(row):
    return {k: row.get(k) for k in ROUND_COLS if k in row and row.get(k) is not None or k in ("round_id", "start_ts", "end_ts", "decision_target_ts", "status")}


def _read_json(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _write_json(path, data):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, separators=(",", ":"), default=str)
    os.replace(tmp, path)


def _bundle(rid):
    path = _db_path()
    if not os.path.exists(path):
        return None
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    try:
        r = con.execute("SELECT * FROM rounds WHERE round_id=?", (rid,)).fetchone()
        if not r:
            return None
        trades = [dict(t) for t in con.execute("SELECT * FROM trades WHERE round_id=?", (rid,)).fetchall()]
        for t in trades:
            t.pop("id", None)
        return {"round": dict(r), "trades": trades}
    finally:
        con.close()


def _load_local_incr():
    return _read_json(_incr_local()) or {"rounds": {}, "ts": 0, "source": os.environ.get("BTC5_FEED_SOURCE", "cloud")}


def _merge_incr(*parts):
    rounds = {}
    for part in parts:
        if not part:
            continue
        src = part.get("rounds") if isinstance(part.get("rounds"), dict) else {}
        for rid, rec in src.items():
            if rid not in rounds or _richer(rec, rounds[rid]):
                rounds[rid] = rec
    return {"ts": time.time(), "source": os.environ.get("BTC5_FEED_SOURCE", "cloud"), "rounds": rounds}


def record_round(round_id, api=None, upload=True):
    """Merge one persisted round into the incremental log and upload it. Never raises to the caller? actually can raise.
    Callers should catch. Local write always happens first so SIGTERM can flush even if the last upload is mid-flight."""
    bundle = _bundle(round_id)
    if not bundle:
        return False
    with _LOCK:
        local = _load_local_incr()
        local = _merge_incr(local, {"rounds": {round_id: bundle}})
        local["ts"] = time.time()
        _write_json(_incr_local(), local)
    STATUS["last_incr_ts"] = local["ts"]
    STATUS["last_incr_round"] = round_id
    if upload:
        ok = _upload_incr(local, api=api)
        STATUS["last_incr_ok"] = ok
        return ok
    STATUS["last_incr_ok"] = True
    return True


def _upload_incr(local, api=None):
    if os.environ.get("BTC5_DB_BACKUP") != "1" or not os.environ.get("HF_TOKEN"):
        return True
    remote = _fetch_json(INCR_REMOTE) or {"rounds": {}}
    merged = _merge_incr(remote, local)
    merged["ts"] = time.time()
    with _LOCK:
        _write_json(_incr_local(), merged)
    from huggingface_hub import HfApi
    api = api or HfApi()
    body = json.dumps(merged, separators=(",", ":"), default=str).encode()
    api.batch_bucket_files(BUCKET, add=[(body, INCR_REMOTE)])
    log(f"incremental backup OK: {len(merged.get('rounds') or {})} rounds, last={STATUS.get('last_incr_round')}")
    return True


def flush_incrementals(api=None):
    """Upload the local incremental log (merged with remote). Fast. Safe to call from a SIGTERM handler."""
    if os.environ.get("BTC5_DB_BACKUP") != "1" or not os.environ.get("HF_TOKEN"):
        return False
    local = _load_local_incr()
    if not (local.get("rounds") or {}):
        # still try remote-only no-op skip
        return False
    try:
        return _upload_incr(local, api=api)
    except Exception as e:
        log(f"incremental flush failed: {e!r}")
        return False


def _apply_trades(db, trades):
    for t in trades or []:
        rec = {k: t.get(k) for k in TRADE_COLS if k in t}
        if not rec.get("round_id") or not rec.get("mode") or not rec.get("status"):
            continue
        if db.one("SELECT round_id FROM trades WHERE round_id=?", (rec["round_id"],)):
            # do not rewrite an existing trade; settlement is trigger-protected
            continue
        cols = ",".join(rec)
        qs = ",".join("?" * len(rec))
        try:
            db.x(f"INSERT INTO trades ({cols}) VALUES ({qs})", tuple(rec.values()))
        except Exception as e:
            log(f"replay trade {rec.get('round_id')} skipped: {e!r}")


def _replace_missed(db, rec):
    """Replace a MISSED placeholder with the original LOCKED (or MARKET_UNVERIFIED) record.
    The immutability triggers would otherwise reject this; we only do it for MISSED rows, and
    we keep the incremental record's original decision_ts / prediction — nothing is invented."""
    rid = rec["round_id"]
    with db.lock:
        cur = db.con.execute("SELECT status FROM rounds WHERE round_id=?", (rid,)).fetchone()
        if not cur or cur["status"] != "MISSED":
            return False
        # SQLite auto-commits on DDL, so we cannot wrap DROP TRIGGER in an explicit transaction.
        db.con.execute("DROP TRIGGER IF EXISTS rounds_nodelete")
        db.con.execute("DROP TRIGGER IF EXISTS rounds_immutable")
        try:
            db.con.execute("DELETE FROM rounds WHERE round_id=? AND status='MISSED'", (rid,))
            if db.con.execute("SELECT changes()").fetchone()[0] != 1:
                return False
            cols = ",".join(rec)
            qs = ",".join("?" * len(rec))
            db.con.execute(f"INSERT INTO rounds ({cols}) VALUES ({qs})", tuple(rec.values()))
            return True
        finally:
            db.con.executescript(_TRIGGERS)


def apply_incremental_round(db, bundle):
    """Idempotent apply of one incremental bundle. Returns a short action name."""
    row = (bundle or {}).get("round") or bundle or {}
    rid = row.get("round_id")
    if not rid:
        return "skipped"
    rec = _round_cols(row)
    trades = (bundle or {}).get("trades") if isinstance(bundle, dict) else None
    existing = db.get_round(rid)
    if not existing:
        if not rec.get("status") or rec.get("start_ts") is None:
            return "skipped"
        if db.insert_round(rec):
            _apply_trades(db, trades)
            return "inserted"
        return "conflict"
    if existing.get("status") == "MISSED" and rec.get("status") in ("LOCKED", "MARKET_UNVERIFIED", "MONITORING_ONLY") and rec.get("prediction") and rec.get("decision_ts") is not None:
        if _replace_missed(db, rec):
            _apply_trades(db, trades)
            return "replaced_missed"
        return "kept"
    if existing.get("official_outcome") is None and rec.get("official_outcome"):
        db.x(
            "UPDATE rounds SET official_outcome=?, official_open=?, official_close=?, resolved_ts=?, resolution_json=?, result=?, failure_analysis=? WHERE round_id=? AND official_outcome IS NULL",
            (rec.get("official_outcome"), rec.get("official_open"), rec.get("official_close"), rec.get("resolved_ts"),
             rec.get("resolution_json"), rec.get("result"), rec.get("failure_analysis"), rid),
        )
        _apply_trades(db, trades)
        return "graded"
    _apply_trades(db, trades)
    return "kept"


def replay_incrementals(data=None):
    """Apply incremental records to the local DB. Missing incrementals are a no-op."""
    from btc5.db import DB
    path = _db_path()
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    remote = data if data is not None else (_fetch_json(INCR_REMOTE) or {"rounds": {}})
    local = _load_local_incr()
    merged = _merge_incr(remote, local)
    rounds = merged.get("rounds") or {}
    if not rounds:
        log("replay: no incremental rounds")
        return {"n": 0, "actions": {}}
    db = DB(path)
    actions = {}
    for rid, bundle in sorted(rounds.items(), key=lambda kv: ((kv[1].get("round") or kv[1] or {}).get("start_ts") or 0, kv[0])):
        try:
            act = apply_incremental_round(db, bundle)
        except Exception as e:
            act = f"error:{e!r}"
            log(f"replay {rid} failed: {e!r}")
        actions[act] = actions.get(act, 0) + 1
    log(f"replay: {len(rounds)} incremental rounds -> {actions}")
    return {"n": len(rounds), "actions": actions}


def backup_once(api=None, force=False):
    from huggingface_hub import HfApi
    if _TRAINING and not force:
        log("backup skipped: training job is running (memory cap)")
        return False
    api = api or HfApi()
    path = _db_path()
    snap = path + ".snap"
    gz_path = path + ".snap.gz"
    s = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    d = sqlite3.connect(snap)
    try:
        s.backup(d)
    finally:
        s.close()
        d.close()
    ok, n, h = counts(snap)
    rm = remote_meta()
    if not ok:
        os.remove(snap)
        log("backup skipped: snapshot failed integrity check")
        return False
    if rm and (rm.get("rounds", 0) > n or rm.get("hist_rounds", 0) > h):
        os.remove(snap)
        log(f"backup skipped: remote backup has more data ({rm.get('rounds')} rounds / {rm.get('hist_rounds')} hist) than local ({n} / {h})")
        return False
    # Stream gzip to disk so we never hold raw+compressed copies in RAM at once (the 40 MB DB
    # plus a training job was enough to OOM the 512 MB Render free instance).
    with open(snap, "rb") as f, gzip.open(gz_path, "wb", compresslevel=6) as g:
        shutil.copyfileobj(f, g, 1024 * 1024)
    os.remove(snap)
    with open(gz_path, "rb") as f:
        gz = f.read()
    os.remove(gz_path)
    meta = {"ts": time.time(), "rounds": n, "hist_rounds": h, "bytes_gz": len(gz), "source": os.environ.get("BTC5_FEED_SOURCE", "cloud")}
    # After a successful full snapshot the incrementals are redundant for rounds already in the
    # DB, but we keep any still-richer incremental records and re-upload the pruned file.
    pruned = _prune_incr_contained_in_db(path)
    adds = [(gz, "db/btc5.db.gz"), (json.dumps(meta).encode(), "db/btc5.meta.json")]
    if pruned is not None and os.environ.get("HF_TOKEN"):
        adds.append((json.dumps(pruned, separators=(",", ":"), default=str).encode(), INCR_REMOTE))
    api.batch_bucket_files(BUCKET, add=adds)
    STATUS["last_full_ts"] = meta["ts"]
    STATUS["last_full_ok"] = True
    log(f"backup OK: {n} rounds, {h} hist, {len(gz) / 1e6:.1f} MB gz")
    return True


def _prune_incr_contained_in_db(path):
    local = _load_local_incr()
    rounds = dict(local.get("rounds") or {})
    if not rounds or not os.path.exists(path):
        return local if rounds else {"ts": time.time(), "source": os.environ.get("BTC5_FEED_SOURCE", "cloud"), "rounds": {}}
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    try:
        keep = {}
        for rid, bundle in rounds.items():
            row = (bundle or {}).get("round") or bundle or {}
            ex = con.execute("SELECT status, prediction, decision_ts, official_outcome FROM rounds WHERE round_id=?", (rid,)).fetchone()
            if not ex:
                keep[rid] = bundle
                continue
            # Drop only if the snapshot already has a record at least as complete.
            if _richer(bundle, {"round": dict(ex)}):
                keep[rid] = bundle
        out = {"ts": time.time(), "source": os.environ.get("BTC5_FEED_SOURCE", "cloud"), "rounds": keep}
        _write_json(_incr_local(), out)
        return out
    finally:
        con.close()


def flush_shutdown(api=None):
    """SIGTERM / atexit: incrementals first (must finish inside Render's grace period), then a full snapshot."""
    log("shutdown: flushing incremental backup")
    flush_incrementals(api=api)
    if os.environ.get("BTC5_DB_BACKUP") == "1" and os.environ.get("HF_TOKEN"):
        try:
            log("shutdown: full DB snapshot")
            backup_once(api=api, force=True)
        except Exception as e:
            log(f"shutdown full backup failed: {e!r}")


def loop():
    time.sleep(float(os.environ.get("BTC5_BACKUP_FIRST", "300")))
    while True:
        try:
            backup_once()
        except Exception as e:
            log(f"backup error: {e!r}")
        time.sleep(EVERY)


if __name__ == "__main__":
    if "restore" in sys.argv:
        restore()
    elif "once" in sys.argv:
        backup_once()
    elif "incr" in sys.argv:
        rid = sys.argv[sys.argv.index("incr") + 1]
        record_round(rid)
    else:
        loop()
