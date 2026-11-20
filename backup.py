#!/usr/bin/env python3
"""Cloud DB persistence for the HF Docker Space (whose disk is ephemeral).
  restore(): on startup, if data/btc5.db is missing, download db/btc5.db.gz from the bucket and verify it.
  loop():    every BTC5_BACKUP_EVERY s (default 1200 = 20 min) take a consistent SQLite snapshot, gzip it and upload
             db/btc5.db.gz + db/btc5.meta.json. Safety: never overwrites a backup that holds MORE rounds than the local DB
             (e.g. after a failed restore), so history can't be lost by a fresh container."""
import gzip, json, os, shutil, sqlite3, sys, time, urllib.request
os.environ.setdefault("HF_HUB_DISABLE_PROGRESS_BARS", "1")
HERE = os.path.dirname(os.path.abspath(__file__))
DB = os.environ.get("BTC5_DB", os.path.join(HERE, "data", "btc5.db"))
BUCKET = os.environ.get("BTC5_BUCKET", "weslo00700/btc5-feed")
EVERY = float(os.environ.get("BTC5_BACKUP_EVERY", "1200"))
URL = f"https://huggingface.co/buckets/{BUCKET}/resolve/"

def log(*a): print(time.strftime("%F %T"), *a, flush=True)

def counts(path):
    c = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        ok = c.execute("PRAGMA quick_check").fetchone()[0] == "ok"
        return ok, c.execute("SELECT COUNT(*) FROM rounds").fetchone()[0], c.execute("SELECT COUNT(*) FROM hist_rounds").fetchone()[0]
    finally: c.close()

def remote_meta():
    try:
        with urllib.request.urlopen(urllib.request.Request(URL + f"db/btc5.meta.json?t={time.time_ns()}", headers={"User-Agent": "btc5-backup"}), timeout=20) as r:
            return json.load(r)
    except Exception: return None

def restore():
    if os.path.exists(DB): log("restore: local DB present, nothing to do"); return True
    os.makedirs(os.path.dirname(DB), exist_ok=True); tmp = DB + ".restore"
    for attempt in range(5):
        try:
            with urllib.request.urlopen(urllib.request.Request(URL + f"db/btc5.db.gz?t={time.time_ns()}", headers={"User-Agent": "btc5-backup"}), timeout=120) as r, \
                 gzip.GzipFile(fileobj=r) as g, open(tmp, "wb") as f:
                shutil.copyfileobj(g, f)
            ok, n, h = counts(tmp)
            if not ok: raise RuntimeError("integrity check failed")
            os.replace(tmp, DB); log(f"restore: OK ({n} rounds, {h} historical rounds)"); return True
        except Exception as e:
            log(f"restore attempt {attempt + 1} failed: {e!r}"); time.sleep(5 * (attempt + 1))
    log("restore: FAILED - starting with an empty DB (backups are protected from being overwritten by it)"); return False

def backup_once(api=None):
    from huggingface_hub import HfApi
    api = api or HfApi()
    snap = DB + ".snap"
    s = sqlite3.connect(f"file:{DB}?mode=ro", uri=True); d = sqlite3.connect(snap)
    try: s.backup(d)
    finally: s.close(); d.close()
    ok, n, h = counts(snap)
    rm = remote_meta()
    if not ok: os.remove(snap); log("backup skipped: snapshot failed integrity check"); return False
    if rm and (rm.get("rounds", 0) > n or rm.get("hist_rounds", 0) > h):
        os.remove(snap); log(f"backup skipped: remote backup has more data ({rm.get('rounds')} rounds / {rm.get('hist_rounds')} hist) than local ({n} / {h})"); return False
    with open(snap, "rb") as f: gz = gzip.compress(f.read(), 6)
    os.remove(snap)
    meta = {"ts": time.time(), "rounds": n, "hist_rounds": h, "bytes_gz": len(gz), "source": os.environ.get("BTC5_FEED_SOURCE", "cloud")}
    api.batch_bucket_files(BUCKET, add=[(gz, "db/btc5.db.gz"), (json.dumps(meta).encode(), "db/btc5.meta.json")])
    log(f"backup OK: {n} rounds, {h} hist, {len(gz) / 1e6:.1f} MB gz"); return True

def loop():
    time.sleep(float(os.environ.get("BTC5_BACKUP_FIRST", "300")))
    while True:
        try: backup_once()
        except Exception as e: log(f"backup error: {e!r}")
        time.sleep(EVERY)

if __name__ == "__main__":
    if "restore" in sys.argv: restore()
    elif "once" in sys.argv: backup_once()
    else: loop()
