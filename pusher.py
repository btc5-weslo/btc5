#!/usr/bin/env python3
"""Push a read-only BTC-5 snapshot to the public Hugging Face bucket (latest.json) every ~10 s; the static HF Space
(phone view) polls it. Contains only public market data and prediction records. Cross-platform (Linux box / Windows PC).

Feed ownership (so several machines never fight over the same latest.json) - priority by source:
  cloud / hf-space (always-on HF Docker Space) 3  >  pc (Windows PC) 2  >  box 1
  Before every push a writer reads the public latest.json and STANDS DOWN while it was written by a HIGHER-priority
  source less than BTC5_STANDDOWN_S (90 s) ago. The highest source never needs to read. If a higher source goes quiet,
  the next one takes over automatically within <= 90 s and hands back as soon as the higher source pushes again.
  (BTC5_FEED_ROLE=primary forces always-push; kept for compatibility.)
Every snapshot carries "source" so the phone view (and the fallback) know who wrote it.
No HF login -> the pusher idles and re-checks every 60 s (it starts pushing by itself after `hf auth login`).
Watchdog: a timer thread hard-exits the process if one push cycle hangs > 60 s; the supervisor restarts it."""
import json, os, sys, time, threading, urllib.request
os.environ.setdefault("HF_HUB_DISABLE_PROGRESS_BARS", "1")
from huggingface_hub import HfApi
BUCKET = os.environ.get("BTC5_BUCKET", "weslo00700/btc5-feed")
LOCAL = os.environ.get("BTC5_LOCAL", "http://127.0.0.1:8700")
INTERVAL = float(os.environ.get("BTC5_PUSH_INTERVAL", "10"))
ROLE = os.environ.get("BTC5_FEED_ROLE", "primary").lower()
SOURCE = os.environ.get("BTC5_FEED_SOURCE", "box" if sys.platform.startswith("linux") else "pc")
STANDDOWN_S = float(os.environ.get("BTC5_STANDDOWN_S", "90"))
PRIORITY = {"cloud": 3, "hf-space": 3, "pc": 2, "box": 1}
def prio(src): return PRIORITY.get(src or "", 0)
PUBLIC_URL = f"https://huggingface.co/buckets/{BUCKET}/resolve/latest.json"
API = HfApi()

def log(*a): print(time.strftime("%F %T"), *a, flush=True)

def get(path):
    with urllib.request.urlopen(LOCAL + path, timeout=10) as r: return json.load(r)

def snapshot():
    st = get("/api/state"); st["logs"] = st.get("logs", [])[-25:]
    bt = get("/api/backtest").get("latest")
    if bt: bt.pop("folds", None)
    m = get("/api/models")
    return {"state": st, "candles": get("/api/candles?tf=1m&n=90"), "history": get("/api/history?limit=60"),
            "performance": get("/api/performance"), "backtest": bt,
            "models": {"champion": m.get("champion"), "challenger_status": m.get("challenger_status")},
            "trades": get("/api/trading/trades?limit=80"), "equity": get("/api/trading/equity?mode=demo")}

def current_owner():
    """(source, age_s) of the public feed right now, or (None, None) if unreadable."""
    req = urllib.request.Request(f"{PUBLIC_URL}?t={time.time_ns()}", headers={"User-Agent": "btc5-pusher", "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=10) as r: d = json.load(r)
    return d.get("source"), time.time() - float(d.get("pushed_at") or 0)

def should_push():
    if prio(SOURCE) >= max(PRIORITY.values()): return True, "primary"
    try: src, age = current_owner()
    except Exception as e: return True, f"feed unreadable ({type(e).__name__}) - pushing"
    if src and src != SOURCE and prio(src) > prio(SOURCE) and age < STANDDOWN_S:
        return False, f"standing down: feed owned by higher-priority '{src}' ({age:.0f}s old)"
    return True, f"active (feed owner '{src}', {age:.0f}s old)"

def logged_in():
    try: API.whoami(); return True
    except Exception: return False

def push(data):
    data["pushed_at"] = time.time(); data["source"] = SOURCE
    API.batch_bucket_files(BUCKET, add=[(json.dumps(data, separators=(",", ":"), default=str).encode(), "latest.json")])

def cycle():
    """One should-push + optional push. Returns the reason string. Raises on hard failures."""
    ok, why = should_push()
    if ok: push(snapshot())
    return why

def main():
    log(f"pusher start: role={ROLE} source={SOURCE} bucket={BUCKET}")
    while not logged_in():
        log("no Hugging Face login on this machine - phone feed NOT pushed from here; run `hf auth login` (re-checking every 60 s)")
        time.sleep(60)
    last_state = None
    while True:
        t0 = time.time()
        wd = threading.Timer(60, lambda: (log("watchdog: push cycle hung > 60 s, exiting for restart"), os._exit(3))); wd.daemon = True; wd.start()
        try:
            why = cycle()
            state = why.split(":")[0].split(" (")[0]
            if state != last_state: log(why); last_state = state
        except Exception as e: log("push error:", repr(e)[:300])
        finally: wd.cancel()
        time.sleep(max(1.0, INTERVAL - (time.time() - t0)))

async def async_loop(stop):
    """Same push loop for an in-process Render/cloud task. Never calls os._exit (that would kill the API)."""
    import asyncio
    log(f"pusher start (in-process): role={ROLE} source={SOURCE} bucket={BUCKET}")
    last_state = None
    while not stop.is_set():
        if not logged_in():
            log("no Hugging Face login on this machine - phone feed NOT pushed from here; set HF_TOKEN (re-checking every 60 s)")
            try:
                await asyncio.wait_for(stop.wait(), timeout=60)
                return
            except asyncio.TimeoutError:
                continue
        t0 = time.time()
        try:
            why = await asyncio.wait_for(asyncio.to_thread(cycle), timeout=60)
            state = why.split(":")[0].split(" (")[0]
            if state != last_state: log(why); last_state = state
        except asyncio.TimeoutError:
            log("watchdog: push cycle hung > 60 s, skipping (in-process, not exiting)")
        except Exception as e:
            log("push error:", repr(e)[:300])
        delay = max(1.0, INTERVAL - (time.time() - t0))
        try:
            await asyncio.wait_for(stop.wait(), timeout=delay)
            return
        except asyncio.TimeoutError:
            pass

if __name__ == "__main__":
    main()
