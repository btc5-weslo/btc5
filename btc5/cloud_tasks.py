"""In-process keep-alive, DB backup and phone-feed push for cloud/Render.

Enabled only by environment variables so the box, the PC and the test suite
are unchanged:

  RENDER_EXTERNAL_URL or BTC5_KEEPALIVE_URL  -> ping /api/health every ~10 min
  BTC5_DB_BACKUP=1 and HF_TOKEN              -> periodic + shutdown backup
  BTC5_INPROCESS_PUSH=1 and HF_TOKEN         -> push latest.json (source from BTC5_FEED_SOURCE)

Failures are logged; they never crash the server.
"""
import asyncio, os, time, logging

log = logging.getLogger("btc5.cloud_tasks")
_stop = None
_started = False
_shutdown_backup_done = False
# Last keep-alive ping, exposed on GET /api/health so a public check can see it.
KEEPALIVE = {"enabled": False, "url": None, "every_s": None, "last_ts": None, "last_status": None, "last_error": None, "pings": 0}


def _say(msg):
    # Print as well as log: Render/Docker capture stdout, and the app's logging default is WARNING.
    print(time.strftime("%Y-%m-%d %H:%M:%S"), "cloud_tasks", msg, flush=True)
    log.info(msg)


def enabled_keepalive():
    return bool(os.environ.get("RENDER_EXTERNAL_URL") or os.environ.get("BTC5_KEEPALIVE_URL"))


def enabled_backup():
    return os.environ.get("BTC5_DB_BACKUP") == "1" and bool(os.environ.get("HF_TOKEN"))


def enabled_push():
    return os.environ.get("BTC5_INPROCESS_PUSH") == "1" and bool(os.environ.get("HF_TOKEN"))


def keepalive_url():
    base = (os.environ.get("BTC5_KEEPALIVE_URL") or os.environ.get("RENDER_EXTERNAL_URL") or "").rstrip("/")
    return f"{base}/api/health" if base else ""


def snapshot():
    out = dict(KEEPALIVE)
    out["enabled"] = enabled_keepalive()
    out["url"] = out["url"] or (keepalive_url() or None)
    out["every_s"] = out["every_s"] or (float(os.environ.get("BTC5_KEEPALIVE_EVERY", "600")) if out["enabled"] else None)
    if out["last_ts"] is not None:
        out["age_s"] = round(time.time() - out["last_ts"], 1)
    return out


async def keepalive_loop(client, stop):
    url = keepalive_url()
    interval = float(os.environ.get("BTC5_KEEPALIVE_EVERY", "600"))
    KEEPALIVE.update(enabled=True, url=url, every_s=interval)
    _say(f"keep-alive start: ping {url} every {interval:.0f}s")
    # Short first delay so a fresh deploy proves the ping works, then settle into the 10-minute cadence.
    first = float(os.environ.get("BTC5_KEEPALIVE_FIRST", "30"))
    try:
        await asyncio.wait_for(stop.wait(), timeout=max(1.0, first))
        return
    except asyncio.TimeoutError:
        pass
    while not stop.is_set():
        try:
            r = await client.get(url, timeout=20.0, follow_redirects=True)
            KEEPALIVE.update(last_ts=time.time(), last_status=r.status_code, last_error=None, pings=KEEPALIVE["pings"] + 1)
            _say(f"keep-alive {url} -> {r.status_code}")
        except Exception as e:
            KEEPALIVE.update(last_ts=time.time(), last_status=None, last_error=f"{type(e).__name__}: {e}", pings=KEEPALIVE["pings"] + 1)
            _say(f"keep-alive failed: {type(e).__name__}: {e}")
        try:
            await asyncio.wait_for(stop.wait(), timeout=max(30.0, interval))
            return
        except asyncio.TimeoutError:
            pass


async def backup_loop(stop):
    import backup
    first = float(os.environ.get("BTC5_BACKUP_FIRST", "120"))
    every = float(os.environ.get("BTC5_BACKUP_EVERY", "600"))
    _say(f"backup loop start: first in {first:.0f}s, then every {every:.0f}s")
    try:
        await asyncio.wait_for(stop.wait(), timeout=max(1.0, first))
        return
    except asyncio.TimeoutError:
        pass
    while not stop.is_set():
        try:
            await asyncio.to_thread(backup.backup_once)
        except Exception as e:
            _say(f"backup error: {e!r}")
        try:
            await asyncio.wait_for(stop.wait(), timeout=max(30.0, every))
            return
        except asyncio.TimeoutError:
            pass


async def pusher_loop(stop):
    import pusher
    _say(f"pusher start: source={pusher.SOURCE} role={pusher.ROLE} bucket={pusher.BUCKET}")
    await pusher.async_loop(stop)


def start(bg, client):
    """Schedule enabled tasks. `bg` is the app's create_task helper. Safe to call once per process."""
    global _stop, _started, _shutdown_backup_done
    if _started:
        return
    _stop = asyncio.Event()
    _shutdown_backup_done = False
    n = 0
    if enabled_keepalive():
        bg(keepalive_loop(client, _stop)); n += 1
    if enabled_backup():
        bg(backup_loop(_stop)); n += 1
    if enabled_push():
        bg(pusher_loop(_stop)); n += 1
    _started = True
    if n:
        _say(f"started {n} in-process cloud task(s)")
    else:
        _say("no cloud tasks enabled (no RENDER_EXTERNAL_URL / HF_TOKEN as required)")


async def shutdown():
    """Stop background tasks and attempt a final DB backup. Never raises."""
    global _started, _shutdown_backup_done
    try:
        if _stop is not None:
            _stop.set()
        if enabled_backup() and not _shutdown_backup_done:
            import backup
            _say("shutdown: flushing incremental + full DB backup")
            await asyncio.to_thread(backup.flush_shutdown)
            _shutdown_backup_done = True
    except Exception as e:
        _say(f"shutdown backup failed: {e!r}")
    finally:
        _started = False
