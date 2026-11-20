#!/usr/bin/env python3
"""Render (and any $PORT host) entrypoint.

  * Honor Render's PORT (default 10000); expose it as BTC5_PORT for the rest of the app.
  * Restore data/btc5.db from the HF bucket when the local disk is empty.
  * Start uvicorn in this process. Keep-alive, periodic DB backup, shutdown backup and
    the phone-feed pusher run as in-process tasks inside the app (see btc5/cloud_tasks.py)
    so a 512 MB free instance is not multiplied by extra Python processes.
  * If HF_TOKEN is missing, the server still runs; backup and feed push stay idle.

No secrets are read from files. Wallet keys and HF_TOKEN come only from environment
variables / host secrets.
"""
import os, sys, atexit, signal

# Thread caps before numpy/sklearn are imported (via uvicorn -> app).
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")
os.environ.setdefault("NUMEXPR_NUM_THREADS", "1")
os.environ.setdefault("HF_HUB_DISABLE_PROGRESS_BARS", "1")
os.environ.setdefault("PYTHONUNBUFFERED", "1")

PORT = int(os.environ.get("PORT") or os.environ.get("BTC5_PORT") or "10000")
os.environ["BTC5_PORT"] = str(PORT)
os.environ.setdefault("BTC5_FEED_SOURCE", "cloud")
os.environ.setdefault("BTC5_FEED_ROLE", "primary")
os.environ.setdefault("BTC5_DB_BACKUP", "1")
os.environ.setdefault("BTC5_BACKUP_EVERY", "600")
os.environ.setdefault("BTC5_INPROCESS_PUSH", "1")
os.environ.setdefault("BTC5_LOW_MEM", "1")
os.environ.setdefault("BTC5_LOCAL", f"http://127.0.0.1:{PORT}")


def log(*a):
    import time
    print(time.strftime("%F %T"), *a, flush=True)


def _shutdown_backup():
    try:
        import backup
        backup.flush_shutdown()
    except Exception as e:
        log(f"shutdown backup failed: {e!r}")


def main():
    import backup
    log(f"render_start: port={PORT} feed_source={os.environ.get('BTC5_FEED_SOURCE')} "
        f"hf_token={'set' if os.environ.get('HF_TOKEN') else 'absent'} "
        f"low_mem={os.environ.get('BTC5_LOW_MEM')} "
        f"keepalive={os.environ.get('RENDER_EXTERNAL_URL') or os.environ.get('BTC5_KEEPALIVE_URL') or 'unset'}")
    try:
        backup.restore()  # full snapshot (if disk empty) + incremental replay, before the engine can mark MISSED
    except Exception as e:
        log(f"restore raised {e!r} — starting with whatever local DB is present")
    atexit.register(_shutdown_backup)
    prev = signal.getsignal(signal.SIGTERM)

    def _on_term(signo, frame):
        log("SIGTERM: flushing incremental backup first")
        try:
            backup.flush_incrementals()
        except Exception as e:
            log(f"SIGTERM incremental flush failed: {e!r}")
        if callable(prev):
            prev(signo, frame)

    try:
        signal.signal(signal.SIGTERM, _on_term)
    except Exception as e:
        log(f"could not install SIGTERM handler: {e!r}")
    import uvicorn
    # One process: the app lifespan starts keep-alive / backup / pusher tasks.
    uvicorn.run("btc5.app:app", host="0.0.0.0", port=PORT, access_log=False)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"render_start crashed: {e!r}")
        sys.exit(1)
