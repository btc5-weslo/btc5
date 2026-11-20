#!/usr/bin/env python3
"""BTC-5 process supervisor (Windows PC; also works on Linux). Run with pythonw for no window:
    .venv\\Scripts\\pythonw.exe supervisor.py
- single instance: an exclusive lock on data/supervisor.lock; a second copy exits at once (safe to launch every 5 min)
- starts the API/engine server (uvicorn, 0.0.0.0:8700) and the phone-feed pusher as hidden child processes
- restarts either one if it exits; health watchdog kills the server after 3 failed /api/health checks (20 s apart)
- kills stale children left by a previous supervisor (pid files) before starting
- logs: data/supervisor.log, data/server.log, data/pusher.log
- data/DISABLED present -> exits without starting anything (stop-btc5.ps1 creates it, start-btc5.ps1 removes it)"""
import os, sys, time, subprocess, urllib.request, signal
HERE = os.path.dirname(os.path.abspath(__file__)); DATA = os.path.join(HERE, "data"); os.makedirs(DATA, exist_ok=True)
PORT = int(os.environ.get("BTC5_PORT", "8700"))
WIN = os.name == "nt"
PY = sys.executable
if WIN and PY.lower().endswith("pythonw.exe"): PY = PY[:-len("pythonw.exe")] + "python.exe"   # children need real stdout for logs
NOWIN = subprocess.CREATE_NO_WINDOW if WIN else 0

def log(msg):
    with open(os.path.join(DATA, "supervisor.log"), "a", encoding="utf-8") as f: f.write(time.strftime("%Y-%m-%d %H:%M:%S ") + msg + "\n")

def lock():
    fh = open(os.path.join(DATA, "supervisor.lock"), "a+")
    try:
        if WIN:
            import msvcrt; fh.seek(0); msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl; fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        return None
    return fh

def alive(pid):
    if not pid: return False
    if WIN:
        out = subprocess.run(["tasklist", "/FI", f"PID eq {pid}", "/NH", "/FO", "CSV"], capture_output=True, text=True, creationflags=NOWIN).stdout
        return f'"{pid}"' in out
    try: os.kill(pid, 0); return True
    except OSError: return False

def kill(pid):
    if not pid: return
    if WIN: subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"], capture_output=True, creationflags=NOWIN)
    else:
        try: os.kill(pid, signal.SIGTERM)
        except OSError: pass

def pidfile(n): return os.path.join(DATA, f"{n}.pid")
def readpid(n):
    try: return int(open(pidfile(n)).read().strip())
    except Exception: return None

def spawn(name, args, env_extra=None):
    env = dict(os.environ, PYTHONUNBUFFERED="1", PYTHONIOENCODING="utf-8", **(env_extra or {}))
    out = open(os.path.join(DATA, f"{name}.log"), "a", encoding="utf-8")
    p = subprocess.Popen(args, cwd=HERE, stdout=out, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, env=env, creationflags=NOWIN)
    open(pidfile(name), "w").write(str(p.pid)); log(f"started {name} pid {p.pid}")
    return p

def healthy():
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{PORT}/api/health", timeout=10) as r: return r.status == 200
    except Exception: return False

def main():
    if os.path.exists(os.path.join(DATA, "DISABLED")): return
    fh = lock()
    if fh is None: return                     # already running
    open(pidfile("supervisor"), "w").write(str(os.getpid()))
    log(f"supervisor start pid {os.getpid()} python {sys.version.split()[0]}")
    for n in ("server", "pusher", "backup"):  # orphans from a crashed supervisor would block port 8700
        p = readpid(n)
        if p and alive(p): log(f"killing stale {n} pid {p}"); kill(p)
    specs = {
        "server": ([PY, "-m", "uvicorn", "btc5.app:app", "--host", "0.0.0.0", "--port", str(PORT), "--no-access-log"], {"BTC5_PORT": str(PORT)}),
        "pusher": ([PY, "pusher.py"], {"BTC5_FEED_ROLE": os.environ.get("BTC5_FEED_ROLE", "primary"), "BTC5_FEED_SOURCE": os.environ.get("BTC5_FEED_SOURCE", "pc"), "BTC5_LOCAL": f"http://127.0.0.1:{PORT}"}),
    }
    if os.environ.get("BTC5_DB_BACKUP") == "1": specs["backup"] = ([PY, "backup.py"], {})
    procs = {n: spawn(n, *specs[n]) for n in specs}
    started = {n: time.time() for n in specs}; fails = 0; last_hc = time.time()
    try:
        while not os.path.exists(os.path.join(DATA, "DISABLED")):
            time.sleep(2)
            for n, p in list(procs.items()):
                rc = p.poll()
                if rc is not None:
                    log(f"{n} exited (rc={rc}), restarting in 3s"); time.sleep(3)
                    procs[n] = spawn(n, *specs[n]); started[n] = time.time()
                    if n == "server": fails = 0
            if time.time() - last_hc >= 20 and time.time() - started["server"] > 60:
                last_hc = time.time()
                if healthy(): fails = 0
                else:
                    fails += 1; log(f"health check failed ({fails})")
                    if fails >= 3:
                        log(f"server unresponsive - killing pid {procs['server'].pid} for restart"); kill(procs["server"].pid); fails = 0
        log("DISABLED flag found - stopping children")
    finally:
        for n, p in procs.items():
            if p.poll() is None: kill(p.pid)
        for n in ("server", "pusher", "backup", "supervisor"):
            try: os.remove(pidfile(n))
            except OSError: pass

if __name__ == "__main__":
    try: main()
    except Exception as e: log(f"supervisor crashed: {e!r}"); raise
