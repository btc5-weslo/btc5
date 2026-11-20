"""Run heavy jobs (training, backtests) in a LOW-PRIORITY CHILD PROCESS so they can never starve the API server.

Why: on Render free (0.1 CPU, 512 MB) an in-process `asyncio.to_thread(training.job)` shares the GIL and the tiny CPU
quota with the event loop. Render's 5 s HTTP health check then timed out and Render restarted the instance mid-round.

What this does:
  * `python -m btc5.train_child` in a fresh process (no GIL sharing). The child lowers its own priority to nice 19
    before importing numpy/sklearn, so inside the container's CPU quota the kernel runs the server first.
  * The child sets oom_score_adj=1000: if memory ever runs out the kernel kills the CHILD, never the server.
  * Memory guard: parent RSS + child RSS (and the container cgroup usage when readable) is sampled twice a second;
    the child is killed if it would push the container near the 512 MB limit.
  * Lock-window guard: the child is paused (SIGSTOP) from T0+55 s to T0+105 s of every round, so the T0+90 s lock
    never competes with training at all, then resumed (SIGCONT). The child only writes to SQLite outside T0+40..120 s
    (see train_child), so it can never be frozen while holding the database write lock.
  * Hard timeout; the child is always reaped. Its result comes back as JSON through a temp file.
"""
import asyncio, json, os, signal, sys, tempfile, time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PAGE_MB = 1024 * 1024

# Live memory/child status, exposed on /api/health ("memory").
MEM = {"child_pid": None, "child_rss_mb": None, "child_peak_mb": None, "last_job_peak_total_mb": None,
       "last_job_peak_child_mb": None, "last_job_peak_cgroup_mb": None, "last_job_s": None, "last_job_kind": None,
       "last_job_error": None, "paused_for_lock": False, "pauses": 0, "killed_for_memory": 0}


def rss_mb(pid="self"):
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return round(int(line.split()[1]) / 1024, 1)
    except Exception:
        return None
    return None


def cgroup_mb():
    """Container memory usage as the host's limit sees it (cgroup v2 / v1), or None."""
    for p in ("/sys/fs/cgroup/memory.current", "/sys/fs/cgroup/memory/memory.usage_in_bytes"):
        try:
            with open(p) as f:
                return round(int(f.read().strip()) / PAGE_MB, 1)
        except Exception:
            continue
    return None


def snapshot():
    out = dict(MEM)
    out["rss_mb"] = rss_mb()
    out["cgroup_mb"] = cgroup_mb()
    if out["child_pid"]:
        out["child_rss_mb"] = rss_mb(out["child_pid"])
    return out


def in_lock_window(now, start=None, end=None):
    """True from T0+start to T0+end of the current 5-minute round (default 55..105 s, around the T0+90 s lock)."""
    start = float(os.environ.get("BTC5_LOCK_GUARD_START_S", "55")) if start is None else start
    end = float(os.environ.get("BTC5_LOCK_GUARD_END_S", "105")) if end is None else end
    el = now % 300
    return start <= el < end


async def run_job(kind, args, now_fn=time.time, timeout=None, mem_limit_mb=None, log=print, sleep=asyncio.sleep):
    """Run `python -m btc5.train_child <kind> <json args>` and return its JSON result (raises on failure)."""
    timeout = float(os.environ.get("BTC5_CHILD_TIMEOUT_S", "1500")) if timeout is None else timeout
    mem_limit_mb = float(os.environ.get("BTC5_CHILD_MEM_LIMIT_MB", "460")) if mem_limit_mb is None else mem_limit_mb
    fd, out_path = tempfile.mkstemp(prefix=f"btc5-{kind}-", suffix=".json"); os.close(fd)
    env = dict(os.environ)
    env.setdefault("OMP_NUM_THREADS", "1"); env.setdefault("OPENBLAS_NUM_THREADS", "1"); env.setdefault("MKL_NUM_THREADS", "1")
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    t0 = time.time(); peak_child = peak_total = peak_cg = 0.0; stopped = False; err = None
    proc = await asyncio.create_subprocess_exec(sys.executable, "-m", "btc5.train_child", kind, json.dumps(args), out_path,
                                                cwd=ROOT, env=env, stdin=asyncio.subprocess.DEVNULL)
    MEM.update(child_pid=proc.pid, child_peak_mb=0.0, paused_for_lock=False, last_job_kind=kind, last_job_error=None)
    log(f"{kind} child started pid={proc.pid} (nice 19, oom_score_adj 1000, mem limit {mem_limit_mb:.0f} MB)")
    try:
        while proc.returncode is None:
            try:
                await asyncio.wait_for(proc.wait(), timeout=0.5)
                break
            except asyncio.TimeoutError:
                pass
            c = rss_mb(proc.pid) or 0.0; p = rss_mb() or 0.0; cg = cgroup_mb() or 0.0
            peak_child = max(peak_child, c); peak_total = max(peak_total, c + p); peak_cg = max(peak_cg, cg)
            MEM.update(child_rss_mb=c, child_peak_mb=peak_child)
            if c + p > mem_limit_mb or (cg and cg > mem_limit_mb + 30):
                MEM["killed_for_memory"] += 1
                err = f"memory guard: server {p:.0f} MB + child {c:.0f} MB (cgroup {cg:.0f} MB) > {mem_limit_mb:.0f} MB - child killed"
                log(err); _signal(proc, signal.SIGCONT); _signal(proc, signal.SIGKILL); break
            if time.time() - t0 > timeout:
                err = f"{kind} child exceeded {timeout:.0f} s - killed"
                log(err); _signal(proc, signal.SIGCONT); _signal(proc, signal.SIGKILL); break
            want_stop = in_lock_window(now_fn())
            if want_stop and not stopped:
                if _signal(proc, signal.SIGSTOP): stopped = True; MEM["pauses"] += 1; MEM["paused_for_lock"] = True
            elif stopped and not want_stop:
                _signal(proc, signal.SIGCONT); stopped = False; MEM["paused_for_lock"] = False
        await proc.wait()
        if err:
            raise RuntimeError(err)
        if proc.returncode != 0:
            raise RuntimeError(f"{kind} child exited with code {proc.returncode}")
        with open(out_path) as f:
            res = json.load(f)
        if isinstance(res, dict) and res.get("child_error"):
            raise RuntimeError(res["child_error"])
        return res
    except BaseException as e:
        MEM["last_job_error"] = str(e)[:300]
        raise
    finally:
        if proc.returncode is None:     # cancelled (shutdown) or error path: never leave an orphan
            _signal(proc, signal.SIGCONT); _signal(proc, signal.SIGKILL)
            try: await asyncio.wait_for(proc.wait(), timeout=5)
            except Exception: pass
        MEM.update(child_pid=None, child_rss_mb=None, paused_for_lock=False, last_job_s=round(time.time() - t0, 1),
                   last_job_peak_total_mb=round(peak_total, 1), last_job_peak_child_mb=round(peak_child, 1),
                   last_job_peak_cgroup_mb=round(peak_cg, 1) if peak_cg else None)
        try: os.unlink(out_path)
        except OSError: pass
        log(f"{kind} child done in {time.time() - t0:.0f}s rc={proc.returncode} peak child {peak_child:.0f} MB, "
            f"server+child {peak_total:.0f} MB, cgroup {peak_cg:.0f} MB, lock-window pauses so far {MEM['pauses']}")


def _signal(proc, sig):
    try:
        if proc.returncode is None:
            os.kill(proc.pid, sig); return True
    except ProcessLookupError:
        pass
    except Exception:
        pass
    return False
