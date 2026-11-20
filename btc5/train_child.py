"""Child-process entry point for heavy jobs: `python -m btc5.train_child <train|backtest> '<json args>' <out.json>`.

Lowers its own CPU priority and volunteers as the OOM victim BEFORE importing numpy/sklearn, so the API server in the
parent process always wins the CPU and memory. Writes a JSON result to <out.json>."""
import json, os, sys, time


def _lower_priority():
    try: os.nice(int(os.environ.get("BTC5_CHILD_NICE", "19")))
    except Exception: pass
    try:
        with open("/proc/self/oom_score_adj", "w") as f: f.write("1000")
    except Exception: pass


def _gate_db_writes():
    """Never write to SQLite inside T0+40..120 s. The parent freezes this process (SIGSTOP) during T0+55..105 s; a frozen
    writer would hold the WAL write lock and block the T0+90 s lock insert. The 15 s margins make that impossible."""
    from btc5 import db as D
    from btc5.childproc import in_lock_window
    lo = float(os.environ.get("BTC5_CHILD_WRITE_GUARD_START_S", "40")); hi = float(os.environ.get("BTC5_CHILD_WRITE_GUARD_END_S", "120"))

    def wait_ok():
        while in_lock_window(time.time(), lo, hi):
            time.sleep(1.0)

    ox, om = D.DB.x, D.DB.many
    def x(self, sql, args=()):
        wait_ok(); return ox(self, sql, args)
    def many(self, sql, rows):
        wait_ok(); return om(self, sql, rows)
    D.DB.x, D.DB.many = x, many


def main(argv):
    kind, args, out = argv[1], json.loads(argv[2]), argv[3]
    _lower_priority()
    if os.environ.get("BTC5_CHILD_WRITE_GUARD", "1") == "1":
        _gate_db_writes()
    t0 = time.time()
    try:
        if kind == "train":
            from btc5 import training
            res = training.job(bool(args.get("backfill")), args.get("days"))
        elif kind == "backtest":
            from btc5.app_jobs import backtest_job
            res = {"summary": backtest_job(args.get("days"))}
        elif kind == "selftest":   # used by the test suite: proves priority/OOM settings and the memory guard
            buf = bytearray(int(args.get("alloc_mb", 0)) * 1024 * 1024)
            for i in range(0, len(buf), 4096): buf[i] = 1
            time.sleep(float(args.get("sleep", 0)))
            c0 = time.process_time()
            while time.process_time() - c0 < float(args.get("spin", 0)): pass
            with open("/proc/self/oom_score_adj") as f: oom = int(f.read().strip())
            res = {"nice": os.nice(0), "oom_score_adj": oom, "allocated_mb": len(buf) // (1024 * 1024)}
        else:
            raise ValueError(f"unknown job kind {kind!r}")
    except Exception as e:
        import traceback; traceback.print_exc()
        res = {"child_error": f"{type(e).__name__}: {e}"}
    res = dict(res, child_s=round(time.time() - t0, 1))
    tmp = out + ".tmp"
    with open(tmp, "w") as f: json.dump(res, f, default=str)
    os.replace(tmp, out)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
