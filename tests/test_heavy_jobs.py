"""Heavy work (training/backtests) must never stop the health check, the T0+90 s lock or grading from responding."""
import asyncio, os, sys, time, threading
import pytest
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def test_lock_window_bounds():
    from btc5.childproc import in_lock_window
    T = 1_795_181_100
    assert not in_lock_window(T + 54.9) and in_lock_window(T + 55) and in_lock_window(T + 90) and not in_lock_window(T + 105)
    assert in_lock_window(T + 45, 40, 120) and not in_lock_window(T + 130, 40, 120)


def test_child_runs_low_priority_and_is_oom_victim():
    from btc5 import childproc
    res = asyncio.run(childproc.run_job("selftest", {"alloc_mb": 5, "sleep": 1.2}, now_fn=lambda: 1200.0, log=lambda m: None))
    assert res["nice"] == min(19, os.nice(0) + 19)
    assert res["oom_score_adj"] == 1000 and res["allocated_mb"] == 5
    assert childproc.MEM["child_pid"] is None and childproc.MEM["last_job_peak_child_mb"] > 0


def test_memory_guard_kills_child_not_server():
    from btc5 import childproc
    with pytest.raises(RuntimeError, match="memory guard"):
        asyncio.run(childproc.run_job("selftest", {"alloc_mb": 150, "sleep": 30}, now_fn=lambda: 1200.0,
                                      mem_limit_mb=(childproc.rss_mb() or 50) + 60, log=lambda m: None))
    assert childproc.MEM["killed_for_memory"] >= 1 and childproc.MEM["child_pid"] is None


def test_child_paused_during_lock_window_and_resumed():
    from btc5 import childproc
    clock = {"t": 1000.0 * 300 + 50.0}           # T0+50 s; advances 10 s of round time per real second
    t0 = time.time()
    now = lambda: clock["t"] + (time.time() - t0) * 10
    before = childproc.MEM["pauses"]
    res = asyncio.run(childproc.run_job("selftest", {"sleep": 5}, now_fn=now, log=lambda m: None))
    assert res["allocated_mb"] == 0 and childproc.MEM["pauses"] > before and childproc.MEM["paused_for_lock"] is False


def test_child_error_is_reported():
    from btc5 import childproc
    with pytest.raises(RuntimeError, match="unknown job kind"):
        asyncio.run(childproc.run_job("nope", {}, now_fn=lambda: 1200.0, log=lambda m: None))


def test_healthz_and_api_health_answer_fast_even_if_status_is_slow(monkeypatch):
    import btc5.app as A
    saved = dict(A.G)
    class Eng: training = {"running": True, "started": time.time(), "last": None, "error": None}
    A.G.update(eng=Eng(), live=object(), started=time.time() - 50)
    A._HEALTH.update(task=None, data={"ok": True, "cached": 1}, ts=time.time() - 3)
    monkeypatch.setattr(A, "health", lambda: (time.sleep(3), {"ok": True})[1])
    monkeypatch.setenv("BTC5_HEALTH_BUDGET_S", "0.3")
    try:
        async def go():
            t = time.perf_counter(); z = await A.healthz(); dz = time.perf_counter() - t
            t = time.perf_counter(); h = await A.api_health(); dh = time.perf_counter() - t
            return z, dz, h, dh
        z, dz, h, dh = asyncio.run(go())
        assert z["ok"] is True and z["uptime_s"] >= 50 and dz < 0.05
        assert dh < 1.0 and h["cached"] == 1 and h["stale_s"] >= 3 and h["training"]["running"] is True and "memory" in h
    finally:
        A.G.clear(); A.G.update(saved); A._HEALTH.update(task=None, data=None, ts=None)


def test_job_endpoints_need_admin_token_when_set(monkeypatch):
    from fastapi.testclient import TestClient
    import btc5.app as A
    monkeypatch.setenv("BTC5_ADMIN_TOKEN", "t0ken-for-test")
    c = TestClient(A.app)
    for path in ("/api/model/retrain", "/api/backtest/run", "/api/history/backfill"):
        assert c.post(path, json={}).status_code == 401, path


def test_low_mem_training_uses_child_process(tmp_path, monkeypatch):
    monkeypatch.setenv("BTC5_LOW_MEM", "1"); monkeypatch.delenv("BTC5_TRAIN_CHILD", raising=False)
    from btc5 import engine as E
    assert E.use_child_process() is True
    monkeypatch.setenv("BTC5_TRAIN_CHILD", "0"); assert E.use_child_process() is False
    monkeypatch.setenv("BTC5_LOW_MEM", "0"); monkeypatch.delenv("BTC5_TRAIN_CHILD"); assert E.use_child_process() is False


def test_child_cpu_budget_duty_cycles_a_cpu_hog(monkeypatch):
    """With a 0.4-CPU quota and a 50 % share the child may use ~0.2 CPU: 0.8 s of spinning must take >= ~3 s wall."""
    from btc5 import childproc
    monkeypatch.setenv("BTC5_CPU_QUOTA", "0.4"); monkeypatch.setenv("BTC5_CHILD_CPU_SHARE", "0.5")
    t = time.time()
    asyncio.run(childproc.run_job("selftest", {"spin": 0.8}, now_fn=lambda: 1200.0, log=lambda m: None))
    wall = time.time() - t
    assert wall >= 3.0, wall
    assert childproc.MEM["cpu_throttle_s"] > 0 and childproc.MEM["cpu_budget_cpus"] == pytest.approx(0.2)


def test_pin_to_one_cpu_is_safe():
    from btc5 import childproc
    before = os.sched_getaffinity(0)
    try:
        cpu = childproc.pin_to_one_cpu()
        assert cpu in before and os.sched_getaffinity(0) == {cpu}
    finally:
        for tid in os.listdir("/proc/self/task"):
            try: os.sched_setaffinity(int(tid), before)
            except Exception: pass
