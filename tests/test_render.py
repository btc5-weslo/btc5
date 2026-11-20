"""Render deployment pieces: startup health, keep-alive never crashes, env-gated tasks, no secrets in repo files."""
import asyncio, os, sys, importlib
from pathlib import Path
import pytest

ROOT = Path(__file__).resolve().parents[1]


def test_health_ok_while_engine_still_starting():
    import btc5.app as A
    saved = dict(A.G)
    A.G.clear()
    try:
        body = A.api_health()
        assert body["ok"] is False and body["starting"] is True and "server_time" in body
    finally:
        A.G.clear(); A.G.update(saved)


def test_cloud_tasks_disabled_without_env(monkeypatch):
    for k in ("RENDER_EXTERNAL_URL", "BTC5_KEEPALIVE_URL", "HF_TOKEN", "BTC5_DB_BACKUP", "BTC5_INPROCESS_PUSH"):
        monkeypatch.delenv(k, raising=False)
    import btc5.cloud_tasks as T; T = importlib.reload(T)
    assert T.enabled_keepalive() is False
    assert T.enabled_backup() is False
    assert T.enabled_push() is False


def test_cloud_tasks_enabled_with_env(monkeypatch):
    monkeypatch.setenv("RENDER_EXTERNAL_URL", "https://example.onrender.com")
    monkeypatch.setenv("HF_TOKEN", "test-not-a-real-token")
    monkeypatch.setenv("BTC5_DB_BACKUP", "1")
    monkeypatch.setenv("BTC5_INPROCESS_PUSH", "1")
    import btc5.cloud_tasks as T; T = importlib.reload(T)
    assert T.enabled_keepalive() is True
    assert T.keepalive_url() == "https://example.onrender.com/api/health"
    assert T.enabled_backup() is True
    assert T.enabled_push() is True
    monkeypatch.delenv("HF_TOKEN")
    T = importlib.reload(T)
    assert T.enabled_backup() is False and T.enabled_push() is False


@pytest.mark.asyncio
async def test_keepalive_logs_failure_and_does_not_raise(monkeypatch):
    monkeypatch.setenv("BTC5_KEEPALIVE_URL", "http://127.0.0.1:9")
    monkeypatch.setenv("BTC5_KEEPALIVE_FIRST", "0")
    monkeypatch.setenv("BTC5_KEEPALIVE_EVERY", "60")
    import btc5.cloud_tasks as T; T = importlib.reload(T)
    stop = asyncio.Event()
    logs = []

    class Boom:
        async def get(self, *a, **k):
            raise ConnectionError("down")

    def say(m):
        logs.append(m)
        if "failed" in m or "->" in m:
            stop.set()

    monkeypatch.setattr(T, "_say", say)
    await asyncio.wait_for(T.keepalive_loop(Boom(), stop), timeout=5)
    assert any("keep-alive failed" in m for m in logs)


@pytest.mark.asyncio
async def test_shutdown_backup_skipped_without_token(monkeypatch):
    monkeypatch.delenv("HF_TOKEN", raising=False)
    monkeypatch.setenv("BTC5_DB_BACKUP", "1")
    import btc5.cloud_tasks as T; T = importlib.reload(T)
    await T.shutdown()  # must not raise


def test_render_blueprint_and_entrypoint():
    d = (ROOT / "Dockerfile").read_text()
    y = (ROOT / "render.yaml").read_text()
    s = (ROOT / "render_start.py").read_text()
    assert "render_start.py" in d
    assert "OMP_NUM_THREADS=1" in d
    assert "plan: free" in y
    assert "healthCheckPath: /api/health" in y
    assert "runtime: docker" in y
    assert "BTC5_FEED_SOURCE" in y and "cloud" in y
    assert "BTC5_FEED_ROLE" in y and "primary" in y
    assert "HF_TOKEN" in y and "sync: false" in y
    assert 'os.environ.get("PORT")' in s
    assert "backup.restore()" in s
    # Actual tokens / key blobs must not be baked into deploy files (the string "HF_TOKEN" as a name is fine).
    import re
    token_re = re.compile(r"hf_[A-Za-z0-9]{20,}|BEGIN (OPENSSH|RSA|EC|PRIVATE)|sk_live_[A-Za-z0-9]+")
    for name, text in ("Dockerfile", d), ("render.yaml", y), ("render_start.py", s):
        assert not token_re.search(text), name
        assert "BEGIN " not in text
