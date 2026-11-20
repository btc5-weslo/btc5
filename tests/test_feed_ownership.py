"""Feed-ownership rules between the Windows PC (primary) and the box (fallback) writing the same phone feed."""
import importlib, os, sys, subprocess, time
import pytest

def load(monkeypatch, role, source):
    monkeypatch.setenv("BTC5_FEED_ROLE", role); monkeypatch.setenv("BTC5_FEED_SOURCE", source)
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    import pusher; return importlib.reload(pusher)

def test_pc_pushes_over_box(monkeypatch):
    p = load(monkeypatch, "primary", "pc")
    monkeypatch.setattr(p, "current_owner", lambda: ("box", 1.0))
    assert p.should_push()[0] is True

@pytest.mark.parametrize("src", ["cloud", "hf-space"])
def test_cloud_always_pushes_without_reading(monkeypatch, src):
    p = load(monkeypatch, "primary", src)
    def boom(): raise AssertionError("highest priority must not need to read the feed")
    monkeypatch.setattr(p, "current_owner", boom)
    assert p.should_push()[0] is True

@pytest.mark.parametrize("me", ["pc", "box"])
def test_pc_and_box_yield_to_fresh_cloud(monkeypatch, me):
    p = load(monkeypatch, "primary" if me == "pc" else "fallback", me)
    monkeypatch.setattr(p, "current_owner", lambda: ("cloud", 8.0)); assert p.should_push()[0] is False
    monkeypatch.setattr(p, "current_owner", lambda: ("hf-space", 8.0)); assert p.should_push()[0] is False
    monkeypatch.setattr(p, "current_owner", lambda: ("cloud", p.STANDDOWN_S + 1)); assert p.should_push()[0] is True   # cloud down -> take over

def test_fallback_stands_down_while_other_source_is_fresh(monkeypatch):
    p = load(monkeypatch, "fallback", "box")
    monkeypatch.setattr(p, "current_owner", lambda: ("pc", 12.0))
    ok, why = p.should_push(); assert ok is False and "standing down" in why

def test_fallback_takes_over_when_primary_goes_quiet(monkeypatch):
    p = load(monkeypatch, "fallback", "box")
    monkeypatch.setattr(p, "current_owner", lambda: ("pc", p.STANDDOWN_S + 5))
    assert p.should_push()[0] is True

def test_fallback_keeps_pushing_its_own_feed_and_legacy_feed(monkeypatch):
    p = load(monkeypatch, "fallback", "box")
    monkeypatch.setattr(p, "current_owner", lambda: ("box", 3.0)); assert p.should_push()[0] is True
    monkeypatch.setattr(p, "current_owner", lambda: (None, 3.0)); assert p.should_push()[0] is True   # pre-upgrade feed has no source

def test_fallback_pushes_if_feed_unreadable(monkeypatch):
    p = load(monkeypatch, "fallback", "box")
    def boom(): raise OSError("offline")
    monkeypatch.setattr(p, "current_owner", boom); assert p.should_push()[0] is True

def test_push_tags_source(monkeypatch):
    p = load(monkeypatch, "primary", "pc"); sent = {}
    monkeypatch.setattr(p.API, "batch_bucket_files", lambda bucket, add: sent.update(bucket=bucket, body=add[0][0]))
    p.push({"state": {}}); import json; d = json.loads(sent["body"])
    assert d["source"] == "pc" and abs(d["pushed_at"] - time.time()) < 5

def test_supervisor_single_instance_lock(tmp_path, monkeypatch):
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    import supervisor as S; S = importlib.reload(S)
    monkeypatch.setattr(S, "DATA", str(tmp_path))
    a = S.lock(); assert a is not None
    code = ("import sys; sys.path.insert(0, %r); import supervisor as S; S.DATA=%r; print('LOCKED' if S.lock() is None else 'GOT')"
            % (os.path.join(os.path.dirname(__file__), ".."), str(tmp_path)))
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=30).stdout
    assert "LOCKED" in out, out          # a second supervisor process cannot start while the first holds the lock
