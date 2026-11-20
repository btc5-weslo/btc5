"""Incremental lock/grade backups survive a missing full snapshot; nothing is invented or re-locked."""
import asyncio, gzip, json, os, sqlite3, importlib, time
import pytest

def load(monkeypatch, db):
    monkeypatch.setenv("BTC5_DB", db)
    monkeypatch.setenv("BTC5_DB_BACKUP", "1")
    monkeypatch.setenv("HF_TOKEN", "test-not-a-real-token")
    monkeypatch.setenv("BTC5_FEED_SOURCE", "cloud")
    import backup
    return importlib.reload(backup)

class FakeApi:
    def __init__(self): self.calls = []
    def batch_bucket_files(self, bucket, add): self.calls.append({p: b for b, p in add})

def _db(path):
    from btc5.db import DB
    return DB(path)

def _locked(rid, T, prediction="DOWN", decision_ts=None, official=None):
    rec = {"round_id": rid, "start_ts": T, "end_ts": T + 300, "decision_target_ts": T + 90, "status": "LOCKED",
           "prediction": prediction, "p_up": 0.2 if prediction == "DOWN" else 0.8, "p_down": 0.8 if prediction == "DOWN" else 0.2,
           "confidence": "HIGH", "decision_ts": decision_ts if decision_ts is not None else T + 90.183, "deviation_ms": 183.0,
           "result": "PENDING"}
    if official:
        rec.update(official_outcome=official, official_open=100.0, official_close=99.0, resolved_ts=T + 310, result="CORRECT" if official == prediction else "INCORRECT")
    return rec


def test_record_round_writes_local_and_uploads_compact_json(tmp_path, monkeypatch):
    dbp = str(tmp_path / "btc5.db")
    d = _db(dbp)
    rec = _locked("btc-updown-5m-1000", 1000, "DOWN", decision_ts=1090.183)
    assert d.insert_round(rec)
    B = load(monkeypatch, dbp)
    monkeypatch.setattr(B, "_fetch_json", lambda *a, **k: {"rounds": {}})
    api = FakeApi()
    assert B.record_round("btc-updown-5m-1000", api=api) is True
    local = json.loads((tmp_path / "incr.json").read_text())
    row = local["rounds"]["btc-updown-5m-1000"]["round"]
    assert row["prediction"] == "DOWN" and row["decision_ts"] == 1090.183 and row["status"] == "LOCKED"
    assert "db/incr.json" in api.calls[-1]
    uploaded = json.loads(api.calls[-1]["db/incr.json"])
    assert uploaded["rounds"]["btc-updown-5m-1000"]["round"]["decision_ts"] == 1090.183


def test_restore_replays_locks_missing_from_full_snapshot(tmp_path, monkeypatch):
    """The 1:36 AM failure mode: full snapshot is stale, incrementals hold the locks."""
    snap = str(tmp_path / "src.db")
    d = _db(snap)
    d.insert_round({"round_id": "old", "start_ts": 100, "end_ts": 400, "decision_target_ts": 190, "status": "MISSED", "result": "MISSED"})
    k = sqlite3.connect(snap); k.execute("PRAGMA wal_checkpoint(TRUNCATE)"); k.close()
    (tmp_path / "db").mkdir()
    (tmp_path / "db" / "btc5.db.gz").write_bytes(gzip.compress(open(snap, "rb").read()))
    locked = _locked("btc-updown-5m-2000", 2000, "UP", decision_ts=2090.001)
    incr = {"ts": time.time(), "source": "cloud", "rounds": {"btc-updown-5m-2000": {"round": locked, "trades": []}}}
    (tmp_path / "db" / "incr.json").write_text(json.dumps(incr))
    target = str(tmp_path / "data" / "btc5.db")
    B = load(monkeypatch, target)
    monkeypatch.setattr(B, "URL", (tmp_path.as_uri() + "/"))
    monkeypatch.setattr(B.time, "sleep", lambda s: None)
    real = B.urllib.request.urlopen
    monkeypatch.setattr(B.urllib.request, "urlopen", lambda req, timeout=None: real(req.full_url.split("?")[0]))
    assert B.restore() is True
    row = sqlite3.connect(target).execute("select status, prediction, decision_ts from rounds where round_id='btc-updown-5m-2000'").fetchone()
    assert row == ("LOCKED", "UP", 2090.001)


def test_replay_replaces_missed_placeholder_with_original_lock(tmp_path, monkeypatch):
    dbp = str(tmp_path / "btc5.db")
    d = _db(dbp)
    d.insert_round({"round_id": "btc-updown-5m-3000", "start_ts": 3000, "end_ts": 3300, "decision_target_ts": 3090,
                    "status": "MISSED", "missed_reason": "system offline", "result": "MISSED"})
    B = load(monkeypatch, dbp)
    bundle = {"round": _locked("btc-updown-5m-3000", 3000, "DOWN", decision_ts=3090.183), "trades": []}
    assert B.apply_incremental_round(d, bundle) == "replaced_missed"
    row = d.get_round("btc-updown-5m-3000")
    assert row["status"] == "LOCKED" and row["prediction"] == "DOWN" and row["decision_ts"] == 3090.183
    # immutability is restored: cannot change the prediction
    with pytest.raises(Exception):
        d.x("UPDATE rounds SET prediction='UP' WHERE round_id=?", ("btc-updown-5m-3000",))


def test_replay_does_not_overwrite_an_existing_lock_or_redecide(tmp_path, monkeypatch):
    dbp = str(tmp_path / "btc5.db")
    d = _db(dbp)
    d.insert_round(_locked("btc-updown-5m-4000", 4000, "DOWN", decision_ts=4090.1))
    B = load(monkeypatch, dbp)
    fake = {"round": _locked("btc-updown-5m-4000", 4000, "UP", decision_ts=4090.9), "trades": []}
    assert B.apply_incremental_round(d, fake) == "kept"
    row = d.get_round("btc-updown-5m-4000")
    assert row["prediction"] == "DOWN" and row["decision_ts"] == 4090.1


def test_replay_applies_a_grade_to_an_existing_lock(tmp_path, monkeypatch):
    dbp = str(tmp_path / "btc5.db")
    d = _db(dbp)
    d.insert_round(_locked("btc-updown-5m-5000", 5000, "DOWN", decision_ts=5090.2))
    B = load(monkeypatch, dbp)
    graded = _locked("btc-updown-5m-5000", 5000, "DOWN", decision_ts=5090.2, official="DOWN")
    assert B.apply_incremental_round(d, {"round": graded, "trades": []}) == "graded"
    row = d.get_round("btc-updown-5m-5000")
    assert row["official_outcome"] == "DOWN" and row["result"] == "CORRECT" and row["decision_ts"] == 5090.2


def test_recover_after_replay_does_not_mark_restored_locks_missed(tmp_path, monkeypatch):
    from helpers import FakeClock, FakeLive, FakePoly, make_sleep
    from btc5.engine import Engine
    from btc5.config import Config
    dbp = str(tmp_path / "btc5.db")
    d = _db(dbp)
    # three restored locks, then a gap, then 'now' is two rounds later
    T = 1_800_000_000 // 300 * 300
    d.insert_round(_locked(f"btc-updown-5m-{T-600}", T - 600, "UP", decision_ts=T - 600 + 90.0))
    d.insert_round(_locked(f"btc-updown-5m-{T-300}", T - 300, "DOWN", decision_ts=T - 300 + 90.2))
    cfg = Config(); clk = FakeClock(T + 10)
    eng = Engine(cfg, d, clk, FakeLive(T), FakePoly(cfg), sleep=make_sleep(clk))
    async def go():
        eng.recover()
        for t in list(eng.tasks):
            t.cancel()
        await asyncio.sleep(0)
    asyncio.run(go())
    locked = {r["start_ts"]: r["status"] for r in d.q("SELECT start_ts,status,prediction FROM rounds")}
    assert locked[T - 600] == "LOCKED" and locked[T - 300] == "LOCKED"
    # last lock is T-300, 'now' is T+10, so recover does not invent a lock for T (range stops before cur).
    row_now = d.get_round(f"btc-updown-5m-{T}")
    assert row_now is None or row_now["prediction"] is None


def test_flush_shutdown_uploads_incrementals_before_full_snapshot(tmp_path, monkeypatch):
    dbp = str(tmp_path / "btc5.db")
    d = _db(dbp)
    d.insert_round(_locked("btc-updown-5m-6000", 6000, "UP", decision_ts=6090.0))
    d.x("INSERT INTO hist_rounds(start_ts,slug) VALUES (?,?)", (1, "s"))
    B = load(monkeypatch, dbp)
    monkeypatch.setattr(B, "_fetch_json", lambda *a, **k: {"rounds": {}})
    monkeypatch.setattr(B, "remote_meta", lambda: {"rounds": 0, "hist_rounds": 0})
    api = FakeApi()
    assert B.record_round("btc-updown-5m-6000", api=api, upload=False) is True
    B.flush_shutdown(api=api)
    kinds = [set(c) for c in api.calls]
    # first upload should include incr; a later (or same) call includes the full snapshot
    assert any("db/incr.json" in c and "db/btc5.db.gz" not in c for c in kinds) or (
        kinds and "db/incr.json" in kinds[0]
    )
    assert any("db/btc5.db.gz" in c for c in kinds)
    # incrementals appear at or before the full snapshot
    incr_i = next(i for i, c in enumerate(api.calls) if "db/incr.json" in c)
    full_i = next(i for i, c in enumerate(api.calls) if "db/btc5.db.gz" in c)
    assert incr_i <= full_i


def test_full_backup_skipped_during_training_unless_forced(tmp_path, monkeypatch):
    dbp = str(tmp_path / "btc5.db")
    d = _db(dbp)
    d.insert_round(_locked("btc-updown-5m-7000", 7000, "UP", 7090.0))
    B = load(monkeypatch, dbp)
    monkeypatch.setattr(B, "remote_meta", lambda: {"rounds": 0, "hist_rounds": 0})
    api = FakeApi()
    B.set_training(True)
    assert B.backup_once(api) is False and api.calls == []
    assert B.backup_once(api, force=True) is True and any("db/btc5.db.gz" in c for c in api.calls)


def test_low_mem_skips_process_pool_and_gbdt(tmp_path, monkeypatch):
    monkeypatch.setenv("BTC5_LOW_MEM", "1")
    monkeypatch.setenv("BTC5_DB", str(tmp_path / "t.db"))
    import importlib
    import btc5.config as C; importlib.reload(C)
    assert C.low_mem() is True
    from btc5.config import Config
    from btc5.db import DB
    from btc5.engine import Engine
    from helpers import FakeClock, FakeLive, FakePoly, make_sleep
    cfg = Config(); db = DB(str(tmp_path / "t.db"))
    eng = Engine(cfg, db, FakeClock(1000), FakeLive(900), FakePoly(cfg), sleep=make_sleep(FakeClock(1000)))
    assert eng.pool is None
    import btc5.training as TR; TR = importlib.reload(TR)
    seen = {}
    def fake_run(db, days=None, archs=("stack", "gbdt", "lr"), max_rounds=None, step_frac=0.1):
        seen["archs"] = archs; seen["days"] = days; seen["max_rounds"] = max_rounds
        return {"error": "only 0 real historical rounds available; need >= 200", "n": 0}
    monkeypatch.setattr(TR.BT, "run", fake_run)
    out = TR.job(do_backfill=True, days=None)
    assert out.get("low_mem") is True
    assert seen["archs"] == ("stack", "lr")
    assert "gbdt" not in seen["archs"]
    assert seen["days"] == 4
    assert "backfilled" not in out  # backfill skipped on low mem
