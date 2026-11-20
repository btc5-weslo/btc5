"""Always-on cloud deployment pieces: read-only public API, DB backup/restore safety."""
import gzip, json, os, sys, sqlite3, importlib
import pytest
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

def test_public_deployment_rejects_writes(monkeypatch):
    from fastapi.testclient import TestClient
    import btc5.app as A
    monkeypatch.setattr(A, "READONLY", True)
    c = TestClient(A.app)                      # no lifespan: no network
    for path in ("/api/settings", "/api/backtest/run", "/api/history/backfill", "/api/model/retrain"):
        r = c.post(path, json={"mode": "monitoring"}); assert r.status_code == 403, path

def mkdb(path, n_rounds, n_hist):
    from btc5.db import DB
    d = DB(path)
    for i in range(n_rounds):
        d.x("INSERT INTO rounds(round_id,start_ts,end_ts,decision_target_ts,status) VALUES (?,?,?,?,?)", (f"r{i}", 1000 + i * 300, 1300 + i * 300, 1090 + i * 300, "MISSED"))
    for i in range(n_hist):
        d.x("INSERT INTO hist_rounds(start_ts,slug) VALUES (?,?)", (2000 + i * 300, f"s{i}"))
    return path

class FakeApi:
    def __init__(self): self.calls = []
    def batch_bucket_files(self, bucket, add): self.calls.append({p: b for b, p in add})

def load(monkeypatch, db):
    monkeypatch.setenv("BTC5_DB", db)
    import backup; return importlib.reload(backup)

def test_backup_uploads_consistent_gzip_snapshot(tmp_path, monkeypatch):
    db = mkdb(str(tmp_path / "btc5.db"), 5, 7); B = load(monkeypatch, db); api = FakeApi()
    monkeypatch.setattr(B, "remote_meta", lambda: {"rounds": 3, "hist_rounds": 7})
    assert B.backup_once(api) is True
    files = api.calls[0]; meta = json.loads(files["db/btc5.meta.json"])
    assert meta["rounds"] == 5 and meta["hist_rounds"] == 7
    out = tmp_path / "x.db"; out.write_bytes(gzip.decompress(files["db/btc5.db.gz"]))
    assert sqlite3.connect(out).execute("select count(*) from rounds").fetchone()[0] == 5

def test_backup_never_overwrites_a_bigger_remote_backup(tmp_path, monkeypatch):
    db = mkdb(str(tmp_path / "btc5.db"), 0, 0); B = load(monkeypatch, db); api = FakeApi()   # fresh container after failed restore
    monkeypatch.setattr(B, "remote_meta", lambda: {"rounds": 1031, "hist_rounds": 3900})
    assert B.backup_once(api) is False and api.calls == []

def test_restore_from_bucket_when_local_db_missing(tmp_path, monkeypatch):
    src = mkdb(str(tmp_path / "src.db"), 4, 2)
    k = sqlite3.connect(src); k.execute("PRAGMA wal_checkpoint(TRUNCATE)"); k.close()
    (tmp_path / "db").mkdir(); (tmp_path / "db" / "btc5.db.gz").write_bytes(gzip.compress(open(src, "rb").read()))
    target = str(tmp_path / "data" / "btc5.db"); B = load(monkeypatch, target); monkeypatch.setattr(B.time, "sleep", lambda s: None)
    monkeypatch.setattr(B, "URL", (tmp_path.as_uri() + "/"))
    real = B.urllib.request.urlopen
    monkeypatch.setattr(B.urllib.request, "urlopen", lambda req, timeout=None: real(req.full_url.split("?")[0]))
    assert B.restore() is True
    assert sqlite3.connect(target).execute("select count(*) from rounds").fetchone()[0] == 4
    assert B.restore() is True       # second call: DB present -> untouched
