"""SQLite persistence (WAL). Predictions are immutable: UNIQUE(round_id) + triggers that reject edits of locked fields."""
import sqlite3, json, threading, time, os, zlib

SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS rounds(
  round_id TEXT PRIMARY KEY, start_ts INTEGER NOT NULL UNIQUE, end_ts INTEGER NOT NULL, decision_target_ts REAL NOT NULL,
  status TEXT NOT NULL,                 -- LOCKED | MISSED | MONITORING_ONLY | MARKET_UNVERIFIED
  market_id TEXT, condition_id TEXT, question TEXT, up_token TEXT, down_token TEXT, resolution_source TEXT, rules TEXT,
  decision_ts REAL, published_ts REAL, deviation_ms REAL, publish_latency_ms REAL, cutoff_ts REAL,
  prediction TEXT, p_up REAL, p_down REAL, edge REAL, confidence TEXT, disagreement REAL, data_quality REAL, degraded INTEGER,
  regime TEXT, model_version TEXT, tie_break TEXT, open_price REAL, open_source TEXT, price_at_prediction REAL, chainlink_at_prediction REAL,
  crowd_up REAL, agents_json TEXT, evidence_json TEXT, missed_reason TEXT,
  -- resolution (filled once, later)
  official_outcome TEXT, official_open REAL, official_close REAL, resolved_ts REAL, resolution_json TEXT, result TEXT, failure_analysis TEXT
);
CREATE TRIGGER IF NOT EXISTS rounds_immutable BEFORE UPDATE OF round_id,start_ts,status,decision_ts,deviation_ms,cutoff_ts,prediction,p_up,p_down,confidence,agents_json,model_version ON rounds
BEGIN SELECT RAISE(ABORT,'locked prediction records are immutable'); END;
CREATE TRIGGER IF NOT EXISTS rounds_publish_once BEFORE UPDATE OF published_ts ON rounds WHEN OLD.published_ts IS NOT NULL
BEGIN SELECT RAISE(ABORT,'publication timestamp is permanent'); END;
CREATE TRIGGER IF NOT EXISTS rounds_resolve_once BEFORE UPDATE OF official_outcome ON rounds WHEN OLD.official_outcome IS NOT NULL
BEGIN SELECT RAISE(ABORT,'official outcome already recorded'); END;
CREATE TRIGGER IF NOT EXISTS rounds_nodelete BEFORE DELETE ON rounds BEGIN SELECT RAISE(ABORT,'prediction history cannot be deleted'); END;
CREATE TABLE IF NOT EXISTS agent_results(round_id TEXT, agent TEXT, direction TEXT, p_up REAL, correct INTEGER, ts REAL, PRIMARY KEY(round_id, agent));
CREATE TABLE IF NOT EXISTS live_features(round_id TEXT PRIMARY KEY, start_ts INTEGER, features_json TEXT, outcome TEXT);
CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, round_id TEXT, source TEXT, level TEXT, message TEXT);
CREATE TABLE IF NOT EXISTS model_versions(version TEXT PRIMARY KEY, created_ts REAL, kind TEXT, role TEXT, n_train INTEGER, metrics_json TEXT, blob BLOB);
CREATE TABLE IF NOT EXISTS backtests(id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, params_json TEXT, results_json TEXT);
CREATE TABLE IF NOT EXISTS kv(k TEXT PRIMARY KEY, v TEXT);
-- historical (real, fetched) data
CREATE TABLE IF NOT EXISTS hist_rounds(start_ts INTEGER PRIMARY KEY, slug TEXT, outcome TEXT, up_token TEXT, crowd_json TEXT, s1 BLOB, fetched_ts REAL, err TEXT);
CREATE TABLE IF NOT EXISTS m1(ts INTEGER PRIMARY KEY, o REAL, h REAL, l REAL, c REAL, v REAL, qv REAL, n INTEGER, tbv REAL);
CREATE TABLE IF NOT EXISTS oi5m(ts INTEGER PRIMARY KEY, oi_usd REAL, vol_usd REAL);
CREATE TABLE IF NOT EXISTS funding(ts INTEGER PRIMARY KEY, rate REAL);
-- automatic trading (demo = paper money at real Polymarket prices; live = real CLOB orders). One row per round, ever.
CREATE TABLE IF NOT EXISTS trades(
  id INTEGER PRIMARY KEY AUTOINCREMENT, round_id TEXT NOT NULL UNIQUE, start_ts INTEGER, end_ts INTEGER,
  mode TEXT NOT NULL CHECK(mode IN ('demo','live')), epoch INTEGER, side TEXT, token TEXT, confidence TEXT, p_side REAL,
  stake REAL, best_ask REAL, entry_price REAL, price_source TEXT, shares REAL, cost REAL, fee REAL,
  status TEXT NOT NULL,                 -- PENDING | SUBMITTING | OPEN | WON | LOST | SKIPPED | FAILED | UNFILLED
  outcome TEXT, payout REAL, pnl REAL, order_id TEXT, order_json TEXT, error TEXT,
  created_ts REAL, placed_ts REAL, settled_ts REAL);
CREATE INDEX IF NOT EXISTS trades_mode ON trades(mode, epoch, status);
CREATE TRIGGER IF NOT EXISTS trades_nodelete BEFORE DELETE ON trades BEGIN SELECT RAISE(ABORT,'trade history cannot be deleted'); END;
CREATE TRIGGER IF NOT EXISTS trades_settle_once BEFORE UPDATE OF pnl ON trades WHEN OLD.pnl IS NOT NULL BEGIN SELECT RAISE(ABORT,'trade already settled'); END;
"""

class DB:
    def __init__(self, path):
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self.path = path
        self.lock = threading.RLock()
        self.con = sqlite3.connect(path, check_same_thread=False, timeout=30, isolation_level=None)
        self.con.row_factory = sqlite3.Row
        self.con.executescript(SCHEMA)

    def x(self, sql, args=()):
        with self.lock:
            cur = self.con.execute(sql, args); return cur.lastrowid

    def many(self, sql, rows):
        with self.lock:
            self.con.execute("BEGIN")
            try: self.con.executemany(sql, rows); self.con.execute("COMMIT")
            except Exception: self.con.execute("ROLLBACK"); raise

    def q(self, sql, args=()):
        with self.lock:
            return [dict(r) for r in self.con.execute(sql, args).fetchall()]

    def one(self, sql, args=()):
        r = self.q(sql, args); return r[0] if r else None

    def insert_round(self, rec: dict) -> bool:
        """Atomic, exactly-once insert. Returns False if the round already has a record (duplicate prevented)."""
        cols = ",".join(rec); qs = ",".join("?" * len(rec))
        with self.lock:
            try:
                self.con.execute("BEGIN IMMEDIATE")
                self.con.execute(f"INSERT INTO rounds ({cols}) VALUES ({qs})", tuple(rec.values()))
                self.con.execute("COMMIT"); return True
            except sqlite3.IntegrityError:
                self.con.execute("ROLLBACK"); return False
            except Exception:
                self.con.execute("ROLLBACK"); raise

    def get_round(self, rid): return self.one("SELECT * FROM rounds WHERE round_id=?", (rid,))

    def kv_get(self, k, default=None):
        r = self.one("SELECT v FROM kv WHERE k=?", (k,)); return json.loads(r["v"]) if r else default
    def kv_set(self, k, v): self.x("INSERT INTO kv(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (k, json.dumps(v)))

    def event(self, round_id, source, level, message):
        return self.x("INSERT INTO events(ts,round_id,source,level,message) VALUES(?,?,?,?,?)", (time.time(), round_id, source, level, message))

def pack(obj): return zlib.compress(json.dumps(obj, separators=(",", ":")).encode())
def unpack(b): return json.loads(zlib.decompress(b)) if b else None
