"""API never exposes a directional prediction before the T90 publication event."""
import asyncio, os, tempfile, json, time
import pytest

def test_prediction_and_agent_directions_redacted_before_T90(monkeypatch):
    import btc5.app as A
    from btc5.config import Config
    from helpers import FakeClock, FakeLive, FakePoly, make_sleep
    from btc5.engine import Engine
    from btc5.db import DB
    T = 1_800_000_000 // 300 * 300
    cfg = Config(); db = DB(os.path.join(tempfile.mkdtemp(), "a.db")); clk = FakeClock(T + 60)
    eng = Engine(cfg, db, clk, FakeLive(T), FakePoly(cfg), sleep=make_sleep(clk))
    eng.state.update(round={"round_id": f"btc-updown-5m-{T}", "start_ts": T, "end_ts": T + 300, "decision_ts_target": T + 90}, phase="ANALYZING",
                     prelim={"p": 0.83, "probs": {"TREND": 0.9}, "info": {}, "f": {}}, open={}, crowd=[])
    live = eng.live; live.composite = lambda: (60000.0, "chainlink")
    monkeypatch.setattr(A, "clock", clk); A.G.update(eng=eng, live=live, started=time.time())
    v = A.round_view()
    assert v["prediction"] is None
    s = json.dumps(v)
    assert "0.83" not in s and "0.9" not in s
    assert all(a.get("direction") is None and a.get("p_up") is None for a in v["agents"].values())
