"""Integration test on the REAL stored history (skipped if no history has been fetched)."""
import os, numpy as np, pytest
from btc5.db import DB
from btc5 import backtest as BT
REAL = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "btc5.db")

@pytest.mark.skipif(not os.path.exists(REAL), reason="no real history")
def test_walk_forward_is_out_of_sample_and_beats_random():
    db = DB(REAL)
    ds = BT.build_dataset(db)[-1200:]
    assert len(ds) >= 600
    assert all(a["T"] < b["T"] for a, b in zip(ds[:-1], ds[1:]))            # strictly time-ordered
    oos, agent_oos, start, folds = BT.walk_forward(ds, archs=("stack",))
    for f in folds:                                                        # every fold trains only on rounds before its test window
        assert ds[f["train_n"] - 1]["T"] < f["test_from"]
    y = np.array([d["y"] for d in ds[start:]]); p = oos["stack"][start:]
    assert not np.isnan(p).any()
    acc = ((p >= 0.5) == y).mean(); n = len(y)
    assert acc - 1.96 * np.sqrt(0.25 / n) > 0.5, f"ensemble OOS accuracy {acc:.3f} not distinguishable from random (n={n})"
