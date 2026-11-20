"""Heavy job bodies that must be importable WITHOUT importing the FastAPI app (they run in btc5.train_child)."""
import json, time


def backtest_job(days):
    from .config import Config
    from .db import DB
    from . import backtest as BT
    from .config import low_mem
    import os
    d = DB(Config().db_path)
    if low_mem():   # 512 MB host: same caps as the training job (no GBDT, capped rounds)
        res = BT.run(d, days if days is not None else float(os.environ.get("BTC5_TRAIN_DAYS", "4")), archs=("stack", "lr"),
                     max_rounds=int(os.environ.get("BTC5_TRAIN_MAX_ROUNDS", "800")), step_frac=float(os.environ.get("BTC5_TRAIN_STEP_FRAC", "0.2")))
    else:
        res = BT.run(d, days)
    if "error" in res: return res["error"]
    d.x("INSERT INTO backtests(ts,params_json,results_json) VALUES (?,?,?)", (time.time(), json.dumps({"days": days, "trigger": "manual"}), json.dumps(res, default=str)))
    return f"{res['n_test']} OOS rounds, ensemble accuracy {res['models']['stack']['accuracy']}"
