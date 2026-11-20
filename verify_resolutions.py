#!/usr/bin/env python3
"""Re-verify every recorded official outcome (and the CORRECT/INCORRECT grade) against Polymarket Gamma.
Read-only by default; prints mismatches. Usage: python verify_resolutions.py [db_path] [--hist N]"""
import json, sqlite3, sys, time, httpx
from btc5.polymarket import parse_resolution, UA
db = sys.argv[1] if len(sys.argv) > 1 and not sys.argv[1].startswith("--") else "data/btc5.db"
hist_n = int(sys.argv[sys.argv.index("--hist") + 1]) if "--hist" in sys.argv else 0
c = sqlite3.connect(f"file:{db}?mode=ro", uri=True); c.row_factory = sqlite3.Row
cl = httpx.Client(headers=UA, timeout=15)
def gamma(T):
    for p in ({"slug": f"btc-updown-5m-{T}"}, {"slug": f"btc-updown-5m-{T}", "closed": "true"}):
        for _ in range(3):
            try: d = cl.get("https://gamma-api.polymarket.com/markets", params=p).json(); break
            except Exception: time.sleep(1)
        if d: return d[0]
    return None
rows = [dict(r) for r in c.execute("SELECT * FROM rounds WHERE official_outcome IS NOT NULL ORDER BY start_ts")]
bad = []; locked = [r for r in rows if r["status"] == "LOCKED"]
for r in rows:
    m = gamma(r["start_ts"]); g = parse_resolution(m) if m else None
    exp = None if r["status"] != "LOCKED" else ("CORRECT" if r["prediction"] == g else "INCORRECT")
    if g != r["official_outcome"] or (exp and exp != r["result"]):
        bad.append((r["round_id"], r["status"], r["prediction"], r["official_outcome"], g, r["result"], exp))
print(f"rounds with outcome: {len(rows)} (locked {len(locked)}: {sum(r['result']=='CORRECT' for r in locked)} CORRECT / {sum(r['result']=='INCORRECT' for r in locked)} INCORRECT)")
print("mismatches vs Gamma:", bad or "none")
if hist_n:
    hb = []
    hs = [dict(r) for r in c.execute("SELECT start_ts, outcome FROM hist_rounds WHERE outcome IS NOT NULL ORDER BY start_ts DESC LIMIT ?", (hist_n,))]
    for h in hs:
        m = gamma(h["start_ts"]); g = parse_resolution(m) if m else None
        if g != h["outcome"]: hb.append((h["start_ts"], h["outcome"], g))
    print(f"training labels checked: {len(hs)}; mismatches: {hb or 'none'}")
