"""Polymarket integration: strict market identification (Gamma), CLOB books, official resolution, Chainlink open/close."""
import json
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
import httpx

UA = {"User-Agent": "Mozilla/5.0 (BTC-5 research client)", "Accept-Encoding": "gzip"}

def iso_ts(s: str) -> float:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()

def round_start_for(t: float, rs=300) -> int:
    return int(t // rs * rs)

class MarketIdentificationError(Exception):
    pass

@dataclass
class MarketInfo:
    slug: str; market_id: str; condition_id: str; question: str
    start_ts: int; end_ts: int; up_token: str; down_token: str
    resolution_source: str; rules: str; accepting_orders: bool
    # trading metadata straight from Gamma (defaults only for older callers/tests)
    fee_rate: float = 0.0; fee_exponent: float = 1.0; order_min_size: float = 0.0; tick_size: str = "0.01"; neg_risk: bool = False
    def to_dict(self): return asdict(self)

def validate_market(m: dict, expected_start: int, cfg) -> MarketInfo:
    """Accept ONLY the BTC 5-minute Up/Down market for this exact start time. Any mismatch raises; never substitutes another market."""
    slug = m.get("slug", "")
    if slug != f"{cfg.slug_prefix}{expected_start}":
        raise MarketIdentificationError(f"slug mismatch: {slug}")
    ev = (m.get("events") or [{}])[0]
    series = [s.get("slug") for s in ev.get("series", []) or []]
    if series and cfg.series_slug not in series:
        raise MarketIdentificationError(f"series mismatch: {series}")
    start = m.get("eventStartTime") or ev.get("startTime")
    if not start or int(iso_ts(start)) != expected_start:
        raise MarketIdentificationError(f"opening timestamp mismatch: {start} vs {expected_start}")
    end = int(iso_ts(m["endDate"]))
    if end != expected_start + cfg.round_seconds:
        raise MarketIdentificationError(f"expiration mismatch: {m['endDate']}")
    outcomes = json.loads(m["outcomes"]) if isinstance(m["outcomes"], str) else m["outcomes"]
    tokens = json.loads(m["clobTokenIds"]) if isinstance(m["clobTokenIds"], str) else m["clobTokenIds"]
    if [o.lower() for o in outcomes] != ["up", "down"] or len(tokens) != 2:
        raise MarketIdentificationError(f"outcomes/tokens invalid: {outcomes}")
    q = m.get("question", "")
    if "bitcoin" not in q.lower():
        raise MarketIdentificationError("question is not about Bitcoin")
    desc = m.get("description", "")
    src = m.get("resolutionSource", "")
    if "chain" not in (src + desc).lower():
        raise MarketIdentificationError(f"unexpected resolution source: {src!r}")
    return MarketInfo(slug=slug, market_id=str(m["id"]), condition_id=m.get("conditionId", ""), question=q,
                      start_ts=expected_start, end_ts=end, up_token=tokens[0], down_token=tokens[1],
                      resolution_source=src, rules=desc, accepting_orders=bool(m.get("acceptingOrders")),
                      fee_rate=float((m.get("feeSchedule") or {}).get("rate") or 0) if m.get("feesEnabled") else 0.0,
                      fee_exponent=float((m.get("feeSchedule") or {}).get("exponent") or 1), order_min_size=float(m.get("orderMinSize") or 0),
                      tick_size=str(m.get("orderPriceMinTickSize") or "0.01"), neg_risk=bool(m.get("negRisk")))

def parse_resolution(m: dict):
    """'UP'/'DOWN' only when Polymarket shows the market closed with a definitive 1/0 payout; otherwise None (not yet official)."""
    if not m or not m.get("closed"):
        return None
    if m.get("umaResolutionStatus") not in (None, "", "resolved"):
        return None
    try:
        prices = [float(x) for x in json.loads(m["outcomePrices"])]
        outcomes = [o.lower() for o in json.loads(m["outcomes"])]
    except Exception:
        return None
    if sorted(prices) != [0.0, 1.0] or len(outcomes) != 2 or sorted(outcomes) != ["down", "up"]:
        return None
    return outcomes[prices.index(1.0)].upper()   # mapped by outcome NAME, never by position

class Polymarket:
    def __init__(self, client: httpx.AsyncClient, cfg):
        self.c = client; self.cfg = cfg

    async def market_raw(self, start_ts: int):
        slug = f"{self.cfg.slug_prefix}{start_ts}"
        for params in ({"slug": slug}, {"slug": slug, "closed": "true"}):
            r = await self.c.get(f"{self.cfg.gamma_url}/markets", params=params, headers=UA, timeout=8)
            r.raise_for_status(); d = r.json()
            if d: return d[0]
        return None

    async def discover(self, start_ts: int) -> MarketInfo:
        m = await self.market_raw(start_ts)
        if not m: raise MarketIdentificationError(f"no Polymarket market for {self.cfg.slug_prefix}{start_ts}")
        return validate_market(m, start_ts, self.cfg)

    async def book(self, token_id: str):
        r = await self.c.get(f"{self.cfg.clob_url}/book", params={"token_id": token_id}, headers=UA, timeout=5)
        r.raise_for_status(); b = r.json()
        bids = sorted(((float(x["price"]), float(x["size"])) for x in b.get("bids", [])), key=lambda x: -x[0])
        asks = sorted(((float(x["price"]), float(x["size"])) for x in b.get("asks", [])), key=lambda x: x[0])
        bb = bids[0][0] if bids else None; ba = asks[0][0] if asks else None
        mid = (bb + ba) / 2 if bb is not None and ba is not None else None
        return {"best_bid": bb, "best_ask": ba, "mid": mid, "spread": None if mid is None else round(ba - bb, 4),
                "bid_usd": round(sum(p * s for p, s in bids[:10]), 2), "ask_usd": round(sum(p * s for p, s in asks[:10]), 2)}

    async def ask_ladder(self, token_id: str, depth: int = 30):
        """Real ascending ask ladder [(price, size)] for one outcome token from the Polymarket CLOB."""
        r = await self.c.get(f"{self.cfg.clob_url}/book", params={"token_id": token_id}, headers=UA, timeout=4)
        r.raise_for_status(); b = r.json()
        return sorted(((float(x["price"]), float(x["size"])) for x in b.get("asks", [])), key=lambda x: x[0])[:depth]

    async def chainlink_open_close(self, start_ts: int):
        """Polymarket's display of Chainlink BTC/USD SPOT values at the round's start and end. Reference/display only:
        the contract settles on the Chainlink BTC/USD 60-second TWAP stream (end-of-window TWAP >= start value), so
        close < open with an official UP (or the reverse) is legitimate. Grading ALWAYS uses the Gamma outcome."""
        f = lambda t: datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        r = await self.c.get("https://polymarket.com/api/crypto/crypto-price", headers=UA, timeout=6,
                             params={"symbol": "BTC", "eventStartTime": f(start_ts), "variant": "fiveminute", "endDate": f(start_ts + self.cfg.round_seconds)})
        r.raise_for_status(); return r.json()

    async def resolution(self, start_ts: int):
        m = await self.market_raw(start_ts)
        if not m: return None, None
        return parse_resolution(m), {k: m.get(k) for k in ("closed", "outcomes", "outcomePrices", "umaResolutionStatus", "closedTime", "resolutionSource")}

    async def price_history(self, token: str, start: int, end: int):
        r = await self.c.get(f"{self.cfg.clob_url}/prices-history", headers=UA, timeout=8,
                             params={"market": token, "startTs": start, "endTs": end, "fidelity": 1})
        r.raise_for_status(); return r.json().get("history", [])
