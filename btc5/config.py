"""BTC-5 configuration. All values can be overridden by environment variables (BTC5_*). No secrets are used or stored."""
import os
from dataclasses import dataclass

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)

def _f(n, d): return float(os.environ.get(n, d))
def _i(n, d): return int(os.environ.get(n, d))

@dataclass
class Config:
    db_path: str = os.environ.get("BTC5_DB", os.path.join(ROOT, "data", "btc5.db"))
    port: int = _i("BTC5_PORT", 8700)
    round_seconds: int = 300
    decision_offset_s: float = 90.0             # T90
    # A decision event that fires later than this after T90 (box paused / process down) is recorded as MISSED, never back-dated.
    late_tolerance_ms: float = _f("BTC5_LATE_TOLERANCE_MS", 2000)
    resolution_timeout_s: float = _f("BTC5_RESOLUTION_TIMEOUT_S", 3600)
    feed_stale_s: float = _f("BTC5_FEED_STALE_S", 5)
    gamma_url: str = "https://gamma-api.polymarket.com"
    clob_url: str = "https://clob.polymarket.com"
    series_slug: str = "btc-up-or-down-5m"
    slug_prefix: str = "btc-updown-5m-"
    binance_rest: str = "https://data-api.binance.vision"   # binance.com REST is geo-blocked (HTTP 451) from this host; the official public-data mirror works
    backfill_days: float = _f("BTC5_BACKFILL_DAYS", 10)
    retrain_every_rounds: int = _i("BTC5_RETRAIN_EVERY", 12)
    promotion_min_rounds: int = _i("BTC5_PROMOTION_MIN_ROUNDS", 300)
    live_feature_min_samples: int = _i("BTC5_LIVE_FEATURE_MIN", 300)

CONFIDENCE_ORDER = ["VERY LOW", "LOW", "MODERATE", "HIGH", "VERY HIGH"]

def confidence_class(p_up):
    """Bucket by calibrated distance from 0.5. Empirical accuracy for each bucket (from walk-forward + live) is reported next to it."""
    d = abs(p_up - 0.5)
    if d < 0.05: return "VERY LOW"
    if d < 0.12: return "LOW"
    if d < 0.22: return "MODERATE"
    if d < 0.35: return "HIGH"
    return "VERY HIGH"
