"""Clock synchronisation: estimates offset of local wall clock vs. exchange time servers.
All scheduling uses corrected wall time to compute targets and the monotonic clock to wait.

Each source is probed several times and only the minimum-RTT sample is kept (Cristian's
algorithm). Probes run in a worker thread so a busy asyncio loop (common on a 0.1 CPU
host while a training job is running) cannot inflate t1 and produce a multi-second
"RTT" that is really scheduling delay. A clock is marked synced only when at least one
high-resolution sample has a tight RTT; agreement between sources is judged against
their RTT error bounds. The offset is never invented: it is the tightest real sample.
"""

import asyncio, json, os, time, urllib.request

# High-resolution sources first. Kraken reports unix time at 1 s granularity (mid-second).
PROBES = (
    ("https://api.exchange.coinbase.com/time", "json_epoch"),
    ("https://api.binance.us/api/v3/time", "binance"),
    ("https://api.kraken.com/0/public/Time", "kraken"),
)

# Default ceiling for a sample to count as "tight". Wider than a quiet LAN RTT, tight
# enough that the leftover uncertainty stays comparable to the 2 s late-lock tolerance.
DEFAULT_MAX_RTT_MS = 2500.0
DEFAULT_ATTEMPTS = 3
DEFAULT_TIMEOUT_S = 8.0


def _parse(kind, body):
    if kind == "json_epoch":
        return float(json.loads(body)["epoch"])
    if kind == "binance":
        return json.loads(body)["serverTime"] / 1000.0
    if kind == "kraken":
        return float(json.loads(body)["result"]["unixtime"]) + 0.5
    raise ValueError(kind)


def probe_once(url, kind, timeout=DEFAULT_TIMEOUT_S):
    """Synchronous Cristian probe: offset = server - (t0+t1)/2, rtt = t1-t0.

    Kept synchronous (and intended to run in a thread) so t0/t1 are not delayed by
    a saturated asyncio event loop. Returns (offset_s, rtt_s).
    """
    t0 = time.time()
    req = urllib.request.Request(url, headers={"User-Agent": "btc5-clock"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        body = r.read()
    t1 = time.time()
    server = _parse(kind, body)
    return server - (t0 + t1) / 2, (t1 - t0)


class SyncedClock:
    def __init__(self, max_offset_ms=1000.0, max_age_s=900, max_rtt_ms=None, attempts=None):
        self.offset = 0.0  # seconds to ADD to local time
        self.last_sync_mono = None
        self.samples = []
        self.max_offset_ms = max_offset_ms
        self.max_age_s = max_age_s
        self.max_rtt_ms = float(os.environ.get("BTC5_CLOCK_MAX_RTT_MS", max_rtt_ms if max_rtt_ms is not None else DEFAULT_MAX_RTT_MS))
        self.attempts = int(os.environ.get("BTC5_CLOCK_ATTEMPTS", attempts if attempts is not None else DEFAULT_ATTEMPTS))
        # Stop retrying a source once a probe is this tight (further retries would barely help).
        self.good_rtt_ms = min(400.0, self.max_rtt_ms)
        self.error = None

    def now(self) -> float:
        return time.time() + self.offset

    def mono(self) -> float:
        return time.monotonic()

    def _hi(self):
        return [x for x in self.samples if "kraken" not in x["source"]]

    def _tight(self):
        return [x for x in self._hi() if x["rtt_ms"] <= self.max_rtt_ms]

    @property
    def synced(self) -> bool:
        if self.last_sync_mono is None:
            return False
        if time.monotonic() - self.last_sync_mono > self.max_age_s:
            return False
        tight = self._tight()
        if not tight:
            return False
        # Pairwise consistency: measured offsets must agree within the base tolerance
        # plus each sample's Cristian error bound (RTT/2). Slow leftover samples in
        # `_tight` therefore cannot look like a 2 s disagreement when the disagreement
        # is smaller than the measurement uncertainty.
        for i, a in enumerate(tight):
            for b in tight[i + 1:]:
                spread = abs(a["offset_ms"] - b["offset_ms"])
                bound = self.max_offset_ms + (a["rtt_ms"] + b["rtt_ms"]) / 2
                if spread > bound:
                    return False
        return True

    def _best_offset(self, samples):
        """Offset from the minimum-RTT high-resolution sample. Fallback: any sample."""
        hi = [s for s in samples if "kraken" not in s[0]] or samples
        return min(hi, key=lambda s: s[2])[1]

    async def sync(self, client=None, probe=None):
        """Refresh samples. `client` is accepted for call-site compatibility (unused;
        probes use a worker thread). `probe` is (url, kind) -> (offset_s, rtt_s), for tests."""
        probe = probe or probe_once
        errors = []

        async def probe_source(url, kind):
            best = None
            last_err = None
            for _ in range(max(1, self.attempts)):
                try:
                    off, rtt = await asyncio.to_thread(probe, url, kind)
                    if best is None or rtt < best[2]:
                        best = (url, off, rtt)
                    if best[2] * 1000 <= self.good_rtt_ms:
                        break
                except Exception as e:
                    last_err = e
            if best is None and last_err is not None:
                errors.append(f"{url.split('/')[2]}: {last_err}")
            return best

        results = await asyncio.gather(*[probe_source(url, kind) for url, kind in PROBES])
        samples = [s for s in results if s]
        self.error = errors[-1] if errors else None
        if not samples:
            return False
        self.offset = self._best_offset(samples)
        self.samples = [{"source": s[0].split("/")[2], "offset_ms": round(s[1] * 1000, 1), "rtt_ms": round(s[2] * 1000, 1)} for s in samples]
        self.last_sync_mono = time.monotonic()
        return True

    def status(self):
        tight = self._tight()
        return {"offset_ms": round(self.offset * 1000, 1), "synced": self.synced, "samples": self.samples,
                "age_s": None if self.last_sync_mono is None else round(time.monotonic() - self.last_sync_mono, 1),
                "error": self.error,
                "best_rtt_ms": min((s["rtt_ms"] for s in self.samples), default=None),
                "tight_samples": len(tight)}

    def drift_check(self):
        """Clock disagreement detection. A large *offset* is not a failure (that is what
        the offset is for); failure means we do not have a tight, consistent sync."""
        if not self.samples:
            return {"ok": False, "reason": "no time samples"}
        src = self._tight() or self._hi()
        offs = [s["offset_ms"] for s in src]
        spread = (max(offs) - min(offs)) if offs else None
        return {"ok": self.synced, "offset_ms": round(self.offset * 1000, 1), "source_spread_ms": spread,
                "best_rtt_ms": min((s["rtt_ms"] for s in self.samples), default=None)}
