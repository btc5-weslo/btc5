"""Clock synchronisation: estimates offset of local wall clock vs. exchange time servers.
All scheduling uses corrected wall time to compute targets and the monotonic clock to wait."""
import time, statistics, email.utils
import httpx

class SyncedClock:
    def __init__(self, max_offset_ms=1000.0, max_age_s=900):
        self.offset = 0.0  # seconds to ADD to local time
        self.last_sync_mono = None
        self.samples = []
        self.max_offset_ms = max_offset_ms
        self.max_age_s = max_age_s
        self.error = None

    def now(self) -> float:
        return time.time() + self.offset

    def mono(self) -> float:
        return time.monotonic()

    @property
    def synced(self) -> bool:
        if self.last_sync_mono is None: return False
        if time.monotonic() - self.last_sync_mono > self.max_age_s: return False
        hi = [x["offset_ms"] for x in self.samples if "kraken" not in x["source"]]
        if not hi: return False
        return (max(hi) - min(hi)) <= self.max_offset_ms

    async def sync(self, client: httpx.AsyncClient):
        samples = []
        async def probe(url, parse):
            t0 = time.time(); r = await client.get(url, timeout=5); t1 = time.time()
            r.raise_for_status()
            server = parse(r)
            samples.append((url, server - (t0 + t1) / 2, (t1 - t0)))
        probes = [
            ("https://api.exchange.coinbase.com/time", lambda r: float(r.json()["epoch"])),
            ("https://api.kraken.com/0/public/Time", lambda r: float(r.json()["result"]["unixtime"]) + 0.5),  # 1s resolution
            ("https://api.binance.us/api/v3/time", lambda r: r.json()["serverTime"] / 1000.0),
        ]
        for url, parse in probes:
            try: await probe(url, parse)
            except Exception as e: self.error = f"{url}: {e}"
        if not samples:
            return False
        # prefer high-resolution sources (coinbase/binance); median
        hi = [s for s in samples if "kraken" not in s[0]] or samples
        self.offset = statistics.median([s[1] for s in hi])
        self.samples = [{"source": s[0].split('/')[2], "offset_ms": round(s[1]*1000, 1), "rtt_ms": round(s[2]*1000, 1)} for s in samples]
        self.last_sync_mono = time.monotonic()
        return True

    def status(self):
        return {"offset_ms": round(self.offset*1000, 1), "synced": self.synced, "samples": self.samples,
                "age_s": None if self.last_sync_mono is None else round(time.monotonic()-self.last_sync_mono, 1),
                "error": self.error}

    def drift_check(self):
        """Clock drift / disagreement detection between time sources."""
        if not self.samples: return {"ok": False, "reason": "no time samples"}
        offs = [s["offset_ms"] for s in self.samples if "kraken" not in s["source"]]
        spread = (max(offs) - min(offs)) if offs else None
        ok = self.synced and abs(self.offset * 1000) < 1000
        return {"ok": ok, "offset_ms": round(self.offset * 1000, 1), "source_spread_ms": spread}
