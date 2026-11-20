"""Clock sync on a slow/jittery host: min-RTT selection, RTT-aware agreement, no fake sync."""
import asyncio, time
from btc5.clock import SyncedClock


def _sample(clk, source, offset_ms, rtt_ms):
    return {"source": source, "offset_ms": offset_ms, "rtt_ms": rtt_ms}


def test_high_rtt_samples_are_not_synced():
    """The boot-time Render case: 4-9 s RTTs whose 2 s offset spread is just measurement noise."""
    c = SyncedClock()
    c.last_sync_mono = time.monotonic()
    c.samples = [
        _sample(c, "api.exchange.coinbase.com", 3528.5, 9142.1),
        _sample(c, "api.kraken.com", 1504.2, 5202.4),
        _sample(c, "api.binance.us", 1439.9, 4206.3),
    ]
    c.offset = 0.14399
    assert c.synced is False
    assert c.drift_check()["ok"] is False


def test_tight_min_rtt_sample_is_synced_even_if_a_sibling_is_slow():
    c = SyncedClock()
    c.last_sync_mono = time.monotonic()
    c.samples = [
        _sample(c, "api.exchange.coinbase.com", 80.0, 180.0),
        _sample(c, "api.binance.us", 120.0, 4000.0),  # too slow to count
    ]
    c.offset = 0.080
    assert c.synced is True
    assert c._tight() == [c.samples[0]]


def test_two_tight_samples_that_truly_disagree_are_not_synced():
    c = SyncedClock(max_offset_ms=1000)
    c.last_sync_mono = time.monotonic()
    c.samples = [
        _sample(c, "api.exchange.coinbase.com", 0.0, 80.0),
        _sample(c, "api.binance.us", 5000.0, 90.0),  # 5 s apart, both tight — real disagreement
    ]
    assert c.synced is False


def test_two_tight_samples_within_rtt_bounds_are_synced():
    c = SyncedClock(max_offset_ms=1000)
    c.last_sync_mono = time.monotonic()
    c.samples = [
        _sample(c, "api.exchange.coinbase.com", 200.0, 400.0),
        _sample(c, "api.binance.us", 900.0, 500.0),
    ]
    assert c.synced is True


def test_sync_keeps_min_rtt_sample_and_uses_its_offset():
    """Coinbase: slow then tight (stop early). Binance slightly slower. Offset comes from the 50 ms probe."""
    def probe(url, kind):
        if "coinbase" in url:
            probe.cb = getattr(probe, "cb", 0) + 1
            return {1: (0.400, 0.900), 2: (0.012, 0.050), 3: (0.080, 0.200)}[probe.cb]
        if "binance" in url:
            return (0.020, 0.080)
        if "kraken" in url:
            return (0.500, 0.300)
        raise AssertionError(url)

    c = SyncedClock(attempts=3, max_rtt_ms=2500)
    assert asyncio.run(c.sync(probe=probe)) is True
    assert c.synced is True
    # min-RTT high-res sample is coinbase 50 ms at +12 ms (tighter than binance 80 ms)
    assert c.offset == 0.012
    by = {s["source"]: s for s in c.samples}
    assert by["api.exchange.coinbase.com"]["rtt_ms"] == 50.0
    assert by["api.binance.us"]["rtt_ms"] == 80.0
    # coinbase stopped after the tight second attempt
    assert probe.cb == 2


def test_sync_retries_when_first_attempts_are_slow():
    def probe(url, kind):
        if "coinbase" in url:
            probe.cb = getattr(probe, "cb", 0) + 1
            # two slow tries, then a tight one
            return {1: (0.300, 4.000), 2: (0.280, 3.500), 3: (0.015, 0.120)}[probe.cb]
        if "binance" in url:
            return (0.018, 0.250)
        return (0.0, 0.200)

    c = SyncedClock(attempts=3, max_rtt_ms=2500)
    assert asyncio.run(c.sync(probe=probe)) is True
    assert probe.cb == 3
    assert c.offset == 0.015
    assert c.synced is True


def test_sync_returns_false_when_every_probe_fails():
    def probe(url, kind):
        raise OSError("offline")

    c = SyncedClock(attempts=2)
    assert asyncio.run(c.sync(probe=probe)) is False
    assert c.synced is False
    assert c.samples == []
    assert c.error and "offline" in c.error


def test_stale_samples_are_not_synced():
    c = SyncedClock(max_age_s=1)
    c.last_sync_mono = time.monotonic() - 5
    c.samples = [_sample(c, "api.binance.us", 10.0, 80.0)]
    assert c.synced is False
