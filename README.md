# POLYMARKET BTC-5 — Render free deployment

Exactly one locked UP/DOWN prediction per Polymarket BTC 5-minute round, published at **T0 + 90 s**,
graded against the official Polymarket (Chainlink 60-second TWAP) resolution. Automatic **DEMO**
trading (paper money at real Polymarket CLOB prices) runs every round by default. The process
**always boots in DEMO**; LIVE trading is never resumed after a restart.

This repo is the Render free web service package. The phone dashboard is the same interactive UI
served at `/`.

## Health

`GET /api/health` — Render `healthCheckPath`. Returns HTTP 200 once the process can accept traffic
(including a `{"ok": false, "starting": true}` body if the engine is still coming up).

## Environment

Set in the Render dashboard (or via `render.yaml`). **Never put values for secrets in git.**

| Variable | Required | Purpose |
|---|---|---|
| `PORT` | set by Render | Listen port (default 10000) |
| `RENDER_EXTERNAL_URL` | set by Render | Public URL; the app pings `{url}/api/health` every 10 minutes so the free instance does not sleep after 15 minutes idle |
| `HF_TOKEN` | for persistence + phone feed | Write token for bucket `weslo00700/btc5-feed`. Without it the app still runs; DB backup and `latest.json` push stay idle |
| `BTC5_FEED_SOURCE` | `cloud` | Phone-feed writer identity (priority: cloud > PC > box) |
| `BTC5_FEED_ROLE` | `primary` | Cloud is the highest-priority feed writer |
| `BTC5_DB_BACKUP` | `1` | Restore on start, backup every `BTC5_BACKUP_EVERY` seconds (default 600) and on shutdown |
| `BTC5_ADMIN_TOKEN` | recommended on a public URL | Passcode required (`X-BTC5-Token`) for trading settings/mode changes. Emergency stop never needs it |
| `POLYMARKET_PRIVATE_KEY`, `POLYMARKET_FUNDER`, `POLYMARKET_SIGNATURE_TYPE` | only for LIVE | Read from env / Render secrets only. Do not paste into chat or the repo. Boot stays DEMO until you confirm LIVE in the UI |
| `BTC5_LIVE_MAX_STAKE_USD` | optional | Hard cap for live stake |

`BTC5_READONLY` is **not** set, so the phone UI is interactive. Anyone who finds the public URL can
change non-trading settings. Set `BTC5_ADMIN_TOKEN` to lock trading controls.

## Persistence

Render's disk is ephemeral. On startup, if `data/btc5.db` is missing, `backup.restore()` downloads
`db/btc5.db.gz` from the public bucket (and will not overwrite a present local DB). When `HF_TOKEN`
is set, a consistent SQLite snapshot is gzipped and uploaded about every 10 minutes and again on
shutdown. A backup is refused if the remote copy holds **more** rounds than the local DB, so a
fresh empty container cannot wipe history.

## Keep-alive

Render free web services sleep after ~15 minutes with no inbound HTTP. A background task pings
`RENDER_EXTERNAL_URL/api/health` every 10 minutes. Failures are logged and never take the server down.

## Memory (512 MB / 0.1 CPU)

One Python process (uvicorn + in-process backup/pusher/keep-alive). BLAS/OpenMP threads are capped
at 1. Model training still runs in a one-worker `ProcessPoolExecutor` so a T90 lock is never delayed;
that child can briefly raise memory use. If the free instance OOMs during a train/backfill, it will
restart and restore the DB from the bucket.

## Local

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python -m pytest -q tests
PORT=10000 .venv/bin/python render_start.py
```

Do not point a local run at the production bucket unless you intend this process to become the
cloud feed writer.

## Trading safety

- Always boots in DEMO (`stake $20`, min confidence VERY LOW, daily loss $100, exposure $200, max entry 90¢, bankroll $1,000).
- LIVE requires wallet env vars + explicit UI confirmation of dollar limits.
- `POST /api/trading/emergency-stop` turns auto-trade off, reverts to DEMO, and cancels open CLOB orders.
- Wallet keys are never stored, logged, or returned by the API.
