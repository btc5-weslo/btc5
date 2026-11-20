#!/usr/bin/env bash
# BTC-5 launcher (idempotent). Starts, only if not already running:
#   1) supervisor loop  -> runs uvicorn on :8700 (pid in data/server.pid), restarts it 3 s after any exit
#   2) health watchdog  -> every 20 s probes /api/health; after 3 consecutive failures kills the uvicorn pid (supervisor restarts it)
#   3) pusher loop      -> uploads the phone-view snapshot to the HF bucket every ~10 s; restarts on exit (60 s watchdog inside; FALLBACK role: stands down while the PC feed is fresh)
# Logs: data/server.log data/supervisor.log data/watchdog.log data/pusher.log      Stop: ./stop.sh     Restart server only: ./restart-server.sh
cd "$(dirname "$0")"; mkdir -p data
PORT="${BTC5_PORT:-8700}"
alive() { [ -f "data/$1.pid" ] && kill -0 "$(cat "data/$1.pid")" 2>/dev/null; }
start() { # name, command
  if alive "$1"; then [ -n "$QUIET" ] || echo "$1 already running (pid $(cat data/$1.pid))"; return; fi
  setsid nohup bash -c "$2" >> "data/$1.log" 2>&1 < /dev/null &
  echo $! > "data/$1.pid"; echo "$1 started (pid $!)"
}
[ -x .venv/bin/uvicorn ] || { echo "venv missing: python3 -m venv .venv && .venv/bin/pip install -r requirements.txt"; exit 1; }
start supervisor "while true; do echo \"\$(date '+%F %T') starting server\"; BTC5_PORT=$PORT .venv/bin/uvicorn btc5.app:app --host 0.0.0.0 --port $PORT --no-access-log >> data/server.log 2>&1 & echo \$! > data/server.pid; wait \$!; rc=\$?; echo \"\$(date '+%F %T') server exited (rc=\$rc), restarting in 3s\"; sleep 3; done"
start watchdog "fails=0; sleep 60; while true; do if curl -sf -m 10 http://127.0.0.1:$PORT/api/health > /dev/null; then fails=0; else fails=\$((fails+1)); echo \"\$(date '+%F %T') health check failed (\$fails)\"; fi; if [ \$fails -ge 3 ]; then echo \"\$(date '+%F %T') server unresponsive - killing pid \$(cat data/server.pid) for restart\"; p=\$(cat data/server.pid); kill \$p; sleep 5; kill -0 \$p 2>/dev/null && kill -9 \$p; fails=0; sleep 30; fi; sleep 20; done"
start pusher "while true; do BTC5_FEED_ROLE=fallback BTC5_FEED_SOURCE=box .venv/bin/python pusher.py; rc=\$?; echo \"\$(date '+%F %T') pusher exited (rc=\$rc), restarting in 5s\"; sleep 5; done"
