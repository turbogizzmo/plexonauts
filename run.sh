cd "$HOME/plexonauts" || exit 1
PIDF=run.pid
if [ -f "$PIDF" ]; then
  OLD=$(cat "$PIDF")
  if kill -0 "$OLD" 2>/dev/null; then
    kill "$OLD" 2>/dev/null
    for _ in $(seq 1 20); do kill -0 "$OLD" 2>/dev/null || break; sleep 0.5; done
    kill -9 "$OLD" 2>/dev/null
  fi
  rm -f "$PIDF"
fi
for _ in $(seq 1 20); do ss -tln 2>/dev/null | grep -q ":8787 " || break; sleep 0.5; done
if ss -tln 2>/dev/null | grep -q ":8787 "; then echo "  port 8787 still busy, aborting"; exit 1; fi
setsid nohup python3 server.py > /tmp/act.log 2>&1 &
echo $! > "$PIDF"
for _ in $(seq 1 30); do ss -tln 2>/dev/null | grep -q ":8787 " && break; sleep 0.5; done
if ss -tln 2>/dev/null | grep -q ":8787 "; then
  echo "  up: pid $(cat $PIDF), listening on 8787"
else
  echo "  FAILED to come up:"; sed 's/^/    /' /tmp/act.log | tail -12; exit 1
fi
