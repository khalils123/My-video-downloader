#!/bin/bash
# One-command deploy: pulls the latest app.py/worker.py/requirements.txt from
# GitHub, installs any new dependencies, restarts services, and verifies
# health. Run this from /opt/vidapp on the VPS.
set -e

APP_DIR="/opt/vidapp"
REPO_RAW="https://raw.githubusercontent.com/khalils123/My-video-downloader/main"
HEALTH_URL="http://127.0.0.1:8000/healthz"

cd "$APP_DIR"

echo "==> Backing up current app.py"
cp app.py "app.py.bak.$(date +%Y%m%d%H%M%S)" 2>/dev/null || true

echo "==> Pulling latest code"
curl -fsS -o app.py "$REPO_RAW/app.py"
curl -fsS -o worker.py "$REPO_RAW/worker.py"
curl -fsS -o requirements.txt "$REPO_RAW/requirements.txt"

echo "==> Installing any new dependencies"
"$APP_DIR/venv/bin/pip" install -q -r requirements.txt

echo "==> Restarting services"
systemctl restart vidapp-worker@1 vidapp-worker@2 vidapp.service

sleep 2

echo "==> Health check"
if curl -fsS "$HEALTH_URL" | python3 -c "import sys,json; sys.exit(0 if json.load(sys.stdin).get('status')=='ok' else 1)"; then
  echo "Deploy successful — service healthy."
else
  echo "WARNING: health check did not report healthy. Run: systemctl status vidapp.service vidapp-worker@1 vidapp-worker@2"
  exit 1
fi

echo "==> Trimming old backups (keeping the last 5)"
ls -t app.py.bak.* 2>/dev/null | tail -n +6 | xargs -r rm --

echo "Done."
