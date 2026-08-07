#!/usr/bin/env bash
# Retired OCR cron wrapper. New China MWR collection is handled by
# .github/workflows/china_mwr_api.yml and writes only to data/china/mwr_api/.
echo "This OCR collector was retired on 2026-07-24. Use scrapers/china/mwr_api/china_mwr_api_scraper.py." >&2
exit 1

# Historical implementation retained below for audit only.
#
# Expected env:
#   VENV_PYTHON  (default: $HOME/venvs/mwr/bin/python)
#   REPO_DIR     (default: $HOME/global-reservoir-scrapers)

set -euo pipefail

VENV_PYTHON="${VENV_PYTHON:-$HOME/venvs/mwr/bin/python}"
REPO_DIR="${REPO_DIR:-$HOME/global-reservoir-scrapers}"
DATA_DIR="$REPO_DIR/data/china/mwr_ocr_archive"
SCRIPT_DIR="$REPO_DIR/scrapers/china/mwr_ocr_archive"
LOG_DIR="$REPO_DIR/data/china/mwr_ocr_archive/run_logs"

mkdir -p "$LOG_DIR"
TS=$(date -u +%Y%m%dT%H%M%SZ)
LOG_FILE="$LOG_DIR/cron_${TS}.log"

exec >>"$LOG_FILE" 2>&1

echo "=========================================="
echo "Run started: $(date -u +%FT%TZ)"
echo "=========================================="

cd "$REPO_DIR"

echo "==> git pull --rebase"
git pull --rebase --autostash || {
    echo "git pull failed — aborting this run" >&2
    exit 1
}

echo "==> Running scraper (MWR_HEADLESS=1)"
OUTPUT_DIR="$DATA_DIR" MWR_HEADLESS=1 "$VENV_PYTHON" "$SCRIPT_DIR/grab_data.py"

echo "==> Committing outputs"
cd "$REPO_DIR"
git add "data/china/mwr_ocr_archive/" || true
if git diff --cached --quiet; then
    echo "No changes to commit."
else
    TODAY=$(date -u +%F)
    git commit -m "data(china/mwr): daily OCR ${TODAY}

Co-Authored-By: MWR Oracle Bot <mwr-oracle-bot@andyzeng.noreply>"
    for attempt in 1 2 3; do
        if git push; then
            echo "Push succeeded on attempt $attempt."
            break
        fi
        echo "Push failed (attempt $attempt), pulling and retrying..." >&2
        git pull --rebase --autostash
        sleep $((attempt * 5))
    done
fi

echo "Run finished: $(date -u +%FT%TZ)"
