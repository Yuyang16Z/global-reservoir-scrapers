#!/usr/bin/env bash
# Bootstrap script for Oracle Cloud Always Free VM (Ubuntu 24.04 ARM64).
#
# What it does:
#  1. Installs system packages: Python, Chromium, ChromeDriver, git, cron
#  2. Creates a Python virtualenv at ~/venvs/mwr and installs requirements
#  3. Does NOT touch git credentials — run setup_git_deploy_key.sh for that
#
# Usage on a fresh VM (assumes you're already SSH'd in as `ubuntu`):
#   sudo bash scrapers/china/mwr/setup_oracle_vm.sh
#
# Idempotent — safe to re-run.

set -euo pipefail

if [[ $EUID -ne 0 ]]; then
    echo "This script needs sudo (installs system packages)." >&2
    exec sudo bash "$0" "$@"
fi

TARGET_USER="${SUDO_USER:-ubuntu}"
TARGET_HOME=$(getent passwd "$TARGET_USER" | cut -d: -f6)
VENV_DIR="$TARGET_HOME/venvs/mwr"

echo "==> Updating apt cache"
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq

echo "==> Installing system packages"
apt-get install -y --no-install-recommends \
    python3 python3-venv python3-dev python3-pip \
    chromium-browser chromium-chromedriver \
    git cron curl ca-certificates \
    libgl1 libglib2.0-0 fonts-noto-cjk \
    build-essential

# Confirm chromedriver and chromium are present and ARM64
echo "==> Chromium / ChromeDriver versions"
chromium-browser --version || true
chromedriver --version || true

echo "==> Creating Python venv at $VENV_DIR"
sudo -u "$TARGET_USER" python3 -m venv "$VENV_DIR"

echo "==> Installing Python requirements"
REPO_DIR=$(cd "$(dirname "$0")/../../.." && pwd)
sudo -u "$TARGET_USER" "$VENV_DIR/bin/pip" install --upgrade pip wheel
sudo -u "$TARGET_USER" "$VENV_DIR/bin/pip" install -r "$REPO_DIR/scrapers/china/mwr/requirements.txt"

echo "==> Done."
echo
echo "Next steps:"
echo "  1. bash $REPO_DIR/scrapers/china/mwr/setup_git_deploy_key.sh"
echo "  2. Test: OUTPUT_DIR=$REPO_DIR/data/china/mwr \\"
echo "           $VENV_DIR/bin/python $REPO_DIR/scrapers/china/mwr/grab_data.py"
echo "  3. Add cron entry (see scrapers/china/mwr/README.md)."
