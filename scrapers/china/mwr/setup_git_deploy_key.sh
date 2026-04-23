#!/usr/bin/env bash
# Configure git + GitHub deploy key on the Oracle VM so the cron job can push
# data/china/mwr/ back to the repo.
#
# Before running:
#   1. On your Mac: scp the private deploy key to the VM (see comment below)
#   2. On GitHub:   add the matching .pub to repo → Settings → Deploy keys
#                   ✓ Allow write access
#
# Usage (on Oracle VM, as the normal user):
#   bash scrapers/china/mwr/setup_git_deploy_key.sh
#
# Prereqs on the VM (before this script):
#   ~/.ssh/github_deploy_key     (private key, mode 600)
#   ~/.ssh/github_deploy_key.pub (optional)

set -euo pipefail

KEY_PATH="${KEY_PATH:-$HOME/.ssh/github_deploy_key}"
REPO_DIR="${REPO_DIR:-$HOME/global-reservoir-scrapers}"
GIT_REMOTE_SSH="git@github.com:Yuyang16Z/global-reservoir-scrapers.git"

if [[ ! -f "$KEY_PATH" ]]; then
    cat <<EOF >&2
Deploy key not found at $KEY_PATH.

On your Mac, run:
  scp -i ~/.ssh/oracle/oracle_mwr \\
      ~/.ssh/oracle/github_deploy_key \\
      ubuntu@<ORACLE_VM_PUBLIC_IP>:~/.ssh/github_deploy_key

Then re-run this script on the VM.
EOF
    exit 1
fi

echo "==> Fixing key permissions"
chmod 600 "$KEY_PATH"

echo "==> Writing ~/.ssh/config host entry"
mkdir -p "$HOME/.ssh"
touch "$HOME/.ssh/config"
chmod 600 "$HOME/.ssh/config"

if ! grep -q "Host github.com-mwr" "$HOME/.ssh/config"; then
    cat >> "$HOME/.ssh/config" <<EOF

Host github.com-mwr
  HostName github.com
  User git
  IdentityFile $KEY_PATH
  IdentitiesOnly yes
EOF
fi

echo "==> Trusting github.com in known_hosts"
ssh-keyscan -t ed25519,rsa github.com >> "$HOME/.ssh/known_hosts" 2>/dev/null || true
sort -u "$HOME/.ssh/known_hosts" -o "$HOME/.ssh/known_hosts"

echo "==> Testing SSH auth to GitHub"
if ssh -T -o StrictHostKeyChecking=accept-new github.com-mwr 2>&1 | grep -q "successfully authenticated"; then
    echo "    OK."
else
    echo "    ⚠️  SSH did not report success — check the deploy key is added on GitHub with write access." >&2
fi

echo "==> Cloning / updating repo at $REPO_DIR"
if [[ -d "$REPO_DIR/.git" ]]; then
    cd "$REPO_DIR"
    git remote set-url origin "git@github.com-mwr:Yuyang16Z/global-reservoir-scrapers.git"
    git pull --rebase
else
    git clone "git@github.com-mwr:Yuyang16Z/global-reservoir-scrapers.git" "$REPO_DIR"
fi

cd "$REPO_DIR"
git config user.email "mwr-oracle-bot@andyzeng.noreply"
git config user.name "mwr-oracle-bot"

echo "==> Done. Repo at $REPO_DIR is ready to push with the deploy key."
