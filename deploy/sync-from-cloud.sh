#!/usr/bin/env bash

set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck source=lib/remote-env.sh
source "$SCRIPT_DIR/lib/remote-env.sh"

load_remote_environment
"$SCRIPT_DIR/check-cloud.sh"
RSYNC_SSH="$(rsync_ssh_command)"

rsync --archive --no-owner --no-group --compress --human-readable --itemize-changes \
  --delete-delay \
  --exclude '.git/' \
  --exclude '.security/' \
  --exclude '.venv/' \
  --exclude 'venv/' \
  --exclude '__pycache__/' \
  --exclude '*.py[cod]' \
  --exclude '.pytest_cache/' \
  --exclude '.mypy_cache/' \
  --exclude '.ruff_cache/' \
  --exclude 'data/' \
  --exclude 'logs/' \
  --exclude 'run/' \
  --exclude 'tmp/' \
  --exclude 'backtest_results/' \
  --exclude '*.db' \
  --exclude '*.sqlite' \
  --exclude '*.csv' \
  --exclude '*.log' \
  --exclude 'config.py' \
  --exclude '.DS_Store' \
  -e "$RSYNC_SSH" \
  "$(remote_target):$CRYPTOMONITOR_REMOTE_PROJECT_DIR/" "$PROJECT_ROOT/"

printf 'Cloud source synchronized to the local project.\n'
