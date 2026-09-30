#!/usr/bin/env bash

set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck source=lib/remote-env.sh
source "$SCRIPT_DIR/lib/remote-env.sh"

DELETE_REMOTE=false
INITIAL_SEED=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    --delete)
      DELETE_REMOTE=true
      ;;
    --initial-seed)
      INITIAL_SEED=true
      ;;
    *)
      printf 'Usage: %s [--initial-seed] [--delete]\n' "${0##*/}" >&2
      exit 2
      ;;
  esac
  shift
done

load_remote_environment
if [[ "$INITIAL_SEED" == true ]]; then
  "$SCRIPT_DIR/check-cloud.sh"
else
  "$SCRIPT_DIR/check-cloud.sh" --require-clean
fi
RSYNC_SSH="$(rsync_ssh_command)"

RSYNC_DELETE_ARGS=()
if [[ "$DELETE_REMOTE" == true ]]; then
  RSYNC_DELETE_ARGS=(--delete-delay)
fi

cd "$PROJECT_ROOT"
rsync --archive --no-owner --no-group --compress --human-readable --itemize-changes \
  "${RSYNC_DELETE_ARGS[@]}" \
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
  ./ "$(remote_target):$CRYPTOMONITOR_REMOTE_PROJECT_DIR/"

printf 'Local source synchronized to the cloud project.\n'
