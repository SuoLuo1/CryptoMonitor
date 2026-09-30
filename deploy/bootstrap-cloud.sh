#!/usr/bin/env bash

set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck source=lib/remote-env.sh
source "$SCRIPT_DIR/lib/remote-env.sh"

load_remote_environment
mapfile -d '' -t SSH_OPTIONS < <(ssh_options)

ORIGIN_URL="$(git -C "$PROJECT_ROOT" config --get remote.origin.url || true)"
if [[ -z "$ORIGIN_URL" ]]; then
  printf 'The local origin remote is not configured.\n' >&2
  exit 1
fi

REMOTE_DIR_QUOTED="$(shell_quote "$CRYPTOMONITOR_REMOTE_PROJECT_DIR")"
REMOTE_PARENT_QUOTED="$(shell_quote "$(dirname "$CRYPTOMONITOR_REMOTE_PROJECT_DIR")")"
ORIGIN_URL_QUOTED="$(shell_quote "$ORIGIN_URL")"

ssh "${SSH_OPTIONS[@]}" "$(remote_target)" \
  "if test -e $REMOTE_DIR_QUOTED; then
     test -d $REMOTE_DIR_QUOTED/.git || { printf 'Cloud path exists but is not a Git repository.\\n' >&2; exit 1; }
   else
     mkdir -p -- $REMOTE_PARENT_QUOTED
     git clone --origin origin $ORIGIN_URL_QUOTED $REMOTE_DIR_QUOTED
   fi
   cd -- $REMOTE_DIR_QUOTED
   git -c safe.directory=$REMOTE_DIR_QUOTED status --short --branch"

printf 'Cloud project is ready.\n'
