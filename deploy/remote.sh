#!/usr/bin/env bash

set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/remote-env.sh
source "$SCRIPT_DIR/lib/remote-env.sh"

usage() {
  printf 'Usage: %s [--check] [--] <command> [args...]\n' "${0##*/}" >&2
}

load_remote_environment
mapfile -d '' -t SSH_OPTIONS < <(ssh_options)

if [[ "${1:-}" == "--check" ]]; then
  exec "$SCRIPT_DIR/check-cloud.sh"
fi

if [[ "${1:-}" == "--" ]]; then
  shift
fi

if [[ $# -eq 0 ]]; then
  usage
  exit 2
fi

COMMAND_ARGUMENTS="$(remote_command_arguments "$CRYPTOMONITOR_REMOTE_PROJECT_DIR" "$@")"
exec ssh "${SSH_OPTIONS[@]}" "$(remote_target)" \
  "exec bash -s -- $COMMAND_ARGUMENTS" <<'REMOTE_SCRIPT'
set -euo pipefail

remote_project_dir="$1"
shift
cd -- "$remote_project_dir"
export GIT_CONFIG_COUNT=1
export GIT_CONFIG_KEY_0=safe.directory
export GIT_CONFIG_VALUE_0="$remote_project_dir"
exec "$@"
REMOTE_SCRIPT
