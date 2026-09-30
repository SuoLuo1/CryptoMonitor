#!/usr/bin/env bash

set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/remote-env.sh
source "$SCRIPT_DIR/lib/remote-env.sh"

REQUIRE_CLEAN=false
if [[ "${1:-}" == "--require-clean" ]]; then
  REQUIRE_CLEAN=true
  shift
fi
if [[ $# -ne 0 ]]; then
  printf 'Usage: %s [--require-clean]\n' "${0##*/}" >&2
  exit 2
fi

load_remote_environment
mapfile -d '' -t SSH_OPTIONS < <(ssh_options)
EXPECTED_ORIGIN="$(git -C "$DEPLOY_ROOT" config --get remote.origin.url || true)"

if [[ -z "$EXPECTED_ORIGIN" ]]; then
  printf 'The local origin remote is not configured.\n' >&2
  exit 1
fi

EXPECTED_ORIGIN="$(canonical_git_origin "$EXPECTED_ORIGIN")"
REMOTE_ARGUMENTS="$(remote_command_arguments "$CRYPTOMONITOR_REMOTE_PROJECT_DIR" "$EXPECTED_ORIGIN" "$REQUIRE_CLEAN")"

ssh "${SSH_OPTIONS[@]}" "$(remote_target)" \
  "exec bash -s -- $REMOTE_ARGUMENTS" <<'REMOTE_SCRIPT'
set -euo pipefail

remote_project_dir="$1"
expected_origin="$2"
require_clean="$3"

if [[ ! -d "$remote_project_dir" ]]; then
  printf 'Cloud project directory is missing.\n' >&2
  exit 1
fi
if [[ ! -d "$remote_project_dir/.git" ]]; then
  printf 'Cloud project directory is not a Git repository.\n' >&2
  exit 1
fi

cd -- "$remote_project_dir"
git_safe() {
  git -c "safe.directory=$remote_project_dir" "$@"
}

remote_origin="$(git_safe config --get remote.origin.url)"
case "$remote_origin" in
  http://*|https://*)
    scheme="${remote_origin%%://*}"
    remote_origin="$scheme://${remote_origin#*://}"
    remote_origin="$scheme://${remote_origin#*@}"
    ;;
esac

if [[ "$remote_origin" != "$expected_origin" ]]; then
  printf 'Cloud origin does not match the local origin.\n' >&2
  exit 1
fi
if [[ "$require_clean" == true && -n "$(git_safe status --porcelain)" ]]; then
  printf 'Cloud Git worktree is dirty; refusing to synchronize.\n' >&2
  exit 1
fi

if [[ "$require_clean" == true ]]; then
  printf 'Cloud repository is clean and matches the local origin.\n'
else
  printf 'Cloud repository matches the local origin.\n'
fi
REMOTE_SCRIPT

printf 'Cloud connection and repository check passed.\n'
