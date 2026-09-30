#!/usr/bin/env bash

set -euo pipefail

readonly DEPLOY_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
readonly DEPLOY_ENV_FILE="$DEPLOY_ROOT/.security/remote-server.env"

load_remote_environment() {
  if [[ ! -r "$DEPLOY_ENV_FILE" ]]; then
    printf 'Missing local cloud configuration: .security/remote-server.env\n' >&2
    return 1
  fi

  set -a
  # shellcheck disable=SC1090
  source "$DEPLOY_ENV_FILE"
  set +a

  local configured_path
  for configured_path in \
    CRYPTOMONITOR_SSH_IDENTITY \
    CRYPTOMONITOR_SSH_CONFIG \
    CRYPTOMONITOR_SSH_KNOWN_HOSTS; do
    if [[ "${!configured_path:-}" != /* ]]; then
      printf -v "$configured_path" '%s/%s' "$DEPLOY_ROOT" "${!configured_path}"
    fi
  done

  local required
  for required in \
    CRYPTOMONITOR_SSH_HOST \
    CRYPTOMONITOR_SSH_PORT \
    CRYPTOMONITOR_SSH_USER \
    CRYPTOMONITOR_SSH_IDENTITY \
    CRYPTOMONITOR_SSH_CONFIG \
    CRYPTOMONITOR_SSH_KNOWN_HOSTS \
    CRYPTOMONITOR_REMOTE_PROJECT_DIR; do
    if [[ -z "${!required:-}" ]]; then
      printf 'Incomplete local cloud configuration.\n' >&2
      return 1
    fi
  done
}

remote_target() {
  printf '%s@%s' "$CRYPTOMONITOR_SSH_USER" "$CRYPTOMONITOR_SSH_HOST"
}

remote_command_arguments() {
  local argument
  for argument in "$@"; do
    printf '%q ' "$argument"
  done
}

canonical_git_origin() {
  local origin="$1"
  local scheme
  local remainder

  case "$origin" in
    http://*|https://*)
      scheme="${origin%%://*}"
      remainder="${origin#*://}"
      remainder="${remainder#*@}"
      printf '%s://%s' "$scheme" "$remainder"
      ;;
    *)
      printf '%s' "$origin"
      ;;
  esac
}

ssh_options() {
  printf '%s\0' \
    -F "$CRYPTOMONITOR_SSH_CONFIG" \
    -i "$CRYPTOMONITOR_SSH_IDENTITY" \
    -p "$CRYPTOMONITOR_SSH_PORT" \
    -o IdentitiesOnly=yes \
    -o "UserKnownHostsFile=$CRYPTOMONITOR_SSH_KNOWN_HOSTS" \
    -o BatchMode=yes \
    -o ConnectTimeout=15
}

rsync_ssh_command() {
  local -a options
  local option
  local command='ssh'

  mapfile -d '' -t options < <(ssh_options)
  for option in "${options[@]}"; do
    printf -v option '%q' "$option"
    command+=" $option"
  done
  printf '%s' "$command"
}

shell_quote() {
  printf '%q' "$1"
}
