# Cloud Development Workflow

This project uses the personal cloud server as the primary development, test,
and validation environment. The local checkout is a synchronized working copy.

## Connection and safety

- Keep all connection parameters only in the Git-ignored
  `.security/remote-server.env` file. It must define
  `CRYPTOMONITOR_SSH_HOST`, `CRYPTOMONITOR_SSH_PORT`,
  `CRYPTOMONITOR_SSH_USER`, `CRYPTOMONITOR_SSH_IDENTITY`,
  `CRYPTOMONITOR_SSH_CONFIG`, `CRYPTOMONITOR_SSH_KNOWN_HOSTS`, and
  `CRYPTOMONITOR_REMOTE_PROJECT_DIR`.
- Never commit, print, copy, or infer server addresses, users, ports, private
  keys, or cloud-only credentials. Keep the private key outside this repository.
- All SSH and rsync operations must load the local configuration with
  `set -a && source .security/remote-server.env && set +a` and explicitly use
  its SSH config, identity, and dedicated known-hosts file. Do not rely on the
  global SSH config or default known-hosts file.
- Before any cloud write, run `bash deploy/check-cloud.sh --require-clean`.
  It performs only a BatchMode read-only connection and repository check.

## Daily workflow

1. Use `bash deploy/bootstrap-cloud.sh` once when the cloud project directory
   has not been created yet.
2. Run cloud commands through `bash deploy/remote.sh -- <command...>`.
3. Implement, install dependencies, and run tests on the cloud host. Use
   `bash deploy/sync-from-cloud.sh` afterwards to mirror cloud source changes
   locally.
4. `bash deploy/sync-to-cloud.sh --initial-seed` is only for the initial
   migration. Afterwards, use `bash deploy/sync-to-cloud.sh` only when an
   intentional local change must be copied to the cloud. It preserves cloud
   secrets and runtime data. It never deletes remote files unless explicitly
   passed `--delete`.
5. `sync-to-cloud` refuses a dirty cloud worktree. `sync-from-cloud` treats
   the cloud copy as authoritative and overwrites synchronized local files;
   do not make independent local source edits.

The cloud server remains the source of truth for behavior that depends on
network access, credentials, or runtime services. Local checks are limited to
static validation of the synchronization tooling unless the task explicitly
requires otherwise.
