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

## systemd service 与凭据管理

CryptoMonitor 通过 `/etc/systemd/system/cryptomonitor.service` 由 systemd 托管。
service 单元的内容不在 git 中跟踪，由 `deploy/systemd-install.sh` 从模板 + 凭据渲染生成。

**凭据直接写入 service 单元的 `Environment=` 行**（单一事实源）。重装 / 迁移主机时，
只需要重跑 `deploy/systemd-install.sh` 即可重建完整运行环境，不必再额外管理外部凭据文件。

`.env.example` 是可选模板文件，仅当你需要把凭据“临时”喂给 service 安装脚本时使用，
或当你想在 shell 手动测试 `set -a && . ./.env && set +a` 时使用。

新装机器复现步骤（推荐）：

1. `sudo cp .env.example .env && sudo $EDITOR .env`（仅当走 .env → service 注入路径时）
2. `chmod 600 .env`，避免凭据被其他进程读取
3. `sudo bash deploy/systemd-install.sh`，幂等安装 / 更新 service 单元
   - 脚本会读取 `.env`（如有）→ 渲染进 service 单元
   - 找不到 `.env` 时会交互式问三个值，直接 TG 也行
4. `systemctl status cryptomonitor.service` 确认 active

`systemd-install.sh` 是幂等的，重复跑不会破坏现有 service；只在用户确认后才覆写 service。
脚本改 service 内容时自动 `daemon-reload` + `restart`，无变化时保持现有 PID 不动。

脱敏：`systemd-install.sh` 在覆盖前会显示 redacted diff（`Environment=... ***`），不会打印真实凭据。

## Git commits

- Use the repository's existing conventional style: `feat:`, `fix:`,
  `chore:`, `docs:`, or another accurate type followed by a concise Chinese
  subject that states what changed and why.
- When a change has meaningful implementation or validation details, add a
  commit body describing the affected behavior, important safeguards, and the
  cloud-side checks that passed. Do not put credentials, server connection
  details, raw runtime data, or company information in a commit message.
