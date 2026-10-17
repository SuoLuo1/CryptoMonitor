#!/usr/bin/env bash
# systemd-install.sh - 安装/更新 CryptoMonitor 的 systemd service 单元
# ------------------------------------------------------------
# 设计原则:
# - 凭据直接写进 service 单元 (单一事实源, 重装/迁移时不用再管 .env)
# - service 文件不在 git: 由本脚本从模板 + 真实凭据渲染而成
# - 安装时需要交互式输入凭据, 或从现有 .env 文件读取一次性灌入
#
# 触发时机:
# - 第一次在新机器上部署: 交互式输入凭据
# - 已部署机器: 重跑幂等
# - service 配置变更: 重跑

set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

readonly SERVICE_NAME="cryptomonitor.service"
readonly SERVICE_PATH="/etc/systemd/system/${SERVICE_NAME}"
readonly ENV_FILE="${PROJECT_ROOT}/.env"

log() {
    printf '[systemd-install] %s\n' "$*"
}

die() {
    printf '[systemd-install] ❌ ERROR: %s\n' "$*" >&2
    exit 1
}

# ============================================================
# 0. 收集凭据 (single source of truth = service 单元)
# ============================================================
# 优先从现有 /root/CryptoMonitor/.env 读, 这样旧部署不会丢凭据
# 找不到 / 不全时再交互输入
collect_credentials() {
    local sourced_from_env=false

    if [[ -f "$ENV_FILE" ]]; then
        # 只读取"裸值", 不 export 到当前 shell
        if grep -qE '^TG_BOT_TOKEN="[^"]+"' "$ENV_FILE" \
           && grep -qE '^TG_CHAT_ID="[^"]+"' "$ENV_FILE"; then
            TG_BOT_TOKEN="$(grep -E '^TG_BOT_TOKEN=' "$ENV_FILE" | sed -E 's/^[^=]+="([^"]+)"$/\1/' | head -1)"
            TG_CHAT_ID="$(grep -E '^TG_CHAT_ID=' "$ENV_FILE" | sed -E 's/^[^=]+="([^"]+)"$/\1/' | head -1)"
            DEEPSEEK_API_KEY="$(grep -E '^DEEPSEEK_API_KEY=' "$ENV_FILE" | sed -E 's/^[^=]+="([^"]+)"$/\1/' | head -1)"
            sourced_from_env=true
            log "📥 从 ${ENV_FILE} 读取凭据"
        fi
    fi

    if [[ "$sourced_from_env" == false ]]; then
        log "⚠️  未找到有效的 .env, 请交互输入凭据 (将直接写入 service 单元)"
        read -r -p "Telegram Bot Token: " TG_BOT_TOKEN
        [[ -n "$TG_BOT_TOKEN" ]] || die "TG_BOT_TOKEN 不能为空"
        read -r -p "Telegram Chat ID: " TG_CHAT_ID
        [[ -n "$TG_CHAT_ID" ]] || die "TG_CHAT_ID 不能为空"
        read -r -p "DeepSeek API Key (空 = 留空, AI 通道目前未启用): " DEEPSEEK_API_KEY
        DEEPSEEK_API_KEY="${DEEPSEEK_API_KEY:-}"
    fi

    # 基本格式检查
    [[ "$TG_BOT_TOKEN" =~ ^[0-9]+:.+ ]] || die "TG_BOT_TOKEN 格式异常 (期望 <数字>:<字符串>)"
    [[ "$TG_CHAT_ID" =~ ^-?[0-9]+$ ]] || die "TG_CHAT_ID 应为纯数字"
}

# ============================================================
# 1. 渲染 service 单元 (这里只是模板, 真实凭据下文替换)
# ============================================================
build_service_template() {
    cat <<'EOF'
[Unit]
Description=CryptoMonitor - 加密货币监控系统
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=__PROJECT_ROOT__
ExecStart=/usr/bin/python3 -u main.py
# 凭据直接写进 service 单元 (systemd Environment= 语法, 单一事实源)
Environment="TG_BOT_TOKEN=__TG_BOT_TOKEN__"
Environment="TG_CHAT_ID=__TG_CHAT_ID__"
Environment="DEEPSEEK_API_KEY=__DEEPSEEK_API_KEY__"
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal
# 删掉 MemoryHigh（之前 384MB 太低，频繁触发内核节流反而导致连接超时）
# 只保留 MemoryMax 作为硬上限 + OOMPolicy 优雅退出 + Restart 自动拉起
MemoryMax=1G
OOMPolicy=stop

[Install]
WantedBy=multi-user.target
EOF
}

# 安全地将值替换进 service 模板 (避免 sed 特殊字符问题)
render_service() {
    local template="$1" out="$2"
    python3 - "$template" "$out" "$PROJECT_ROOT" "$TG_BOT_TOKEN" "$TG_CHAT_ID" "$DEEPSEEK_API_KEY" <<'PYEOF'
import sys
template_path, out_path, project_root, tg_token, tg_chat, ds_key = sys.argv[1:7]
with open(template_path) as f:
    content = f.read()
content = (content
    .replace("__PROJECT_ROOT__", project_root)
    .replace("__TG_BOT_TOKEN__", tg_token)
    .replace("__TG_CHAT_ID__", tg_chat)
    .replace("__DEEPSEEK_API_KEY__", ds_key))
with open(out_path, "w") as f:
    f.write(content)
PYEOF
}

# ============================================================
# 主流程
# ============================================================
[[ "$(id -u)" -eq 0 ]] || die "需要 root 权限安装 systemd 单元, 请用 sudo"

collect_credentials

readonly SERVICE_TEMPLATE_STR="$(build_service_template)"
TMP_TEMPLATE="$(mktemp)"
TMP_SERVICE="$(mktemp)"
trap 'rm -f "$TMP_TEMPLATE" "$TMP_SERVICE"' EXIT

printf '%s' "$SERVICE_TEMPLATE_STR" > "$TMP_TEMPLATE"
render_service "$TMP_TEMPLATE" "$TMP_SERVICE"

# 对比现有 service
SERVICE_CHANGED=false
if [[ -f "$SERVICE_PATH" ]]; then
    if diff -q "$SERVICE_PATH" "$TMP_SERVICE" > /dev/null 2>&1; then
        log "✅ ${SERVICE_PATH} 已是最新, 跳过写入"
    else
        log "⚠️  ${SERVICE_PATH} 与新模板不一致, 显示 diff (含凭据行已脱敏):"
        # 脱敏展示: 把 "Environment=\"KEY=...
        sed -E 's/(Environment="[A-Z_]+=).+"/\1***"/g' "$SERVICE_PATH" > /tmp/.old_redacted
        sed -E 's/(Environment="[A-Z_]+=).+"/\1***"/g' "$TMP_SERVICE"  > /tmp/.new_redacted
        diff -u /tmp/.old_redacted /tmp/.new_redacted || true
        rm -f /tmp/.old_redacted /tmp/.new_redacted
        printf '\n确认覆写 (会重启 service)? [y/N] '
        read -r -n 1 answer
        printf '\n'
        if [[ "$answer" =~ ^[Yy]$ ]]; then
            cp "$TMP_SERVICE" "$SERVICE_PATH"
            SERVICE_CHANGED=true
            log "✅ service 单元已更新"
        else
            log "⏭️  跳过 service 更新"
            exit 0
        fi
    fi
else
    log "📝 首次安装 ${SERVICE_PATH}"
    cp "$TMP_SERVICE" "$SERVICE_PATH"
    SERVICE_CHANGED=true
fi

if [[ "$SERVICE_CHANGED" == true ]]; then
    systemctl daemon-reload
    systemctl enable "${SERVICE_NAME}"
    if systemctl is-active --quiet "${SERVICE_NAME}"; then
        log "🔄 重启 ${SERVICE_NAME} 以应用新凭据..."
        systemctl restart "${SERVICE_NAME}"
        sleep 2
    fi
fi

# 验证
if systemctl is-active --quiet "${SERVICE_NAME}"; then
    PID="$(systemctl show "${SERVICE_NAME}" -p MainPID --value)"
    log "✅ ${SERVICE_NAME} active, PID=${PID}"
    # 抽检: 进程能看到凭据吗
    if [[ -r "/proc/${PID}/environ" ]]; then
        if grep -q "^TG_BOT_TOKEN=${TG_BOT_TOKEN}$" "/proc/${PID}/environ" 2>/dev/null; then
            log "✅ 进程环境确认含凭据 (env vars 正确注入)"
        fi
    fi
else
    die "${SERVICE_NAME} 未处于 active 状态, 请执行: journalctl -u ${SERVICE_NAME} -n 50"
fi

log "🎉 systemd 单元安装完成 (凭据已固化进 service 单元)"
log "    可选后续: 如果你不再需要 .env, 可以 rm /root/CryptoMonitor/.env"
