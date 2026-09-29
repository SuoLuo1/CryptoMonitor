#!/usr/bin/env python3
"""
把审计报告推送到【Hermes Agent 对话用的 TG bot】(不是 CryptoMonitor 的告警 bot)

凭证来源: /root/.hermes/config.yaml 的 telegram.api_key + telegram.allowed_chats
          —— 不硬编码 token, 不在日志/聊天里打印明文

用法:
    python3 push_report_to_tg.py <报告文件> [标题]
    python3 push_report_to_tg.py /tmp/audit_full.log "信号完整性审计 9-19~9-28"
"""
import sys
import asyncio
import aiohttp
import yaml

HERMES_CONFIG = '/root/.hermes/config.yaml'
TG_LIMIT = 4000          # TG 单条上限 4096, 留余量


def load_hermes_tg():
    """从 Hermes 配置读取 bot token + chat_id"""
    cfg = yaml.safe_load(open(HERMES_CONFIG, encoding='utf-8'))
    tg = cfg.get('telegram') or {}
    token = tg.get('api_key')
    chats = tg.get('allowed_chats')
    if isinstance(chats, (list, tuple)):
        chat_id = str(chats[0])
    else:
        chat_id = str(chats).split(',')[0].strip()
    if not token or not chat_id:
        raise RuntimeError(f"Hermes telegram 配置不完整: token={'有' if token else '无'} chat={chat_id}")
    return token, chat_id


async def send(token, chat_id, text):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    async with aiohttp.ClientSession() as s:
        async with s.post(url, json={'chat_id': chat_id, 'text': text},
                          timeout=aiohttp.ClientTimeout(total=15)) as r:
            body = await r.text()
            if r.status != 200:
                print(f"❌ HTTP {r.status}: {body[:200]}")
                return False
            return True


def split_msg(text, limit=TG_LIMIT):
    """按行切分, 每段 <= limit"""
    out, cur = [], ''
    for line in text.splitlines():
        if len(line) > limit:
            if cur:
                out.append(cur); cur = ''
            for i in range(0, len(line), limit):
                out.append(line[i:i + limit])
            continue
        if len(cur) + len(line) + 1 > limit:
            out.append(cur)
            cur = line
        else:
            cur = f"{cur}\n{line}" if cur else line
    if cur:
        out.append(cur)
    return out


def extract_report(raw):
    """截取报告主体, 去掉进度条噪音"""
    marker = '📊 信号完整性审计报告'
    idx = raw.find(marker)
    body = raw[idx:] if idx >= 0 else raw[-4000:]
    # 去掉进度行 (以"  进度 "开头的)
    body = '\n'.join(l for l in body.splitlines() if not l.strip().startswith('进度 '))
    return '\n'.join(l.rstrip() for l in body.splitlines())


async def main():
    if len(sys.argv) < 2:
        print("用法: python3 push_report_to_tg.py <报告文件> [标题]")
        return 1
    path = sys.argv[1]
    title = sys.argv[2] if len(sys.argv) > 2 else '审计报告'

    token, chat_id = load_hermes_tg()
    print(f"目标: Hermes bot (chat {chat_id})")

    raw = open(path, encoding='utf-8', errors='replace').read()
    body = extract_report(raw)
    parts = split_msg(body)
    print(f"报告 {len(body)} 字符 → {len(parts)} 段")

    sent = 0
    for i, p in enumerate(parts, 1):
        head = f"🐾 {title} ({i}/{len(parts)})\n" if len(parts) > 1 else f"🐾 {title}\n"
        if await send(token, chat_id, head + p):
            sent += 1
            print(f"  段{i}/{len(parts)}: ✅ {len(head)+len(p)}字符")
        else:
            print(f"  段{i}: ❌ 停止")
            break
        await asyncio.sleep(1.0)
    print(f"完成: {sent}/{len(parts)} 段已送达")
    return 0 if sent == len(parts) else 2


if __name__ == '__main__':
    sys.exit(asyncio.run(main()))
