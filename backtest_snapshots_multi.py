"""历史 Top 快照回测: 用每个历史时点的 Top 名单, 比较"第 N 次报警入场"。

去偏原理:
  - 当前 Top 是"事后看哪些币还活着" → 幸存者偏差
  - 历史快照 (如昨天零点) 的 Top 是当时真实可见的名单, 不含未来信息
  - analyze_entry 的 as_of 参数同时限制 K 线向前查找范围 → 无前视

每个快照内部做"同币配对"对比 (第1次 vs 第k次), 消除样本差异。
"""
import sys, os, csv, sqlite3, datetime as dt, statistics, argparse
from collections import defaultdict

sys.path.insert(0, '/root/CryptoMonitor')
import backtest_current_top_high as B

TZ = B.TZ
CACHE = '/root/CryptoMonitor/backtest_current_top/top_high_klines_cache.sqlite3'
ALERTS_DB = '/root/CryptoMonitor/alerts.db'
MAX_K = 5

DIR_SIGNAL = {
    "🚀暴涨": 1, "🚀暴涨3m": 1, "📈爬升": 1,
    "🩸暴跌": -1, "🩸暴跌3m": -1, "📉阴跌": -1,
}

parser = argparse.ArgumentParser()
parser.add_argument("--start", default="2026-10-14 00:00:00")
parser.add_argument("--end", default="2026-10-24 00:00:00")
parser.add_argument("--step-hours", type=int, default=24)
parser.add_argument("--limit", type=int, default=500)
parser.add_argument("--min-count", type=int, default=2)
parser.add_argument("--out", default="/root/CryptoMonitor/backtest_snapshots/snapshot_rows.csv")
parser.add_argument("--request-delay", type=float, default=0.12)
args = parser.parse_args()

os.makedirs(os.path.dirname(args.out), exist_ok=True)

cache = sqlite3.connect(CACHE)
conn = sqlite3.connect(ALERTS_DB)
conn.row_factory = sqlite3.Row


def load_segment(symbol, as_of):
    """取该币在 as_of 时点的 12h 连续段内的全部信号 (只看 as_of 之前)"""
    rows = conn.execute(
        """SELECT timestamp, signal_type, price_at_alert FROM alerts
           WHERE priority='urgent' AND symbol=? AND price_at_alert IS NOT NULL
             AND timestamp <= ?
           ORDER BY timestamp ASC""", (symbol, B.fmt_dt(as_of))).fetchall()
    if not rows:
        return []
    parsed = []
    for r in rows:
        try:
            t = B.parse_db_dt(r["timestamp"])
            p = float(r["price_at_alert"])
        except (TypeError, ValueError):
            continue
        if t and p > 0:
            parsed.append((t, str(r["signal_type"] or "").strip(), p))
    if not parsed:
        return []
    # 12h 切段 + 24h 活跃过滤 (与 utils.get_today_symbols_top 一致)
    last = parsed[-1][0]
    if (as_of - last).total_seconds() > 24 * 3600:
        return []
    start = len(parsed) - 1
    for i in range(len(parsed) - 2, -1, -1):
        if (parsed[i + 1][0] - parsed[i][0]).total_seconds() > 12 * 3600:
            start = i + 1
            break
    return parsed[start:]


# 生成快照时点
t0 = B.parse_dt(args.start)
t1 = B.parse_dt(args.end)
step = dt.timedelta(hours=args.step_hours)
as_of_list = []
cur = t0
while cur <= t1:
    as_of_list.append(cur)
    cur += step

print("=" * 80)
print("历史 Top 快照回测")
print("=" * 80)
print("  快照时点: %s → %s, 步长 %dh, 共 %d 个" % (
    args.start, args.end, args.step_hours, len(as_of_list)))
print("  Top 规则: count>=%d, limit=%d" % (args.min_count, args.limit))
print("  输出: %s" % args.out)
print()

rows_out = []
existing = set()
if os.path.exists(args.out):
    with open(args.out, encoding='utf-8-sig') as f:
        for r in csv.DictReader(f):
            existing.add((r['as_of'], r['symbol'], r['k']))
    print("  已有 %d 行, 跳过已算过的" % len(existing))

for idx, as_of in enumerate(as_of_list, 1):
    try:
        top = B.load_current_top(ALERTS_DB, as_of, args.limit, args.min_count)
    except Exception as exc:
        print("  [%d/%d] %s Top 读取失败: %s" % (idx, len(as_of_list), as_of, exc))
        continue

    done = 0
    for coin in top:
        seg = load_segment(coin.symbol, as_of)
        if len(seg) < 1:
            continue
        for k in range(1, min(MAX_K, len(seg)) + 1):
            key = (B.fmt_dt(as_of), coin.symbol, str(k))
            if key in existing:
                continue
            t, st, p = seg[k - 1]
            direction = DIR_SIGNAL.get(st)
            try:
                r = B.analyze_entry(coin.symbol, t, p, as_of, cache, args.request_delay)
            except Exception as exc:
                print("    %s 第%d次 失败: %s" % (coin.symbol, k, exc))
                continue
            if not r:
                continue
            rows_out.append({
                'as_of': B.fmt_dt(as_of),
                'symbol': coin.symbol,
                'k': k,
                'seg_count': len(seg),
                'signal_time': B.fmt_dt(t),
                'signal_type': st,
                'direction': direction or 0,
                'entry_price': p,
                'highest': r.get('highest_price_after_entry'),
                'lowest': r.get('lowest_price_after_entry'),
                'roi_to_high_pct': r.get('long_roi_to_high_pct'),
                'return_to_low_pct': r.get('long_return_to_low_pct'),
                'directional_mfe_pct': r.get('directional_mfe_pct'),
            })
            done += 1
    print("  [%d/%d] %s  Top=%d 个, 新算 %d 个入场点" % (
        idx, len(as_of_list), B.fmt_dt(as_of), len(top), done))

    # 增量落盘, 中断也不丢
    mode = 'a' if os.path.exists(args.out) and (existing or rows_out[:-done]) else 'w'
    with open(args.out, mode, newline='', encoding='utf-8-sig') as f:
        w = csv.DictWriter(f, fieldnames=list(rows_out[0].keys()))
        if mode == 'w':
            w.writeheader()
        w.writerows(rows_out[-done:] if done else [])
    existing.update((r['as_of'], r['symbol'], str(r['k'])) for r in rows_out[-done:] if done)

cache.close()
conn.close()
print("\n完成, 共 %d 行" % len(existing))
