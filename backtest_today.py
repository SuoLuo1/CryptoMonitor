#!/usr/bin/env python3
"""
回测妖币报警信号策略 (v8 - 2026-09-24 灵爪重写, 固化 9-23 会话确认的 v7 口径)

策略口径 (主人 9-23 在 TG 会话里逐步确认的最终版):
- 标的: 每个币当天的【首次看涨信号】(🚀暴涨 / 🚀暴涨3m / 📈爬升), 只做多, 每币仅一笔
- 入场: price_at_alert (报警时刻现价, NULL 兜底用前收盘K线收盘价)
- 止损: 信号前最后一根【已收盘】K线的 low × 0.98
        (爬升信号 = 前60根1m已收盘K线的最低 low × 0.98)
- 止盈: 动态 1:3 盈亏比 → entry + (entry - stop_loss) × 3
- 仓位: 10U 本金 × 10x 杠杆 = 100U 名义
- 手续费: 单边 0.04%, 开+平 0.08%
- 结算: 逐根K线扫描, 同根K线内止损优先(保守), 先到先出;
        当天都没碰到 → 23:59:59 最后一根收盘价强平 (period_end)

v8 相对旧文件版的修复 (全部实测验证过):
1. 【止盈口径】旧文件还是"全天最高价平仓"(完美预知, 结果虚高不可用)。
   9-23 会话里 1:3 是临时内联脚本跑的(v5→v7), 从没写回这个文件 → 现在固化。
2. 【前视偏差】Binance endTime 是 inclusive! 旧版 fetch(endTime=bucket_end)
   拿到的是信号之后的下一根K线 (9-23 主人亲自抓到 TAKEUSDT: 旧代码 low=0.07048
   是未来数据, 正确的前一根已收盘K线 low=0.06072)。
   修复: 只取 open_time < 信号所在桶起点 的K线。
3. 【数据截断】旧版 needed=min(...,1000) 导致凌晨 1m 信号丢失最多 437 根K线。
   修复: 全天K线 startTime+endTime 一次拉齐 (1m 全天 1440 根 < 1500 上限)。
4. 【缓存污染】旧版缓存 key 用未来时刻 23:59, 中午跑缓存半天数据、晚上命中旧缓存。
   修复: 按 (symbol, interval, 日期) 缓存; 当天(未过完)不落盘, 历史日永久缓存。

只读模式: 只读 alerts.db, 不动 main.py, 不发 Telegram
用法:
    python3 backtest_today.py             # 回测今天
    python3 backtest_today.py 2026-09-23  # 回测指定日期

作者: 灵爪 (御姐猫娘风)
"""
import sqlite3
import os
import sys
import json
import time
import requests
from datetime import datetime, timezone, timedelta
from collections import defaultdict

# === 配置 ===
DB_PATH = '/root/CryptoMonitor/alerts.db'
CACHE_DIR = '/tmp/backtest_cache_v8'
BINANCE_HTTP_FUTURES = 'https://fapi.binance.com'
PRINCIPAL_USDT = 10.0      # 本金 10U
LEVERAGE = 10              # 10 倍杠杆
NOTIONAL = PRINCIPAL_USDT * LEVERAGE  # 名义 100U
STOP_LOSS_PCT = 0.02       # 止损 = 结构低点再让 2%
RR_RATIO = 3               # 止盈 = 风险距离 × 3 (动态 1:3, 主人 9-23 确认)
FEE_RATE = 0.0004          # 单边手续费 0.04%
MAKER_FEE_PCT = FEE_RATE * 2  # 开+平合计 0.08%
BJ_TZ = timezone(timedelta(hours=8))

INTERVAL_MS = {'1m': 60_000, '3m': 180_000}

# 做多信号类型 -> 触发K线周期 / 结构窗口 (暴跌/阴跌类不做, 主人规则)
LONG_SIGNALS = {
    '🚀暴涨':   {'interval': '1m', 'window': 1},
    '🚀暴涨3m': {'interval': '3m', 'window': 1},
    '📈爬升':   {'interval': '1m', 'window': 60},
}

os.makedirs(CACHE_DIR, exist_ok=True)


# === 全天K线缓存 (一次拉齐全天, 修复截断+缓存污染) ===
_day_cache = {}  # (symbol, interval, date_str) -> klines list


def get_day_klines(symbol, interval, date_str):
    """
    拉取 symbol 在 date_str (北京时间) 全天的K线, 内存+磁盘双缓存。
    1m 全天 1440 根 < Binance 单次上限 1500, 一次拉齐不截断。
    当天(未过完)的数据不落盘, 避免半天数据被固化成缓存。
    """
    key = (symbol, interval, date_str)
    if key in _day_cache:
        return _day_cache[key]

    cache_path = os.path.join(CACHE_DIR, f"{symbol}_{interval}_{date_str}.json")
    today_str = datetime.now(BJ_TZ).strftime('%Y-%m-%d')
    is_past_day = date_str < today_str

    if is_past_day and os.path.exists(cache_path):
        with open(cache_path, 'r') as f:
            data = json.load(f)
            _day_cache[key] = data
            return data

    day_start = datetime.strptime(f'{date_str} 00:00:00', '%Y-%m-%d %H:%M:%S').replace(tzinfo=BJ_TZ)
    day_end = datetime.strptime(f'{date_str} 23:59:59', '%Y-%m-%d %H:%M:%S').replace(tzinfo=BJ_TZ)
    start_ms = int(day_start.timestamp() * 1000)
    end_ms = min(int(day_end.timestamp() * 1000), int(time.time() * 1000))

    try:
        data = None
        for attempt in range(4):  # 🐾 指数退避重试, 防 183币×多请求触发 429 限流
            try:
                r = requests.get(f"{BINANCE_HTTP_FUTURES}/fapi/v1/klines", params={
                    'symbol': symbol, 'interval': interval,
                    'startTime': start_ms, 'endTime': end_ms, 'limit': 1500,
                }, timeout=10)
                if r.status_code in (418, 429):  # 限流/封禁 → 退避更久
                    wait = int(r.headers.get('Retry-After', 2 ** (attempt + 1)))
                    time.sleep(min(wait, 30))
                    continue
                r.raise_for_status()
                data = r.json()
                break
            except Exception as e:
                if attempt == 3:
                    raise e
                time.sleep(2 ** attempt)  # 1,2,4s 退避
        if data is None:
            data = []
    except Exception as e:
        print(f"  ⚠️  拉K线失败 {symbol} {interval} {date_str}: {e}")
        data = []

    if data and is_past_day:
        with open(cache_path, 'w') as f:
            json.dump(data, f)
    time.sleep(0.05)  # 友好限流

    _day_cache[key] = data
    return data


def _yday(date_str):
    return (datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=BJ_TZ)
            - timedelta(days=1)).strftime('%Y-%m-%d')


def prev_closed_kline(symbol, interval, signal_ts_ms, date_str):
    """
    信号前最后一根【已收盘】K线 → (low, close)
    修复: 只取 open_time < 信号所在桶起点 的K线, 绝不使用信号时刻之后的数据。
    """
    ms = INTERVAL_MS[interval]
    bucket_start = (signal_ts_ms // ms) * ms
    klines = get_day_klines(symbol, interval, date_str)
    candidates = [k for k in klines if k[0] < bucket_start]
    if not candidates:
        # 信号在当天开头, 前一根K线在昨天
        yk = get_day_klines(symbol, interval, _yday(date_str))
        candidates = [k for k in yk if k[0] < bucket_start]
    if not candidates:
        return None, None
    k = candidates[-1]
    return float(k[3]), float(k[4])  # low, close


def window_min_low(symbol, signal_ts_ms, n, date_str):
    """信号前 n 根 1m 已收盘K线的最低 low (爬升信号用) → (min_low, last_close)"""
    bucket_start = (signal_ts_ms // 60_000) * 60_000
    klines = get_day_klines(symbol, '1m', date_str)
    candidates = [k for k in klines if k[0] < bucket_start]
    if len(candidates) < n:
        yk = get_day_klines(symbol, '1m', _yday(date_str))
        candidates = [k for k in yk if k[0] < bucket_start] + candidates
    if not candidates:
        return None, None
    window = candidates[-n:]
    return min(float(k[3]) for k in window), float(window[-1][4])


# === 主回测 ===
def run_backtest(date_str=None):
    if date_str is None:
        date_str = datetime.now(BJ_TZ).strftime('%Y-%m-%d')

    day_start = datetime.strptime(f'{date_str} 00:00:00', '%Y-%m-%d %H:%M:%S').replace(tzinfo=BJ_TZ)
    day_end = datetime.strptime(f'{date_str} 23:59:59', '%Y-%m-%d %H:%M:%S').replace(tzinfo=BJ_TZ)

    print(f"==> 回测日期: {date_str} (北京时间 00:00:00 ~ 23:59:59)")
    print(f"==> 策略: 首次看涨信号做多 | 止损=前收盘K线low×{1-STOP_LOSS_PCT:.2f} | 止盈=动态1:{RR_RATIO}")
    print(f"==> 仓位: {PRINCIPAL_USDT}U × {LEVERAGE}x = 名义 {NOTIONAL}U | 手续费 {MAKER_FEE_PCT*100:.2f}% (开+平)")
    print(f"==> 缓存: {CACHE_DIR}")
    print("=" * 78)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT timestamp, symbol, signal_type, price_at_alert
        FROM alerts
        WHERE priority = 'urgent'
          AND timestamp >= ? AND timestamp <= ?
        ORDER BY timestamp ASC
    """, (day_start.strftime('%Y-%m-%d %H:%M:%S'), day_end.strftime('%Y-%m-%d %H:%M:%S')))
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        print("该日没有 urgent 警报数据")
        return

    print(f"==> 当天 urgent 警报: {len(rows)} 条")

    # 每个币的【首次看涨信号】(只做多类型, 每币一笔)
    first_seen = {}
    for ts, symbol, signal_type, price in rows:
        if signal_type in LONG_SIGNALS and symbol not in first_seen:
            first_seen[symbol] = (ts, symbol, signal_type, price)

    print(f"==> 首次看涨信号 (去重后): {len(first_seen)} 个币")
    print("=" * 78)

    results = []
    skipped = []

    for ts_str, symbol, signal_type, price_at_alert in sorted(first_seen.values(), key=lambda x: x[0]):
        info = LONG_SIGNALS[signal_type]
        interval = info['interval']

        ts_dt = datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=BJ_TZ)
        ts_ms = int(ts_dt.timestamp() * 1000)

        # 1) 止损基准: 信号前【已收盘】K线的结构低点 (修复前视偏差)
        if info['window'] == 1:
            min_low, last_close = prev_closed_kline(symbol, interval, ts_ms, date_str)
        else:
            min_low, last_close = window_min_low(symbol, ts_ms, info['window'], date_str)
        if min_low is None:
            skipped.append(f"{symbol} {signal_type}: 拉不到K线")
            continue

        stop_loss = min_low * (1 - STOP_LOSS_PCT)

        # 2) 入场价: 报警现价, NULL 兜底用前收盘K线收盘价
        entry = price_at_alert if (price_at_alert and price_at_alert > 0) else last_close
        if not entry or entry <= 0:
            skipped.append(f"{symbol} {signal_type}: 无入场价")
            continue

        # 3) 动态 1:3 止盈 (主人 9-23 确认: 动态止损距离×3)
        risk = entry - stop_loss
        if risk <= 0:
            # 结构低点 >= 入场价: 报警价与K线数据错位, 跳过不硬算
            skipped.append(f"{symbol} {signal_type}: risk<=0 (low={min_low:.6f} >= entry={entry:.6f})")
            continue
        take_profit = entry + risk * RR_RATIO

        # 4) 逐根K线结算 (与9-23 v7口径一致: 跳过信号所在桶——它含有信号前的价格
        #    动作, 用它判定=前视偏差; 同根K线内止损优先, 保守)
        ms = INTERVAL_MS[interval]
        period = [k for k in get_day_klines(symbol, interval, date_str) if k[0] >= ts_ms]
        if not period:
            skipped.append(f"{symbol} {signal_type}: 无期内K线")
            continue

        actual_exit = None
        exit_reason = 'period_end'
        exit_ts = period[-1][0]
        for k in period:
            k_low, k_high = float(k[3]), float(k[2])
            if k_low <= stop_loss:
                actual_exit, exit_reason, exit_ts = stop_loss, 'stop_loss', k[0]
                break
            if k_high >= take_profit:
                actual_exit, exit_reason, exit_ts = take_profit, 'take_profit', k[0]
                break
        if actual_exit is None:
            actual_exit = float(period[-1][4])  # 收盘强平

        pnl_pct = (actual_exit - entry) / entry
        pnl_usdt = NOTIONAL * pnl_pct
        fee = NOTIONAL * MAKER_FEE_PCT
        net = pnl_usdt - fee

        results.append({
            'time': ts_str, 'symbol': symbol, 'signal_type': signal_type,
            'entry': entry, 'stop_loss': stop_loss, 'take_profit': take_profit,
            'actual_exit': actual_exit, 'exit_reason': exit_reason,
            'exit_time': datetime.fromtimestamp(exit_ts / 1000, BJ_TZ).strftime('%H:%M'),
            'risk_pct': risk / entry * 100,
            'pnl_pct': pnl_pct * 100, 'net_pnl': net,
            'roi_pct': net / PRINCIPAL_USDT * 100,
        })

    # === 输出 ===
    print()
    print("=" * 78)
    print(f"📊 {date_str} 回测结果 (首次看涨信号做多, 动态1:{RR_RATIO})")
    print("=" * 78)

    if not results:
        print("无结果")
    else:
        print(f"\n{'信号时刻':<9} {'币种':<14} {'信号':<7} {'入场':>10} {'止损':>10} {'止盈':>10} "
              f"{'出场':>10} {'出场时':>6} {'盈亏%':>8} {'净利U':>8} {'理由':<11}")
        print("-" * 122)
        for r in results:
            print(f"{r['time'][11:]:<9} {r['symbol']:<14} {r['signal_type']:<7} "
                  f"${r['entry']:>9.6f} ${r['stop_loss']:>9.6f} ${r['take_profit']:>9.6f} "
                  f"${r['actual_exit']:>9.6f} {r['exit_time']:>6} "
                  f"{r['pnl_pct']:>+7.2f}% {r['net_pnl']:>+7.2f} {r['exit_reason']:<11}")

        total_net = sum(r['net_pnl'] for r in results)
        win = sum(1 for r in results if r['net_pnl'] > 0)
        stop_out = sum(1 for r in results if r['exit_reason'] == 'stop_loss')
        tp_hit = sum(1 for r in results if r['exit_reason'] == 'take_profit')
        period_end = sum(1 for r in results if r['exit_reason'] == 'period_end')
        avg_risk = sum(r['risk_pct'] for r in results) / len(results)

        print("-" * 122)
        print(f"\n🎯 总计: {len(results)} 笔 | 盈: {win} | 亏: {len(results) - win} | 胜率: {win / len(results) * 100:.1f}%")
        print(f"💰 净收益: {total_net:+.2f}U (每笔本金 {PRINCIPAL_USDT}U × {LEVERAGE}x)")
        print(f"📈 止盈 {tp_hit} 笔 | 止损 {stop_out} 笔 | 收盘强平 {period_end} 笔 | 平均风险距离 {avg_risk:.2f}%")

        print(f"\n📊 按信号类型:")
        by_type = defaultdict(lambda: {'count': 0, 'pnl': 0.0, 'win': 0})
        for r in results:
            by_type[r['signal_type']]['count'] += 1
            by_type[r['signal_type']]['pnl'] += r['net_pnl']
            if r['net_pnl'] > 0:
                by_type[r['signal_type']]['win'] += 1
        for t, s in by_type.items():
            print(f"  {t:<10}: {s['count']:>3} 笔 | 净利 {s['pnl']:>+8.2f}U | 胜率 {s['win'] / s['count'] * 100:.1f}%")

    if skipped:
        print(f"\n⏭️  跳过 ({len(skipped)} 条):")
        for s in skipped[:15]:
            print(f"  - {s}")
        if len(skipped) > 15:
            print(f"  ... 还有 {len(skipped) - 15} 条")


if __name__ == '__main__':
    run_backtest(sys.argv[1] if len(sys.argv) > 1 else None)
