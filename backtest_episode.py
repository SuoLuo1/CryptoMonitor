#!/usr/bin/env python3
"""
Episode 波段连续回测 (v10) —— 修复"分天回测把延续信号当首次信号"的口径缺陷

背景 (主人指出的问题):
    分天回测里"当天首次信号"是人为日切: 一个币昨天就在报警拉升,
    今天 00:02 再报警会被当成"新机会"进场 —— 实际那是行情的延续,
    很可能是回调段/出货末期。实测 9-19~9-26 每天 34%~66% 的"首次信号"
    昨天就有信号。同时 23:59 强制平仓也是日切失真。

Episode 口径:
    1. 波段定义: 某币两次看涨信号间隔 > GAP 小时(静默期) → 新波段开始
       波段起点信号才允许进场 (延续信号一律跳过)
    2. 单币单仓: 持仓未平时, 该币所有新信号忽略 (不加仓不重进)
    3. 平仓后: 下一个波段起点信号才能再进场
    4. 持仓跨天: 不再 23:59 强平, 直到触发止损/止盈/移动止盈,
       或达到 MAX_HOLD_DAYS 上限, 或数据尽头 (data_end, 按最后收盘价估值)
    5. 入场/止损与 v8 完全一致:
       entry = price_at_alert; 止损 = 信号前已收盘K线 low × 0.98 (爬升=前60根1m最低)
    6. 出场模式:
       rr       固定止盈 entry + risk×3 (动态1:3), 同根K线止损优先
       trailing 移动止盈: 出场线 = max(初始止损, peak×(1-trail%)), 保守判定
    7. 仓位: 每笔独立 10U × 10x = 100U 名义, 手续费开平共 0.08%

用法:
    python3 backtest_episode.py 2026-09-19 2026-09-26 trailing 3 12
                                start      end        mode   trail% gap小时
    python3 backtest_episode.py 2026-09-19 2026-09-26 rr
    python3 backtest_episode.py 2026-09-19 2026-09-25 rr 3 12 \
        --reconstructed-signals backtest_results/rebuilt_signals_2026-09-18_2026-09-25.csv

信号源:
    默认读取 alerts.db 的生产入库记录。--reconstructed-signals 在保留所有
    原始入库信号的基础上，仅追加重建文件中 source=rebuilt 的漏报看涨信号，
    不向 alerts.db 回写任何数据；输出文件会追加 _repaired，避免覆盖原始
    数据库口径的回测结果。
输出:
    终端汇总 + backtest_results/episode_<mode>_<gap>h_<start>_<end>.csv
"""
import sys
import os
import csv
import math
import re
import sqlite3
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, '/root/CryptoMonitor')
import backtest_today as bt   # 复用K线缓存/结构低点逻辑 (与v8同源, 已通过独立验证)

BJ = bt.BJ_TZ
DB_PATH = bt.DB_PATH
NOTIONAL = bt.NOTIONAL          # 100U
PRINCIPAL = bt.PRINCIPAL_USDT   # 10U
FEE = bt.MAKER_FEE_PCT          # 0.0008
SL_BUF = bt.STOP_LOSS_PCT       # 0.02
RR = bt.RR_RATIO                # 3
LONG_SIGNALS = bt.LONG_SIGNALS  # 🚀暴涨/🚀暴涨3m/📈爬升 -> interval, window
INTERVAL_MS = bt.INTERVAL_MS
MAX_HOLD_DAYS = 7               # 持仓上限(天), 到期按收盘价强平

_ALERT_PRICE_PATTERN = re.compile(
    r'现价[:：]\s*'
    r'([0-9]+(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?)'
)


def price_from_alert_message(message):
    """Recover an alert price without losing scientific notation."""
    match = _ALERT_PRICE_PATTERN.search(message or '')
    if not match:
        return None
    try:
        price = float(match.group(1))
    except ValueError:
        return None
    return price if math.isfinite(price) and price > 0 else None


def day_list(start, end):
    d = datetime.strptime(start, '%Y-%m-%d').replace(tzinfo=BJ)
    e = datetime.strptime(end, '%Y-%m-%d').replace(tzinfo=BJ)
    out = []
    while d <= e:
        out.append(d.strftime('%Y-%m-%d'))
        d += timedelta(days=1)
    return out


def load_long_signals(start, end, reconstructed_signals=None, warmup_hours=0.0):
    """按币分组读取看涨信号，并返回来源说明与回测起始时间。"""
    evaluation_start = datetime.strptime(
        f'{start} 00:00:00', '%Y-%m-%d %H:%M:%S'
    ).replace(tzinfo=BJ)
    signal_start = evaluation_start - timedelta(hours=warmup_hours)
    evaluation_end = datetime.strptime(
        f'{end} 23:59:59', '%Y-%m-%d %H:%M:%S'
    ).replace(tzinfo=BJ)

    conn = sqlite3.connect(f'file:{DB_PATH}?mode=ro', uri=True)
    database_rows = conn.execute("""SELECT timestamp, symbol, signal_type, price_at_alert, message FROM alerts
        WHERE priority='urgent' AND signal_type IN (?,?,?)
        AND timestamp >= ? AND timestamp <= ? ORDER BY timestamp""",
        (*LONG_SIGNALS.keys(), signal_start.strftime('%Y-%m-%d %H:%M:%S'),
         evaluation_end.strftime('%Y-%m-%d %H:%M:%S'))).fetchall()
    conn.close()

    rows = []
    recovered_prices = 0
    for timestamp, symbol, signal_type, stored_price, message in database_rows:
        message_price = price_from_alert_message(message)
        price = message_price if message_price is not None else stored_price
        if message_price is not None and message_price != stored_price:
            recovered_prices += 1
        rows.append((timestamp, symbol, signal_type, price, 'db'))
    identities = {(timestamp, symbol, signal_type)
                  for timestamp, symbol, signal_type, _, _ in database_rows}
    source_label = f'alerts.db 生产入库信号（从原始消息恢复 {recovered_prices} 条价格）'
    reconstructed_added = 0

    if reconstructed_signals is not None:
        source_path = Path(reconstructed_signals)
        if not source_path.is_file():
            raise FileNotFoundError(f'重建信号文件不存在: {source_path}')

        required_columns = {'timestamp', 'symbol', 'signal_type', 'price', 'source'}
        with source_path.open(newline='', encoding='utf-8-sig') as fh:
            reader = csv.DictReader(fh)
            if reader.fieldnames is None or not required_columns.issubset(reader.fieldnames):
                raise ValueError(f'重建信号文件缺少必要列: {source_path}')

            for row in reader:
                timestamp = (row['timestamp'] or '').strip()
                symbol = (row['symbol'] or '').strip()
                signal_type = (row['signal_type'] or '').strip()

                try:
                    timestamp_dt = datetime.strptime(
                        timestamp, '%Y-%m-%d %H:%M:%S'
                    ).replace(tzinfo=BJ)
                    price = float(row['price'])
                except (TypeError, ValueError) as exc:
                    raise ValueError(f'重建信号文件第 {reader.line_num} 行格式无效') from exc
                if signal_type not in LONG_SIGNALS or not (signal_start <= timestamp_dt <= evaluation_end):
                    continue
                if row['source'] != 'rebuilt':
                    continue
                if not symbol or price <= 0:
                    raise ValueError(f'重建信号文件第 {reader.line_num} 行缺少有效币种或价格')

                identity = (timestamp, symbol, signal_type)
                if identity in identities:
                    continue
                identities.add(identity)
                rows.append((timestamp, symbol, signal_type, price, 'rebuilt'))
                reconstructed_added += 1

        source_label = (
            f'alerts.db + {reconstructed_added} 条重建漏报信号 '
            f'({source_path.name})'
        )

    by_sym = defaultdict(list)
    for ts, sym, sig, price, source in rows:
        by_sym[sym].append((ts, sig, price, source))
    return by_sym, source_label, evaluation_start


def kline_stream(symbol, interval, from_day, ts_ms, until_day):
    """拼接 from_day..until_day 的K线流 (只取 >= ts_ms 的), 供跨天结算"""
    out = []
    for day in day_list(from_day, until_day):
        ks = bt.get_day_klines(symbol, interval, day)
        for k in ks:
            if k[0] >= ts_ms:
                out.append(k)
    return out


def simulate_trade(sym, ts_str, sig, price_at, mode, trail_pct, data_end,
                   signal_source='db', eod_close=False):
    """模拟单笔交易, 返回 trade dict 或 None(跳过原因)
    eod_close=True: 与旧分天回测一致, 当天23:59收盘强平
    eod_close=False: 持仓跨天直到止损/止盈触发 (上限MAX_HOLD_DAYS天)"""
    info = LONG_SIGNALS[sig]
    interval, window = info['interval'], info['window']
    date_str = ts_str[:10]
    ts_ms = int(datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=BJ).timestamp() * 1000)

    if window == 1:
        min_low, last_close = bt.prev_closed_kline(sym, interval, ts_ms, date_str)
    else:
        min_low, last_close = bt.window_min_low(sym, ts_ms, window, date_str)
    if min_low is None:
        return None, 'nokline'
    stop = min_low * (1 - SL_BUF)
    entry = price_at if (price_at and price_at > 0) else last_close
    if not entry or entry <= 0:
        return None, 'noprice'
    risk = entry - stop
    if risk <= 0:
        return None, 'risk<=0'
    tp = entry + risk * RR

    if eod_close:
        until = date_str
    else:
        until = (datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=BJ)
                 + timedelta(days=MAX_HOLD_DAYS)).strftime('%Y-%m-%d')
        if until > data_end:
            until = data_end
    period = kline_stream(sym, interval, date_str, ts_ms, until)
    if not period:
        return None, 'noperiod'

    actual_exit = None
    reason = 'data_end'
    exit_ts = period[-1][0]
    if mode == 'trailing':
        peak = entry
        for k in period:
            k_low, k_high = float(k[3]), float(k[2])
            line = max(stop, peak * (1 - trail_pct / 100))
            if k_low <= line:
                actual_exit = line
                reason = 'trailing_stop' if line > stop else 'stop_loss'
                exit_ts = k[0]
                break
            peak = max(peak, k_high)
    else:
        for k in period:
            k_low, k_high = float(k[3]), float(k[2])
            if k_low <= stop:
                actual_exit, reason, exit_ts = stop, 'stop_loss', k[0]
                break
            if k_high >= tp:
                actual_exit, reason, exit_ts = tp, 'take_profit', k[0]
                break
    if actual_exit is None:
        actual_exit = float(period[-1][4])
        # 区分"到期强平"和"数据尽头仍持仓"
        last_day = datetime.fromtimestamp(period[-1][0] / 1000, BJ).strftime('%Y-%m-%d')
        reason = 'data_end' if last_day >= data_end else 'max_hold'

    hold_h = (exit_ts - ts_ms) / 3600_000
    pnl_pct = (actual_exit - entry) / entry
    net = NOTIONAL * pnl_pct - NOTIONAL * FEE
    return {
        'entry_time': ts_str, 'symbol': sym, 'signal_type': sig,
        'signal_source': signal_source,
        'entry': entry, 'stop': stop, 'tp': tp,
        'exit': actual_exit, 'exit_time': datetime.fromtimestamp(exit_ts / 1000, BJ).strftime('%m-%d %H:%M'),
        'reason': reason, 'hold_hours': hold_h,
        'risk_pct': risk / entry * 100, 'pnl_pct': pnl_pct * 100,
        'net': net, 'roi_pct': net / PRINCIPAL * 100,
    }, None


def run(start, end, mode='rr', trail_pct=3.0, gap_hours=12.0, eod_close=False,
        reconstructed_signals=None):
    data_end = datetime.now(BJ).strftime('%Y-%m-%d')
    if data_end < end:
        data_end = end
    today = datetime.now(BJ).strftime('%Y-%m-%d')
    if data_end > today:
        data_end = today

    print(f"==> Episode 回测: {start} ~ {end} | 出场={mode}"
          + (f"(回撤{trail_pct:g}%)" if mode == 'trailing' else f"(1:{RR})")
          + f" | 静默期={gap_hours:g}h | 持仓: {'当天23:59收盘平(与分天版一致)' if eod_close else f'跨天(上限{MAX_HOLD_DAYS}天)'}")
    print(f"==> 仓位: {PRINCIPAL}U × 10x = {NOTIONAL}U/笔 | 手续费 {FEE*100:.2f}% | 数据尽头: {data_end}")
    if start == '2026-09-19':
        print("⚠️  注意: DB 在 04-17~09-19 之间无数据, 09-19 当天的波段起点判定无昨日参照 (该日结果略偏乐观)")
    print("=" * 78)

    warmup_hours = gap_hours if reconstructed_signals is not None else 0.0
    by_sym, source_label, evaluation_start = load_long_signals(
        start,
        end,
        reconstructed_signals=reconstructed_signals,
        warmup_hours=warmup_hours,
    )
    print(f"==> 信号源: {source_label}")
    if warmup_hours:
        print(f"==> 波段预热: 回测前 {warmup_hours:g}h 信号仅用于判定延续/持仓，不计入结果")
    gap_ms = gap_hours * 3600_000
    evaluation_start_ms = int(evaluation_start.timestamp() * 1000)

    trades = []
    stats = {'cont_skip': 0, 'open_skip': 0, 'total_sig': 0, 'warmup_sig': 0}
    skip_reasons = defaultdict(int)

    for sym, sigs in by_sym.items():
        sigs.sort(key=lambda x: x[0])
        last_sig_ms = None      # 该币上一个看涨信号 (不论是否交易)
        pos_exit_ms = None      # 当前/最近一笔的出场时刻
        for ts_str, sig, price_at, signal_source in sigs:
            ts_ms = int(datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=BJ).timestamp() * 1000)
            is_evaluation_signal = ts_ms >= evaluation_start_ms
            if is_evaluation_signal:
                stats['total_sig'] += 1
            else:
                stats['warmup_sig'] += 1
            is_episode_start = (last_sig_ms is None) or (ts_ms - last_sig_ms > gap_ms)
            last_sig_ms = ts_ms
            if not is_evaluation_signal:
                if is_episode_start and (pos_exit_ms is None or ts_ms >= pos_exit_ms):
                    warmup_trade, _ = simulate_trade(
                        sym, ts_str, sig, price_at, mode, trail_pct, data_end,
                        signal_source=signal_source, eod_close=eod_close,
                    )
                    if warmup_trade is not None:
                        warmup_exit = datetime.strptime(
                            '2026-' + warmup_trade['exit_time'], '%Y-%m-%d %H:%M'
                        ).replace(tzinfo=BJ)
                        pos_exit_ms = int(warmup_exit.timestamp() * 1000)
                continue
            if not is_episode_start:
                stats['cont_skip'] += 1     # 延续信号 → 跳过 (主人指出的那批)
                continue
            if pos_exit_ms is not None and ts_ms < pos_exit_ms:
                stats['open_skip'] += 1     # 持仓未平 → 跳过
                continue
            trade, skip = simulate_trade(
                sym, ts_str, sig, price_at, mode, trail_pct, data_end,
                signal_source=signal_source, eod_close=eod_close,
            )
            if trade is None:
                skip_reasons[skip] += 1
                continue
            ex = datetime.strptime('2026-' + trade['exit_time'], '%Y-%m-%d %H:%M').replace(tzinfo=BJ)
            pos_exit_ms = int(ex.timestamp() * 1000)
            trades.append(trade)

    trades.sort(key=lambda t: t['entry_time'])

    # CSV 落盘
    out_dir = '/root/CryptoMonitor/backtest_results'
    os.makedirs(out_dir, exist_ok=True)
    tag = f"{mode}{trail_pct:g}" if mode == 'trailing' else 'rr1x3'
    if eod_close:
        tag += '_eod'
    source_suffix = '_repaired' if reconstructed_signals is not None else ''
    csv_path = f"{out_dir}/episode_{tag}_{gap_hours:g}h_{start}_{end}{source_suffix}.csv"
    with open(csv_path, 'w', newline='', encoding='utf-8-sig') as fh:
        w = csv.writer(fh)
        w.writerow(['entry_time', 'symbol', 'signal_type', 'signal_source', 'entry', 'stop_loss', 'take_profit',
                    'exit_price', 'exit_time', 'exit_reason', 'hold_hours',
                    'risk_pct', 'pnl_pct', 'net_pnl_u', 'roi_pct'])
        for t in trades:
            w.writerow([t['entry_time'], t['symbol'], t['signal_type'], t['signal_source'],
                        f"{t['entry']:.8f}", f"{t['stop']:.8f}", f"{t['tp']:.8f}",
                        f"{t['exit']:.8f}", t['exit_time'], t['reason'], f"{t['hold_hours']:.1f}",
                        f"{t['risk_pct']:.2f}", f"{t['pnl_pct']:.2f}", f"{t['net']:.2f}", f"{t['roi_pct']:.2f}"])

    # 汇总
    print(f"\n信号总数 {stats['total_sig']} | 预热信号 {stats['warmup_sig']} | 延续信号跳过 {stats['cont_skip']} "
          f"({stats['cont_skip']/max(stats['total_sig'],1)*100:.0f}%) | 持仓中跳过 {stats['open_skip']} "
          f"| 数据不足跳过 {sum(skip_reasons.values())} {dict(skip_reasons) if skip_reasons else ''}")
    if not trades:
        print("无交易"); return
    total = sum(t['net'] for t in trades)
    win = sum(1 for t in trades if t['net'] > 0)
    print(f"\n🎯 总笔数: {len(trades)} | 盈 {win} 亏 {len(trades)-win} | 胜率 {win/len(trades)*100:.1f}%")
    print(f"💰 净收益: {total:+.2f}U | 单笔期望 {total/len(trades):+.3f}U")
    reasons = defaultdict(lambda: [0, 0.0])
    for t in trades:
        reasons[t['reason']][0] += 1; reasons[t['reason']][1] += t['net']
    print("📈 出场分布:", " | ".join(f"{r} {c}笔 {p:+.1f}U" for r, (c, p) in reasons.items()))
    holds = sorted(t['hold_hours'] for t in trades)
    print(f"⏱ 持仓时长: 中位 {holds[len(holds)//2]:.1f}h | p90 {holds[int(len(holds)*0.9)]:.1f}h | 最长 {holds[-1]:.1f}h")

    print(f"\n📅 按进场日:")
    by_day = defaultdict(lambda: [0, 0.0, 0])
    for t in trades:
        d = t['entry_time'][:10]
        by_day[d][0] += 1; by_day[d][1] += t['net']; by_day[d][2] += 1 if t['net'] > 0 else 0
    for d in sorted(by_day):
        c, p, w = by_day[d]
        print(f"  {d}: {c:>3}笔 {p:>+9.2f}U 胜率{w/c*100:>3.0f}%")

    print(f"\n📊 按信号类型:")
    by_sig = defaultdict(lambda: [0, 0.0, 0])
    for t in trades:
        by_sig[t['signal_type']][0] += 1; by_sig[t['signal_type']][1] += t['net']
        by_sig[t['signal_type']][2] += 1 if t['net'] > 0 else 0
    for s, (c, p, w) in by_sig.items():
        print(f"  {s:<10}: {c:>3}笔 {p:>+9.2f}U 胜率{w/c*100:>3.0f}%")

    by_source = defaultdict(lambda: [0, 0.0, 0])
    for t in trades:
        by_source[t['signal_source']][0] += 1
        by_source[t['signal_source']][1] += t['net']
        by_source[t['signal_source']][2] += 1 if t['net'] > 0 else 0
    print("\n📌 按信号来源:")
    for source, (count, pnl, wins) in sorted(by_source.items()):
        print(f"  {source:<10}: {count:>3}笔 {pnl:>+9.2f}U 胜率{wins/count*100:>3.0f}%")

    print(f"\n📁 明细: {csv_path}")


if __name__ == '__main__':
    a = sys.argv[1:]
    _reconstructed_signals = None
    if '--reconstructed-signals' in a:
        source_index = a.index('--reconstructed-signals')
        if source_index + 1 >= len(a):
            raise SystemExit('--reconstructed-signals 需要提供重建信号 CSV 路径')
        _reconstructed_signals = a[source_index + 1]
        del a[source_index:source_index + 2]
    _start = a[0] if len(a) > 0 else '2026-09-19'
    _end = a[1] if len(a) > 1 else '2026-09-26'
    _mode = a[2] if len(a) > 2 else 'rr'
    _trail = float(a[3]) if len(a) > 3 else 3.0
    _gap = float(a[4]) if len(a) > 4 else 12.0
    _eod = 'eod' in a[5:]  # 可选: 当天收盘强平 (与旧分天口径对齐)
    run(_start, _end, _mode, _trail, _gap, eod_close=_eod,
        reconstructed_signals=_reconstructed_signals)
