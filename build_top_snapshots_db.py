#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
从 alerts.db 重建多日 Top 榜快照 (纯 DB, 不请求 Binance, 无 429 风险)

与 backtest_snapshots_multi.py 口径一致:
  · Top 名单 = 截至 as_of 的 urgent 信号, 24h 活跃, 12h 连续段, count>=min_count
  · 每币输出 k=1..MAX_K 个入场点 (第1次/第2次/... 信号)
  · signal_time = 该入场点的信号时间

差别: 旧脚本会顺手拉 K线算 highest/roi, 本脚本只出快照行, 不需要网络。
"""
import argparse
import csv
import datetime as dt
import os
import sqlite3
import sys
from collections import defaultdict

TZ = dt.timezone(dt.timedelta(hours=8))
ALERTS_DB = "/root/CryptoMonitor/alerts.db"

DIR_SIGNAL = {
    "🚀暴涨": 1, "🚀暴涨3m": 1, "📈爬升": 1,
    "🩸暴跌": -1, "🩸暴跌3m": -1, "📉阴跌": -1,
}


def parse_db_dt(value):
    """alerts.db 的 timestamp 是 'YYYY-MM-DD HH:MM:SS' 本地(UTC+8) 字符串。"""
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        parsed = dt.datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=TZ)
    return parsed.astimezone(TZ)


def fmt_dt(value):
    return value.strftime("%Y-%m-%d %H:%M:%S")


def load_all_signals(conn):
    """一次性载入全部 urgent 信号 (按 symbol, timestamp 排序)。"""
    rows = conn.execute(
        """SELECT timestamp, symbol, signal_type, price_at_alert
             FROM alerts
            WHERE priority='urgent' AND price_at_alert IS NOT NULL
            ORDER BY symbol ASC, timestamp ASC"""
    ).fetchall()
    by_symbol = defaultdict(list)
    for ts, sym, stype, price in rows:
        t = parse_db_dt(ts)
        try:
            p = float(price)
        except (TypeError, ValueError):
            continue
        sym = str(sym or "").strip().upper()
        if not t or not sym or p <= 0:
            continue
        by_symbol[sym].append((t, str(stype or "").strip(), p))
    return by_symbol


def load_segment(parsed, as_of, active_hours=24, gap_hours=12):
    """取该币在 as_of 时的 12h 连续段内全部信号 (只含 as_of 之前的)。

    返回 (segment, display_start)
      segment       = 连续段内的信号列表
      display_start = 当前 Top 源码显示的"首次信号时间"(段内第一条)
    """
    if not parsed:
        return [], None
    # 只考虑 as_of 之前的信号 (防止用未来信号)
    parsed = [x for x in parsed if x[0] <= as_of]
    if not parsed:
        return [], None
    last = parsed[-1][0]
    if (as_of - last).total_seconds() > active_hours * 3600:
        return [], None
    start = len(parsed) - 1
    for i in range(len(parsed) - 2, -1, -1):
        if (parsed[i + 1][0] - parsed[i][0]).total_seconds() > gap_hours * 3600:
            start = i + 1
            break
    segment = parsed[start:]
    return segment, (segment[0][0] if segment else None)


def build_snapshot(by_symbol, as_of, limit, min_count):
    """返回该时点的 Top 名单: [(symbol, segment, display_start, count)]"""
    scored = []
    for sym, parsed in by_symbol.items():
        segment, display_start = load_segment(parsed, as_of)
        if not segment:
            continue
        scored.append((sym, segment, display_start, len(segment)))
    scored.sort(key=lambda x: (-x[3], x[0]))
    if min_count > 0:
        scored = [x for x in scored if x[3] >= min_count]
    if limit > 0:
        scored = scored[:limit]
    return scored


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2026-09-19 00:00:00")
    ap.add_argument("--end", default="2026-11-13 00:00:00")
    ap.add_argument("--step-hours", type=int, default=24)
    ap.add_argument("--limit", type=int, default=500)
    ap.add_argument("--min-count", type=int, default=2)
    ap.add_argument("--max-k", type=int, default=5)
    ap.add_argument("--out", default="/root/CryptoMonitor/backtest_snapshots/snapshot_rows_full.csv")
    ap.add_argument("--db", default=ALERTS_DB)
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    print("载入 alerts.db ...", flush=True)
    by_symbol = load_all_signals(conn)
    conn.close()
    print(f"  {len(by_symbol)} 个币, {sum(len(v) for v in by_symbol.values())} 条信号")

    t0 = dt.datetime.fromisoformat(args.start).replace(tzinfo=TZ)
    t1 = dt.datetime.fromisoformat(args.end).replace(tzinfo=TZ)
    step = dt.timedelta(hours=args.step_hours)
    as_of_list = []
    cur = t0
    while cur <= t1:
        as_of_list.append(cur)
        cur += step
    print(f"  快照时点 {len(as_of_list)} 个: {fmt_dt(t0)} → {fmt_dt(t1)} (每 {args.step_hours}h)")

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    rows_out = []
    for idx, as_of in enumerate(as_of_list, 1):
        top = build_snapshot(by_symbol, as_of, args.limit, args.min_count)
        for sym, segment, display_start, cnt in top:
            for k in range(1, min(args.max_k, len(segment)) + 1):
                t, stype, price = segment[k - 1]
                rows_out.append({
                    "as_of": fmt_dt(as_of),
                    "symbol": sym,
                    "k": k,
                    "seg_count": cnt,
                    "signal_time": fmt_dt(t),
                    "signal_type": stype,
                    "direction": DIR_SIGNAL.get(stype, 0),
                    "entry_price": price,
                })
        print(f"  [{idx}/{len(as_of_list)}] {fmt_dt(as_of)}  Top={len(top):>4}  累计 {len(rows_out)} 行", flush=True)

    with open(args.out, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "as_of", "symbol", "k", "seg_count", "signal_time", "signal_type",
            "direction", "entry_price",
        ])
        writer.writeheader()
        writer.writerows(rows_out)
    print(f"写出 {len(rows_out)} 行 → {args.out}")


if __name__ == "__main__":
    sys.exit(main())
