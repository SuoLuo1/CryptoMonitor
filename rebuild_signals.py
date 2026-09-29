#!/usr/bin/env python3
"""
重建完整信号集 (9-18 ~ 9-28) —— 把 DB 里因故障漏掉的信号补回来

为什么需要:
    9-19~9-25 期间服务处于 WS 反复重连 + 内存节流冻结状态,
    rolling_window(内存deque) 断档/清零 → 该报的信号没触发 → DB 残缺
    (实测: 9-19 爬升完整度仅12%, 9-21 28%, 9-22 49%, 9-23 52%)
    用残缺 DB 回测 = 拿"幸存者"评估策略, 结论必然失真。

做法:
    复刻 monitors.py 三种信号的全部条件, 从K线缓存反推【应该产生的每一条信号】,
    输出完整信号表 (时间戳/币/类型/报警价), 再标注哪些是 DB 已有的、哪些是补的。

复刻条件 (与 audit_signal_completeness.py 一致):
  A) 📈爬升/📉阴跌: q>=5000U; deque(60)收盘价 append后判断; len>=10;
     |(close-win[0])/win[0]| > 5%; 冷却1800s (key={sym}_TREND, 涨跌共用)
  B) 🚀暴涨/🩸暴跌: q>=5000U; |(close-open)/open| > 5%; 冷却60s (PUMP/DUMP分开)
  C) 🚀暴涨3m/🩸暴跌3m: q>=5000U; |3m涨跌|>2% 且 本根q > 前5根均量×3; 冷却60s

⚠️ 方法论限制 (必须声明):
  B/C 类生产是在 WS 流式 tick 上判断(含未收盘K线的瞬时值), 审计只能用已收盘K线,
  所以 B/C 的重建是【下界】—— 真实信号数 >= 重建数 (盘中冲高回落的抓不到)。
  A 类(趋势)用收盘价序列, 与生产口径基本一致, 重建最可信。
  → 重建后的回测: A类结论可信, B/C类偏保守(真实信号更多, 但方向一致)

内存控制: 逐币处理, 处理完即释放 (不一次性载入全部K线, 避免OOM)
输出: backtest_results/rebuilt_signals_<start>_<end>.csv
      字段: timestamp, symbol, signal_type, price, in_db(1=DB已有/0=补的漏报)
"""
import sqlite3
import json
import os
import sys
import csv
import gc
from datetime import datetime, timezone, timedelta
from collections import deque, defaultdict

BJ = timezone(timedelta(hours=8))
DB_PATH = '/root/CryptoMonitor/alerts.db'
CACHE = '/tmp/backtest_cache_v8'

VOL_MIN = 5000.0
TREND_TH = 0.05
TREND_COOLDOWN = 1800_000
WINDOW_SIZE = 60
WINDOW_MIN = 10
PUMP_TH = 0.05
PUMP_COOLDOWN = 60_000
PUMP3M_TH = 0.02
VOL_MA5_MULT = 3.0
VOL3M_WIN = 5


def load_day(symbol, interval, date_str):
    ck = f"{CACHE}/{symbol}_{interval}_{date_str}.json"
    if not os.path.exists(ck):
        return None
    try:
        return json.load(open(ck))
    except Exception:
        return None


def sim_1m(klines, day_start_ms, day_end_ms, out):
    """复刻 process_kline 1m → 追加信号明细到 out"""
    win = deque(maxlen=WINDOW_SIZE)
    last_trend = -10**15
    last_pump = -10**15
    last_dump = -10**15
    for k in klines:
        if float(k[7]) < VOL_MIN:
            continue
        c, o, t = float(k[4]), float(k[1]), k[0]
        win.append(c)
        if not (day_start_ms <= t < day_end_ms):
            continue
        if len(win) >= WINDOW_MIN and win[0] > 0:
            trend = (c - win[0]) / win[0]
            if abs(trend) > TREND_TH and t - last_trend > TREND_COOLDOWN:
                last_trend = t
                out.append((t, '📈爬升' if trend > 0 else '📉阴跌', c))
        if o > 0:
            chg = (c - o) / o
            if chg > PUMP_TH and t - last_pump >= PUMP_COOLDOWN:
                last_pump = t
                out.append((t, '🚀暴涨', c))
            elif chg < -PUMP_TH and t - last_dump >= PUMP_COOLDOWN:
                last_dump = t
                out.append((t, '🩸暴跌', c))


def sim_3m(klines, day_start_ms, day_end_ms, out):
    """复刻 _process_kline_3m → 追加信号明细"""
    vols = deque(maxlen=VOL3M_WIN)
    last_p = -10**15
    last_d = -10**15
    for k in klines:
        q = float(k[7])
        if q < VOL_MIN:
            continue
        o, c, t = float(k[1]), float(k[4]), k[0]
        if day_start_ms <= t < day_end_ms and len(vols) >= VOL3M_WIN and o > 0:
            chg = (c - o) / o
            ma5 = sum(vols) / len(vols)
            if abs(chg) > PUMP3M_TH and ma5 > 0 and q > ma5 * VOL_MA5_MULT:
                if chg > 0 and t - last_p >= PUMP_COOLDOWN:
                    last_p = t
                    out.append((t, '🚀暴涨3m', c))
                elif chg < 0 and t - last_d >= PUMP_COOLDOWN:
                    last_d = t
                    out.append((t, '🩸暴跌3m', c))
        vols.append(q)


def main():
    start_d = sys.argv[1] if len(sys.argv) > 1 else '2026-09-19'
    end_d = sys.argv[2] if len(sys.argv) > 2 else '2026-09-28'
    days = []
    cur = datetime.strptime(start_d, '%Y-%m-%d').replace(tzinfo=BJ)
    while cur <= datetime.strptime(end_d, '%Y-%m-%d').replace(tzinfo=BJ):
        days.append(cur.strftime('%Y-%m-%d'))
        cur += timedelta(days=1)
    warm = (datetime.strptime(days[0], '%Y-%m-%d').replace(tzinfo=BJ) - timedelta(days=1)).strftime('%Y-%m-%d')

    conn = sqlite3.connect(DB_PATH)
    syms = [r[0] for r in conn.execute(
        "SELECT DISTINCT symbol FROM alerts WHERE timestamp >= ? AND timestamp < datetime(?, '+1 day')",
        (f'{days[0]} 00:00:00', days[-1]))]
    # DB 已有信号集合 (symbol, type, 时间戳精确到秒) 用于标注 in_db
    print(f"载入 DB 已有信号索引...", flush=True)
    db_set = set()
    for ts, sym, sig in conn.execute(
            "SELECT timestamp, symbol, signal_type FROM alerts WHERE priority='urgent' AND timestamp >= ? AND timestamp < datetime(?, '+1 day')",
            (f'{days[0]} 00:00:00', days[-1])):
        db_set.add((sym, sig, ts))
    # 同时建一个"同币同类型±5分钟"的宽松索引 (生产是流式tick, 时间戳会与K线收盘差几十秒)
    db_loose = defaultdict(set)
    for ts, sym, sig in conn.execute(
            "SELECT timestamp, symbol, signal_type FROM alerts WHERE priority='urgent' AND timestamp >= ? AND timestamp < datetime(?, '+1 day')",
            (f'{days[0]} 00:00:00', days[-1])):
        db_loose[(sym, sig)].add(int(datetime.strptime(ts, '%Y-%m-%d %H:%M:%S').replace(tzinfo=BJ).timestamp()))
    conn.close()
    print(f"  DB urgent 信号 {len(db_set)} 条, 币数 {len(syms)}", flush=True)

    out_dir = '/root/CryptoMonitor/backtest_results'
    os.makedirs(out_dir, exist_ok=True)
    out_path = f"{out_dir}/rebuilt_signals_{start_d}_{end_d}.csv"

    # 🐾 三层嵌套: day -> signal_type -> {'rebuilt','matched','missing'} -> int
    # (写成 defaultdict(lambda: defaultdict(int)) 会让 stats[d][sig] 是 int 而非 dict)
    stats = defaultdict(lambda: defaultdict(lambda: {'rebuilt': 0, 'matched': 0, 'missing': 0}))
    total_rebuilt = total_matched = 0
    no_cache = 0

    with open(out_path, 'w', newline='', encoding='utf-8-sig') as fh:
        w = csv.writer(fh)
        w.writerow(['timestamp', 'symbol', 'signal_type', 'price', 'in_db', 'source'])
        for i, sym in enumerate(syms):
            if (i + 1) % 50 == 0:
                print(f"  进度 {i+1}/{len(syms)} | 重建{total_rebuilt} 匹配{total_matched} "
                      f"补漏{total_rebuilt-total_matched}", flush=True)
            w1 = load_day(sym, '1m', warm)
            w3 = load_day(sym, '3m', warm)
            for d in days:
                k1 = load_day(sym, '1m', d)
                k3 = load_day(sym, '3m', d)
                if k1 is None and k3 is None:
                    no_cache += 1
                    w1, w3 = None, None
                    continue
                ds = int(datetime.strptime(f'{d} 00:00:00', '%Y-%m-%d %H:%M:%S').replace(tzinfo=BJ).timestamp() * 1000)
                de = ds + 24 * 3600_000
                sigs = []
                if k1 is not None:
                    sim_1m((w1 or []) + k1, ds, de, sigs)
                if k3 is not None:
                    sim_3m((w3 or []) + k3, ds, de, sigs)
                for t_ms, sig, price in sorted(sigs, key=lambda x: x[0]):
                    ts_str = datetime.fromtimestamp(t_ms / 1000, BJ).strftime('%Y-%m-%d %H:%M:%S')
                    total_rebuilt += 1
                    stats[d][sig]['rebuilt'] += 1
                    # 宽松匹配: 同币同类型, DB里有 ±120秒 内的记录 → 认为已入库
                    cands = db_loose.get((sym, sig))
                    matched = 0
                    if cands:
                        for ct in cands:
                            if abs(ct - t_ms / 1000) <= 120:
                                matched = 1
                                break
                    if matched:
                        total_matched += 1
                        stats[d][sig]['matched'] += 1
                        w.writerow([ts_str, sym, sig, f"{price:.10f}", 1, 'db'])
                    else:
                        stats[d][sig]['missing'] += 1
                        w.writerow([ts_str, sym, sig, f"{price:.10f}", 0, 'rebuilt'])
                w1, w3 = k1, k3
            # 释放
            w1 = w3 = None
            if i % 100 == 99:
                gc.collect()

    print("\n" + "=" * 104)
    print("📊 信号重建结果 (rebuilt=K线反推应报, matched=DB已有, missing=补回的漏报)")
    print("=" * 104)
    print(f"{'日期':<12}{'重建':>7}{'DB已有':>8}{'补漏':>7}{'完整度':>8}   分类型补漏明细")
    print("-" * 104)
    for d in days:
        s = stats[d]
        rb = sum(v['rebuilt'] for v in s.values())
        mt = sum(v['matched'] for v in s.values())
        ms = sum(v['missing'] for v in s.values())
        if not rb:
            print(f"{d:<12}{0:>7}{0:>8}{0:>7}{'无缓存':>8}")
            continue
        det = " ".join(f"{k[1:]}+{v['missing']}" for k, v in sorted(s.items()) if v['missing'])
        print(f"{d:<12}{rb:>7}{mt:>8}{ms:>7}{mt/rb*100:>7.0f}%   {det}")
    print("-" * 104)
    print(f"合计: 重建 {total_rebuilt} 条 | DB已有 {total_matched} | 补回漏报 {total_rebuilt-total_matched} "
          f"({(total_rebuilt-total_matched)/max(total_rebuilt,1)*100:.0f}%)")
    print(f"无缓存跳过: {no_cache} 币日")
    print(f"\n📁 完整信号表: {out_path}")
    print("   in_db=1 → DB原有信号; in_db=0 → K线重建补回的漏报信号")
    print("\n⚠️ 声明: 🚀暴涨/🩸暴跌/3m类 重建值是【下界】(生产用流式tick, 盘中瞬时突破抓不到)")
    print("   📈爬升/📉阴跌 用收盘价序列, 与生产口径一致, 重建最可信")


if __name__ == '__main__':
    main()
