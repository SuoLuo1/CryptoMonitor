#!/usr/bin/env python3
"""
全量信号完整性审计 (9-19 ~ 9-28) —— 三种信号全覆盖

目的: 用K线反推"按生产逻辑应该报多少信号", 对比 alerts.db 实际入库数,
      定位哪些天数据残缺 (WS重连/内存节流导致 rolling_window 断档、信号没触发)

审计范围: 指定区间内任一天在DB出现过信号的币 (并集去重, 约555个)
          用并集而非"当天名单", 才能抓到"当天0条但本该报"的漏报
          (实证: ARPA 9-19 06:01 窗口+5.56% 应报爬升, DB 0条)

复刻 monitors.py 三种信号的全部条件:
  A) 📈爬升/📉阴跌 (process_kline, 1m)
     成交额 q >= 5000U (不足则整根跳过, 不进窗口)
     deque(maxlen=60) 存收盘价, 先 append 再判断, len >= 10 才判
     trend = (close - window[0]) / window[0], |trend| > 5%
     冷却 key=f"{sym}_TREND" 1800s —— 涨跌共用同一个key (生产如此)
  B) 🚀暴涨/🩸暴跌 (process_kline, 1m)
     成交额 q >= 5000U; change = (close-open)/open; |change| > 5%
     冷却 key=f"{sym}_PUMP" / f"{sym}_DUMP" 各 60s (涨跌分开)
  C) 🚀暴涨3m/🩸暴跌3m (_process_kline_3m, 3m)
     成交额 q >= 5000U; 已收盘K线的 q 进 deque(maxlen=5)
     |3m涨跌| > 2% 且 本根q > 前5根均量 × 3
     冷却 key=f"{sym}_3M_PUMP" / f"{sym}_3M_DUMP" 各 60s

已知近似 (必须声明):
  生产是在 WS 流式消息上逐笔判断(含未收盘K线), 审计只能用历史【已收盘】K线,
  所以 B/C 的"应报数"是近似值 —— 盘中瞬时冲破阈值后又收回的情况无法还原。
  A(趋势)用收盘价序列, 与生产口径最接近, 结论最可信。
  → 本审计用于发现【大规模缺失】(如完整度0%/40%), 不用于精确到个位数。

安全: 币安权重全局超过 1900 即等待下一分钟 (生产服务自身吃~600/min, 留足余量)
      绝不冒险触发418封IP, 那会让线上监控全挂

缓存: 复用 backtest_cache_v8 (与回测同格式 {sym}_{iv}_{date}.json), 已下载的K线不重复请求

用法: python3 audit_signal_completeness.py [start] [end]
      python3 audit_signal_completeness.py 2026-09-19 2026-09-28
"""
import sqlite3
import requests
import time
import json
import os
import sys
from datetime import datetime, timezone, timedelta
from collections import deque, defaultdict

BJ = timezone(timedelta(hours=8))
DB_PATH = '/root/CryptoMonitor/alerts.db'
# 复用回测的K线缓存 (格式完全兼容: {sym}_{interval}_{date}.json = 当天00:00~23:59)
CACHE = '/tmp/backtest_cache_v8'
os.makedirs(CACHE, exist_ok=True)
FUTURES = 'https://fapi.binance.com'

# === 生产参数 (monitors.py / config.py 实测值) ===
VOL_MIN = 5000.0           # 成交额门槛 (USDT)
TREND_TH = 0.05            # A: 60根窗口 5%
TREND_COOLDOWN = 1800_000  # A: 1800s, 涨跌共用key
WINDOW_SIZE = 60
WINDOW_MIN = 10
PUMP_TH = 0.05             # B: 1m单根 5%
PUMP_COOLDOWN = 60_000     # B/C: 60s, 涨跌分开
PUMP3M_TH = 0.02           # C: 3m涨跌 2%
VOL_MA5_MULT = 3.0         # C: 本根量 > 前5根均量 × 3
VOL3M_WIN = 5

WAIT_ABOVE = 1900          # 全局权重超过此值 → 等下一分钟 (给生产留 500+)

S = requests.Session()
_weight = {'used': 0, 'reset': time.time()}
_stats = {'req': 0, 'cache_hit': 0, 'wait': 0.0, 'throttled': 0}


def get_day_klines(symbol, interval, date_str):
    """拉取某币某天(北京时间)全天K线, 与回测共用缓存目录"""
    ck = f"{CACHE}/{symbol}_{interval}_{date_str}.json"
    if os.path.exists(ck):
        _stats['cache_hit'] += 1
        try:
            return json.load(open(ck))
        except Exception:
            pass
    ds = int(datetime.strptime(f'{date_str} 00:00:00', '%Y-%m-%d %H:%M:%S').replace(tzinfo=BJ).timestamp() * 1000)
    de = int(datetime.strptime(f'{date_str} 23:59:59', '%Y-%m-%d %H:%M:%S').replace(tzinfo=BJ).timestamp() * 1000)
    limit = 1500 if interval == '1m' else 500
    for att in range(5):
        if _weight['used'] > WAIT_ABOVE:
            wait = max(1.0, 61 - (time.time() - _weight['reset']))
            _stats['wait'] += wait
            time.sleep(wait)
        try:
            r = S.get(f"{FUTURES}/fapi/v1/klines", params={
                'symbol': symbol, 'interval': interval,
                'startTime': ds, 'endTime': de, 'limit': limit}, timeout=12)
            _stats['req'] += 1
            uw = r.headers.get('X-MBX-USED-WEIGHT-1M')
            if uw:
                _weight['used'] = int(uw)   # 币安返回的是全局权重
                if time.time() - _weight['reset'] > 60:
                    _weight['reset'] = time.time()
            if r.status_code in (418, 429):
                nap = min(int(r.headers.get('Retry-After', 30)), 120)
                _stats['throttled'] += 1
                print(f"  ⛔ 限流{r.status_code}, 睡{nap}s (全局weight={_weight['used']})", flush=True)
                time.sleep(nap)
                _weight['used'] = 0
                _weight['reset'] = time.time()
                continue
            r.raise_for_status()
            data = r.json()
            json.dump(data, open(ck, 'w'))
            time.sleep(0.02)
            return data
        except Exception as e:
            if att == 4:
                print(f"  !! {symbol} {interval} {date_str} 失败: {e}", flush=True)
                return []
            time.sleep(2 ** att)
    return []


def simulate_1m(klines, day_start_ms, day_end_ms):
    """复刻 process_kline 的 1m 逻辑 → (爬升, 阴跌, 暴涨, 暴跌)"""
    win = deque(maxlen=WINDOW_SIZE)
    last_trend = -10**15      # A: 涨跌共用冷却
    last_pump = -10**15       # B: 涨/跌各自冷却
    last_dump = -10**15
    up_t = down_t = up_p = down_p = 0
    for k in klines:
        if float(k[7]) < VOL_MIN:      # 成交额不足 → 整根跳过, 不进窗口
            continue
        c, o, t = float(k[4]), float(k[1]), k[0]
        win.append(c)
        in_day = day_start_ms <= t < day_end_ms
        if not in_day:
            continue

        # A) 趋势信号 (爬升/阴跌)
        if len(win) >= WINDOW_MIN and win[0] > 0:
            trend = (c - win[0]) / win[0]
            if abs(trend) > TREND_TH and t - last_trend > TREND_COOLDOWN:
                last_trend = t
                if trend > 0:
                    up_t += 1
                else:
                    down_t += 1

        # B) 单根暴涨/暴跌
        if o > 0:
            chg = (c - o) / o
            if chg > PUMP_TH and t - last_pump >= PUMP_COOLDOWN:
                last_pump = t
                up_p += 1
            elif chg < -PUMP_TH and t - last_dump >= PUMP_COOLDOWN:
                last_dump = t
                down_p += 1
    return up_t, down_t, up_p, down_p


def simulate_3m(klines, day_start_ms, day_end_ms):
    """复刻 _process_kline_3m → (暴涨3m, 暴跌3m)"""
    vols = deque(maxlen=VOL3M_WIN)
    last_p = -10**15
    last_d = -10**15
    up = down = 0
    for k in klines:
        q = float(k[7])
        if q < VOL_MIN:
            continue
        o, c, t = float(k[1]), float(k[4]), k[0]
        # 生产: 已收盘K线只入量窗口, 信号判断发生在流式未收盘消息上
        # 审计近似: 用已收盘K线最终 close 判断 (盘中瞬时突破无法还原)
        if day_start_ms <= t < day_end_ms and len(vols) >= VOL3M_WIN and o > 0:
            chg = (c - o) / o
            ma5 = sum(vols) / len(vols)
            if abs(chg) > PUMP3M_TH and ma5 > 0 and q > ma5 * VOL_MA5_MULT:
                if chg > 0 and t - last_p >= PUMP_COOLDOWN:
                    last_p = t
                    up += 1
                elif chg < 0 and t - last_d >= PUMP_COOLDOWN:
                    last_d = t
                    down += 1
        vols.append(q)
    return up, down


def prev_day(d):
    return (datetime.strptime(d, '%Y-%m-%d').replace(tzinfo=BJ) - timedelta(days=1)).strftime('%Y-%m-%d')


SIG_LABELS = ['📈爬升', '🚀暴涨', '🚀暴涨3m', '📉阴跌', '🩸暴跌', '🩸暴跌3m']
SIG_KEYS = ['up_trend', 'up_pump', 'up_3m', 'down_trend', 'down_pump', 'down_3m']


def main():
    start_d = sys.argv[1] if len(sys.argv) > 1 else '2026-09-19'
    end_d = sys.argv[2] if len(sys.argv) > 2 else '2026-09-28'
    days = []
    cur = datetime.strptime(start_d, '%Y-%m-%d').replace(tzinfo=BJ)
    enddt = datetime.strptime(end_d, '%Y-%m-%d').replace(tzinfo=BJ)
    while cur <= enddt:
        days.append(cur.strftime('%Y-%m-%d'))
        cur += timedelta(days=1)

    conn = sqlite3.connect(DB_PATH)
    syms = [r[0] for r in conn.execute(
        "SELECT DISTINCT symbol FROM alerts WHERE timestamp >= ? AND timestamp < datetime(?, '+1 day')",
        (f'{days[0]} 00:00:00', days[-1]))]
    warm_day = prev_day(days[0])

    print(f"==> 审计 {start_d} ~ {end_d} ({len(days)}天) | 币数 {len(syms)} (区间内报过警的币)")
    print(f"==> 信号种类: {' '.join(SIG_LABELS)}")
    print(f"==> 预计 {len(syms)*(len(days)+1)*2} 次请求 (1m+3m), 缓存命中可大幅减少")
    print(f"==> 权重保护: 全局>1900自动等待, 不挤占生产服务(~600/min)")
    print("=" * 100, flush=True)

    res = {d: defaultdict(int) for d in days}
    missing = defaultdict(list)    # day -> [(sym, label, should, actual)]

    def db_count(sym, label, d):
        return conn.execute(
            "SELECT COUNT(*) FROM alerts WHERE symbol=? AND signal_type=? AND timestamp>=? AND timestamp<datetime(?,'+1 day')",
            (sym, label, f'{d} 00:00:00', f'{d} 00:00:00')).fetchone()[0]

    t0 = time.time()
    for i, sym in enumerate(syms):
        warm1 = get_day_klines(sym, '1m', warm_day)
        warm3 = get_day_klines(sym, '3m', warm_day)
        for d in days:
            ds = int(datetime.strptime(f'{d} 00:00:00', '%Y-%m-%d %H:%M:%S').replace(tzinfo=BJ).timestamp() * 1000)
            de = ds + 24 * 3600_000
            k1 = get_day_klines(sym, '1m', d)
            k3 = get_day_klines(sym, '3m', d)
            if not k1 and not k3:
                res[d]['fail'] += 1
                continue
            stream1 = (warm1 or []) + (k1 or [])   # 拼暖窗填满 deque
            stream3 = (warm3 or []) + (k3 or [])
            ut, dt, up_, dp = simulate_1m(stream1, ds, de)
            u3, d3 = simulate_3m(stream3, ds, de)
            should = {'up_trend': ut, 'down_trend': dt, 'up_pump': up_,
                      'down_pump': dp, 'up_3m': u3, 'down_3m': d3}
            for kk, vv in should.items():
                res[d][kk] += vv
            if any(should.values()):
                for kk, lbl in zip(SIG_KEYS, SIG_LABELS):
                    sh = should[kk]
                    if sh:
                        ac = db_count(sym, lbl, d)
                        if sh > ac:
                            missing[d].append((sym, lbl, sh, ac))
            warm1, warm3 = k1, k3    # 滚动暖窗

        if (i + 1) % 20 == 0:
            el = time.time() - t0
            eta = el / (i + 1) * (len(syms) - i - 1)
            print(f"  进度 {i+1}/{len(syms)} | 已用{el/60:.1f}min 剩余≈{eta/60:.1f}min "
                  f"| 请求{_stats['req']} 缓存{_stats['cache_hit']} weight={_weight['used']} "
                  f"等待{_stats['wait']/60:.1f}min 限流{_stats['throttled']}", flush=True)

    # === 报告 ===
    print("\n" + "=" * 100)
    print("📊 信号完整性审计报告 (每格 = 应报/实报 完整度)")
    print("=" * 100)
    line1 = f"{'日期':<11}" + "".join(f"{s:>15}" for s in SIG_LABELS) + f"{'做多均值':>10}{'判定':>9}"
    print(line1)
    print("-" * len(line1.expandtabs()))
    verdict = {}
    for d in days:
        r = res[d]
        cells = []
        long_ratios = []
        for idx, kk in enumerate(SIG_KEYS):
            sh = r[kk]
            ac = conn.execute(
                "SELECT COUNT(*) FROM alerts WHERE signal_type=? AND priority='urgent' AND timestamp>=? AND timestamp<datetime(?,'+1 day')",
                (SIG_LABELS[idx], f'{d} 00:00:00', f'{d} 00:00:00')).fetchone()[0]
            pct = (ac / sh * 100) if sh else None
            if idx < 3 and pct is not None:
                long_ratios.append(min(pct, 100))
            cells.append(f"{sh:>4}/{ac:<4}" + ('  -  ' if pct is None else f"{pct:>4.0f}%"))
        avg = sum(long_ratios) / len(long_ratios) if long_ratios else 100.0
        verdict[d] = avg
        flag = '✅可信' if avg >= 85 else ('⚠️偏差' if avg >= 60 else '❌残缺')
        print(f"{d:<11}" + "".join(f"{c:>15}" for c in cells) + f"{avg:>9.0f}%{flag:>9}")

    print("\n🎯 做多信号完整度 (📈爬升/🚀暴涨/🚀暴涨3m 均值)")
    for d in days:
        v = verdict[d]
        print(f"  {d}: {v:>5.1f}% {'█' * int(v / 5)}")
    ok = [d for d in days if verdict[d] >= 85]
    mid = [d for d in days if 60 <= verdict[d] < 85]
    bad = [d for d in days if verdict[d] < 60]
    print(f"\n  ✅ 可信(>=85%): {ok if ok else '无'}")
    print(f"  ⚠️ 偏差(60-85%): {mid if mid else '无'}")
    print(f"  ❌ 残缺(<60%):  {bad if bad else '无'}")
    print(f"\n  → 回测应只用【可信】日期: {ok if ok else '(无)'}")

    print("\n📋 漏报最严重的日期 TOP (应报>实报 的币数)")
    for d in sorted(days, key=lambda x: -len(missing[x]))[:5]:
        ms = sorted(missing[d], key=lambda x: -(x[2] - x[3]))[:5]
        print(f"  {d}: {len(missing[d])}个币漏报 | 最严重: " +
              ", ".join(f"{s}{lbl[1:]}应{sh}实{ac}" for s, lbl, sh, ac in ms))

    print(f"\n统计: 新请求{_stats['req']}次, 缓存命中{_stats['cache_hit']}次, "
          f"限流{_stats['throttled']}次, 权重等待{_stats['wait']/60:.1f}min, "
          f"K线拉取失败{sum(res[d]['fail'] for d in days)}次, 总耗时{(time.time()-t0)/60:.1f}min")
    conn.close()


if __name__ == '__main__':
    main()
