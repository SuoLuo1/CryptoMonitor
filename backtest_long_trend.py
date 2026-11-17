"""只做多 + 趋势跟踪出场 + 贝叶斯期望 (K线只加载一次)。

出场规则 (模拟"走势不好提前平, 走势好拿一拿"):
  1. 目标止盈: 达到 TARGET_GAIN% 即出场
  2. 移动止损: 从持仓后的最高点回撤 TRAIL% 即出场 (多头用 low 判定, 保守)
  3. 时间止损: 持有满 MIN_HOLD 分钟后仍未达 MIN_GAIN% 则离场
  4. 超时强平: 最多持有 MAX_HOLD 分钟

出场价取保守值: 止损用 min(stop, open), 止动用目标价, 其余用 close。
手续费+滑点单边 FEE%, 往返 2*FEE%。

贝叶斯: Beta(0.5,0.5) 先验 -> 胜率后验; 对数收益正态后验 -> 期望收益后验。
"""
import csv, sqlite3, datetime as dt, statistics, math, random
from collections import defaultdict

TZ = dt.timezone(dt.timedelta(hours=8))
CACHE = '/root/CryptoMonitor/backtest_current_top/top_high_klines_cache.sqlite3'
SNAP = '/root/CryptoMonitor/backtest_snapshots/snapshot_rows.csv'
MINUTE = 60_000
FEE = 0.1
LONG_TYPES = {'🚀暴涨', '🚀暴涨3m', '📈爬升'}


def load_all():
    cache = sqlite3.connect(CACHE)
    bars = defaultdict(list)
    for sym, ot, o, h, l, c in cache.execute(
            "SELECT symbol, open_time_ms, open, high, low, close FROM top_roi_klines WHERE interval='1m'"):
        bars[sym].append((ot, o, h, l, c))
    for s in bars:
        bars[s].sort()
    cache.close()

    with open(SNAP, encoding='utf-8-sig') as f:
        rows = list(csv.DictReader(f))
    first_snap = {}
    for r in rows:
        s = r['symbol']
        if s not in first_snap or r['as_of'] < first_snap[s]:
            first_snap[s] = r['as_of']
    entries, seen = [], set()
    for r in rows:
        if r['as_of'] != first_snap[r['symbol']]:
            continue
        key = (r['symbol'], r['signal_time'], r['k'])
        if key in seen:
            continue
        seen.add(key)
        if r['signal_type'] not in LONG_TYPES:
            continue
        t_ms = int(dt.datetime.fromisoformat(r['signal_time']).timestamp() * 1000)
        entries.append({'sym': r['symbol'], 'k': int(r['k']), 'type': r['signal_type'],
                        't_ms': t_ms})
    return bars, entries


def find_entry(seq, t_ms):
    for i, bar in enumerate(seq):
        if bar[0] > t_ms:
            return i, bar
    return None, None


def simulate(seq, idx, entry_price, trail_pct, target_pct, min_gain_pct, min_hold, max_hold):
    trail = trail_pct / 100.0
    target = target_pct / 100.0
    min_gain = min_gain_pct / 100.0
    entry_ot = seq[idx][0]
    deadline = entry_ot + (max_hold - 1) * MINUTE
    peak = entry_price
    high_water = entry_price
    for i in range(idx, len(seq)):
        ot, o, h, l, c = seq[i]
        if ot > deadline:
            break
        if target < 900 and h >= entry_price * (1 + target):
            return entry_price * (1 + target), ot, 'target', (ot - entry_ot) // MINUTE + 1
        peak = max(peak, h)
        high_water = max(high_water, h)
        if peak > entry_price * (1 + min_gain):
            stop = peak * (1 - trail)
            if l <= stop:
                exit_px = min(stop, o) if o < stop else stop
                return exit_px, ot, 'trail', (ot - entry_ot) // MINUTE + 1
        if (i - idx + 1) >= min_hold and (high_water - entry_price) / entry_price < min_gain:
            return c, ot, 'time', (ot - entry_ot) // MINUTE + 1
    ot, o, h, l, c = seq[-1]
    return c, ot, 'timeout', (ot - entry_ot) // MINUTE + 1


def bayes(returns):
    n = len(returns)
    if n == 0:
        return None
    wins = len([x for x in returns if x > 0])
    a, b = wins + 0.5, (n - wins) + 0.5
    p = a / (a + b)
    sd = math.sqrt((a * b) / ((a + b) ** 2 * (a + b + 1)))
    logs = [math.log(max(1 + x / 100, 1e-9)) for x in returns]
    mu = statistics.mean(logs)
    s = statistics.stdev(logs) if n > 1 else 0.0
    se = s / math.sqrt(n)
    return {
        'n': n, 'wins': wins, 'p': p,
        'p_lo': max(0.0, p - 1.96 * sd), 'p_hi': min(1.0, p + 1.96 * sd),
        'exp': (math.exp(mu + 0.5 * s * s) - 1) * 100,
        'exp_lo': (math.exp(mu - 1.96 * se) - 1) * 100,
        'exp_hi': (math.exp(mu + 1.96 * se) - 1) * 100,
        'median': statistics.median(returns),
    }


def main():
    print("加载 K 线与入场点...", flush=True)
    bars, entries = load_all()
    print("  币 %d 个, 1m bar %d 根, 只做多入场点 %d 个" % (
        len(bars), sum(len(v) for v in bars.values()), len(entries)), flush=True)

    parsed = []
    for e in entries:
        seq = bars.get(e['sym'])
        if not seq:
            continue
        idx, bar = find_entry(seq, e['t_ms'])
        if idx is None or not bar[1] or bar[1] <= 0:
            continue
        parsed.append((e, seq, idx, bar[1]))
    print("  可评估 %d 个" % len(parsed), flush=True)

    configs = []
    for target in [10, 20, 40, 999]:
        for trail in [2, 3, 5, 8]:
            for min_hold in [60, 240]:
                configs.append((trail, target, 1.0, min_hold, 720))

    results = {}
    print("\n扫描 %d 组配置..." % len(configs), flush=True)
    for ci, (trail, target, min_gain, min_hold, max_hold) in enumerate(configs):
        res = []
        for e, seq, idx, entry_price in parsed:
            px, ot, reason, hold = simulate(seq, idx, entry_price, trail, target,
                                           min_gain, min_hold, max_hold)
            gross = (px / entry_price - 1) * 100
            res.append({'sym': e['sym'], 'k': e['k'], 'type': e['type'],
                        'net': gross - 2 * FEE, 'reason': reason, 'hold': hold})
        results[(trail, target, min_gain, min_hold, max_hold)] = res
        if (ci + 1) % 8 == 0:
            print("  ... %d/%d" % (ci + 1, len(configs)), flush=True)

    table = []
    for key, res in results.items():
        nets = [x['net'] for x in res]
        b = bayes(nets)
        if b:
            table.append((b['exp'], b['p'], key, b))
    table.sort(reverse=True)

    print("\n" + "=" * 88)
    print("参数扫描结果 (按期望收益排序, 只做多, 趋势跟踪出场)")
    print("=" * 88)
    print("  %-10s %-10s %-10s %-8s %10s %14s" % (
        "止盈", "移动止损%", "最短持有", "样本", "胜率", "期望收益"))
    print("  " + "-" * 70)
    for exp, p, key, b in table[:10]:
        trail, target, mg, mh, xh = key
        print("  %-10s %-10.0f %-10d %-8d %9.1f%% %13.3f%%" % (
            ("无上限" if target > 900 else "%.0f%%" % target), trail, mh, b['n'],
            p * 100, exp))
    print("  ...")
    for exp, p, key, b in table[-3:]:
        trail, target, mg, mh, xh = key
        print("  %-10s %-10.0f %-10d %-8d %9.1f%% %13.3f%%" % (
            ("无上限" if target > 900 else "%.0f%%" % target), trail, mh, b['n'],
            p * 100, exp))

    exp, p, key, b = table[0]
    trail, target, mg, mh, xh = key
    res = results[key]
    nets = [x['net'] for x in res]
    print()
    print("=" * 88)
    print("最佳配置: 止盈%s / 移动止损%.0f%% / 最短持有%dmin / 最长%dmin" % (
        ("无上限" if target > 900 else "%.0f%%" % target), trail, mh, xh))
    print("=" * 88)
    print("  样本 %d" % b['n'])
    print("  中位净收益   %+.3f%%" % b['median'])
    print("  均值净收益   %+.3f%%" % statistics.mean(nets))
    print("  胜率后验     %.1f%%  (95%%CI %.1f%% ~ %.1f%%)" % (
        b['p'] * 100, b['p_lo'] * 100, b['p_hi'] * 100))
    print("  期望收益后验 %+.3f%%  (95%%CI %+.3f%% ~ %+.3f%%)" % (
        b['exp'], b['exp_lo'], b['exp_hi']))
    from collections import Counter
    print("  出场原因:", dict(Counter(x['reason'] for x in res)))
    print("  持有时长中位: %d 分钟" % statistics.median([x['hold'] for x in res]))

    print()
    print("  %-14s %8s %12s %10s %14s" % ("信号类型", "样本", "中位净收益", "胜率", "期望收益"))
    print("  " + "-" * 62)
    bt = defaultdict(list)
    for x in res:
        bt[x['type']].append(x['net'])
    for t, lst in sorted(bt.items(), key=lambda kv: -len(kv[1])):
        bb = bayes(lst)
        print("  %-14s %8d %11.3f%% %9.1f%% %13.3f%%" % (
            t, len(lst), bb['median'], bb['p'] * 100, bb['exp']))

    print()
    print("  %-8s %8s %12s %10s %14s" % ("第k次", "样本", "中位净收益", "胜率", "期望收益"))
    print("  " + "-" * 56)
    bk = defaultdict(list)
    for x in res:
        bk[x['k']].append(x['net'])
    for k in sorted(bk):
        bb = bayes(bk[k])
        print("  %-8d %8d %11.3f%% %9.1f%% %13.3f%%" % (
            k, len(bk[k]), bb['median'], bb['p'] * 100, bb['exp']))

    print()
    print("=" * 88)
    print("对照: 同一批币, 随机时刻入场 (相同出场规则)")
    print("=" * 88)
    random.seed(7)
    base = []
    for e, seq, idx, entry_price in parsed:
        if len(seq) < idx + 400:
            continue
        cand = [i for i in range(len(seq)) if abs(seq[i][0] - e['t_ms']) > 120 * MINUTE]
        if not cand:
            continue
        j = cand[len(cand) // 2]
        bp = seq[j][1]
        if not bp or bp <= 0:
            continue
        px, ot, reason, hold = simulate(seq, j, bp, trail, target, mg, mh, xh)
        base.append((px / bp - 1) * 100 - 2 * FEE)
    bb = bayes(base)
    print("  随机入场: 样本 %d  中位 %+.3f%%  胜率 %.1f%%  期望 %+.3f%%" % (
        bb['n'], bb['median'], bb['p'] * 100, bb['exp']))
    print("  信号入场: 样本 %d  中位 %+.3f%%  胜率 %.1f%%  期望 %+.3f%%" % (
        b['n'], b['median'], b['p'] * 100, b['exp']))
    print("  超额期望: %+.3f%%" % (b['exp'] - bb['exp']))


if __name__ == '__main__':
    main()
