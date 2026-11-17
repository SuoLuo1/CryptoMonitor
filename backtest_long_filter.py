"""只做多: 选币过滤 + 拿久 + 贝叶斯期望。

核心思路 (来自前面分析):
  - 信号是"异动已发生"的确认 → 进场时币已涨过 (中位 +0.755%)
  - 所以应该【不追高】: 只买信号前涨幅小的
  - 而且"碰上妖币就发了" → 持有时间要长, 别用紧移动止损

测试维度:
  A. 信号前 N 分钟涨幅过滤 (不追高)
  B. 持有时长 (简单持有, 无移动止损)
  C. 段内信号次数 (第k次)
  D. 与"同币随机时刻"基线对比 (真正的 alpha)

入场价: 信号后下一分钟开盘价 (可成交)
出场: 简单持有 H 分钟的收盘价 (不含移动止损)
成本: 往返 0.2%
"""
import csv, sqlite3, datetime as dt, statistics, math, random
from collections import defaultdict

CACHE = '/root/CryptoMonitor/backtest_current_top/top_high_klines_cache.sqlite3'
SNAP = '/root/CryptoMonitor/backtest_snapshots/snapshot_rows.csv'
MINUTE = 60_000
FEE = 0.1
LONG_TYPES = {'🚀暴涨', '🚀暴涨3m', '📈爬升'}


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
        'mean': statistics.mean(returns),
    }


def main():
    cache = sqlite3.connect(CACHE)
    bars = defaultdict(list)
    for sym, ot, o, h, l, c in cache.execute(
            "SELECT symbol, open_time_ms, open, high, low, close FROM top_roi_klines WHERE interval='1m'"):
        bars[sym].append((ot, o, h, l, c))
    for s in bars:
        bars[s].sort()
    cache.close()
    print("K线: %d 币, %d bar" % (len(bars), sum(len(v) for v in bars.values())), flush=True)

    with open(SNAP, encoding='utf-8-sig') as f:
        rows = list(csv.DictReader(f))
    first = {}
    for r in rows:
        s = r['symbol']
        if s not in first or r['as_of'] < first[s]:
            first[s] = r['as_of']
    ent, seen = [], set()
    for r in rows:
        if r['as_of'] != first[r['symbol']]:
            continue
        key = (r['symbol'], r['signal_time'], r['k'])
        if key in seen:
            continue
        seen.add(key)
        if r['signal_type'] not in LONG_TYPES:
            continue
        ent.append({'sym': r['symbol'], 'k': int(r['k']), 'type': r['signal_type'],
                    't_ms': int(dt.datetime.fromisoformat(r['signal_time']).timestamp() * 1000)})
    print("只做多入场点: %d" % len(ent), flush=True)

    # ── 预计算: 每个入场点的 (pre_rise, 各持有期收益) ──
    samples = []
    for e in ent:
        seq = bars.get(e['sym'])
        if not seq or len(seq) < 100:
            continue
        idx = None
        for i, b in enumerate(seq):
            if b[0] > e['t_ms']:
                idx = i
                break
        if idx is None:
            continue
        ep = seq[idx][1]
        if not ep or ep <= 0:
            continue
        # 信号前 60 分钟涨幅 (不追高指标)
        pre_rise = None
        if idx >= 60:
            p0 = seq[idx - 60][1]
            if p0:
                pre_rise = (ep / p0 - 1) * 100
        # 各持有期收益
        rets = {}
        for hz in [60, 240, 720, 1440]:
            if idx + hz < len(seq):
                ec = seq[idx + hz][3]
                if ec:
                    rets[hz] = (ec / ep - 1) * 100 - 2 * FEE
        if rets:
            samples.append({'sym': e['sym'], 'k': e['k'], 'type': e['type'],
                            'pre_rise': pre_rise, 'rets': rets, 'seq': seq, 'idx': idx})
    print("可评估: %d 个" % len(samples), flush=True)

    HORIZONS = [60, 240, 720, 1440]

    # ═══ A. 不追高过滤 ═══
    print()
    print("=" * 92)
    print("[A] 不追高: 按『信号前60分钟涨幅』过滤 (持有 720min)")
    print("=" * 92)
    print("  %-26s %8s %11s %10s %14s" % ("过滤条件", "样本", "中位净收益", "胜率", "期望收益"))
    print("  " + "-" * 74)
    for label, lo, hi in [
        ("全部 (不过滤)", -999, 999),
        ("信号前涨幅 < 0%", -999, 0),
        ("信号前涨幅 < 0.5%", -999, 0.5),
        ("信号前涨幅 < 1%", -999, 1.0),
        ("信号前涨幅 < 2%", -999, 2.0),
        ("信号前涨幅 < 3%", -999, 3.0),
        ("信号前涨幅 0%~1%", 0, 1.0),
        ("信号前涨幅 1%~3%", 1.0, 3.0),
        ("信号前涨幅 > 3%", 3.0, 999),
    ]:
        sub = [x['rets'][720] for x in samples
               if 720 in x['rets'] and x['pre_rise'] is not None
               and lo <= x['pre_rise'] < hi]
        if len(sub) < 20:
            continue
        b = bayes(sub)
        print("  %-26s %8d %10.3f%% %9.1f%% %13.3f%%" % (
            label, len(sub), b['median'], b['p'] * 100, b['exp']))

    # ═══ B. 持有时长 ═══
    print()
    print("=" * 92)
    print("[B] 持有时长 (不过滤, 简单持有)")
    print("=" * 92)
    print("  %-10s %8s %11s %11s %10s %14s" % (
        "持有", "样本", "中位净收益", "均值净收益", "胜率", "期望收益"))
    print("  " + "-" * 72)
    for hz in HORIZONS:
        sub = [x['rets'][hz] for x in samples if hz in x['rets']]
        if not sub:
            continue
        b = bayes(sub)
        print("  %-10s %8d %10.3f%% %10.3f%% %9.1f%% %13.3f%%" % (
            "%dmin" % hz, len(sub), b['median'], b['mean'], b['p'] * 100, b['exp']))

    # ═══ C. 不追高 x 持有时长 ═══
    print()
    print("=" * 92)
    print("[C] 不追高(<1%) x 持有时长")
    print("=" * 92)
    print("  %-10s %8s %11s %10s %14s" % ("持有", "样本", "中位净收益", "胜率", "期望收益"))
    print("  " + "-" * 58)
    for hz in HORIZONS:
        sub = [x['rets'][hz] for x in samples
               if hz in x['rets'] and x['pre_rise'] is not None and x['pre_rise'] < 1.0]
        if len(sub) < 20:
            continue
        b = bayes(sub)
        print("  %-10s %8d %10.3f%% %9.1f%% %13.3f%%" % (
            "%dmin" % hz, len(sub), b['median'], b['p'] * 100, b['exp']))

    # ═══ D. 与同币随机时刻基线对比 ═══
    print()
    print("=" * 92)
    print("[D] 同币基线: 信号入场 vs 同币随机时刻入场 (同样持有期)")
    print("=" * 92)
    random.seed(11)
    for hz in [240, 720]:
        sig_sub = [x for x in samples if hz in x['rets'] and x['pre_rise'] is not None and x['pre_rise'] < 1.0]
        if not sig_sub:
            continue
        sig = [x['rets'][hz] for x in sig_sub]
        base = []
        for x in sig_sub:
            seq = x['seq']
            cand = [i for i in range(x['idx'] + 1, min(x['idx'] + 1500, len(seq)))
                    if seq[i][0] > seq[x['idx']][0] + 120 * MINUTE]
            if not cand:
                continue
            j = cand[len(cand) // 2]
            bp = seq[j][1]
            if not bp or bp <= 0 or j + hz >= len(seq):
                continue
            bc = seq[j + hz][3]
            if not bc:
                continue
            base.append((bc / bp - 1) * 100 - 2 * FEE)
        if not base:
            continue
        bs, bb = bayes(sig), bayes(base)
        print("  --- 持有 %dmin, 不追高(<1%%) ---" % hz)
        print("     信号入场   n=%4d 中位 %+.3f%% 胜率 %.1f%% 期望 %+.3f%%" % (
            bs['n'], bs['median'], bs['p'] * 100, bs['exp']))
        print("     同币随机   n=%4d 中位 %+.3f%% 胜率 %.1f%% 期望 %+.3f%%" % (
            bb['n'], bb['median'], bb['p'] * 100, bb['exp']))
        print("     超额期望   %+.3f%%   (信号优于随机的比例)" % (bs['exp'] - bb['exp']))

    # ═══ E. 分组: 不追高 + 持有720 ═══
    print()
    print("=" * 92)
    print("[E] 不追高(<1%) + 持有720min 的分组")
    print("=" * 92)
    base_sub = [x for x in samples if 720 in x['rets'] and x['pre_rise'] is not None and x['pre_rise'] < 1.0]
    for dim, keyf, label in [
        ('signal_type', lambda x: x['type'], '信号类型'),
        ('k', lambda x: x['k'], '第k次'),
    ]:
        print()
        print("  %-16s %8s %12s %10s %14s" % (label, "样本", "中位净收益", "胜率", "期望收益"))
        print("  " + "-" * 64)
        grp = defaultdict(list)
        for x in base_sub:
            grp[keyf(x)].append(x['rets'][720])
        for k in sorted(grp, key=lambda z: -len(grp[z])):
            if len(grp[k]) < 15:
                continue
            b = bayes(grp[k])
            print("  %-16s %8d %11.3f%% %9.1f%% %13.3f%%" % (
                k, len(grp[k]), b['median'], b['p'] * 100, b['exp']))


if __name__ == '__main__':
    main()
