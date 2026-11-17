"""首次信号后的最大涨幅: 均值 32% 能不能吃到?

关键问题:
  MFE (最大涨幅) 均值 ~32%, 中位数 ~5% → 重尾分布, 少数妖币拉高均值
  但"吃到"需要:
    1. 拿够久 (到峰值的时间有多长?)
    2. 不中途下车 (简单持有的捕获率)
    3. 识别出哪些是妖币 (否则平均还是负)

本脚本算:
  A. MFE 分布 + 到峰值的时间分布
  B. 各持有期的"捕获率" = 实际收益 / MFE
  C. 均值贡献分解: 头部 10% 币贡献了多少均值
  D. 如果能提前识别头部 (用信号前特征), 期望收益是多少
"""
import csv, sqlite3, datetime as dt, statistics, math
from collections import defaultdict

CACHE = '/root/CryptoMonitor/backtest_current_top/top_high_klines_cache.sqlite3'
SNAP = '/root/CryptoMonitor/backtest_snapshots/snapshot_rows.csv'
MINUTE = 60_000
FEE = 0.1
LONG_TYPES = {'🚀暴涨', '🚀暴涨3m', '📈爬升'}
HORIZONS = [60, 240, 720, 1440, 2880, 4320]


def bayes(rets):
    n = len(rets)
    if n == 0:
        return None
    wins = len([x for x in rets if x > 0])
    a, b = wins + 0.5, (n - wins) + 0.5
    p = a / (a + b)
    logs = [math.log(max(1 + x / 100, 1e-9)) for x in rets]
    mu = statistics.mean(logs)
    s = statistics.stdev(logs) if n > 1 else 0.0
    se = s / math.sqrt(n)
    return {'n': n, 'p': p, 'exp': (math.exp(mu + 0.5 * s * s) - 1) * 100,
            'median': statistics.median(rets), 'mean': statistics.mean(rets)}


def main():
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

    samples = []
    for e in ent:
        seq = bars.get(e['sym'])
        if not seq:
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
        # MFE + 到峰值时间 + 各持有期收益
        mfe = None
        peak_idx = None
        for j in range(idx, len(seq)):
            if seq[j][2] > ep and (mfe is None or seq[j][2] > mfe):
                mfe = seq[j][2]
                peak_idx = j
        if mfe is None:
            continue
        mfe_pct = (mfe / ep - 1) * 100
        peak_min = (seq[peak_idx][0] - seq[idx][0]) // MINUTE
        rets = {}
        for hz in HORIZONS:
            if idx + hz < len(seq):
                ec = seq[idx + hz][3]
                if ec:
                    rets[hz] = (ec / ep - 1) * 100 - 2 * FEE
        # 信号前 60 分钟涨幅 + 信号前 24 小时涨幅 (用于提前识别)
        pre60 = None
        if idx >= 60:
            p0 = seq[idx - 60][1]
            if p0:
                pre60 = (ep / p0 - 1) * 100
        samples.append({'sym': e['sym'], 'k': e['k'], 'type': e['type'],
                        'mfe': mfe_pct, 'peak_min': peak_min, 'rets': rets,
                        'pre60': pre60, 'seq': seq, 'idx': idx})
    print("可评估: %d 个" % len(samples), flush=True)

    # ═══ A. MFE 分布 ═══
    print()
    print("=" * 92)
    print("[A] 首次信号后的最大涨幅 (MFE) 分布 —— 重尾, 均值被少数妖币拉高")
    print("=" * 92)
    mfes = sorted(x['mfe'] for x in samples)
    n = len(mfes)
    print("  样本 %d" % n)
    print("  均值   %+.2f%%" % statistics.mean(mfes))
    print("  中位数 %+.2f%%" % statistics.median(mfes))
    for p in [25, 50, 75, 90, 95, 99]:
        print("  P%-3d %+.2f%%" % (p, mfes[min(int(n * p / 100), n - 1)]))
    for thr in [5, 10, 20, 50, 100, 200]:
        c = len([v for v in mfes if v >= thr])
        print("  >=%+4d%%: %4d 个 (%.1f%%)" % (thr, c, c / n * 100))

    # ═══ B. 到峰值的时间 ═══
    print()
    print("=" * 92)
    print("[B] 峰值出现时间 (决定要拿多久)")
    print("=" * 92)
    peaks = sorted(x['peak_min'] for x in samples)
    print("  中位 %.0f 分钟, 均值 %.0f 分钟" % (statistics.median(peaks), statistics.mean(peaks)))
    for p in [25, 50, 75, 90]:
        print("  P%-3d %.0f 分钟 (%.1f 小时)" % (
            p, peaks[min(int(n * p / 100), n - 1)], peaks[min(int(n * p / 100), n - 1)] / 60))

    # ═══ C. 捕获率 ═══
    print()
    print("=" * 92)
    print("[C] 捕获率: 简单持有 H 分钟后平仓, 吃到 MFE 的百分之几")
    print("=" * 92)
    print("  %-10s %10s %14s %16s %14s" % (
        "持有", "中位捕获率", "均值捕获率", "中位实际收益", "实际收益均值"))
    print("  " + "-" * 70)
    for hz in HORIZONS:
        caps, acts = [], []
        for x in samples:
            if hz not in x['rets']:
                continue
            act_gross = x['rets'][hz] + 2 * FEE
            caps.append(act_gross / x['mfe'] * 100 if x['mfe'] > 0 else 0)
            acts.append(x['rets'][hz])
        if not caps:
            continue
        print("  %-10s %9.1f%% %13.1f%% %15.2f%% %13.2f%%" % (
            "%dmin" % hz, statistics.median(caps), statistics.mean(caps),
            statistics.median(acts), statistics.mean(acts)))

    # ═══ D. 均值贡献分解 ═══
    print()
    print("=" * 92)
    print("[D] 均值贡献分解: 头部币贡献了多少?")
    print("=" * 92)
    total = sum(mfes)
    for frac in [0.05, 0.10, 0.20]:
        k = max(1, int(n * frac))
        head = sorted(mfes, reverse=True)[:k]
        rest = mfes[:n - k]
        print("  头部 %2d%% (%d 个): 贡献均值 %+.2f%% (占总均值 %.1f%%)" % (
            frac * 100, k, statistics.mean(head), sum(head) / total * 100))
        print("        其余 %d 个: 均值 %+.2f%%" % (n - k, statistics.mean(rest)))
    print()
    print("  → 如果你【只拿到平均水平的币】, 期望就是 %+.2f%%" % statistics.mean(mfes[len(mfes) // 2:]))
    print("  → MFE 中位数 %+.2f%% 是[无脑进+拿住]的典型结果" % statistics.median(mfes))

    # ═══ E. 能否提前识别头部 ═══
    print()
    print("=" * 92)
    print("[E] 用信号前特征识别头部 (头部= MFE 前 20%)")
    print("=" * 92)
    ranked = sorted(samples, key=lambda x: -x['mfe'])
    k_head = max(1, int(n * 0.2))
    head_set = {id(x) for x in ranked[:k_head]}

    # 特征: 信号前60分钟涨幅
    buckets = defaultdict(lambda: [0, 0])
    for x in samples:
        if x['pre60'] is None:
            continue
        b = '未知'
        if x['pre60'] < 0:
            b = '<0%'
        elif x['pre60'] < 1:
            b = '0~1%'
        elif x['pre60'] < 3:
            b = '1~3%'
        elif x['pre60'] < 6:
            b = '3~6%'
        else:
            b = '>6%'
        buckets[b][0] += 1
        if id(x) in head_set:
            buckets[b][1] += 1
    print("  %-10s %8s %10s %14s" % ("信号前涨幅", "样本", "头部数", "命中率"))
    print("  " + "-" * 46)
    for b in ['<0%', '0~1%', '1~3%', '3~6%', '>6%', '未知']:
        if b not in buckets:
            continue
        tot, hd = buckets[b]
        if not tot:
            continue
        print("  %-10s %8d %10d %13.1f%%" % (b, tot, hd, hd / tot * 100))
    print("  (整体头部率 = %.1f%%)" % (k_head / n * 100))

    # ═══ F. 头部币的实际收益 ═══
    print()
    print("=" * 92)
    print("[F] 头部币 (MFE 前20%) 的实际持有收益 —— 妖币到底能不能吃到")
    print("=" * 92)
    head = ranked[:k_head]
    print("  %-10s %8s %12s %12s %10s" % ("持有", "样本", "中位收益", "均值收益", "胜率"))
    print("  " + "-" * 56)
    for hz in HORIZONS:
        lst = [x['rets'][hz] for x in head if hz in x['rets']]
        if not lst:
            continue
        b = bayes(lst)
        print("  %-10s %8d %11.2f%% %11.2f%% %9.1f%%" % (
            "%dmin" % hz, len(lst), b['median'], b['mean'], b['p'] * 100))
    print()
    print("  头部币的 MFE 中位: %+.2f%%" % statistics.median([x['mfe'] for x in head]))
    print("  头部币峰值时间中位: %.0f 分钟" % statistics.median([x['peak_min'] for x in head]))


if __name__ == '__main__':
    main()
