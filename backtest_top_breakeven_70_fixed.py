#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""CryptoMonitor Top 榜历史快照回测：20% 保本触发 + 区间最高价 × 70% 平仓估算。

核心口径
--------
1. 输入是多次 Top 榜快照 CSV；同一币种、同一首次报警时间只算一笔。
2. 同一币种出现新的首次报警时间，上一笔窗口在新时间处结束，新一笔从新时间开始。
3. 每个币最后一笔统计到全体快照的最晚 as_of（或 --until 指定的截止时间），
   不因该币中途离开 Top 榜而提前结束。
4. 入场分钟整根排除，只统计信号分钟之后的完整 1m K 线；窗口右端不包含下一笔信号时间。
5. 不设初始止损。窗口内最高价达到入场价的 120% 后，保本保护才算触发。
6. 触发保本后，人工平仓参考价 = max(入场价, 区间最高价 × 0.70)。
   如果最高价 × 70% 不高于入场价，参考价就是入场价（保本）。
7. 如果整个窗口最高价都没有达到 +20%，保本从未触发；不允许凭空按保本价出场，
   按窗口内最后一根完整 1m K 线收盘价估算窗口末退出。

注意
----
- 最高价 × 70% 是事后人工平仓估算，不是可实时预知/自动执行的止盈策略。
- 本脚本不模拟 Binance 强平；无初始止损 + 杠杆仓位可能在触发保本前强平。
- --funding-rate 是固定费率近似值，按窗口时长 / 8 小时折算，不等于 Binance
  按每个实际结算时点费率扣款的精确历史资金费。
- 默认只回测做多信号（🚀暴涨、🚀暴涨3m、📈爬升），但所有有效信号时间都会
  参与切分窗口，避免中间出现非做多新信号时，上一笔窗口错误延长。

默认输入：backtest_snapshots/snapshot_rows.csv
也兼容项目现有 current_top_snapshot.csv 的 top_display_* / continuous_segment_* 字段。
对不含 as_of 的单次 current_top_snapshot.csv，必须显式传 --until；单次快照无法
还原多天 Top 榜历史变化，真正的多日去重回测必须输入多日快照汇总 CSV。

仅使用 Python 标准库；K 线数据从 Binance USDⓈ-M Futures REST API 获取，单独缓存，
不读写 alerts.db，不发送 Telegram。
"""

import argparse
import csv
import datetime as dt
import json
import os
import sqlite3
import statistics
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

# CryptoMonitor 项目时间字段以 UTC+8 保存；Binance K 线时间戳本身是 UTC epoch。
TZ = dt.timezone(dt.timedelta(hours=8))
UTC = dt.timezone.utc
MINUTE_MS = 60_000
DAY_MS = 86_400_000
KLINE_LIMIT = 1500
BINANCE_BASE = "https://fapi.binance.com"

ROOT = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CSV = os.path.join(ROOT, "backtest_snapshots", "snapshot_rows.csv")
DEFAULT_CACHE = os.path.join(ROOT, "backtest_top_breakeven_70", "klines_cache.sqlite3")
DEFAULT_OUT_DIR = os.path.join(ROOT, "backtest_top_breakeven_70")

PRINCIPAL_USDT = 10.0
LEVERAGE = 10
ENTRY_NOTIONAL = PRINCIPAL_USDT * LEVERAGE
FEE_RATE = 0.0004  # 单边 0.04%
BREAKEVEN_TRIGGER = 0.20
PEAK_EXIT_RATIO = 0.70
LONG_TYPES = {"🚀暴涨", "🚀暴涨3m", "📈爬升"}


@dataclass
class CandidateRow:
    row_no: int
    symbol: str
    signal_time: dt.datetime
    signal_type: str
    entry_price: Optional[float]
    as_of: Optional[dt.datetime]


@dataclass
class EventAgg:
    symbol: str
    signal_time: dt.datetime
    candidates: List[CandidateRow] = field(default_factory=list)


@dataclass
class Sample:
    symbol: str
    signal_time: dt.datetime
    signal_type: str
    entry_price: float
    window_start: dt.datetime
    window_end: dt.datetime


def parse_dt(value: str) -> dt.datetime:
    """解析 ISO/常见时间字符串；无时区字段按项目 UTC+8 解释。"""
    raw = str(value or "").strip()
    if not raw:
        raise ValueError("时间为空")
    parsed = dt.datetime.fromisoformat(raw.replace("Z", "+00:00").replace("T", " "))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=TZ)
    return parsed.astimezone(TZ)


def fmt_dt(value: Optional[dt.datetime], millis: bool = False) -> str:
    if value is None:
        return ""
    value = value.astimezone(TZ)
    if millis:
        return value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    return value.strftime("%Y-%m-%d %H:%M:%S")


def first_nonempty(row: dict, names: List[str]) -> str:
    for name in names:
        value = row.get(name)
        if value is not None and str(value).strip() != "":
            return str(value).strip()
    return ""


def get_signal_columns(source: str) -> Tuple[List[str], List[str], List[str]]:
    """返回首次信号时间、类型、价格的候选列名，兼容当前项目快照导出。"""
    if source == "continuous_segment":
        prefix = "continuous_segment_first_signal_"
    else:
        prefix = "top_display_first_signal_"
    time_cols = [
        "signal_time", "signal_time_utc8", "first_signal_time", "first_signal_time_utc8",
        prefix + "time_utc8", prefix + "time",
    ]
    type_cols = [
        "signal_type", "first_signal_type", prefix + "type",
    ]
    price_cols = [
        "entry_price", "first_signal_price", prefix + "price",
    ]
    return time_cols, type_cols, price_cols


def load_samples(csv_path: str, until_text: str = "", long_only: bool = True,
                 first_signal_source: str = "top_display") -> Tuple[List[Sample], dict]:
    """快照全量去重，并为每币每个首次信号事件分配互斥时间窗口。

    关键点：先收集所有方向的有效事件时间用于切窗，再按 long_only 决定哪些事件
    产生做多交易。这样中间的下跌/其他新事件仍然会截断上一笔做多事件。
    """
    if not os.path.isfile(csv_path):
        raise FileNotFoundError(f"找不到快照 CSV：{csv_path}")

    time_cols, type_cols, price_cols = get_signal_columns(first_signal_source)
    asof_cols = ["as_of", "snapshot_time_utc8", "snapshot_time", "snapshot_at", "captured_at"]
    events: Dict[Tuple[str, dt.datetime], EventAgg] = {}
    all_as_of: List[dt.datetime] = []
    total_rows = 0
    invalid_rows = 0
    price_conflicts = 0

    with open(csv_path, "r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise RuntimeError("CSV 没有表头")
        for row_no, row in enumerate(reader, start=2):
            total_rows += 1
            symbol = (row.get("symbol") or "").strip().upper()
            raw_signal_time = first_nonempty(row, time_cols)
            raw_type = first_nonempty(row, type_cols)
            raw_price = first_nonempty(row, price_cols)
            raw_as_of = first_nonempty(row, asof_cols)

            if raw_as_of:
                try:
                    as_of = parse_dt(raw_as_of)
                    all_as_of.append(as_of)
                except ValueError:
                    as_of = None
                    invalid_rows += 1
            else:
                as_of = None

            if not symbol or not raw_signal_time:
                invalid_rows += 1
                continue
            try:
                signal_time = parse_dt(raw_signal_time)
            except ValueError:
                invalid_rows += 1
                continue

            entry_price: Optional[float]
            try:
                entry_price = float(raw_price)
                if entry_price <= 0:
                    entry_price = None
            except (TypeError, ValueError):
                entry_price = None

            key = (symbol, signal_time)
            agg = events.setdefault(key, EventAgg(symbol=symbol, signal_time=signal_time))
            agg.candidates.append(CandidateRow(
                row_no=row_no,
                symbol=symbol,
                signal_time=signal_time,
                signal_type=raw_type,
                entry_price=entry_price,
                as_of=as_of,
            ))

    explicit_until = parse_dt(until_text) if until_text else None
    if explicit_until:
        global_end = explicit_until
        end_source = "--until"
    elif all_as_of:
        global_end = max(all_as_of)
        end_source = "CSV 最晚 as_of"
    else:
        raise RuntimeError(
            "CSV 不含 as_of/snapshot_time，无法确定最后一笔统计到何时。"
            "若输入是 current_top_snapshot.csv 这种单次快照，请显式传 --until 'YYYY-MM-DD HH:MM:SS'。"
        )

    # 每个唯一事件选取最早快照中记录的信号类型和入场价；后续重复快照只用于去重。
    # 若同一个 key 的 entry_price 出现差异，保留最早有效值并计数提醒。
    chosen: Dict[Tuple[str, dt.datetime], Tuple[str, float]] = {}
    for key, agg in events.items():
        ordered = sorted(
            agg.candidates,
            key=lambda x: (x.as_of is None, x.as_of or dt.datetime.max.replace(tzinfo=TZ), x.row_no),
        )
        typed = next((x.signal_type for x in ordered if x.signal_type), "")
        priced = [x.entry_price for x in ordered if x.entry_price is not None]
        price = priced[0] if priced else None
        if len({round(p, 12) for p in priced}) > 1:
            price_conflicts += 1
        if price is not None:
            chosen[key] = (typed, price)
        else:
            chosen[key] = (typed, 0.0)

    # 先按币分组所有首次信号（不先过滤方向），确定相邻事件边界。
    by_symbol: Dict[str, List[dt.datetime]] = defaultdict(list)
    for symbol, signal_time in events:
        if signal_time <= global_end:
            by_symbol[symbol].append(signal_time)

    samples: List[Sample] = []
    event_count = 0
    for symbol, times in by_symbol.items():
        ordered_times = sorted(set(times))
        for idx, signal_time in enumerate(ordered_times):
            next_signal_time = ordered_times[idx + 1] if idx + 1 < len(ordered_times) else global_end
            window_end = min(next_signal_time, global_end)
            event_count += 1
            signal_type, entry_price = chosen[(symbol, signal_time)]
            if long_only and signal_type not in LONG_TYPES:
                continue
            if entry_price <= 0:
                continue
            if window_end <= signal_time:
                continue
            samples.append(Sample(
                symbol=symbol,
                signal_time=signal_time,
                signal_type=signal_type,
                entry_price=entry_price,
                window_start=signal_time,
                window_end=window_end,
            ))

    samples.sort(key=lambda x: (x.signal_time, x.symbol))
    stats = {
        "total_rows": total_rows,
        "invalid_rows": invalid_rows,
        "unique_events_all_directions": len(events),
        "events_before_cutoff": event_count,
        "trade_samples": len(samples),
        "symbols": len(by_symbol),
        "global_end": global_end,
        "end_source": end_source,
        "price_conflicts": price_conflicts,
    }
    return samples, stats


def init_cache(path: str) -> sqlite3.Connection:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("""CREATE TABLE IF NOT EXISTS klines_1m (
        symbol TEXT NOT NULL,
        open_time_ms INTEGER NOT NULL,
        open REAL NOT NULL,
        high REAL NOT NULL,
        low REAL NOT NULL,
        close REAL NOT NULL,
        PRIMARY KEY(symbol, open_time_ms)
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_klines_1m_time ON klines_1m(symbol, open_time_ms)")
    # 日线缓存: peak 的主要来源, 数据量比 1m 小 1440 倍
    conn.execute("""CREATE TABLE IF NOT EXISTS klines_1d (
        symbol TEXT NOT NULL,
        open_time_ms INTEGER NOT NULL,
        open REAL NOT NULL,
        high REAL NOT NULL,
        low REAL NOT NULL,
        close REAL NOT NULL,
        PRIMARY KEY(symbol, open_time_ms)
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_klines_1d_time ON klines_1d(symbol, open_time_ms)")
    conn.commit()
    return conn


def load_daily(conn: sqlite3.Connection, symbol: str) -> List[tuple]:
    return conn.execute(
        "SELECT open_time_ms, open, high, low, close FROM klines_1d "
        "WHERE symbol=? ORDER BY open_time_ms",
        (symbol,),
    ).fetchall()


def load_daily_range(conn: sqlite3.Connection, symbol: str,
                     start_ms: int, end_ms: int) -> List[tuple]:
    return conn.execute(
        "SELECT open_time_ms, open, high, low, close FROM klines_1d "
        "WHERE symbol=? AND open_time_ms>=? AND open_time_ms<=? ORDER BY open_time_ms",
        (symbol, start_ms, end_ms),
    ).fetchall()


def missing_daily_ranges(conn: sqlite3.Connection, symbol: str,
                         start_ms: int, end_ms: int) -> List[Tuple[int, int]]:
    have = [row[0] for row in conn.execute(
        "SELECT open_time_ms FROM klines_1d WHERE symbol=? AND open_time_ms BETWEEN ? AND ? ORDER BY open_time_ms",
        (symbol, start_ms, end_ms),
    )]
    if not have:
        return [(start_ms, end_ms)]
    gaps: List[Tuple[int, int]] = []
    cursor = start_ms
    for timestamp in have:
        if timestamp > cursor:
            gaps.append((cursor, timestamp - DAY_MS))
        cursor = timestamp + DAY_MS
    if cursor <= end_ms:
        gaps.append((cursor, end_ms))
    return [(a, b) for a, b in gaps if a <= b]


def load_bars(conn: sqlite3.Connection, symbol: str, start_ms: int, end_ms: int) -> List[tuple]:
    return conn.execute(
        "SELECT open_time_ms, open, high, low, close FROM klines_1m "
        "WHERE symbol=? AND open_time_ms>=? AND open_time_ms<=? ORDER BY open_time_ms",
        (symbol, start_ms, end_ms),
    ).fetchall()


def missing_ranges(conn: sqlite3.Connection, symbol: str, start_ms: int, end_ms: int) -> List[Tuple[int, int]]:
    if end_ms < start_ms:
        return []
    have = [r[0] for r in conn.execute(
        "SELECT open_time_ms FROM klines_1m WHERE symbol=? AND open_time_ms BETWEEN ? AND ? ORDER BY open_time_ms",
        (symbol, start_ms, end_ms),
    )]
    if not have:
        return [(start_ms, end_ms)]
    gaps: List[Tuple[int, int]] = []
    cursor = start_ms
    for timestamp in have:
        if timestamp > cursor:
            gaps.append((cursor, timestamp - MINUTE_MS))
        cursor = timestamp + MINUTE_MS
    if cursor <= end_ms:
        gaps.append((cursor, end_ms))
    return [(a, b) for a, b in gaps if a <= b]


def request_json(url: str, timeout: int = 20, retries: int = 5):
    last_error = None
    for attempt in range(retries):
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "CryptoMonitor-Top-Breakeven70-Backtest/1.0", "Accept": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")[:300]
            last_error = RuntimeError(f"HTTP {exc.code}: {detail}")
            if exc.code not in (418, 429, 500, 502, 503, 504):
                raise last_error
            retry_after = exc.headers.get("Retry-After")
            try:
                wait = min(max(float(retry_after), 1.0), 60.0) if retry_after else min(2 ** attempt, 20)
            except ValueError:
                wait = min(2 ** attempt, 20)
            print(f"    Binance HTTP {exc.code}，{wait:.0f}s 后重试", flush=True)
            time.sleep(wait)
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_error = exc
            wait = min(2 ** attempt, 16)
            print(f"    K线请求异常：{exc}；{wait}s 后重试", flush=True)
            time.sleep(wait)
    raise RuntimeError(f"请求 Binance 失败，重试 {retries} 次：{last_error}")


def fetch_range(symbol: str, start_ms: int, end_ms: int, delay: float = 0.12,
                interval: str = "1m") -> List[list]:
    """按 Binance 1500 根上限分页拉取 K 线 (1m / 1d)。"""
    output: List[list] = []
    cursor = start_ms
    step = MINUTE_MS if interval == "1m" else DAY_MS
    while cursor <= end_ms:
        params = urllib.parse.urlencode({
            "symbol": symbol,
            "interval": interval,
            "startTime": cursor,
            "endTime": end_ms,
            "limit": KLINE_LIMIT,
        })
        data = request_json(f"{BINANCE_BASE}/fapi/v1/klines?{params}")
        if not isinstance(data, list) or not data:
            break
        output.extend(data)
        next_cursor = int(data[-1][0]) + step
        if next_cursor <= cursor:
            raise RuntimeError(f"{symbol} {interval} K线分页游标未前进：{cursor} -> {next_cursor}")
        cursor = next_cursor
        if len(data) < KLINE_LIMIT:
            break
        time.sleep(delay)
    return output


def get_klines_cached(conn: sqlite3.Connection, symbol: str, start_ms: int, end_ms: int,
                      delay: float = 0.12) -> None:
    for gap_start, gap_end in missing_ranges(conn, symbol, start_ms, end_ms):
        if gap_end < gap_start:
            continue
        rows = fetch_range(symbol, gap_start, gap_end, delay)
        if rows:
            conn.executemany(
                "INSERT OR REPLACE INTO klines_1m(symbol,open_time_ms,open,high,low,close) VALUES(?,?,?,?,?,?)",
                [(symbol, int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4])) for r in rows],
            )
            conn.commit()
        time.sleep(delay)


def get_daily_cached(conn: sqlite3.Connection, symbol: str, start_ms: int, end_ms: int,
                     delay: float = 0.12) -> None:
    """抓取日线并写入缓存。start_ms/end_ms 对齐到 UTC 日边界。"""
    day_start = (start_ms // DAY_MS) * DAY_MS
    day_end = (end_ms // DAY_MS) * DAY_MS
    for gap_start, gap_end in missing_daily_ranges(conn, symbol, day_start, day_end):
        if gap_end < gap_start:
            continue
        rows = fetch_range(symbol, gap_start, gap_end, delay, interval="1d")
        if rows:
            conn.executemany(
                "INSERT OR REPLACE INTO klines_1d(symbol,open_time_ms,open,high,low,close) VALUES(?,?,?,?,?,?)",
                [(symbol, int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4])) for r in rows],
            )
            conn.commit()
        time.sleep(delay)


def get_bar_range(entry_time: dt.datetime, window_end: dt.datetime) -> Tuple[int, int, int]:
    """只返回完整 1m K 线范围；信号所在分钟整根排除，window_end 为右开边界。"""
    entry_ms = int(entry_time.timestamp() * 1000)
    end_ms = int(window_end.timestamp() * 1000)
    first_open = (entry_ms // MINUTE_MS + 1) * MINUTE_MS
    # K线必须在 window_end 之前完整收盘，因此 open + 60s <= window_end。
    last_open = ((end_ms - MINUTE_MS) // MINUTE_MS) * MINUTE_MS
    if last_open < first_open:
        return first_open, last_open, 0
    expected = (last_open - first_open) // MINUTE_MS + 1
    return first_open, last_open, expected


def compute_pre_run(conn: sqlite3.Connection, symbol: str, entry_time: dt.datetime,
                    entry_price: float, days: int) -> Optional[float]:
    """入场前 N 天的涨幅 (%) = 入场价 / N根之前的日线close - 1。

    用于识别"已经拉太高"（可能是出货信号）和"长期下跌"（弱势）的样本。
    返回 None 表示日线不足, 无法计算。
    """
    d = load_daily(conn, symbol)
    if len(d) < days:
        return None
    entry_ms = int(entry_time.timestamp() * 1000)
    # 只用入场时刻之前已经收盘的日线 (UTC 日边界)
    past = [row for row in d if row[0] < entry_ms - (entry_ms % DAY_MS)]
    if len(past) < days:
        return None
    base_close = float(past[-days][4])
    if base_close <= 0:
        return None
    return (entry_price / base_close - 1.0) * 100.0


def analyze_window(conn: sqlite3.Connection, sample: Sample, delay: float = 0.12) -> dict:
    start_ms, end_ms, expected = get_bar_range(sample.window_start, sample.window_end)
    if expected <= 0:
        return {"ok": False, "reason": "窗口内没有完整的信号后 1m K 线", "expected": expected}

    get_klines_cached(conn, sample.symbol, start_ms, end_ms, delay)
    bars = load_bars(conn, sample.symbol, start_ms, end_ms)
    timestamps = [int(row[0]) for row in bars]
    missing = expected - len(timestamps)
    # 检查是否有重复/越界/时间断档，避免部分行情被误当成完整最高价。
    complete = len(timestamps) == expected and all(
        timestamp == start_ms + i * MINUTE_MS for i, timestamp in enumerate(timestamps)
    )
    if not bars:
        return {"ok": False, "reason": "没有获取到K线", "expected": expected, "loaded": 0, "missing": expected}

    # ---- peak: 入场当日 1m + 之后各日日线 high 取大 ----
    # 1m 只覆盖信号当天; 跨天的涨幅用日线, 数据量小 1440 倍。
    peak = max([sample.entry_price] + [float(bar[2]) for bar in bars])
    last_close = float(bars[-1][4])

    # 之后日期的日线 (含信号当日之后的每一天)
    get_daily_cached(conn, sample.symbol, start_ms, end_ms, delay)
    day_bars = load_daily_range(conn, sample.symbol, start_ms, end_ms)
    if day_bars:
        peak = max(peak, max(float(d[2]) for d in day_bars))

    mfe_pct = (peak / sample.entry_price - 1.0) * 100.0
    armed = peak >= sample.entry_price * (1.0 + BREAKEVEN_TRIGGER)

    if armed:
        estimated_exit = max(sample.entry_price, peak * PEAK_EXIT_RATIO)
        if peak * PEAK_EXIT_RATIO <= sample.entry_price:
            exit_reason = "保本估算(峰值×70%低于入场价)"
        else:
            exit_reason = "最高价×70%人工平仓估算"
        exit_price = estimated_exit
        exit_time = ""  # 该价格是事后估算值，不伪造一个实际成交时间。
    else:
        # 没有涨到 +20%，保本保护从未激活；无初始止损，按区间末收盘估算。
        exit_price = last_close
        exit_reason = "未触发20%保本，窗口末收盘"
        exit_time = fmt_dt(dt.datetime.fromtimestamp((bars[-1][0] + MINUTE_MS) / 1000, TZ))

    window_hours = (sample.window_end - sample.window_start).total_seconds() / 3600.0
    gross = ENTRY_NOTIONAL * (exit_price / sample.entry_price - 1.0)
    return {
        "ok": True,
        "complete": complete,
        "expected": expected,
        "loaded": len(bars),
        "missing": max(missing, 0),
        "peak": peak,
        "mfe_pct": mfe_pct,
        "breakeven_triggered": armed,
        "window_end_close": last_close,
        "exit_price": exit_price,
        "exit_reason": exit_reason,
        "exit_time": exit_time,
        "window_hours": window_hours,
        "gross": gross,
    }


def write_csv(path: str, rows: List[dict]) -> None:
    if not rows:
        print(f"  没有数据，未生成：{path}")
        return
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    fields = list(rows[0].keys())
    with open(path, "w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def summarize(results: List[dict]) -> List[dict]:
    groups: Dict[str, List[dict]] = defaultdict(list)
    groups["ALL"] = list(results)
    for row in results:
        groups[row["signal_type"] or "UNKNOWN"].append(row)
    output = []
    for name, rows in groups.items():
        nets = [float(x["net_pnl_usdt"]) for x in rows]
        rois = [float(x["roi_on_margin_pct"]) for x in rows]
        output.append({
            "signal_type": name,
            "trades": len(rows),
            "win_rate_pct": round(sum(1 for x in nets if x > 0) / len(nets) * 100, 2) if nets else 0,
            "total_net_pnl_usdt": round(sum(nets), 6),
            "avg_net_pnl_usdt": round(statistics.mean(nets), 6) if nets else 0,
            "avg_roi_on_margin_pct": round(statistics.mean(rois), 4) if rois else 0,
            "median_roi_on_margin_pct": round(statistics.median(rois), 4) if rois else 0,
        })
    return output


def resolve_default_csv() -> str:
    candidates = [
        DEFAULT_CSV,
        os.path.join(ROOT, "snapshot_rows.csv"),
        os.path.join(ROOT, "current_top_snapshot.csv"),
    ]
    return next((path for path in candidates if os.path.isfile(path)), DEFAULT_CSV)


def main() -> int:
    global ENTRY_NOTIONAL, PRINCIPAL_USDT, LEVERAGE, FEE_RATE
    parser = argparse.ArgumentParser(
        description="CryptoMonitor Top 榜去重回测：无初始止损、涨20%保本、区间最高价×70%人工平仓估算。"
    )
    parser.add_argument("--csv", default=resolve_default_csv(), help="多日 Top 快照 CSV")
    parser.add_argument("--until", default="", help="手动指定统计截止时间(UTC+8)，如 2026-10-24 00:50:00；覆盖 CSV 最大 as_of")
    parser.add_argument("--cache", default=DEFAULT_CACHE, help="独立 1m K 线 SQLite 缓存")
    parser.add_argument("--out-dir", default=DEFAULT_OUT_DIR, help="结果输出目录")
    parser.add_argument("--funding-rate", type=float, default=0.0, help="固定资金费率近似值；默认0，例如 0.0001")
    parser.add_argument("--fee-rate", type=float, default=FEE_RATE, help="单边手续费率，默认0.0004")
    parser.add_argument("--principal", type=float, default=PRINCIPAL_USDT, help="每笔保证金，默认10 USDT")
    parser.add_argument("--leverage", type=float, default=LEVERAGE, help="杠杆倍数，默认10")
    parser.add_argument("--first-signal-source", choices=["top_display", "continuous_segment"], default="top_display",
                        help="current_top_snapshot.csv 使用哪组首次信号字段，默认 top_display")
    parser.add_argument("--allow-incomplete", action="store_true", help="K线有缺口时仍计算；默认缺口样本跳过")
    parser.add_argument("--delay", type=float, default=0.12, help="分页请求间隔秒数")
    parser.add_argument("--pre-days", type=int, default=7,
                        help="计算入场前涨幅的回看天数（用日线close），默认7")
    parser.add_argument("--pre-min", type=float, default=None,
                        help="入场前涨幅下限%%：低于该值视为长期弱势，不做")
    parser.add_argument("--pre-max", type=float, default=None,
                        help="入场前涨幅上限%%：高于该值视为拉太高（可能出货），不做")
    parser.add_argument("--pre-min-days", type=int, default=3,
                        help="配合 --pre-min 使用的回看天数，默认3")
    parser.add_argument("--pre-max-days", type=int, default=3,
                        help="配合 --pre-max 使用的回看天数，默认3")
    args = parser.parse_args()

    if args.principal <= 0 or args.leverage <= 0:
        parser.error("--principal 和 --leverage 必须大于0")
    if not (0 <= args.fee_rate < 1):
        parser.error("--fee-rate 必须在 [0,1) 之间")
    if not (-1 < args.funding_rate < 1):
        parser.error("--funding-rate 必须在 (-1,1) 之间，负值表示多头收取资金费")

    # 使用参数值更新本次回测的仓位与资金费率。
    PRINCIPAL_USDT = args.principal
    LEVERAGE = args.leverage
    ENTRY_NOTIONAL = PRINCIPAL_USDT * LEVERAGE
    FEE_RATE = args.fee_rate

    long_only = True
    samples, stats = load_samples(
        args.csv,
        until_text=args.until,
        long_only=long_only,
        first_signal_source=args.first_signal_source,
    )

    print("=" * 78)
    print("CryptoMonitor Top 榜回测：20% 保本触发 + 区间最高价×70%平仓估算")
    print("=" * 78)
    print(f"快照 CSV             : {args.csv}")
    print(f"快照总行数           : {stats['total_rows']}")
    print(f"无效行               : {stats['invalid_rows']}")
    print(f"全方向唯一事件数     : {stats['unique_events_all_directions']}")
    print(f"参与回测事件数       : {stats['trade_samples']}")
    print(f"币种数               : {stats['symbols']}")
    print(f"统计截止时间         : {fmt_dt(stats['global_end'], True)} ({stats['end_source']})")
    print(f"同一事件入场价冲突数 : {stats['price_conflicts']}（保留最早快照中的有效价格）")
    print(f"交易规则             : 无初始止损 | +20%后保本 | max(入场价, 区间最高价×70%)")
    print(f"仓位                 : {PRINCIPAL_USDT:g}U × {LEVERAGE:g}x = {ENTRY_NOTIONAL:g}U")
    print(f"成本                 : 单边手续费 {FEE_RATE:.4%} | 固定资金费率近似 {args.funding_rate:.6%}")
    print("说明                 : 不模拟强平；K线缺口默认跳过；最高价×70%为事后人工平仓估算")

    if not samples:
        print("没有符合条件的做多样本。")
        return 0

    spans = [(x.window_end - x.window_start).total_seconds() / 3600 for x in samples]
    print(f"窗口时长             : 中位 {statistics.median(spans):.2f}h / 均值 {statistics.mean(spans):.2f}h / 最大 {max(spans):.2f}h")

    conn = init_cache(args.cache)
    results: List[dict] = []
    skipped: List[dict] = []
    filtered = 0
    try:
        for index, sample in enumerate(samples, start=1):
            # ---- 前期涨幅筛选 (先算, 避免为被筛掉的样本浪费 1m K线请求) ----
            pre_run = None
            if args.pre_min is not None or args.pre_max is not None:
                # 先确保日线已缓存 (峰值计算也要用)
                entry_ms = int(sample.signal_time.timestamp() * 1000)
                end_ms = int(sample.window_end.timestamp() * 1000)
                get_daily_cached(conn, sample.symbol, entry_ms, end_ms, max(args.delay, 0.0))
                if args.pre_min is not None:
                    pre_run = compute_pre_run(conn, sample.symbol, sample.signal_time,
                                             sample.entry_price, args.pre_min_days)
                    if pre_run is not None and pre_run < args.pre_min:
                        filtered += 1
                        continue
                if args.pre_max is not None:
                    pre_max_val = compute_pre_run(conn, sample.symbol, sample.signal_time,
                                                  sample.entry_price, args.pre_max_days)
                    if pre_max_val is not None and pre_max_val > args.pre_max:
                        filtered += 1
                        continue

            try:
                data = analyze_window(conn, sample, delay=max(args.delay, 0.0))
            except Exception as exc:
                skipped.append({
                    "symbol": sample.symbol,
                    "signal_time": fmt_dt(sample.signal_time, True),
                    "window_end": fmt_dt(sample.window_end, True),
                    "reason": f"K线请求/计算异常：{exc}",
                })
                print(f"[{index}/{len(samples)}] {sample.symbol} 失败：{exc}", flush=True)
                continue

            if not data.get("ok"):
                skipped.append({
                    "symbol": sample.symbol,
                    "signal_time": fmt_dt(sample.signal_time, True),
                    "window_end": fmt_dt(sample.window_end, True),
                    "reason": data.get("reason", "unknown"),
                })
                continue
            if not data["complete"] and not args.allow_incomplete:
                skipped.append({
                    "symbol": sample.symbol,
                    "signal_time": fmt_dt(sample.signal_time, True),
                    "window_end": fmt_dt(sample.window_end, True),
                    "reason": f"1m K线不完整：期望{data['expected']}根，实际{data['loaded']}根，缺{data['missing']}根",
                })
                continue

            gross = data["gross"]
            fee = (ENTRY_NOTIONAL + ENTRY_NOTIONAL * data["exit_price"] / sample.entry_price) * FEE_RATE
            funding = (data["window_hours"] / 8.0) * args.funding_rate * ENTRY_NOTIONAL
            net = gross - fee - funding
            result = {
                "symbol": sample.symbol,
                "signal_type": sample.signal_type,
                "signal_time_utc8": fmt_dt(sample.signal_time, True),
                "window_end_exclusive_utc8": fmt_dt(sample.window_end, True),
                "entry_price": sample.entry_price,
                "peak_price": data["peak"],
                "mfe_pct": round(data["mfe_pct"], 4),
                "pre_run_pct": ("" if pre_run is None else round(pre_run, 4)),
                "breakeven_triggered_20pct": data["breakeven_triggered"],
                "window_end_close": data["window_end_close"],
                "estimated_exit_price": data["exit_price"],
                "exit_reason": data["exit_reason"],
                "estimated_exit_time_utc8": data["exit_time"],
                "window_hours": round(data["window_hours"], 4),
                "bars_expected": data["expected"],
                "bars_loaded": data["loaded"],
                "gross_pnl_usdt": round(gross, 8),
                "fee_usdt": round(fee, 8),
                "funding_usdt_approx": round(funding, 8),
                "net_pnl_usdt": round(net, 8),
                "roi_on_margin_pct": round(net / PRINCIPAL_USDT * 100.0, 4),
                "kline_complete": data["complete"],
            }
            results.append(result)

            if index % 25 == 0 or index == len(samples):
                print(f"进度 {index}/{len(samples)} | 有效 {len(results)} | 跳过 {len(skipped)}", flush=True)
    finally:
        conn.close()

    os.makedirs(args.out_dir, exist_ok=True)
    result_path = os.path.join(args.out_dir, "backtest_results.csv")
    summary_path = os.path.join(args.out_dir, "backtest_summary.csv")
    skipped_path = os.path.join(args.out_dir, "backtest_skipped.csv")
    write_csv(result_path, results)
    write_csv(summary_path, summarize(results))
    write_csv(skipped_path, skipped)

    print("=" * 78)
    print(f"回测结束：有效 {len(results)} 笔，跳过 {len(skipped)} 笔，前期涨幅筛掉 {filtered} 笔")
    if results:
        total_net = sum(x["net_pnl_usdt"] for x in results)
        avg_roi = statistics.mean(x["roi_on_margin_pct"] for x in results)
        win_rate = sum(1 for x in results if x["net_pnl_usdt"] > 0) / len(results) * 100
        armed_count = sum(1 for x in results if x["breakeven_triggered_20pct"])
        print(f"净利润合计：{total_net:+.4f} USDT")
        print(f"平均保证金 ROI：{avg_roi:+.2f}% | 胜率：{win_rate:.2f}% | 触发保本：{armed_count}/{len(results)}")
    print(f"结果文件：{result_path}")
    print(f"汇总文件：{summary_path}")
    print(f"跳过明细：{skipped_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
