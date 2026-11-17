#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""回测当前 CryptoMonitor Top 币种：首次上榜信号价 -> 信号之后的最高价。

规则与 utils.py 的 get_today_symbols_top() 对齐：
- 数据来源：alerts.db 中 priority='urgent' 且 price_at_alert 不为空的记录。
- 每个币取时间最新的连续信号段；相邻信号间隔超过 12 小时就从后一个信号开启新段。
- 最后一条信号距回测时点超过 24 小时的币不进入当前 Top。
- 默认最多取 Top 100，按当前段信号数降序、最后信号时间降序排序。
- 首次入场时间/价格就是当前 Top 这一段的 first_ts/first_price。

高点计算避免 1d K 线前视偏差：
- Binance 1d K 线按 UTC 日切分。信号所在 UTC 日完全不用 1d high/low，而是取信号所在分钟之后的 1m K 线。
- 只有从下一个 UTC 日开始，才使用 1d K 线的 high/low。
- 如果最高点/最低点来自后续的 1d K 线，会再下载那一天的 1m K 线定位到分钟。
- 信号触发所在的 1m K 线被排除，因为 OHLC 无法确定该分钟内最高/最低价是在报警之前还是之后；这是保守处理。

仅用 Python 标准库。不会修改 alerts.db；K 线缓存单独保存在输出目录。
"""
import argparse
import csv
import datetime as dt
import json
import os
import re
import sqlite3
import statistics
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

BINANCE_BASE = "https://fapi.binance.com"
TZ = dt.timezone(dt.timedelta(hours=8))
UTC = dt.timezone.utc
MINUTE_MS = 60_000
DAY_MS = 86_400_000
KLINE_LIMIT = 1500
ACTIVE_HOURS = 24
SILENT_HOURS = 12

SIGNAL_DIRECTION = {
    "🚀暴涨": 1,
    "🚀暴涨3m": 1,
    "📈爬升": 1,
    "🩸暴跌": -1,
    "🩸暴跌3m": -1,
    "📉阴跌": -1,
}


@dataclass
class AlertRow:
    row_id: Optional[int]
    timestamp: dt.datetime
    symbol: str
    signal_type: str
    price: float
    message: str = ""

    @property
    def epoch_ms(self) -> int:
        return int(self.timestamp.timestamp() * 1000)


@dataclass
class TopCoin:
    rank: int
    symbol: str
    # current Top 函数当前实际显示的 first_ts / first_price（保留现有行为）
    count: int
    display_first: AlertRow
    latest: AlertRow
    # 按 Top 函数注释本来想表达的“最近一个连续信号段”起点
    segment_count: int
    segment_first: AlertRow


def parse_dt(text: str) -> dt.datetime:
    value = dt.datetime.fromisoformat(text.strip().replace("T", " "))
    if value.tzinfo is None:
        value = value.replace(tzinfo=TZ)
    return value.astimezone(TZ)


def parse_db_dt(value) -> Optional[dt.datetime]:
    if value is None:
        return None
    try:
        parsed = dt.datetime.fromisoformat(str(value).strip().replace("T", " "))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=TZ)
        return parsed.astimezone(TZ)
    except (TypeError, ValueError):
        return None


def fmt_dt(value: Optional[dt.datetime]) -> str:
    return value.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S") if value else ""


def load_current_top(db_path: str, as_of: dt.datetime, limit: int,
                     min_count: int = 0) -> List[TopCoin]:
    if not os.path.isfile(db_path):
        raise FileNotFoundError(f"找不到 alerts.db：{db_path}")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        columns = {row["name"] for row in conn.execute("PRAGMA table_info(alerts)")}
        required = {"id", "timestamp", "symbol", "signal_type", "price_at_alert", "priority"}
        missing = required - columns
        if missing:
            raise RuntimeError(f"alerts 表缺少字段 {sorted(missing)}；实际字段：{sorted(columns)}")
        message_expr = "message" if "message" in columns else "'' AS message"
        # 不指定固定 since/until；从历史表重建当前 Top。
        rows = conn.execute(
            f"""SELECT id, timestamp, symbol, signal_type, price_at_alert, {message_expr}
                FROM alerts
                WHERE priority='urgent'
                  AND price_at_alert IS NOT NULL
                  AND timestamp <= ?
                ORDER BY symbol ASC, timestamp ASC""",
            (fmt_dt(as_of),),
        ).fetchall()
    finally:
        conn.close()

    by_symbol: Dict[str, List[AlertRow]] = defaultdict(list)
    ignored = 0
    for row in rows:
        timestamp = parse_db_dt(row["timestamp"])
        try:
            price = float(row["price_at_alert"])
        except (ValueError, TypeError):
            ignored += 1
            continue
        symbol = str(row["symbol"] or "").strip().upper()
        if not timestamp or not symbol or price <= 0:
            ignored += 1
            continue
        by_symbol[symbol].append(AlertRow(
            row_id=int(row["id"]) if row["id"] is not None else None,
            timestamp=timestamp,
            symbol=symbol,
            signal_type=str(row["signal_type"] or "").strip(),
            price=price,
            message=str(row["message"] or ""),
        ))

    as_of_epoch = as_of.timestamp()
    active_seconds = ACTIVE_HOURS * 3600
    silent_seconds = SILENT_HOURS * 3600
    candidates = []
    no_gap_symbols = 0

    for symbol, alerts in by_symbol.items():
        alerts.sort(key=lambda item: (item.timestamp, item.row_id or 0))
        if not alerts:
            continue
        last_epoch = alerts[-1].timestamp.timestamp()
        if as_of_epoch - last_epoch > active_seconds:
            continue

        # 与当前 get_today_symbols_top() 完全一致的“显示起点”算法。
        # 当前函数从最后一条开始倒查；若找到 >12h 的间隔，就从间隔后的记录开始。
        # 但原代码的默认值是最后一条记录，若整段从未出现 >12h 间隔，就会错误地只保留最后一条。
        display_start_idx = len(alerts) - 1
        found_gap = False
        for i in range(len(alerts) - 2, -1, -1):
            gap = (alerts[i + 1].timestamp - alerts[i].timestamp).total_seconds()
            if gap > silent_seconds:
                display_start_idx = i + 1
                found_gap = True
                break
        if not found_gap:
            no_gap_symbols += 1

        # 按函数注释的意图，若没有 >12h 的断点，则连续段应从最早记录开始；
        # 若找到断点，则与显示算法一致，从断点之后的第一条开始。
        segment_start_idx = display_start_idx if found_gap else 0
        display_segment = alerts[display_start_idx:]
        intended_segment = alerts[segment_start_idx:]
        candidates.append((
            len(display_segment),
            alerts[-1].timestamp.timestamp(),
            symbol,
            display_segment[0],
            alerts[-1],
            len(intended_segment),
            intended_segment[0],
        ))

    # 排名仍按实际运行中的 Top 逻辑排序，确保选出来的币种尽量与当前显示一致。
    candidates.sort(key=lambda item: (-item[0], -item[1]))

    # 🐾 可选: 按窗口内信号次数过滤 (min_count)。
    #    count>=2 能剔除"单次信号就沉寂"的冷门币, 但这类币仍可能有大波动。
    total_candidates = len(candidates)
    if min_count > 0:
        candidates = [item for item in candidates if item[0] >= min_count]

    output = []
    for rank, (display_count, _last_epoch, symbol, display_first, latest, segment_count, segment_first) in enumerate(candidates[:limit], start=1):
        output.append(TopCoin(
            rank=rank,
            symbol=symbol,
            count=display_count,
            display_first=display_first,
            latest=latest,
            segment_count=segment_count,
            segment_first=segment_first,
        ))

    print(f"alerts.db 有效 urgent 价格记录：{sum(len(v) for v in by_symbol.values())} 条；无效行跳过：{ignored} 条")
    print(f"最近 {ACTIVE_HOURS} 小时活跃币种：{total_candidates} 个")
    if min_count > 0:
        print(f"过滤 count >= {min_count} 后剩余：{len(candidates)} 个")
    print(f"本次分析 Top {len(output)}")
    if no_gap_symbols:
        print(f"[注意] {no_gap_symbols} 个活跃币没有任何相邻信号间隔超过 {SILENT_HOURS}h。当前 Top 源码会把它们的显示起点错误设为最后一条；CSV 会同时输出源码显示起点和连续段起点。")
    return output

def request_json(url: str, timeout: int = 20, retries: int = 4):
    last_error = None
    for attempt in range(retries):
        request = urllib.request.Request(
            url,
            headers={"User-Agent": "CryptoMonitor-TopHigh-Backtest/1.0", "Accept": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                body = response.read()
            return json.loads(body.decode("utf-8"))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")[:500]
            last_error = RuntimeError(f"HTTP {exc.code}: {detail}")
            if exc.code not in (418, 429, 500, 502, 503, 504):
                raise last_error
            wait = min(2 ** attempt, 16)
            print(f"  [Binance 限流/服务错误] HTTP {exc.code}，{wait}s 后重试", flush=True)
            time.sleep(wait)
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_error = exc
            wait = min(2 ** attempt, 16)
            print(f"  [网络错误] {exc}，{wait}s 后重试", flush=True)
            time.sleep(wait)
    raise RuntimeError(f"请求失败，重试 {retries} 次仍失败：{last_error}")


def fetch_klines(symbol: str, interval: str, start_ms: int, end_ms: int, delay: float):
    """下载 [start_ms, end_ms] 范围内的 1m 或 1d K 线；时间参数是 K 线 open_time。"""
    if start_ms > end_ms:
        return []
    step = MINUTE_MS if interval == "1m" else DAY_MS
    url = f"{BINANCE_BASE}/fapi/v1/klines"
    cursor = start_ms
    output = []
    while cursor <= end_ms:
        params = urllib.parse.urlencode({
            "symbol": symbol,
            "interval": interval,
            "startTime": cursor,
            "endTime": end_ms,
            "limit": KLINE_LIMIT,
        })
        data = request_json(f"{url}?{params}")
        if not isinstance(data, list):
            raise RuntimeError(f"K 线接口响应格式异常：{str(data)[:300]}")
        if not data:
            break
        for item in data:
            try:
                open_ms = int(item[0])
                if start_ms <= open_ms <= end_ms:
                    output.append({
                        "open_time_ms": open_ms,
                        "open": float(item[1]),
                        "high": float(item[2]),
                        "low": float(item[3]),
                        "close": float(item[4]),
                    })
            except (IndexError, TypeError, ValueError):
                continue
        newest = int(data[-1][0])
        if newest < cursor or newest + step <= cursor:
            raise RuntimeError(f"K 线分页游标未推进：{symbol} {interval} cursor={cursor} newest={newest}")
        if len(data) < KLINE_LIMIT or newest >= end_ms:
            break
        cursor = newest + step
        if delay > 0:
            time.sleep(delay)
    return output


def init_cache(path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""CREATE TABLE IF NOT EXISTS top_roi_klines (
        symbol TEXT NOT NULL, interval TEXT NOT NULL, open_time_ms INTEGER NOT NULL,
        open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL, close REAL NOT NULL,
        PRIMARY KEY(symbol, interval, open_time_ms)
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS top_roi_coverage (
        symbol TEXT NOT NULL, interval TEXT NOT NULL, start_ms INTEGER NOT NULL,
        end_ms INTEGER NOT NULL, fetched_at TEXT NOT NULL,
        PRIMARY KEY(symbol, interval, start_ms, end_ms)
    )""")
    conn.commit()
    return conn


def missing_ranges(conn: sqlite3.Connection, symbol: str, interval: str, start_ms: int, end_ms: int):
    step = MINUTE_MS if interval == "1m" else DAY_MS
    rows = conn.execute(
        """SELECT start_ms,end_ms FROM top_roi_coverage
           WHERE symbol=? AND interval=? AND end_ms>=? AND start_ms<=? ORDER BY start_ms""",
        (symbol, interval, start_ms, end_ms),
    ).fetchall()
    ranges = []
    cursor = start_ms
    for covered_start, covered_end in rows:
        covered_start = max(int(covered_start), start_ms)
        covered_end = min(int(covered_end), end_ms)
        if covered_end < cursor:
            continue
        if covered_start > cursor:
            ranges.append((cursor, covered_start - step))
        cursor = max(cursor, covered_end + step)
        if cursor > end_ms:
            break
    if cursor <= end_ms:
        ranges.append((cursor, end_ms))
    return [(left, right) for left, right in ranges if left <= right]


def get_klines_cached(conn: sqlite3.Connection, symbol: str, interval: str,
                      start_ms: int, end_ms: int, delay: float):
    if start_ms > end_ms:
        return []
    step = MINUTE_MS if interval == "1m" else DAY_MS
    for missing_start, missing_end in missing_ranges(conn, symbol, interval, start_ms, end_ms):
        rows = fetch_klines(symbol, interval, missing_start, missing_end, delay)
        fetched_at = dt.datetime.now(TZ).isoformat(timespec="seconds")
        with conn:
            conn.executemany(
                """INSERT OR REPLACE INTO top_roi_klines
                   (symbol,interval,open_time_ms,open,high,low,close) VALUES(?,?,?,?,?,?,?)""",
                [(symbol, interval, row["open_time_ms"], row["open"], row["high"], row["low"], row["close"])
                 for row in rows],
            )
            # 请求成功后把范围标为已查询；下架/尚未上市的空结果也不需要每次重查。
            conn.execute(
                "INSERT OR REPLACE INTO top_roi_coverage(symbol,interval,start_ms,end_ms,fetched_at) VALUES(?,?,?,?,?)",
                (symbol, interval, missing_start, missing_end, fetched_at),
            )
        if delay > 0:
            time.sleep(delay)
    rows = conn.execute(
        """SELECT open_time_ms,open,high,low,close FROM top_roi_klines
           WHERE symbol=? AND interval=? AND open_time_ms>=? AND open_time_ms<=?
           ORDER BY open_time_ms""",
        (symbol, interval, start_ms, end_ms),
    ).fetchall()
    return [
        {"open_time_ms": int(r[0]), "open": float(r[1]), "high": float(r[2]), "low": float(r[3]), "close": float(r[4])}
        for r in rows
    ]


def bar_extreme(bars, field: str, want_max: bool):
    if not bars:
        return None
    key_fn = (lambda bar: bar[field])
    return (max if want_max else min)(bars, key=key_fn)


def analyze_entry(symbol: str, entry_time: dt.datetime, entry_price: float,
                  as_of: dt.datetime, cache: sqlite3.Connection, delay: float):
    """计算某一个给定信号时间/价格之后的最高价、最低价与方向性 MFE。"""
    as_of_ms = int(as_of.timestamp() * 1000)
    now_minute_open = (as_of_ms // MINUTE_MS) * MINUTE_MS
    entry_ms = int(entry_time.timestamp() * 1000)
    entry_minute_open = (entry_ms // MINUTE_MS) * MINUTE_MS
    utc_day_open = (entry_ms // DAY_MS) * DAY_MS
    utc_day_end_minute = utc_day_open + DAY_MS - MINUTE_MS

    # 排除信号所在分钟，避免把该分钟内报警之前出现的 high/low 纳入结果。
    minute_start = entry_minute_open + MINUTE_MS
    minute_end = min(utc_day_end_minute, now_minute_open)
    same_utc_day_bars = get_klines_cached(
        cache, symbol, "1m", minute_start, minute_end, delay
    ) if minute_start <= minute_end else []

    # Binance 1d K 线按 UTC 日切分。只有信号所在 UTC 日之后的日 K 才能安全使用。
    next_day_open = utc_day_open + DAY_MS
    current_utc_day_open = (as_of_ms // DAY_MS) * DAY_MS
    daily_bars = get_klines_cached(
        cache, symbol, "1d", next_day_open, current_utc_day_open, delay
    ) if next_day_open <= current_utc_day_open else []

    signal_day_high = bar_extreme(same_utc_day_bars, "high", True)
    signal_day_low = bar_extreme(same_utc_day_bars, "low", False)
    later_day_high = bar_extreme(daily_bars, "high", True)
    later_day_low = bar_extreme(daily_bars, "low", False)

    high_value = None
    high_open_ms = None
    high_precision = "no_data"
    high_day_source = None
    if signal_day_high is not None:
        high_value = signal_day_high["high"]
        high_open_ms = signal_day_high["open_time_ms"]
        high_precision = "1m"
        high_day_source = "signal_utc_day_1m"
    if later_day_high is not None and (high_value is None or later_day_high["high"] > high_value):
        high_value = later_day_high["high"]
        high_open_ms = later_day_high["open_time_ms"]
        high_precision = "1d_day_only"
        high_day_source = "later_utc_day_1d"

    low_value = None
    low_open_ms = None
    low_precision = "no_data"
    low_day_source = None
    if signal_day_low is not None:
        low_value = signal_day_low["low"]
        low_open_ms = signal_day_low["open_time_ms"]
        low_precision = "1m"
        low_day_source = "signal_utc_day_1m"
    if later_day_low is not None and (low_value is None or later_day_low["low"] < low_value):
        low_value = later_day_low["low"]
        low_open_ms = later_day_low["open_time_ms"]
        low_precision = "1d_day_only"
        low_day_source = "later_utc_day_1d"

    target_days = set()
    if high_day_source == "later_utc_day_1d" and high_open_ms is not None:
        target_days.add(high_open_ms)
    if low_day_source == "later_utc_day_1d" and low_open_ms is not None:
        target_days.add(low_open_ms)
    precise_by_day = {}
    for day_open in sorted(target_days):
        day_end_minute = min(day_open + DAY_MS - MINUTE_MS, now_minute_open)
        day_bars = get_klines_cached(cache, symbol, "1m", day_open, day_end_minute, delay)
        if day_bars:
            precise_by_day[day_open] = day_bars

    high_time_label = ""
    if high_value is not None and high_open_ms is not None:
        if high_day_source == "signal_utc_day_1m":
            high_time_label = dt.datetime.fromtimestamp(high_open_ms / 1000, TZ).strftime("%Y-%m-%d %H:%M:%S")
        elif high_open_ms in precise_by_day:
            precise = bar_extreme(precise_by_day[high_open_ms], "high", True)
            if precise and precise["high"] >= high_value * (1 - 1e-8):
                high_value = precise["high"]
                high_open_ms = precise["open_time_ms"]
                high_precision = "1m"
                high_time_label = dt.datetime.fromtimestamp(high_open_ms / 1000, TZ).strftime("%Y-%m-%d %H:%M:%S")
        if not high_time_label:
            high_time_label = dt.datetime.fromtimestamp(high_open_ms / 1000, UTC).strftime("%Y-%m-%d UTC日（具体分钟未知）")

    low_time_label = ""
    if low_value is not None and low_open_ms is not None:
        if low_day_source == "signal_utc_day_1m":
            low_time_label = dt.datetime.fromtimestamp(low_open_ms / 1000, TZ).strftime("%Y-%m-%d %H:%M:%S")
        elif low_open_ms in precise_by_day:
            precise = bar_extreme(precise_by_day[low_open_ms], "low", False)
            if precise and precise["low"] <= low_value * (1 + 1e-8):
                low_value = precise["low"]
                low_open_ms = precise["open_time_ms"]
                low_precision = "1m"
                low_time_label = dt.datetime.fromtimestamp(low_open_ms / 1000, TZ).strftime("%Y-%m-%d %H:%M:%S")
        if not low_time_label:
            low_time_label = dt.datetime.fromtimestamp(low_open_ms / 1000, UTC).strftime("%Y-%m-%d UTC日（具体分钟未知）")

    long_roi_to_high = (high_value / entry_price - 1) * 100 if high_value and entry_price > 0 else None
    long_return_to_low = (low_value / entry_price - 1) * 100 if low_value and entry_price > 0 else None
    return {
        "entry_time_utc8": fmt_dt(entry_time),
        "entry_price": entry_price,
        "highest_price_after_entry": high_value,
        "highest_price_time_utc8_or_utc_day": high_time_label,
        "high_time_precision": high_precision,
        "long_roi_to_high_pct": long_roi_to_high,
        "lowest_price_after_entry": low_value,
        "lowest_price_time_utc8_or_utc_day": low_time_label,
        "low_time_precision": low_precision,
        "long_return_to_low_pct": long_return_to_low,
        "high_source": high_day_source or "no_data",
        "data_status": "ok" if high_value is not None else "no_future_kline_data",
    }


def analyze_coin(coin: TopCoin, as_of: dt.datetime, cache: sqlite3.Connection, delay: float):
    # 主结果严格对应现有 Top 消息源码显示的 first_ts / first_price。
    shown = analyze_entry(coin.symbol, coin.display_first.timestamp, coin.display_first.price, as_of, cache, delay)
    if (coin.segment_first.timestamp == coin.display_first.timestamp
            and coin.segment_first.price == coin.display_first.price):
        intended = shown
    else:
        intended = analyze_entry(coin.symbol, coin.segment_first.timestamp, coin.segment_first.price, as_of, cache, delay)

    shown_direction = SIGNAL_DIRECTION.get(coin.display_first.signal_type, 0)
    shown_directional_mfe = None
    if shown_direction > 0 and shown["highest_price_after_entry"]:
        shown_directional_mfe = (shown["highest_price_after_entry"] / coin.display_first.price - 1) * 100
    elif shown_direction < 0 and shown["lowest_price_after_entry"]:
        shown_directional_mfe = (coin.display_first.price / shown["lowest_price_after_entry"] - 1) * 100

    intended_direction = SIGNAL_DIRECTION.get(coin.segment_first.signal_type, 0)
    intended_directional_mfe = None
    if intended_direction > 0 and intended["highest_price_after_entry"]:
        intended_directional_mfe = (intended["highest_price_after_entry"] / coin.segment_first.price - 1) * 100
    elif intended_direction < 0 and intended["lowest_price_after_entry"]:
        intended_directional_mfe = (coin.segment_first.price / intended["lowest_price_after_entry"] - 1) * 100

    return {
        "rank": coin.rank,
        "symbol": coin.symbol,
        "top_display_count": coin.count,
        "continuous_segment_count_by_intended_rule": coin.segment_count,
        "top_display_first_signal_time_utc8": fmt_dt(coin.display_first.timestamp),
        "top_display_first_signal_type": coin.display_first.signal_type,
        "top_display_first_signal_price": coin.display_first.price,
        "top_display_highest_price_after_first_signal": shown["highest_price_after_entry"],
        "top_display_highest_price_time_utc8_or_utc_day": shown["highest_price_time_utc8_or_utc_day"],
        "top_display_high_time_precision": shown["high_time_precision"],
        "top_display_long_roi_to_high_pct": shown["long_roi_to_high_pct"],
        "top_display_lowest_price_after_first_signal": shown["lowest_price_after_entry"],
        "top_display_lowest_price_time_utc8_or_utc_day": shown["lowest_price_time_utc8_or_utc_day"],
        "top_display_long_return_to_low_pct": shown["long_return_to_low_pct"],
        "top_display_first_signal_direction": "LONG" if shown_direction > 0 else ("SHORT" if shown_direction < 0 else "UNKNOWN"),
        "top_display_directional_mfe_pct": shown_directional_mfe,
        "continuous_segment_first_signal_time_utc8": fmt_dt(coin.segment_first.timestamp),
        "continuous_segment_first_signal_type": coin.segment_first.signal_type,
        "continuous_segment_first_signal_price": coin.segment_first.price,
        "segment_highest_price_after_first_signal": intended["highest_price_after_entry"],
        "segment_highest_price_time_utc8_or_utc_day": intended["highest_price_time_utc8_or_utc_day"],
        "segment_high_time_precision": intended["high_time_precision"],
        "segment_long_roi_to_high_pct": intended["long_roi_to_high_pct"],
        "segment_lowest_price_after_first_signal": intended["lowest_price_after_entry"],
        "segment_lowest_price_time_utc8_or_utc_day": intended["lowest_price_time_utc8_or_utc_day"],
        "segment_long_return_to_low_pct": intended["long_return_to_low_pct"],
        "segment_first_signal_direction": "LONG" if intended_direction > 0 else ("SHORT" if intended_direction < 0 else "UNKNOWN"),
        "segment_directional_mfe_pct": intended_directional_mfe,
        "latest_alert_time_utc8": fmt_dt(coin.latest.timestamp),
        "latest_alert_type": coin.latest.signal_type,
        "latest_alert_price_db": coin.latest.price,
        "latest_alert_price_change_from_display_first_pct": (coin.latest.price / coin.display_first.price - 1) * 100,
        "data_status": shown["data_status"],
        "note": "主 ROI 按当前 Top 实际显示起点计算；segment_* 列按注释预期的连续段起点计算。",
    }

def write_csv(path: str, rows: List[dict]):
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    if not rows:
        with open(path, "w", newline="", encoding="utf-8-sig") as handle:
            handle.write("\n")
        return
    with open(path, "w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()), extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def build_parser():
    parser = argparse.ArgumentParser(
        description="读取当前 CryptoMonitor Top 币种，计算首次上榜信号价之后的最高价与 ROI。",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--db", default="/root/CryptoMonitor/alerts.db", help="alerts.db 路径")
    parser.add_argument("--limit", type=int, default=100, help="当前 Top 取前多少个币，默认 100，与项目排行一致")
    parser.add_argument("--min-count", type=int, default=0,
                        help="只分析窗口内信号次数 >= 该值的币，0 表示不过滤。"
                             "count>=2 可剔除只出现一次的冷门币")
    parser.add_argument("--as-of", default=None, help="可选：固定当前 Top 的统计时点，格式 YYYY-MM-DD HH:MM:SS；默认运行时刻")
    parser.add_argument("--out-dir", default="/root/CryptoMonitor/backtest_current_top", help="CSV 和 K 线缓存输出目录")
    parser.add_argument("--cache", default=None, help="K 线缓存 SQLite 路径；默认在输出目录下")
    parser.add_argument("--request-delay", type=float, default=0.12, help="每次 Binance 请求间隔秒数")
    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    if args.limit <= 0:
        parser.error("--limit 必须大于 0")
    if args.min_count < 0:
        parser.error("--min-count 不能小于 0")
    if args.request_delay < 0:
        parser.error("--request-delay 不能小于 0")

    as_of = parse_dt(args.as_of) if args.as_of else dt.datetime.now(TZ)
    db_path = os.path.abspath(args.db)
    out_dir = os.path.abspath(args.out_dir)
    os.makedirs(out_dir, exist_ok=True)
    cache_path = os.path.abspath(args.cache) if args.cache else os.path.join(out_dir, "top_high_klines_cache.sqlite3")

    print("=" * 78)
    print("CryptoMonitor 当前 Top：首次信号价格 -> 信号之后最高价")
    print(f"数据库       : {db_path}")
    print(f"Top 截止时点 : {fmt_dt(as_of)} UTC+8")
    print(f"Top 规则      : 最后一条信号距今 <= {ACTIVE_HOURS}h；间隔 > {SILENT_HOURS}h 则开启新统计段；取前 {args.limit}")
    print("最高价规则    : 信号所在 UTC 日用后续 1m；后续 UTC 日用 1d，规避信号前的当日最高价")
    print("=" * 78, flush=True)

    try:
        top = load_current_top(db_path, as_of, args.limit, args.min_count)
    except Exception as exc:
        print(f"读取当前 Top 失败：{exc}", file=sys.stderr)
        return 2
    if not top:
        print("当前没有满足 Top 活跃条件的币种。")
        return 0

    snapshot_rows = []
    for coin in top:
        snapshot_rows.append({
            "rank": coin.rank,
            "symbol": coin.symbol,
            "top_display_alert_count": coin.count,
            "continuous_segment_count_by_intended_rule": coin.segment_count,
            "top_display_first_signal_time_utc8": fmt_dt(coin.display_first.timestamp),
            "top_display_first_signal_type": coin.display_first.signal_type,
            "top_display_first_signal_price": coin.display_first.price,
            "continuous_segment_first_signal_time_utc8": fmt_dt(coin.segment_first.timestamp),
            "continuous_segment_first_signal_type": coin.segment_first.signal_type,
            "continuous_segment_first_signal_price": coin.segment_first.price,
            "latest_alert_time_utc8": fmt_dt(coin.latest.timestamp),
            "latest_alert_type": coin.latest.signal_type,
            "latest_alert_price_db": coin.latest.price,
        })
    snapshot_path = os.path.join(out_dir, "current_top_snapshot.csv")
    write_csv(snapshot_path, snapshot_rows)

    cache = init_cache(cache_path)
    results = []
    errors = []
    try:
        for index, coin in enumerate(top, start=1):
            print(f"[{index}/{len(top)}] {coin.symbol}: Top显示起点 {fmt_dt(coin.display_first.timestamp)} @ {coin.display_first.price}; 连续段起点 {fmt_dt(coin.segment_first.timestamp)} @ {coin.segment_first.price}", flush=True)
            try:
                result = analyze_coin(coin, as_of, cache, args.request_delay)
                results.append(result)
                high_text = f"{result['top_display_highest_price_after_first_signal']} ({result['top_display_long_roi_to_high_pct']:+.2f}%)" if result["top_display_long_roi_to_high_pct"] is not None else "无后续 K 线"
                segment_text = f"{result['segment_highest_price_after_first_signal']} ({result['segment_long_roi_to_high_pct']:+.2f}%)" if result["segment_long_roi_to_high_pct"] is not None else "无后续 K 线"
                print(f"    Top显示起点后最高={high_text}; 连续段起点后最高={segment_text}", flush=True)
            except Exception as exc:
                errors.append({"rank": coin.rank, "symbol": coin.symbol, "error": str(exc)})
                print(f"    [失败] {coin.symbol}: {exc}", flush=True)
    finally:
        cache.close()

    results.sort(key=lambda row: row["rank"])
    result_path = os.path.join(out_dir, "current_top_high_roi.csv")
    errors_path = os.path.join(out_dir, "current_top_errors.csv")
    write_csv(result_path, results)
    write_csv(errors_path, errors)

    valid = [row for row in results if isinstance(row["top_display_long_roi_to_high_pct"], (int, float))]
    long_rois = [float(row["top_display_long_roi_to_high_pct"]) for row in valid]
    segment_rois = [float(row["segment_long_roi_to_high_pct"]) for row in results if isinstance(row["segment_long_roi_to_high_pct"], (int, float))]
    directional = [float(row["top_display_directional_mfe_pct"]) for row in results if isinstance(row["top_display_directional_mfe_pct"], (int, float))]
    report_path = os.path.join(out_dir, "README_当前Top回测说明.txt")
    with open(report_path, "w", encoding="utf-8") as report:
        report.write("CryptoMonitor 当前 Top 首次信号 -> 后续最高价回测\n")
        report.write("=" * 60 + "\n")
        report.write(f"统计时点 UTC+8：{fmt_dt(as_of)}\n")
        report.write(f"Top 数量：{len(top)}\n")
        report.write(f"成功计算后续最高价：{len(valid)}\n")
        report.write(f"K 线失败币种：{len(errors)}\n")
        if long_rois:
            report.write(f"当前 Top 显示起点的平均 long ROI to high：{statistics.mean(long_rois):.4f}%\n")
            report.write(f"当前 Top 显示起点的中位数 long ROI to high：{statistics.median(long_rois):.4f}%\n")
        if segment_rois:
            report.write(f"连续段真实起点的平均 long ROI to high：{statistics.mean(segment_rois):.4f}%\n")
            report.write(f"连续段真实起点的中位数 long ROI to high：{statistics.median(segment_rois):.4f}%\n")
        if directional:
            report.write(f"有明确方向首信号的 Top 显示起点样本：{len(directional)}\n")
            report.write(f"方向性 MFE 均值：{statistics.mean(directional):.4f}%\n")
            report.write(f"方向性 MFE 中位数：{statistics.median(directional):.4f}%\n")
        report.write("\n重要口径：\n")
        report.write("1. Top 名单和排名顺序尽量逐字复现 utils.py 的 get_today_symbols_top()，不是指定一段历史 since/until。\n")
        report.write("2. top_display_* 是当前 Top 实际显示的 first_ts/first_price 后的结果。\n")
        report.write("3. segment_* 是按 Top 函数注释的意图计算的连续信号段起点后结果；若两者不同，说明 Top 源码可能将 first_ts 错设为最新一条。\n")
        report.write("4. long ROI to high=(信号后最高价/首次信号价-1)*100，是事后最大上行空间，不是实际可实现收益。\n")
        report.write("5. 方向性 MFE：首信号为 LONG 看后续最高价；为 SHORT 则看首信号价到后续最低价的空头有利幅度；OI/未知方向不计算方向性 MFE。\n")
        report.write("6. 信号所在分钟被排除，避免把报警前的分钟内最高/最低价纳入；可能保守漏掉报警后同一分钟内的极值。\n")
        report.write("7. Binance 1d K 线按 UTC 日切分；信号所在 UTC 日使用信号分钟之后的 1m K 线，后续 UTC 日才使用 1d high/low。\n")
        report.write("8. high/low 来自后续 1d 的，如果极值日 1m 数据成功取得，会定位极值分钟；否则只给 UTC 日，不伪造准确分钟。\n")
        report.write("9. current_top_snapshot.csv 保存本次 Top 名单快照；current_top_high_roi.csv 是逐币结果；current_top_errors.csv 记录失败交易对。\n")
        report.write("10. K 线缓存单独放置，不修改 alerts.db；再次运行会复用已下载的 K 线。\n")

    print("\n完成：")
    print(f"  Top 快照  : {snapshot_path}")
    print(f"  ROI 明细  : {result_path}")
    print(f"  异常列表  : {errors_path}")
    print(f"  说明文件  : {report_path}")
    print(f"  K线缓存   : {cache_path}")
    if long_rois:
        print(f"  Top 显示起点平均最高上行幅度：{statistics.mean(long_rois):.2f}%；中位数 {statistics.median(long_rois):.2f}%")
    if segment_rois:
        print(f"  连续段起点平均最高上行幅度：{statistics.mean(segment_rois):.2f}%；中位数 {statistics.median(segment_rois):.2f}%")
    return 0


if __name__ == "__main__":
    sys.exit(main())
