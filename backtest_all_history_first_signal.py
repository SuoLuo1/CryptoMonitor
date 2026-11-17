#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""CryptoMonitor 全历史首次有效价格信号回测：MFE/MAE、固定窗口收益、TP/SL。

默认从 alerts.db 中最早的有效 urgent 记录开始，直到当前时间；可用 --since/--until 限定区间。
仅使用 Python 标准库。默认根据 alerts 表近似重放事件状态机，因为 alerts 表没有保存 event_id。
如果指定 --trace-log，则只使用该日志文件中实际记录的 ALLOW/event_id，适合日志覆盖的时间段；
切勿把只保留数天的轮转日志当成整个数据库历史。

注意：MFE 最高点离场是理论最大有利空间，不代表实际可实现收益。
为避免下载信号之间的大段无用 K 线，本脚本只下载每个首次信号之后所需的观察窗口，并复用独立缓存。
"""
import argparse
import csv
import datetime as dt
import json
import math
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

TZ = dt.timezone(dt.timedelta(hours=8))  # CryptoMonitor 的 DB timestamp 使用北京时间/UTC+8
BINANCE_BASE = "https://fapi.binance.com"
ONE_MINUTE_MS = 60_000
KLINE_LIMIT = 1000  # Binance USDⓈ-M Futures /fapi/v1/klines 支持最多 1500；取 1000 控制单次请求权重

# DB 中的 signal_type -> (direction, internal kind)
PRICE_TYPES = {
    "🚀暴涨": (1, "initial_1m"),
    "🩸暴跌": (-1, "initial_1m"),
    "🚀暴涨3m": (1, "initial_3m"),
    "🩸暴跌3m": (-1, "initial_3m"),
}
TREND_TYPES = {"📈爬升": 1, "📉阴跌": -1}
DEFAULT_HORIZONS = [5, 15, 30, 60, 240, 720]


@dataclass
class Alert:
    row_id: Optional[int]
    timestamp: dt.datetime
    symbol: str
    signal_type: str
    price: float
    message: str = ""
    direction: int = 0
    kind: str = ""
    event_id: str = ""
    source: str = "db_reconstructed"
    trace_id: str = ""
    delivery_status: str = "unknown"
    telegram_message_id: str = ""

    @property
    def epoch(self) -> float:
        return self.timestamp.timestamp()

    @property
    def epoch_ms(self) -> int:
        return int(self.epoch * 1000)


@dataclass
class EventState:
    direction: int
    last_activity: float
    last_material_price: float
    last_price_signal: float


def parse_datetime(value: str, arg_name: str) -> dt.datetime:
    try:
        value = value.strip().replace("T", " ")
        value_dt = dt.datetime.fromisoformat(value)
        if value_dt.tzinfo is None:
            value_dt = value_dt.replace(tzinfo=TZ)
        return value_dt.astimezone(TZ)
    except Exception as exc:
        raise argparse.ArgumentTypeError(
            f"{arg_name} 时间格式不正确：{value!r}。请使用 YYYY-MM-DD HH:MM:SS"
        ) from exc


def db_time_string(value: dt.datetime) -> str:
    return value.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S")


def parse_db_timestamp(value) -> Optional[dt.datetime]:
    if value is None:
        return None
    try:
        parsed = dt.datetime.fromisoformat(str(value).strip().replace("T", " "))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=TZ)
        return parsed.astimezone(TZ)
    except Exception:
        return None



def get_db_bounds(db_path: str, until: dt.datetime) -> Optional[dt.datetime]:
    """返回 alerts 表中最早的有效 urgent 价格信号时间。"""
    if not os.path.isfile(db_path):
        raise FileNotFoundError(f"找不到 alerts.db：{db_path}")
    conn = sqlite3.connect(db_path)
    try:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(alerts)")}
        required = {"timestamp", "price_at_alert", "priority"}
        missing = required - columns
        if missing:
            raise RuntimeError(f"alerts 表缺少字段 {sorted(missing)}；实际字段：{sorted(columns)}")
        row = conn.execute(
            """SELECT MIN(timestamp) FROM alerts
               WHERE priority='urgent' AND price_at_alert IS NOT NULL
                 AND CAST(price_at_alert AS REAL) > 0 AND timestamp <= ?""",
            (db_time_string(until),),
        ).fetchone()
        return parse_db_timestamp(row[0]) if row and row[0] else None
    finally:
        conn.close()


def load_alerts(db_path: str, query_start: dt.datetime, query_end: dt.datetime) -> List[Alert]:
    if not os.path.isfile(db_path):
        raise FileNotFoundError(f"找不到 alerts.db：{db_path}")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        columns = {row["name"] for row in conn.execute("PRAGMA table_info(alerts)")}
        required = {"id", "timestamp", "symbol", "signal_type", "price_at_alert", "priority"}
        missing = required - columns
        if missing:
            raise RuntimeError(
                f"alerts 表缺少字段 {sorted(missing)}；实际字段：{sorted(columns)}"
            )
        message_expr = "message" if "message" in columns else "'' AS message"
        sql = f"""
            SELECT id, timestamp, symbol, signal_type, price_at_alert,
                   {message_expr}, priority
            FROM alerts
            WHERE priority = 'urgent'
              AND price_at_alert IS NOT NULL
              AND timestamp >= ?
              AND timestamp <= ?
            ORDER BY timestamp ASC, id ASC
        """
        rows = conn.execute(
            sql, (db_time_string(query_start), db_time_string(query_end))
        ).fetchall()
    finally:
        conn.close()

    output: List[Alert] = []
    for row in rows:
        timestamp = parse_db_timestamp(row["timestamp"])
        try:
            price = float(row["price_at_alert"])
        except (TypeError, ValueError):
            continue
        symbol = str(row["symbol"] or "").strip().upper()
        signal_type = str(row["signal_type"] or "").strip()
        if not timestamp or not symbol or price <= 0:
            continue
        output.append(
            Alert(
                row_id=int(row["id"]) if row["id"] is not None else None,
                timestamp=timestamp,
                symbol=symbol,
                signal_type=signal_type,
                price=price,
                message=str(row["message"] or ""),
            )
        )
    return output


def trace_key_values(line: str) -> Dict[str, str]:
    marker = line.find("[SIGNAL_TRACE]")
    if marker < 0:
        return {}
    tail = line[marker + len("[SIGNAL_TRACE]"):]
    # 所需字段均为无空格的 key=value；price、方向和 symbol 也兼容中文 token。
    return dict(re.findall(r"([A-Za-z_][A-Za-z0-9_]*)=([^\s]+)", tail))


def parse_trace_timestamp(kv: Dict[str, str]) -> Optional[dt.datetime]:
    try:
        if kv.get("ts"):
            return dt.datetime.fromtimestamp(float(kv["ts"]), TZ)
        trace_id = kv.get("trace_id", "")
        parts = trace_id.split(":")
        if len(parts) >= 3 and parts[-2].isdigit():
            return dt.datetime.fromtimestamp(int(parts[-2]) / 1000.0, TZ)
    except (ValueError, OverflowError, OSError):
        return None
    return None


def match_db_row(candidate: Alert, all_alerts: List[Alert], used_ids: set) -> Optional[Alert]:
    # trace 证据时间通常比 DB timestamp 早/晚几秒；按币种、信号类别和最近时间精确关联。
    candidates = []
    for row in all_alerts:
        if row.row_id in used_ids:
            continue
        if row.symbol != candidate.symbol or row.signal_type != candidate.signal_type:
            continue
        delta = abs((row.timestamp - candidate.timestamp).total_seconds())
        if delta <= 30:
            price_delta = abs(row.price - candidate.price) / candidate.price if candidate.price else 1.0
            candidates.append((delta, price_delta, row))
    if not candidates:
        return None
    candidates.sort(key=lambda x: (x[0], x[1]))
    matched = candidates[0][2]
    if matched.row_id is not None:
        used_ids.add(matched.row_id)
    return matched


def load_trace_entries(
    trace_path: str,
    since: dt.datetime,
    until: dt.datetime,
    all_alerts: List[Alert],
) -> Tuple[List[Alert], Dict[str, int]]:
    if not os.path.isfile(trace_path):
        raise FileNotFoundError(f"找不到 trace 日志：{trace_path}")
    parsed_lines = 0
    allow_price_lines = 0
    event_candidates: Dict[str, Alert] = {}
    used_db_ids = set()

    with open(trace_path, "r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            if "[SIGNAL_TRACE]" not in line:
                continue
            kv = trace_key_values(line)
            if not kv:
                continue
            parsed_lines += 1
            kind = kv.get("kind", "")
            if kind not in ("initial_1m", "initial_3m"):
                continue
            if kv.get("decision") != "ALLOW" or kv.get("reason") != kind:
                # 排除 SUPPRESS，也排除虽允许发送但只是 OI + 3m 联合确认的后续消息。
                continue
            timestamp = parse_trace_timestamp(kv)
            event_id = kv.get("event_id", "")
            symbol = kv.get("symbol", "").upper()
            try:
                direction = 1 if float(kv.get("direction", "0")) > 0 else -1
                price = float(kv.get("price", "0"))
            except ValueError:
                continue
            if timestamp is None or not event_id or event_id == "-" or not symbol or price <= 0:
                continue
            if timestamp < since or timestamp > until:
                continue
            signal_type = (
                ("🚀暴涨" if direction > 0 else "🩸暴跌")
                if kind == "initial_1m"
                else ("🚀暴涨3m" if direction > 0 else "🩸暴跌3m")
            )
            candidate = Alert(
                row_id=None,
                timestamp=timestamp,
                symbol=symbol,
                signal_type=signal_type,
                price=price,
                direction=direction,
                kind=kind,
                event_id=event_id,
                source="trace_allow_event_id",
                trace_id=kv.get("trace_id", ""),
            )
            # 每个 event_id 只进入一次；若日志重复导出，保留最早一条。
            if event_id not in event_candidates or candidate.timestamp < event_candidates[event_id].timestamp:
                event_candidates[event_id] = candidate

    output = sorted(event_candidates.values(), key=lambda x: (x.timestamp, x.symbol))
    for candidate in output:
        matched = match_db_row(candidate, all_alerts, used_db_ids)
        if matched:
            candidate.row_id = matched.row_id
            # trace 价格是仲裁时证据价格，保留它作为信号价；同时只使用 DB ID 做可追溯关联。
    allow_price_lines = len(output)
    return output, {
        "trace_lines_parsed": parsed_lines,
        "trace_first_price_events": allow_price_lines,
        "trace_db_rows_matched": sum(1 for item in output if item.row_id is not None),
        "trace_db_rows_unmatched": sum(1 for item in output if item.row_id is None),
    }


def apply_delivery_log(entries: List[Alert], delivery_path: str) -> Dict[str, int]:
    """把 trace_id 对应到 TG_QUEUE/TG_DELIVERY；可据此只回测成功实际送达的信号。"""
    if not os.path.isfile(delivery_path):
        raise FileNotFoundError(f"找不到 Telegram 投递日志：{delivery_path}")

    status_by_trace: Dict[str, str] = {}
    message_by_trace: Dict[str, str] = {}
    queue_seen = set()
    parsed_delivery_lines = 0
    with open(delivery_path, "r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            if "[TG_QUEUE]" in line:
                kv = trace_key_values(line.replace("[TG_QUEUE]", "[SIGNAL_TRACE]", 1))
                trace_id = kv.get("trace_id", "")
                status = kv.get("status", "")
                if trace_id:
                    queue_seen.add(trace_id)
                    if status == "dropped":
                        status_by_trace[trace_id] = "queue_dropped"
                    elif status == "accepted" and trace_id not in status_by_trace:
                        status_by_trace[trace_id] = "queue_accepted"
            if "[TG_DELIVERY]" not in line:
                continue
            parsed_delivery_lines += 1
            kv = trace_key_values(line.replace("[TG_DELIVERY]", "[SIGNAL_TRACE]", 1))
            trace_ids = [value for value in kv.get("trace_ids", "").split(",") if value and value != "-"]
            delivery_status = kv.get("status", "unknown")
            telegram_message_id = kv.get("telegram_message_id", "")
            if telegram_message_id == "-":
                telegram_message_id = ""
            for trace_id in trace_ids:
                status_by_trace[trace_id] = delivery_status
                if telegram_message_id:
                    message_by_trace[trace_id] = telegram_message_id

    for entry in entries:
        if not entry.trace_id:
            entry.delivery_status = "unknown_no_trace_id"
            continue
        entry.delivery_status = status_by_trace.get(entry.trace_id, "not_found_in_delivery_log")
        entry.telegram_message_id = message_by_trace.get(entry.trace_id, "")

    status_counts = defaultdict(int)
    for entry in entries:
        status_counts[entry.delivery_status] += 1
    return {
        "delivery_log_lines": parsed_delivery_lines,
        "queue_trace_ids_seen": len(queue_seen),
        "entries_delivery_success": status_counts["success"],
        "entries_delivery_failed": sum(
            count for status, count in status_counts.items()
            if status in ("failed", "http_error", "network_error", "queue_dropped", "session_closed")
        ),
        "entries_delivery_unmatched": sum(
            count for status, count in status_counts.items()
            if status.startswith("not_found") or status.startswith("unknown")
        ),
    }


def reconstruct_entries_from_db(
    alerts: List[Alert], since: dt.datetime, until: dt.datetime,
    event_idle_close: int = 300, event_link_window: int = 180,
    material_move_pct: float = 0.01,
) -> Tuple[List[Alert], Dict[str, int]]:
    """仅凭 alerts 表近似重放状态机；并非替代真实 trace/event_id。"""
    states: Dict[str, EventState] = {}
    entries: List[Alert] = []
    counts = defaultdict(int)

    for row in alerts:
        now = row.epoch
        state = states.get(row.symbol)
        if state and now - state.last_activity >= event_idle_close:
            state = None
            states.pop(row.symbol, None)

        if row.signal_type in PRICE_TYPES:
            direction, kind = PRICE_TYPES[row.signal_type]
            if state is None or state.direction != direction:
                row.direction = direction
                row.kind = kind
                row.event_id = f"db-reconstructed:{row.symbol}:{row.row_id}"
                row.source = "db_reconstructed_approx"
                states[row.symbol] = EventState(
                    direction=direction,
                    last_activity=now,
                    last_material_price=row.price,
                    last_price_signal=now,
                )
                if since <= row.timestamp <= until:
                    entries.append(row)
                    counts["first_price_events"] += 1
            else:
                # 分支中的实现：同方向信号本身刷新事件活跃时间；价格达到 1% 另记实质推进。
                base = state.last_material_price
                move = (row.price - base) / base if base > 0 else 0.0
                if (direction > 0 and move >= material_move_pct) or (
                    direction < 0 and move <= -material_move_pct
                ):
                    state.last_material_price = row.price
                state.last_activity = now
                state.last_price_signal = now
                counts["same_direction_suppressed"] += 1
            continue

        if row.signal_type in TREND_TYPES:
            trend_dir = TREND_TYPES[row.signal_type]
            if state and state.direction == trend_dir:
                state.last_activity = now
                counts["trend_touches_event"] += 1
            continue

        if row.signal_type == "🚨强反向冲击":
            if state is not None:
                state.last_activity = now
            continue

        if row.signal_type == "📊OI异动":
            if state and 0 <= now - state.last_price_signal <= event_link_window:
                state.last_activity = now
            continue

    entries.sort(key=lambda x: (x.timestamp, x.symbol))
    return entries, {
        "first_price_events": counts["first_price_events"],
        "same_direction_suppressed_approx": counts["same_direction_suppressed"],
        "trend_activity_touches": counts["trend_touches_event"],
        "trace_lines_parsed": 0,
        "trace_first_price_events": 0,
        "trace_db_rows_matched": 0,
        "trace_db_rows_unmatched": 0,
    }


def request_json(url: str, timeout: int = 20, retries: int = 4):
    last_error = None
    for attempt in range(retries):
        request = urllib.request.Request(
            url,
            headers={"User-Agent": "CryptoMonitor-backtest/1.0", "Accept": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                body = response.read()
            return json.loads(body.decode("utf-8"))
        except urllib.error.HTTPError as exc:
            try:
                detail = exc.read().decode("utf-8", errors="replace")[:500]
            except Exception:
                detail = str(exc)
            last_error = RuntimeError(f"HTTP {exc.code}: {detail}")
            # 4xx 通常为下架合约/非法交易对等问题；429、418 与 5xx 可以重试。
            if exc.code not in (418, 429, 500, 502, 503, 504):
                raise last_error
            wait_seconds = min(2 ** attempt, 16)
            print(f"  [限流/服务错误] HTTP {exc.code}，{wait_seconds}s 后重试", flush=True)
            time.sleep(wait_seconds)
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_error = exc
            wait_seconds = min(2 ** attempt, 16)
            print(f"  [网络错误] {exc}，{wait_seconds}s 后重试", flush=True)
            time.sleep(wait_seconds)
    raise RuntimeError(f"请求失败，已重试 {retries} 次：{last_error}")


def init_cache(cache_path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(os.path.abspath(cache_path)), exist_ok=True)
    conn = sqlite3.connect(cache_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(
        """CREATE TABLE IF NOT EXISTS klines_1m (
            symbol TEXT NOT NULL,
            open_time_ms INTEGER NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            PRIMARY KEY (symbol, open_time_ms)
        )"""
    )
    conn.execute(
        """CREATE TABLE IF NOT EXISTS coverage (
            symbol TEXT NOT NULL,
            start_ms INTEGER NOT NULL,
            end_ms INTEGER NOT NULL,
            fetched_at TEXT NOT NULL,
            PRIMARY KEY (symbol, start_ms, end_ms)
        )"""
    )
    conn.commit()
    return conn


def missing_ranges(conn: sqlite3.Connection, symbol: str, start_ms: int, end_ms: int) -> List[Tuple[int, int]]:
    rows = conn.execute(
        "SELECT start_ms, end_ms FROM coverage WHERE symbol=? AND end_ms>=? AND start_ms<=? ORDER BY start_ms",
        (symbol, start_ms, end_ms),
    ).fetchall()
    ranges = []
    cursor = start_ms
    for covered_start, covered_end in rows:
        covered_start = max(int(covered_start), start_ms)
        covered_end = min(int(covered_end), end_ms)
        if covered_end < cursor:
            continue
        if covered_start > cursor:
            ranges.append((cursor, covered_start - ONE_MINUTE_MS))
        cursor = max(cursor, covered_end + ONE_MINUTE_MS)
        if cursor > end_ms:
            break
    if cursor <= end_ms:
        ranges.append((cursor, end_ms))
    return [(start, end) for start, end in ranges if start <= end]


def fetch_kline_range(symbol: str, start_ms: int, end_ms: int, delay: float) -> List[Tuple[int, float, float, float, float]]:
    url = f"{BINANCE_BASE}/fapi/v1/klines"
    cursor = start_ms
    result = []
    while cursor <= end_ms:
        params = urllib.parse.urlencode({
            "symbol": symbol,
            "interval": "1m",
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
                    result.append((open_ms, float(item[1]), float(item[2]), float(item[3]), float(item[4])))
            except (IndexError, TypeError, ValueError):
                continue
        newest = int(data[-1][0])
        if newest < cursor or newest + ONE_MINUTE_MS <= cursor:
            raise RuntimeError(f"K 线分页游标未推进，停止以避免死循环：{symbol}, cursor={cursor}, newest={newest}")
        if len(data) < KLINE_LIMIT or newest >= end_ms:
            break
        cursor = newest + ONE_MINUTE_MS
        if delay > 0:
            time.sleep(delay)
    return result


def get_klines_for_symbol(
    symbol: str, start_ms: int, end_ms: int, cache: sqlite3.Connection, delay: float
) -> Dict[int, dict]:
    if start_ms > end_ms:
        return {}
    todo = missing_ranges(cache, symbol, start_ms, end_ms)
    for missing_start, missing_end in todo:
        fresh = fetch_kline_range(symbol, missing_start, missing_end, delay)
        with cache:
            cache.executemany(
                "INSERT OR REPLACE INTO klines_1m(symbol, open_time_ms, open, high, low, close) VALUES(?,?,?,?,?,?)",
                [(symbol, *item) for item in fresh],
            )
            cache.execute(
                "INSERT OR REPLACE INTO coverage(symbol,start_ms,end_ms,fetched_at) VALUES(?,?,?,?)",
                (symbol, missing_start, missing_end, dt.datetime.now(TZ).isoformat(timespec="seconds")),
            )
        if delay > 0:
            time.sleep(delay)
    rows = cache.execute(
        "SELECT open_time_ms, open, high, low, close FROM klines_1m WHERE symbol=? AND open_time_ms>=? AND open_time_ms<=? ORDER BY open_time_ms",
        (symbol, start_ms, end_ms),
    ).fetchall()
    return {
        int(row[0]): {"open": float(row[1]), "high": float(row[2]), "low": float(row[3]), "close": float(row[4])}
        for row in rows
    }


def trade_return_pct(direction: int, entry: float, exit_price: float) -> float:
    """USDT-M 线性合约方向收益，按名义本金计算，未扣手续费/滑点。"""
    if entry <= 0:
        return float("nan")
    return direction * (exit_price / entry - 1.0) * 100.0


def net_return_pct(gross_pct: float, fee_rate: float, slippage_rate: float) -> float:
    # 近似按入场、出场两侧名义本金扣成本，不模拟资金费率、强平与滑点随波动放大。
    return gross_pct - 2.0 * (fee_rate + slippage_rate) * 100.0


def trade_excursions(direction: int, entry: float, bars: List[dict]) -> Tuple[float, float, float, float, float]:
    max_high = max(bar["high"] for bar in bars)
    min_low = min(bar["low"] for bar in bars)
    final_close = bars[-1]["close"]
    # 所有指标统一为“做该方向交易”的收益百分比；MAE 通常为负数。
    mfe = (max_high / entry - 1.0) * direction * 100.0 if direction > 0 else (1.0 - min_low / entry) * 100.0
    mae = (min_low / entry - 1.0) * 100.0 if direction > 0 else (1.0 - max_high / entry) * 100.0
    close_ret = trade_return_pct(direction, entry, final_close)
    return mfe, mae, close_ret, max_high, min_low


def tp_sl_result(
    direction: int, entry: float, bars: List[dict], full_window: bool,
    tp_rate: float, sl_rate: float, fee_rate: float, slippage_rate: float,
) -> Tuple[str, Optional[float], Optional[float], Optional[int]]:
    if entry <= 0 or not bars:
        return "no_data", None, None, None
    tp_price = entry * (1.0 + direction * tp_rate)
    sl_price = entry * (1.0 - direction * sl_rate)

    for idx, bar in enumerate(bars):
        if direction > 0:
            tp_hit = bar["high"] >= tp_price
            sl_hit = bar["low"] <= sl_price
            # 发生跳空越过止损时按开盘价成交，避免虚高估计。
            sl_fill = min(bar["open"], sl_price) if sl_hit else None
            tp_fill = max(bar["open"], tp_price) if tp_hit else None
        else:
            tp_hit = bar["low"] <= tp_price
            sl_hit = bar["high"] >= sl_price
            sl_fill = max(bar["open"], sl_price) if sl_hit else None
            tp_fill = min(bar["open"], tp_price) if tp_hit else None

        if tp_hit and sl_hit:
            # 1m OHLC 不知道同一根 K 线里哪个先发生，保守按止损先触发。
            exit_price = float(sl_fill)
            gross = trade_return_pct(direction, entry, exit_price)
            return "both_same_bar_assume_sl", exit_price, net_return_pct(gross, fee_rate, slippage_rate), idx
        if sl_hit:
            exit_price = float(sl_fill)
            gross = trade_return_pct(direction, entry, exit_price)
            return "sl", exit_price, net_return_pct(gross, fee_rate, slippage_rate), idx
        if tp_hit:
            exit_price = float(tp_fill)
            gross = trade_return_pct(direction, entry, exit_price)
            return "tp", exit_price, net_return_pct(gross, fee_rate, slippage_rate), idx

    if not full_window:
        return "incomplete", None, None, None
    exit_price = bars[-1]["close"]
    gross = trade_return_pct(direction, entry, exit_price)
    return "timeout", exit_price, net_return_pct(gross, fee_rate, slippage_rate), len(bars) - 1


def write_csv(path: str, rows: List[dict]) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    if not rows:
        with open(path, "w", newline="", encoding="utf-8-sig") as handle:
            handle.write("\n")
        return
    fieldnames = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def percentile(values: List[float], q: float) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(values)
    pos = (len(ordered) - 1) * q
    lower = int(math.floor(pos))
    upper = int(math.ceil(pos))
    if lower == upper:
        return ordered[lower]
    return ordered[lower] * (upper - pos) + ordered[upper] * (pos - lower)


def summarize_results(result_rows: List[dict]) -> List[dict]:
    groups = defaultdict(list)
    for row in result_rows:
        if str(row.get("window_complete")) not in ("True", "1", "true"):
            continue
        if row.get("net_close_return_pct") in (None, ""):
            continue
        key = (row["signal_type"], row["direction_text"], int(row["horizon_minutes"]), row["entry_model"])
        groups[key].append(row)

    summaries = []
    for (signal_type, direction_text, horizon, entry_model), rows in sorted(groups.items()):
        close_returns = [float(r["net_close_return_pct"]) for r in rows]
        mfes = [float(r["mfe_pct"]) for r in rows if r.get("mfe_pct") not in (None, "")]
        maes = [float(r["mae_pct"]) for r in rows if r.get("mae_pct") not in (None, "")]
        wins = [x for x in close_returns if x > 0]
        losses = [x for x in close_returns if x < 0]
        pf = sum(wins) / abs(sum(losses)) if losses and sum(losses) != 0 else (None if not wins else "inf")
        tp_count = sum(1 for r in rows if str(r.get("tp_sl_outcome", "")).startswith("tp"))
        sl_count = sum(1 for r in rows if r.get("tp_sl_outcome") in ("sl", "both_same_bar_assume_sl"))
        timeout_count = sum(1 for r in rows if r.get("tp_sl_outcome") == "timeout")
        summaries.append({
            "signal_type": signal_type,
            "direction": direction_text,
            "horizon_minutes": horizon,
            "entry_model": entry_model,
            "completed_samples": len(rows),
            "win_rate_close_net_pct": round(len(wins) / len(rows) * 100, 3) if rows else None,
            "mean_close_net_return_pct": round(statistics.mean(close_returns), 4) if rows else None,
            "median_close_net_return_pct": round(statistics.median(close_returns), 4) if rows else None,
            "profit_factor_close_net": round(pf, 4) if isinstance(pf, (int, float)) else pf,
            "mean_mfe_pct": round(statistics.mean(mfes), 4) if mfes else None,
            "median_mfe_pct": round(statistics.median(mfes), 4) if mfes else None,
            "p75_mfe_pct": round(percentile(mfes, 0.75), 4) if mfes else None,
            "mean_mae_pct": round(statistics.mean(maes), 4) if maes else None,
            "median_mae_pct": round(statistics.median(maes), 4) if maes else None,
            "tp_rate_pct_of_completed": round(tp_count / len(rows) * 100, 3) if rows else None,
            "sl_rate_pct_of_completed": round(sl_count / len(rows) * 100, 3) if rows else None,
            "timeout_rate_pct_of_completed": round(timeout_count / len(rows) * 100, 3) if rows else None,
        })
    return summaries



def merge_intervals(intervals: List[Tuple[int, int]]) -> List[Tuple[int, int]]:
    """合并重叠或相邻的分钟 K 线请求区间；不跨越长时间空档下载无用数据。"""
    if not intervals:
        return []
    ordered = sorted((int(start), int(end)) for start, end in intervals if start <= end)
    if not ordered:
        return []
    merged = [list(ordered[0])]
    for start, end in ordered[1:]:
        # 相邻一分钟也可以合并，方便连续分页。
        if start <= merged[-1][1] + ONE_MINUTE_MS:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])
    return [(start, end) for start, end in merged]


def run_backtest(args) -> int:
    now = dt.datetime.now(TZ)
    until = parse_datetime(args.until, "--until") if args.until else now
    try:
        db_earliest = get_db_bounds(args.db, until)
    except Exception as exc:
        print(f"读取数据库时间范围失败：{exc}", file=sys.stderr)
        return 2
    if db_earliest is None:
        print("alerts.db 中没有截至结束时间的有效 urgent 价格记录。", file=sys.stderr)
        return 1

    if args.since:
        since = parse_datetime(args.since, "--since")
    elif args.days is not None:
        since = until - dt.timedelta(days=args.days)
    else:
        # 默认全历史：以数据库最早的有效价格记录为起点，而不是默认最近 7 天。
        since = db_earliest
    if since >= until:
        print("错误：--since 必须早于 --until", file=sys.stderr)
        return 2

    max_horizon = max(args.horizons)
    # 为完整重放事件状态机，默认读取从数据库最早记录开始的所有信号；
    # 输出入场事件时再按 since/until 过滤，避免区间开始处把已有事件误判为新事件。
    warmup_start = db_earliest - dt.timedelta(seconds=args.event_idle_close)
    print("=" * 76)
    print("CryptoMonitor 首次信号回测")
    print(f"DB             : {os.path.abspath(args.db)}")
    print(f"信号区间       : {db_time_string(since)} -> {db_time_string(until)} (UTC+8)")
    print(f"数据库最早记录 : {db_time_string(db_earliest)} (全历史回测默认起点)")
    print(f"重放起点       : {db_time_string(warmup_start)}")
    if not args.since and args.days is None:
        print("模式           : 全历史（alerts.db 最早有效 urgent 价格记录 -> 结束时间）")
    print(f"未来观察窗口   : {', '.join(str(x) + 'm' for x in args.horizons)}")
    print(f"止盈/止损      : +{args.tp*100:.2f}% / -{args.sl*100:.2f}%")
    print(f"单边费率/滑点  : {args.fee_rate*100:.3f}% / {args.slippage_rate*100:.3f}%")
    print("=" * 76)

    try:
        all_alerts = load_alerts(args.db, warmup_start, until)
    except Exception as exc:
        print(f"读取数据库失败：{exc}", file=sys.stderr)
        return 2
    print(f"数据库查询到 urgent 且价格有效记录：{len(all_alerts)} 条", flush=True)

    trace_stats = {}
    if args.trace_log:
        try:
            entries, trace_stats = load_trace_entries(args.trace_log, since, until, all_alerts)
        except Exception as exc:
            print(f"读取 trace 日志失败：{exc}", file=sys.stderr)
            return 2
        source_mode = "trace_exact_allow"
        print(f"读取 SIGNAL_TRACE：{trace_stats.get('trace_lines_parsed', 0)} 行可解析决策；"
              f"筛选出 {len(entries)} 个首次价格事件；"
              f"匹配 DB ID {trace_stats.get('trace_db_rows_matched', 0)} 条；"
              f"未匹配 {trace_stats.get('trace_db_rows_unmatched', 0)} 条。", flush=True)
    else:
        entries, trace_stats = reconstruct_entries_from_db(
            all_alerts, since, until,
            event_idle_close=args.event_idle_close,
            event_link_window=args.event_link_window,
            material_move_pct=args.material_move_pct,
        )
        source_mode = "db_reconstructed_approx"
        print("未提供 --trace-log：使用 alerts.db 时间序列近似重放事件。", flush=True)
        print("注意：DB timestamp 只有秒级、未保存 event_id，且未保存每次 observe_price；结果是近似值。", flush=True)
        print(f"近似重建首次价格事件：{len(entries)}；同方向抑制估计：{trace_stats.get('same_direction_suppressed_approx', 0)}。", flush=True)

    delivery_stats = {}
    if args.delivery_log:
        try:
            delivery_stats = apply_delivery_log(entries, args.delivery_log)
        except Exception as exc:
            print(f"解析 Telegram 投递日志失败：{exc}", file=sys.stderr)
            return 2
        print(
            f"TG 投递日志：成功 {delivery_stats.get('entries_delivery_success', 0)} 个候选事件；"
            f"未匹配投递记录 {delivery_stats.get('entries_delivery_unmatched', 0)} 个。",
            flush=True,
        )
        if args.only_delivered:
            before_count = len(entries)
            entries = [entry for entry in entries if entry.delivery_status == "success"]
            print(f"--only-delivered 已启用：{before_count} -> {len(entries)} 个真正有成功回执的候选事件。", flush=True)
    elif args.only_delivered:
        print("错误：--only-delivered 必须同时提供 --delivery-log。", file=sys.stderr)
        return 2

    if not entries:
        print("这个时间范围没有可回测的首次 1m/3m 价格事件。请检查时间范围、trace 文件或投递日志。", file=sys.stderr)
        return 1

    # 对候选入场去重同一 event_id，trace 精确模式下 event_id 来自实际运行决策。
    seen = set()
    unique_entries = []
    for entry in sorted(entries, key=lambda x: (x.timestamp, x.symbol)):
        dedupe_key = entry.event_id or f"{entry.symbol}:{entry.epoch_ms}:{entry.kind}"
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        unique_entries.append(entry)
    entries = unique_entries

    # 只查询完整未来分钟线；避免拿未收盘的当前 K 线回测。
    now_ms = int(time.time() * 1000)
    latest_closed_open_ms = (now_ms // ONE_MINUTE_MS) * ONE_MINUTE_MS - ONE_MINUTE_MS
    per_symbol = defaultdict(list)
    for entry in entries:
        next_bar_ms = (entry.epoch_ms // ONE_MINUTE_MS + 1) * ONE_MINUTE_MS
        entry.next_bar_ms = next_bar_ms
        per_symbol[entry.symbol].append(entry)

    cache_path = args.cache or os.path.join(os.path.dirname(os.path.abspath(args.db)), "backtest_klines_cache.sqlite3")
    cache = init_cache(cache_path)
    candles: Dict[str, Dict[int, dict]] = {}
    fetch_errors = []
    try:
        for index, (symbol, symbol_entries) in enumerate(sorted(per_symbol.items()), start=1):
            raw_ranges = []
            for entry in symbol_entries:
                start_ms = entry.next_bar_ms
                end_ms = min(
                    start_ms + max_horizon * ONE_MINUTE_MS - ONE_MINUTE_MS,
                    latest_closed_open_ms,
                )
                if start_ms <= end_ms:
                    raw_ranges.append((start_ms, end_ms))
            ranges = merge_intervals(raw_ranges)
            print(
                f"[{index}/{len(per_symbol)}] 获取 {symbol} 1m K线："
                f"{len(symbol_entries)} 个事件，合并为 {len(ranges)} 个所需窗口",
                flush=True,
            )
            symbol_candles: Dict[int, dict] = {}
            symbol_errors = []
            for range_index, (start_ms, end_ms) in enumerate(ranges, start=1):
                try:
                    fresh = get_klines_for_symbol(
                        symbol, start_ms, end_ms, cache, args.request_delay
                    )
                    symbol_candles.update(fresh)
                except Exception as exc:
                    symbol_errors.append(
                        f"区间 {dt.datetime.fromtimestamp(start_ms/1000,TZ):%Y-%m-%d %H:%M}"
                        f"~{dt.datetime.fromtimestamp(end_ms/1000,TZ):%Y-%m-%d %H:%M}: {exc}"
                    )
                    print(f"    [区间失败 {range_index}/{len(ranges)}] {exc}", flush=True)
            candles[symbol] = symbol_candles
            if symbol_errors:
                fetch_errors.append({"symbol": symbol, "error": " | ".join(symbol_errors)})
            print(f"    已有/下载 K线：{len(symbol_candles)} 根", flush=True)
    finally:
        cache.close()

    entry_rows = []
    result_rows = []
    no_data_events = 0
    for event_index, entry in enumerate(entries, start=1):
        bars_map = candles.get(entry.symbol, {})
        next_bar_ms = entry.next_bar_ms
        first_future_bar = bars_map.get(next_bar_ms)
        next_open = first_future_bar["open"] if first_future_bar else None
        if next_open is None:
            no_data_events += 1

        entry_rows.append({
            "source_mode": source_mode,
            "event_id": entry.event_id,
            "db_alert_id": entry.row_id if entry.row_id is not None else "",
            "signal_time_utc8": entry.timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            "symbol": entry.symbol,
            "signal_type": entry.signal_type,
            "kind": entry.kind,
            "direction": entry.direction,
            "direction_text": "LONG" if entry.direction > 0 else "SHORT",
            "signal_price": round(entry.price, 12),
            "next_1m_open_time_utc8": dt.datetime.fromtimestamp(next_bar_ms/1000, TZ).strftime("%Y-%m-%d %H:%M:%S"),
            "next_1m_open_price": round(next_open, 12) if next_open else "",
            "bars_available_from_next_open": sum(1 for t in bars_map if t >= next_bar_ms),
            "data_source": entry.source,
            "trace_id": entry.trace_id,
            "delivery_status": entry.delivery_status,
            "telegram_message_id": entry.telegram_message_id,
        })

        for model, entry_price in (("signal_price", entry.price), ("next_1m_open", next_open)):
            if entry_price is None or entry_price <= 0:
                continue
            for horizon in args.horizons:
                window_bars = []
                for offset in range(horizon):
                    bar = bars_map.get(next_bar_ms + offset * ONE_MINUTE_MS)
                    if bar is None:
                        break
                    window_bars.append(bar)
                observed = len(window_bars)
                full_window = observed == horizon
                if observed:
                    mfe, mae, close_gross, max_high, min_low = trade_excursions(entry.direction, entry_price, window_bars)
                    close_net = net_return_pct(close_gross, args.fee_rate, args.slippage_rate)
                    final_close = window_bars[-1]["close"]
                else:
                    mfe = mae = close_gross = close_net = max_high = min_low = final_close = None

                outcome, tp_sl_exit_price, tp_sl_net, exit_offset = tp_sl_result(
                    entry.direction, entry_price, window_bars, full_window,
                    args.tp, args.sl, args.fee_rate, args.slippage_rate,
                )
                tp_sl_exit_time = ""
                if exit_offset is not None:
                    tp_sl_exit_time = dt.datetime.fromtimestamp(
                        (next_bar_ms + exit_offset * ONE_MINUTE_MS) / 1000, TZ
                    ).strftime("%Y-%m-%d %H:%M:%S")
                result_rows.append({
                    "source_mode": source_mode,
                    "event_id": entry.event_id,
                    "trace_id": entry.trace_id,
                    "db_alert_id": entry.row_id if entry.row_id is not None else "",
                    "delivery_status": entry.delivery_status,
                    "telegram_message_id": entry.telegram_message_id,
                    "signal_time_utc8": entry.timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                    "symbol": entry.symbol,
                    "signal_type": entry.signal_type,
                    "direction_text": "LONG" if entry.direction > 0 else "SHORT",
                    "signal_price": round(entry.price, 12),
                    "entry_model": model,
                    "entry_price": round(entry_price, 12),
                    "window_start_utc8": dt.datetime.fromtimestamp(next_bar_ms/1000, TZ).strftime("%Y-%m-%d %H:%M:%S"),
                    "horizon_minutes": horizon,
                    "bars_observed": observed,
                    "window_complete": full_window,
                    "max_high": round(max_high, 12) if max_high is not None else "",
                    "min_low": round(min_low, 12) if min_low is not None else "",
                    "final_close": round(final_close, 12) if final_close is not None else "",
                    "mfe_pct": round(mfe, 5) if mfe is not None else "",
                    "mae_pct": round(mae, 5) if mae is not None else "",
                    "gross_close_return_pct": round(close_gross, 5) if close_gross is not None else "",
                    "net_close_return_pct": round(close_net, 5) if close_net is not None else "",
                    "tp_pct": args.tp * 100,
                    "sl_pct": args.sl * 100,
                    "tp_sl_outcome": outcome,
                    "tp_sl_exit_time_utc8": tp_sl_exit_time,
                    "tp_sl_exit_price": round(tp_sl_exit_price, 12) if tp_sl_exit_price is not None else "",
                    "tp_sl_net_return_pct": round(tp_sl_net, 5) if tp_sl_net is not None else "",
                })
        if event_index % 50 == 0:
            print(f"已计算 {event_index}/{len(entries)} 个事件", flush=True)

    os.makedirs(args.out_dir, exist_ok=True)
    entries_path = os.path.join(args.out_dir, "backtest_entries.csv")
    results_path = os.path.join(args.out_dir, "backtest_results.csv")
    summary_path = os.path.join(args.out_dir, "backtest_summary.csv")
    errors_path = os.path.join(args.out_dir, "backtest_fetch_errors.csv")
    write_csv(entries_path, entry_rows)
    write_csv(results_path, result_rows)
    write_csv(summary_path, summarize_results(result_rows))
    write_csv(errors_path, fetch_errors)

    report_path = os.path.join(args.out_dir, "README_回测说明.txt")
    with open(report_path, "w", encoding="utf-8") as report:
        report.write("CryptoMonitor 首次信号回测结果说明\n")
        report.write("=" * 50 + "\n")
        report.write(f"DB: {os.path.abspath(args.db)}\n")
        report.write(f"信号区间 UTC+8: {db_time_string(since)} -> {db_time_string(until)}\n")
        report.write(f"事件来源: {source_mode}\n")
        report.write(f"首个价格事件数: {len(entries)}\n")
        report.write(f"成功取得 K 线的事件数: {len(entries) - no_data_events}\n")
        report.write(f"K 线请求失败币种数: {len(fetch_errors)}\n")
        if delivery_stats:
            report.write(f"投递日志: {os.path.abspath(args.delivery_log)}\n")
            report.write(f"候选事件投递成功数: {delivery_stats.get('entries_delivery_success', 0)}\n")
            report.write(f"候选事件投递未匹配数: {delivery_stats.get('entries_delivery_unmatched', 0)}\n")
            report.write(f"only_delivered: {args.only_delivered}\n")
        report.write(f"止盈/止损: {args.tp*100:.2f}% / {args.sl*100:.2f}%\n")
        report.write(f"单边费率: {args.fee_rate*100:.3f}%；单边滑点: {args.slippage_rate*100:.3f}%\n\n")
        report.write("文件说明：\n")
        report.write("1. backtest_entries.csv：每个候选首次事件一行，含信号价与下一根完整 1m K 线开盘价。\n")
        report.write("2. backtest_results.csv：每个事件 x 时间窗口 x 入场价格模型一行；包含 MFE/MAE、窗口收盘收益、TP/SL 模拟。\n")
        report.write("3. backtest_summary.csv：只统计 future bars 完整的样本，按信号类型/方向/窗口/入场模型聚合。\n")
        report.write("4. backtest_fetch_errors.csv：历史 K 线无法获取的交易对及错误。\n\n")
        report.write("指标定义：\n")
        report.write("- MFE：窗口内最高/最低价给出的最大有利价格收益，仅表示理论机会，不是可实现的策略净收益。\n")
        report.write("- MAE：窗口内最大不利收益，通常是负数；是对信号持有过程风险的估计。\n")
        report.write("- close return：窗口最后一根完整 1m K 线收盘退出；gross 为毛收益，net 扣除双边手续费与滑点近似值。\n")
        report.write("- TP/SL：每个 1m OHLC 依序检查止盈/止损；同一根 K 线都命中时，保守按止损先触发。\n")
        report.write("- next_1m_open：为了避免信号发生的那根 K 线存在前视偏差，使用下一个完整 1m K 线开盘价入场。\n")
        report.write("- USDT-M 线性收益按方向 * (exit / entry - 1) 计算；不模拟杠杆强平、资金费率及交易所实际盘口冲击。\n")
        report.write("- 默认全历史模式 source_mode=db_reconstructed_approx：事件从 alerts 表近似重放，因为表里未保存 event_id/每次实时价格更新；若指定 --trace-log，则只覆盖该 trace 文件实际包含的时间段。\n")

    print("\n回测完成：")
    print(f"  首次事件   : {len(entries)}")
    print(f"  无可用未来 K线的事件: {no_data_events}")
    print(f"  K线失败币种: {len(fetch_errors)}")
    print(f"  事件明细   : {entries_path}")
    print(f"  回测明细   : {results_path}")
    print(f"  分组摘要   : {summary_path}")
    print(f"  获取错误   : {errors_path}")
    print(f"  说明       : {report_path}")
    print(f"  K线缓存    : {cache_path}")
    print("\n重点先看 backtest_summary.csv：MFE 是理论最高空间，net_close_return_pct 才是固定时间退出后的净收益估计。")
    if source_mode == "db_reconstructed_approx":
        print("注意：全历史从 alerts.db 近似重建首次事件；数据库没有存 event_id，结果不等于实时 SIGNAL_TRACE 的精确事件分组。")
    return 0


def build_parser():
    parser = argparse.ArgumentParser(
        description="从 CryptoMonitor alerts.db 全历史重建首次有效 1m/3m 价格信号，计算未来 MFE/MAE 与策略 ROI。默认覆盖数据库全历史。",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--db", default="/root/CryptoMonitor/alerts.db", help="CryptoMonitor alerts.db 路径")
    parser.add_argument("--since", default=None, help="起始信号时间（UTC+8），例如 '2026-10-18 00:01:35'")
    parser.add_argument("--until", default=None, help="结束信号时间（UTC+8），例如 '2026-10-18 15:09:58'；默认当前时间")
    parser.add_argument("--days", type=float, default=None, help="可选：未指定 --since 时只回看 N 天；不指定则回测数据库全历史")
    parser.add_argument("--trace-log", default=None, help="可选：指定后仅回测该 SIGNAL_TRACE 文件覆盖的时间段，按真实 ALLOW/event_id 选事件；不要把数天轮转日志当作全历史")
    parser.add_argument("--delivery-log", default=None, help="包含 [TG_QUEUE]/[TG_DELIVERY] 的 cryptomonitor.log，用 trace_id 核对是否真的送达")
    parser.add_argument("--only-delivered", action="store_true", help="只回测有 Telegram status=success/message_id 的信号；必须提供 --trace-log 和 --delivery-log")
    parser.add_argument("--out-dir", default="backtest_roi_output", help="回测结果输出目录")
    parser.add_argument("--cache", default=None, help="1m K 线缓存 DB；默认放在 alerts.db 同目录，不修改 alerts.db")
    parser.add_argument("--horizons", default=",".join(map(str, DEFAULT_HORIZONS)), help="逗号分隔的未来观察分钟数，如 5,15,30,60,240,720")
    parser.add_argument("--tp", type=float, default=0.03, help="止盈比例，例如 0.03 表示 3%%")
    parser.add_argument("--sl", type=float, default=0.02, help="止损比例，例如 0.02 表示 2%%")
    parser.add_argument("--fee-rate", type=float, default=0.0005, help="单边手续费率，例如 0.0005 表示 0.05%%")
    parser.add_argument("--slippage-rate", type=float, default=0.0005, help="单边滑点假设，例如 0.0005 表示 0.05%%")
    parser.add_argument("--event-idle-close", type=int, default=300, help="DB 近似重建事件的空闲关闭秒数")
    parser.add_argument("--event-link-window", type=int, default=180, help="DB 近似重建 OI 与价格事件关联的秒数")
    parser.add_argument("--material-move-pct", type=float, default=0.01, help="DB 近似重建实质价格变化阈值，例如 0.01 表示 1%%")
    parser.add_argument("--request-delay", type=float, default=0.15, help="Binance 每页请求后的等待秒数")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        horizons = sorted({int(x.strip()) for x in args.horizons.split(",") if x.strip()})
    except ValueError:
        parser.error("--horizons 必须是用逗号分隔的正整数，例如 5,15,30,60,240,720")
    if not horizons or any(value <= 0 for value in horizons):
        parser.error("--horizons 至少包含一个正整数")
    args.horizons = horizons
    if args.days is not None and args.days <= 0:
        parser.error("--days 必须大于 0")
    if not (0 < args.tp < 1 and 0 < args.sl < 1):
        parser.error("--tp 与 --sl 必须在 0 到 1 之间，例如 0.03 和 0.02")
    if args.fee_rate < 0 or args.slippage_rate < 0 or args.request_delay < 0:
        parser.error("手续费、滑点和请求等待时间不能为负数")
    if args.event_idle_close <= 0 or args.event_link_window < 0 or args.material_move_pct < 0:
        parser.error("事件参数不合法")
    if args.only_delivered and (not args.trace_log or not args.delivery_log):
        parser.error("--only-delivered 必须同时提供 --trace-log 与 --delivery-log")
    if args.trace_log and args.since is None and args.days is None:
        parser.error(
            "全历史回测默认应从 alerts.db 重建事件，不要传 --trace-log；"
            "trace 日志通常只覆盖有限时间段。若只回测 trace 覆盖期，请显式指定 --since/--until。"
        )
    return run_backtest(args)


if __name__ == "__main__":
    sys.exit(main())
