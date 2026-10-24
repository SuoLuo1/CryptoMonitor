# monitors.py
import asyncio
import logging
import time
from collections import deque

from config import *
from utils import notifier
from signal_deduplicator import SignalDeduplicator

# 🐾 信号决策链路用 logger (主人偏好), 这样 journal 和 logs/cryptomonitor.log 都能查到
logger = logging.getLogger("Monitors")


class Monitors:
    # 🐾 10-06: 价格格式化辅助函数, 避免科学计数法 (主人反馈 SPELL/NEIRO 显示 '9.85e-05')
    @staticmethod
    def _fmt_price(p):
        """价格格式化: 不使用科学计数法, 保留合适精度"""
        if p == 0:
            return "0"
        if abs(p) >= 1:
            s = f"{p:.2f}"
            return s.rstrip('0').rstrip('.') if '.' in s else s
        return f"{p:.8f}"

    # --- 核心修复：添加一个内存字典，记录上一次报警时间 ---
    # 格式: { "BTCUSDT_WALL": 1700000000, "ETHUSDT_PUMP": 1700000000 }
    alert_history = {}
    # 1. 价格池 (由 WebSocket 毫秒级更新)
    price_map = {}
    # 2. 持仓池 (由 轮询 60秒级更新)
    oi_cache = {}
    kline_stats = {}
    valid_symbols = set()
    vip_symbols = set()
    wall_hits = {}
    depth_counter = {}
    agg_trade_window = {}
    # 滑动窗口缓存
    # 格式: { "BTCUSDT": deque([p1, p2, ..., p15], maxlen=15) }
    rolling_window = {}
    oi_window = {}
    # 3m K线成交量窗口 (存前5根已收盘K线的成交额, 用于放量确认)
    vol_window_3m = {}
    vol_window_1m = {}  # 前5根已收盘1m K线成交额，用于反向冲击确认
    # 设定窗口大小：60 分钟 (看过去 60根 1m K线 = 1小时趋势)
    WINDOW_SIZE = 60
    # 设定趋势阈值：1小时累计涨跌超过 5%
    TREND_THRESHOLD = 0.05
    @classmethod
    def update_valid_symbols(cls, all_symbols, vip_subset):
        cls.valid_symbols = set(s.upper() for s in all_symbols)
        cls.vip_symbols = set(s.upper() for s in vip_subset)
        logger.info(f"🛡️ 监控名单: {len(cls.valid_symbols)} 个 | 🧠 AI名单: {len(cls.vip_symbols)} 个")
    @classmethod
    def _check_cooldown(cls, key, cooldown_seconds=300):
        """
        检查是否在冷却时间内
        :param key: 唯一键值，例如 'PEPEUSDT_WALL_BUY'
        :param cooldown_seconds: 默认冷却 300秒 (5分钟)
        :return: True=可以发送, False=在冷却中
        """
        current_time = time.time()
        cls.alert_history = {
            k: v for k, v in cls.alert_history.items()
            if current_time - v < 3600
        }
        last_time = cls.alert_history.get(key, 0)

        if current_time - last_time > cooldown_seconds:
            cls.alert_history[key] = current_time  # 更新时间
            return True
        return False

    @staticmethod
    def _get_thresholds(symbol):
        # 1. 如果是 BTC / ETH
        if symbol in KINGS:
            return WALL_SIZE_KING, WHALE_MIN_KING

        # 2. 如果是 SOL / XRP / DOGE 等
        elif symbol in MAJORS:
            return WALL_SIZE_MAJOR, WHALE_MIN_MAJOR

        # 3. 其他所有山寨币
        else:
            return WALL_SIZE_ALT, WHALE_MIN_ALT

    @staticmethod
    def get_impact_pct(symbol):
        if symbol in KINGS:
            return 0.01  # 1.0%
        elif symbol in MAJORS:
            return 0.012  # 1.2%
        else:
            return 0.015  # 1.5%

    @staticmethod
    def _get_oi_threshold(symbol):
        if symbol in KINGS:
            return OI_THRESHOLD_KING
        elif symbol in MAJORS:
            return OI_THRESHOLD_MAJOR
        else:
            return OI_THRESHOLD_ALT

    @classmethod
    def _get_current_price(cls, symbol):
        return cls.price_map.get(symbol, 0)

    @classmethod
    def _get_last_oi(cls, symbol):
        # 返回 (数量, 更新时间)
        data = cls.oi_cache.get(symbol)
        if data:
            return data['amount'], data['time'], data['delta']
        return 0, 0, 0
    # ---------------------------------------------------------
    @classmethod
    async def _record_event_signal(
        cls, signal_kind, symbol, direction, evidence, msg, signal_type,
        price, *, chart=False, buffered=False
    ):
        """所有事件信号先入库，再独立裁决是否推送，确保被抑制信号仍进入历史统计。"""
        # 原始信号必须先入库供 Top 统计；同时在写入事务内判断它是否为 12h 新段首条。
        # 不能先插入再调用 is_symbol_first_notify，否则当前记录会把自己算成“历史信号”。
        db_result = await notifier.save_to_db_now(
            symbol, signal_type, msg, price, priority="urgent",
            check_first_window=True,
            startup_age_seconds=notifier.startup_age_seconds,
        )
        db_status = "ok" if db_result and db_result[0] else "failed"
        if db_status == "ok" and len(db_result) > 3:
            first_in_window = bool(db_result[3])
        else:
            # 数据库写入异常时的降级路径: 用本次记录主键精确排除"自己"。
            # 🐾 不用旧的 count-based 判定: 那套逻辑 count 恒 >= 1 → 永久静默。
            first_in_window = notifier.is_symbol_first_notify(
                symbol, exclude_id=db_result[4] if db_result and len(db_result) > 4 else None
            )

        allow, reason = SignalDeduplicator.arbitrate(
            signal_kind, symbol, direction, evidence
        )
        timestamp = float(evidence.get("timestamp", time.time()))
        trace_id = f"{symbol}:{int(timestamp * 1000)}:{signal_kind}"

        # 事件级裁决和 12h 分段首条是两个独立条件：两者都满足才发送。
        if allow and not first_in_window:
            allow = False
            startup_ignored = bool(db_result and len(db_result) > 5 and db_result[5])
            reason = (
                "stale_history_during_startup"
                if startup_ignored else "symbol_already_notified"
            )

        event = SignalDeduplicator.active_events.get(symbol)
        event_id = event.event_id if event is not None else "-"
        decision = "ALLOW" if allow else "SUPPRESS"
        logger.info(
            f"[SIGNAL_TRACE] ts={timestamp:.3f} trace_id={trace_id} "
            f"event_id={event_id} symbol={symbol} kind={signal_kind} "
            f"direction={direction:+d} decision={decision} reason={reason} "
            f"db={db_status} price={price}"
        )
        if not allow:
            logger.info(f"[DEDUP] {symbol} {signal_kind} 抑制推送：{reason}")
            return False, reason

        push_msg = msg
        if reason == "oi_3m_confirm":
            push_msg = "✅ <b>联合确认：价格异动 + OI + 3m放量</b>\n" + msg
        elif reason == "oi_strong":
            push_msg = "🚨 <b>OI 强升级</b>\n" + msg

        await notifier.send_urgent(
            push_msg,
            symbol=symbol,
            signal_type=signal_type,
            price=price,
            save_to_db=False,
            trace_id=trace_id,
        )
        # 图表只随首条价格信号发送，确认/升级不重复生成图表。
        if chart and reason in ("initial_1m", "initial_3m"):
            asyncio.create_task(
                notifier.send_chart(
                    symbol, timeframe="15m", caption=f"📊 {symbol} 15m K线形态"
                )
            )
        if buffered and symbol in cls.vip_symbols:
            await notifier.send_buffered(msg)
        return True, reason

    @classmethod
    async def _check_reverse_shock(
        cls, symbol, open_price, current_price, change_pct,
        current_quote_volume, previous_volume_mean
    ):
        """检查事件高/低点的强反向冲击；冲击提醒不在本阶段切换事件方向。"""
        event = SignalDeduplicator.get_event(symbol)
        if event is None or event.notif_reverse_shock:
            return False
        if previous_volume_mean <= 0:
            return False
        if current_quote_volume < previous_volume_mean * REVERSE_SHOCK_VOL_RATIO:
            return False

        reverse_pct = 0.0
        if event.direction > 0 and change_pct < 0 and event.event_high > 0:
            reverse_pct = (event.event_high - current_price) / event.event_high
            reverse_direction = -1
        elif event.direction < 0 and change_pct > 0 and event.event_low > 0:
            reverse_pct = (current_price - event.event_low) / event.event_low
            reverse_direction = 1
        else:
            return False

        if reverse_pct < REVERSE_SHOCK_PCT:
            return False

        evidence = {
            "timestamp": time.time(),
            "price": current_price,
            "change_pct": change_pct,
            "reverse_pct": reverse_pct,
            "vol_ratio": current_quote_volume / previous_volume_mean,
        }
        msg = (
            f"🚨 <b>强反向冲击: {symbol}</b>\n"
            f"原事件方向: {'上涨' if event.direction > 0 else '下跌'}\n"
            f"当前反向幅度: {reverse_pct * 100:.2f}%\n"
            f"1m 成交额量比: {evidence['vol_ratio']:.1f}x\n"
            f"现价: {cls._fmt_price(current_price)}\n"
            f"说明: 已触发强反向冲击，暂不自动判定趋势反转"
        )
        allow, reason = await cls._record_event_signal(
            "reverse_shock", symbol, reverse_direction, evidence, msg,
            "🚨强反向冲击", current_price,
        )
        return allow and reason == "reverse_shock"

    # 1. 深度数据处理 (已修复刷屏)
    # ---------------------------------------------------------
    @classmethod
    async def process_depth(cls, data):
        symbol = data.get('s')
        if symbol not in cls.vip_symbols:
            return
        count = cls.depth_counter.get(symbol, 0)
        cls.depth_counter[symbol] = count + 1
        if count % 2 != 0:
            return
        bids = data.get('bids') or data.get('b')
        asks = data.get('asks') or data.get('a')

        if not bids or not asks: return

        try:
            bid1 = float(bids[0][0])
            ask1 = float(asks[0][0])
            mid_price = (bid1 + ask1) / 2
            impact = cls.get_impact_pct(symbol)
            target_up = mid_price * (1 + impact)
            target_down = mid_price * (1 - impact)

            # 计算卖盘阻力 (Resistance): 把 asks 从卖一加到 target_up
            cost_to_pump = 0.0
            reached_target_up = False
            for p, q in asks:
                p = float(p)
                if p > target_up:
                    reached_target_up = True
                    break
                cost_to_pump += p * float(q)

            if not reached_target_up:
                return

            # 计算买盘支撑 (Support): 把 bids 从买一加到 target_down
            cost_to_dump = 0.0
            reached_target_down = False
            for p, q in bids:
                p = float(p)
                if p < target_down:
                    reached_target_down = True
                    break
                cost_to_dump += p * float(q)

            if not reached_target_down:
                return

            # 防止除零错误 (极少见)
            if cost_to_pump == 0 or cost_to_dump == 0:
                return

            # 信号判定：寻找“薄弱环节”

            # 场景 A: 上方极度空虚 (易拉升)
            # 逻辑：只要很少的钱(比如支撑的 1/5)就能拉上去
            # 比如：砸下去要 100万，拉上去只要 20万 -> 阻力最小方向向上
            if cost_to_dump > cost_to_pump * 5.0:
                # 增加一个金额门槛，防止盘口太浅的垃圾时间误报
                # 比如山寨币至少要有 5万 U 的抛压，否则可能是随便挂的
                threshold = cls._get_thresholds(symbol)[0] / 4  # 动态取阈值的 1/4

                if cost_to_pump > threshold:
                    if cls._check_cooldown(f"{symbol}_DEPTH_BULL", 600):
                        ratio = cost_to_dump / cost_to_pump
                        await notifier.send_buffered(
                            f"🚀 <b>阻力薄弱 (易拉升): {symbol}</b>\n"
                            f"⬆️ 拉升{impact * 100}%成本: ${cost_to_pump:,.0f} (薄)\n"
                            f"⬇️ 砸盘{impact * 100}%成本: ${cost_to_dump:,.0f} (厚)\n"
                            f"📈 潜在盈亏比: {ratio:.1f}x\n"
                            f"信号: 上方卖压真空，主力控盘"
                        )

            # 场景 B: 下方极度空虚 (易砸盘)
            # 逻辑：支撑很薄，抛压很重
            elif cost_to_pump > cost_to_dump * 5.0:
                threshold = cls._get_thresholds(symbol)[0] / 4

                if cost_to_dump > threshold:
                    if cls._check_cooldown(f"{symbol}_DEPTH_BEAR", 600):
                        ratio = cost_to_pump / cost_to_dump
                        await notifier.send_buffered(
                            f"🩸 <b>支撑薄弱 (易暴跌): {symbol}</b>\n"
                            f"⬇️ 砸盘{impact * 100}%成本: ${cost_to_dump:,.0f} (薄)\n"
                            f"⬆️ 拉升{impact * 100}%成本: ${cost_to_pump:,.0f} (厚)\n"
                            f"📉 下跌阻力比: {ratio:.1f}x\n"
                            f"信号: 下方承接无力，小心画门"
                        )

        except Exception as e:
            logger.error(f"[DEPTH ERROR] {symbol}: {e}")

    # ---------------------------------------------------------
    # 2. K线数据处理
    # ---------------------------------------------------------
    @classmethod
    async def process_kline(cls, data):
        k = data.get("k")
        if not k:
            return
        if k.get("i") == "3m":
            await cls._process_kline_3m(k)
            return

        symbol = data.get("s") or k.get("s")
        if not symbol:
            return
        try:
            quote_volume = float(k["q"])
            open_price = float(k["o"])
            price = float(k["c"])
            closed = bool(k["x"])
        except (KeyError, TypeError, ValueError):
            return
        if open_price <= 0 or price <= 0:
            return

        cls.price_map[symbol] = price
        change_pct = (price - open_price) / open_price
        cls.kline_stats[symbol] = change_pct

        # 仅使用之前已收盘的 1m K 线计算量能基准。
        volume_window = cls.vol_window_1m.setdefault(symbol, deque(maxlen=5))
        volume_mean = sum(volume_window) / len(volume_window) if len(volume_window) == 5 else 0.0

        # 更新事件价格极值，并在常规价格信号前检测强反向冲击。
        SignalDeduplicator.observe_price(symbol, price)
        shock_sent = False
        if quote_volume >= 5000 and volume_mean > 0:
            shock_sent = await cls._check_reverse_shock(
                symbol, open_price, price, change_pct, quote_volume, volume_mean
            )

        if symbol not in cls.rolling_window:
            cls.rolling_window[symbol] = deque(maxlen=cls.WINDOW_SIZE)
        if closed:
            cls.rolling_window[symbol].append(price)

        # 1m 强异动：所有达到原始冷却规则的信号都入库，TG 是否推送由事件层决定。
        if quote_volume >= 5000 and abs(change_pct) > PUMP_THRESHOLD:
            direction = 1 if change_pct > 0 else -1
            direction_name = "UP" if direction > 0 else "DOWN"
            if cls._check_cooldown(f"{symbol}_1M_RAW_{direction_name}", DB_DEDUP_COOLDOWN):
                emoji = "🚀 暴涨" if direction > 0 else "🩸 暴跌"
                oi_amount, oi_time, oi_delta = cls._get_last_oi(symbol)
                oi_text = ""
                if oi_amount > 0:
                    oi_value = oi_amount * price
                    lag = max(0, int(time.time() - oi_time))
                    oi_text = f"\n持仓总量: $" + f"{oi_value:,.0f} ({lag}s前)"
                    if oi_delta is not None:
                        oi_emoji = "🔥 激增" if oi_delta > 0.02 else ("📉 骤减" if oi_delta < -0.02 else "➡️")
                        oi_text += f"\n持仓变化: {oi_emoji} {oi_delta * 100:+.2f}%"
                message = (
                    f"{emoji} <b>{symbol}</b> (1m)\n"
                    f"幅度: {change_pct * 100:.2f}%\n"
                    f"现价: {cls._fmt_price(price)}{oi_text}"
                )
                signal_type = "🚀暴涨" if direction > 0 else "🩸暴跌"
                evidence = {
                    "timestamp": time.time(), "price": price,
                    "change_pct": change_pct, "quote_volume": quote_volume,
                }
                if shock_sent:
                    # 强反向冲击已通知，本次方向信号只入库，避免一根 K 线双发。
                    db_result = await notifier.save_to_db_now(
                        symbol, signal_type, message, price, priority="urgent"
                    )
                    db_status = "ok" if db_result and db_result[0] else "failed"
                    timestamp = time.time()
                    trace_id = f"{symbol}:{int(timestamp * 1000)}:initial_1m"
                    event = SignalDeduplicator.active_events.get(symbol)
                    event_id = event.event_id if event is not None else "-"
                    logger.info(
                        f"[SIGNAL_TRACE] ts={timestamp:.3f} trace_id={trace_id} "
                        f"event_id={event_id} symbol={symbol} kind=initial_1m "
                        f"direction={direction:+d} decision=SUPPRESS "
                        f"reason=reverse_shock_sent_same_bar db={db_status} price={price}"
                    )
                else:
                    await cls._record_event_signal(
                        "initial_1m", symbol, direction, evidence, message,
                        signal_type, price, chart=True, buffered=True,
                    )

        # 趋势信号若命中活动事件，只记为背景；否则保留独立趋势推送。
        if quote_volume >= 5000 and len(cls.rolling_window[symbol]) >= 10:
            start_price = cls.rolling_window[symbol][0]
            if start_price > 0:
                trend_pct = (price - start_price) / start_price
                if abs(trend_pct) > cls.TREND_THRESHOLD and cls._check_cooldown(f"{symbol}_TREND", 1800):
                    trend_direction = 1 if trend_pct > 0 else -1
                    trend_title = "📈 稳步爬升" if trend_direction > 0 else "📉 持续阴跌"
                    trend_label = (
                        "1H趋势" if len(cls.rolling_window[symbol]) >= 60
                        else f"趋势起步({len(cls.rolling_window[symbol])}根1m)"
                    )
                    oi_record = cls.oi_cache.get(symbol)
                    oi_text = ""
                    if oi_record:
                        oi_trend = oi_record.get("delta_1h", 0)
                        oi_value = oi_record["amount"] * price
                        if abs(oi_trend) > 0.01:
                            oi_title = "🔥 吸筹" if oi_trend > 0 else "💨 撤退"
                            oi_text = f"\n资金配合: {oi_title} {oi_trend * 100:+.2f}% (OI窗口)\n总持仓: $" + f"{oi_value:,.0f}"
                        else:
                            oi_text = f"\n资金配合: 😐 无明显变化 ({oi_trend * 100:+.2f}%)"
                    trend_message = (
                        f"{trend_title} <b>{symbol}</b> ({trend_label})\n"
                        f"累计幅度: {trend_pct * 100:+.2f}% (近{len(cls.rolling_window[symbol])}根收盘价)\n"
                        f"现价: {cls._fmt_price(price)}{oi_text}\n"
                        f"信号: 价格与持仓趋势背景"
                    )
                    trend_type = "📈爬升" if trend_direction > 0 else "📉阴跌"
                    trend_db_result = await notifier.save_to_db_now(
                        symbol, trend_type, trend_message, price, priority="urgent",
                        check_first_window=True,
                        startup_age_seconds=notifier.startup_age_seconds,
                    )
                    trend_db_status = "ok" if trend_db_result and trend_db_result[0] else "failed"
                    if trend_db_status == "ok" and len(trend_db_result) > 3:
                        trend_first_in_window = bool(trend_db_result[3])
                    else:
                        # 同上: 降级路径用主键精确排除"自己", 避免 count-based 永久静默
                        trend_first_in_window = notifier.is_symbol_first_notify(
                            symbol,
                            exclude_id=(
                                trend_db_result[4]
                                if trend_db_result and len(trend_db_result) > 4
                                else None
                            ),
                        )
                    event = SignalDeduplicator.get_event(symbol)
                    if event is None:
                        trend_trace = f"{symbol}:{int(time.time() * 1000)}:trend"
                        # 趋势信号使用与 Top 相同的 12h 分段边界；所有信号仍然入库。
                        if not trend_first_in_window:
                            startup_ignored = bool(
                                trend_db_result
                                and len(trend_db_result) > 5
                                and trend_db_result[5]
                            )
                            suppress_reason = (
                                "stale_history_during_startup"
                                if startup_ignored else "symbol_already_notified"
                            )
                            logger.info(
                                f"[SIGNAL_TRACE] trace_id={trend_trace} symbol={symbol} "
                                f"kind=trend decision=SUPPRESS "
                                f"reason={suppress_reason} "
                                f"event_id=- db={trend_db_status} price={price}"
                            )
                            return
                        logger.info(
                            f"[SIGNAL_TRACE] trace_id={trend_trace} symbol={symbol} "
                            f"kind=trend decision=ALLOW reason=no_active_event "
                            f"event_id=- db={trend_db_status} price={price}"
                        )
                        await notifier.send_urgent(
                            trend_message, symbol=symbol, signal_type=trend_type,
                            price=price, save_to_db=False, trace_id=trend_trace,
                        )
                        asyncio.create_task(
                            notifier.send_chart(
                                symbol, timeframe="15m",
                                caption=f"📊 {symbol} 15m K线形态"
                            )
                        )
                    elif event.direction == trend_direction:
                        SignalDeduplicator.touch_event(symbol)
                        logger.info(
                            f"[SIGNAL_TRACE] symbol={symbol} kind=trend "
                            f"decision=SUPPRESS reason=attached_to_event "
                            f"event_id={event.event_id} db={trend_db_status} price={price}"
                        )
                    else:
                        logger.info(
                            f"[SIGNAL_TRACE] symbol={symbol} kind=trend "
                            f"decision=SUPPRESS reason=opposite_active_event "
                            f"event_id={event.event_id} db={trend_db_status} price={price}"
                        )

        if closed:
            volume_window.append(quote_volume)

    @classmethod
    async def _process_kline_3m(cls, k):
        symbol = k.get("s")
        if not symbol:
            return
        try:
            quote_volume = float(k["q"])
            open_price = float(k["o"])
            price = float(k["c"])
            closed = bool(k["x"])
        except (KeyError, TypeError, ValueError):
            return

        volume_window = cls.vol_window_3m.setdefault(symbol, deque(maxlen=5))
        # 收盘时只更新基准，当前未收盘的 K 线才参与实时异动判断。
        if closed:
            volume_window.append(quote_volume)
            return
        if quote_volume < 5000 or open_price <= 0 or len(volume_window) < 5:
            return

        change_pct = (price - open_price) / open_price
        if abs(change_pct) <= PUMP_3M_THRESHOLD:
            return
        mean_volume = sum(volume_window) / len(volume_window)
        if mean_volume <= 0 or quote_volume < mean_volume * VOL_MA5_MULTIPLIER:
            return

        direction = 1 if change_pct > 0 else -1
        direction_name = "UP" if direction > 0 else "DOWN"
        if not cls._check_cooldown(
            f"{symbol}_3M_RAW_{direction_name}", DB_DEDUP_COOLDOWN
        ):
            return

        emoji = "🚀 暴涨" if direction > 0 else "🩸 暴跌"
        volume_ratio = quote_volume / mean_volume
        oi_amount, oi_time, oi_delta = cls._get_last_oi(symbol)
        oi_text = ""
        if oi_amount > 0:
            oi_value = oi_amount * price
            lag = max(0, int(time.time() - oi_time))
            oi_text = f"\n持仓总量: $" + f"{oi_value:,.0f} ({lag}s前)"
            if oi_delta is not None:
                oi_emoji = "🔥 激增" if oi_delta > 0.02 else ("📉 骤减" if oi_delta < -0.02 else "➡️")
                oi_text += f"\n持仓变化: {oi_emoji} {oi_delta * 100:+.2f}%"

        message = (
            f"{emoji} <b>{symbol}</b> (3m放量)\n"
            f"幅度: {change_pct * 100:.2f}%\n"
            f"放量: {volume_ratio:.1f}x (vs 前5根均量)\n"
            f"成交额: $" + f"{quote_volume:,.0f}" + "\n"
            f"现价: {cls._fmt_price(price)}{oi_text}"
        )
        evidence = {
            "timestamp": time.time(),
            "price": price,
            "change_pct": change_pct,
            "vol_ratio": volume_ratio,
            "quote_volume": quote_volume,
        }
        signal_type = "🚀暴涨3m" if direction > 0 else "🩸暴跌3m"
        await cls._record_event_signal(
            "initial_3m", symbol, direction, evidence, message,
            signal_type, price, chart=True, buffered=True,
        )

    @classmethod
    async def process_oi_change_wrapper(cls, data):
        symbol = data["symbol"]
        current_oi = float(data["openInterest"])
        current_time = time.time()

        if symbol not in cls.oi_window:
            cls.oi_window[symbol] = deque(maxlen=60)
        cls.oi_window[symbol].append(current_oi)
        oi_trend_1h = 0.0
        if len(cls.oi_window[symbol]) >= 30:
            start_oi = cls.oi_window[symbol][0]
            if start_oi > 0:
                oi_trend_1h = (current_oi - start_oi) / start_oi

        price = cls._get_current_price(symbol)
        if price <= 0:
            return

        # 先用上一份有效采样计算 delta 和采样年龄，再刷新缓存。
        last_record = cls.oi_cache.get(symbol)
        delta_pct = 0.0
        sample_age = None
        if last_record and last_record["amount"] > 0:
            delta_pct = (current_oi - last_record["amount"]) / last_record["amount"]
            sample_age = current_time - last_record["time"]

        cls.oi_cache[symbol] = {
            "amount": current_oi,
            "time": current_time,
            "delta": delta_pct,
            "delta_1h": oi_trend_1h,
        }

        if current_oi * price < MIN_OI_VALUE or last_record is None:
            return
        if sample_age is None or sample_age > OI_MAX_STALENESS:
            return

        base_threshold = cls._get_oi_threshold(symbol)
        if abs(delta_pct) <= base_threshold:
            return

        strong_threshold = base_threshold * OI_STRONG_MULTIPLIER
        signal_kind = "oi_strong" if abs(delta_pct) > strong_threshold else "oi_base"
        oi_direction = 1 if delta_pct > 0 else -1
        direction_name = "UP" if oi_direction > 0 else "DOWN"
        level_name = "STRONG" if signal_kind == "oi_strong" else "BASE"
        # 基础档和强档各自限频，确保基础档不会挡住随后到来的强升级档。
        if not cls._check_cooldown(
            f"{symbol}_OI_{level_name}_{direction_name}", DB_DEDUP_COOLDOWN
        ):
            return

        oi_value = current_oi * price
        delta_value = (current_oi - last_record["amount"]) * price
        direction_text = "📈 增仓" if delta_pct > 0 else "📉 减仓"
        last_kline_pct = cls.kline_stats.get(symbol)
        kline_text = ""
        if last_kline_pct is not None:
            kline_emoji = "🔴" if last_kline_pct < 0 else "🟢"
            kline_text = f"\n对应币价: {kline_emoji} {last_kline_pct * 100:+.2f}% (1m)"

        level_text = "强异动" if signal_kind == "oi_strong" else "异动"
        message = (
            f"📊 持仓{level_text} (采样间隔{sample_age:.0f}s): <b>{symbol}</b>\n"
            f"方向: {direction_text} {delta_pct * 100:+.2f}%\n"
            f"变动: $" + f"{delta_value:,.0f}" + "\n"
            f"总量: $" + f"{oi_value:,.0f}" + "\n"
            f"现价: {cls._fmt_price(price)}{kline_text}"
        )
        evidence = {
            "timestamp": current_time,
            "price": price,
            "oi_delta": delta_pct,
            "oi_value": oi_value,
            "oi_base_threshold": base_threshold,
            "oi_strong_threshold": strong_threshold,
            "sample_age": sample_age,
        }
        await cls._record_event_signal(
            signal_kind, symbol, oi_direction, evidence, message,
            "📊OI异动", price, chart=False, buffered=True,
        )

    @classmethod
    async def process_agg_trade(cls, data):
        symbol = data['s']
        if symbol not in cls.vip_symbols:
            return
        price = float(data['p'])
        qty = float(data['q'])
        value = price * qty
        ts = time.time()
        # 动态获取阈值
        _, whale_threshold = cls._get_thresholds(symbol)
        if symbol not in KINGS and value < whale_threshold:
            return
        is_sell = data['m']
        side = "SELL" if is_sell else "BUY"
        if symbol in KINGS:
            buf = cls.agg_trade_window.setdefault(symbol, [])

            # 只保留最近 5 秒
            buf.append((ts, value, side))
            cls.agg_trade_window[symbol] = [
                x for x in buf if ts - x[0] <= 5
            ]

            side_sum = sum(v for _, v, s in cls.agg_trade_window[symbol] if s == side)

            if side_sum < whale_threshold:
                return

            if cls._check_cooldown(f"{symbol}_WHALE_{side}", 30):
                emoji = "🔴 巨鲸出货" if is_sell else "🟢 巨鲸进场"
                msg = (f"🐋 <b>{emoji} {symbol}</b>\n"
                       f"5秒累计成交: ${side_sum:,.0f}\n"
                       f"方向: {side}\n"
                       f"现价: {cls._fmt_price(price)}")
                if side_sum > 30000000:
                    await notifier.send_buffered(msg)
            return
        if value < whale_threshold:
            return
        if cls._check_cooldown(f"{symbol}_WHALE_{side}", 5):
            emoji = "🔴 巨鲸出货" if data['m'] else "🟢 巨鲸进场"
            prefix = "🐋" if symbol in MAJORS else ""

            await notifier.send_buffered(
                f"{prefix} {emoji} <b>{symbol}</b>\n"
                f"成交额: ${value:,.0f}\n"
                f"价格: {price}"
            )

    # ---------------------------------------------------------
    # 4. 合约爆仓处理
    # ---------------------------------------------------------
    @classmethod
    async def process_force_order(cls, data):
        order = data.get('o')
        if not order:
            return

        symbol = order['s']
        if symbol not in cls.vip_symbols:
            return
        amount = float(order['q']) * float(order['p'])

        if "USDT" not in symbol: return
        if amount < LIQUIDATION_MIN_VAL: return

        # 爆仓也不需要长冷却，每一笔都重要
        # 设个 1秒 防止重复推送
        if cls._check_cooldown(f"{symbol}_LIQ", 1):
            liq_side = "💀 多军爆仓" if order['S'] == "SELL" else "💀 空军爆仓"
            await notifier.send_buffered(
                f"{liq_side} <b>{symbol}</b>\n"
                f"爆仓金额: ${amount:,.0f}"
            )

    # 5. 期现价差与费率监控 (!markPrice@arr)
    @classmethod
    async def process_mark_price(cls, data_list):
        for item in data_list:
            symbol = item['s']
            if symbol not in cls.vip_symbols:
                continue
            if not symbol.endswith(QUOTE_ASSET):
                continue
            try:
                funding_rate = float(item['r'])
                mark_price = float(item['p'])  # 合约价格
                cls.price_map[symbol] = mark_price
                index_price = float(item['i'])  # 现货价格

                if index_price == 0:
                    continue

                # 计算溢价率
                premium_pct = (mark_price - index_price) / index_price
                # --- 场景 A: 现货强拉 (负溢价) ---
                if premium_pct < PREMIUM_NEGATIVE_THRESHOLD:
                    if cls._check_cooldown(f"{symbol}_PREMIUM_NEG", 600):
                        await notifier.send_buffered(
                            f"💎 <b>现货强拉 (金铲子): {symbol}</b>\n"
                            f"负溢价: {premium_pct * 100:.2f}%\n"
                            # 【修复】这里把 > 改成了 &gt;
                            f"现货: {index_price} &gt; 合约: {mark_price}\n"
                            f"信号: 主力买现货，轧空前兆"
                        )

                # --- 场景 B: 现货出货 (正溢价) ---
                elif premium_pct > PREMIUM_POSITIVE_THRESHOLD:
                    if funding_rate > 0:
                        if cls._check_cooldown(f"{symbol}_PREMIUM_POS", 600):
                            await notifier.send_buffered(
                                f"⚠️ <b>现货出货 (多头陷阱): {symbol}</b>\n"
                                f"正溢价: +{premium_pct * 100:.2f}%\n"
                                # 【修复】这里把 < 改成了 &lt;
                                f"现货: {index_price} &lt; 合约: {mark_price}\n"
                                f"费率: {funding_rate * 100:.4f}% (多头拥挤)\n"
                                f"信号: 大户抛售现货，合约多头即将踩踏"
                            )
            except Exception as e:
                logger.error(f"Error processing {symbol}: {e}")
                continue