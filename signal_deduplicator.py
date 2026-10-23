"""事件级报警去重与通知仲裁。

本模块只管理内存中的行情事件和 Telegram 通知配额，不操作数据库、不发送消息。
调用方应先按原始信号限频保存原始报警，再调用 arbitrate() 判断是否推送。

🐾 12h 首次推送窗口 (主人 10-22 拍板):
  - 与 Top 排名的 12h 滑动窗口完全同一套语义
  - 同一币在"当前 12h 窗口"内只有第一条信号会推送 Telegram
  - 窗口内后续信号全部只入库, 不打扰主人
  - 该币沉寂超过 12h → 视为新冒头 → 又可以推首次
"""
import time
import uuid
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

from config import EVENT_IDLE_CLOSE, EVENT_LINK_WINDOW, EVENT_MATERIAL_MOVE_PCT

# 🐾 与 utils.TelegramNotifier.SILENT_HOURS 保持一致 (12 小时)
#    沉寂超过此时长视为新冒头, 重置"已推送"记录
SILENT_HOURS = 12.0


@dataclass
class EventState:
    symbol: str
    direction: int
    event_start_time: float
    event_start_price: float
    event_high: float
    event_high_ts: float
    event_low: float
    event_low_ts: float
    # 用独立 event_id 把同一轮行情的决策日志关联起来。
    event_id: str
    # last_activity_time 用于事件空闲过期；last_material_time 只记录实质价格推进。
    last_activity_time: float
    last_material_time: float
    last_material_price: float
    last_1m_time: float = 0.0
    last_3m_time: float = 0.0
    oi_base_recorded_ts: float = 0.0
    notif_initial: bool = False
    notif_confirmation: bool = False
    notif_reverse_shock: bool = False
    last_3m_vol_ratio: float = 0.0


class SignalDeduplicator:
    """每个币最多维护一个活动事件。"""

    active_events: Dict[str, EventState] = {}

    # 🐾 12h 首次推送窗口 (主人 10-22)
    #    notified_symbols[symbol] = 该币当前 12h 窗口内首次推送的时间戳
    #    窗口内后续信号只入库不推送; 沉寂超 12h 后过期, 允许再次推送首次
    notified_symbols: Dict[str, float] = {}

    @classmethod
    def reset(cls) -> None:
        """清空内存状态，供测试和受控重启使用。"""
        cls.active_events.clear()
        cls.notified_symbols.clear()

    @classmethod
    def _expire_notified(cls, now: float) -> None:
        """清理沉寂超过 12h 的"已推送"记录, 让这些币可以再次推送首次。"""
        silent = SILENT_HOURS * 3600.0
        expired = [sym for sym, ts in cls.notified_symbols.items() if now - ts > silent]
        for sym in expired:
            del cls.notified_symbols[sym]

    @classmethod
    def is_first_notify(cls, symbol: str, now: Optional[float] = None) -> bool:
        """判断该币在当前 12h 窗口内是否还没推送过。

        返回 True 表示"可以推首次", False 表示"本窗口已推过, 应抑制"。
        """
        current_time = time.time() if now is None else now
        cls._expire_notified(current_time)
        return symbol not in cls.notified_symbols

    @classmethod
    def mark_notified(cls, symbol: str, now: Optional[float] = None) -> None:
        """记录该币已推送 (进入 12h 窗口)。"""
        current_time = time.time() if now is None else now
        cls._expire_notified(current_time)
        cls.notified_symbols[symbol] = current_time

    @classmethod
    def _expire_if_idle(cls, symbol: str, now: float) -> Optional[EventState]:
        event = cls.active_events.get(symbol)
        if event is not None and now - event.last_activity_time >= EVENT_IDLE_CLOSE:
            del cls.active_events[symbol]
            return None
        return event

    @classmethod
    def get_event(cls, symbol: str, now: Optional[float] = None) -> Optional[EventState]:
        current_time = time.time() if now is None else now
        return cls._expire_if_idle(symbol, current_time)

    @classmethod
    def _new_event(
        cls, symbol: str, direction: int, price: float, now: float,
        signal_kind: str, evidence: dict
    ) -> EventState:
        event = EventState(
            symbol=symbol,
            direction=1 if direction > 0 else -1,
            event_start_time=now,
            event_start_price=price,
            event_high=price,
            event_high_ts=now,
            event_low=price,
            event_low_ts=now,
            event_id=uuid.uuid4().hex[:10],
            last_activity_time=now,
            last_material_time=now,
            last_material_price=price,
            notif_initial=True,
        )
        if signal_kind == "initial_1m":
            event.last_1m_time = now
        elif signal_kind == "initial_3m":
            event.last_3m_time = now
            event.last_3m_vol_ratio = float(evidence.get("vol_ratio", 0.0) or 0.0)
        cls.active_events[symbol] = event
        return event

    @classmethod
    def observe_price(
        cls, symbol: str, price: float, now: Optional[float] = None
    ) -> Optional[EventState]:
        """更新事件极值；价格推进与事件活跃计时分开记录。"""
        current_time = time.time() if now is None else now
        event = cls._expire_if_idle(symbol, current_time)
        if event is None or price <= 0:
            return event

        if price > event.event_high:
            event.event_high = price
            event.event_high_ts = current_time
        if price < event.event_low:
            event.event_low = price
            event.event_low_ts = current_time

        base = event.last_material_price
        if base > 0:
            move = (price - base) / base
            material_move = (
                move >= EVENT_MATERIAL_MOVE_PCT
                if event.direction > 0
                else move <= -EVENT_MATERIAL_MOVE_PCT
            )
            if material_move:
                event.last_material_time = current_time
                event.last_material_price = price
                event.last_activity_time = current_time
        return event

    @classmethod
    def touch_event(cls, symbol: str, now: Optional[float] = None) -> None:
        """将趋势背景并入活动事件，不触发通知，也不改变方向。"""
        current_time = time.time() if now is None else now
        event = cls._expire_if_idle(symbol, current_time)
        if event is not None:
            event.last_activity_time = current_time

    @classmethod
    def _is_linked(cls, event: EventState, now: float) -> bool:
        last_price_signal = max(event.last_1m_time, event.last_3m_time)
        return last_price_signal > 0 and 0 <= now - last_price_signal <= EVENT_LINK_WINDOW

    @classmethod
    def arbitrate(
        cls, signal_kind: str, symbol: str, direction: int, evidence: dict
    ) -> Tuple[bool, str]:
        """返回 (是否允许推送, 原因)。

        signal_kind:
          initial_1m / initial_3m / oi_base / oi_strong / reverse_shock。
        OI 的 direction 是 OI 增减方向，不用来切换价格事件方向。
        """
        now = float(evidence.get("timestamp", time.time()))
        price = float(evidence.get("price", 0.0) or 0.0)
        event = cls._expire_if_idle(symbol, now)

        if signal_kind in ("initial_1m", "initial_3m"):
            price_direction = 1 if direction > 0 else -1
            if event is None or event.direction != price_direction:
                cls._new_event(symbol, price_direction, price, now, signal_kind, evidence)
                return True, signal_kind

            if price > 0:
                cls.observe_price(symbol, price, now)
            # 新的同方向原始信号本身也说明行情仍在活动。
            # 不要求价格先走满 1%，否则连续每分钟触发的同一波异动可能被误切成新事件。
            event.last_activity_time = now
            if signal_kind == "initial_1m":
                event.last_1m_time = now
            else:
                event.last_3m_time = now
                event.last_3m_vol_ratio = float(evidence.get("vol_ratio", 0.0) or 0.0)
                if (
                    event.oi_base_recorded_ts > 0
                    and now - event.oi_base_recorded_ts <= EVENT_LINK_WINDOW
                    and not event.notif_confirmation
                ):
                    event.notif_confirmation = True
                    event.last_activity_time = now
                    return True, "oi_3m_confirm"
            return False, "same_direction_event"

        if signal_kind in ("oi_base", "oi_strong"):
            if event is None or not cls._is_linked(event, now):
                return True, "no_event"

            if signal_kind == "oi_base":
                event.oi_base_recorded_ts = now
                event.last_activity_time = now
                if (
                    event.last_3m_time > 0
                    and now - event.last_3m_time <= EVENT_LINK_WINDOW
                    and not event.notif_confirmation
                ):
                    event.notif_confirmation = True
                    event.last_activity_time = now
                    return True, "oi_3m_confirm"
                return False, "oi_base_attached"

            event.last_activity_time = now
            if not event.notif_confirmation:
                event.notif_confirmation = True
                return True, "oi_strong"
            return False, "confirmation_already_sent"

        if signal_kind == "reverse_shock":
            if event is None:
                return False, "no_active_event"
            if direction != -event.direction:
                return False, "not_opposite_direction"
            if event.notif_reverse_shock:
                return False, "reverse_shock_already_sent"
            event.notif_reverse_shock = True
            event.last_activity_time = now
            return True, "reverse_shock"

        return False, "unknown_signal_kind"
