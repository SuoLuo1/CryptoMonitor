"""信号 12 小时分段判断。纯标准库模块，便于测试。"""

from datetime import datetime, timedelta, timezone


BEIJING_TZ = timezone(timedelta(hours=8))


def _parse_beijing_time(value):
    """将数据库时间解析为北京时间的 naive datetime。旧数据无时区时按北京时间解释。"""
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(value)
    if parsed.tzinfo is not None:
        return parsed.astimezone(BEIJING_TZ).replace(tzinfo=None)
    return parsed


def is_first_signal_after_silence(
    previous_timestamp, current_timestamp, silent_hours=12.0, active_hours=None
):
    """
    判断当前信号是否开启新的连续段。

    规则和 Top 统计一致：没有上一条有效价格信号，或相邻信号间隔严格大于
    silent_hours，才算新段首条；刚好 12 小时不算新段。

    🐾 active_hours (可选): 僵尸币门槛。
      上一条信号距今超过 active_hours 时，说明该币早已沉寂到不该再被打扰，
      视为"无历史" → 返回 True 让它按新冒头处理。
      这与 Top 排名的 ACTIVE_HOURS 僵尸过滤是同一套语义。
      为 None 时不做僵尸过滤（纯 12h 分段判断）。
    """
    if not previous_timestamp:
        return True

    try:
        previous = _parse_beijing_time(previous_timestamp)
        current = _parse_beijing_time(current_timestamp)
    except (TypeError, ValueError):
        # 历史时间无法解析时采取保守策略，避免误发重复通知。
        return False

    if active_hours is not None:
        silence = (current - previous).total_seconds()
        # 沉寂过久 → 该币已是僵尸, 不再推送, 交给 Top 统计口径处理
        if silence > float(active_hours) * 3600.0:
            return False

    return (current - previous).total_seconds() > float(silent_hours) * 3600.0
