"""信号 12 小时分段判断。纯标准库模块，便于测试。"""

from datetime import datetime, timedelta, timezone


BEIJING_TZ = timezone(timedelta(hours=8))


def _parse_beijing_time(value):
    """将数据库时间解析为北京时间的 naive datetime。旧数据无时区时按北京时间解释。"""
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(value)
    if parsed.tzinfo is not None:
        return parsed.astimezone(BEIJING_TZ).replace(tzinfo=None)
    return parsed


def is_first_signal_after_silence(previous_timestamp, current_timestamp, silent_hours=12.0):
    """
    判断当前信号是否开启新的连续段。

    规则和 Top 统计一致：没有上一条有效价格信号，或相邻信号间隔严格大于
    silent_hours，才算新段首条；刚好 12 小时不算新段。
    """
    if not previous_timestamp:
        return True

    try:
        previous = _parse_beijing_time(previous_timestamp)
        current = _parse_beijing_time(current_timestamp)
    except (TypeError, ValueError):
        # 历史时间无法解析时采取保守策略，避免误发重复通知。
        return False

    return (current - previous).total_seconds() > float(silent_hours) * 3600.0
