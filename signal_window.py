"""信号 12 小时分段判断及启动期历史噪声保护。"""

from datetime import datetime, timedelta, timezone


BEIJING_TZ = timezone(timedelta(hours=8))


def _parse_beijing_time(value):
    """将数据库时间解析为北京时间的 naive datetime。旧数据无时区时按北京时间解释。"""
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(value)
    if parsed.tzinfo is not None:
        return parsed.astimezone(BEIJING_TZ).replace(tzinfo=None)
    return parsed


def _seconds_between(previous_timestamp, current_timestamp):
    previous = _parse_beijing_time(previous_timestamp)
    current = _parse_beijing_time(current_timestamp)
    return (current - previous).total_seconds()


def is_first_signal_after_silence(previous_timestamp, current_timestamp, silent_hours=12.0):
    """
    判断当前信号是否开启新的连续段。

    没有上一条有效信号，或相邻信号间隔严格大于 silent_hours，才算新段首条。
    刚好 12 小时不算新段。此函数只负责 12h 分段，不承担启动期历史噪声过滤。
    """
    if not previous_timestamp:
        return True

    try:
        return _seconds_between(previous_timestamp, current_timestamp) > float(silent_hours) * 3600.0
    except (TypeError, ValueError):
        # 历史时间无法解析时采取保守策略，避免误发重复通知。
        return False


def should_ignore_stale_signal_during_startup(
    previous_timestamp,
    current_timestamp,
    startup_age_seconds,
    startup_grace_seconds=900.0,
    active_hours=24.0,
):
    """
    判断是否只应忽略启动暖机期的陈旧历史触发。

    仅当进程启动未超过 startup_grace_seconds，且上一条有效信号距当前超过
    active_hours，才返回 True。调用方仍可将该条原始记录写入 Top 数据，但
    应将其标记为不推进通知窗口，避免它挡住启动暖机结束后的真实新信号。
    """
    if not previous_timestamp or startup_age_seconds is None:
        return False

    try:
        if float(startup_age_seconds) >= float(startup_grace_seconds):
            return False
        return _seconds_between(previous_timestamp, current_timestamp) > float(active_hours) * 3600.0
    except (TypeError, ValueError):
        # 时间不可解析时不把记录标成启动期忽略记录。
        return False
