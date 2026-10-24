"""12h 首次信号分段边界测试。"""

import unittest
from signal_window import is_first_signal_after_silence


class SignalWindowTests(unittest.TestCase):
    def test_no_previous_signal_starts_a_new_window(self):
        self.assertTrue(
            is_first_signal_after_silence(
                None, "2026-10-24 12:00:00"
            )
        )

    def test_gap_below_12_hours_is_not_a_new_window(self):
        self.assertFalse(
            is_first_signal_after_silence(
                "2026-10-24 00:01:00",
                "2026-10-24 12:00:59",
            )
        )

    def test_gap_exactly_12_hours_is_not_a_new_window(self):
        self.assertFalse(
            is_first_signal_after_silence(
                "2026-10-24 00:00:00",
                "2026-10-24 12:00:00",
            )
        )

    def test_gap_over_12_hours_starts_a_new_window(self):
        self.assertTrue(
            is_first_signal_after_silence(
                "2026-10-24 00:00:00",
                "2026-10-24 12:00:01",
            )
        )

    def test_timezone_aware_history_uses_beijing_time(self):
        self.assertTrue(
            is_first_signal_after_silence(
                "2026-10-23T12:00:00+08:00",
                "2026-10-24 00:00:01",
            )
        )

    # ── 🐾 僵尸币门槛 (active_hours) ──
    #    沉寂超过 active_hours 的币不再推送, 避免上线瞬间把 4 月的僵尸币全推一遍。

    def test_active_hours_blocks_long_silent_zombie(self):
        self.assertFalse(
            is_first_signal_after_silence(
                "2026-04-15 16:59:55",
                "2026-10-24 13:00:00",
                silent_hours=12.0,
                active_hours=24.0,
            )
        )

    def test_active_hours_allows_recent_silence_beyond_silent_hours(self):
        # 沉寂 13h: 超过 12h 但不足 24h → 仍是新段首条
        self.assertTrue(
            is_first_signal_after_silence(
                "2026-10-24 00:00:00",
                "2026-10-24 13:00:00",
                silent_hours=12.0,
                active_hours=24.0,
            )
        )

    def test_active_hours_none_keeps_pure_gap_semantics(self):
        # 不传 active_hours → 只按 12h 分段判断, 沉寂多久都算新段首条
        self.assertTrue(
            is_first_signal_after_silence(
                "2026-04-15 16:59:55",
                "2026-10-24 13:00:00",
                silent_hours=12.0,
            )
        )

    def test_active_hours_does_not_affect_short_gap(self):
        # 间隔不足 12h 时, 无论 active_hours 如何都不是首次
        self.assertFalse(
            is_first_signal_after_silence(
                "2026-10-24 12:00:00",
                "2026-10-24 12:30:00",
                silent_hours=12.0,
                active_hours=24.0,
            )
        )

    def test_no_previous_signal_is_first_even_with_active_hours(self):
        # 从没冒过头的币, 不受僵尸门槛影响
        self.assertTrue(
            is_first_signal_after_silence(
                None,
                "2026-10-24 13:00:00",
                silent_hours=12.0,
                active_hours=24.0,
            )
        )

    def test_unparsable_history_falls_back_to_suppress(self):
        # 历史时间解析不了 → 保守抑制, 不误发
        self.assertFalse(
            is_first_signal_after_silence(
                "not-a-timestamp",
                "2026-10-24 13:00:00",
            )
        )


if __name__ == "__main__":
    unittest.main()
