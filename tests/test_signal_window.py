"""12h 首次信号分段与启动期陈旧记录保护测试。"""

import unittest

from signal_window import (
    is_first_signal_after_silence,
    should_ignore_stale_signal_during_startup,
)


class SignalWindowTests(unittest.TestCase):
    def test_no_previous_signal_starts_a_new_window(self):
        self.assertTrue(
            is_first_signal_after_silence(None, "2026-10-24 12:00:00")
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

    def test_stale_april_record_is_ignored_only_during_startup_warmup(self):
        self.assertTrue(
            should_ignore_stale_signal_during_startup(
                "2026-04-15 16:59:55",
                "2026-10-24 13:00:00",
                startup_age_seconds=60,
                startup_grace_seconds=900,
                active_hours=24,
            )
        )

    def test_exactly_24_hour_gap_is_not_treated_as_stale(self):
        self.assertFalse(
            should_ignore_stale_signal_during_startup(
                "2026-10-23 13:00:00",
                "2026-10-24 13:00:00",
                startup_age_seconds=60,
                startup_grace_seconds=900,
                active_hours=24,
            )
        )

    def test_old_record_does_not_block_a_signal_after_startup_warmup(self):
        self.assertFalse(
            should_ignore_stale_signal_during_startup(
                "2026-04-15 16:59:55",
                "2026-10-24 13:00:00",
                startup_age_seconds=901,
                startup_grace_seconds=900,
                active_hours=24,
            )
        )
        # 脱离启动暖机期后，正常分段规则仍会认定 >12h 是新段首条。
        self.assertTrue(
            is_first_signal_after_silence(
                "2026-04-15 16:59:55",
                "2026-10-24 13:00:00",
            )
        )

    def test_13_hour_gap_is_new_window_but_not_stale_startup_noise(self):
        self.assertFalse(
            should_ignore_stale_signal_during_startup(
                "2026-10-24 00:00:00",
                "2026-10-24 13:00:00",
                startup_age_seconds=60,
                startup_grace_seconds=900,
                active_hours=24,
            )
        )
        self.assertTrue(
            is_first_signal_after_silence(
                "2026-10-24 00:00:00",
                "2026-10-24 13:00:00",
            )
        )

    def test_no_history_does_not_trigger_stale_startup_filter(self):
        self.assertFalse(
            should_ignore_stale_signal_during_startup(
                None,
                "2026-10-24 13:00:00",
                startup_age_seconds=60,
                startup_grace_seconds=900,
                active_hours=24,
            )
        )

    def test_invalid_history_is_not_marked_as_ignored(self):
        self.assertFalse(
            should_ignore_stale_signal_during_startup(
                "not-a-timestamp",
                "2026-10-24 13:00:00",
                startup_age_seconds=60,
                startup_grace_seconds=900,
                active_hours=24,
            )
        )
        self.assertFalse(
            is_first_signal_after_silence(
                "not-a-timestamp",
                "2026-10-24 13:00:00",
            )
        )

    def test_missing_startup_age_does_not_enable_startup_filter(self):
        self.assertFalse(
            should_ignore_stale_signal_during_startup(
                "2026-04-15 16:59:55",
                "2026-10-24 13:00:00",
                startup_age_seconds=None,
                startup_grace_seconds=900,
                active_hours=24,
            )
        )


if __name__ == "__main__":
    unittest.main()
