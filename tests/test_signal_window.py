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


if __name__ == "__main__":
    unittest.main()
