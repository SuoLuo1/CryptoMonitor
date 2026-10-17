"""SignalDeduplicator 的纯单元测试：不访问网络、数据库或 Telegram。"""
import unittest

from signal_deduplicator import SignalDeduplicator


class SignalDeduplicatorTests(unittest.TestCase):
    def setUp(self):
        SignalDeduplicator.reset()

    def test_first_signal_sends_same_direction_duplicate_suppressed(self):
        allow, reason = SignalDeduplicator.arbitrate(
            "initial_1m", "ABCUSDT", 1, {"timestamp": 1000, "price": 1.0}
        )
        self.assertTrue(allow)
        self.assertEqual(reason, "initial_1m")

        allow, reason = SignalDeduplicator.arbitrate(
            "initial_3m", "ABCUSDT", 1,
            {"timestamp": 1060, "price": 1.03, "vol_ratio": 3.2},
        )
        self.assertFalse(allow)
        self.assertEqual(reason, "same_direction_event")

    def test_oi_base_and_3m_make_one_joint_confirmation(self):
        SignalDeduplicator.arbitrate(
            "initial_1m", "ABCUSDT", 1, {"timestamp": 1000, "price": 1.0}
        )
        allow, reason = SignalDeduplicator.arbitrate(
            "oi_base", "ABCUSDT", 1, {"timestamp": 1030, "price": 1.01}
        )
        self.assertFalse(allow)
        self.assertEqual(reason, "oi_base_attached")

        allow, reason = SignalDeduplicator.arbitrate(
            "initial_3m", "ABCUSDT", 1,
            {"timestamp": 1100, "price": 1.04, "vol_ratio": 3.5},
        )
        self.assertTrue(allow)
        self.assertEqual(reason, "oi_3m_confirm")

    def test_strong_oi_confirms_once_in_active_event(self):
        SignalDeduplicator.arbitrate(
            "initial_1m", "ABCUSDT", 1, {"timestamp": 1000, "price": 1.0}
        )
        allow, reason = SignalDeduplicator.arbitrate(
            "oi_strong", "ABCUSDT", -1, {"timestamp": 1050, "price": 1.02}
        )
        self.assertTrue(allow)
        self.assertEqual(reason, "oi_strong")

        allow, reason = SignalDeduplicator.arbitrate(
            "oi_strong", "ABCUSDT", 1, {"timestamp": 1070, "price": 1.03}
        )
        self.assertFalse(allow)
        self.assertEqual(reason, "confirmation_already_sent")

    def test_opposite_initial_signal_replaces_event(self):
        SignalDeduplicator.arbitrate(
            "initial_1m", "ABCUSDT", 1, {"timestamp": 1000, "price": 1.0}
        )
        allow, reason = SignalDeduplicator.arbitrate(
            "initial_3m", "ABCUSDT", -1,
            {"timestamp": 1060, "price": 0.94, "vol_ratio": 3.4},
        )
        self.assertTrue(allow)
        self.assertEqual(reason, "initial_3m")
        event = SignalDeduplicator.get_event("ABCUSDT", now=1060)
        self.assertEqual(event.direction, -1)

    def test_event_expires_after_five_minutes_without_material_move(self):
        SignalDeduplicator.arbitrate(
            "initial_1m", "ABCUSDT", 1, {"timestamp": 1000, "price": 1.0}
        )
        self.assertIsNotNone(SignalDeduplicator.get_event("ABCUSDT", now=1299))
        self.assertIsNone(SignalDeduplicator.get_event("ABCUSDT", now=1300))

    def test_reverse_shock_has_one_notification_quota(self):
        SignalDeduplicator.arbitrate(
            "initial_1m", "ABCUSDT", 1, {"timestamp": 1000, "price": 1.0}
        )
        allow, reason = SignalDeduplicator.arbitrate(
            "reverse_shock", "ABCUSDT", -1, {"timestamp": 1010, "price": 0.95}
        )
        self.assertTrue(allow)
        self.assertEqual(reason, "reverse_shock")

        allow, reason = SignalDeduplicator.arbitrate(
            "reverse_shock", "ABCUSDT", -1, {"timestamp": 1020, "price": 0.94}
        )
        self.assertFalse(allow)
        self.assertEqual(reason, "reverse_shock_already_sent")


if __name__ == "__main__":
    unittest.main()
