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

    # ── 🐾 12h 首次推送窗口 (主人 10-22) ──
    #    该窗口的判断已移到 utils.TelegramNotifier.is_symbol_first_notify()
    #    (复用 Top 排名的 12h 算法, 查 alerts.db), 因此重启后行为一致。
    #    纯状态机测试不涉及 DB, 这里只保留事件级去重的原有覆盖。

    def test_repeated_same_direction_signals_keep_one_event_alive(self):
        # 价格每次变化都不到 1%，但持续出现的同方向异动应属于同一事件。
        allow, reason = SignalDeduplicator.arbitrate(
            "initial_1m", "ABCUSDT", 1, {"timestamp": 1000, "price": 1.0}
        )
        self.assertTrue(allow)

        for ts, price in [
            (1061, 1.002),
            (1122, 1.004),
            (1183, 1.006),
            (1244, 1.008),
            (1305, 1.009),
        ]:
            allow, reason = SignalDeduplicator.arbitrate(
                "initial_1m", "ABCUSDT", 1,
                {"timestamp": ts, "price": price},
            )
            self.assertFalse(allow, f"同方向重复信号不应再次推送: ts={ts}, reason={reason}")

        event = SignalDeduplicator.get_event("ABCUSDT", now=1604)
        self.assertIsNotNone(event)
        self.assertEqual(event.event_start_time, 1000)
        self.assertEqual(event.last_activity_time, 1305)

        # 最后一次有效信号之后满 300 秒，事件才进入空闲过期状态。
        self.assertIsNone(SignalDeduplicator.get_event("ABCUSDT", now=1605))

    def test_material_price_move_refreshes_event_activity(self):
        SignalDeduplicator.arbitrate(
            "initial_1m", "ABCUSDT", 1, {"timestamp": 1000, "price": 1.0}
        )
        SignalDeduplicator.observe_price("ABCUSDT", 1.01, now=1290)
        event = SignalDeduplicator.get_event("ABCUSDT", now=1589)
        self.assertIsNotNone(event)
        self.assertEqual(event.last_material_time, 1290)
        self.assertEqual(event.last_activity_time, 1290)

    def test_oi_confirmation_refreshes_activity_without_moving_price_anchor(self):
        SignalDeduplicator.arbitrate(
            "initial_1m", "ABCUSDT", 1, {"timestamp": 1000, "price": 1.0}
        )
        allow, reason = SignalDeduplicator.arbitrate(
            "oi_strong", "ABCUSDT", 1, {"timestamp": 1100, "price": 1.002}
        )
        self.assertTrue(allow)
        self.assertEqual(reason, "oi_strong")
        event = SignalDeduplicator.get_event("ABCUSDT", now=1100)
        self.assertEqual(event.last_activity_time, 1100)
        self.assertEqual(event.last_material_time, 1000)
        self.assertEqual(event.last_material_price, 1.0)

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
