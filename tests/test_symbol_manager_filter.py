"""symbol_manager 监控名单过滤的纯单元测试：不访问网络、数据库或 Telegram。

重点回归：TradFi 永续（NVDA/TSLA/XAU 等）contractType 是 TRADIFI_PERPETUAL，
不能被 `!= 'PERPETUAL'` 一刀切掉，但也不能整类放行 —— 只放行 config 里的白名单。
"""
import asyncio
import unittest

import config
import symbol_manager


def _sym(symbol, contract_type="PERPETUAL", status="TRADING", quote="USDT"):
    return {
        "symbol": symbol,
        "contractType": contract_type,
        "status": status,
        "quoteAsset": quote,
    }


def _ticker(symbol, quote_volume):
    return {"symbol": symbol, "quoteVolume": str(quote_volume)}


class SymbolManagerFilterTests(unittest.TestCase):
    def _run(self, symbols, tickers):
        async def fake_get(session, url, **kwargs):
            if url.endswith("/exchangeInfo"):
                return {"symbols": symbols}
            return tickers

        original = symbol_manager.limited_get_async
        symbol_manager.limited_get_async = fake_get
        try:
            return asyncio.run(symbol_manager.get_active_symbols(None))
        finally:
            symbol_manager.limited_get_async = original

    def test_plain_perpetual_included(self):
        result = self._run([_sym("BTCUSDT")], [_ticker("BTCUSDT", 5_000_000_000)])
        self.assertIn("BTCUSDT", result["all"])
        self.assertIn("BTCUSDT", result["vip"])

    def test_tradifi_in_whitelist_included(self):
        self.assertIn("NVDAUSDT", config.TRADIFI_WHITELIST)
        result = self._run(
            [_sym("NVDAUSDT", "TRADIFI_PERPETUAL")],
            [_ticker("NVDAUSDT", 3_700_000_000)],
        )
        self.assertIn("NVDAUSDT", result["all"])
        self.assertIn("NVDAUSDT", result["vip"])

    def test_tradifi_not_in_whitelist_excluded(self):
        excluded = "AMGNUSDT"
        self.assertNotIn(excluded, config.TRADIFI_WHITELIST)
        result = self._run(
            [_sym(excluded, "TRADIFI_PERPETUAL")],
            [_ticker(excluded, 10_000_000)],
        )
        self.assertEqual(result["all"], [])
        self.assertEqual(result["vip"], [])

    def test_delivery_and_other_types_excluded(self):
        result = self._run(
            [
                _sym("BTCUSDT_261225", "CURRENT_QUARTER"),
                _sym("BFUSDUSDT", "PERPETUAL", quote="USDC"),
            ],
            [_ticker("BTCUSDT_261225", 1e12), _ticker("BFUSDUSDT", 1e12)],
        )
        self.assertEqual(result["all"], [])

    def test_blacklist_and_non_trading_excluded(self):
        result = self._run(
            [
                _sym("BTCDOMUSDT"),
                _sym("SOLUSDT", status="BREAK"),
            ],
            [_ticker("BTCDOMUSDT", 1e10), _ticker("SOLUSDT", 1e10)],
        )
        self.assertEqual(result["all"], [])

    def test_low_volume_kept_out_of_vip_but_monitored(self):
        result = self._run(
            [_sym("SMALLCAPUSDT")], [_ticker("SMALLCAPUSDT", 500_000)]
        )
        self.assertEqual(result["all"], ["SMALLCAPUSDT"])
        self.assertEqual(result["vip"], [])

    def test_whitelist_size_and_shape(self):
        self.assertEqual(len(config.TRADIFI_WHITELIST), 50)
        for sym in config.TRADIFI_WHITELIST:
            self.assertTrue(sym.endswith("USDT"), sym)


if __name__ == "__main__":
    unittest.main()
