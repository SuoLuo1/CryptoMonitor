"""按 24h 成交额重新生成 config.TRADIFI_WHITELIST（只打印，不改文件）。

TradFi 永续（股票/ETF/商品/外汇）在币安是 contractType=TRADIFI_PERPETUAL，
不属于普通 PERPETUAL，所以 symbol_manager 默认整类不监控。
本脚本拉一次 exchangeInfo + ticker，取成交额前 N 名，输出可直接粘贴的配置块。

用法：
    venv/bin/python3 refresh_tradifi_whitelist.py [N]      # N 默认 50
"""
import json
import sys
import urllib.request

import config

TOP_N = 50


def _get(path):
    url = f"{config.BINANCE_HTTP_FUTURES}{path}"
    with urllib.request.urlopen(url, timeout=20) as resp:
        return json.load(resp)


def main():
    top_n = int(sys.argv[1]) if len(sys.argv) > 1 else TOP_N

    info = _get("/fapi/v1/exchangeInfo")
    ticker = _get("/fapi/v1/ticker/24hr")

    volume = {t["symbol"]: float(t["quoteVolume"]) for t in ticker}
    tradifi = [
        s["symbol"]
        for s in info["symbols"]
        if s["contractType"] == "TRADIFI_PERPETUAL"
        and s["quoteAsset"] == config.QUOTE_ASSET
        and s["status"] == "TRADING"
    ]
    tradifi.sort(key=lambda sym: -volume.get(sym, 0.0))

    total = sum(volume.get(s, 0.0) for s in tradifi)
    picked = tradifi[:top_n]
    picked_vol = sum(volume.get(s, 0.0) for s in picked)
    share = picked_vol / total * 100 if total else 0.0

    print(f"# TradFi 在架 {len(tradifi)} 个，24h 合计 {total / 1e8:.1f} 亿U")
    print(f"# 取前 {len(picked)} 个：{picked_vol / 1e8:.1f} 亿U，占 {share:.1f}%")
    print(f"# 生成时间点：{__import__('datetime').datetime.now():%Y-%m-%d %H:%M}")
    print("TRADIFI_WHITELIST = {")
    for i in range(0, len(picked), 5):
        row = ", ".join(f"'{s}'" for s in picked[i:i + 5])
        print(f"    {row},")
    print("}")
    print()
    print("# 排名明细（用于人工核对）")
    for idx, sym in enumerate(picked, 1):
        print(f"# {idx:2d}. {sym:22s} {volume.get(sym, 0.0) / 1e8:6.2f} 亿U")


if __name__ == "__main__":
    main()
