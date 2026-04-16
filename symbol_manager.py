# symbol_manager.py
import aiohttp
import logging
from config import BINANCE_HTTP_FUTURES, QUOTE_ASSET, MIN_VOLUME_24H, BLACKLIST, MIN_VOLUME_TG, MIN_VOLUME_AI
from shared.binance_limiter import fetch_with_limit

logger = logging.getLogger("SymbolMgr")


async def get_active_symbols(session):
    """
    通过 exchangeInfo (获取合法名单) + ticker (获取流动性) 双重过滤
    """
    # 1. 获取所有"户籍"信息
    info_url = f"{BINANCE_HTTP_FUTURES}/fapi/v1/exchangeInfo"
    # 2. 获取所有"行情"信息
    ticker_url = f"{BINANCE_HTTP_FUTURES}/fapi/v1/ticker/24hr"

    active_perpetuals = set()

    try:
        # --- 第一步：查户口 (exchangeInfo) ---
        info_data = await fetch_with_limit(
            info_url,
            endpoint="exchangeInfo",
            infinite_wait=True,
        )
        if not info_data:
            logger.error("ExchangeInfo 请求失败")
            return {"all": [], "vip": []}
        
        for s in info_data['symbols']:
            # 【核心过滤参数】
            # 1. 必须是交易中 (TRADING)
            if s['status'] != 'TRADING': continue
            # 2. 必须是永续合约 (PERPETUAL) -> 自动过滤掉交割单、BFUSD 等奇怪东西
            if s['contractType'] != 'PERPETUAL': continue
            # 3. 必须是 USDT 结算
            if s['quoteAsset'] != QUOTE_ASSET: continue

            active_perpetuals.add(s['symbol'])

        logger.info(f"Binance 在架活跃永续合约共：{len(active_perpetuals)} 个")

        # --- 第二步：查流水 (ticker) ---
        ticker_data = await fetch_with_limit(
            ticker_url,
            endpoint="ticker/24hr",
            infinite_wait=True,
        )
        if not ticker_data:
            logger.error("Ticker 请求失败")
            return {"all": [], "vip": []}
            tg_symbols = []  # 对应 500w+
            vip_symbols = []  # 对应 1000w+

            for item in ticker_data:
                symbol = item['symbol']

                # 1. 必须在"户口本"里 (排除掉已经下架但api还有残留的)
                if symbol not in active_perpetuals:
                    continue

                base_asset = symbol.replace(QUOTE_ASSET, "")
                if base_asset in BLACKLIST:
                    continue
                # 2. 流动性过滤
                vol = float(item['quoteVolume'])
                if vol >= MIN_VOLUME_TG:
                    tg_symbols.append(symbol)

                    # 2. 如果同时也大于 AI 门槛 (1000w)，加入 VIP 名单
                    if vol >= MIN_VOLUME_AI:
                        vip_symbols.append(symbol)


        logger.info(f"双重过滤结果: 监控总数 {len(tg_symbols)} 个 | 其中 VIP(AI) {len(vip_symbols)} 个")
        return {
            "all": tg_symbols,  # 给 ConnectionManager 用 (去连接)
            "vip": vip_symbols  # 给 Monitors 用 (做 AI 过滤)
        }

    except Exception as e:
        logger.error(f"获取交易对失败: {e}")
        return {"all": [], "vip": []}