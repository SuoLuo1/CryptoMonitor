# main.py
import asyncio

import aiohttp

from utils import notifier
from symbol_manager import get_active_symbols
from connection_manager import MultiConnectionManager


async def main():
    print(">>> 全市场异动监控系统启动中...")

    # 1. 启动TG发送器
    await notifier.start()

    # 2. 获取市场列表
    async with aiohttp.ClientSession() as session:
        try:
            print(">>> 正在获取交易对白名单...")
            result_map = await get_active_symbols(session)
        except Exception as e:
            print(f">>> 初始化获取失败: {e}")
            return
    latest_all = result_map['all']  # 500w+ (监控列表)
    latest_vip = result_map['vip']  # 1000w+ (AI列表)

    print(f">>> 扫描到 {len(latest_all)} 个活跃交易对 (其中 VIP: {len(latest_vip)} 个)")

    if not latest_all:
        print(">>> 错误：未获取到交易对")
        return

    # 3. 启动连接
    manager = MultiConnectionManager(latest_all, latest_vip)
    try:
        await manager.start()
    finally:
        await notifier.stop()


if __name__ == "__main__":
    try:
        # Windows 兼容性设置
        if asyncio.get_event_loop_policy().__class__.__name__ == 'WindowsProactorEventLoopPolicy':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(main())
    except KeyboardInterrupt:
        print(">>> 停止")