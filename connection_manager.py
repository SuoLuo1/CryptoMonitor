# connection_manager.py
import asyncio
import time
from datetime import datetime

import aiohttp
import json
import logging

import symbol_manager
from config import BINANCE_WS_FUTURES_COMBINED, BINANCE_WS_FUTURES_RAW, BINANCE_HTTP_FUTURES
from monitors import Monitors

logger = logging.getLogger("ConnMgr")


class MultiConnectionManager:
    def __init__(self, initial_symbols, initial_vip_symbols):
        self.monitored_symbols = set(initial_symbols)
        Monitors.update_valid_symbols(self.monitored_symbols, set(initial_vip_symbols))
        self.hot_shards_map = {}

    def _chunks(self, lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    async def start(self):
        tasks = []

        # 1. 全市场爆仓监控 (合约特有)
        tasks.append(asyncio.create_task(self._connect_force_order()))

        # 2. 【新增】全市场 标记价格/费率/现货指数 监控
        tasks.append(asyncio.create_task(self._connect_mark_price()))

        tasks.append(asyncio.create_task(self._poll_open_interest()))
        # 启动“雷达”：每隔几分钟扫描有没有新晋热门币
        tasks.append(asyncio.create_task(self._watch_new_hot_symbols()))
        # 3. 分片监控 (K线, 深度, 成交) - 减少分片压力
        chunk_size = 10
        initial_list = list(self.monitored_symbols)
        symbol_chunks = list(self._chunks(initial_list, chunk_size))

        logger.info(f"🚀 启动初始监控，共 {len(initial_list)} 个币种...")

        for idx, chunk in enumerate(symbol_chunks):
            await self._start_new_shard(chunk, f"初始分片#{idx}", is_dynamic=True)
            # 防止瞬间并发太高被币安限流
            await asyncio.sleep(0.2)

        await asyncio.gather(*tasks)

    async def _start_new_shard(self, symbols_list, shard_name, is_dynamic=False):
        stream_list = []
        for s in symbols_list:
            stream_list.append(f"{s.lower()}@aggTrade")
            stream_list.append(f"{s.lower()}@kline_1m")
            stream_list.append(f"{s.lower()}@depth50@500ms")

        full_url = BINANCE_WS_FUTURES_COMBINED + "/".join(stream_list)

        # 使用 create_task 把它扔到后台运行，不阻塞主线程
        task = asyncio.create_task(self._connect_shard(full_url, shard_name))
        logger.info(f"➕ [{shard_name}] 已启动监控: {symbols_list}")

        # 【新增】如果是动态分片，必须记账！
        if is_dynamic:
            self.hot_shards_map[shard_name] = {
                "coins": set(symbols_list),  # 记录这个分片里有哪些币
                "task": task  # 记录任务句柄，方便杀进程
            }

    async def _defrag_shards(self):
        """
        🧩 碎片整理：合并那些只有 1-2 个币的低效连接
        """
        # 1. 找出所有“稀疏分片” (比如监控数量 < 3 的)
        sparse_shards = []
        total_sparse_coins = 0

        # 遍历时要复制 keys，防止修改字典报错
        for name, info in list(self.hot_shards_map.items()):
            coin_count = len(info['coins'])
            # 如果这个连接只监控不到 3 个币，且不是刚刚建立的(防止误杀新分片)
            if 0 < coin_count < 3 and "重组" not in name:
                sparse_shards.append(name)
                total_sparse_coins += coin_count

        # 2. 如果稀疏币的总量凑够了一桌 (比如 > 10 个)，就合并
        if total_sparse_coins >= 10:
            logger.info(f"🧹 触发碎片整理: 发现 {len(sparse_shards)} 个稀疏连接，共 {total_sparse_coins} 个币")

            # 收集所有需要“搬家”的币
            coins_to_move = set()
            for s_name in sparse_shards:
                coins_to_move.update(self.hot_shards_map[s_name]['coins'])

            # 3. 启动一个新的“大分片”接管这些币
            new_shard_name = f"重组_{datetime.now().strftime('%d日%H:%M')}"
            # 注意：这会瞬间建立新连接
            await self._start_new_shard(list(coins_to_move), new_shard_name, is_dynamic=True)

            # 4. 从旧分片中移除这些币 (以此触发旧分片的“空置关闭”逻辑)
            for s_name in sparse_shards:
                shard_info = self.hot_shards_map.get(s_name)
                if shard_info:
                    # 1. 物理断开连接
                    shard_info['task'].cancel()
                    # 2. 从字典中移除
                    del self.hot_shards_map[s_name]
                    logger.info(f"⚡ [碎片整理] 立即关闭旧分片: {s_name}")

            logger.info(f"✨ 碎片整理完成: {len(sparse_shards)} 个旧连接将被回收 -> 合并为 [{new_shard_name}]")


    async def _watch_new_hot_symbols(self):
        logger.info("📡 新币雷达已启动 (每 5分钟 扫描一次)...")
        connector = aiohttp.TCPConnector(limit=20, ttl_dns_cache=300)
        async with aiohttp.ClientSession(connector=connector) as session:
            while True:
                await asyncio.sleep(300)  # 5分钟轮询一次

                try:
                    # 1. 重新获取当前所有符合条件的热门币
                    # 注意：这里会复用 symbol_manager 的逻辑 (成交额 > 1000万)
                    result_map = await symbol_manager.get_active_symbols(session)
                    if result_map is None:
                        logger.warning("⚠️ 网络波动，跳过本次雷达扫描")
                        continue
                    latest_all = set(result_map['all'])  # 500w+
                    latest_vip = set(result_map['vip'])  # 1000w+

                    new_coins = []
                    for s in latest_all:
                        if s not in self.monitored_symbols:
                            new_coins.append(s)
                    # 3. 如果有新币，动态启动监控
                    if new_coins:
                        logger.info(f"🔥 发现 {len(new_coins)} 个新晋热门币! {new_coins}")
                        # 为这批新币单独开一个 WebSocket 连接
                        # 动态名字: "热更分片_12:30:05"
                        if len(new_coins) > 10:
                            chunked = list(self._chunks(new_coins, 10))
                            for i, chunk in enumerate(chunked):
                                await self._start_new_shard(chunk, f"热更_{datetime.now().strftime('%H:%M')}_{i}", is_dynamic=True)
                        else:
                            await self._start_new_shard(new_coins, f"热更_{datetime.now().strftime('%H:%M')}", is_dynamic=True)
                        self.monitored_symbols.update(new_coins)
                    else:
                        logger.info("💤 暂无新晋热门币种...")
                    dead_candidates = []
                    for s in self.monitored_symbols:
                        if s not in latest_all:
                            dead_candidates.append(s)
                    if dead_candidates:
                        logger.info(f"❄️ 检测到凉凉币: {dead_candidates}")
                        shards_to_remove = []
                        for shard_name, info in self.hot_shards_map.items():
                            shard_coins = info['coins']
                            # 从这个分片的名单里，剔除死币
                            # intersection 是取交集
                            dead_in_shard = shard_coins.intersection(set(dead_candidates))

                            if dead_in_shard:
                                # 从分片维护的集合里删掉它们
                                info['coins'] -= dead_in_shard
                                logger.info(f"-> 从 [{shard_name}] 移除: {dead_in_shard}")

                                # 同时从全局监控名单里移除 (停止 OI 轮询)
                                for d in dead_in_shard:
                                    if d in self.monitored_symbols:
                                        self.monitored_symbols.remove(d)

                                # 【关键点】如果这个分片被掏空了，里面没有币了
                                if len(info['coins']) == 0:
                                    logger.info(f"🛑 [{shard_name}] 已空，正在关闭连接...")
                                    # 1. 取消异步任务 (断开 WebSocket)
                                    info['task'].cancel()
                                    # 2. 标记为待删除
                                    shards_to_remove.append(shard_name)
                            # C. 清理账本
                        for s_name in shards_to_remove:
                            if s_name in self.hot_shards_map:
                                del self.hot_shards_map[s_name]
                                logger.info(f"🗑️ [{s_name}] 资源已回收，连接数 -1")
                    await self._defrag_shards()
                    Monitors.update_valid_symbols(self.monitored_symbols, latest_vip)
                except Exception as e:
                    logger.error(f"雷达扫描出错: {e}")

    async def _keep_alive_connection(self, url, name, callback_func):
        """
        一个健壮的、永不断的连接器
        """
        while True:
            session = None
            try:
                # 设置超时: 连接15秒超时, 心跳接收90秒超时
                timeout = aiohttp.ClientTimeout(total=None, connect=15, sock_read=90)
                session = aiohttp.ClientSession(timeout=timeout)

                # heartbeat=20: 每20秒发一次心跳，防止僵尸连接
                logger.info(f"🔵 [{name}] 正在连接...")
                async with session.ws_connect(url, heartbeat=20) as ws:
                    logger.info(f"🟢 [{name}] 连接成功!")

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            payload = json.loads(msg.data)
                            await callback_func(payload)
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            logger.error(f"🔴 [{name}] WS Error: {ws.exception()}")
                            break

            except asyncio.CancelledError:
                # 这是一个正常的停止信号，不要报错
                logger.info(f"🛑 [{name}] 任务被取消")
                if session: await session.close()
                return

            except Exception as e:
                logger.warning(f"⚠️ [{name}] 连接断开/异常: {e}")

            finally:
                # 【至关重要】无论上面发生了什么，这里必须关闭 Session
                # 这就是解决你"10小时后卡死"的关键
                if session:
                    await session.close()
                    # logger.info(f"🧹 [{name}] 清理旧 Session 完成")

            # 断开后，休息 5 秒再重连，防止死循环刷爆 CPU
            await asyncio.sleep(5)


    async def _connect_shard(self, url, shard_name):
        async def callback(payload):
            # 处理分片数据
            stream_name = payload['stream']
            core_data = payload['data']
            symbol_upper = stream_name.split('@')[0].upper()
            core_data['s'] = symbol_upper

            if 'kline' in stream_name:
                await Monitors.process_kline(core_data)
            elif 'aggTrade' in stream_name:
                await Monitors.process_agg_trade(core_data)
            elif 'depth' in stream_name:
                await Monitors.process_depth(core_data)

            # 这允许 asyncio 去处理心跳包和其他紧急任务
            await asyncio.sleep(0)
        await self._keep_alive_connection(url, shard_name, callback)

    async def _connect_force_order(self):
        url = f"{BINANCE_WS_FUTURES_RAW}/!forceOrder@arr"

        async def callback(payload):
            if payload.get('e') == 'forceOrder':
                await Monitors.process_force_order(payload)

        await self._keep_alive_connection(url, "全爆仓流", callback)

    async def _connect_mark_price(self):
        url = f"{BINANCE_WS_FUTURES_RAW}/!markPrice@arr@1s"

        async def callback(payload):
            await Monitors.process_mark_price(payload)

        await self._keep_alive_connection(url, "全价格流", callback)

    async def _poll_open_interest(self):
        url = f"{BINANCE_HTTP_FUTURES}/fapi/v1/openInterest"
        sem = asyncio.Semaphore(5) # 并发限制
        connector = aiohttp.TCPConnector(limit=20, ttl_dns_cache=300, keepalive_timeout=15)

        async with aiohttp.ClientSession(connector=connector) as session:
            while True:
                start_time = time.time()
                try:
                    tasks = []
                    current_symbols = list(self.monitored_symbols)
                    for symbol in current_symbols:
                        tasks.append(self._fetch_with_sem(sem, session, url, symbol))

                    results = await asyncio.gather(*tasks, return_exceptions=True)

                    valid = 0
                    for res in results:
                        if isinstance(res, dict) and 'symbol' in res:
                            await Monitors.process_oi_change_wrapper(res)
                            valid += 1

                    logger.info(f"OI 轮询: {valid}/{len(current_symbols)} OK")

                except Exception as e:
                    logger.error(f"OI Loop Err: {e}")

                elapsed = time.time() - start_time
                await asyncio.sleep(max(10, 60 - elapsed))


    async def _fetch_with_sem(self, sem, session, url, symbol):
        async with sem:
            try:
                # 增加 timeout 防止卡死
                async with session.get(url, params={"symbol": symbol}, timeout=5) as resp:
                    if resp.status == 200:
                        return await resp.json()
            except Exception:
                pass
            return None