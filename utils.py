# utils.py
import asyncio
import gc
import io
import re
from datetime import datetime

import aiohttp
import logging
from logging.handlers import TimedRotatingFileHandler
import pandas as pd
import os
import matplotlib.font_manager as fm
import matplotlib.pyplot as plt
import mplfinance as mpf
import talib
import matplotlib
from concurrent.futures import ThreadPoolExecutor
from config import TG_BOT_TOKEN, TG_CHAT_ID, BINANCE_HTTP_FUTURES
from llm_agent import DeepSeekAgent

matplotlib.use('Agg')

# 日志输出：stderr（journal）+ 文件按天轮转（兜底复盘）
LOG_DIR = "/root/CryptoMonitor/logs"
os.makedirs(LOG_DIR, exist_ok=True)
_log_formatter = logging.Formatter('%(asctime)s [%(levelname)s] [%(name)s] %(message)s')
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] [%(name)s] %(message)s')
_file_handler = TimedRotatingFileHandler(
    filename=os.path.join(LOG_DIR, "cryptomonitor.log"),
    when="midnight",
    interval=1,
    backupCount=7,
    encoding="utf-8",
    utc=False,
)
_file_handler.setFormatter(_log_formatter)
logging.getLogger().addHandler(_file_handler)
logger = logging.getLogger("Notifier")


def _draw_chart_task(df, symbol, timeframe):
    """
    使用 mplfinance 画图 (CPU密集型任务)
    """
    try:
        font_path = 'SimHei.ttf'
        my_font = None

        if os.path.exists(font_path):
            # 1. 动态加载字体文件
            fm.fontManager.addfont(font_path)
            # 2. 设置 Matplotlib 全局字体为 SimHei
            plt.rcParams['font.sans-serif'] = ['SimHei']
            # 3. 解决负号显示为方块的问题
            plt.rcParams['axes.unicode_minus'] = False
            # 4. 获取 font 属性传给 style
            my_font = fm.FontProperties(fname=font_path)
        else:
            print("⚠️ 未找到 SimHei.ttf，中文可能乱码。请下载字体文件。")
        # 1. 数据清洗
        plot_df = df.copy()
        plot_df['close'] = plot_df['close'].astype(float)
        plot_df['high'] = plot_df['high'].astype(float)
        plot_df['low'] = plot_df['low'].astype(float)
        plot_df['open'] = plot_df['open'].astype(float)
        plot_df['volume'] = plot_df['volume'].astype(float)

        # 2. 时间处理 (UTC -> UTC+8 北京时间)
        if not pd.api.types.is_datetime64_any_dtype(plot_df['time']):
            plot_df['time'] = pd.to_datetime(plot_df['time'], unit='ms')

        plot_df['time'] = plot_df['time'] + pd.Timedelta(hours=8)
        plot_df.set_index('time', inplace=True)

        # 3. 计算 EMA
        ema20 = talib.EMA(plot_df['close'].values, timeperiod=20)
        ema60 = talib.EMA(plot_df['close'].values, timeperiod=60)
        ema120 = talib.EMA(plot_df['close'].values, timeperiod=120)

        # 4. 设置 EMA 颜色 (白、蓝、紫)
        apds = [
            mpf.make_addplot(ema20, color='white', width=1.2),
            mpf.make_addplot(ema60, color='#2196F3', width=1.2),
            mpf.make_addplot(ema120, color='#9C27B0', width=1.2),
        ]

        # 5. 设置样式
        mc = mpf.make_marketcolors(
            up='#2ebd85', down='#f6465d',
            edge='inherit', wick='inherit',
            volume='in',
            inherit=True
        )

        style_config = {
            'font.size': 12,
            'font.family': 'SimHei' if os.path.exists(font_path) else 'sans-serif'
        }

        s = mpf.make_mpf_style(
            base_mpf_style='nightclouds',
            marketcolors=mc,
            gridstyle=':',
            y_on_right=True,
            rc=style_config
        )

        # 6. 准备缓冲区
        buf = io.BytesIO()

        # 7. 画超大图
        fig, axes = mpf.plot(
            plot_df,
            type='candle',
            style=s,
            addplot=apds,
            volume=True,
            title=f"\n{symbol} ({timeframe}) - Beijing Time",
            figsize=(18, 10),
            datetime_format='%m-%d %H:%M',
            tight_layout=True,
            savefig=dict(fname=buf, dpi=100, bbox_inches='tight', transparent=False),
            block=False,
            returnfig=True
        )

        buf.seek(0)
        return buf

    except Exception as e:
        logger.error(f"❌ 画图失败: {e}")
        return None
    finally:
        plt.close('all')
        try:
            if 'fig' in locals():
                fig.clear()
                plt.close(fig)
        except:
            pass

        gc.collect()


class TelegramNotifier:
    def __init__(self):
        # 紧急队列 (暴涨暴跌)
        self.queue = asyncio.Queue(maxsize=200)

        # 缓冲列表 (存巨鲸、价差等)
        self.buffer_list = []
        self.buffer_lock = asyncio.Lock()

        self.running = False
        self.session = None

        # 实例化 AI
        self.llm = DeepSeekAgent()
        self.last_ai_summary = None
        self.last_summary_time = None

        self.chart_executor = ThreadPoolExecutor(max_workers=2)

    async def start(self):
        self.running = True
        connector = aiohttp.TCPConnector(limit=10, ttl_dns_cache=300)
        timeout = aiohttp.ClientTimeout(total=10)
        self.session = aiohttp.ClientSession(connector=connector, timeout=timeout)

        # 启动两个任务：
        # 1. 负责秒发紧急消息
        asyncio.create_task(self._urgent_worker())
        # 2. 负责每分钟总结非紧急消息
        asyncio.create_task(self._buffer_flush_loop())
        logger.info("✅ TelegramNotifier 已启动 (连接池限制: 10)")

    async def stop(self):
        self.running = False
        if self.session:
            await self.session.close()

    # --- 方法 A: 紧急发送 (直接进队列) ---
    async def send_urgent(self, msg):
        # ✅ 保存警报到数据库
        symbol, signal_type, price = await self._save_alert_to_db(msg, priority='urgent')

        # 🔥 【新版】拼装 Top 排行 + 当前信号（全局视角，老板一眼看全市场热点）
        if symbol and signal_type and price:
            final_msg = self._build_top_ranked_msg(symbol, signal_type, msg)
            msg = final_msg

        logger.info(f"🔥 [send_urgent] 收到紧急消息：{msg[:50]}...")
        try:
            self.queue.put_nowait(msg)
            logger.info(f"✅ [send_urgent] 消息已放入队列，当前队列大小：{self.queue.qsize()}")
            print(f"[🔥URGENT] {msg[:20]}...")
        except asyncio.QueueFull:
            logger.error("❌ [send_urgent] 紧急队列已满，消息丢弃！")
            print("[DROP] 紧急队列满")
        except Exception as e:
            logger.error(f"❌ [send_urgent] 异常：{e}", exc_info=True)

    def _build_top_ranked_msg(self, current_symbol, current_signal_type, current_msg):
        """
        【新版】构建带今日 Top 排行的推送消息

        格式：
        ══════════════════════════════
        📊 今日所有信号 Top (14:32) · 共 23 个币
        ══════════════════════════════
        1. CHRUSDT      8次🔥🔥🔥  🚀 +14.2%
        2. MUBARAKUSDT  5次🔥🔥🔥  🚀 +8.5%
        ...
        ────── 🚀 当前信号：JOEUSDT ──────
        🚀 暴涨 JOEUSDT (3m放量)
        ...
        """
        from datetime import datetime, timezone, timedelta
        BJ_TZ = timezone(timedelta(hours=8))
        now_str = datetime.now(BJ_TZ).strftime('%H:%M')

        # 1. 查今日所有 symbol 排行
        top_list = self.get_today_symbols_top(limit=100)

        # 2. 渲染顶部
        header = (
            "═══════════════════════════════\n"
            f"📊 今日所有信号 Top ({now_str}) · 共 {len(top_list)} 个币\n"
            "═══════════════════════════════\n"
        )

        # 3. 渲染排行
        lines = []
        for i, (sym, cnt, first_p, latest_p) in enumerate(top_list, 1):
            # 🔥 等级：第1次无标记，第2-3次 🔥，第4+ 次 🔥🔥🔥
            if cnt >= 4:
                fire = "🔥🔥🔥"
            elif cnt >= 2:
                fire = "🔥"
            else:
                fire = ""

            # 主方向 + 涨跌幅：仅当 ≥2 次才有意义（1次时首次就是自己，没比较）
            if cnt >= 2 and latest_p is not None and first_p and first_p > 0:
                if latest_p > first_p:
                    main_dir = "🚀"
                    pct_str = f"+{(latest_p - first_p) / first_p * 100:.2f}%"
                elif latest_p < first_p:
                    main_dir = "🩸"
                    pct_str = f"{(latest_p - first_p) / first_p * 100:.2f}%"
                else:
                    main_dir = "➖"
                    pct_str = "持平"
            else:
                main_dir = ""
                pct_str = ""

            # 当前信号标记
            current_mark = " ◀" if sym == current_symbol else ""

            line = f"{i:2d}. {sym:13s} {cnt}次{fire:6s} {main_dir} {pct_str}{current_mark}"
            lines.append(line)

        # 4. 分隔 + 当前信号
        separator = f"\n────── {current_signal_type} 当前信号：{current_symbol} ──────\n"

        return header + "\n".join(lines) + separator + current_msg

    def get_today_symbols_top(self, limit=100):
        """
        【新版】查询今日所有 (symbol) 聚合排行

        返回: [(symbol, total_count, first_price, latest_price), ...]
        按 total_count DESC, last_alert DESC 排序

        注意：latest_price 必须取时间戳最大那一行的价格，不能用 MIN/MAX 替代
        """
        import sqlite3
        from datetime import datetime, timezone, timedelta
        BJ_TZ = timezone(timedelta(hours=8))
        today_start = datetime.now(BJ_TZ).strftime('%Y-%m-%d 00:00:00')

        try:
            conn = sqlite3.connect('/root/CryptoMonitor/alerts.db')
            cursor = conn.cursor()
            # 先取最新一次的时间戳，再 JOIN 回去取价格
            cursor.execute("""
                WITH last_per_symbol AS (
                    SELECT symbol, MAX(timestamp) as last_ts
                    FROM alerts
                    WHERE priority = 'urgent' AND timestamp >= ?
                    GROUP BY symbol
                )
                SELECT a.symbol,
                       COUNT(*) as cnt,
                       MIN(a.price_at_alert) as first_price,
                       (SELECT price_at_alert FROM alerts a2
                        WHERE a2.symbol = a.symbol
                          AND a2.priority = 'urgent'
                          AND a2.timestamp >= ?
                          AND a2.timestamp = l.last_ts
                        LIMIT 1) as latest_price,
                       l.last_ts as last_alert
                FROM alerts a
                JOIN last_per_symbol l ON a.symbol = l.symbol
                WHERE a.priority = 'urgent' AND a.timestamp >= ?
                GROUP BY a.symbol
                ORDER BY cnt DESC, last_alert DESC
                LIMIT ?
            """, (today_start, today_start, today_start, limit))
            rows = cursor.fetchall()
            conn.close()
            return [(r[0], r[1], r[2], r[3]) for r in rows]
        except Exception as e:
            logger.error(f"❌ [get_today_symbols_top] 查询失败：{e}", exc_info=True)
            return []

    # --- 方法 B: 缓冲发送 (进篮子) ---

    async def _save_alert_to_db(self, msg, priority='buffered'):
        """✅ 保存警报到 SQLite 数据库
        
        Args:
            msg: 警报消息
            priority: 'urgent' (紧急) 或 'buffered' (缓冲)
        """
        import sqlite3
        
        try:
            # 解析币种 (例如：JCTUSDT, GWEIUSDT, 1000FLOKIUSDT, 龙虾USDT)
            # 🐾 修复: [A-Z]+USDT 匹配不到带数字/中文的币名 → symbol=None
            symbol_match = re.search(r'<b>([^<>]+USDT)</b>', msg)
            symbol = symbol_match.group(1) if symbol_match else None
            
            if not symbol:
                # 🐾 修复: 裸 return 只返回单个 None, 调用方解包 3 个变量直接
                # TypeError 炸穿 WS callback → 整条连接被杀 → 无限重连
                return None, None, None
            
            # 解析信号类型
            signal_type = "未知"
            is_3m = "(3m放量)" in msg
            if "🚀 暴涨" in msg:
                signal_type = "🚀暴涨3m" if is_3m else "🚀暴涨"
            elif "📈 稳步爬升" in msg:
                signal_type = "📈爬升"
            elif "📉 持续阴跌" in msg:
                signal_type = "📉阴跌"
            elif "🩸 暴跌" in msg:
                signal_type = "🩸暴跌3m" if is_3m else "🩸暴跌"
            elif "阻力薄弱" in msg:
                signal_type = "🧱阻力薄弱"
            elif "支撑薄弱" in msg:
                signal_type = "🧱支撑薄弱"
            elif "巨鲸进场" in msg:
                signal_type = "🐋巨鲸"
            
            # 解析价格 (如果有) - 支持多种格式
            price = None
            
            # 格式 1: "现价：3.283" (中文冒号)
            price_match = re.search(r'现价：\s*([0-9.]+)', msg)
            if price_match:
                price = float(price_match.group(1))
            
            # 格式 2: "现价：3.283" (英文冒号)
            if not price:
                price_match = re.search(r'现价:\s*([0-9.]+)', msg)
                if price_match:
                    price = float(price_match.group(1))
            
            # 格式 3: "$3.283"
            if not price:
                price_match = re.search(r'\$([0-9.]+)', msg)
                if price_match:
                    price = float(price_match.group(1))
            
            # 写入数据库 (使用北京时间)
            from datetime import datetime, timezone, timedelta
            beijing_time = datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S')
            
            conn = sqlite3.connect('/root/CryptoMonitor/alerts.db')
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO alerts (symbol, signal_type, message, price_at_alert, priority, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (symbol, signal_type, msg, price, priority, beijing_time))
            conn.commit()
            conn.close()

            print(f"[📝DB] 保存警报：{symbol} {signal_type} [{priority}]")
            return symbol, signal_type, price

        except Exception as e:
            print(f"[❌DB] 保存警报失败：{e}")
            return None, None, None

    def get_today_signal_stats(self, symbol, signal_type):
        """【新增】查询今日同币种同信号类型的累计次数 + 首次价格

        返回: (count, first_price) 或 (0, None) 当首次报警
        用于在报警消息中叠加"今日第N次"和"vs首次涨跌幅"
        """
        import sqlite3
        try:
            from datetime import datetime, timezone, timedelta
            today_start = datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d 00:00:00')
            conn = sqlite3.connect('/root/CryptoMonitor/alerts.db')
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*), MIN(price_at_alert)
                FROM alerts
                WHERE symbol = ?
                  AND signal_type = ?
                  AND priority = 'urgent'
                  AND timestamp >= ?
            """, (symbol, signal_type, today_start))
            row = cursor.fetchone()
            conn.close()
            count = row[0] or 0
            first_price = row[1] if count > 0 else None
            return count, first_price
        except Exception as e:
            print(f"[❌DB] 查询今日统计失败：{e}")
            return 0, None


    async def send_buffered(self, msg):
        # ❌ 不保存缓冲消息 (只保存紧急警报)

        async with self.buffer_lock:
            # 去掉 HTML 标签，方便 LLM 阅读 (简单处理)
            clean_msg = msg.replace("<b>", "").replace("</b>", "")
            self.buffer_list.append(clean_msg)
            print(f"[🧊BUFFER] 存入缓冲区: {len(self.buffer_list)}")

    # --- 方法 C: 发送图表 (新增) ---
    async def send_chart(self, symbol, timeframe="15m", caption=""):
        """
        拉取 K 线 -> 后台画图 -> 发送图片
        三层 try/except 防护，单条图表失败绝不影响主进程：
          内层：拉 K 线/画图 抛出的业务异常
          中层：上传图片时的网络超时/连接错误
          外层：兜住 CancelledError/Exception，防止拖死 asyncio 主循环
        """
        if not self.session: return

        # ---------- 内层：拉 K 线 + 画图 ----------
        try:
            # 1. 临时拉取 K 线（为了均线准确，拉 200 根）
            url = f"{BINANCE_HTTP_FUTURES}/fapi/v1/klines"
            params = {'symbol': symbol, 'interval': timeframe, 'limit': 200}

            # 使用限流器
            from shared.binance_limiter import limited_get
            resp = limited_get(
                url,
                params=params,
                endpoint="klines",
            )

            if not resp:
                logger.warning(f"❌ 拉 K 线失败：{symbol}")
                return
            raw_data = resp.json()

            # 2. 转换数据
            df = pd.DataFrame(raw_data, columns=[
                'time', 'open', 'high', 'low', 'close', 'volume',
                'close_time', 'qav', 'num', 'taker_buy_vol', 'taker_buy_qav', 'ignore'
            ])

            # 3. 扔到线程池去画图 (关键步骤，不阻塞)
            loop = asyncio.get_event_loop()
            img_buf = await loop.run_in_executor(
                self.chart_executor,
                _draw_chart_task,
                df,
                symbol,
                timeframe
            )

            if not img_buf: return

        except (asyncio.TimeoutError, aiohttp.ClientError) as e:
            # 拉 K 线 / 画图阶段的网络超时 —— 单条失败，不影响其它图表
            logger.error(f"❌ [{symbol}] 拉K线/画图阶段网络异常：{type(e).__name__}: {e}")
            return
        except asyncio.CancelledError:
            # 上层 task 取消（如优雅退出）—— 必须 re-raise，不能吞
            raise
        except Exception as e:
            # 其它业务异常（数据解析、画图崩溃等）—— 隔离在本条图表内
            logger.error(f"❌ [{symbol}] 拉K线/画图阶段出错：{e}", exc_info=True)
            return

        # ---------- 中层：上传图片到 Telegram ----------
        try:
            url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendPhoto"
            data = aiohttp.FormData()
            data.add_field('chat_id', str(TG_CHAT_ID))
            data.add_field('caption', caption)
            data.add_field('photo', img_buf, filename=f'{symbol}.png', content_type='image/png')

            async with self.session.post(url, data=data, timeout=30) as resp:
                if resp.status != 200:
                    logger.error(f"❌ [{symbol}] 发图失败：{await resp.text()}")

        except (asyncio.TimeoutError, aiohttp.ClientError) as e:
            # Telegram 上传超时/断连 —— 最常见的崩溃源，单独兜住
            logger.error(f"❌ [{symbol}] 发图阶段网络异常：{type(e).__name__}: {e}")
        except asyncio.CancelledError:
            # 优雅退出信号，必须 re-raise
            raise
        except Exception as e:
            # 兜底：其它未知错误（编码、磁盘满等）—— 绝不外冒
            logger.error(f"❌ [{symbol}] 发图阶段出错：{e}", exc_info=True)

    # --- 任务 1: 紧急消息发送循环---
    async def _urgent_worker(self):
        while self.running:
            try:
                msgs = []
                msgs.append(await self.queue.get())
                # 稍微聚合一下紧急的，比如 0.5秒内连发了两条暴跌
                for _ in range(2):
                    if self.queue.empty(): break
                    msgs.append(self.queue.get_nowait())

                if not msgs: continue
                text = "\n\n".join(msgs)
                await self._send_to_tg(text)
                await asyncio.sleep(0.5)  # 紧急通道稍微快点
            except Exception as e:
                logger.error(f"Urgent Worker Err: {e}")

    # --- 任务 2: 定时总结循环---
    async def _buffer_flush_loop(self):
        while self.running:
            # 等待 900 秒
            await asyncio.sleep(900)

            # 1. 取出所有积压的消息
            async with self.buffer_lock:
                total_count = len(self.buffer_list)

                # 如果 15分钟内报警少于 5 条，说明行情像死水一样，别浪费钱问 AI 了
                if total_count < 5:
                    if total_count > 0:
                        logger.info(f"本周期仅 {total_count} 条日志，行情冷淡，跳过 AI 分析以省钱。")
                        self.buffer_list.clear()
                    continue

                current_batch = list(self.buffer_list)
                self.buffer_list.clear()

            # logger.info(f"正在让 DeepSeek 分析 {len(current_batch)} 条数据...")

            # 2. 调用 AI 分析
            # summary = await self.llm.analyze_market(current_batch, self.last_ai_summary, self.last_summary_time)

            # 3. 发送总结到 TG
            # if summary:
            #     self.last_ai_summary = summary
            #     self.last_summary_time = datetime.now().strftime("%H:%M:%S")
            #     await self._send_to_tg(summary)
            # else:
            #     logger.warning("AI 分析未响应，转为发送原始格式化日志。")
            #
            #     # 1. 构造标题
            #     header = f"⚠️ **AI 分析暂不可用，展示原始日志 ({len(current_batch)}条)**\n"
            #
            #     # 2. 美化拼接
            #     # 使用 strip() 去除首尾空格
            #     # 在每条日志前加一个小圆点，看起来更像列表
            #     formatted_lines = [f"• {log.strip()}" for log in current_batch]
            #
            #     # 用两个换行符连接，这样每条日志之间会空一行，清晰度极高
            #     body = "\n\n".join(formatted_lines)
            #
            #     full_text = header + "\n" + body
            #
            #     # 3. 智能分段发送 (Telegram 限制 4096 字符)
            #     if len(full_text) > 4000:
            #         # 如果太长，就切片发送
            #         # 每次发 15 条日志左右 (大约 3000-4000 字)
            #         chunk_size = 4000
            #         for i in range(0, len(full_text), chunk_size):
            #             chunk = full_text[i:i + chunk_size]
            #             # 给切片加个页码提示
            #             if i > 0: chunk = f"(接上文)...\n{chunk}"
            #             await self._send_to_tg(chunk)
            #             await asyncio.sleep(0.5)  # 防止发太快触发限流
            #     else:
            #         await self._send_to_tg(full_text)

    # --- 底层发送逻辑 ---
    async def _send_to_tg(self, text):
        """
        【修复】加重试上限，避免 TG 不可达时死磕导致连接堆积
        """
        if not text:
            logger.warning("⚠️ [_send_to_tg] 消息为空，跳过")
            return
        if self.session is None or self.session.closed:
            logger.error(f"❌ [_send_to_tg] Session 已关闭！无法发送。session={self.session}, closed={self.session.closed if self.session else 'None'}")
            return

        url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"}
        logger.info(f"📤 [_send_to_tg] 准备发送 (len={len(text)})")

        MAX_RETRIES = 3
        last_err = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                async with self.session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    response_text = await resp.read()
                    if resp.status == 200:
                        logger.info(f"✅ [_send_to_tg] 发送成功 (attempt {attempt})")
                        return
                    logger.error(f"❌ [_send_to_tg] 非 200 ({attempt}/{MAX_RETRIES}): {resp.status} body={response_text[:200]}")
                    last_err = f"HTTP {resp.status}"
                    # 4xx 是参数问题，不再重试
                    if 400 <= resp.status < 500:
                        break
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logger.warning(f"⚠️ [_send_to_tg] 网络异常 ({attempt}/{MAX_RETRIES}): {e}")
                last_err = str(e)
            # 指数退避后重试
            if attempt < MAX_RETRIES:
                await asyncio.sleep(2 ** attempt)

        # 全部失败：进 dead-letter，不再阻塞
        logger.error(f"❌ [_send_to_tg] 彻底失败 ({MAX_RETRIES}次)，丢弃消息进 dead-letter: last_err={last_err}, text={text[:80]}")
        # TODO: 写入本地 dead-letter 文件供事后回溯


notifier = TelegramNotifier()