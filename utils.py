# utils.py
import asyncio
import gc
import io
import json
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

    # --- 方法 A: 紧急消息发送 ---
    async def save_to_db_now(
        self, symbol, signal_type, msg, price=None, priority="urgent"
    ):
        """只保存原始信号，不发送 Telegram；供事件层抑制消息后保留历史记录。"""
        return await self._save_alert_to_db(
            msg, priority=priority, symbol=symbol, signal_type=signal_type, price=price
        )

    async def send_urgent(
        self, msg, *, symbol=None, signal_type=None, price=None,
        save_to_db=True, trace_id=None
    ):
        """紧急推送。默认兼容旧调用（先入库再发送）；新事件流显式传 save_to_db=False。"""
        if save_to_db:
            symbol, signal_type, price = await self._save_alert_to_db(
                msg,
                priority="urgent",
                symbol=symbol,
                signal_type=signal_type,
                price=price,
            )

        # Top 排名仍从 alerts.db 读取，因此调用方必须在发消息前先完成入库。
        if symbol and signal_type and price is not None:
            loop = asyncio.get_event_loop()
            final_msg = await loop.run_in_executor(
                None, self._build_top_ranked_msg, symbol, signal_type, msg
            )
            msg = final_msg

        logger.info(f"🔥 [send_urgent] 收到紧急消息：{msg[:50]}...")
        try:
            # 将追踪 ID 与消息一起入队，后续即使聚合发送也能反查实际 TG message_id。
            self.queue.put_nowait({"text": msg, "trace_id": trace_id})
            logger.info(
                f"[TG_QUEUE] trace_id={trace_id or '-'} status=accepted "
                f"queue_size={self.queue.qsize()}"
            )
            print(f"[🔥URGENT] {msg[:20]}...")
            return True
        except asyncio.QueueFull:
            logger.error(
                f"[TG_QUEUE] trace_id={trace_id or '-'} status=dropped reason=queue_full"
            )
            print("[DROP] 紧急队列满")
            return False
        except Exception as e:
            logger.error(
                f"[TG_QUEUE] trace_id={trace_id or '-'} status=error error={e}",
                exc_info=True,
            )
            return False

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

        # 🐾 跨天日期显示需要 datetime 类 (10-04)
        _dt = datetime

        # 1. 查今日所有 symbol 排行
        top_list = self.get_today_symbols_top(limit=100)

        # 2. 渲染顶部
        header = (
            "═══════════════════════════════\n"
            f"📊 今日所有信号 Top ({now_str}) · 共 {len(top_list)} 个币\n"
            "═══════════════════════════════\n"
        )

        # 3. 渲染排行
        # 🐾 加首次信号时间 ⏱HH:MM (区分凌晨旧信号 vs 刚冒头的新机会)
        # 🐾 长度保护: TG 上限 4096, 加时间戳后100币必超, 按预算截断
        MSG_BUDGET = 3800  # 给分隔符+当前信号正文留 buffer
        lines = []
        used_len = len(header)
        shown = 0
        current_line = None  # 当前信号的币, 单独保底
        for i, (sym, cnt, first_p, latest_p, first_ts) in enumerate(top_list, 1):
            # 🔥 等级：第1次无标记，第2-3次 🔥，第4+ 次 🔥🔥🔥
            if cnt >= 4:
                fire = "🔥🔥🔥"
            elif cnt >= 2:
                fire = "🔥"
            else:
                fire = ""

            # ⏱ 首次信号时间 (HH:MM), 跨天则带日期 (M/D HH:MM)
            # 🐾 10-04: 12h 滑动窗口后, 首次时间可能是昨天/前天, 加日期主人能看清"老面孔"
            if first_ts and len(first_ts) >= 16:
                try:
                    first_dt = _dt.fromisoformat(first_ts)
                    now_bj = _dt.now()
                    if first_dt.date() == now_bj.date():
                        first_time = first_dt.strftime('%H:%M')
                    else:
                        first_time = first_dt.strftime('%m/%d %H:%M')
                except Exception:
                    first_time = first_ts[11:16]
            else:
                first_time = "--:--"

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

            line = f"{i:2d}. {sym:13s} ⏱{first_time} {cnt}次{fire:6s} {main_dir} {pct_str}{current_mark}"

            if sym == current_symbol:
                current_line = line  # 保底: 当前信号行不参与截断

            # 长度预算控制 (预留 separator + 省略提示 ≈ 250 字符)
            if used_len + len(line) + 1 <= MSG_BUDGET - 250 or (current_msg is None):
                lines.append(line)
                used_len += len(line) + 1
                shown += 1
            elif sym == current_symbol:
                lines.append(line)
                used_len += len(line) + 1
                shown += 1
                current_line = None  # 已入列, 不用保底

        # 被截断的剩余币数
        omitted = len(top_list) - shown
        if omitted > 0:
            note = f"... 还有 {omitted} 个币未显示"
            if current_line:  # 当前信号的币被截断了, 补在最后
                note += f"\n{current_line}"
            lines.append(note)

        # 4. 分隔 + 当前信号
        separator = f"\n────── {current_signal_type} 当前信号：{current_symbol} ──────\n"

        return header + "\n".join(lines) + separator + current_msg

    # 🐾 12h 滑动窗口阈值: 该币沉寂超过此时长视为新冒头, 重置"首次"
    SILENT_HOURS = 12.0
    # 🐾 僵尸币过滤: 超过此时长无任何 urgent 信号 → 不进 Top (否则几周前的僵尸会污染排名)
    ACTIVE_HOURS = 24.0

    def get_today_symbols_top(self, limit=100):
        """
        查询所有 (symbol) 聚合排行 —— 【12h 滑动窗口版】

        返回: [(symbol, total_count, first_price, latest_price, first_ts), ...]
        按 total_count DESC, last_alert DESC 排序

        🐾 12h 滑动窗口语义 (主人 10-04 拍板):
          - 每币每信号独立窗口
          - 沉寂 < 12h → 跟上一波接着算 (首次 = 当前窗口内最早一条)
          - 沉寂 ≥ 12h → 视为新冒头, 重置 (首次 = 本次这一条)
          - 涨幅 = 窗口首次价 → 最新价 (跟 UI 上显示的⏱首次时间是同一个基准)

        🐾 历史价格 bug 修复沿用:
          1. first_price 取"窗口首次时间点"的价 (不是 MIN 价, 不是 DB 最早价)
          2. latest_price 取"时间最新且 price_at_alert 非 NULL"的价
        """
        import sqlite3
        from datetime import datetime, timezone, timedelta

        try:
            conn = sqlite3.connect('/root/CryptoMonitor/alerts.db')
            cursor = conn.cursor()

            # 🐾 一次性拿到所有 urgent 记录的 (symbol, timestamp, price_at_alert)
            #   在 Python 侧按 12h 窗口切分, 简单可靠 (避免 SQLite 复杂子查询难调)
            cursor.execute("""
                SELECT symbol, timestamp, price_at_alert
                FROM alerts
                WHERE priority = 'urgent'
                  AND price_at_alert IS NOT NULL
                ORDER BY symbol ASC, timestamp ASC
            """)
            raw_rows = cursor.fetchall()
            conn.close()

            if not raw_rows:
                return []

            from collections import defaultdict
            from datetime import datetime as _dt

            # 1. 按 symbol 分组, 每组按时间升序 (timestamp 转 datetime, 方便算间隔)
            per_sym = defaultdict(list)
            for sym, ts, price in raw_rows:
                per_sym[sym].append((_dt.fromisoformat(ts), price))

            # 2. 对每只币: 找到"当前 12h 窗口"的首次
            #    算法: 从该币最后一条往前回溯, 遇到第一个间隔 >12h 的就停,
            #          那个间隔之后的"连续段"就是当前窗口
            #    兜底: 如果最后一条本身就沉寂超 ACTIVE_HOURS, 整只币视作僵尸, 跳过
            SILENT = self.SILENT_HOURS * 3600.0
            ACTIVE = self.ACTIVE_HOURS * 3600.0
            now_epoch = _dt.now().timestamp()
            results = []
            for sym, recs in per_sym.items():
                if not recs:
                    continue
                last_ts, last_price = recs[-1]

                # 🐾 僵尸币过滤: 最后一次 urgent 距今 > ACTIVE_HOURS → 跳过
                last_epoch = last_ts.timestamp() if isinstance(last_ts, _dt) else _dt.fromisoformat(last_ts).timestamp()
                if now_epoch - last_epoch > ACTIVE:
                    continue

                # 🐾 主人语义 (10-05): 从末条往前扫描, 找到【第一个】相邻间隔 >12h 的位置
                #    那个间隔之后的"起点"就是当前窗口的首次 (前面的整段抛弃)
                #    不需要继续往前追溯, 因为"超过 12h 就清空"只看相邻间隔
                win_start_idx = len(recs) - 1
                for i in range(len(recs) - 2, -1, -1):
                    diff = (recs[i + 1][0] - recs[i][0]).total_seconds()
                    if diff > SILENT:
                        win_start_idx = i + 1  # 这条(后者)是新的窗口起点
                        break  # 找到第一个 >12h 边界就停

                win_start_ts, win_start_price = recs[win_start_idx]
                cnt = len(recs) - win_start_idx  # 当前窗口内的累计次数

                # latest_price 沿用"该窗口内时间最新的价" (窗口内每条都有 price, 所以就是最后一条)
                latest_price = last_price

                results.append((sym, cnt, win_start_price, latest_price, win_start_ts, last_ts))

            # 3. 排序: cnt DESC, last_alert DESC, 截 limit
            results.sort(key=lambda x: (-x[1], -x[5].timestamp()))

            # 🐾 出口转字符串: 内部用 datetime 是为了算间隔方便, 渲染层继续用 ISO 字符串 (向后兼容)
            out = []
            for r in results[:limit]:
                sym, cnt, fp, lp, ws_ts, last_ts = r
                ws_str = ws_ts.strftime('%Y-%m-%d %H:%M:%S') if isinstance(ws_ts, _dt) else ws_ts
                out.append((sym, cnt, fp, lp, ws_str))
            return out

        except Exception as e:
            logger.error(f"❌ [get_today_symbols_top] 查询失败：{e}", exc_info=True)
            return []

    # --- 方法 B: 缓冲发送 (进篮子) ---

    async def _save_alert_to_db(
        self, msg, priority="buffered", *, symbol=None, signal_type=None, price=None
    ):
        """保存原始警报。允许调用方传入显式字段，避免通过消息文本猜测 OI 类型。"""
        import sqlite3

        try:
            if not symbol:
                symbol_match = re.search(r"<b>([^<>]+USDT)</b>", msg)
                symbol = symbol_match.group(1) if symbol_match else None
            if not symbol:
                return None, None, None

            # 保留旧消息解析逻辑；新事件路径优先使用显式 signal_type。
            if signal_type is None:
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

            if price is None:
                # 支持科学计数法和正负指数。
                price_match = re.search(r"现价：\s*([0-9.eE+-]+)", msg)
                if not price_match:
                    price_match = re.search(r"现价:\s*([0-9.eE+-]+)", msg)
                if not price_match:
                    price_match = re.search(r"\$([0-9.eE+-]+)", msg)
                if price_match:
                    try:
                        price = float(price_match.group(1))
                    except ValueError:
                        price = None

            from datetime import datetime, timezone, timedelta
            beijing_time = datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")

            def _db_insert():
                conn = sqlite3.connect("/root/CryptoMonitor/alerts.db")
                try:
                    cursor = conn.cursor()
                    cursor.execute(
                        """
                        INSERT INTO alerts (symbol, signal_type, message, price_at_alert, priority, timestamp)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (symbol, signal_type, msg, price, priority, beijing_time),
                    )
                    conn.commit()
                finally:
                    conn.close()

            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, _db_insert)
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

            # 🐾 修复: 原 limited_get 是同步 requests, 会冻结事件循环
            # (每次信号触发 send_chart 都阻塞一次)。改用异步版。
            from shared.binance_limiter import limited_get_async
            raw_data = await limited_get_async(
                self.session,
                url,
                params=params,
                weight=2,
                timeout=10,
            )

            if not raw_data:
                logger.warning(f"❌ 拉 K 线失败：{symbol}")
                return

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
                queued_items = [await self.queue.get()]
                # 稍微聚合一下紧急的，比如 0.5秒内连发了两条暴跌
                for _ in range(2):
                    if self.queue.empty():
                        break
                    queued_items.append(self.queue.get_nowait())

                messages = []
                trace_ids = []
                for item in queued_items:
                    # 兼容旧式直接入队的纯字符串消息。
                    if isinstance(item, dict):
                        message = item.get("text", "")
                        trace_id = item.get("trace_id")
                    else:
                        message = str(item)
                        trace_id = None
                    if message:
                        messages.append(message)
                    if trace_id:
                        trace_ids.append(str(trace_id))

                if not messages:
                    continue
                text = "\n\n".join(messages)
                await self._send_to_tg(text, trace_ids=trace_ids)
                await asyncio.sleep(0.5)  # 紧急通道稍微快点
            except Exception as e:
                logger.error(f"Urgent Worker Err: {e}", exc_info=True)

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
    async def _send_to_tg(self, text, trace_ids=None):
        """
        【修复】加重试上限，避免 TG 不可达时死磕导致连接堆积。
        trace_ids 为本次聚合消息包含的信号追踪 ID。
        """
        trace_ids = trace_ids or []
        trace_label = ",".join(trace_ids) if trace_ids else "-"
        if not text:
            logger.warning("⚠️ [_send_to_tg] 消息为空，跳过")
            return
        if self.session is None or self.session.closed:
            logger.error(
                f"[TG_DELIVERY] trace_ids={trace_label} status=failed reason=session_closed"
            )
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
                        message_id = None
                        try:
                            response_json = json.loads(response_text.decode("utf-8"))
                            message_id = response_json.get("result", {}).get("message_id")
                        except (UnicodeDecodeError, json.JSONDecodeError, AttributeError):
                            pass
                        logger.info(
                            f"[TG_DELIVERY] trace_ids={trace_label} status=success "
                            f"telegram_message_id={message_id or '-'} attempt={attempt} "
                            f"batched_messages={len(trace_ids) or 1}"
                        )
                        return
                    logger.error(
                        f"[TG_DELIVERY] trace_ids={trace_label} status=http_error "
                        f"attempt={attempt}/{MAX_RETRIES} http_status={resp.status} "
                        f"body={response_text[:200]}"
                    )
                    last_err = f"HTTP {resp.status}"
                    # 4xx 是参数问题，不再重试
                    if 400 <= resp.status < 500:
                        break
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logger.warning(
                    f"[TG_DELIVERY] trace_ids={trace_label} status=network_error "
                    f"attempt={attempt}/{MAX_RETRIES} error={e}"
                )
                last_err = str(e)
            # 指数退避后重试
            if attempt < MAX_RETRIES:
                await asyncio.sleep(2 ** attempt)

        # 全部失败：进 dead-letter，不再阻塞
        logger.error(
            f"[TG_DELIVERY] trace_ids={trace_label} status=failed "
            f"attempts={MAX_RETRIES} error={last_err} text={text[:80]}"
        )
        # TODO: 写入本地 dead-letter 文件供事后回溯


notifier = TelegramNotifier()