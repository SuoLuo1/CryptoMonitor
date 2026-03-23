# llm_agent.py
import asyncio
from datetime import datetime

import aiohttp
import logging
from config import DEEPSEEK_API_KEY, DEEPSEEK_BASE_URL, LLM_MODEL

logger = logging.getLogger("LLM")

class DeepSeekAgent:
    def __init__(self):
        self.headers = {
            "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
            "Content-Type": "application/json"
        }
        self.url = f"{DEEPSEEK_BASE_URL}/chat/completions"

    async def analyze_market(self, alerts_list, last_summary=None, last_summary_time=None):
        """
        将一分钟内的报警列表发给 DeepSeek 进行总结
        """
        if not alerts_list:
            return None
        total_count = len(alerts_list)
        # 如果行情太火爆（比如 200条），全发给 AI 会很贵。
        # 策略：只取【最近的 60 条】。因为 15分钟前的消息已经失效了，没必要分析。
        if total_count > 60:
            current_batch = alerts_list[-60:]  # 只取最后 60 条
            logger.warning(f"日志过多({total_count}条)，截取最近 60 条发送。")
        else:
            current_batch = alerts_list
        # 1. 把报警列表拼接成文本
        current_time_str = datetime.now().strftime("%H:%M:%S")
        raw_text = "\n".join(current_batch)
        context_prompt = ""
        if last_summary and last_summary_time:
            # 只取前 1500 个字符，防止 token 爆炸，只要核心结论
            clean_summary = last_summary.replace("🤖 DeepSeek 市场洞察", "").strip()[:1500]
            context_prompt = (
                f"========== 【历史记忆 (生成时间: {last_summary_time}】 ==========\n"
                f"{clean_summary}\n"
                f"================================================\n\n"
            )
        elif last_summary:
            context_prompt = f"========== 【历史记忆】 ==========\n{last_summary}\n\n"
        # 2. 精心设计的 Prompt (人设：高频交易员)
        system_prompt = (
            f"你是一个加密货币高频量化分析师。你的任务是基于【本周期(15m)监控日志】生成快报，并结合【历史记忆】进行连续性推演,当前系统时间是【{current_time_str}】。\n"
            "为了做出最准确的判断，请熟读本系统的【底层算法逻辑】：\n\n"
            
            "【重要原则：去重与防幻觉】\n"
            "1. 合并分析 (优先级最高)：如果一个币种既有【历史记忆】又有【本周期监控日志】，请在第一部分直接结合历史进行综合分析！\n"
            "2. 严禁重复：凡是出现在第一部分的币种，绝对不允许再出现在第二部分的趋势追踪里！\n"
            "3. 趋势追踪：第二部分仅用于那些【本周期无新数据】但在【历史记忆】中存在的旧币种。\n\n"
            "4. 合并同类项：如果出现 >3 个币种有相同信号（如金铲子集群）并且这些币种只有这一个信号时，**必须合并**写成一条！\n"
            
            "【数据来源与指标说明】\n"
            "数据源于币安的websocket接口实时流，汇总于15分钟"
            "1. 暴涨 / 暴跌:\n"
            "   - 来源：1分钟 K线数据。\n"
            "   - 意义：这是最直接的市场结果。\n\n"

            "2. 持仓异动:\n"
            "   - 来源：每 60秒 轮询一次币安 API。\n"
            "   - 逻辑：OI 代表市场上的持仓总量。\n\n"

            "3. 合约爆仓:\n"
            "   - 来源：交易所强制平仓流。\n"
            "   - 意义：这是行情的“燃料”。\n"

            "4. 流动性枯竭:\n"
            "   - 算法：计算 (卖一价 - 买一价) 的价差。\n"
            "   - 意义：当价差超过阈值0.5%时触发。\n\n"

            "5. 正溢价/负溢价:\n"
            "   - 来源：`标记价格` vs `全网指数价格`。\n"
            "   - 意义：当负溢价出现时，代表合约价格小于现货价格，正溢价相反。\n\n"

            "6. 巨鲸成交:\n"
            "   - 来源：(BTC/ETH)：识别 5秒内 的【拆单】累计买入/卖出.(山寨币)：识别单笔大额脉冲成交。\n"
            "   - 意义：代表机构或大户的真实建仓/出货意图。\n\n"

            "7. 阻力薄弱/支撑薄弱:\n"
            "   - 算法：计算拉升/砸盘 1%~1.5%所需的资金成本。\n"
            "   - 意义：阻力最小方向原则。\n"
            "     * 若 拉升成本(低) vs 砸盘成本(高) -> 阻力向上，上方卖压真空，易拉升。\n"
            "     * 若 砸盘成本(低) vs 拉升成本(高) -> 阻力向下，下方承接无力，易暴跌。\n"

            "【分析任务】\n"
            "请对本周期的数据进行【微观诊断】：\n"
            "1. 分类聚合：将同一个币的所有信号（如 DOGE 的暴涨+爆仓+巨鲸）串联起来。\n"
            "2. 板块识别：自动标注币种所属板块 (Meme/AI/L2 等)。\n"
            "3. 逻辑推演：结合新旧数据的简洁判断\n"

            "【输出格式】\n"
            f"第一部分：🤖 DeepSeek 市场洞察 ({current_time_str})\n"
            "(仅列出【本周期日志】中的新异动)结论和依据都要简介明了\n"
            "格式如下:"
            "<币种> (<板块>)\n"
            "结论：<结合新旧数据的综合判断>\n"
            "依据：<本分钟数据> + <历史数据(如有)>\n"
            "第二部分：📉 趋势追踪\n"
            "(仅追踪上个周期有异动、但本周期静默的旧币种)\n"
            "1. 去重检查：写入本栏目前，先检查该币种是否已在第一部分出现过？如果是，跳过。\n"
            "2. **时间衰减描述**：不要只会写'维持判断'。如果连续无新数据，请使用'缩量横盘'、'获利盘消化中'、'动能衰减'、'深度未修复'等更生动的交易术语。\n"
            "格式如下：\n"
            "<旧币种名> (<板块>)：<简洁明了的回顾 + 基于时间推移的最新状态描述>\n"
        )

        user_content = (
            f"{context_prompt}"
            f"========== 【本周期(15m)监控日志】 (数据时间: {current_time_str}) ==========\n"
            f"{raw_text}\n"
            f"====================================================================="
        )

        payload = {
            "model": LLM_MODEL,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content}
            ],
            "stream": False,
            "temperature": 0.5  # 稍微有点创造性，但别胡说
        }

        # 3. 发送请求
        try:
            timeout = aiohttp.ClientTimeout(total=90)

            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(self.url, headers=self.headers, json=payload) as resp:
                    if resp.status == 200:
                        res_json = await resp.json()
                        content = res_json['choices'][0]['message']['content']
                        return content
                    else:
                        err = await resp.text()
                        logger.error(f"DeepSeek API Error ({resp.status}): {err}")
                        return None
        except asyncio.TimeoutError:
            logger.error("LLM Net Err: ⏳ 请求超时 (Timeout)！DeepSeek 生成太慢，已把超时时间延长至 90秒。")
            return None

        except Exception as e:
            logger.error(f"LLM Net Err: {repr(e)}")
            return None