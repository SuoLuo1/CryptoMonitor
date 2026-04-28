# config.py

# Telegram
TG_BOT_TOKEN = "8462655201:AAEPI0yXTt2LpnwzxybmtmIldF8lWhC9WTo"
TG_CHAT_ID = "2014554417"

DEEPSEEK_API_KEY = "sk-11c03a0e08884bd78bfe0a3f5797fb73" #deepseek 私钥

LLM_MODEL = "deepseek-chat"
DEEPSEEK_BASE_URL = "https://api.deepseek.com/v1"

# 基础过滤
QUOTE_ASSET = "USDT"
MIN_VOLUME_24H = 10000000
MIN_VOLUME_TG = 5000000
MIN_VOLUME_AI = 10000000
BLACKLIST = ['USDC', 'USDE', 'BTCDOM']
KINGS = ['BTCUSDT', 'ETHUSDT']
MAJORS = [
    'SOLUSDT', 'BNBUSDT',
    'XRPUSDT', 'DOGEUSDT', 'TRXUSDT', 'ADAUSDT',
    'LINKUSDT', 'LTCUSDT', 'SUIUSDT', '1000PEPEUSDT',
    'AVAXUSDT', 'WIFUSDT'
]

# 1. 资金费率异常 (负费率)
# 当费率低于 -0.05% 时报警 (通常意味着现货比合约贵很多，有人在现货拉盘)
FUNDING_RATE_NEGATIVE_THRESHOLD = -0.0005

# 当费率高于 0.1% 时报警 (多头太拥挤，可能要回调)
FUNDING_RATE_POSITIVE_THRESHOLD = 0.001

# 2. 期现价差 (Premium)
PREMIUM_POSITIVE_THRESHOLD = 0.01

PREMIUM_NEGATIVE_THRESHOLD = -0.01

WALL_SIZE_ALT = 200000       # 山寨币挂单墙 > 50万U
WALL_SIZE_MAJOR = 1000000    # 主流币挂单墙 > 500万U
WALL_SIZE_KING  = 500000
SPREAD_THRESHOLD = 0.005     # 价差 > 0.5%
PUMP_THRESHOLD = 0.02        # 1分钟涨跌 > 2%
WHALE_MIN_ALT = 100000        # 山寨巨鲸 > 5万U
WHALE_MIN_MAJOR = 500000     # 主流巨鲸 > 50万U
WHALE_MIN_KING  = 1000000
LIQUIDATION_MIN_VAL = 50000  # 爆仓 > 5万U
# --- 持仓量 (Open Interest) 监控阈值 (1分钟变化) ---
# 👑 帝王级 (BTC/ETH): 盘子太大，1% 就是惊天巨变
OI_THRESHOLD_KING = 0.01  # 1%

# 🏰 诸侯级 (SOL/PEPE等): 活跃度高，2% 算异动
OI_THRESHOLD_MAJOR = 0.02 # 2%

# 🐟 平民级 (其他山寨): 波动大，需要 3% 才算异动，防止乱报
OI_THRESHOLD_ALT = 0.03    # 3%

# 最小持仓价值 (过滤垃圾币)
MIN_OI_VALUE = 5000000
BINANCE_HTTP_FUTURES = "https://fapi.binance.com"
# 币安 WebSocket URL（新架构：/market 用于常规市场数据 ticker/kline/markPrice/aggTrade）
BINANCE_WS_FUTURES_COMBINED = "wss://fstream.binance.com/market/stream?streams="
BINANCE_WS_FUTURES_RAW = "wss://fstream.binance.com/market/ws"