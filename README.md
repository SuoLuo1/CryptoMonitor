# CryptoMonitor - 加密货币监控系统

📊 **实时市场监控 + LLM 智能分析**

---

## 📊 核心功能

### 1. 实时监控

- WebSocket 连接 Binance Futures
- 实时价格、成交量、持仓量监控
- 多维度数据采集

### 2. LLM 智能分析

- 集成大语言模型分析
- 市场情绪判断
- 交易信号生成

### 3. 连接管理

- 自动重连机制
- 心跳检测
- 故障恢复

---

## 🔧 技术栈

- **语言**: Python 3.12
- **WebSocket**: Binance Futures WebSocket
- **LLM**: 大语言模型 API
- **数据处理**: pandas, numpy

---

## 📁 项目结构

```
CryptoMonitor/
├── main.py                 # 主程序入口
├── monitors.py             # 监控器实现
├── connection_manager.py   # WebSocket 连接管理
├── llm_agent.py            # LLM 智能代理
├── symbol_manager.py       # 币种管理
├── utils.py                # 工具函数
├── config.py               # 配置文件 (已忽略)
└── SimHei.ttf              # 中文字体 (已忽略)
```

---

## 🚀 快速开始

### 1. 安装依赖

```bash
cd /root/CryptoMonitor
python3 -m venv venv
source venv/bin/activate
pip install websockets requests pandas numpy aiohttp
```

### 2. 配置

```bash
# 编辑配置文件
vim config.py

# 关键配置
LLM_API_KEY = "YOUR_LLM_API_KEY"
LLM_MODEL = "qwen-plus"
BINANCE_WS_URL = "wss://fstream.binance.com/ws"
```

### 3. 启动

```bash
# 手动启动
source venv/bin/activate
python -u main.py > monitor.log 2>&1 &

# 或使用 systemd
systemctl enable cryptomonitor
systemctl start cryptomonitor
```

### 4. 查看状态

```bash
# 查看服务状态
systemctl status cryptomonitor

# 查看日志
tail -f monitor.log

# 查看进程
ps aux | grep "python.*main.py"
```

---

## 📈 监控维度

### 1. 价格监控

- 实时价格更新
- 价格异动检测
- 突破信号识别

### 2. 成交量监控

- 实时成交量
- 量比计算
- 放量信号检测

### 3. 持仓量监控

- 未平仓合约数
- 持仓量变化
- 大户动向分析

---

## 🔐 安全配置

### .gitignore 已包含

- ✅ 配置文件 (含 API Key)
- ✅ 日志文件
- ✅ 数据文件
- ✅ 字体文件 (SimHei.ttf)
- ✅ 虚拟环境

### 敏感信息

**不要上传到 GitHub**:
```bash
config.py           # 含 LLM API Key
*.log               # 日志文件
data/*              # 数据文件
SimHei.ttf          # 中文字体 (文件大)
```

---

## 📝 模块说明

### main.py

主程序入口，负责：
- 初始化各模块
- 启动监控循环
- 异常处理

### monitors.py

监控器实现，负责：
- 多维度数据采集
- 实时监控逻辑
- 信号生成

### connection_manager.py

连接管理，负责：
- WebSocket 连接
- 自动重连
- 心跳检测

### llm_agent.py

LLM 代理，负责：
- 调用大语言模型 API
- 市场情绪分析
- 交易建议生成

### symbol_manager.py

币种管理，负责：
- 币种列表维护
- 成交额筛选
- 动态添加/删除

### utils.py

工具函数，负责：
- 数据处理
- 图表生成
- 日志记录

---

## 🐛 已知问题

1. **字体文件大** - SimHei.ttf 约 9MB，已加入 .gitignore
2. **LLM API 成本** - 高频调用可能产生较高费用
3. **WebSocket 断连** - 已添加自动重连，但极端情况可能丢失数据

---

## 📝 版本历史

### v1.0.0 (2026-03-23)

- ✅ WebSocket 实时监控
- ✅ LLM 智能分析
- ✅ 多维度数据采集
- ✅ 自动重连机制
- ✅ 币种动态管理

---

## 📞 支持

- **GitHub**: https://github.com/SuoLuo1/CryptoMonitor
- **问题反馈**: 提交 Issue

---

## 📄 License

MIT License
