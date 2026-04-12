# CryptoMonitor 警报历史系统

## 📊 功能说明

自动保存所有 Telegram 警报到 SQLite 数据库，积累一段时间后分析哪些信号最准确。

---

## 🛠️ 已安装组件

### 1. 数据库
- **位置**: `/root/CryptoMonitor/alerts.db`
- **表结构**:
```sql
alerts (
    id, timestamp, symbol, signal_type, message,
    price_at_alert, price_1h_after, change_1h_pct,
    is_success, analyzed
)
```

### 2. 自动保存
- **修改文件**: `/root/CryptoMonitor/utils.py`
- **功能**: 每次发送 Telegram 警报前自动保存到数据库

### 3. 定时任务
- **每小时**: 更新警报后续走势 (`update_alert_outcomes.py`)
- **每天 8 点**: 生成分析报告 (`analyze_alerts.py`)

---

## 📈 使用方法

### 查看实时统计
```bash
python3 /root/CryptoMonitor/view_stats.py
```

### 生成详细分析
```bash
python3 /root/CryptoMonitor/analyze_alerts.py
```

### 查看定时任务
```bash
crontab -l
```

### 查看日志
```bash
tail -f /root/CryptoMonitor/outcomes.log
tail -f /root/CryptoMonitor/analysis.log
```

---

## 📊 数据积累流程

### 第 1 天
```
📁 总警报数：~100
✅ 已分析：0 (等待 1h 后)
```

### 第 7 天
```
📁 总警报数：~700
✅ 已分析：~650
🎯 成功率：35%
```

### 第 14 天
```
📁 总警报数：~1400
✅ 已分析：~1350

🎯 发现规律:
- 连续 3 次🚀信号 → 成功率 68%
- 14:00-16:00 信号 → 成功率 45%
- GWEI/DUSK/BR → 成功率 65%
```

---

## 🔍 分析维度

### 1. 按信号类型
- 🚀暴涨
- 📈爬升
- 📉阴跌
- 🩸暴跌
- 🧱阻力薄弱
- 🧱支撑薄弱

### 2. 按币种
- 统计每个币种的成功率
- 找出最准确的币种

### 3. 按时间段
- 哪个时段的信号最准
- 找出最佳交易时间

### 4. 成功案例
- 最近成功的警报
- 涨幅统计

---

## 🎯 未来优化方向

积累 2 周数据后，可以：

1. **量化规则**
   - 连续 3 次🚀信号 → 重点关注
   - 14:00-16:00 时段 → 提高权重
   - 高频成功币种 → 特别监控

2. **推送优化**
   - 只推送高成功率信号
   - 低成功率信号仅记录

3. **评分系统**
   - 综合评分 >80 → 立即推送
   - 综合评分 60-80 → 汇总推送
   - 综合评分 <60 → 不推送

---

## 📝 文件清单

| 文件 | 用途 |
|------|------|
| `alerts.db` | SQLite 数据库 |
| `utils.py` | 已修改，添加保存功能 |
| `update_alert_outcomes.py` | 每小时更新结果 |
| `analyze_alerts.py` | 每天生成报告 |
| `view_stats.py` | 快速查看统计 |
| `outcomes.log` | 更新日志 |
| `analysis.log` | 分析日志 |

---

## ✅ 系统状态

- [x] 数据库创建
- [x] 自动保存启用
- [x] 定时任务配置
- [x] CryptoMonitor 重启
- [ ] 数据积累中...

---

**开始时间**: 2026-04-12  
**预计可用数据**: 2026-04-19 (7 天)  
**首次完整分析**: 2026-04-26 (14 天)
